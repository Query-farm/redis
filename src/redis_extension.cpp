#define DUCKDB_EXTENSION_MAIN

#include "redis_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include <boost/asio.hpp>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include "query_farm_telemetry.hpp"

namespace duckdb {

using boost::asio::ip::tcp;

//===--------------------------------------------------------------------===//
// DuckDB API shims (the only code that differs between the v1.5 and v2.0 branches)
//===--------------------------------------------------------------------===//
static void ToUnified(Vector &vector, idx_t count, UnifiedVectorFormat &format) {
	(void)count;
	vector.ToUnifiedFormat(format);
}

// Sequential writer for the result vector of a scalar function
template <class T>
class RedisResultWriter {
public:
	RedisResultWriter(Vector &result, idx_t count) : writer(FlatVector::Writer<T>(result, count)) {
	}
	void Write(const T &value) {
		writer.WriteValue(value);
	}
	void WriteNull() {
		writer.WriteNull();
	}

private:
	VectorWriter<T> writer;
};

template <>
class RedisResultWriter<string_t> {
public:
	RedisResultWriter(Vector &result, idx_t count) : writer(FlatVector::Writer<string_t>(result, count)) {
	}
	void Write(const std::string &value) {
		writer.WriteValue(string_t(value.data(), UnsafeNumericCast<uint32_t>(value.size())));
	}
	void WriteNull() {
		writer.WriteNull();
	}

private:
	VectorWriter<string_t> writer;
};

static void SetOutputValue(DataChunk &output, idx_t col, idx_t row, const Value &value) {
	output.data[col].SetValue(row, value);
}

static void SetOutputCardinality(DataChunk &output, idx_t count) {
	output.SetChildCardinality(count);
}

using redis_column_names_t = vector<Identifier>;

//===--------------------------------------------------------------------===//
// RESP protocol
//===--------------------------------------------------------------------===//

// A parsed RESP reply. RESP3 aggregate types (map, set, push) are flattened into arrays.
struct RedisReply {
	enum class Type { RESP_STRING, RESP_STATUS, RESP_ERROR, RESP_INTEGER, RESP_NIL, RESP_ARRAY };

	Type type = Type::RESP_NIL;
	std::string str;
	int64_t integer = 0;
	std::vector<RedisReply> elements;

	bool IsNil() const {
		return type == Type::RESP_NIL;
	}
	bool IsError() const {
		return type == Type::RESP_ERROR;
	}

	static const char *TypeName(Type type) {
		switch (type) {
		case Type::RESP_STRING:
			return "bulk string";
		case Type::RESP_STATUS:
			return "simple string";
		case Type::RESP_ERROR:
			return "error";
		case Type::RESP_INTEGER:
			return "integer";
		case Type::RESP_NIL:
			return "nil";
		case Type::RESP_ARRAY:
			return "array";
		}
		return "unknown";
	}

	// The reply as text: bulk strings, simple strings and integers
	std::string AsString() const {
		switch (type) {
		case Type::RESP_STRING:
		case Type::RESP_STATUS:
			return str;
		case Type::RESP_INTEGER:
			return std::to_string(integer);
		default:
			throw InvalidInputException("Unexpected Redis reply: expected a string, got %s", TypeName(type));
		}
	}

	int64_t AsInteger() const {
		if (type != Type::RESP_INTEGER) {
			throw InvalidInputException("Unexpected Redis reply: expected an integer, got %s", TypeName(type));
		}
		return integer;
	}

	const std::vector<RedisReply> &AsArray() const {
		if (type != Type::RESP_ARRAY) {
			throw InvalidInputException("Unexpected Redis reply: expected an array, got %s", TypeName(type));
		}
		return elements;
	}

	// SCAN / HSCAN reply: [cursor, [item, item, ...]]
	void AsScanReply(std::string &cursor, std::vector<std::string> &items) const {
		auto &parts = AsArray();
		if (parts.size() != 2) {
			throw InvalidInputException("Unexpected Redis SCAN reply: expected 2 elements, got %llu",
			                            (unsigned long long)parts.size());
		}
		cursor = parts[0].AsString();
		items.clear();
		for (auto &item : parts[1].AsArray()) {
			items.push_back(item.AsString());
		}
	}
};

class RedisProtocol {
public:
	// Encode a command as a RESP array of bulk strings
	static std::string formatCommand(const std::vector<std::string> &args) {
		std::string cmd = "*" + std::to_string(args.size()) + "\r\n";
		for (auto &arg : args) {
			cmd += "$" + std::to_string(arg.size()) + "\r\n";
			cmd += arg;
			cmd += "\r\n";
		}
		return cmd;
	}
};

// Incremental RESP parser. Bytes are appended with Feed(); TryParse() returns true once a complete reply is
// buffered. Handles nested arrays, null bulk strings/arrays and the RESP3 types.
class RedisReplyParser {
public:
	void Feed(const char *data, size_t len) {
		buffer_.append(data, len);
	}

	bool TryParse(RedisReply &reply) {
		size_t pos = pos_;
		if (!ParseValue(pos, reply, 0)) {
			return false;
		}
		pos_ = pos;
		if (pos_ == buffer_.size()) {
			buffer_.clear();
			pos_ = 0;
		}
		return true;
	}

	void Reset() {
		buffer_.clear();
		pos_ = 0;
	}

private:
	static constexpr int MAX_DEPTH = 128;
	static constexpr int64_t MAX_BULK_LENGTH = 512LL * 1024 * 1024;

	bool ReadLine(size_t &pos, std::string &line) {
		auto end = buffer_.find("\r\n", pos);
		if (end == std::string::npos) {
			return false;
		}
		line = buffer_.substr(pos, end - pos);
		pos = end + 2;
		return true;
	}

	static int64_t ParseInteger(const std::string &text) {
		if (text.empty()) {
			throw InvalidInputException("Redis protocol error: empty integer");
		}
		errno = 0;
		char *end = nullptr;
		auto value = std::strtoll(text.c_str(), &end, 10);
		if (errno != 0 || end != text.c_str() + text.size()) {
			throw InvalidInputException("Redis protocol error: invalid integer '%s'", text);
		}
		return value;
	}

	// Bulk payload of `length` bytes followed by CRLF
	bool ReadBulk(size_t &pos, int64_t length, std::string &out) {
		if (length > MAX_BULK_LENGTH) {
			throw InvalidInputException("Redis protocol error: bulk string too large (%lld bytes)", (long long)length);
		}
		auto len = static_cast<size_t>(length);
		if (buffer_.size() - pos < len + 2) {
			return false;
		}
		if (buffer_[pos + len] != '\r' || buffer_[pos + len + 1] != '\n') {
			throw InvalidInputException("Redis protocol error: bulk string not terminated by CRLF");
		}
		out = buffer_.substr(pos, len);
		pos += len + 2;
		return true;
	}

	bool ParseAggregate(size_t &pos, int64_t count, RedisReply &reply, int depth) {
		reply.type = RedisReply::Type::RESP_ARRAY;
		reply.elements.clear();
		reply.elements.reserve(static_cast<size_t>(std::min<int64_t>(count, 1024)));
		for (int64_t i = 0; i < count; i++) {
			RedisReply element;
			if (!ParseValue(pos, element, depth + 1)) {
				return false;
			}
			reply.elements.push_back(std::move(element));
		}
		return true;
	}

	bool ParseValue(size_t &pos, RedisReply &reply, int depth) {
		if (depth > MAX_DEPTH) {
			throw InvalidInputException("Redis protocol error: reply nested too deeply");
		}
		std::string line;
		if (!ReadLine(pos, line)) {
			return false;
		}
		if (line.empty()) {
			throw InvalidInputException("Redis protocol error: empty reply line");
		}
		auto type = line[0];
		auto rest = line.substr(1);
		reply = RedisReply();
		switch (type) {
		case '+': // simple string
			reply.type = RedisReply::Type::RESP_STATUS;
			reply.str = rest;
			return true;
		case '-': // error
			reply.type = RedisReply::Type::RESP_ERROR;
			reply.str = rest;
			return true;
		case ':': // integer
			reply.type = RedisReply::Type::RESP_INTEGER;
			reply.integer = ParseInteger(rest);
			return true;
		case '_': // RESP3 null
			reply.type = RedisReply::Type::RESP_NIL;
			return true;
		case '#': // RESP3 boolean
			reply.type = RedisReply::Type::RESP_INTEGER;
			reply.integer = rest == "t" ? 1 : 0;
			return true;
		case ',': // RESP3 double
		case '(': // RESP3 big number
			reply.type = RedisReply::Type::RESP_STATUS;
			reply.str = rest;
			return true;
		case '$':   // bulk string
		case '!':   // RESP3 bulk error
		case '=': { // RESP3 verbatim string ("txt:" prefix)
			auto length = ParseInteger(rest);
			if (length < 0) {
				reply.type = RedisReply::Type::RESP_NIL;
				return true;
			}
			if (!ReadBulk(pos, length, reply.str)) {
				return false;
			}
			if (type == '!') {
				reply.type = RedisReply::Type::RESP_ERROR;
			} else {
				reply.type = RedisReply::Type::RESP_STRING;
				if (type == '=' && reply.str.size() >= 4 && reply.str[3] == ':') {
					reply.str = reply.str.substr(4);
				}
			}
			return true;
		}
		case '*':   // array
		case '~':   // RESP3 set
		case '>': { // RESP3 push
			auto count = ParseInteger(rest);
			if (count < 0) {
				reply.type = RedisReply::Type::RESP_NIL;
				return true;
			}
			return ParseAggregate(pos, count, reply, depth);
		}
		case '%': { // RESP3 map, flattened to [key, value, key, value, ...]
			auto count = ParseInteger(rest);
			if (count < 0) {
				reply.type = RedisReply::Type::RESP_NIL;
				return true;
			}
			return ParseAggregate(pos, count * 2, reply, depth);
		}
		case '|': { // RESP3 attribute: skip it, the actual reply follows
			auto count = ParseInteger(rest);
			RedisReply attributes;
			if (!ParseAggregate(pos, count * 2, attributes, depth)) {
				return false;
			}
			return ParseValue(pos, reply, depth);
		}
		default:
			throw InvalidInputException("Redis protocol error: unknown reply type '%c'", type);
		}
	}

	std::string buffer_;
	size_t pos_ = 0;
};

//===--------------------------------------------------------------------===//
// Connections
//===--------------------------------------------------------------------===//
class RedisConnection {
public:
	RedisConnection(const std::string &host, const std::string &port, const std::string &password = "")
	    : io_context_(), socket_(io_context_) {
		try {
			tcp::resolver resolver(io_context_);
			auto endpoints = resolver.resolve(host, port);
			boost::asio::connect(socket_, endpoints);

			if (!password.empty()) {
				execute({"AUTH", password});
			}
		} catch (std::exception &e) {
			throw InvalidInputException("Redis connection error: %s", ErrorData(e).RawMessage());
		}
	}

	// Send a command and read its complete reply. Error replies throw unless allow_error is set.
	RedisReply execute(const std::vector<std::string> &args, bool allow_error = false) {
		RedisReply reply;
		{
			std::lock_guard<std::mutex> lock(mutex_);
			if (broken_) {
				throw InvalidInputException("Redis execution error: connection is broken");
			}
			try {
				boost::asio::write(socket_, boost::asio::buffer(RedisProtocol::formatCommand(args)));
				char chunk[16384];
				while (!parser_.TryParse(reply)) {
					auto n = socket_.read_some(boost::asio::buffer(chunk, sizeof(chunk)));
					parser_.Feed(chunk, n);
				}
			} catch (std::exception &e) {
				// The stream position is unknown now, so the connection can't be reused
				broken_ = true;
				parser_.Reset();
				boost::system::error_code ignored;
				socket_.close(ignored);
				throw InvalidInputException("Redis execution error: %s", ErrorData(e).RawMessage());
			}
		}
		if (reply.IsError() && !allow_error) {
			throw InvalidInputException("Redis error: %s", reply.str);
		}
		return reply;
	}

	bool isBroken() {
		std::lock_guard<std::mutex> lock(mutex_);
		return broken_;
	}

private:
	boost::asio::io_context io_context_;
	tcp::socket socket_;
	std::mutex mutex_;
	RedisReplyParser parser_;
	bool broken_ = false;
};

// Connection pool manager
class ConnectionPool {
public:
	static ConnectionPool &getInstance() {
		static ConnectionPool instance;
		return instance;
	}

	std::shared_ptr<RedisConnection> getConnection(const std::string &host, const std::string &port,
	                                               const std::string &password = "") {
		std::string key = host + ":" + port;
		std::lock_guard<std::mutex> lock(mutex_);

		auto it = connections_.find(key);
		if (it == connections_.end() || it->second->isBroken()) {
			auto conn = std::make_shared<RedisConnection>(host, port, password);
			connections_[key] = conn;
			return conn;
		}
		return it->second;
	}

private:
	ConnectionPool() {
	}
	std::mutex mutex_;
	std::unordered_map<std::string, std::shared_ptr<RedisConnection>> connections_;
};

static bool GetRedisSecret(ClientContext &context, const string &secret_name, string &host, string &port,
                           string &password) {
	auto &secret_manager = SecretManager::Get(context);
	try {
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto secret_match = secret_manager.LookupSecret(transaction, secret_name, "redis");
		if (secret_match.HasMatch()) {
			auto &secret = secret_match.GetSecret();
			if (secret.GetType() != "redis") {
				throw InvalidInputException("Invalid secret type. Expected 'redis', got '%s'", secret.GetType());
			}
			const auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&secret);
			if (!kv_secret) {
				throw InvalidInputException("Invalid secret format for 'redis' secret");
			}

			Value host_val, port_val, password_val;
			if (!kv_secret->TryGetValue("host", host_val) || !kv_secret->TryGetValue("port", port_val) ||
			    !kv_secret->TryGetValue("password", password_val)) {
				return false;
			}

			host = host_val.ToString();
			port = port_val.ToString();
			password = password_val.ToString();
			return true;
		}
	} catch (...) {
		return false;
	}
	return false;
}

static std::shared_ptr<RedisConnection> GetRedisConnection(ClientContext &context, const string &secret_name) {
	string host, port, password;
	if (!GetRedisSecret(context, secret_name, host, port, password)) {
		throw InvalidInputException("Redis secret not found");
	}
	return ConnectionPool::getInstance().getConnection(host, port, password);
}

//===--------------------------------------------------------------------===//
// Scalar function execution
//===--------------------------------------------------------------------===//

// Per-row access to the arguments of a scalar function
class RedisArgs {
public:
	explicit RedisArgs(DataChunk &args) : formats_(args.ColumnCount()) {
		for (idx_t col = 0; col < args.ColumnCount(); col++) {
			ToUnified(args.data[col], args.size(), formats_[col]);
		}
	}

	bool AnyNull(idx_t row) const {
		for (auto &format : formats_) {
			if (!format.validity.RowIsValid(format.sel->get_index(row))) {
				return true;
			}
		}
		return false;
	}

	std::string GetString(idx_t col, idx_t row) const {
		auto &format = formats_[col];
		return UnifiedVectorFormat::GetData<string_t>(format)[format.sel->get_index(row)].GetString();
	}

	int64_t GetBigint(idx_t col, idx_t row) const {
		auto &format = formats_[col];
		return UnifiedVectorFormat::GetData<int64_t>(format)[format.sel->get_index(row)];
	}

private:
	vector<UnifiedVectorFormat> formats_;
};

template <class T>
struct RedisResultValue {
	using type = T;
};
template <>
struct RedisResultValue<string_t> {
	using type = std::string;
};

// Runs `op` for every row. The secret name is always the last argument and is read per row; connections are
// looked up once per distinct secret. A NULL argument gives a NULL result. `op` returns false for a NULL result.
template <class T, class OP>
static void RedisExecute(DataChunk &args, ExpressionState &state, Vector &result, const char *command, OP op) {
	const idx_t count = args.size();
	const idx_t secret_col = args.ColumnCount() - 1;
	RedisArgs input(args);
	std::unordered_map<std::string, std::shared_ptr<RedisConnection>> connections;
	RedisResultWriter<T> writer(result, count);
	for (idx_t row = 0; row < count; row++) {
		if (input.AnyNull(row)) {
			writer.WriteNull();
			continue;
		}
		typename RedisResultValue<T>::type value;
		bool valid;
		try {
			auto secret_name = input.GetString(secret_col, row);
			auto entry = connections.find(secret_name);
			if (entry == connections.end()) {
				entry = connections.emplace(secret_name, GetRedisConnection(state.GetContext(), secret_name)).first;
			}
			valid = op(input, row, *entry->second, value);
		} catch (std::exception &e) {
			throw InvalidInputException("Redis %s error: %s", command, ErrorData(e).RawMessage());
		}
		if (valid) {
			writer.Write(value);
		} else {
			writer.WriteNull();
		}
	}
}

static std::string JoinStrings(const std::vector<std::string> &values, const std::string &separator) {
	std::string joined;
	for (size_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			joined += separator;
		}
		joined += values[i];
	}
	return joined;
}

// redis_get(key, secret) -> VARCHAR, NULL when the key does not exist
static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "GET",
	                       [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		                       auto reply = conn.execute({"GET", in.GetString(0, row)});
		                       if (reply.IsNil()) {
			                       return false;
		                       }
		                       out = reply.AsString();
		                       return true;
	                       });
}

// redis_set(key, value, secret) -> VARCHAR ('OK')
static void RedisSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "SET",
	                       [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		                       auto reply = conn.execute({"SET", in.GetString(0, row), in.GetString(1, row)});
		                       if (reply.IsNil()) {
			                       return false;
		                       }
		                       out = reply.AsString();
		                       return true;
	                       });
}

// redis_hget(key, field, secret) -> VARCHAR, NULL when the field does not exist
static void RedisHGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "HGET",
	                       [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		                       auto reply = conn.execute({"HGET", in.GetString(0, row), in.GetString(1, row)});
		                       if (reply.IsNil()) {
			                       return false;
		                       }
		                       out = reply.AsString();
		                       return true;
	                       });
}

// redis_hset(key, field, value, secret) -> BIGINT (number of fields added)
static void RedisHSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(
	    args, state, result, "HSET", [](const RedisArgs &in, idx_t row, RedisConnection &conn, int64_t &out) {
		    out = conn.execute({"HSET", in.GetString(0, row), in.GetString(1, row), in.GetString(2, row)}).AsInteger();
		    return true;
	    });
}

// redis_lpush(key, value, secret) -> BIGINT (list length after the push)
static void RedisLPushFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(args, state, result, "LPUSH",
	                      [](const RedisArgs &in, idx_t row, RedisConnection &conn, int64_t &out) {
		                      out = conn.execute({"LPUSH", in.GetString(0, row), in.GetString(1, row)}).AsInteger();
		                      return true;
	                      });
}

// redis_lrange(key, start, stop, secret) -> VARCHAR (comma-separated elements)
static void RedisLRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "LRANGE", [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		    auto reply = conn.execute({"LRANGE", in.GetString(0, row), std::to_string(in.GetBigint(1, row)),
		                               std::to_string(in.GetBigint(2, row))});
		    std::vector<std::string> values;
		    for (auto &element : reply.AsArray()) {
			    values.push_back(element.AsString());
		    }
		    out = JoinStrings(values, ",");
		    return true;
	    });
}

// redis_mget(keys, secret) -> VARCHAR (comma-separated values, empty for missing keys so positions line up)
static void RedisMGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "MGET", [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		    std::vector<std::string> command {"MGET"};
		    std::string key_list = in.GetString(0, row);
		    size_t start = 0;
		    while (true) {
			    auto comma = key_list.find(',', start);
			    auto key = key_list.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
			    if (!key.empty()) {
				    command.push_back(key);
			    }
			    if (comma == std::string::npos) {
				    break;
			    }
			    start = comma + 1;
		    }
		    if (command.size() == 1) {
			    out = "";
			    return true;
		    }
		    auto reply = conn.execute(command);
		    std::vector<std::string> values;
		    for (auto &element : reply.AsArray()) {
			    values.push_back(element.IsNil() ? std::string() : element.AsString());
		    }
		    out = JoinStrings(values, ",");
		    return true;
	    });
}

// redis_scan(cursor, pattern, count, secret) -> VARCHAR ('next_cursor:key1,key2,...')
static void RedisScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "SCAN",
	                       [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		                       auto reply = conn.execute({"SCAN", in.GetString(0, row), "MATCH", in.GetString(1, row),
		                                                  "COUNT", std::to_string(in.GetBigint(2, row))});
		                       std::string cursor;
		                       std::vector<std::string> keys;
		                       reply.AsScanReply(cursor, keys);
		                       out = cursor + ":" + JoinStrings(keys, ",");
		                       return true;
	                       });
}

// redis_hscan(key, cursor, pattern, count, secret) -> VARCHAR ('next_cursor:field1=value1,field2=value2,...')
static void RedisHScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "HSCAN", [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		    auto reply = conn.execute({"HSCAN", in.GetString(0, row), in.GetString(1, row), "MATCH",
		                               in.GetString(2, row), "COUNT", std::to_string(in.GetBigint(3, row))});
		    std::string cursor;
		    std::vector<std::string> kvs;
		    reply.AsScanReply(cursor, kvs);
		    std::vector<std::string> pairs;
		    for (size_t i = 0; i + 1 < kvs.size(); i += 2) {
			    pairs.push_back(kvs[i] + "=" + kvs[i + 1]);
		    }
		    out = cursor + ":" + JoinStrings(pairs, ",");
		    return true;
	    });
}

// redis_del(key, secret) -> BOOLEAN
static void RedisDelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(args, state, result, "DEL",
	                   [](const RedisArgs &in, idx_t row, RedisConnection &conn, bool &out) {
		                   out = conn.execute({"DEL", in.GetString(0, row)}).AsInteger() > 0;
		                   return true;
	                   });
}

// redis_exists(key, secret) -> BOOLEAN
static void RedisExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(args, state, result, "EXISTS",
	                   [](const RedisArgs &in, idx_t row, RedisConnection &conn, bool &out) {
		                   out = conn.execute({"EXISTS", in.GetString(0, row)}).AsInteger() > 0;
		                   return true;
	                   });
}

// redis_type(key, secret) -> VARCHAR
static void RedisTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "TYPE",
	                       [](const RedisArgs &in, idx_t row, RedisConnection &conn, std::string &out) {
		                       out = conn.execute({"TYPE", in.GetString(0, row)}).AsString();
		                       return true;
	                       });
}

// redis_expire(key, seconds, secret) -> BOOLEAN
static void RedisExpireFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "EXPIRE", [](const RedisArgs &in, idx_t row, RedisConnection &conn, bool &out) {
		    out = conn.execute({"EXPIRE", in.GetString(0, row), std::to_string(in.GetBigint(1, row))}).AsInteger() == 1;
		    return true;
	    });
}

// redis_ttl(key, secret) -> BIGINT (-2 if the key does not exist, -1 if it has no expiry)
static void RedisTTLFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(args, state, result, "TTL",
	                      [](const RedisArgs &in, idx_t row, RedisConnection &conn, int64_t &out) {
		                      out = conn.execute({"TTL", in.GetString(0, row)}).AsInteger();
		                      return true;
	                      });
}

// redis_expireat(key, timestamp, secret) -> BOOLEAN
static void RedisExpireAtFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "EXPIREAT", [](const RedisArgs &in, idx_t row, RedisConnection &conn, bool &out) {
		    out =
		        conn.execute({"EXPIREAT", in.GetString(0, row), std::to_string(in.GetBigint(1, row))}).AsInteger() == 1;
		    return true;
	    });
}

//===--------------------------------------------------------------------===//
// Table functions (bind data holds the arguments, iteration state lives in the global state)
//===--------------------------------------------------------------------===//
struct RedisTableBindData : public TableFunctionData {
	std::string host, port, password;

	void LookupSecret(ClientContext &context, const std::string &secret_name) {
		if (!GetRedisSecret(context, secret_name, host, port, password)) {
			throw InvalidInputException("Redis secret not found");
		}
	}
	std::shared_ptr<RedisConnection> Connect() const {
		return ConnectionPool::getInstance().getConnection(host, port, password);
	}
};

// redis_hscan_over_scan(scan_pattern, hscan_pattern, count, secret) -> (key, field, value)
struct RedisHScanOverScanBindData : public RedisTableBindData {
	std::string scan_pattern;
	std::string hscan_pattern;
	int64_t count;
};

struct RedisHScanOverScanState : public GlobalTableFunctionState {
	std::shared_ptr<RedisConnection> conn;
	// SCAN over the keys
	std::string scan_cursor = "0";
	bool scan_done = false;
	std::vector<std::string> keys;
	size_t key_idx = 0;
	std::unordered_set<std::string> seen_keys;
	// HSCAN over the current key
	bool have_key = false;
	std::string key;
	std::string hscan_cursor = "0";
	bool hscan_done = true;
	std::vector<std::string> kvs;
	size_t kv_idx = 0;
	std::unordered_set<std::string> seen_fields;
};

static unique_ptr<FunctionData> RedisHScanOverScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, redis_column_names_t &names) {
	auto result = make_uniq<RedisHScanOverScanBindData>();
	result->scan_pattern = input.inputs[0].ToString();
	result->hscan_pattern = input.inputs[1].ToString();
	result->count = input.inputs[2].GetValue<int64_t>();
	result->LookupSecret(context, input.inputs[3].ToString());
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"key", "field", "value"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RedisHScanOverScanInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto state = make_uniq<RedisHScanOverScanState>();
	state->conn = input.bind_data->Cast<RedisHScanOverScanBindData>().Connect();
	return std::move(state);
}

static void RedisHScanOverScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<RedisHScanOverScanBindData>();
	auto &state = data_p.global_state->Cast<RedisHScanOverScanState>();
	auto count = std::to_string(bind.count);
	idx_t out_row = 0;
	while (out_row < STANDARD_VECTOR_SIZE) {
		// Emit buffered field/value pairs of the current key (HSCAN may repeat a field, emit it once)
		if (state.kv_idx + 1 < state.kvs.size()) {
			auto &field = state.kvs[state.kv_idx];
			auto &value = state.kvs[state.kv_idx + 1];
			state.kv_idx += 2;
			if (state.seen_fields.insert(field).second) {
				SetOutputValue(output, 0, out_row, Value(state.key));
				SetOutputValue(output, 1, out_row, Value(field));
				SetOutputValue(output, 2, out_row, Value(value));
				out_row++;
			}
			continue;
		}
		// Fetch the next HSCAN page of the current key
		if (state.have_key && !state.hscan_done) {
			auto reply = state.conn->execute(
			    {"HSCAN", state.key, state.hscan_cursor, "MATCH", bind.hscan_pattern, "COUNT", count}, true);
			if (reply.IsError()) {
				// Keys matching the pattern that aren't hashes are skipped
				if (reply.str.rfind("WRONGTYPE", 0) != 0) {
					throw InvalidInputException("Redis error: %s", reply.str);
				}
				state.kvs.clear();
				state.hscan_done = true;
			} else {
				reply.AsScanReply(state.hscan_cursor, state.kvs);
				state.hscan_done = state.hscan_cursor == "0";
			}
			state.kv_idx = 0;
			continue;
		}
		// Move on to the next key (SCAN may repeat a key, visit it once)
		if (state.key_idx < state.keys.size()) {
			auto &next_key = state.keys[state.key_idx++];
			if (!state.seen_keys.insert(next_key).second) {
				continue;
			}
			state.have_key = true;
			state.key = next_key;
			state.hscan_cursor = "0";
			state.hscan_done = false;
			state.kvs.clear();
			state.kv_idx = 0;
			state.seen_fields.clear();
			continue;
		}
		// Fetch the next SCAN page
		if (!state.scan_done) {
			auto reply = state.conn->execute({"SCAN", state.scan_cursor, "MATCH", bind.scan_pattern, "COUNT", count});
			reply.AsScanReply(state.scan_cursor, state.keys);
			state.key_idx = 0;
			state.scan_done = state.scan_cursor == "0";
			continue;
		}
		break;
	}
	SetOutputCardinality(output, out_row);
}

// redis_keys(pattern, secret) -> (key)
struct RedisKeysBindData : public RedisTableBindData {
	std::string pattern;
};

struct RedisKeysState : public GlobalTableFunctionState {
	std::shared_ptr<RedisConnection> conn;
	std::string cursor = "0";
	bool scan_done = false;
	std::vector<std::string> keys;
	size_t key_idx = 0;
	std::unordered_set<std::string> seen;
};

static unique_ptr<FunctionData> RedisKeysBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, redis_column_names_t &names) {
	auto result = make_uniq<RedisKeysBindData>();
	result->pattern = input.inputs[0].ToString();
	result->LookupSecret(context, input.inputs[1].ToString());
	return_types = {LogicalType::VARCHAR};
	names = {"key"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RedisKeysInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RedisKeysState>();
	state->conn = input.bind_data->Cast<RedisKeysBindData>().Connect();
	return std::move(state);
}

static void RedisKeysFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<RedisKeysBindData>();
	auto &state = data_p.global_state->Cast<RedisKeysState>();
	idx_t out_row = 0;
	while (out_row < STANDARD_VECTOR_SIZE) {
		if (state.key_idx < state.keys.size()) {
			auto &key = state.keys[state.key_idx++];
			// SCAN may return a key more than once
			if (state.seen.insert(key).second) {
				SetOutputValue(output, 0, out_row++, Value(key));
			}
			continue;
		}
		if (state.scan_done) {
			break;
		}
		auto reply = state.conn->execute({"SCAN", state.cursor, "MATCH", bind.pattern, "COUNT", "100"});
		reply.AsScanReply(state.cursor, state.keys);
		state.key_idx = 0;
		state.scan_done = state.cursor == "0";
	}
	SetOutputCardinality(output, out_row);
}

// Shared state for table functions that fetch one reply and emit it
struct RedisFetchState : public GlobalTableFunctionState {
	std::shared_ptr<RedisConnection> conn;
	std::vector<std::string> values;
	size_t idx = 0;
	bool fetched = false;
};

static unique_ptr<GlobalTableFunctionState> RedisFetchInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RedisFetchState>();
	state->conn = input.bind_data->Cast<RedisTableBindData>().Connect();
	return std::move(state);
}

static std::vector<std::string> ReplyStrings(const RedisReply &reply) {
	std::vector<std::string> values;
	for (auto &element : reply.AsArray()) {
		values.push_back(element.AsString());
	}
	return values;
}

// redis_hgetall(key, secret) -> (field, value)
struct RedisHGetAllBindData : public RedisTableBindData {
	std::string key;
};

static unique_ptr<FunctionData> RedisHGetAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, redis_column_names_t &names) {
	auto result = make_uniq<RedisHGetAllBindData>();
	result->key = input.inputs[0].ToString();
	result->LookupSecret(context, input.inputs[1].ToString());
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"field", "value"};
	return std::move(result);
}

static void RedisHGetAllFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<RedisHGetAllBindData>();
	auto &state = data_p.global_state->Cast<RedisFetchState>();
	if (!state.fetched) {
		state.values = ReplyStrings(state.conn->execute({"HGETALL", bind.key}));
		state.fetched = true;
	}
	idx_t out_row = 0;
	while (state.idx + 1 < state.values.size() && out_row < STANDARD_VECTOR_SIZE) {
		SetOutputValue(output, 0, out_row, Value(state.values[state.idx]));
		SetOutputValue(output, 1, out_row, Value(state.values[state.idx + 1]));
		state.idx += 2;
		out_row++;
	}
	SetOutputCardinality(output, out_row);
}

// redis_lrange_table(key, start, stop, secret) -> (index, value)
struct RedisLRangeTableBindData : public RedisTableBindData {
	std::string key;
	int64_t start;
	int64_t stop;
};

static unique_ptr<FunctionData> RedisLRangeTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, redis_column_names_t &names) {
	auto result = make_uniq<RedisLRangeTableBindData>();
	result->key = input.inputs[0].ToString();
	result->start = input.inputs[1].GetValue<int64_t>();
	result->stop = input.inputs[2].GetValue<int64_t>();
	result->LookupSecret(context, input.inputs[3].ToString());
	return_types = {LogicalType::BIGINT, LogicalType::VARCHAR};
	names = {"index", "value"};
	return std::move(result);
}

static void RedisLRangeTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind = data_p.bind_data->Cast<RedisLRangeTableBindData>();
	auto &state = data_p.global_state->Cast<RedisFetchState>();
	if (!state.fetched) {
		state.values = ReplyStrings(
		    state.conn->execute({"LRANGE", bind.key, std::to_string(bind.start), std::to_string(bind.stop)}));
		state.fetched = true;
	}
	idx_t out_row = 0;
	for (; state.idx < state.values.size() && out_row < STANDARD_VECTOR_SIZE; state.idx++, out_row++) {
		SetOutputValue(output, 0, out_row, Value::BIGINT(NumericCast<int64_t>(state.idx)));
		SetOutputValue(output, 1, out_row, Value(state.values[state.idx]));
	}
	SetOutputCardinality(output, out_row);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the secret functions first!
	CreateRedisSecretFunctions::Register(loader);

	// Helper to add description to a scalar function
	auto add_scalar_function = [&](ScalarFunction func, const string &description, const vector<string> &param_names,
	                               const vector<string> &examples, const string &category = "redis") {
		// Every redis scalar function reports its errors by throwing
		func.SetFallible();
		// They talk to an external server (and most change it), so they must run for every row and never be
		// constant-folded or deduplicated
		func.SetVolatile();
		CreateScalarFunctionInfo info(std::move(func));
		FunctionDescription func_desc;
		func_desc.description = description;
		func_desc.parameter_names = param_names;
		func_desc.examples = examples;
		func_desc.categories = {category};
		info.descriptions.push_back(std::move(func_desc));
		loader.RegisterFunction(std::move(info));
	};

	// Helper to add description to a table function
	auto add_table_function = [&](TableFunction func, const string &description, const vector<string> &param_names,
	                              const vector<string> &examples, const string &category = "redis") {
		CreateTableFunctionInfo info(std::move(func));
		FunctionDescription func_desc;
		func_desc.description = description;
		func_desc.parameter_names = param_names;
		func_desc.examples = examples;
		func_desc.categories = {category};
		info.descriptions.push_back(std::move(func_desc));
		loader.RegisterFunction(std::move(info));
	};

	// Register redis_get scalar function
	add_scalar_function(ScalarFunction("redis_get", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   RedisGetFunction),
	                    "Get the value of a key from Redis. Returns the value associated with the specified key, or "
	                    "NULL if the key does not exist.",
	                    {"key", "secret_name"},
	                    {"SELECT redis_get('mykey', 'my_redis_secret');",
	                     "SELECT redis_get(key_column, 'my_redis_secret') FROM my_table;"});

	// Register redis_set scalar function
	add_scalar_function(ScalarFunction("redis_set", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   LogicalType::VARCHAR, RedisSetFunction),
	                    "Set the value of a key in Redis. Returns 'OK' on success.", {"key", "value", "secret_name"},
	                    {"SELECT redis_set('mykey', 'myvalue', 'my_redis_secret');"});

	// Register redis_hget scalar function
	add_scalar_function(ScalarFunction("redis_hget", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   LogicalType::VARCHAR, RedisHGetFunction),
	                    "Get the value of a field in a Redis hash. Returns the value associated with the field, or "
	                    "NULL if the field does not exist.",
	                    {"key", "field", "secret_name"}, {"SELECT redis_hget('myhash', 'field1', 'my_redis_secret');"});

	// Register redis_hset scalar function
	add_scalar_function(
	    ScalarFunction("redis_hset",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::BIGINT, RedisHSetFunction),
	    "Set the value of a field in a Redis hash. Returns the number of fields that were added (0 if the field "
	    "existed and was updated, 1 if it was created).",
	    {"key", "field", "value", "secret_name"},
	    {"SELECT redis_hset('myhash', 'field1', 'value1', 'my_redis_secret');"});

	// Register redis_lpush scalar function
	add_scalar_function(
	    ScalarFunction("redis_lpush", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::BIGINT, RedisLPushFunction),
	    "Prepend a value to a Redis list. Returns the length of the list after the push operation.",
	    {"key", "value", "secret_name"}, {"SELECT redis_lpush('mylist', 'value1', 'my_redis_secret');"});

	// Register redis_lrange scalar function
	add_scalar_function(
	    ScalarFunction("redis_lrange",
	                   {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisLRangeFunction),
	    "Get a range of elements from a Redis list. Returns elements as a comma-separated string. Use 0 for start and "
	    "-1 for stop to get all elements.",
	    {"key", "start", "stop", "secret_name"},
	    {"SELECT redis_lrange('mylist', 0, -1, 'my_redis_secret');",
	     "SELECT redis_lrange('mylist', 0, 10, 'my_redis_secret');"});

	// Register redis_mget scalar function
	add_scalar_function(ScalarFunction("redis_mget", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   RedisMGetFunction),
	                    "Get the values of multiple keys from Redis. Keys should be comma-separated. Returns values as "
	                    "a comma-separated string, with an empty entry for each key that does not exist.",
	                    {"keys", "secret_name"}, {"SELECT redis_mget('key1,key2,key3', 'my_redis_secret');"});

	// Register redis_scan scalar function
	add_scalar_function(
	    ScalarFunction("redis_scan",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisScanFunction),
	    "Incrementally iterate over keys in Redis matching a pattern. Returns 'cursor:key1,key2,...'. Use cursor '0' "
	    "to start, and continue until cursor returns '0'.",
	    {"cursor", "pattern", "count", "secret_name"},
	    {"SELECT redis_scan('0', '*', 100, 'my_redis_secret');",
	     "SELECT redis_scan('0', 'user:*', 10, 'my_redis_secret');"});

	// Register redis_hscan scalar function
	add_scalar_function(ScalarFunction("redis_hscan",
	                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                    LogicalType::BIGINT, LogicalType::VARCHAR},
	                                   LogicalType::VARCHAR, RedisHScanFunction),
	                    "Incrementally iterate over fields in a Redis hash matching a pattern. Returns "
	                    "'cursor:field1=value1,field2=value2,...'.",
	                    {"key", "cursor", "pattern", "count", "secret_name"},
	                    {"SELECT redis_hscan('myhash', '0', '*', 100, 'my_redis_secret');"});

	// Register redis_del scalar function
	add_scalar_function(ScalarFunction("redis_del", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                                   RedisDelFunction),
	                    "Delete a key from Redis. Returns true if the key was deleted, false if it did not exist.",
	                    {"key", "secret_name"}, {"SELECT redis_del('mykey', 'my_redis_secret');"});

	// Register redis_exists scalar function
	add_scalar_function(ScalarFunction("redis_exists", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   LogicalType::BOOLEAN, RedisExistsFunction),
	                    "Check if a key exists in Redis. Returns true if the key exists, false otherwise.",
	                    {"key", "secret_name"}, {"SELECT redis_exists('mykey', 'my_redis_secret');"});

	// Register redis_type scalar function
	add_scalar_function(ScalarFunction("redis_type", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   RedisTypeFunction),
	                    "Get the type of a key in Redis. Returns the type as a string (string, list, set, zset, hash, "
	                    "stream) or 'none' if the key does not exist.",
	                    {"key", "secret_name"}, {"SELECT redis_type('mykey', 'my_redis_secret');"});

	// Register redis_expire scalar function
	add_scalar_function(ScalarFunction("redis_expire",
	                                   {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                                   LogicalType::BOOLEAN, RedisExpireFunction),
	                    "Set a time-to-live (TTL) in seconds for a key. Returns true if the TTL was set, false if the "
	                    "key does not exist.",
	                    {"key", "seconds", "secret_name"},
	                    {"SELECT redis_expire('mykey', 3600, 'my_redis_secret');",
	                     "SELECT redis_expire('session:' || id, 300, 'my_redis_secret') FROM users;"});

	// Register redis_ttl scalar function
	add_scalar_function(
	    ScalarFunction("redis_ttl", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                   RedisTTLFunction),
	    "Get the remaining time-to-live (TTL) of a key in seconds. Returns -2 if the key does not exist, "
	    "-1 if the key exists but has no expiry set.",
	    {"key", "secret_name"},
	    {"SELECT redis_ttl('mykey', 'my_redis_secret');",
	     "SELECT key, redis_ttl(key, 'my_redis_secret') FROM redis_keys('session:*', 'my_redis_secret');"});

	// Register redis_expireat scalar function
	add_scalar_function(
	    ScalarFunction("redis_expireat", {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::BOOLEAN, RedisExpireAtFunction),
	    "Set an expiry time (Unix timestamp) for a key. Returns true if the expiry was set, false if the key does not "
	    "exist.",
	    {"key", "timestamp", "secret_name"},
	    {"SELECT redis_expireat('mykey', 1736918400, 'my_redis_secret');",
	     "SELECT redis_expireat('event:' || id, EXTRACT(EPOCH FROM (event_time + INTERVAL '1 day'))::BIGINT, "
	     "'my_redis_secret') FROM events;"});

	// Register redis_keys table function
	add_table_function(TableFunction("redis_keys", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RedisKeysFunction,
	                                 RedisKeysBind, RedisKeysInit),
	                   "Scan all keys in Redis matching a pattern. Returns a table with a single 'key' column "
	                   "containing all matching keys.",
	                   {"pattern", "secret_name"},
	                   {"SELECT * FROM redis_keys('*', 'my_redis_secret');",
	                    "SELECT * FROM redis_keys('user:*', 'my_redis_secret');"});

	// Register redis_hgetall table function
	add_table_function(TableFunction("redis_hgetall", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 RedisHGetAllFunction, RedisHGetAllBind, RedisFetchInit),
	                   "Get all fields and values from a Redis hash. Returns a table with 'field' and 'value' columns.",
	                   {"key", "secret_name"}, {"SELECT * FROM redis_hgetall('myhash', 'my_redis_secret');"});

	// Register redis_lrange_table table function
	add_table_function(
	    TableFunction("redis_lrange_table",
	                  {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	                  RedisLRangeTableFunction, RedisLRangeTableBind, RedisFetchInit),
	    "Get a range of elements from a Redis list as a table. Returns a table with 'index' and 'value' columns.",
	    {"key", "start", "stop", "secret_name"},
	    {"SELECT * FROM redis_lrange_table('mylist', 0, -1, 'my_redis_secret');"});

	// Register redis_hscan_over_scan table function
	add_table_function(
	    TableFunction("redis_hscan_over_scan",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                  RedisHScanOverScanFunction, RedisHScanOverScanBind, RedisHScanOverScanInit),
	    "Scan all hash keys matching a pattern, then scan fields within each hash. Returns a table with 'key', "
	    "'field', and 'value' columns; matching keys that are not hashes are skipped. Useful for iterating over "
	    "multiple hashes at once.",
	    {"scan_pattern", "hscan_pattern", "count", "secret_name"},
	    {"SELECT * FROM redis_hscan_over_scan('user:*', '*', 100, 'my_redis_secret');"});

	QueryFarmSendTelemetry(loader, "redis", "2026102003");
}

void RedisExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RedisExtension::Name() {
	return "redis";
}

std::string RedisExtension::Version() const {
	return "2026102003";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(redis, loader) {
	duckdb::LoadInternal(loader);
}
}
