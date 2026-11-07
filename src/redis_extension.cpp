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
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include <boost/asio.hpp>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <stdexcept>
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

// Extension version, also reported by telemetry
static constexpr const char *REDIS_EXTENSION_VERSION = "2026110701";

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
	static void AppendCommand(std::string &out, const std::vector<std::string> &args) {
		out += "*" + std::to_string(args.size()) + "\r\n";
		for (auto &arg : args) {
			out += "$" + std::to_string(arg.size()) + "\r\n";
			out += arg;
			out += "\r\n";
		}
	}
};

// Incremental RESP parser. Bytes are appended with Feed(); TryParse() returns true once a complete reply is
// buffered. Handles nested arrays, null bulk strings/arrays and the RESP3 types.
//
// Completeness is tracked by a resumable scanner that looks at every byte once, so a large reply arriving in many
// reads costs linear time; the reply is materialised only once it is complete.
class RedisReplyParser {
public:
	void Feed(const char *data, size_t len) {
		buffer_.append(data, len);
	}

	bool TryParse(RedisReply &reply) {
		if (!ScanComplete()) {
			return false;
		}
		size_t pos = pos_;
		if (!ParseValue(pos, reply, 0)) {
			throw InternalException("Redis protocol error: scanner and parser disagree");
		}
		pos_ = pos;
		if (pos_ == buffer_.size()) {
			buffer_.clear();
			pos_ = 0;
		} else if (pos_ > COMPACT_THRESHOLD) {
			buffer_.erase(0, pos_);
			pos_ = 0;
		}
		return true;
	}

	void Reset() {
		buffer_.clear();
		pos_ = 0;
		scan_stack_.clear();
		scan_pos_ = 0;
	}

	// Bytes buffered but not yet returned as a reply
	bool HasPendingData() const {
		return pos_ < buffer_.size();
	}

private:
	static constexpr int MAX_DEPTH = 128;
	static constexpr int64_t MAX_BULK_LENGTH = 512LL * 1024 * 1024;
	// Upper bound on the element count of an aggregate. Each element takes at least 3 bytes, so a larger count
	// can't be backed by real data; bounding it also keeps `count * 2` for maps from overflowing.
	static constexpr int64_t MAX_AGGREGATE_COUNT = 1LL << 30;
	// Simple strings, errors and headers are short; a longer line without CRLF is a protocol error
	static constexpr size_t MAX_LINE_LENGTH = 64ULL * 1024 * 1024;
	static constexpr size_t COMPACT_THRESHOLD = 1ULL << 20;

	enum class Kind { SCALAR, BULK, AGGREGATE, ATTRIBUTE };

	// Finds the end of the line starting at pos. Returns false when the CRLF hasn't arrived yet.
	bool FindLine(size_t pos, size_t &end) const {
		end = buffer_.find("\r\n", pos);
		if (end == std::string::npos) {
			if (buffer_.size() - pos > MAX_LINE_LENGTH) {
				throw InvalidInputException("Redis protocol error: reply line too long");
			}
			return false;
		}
		return true;
	}

	static int64_t ParseInteger(const char *data, size_t len) {
		if (len == 0) {
			throw InvalidInputException("Redis protocol error: empty integer");
		}
		std::string text(data, len);
		errno = 0;
		char *end = nullptr;
		auto value = std::strtoll(text.c_str(), &end, 10);
		if (errno != 0 || end != text.c_str() + text.size()) {
			throw InvalidInputException("Redis protocol error: invalid integer '%s'", text);
		}
		return value;
	}

	static int64_t CheckedCount(int64_t count) {
		if (count > MAX_AGGREGATE_COUNT) {
			throw InvalidInputException("Redis protocol error: aggregate too large (%lld elements)", (long long)count);
		}
		return count;
	}

	static int64_t CheckedBulkLength(int64_t length) {
		if (length > MAX_BULK_LENGTH) {
			throw InvalidInputException("Redis protocol error: bulk string too large (%lld bytes)", (long long)length);
		}
		return length;
	}

	// Classifies a reply header line. For bulk strings `n` is the length, for aggregates the number of child
	// values (map entries count twice); negative means nil.
	static Kind Classify(char type, const char *rest, size_t rest_len, int64_t &n) {
		n = 0;
		switch (type) {
		case '+':
		case '-':
		case ':':
		case '_':
		case '#':
		case ',':
		case '(':
			if (type == ':') {
				ParseInteger(rest, rest_len);
			}
			return Kind::SCALAR;
		case '$':
		case '!':
		case '=':
			n = ParseInteger(rest, rest_len);
			if (n >= 0) {
				CheckedBulkLength(n);
			}
			return Kind::BULK;
		case '*':
		case '~':
		case '>':
			n = CheckedCount(ParseInteger(rest, rest_len));
			return Kind::AGGREGATE;
		case '%':
			n = CheckedCount(ParseInteger(rest, rest_len));
			if (n > 0) {
				n *= 2;
			}
			return Kind::AGGREGATE;
		case '|':
			n = CheckedCount(ParseInteger(rest, rest_len));
			n = n > 0 ? n * 2 : 0;
			return Kind::ATTRIBUTE;
		default:
			throw InvalidInputException("Redis protocol error: unknown reply type '%c'", type);
		}
	}

	// Advances the completeness scan as far as the buffered bytes allow. Returns true once one complete reply
	// starting at pos_ is buffered.
	bool ScanComplete() {
		if (scan_stack_.empty()) {
			scan_pos_ = pos_;
			scan_stack_.push_back(1);
		}
		while (!scan_stack_.empty()) {
			if (scan_stack_.size() > MAX_DEPTH + 1) {
				throw InvalidInputException("Redis protocol error: reply nested too deeply");
			}
			size_t end;
			if (!FindLine(scan_pos_, end)) {
				return false;
			}
			if (end == scan_pos_) {
				throw InvalidInputException("Redis protocol error: empty reply line");
			}
			int64_t n;
			auto kind = Classify(buffer_[scan_pos_], buffer_.data() + scan_pos_ + 1, end - scan_pos_ - 1, n);
			size_t next = end + 2;
			if (kind == Kind::BULK && n >= 0) {
				auto len = static_cast<size_t>(n);
				if (buffer_.size() - next < len + 2) {
					return false;
				}
				if (buffer_[next + len] != '\r' || buffer_[next + len + 1] != '\n') {
					throw InvalidInputException("Redis protocol error: bulk string not terminated by CRLF");
				}
				next += len + 2;
			}
			scan_pos_ = next;
			// An attribute precedes the value it annotates, so it doesn't use up an element of its parent
			if (kind != Kind::ATTRIBUTE) {
				scan_stack_.back()--;
			}
			if ((kind == Kind::AGGREGATE || kind == Kind::ATTRIBUTE) && n > 0) {
				scan_stack_.push_back(n);
			}
			while (!scan_stack_.empty() && scan_stack_.back() == 0) {
				scan_stack_.pop_back();
			}
		}
		return true;
	}

	bool ReadLine(size_t &pos, std::string &line) {
		size_t end;
		if (!FindLine(pos, end)) {
			return false;
		}
		line = buffer_.substr(pos, end - pos);
		pos = end + 2;
		return true;
	}

	// Bulk payload of `length` bytes followed by CRLF
	bool ReadBulk(size_t &pos, int64_t length, std::string &out) {
		auto len = static_cast<size_t>(CheckedBulkLength(length));
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
		int64_t n;
		auto kind = Classify(type, rest.data(), rest.size(), n);
		reply = RedisReply();
		switch (kind) {
		case Kind::SCALAR:
			switch (type) {
			case '+': // simple string
			case ',': // RESP3 double
			case '(': // RESP3 big number
				reply.type = RedisReply::Type::RESP_STATUS;
				reply.str = rest;
				break;
			case '-': // error
				reply.type = RedisReply::Type::RESP_ERROR;
				reply.str = rest;
				break;
			case ':': // integer
				reply.type = RedisReply::Type::RESP_INTEGER;
				reply.integer = ParseInteger(rest.data(), rest.size());
				break;
			case '#': // RESP3 boolean
				reply.type = RedisReply::Type::RESP_INTEGER;
				reply.integer = rest == "t" ? 1 : 0;
				break;
			default: // '_' RESP3 null
				reply.type = RedisReply::Type::RESP_NIL;
				break;
			}
			return true;
		case Kind::BULK: // '$' bulk string, '!' RESP3 bulk error, '=' RESP3 verbatim string ("txt:" prefix)
			if (n < 0) {
				reply.type = RedisReply::Type::RESP_NIL;
				return true;
			}
			if (!ReadBulk(pos, n, reply.str)) {
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
		case Kind::AGGREGATE: // arrays, RESP3 sets/pushes, and maps flattened to [key, value, key, value, ...]
			if (n < 0) {
				reply.type = RedisReply::Type::RESP_NIL;
				return true;
			}
			return ParseAggregate(pos, n, reply, depth);
		case Kind::ATTRIBUTE: { // RESP3 attribute: skip it, the actual reply follows
			RedisReply attributes;
			if (!ParseAggregate(pos, n, attributes, depth)) {
				return false;
			}
			return ParseValue(pos, reply, depth);
		}
		}
		return false;
	}

	std::string buffer_;
	size_t pos_ = 0;
	// Completeness scan: position of the next header and the number of values still expected per nesting level
	std::vector<int64_t> scan_stack_;
	size_t scan_pos_ = 0;
};

//===--------------------------------------------------------------------===//
// Connection settings
//===--------------------------------------------------------------------===//
static constexpr const char *REDIS_TIMEOUT_SETTING = "redis_timeout_ms";
static constexpr int64_t REDIS_DEFAULT_TIMEOUT_MS = 30000;

struct RedisConnectionConfig {
	std::string host;
	std::string port;
	std::string username;
	std::string password;
	int64_t db = 0;
	// Connect timeout, and the longest wait for reply data; 0 waits forever (the query can still be interrupted)
	int64_t timeout_ms = REDIS_DEFAULT_TIMEOUT_MS;

	// Pool key. It includes the credentials and database index, so a connection authenticated (and SELECTed)
	// for one secret is never handed out for a secret with other credentials. Fields are length-prefixed so
	// they can't run into each other. The key only lives in memory, like the secret itself.
	std::string PoolKey() const {
		std::string key;
		for (auto *field : {&host, &port, &username, &password}) {
			key += std::to_string(field->size());
			key += ':';
			key += *field;
		}
		key += std::to_string(db);
		return key;
	}
};

//===--------------------------------------------------------------------===//
// Connections
//===--------------------------------------------------------------------===//

// The server closed a connection taken from the pool before replying (e.g. its idle timeout fired). The commands
// were not processed, so they can be retried on a new connection.
class RedisStaleConnectionError : public std::runtime_error {
public:
	explicit RedisStaleConnectionError(const std::string &msg) : std::runtime_error(msg) {
	}
};

// One connection to a Redis server. All I/O is asynchronous on a private io_context, driven in short slices so
// that every wait is bounded by the timeout and the query can be interrupted. A connection is used by one thread
// at a time (it is checked out of the pool), so there is no lock around the I/O.
class RedisConnection {
public:
	RedisConnection(const RedisConnectionConfig &config, ClientContext *context)
	    : timeout_ms_(config.timeout_ms), socket_(io_context_) {
		try {
			Connect(config, context);
			if (!config.password.empty()) {
				if (config.username.empty()) {
					Execute({"AUTH", config.password}, context);
				} else {
					Execute({"AUTH", config.username, config.password}, context);
				}
			}
			if (config.db != 0) {
				Execute({"SELECT", std::to_string(config.db)}, context);
			}
		} catch (InterruptException &) {
			throw;
		} catch (std::exception &e) {
			throw IOException("Redis connection error: %s", ErrorData(e).RawMessage());
		}
	}

	void SetTimeout(int64_t timeout_ms) {
		timeout_ms_ = timeout_ms;
	}

	// Sends all commands in one write (pipelining) and reads one reply per command. Error replies are returned,
	// not thrown. Throws RedisStaleConnectionError when the server had closed the connection before any reply
	// byte arrived.
	std::vector<RedisReply> ExecuteMany(const std::vector<std::vector<std::string>> &commands, ClientContext *context) {
		if (broken_) {
			throw IOException("Redis execution error: connection is broken");
		}
		std::vector<RedisReply> replies;
		replies.reserve(commands.size());
		bytes_received_ = 0;
		try {
			out_buffer_.clear();
			for (auto &command : commands) {
				RedisProtocol::AppendCommand(out_buffer_, command);
			}
			// Replies are read while the commands are still being written, so a large pipeline can't deadlock
			// with both sides' socket buffers full
			auto write_op = StartWrite();
			for (size_t i = 0; i < commands.size(); i++) {
				RedisReply reply;
				while (!parser_.TryParse(reply)) {
					Read(context, *write_op);
				}
				replies.push_back(std::move(reply));
			}
			// Every command was answered, so the write has finished
			if (!write_op->done) {
				Await(*write_op, context, "sending the command");
			}
			if (write_op->ec) {
				ThrowIOError(write_op->ec);
			}
		} catch (InterruptException &) {
			MarkBroken();
			throw;
		} catch (RedisStaleConnectionError &) {
			MarkBroken();
			throw;
		} catch (std::exception &e) {
			// The stream position is unknown now, so the connection can't be reused
			MarkBroken();
			throw IOException("Redis execution error: %s", ErrorData(e).RawMessage());
		}
		return replies;
	}

	// Sends one command and reads its reply. Error replies throw unless allow_error is set.
	RedisReply Execute(const std::vector<std::string> &args, ClientContext *context, bool allow_error = false) {
		auto replies = ExecuteMany({args}, context);
		auto &reply = replies[0];
		if (reply.IsError() && !allow_error) {
			throw InvalidInputException("Redis error: %s", reply.str);
		}
		return std::move(reply);
	}

	bool IsBroken() const {
		return broken_;
	}

	std::chrono::steady_clock::time_point last_used;

private:
	struct PendingOp {
		bool done = false;
		boost::system::error_code ec;
		size_t bytes = 0;
	};

	void MarkBroken() {
		broken_ = true;
		parser_.Reset();
		boost::system::error_code ignored;
		socket_.close(ignored);
	}

	// Runs the io_context until `op` completes, the timeout expires or the query is interrupted
	void Await(PendingOp &op, ClientContext *context, const char *what) {
		using clock = std::chrono::steady_clock;
		const auto start = clock::now();
		while (!op.done) {
			auto slice = std::chrono::milliseconds(100);
			if (timeout_ms_ > 0) {
				auto remaining = std::chrono::milliseconds(timeout_ms_) -
				                 std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - start);
				if (remaining.count() <= 0) {
					MarkBroken();
					throw IOException("timed out %s after %lld ms", what, (long long)timeout_ms_);
				}
				slice = std::min(slice, remaining);
			}
			io_context_.run_for(slice);
			if (op.done) {
				break;
			}
			if (context && context->IsInterrupted()) {
				MarkBroken();
				throw InterruptException();
			}
		}
	}

	void Connect(const RedisConnectionConfig &config, ClientContext *context) {
		// Handlers hold the operation state by shared_ptr: after a timeout they may still run (or be destroyed)
		// when this connection is torn down.
		auto resolved = std::make_shared<PendingOp>();
		auto endpoints = std::make_shared<tcp::resolver::results_type>();
		tcp::resolver resolver(io_context_);
		io_context_.restart();
		resolver.async_resolve(
		    config.host, config.port,
		    [resolved, endpoints](const boost::system::error_code &ec, tcp::resolver::results_type results) {
			    resolved->ec = ec;
			    *endpoints = std::move(results);
			    resolved->done = true;
		    });
		try {
			Await(*resolved, context, "resolving the host");
		} catch (...) {
			resolver.cancel();
			throw;
		}
		if (resolved->ec) {
			throw IOException("could not resolve '%s': %s", config.host, resolved->ec.message());
		}
		auto connected = std::make_shared<PendingOp>();
		io_context_.restart();
		boost::asio::async_connect(socket_, *endpoints,
		                           [connected](const boost::system::error_code &ec, const tcp::endpoint &) {
			                           connected->ec = ec;
			                           connected->done = true;
		                           });
		Await(*connected, context, "connecting");
		if (connected->ec) {
			MarkBroken();
			throw IOException("%s", connected->ec.message());
		}
		boost::system::error_code ignored;
		socket_.set_option(tcp::no_delay(true), ignored);
	}

	static bool IsDisconnect(const boost::system::error_code &ec) {
		return ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset ||
		       ec == boost::asio::error::broken_pipe || ec == boost::asio::error::connection_aborted;
	}

	[[noreturn]] void ThrowIOError(const boost::system::error_code &ec) {
		if (IsDisconnect(ec) && bytes_received_ == 0) {
			throw RedisStaleConnectionError(ec.message());
		}
		throw IOException("%s", ec == boost::asio::error::eof ? "connection closed by the server" : ec.message());
	}

	// Starts writing out_buffer_; the write progresses while the io_context runs for the reads
	std::shared_ptr<PendingOp> StartWrite() {
		auto op = std::make_shared<PendingOp>();
		io_context_.restart();
		boost::asio::async_write(socket_, boost::asio::buffer(out_buffer_),
		                         [op](const boost::system::error_code &ec, size_t n) {
			                         op->ec = ec;
			                         op->bytes = n;
			                         op->done = true;
		                         });
		return op;
	}

	void Read(ClientContext *context, const PendingOp &write_op) {
		auto op = std::make_shared<PendingOp>();
		io_context_.restart();
		socket_.async_read_some(boost::asio::buffer(in_buffer_), [op](const boost::system::error_code &ec, size_t n) {
			op->ec = ec;
			op->bytes = n;
			op->done = true;
		});
		Await(*op, context, "waiting for a reply");
		if (write_op.done && write_op.ec) {
			ThrowIOError(write_op.ec);
		}
		if (op->ec) {
			ThrowIOError(op->ec);
		}
		bytes_received_ += op->bytes;
		parser_.Feed(in_buffer_.data(), op->bytes);
	}

	int64_t timeout_ms_;
	// The I/O buffers are declared before the io_context so they outlive any handler still queued on it
	std::array<char, 65536> in_buffer_;
	std::string out_buffer_;
	boost::asio::io_context io_context_;
	tcp::socket socket_;
	RedisReplyParser parser_;
	bool broken_ = false;
	size_t bytes_received_ = 0;
};

// Idle connections of one database, keyed by RedisConnectionConfig::PoolKey(). Lives in the database's object
// cache, so it is per database instance and closed when the database shuts down. A connection is checked out for
// exclusive use (a chunk of a scalar function, or a whole table function scan) and returned afterwards.
class RedisConnectionPool : public ObjectCacheEntry {
public:
	static constexpr size_t MAX_IDLE_PER_KEY = 8;
	static constexpr int64_t MAX_IDLE_SECONDS = 120;

	static std::string ObjectType() {
		return "redis_connection_pool";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(); // never evicted
	}

	static shared_ptr<RedisConnectionPool> Get(ClientContext &context) {
		auto pool =
		    DatabaseInstance::GetDatabase(context).GetObjectCache().GetOrCreate<RedisConnectionPool>(ObjectType());
		if (!pool) {
			throw InternalException("Could not create the redis connection pool");
		}
		return pool;
	}

	// An idle connection for `key`, or nullptr
	std::unique_ptr<RedisConnection> TakeIdle(const std::string &key) {
		std::lock_guard<std::mutex> lock(mutex_);
		EvictExpired();
		auto entry = idle_.find(key);
		if (entry == idle_.end() || entry->second.empty()) {
			return nullptr;
		}
		auto conn = std::move(entry->second.back());
		entry->second.pop_back();
		return conn;
	}

	void Return(const std::string &key, std::unique_ptr<RedisConnection> conn) {
		if (!conn || conn->IsBroken()) {
			return;
		}
		conn->last_used = std::chrono::steady_clock::now();
		std::lock_guard<std::mutex> lock(mutex_);
		EvictExpired();
		auto &list = idle_[key];
		if (list.size() < MAX_IDLE_PER_KEY) {
			list.push_back(std::move(conn));
		}
	}

private:
	void EvictExpired() {
		auto now = std::chrono::steady_clock::now();
		for (auto it = idle_.begin(); it != idle_.end();) {
			auto &list = it->second;
			list.erase(std::remove_if(list.begin(), list.end(),
			                          [&](const std::unique_ptr<RedisConnection> &conn) {
				                          return now - conn->last_used > std::chrono::seconds(MAX_IDLE_SECONDS);
			                          }),
			           list.end());
			it = list.empty() ? idle_.erase(it) : std::next(it);
		}
	}

	std::mutex mutex_;
	std::unordered_map<std::string, std::vector<std::unique_ptr<RedisConnection>>> idle_;
};

// A connection checked out of the pool for exclusive use; returned to the pool when the session ends. Retries once
// on a new connection when a pooled connection turns out to have been closed by the server.
class RedisSession {
public:
	RedisSession(ClientContext &context, RedisConnectionConfig config_p)
	    : pool_(RedisConnectionPool::Get(context)), config_(std::move(config_p)), key_(config_.PoolKey()) {
		conn_ = pool_->TakeIdle(key_);
		reused_ = conn_ != nullptr;
		if (conn_) {
			conn_->SetTimeout(config_.timeout_ms);
		} else {
			conn_ = std::unique_ptr<RedisConnection>(new RedisConnection(config_, &context));
		}
	}

	~RedisSession() {
		if (!discard_) {
			pool_->Return(key_, std::move(conn_));
		}
	}

	// Close the connection when the session ends instead of returning it to the pool (used after arbitrary
	// commands, which may have changed the connection's state, e.g. SELECT or SUBSCRIBE)
	void Discard() {
		discard_ = true;
	}

	std::vector<RedisReply> ExecuteMany(const std::vector<std::vector<std::string>> &commands, ClientContext &context) {
		try {
			return conn_->ExecuteMany(commands, &context);
		} catch (RedisStaleConnectionError &e) {
			if (!reused_) {
				throw IOException("Redis execution error: connection closed by the server (%s)", e.what());
			}
		}
		// The pooled connection had gone stale; retry once on a fresh one
		reused_ = false;
		conn_ = std::unique_ptr<RedisConnection>(new RedisConnection(config_, &context));
		try {
			return conn_->ExecuteMany(commands, &context);
		} catch (RedisStaleConnectionError &e) {
			throw IOException("Redis execution error: connection closed by the server (%s)", e.what());
		}
	}

	RedisReply Execute(const std::vector<std::string> &args, ClientContext &context, bool allow_error = false) {
		auto replies = ExecuteMany({args}, context);
		auto &reply = replies[0];
		if (reply.IsError() && !allow_error) {
			throw InvalidInputException("Redis error: %s", reply.str);
		}
		return std::move(reply);
	}

private:
	shared_ptr<RedisConnectionPool> pool_;
	RedisConnectionConfig config_;
	std::string key_;
	std::unique_ptr<RedisConnection> conn_;
	bool reused_ = false;
	bool discard_ = false;
};

// Redis functions open network connections, so they are refused when external access is disabled
static void CheckExternalAccess(ClientContext &context) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Redis functions are disabled because enable_external_access is false");
	}
}

static int64_t GetTimeoutSetting(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting(REDIS_TIMEOUT_SETTING, value) && !value.IsNull()) {
		return value.GetValue<int64_t>();
	}
	return REDIS_DEFAULT_TIMEOUT_MS;
}

// Looks up a redis secret by name and reads its connection settings. Throws if the secret doesn't exist or lacks
// host/port. The password, username, db and timeout_ms are optional.
static RedisConnectionConfig GetRedisConfig(ClientContext &context, const string &secret_name) {
	CheckExternalAccess(context);
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	// Look the secret up by name. LookupSecret() matches by scope, so an unscoped redis secret would be returned
	// for any name.
	auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name);
	if (!secret_entry || !secret_entry->secret) {
		throw InvalidInputException("Redis secret not found");
	}
	auto &secret = *secret_entry->secret;
	if (secret.GetType() != "redis") {
		throw InvalidInputException("Invalid secret type. Expected 'redis', got '%s'",
		                            secret.GetType().GetIdentifierName());
	}
	const auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&secret);
	if (!kv_secret) {
		throw InvalidInputException("Invalid secret format for 'redis' secret");
	}

	RedisConnectionConfig config;
	Value value;
	if (!kv_secret->TryGetValue("host", value) || value.IsNull()) {
		throw InvalidInputException("Redis secret '%s' is missing required key 'host'", secret_name);
	}
	config.host = value.ToString();
	if (!kv_secret->TryGetValue("port", value) || value.IsNull()) {
		throw InvalidInputException("Redis secret '%s' is missing required key 'port'", secret_name);
	}
	config.port = value.ToString();
	if (kv_secret->TryGetValue("password", value) && !value.IsNull()) {
		config.password = value.ToString();
	}
	if (kv_secret->TryGetValue("username", value) && !value.IsNull()) {
		config.username = value.ToString();
		if (config.password.empty()) {
			throw InvalidInputException("Redis secret '%s' sets 'username' but no 'password'", secret_name);
		}
	}
	if (kv_secret->TryGetValue("db", value) && !value.IsNull()) {
		config.db = value.GetValue<int64_t>();
		if (config.db < 0) {
			throw InvalidInputException("Redis secret '%s': 'db' must be 0 or greater", secret_name);
		}
	}
	config.timeout_ms = GetTimeoutSetting(context);
	if (kv_secret->TryGetValue("timeout_ms", value) && !value.IsNull()) {
		config.timeout_ms = value.GetValue<int64_t>();
	}
	if (config.timeout_ms < 0) {
		throw InvalidInputException("Redis timeout must be 0 (no timeout) or a positive number of milliseconds");
	}
	return config;
}

//===--------------------------------------------------------------------===//
// Scalar function execution
//===--------------------------------------------------------------------===//

// Per-row access to the arguments of a scalar function
class RedisArgs {
public:
	explicit RedisArgs(DataChunk &args) : args_(args), formats_(args.ColumnCount()) {
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

	// A VARCHAR[] argument; NULL elements are rejected
	std::vector<std::string> GetStringList(idx_t col, idx_t row, const char *what) const {
		auto list = args_.data[col].GetValue(row);
		std::vector<std::string> result;
		for (auto &element : ListValue::GetChildren(list)) {
			if (element.IsNull()) {
				throw InvalidInputException("%s must not contain NULL", what);
			}
			result.push_back(element.GetValue<string>());
		}
		return result;
	}

private:
	DataChunk &args_;
	vector<UnifiedVectorFormat> formats_;
};

// Writes results of nested types (LIST, STRUCT, MAP) row by row
class RedisValueWriter {
public:
	RedisValueWriter(Vector &result, idx_t count) : result_(result) {
		(void)count;
	}
	void Write(const Value &value) {
		result_.SetValue(row_++, value);
	}
	void WriteNull() {
		result_.SetValue(row_++, Value(result_.GetType()));
	}

private:
	Vector &result_;
	idx_t row_ = 0;
};

template <class T>
struct RedisResultValue {
	using type = T;
	using writer = RedisResultWriter<T>;
};
template <>
struct RedisResultValue<string_t> {
	using type = std::string;
	using writer = RedisResultWriter<string_t>;
};
template <>
struct RedisResultValue<Value> {
	using type = Value;
	using writer = RedisValueWriter;
};

static RedisReply EmptyArrayReply() {
	RedisReply reply;
	reply.type = RedisReply::Type::RESP_ARRAY;
	return reply;
}

// Runs a Redis command for every row. The secret name is always the last argument and is read per row. A NULL
// argument gives a NULL result.
//
// `build(in, row, command)` fills in the command for a row; returning false skips the server and hands `convert`
// an empty array reply. The commands of a chunk are pipelined: one write and one round trip per distinct secret.
// `convert(in, row, reply, value)` turns a reply into the result; it returns false for a NULL result. Error replies
// raise an error for the first failing row.
template <class T, class BUILD, class CONVERT>
static void RedisExecute(DataChunk &args, ExpressionState &state, Vector &result, const char *command, BUILD build,
                         CONVERT convert, bool discard_connections = false) {
	auto &context = state.GetContext();
	const idx_t count = args.size();
	const idx_t secret_col = args.ColumnCount() - 1;
	RedisArgs input(args);

	struct Batch {
		RedisConnectionConfig config;
		std::unique_ptr<RedisSession> session;
		std::vector<std::vector<std::string>> commands;
		std::vector<RedisReply> replies;
	};
	std::vector<Batch> batches;
	std::unordered_map<std::string, idx_t> batch_of_secret;
	// Per row: the batch and the reply index in it; INVALID_INDEX batch for rows without a command
	std::vector<std::pair<idx_t, idx_t>> row_reply(count, {DConstants::INVALID_INDEX, 0});

	auto wrap_error = [&](std::exception &e) -> InvalidInputException {
		return InvalidInputException("Redis %s error: %s", command, ErrorData(e).RawMessage());
	};
	try {
		for (idx_t row = 0; row < count; row++) {
			if (input.AnyNull(row)) {
				continue;
			}
			// Validate the arguments before connecting
			std::vector<std::string> cmd;
			const bool send = build(input, row, cmd);
			auto secret_name = input.GetString(secret_col, row);
			auto entry = batch_of_secret.find(secret_name);
			if (entry == batch_of_secret.end()) {
				Batch batch;
				batch.config = GetRedisConfig(context, secret_name);
				batches.push_back(std::move(batch));
				entry = batch_of_secret.emplace(secret_name, batches.size() - 1).first;
			}
			auto &batch = batches[entry->second];
			if (!send) {
				continue;
			}
			row_reply[row] = {entry->second, batch.commands.size()};
			batch.commands.push_back(std::move(cmd));
		}
		for (auto &batch : batches) {
			if (batch.commands.empty()) {
				continue;
			}
			// Connect only for secrets that have commands to run
			batch.session.reset(new RedisSession(context, batch.config));
			if (discard_connections) {
				batch.session->Discard();
			}
			batch.replies = batch.session->ExecuteMany(batch.commands, context);
		}
	} catch (InterruptException &) {
		throw;
	} catch (PermissionException &) {
		throw;
	} catch (std::exception &e) {
		throw wrap_error(e);
	}

	typename RedisResultValue<T>::writer writer(result, count);
	const auto empty_reply = EmptyArrayReply();
	for (idx_t row = 0; row < count; row++) {
		if (input.AnyNull(row)) {
			writer.WriteNull();
			continue;
		}
		auto &location = row_reply[row];
		auto &reply = location.first == DConstants::INVALID_INDEX ? empty_reply
		                                                          : batches[location.first].replies[location.second];
		typename RedisResultValue<T>::type value;
		bool valid;
		try {
			if (reply.IsError()) {
				throw InvalidInputException("Redis error: %s", reply.str);
			}
			valid = convert(input, row, reply, value);
		} catch (std::exception &e) {
			throw wrap_error(e);
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

using RedisCommand = std::vector<std::string>;

// Reply conversions
static bool NullableString(const RedisReply &reply, std::string &out) {
	if (reply.IsNil()) {
		return false;
	}
	out = reply.AsString();
	return true;
}

static std::vector<std::string> ReplyStrings(const RedisReply &reply) {
	std::vector<std::string> values;
	for (auto &element : reply.AsArray()) {
		values.push_back(element.AsString());
	}
	return values;
}

static Value StringList(const std::vector<std::string> &values) {
	vector<Value> list;
	list.reserve(values.size());
	for (auto &value : values) {
		list.emplace_back(value);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(list));
}

// field/value pairs as a MAP; a field repeated within the reply keeps its first value
static Value StringMap(const std::vector<std::string> &kvs) {
	vector<Value> keys, values;
	std::unordered_set<std::string> seen;
	for (size_t i = 0; i + 1 < kvs.size(); i += 2) {
		if (seen.insert(kvs[i]).second) {
			keys.emplace_back(kvs[i]);
			values.emplace_back(kvs[i + 1]);
		}
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

static LogicalType StringMapType() {
	return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
}

static LogicalType ScanPageType() {
	child_list_t<LogicalType> children;
	children.emplace_back(Identifier("cursor"), LogicalType::VARCHAR);
	children.emplace_back(Identifier("keys"), LogicalType::LIST(LogicalType::VARCHAR));
	return LogicalType::STRUCT(std::move(children));
}

static LogicalType HScanPageType() {
	child_list_t<LogicalType> children;
	children.emplace_back(Identifier("cursor"), LogicalType::VARCHAR);
	children.emplace_back(Identifier("entries"), StringMapType());
	return LogicalType::STRUCT(std::move(children));
}

// redis_get(key, secret) -> VARCHAR, NULL when the key does not exist
static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "GET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"GET", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) { return NullableString(reply, out); });
}

// redis_set(key, value, secret) -> VARCHAR ('OK')
static void RedisSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "SET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"SET", in.GetString(0, row), in.GetString(1, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) { return NullableString(reply, out); });
}

// redis_hget(key, field, secret) -> VARCHAR, NULL when the field does not exist
static void RedisHGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "HGET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"HGET", in.GetString(0, row), in.GetString(1, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) { return NullableString(reply, out); });
}

static bool IntegerReply(const RedisReply &reply, int64_t &out) {
	out = reply.AsInteger();
	return true;
}

// redis_hset(key, field, value, secret) -> BIGINT (number of fields added)
static void RedisHSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(
	    args, state, result, "HSET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"HSET", in.GetString(0, row), in.GetString(1, row), in.GetString(2, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, int64_t &out) { return IntegerReply(reply, out); });
}

// redis_lpush(key, value, secret) -> BIGINT (list length after the push)
static void RedisLPushFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(
	    args, state, result, "LPUSH",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"LPUSH", in.GetString(0, row), in.GetString(1, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, int64_t &out) { return IntegerReply(reply, out); });
}

static bool BuildLRange(const RedisArgs &in, idx_t row, RedisCommand &cmd) {
	cmd = {"LRANGE", in.GetString(0, row), std::to_string(in.GetBigint(1, row)), std::to_string(in.GetBigint(2, row))};
	return true;
}

// redis_lrange(key, start, stop, secret) -> VARCHAR (comma-separated elements)
static void RedisLRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "LRANGE", BuildLRange,
	                       [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
		                       out = JoinStrings(ReplyStrings(reply), ",");
		                       return true;
	                       });
}

// redis_lrange_list(key, start, stop, secret) -> VARCHAR[]
static void RedisLRangeListFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<Value>(args, state, result, "LRANGE", BuildLRange,
	                    [](const RedisArgs &, idx_t, const RedisReply &reply, Value &out) {
		                    out = StringList(ReplyStrings(reply));
		                    return true;
	                    });
}

// redis_mget(keys, secret) -> VARCHAR (comma-separated values, empty for missing keys so positions line up)
static void RedisMGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "MGET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"MGET"};
		    std::string key_list = in.GetString(0, row);
		    size_t start = 0;
		    while (true) {
			    auto comma = key_list.find(',', start);
			    auto key = key_list.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
			    if (!key.empty()) {
				    cmd.push_back(key);
			    }
			    if (comma == std::string::npos) {
				    break;
			    }
			    start = comma + 1;
		    }
		    return cmd.size() > 1;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
		    std::vector<std::string> values;
		    for (auto &element : reply.AsArray()) {
			    values.push_back(element.IsNil() ? std::string() : element.AsString());
		    }
		    out = JoinStrings(values, ",");
		    return true;
	    });
}

// redis_mget_list(keys VARCHAR[], secret) -> VARCHAR[] (NULL elements for missing keys)
static void RedisMGetListFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<Value>(
	    args, state, result, "MGET",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    auto keys = in.GetStringList(0, row, "redis_mget_list keys");
		    if (keys.empty()) {
			    return false;
		    }
		    cmd = {"MGET"};
		    cmd.insert(cmd.end(), keys.begin(), keys.end());
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, Value &out) {
		    vector<Value> values;
		    for (auto &element : reply.AsArray()) {
			    values.push_back(element.IsNil() ? Value(LogicalType::VARCHAR) : Value(element.AsString()));
		    }
		    out = Value::LIST(LogicalType::VARCHAR, std::move(values));
		    return true;
	    });
}

static bool BuildScan(const RedisArgs &in, idx_t row, RedisCommand &cmd) {
	cmd = {"SCAN", in.GetString(0, row), "MATCH", in.GetString(1, row), "COUNT", std::to_string(in.GetBigint(2, row))};
	return true;
}

// redis_scan(cursor, pattern, count, secret) -> VARCHAR ('next_cursor:key1,key2,...')
static void RedisScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "SCAN", BuildScan,
	                       [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
		                       std::string cursor;
		                       std::vector<std::string> keys;
		                       reply.AsScanReply(cursor, keys);
		                       out = cursor + ":" + JoinStrings(keys, ",");
		                       return true;
	                       });
}

// redis_scan_page(cursor, pattern, count, secret) -> STRUCT(cursor VARCHAR, keys VARCHAR[])
static void RedisScanPageFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<Value>(args, state, result, "SCAN", BuildScan,
	                    [](const RedisArgs &, idx_t, const RedisReply &reply, Value &out) {
		                    std::string cursor;
		                    std::vector<std::string> keys;
		                    reply.AsScanReply(cursor, keys);
		                    child_list_t<Value> fields;
		                    fields.emplace_back(Identifier("cursor"), Value(cursor));
		                    fields.emplace_back(Identifier("keys"), StringList(keys));
		                    out = Value::STRUCT(std::move(fields));
		                    return true;
	                    });
}

static bool BuildHScan(const RedisArgs &in, idx_t row, RedisCommand &cmd) {
	cmd = {"HSCAN",
	       in.GetString(0, row),
	       in.GetString(1, row),
	       "MATCH",
	       in.GetString(2, row),
	       "COUNT",
	       std::to_string(in.GetBigint(3, row))};
	return true;
}

// redis_hscan(key, cursor, pattern, count, secret) -> VARCHAR ('next_cursor:field1=value1,field2=value2,...')
static void RedisHScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(args, state, result, "HSCAN", BuildHScan,
	                       [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
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

// redis_hscan_page(key, cursor, pattern, count, secret) -> STRUCT(cursor VARCHAR, entries MAP(VARCHAR, VARCHAR))
static void RedisHScanPageFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<Value>(args, state, result, "HSCAN", BuildHScan,
	                    [](const RedisArgs &, idx_t, const RedisReply &reply, Value &out) {
		                    std::string cursor;
		                    std::vector<std::string> kvs;
		                    reply.AsScanReply(cursor, kvs);
		                    child_list_t<Value> fields;
		                    fields.emplace_back(Identifier("cursor"), Value(cursor));
		                    fields.emplace_back(Identifier("entries"), StringMap(kvs));
		                    out = Value::STRUCT(std::move(fields));
		                    return true;
	                    });
}

// redis_hgetall_map(key, secret) -> MAP(VARCHAR, VARCHAR) (empty map when the key does not exist)
static void RedisHGetAllMapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<Value>(
	    args, state, result, "HGETALL",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"HGETALL", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, Value &out) {
		    out = StringMap(ReplyStrings(reply));
		    return true;
	    });
}

static void AppendJsonString(std::string &out, const std::string &value) {
	out += '"';
	for (unsigned char c : value) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				char escaped[8];
				snprintf(escaped, sizeof(escaped), "\\u%04x", c);
				out += escaped;
			} else {
				out += static_cast<char>(c);
			}
		}
	}
	out += '"';
}

static void AppendJson(std::string &out, const RedisReply &reply) {
	switch (reply.type) {
	case RedisReply::Type::RESP_STRING:
	case RedisReply::Type::RESP_STATUS:
	case RedisReply::Type::RESP_ERROR:
		AppendJsonString(out, reply.str);
		break;
	case RedisReply::Type::RESP_INTEGER:
		out += std::to_string(reply.integer);
		break;
	case RedisReply::Type::RESP_NIL:
		out += "null";
		break;
	case RedisReply::Type::RESP_ARRAY:
		out += '[';
		for (size_t i = 0; i < reply.elements.size(); i++) {
			if (i > 0) {
				out += ',';
			}
			AppendJson(out, reply.elements[i]);
		}
		out += ']';
		break;
	}
}

// redis_command(args VARCHAR[], secret) -> VARCHAR: runs any command. Strings and integers are returned as text,
// nil as NULL, and arrays (also RESP3 maps and sets, flattened) as JSON arrays.
static void RedisCommandFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "command",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = in.GetStringList(0, row, "redis_command args");
		    if (cmd.empty()) {
			    throw InvalidInputException("redis_command needs at least the command name");
		    }
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
		    if (reply.IsNil()) {
			    return false;
		    }
		    if (reply.type == RedisReply::Type::RESP_ARRAY) {
			    out.clear();
			    AppendJson(out, reply);
		    } else {
			    out = reply.AsString();
		    }
		    return true;
	    },
	    true);
}

static bool ConvertPositive(const RedisReply &reply, bool &out) {
	out = reply.AsInteger() > 0;
	return true;
}

// redis_del(key, secret) -> BOOLEAN
static void RedisDelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "DEL",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"DEL", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, bool &out) { return ConvertPositive(reply, out); });
}

// redis_exists(key, secret) -> BOOLEAN
static void RedisExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "EXISTS",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"EXISTS", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, bool &out) { return ConvertPositive(reply, out); });
}

// redis_type(key, secret) -> VARCHAR
static void RedisTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<string_t>(
	    args, state, result, "TYPE",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"TYPE", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, std::string &out) {
		    out = reply.AsString();
		    return true;
	    });
}

// redis_expire(key, seconds, secret) -> BOOLEAN
static void RedisExpireFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "EXPIRE",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"EXPIRE", in.GetString(0, row), std::to_string(in.GetBigint(1, row))};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, bool &out) {
		    out = reply.AsInteger() == 1;
		    return true;
	    });
}

// redis_ttl(key, secret) -> BIGINT (-2 if the key does not exist, -1 if it has no expiry)
static void RedisTTLFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<int64_t>(
	    args, state, result, "TTL",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"TTL", in.GetString(0, row)};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, int64_t &out) { return IntegerReply(reply, out); });
}

// redis_expireat(key, timestamp, secret) -> BOOLEAN
static void RedisExpireAtFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RedisExecute<bool>(
	    args, state, result, "EXPIREAT",
	    [](const RedisArgs &in, idx_t row, RedisCommand &cmd) {
		    cmd = {"EXPIREAT", in.GetString(0, row), std::to_string(in.GetBigint(1, row))};
		    return true;
	    },
	    [](const RedisArgs &, idx_t, const RedisReply &reply, bool &out) {
		    out = reply.AsInteger() == 1;
		    return true;
	    });
}

//===--------------------------------------------------------------------===//
// Table functions (bind data holds the arguments, iteration state lives in the global state)
//===--------------------------------------------------------------------===//
struct RedisTableBindData : public TableFunctionData {
	RedisConnectionConfig config;

	void LookupSecret(ClientContext &context, const std::string &secret_name) {
		config = GetRedisConfig(context, secret_name);
	}
	// A connection for one scan, checked out of the pool until the scan's global state is destroyed
	std::unique_ptr<RedisSession> Connect(ClientContext &context) const {
		CheckExternalAccess(context);
		return std::unique_ptr<RedisSession>(new RedisSession(context, config));
	}
};

// redis_hscan_over_scan(scan_pattern, hscan_pattern, count, secret) -> (key, field, value)
struct RedisHScanOverScanBindData : public RedisTableBindData {
	std::string scan_pattern;
	std::string hscan_pattern;
	int64_t count;
};

struct RedisHScanOverScanState : public GlobalTableFunctionState {
	std::unique_ptr<RedisSession> conn;
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
	state->conn = input.bind_data->Cast<RedisHScanOverScanBindData>().Connect(context);
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
			auto reply = state.conn->Execute(
			    {"HSCAN", state.key, state.hscan_cursor, "MATCH", bind.hscan_pattern, "COUNT", count}, context, true);
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
			auto reply =
			    state.conn->Execute({"SCAN", state.scan_cursor, "MATCH", bind.scan_pattern, "COUNT", count}, context);
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
	std::unique_ptr<RedisSession> conn;
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
	state->conn = input.bind_data->Cast<RedisKeysBindData>().Connect(context);
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
		auto reply = state.conn->Execute({"SCAN", state.cursor, "MATCH", bind.pattern, "COUNT", "100"}, context);
		reply.AsScanReply(state.cursor, state.keys);
		state.key_idx = 0;
		state.scan_done = state.cursor == "0";
	}
	SetOutputCardinality(output, out_row);
}

// Shared state for table functions that fetch one reply and emit it
struct RedisFetchState : public GlobalTableFunctionState {
	std::unique_ptr<RedisSession> conn;
	std::vector<std::string> values;
	size_t idx = 0;
	bool fetched = false;
	// Index of the first value (redis_lrange_table)
	int64_t base_index = 0;
};

static unique_ptr<GlobalTableFunctionState> RedisFetchInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RedisFetchState>();
	state->conn = input.bind_data->Cast<RedisTableBindData>().Connect(context);
	return std::move(state);
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
		state.values = ReplyStrings(state.conn->Execute({"HGETALL", bind.key}, context));
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
		std::vector<RedisCommand> commands;
		if (bind.start < 0) {
			// A negative start counts from the end; the list length gives the index of the first element
			commands.push_back({"LLEN", bind.key});
		}
		commands.push_back({"LRANGE", bind.key, std::to_string(bind.start), std::to_string(bind.stop)});
		auto replies = state.conn->ExecuteMany(commands, context);
		for (auto &reply : replies) {
			if (reply.IsError()) {
				throw InvalidInputException("Redis error: %s", reply.str);
			}
		}
		state.base_index = bind.start;
		if (bind.start < 0) {
			state.base_index = std::max<int64_t>(0, replies[0].AsInteger() + bind.start);
		}
		state.values = ReplyStrings(replies.back());
		state.fetched = true;
	}
	idx_t out_row = 0;
	for (; state.idx < state.values.size() && out_row < STANDARD_VECTOR_SIZE; state.idx++, out_row++) {
		SetOutputValue(output, 0, out_row, Value::BIGINT(state.base_index + NumericCast<int64_t>(state.idx)));
		SetOutputValue(output, 1, out_row, Value(state.values[state.idx]));
	}
	SetOutputCardinality(output, out_row);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the secret functions first!
	CreateRedisSecretFunctions::Register(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(REDIS_TIMEOUT_SETTING,
	                          "Timeout in milliseconds for connecting to Redis and for each wait for reply data (0 "
	                          "waits forever; queries can still be interrupted). A redis secret's timeout_ms "
	                          "overrides it",
	                          LogicalType::BIGINT, Value::BIGINT(REDIS_DEFAULT_TIMEOUT_MS));

	// DuckDB 2.0 names a parameter registered with a bare type "col<N>" in duckdb_functions(). Give each one its
	// documented name instead; the parameters stay positional-only, so calls work exactly as before.
	auto name_parameters = [](FunctionSignature &signature, const vector<string> &param_names) {
		if (signature.GetParameterCount() != param_names.size()) {
			throw InternalException("redis: %llu parameter names given for %llu parameters", param_names.size(),
			                        signature.GetParameterCount());
		}
		for (idx_t i = 0; i < param_names.size(); i++) {
			signature.GetParameter(i).SetName(Identifier(param_names[i]));
		}
	};

	// Helper to add description to a scalar function
	auto add_scalar_function = [&](ScalarFunction func, const string &description, const vector<string> &param_names,
	                               const vector<string> &examples, const string &category = "redis") {
		// Every redis scalar function reports its errors by throwing
		func.SetFallible();
		// They talk to an external server (and most change it), so they must run for every row and never be
		// constant-folded or deduplicated
		func.SetVolatile();
		name_parameters(func.GetSignature(), param_names);
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
		name_parameters(func.GetSignature(), param_names);
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
	    "Get a range of elements from a Redis list. Returns elements as a comma-separated string, which is ambiguous "
	    "when elements contain commas; prefer redis_lrange_list. Use 0 for start and -1 for stop to get all elements.",
	    {"key", "start", "stop", "secret_name"},
	    {"SELECT redis_lrange('mylist', 0, -1, 'my_redis_secret');",
	     "SELECT redis_lrange('mylist', 0, 10, 'my_redis_secret');"});

	// Register redis_mget scalar function
	add_scalar_function(ScalarFunction("redis_mget", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   RedisMGetFunction),
	                    "Get the values of multiple keys from Redis. Keys should be comma-separated. Returns values as "
	                    "a comma-separated string, with an empty entry for each key that does not exist; prefer "
	                    "redis_mget_list, which keeps commas and missing keys unambiguous.",
	                    {"keys", "secret_name"}, {"SELECT redis_mget('key1,key2,key3', 'my_redis_secret');"});

	// Register redis_scan scalar function
	add_scalar_function(
	    ScalarFunction("redis_scan",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisScanFunction),
	    "Incrementally iterate over keys in Redis matching a pattern. Returns 'cursor:key1,key2,...'. Use cursor '0' "
	    "to start, and continue until cursor returns '0'. Prefer redis_scan_page (typed result) or redis_keys.",
	    {"cursor", "pattern", "count", "secret_name"},
	    {"SELECT redis_scan('0', '*', 100, 'my_redis_secret');",
	     "SELECT redis_scan('0', 'user:*', 10, 'my_redis_secret');"});

	// Register redis_hscan scalar function
	add_scalar_function(ScalarFunction("redis_hscan",
	                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                    LogicalType::BIGINT, LogicalType::VARCHAR},
	                                   LogicalType::VARCHAR, RedisHScanFunction),
	                    "Incrementally iterate over fields in a Redis hash matching a pattern. Returns "
	                    "'cursor:field1=value1,field2=value2,...'. Prefer redis_hscan_page (typed result).",
	                    {"key", "cursor", "pattern", "count", "secret_name"},
	                    {"SELECT redis_hscan('myhash', '0', '*', 100, 'my_redis_secret');"});

	add_scalar_function(
	    ScalarFunction("redis_lrange_list",
	                   {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::LIST(LogicalType::VARCHAR), RedisLRangeListFunction),
	    "Get a range of elements from a Redis list as a VARCHAR[]. start and stop are zero-based and inclusive; "
	    "negative values count from the end (0, -1 is the whole list). An empty list when the key does not exist.",
	    {"key", "start", "stop", "secret_name"},
	    {"SELECT redis_lrange_list('mylist', 0, -1, 'my_redis_secret');",
	     "SELECT unnest(redis_lrange_list('mylist', 0, 9, 'my_redis_secret')) AS value;"});

	add_scalar_function(
	    ScalarFunction("redis_mget_list", {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
	                   LogicalType::LIST(LogicalType::VARCHAR), RedisMGetListFunction),
	    "Get the values of multiple keys from Redis in one round trip. Takes the keys as a VARCHAR[] and returns a "
	    "VARCHAR[] of the same length, with NULL for each key that does not exist.",
	    {"keys", "secret_name"},
	    {"SELECT redis_mget_list(['key1', 'key2', 'key3'], 'my_redis_secret');",
	     "SELECT redis_mget_list(list(key), 'my_redis_secret') FROM my_keys;"});

	add_scalar_function(
	    ScalarFunction("redis_scan_page",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   ScanPageType(), RedisScanPageFunction),
	    "Run one SCAN step over the keys matching a pattern. Returns STRUCT(cursor VARCHAR, keys VARCHAR[]); start "
	    "with cursor '0' and continue with the returned cursor until it is '0' again. A key may be returned more "
	    "than once. redis_keys does the whole iteration as a table function.",
	    {"cursor", "pattern", "count", "secret_name"},
	    {"SELECT redis_scan_page('0', 'user:*', 100, 'my_redis_secret');",
	     "SELECT unnest(redis_scan_page('0', '*', 1000, 'my_redis_secret').keys) AS key;"});

	add_scalar_function(ScalarFunction("redis_hscan_page",
	                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                    LogicalType::BIGINT, LogicalType::VARCHAR},
	                                   HScanPageType(), RedisHScanPageFunction),
	                    "Run one HSCAN step over the fields of a Redis hash matching a pattern. Returns "
	                    "STRUCT(cursor VARCHAR, entries MAP(VARCHAR, VARCHAR)); continue with the returned cursor "
	                    "until it is '0'.",
	                    {"key", "cursor", "pattern", "count", "secret_name"},
	                    {"SELECT redis_hscan_page('myhash', '0', '*', 100, 'my_redis_secret');",
	                     "SELECT redis_hscan_page('myhash', '0', '*', 100, 'my_redis_secret').entries['field1'];"});

	add_scalar_function(ScalarFunction("redis_hgetall_map", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   StringMapType(), RedisHGetAllMapFunction),
	                    "Get all fields and values of a Redis hash as a MAP(VARCHAR, VARCHAR). An empty map when the "
	                    "key does not exist.",
	                    {"key", "secret_name"},
	                    {"SELECT redis_hgetall_map('myhash', 'my_redis_secret');",
	                     "SELECT id, redis_hgetall_map('user:' || id, 'my_redis_secret')['name'] FROM users;"});

	add_scalar_function(
	    ScalarFunction("redis_command", {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisCommandFunction),
	    "Run any Redis command, given as a VARCHAR[] of the command name and its arguments. Strings and integers "
	    "are returned as text, nil as NULL, and array replies as JSON arrays. Error replies raise an error. The "
	    "connection is not reused afterwards, since the command may change its state.",
	    {"args", "secret_name"},
	    {"SELECT redis_command(['SADD', 'myset', 'a'], 'my_redis_secret');",
	     "SELECT redis_command(['SMEMBERS', 'myset'], 'my_redis_secret')::JSON;",
	     "SELECT redis_command(['INCRBY', 'counter', '5'], 'my_redis_secret')::BIGINT;"});

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
	    "Get a range of elements from a Redis list as a table. Returns a table with 'index' (the element's position "
	    "in the list) and 'value' columns.",
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

	QueryFarmSendTelemetry(loader, "redis", REDIS_EXTENSION_VERSION);
}

void RedisExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RedisExtension::Name() {
	return "redis";
}

std::string RedisExtension::Version() const {
	return REDIS_EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(redis, loader) {
	duckdb::LoadInternal(loader);
}
}
