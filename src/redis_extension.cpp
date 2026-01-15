#define DUCKDB_EXTENSION_MAIN

#include "redis_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <boost/asio.hpp>
#include <string>
#include <mutex>
#include <unordered_map>
#include <memory>
#include "query_farm_telemetry.hpp"

namespace duckdb {

using boost::asio::ip::tcp;

// Simple Redis protocol formatter
class RedisProtocol {
public:
	static std::string formatAuth(const std::string &password) {
		return "*2\r\n$4\r\nAUTH\r\n$" + std::to_string(password.length()) + "\r\n" + password + "\r\n";
	}

	static std::string formatGet(const std::string &key) {
		return "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
	}

	static std::string formatSet(const std::string &key, const std::string &value) {
		return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(value.length()) + "\r\n" + value + "\r\n";
	}

	static std::string parseResponse(const std::string &response) {
		if (response.empty())
			return "";
		if (response[0] == '$') {
			// Bulk string response
			size_t pos = response.find("\r\n");
			if (pos == std::string::npos)
				return "";

			// Skip the length prefix and first \r\n
			pos += 2;
			std::string value = response.substr(pos);

			// Remove trailing \r\n if present
			if (value.size() >= 2 && value.substr(value.size() - 2) == "\r\n") {
				value = value.substr(0, value.size() - 2);
			}
			return value;
		} else if (response[0] == '+') {
			// Simple string response
			return response.substr(1, response.find("\r\n") - 1);
		} else if (response[0] == '-') {
			// Error response
			throw InvalidInputException("Redis error: " + response.substr(1));
		}
		return response;
	}

	// Hash operations
	static std::string formatHGet(const std::string &key, const std::string &field) {
		return "*3\r\n$4\r\nHGET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(field.length()) + "\r\n" + field + "\r\n";
	}

	static std::string formatHSet(const std::string &key, const std::string &field, const std::string &value) {
		return "*4\r\n$4\r\nHSET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(field.length()) + "\r\n" + field + "\r\n$" + std::to_string(value.length()) + "\r\n" +
		       value + "\r\n";
	}

	static std::string formatHGetAll(const std::string &key) {
		return "*2\r\n$7\r\nHGETALL\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
	}

	// List operations
	static std::string formatLPush(const std::string &key, const std::string &value) {
		return "*3\r\n$5\r\nLPUSH\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(value.length()) + "\r\n" + value + "\r\n";
	}

	static std::string formatLRange(const std::string &key, int64_t start, int64_t stop) {
		auto start_str = std::to_string(start);
		auto stop_str = std::to_string(stop);
		return "*4\r\n$6\r\nLRANGE\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(start_str.length()) + "\r\n" + start_str + "\r\n$" + std::to_string(stop_str.length()) +
		       "\r\n" + stop_str + "\r\n";
	}

	// Key scanning
	static std::string formatScan(const std::string &cursor, const std::string &pattern = "*", int64_t count = 10) {
		std::string cmd = "*6\r\n$4\r\nSCAN\r\n";
		cmd += "$" + std::to_string(cursor.length()) + "\r\n" + cursor + "\r\n";
		cmd += "$5\r\nMATCH\r\n";
		cmd += "$" + std::to_string(pattern.length()) + "\r\n" + pattern + "\r\n";
		cmd += "$5\r\nCOUNT\r\n";
		auto count_str = std::to_string(count);
		cmd += "$" + std::to_string(count_str.length()) + "\r\n" + count_str + "\r\n";
		return cmd;
	}

	static std::string formatHScan(const std::string &key, const std::string &cursor, const std::string &pattern = "*",
	                               int64_t count = 10) {
		std::string cmd = "*6\r\n$5\r\nHSCAN\r\n";
		cmd += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
		cmd += "$" + std::to_string(cursor.length()) + "\r\n" + cursor + "\r\n";
		cmd += "$5\r\nMATCH\r\n";
		cmd += "$" + std::to_string(pattern.length()) + "\r\n" + pattern + "\r\n";
		cmd += "$5\r\nCOUNT\r\n";
		auto count_str = std::to_string(count);
		cmd += "$" + std::to_string(count_str.length()) + "\r\n" + count_str + "\r\n";
		return cmd;
	}

	static std::vector<std::string> parseArrayResponse(const std::string &response) {
		std::vector<std::string> result;
		if (response.empty() || response[0] != '*')
			return result;

		size_t pos = 1;
		size_t end = response.find("\r\n", pos);
		int array_size = std::stoi(response.substr(pos, end - pos));
		pos = end + 2;

		for (int i = 0; i < array_size; i++) {
			if (response[pos] == '$') {
				pos++;
				end = response.find("\r\n", pos);
				int str_len = std::stoi(response.substr(pos, end - pos));
				pos = end + 2;
				if (str_len >= 0) {
					result.push_back(response.substr(pos, str_len));
					pos += str_len + 2;
				}
			}
		}
		return result;
	}

	static std::string formatMGet(const std::vector<std::string> &keys) {
		std::string cmd = "*" + std::to_string(keys.size() + 1) + "\r\n$4\r\nMGET\r\n";
		for (const auto &key : keys) {
			cmd += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
		}
		return cmd;
	}

	static std::string formatExpire(const std::string &key, int64_t seconds) {
		return "*3\r\n$6\r\nEXPIRE\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(std::to_string(seconds).length()) + "\r\n" + std::to_string(seconds) + "\r\n";
	}

	static std::string formatExpireAt(const std::string &key, int64_t timestamp) {
		return "*3\r\n$8\r\nEXPIREAT\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n$" +
		       std::to_string(std::to_string(timestamp).length()) + "\r\n" + std::to_string(timestamp) + "\r\n";
	}

	static std::string formatTtl(const std::string &key) {
		return "*2\r\n$3\r\nTTL\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
	}

	static int64_t parseIntegerResponse(const std::string &response) {
		if (response.empty() || response[0] != ':') {
			throw InvalidInputException("Invalid Redis integer response");
		}
		size_t end = response.find("\r\n");
		if (end == std::string::npos) {
			throw InvalidInputException("Invalid Redis integer response");
		}
		try {
			return std::stoll(response.substr(1, end - 1));
		} catch (const std::exception &e) {
			throw InvalidInputException("Failed to parse Redis integer response: %s", e.what());
		}
	}
};

// Redis connection class
class RedisConnection {
public:
	RedisConnection(const std::string &host, const std::string &port, const std::string &password = "")
	    : io_context_(), socket_(io_context_) {
		try {
			tcp::resolver resolver(io_context_);
			auto endpoints = resolver.resolve(host, port);
			boost::asio::connect(socket_, endpoints);

			if (!password.empty()) {
				std::string auth_cmd = RedisProtocol::formatAuth(password);
				boost::asio::write(socket_, boost::asio::buffer(auth_cmd));

				boost::asio::streambuf response;
				boost::asio::read_until(socket_, response, "\r\n");

				std::string auth_response((std::istreambuf_iterator<char>(&response)),
				                          std::istreambuf_iterator<char>());
				RedisProtocol::parseResponse(auth_response);
			}
		} catch (std::exception &e) {
			throw InvalidInputException("Redis connection error: " + std::string(e.what()));
		}
	}

	std::string execute(const std::string &command) {
		std::lock_guard<std::mutex> lock(mutex_);
		try {
			boost::asio::write(socket_, boost::asio::buffer(command));

			boost::asio::streambuf response;
			boost::asio::read_until(socket_, response, "\r\n");

			return std::string((std::istreambuf_iterator<char>(&response)), std::istreambuf_iterator<char>());
		} catch (std::exception &e) {
			throw InvalidInputException("Redis execution error: " + std::string(e.what()));
		}
	}

private:
	boost::asio::io_context io_context_;
	tcp::socket socket_;
	std::mutex mutex_;
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
		if (it == connections_.end()) {
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

// Add this helper function
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

// Modify the function signatures to accept secret name instead of connection details
static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &secret_vector = args.data[1];

	UnaryExecutor::Execute<string_t, string_t>(key_vector, result, args.size(), [&](string_t key) {
		try {
			string host, port, password;
			if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				throw InvalidInputException("Redis secret not found");
			}

			auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			auto response = conn->execute(RedisProtocol::formatGet(key.GetString()));
			return StringVector::AddString(result, RedisProtocol::parseResponse(response));
		} catch (std::exception &e) {
			throw InvalidInputException("Redis GET error: %s", e.what());
		}
	});
}

static void RedisSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &secret_vector = args.data[2];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    key_vector, value_vector, result, args.size(), [&](string_t key, string_t value) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response = conn->execute(RedisProtocol::formatSet(key.GetString(), value.GetString()));
			    return StringVector::AddString(result, RedisProtocol::parseResponse(response));
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis SET error: %s", e.what());
		    }
	    });
}

// Hash operations
static void RedisHGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &field_vector = args.data[1];
	auto &secret_vector = args.data[2];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    key_vector, field_vector, result, args.size(), [&](string_t key, string_t field) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response = conn->execute(RedisProtocol::formatHGet(key.GetString(), field.GetString()));
			    return StringVector::AddString(result, RedisProtocol::parseResponse(response));
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis HGET error: %s", e.what());
		    }
	    });
}

static void RedisHSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &field_vector = args.data[1];
	auto &value_vector = args.data[2];
	auto &secret_vector = args.data[3];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    key_vector, field_vector, result, args.size(), [&](string_t key, string_t field) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response = conn->execute(
			        RedisProtocol::formatHSet(key.GetString(), field.GetString(), value_vector.GetValue(0).ToString()));
			    return StringVector::AddString(result, RedisProtocol::parseResponse(response));
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis HSET error: %s", e.what());
		    }
	    });
}

// List operations
static void RedisLPushFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &value_vector = args.data[1];
	auto &secret_vector = args.data[2];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    key_vector, value_vector, result, args.size(), [&](string_t key, string_t value) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response = conn->execute(RedisProtocol::formatLPush(key.GetString(), value.GetString()));
			    return StringVector::AddString(result, RedisProtocol::parseResponse(response));
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis LPUSH error: %s", e.what());
		    }
	    });
}

static void RedisLRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &start_vector = args.data[1];
	auto &stop_vector = args.data[2];
	auto &secret_vector = args.data[3];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    key_vector, start_vector, result, args.size(), [&](string_t key, int64_t start) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto stop = stop_vector.GetValue(0).GetValue<int64_t>();
			    auto response = conn->execute(RedisProtocol::formatLRange(key.GetString(), start, stop));
			    auto values = RedisProtocol::parseArrayResponse(response);
			    // Join array values with comma for string result
			    std::string joined;
			    for (size_t i = 0; i < values.size(); i++) {
				    if (i > 0)
					    joined += ",";
				    joined += values[i];
			    }
			    return StringVector::AddString(result, joined);
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis LRANGE error: %s", e.what());
		    }
	    });
}

static void RedisMGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &keys_list = args.data[0];
	auto &secret_vector = args.data[1];

	UnaryExecutor::Execute<string_t, string_t>(keys_list, result, args.size(), [&](string_t keys_str) {
		try {
			// Split comma-separated keys
			std::vector<std::string> keys;
			std::string key_list = keys_str.GetString();
			size_t pos = 0;
			while ((pos = key_list.find(',')) != std::string::npos) {
				keys.push_back(key_list.substr(0, pos));
				key_list.erase(0, pos + 1);
			}
			if (!key_list.empty()) {
				keys.push_back(key_list);
			}

			string host, port, password;
			if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				throw InvalidInputException("Redis secret not found");
			}

			auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			auto response = conn->execute(RedisProtocol::formatMGet(keys));
			auto values = RedisProtocol::parseArrayResponse(response);

			// Join results with comma
			std::string joined;
			for (size_t i = 0; i < values.size(); i++) {
				if (i > 0)
					joined += ",";
				joined += values[i];
			}
			return StringVector::AddString(result, joined);
		} catch (std::exception &e) {
			throw InvalidInputException("Redis MGET error: %s", e.what());
		}
	});
}

static void RedisScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &cursor_vector = args.data[0];
	auto &pattern_vector = args.data[1];
	auto &count_vector = args.data[2];
	auto &secret_vector = args.data[3];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    cursor_vector, pattern_vector, result, args.size(), [&](string_t cursor, string_t pattern) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto count = count_vector.GetValue(0).GetValue<int64_t>();
			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response =
			        conn->execute(RedisProtocol::formatScan(cursor.GetString(), pattern.GetString(), count));
			    auto scan_result = RedisProtocol::parseArrayResponse(response);

			    if (scan_result.size() >= 2) {
				    // First element is the new cursor, second element is array of keys
				    std::string result_str = scan_result[0] + ":";
				    auto keys = RedisProtocol::parseArrayResponse(scan_result[1]);
				    for (size_t i = 0; i < keys.size(); i++) {
					    if (i > 0)
						    result_str += ",";
					    result_str += keys[i];
				    }
				    return StringVector::AddString(result, result_str);
			    }
			    return StringVector::AddString(result, "0:");
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis SCAN error: %s", e.what());
		    }
	    });
}

static void RedisHScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &cursor_vector = args.data[1];
	auto &pattern_vector = args.data[2];
	auto &count_vector = args.data[3];
	auto &secret_vector = args.data[4];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    key_vector, cursor_vector, result, args.size(), [&](string_t key, string_t cursor) {
		    try {
			    string host, port, password;
			    if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
				    throw InvalidInputException("Redis secret not found");
			    }

			    auto pattern = pattern_vector.GetValue(0).ToString();
			    auto count = count_vector.GetValue(0).GetValue<int64_t>();
			    auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
			    auto response =
			        conn->execute(RedisProtocol::formatHScan(key.GetString(), cursor.GetString(), pattern, count));

			    // HSCAN returns [cursor, [field1, value1, field2, value2, ...]]
			    auto scan_result = RedisProtocol::parseArrayResponse(response);
			    std::string result_str;
			    if (scan_result.size() >= 2) {
				    result_str = scan_result[0] + ":";
				    auto kvs = RedisProtocol::parseArrayResponse(scan_result[1]);
				    for (size_t i = 0; i < kvs.size(); i += 2) {
					    if (i > 0)
						    result_str += ",";
					    result_str += kvs[i] + "=" + ((i + 1) < kvs.size() ? kvs[i + 1] : "");
				    }
			    } else {
				    result_str = "0:";
			    }
			    return StringVector::AddString(result, result_str);
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis HSCAN error: %s", e.what());
		    }
	    });
}

struct RedisHScanOverScanBindData : public duckdb::TableFunctionData {
	std::string scan_pattern;
	std::string hscan_pattern;
	int64_t count;
	std::string secret_name;
	// State for SCAN
	std::string scan_cursor = "0";
	std::vector<std::string> scan_keys;
	size_t scan_key_idx = 0;
	// State for HSCAN
	std::string hscan_cursor = "0";
	std::vector<std::string> hscan_kvs;
	size_t hscan_kv_idx = 0;
	// Connection
	std::shared_ptr<RedisConnection> conn;
	std::string host, port, password;
	bool scan_complete = false;
};

static duckdb::unique_ptr<duckdb::FunctionData>
RedisHScanOverScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                       duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<std::string> &names) {
	auto result = duckdb::make_uniq<RedisHScanOverScanBindData>();
	result->scan_pattern = input.inputs[0].ToString();
	result->hscan_pattern = input.inputs[1].ToString();
	result->count = input.inputs[2].GetValue<int64_t>();
	result->secret_name = input.inputs[3].ToString();
	// Output columns
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	names = {"key", "field", "value"};
	// Get connection info
	if (!GetRedisSecret(context, result->secret_name, result->host, result->port, result->password)) {
		throw duckdb::InvalidInputException("Redis secret not found");
	}
	result->conn = ConnectionPool::getInstance().getConnection(result->host, result->port, result->password);
	return std::move(result);
}

static void RedisHScanOverScanFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                       duckdb::DataChunk &output) {
	auto &data = (RedisHScanOverScanBindData &)*data_p.bind_data;
	idx_t out_row = 0;
	const idx_t max_rows = output.size();
	while (out_row < max_rows) {
		// If we have hscan_kvs buffered, emit them
		while (data.hscan_kv_idx + 1 < data.hscan_kvs.size() && out_row < max_rows) {
			// hscan_kvs = [field1, value1, field2, value2, ...]
			output.SetValue(0, out_row, duckdb::Value(data.scan_keys[data.scan_key_idx]));
			output.SetValue(1, out_row, duckdb::Value(data.hscan_kvs[data.hscan_kv_idx]));
			output.SetValue(2, out_row, duckdb::Value(data.hscan_kvs[data.hscan_kv_idx + 1]));
			data.hscan_kv_idx += 2;
			out_row++;
		}
		// If we exhausted hscan_kvs, move to next key
		if (data.hscan_kv_idx >= data.hscan_kvs.size() && data.scan_key_idx < data.scan_keys.size()) {
			// Next key
			data.hscan_kv_idx = 0;
			data.hscan_kvs.clear();
			data.hscan_cursor = "0";
			// HSCAN for this key
			std::string hscan_resp = data.conn->execute(RedisProtocol::formatHScan(
			    data.scan_keys[data.scan_key_idx], data.hscan_cursor, data.hscan_pattern, data.count));
			auto hscan_result = RedisProtocol::parseArrayResponse(hscan_resp);
			if (hscan_result.size() >= 2) {
				data.hscan_cursor = hscan_result[0];
				data.hscan_kvs = RedisProtocol::parseArrayResponse(hscan_result[1]);
			}
			// If no fields, skip to next key
			if (data.hscan_kvs.empty()) {
				data.scan_key_idx++;
				continue;
			}
			// Otherwise, emit from hscan_kvs
			continue;
		}
		// If we exhausted keys, SCAN for more
		if (data.scan_key_idx >= data.scan_keys.size() && !data.scan_complete) {
			std::string scan_resp =
			    data.conn->execute(RedisProtocol::formatScan(data.scan_cursor, data.scan_pattern, data.count));
			auto scan_result = RedisProtocol::parseArrayResponse(scan_resp);
			if (scan_result.size() >= 2) {
				data.scan_cursor = scan_result[0];
				data.scan_keys = RedisProtocol::parseArrayResponse(scan_result[1]);
				data.scan_key_idx = 0;
				if (data.scan_cursor == "0") {
					data.scan_complete = true;
				}
			} else {
				data.scan_complete = true;
				break;
			}
			continue;
		}
		// If all done, break
		if ((data.scan_complete && data.scan_key_idx >= data.scan_keys.size()) || out_row == 0) {
			break;
		}
	}
	output.SetCardinality(out_row);
}

// Table function: redis_keys(pattern, secret_name) -> (key VARCHAR)
struct RedisKeysBindData : public duckdb::TableFunctionData {
	std::string pattern;
	std::string secret_name;
	std::string host, port, password;
	std::shared_ptr<RedisConnection> conn;
	std::string cursor = "0";
	std::vector<std::string> keys;
	size_t key_idx = 0;
	bool scan_complete = false;
};

static duckdb::unique_ptr<duckdb::FunctionData> RedisKeysBind(duckdb::ClientContext &context,
                                                              duckdb::TableFunctionBindInput &input,
                                                              duckdb::vector<duckdb::LogicalType> &return_types,
                                                              duckdb::vector<std::string> &names) {
	auto result = duckdb::make_uniq<RedisKeysBindData>();
	result->pattern = input.inputs[0].ToString();
	result->secret_name = input.inputs[1].ToString();
	return_types = {duckdb::LogicalType::VARCHAR};
	names = {"key"};
	if (!GetRedisSecret(context, result->secret_name, result->host, result->port, result->password)) {
		throw duckdb::InvalidInputException("Redis secret not found");
	}
	result->conn = ConnectionPool::getInstance().getConnection(result->host, result->port, result->password);
	return std::move(result);
}

static void RedisKeysFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                              duckdb::DataChunk &output) {
	auto &data = (RedisKeysBindData &)*data_p.bind_data;
	idx_t out_row = 0;
	const idx_t max_rows = output.size();
	while (out_row < max_rows) {
		while (data.key_idx < data.keys.size() && out_row < max_rows) {
			output.SetValue(0, out_row, duckdb::Value(data.keys[data.key_idx]));
			data.key_idx++;
			out_row++;
		}
		if (data.key_idx >= data.keys.size() && !data.scan_complete) {
			std::string scan_resp = data.conn->execute(RedisProtocol::formatScan(data.cursor, data.pattern, 100));
			auto scan_result = RedisProtocol::parseArrayResponse(scan_resp);
			if (scan_result.size() >= 2) {
				data.cursor = scan_result[0];
				data.keys = RedisProtocol::parseArrayResponse(scan_result[1]);
				data.key_idx = 0;
				if (data.cursor == "0") {
					data.scan_complete = true;
				}
			} else {
				data.scan_complete = true;
				break;
			}
			continue;
		}
		if (data.scan_complete && data.key_idx >= data.keys.size()) {
			break;
		}
	}
	output.SetCardinality(out_row);
}

// Table function: redis_hgetall(key, secret_name) -> (field VARCHAR, value VARCHAR)
struct RedisHGetAllBindData : public duckdb::TableFunctionData {
	std::string key;
	std::string secret_name;
	std::string host, port, password;
	std::shared_ptr<RedisConnection> conn;
	std::vector<std::string> kvs;
	size_t kv_idx = 0;
	bool fetched = false;
};

static duckdb::unique_ptr<duckdb::FunctionData> RedisHGetAllBind(duckdb::ClientContext &context,
                                                                 duckdb::TableFunctionBindInput &input,
                                                                 duckdb::vector<duckdb::LogicalType> &return_types,
                                                                 duckdb::vector<std::string> &names) {
	auto result = duckdb::make_uniq<RedisHGetAllBindData>();
	result->key = input.inputs[0].ToString();
	result->secret_name = input.inputs[1].ToString();
	return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
	names = {"field", "value"};
	if (!GetRedisSecret(context, result->secret_name, result->host, result->port, result->password)) {
		throw duckdb::InvalidInputException("Redis secret not found");
	}
	result->conn = ConnectionPool::getInstance().getConnection(result->host, result->port, result->password);
	return std::move(result);
}

static void RedisHGetAllFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                 duckdb::DataChunk &output) {
	auto &data = (RedisHGetAllBindData &)*data_p.bind_data;
	if (!data.fetched) {
		std::string resp = data.conn->execute(RedisProtocol::formatHGetAll(data.key));
		data.kvs = RedisProtocol::parseArrayResponse(resp);
		data.kv_idx = 0;
		data.fetched = true;
	}
	idx_t out_row = 0;
	const idx_t max_rows = output.size();
	while (data.kv_idx + 1 < data.kvs.size() && out_row < max_rows) {
		output.SetValue(0, out_row, duckdb::Value(data.kvs[data.kv_idx]));
		output.SetValue(1, out_row, duckdb::Value(data.kvs[data.kv_idx + 1]));
		data.kv_idx += 2;
		out_row++;
	}
	output.SetCardinality(out_row);
}

// Table function: redis_lrange_table(key, start, stop, secret_name) -> (index BIGINT, value VARCHAR)
struct RedisLRangeTableBindData : public duckdb::TableFunctionData {
	std::string key;
	int64_t start;
	int64_t stop;
	std::string secret_name;
	std::string host, port, password;
	std::shared_ptr<RedisConnection> conn;
	std::vector<std::string> values;
	size_t idx = 0;
	bool fetched = false;
};

static duckdb::unique_ptr<duckdb::FunctionData> RedisLRangeTableBind(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionBindInput &input,
                                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                                     duckdb::vector<std::string> &names) {
	auto result = duckdb::make_uniq<RedisLRangeTableBindData>();
	result->key = input.inputs[0].ToString();
	result->start = input.inputs[1].GetValue<int64_t>();
	result->stop = input.inputs[2].GetValue<int64_t>();
	result->secret_name = input.inputs[3].ToString();
	return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR};
	names = {"index", "value"};
	if (!GetRedisSecret(context, result->secret_name, result->host, result->port, result->password)) {
		throw duckdb::InvalidInputException("Redis secret not found");
	}
	result->conn = ConnectionPool::getInstance().getConnection(result->host, result->port, result->password);
	return std::move(result);
}

static void RedisLRangeTableFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                     duckdb::DataChunk &output) {
	auto &data = (RedisLRangeTableBindData &)*data_p.bind_data;
	if (!data.fetched) {
		std::string resp = data.conn->execute(RedisProtocol::formatLRange(data.key, data.start, data.stop));
		data.values = RedisProtocol::parseArrayResponse(resp);
		data.idx = 0;
		data.fetched = true;
	}
	idx_t out_row = 0;
	const idx_t max_rows = output.size();
	for (; data.idx < data.values.size() && out_row < max_rows; data.idx++, out_row++) {
		output.SetValue(0, out_row, duckdb::Value::BIGINT(data.idx));
		output.SetValue(1, out_row, duckdb::Value(data.values[data.idx]));
	}
	output.SetCardinality(out_row);
}

// Scalar function: redis_del(key, secret_name) -> BOOLEAN
static void RedisDelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &secret_vector = args.data[1];
	UnaryExecutor::Execute<string_t, bool>(key_vector, result, args.size(), [&](string_t key) {
		string host, port, password;
		if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
			throw InvalidInputException("Redis secret not found");
		}
		auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
		std::string cmd =
		    "*2\r\n$3\r\nDEL\r\n$" + std::to_string(key.GetString().length()) + "\r\n" + key.GetString() + "\r\n";
		auto resp = conn->execute(cmd);
		// DEL returns :1\r\n if deleted, :0\r\n if not
		return resp.find(":1\r\n") != std::string::npos;
	});
}

// Scalar function: redis_exists(key, secret_name) -> BOOLEAN
static void RedisExistsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &secret_vector = args.data[1];
	UnaryExecutor::Execute<string_t, bool>(key_vector, result, args.size(), [&](string_t key) {
		string host, port, password;
		if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
			throw InvalidInputException("Redis secret not found");
		}
		auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
		std::string cmd =
		    "*2\r\n$6\r\nEXISTS\r\n$" + std::to_string(key.GetString().length()) + "\r\n" + key.GetString() + "\r\n";
		auto resp = conn->execute(cmd);
		// EXISTS returns :1\r\n if exists, :0\r\n if not
		return resp.find(":1\r\n") != std::string::npos;
	});
}

// Scalar function: redis_type(key, secret_name) -> VARCHAR
static void RedisTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &secret_vector = args.data[1];
	UnaryExecutor::Execute<string_t, string_t>(key_vector, result, args.size(), [&](string_t key) {
		string host, port, password;
		if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
			throw InvalidInputException("Redis secret not found");
		}
		auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
		std::string cmd =
		    "*2\r\n$4\r\nTYPE\r\n$" + std::to_string(key.GetString().length()) + "\r\n" + key.GetString() + "\r\n";
		auto resp = conn->execute(cmd);
		// TYPE returns +type\r\n
		if (resp.size() > 1 && resp[0] == '+') {
			auto end = resp.find("\r\n");
			if (end != std::string::npos)
				return string_t(resp.substr(1, end - 1));
		}
		return string_t("");
	});
}

static void RedisExpireFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &seconds_vector = args.data[1];
	auto &secret_vector = args.data[2];

	// Extract secret once before executor loop (optimization)
	string host, port, password;
	if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
		throw InvalidInputException("Redis secret not found");
	}
	auto conn = ConnectionPool::getInstance().getConnection(host, port, password);

	BinaryExecutor::Execute<string_t, int64_t, bool>(
	    key_vector, seconds_vector, result, args.size(), [&](string_t key, int64_t seconds) {
		    try {
			    auto response = conn->execute(RedisProtocol::formatExpire(key.GetString(), seconds));
			    auto result_int = RedisProtocol::parseIntegerResponse(response);
			    // Returns 1 if TTL was set (key exists), 0 if key doesn't exist
			    return result_int == 1;
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis EXPIRE error: %s", e.what());
		    }
	    });
}

static void RedisTTLFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &secret_vector = args.data[1];

	// Extract secret once before executor loop (optimization)
	string host, port, password;
	if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
		throw InvalidInputException("Redis secret not found");
	}
	auto conn = ConnectionPool::getInstance().getConnection(host, port, password);

	UnaryExecutor::Execute<string_t, int64_t>(key_vector, result, args.size(), [&](string_t key) {
		try {
			auto response = conn->execute(RedisProtocol::formatTtl(key.GetString()));
			// Returns: -2 if key doesn't exist, -1 if key has no expiry, or positive TTL in seconds
			return RedisProtocol::parseIntegerResponse(response);
		} catch (std::exception &e) {
			throw InvalidInputException("Redis TTL error: %s", e.what());
		}
	});
}

static void RedisExpireAtFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &key_vector = args.data[0];
	auto &timestamp_vector = args.data[1];
	auto &secret_vector = args.data[2];

	// Extract secret once before executor loop (optimization)
	string host, port, password;
	if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), host, port, password)) {
		throw InvalidInputException("Redis secret not found");
	}
	auto conn = ConnectionPool::getInstance().getConnection(host, port, password);

	BinaryExecutor::Execute<string_t, int64_t, bool>(
	    key_vector, timestamp_vector, result, args.size(), [&](string_t key, int64_t timestamp) {
		    try {
			    auto response = conn->execute(RedisProtocol::formatExpireAt(key.GetString(), timestamp));
			    auto result_int = RedisProtocol::parseIntegerResponse(response);
			    // Returns 1 if expiry was set (key exists), 0 if key doesn't exist
			    return result_int == 1;
		    } catch (std::exception &e) {
			    throw InvalidInputException("Redis EXPIREAT error: %s", e.what());
		    }
	    });
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the secret functions first!
	CreateRedisSecretFunctions::Register(loader);

	// Helper to add description to a scalar function
	auto add_scalar_function = [&](ScalarFunction func, const string &description, const vector<string> &param_names,
	                               const vector<string> &examples, const string &category = "redis") {
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
	                    "Get the value of a key from Redis. Returns the value associated with the specified key, or an "
	                    "empty string if the key does not exist.",
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
	                    "Get the value of a field in a Redis hash. Returns the value associated with the field, or an "
	                    "empty string if the field does not exist.",
	                    {"key", "field", "secret_name"}, {"SELECT redis_hget('myhash', 'field1', 'my_redis_secret');"});

	// Register redis_hset scalar function
	add_scalar_function(
	    ScalarFunction("redis_hset",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisHSetFunction),
	    "Set the value of a field in a Redis hash. Returns the number of fields that were added (0 if the field "
	    "existed and was updated, 1 if it was created).",
	    {"key", "field", "value", "secret_name"},
	    {"SELECT redis_hset('myhash', 'field1', 'value1', 'my_redis_secret');"});

	// Register redis_lpush scalar function
	add_scalar_function(
	    ScalarFunction("redis_lpush", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RedisLPushFunction),
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
	                    "a comma-separated string.",
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
	add_scalar_function(
	    ScalarFunction("redis_expire", {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                   LogicalType::BOOLEAN, RedisExpireFunction),
	    "Set a time-to-live (TTL) in seconds for a key. Returns true if the TTL was set, false if the key does not exist.",
	    {"key", "seconds", "secret_name"},
	    {"SELECT redis_expire('mykey', 3600, 'my_redis_secret');",
	     "SELECT redis_expire('session:' || id, 300, 'my_redis_secret') FROM users;"});

	// Register redis_ttl scalar function
	add_scalar_function(ScalarFunction("redis_ttl", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                                   RedisTTLFunction),
	                    "Get the remaining time-to-live (TTL) of a key in seconds. Returns -2 if the key does not exist, "
	                    "-1 if the key exists but has no expiry set.",
	                    {"key", "secret_name"},
	                    {"SELECT redis_ttl('mykey', 'my_redis_secret');",
	                     "SELECT key, redis_ttl(key, 'my_redis_secret') FROM (SELECT redis_get(key) as key FROM "
	                     "redis_keys('*', 'my_redis_secret')); "});

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
	add_table_function(
	    TableFunction("redis_keys", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RedisKeysFunction, RedisKeysBind),
	    "Scan all keys in Redis matching a pattern. Returns a table with a single 'key' column containing all matching "
	    "keys.",
	    {"pattern", "secret_name"},
	    {"SELECT * FROM redis_keys('*', 'my_redis_secret');",
	     "SELECT * FROM redis_keys('user:*', 'my_redis_secret');"});

	// Register redis_hgetall table function
	add_table_function(TableFunction("redis_hgetall", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 RedisHGetAllFunction, RedisHGetAllBind),
	                   "Get all fields and values from a Redis hash. Returns a table with 'field' and 'value' columns.",
	                   {"key", "secret_name"}, {"SELECT * FROM redis_hgetall('myhash', 'my_redis_secret');"});

	// Register redis_lrange_table table function
	add_table_function(
	    TableFunction("redis_lrange_table",
	                  {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	                  RedisLRangeTableFunction, RedisLRangeTableBind),
	    "Get a range of elements from a Redis list as a table. Returns a table with 'index' and 'value' columns.",
	    {"key", "start", "stop", "secret_name"},
	    {"SELECT * FROM redis_lrange_table('mylist', 0, -1, 'my_redis_secret');"});

	// Register redis_hscan_over_scan table function
	add_table_function(
	    TableFunction("redis_hscan_over_scan",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                  RedisHScanOverScanFunction, RedisHScanOverScanBind),
	    "Scan all hash keys matching a pattern, then scan fields within each hash. Returns a table with 'key', "
	    "'field', and 'value' columns. Useful for iterating over multiple hashes at once.",
	    {"scan_pattern", "hscan_pattern", "count", "secret_name"},
	    {"SELECT * FROM redis_hscan_over_scan('user:*', '*', 100, 'my_redis_secret');"});

	QueryFarmSendTelemetry(loader, "redis", "2026011401");
}

void RedisExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RedisExtension::Name() {
	return "redis";
}

std::string RedisExtension::Version() const {
	return "2026011401";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(redis, loader) {
	duckdb::LoadInternal(loader);
}
}
