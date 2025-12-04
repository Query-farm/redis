#include "redis_secret.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void CopySecret(const std::string &key, const CreateSecretInput &input, KeyValueSecret &result) {
	auto val = input.options.find(key);
	if (val != input.options.end()) {
		result.secret_map[key] = val->second;
	}
}

static void RegisterCommonSecretParameters(CreateSecretFunction &function) {
	// Register redis connection parameters
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
}

static void RedactCommonKeys(KeyValueSecret &result) {
	// Redact sensitive information
	result.redact_keys.insert("password");
}

static unique_ptr<BaseSecret> CreateRedisSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Copy all relevant secrets
	CopySecret("host", input, *result);
	CopySecret("port", input, *result);
	CopySecret("password", input, *result);

	// Redact sensitive keys
	RedactCommonKeys(*result);

	return result;
}

static unique_ptr<BaseSecret> RedisSecretDeserialize(Deserializer &deserializer, BaseSecret base_secret) {
	auto result = KeyValueSecret::Deserialize<KeyValueSecret>(deserializer, std::move(base_secret));
	auto kv_secret = dynamic_cast<KeyValueSecret *>(result.get());
	if (kv_secret) {
		RedactCommonKeys(*kv_secret);
	}
	return result;
}

void CreateRedisSecretFunctions::Register(ExtensionLoader &loader) {
	string type = "redis";

	// Register the new type
	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = RedisSecretDeserialize;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	// Register the config secret provider
	CreateSecretFunction config_function = {type, "config", CreateRedisSecretFromConfig};
	RegisterCommonSecretParameters(config_function);
	loader.RegisterFunction(config_function);
}

} // namespace duckdb
