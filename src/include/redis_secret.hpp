#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

class ExtensionLoader;

class CreateRedisSecretFunctions {
public:
    static void Register(ExtensionLoader &loader);
};

} // namespace duckdb 
