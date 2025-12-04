#pragma once

#include "duckdb.hpp"
#include "redis_secret.hpp"

namespace duckdb {

class ExtensionLoader;

class RedisExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override;
    std::string Version() const override;
};

} // namespace duckdb 
