#pragma once

#include <cstdint>

namespace tinyptr {
struct BenchmarkCaseType {
    enum {
        INVALID = -1,
        INSERT_ONLY_LOAD_FACTOR_SUPPORT = 0,
        INSERT_ONLY,
        UPDATE_ONLY,
        ERASE_ONLY,
        ALTERNATING_INSERT_ERASE,
        QUERY_HIT_ONLY,
        QUERY_MISS_ONLY,
        QUERY_HIT_PERCENT,
        QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR,
        QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR,
        QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR,
        ALL_OPERATION_RAND,
        COUNT
    };

    BenchmarkCaseType() = default;
    BenchmarkCaseType(BenchmarkCaseType& b) = default;

    BenchmarkCaseType(int b) : val(b) {}

    int val;
};

}  // namespace tinyptr