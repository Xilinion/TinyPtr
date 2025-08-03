#pragma once

#include <cstdint>

namespace tinyptr {
struct BenchmarkCaseType {
    enum {
        INVALID = -1,
        INSERT_ONLY_LOAD_FACTOR_SUPPORT = 0,
        INSERT_ONLY = 1,
        UPDATE_ONLY = 2,
        ERASE_ONLY = 3,
        ALTERNATING_INSERT_ERASE = 4,
        ALL_OPERATION_RAND = 5,
        QUERY_HIT_ONLY = 6,
        QUERY_MISS_ONLY = 7,
        QUERY_HIT_PERCENT = 8,
        QUERY_HIT_ONLY_CUSTOM_LOAD_FACTOR = 9,
        QUERY_MISS_ONLY_CUSTOM_LOAD_FACTOR = 10,
        QUERY_HIT_PERCENT_CUSTOM_LOAD_FACTOR = 11,
        XXHASH64_THROUGHPUT = 12,
        PRNG_THROUGHPUT = 13,
        LATENCY_VARYING_CHAINLENGTH = 14,
        QUERY_NO_MEM = 15,
        INSERT_DELETE_LOAD_FACTOR_SUPPORT = 16,
        YCSB_A = 17,
        YCSB_B = 18,
        YCSB_C = 19,
        YCSB_NEG_A = 20,
        YCSB_NEG_B = 21,
        YCSB_NEG_C = 22,
        HASH_DISTRUBUTION = 23,
        COUNT = 24
    };

    BenchmarkCaseType() = default;
    BenchmarkCaseType(BenchmarkCaseType& b) = default;

    BenchmarkCaseType(int b) : val(b) {}

    int val;
};

}  // namespace tinyptr