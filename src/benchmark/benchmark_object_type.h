#pragma once

#include <cstdint>

namespace tinyptr {

struct BenchmarkObjectType {
    enum {
        INVALID = -1,
        DEREFTAB64 = 0,
        CHAINEDHT64 = 1,
        INTARRAY64 = 2,
        STDUNORDEREDMAP64 = 3,
        BYTEARRAYCHAINEDHT = 4,
        CLHT = 5,
        COUNT = 6
    };

    BenchmarkObjectType(const BenchmarkObjectType& b) = default;
    BenchmarkObjectType(BenchmarkObjectType& b) = default;

    BenchmarkObjectType(const int b) : val(b) {}

    const int val;
};

}  // namespace tinyptr