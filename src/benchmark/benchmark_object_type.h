#pragma once

#include <cstdint>

namespace tinyptr {

struct BenchmarkObjectType {
    enum {
        INVALID = -1,
        DEREFTAB64 = 0,
        CHAINEDHT64,
        INTARRAY64,
        STDUNORDEREDMAP64,
        COUNT
    };

    BenchmarkObjectType(const BenchmarkObjectType& b) = default;
    BenchmarkObjectType(BenchmarkObjectType& b) = default;

    BenchmarkObjectType(const int b) : val(b) {}

    const int val;
};

}  // namespace tinyptr