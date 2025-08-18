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
        CUCKOO = 6,
        ICEBERG = 7,
        BINAWARECHAINEDHT = 8,
        SAMEBINCHAINEDHT = 9,
        YARDEDTTPHT = 10,
        FLATHASH = 11,
        SKULKERHT = 12,
        GROWT = 13,
        CONCURRENT_SKULKERHT = 14,
        JUNCTION = 15,
        RESIZABLE_SKULKERHT = 16,
        CONCURRENT_BYTEARRAYCHAINEDHT = 17,
        RESIZABLE_BYTEARRAYCHAINEDHT = 18,
        BOLT = 19,
        BLAST = 20,
        RESIZABLE_BLAST = 21,
        NONCONC_BLAST = 22,
        COUNT = 23
    };

    BenchmarkObjectType(const BenchmarkObjectType& b) = default;
    BenchmarkObjectType(BenchmarkObjectType& b) = default;

    BenchmarkObjectType(const int b) : val(b) {}

    const int val;
};

}  // namespace tinyptr