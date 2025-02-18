#pragma once

#include <cmath>
#include <cstdint>
#include "../byte_array_chained_ht.h"
#include "benchmark_chained.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkByteArrayChained : public BenchmarkChained {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkByteArrayChained(int n, uint8_t quotienting_tail_length,
                              uint16_t bin_size);

    ~BenchmarkByteArrayChained() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    bool Query(uint64_t key, uint64_t* value_ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    double AvgChainLength();
    uint32_t MaxChainLength();
    uint64_t* ChainLengthHistogram();
    void FillChainLength(uint8_t chain_length);
    void set_chain_length(uint64_t chain_length);
    bool QueryNoMem(uint64_t key, uint64_t* value_ptr);
    uint64_t QueryEntryCnt();

   private:
    ByteArrayChainedHT* tab;
};

}  // namespace tinyptr