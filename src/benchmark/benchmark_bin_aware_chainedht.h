#pragma once

#include <cmath>
#include <cstdint>
#include "../bin_aware_chained_ht.h"
#include "benchmark_bytearray_chainedht.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkBinAwareChained : public BenchmarkChained {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkBinAwareChained(int n, uint16_t bin_size,
                         uint8_t double_slot_num = 126);

    ~BenchmarkBinAwareChained() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    double AvgChainLength();
    uint32_t MaxChainLength();
    uint64_t* ChainLengthHistogram();
    uint64_t* DoubleSlotStatistics();

   private:
    BinAwareChainedHT* tab;
};

}  // namespace tinyptr