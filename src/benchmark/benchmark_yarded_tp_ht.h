#pragma once

#include <cmath>
#include <cstdint>
#include "../yarded_tp_ht.h"
#include "benchmark_chained.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkYardedTPHT : public BenchmarkChained {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkYardedTPHT(int n, uint16_t bin_size);

    ~BenchmarkYardedTPHT() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

   public:
    double AvgChainLength();
    uint32_t MaxChainLength();
    uint64_t* ChainLengthHistogram();

   private:
    YardedTPHT* tab;
};

}  // namespace tinyptr