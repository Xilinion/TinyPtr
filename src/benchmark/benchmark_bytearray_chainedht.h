#pragma once

#include <cmath>
#include <cstdint>
#include "../byte_array_chained_ht.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkByteArrayChained : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkByteArrayChained(int n, uint8_t quotiented_tail_length, uint16_t bin_size);

    ~BenchmarkByteArrayChained() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);
    
    double AvgChainLength();
    uint32_t MaxChainLength();

   private:
    ByteArrayChainedHT* tab;
};

}  // namespace tinyptr