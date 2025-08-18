#pragma once

#include <emmintrin.h>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include "clht.h"

namespace tinyptr {

class BenchmarkCLHT : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkCLHT(int n);

    ~BenchmarkCLHT();

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    void YCSBFill(std::vector<uint64_t>& keys, int num_threads);
    void YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                 int num_threads);

    void ConcurrentRun(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads);

   private:
    clht_t* tab;
};

}  // namespace tinyptr
