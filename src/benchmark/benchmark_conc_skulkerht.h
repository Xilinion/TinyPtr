#pragma once

#include <vector>
#include "benchmark_object_64.h"
#include "concurrent_skulker_ht.h"

namespace tinyptr {

class BenchmarkConcSkulkerHT : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkConcSkulkerHT(uint64_t size, uint16_t bin_size);

    ~BenchmarkConcSkulkerHT() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    void Fill(std::vector<uint64_t>& keys, int num_threads);
    void Run(std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads);

   private:
    ConcurrentSkulkerHT* tab;
};

}  // namespace tinyptr