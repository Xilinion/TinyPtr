#pragma once

#include <cstdint>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include "blast_ht.h"

namespace tinyptr {

class BenchmarkBlastHT : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkBlastHT(uint64_t size, uint16_t bin_size);

    ~BenchmarkBlastHT() = default;

   public:
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

    void Stats();

   private:
    BlastHT* tab;
};

}  // namespace tinyptr
