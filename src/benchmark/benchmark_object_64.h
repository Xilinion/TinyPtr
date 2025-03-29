#pragma once

#include <cstdint>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>
#include "benchmark_object_type.h"
#include "conc_opt_type.h"

namespace tinyptr {

class BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkObject64(BenchmarkObjectType type_) : type(type_) {}

    virtual ~BenchmarkObject64() {}

    virtual uint8_t Insert(uint64_t key, uint64_t value) = 0;
    virtual uint64_t Query(uint64_t key, uint8_t ptr) = 0;
    virtual void Update(uint64_t key, uint8_t ptr, uint64_t value) = 0;
    virtual void Erase(uint64_t key, uint8_t ptr) = 0;

    virtual void YCSBFill(std::vector<uint64_t>& keys, int num_threads) {}

    virtual void YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                         int num_threads) {}

    virtual void ConcurrentRun(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads) {}

   public:
    BenchmarkObjectType type;
};

}  // namespace tinyptr