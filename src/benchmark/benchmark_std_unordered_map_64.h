#pragma once

#include <cstdint>
#include <unordered_map>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkStdUnorderedMap64 : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkStdUnorderedMap64(int n);

    ~BenchmarkStdUnorderedMap64() = default;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

   private:
    std::unordered_map<uint64_t, uint64_t> ht;
};

}  // namespace tinyptr