#pragma once

#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {
class BenchmarkIntArray64 : public BenchmarkObject64 {
   public:
    static constexpr BenchmarkObjectType TYPE = BenchmarkObjectType::INTARRAY64;

   public:
    BenchmarkIntArray64(int n);

    ~BenchmarkIntArray64() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

   private:
    int tab_size;
    uint64_t* tab;
};

}  // namespace tinyptr