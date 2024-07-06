#pragma once

#include "../dereference_table_64.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkDerefTab64 : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkDerefTab64(int n);

    ~BenchmarkDerefTab64() = default;

    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

   private:
    DereferenceTable64* tab;
};

const BenchmarkObjectType BenchmarkDerefTab64::TYPE =
    BenchmarkObjectType::DEREFTAB64;

}  // namespace tinyptr