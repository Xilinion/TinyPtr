#pragma once

#include "../po2c_table.h"
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkPo2CTable : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkPo2CTable(int n);

    ~BenchmarkPo2CTable() = default;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

   private:
    Po2CTable* tab;
};

}  // namespace tinyptr
