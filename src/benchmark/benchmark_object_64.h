#pragma once

#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkObject64(BenchmarkObjectType type_) : type(type_) {}

    virtual ~BenchmarkObject64() {}

    virtual uint8_t Insert(uint64_t key, uint64_t value);
    virtual uint64_t Query(uint64_t key, uint8_t ptr);
    virtual void Update(uint64_t key, uint8_t ptr, uint64_t value);
    virtual void Erase(uint64_t key, uint8_t ptr);

   public:
    BenchmarkObjectType type;
};

const BenchmarkObjectType BenchmarkObject64::TYPE =
    BenchmarkObjectType::INVALID;

}  // namespace tinyptr