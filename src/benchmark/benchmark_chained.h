#pragma once

#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

namespace tinyptr {

class BenchmarkChained : public BenchmarkObject64 {
   public:
    BenchmarkChained(BenchmarkObjectType type_) : BenchmarkObject64(type_) {}

    virtual ~BenchmarkChained() = default;

   public:
    virtual double AvgChainLength() = 0;
    virtual uint32_t MaxChainLength() = 0;
    virtual uint64_t* ChainLengthHistogram() = 0;
};

}  // namespace tinyptr