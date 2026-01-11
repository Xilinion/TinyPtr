#pragma once

#include "benchmark_object_64.h"
#include "benchmark_object_type.h"
#include <memory>

namespace tinyptr {

// Base class for third-party TinyPointers variants
class BenchmarkThirdPartyTinyPointersBase : public BenchmarkObject64 {
   public:
    BenchmarkThirdPartyTinyPointersBase(BenchmarkObjectType type, int n);
    virtual ~BenchmarkThirdPartyTinyPointersBase();

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

   protected:
    // Use pimpl pattern to hide third-party implementation details
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Simple variant
class BenchmarkThirdPartyTinyPointersSimple : public BenchmarkThirdPartyTinyPointersBase {
   public:
    static const BenchmarkObjectType TYPE;
    BenchmarkThirdPartyTinyPointersSimple(int n);
};

// Fixed-size variant
class BenchmarkThirdPartyTinyPointersFixed : public BenchmarkThirdPartyTinyPointersBase {
   public:
    static const BenchmarkObjectType TYPE;
    BenchmarkThirdPartyTinyPointersFixed(int n);
};

// Variable-size variant
class BenchmarkThirdPartyTinyPointersVariable : public BenchmarkThirdPartyTinyPointersBase {
   public:
    static const BenchmarkObjectType TYPE;
    BenchmarkThirdPartyTinyPointersVariable(int n);
};

}  // namespace tinyptr
