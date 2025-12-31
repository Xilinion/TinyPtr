#pragma once

#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

#include <memory>

namespace tinyptr {

class BenchmarkBucketTable : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkBucketTable(int n);
    ~BenchmarkBucketTable() override;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct BucketImpl;
    std::unique_ptr<BucketImpl> impl_;
};

class BenchmarkGroupChaining : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkGroupChaining(int n);
    ~BenchmarkGroupChaining() override;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct GroupImpl;
    std::unique_ptr<GroupImpl> impl_;
};

class BenchmarkClearyPlain : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkClearyPlain(int n);
    ~BenchmarkClearyPlain() override = default;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct ClearyPlainImpl;
    std::unique_ptr<ClearyPlainImpl> impl_;
};

class BenchmarkClearySparse : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkClearySparse(int n);
    ~BenchmarkClearySparse() override;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct ClearySparseImpl;
    std::unique_ptr<ClearySparseImpl> impl_;
};

class BenchmarkLayeredPlain : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkLayeredPlain(int n);
    ~BenchmarkLayeredPlain() override;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct LayeredPlainImpl;
    std::unique_ptr<LayeredPlainImpl> impl_;
};

class BenchmarkLayeredSparse : public BenchmarkObject64 {
  public:
    static const BenchmarkObjectType TYPE;

    BenchmarkLayeredSparse(int n);
    ~BenchmarkLayeredSparse() override;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

  private:
    struct LayeredSparseImpl;
    std::unique_ptr<LayeredSparseImpl> impl_;
};

}  // namespace tinyptr

