#include "benchmark_hashbench_tables.h"

#include <separate/bucket.hpp>
#include <separate/bucket_table.hpp>
#include <separate/group_chaining.hpp>
#include <tudocomp/util/compact_hash/map/typedefs.hpp>

namespace tinyptr {

struct BenchmarkBucketTable::BucketImpl {
    using bucket_table_t =
        separate_chaining::bucket_table<
            separate_chaining::varwidth_bucket<uint64_t>,
            separate_chaining::plain_bucket<uint64_t>,
            separate_chaining::incremental_resize>;
    bucket_table_t table;

    explicit BucketImpl(int /*expected*/) : table(64) {}
};

struct BenchmarkGroupChaining::GroupImpl {
    using group_table_t = separate_chaining::group::group_chaining_table<>;
    group_table_t table;

    explicit GroupImpl(int expected) : table(64, 64) {
        const size_t reserve_size =
            expected > 0 ? static_cast<size_t>(expected) : size_t(1);
        table.reserve(reserve_size);
    }
};

const BenchmarkObjectType BenchmarkBucketTable::TYPE =
    BenchmarkObjectType::BUCKET_TABLE;

BenchmarkBucketTable::BenchmarkBucketTable(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<BucketImpl>(n)) {}

BenchmarkBucketTable::~BenchmarkBucketTable() = default;

uint8_t BenchmarkBucketTable::Insert(uint64_t key, uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename BucketImpl::bucket_table_t::value_type(value));
    (void)navigator;
    return 0;
}

uint64_t BenchmarkBucketTable::Query(uint64_t key, uint8_t /*ptr*/) {
    auto it = impl_->table.find(key);
    if (it == impl_->table.cend()) {
        return 0;
    }
    return it->second;
}

void BenchmarkBucketTable::Update(uint64_t key, uint8_t /*ptr*/,
                                  uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename BucketImpl::bucket_table_t::value_type(value));
    navigator.value_ref() = value;
}

void BenchmarkBucketTable::Erase(uint64_t key, uint8_t /*ptr*/) {
    impl_->table.erase(key);
}

const BenchmarkObjectType BenchmarkGroupChaining::TYPE =
    BenchmarkObjectType::GRP_CHAINING;

BenchmarkGroupChaining::BenchmarkGroupChaining(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<GroupImpl>(n)) {}

BenchmarkGroupChaining::~BenchmarkGroupChaining() = default;

uint8_t BenchmarkGroupChaining::Insert(uint64_t key, uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename GroupImpl::group_table_t::value_type(value));
    navigator = value;
    return 0;
}

uint64_t BenchmarkGroupChaining::Query(uint64_t key, uint8_t /*ptr*/) {
    auto it = impl_->table.find(key);
    if (it == impl_->table.cend()) {
        return 0;
    }
    return it->second;
}

void BenchmarkGroupChaining::Update(uint64_t key, uint8_t /*ptr*/,
                                   uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename GroupImpl::group_table_t::value_type(value));
    navigator = value;
}

void BenchmarkGroupChaining::Erase(uint64_t key, uint8_t /*ptr*/) {
    impl_->table.erase(key);
}

struct BenchmarkClearyPlain::ClearyPlainImpl {
    using map_type = tdc::compact_hash::map::plain_cv_hashmap_t<uint64_t>;
    map_type map_;
    
    ClearyPlainImpl(int n) : map_(n, 64, 64) {}
};

const BenchmarkObjectType BenchmarkClearyPlain::TYPE =
    BenchmarkObjectType::CLEARY_PLAIN;

BenchmarkClearyPlain::BenchmarkClearyPlain(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<ClearyPlainImpl>(n)) {}

uint8_t BenchmarkClearyPlain::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key, typename ClearyPlainImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkClearyPlain::Query(uint64_t key, uint8_t /*ptr*/) {
    auto ptr = impl_->map_.find(key);
    if (ptr == typename ClearyPlainImpl::map_type::pointer_type()) {
        return 0;
    }
    return *ptr;
}

void BenchmarkClearyPlain::Update(uint64_t key, uint8_t /*ptr*/,
                                  uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkClearyPlain::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

struct BenchmarkClearySparse::ClearySparseImpl {
    using map_type = tdc::compact_hash::map::sparse_cv_hashmap_t<uint64_t>;
    map_type map_;
    
    ClearySparseImpl(int n) : map_(n, 64, 64) {}
};

const BenchmarkObjectType BenchmarkClearySparse::TYPE =
    BenchmarkObjectType::CLEARY_SPARSE;

BenchmarkClearySparse::BenchmarkClearySparse(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<ClearySparseImpl>(n)) {}

BenchmarkClearySparse::~BenchmarkClearySparse() = default;

uint8_t BenchmarkClearySparse::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key, typename ClearySparseImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkClearySparse::Query(uint64_t key, uint8_t /*ptr*/) {
    auto ptr = impl_->map_.find(key);
    if (ptr == typename ClearySparseImpl::map_type::pointer_type()) {
        return 0;
    }
    return *ptr;
}

void BenchmarkClearySparse::Update(uint64_t key, uint8_t /*ptr*/,
                                   uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkClearySparse::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

struct BenchmarkLayeredPlain::LayeredPlainImpl {
    using map_type = tdc::compact_hash::map::plain_layered_hashmap_t<uint64_t>;
    map_type map_;
    
    LayeredPlainImpl(int n) : map_(n, 64, 64) {}
};

const BenchmarkObjectType BenchmarkLayeredPlain::TYPE =
    BenchmarkObjectType::LAYERED_PLAIN;

BenchmarkLayeredPlain::BenchmarkLayeredPlain(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<LayeredPlainImpl>(n)) {}

BenchmarkLayeredPlain::~BenchmarkLayeredPlain() = default;

uint8_t BenchmarkLayeredPlain::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key, typename LayeredPlainImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkLayeredPlain::Query(uint64_t key, uint8_t /*ptr*/) {
    auto ptr = impl_->map_.find(key);
    if (ptr == typename LayeredPlainImpl::map_type::pointer_type()) {
        return 0;
    }
    return *ptr;
}

void BenchmarkLayeredPlain::Update(uint64_t key, uint8_t /*ptr*/,
                                   uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkLayeredPlain::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

struct BenchmarkLayeredSparse::LayeredSparseImpl {
    using map_type = tdc::compact_hash::map::sparse_layered_hashmap_t<uint64_t>;
    map_type map_;
    
    LayeredSparseImpl(int n) : map_(n, 64, 64) {}
};

const BenchmarkObjectType BenchmarkLayeredSparse::TYPE =
    BenchmarkObjectType::LAYERED_SPARSE;

BenchmarkLayeredSparse::BenchmarkLayeredSparse(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<LayeredSparseImpl>(n)) {}

BenchmarkLayeredSparse::~BenchmarkLayeredSparse() = default;

uint8_t BenchmarkLayeredSparse::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key, typename LayeredSparseImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkLayeredSparse::Query(uint64_t key, uint8_t /*ptr*/) {
    auto ptr = impl_->map_.find(key);
    if (ptr == typename LayeredSparseImpl::map_type::pointer_type()) {
        return 0;
    }
    return *ptr;
}

void BenchmarkLayeredSparse::Update(uint64_t key, uint8_t /*ptr*/,
                                    uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkLayeredSparse::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

}  // namespace tinyptr

