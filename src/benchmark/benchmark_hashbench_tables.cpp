#include "benchmark_hashbench_tables.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <execution>
#include <iostream>
#include <separate/bucket.hpp>
#include <separate/compact_chaining_map.hpp>
#include <separate/group_chaining.hpp>
#include <separate/hash.hpp>
#include <separate/separate_chaining_table.hpp>
#include <tudocomp/util/compact_hash/map/typedefs.hpp>

namespace tinyptr {

namespace {
inline size_t pot_capacity(size_t requested) {
    size_t cap = 2;
    while (cap < requested) {
        cap <<= 1;
    }
    return cap;
}
}  // namespace

struct BenchmarkBucketTable::BucketImpl {
    using key_bucket_type = separate_chaining::plain_bucket<uint64_t>;
    using value_bucket_type = separate_chaining::varwidth_bucket<>;
    using hash_t =
        separate_chaining::hash_mapping_adapter<uint64_t,
                                                separate_chaining::SplitMix>;
    // using bucket_table_t =
    //     separate_chaining::separate_chaining_map<key_bucket_type,
    //                                              value_bucket_type, hash_t>;

    using bucket_table_t = separate_chaining::compact_chaining_map<
        separate_chaining::multiplicative_hash<>, uint64_t>;

    bucket_table_t table;

    explicit BucketImpl(int expected) {
        // const size_t reserve_size =
        //     expected > 0 ? static_cast<size_t>(expected) : size_t(1);
        // table.reserve(reserve_size);
    }
};

[[gnu::noinline]] uint64_t BenchmarkBucketTable::QueryBucketImpl(
    BucketImpl& impl, uint64_t key) {
    auto it = impl.table.find(key);
    volatile uint64_t value = 0;
    if (it != impl.table.cend()) {
        value = it.value();
    }
    asm volatile("" ::: "memory");
    return value;
}

struct BenchmarkGroupChaining::GroupImpl {
    using group_table_t = separate_chaining::group::group_chaining_table<
        separate_chaining::multiplicative_hash<>>;
    group_table_t table;

    explicit GroupImpl(int expected) : table(64, 64) {
        const size_t reserve_size =
            expected > 0 ? static_cast<size_t>(expected) : size_t(1);
        table.reserve(reserve_size / 64);
    }
};

[[gnu::noinline]] uint64_t BenchmarkGroupChaining::QueryGroupImpl(
    const GroupImpl& impl, uint64_t key) {
    auto it = impl.table.find(key);
    if (it == impl.table.cend()) {
        asm volatile("" ::: "memory");
        return 0;
    }

    volatile uint64_t value = it.value();
    asm volatile("" ::: "memory");
    return value;
}

const BenchmarkObjectType BenchmarkBucketTable::TYPE =
    BenchmarkObjectType::BUCKET_TABLE;

BenchmarkBucketTable::BenchmarkBucketTable(int n)
    : BenchmarkObject64(TYPE),
      impl_(
          std::make_unique<BucketImpl>(pot_capacity(static_cast<size_t>(n)))) {}

BenchmarkBucketTable::~BenchmarkBucketTable() = default;

uint8_t BenchmarkBucketTable::Insert(uint64_t key, uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename BucketImpl::bucket_table_t::value_type(value));
    (void)navigator;
    return 0;
}

uint64_t BenchmarkBucketTable::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryBucketImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkBucketTable::Update(uint64_t key, uint8_t /*ptr*/,
                                  uint64_t value) {
    auto navigator = impl_->table.find_or_insert(
        key, typename BucketImpl::bucket_table_t::value_type(value));
    navigator = value;
}

void BenchmarkBucketTable::Erase(uint64_t key, uint8_t /*ptr*/) {
    impl_->table.erase(key);
}

void BenchmarkBucketTable::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}


const BenchmarkObjectType BenchmarkGroupChaining::TYPE =
    BenchmarkObjectType::GRP_CHAINING;

BenchmarkGroupChaining::BenchmarkGroupChaining(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<GroupImpl>(n)) {}

BenchmarkGroupChaining::~BenchmarkGroupChaining() = default;

uint8_t BenchmarkGroupChaining::Insert(uint64_t key, uint64_t value) {
    impl_->table[key] = value;
    return 0;
}

uint64_t BenchmarkGroupChaining::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryGroupImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkGroupChaining::Update(uint64_t key, uint8_t /*ptr*/,
                                    uint64_t value) {
    impl_->table[key] = value;
}

void BenchmarkGroupChaining::Erase(uint64_t key, uint8_t /*ptr*/) {
    impl_->table.erase(key);
}

void BenchmarkGroupChaining::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}


struct BenchmarkClearyPlain::ClearyPlainImpl {
    using map_type = tdc::compact_hash::map::plain_cv_hashmap_t<uint64_t>;
    map_type map_;

    ClearyPlainImpl(int n)
        : map_(pot_capacity(static_cast<size_t>(n)), 64, 64) {
        map_.max_load_factor(0.95);
    }
};

[[gnu::noinline]] uint64_t BenchmarkClearyPlain::QueryClearyImpl(
    ClearyPlainImpl& impl, uint64_t key) {
    auto ptr = impl.map_.find(key);
    if (ptr == typename ClearyPlainImpl::map_type::pointer_type()) {
        asm volatile("" ::: "memory");
        return 0;
    }

    uint64_t value = *ptr;
    asm volatile("" ::: "memory");
    return value;
}

const BenchmarkObjectType BenchmarkClearyPlain::TYPE =
    BenchmarkObjectType::CLEARY_PLAIN;

BenchmarkClearyPlain::BenchmarkClearyPlain(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<ClearyPlainImpl>(n)) {}

BenchmarkClearyPlain::~BenchmarkClearyPlain() = default;

uint8_t BenchmarkClearyPlain::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key,
                       typename ClearyPlainImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkClearyPlain::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryClearyImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkClearyPlain::Update(uint64_t key, uint8_t /*ptr*/,
                                  uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkClearyPlain::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

void BenchmarkClearyPlain::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

struct BenchmarkClearySparse::ClearySparseImpl {
    using map_type = tdc::compact_hash::map::sparse_cv_hashmap_t<uint64_t>;
    map_type map_;

    ClearySparseImpl(int n)
        : map_(pot_capacity(static_cast<size_t>(n)), 64, 64) {
        map_.max_load_factor(0.95);
    }
};

[[gnu::noinline]] uint64_t BenchmarkClearySparse::QueryClearyImpl(
    ClearySparseImpl& impl, uint64_t key) {
    auto ptr = impl.map_.find(key);
    if (ptr == typename ClearySparseImpl::map_type::pointer_type()) {
        asm volatile("" ::: "memory");
        return 0;
    }

    uint64_t value = *ptr;
    asm volatile("" ::: "memory");
    return value;
}

const BenchmarkObjectType BenchmarkClearySparse::TYPE =
    BenchmarkObjectType::CLEARY_SPARSE;

BenchmarkClearySparse::BenchmarkClearySparse(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<ClearySparseImpl>(n)) {}

BenchmarkClearySparse::~BenchmarkClearySparse() = default;

uint8_t BenchmarkClearySparse::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key,
                       typename ClearySparseImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkClearySparse::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryClearyImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkClearySparse::Update(uint64_t key, uint8_t /*ptr*/,
                                   uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkClearySparse::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

void BenchmarkClearySparse::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

struct BenchmarkLayeredPlain::LayeredPlainImpl {
    using map_type = tdc::compact_hash::map::plain_layered_hashmap_t<uint64_t>;
    map_type map_;

    LayeredPlainImpl(int n)
        : map_(pot_capacity(static_cast<size_t>(n)), 64, 64) {
        map_.max_load_factor(0.95);
    }
};

[[gnu::noinline]] uint64_t BenchmarkLayeredPlain::QueryClearyImpl(
    LayeredPlainImpl& impl, uint64_t key) {
    auto ptr = impl.map_.find(key);
    if (ptr == typename LayeredPlainImpl::map_type::pointer_type()) {
        asm volatile("" ::: "memory");
        return 0;
    }

    uint64_t value = *ptr;
    asm volatile("" ::: "memory");
    return value;
}

const BenchmarkObjectType BenchmarkLayeredPlain::TYPE =
    BenchmarkObjectType::LAYERED_PLAIN;

BenchmarkLayeredPlain::BenchmarkLayeredPlain(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<LayeredPlainImpl>(n)) {}

BenchmarkLayeredPlain::~BenchmarkLayeredPlain() = default;

uint8_t BenchmarkLayeredPlain::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key,
                       typename LayeredPlainImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkLayeredPlain::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryClearyImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkLayeredPlain::Update(uint64_t key, uint8_t /*ptr*/,
                                   uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkLayeredPlain::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

void BenchmarkLayeredPlain::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

struct BenchmarkLayeredSparse::LayeredSparseImpl {
    using map_type = tdc::compact_hash::map::sparse_layered_hashmap_t<uint64_t>;
    map_type map_;

    LayeredSparseImpl(int n)
        : map_(pot_capacity(static_cast<size_t>(n)), 64, 64) {
        map_.max_load_factor(0.95);
    }
};

[[gnu::noinline]] uint64_t BenchmarkLayeredSparse::QueryClearyImpl(
    LayeredSparseImpl& impl, uint64_t key) {
    auto ptr = impl.map_.find(key);
    if (ptr == typename LayeredSparseImpl::map_type::pointer_type()) {
        asm volatile("" ::: "memory");
        return 0;
    }

    uint64_t value = *ptr;
    asm volatile("" ::: "memory");
    return value;
}

const BenchmarkObjectType BenchmarkLayeredSparse::TYPE =
    BenchmarkObjectType::LAYERED_SPARSE;

BenchmarkLayeredSparse::BenchmarkLayeredSparse(int n)
    : BenchmarkObject64(TYPE), impl_(std::make_unique<LayeredSparseImpl>(n)) {}

BenchmarkLayeredSparse::~BenchmarkLayeredSparse() = default;

uint8_t BenchmarkLayeredSparse::Insert(uint64_t key, uint64_t value) {
    impl_->map_.insert(key,
                       typename LayeredSparseImpl::map_type::value_type(value));
    return 0;
}

uint64_t BenchmarkLayeredSparse::Query(uint64_t key, uint8_t /*ptr*/) {
    uint64_t value = QueryClearyImpl(*impl_, key);
    asm volatile("nop" ::: "memory");
    return value;
}

void BenchmarkLayeredSparse::Update(uint64_t key, uint8_t /*ptr*/,
                                    uint64_t value) {
    impl_->map_.access(key) = value;
}

void BenchmarkLayeredSparse::Erase(uint64_t /*key*/, uint8_t /*ptr*/) {
    // Not supported by this hashtable implementation.
}

void BenchmarkLayeredSparse::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), 0);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace tinyptr
