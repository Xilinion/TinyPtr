#pragma once

#if 0

#include <atomic>
#include <cstdint>
#include <random>
#include <thread>
#include <unordered_map>
#include "benchmark_object_64.h"
#include "benchmark_object_type.h"

#include "allocator/alignedallocator.hpp"
#include "data-structures/hash_table_mods.hpp"
#include "utils/hash/murmur2_hash.hpp"

using hasher_type = utils_tm::hash_tm::murmur2_hash;
using allocator_type = growt::AlignedAllocator<>;

#include "data-structures/table_config.hpp"

using table_type =
    typename growt::table_config<uint64_t, uint64_t, hasher_type,
                                 allocator_type, hmod::growable>::table_type;

namespace tinyptr {

class BenchmarkGrowt : public BenchmarkObject64 {

   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkGrowt(int n);

    ~BenchmarkGrowt() = default;

    uint8_t Insert(uint64_t key, uint64_t value) override;
    uint64_t Query(uint64_t key, uint8_t ptr) override;
    void Update(uint64_t key, uint8_t ptr, uint64_t value) override;
    void Erase(uint64_t key, uint8_t ptr) override;

   private:
    table_type ht;

    // Wrapper functions
    [[gnu::noinline]] void insert_wrapper(uint64_t k, uint64_t v);
    [[gnu::noinline]] uint64_t query_wrapper(uint64_t k);
    [[gnu::noinline]] void update_wrapper(uint64_t k, uint64_t v);
    [[gnu::noinline]] void erase_wrapper(uint64_t k);
};

}  // namespace tinyptr
#endif