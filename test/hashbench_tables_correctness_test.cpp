#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "benchmark/benchmark_hashbench_tables.h"

namespace {

constexpr std::size_t kHashbenchTableSize = 1 << 16;
constexpr uint64_t kSaltA = 0xC0FFEEBAADF00DULL;
constexpr uint64_t kSaltB = 0x123456789ABCDEF0ULL;

inline uint64_t ValueForKey(uint64_t key, uint64_t salt) {
    return (key * 6364136223846793005ULL) ^ salt;
}

template <typename Table>
void RunInsertThenUpdate(Table& table) {
    for (uint64_t key = 0; key < kHashbenchTableSize; ++key) {
        table.Insert(key, ValueForKey(key, kSaltA));
    }

    for (uint64_t key = 0; key < kHashbenchTableSize; ++key) {
        ASSERT_EQ(table.Query(key, 0), ValueForKey(key, kSaltA));
        table.Update(key, 0, ValueForKey(key, kSaltB));
        EXPECT_EQ(table.Query(key, 0), ValueForKey(key, kSaltB));
    }
}

template <typename Table>
void RunErase(Table& table) {
    for (uint64_t key = 0; key < kHashbenchTableSize; ++key) {
        table.Erase(key, 0);
        EXPECT_EQ(table.Query(key, 0), 0);
    }
}

template <typename Table>
void RunNegativeQueries(Table& table) {
    constexpr uint64_t kNegativeStart = kHashbenchTableSize;
    constexpr uint64_t kNegativeEnd = kNegativeStart + 1024;
    volatile uint64_t accumulator = 0;
    for (uint64_t key = kNegativeStart; key < kNegativeEnd; ++key) {
        const uint64_t value = table.Query(key, 0);
        accumulator += value;
        EXPECT_EQ(value, 0);
    }
    EXPECT_EQ(accumulator, 0);
}

}  // namespace

using namespace tinyptr;

TEST(HashbenchTables, BucketTableBasicOps) {
    tinyptr::BenchmarkBucketTable table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
    RunErase(table);
}

TEST(HashbenchTables, GroupChainingBasicOps) {
    tinyptr::BenchmarkGroupChaining table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
    RunErase(table);
}

TEST(HashbenchTables, ClearyPlainBasicOps) {
    tinyptr::BenchmarkClearyPlain table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
}

TEST(HashbenchTables, ClearySparseBasicOps) {
    tinyptr::BenchmarkClearySparse table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
}

TEST(HashbenchTables, LayeredPlainBasicOps) {
    tinyptr::BenchmarkLayeredPlain table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
}

TEST(HashbenchTables, LayeredSparseBasicOps) {
    tinyptr::BenchmarkLayeredSparse table(kHashbenchTableSize);
    RunInsertThenUpdate(table);
    RunNegativeQueries(table);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

