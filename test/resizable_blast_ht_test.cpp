#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>
#include "concurrent_skulker_ht.h"
#include "resizable_ht.h"
#include "utils/rng.h"

rng::rng64 rng64(123456789);

using namespace tinyptr;
using namespace std;

// Ensure SlowXXHash64 is defined or included
// If SlowXXHash64 is part of an external library, ensure it is linked correctly

// Ensure ConcurrentSkulkerHT is defined in concurrent_skulker_ht.h
// If not, include the correct header or define the class

uint64_t my_int_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
}

uint64_t my_value_rand() {
    // int tmp = (rand() | (rand() >> 10 << 15));
    return rng64();
    // cout << tmp << endl;
    // return SlowXXHash64::hash(&tmp, sizeof(int32_t), 1);
}

void concurrent_insert(ResizableBlastHT& ht,
                       const vector<pair<uint64_t, uint64_t>>& data, int start,
                       int end) {

    uint64_t handle = ht.GetHandle();
    for (int i = start; i < end; ++i) {
        if (!ht.Insert(handle, data[i].first, data[i].second)) {
            printf("insert failed: %lu, %lu\n", data[i].first,
                   data[i].second);
            // printf("handle: %llu\n", handle);
            // printf("i: %llu\n", i);
            // ht.FreeHandle(handle);
            // exit(0);
        }
    }
    ht.FreeHandle(handle);
}

void concurrent_query(ResizableBlastHT& ht,
                      const vector<pair<uint64_t, uint64_t>>& data, int start,
                      int end) {
    uint64_t handle = ht.GetHandle();
    for (int i = start; i < end; ++i) {
        uint64_t val = 0;
        // std::cout << i - start << std::endl;
        // ht.Query(data[i].first + 1000000000000000000ull, &val);
        // ht.Query(handle, data[i].first, &val);
        ASSERT_TRUE(ht.Query(handle, data[i].first, &val));
        ASSERT_EQ(val, data[i].second);
    }
    ht.FreeHandle(handle);
}

TEST(ResizableBlastHT_TESTSUITE, ParallelInsertQuery) {
    srand(233);

    // int num_threads = std::thread::hardware_concurrency();
    int num_threads = 10;
    int num_operations = 134217728;  // Large number of operations
    int part_num = 10;
    // Declare start and end for timing
    chrono::time_point<chrono::high_resolution_clock> start, end;

    // Ensure data is declared and initialized
    vector<pair<uint64_t, uint64_t>> data(num_operations);
    for (int i = 0; i < num_operations; ++i) {
        // cout << i << endl;
        data[i] = {static_cast<uint64_t>(i) * 233ULL, my_value_rand()};
    }

    // Ensure SlowXXHash64 is defined or included
    // If SlowXXHash64 is part of an external library, ensure it is linked correctly

    // Ensure ConcurrentSkulkerHT is defined in concurrent_skulker_ht.h
    // If not, include the correct header or define the class

    start = chrono::high_resolution_clock::now();

    ResizableBlastHT ht(1 * 1 * 16777216 / part_num, part_num, num_threads);
    // ResizableEmptyHT ht(num_threads, part_num, 1000000/part_num);

    end = chrono::high_resolution_clock::now();
    std::cout
        << "init time: "
        << chrono::duration_cast<chrono::milliseconds>(end - start).count()
        << "ms" << std::endl;

    start = chrono::high_resolution_clock::now();

    // Concurrent insertions
    vector<thread> insert_threads;
    int operations_per_thread = num_operations / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        int start = i * operations_per_thread;
        int end = (i == num_threads - 1) ? num_operations
                                         : start + operations_per_thread;
        insert_threads.emplace_back(concurrent_insert, std::ref(ht),
                                    std::cref(data), start, end);
    }

    for (auto& thread : insert_threads) {
        thread.join();
    }

    end = chrono::high_resolution_clock::now();
    std::cout
        << "insert time: "
        << chrono::duration_cast<chrono::milliseconds>(end - start).count()
        << "ms" << std::endl;

    start = chrono::high_resolution_clock::now();
    // Concurrent queries
    vector<thread> query_threads;
    for (int i = 0; i < num_threads; ++i) {
        int start = i * operations_per_thread;
        int end = (i == num_threads - 1) ? num_operations
                                         : start + operations_per_thread;
        query_threads.emplace_back(concurrent_query, std::ref(ht),
                                   std::cref(data), start, end);
    }

    for (auto& thread : query_threads) {
        thread.join();
    }

    end = chrono::high_resolution_clock::now();
    std::cout
        << "query time: "
        << chrono::duration_cast<chrono::milliseconds>(end - start).count()
        << "ms" << std::endl;
}

int main(int argc, char** argv) {
    srand(233);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
