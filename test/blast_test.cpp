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
#include "blast_ht.h"
#include "utils/rng.h"

rng::rng64 rng64(123456789);

using namespace tinyptr;
using namespace std;

// Ensure SlowXXHash64 is defined or included
// If SlowXXHash64 is part of an external library, ensure it is linked correctly

// Ensure BlastHT is defined in concurrent_blast_ht.h
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

uint64_t my_sparse_key_rand() {
    // int tmp = (rand() & ((1 << 8) - 1));
    int tmp = (rand() & ((1 << 15) - 1));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
    // return tmp;
}

void concurrent_insert(BlastHT& ht,
                       const vector<pair<uint64_t, uint64_t>>& data, int start,
                       int end) {
    for (int i = start; i < end; ++i) {
        // ht.Insert(data[i].first, data[i].second);
        if (!ht.Insert(data[i].first, data[i].second)) {
            printf("insert failed: %d %llu, %llu\n", i, data[i].first,
                   data[i].second);
            exit(0);
        }
    }
}

void concurrent_query(BlastHT& ht, const vector<pair<uint64_t, uint64_t>>& data,
                      int start, int end) {
    for (int i = start; i < end; ++i) {
        uint64_t val = 0;
        // std::cout << i - start << std::endl;
        // ht.Query(data[i].first, &val);
        // bool res = ht.Query(data[i].first, &val);
        // if (!res) {
        //     std::cout << "query failed: " << i << " " << data[i].first
        //               << std::endl;
        //     exit(0);
        // }
        // if (val != data[i].second) {
        //     std::cout << "query failed: " << i << " " << data[i].first << " "
        //               << val << " " << data[i].second << std::endl;
        //     exit(0);
        // }
        ASSERT_TRUE(ht.Query(data[i].first, &val));
        ASSERT_EQ(val, data[i].second);
    }
}

TEST(BlastHT_TESTSUITE, ParallelInsertQuery) {
    srand(233);

    // int num_threads = std::thread::hardware_concurrency();
    int num_threads = 16;
    int num_operations = (1ull << 27) - 1;  // Large number of operations

    // Declare start and end for timing
    chrono::time_point<chrono::high_resolution_clock> start, end;

    // Ensure data is declared and initialized
    vector<pair<uint64_t, uint64_t>> data(num_operations);
    for (int i = 0; i < num_operations; ++i) {
        // cout << i << endl;
        data[i] = {static_cast<uint64_t>(i * 233), my_value_rand()};
    }

    // Ensure SlowXXHash64 is defined or included
    // If SlowXXHash64 is part of an external library, ensure it is linked correctly

    // Ensure BlastHT is defined in concurrent_blast_ht.h
    // If not, include the correct header or define the class

    // Correct the namespace usage
    using namespace std;
    using namespace tinyptr;  // Ensure tinyptr is a valid namespace

    start = chrono::high_resolution_clock::now();
    BlastHT ht(static_cast<uint64_t>(num_operations), 0);
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


TEST(BlastHT_TESTSUITE, StdMapCompliance) {
    srand(233);

    int n = 1e6, m = 1 << 14;
    std::unordered_map<uint64_t, uint64_t> lala;
    tinyptr::BlastHT blast_ht(m, 127);

    int flag = 0;

    while (n--) {
        uint64_t key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        if (lala.find(key) == lala.end() && blast_ht.Insert(key, new_val)) {
            lala[key] = new_val;
        }

        key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        if (lala.find(key) != lala.end()) {
            ASSERT_TRUE(blast_ht.Query(key, &val));
            ASSERT_EQ(val, lala[key]);
        }

        if (blast_ht.Update(key, new_val)) {
            lala[key] = new_val;
        }

        if (3 > (rand() & ((1 << 3) - 1))) {
            blast_ht.Free(key), lala.erase(key);
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
