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
#include "nonconc_blast_ht.h"
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

TEST(BlastHT_TESTSUITE, StdMapCompliance_INSERT_QUERY) {
    srand(233);

    int n = 1 << 15, m = 1 << 15;
    // int n = 1e8, m = 1e8;
    // int n = 1000000, m = 1 << 16;

    std::unordered_map<uint64_t, uint64_t> lala;
    // tinyptr::ByteArrayChainedHT skulker_ht(m, 24, 127);
    tinyptr::NonConcBlastHT blast_ht(m, 127);

    int cnt = 0;
    while (n--) {

        // cout << "cnt: " << cnt++ << endl;

        uint64_t key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        if (lala.find(key) == lala.end() && blast_ht.Insert(key, new_val)) {
            lala[key] = new_val;
        }

        key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        if (lala.find(key) != lala.end()) {
            ASSERT_TRUE(blast_ht.Query(key, &val));
            ASSERT_EQ(val, lala[key]);
        }
    }
}


TEST(BlastHT_TESTSUITE, StdMapCompliance) {
    srand(233);

    int n = 1e6, m = 1 << 14;
    std::unordered_map<uint64_t, uint64_t> lala;
    tinyptr::NonConcBlastHT blast_ht(m, 127);

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
