#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <unordered_map>
#include <utility>
#include "same_bin_chained_ht.h"

using namespace tinyptr;
using namespace std;

uint64_t my_int_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
}

uint64_t my_sparse_key_rand() {
    int tmp = (rand() & ((1 << 15) - 1));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
}

uint64_t my_key_rand() {
    // int tmp = (rand() | (rand() >> 10 << 15));
    int tmp = (rand() & ((1 << 8) - 1));
    // constrain the key range to test the function within bin more intensively
    return tmp;
}

uint64_t my_value_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 1);
}

TEST(SameBinChainedHT_TESTSUITE, StdMapCompliance) {
    srand(233);

    int n = 1e6, m = 1e6;
    // int n = 1e8, m = 1e8;

    std::unordered_map<uint64_t, uint64_t> lala;
    // tinyptr::ByteArrayChainedHT chained_ht(m, 24, 127);
    tinyptr::SameBinChainedHT chained_ht(m, 127);

    while (n--) {
        uint64_t key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;
        
        if (lala.find(key) == lala.end() && chained_ht.Insert(key, new_val)) {
            lala[key] = new_val;
        }

        key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        if (chained_ht.Query(key, &val)) {
            ASSERT_EQ(val, lala[key]);
        }

        if (chained_ht.Update(key, new_val)) {
            lala[key] = new_val;
        }

        if (!(rand() & ((1 << 3) - 1)))
            chained_ht.Free(key), lala.erase(key);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
