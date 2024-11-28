#include "skulker_ht.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <unordered_map>
#include <utility>

using namespace tinyptr;
using namespace std;

uint64_t my_int_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
}

uint64_t my_sparse_key_rand() {
    // int tmp = (rand() & ((1 << 8) - 1));
    int tmp = (rand() & ((1 << 15) - 1));
    return SlowXXHash64::hash(&tmp, sizeof(int32_t), 0);
    // return tmp;
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

TEST(SkulkerHT_TESTSUITE, StdMapCompliance_INSERT_QUERY) {
    srand(233);

    int n = 1e6, m = 1e6;
    // int n = 1e8, m = 1e8;
    // int n = 1000000, m = 1 << 16;

    std::unordered_map<uint64_t, uint64_t> lala;
    // tinyptr::ByteArrayChainedHT chained_ht(m, 24, 127);
    tinyptr::SkulkerHT skulker_ht(m, 127);


    int cnt = 0;    
    while (n--) {

        // cout << "cnt: " << cnt++ << endl;

        uint64_t key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        // if (((key >> 17) << 17) == 17860047068280782848ull) {
        //     cout << "key: " << key << endl;
        // }

        // if (new_val == 5605366881116195956ull) {
        //     cout << "insert key: " << key << endl;
        //     cout << "new_val: " << new_val << endl;
        // }
        // if (new_val == 2879750557686564615ull) {
        //     cout << "insert key: " << key << endl;
        //     cout << "new_val: " << new_val << endl;
        // }

        if (lala.find(key) == lala.end() && skulker_ht.Insert(key, new_val)) {
            lala[key] = new_val;
        }

        key = my_sparse_key_rand(), new_val = my_value_rand(), val = 0;

        
        // if (((key >> 17) << 17) == 17860047068280782848ull) {
        //     cout << "query key: " << key << endl;
        // }

        if (lala.find(key) != lala.end()) {
            ASSERT_TRUE(skulker_ht.Query(key, &val));
            ASSERT_EQ(val, lala[key]);
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
