#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <utility>
#include "chained_ht_64.h"
#include "dereference_table_64.h"
#include "xxhash64.h"

using namespace tinyptr;
using namespace std;

uint64_t my_int_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return XXHash64::hash(&tmp, sizeof(int32_t), 0);
}

uint64_t my_key_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    // constrain the key range to test the function within bin more intensively
    return tmp;
}

uint64_t my_value_rand() {
    int tmp = (rand() | (rand() >> 10 << 15));
    return XXHash64::hash(&tmp, sizeof(int32_t), 1);
}

TEST(ChainedHT64_TESTSUITE, STDMapCompliance) {
    srand(233);

    int n = 1e8, m = 1e6;

    map<uint64_t, uint64_t> lala;
    ChainedHT64 chained_ht_64(m);

    while (n--) {
        uint64_t key = my_int_rand(), new_val = my_int_rand(), val;

        val = chained_ht_64.Query(key);
        ASSERT_EQ(val, lala[key]);

        chained_ht_64.Update(key, new_val);
        lala[key] = new_val;
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
