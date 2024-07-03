#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <utility>
#include "dereference_table_64.h"
#include "xxhash64.h"

using namespace tinyptr;
using namespace std;

uint64_t my_int_rand() {
    int tmp = (rand() & ((1 << 7) - 1));
    // int tmp = (rand() | (rand() >> 10 << 15));
    return XXHash64::hash(&tmp, sizeof(int32_t), 0);
}

TEST(DereferenceTable64_TESTSUITE, RandomAll) {
    srand(233);

    int n = 1e6, m = 1e4;
    // int n = 1e8, m = 1e6;

    map<uint64_t, pair<uint8_t, uint64_t>> lala;
    DereferenceTable64 dereftab(m);

    while (n--) {
        uint64_t key = my_int_rand(), new_val = my_int_rand(), val;
        uint8_t ptr;

        if (lala.find(key) != lala.end()) {
            ASSERT_TRUE(dereftab.Query(key, lala[key].first, &val));
            ASSERT_EQ(lala[key].second, val);

            if (rand() & 1) {
                ASSERT_TRUE(dereftab.Update(key, lala[key].first, new_val));
                lala[key].second = new_val;
            } else {
                ASSERT_TRUE(dereftab.Free(key, lala[key].first));
                lala.erase(key);
            }
        } else {
            if (rand() & 1)
                ASSERT_FALSE(dereftab.Free(key, my_int_rand()));
            if (rand() & 1)
                ASSERT_FALSE(dereftab.Update(key, my_int_rand(), new_val));
            if (rand() & 1)
                ASSERT_FALSE(dereftab.Query(key, my_int_rand(), &val));

            ptr = dereftab.Allocate(key, new_val);
            lala[key] = make_pair(ptr, new_val);
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
