#include <gtest/gtest.h>
#include <junction/ConcurrentMap_Grampa.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <junction/ConcurrentMap_Linear.h>
#include <turf/Util.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>
#include "utils/rng.h"

using namespace std;

rng::rng64 rng64(123456789);

TEST(Junction_TESTSUITE, ProtocolCorrectness) {

    auto tab =
        new junction::ConcurrentMap_Linear<uint64_t, uint64_t>(1 << 26);
    // junction::ConcurrentMap_Grampa<turf::u32, uint32_t> tab(1<<25);

    auto keys = new uint64_t[1 << 26];
    for (int i = 2; i < (1 << 25); i++) {
        // cout << "assign " << i << endl;
        keys[i] = i * 1ll << 32;
        tab->assign(keys[i], i);
        // cout << "get " << i << endl;
    }

    for (int i = 2; i < (1 << 25); i++) {
        ASSERT_EQ(tab->get(keys[i]), i);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}