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


TEST(Junction_TESTSUITE, ProtocolCorrectness) {

    auto tab = new junction::ConcurrentMap_Leapfrog<uint32_t, uint32_t>(1 << 4);
    // junction::ConcurrentMap_Grampa<turf::u32, uint32_t> tab(1<<25);

    for (int i = 1; i < 1000000; i++) {
        // cout << "assign " << i << endl;
        tab->assign(7, i+1);
        // cout << "get " << i << endl;
        tab->get(i+1);
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}