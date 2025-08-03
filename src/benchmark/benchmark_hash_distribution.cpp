#include "benchmark_hash_distribution.h"
#include <cstdint>

namespace tinyptr {

const BenchmarkObjectType BenchmarkHashDistribution::TYPE =
    BenchmarkObjectType::HASH_DISTRIBUTION;

BenchmarkHashDistribution::BenchmarkHashDistribution(int n)
    : BenchmarkObject64(TYPE) {
    bin_vec = std::vector<std::atomic<uint64_t> >(n, 0);
    cnt_vec = std::vector<std::atomic<uint64_t> >(n, 0);
    kQuotientingTailLength = 0;
    while (n > 0) {
        n >>= 1;
        kQuotientingTailLength++;
    }
    kQuotientingTailMask = (1 << kQuotientingTailLength) - 1;
    assert(kQuotientingTailMask == n - 1);
    kHashSeed1 = 0;
}

uint8_t BenchmarkHashDistribution::Insert(uint64_t key, uint64_t value) {
    return 0;
}

uint64_t BenchmarkHashDistribution::Query(uint64_t key, uint8_t ptr) {
    return 0;
}

void BenchmarkHashDistribution::Update(uint64_t key, uint8_t ptr,
                                       uint64_t value) {}

void BenchmarkHashDistribution::Erase(uint64_t key, uint8_t ptr) {}

void BenchmarkHashDistribution::Concurrent_Simulation(int num_threads) {
    std::vector<std::thread> threads;

    size_t chunk_size = op_cnt / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? op_cnt : start_index + chunk_size;

        threads.emplace_back([this, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t key = j;
                uint64_t hash_val = hash(key);
                bin_vec[hash_val].fetch_add(1);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Clear threads vector before reusing it
    threads.clear();
    
    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? op_cnt : start_index + chunk_size;

        threads.emplace_back([this, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                cnt_vec[bin_vec[j]].fetch_add(1);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }    
}

std::vector<std::pair<uint64_t, uint64_t> > BenchmarkHashDistribution::Occupancy_Distribution() {
    std::vector<std::pair<uint64_t, uint64_t> > occupancy_distribution;

    uint64_t cnt = 0;

    for (size_t i = 0; i < cnt_vec.size(); ++i) {
        occupancy_distribution.push_back(std::make_pair(i, cnt_vec[i].load()));
        cnt += cnt_vec[i].load() * i;

        if (cnt == op_cnt) {
            break;
        }
    }
    return occupancy_distribution;
}

}  // namespace tinyptr