#include "benchmark_hash_distribution.h"
#include <cstdint>
#include "utils/rng.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkHashDistribution::TYPE =
    BenchmarkObjectType::HASH_DISTRIBUTION;

BenchmarkHashDistribution::BenchmarkHashDistribution(uint64_t n)
    : BenchmarkObject64(TYPE),
      kHashSeed1(rand()),
      bin_vec(n),
      cnt_vec(n),
      op_cnt(n) {
    // Atomic elements will be initialized in Concurrent_Simulation
    kQuotientingTailLength = 0;
    uint64_t temp_n = n;
    while (temp_n > 1) {
        temp_n >>= 1;
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

    // Multithreaded initialization of atomic vectors
    size_t vector_size = bin_vec.size();
    size_t init_chunk_size = vector_size / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * init_chunk_size;
        size_t end_index = (i == num_threads - 1)
                               ? vector_size
                               : start_index + init_chunk_size;

        threads.emplace_back([this, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                bin_vec[j].store(0);
                cnt_vec[j].store(0);
            }
        });
    }

    // Wait for initialization to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Clear threads vector before reusing it
    threads.clear();

    // Main simulation workload
    size_t chunk_size = op_cnt / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? op_cnt : start_index + chunk_size;

        threads.emplace_back([this, start_index, end_index, i]() {
            rng::rng64 rgen64(rng::random_device_seed{}());
            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t key = rgen64();
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

std::vector<std::pair<uint64_t, uint64_t>>
BenchmarkHashDistribution::Occupancy_Distribution() {
    std::vector<std::pair<uint64_t, uint64_t>> occupancy_distribution;

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