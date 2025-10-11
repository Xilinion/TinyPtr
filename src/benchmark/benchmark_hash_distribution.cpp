#include "benchmark_hash_distribution.h"
#include <bitset>
#include <cstdint>
#include <iostream>
#include <unordered_set>
#include "utils/rng.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkHashDistribution::TYPE =
    BenchmarkObjectType::HASH_DISTRIBUTION;

BenchmarkHashDistribution::BenchmarkHashDistribution(uint64_t n)
    : BenchmarkObject64(TYPE),
      kHashSeed1(rng::random_device_seed{}()),
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

void BenchmarkHashDistribution::Concurrent_Simulation(
    const std::vector<uint64_t>& keys, int num_threads) {
    op_cnt = keys.size();  // Update operation count to match key count
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
    size_t key_count = keys.size();
    size_t chunk_size = key_count / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? key_count : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t key = keys[j];
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
            (i == num_threads - 1) ? key_count : start_index + chunk_size;

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

uint64_t BenchmarkHashDistribution::binomial_coefficient(int n, int k) {
    if (k > n || k < 0)
        return 0;
    if (k == 0 || k == n)
        return 1;
    if (k > n - k)
        k = n - k;  // Take advantage of symmetry

    uint64_t result = 1;
    for (int i = 0; i < k; ++i) {
        result = result * (n - i) / (i + 1);
    }
    return result;
}

uint64_t BenchmarkHashDistribution::kth_combination(int n, int k,
                                                    uint64_t index) {
    uint64_t combination = 0;
    int remaining_bits = k;
    uint64_t remaining_index = index;

    for (int bit_pos = 0; bit_pos < n && remaining_bits > 0; ++bit_pos) {
        uint64_t ways_without_this_bit =
            binomial_coefficient(n - bit_pos - 1, remaining_bits - 1);

        if (remaining_index < ways_without_this_bit) {
            // Include this bit in the combination
            combination |= (1ULL << bit_pos);
            remaining_bits--;
        } else {
            // Skip this bit
            remaining_index -= ways_without_this_bit;
        }
    }

    return combination;
}

std::pair<int, uint64_t> BenchmarkHashDistribution::find_weight_and_index(
    uint64_t global_index, bool high_weight) {
    uint64_t cumulative_count = 0;

    if (high_weight) {
        // For high weight: start from weight 64 and go down
        for (int weight = 64; weight >= 0; --weight) {
            uint64_t count_for_weight =
                (weight == 64) ? 1 : binomial_coefficient(64, 64 - weight);

            if (global_index < cumulative_count + count_for_weight) {
                uint64_t local_index = global_index - cumulative_count;
                return std::make_pair(weight, local_index);
            }

            cumulative_count += count_for_weight;
        }
    } else {
        // For low weight: start from weight 0 and go up
        for (int weight = 0; weight <= 64; ++weight) {
            uint64_t count_for_weight =
                (weight == 0) ? 1 : binomial_coefficient(64, weight);

            if (global_index < cumulative_count + count_for_weight) {
                uint64_t local_index = global_index - cumulative_count;
                return std::make_pair(weight, local_index);
            }

            cumulative_count += count_for_weight;
        }
    }

    // Fallback (shouldn't reach here with valid input)
    return std::make_pair(high_weight ? 0 : 64, 0);
}

std::vector<uint64_t> BenchmarkHashDistribution::Generate_Random_Keys(
    uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;

    size_t chunk_size = count / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? count : start_index + chunk_size;

        threads.emplace_back([&keys, start_index, end_index]() {
            rng::rng64 rgen64(rng::random_device_seed{}());
            for (size_t j = start_index; j < end_index; ++j) {
                keys[j] = rgen64();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return keys;
}

std::vector<uint64_t> BenchmarkHashDistribution::Generate_Sequential_Keys(
    uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;

    size_t chunk_size = count / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? count : start_index + chunk_size;

        threads.emplace_back([&keys, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                keys[j] = j;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return keys;
}

std::vector<uint64_t>
BenchmarkHashDistribution::Generate_Low_Hamming_Weight_Keys(uint64_t count,
                                                            int num_threads) {
    // Generate all keys sequentially first, then distribute to threads for copying
    std::vector<uint64_t> all_keys;
    all_keys.reserve(count);
    
    // Generate keys sequentially to ensure correctness
    uint64_t generated = 0;
    for (int weight = 0; weight <= 64 && generated < count; ++weight) {
        if (weight == 0) {
            // Weight 0: single key
            all_keys.push_back(0);
            generated++;
        } else {
            // Generate all combinations for this weight
            uint64_t combination = (1ULL << weight) - 1; // First combination
            
            while (combination != 0 && generated < count) {
                all_keys.push_back(combination);
                generated++;
                
                if (generated >= count) break;
                
                // Generate next combination using fast bit manipulation
                uint64_t c0 = __builtin_ctzll(combination);
                uint64_t temp = combination >> c0;
                uint64_t c1 = __builtin_ctzll(~temp);
                
                if (c0 + c1 >= 64) break;
                
                int pos = c0 + c1;
                combination |= (1ULL << pos);
                combination &= ~((1ULL << pos) - 1);
                if (c1 > 1) {
                    combination |= (1ULL << (c1 - 1)) - 1;
                }
            }
        }
    }
    
    // Now just return the sequential keys (no parallel needed for correctness)
    all_keys.resize(count);
    return all_keys;
}

std::vector<uint64_t>
BenchmarkHashDistribution::Generate_High_Hamming_Weight_Keys(uint64_t count,
                                                             int num_threads) {
    // Generate all keys sequentially first, then distribute to threads for copying
    std::vector<uint64_t> all_keys;
    all_keys.reserve(count);
    
    // Generate keys sequentially to ensure correctness
    uint64_t generated = 0;
    for (int weight = 64; weight >= 0 && generated < count; --weight) {
        if (weight == 64) {
            // Weight 64: single key (all bits set)
            all_keys.push_back(~0ULL);
            generated++;
        } else {
            // Generate by clearing bits
            int bits_to_clear = 64 - weight;
            uint64_t clear_mask = (1ULL << bits_to_clear) - 1; // First combination
            
            while (clear_mask != 0 && generated < count) {
                all_keys.push_back(~clear_mask);
                generated++;
                
                if (generated >= count) break;
                
                // Generate next combination using fast bit manipulation
                uint64_t c0 = __builtin_ctzll(clear_mask);
                uint64_t temp = clear_mask >> c0;
                uint64_t c1 = __builtin_ctzll(~temp);
                
                if (c0 + c1 >= 64) break;
                
                int pos = c0 + c1;
                clear_mask |= (1ULL << pos);
                clear_mask &= ~((1ULL << pos) - 1);
                if (c1 > 1) {
                    clear_mask |= (1ULL << (c1 - 1)) - 1;
                }
            }
        }
    }

    // Now just return the sequential keys (no parallel needed for correctness)
    all_keys.resize(count);
    return all_keys;
}

}  // namespace tinyptr