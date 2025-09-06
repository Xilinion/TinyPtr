#include "benchmark_hash_distribution.h"
#include <cstdint>
#include <bitset>
#include <iostream>
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

void BenchmarkHashDistribution::Concurrent_Simulation(const std::vector<uint64_t>& keys, int num_threads) {
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
    if (k > n || k < 0) return 0;
    if (k == 0 || k == n) return 1;
    if (k > n - k) k = n - k; // Take advantage of symmetry
    
    uint64_t result = 1;
    for (int i = 0; i < k; ++i) {
        result = result * (n - i) / (i + 1);
    }
    return result;
}

uint64_t BenchmarkHashDistribution::kth_combination(int n, int k, uint64_t index) {
    uint64_t combination = 0;
    int remaining_bits = k;
    uint64_t remaining_index = index;
    
    for (int bit_pos = 0; bit_pos < n && remaining_bits > 0; ++bit_pos) {
        uint64_t ways_without_this_bit = binomial_coefficient(n - bit_pos - 1, remaining_bits - 1);
        
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

std::vector<uint64_t> BenchmarkHashDistribution::Generate_Random_Keys(uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;
    
    size_t chunk_size = count / num_threads;
    
    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? count : start_index + chunk_size;
        
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

std::vector<uint64_t> BenchmarkHashDistribution::Generate_Sequential_Keys(uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;
    
    size_t chunk_size = count / num_threads;
    
    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index = (i == num_threads - 1) ? count : start_index + chunk_size;
        
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

std::vector<uint64_t> BenchmarkHashDistribution::Generate_Low_Hamming_Weight_Keys(uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;
    
    // Calculate total available keys and distribute work
    uint64_t total_generated = 0;
    std::vector<std::pair<int, std::pair<uint64_t, uint64_t>>> weight_ranges; // weight, (start_idx, count)
    
    // Calculate how many keys we need from each weight
    for (int weight = 0; weight <= 64 && total_generated < count; ++weight) {
        uint64_t available_for_weight = (weight == 0) ? 1 : binomial_coefficient(64, weight);
        uint64_t needed_for_weight = std::min(available_for_weight, count - total_generated);
        
        if (needed_for_weight > 0) {
            weight_ranges.emplace_back(weight, std::make_pair(total_generated, needed_for_weight));
            total_generated += needed_for_weight;
        }
    }
    
    // Distribute ranges across threads
    size_t total_work = count;
    size_t work_per_thread = total_work / num_threads;
    
    for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
        size_t start_idx = thread_id * work_per_thread;
        size_t end_idx = (thread_id == num_threads - 1) ? total_work : (thread_id + 1) * work_per_thread;
        
        threads.emplace_back([&, thread_id, start_idx, end_idx]() {
            for (size_t global_idx = start_idx; global_idx < end_idx; ++global_idx) {
                // Find which weight this index belongs to
                for (const auto& weight_range : weight_ranges) {
                    int weight = weight_range.first;
                    uint64_t range_start = weight_range.second.first;
                    uint64_t range_count = weight_range.second.second;
                    
                    if (global_idx >= range_start && global_idx < range_start + range_count) {
                        uint64_t local_idx = global_idx - range_start;
                        
                        if (weight == 0) {
                            keys[global_idx] = 0;
                        } else {
                            keys[global_idx] = kth_combination(64, weight, local_idx);
                        }
                        break;
                    }
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    return keys;
}

std::vector<uint64_t> BenchmarkHashDistribution::Generate_High_Hamming_Weight_Keys(uint64_t count, int num_threads) {
    std::vector<uint64_t> keys(count);
    std::vector<std::thread> threads;
    
    // Calculate total available keys and distribute work (high weight first)
    uint64_t total_generated = 0;
    std::vector<std::pair<int, std::pair<uint64_t, uint64_t>>> weight_ranges; // weight, (start_idx, count)
    
    // Calculate how many keys we need from each weight (64, 63, 62, ...)
    for (int weight = 64; weight >= 0 && total_generated < count; --weight) {
        uint64_t available_for_weight = (weight == 64) ? 1 : binomial_coefficient(64, 64 - weight);
        uint64_t needed_for_weight = std::min(available_for_weight, count - total_generated);
        
        if (needed_for_weight > 0) {
            weight_ranges.emplace_back(weight, std::make_pair(total_generated, needed_for_weight));
            total_generated += needed_for_weight;
        }
    }
    
    // Distribute ranges across threads
    size_t total_work = count;
    size_t work_per_thread = total_work / num_threads;
    
    for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
        size_t start_idx = thread_id * work_per_thread;
        size_t end_idx = (thread_id == num_threads - 1) ? total_work : (thread_id + 1) * work_per_thread;
        
        threads.emplace_back([&, thread_id, start_idx, end_idx]() {
            for (size_t global_idx = start_idx; global_idx < end_idx; ++global_idx) {
                // Find which weight this index belongs to
                for (const auto& weight_range : weight_ranges) {
                    int weight = weight_range.first;
                    uint64_t range_start = weight_range.second.first;
                    uint64_t range_count = weight_range.second.second;
                    
                    if (global_idx >= range_start && global_idx < range_start + range_count) {
                        uint64_t local_idx = global_idx - range_start;
                        
                        if (weight == 64) {
                            keys[global_idx] = ~0ULL;
                        } else {
                            // Generate by clearing bits: use kth_combination for bits to clear
                            int bits_to_clear = 64 - weight;
                            uint64_t clear_mask = kth_combination(64, bits_to_clear, local_idx);
                            keys[global_idx] = ~clear_mask;
                        }
                        break;
                    }
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    return keys;
}

}  // namespace tinyptr