#include "benchmark_cuckoo.h"
#include <cstdint>
#include <chrono>
#include <algorithm>
#include <execution>

namespace tinyptr {

const BenchmarkObjectType BenchmarkCuckoo::TYPE = BenchmarkObjectType::CUCKOO;

BenchmarkCuckoo::BenchmarkCuckoo(int n) : BenchmarkObject64(TYPE) {
    tab = new libcuckoo::cuckoohash_map<uint64_t, uint64_t>(n);
}

// Wrapper function definitions
[[gnu::noinline]] bool BenchmarkCuckoo::insert_wrapper(uint64_t k, uint64_t v) {
    bool result = tab->insert(k, v);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
    return result;
}

[[gnu::noinline]] bool BenchmarkCuckoo::find_wrapper(uint64_t k, uint64_t& v) {
    bool result = tab->find(k, v);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
    return result;
}

[[gnu::noinline]] void BenchmarkCuckoo::update_wrapper(uint64_t k, uint64_t v) {
    tab->update(k, v);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
}

[[gnu::noinline]] bool BenchmarkCuckoo::erase_wrapper(uint64_t k) {
    bool result = tab->erase(k);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
    return result;
}

uint8_t BenchmarkCuckoo::Insert(uint64_t key, uint64_t value) {
    bool result = insert_wrapper(key, value);
    asm volatile("nop" ::: "memory");  // Single nop instruction to prevent tail call
    return result ? 0 : ~0;
}

uint64_t BenchmarkCuckoo::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    find_wrapper(key, value);
    asm volatile("nop" ::: "memory");  // Single nop instruction to prevent tail call
    return value;
}

void BenchmarkCuckoo::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    update_wrapper(key, value);
    asm volatile("nop" ::: "memory");  // Single nop instruction to prevent tail call
}

void BenchmarkCuckoo::Erase(uint64_t key, uint8_t ptr) {
    erase_wrapper(key);
    asm volatile("nop" ::: "memory");  // Single nop instruction to prevent tail call
}

void BenchmarkCuckoo::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                insert_wrapper(keys[j], 0);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkCuckoo::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                              int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    insert_wrapper(ops[j].second, 0);
                } else if (ops[j].first == 2) {
                    erase_wrapper(ops[j].second);
                } else {
                    find_wrapper(ops[j].second, value);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> BenchmarkCuckoo::YCSBRunWithLatencyRecording(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads, uint64_t record_num,
    const std::vector<double>& percentiles) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> thread_latencies(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, &thread_latencies, i]() {
            std::vector<std::pair<uint64_t, uint64_t>> local_latencies;
            local_latencies.reserve(end_index - start_index);
            
            uint64_t value;
            
            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
                
                if (ops[j].first == 1) {
                    insert_wrapper(ops[j].second, 0);
                } else {
                    find_wrapper(ops[j].second, value);
                }
                
                uint64_t end_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
                uint64_t latency = end_time - start_time;
                local_latencies.emplace_back(ops[j].first, latency);
            }
            
            // Store the results in the thread-specific vector
            thread_latencies[i] = std::move(local_latencies);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return ComputeYCSBLatencyPercentiles(thread_latencies, percentiles);
}

void BenchmarkCuckoo::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    insert_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    find_wrapper(std::get<1>(ops[j]), value);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    update_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    erase_wrapper(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> BenchmarkCuckoo::ConcurrentRunWithLatencyRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads, uint64_t record_num,
    const std::vector<double>& percentiles) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> thread_latencies(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, &thread_latencies, i]() {
            std::vector<std::pair<uint64_t, uint64_t>> local_latencies;
            local_latencies.reserve(end_index - start_index);
            
            uint64_t value;
            
            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
                
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    insert_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    find_wrapper(std::get<1>(ops[j]), value);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    update_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    erase_wrapper(std::get<1>(ops[j]));
                }
                
                uint64_t end_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::high_resolution_clock::now().time_since_epoch()).count();
                uint64_t latency = end_time - start_time;
                local_latencies.emplace_back(std::get<0>(ops[j]), latency);
            }
            
            // Store the results in the thread-specific vector
            thread_latencies[i] = std::move(local_latencies);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return ComputeConcurrentLatencyPercentiles(thread_latencies, percentiles);
}

std::vector<uint64_t> BenchmarkCuckoo::ConcurrentInsertWithTimestampRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads, uint64_t k) {
    return RecordInsertionTimestamps(
        ops, num_threads, k,
        [this, &ops](size_t j, int /*thread_id*/) {
            // Execute the actual insertion operation using wrapper
            if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                insert_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
            }
        });
}

}  // namespace tinyptr