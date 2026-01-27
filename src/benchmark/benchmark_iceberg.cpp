#include "benchmark_iceberg.h"
#include <pthread.h>
#include <fstream>
#include <thread>
#include "unistd.h"
#include <chrono>
#include <algorithm>
#include <execution>

namespace tinyptr {

const BenchmarkObjectType BenchmarkIceberg::TYPE = BenchmarkObjectType::ICEBERG;

uint64_t BenchmarkIceberg::auto_iceberg_slot_num(uint64_t size) {
    uint64_t res = 0;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

BenchmarkIceberg::BenchmarkIceberg(int n) : BenchmarkObject64(TYPE) {
    iceberg_init(&tab, auto_iceberg_slot_num(n));
    tid = gettid();
}

uint8_t BenchmarkIceberg::Insert(uint64_t key, uint64_t value) {
    return iceberg_insert(&tab, key, value, tid);
}

uint64_t BenchmarkIceberg::Query(uint64_t key, uint8_t ptr) {
    uint64_t res;
    iceberg_get_value(&tab, key, &res, tid);
    return res;
}

void BenchmarkIceberg::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    iceberg_insert(&tab, key, value, tid);
}

void BenchmarkIceberg::Erase(uint64_t key, uint8_t ptr) {
    iceberg_remove(&tab, key, tid);
}

void BenchmarkIceberg::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                iceberg_insert(&tab, keys[j], 0, i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkIceberg::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                               int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;
    const auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    iceberg_insert(&tab, ops[j].second, 0, i);
                } else if (ops[j].first == 2) {
                    iceberg_remove(&tab, ops[j].second, i);
                } else {
                    iceberg_get_value(&tab, ops[j].second, &value, i);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> BenchmarkIceberg::YCSBRunWithLatencyRecording(
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
                    iceberg_insert(&tab, ops[j].second, 0, i);
                } else {
                    iceberg_get_value(&tab, ops[j].second, &value, i);
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

void BenchmarkIceberg::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    iceberg_get_value(&tab, std::get<1>(ops[j]), &value, i);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    iceberg_remove(&tab, std::get<1>(ops[j]), i);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>> BenchmarkIceberg::ConcurrentRunWithLatencyRecording(
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
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    iceberg_get_value(&tab, std::get<1>(ops[j]), &value, i);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    iceberg_insert(&tab, std::get<1>(ops[j]),
                                   std::get<2>(ops[j]), i);
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    iceberg_remove(&tab, std::get<1>(ops[j]), i);
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

std::vector<uint64_t>
BenchmarkIceberg::ConcurrentInsertWithTimestampRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads, uint64_t k) {
    std::vector<std::vector<uint64_t>> thread_timestamps(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;
    const auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, &thread_timestamps, i, k, start_time]() {
            std::vector<uint64_t> local_timestamps;
            local_timestamps.reserve((end_index - start_index) / k + 1);

            uint64_t local_op_count = 0;

            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    iceberg_insert(&tab, std::get<1>(ops[j]), std::get<2>(ops[j]), i);
                }

                local_op_count++;
                // Record timestamp after every k operations
                if (local_op_count % k == 0) {
                    uint64_t timestamp =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - start_time)
                            .count();
                    local_timestamps.emplace_back(timestamp);
                }
            }

            thread_timestamps[i] = std::move(local_timestamps);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Merge all thread timestamps
    std::vector<uint64_t> all_timestamps;
    for (const auto& thread_result : thread_timestamps) {
        all_timestamps.insert(all_timestamps.end(), thread_result.begin(),
                              thread_result.end());
    }

    // Sort by timestamp
    std::sort(std::execution::par, all_timestamps.begin(), all_timestamps.end(),
              [](uint64_t a, uint64_t b) { return a < b; });

    return all_timestamps;
}

}  // namespace tinyptr