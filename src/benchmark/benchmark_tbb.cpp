#include "benchmark_tbb.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <execution>

namespace tinyptr {

const BenchmarkObjectType BenchmarkTBB::TYPE = BenchmarkObjectType::TBB;

BenchmarkTBB::BenchmarkTBB(int n) : BenchmarkObject64(TYPE), tab(n) {
    tab.max_load_factor(0.99);
}

uint8_t BenchmarkTBB::Insert(uint64_t key, uint64_t value) {
    tab.insert(std::make_pair(key, value));
    return 0;
}

uint64_t BenchmarkTBB::Query(uint64_t key, uint8_t ptr) {
    auto it = tab.find(key);
    if (it == tab.end()) {
        return 0;
    }
    return it->second;
}

void BenchmarkTBB::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab[key] = value;
}

void BenchmarkTBB::Erase(uint64_t key, uint8_t ptr) {
    tab.unsafe_erase(key);
}

void BenchmarkTBB::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                tab.insert(std::make_pair(keys[j], 0));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkTBB::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                           int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    tab.insert(std::make_pair(ops[j].second, 0));
                } else if (ops[j].first == 2) {
                    tab.unsafe_erase(ops[j].second);
                } else {
                    tab.find(ops[j].second);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkTBB::YCSBRunWithLatencyRecording(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads,
    uint64_t record_num, const std::vector<double>& percentiles) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> thread_latencies(
        num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &ops, start_index, end_index, &thread_latencies, i]() {
                std::vector<std::pair<uint64_t, uint64_t>> local_latencies;
                local_latencies.reserve(end_index - start_index);

                uint64_t value;

                for (size_t j = start_index; j < end_index; ++j) {
                    uint64_t start_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();

                    if (ops[j].first == 1) {
                        tab.insert(std::make_pair(ops[j].second, 0));
                    } else {
                        tab.find(ops[j].second);
                    }

                    uint64_t end_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();
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

void BenchmarkTBB::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    tab.insert(std::make_pair(std::get<1>(ops[j]),
                                              std::get<2>(ops[j])));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    tab.find(std::get<1>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    tab[std::get<1>(ops[j])] = std::get<2>(ops[j]);
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    tab.unsafe_erase(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkTBB::ConcurrentRunWithLatencyRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads,
    uint64_t record_num, const std::vector<double>& percentiles) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> thread_latencies(
        num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &ops, start_index, end_index, &thread_latencies, i]() {
                std::vector<std::pair<uint64_t, uint64_t>> local_latencies;
                local_latencies.reserve(end_index - start_index);

                for (size_t j = start_index; j < end_index; ++j) {
                    uint64_t start_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();

                    if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                        tab.insert(std::make_pair(std::get<1>(ops[j]),
                                                  std::get<2>(ops[j])));
                    } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                        tab.find(std::get<1>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                        tab[std::get<1>(ops[j])] = std::get<2>(ops[j]);
                    } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                        tab.unsafe_erase(std::get<1>(ops[j]));
                    }

                    uint64_t end_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();
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

std::vector<uint64_t> BenchmarkTBB::ConcurrentInsertWithTimestampRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads, uint64_t k) {
    return RecordInsertionTimestamps(
        ops, num_threads, k,
        [this, &ops](size_t j, int /*thread_id*/) {
            // Execute the actual insertion operation
            if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                tab.insert(std::make_pair(std::get<1>(ops[j]), std::get<2>(ops[j])));
            }
        });
}

}  // namespace tinyptr