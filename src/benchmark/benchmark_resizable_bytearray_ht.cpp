#include "benchmark_resizable_bytearray_ht.h"
#include <algorithm>
#include <chrono>
#include <execution>

namespace tinyptr {

const BenchmarkObjectType BenchmarkResizableByteArrayChainedHT::TYPE =
    BenchmarkObjectType::RESIZABLE_BYTEARRAYCHAINEDHT;

BenchmarkResizableByteArrayChainedHT::BenchmarkResizableByteArrayChainedHT(
    uint64_t initial_size_per_part_, uint64_t part_num_, uint32_t thread_num_,
    double resize_threshold_, double resize_factor_)
    : BenchmarkObject64(TYPE) {
    tab = new ResizableByteArrayChainedHT(initial_size_per_part_, part_num_,
                                          thread_num_, false, resize_threshold_,
                                          resize_factor_);
    if (!thread_num_) {
        single_handle = tab->GetHandle();
    }
    thread_num = thread_num_;
}

BenchmarkResizableByteArrayChainedHT::~BenchmarkResizableByteArrayChainedHT() {
    if (!thread_num) {
        tab->FreeHandle(single_handle);
    }
    delete tab;
}

uint8_t BenchmarkResizableByteArrayChainedHT::Insert(uint64_t key,
                                                     uint64_t value) {
    return tab->Insert(single_handle, key, value);
}

uint64_t BenchmarkResizableByteArrayChainedHT::Query(uint64_t key,
                                                     uint8_t ptr) {
    uint64_t value;
    tab->Query(single_handle, key, &value);
    return value;
}

void BenchmarkResizableByteArrayChainedHT::Update(uint64_t key, uint8_t ptr,
                                                  uint64_t value) {
    tab->Update(single_handle, key, value);
}

void BenchmarkResizableByteArrayChainedHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Erase(single_handle, key);
}

void BenchmarkResizableByteArrayChainedHT::YCSBFill(std::vector<uint64_t>& keys,
                                                    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index]() {
            uint64_t handle = tab->GetHandle();
            for (size_t j = start_index; j < end_index; ++j) {
                tab->Insert(handle, keys[j], 0);
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkResizableByteArrayChainedHT::YCSBRun(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index]() {
            uint64_t handle = tab->GetHandle();
            uint64_t value;
            for (size_t j = start_index; j < end_index; ++j) {
                if (ops[j].first == 1) {
                    tab->Insert(handle, ops[j].second, 0);
                } else if (ops[j].first == 2) {
                    tab->Erase(handle, ops[j].second);
                } else {
                    tab->Query(handle, ops[j].second, &value);
                }
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkResizableByteArrayChainedHT::YCSBRunWithLatencyRecording(
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

                uint64_t handle = tab->GetHandle();
                uint64_t value;

                for (size_t j = start_index; j < end_index; ++j) {
                    uint64_t start_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();

                    if (ops[j].first == 1) {
                        tab->Insert(handle, ops[j].second, 0);
                    } else {
                        tab->Query(handle, ops[j].second, &value);
                    }

                    uint64_t end_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();
                    uint64_t latency = end_time - start_time;
                    local_latencies.emplace_back(ops[j].first, latency);
                }

                tab->FreeHandle(handle);

                // Store the results in the thread-specific vector
                thread_latencies[i] = std::move(local_latencies);
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    return ComputeYCSBLatencyPercentiles(thread_latencies, percentiles);
}

void BenchmarkResizableByteArrayChainedHT::ConcurrentRun(
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
            uint64_t handle = tab->GetHandle();
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    tab->Insert(handle, std::get<1>(ops[j]),
                                std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    tab->Query(handle, std::get<1>(ops[j]), &value);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    tab->Update(handle, std::get<1>(ops[j]),
                                std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    tab->Erase(handle, std::get<1>(ops[j]));
                }
            }
            tab->FreeHandle(handle);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkResizableByteArrayChainedHT::ConcurrentRunWithLatencyRecording(
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

                uint64_t value;
                uint64_t handle = tab->GetHandle();

                for (size_t j = start_index; j < end_index; ++j) {
                    uint64_t start_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();

                    if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                        tab->Insert(handle, std::get<1>(ops[j]),
                                    std::get<2>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                        tab->Query(handle, std::get<1>(ops[j]), &value);
                    } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                        tab->Update(handle, std::get<1>(ops[j]),
                                    std::get<2>(ops[j]));
                    } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                        tab->Erase(handle, std::get<1>(ops[j]));
                    }

                    uint64_t end_time =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::high_resolution_clock::now()
                                .time_since_epoch())
                            .count();
                    uint64_t latency = end_time - start_time;
                    local_latencies.emplace_back(std::get<0>(ops[j]), latency);
                }

                tab->FreeHandle(handle);

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
BenchmarkResizableByteArrayChainedHT::ConcurrentInsertWithTimestampRecording(
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
            uint64_t handle = tab->GetHandle();

            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    tab->Insert(handle, std::get<1>(ops[j]), std::get<2>(ops[j]));
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

            tab->FreeHandle(handle);
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
