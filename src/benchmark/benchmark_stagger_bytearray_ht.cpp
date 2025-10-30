#include "benchmark_stagger_bytearray_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkStaggerByteArrayHT::TYPE =
    BenchmarkObjectType::STAGGER_BYTEARRAYCHAINEDHT;

BenchmarkStaggerByteArrayHT::BenchmarkStaggerByteArrayHT(
    uint64_t initial_size_per_part_, uint64_t part_num_, uint32_t thread_num_,
    double resize_threshold_, double resize_factor_)
    : BenchmarkObject64(TYPE) {
    tab = new ResizableByteArrayChainedHT(initial_size_per_part_, part_num_,
                                          thread_num_, true, resize_threshold_,
                                          resize_factor_);
    if (!thread_num_) {
        single_handle = tab->GetHandle();
    }
    thread_num = thread_num_;
}

BenchmarkStaggerByteArrayHT::~BenchmarkStaggerByteArrayHT() {
    if (!thread_num) {
        tab->FreeHandle(single_handle);
    }
    delete tab;
}

uint8_t BenchmarkStaggerByteArrayHT::Insert(uint64_t key, uint64_t value) {
    return tab->Insert(single_handle, key, value);
}

uint64_t BenchmarkStaggerByteArrayHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(single_handle, key, &value);
    return value;
}

void BenchmarkStaggerByteArrayHT::Update(uint64_t key, uint8_t ptr,
                                         uint64_t value) {
    tab->Update(single_handle, key, value);
}

void BenchmarkStaggerByteArrayHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Erase(single_handle, key);
}

void BenchmarkStaggerByteArrayHT::YCSBFill(std::vector<uint64_t>& keys,
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

void BenchmarkStaggerByteArrayHT::YCSBRun(
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
BenchmarkStaggerByteArrayHT::YCSBRunWithLatencyRecording(
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

    // Combine all thread results
    std::vector<std::pair<uint64_t, uint64_t>> all_latencies;
    for (const auto& thread_result : thread_latencies) {
        all_latencies.insert(all_latencies.end(), thread_result.begin(),
                             thread_result.end());
    }

    // Separate insert and query latencies
    std::vector<uint64_t> insert_latencies;
    std::vector<uint64_t> query_latencies;

    for (const auto& latency_pair : all_latencies) {
        if (latency_pair.first == 1) {  // Insert operation
            insert_latencies.push_back(latency_pair.second);
        } else {  // Query operation
            query_latencies.push_back(latency_pair.second);
        }
    }

    // Sort both vectors for percentile analysis
    std::sort(std::execution::par, insert_latencies.begin(),
              insert_latencies.end());
    std::sort(std::execution::par, query_latencies.begin(),
              query_latencies.end());

    // Calculate percentiles for both operation types
    std::vector<std::tuple<uint64_t, double, uint64_t>> result;

    // Calculate percentiles for insert latencies
    if (!insert_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? insert_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (insert_latencies.size() - 1));
            result.emplace_back(1, percentile, insert_latencies[index]);
        }
    }

    // Calculate percentiles for query latencies
    if (!query_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? query_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (query_latencies.size() - 1));
            result.emplace_back(0, percentile, query_latencies[index]);
        }
    }

    return result;
}

void BenchmarkStaggerByteArrayHT::ConcurrentRun(
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
BenchmarkStaggerByteArrayHT::ConcurrentRunWithLatencyRecording(
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

    // Combine all thread results
    std::vector<std::pair<uint64_t, uint64_t>> all_latencies;
    for (const auto& thread_result : thread_latencies) {
        all_latencies.insert(all_latencies.end(), thread_result.begin(),
                             thread_result.end());
    }

    // Separate latencies by operation type
    std::vector<uint64_t> insert_latencies;
    std::vector<uint64_t> query_latencies;
    std::vector<uint64_t> update_latencies;
    std::vector<uint64_t> erase_latencies;

    for (const auto& latency_pair : all_latencies) {
        if (latency_pair.first == ConcOptType::INSERT) {
            insert_latencies.push_back(latency_pair.second);
        } else if (latency_pair.first == ConcOptType::QUERY) {
            query_latencies.push_back(latency_pair.second);
        } else if (latency_pair.first == ConcOptType::UPDATE) {
            update_latencies.push_back(latency_pair.second);
        } else if (latency_pair.first == ConcOptType::ERASE) {
            erase_latencies.push_back(latency_pair.second);
        }
    }

    // Sort all vectors for percentile analysis
    std::sort(std::execution::par, insert_latencies.begin(),
              insert_latencies.end());
    std::sort(std::execution::par, query_latencies.begin(),
              query_latencies.end());
    std::sort(std::execution::par, update_latencies.begin(),
              update_latencies.end());
    std::sort(std::execution::par, erase_latencies.begin(),
              erase_latencies.end());

    // Calculate percentiles for all operation types
    std::vector<std::tuple<uint64_t, double, uint64_t>> result;

    // Calculate percentiles for insert latencies
    if (!insert_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? insert_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (insert_latencies.size() - 1));
            result.emplace_back(ConcOptType::INSERT, percentile,
                                insert_latencies[index]);
        }
    }

    // Calculate percentiles for query latencies
    if (!query_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? query_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (query_latencies.size() - 1));
            result.emplace_back(ConcOptType::QUERY, percentile,
                                query_latencies[index]);
        }
    }

    // Calculate percentiles for update latencies
    if (!update_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? update_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (update_latencies.size() - 1));
            result.emplace_back(ConcOptType::UPDATE, percentile,
                                update_latencies[index]);
        }
    }

    // Calculate percentiles for erase latencies
    if (!erase_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? erase_latencies.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (erase_latencies.size() - 1));
            result.emplace_back(ConcOptType::ERASE, percentile,
                                erase_latencies[index]);
        }
    }

    return result;
}

}  // namespace tinyptr
