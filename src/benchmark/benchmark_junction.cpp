#include "benchmark_junction.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <execution>
#include <iostream>

namespace tinyptr {

const BenchmarkObjectType BenchmarkJunction::TYPE =
    BenchmarkObjectType::JUNCTION;

BenchmarkJunction::BenchmarkJunction(int n) : BenchmarkObject64(TYPE) {
    uint64_t table_size = 1;
    while (table_size < n) {
        table_size *= 2;
    }
    // tab = new junction::ConcurrentMap_Grampa<uint64_t, uint64_t>(n);
    // tab = new junction::ConcurrentMap_Leapfrog<uint64_t, uint64_t>(table_size);
    tab = new junction::ConcurrentMap_Linear<uint64_t, uint64_t>(table_size);
}

// Wrapper function definitions
[[gnu::noinline]] void BenchmarkJunction::assign_wrapper(uint64_t k,
                                                         uint64_t v) {
    tab->assign(k, v);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
}

[[gnu::noinline]] uint64_t BenchmarkJunction::get_wrapper(uint64_t k) {
    auto result = tab->get(k);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
    return result;
}

[[gnu::noinline]] uint64_t BenchmarkJunction::exchange_wrapper(uint64_t k,
                                                               uint64_t v) {
    auto result = tab->exchange(k, v);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
    return result;
}

[[gnu::noinline]] void BenchmarkJunction::erase_wrapper(uint64_t k) {
    tab->erase(k);
    // Prevent tail call optimization with compiler barrier (zero runtime cost)
    asm volatile("" ::: "memory");
}

uint8_t BenchmarkJunction::Insert(uint64_t key, uint64_t value) {
    assign_wrapper(key, value);
    asm volatile(
        "nop" ::
            : "memory");  // Single nop instruction to prevent tail call
    return 0;
}

uint64_t BenchmarkJunction::Query(uint64_t key, uint8_t ptr) {
    auto result = get_wrapper(key);
    asm volatile(
        "nop" ::
            : "memory");  // Single nop instruction to prevent tail call
    return result;
}

void BenchmarkJunction::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    exchange_wrapper(key, value);
    asm volatile(
        "nop" ::
            : "memory");  // Single nop instruction to prevent tail call
}

void BenchmarkJunction::Erase(uint64_t key, uint8_t ptr) {
    erase_wrapper(key);
    asm volatile(
        "nop" ::
            : "memory");  // Single nop instruction to prevent tail call
}

void BenchmarkJunction::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &keys, start_index, end_index, i, context]() {
                for (size_t j = start_index; j < end_index; ++j) {
                    assign_wrapper(keys[j], 0);
                    if (j & ((1 << 10) - 1) == 0) {
                        junction::DefaultQSBR.update(context);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    junction::DefaultQSBR.destroyContext(context);
}

void BenchmarkJunction::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                                int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [this, &ops, start_index, end_index, i, context]() {
                for (size_t j = start_index; j < end_index; ++j) {
                    uint64_t  value;
                    if (ops[j].first == 1) {
                        assign_wrapper(ops[j].second, 0);
                    } else if (ops[j].first == 2) {
                        erase_wrapper(ops[j].second);
                    } else {
                        value = get_wrapper(ops[j].second);
                    }

                    if (j & ((1 << 10) - 1) == 0) {
                        junction::DefaultQSBR.update(context);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    junction::DefaultQSBR.destroyContext(context);
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkJunction::YCSBRunWithLatencyRecording(
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
                        assign_wrapper(ops[j].second, 0);
                    } else {
                        get_wrapper(ops[j].second);
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

void BenchmarkJunction::ConcurrentRun(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, i,
                              context]() {
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    assign_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    get_wrapper(std::get<1>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    exchange_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    erase_wrapper(std::get<1>(ops[j]));
                }

                if (j & ((1 << 10) - 1) == 0) {
                    junction::DefaultQSBR.update(context);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    junction::DefaultQSBR.destroyContext(context);
}

std::vector<std::tuple<uint64_t, double, uint64_t>>
BenchmarkJunction::ConcurrentRunWithLatencyRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads,
    uint64_t record_num, const std::vector<double>& percentiles) {
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> thread_latencies(
        num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index,
                              &thread_latencies, i, context]() {
            std::vector<std::pair<uint64_t, uint64_t>> local_latencies;
            local_latencies.reserve(end_index - start_index);

            for (size_t j = start_index; j < end_index; ++j) {
                uint64_t start_time =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::high_resolution_clock::now()
                            .time_since_epoch())
                        .count();

                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    assign_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    get_wrapper(std::get<1>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    exchange_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    erase_wrapper(std::get<1>(ops[j]));
                }

                if (j & ((1 << 10) - 1) == 0) {
                    junction::DefaultQSBR.update(context);
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

    junction::DefaultQSBR.destroyContext(context);

    return ComputeConcurrentLatencyPercentiles(thread_latencies, percentiles);
}

std::vector<uint64_t>
BenchmarkJunction::ConcurrentInsertWithTimestampRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads, uint64_t k) {
    std::vector<std::vector<uint64_t>> thread_timestamps(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;
    const auto start_time = std::chrono::steady_clock::now();

    junction::QSBR::Context context = junction::DefaultQSBR.createContext();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back([this, &ops, start_index, end_index, &thread_timestamps, i, k, context, start_time]() {
            std::vector<uint64_t> local_timestamps;
            local_timestamps.reserve((end_index - start_index) / k + 1);

            uint64_t local_op_count = 0;

            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    assign_wrapper(std::get<1>(ops[j]), std::get<2>(ops[j]));
                }

                if (j & ((1 << 10) - 1) == 0) {
                    junction::DefaultQSBR.update(context);
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

    junction::DefaultQSBR.destroyContext(context);

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