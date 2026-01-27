#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <execution>
#include <functional>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>
#include "benchmark_object_type.h"
#include "conc_opt_type.h"

namespace tinyptr {

class BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkObject64(BenchmarkObjectType type_) : type(type_) {}

    virtual ~BenchmarkObject64() {}

    virtual uint8_t Insert(uint64_t key, uint64_t value) = 0;
    virtual uint64_t Query(uint64_t key, uint8_t ptr) = 0;
    virtual void Update(uint64_t key, uint8_t ptr, uint64_t value) = 0;
    virtual void Erase(uint64_t key, uint8_t ptr) = 0;

    virtual void YCSBFill(std::vector<uint64_t>& keys, int num_threads) {}

    virtual void YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                         int num_threads) {}

    virtual std::vector<std::tuple<uint64_t, double, uint64_t>>
    YCSBRunWithLatencyRecording(std::vector<std::pair<uint64_t, uint64_t>>& ops,
                                int num_threads, uint64_t record_num,
                                const std::vector<double>& percentiles) {
        return {};
    }

    virtual void ConcurrentRun(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads) {}

    virtual std::vector<std::tuple<uint64_t, double, uint64_t>>
    ConcurrentRunWithLatencyRecording(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads, uint64_t record_num,
        const std::vector<double>& percentiles) {
        return {};
    }

    virtual std::vector<uint64_t>
    ConcurrentInsertWithTimestampRecording(
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
        int num_threads, uint64_t k);

   public:
    BenchmarkObjectType type;
};

// Helper: compute latency percentiles for YCSB-style benchmarks.
// thread_latencies: per-thread vectors of (op_type, latency_ns), where
//   op_type == 1 for insert, 0 for query.
inline std::vector<std::tuple<uint64_t, double, uint64_t>>
ComputeYCSBLatencyPercentiles(
    const std::vector<std::vector<std::pair<uint64_t, uint64_t>>>&
        thread_latencies,
    const std::vector<double>& percentiles) {
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

    // Insert latencies
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

    // Query latencies
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

// Helper: compute latency percentiles for concurrent benchmarks.
// thread_latencies: per-thread vectors of (op_type, latency_ns), where
//   op_type is one of ConcOptType::INSERT / QUERY / UPDATE / ERASE.
inline std::vector<std::tuple<uint64_t, double, uint64_t>>
ComputeConcurrentLatencyPercentiles(
    const std::vector<std::vector<std::pair<uint64_t, uint64_t>>>&
        thread_latencies,
    const std::vector<double>& percentiles) {
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

    std::vector<std::tuple<uint64_t, double, uint64_t>> result;

    auto emit = [&](const std::vector<uint64_t>& vec, uint64_t op_tag) {
        if (vec.empty()) return;
        for (double percentile : percentiles) {
            size_t index =
                (percentile == 100.0)
                    ? vec.size() - 1
                    : static_cast<size_t>((percentile / 100.0) *
                                          (vec.size() - 1));
            result.emplace_back(op_tag, percentile, vec[index]);
        }
    };

    emit(insert_latencies, ConcOptType::INSERT);
    emit(query_latencies, ConcOptType::QUERY);
    emit(update_latencies, ConcOptType::UPDATE);
    emit(erase_latencies, ConcOptType::ERASE);

    return result;
}

// Helper: record timestamps during concurrent insertion operations.
// Each thread records a timestamp after every k operations.
// operation_executor: lambda that executes operation at index j (thread_id provided).
// Returns: merged and sorted vector of timestamps.
template<typename Func>
inline std::vector<uint64_t>
RecordInsertionTimestamps(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops, int num_threads,
    uint64_t k, Func operation_executor) {
    std::vector<std::vector<uint64_t>> thread_timestamps(num_threads);

    std::vector<std::thread> threads;
    size_t chunk_size = ops.size() / num_threads;
    const auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? ops.size() : start_index + chunk_size;

        threads.emplace_back(
            [&ops, start_index, end_index, &thread_timestamps, i, k,
             &operation_executor, start_time]() {
                std::vector<uint64_t> local_timestamps;
                // Reserve space for approximate number of timestamps
                local_timestamps.reserve((end_index - start_index) / k + 1);

                uint64_t local_op_count = 0;

                for (size_t j = start_index; j < end_index; ++j) {
                    operation_executor(j, i);

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

// Default implementation of ConcurrentInsertWithTimestampRecording
inline std::vector<uint64_t>
BenchmarkObject64::ConcurrentInsertWithTimestampRecording(
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>& ops,
    int num_threads, uint64_t k) {
    // Default implementation uses the helper function
    return RecordInsertionTimestamps(
        ops, num_threads, k,
        [this, &ops](size_t j, int /*thread_id*/) {
            // Execute the actual insertion operation
            Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
        });
}

}  // namespace tinyptr