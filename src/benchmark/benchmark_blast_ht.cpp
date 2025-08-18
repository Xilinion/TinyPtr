#include "benchmark_blast_ht.h"
#include <thread>
#include <vector>
#include <chrono>
#include <algorithm>
#include <execution>
#include "blast_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkBlastHT::TYPE = BenchmarkObjectType::BLAST;

BenchmarkBlastHT::BenchmarkBlastHT(uint64_t size, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new BlastHT(size, bin_size, false);
}

uint8_t BenchmarkBlastHT::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkBlastHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkBlastHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkBlastHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

void BenchmarkBlastHT::YCSBFill(std::vector<uint64_t>& keys, int num_threads) {
    std::vector<std::thread> threads;
    size_t chunk_size = keys.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start_index = i * chunk_size;
        size_t end_index =
            (i == num_threads - 1) ? keys.size() : start_index + chunk_size;

        threads.emplace_back([this, &keys, start_index, end_index]() {
            for (size_t j = start_index; j < end_index; ++j) {
                tab->Insert(keys[j], 0);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkBlastHT::YCSBRun(std::vector<std::pair<uint64_t, uint64_t>>& ops,
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
                    tab->Insert(ops[j].second, 0);
                } else {
                    tab->Query(ops[j].second, &value);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}


std::vector<std::tuple<uint64_t, double, uint64_t>> BenchmarkBlastHT::YCSBRunWithLatencyRecording(
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
                    tab->Insert(ops[j].second, 0);
                } else {
                    tab->Query(ops[j].second, &value);
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
    
    // Combine all thread results
    std::vector<std::pair<uint64_t, uint64_t>> all_latencies;
    for (const auto& thread_result : thread_latencies) {
        all_latencies.insert(all_latencies.end(), 
                           thread_result.begin(), thread_result.end());
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
    std::sort(std::execution::par, insert_latencies.begin(), insert_latencies.end());
    std::sort(std::execution::par, query_latencies.begin(), query_latencies.end());
    
    // Calculate percentiles for both operation types
    std::vector<std::tuple<uint64_t, double, uint64_t>> result;
    
    // Calculate percentiles for insert latencies
    if (!insert_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index = (percentile == 100.0) ? 
                          insert_latencies.size() - 1 : 
                          static_cast<size_t>((percentile / 100.0) * (insert_latencies.size() - 1));
            result.emplace_back(1, percentile, insert_latencies[index]);
        }
    }
    
    // Calculate percentiles for query latencies
    if (!query_latencies.empty()) {
        for (double percentile : percentiles) {
            size_t index = (percentile == 100.0) ? 
                          query_latencies.size() - 1 : 
                          static_cast<size_t>((percentile / 100.0) * (query_latencies.size() - 1));
            result.emplace_back(0, percentile, query_latencies[index]);
        }
    }
    
    return result;
}

void BenchmarkBlastHT::ConcurrentRun(
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
                    tab->Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    tab->Query(std::get<1>(ops[j]), &value);
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    tab->Update(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    tab->Free(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

void BenchmarkBlastHT::Stats() {
    tab->Scan4Stats();
}

}  // namespace tinyptr
