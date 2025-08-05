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

uint8_t BenchmarkCuckoo::Insert(uint64_t key, uint64_t value) {
    return tab->insert(key, value);
}

uint64_t BenchmarkCuckoo::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->find(key, value);
    return value;
}

void BenchmarkCuckoo::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->update(key, value);
}

void BenchmarkCuckoo::Erase(uint64_t key, uint8_t ptr) {
    tab->erase(key);
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
                tab->insert(keys[j], 0);
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
                    tab->insert(ops[j].second, 0);
                } else {
                    tab->find(ops[j].second, value);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

std::vector<std::pair<uint64_t, uint64_t>> BenchmarkCuckoo::YCSBRunWithLatencyRecording(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads, uint64_t record_num) {
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
                    tab->insert(ops[j].second, 0);
                } else {
                    tab->find(ops[j].second, value);
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
    
    // Sample evenly from both sorted vectors
    std::vector<std::pair<uint64_t, uint64_t>> result;
    result.reserve(record_num * 2);  // record_num for each operation type
    
    // Sample insert latencies
    if (!insert_latencies.empty()) {
        uint64_t insert_step = insert_latencies.size() / record_num;
        if (insert_step == 0) insert_step = 1;
        
        for (uint64_t i = 0; i < record_num && i * insert_step < insert_latencies.size(); ++i) {
            result.emplace_back(1, insert_latencies[i * insert_step]);
        }
    }
    
    // Sample query latencies
    if (!query_latencies.empty()) {
        uint64_t query_step = query_latencies.size() / record_num;
        if (query_step == 0) query_step = 1;
        
        for (uint64_t i = 0; i < record_num && i * query_step < query_latencies.size(); ++i) {
            result.emplace_back(0, query_latencies[i * query_step]);
        }
    }
    
    return result;
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
            for (size_t j = start_index; j < end_index; ++j) {
                if (std::get<0>(ops[j]) == ConcOptType::INSERT) {
                    tab->insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    tab->find(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    tab->update(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    tab->erase(std::get<1>(ops[j]));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

}  // namespace tinyptr