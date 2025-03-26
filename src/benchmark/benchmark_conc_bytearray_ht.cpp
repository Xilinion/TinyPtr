#include "benchmark_conc_bytearray_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkConcByteArrayChainedHT::TYPE =
    BenchmarkObjectType::CONCURRENT_BYTEARRAYCHAINEDHT;

BenchmarkConcByteArrayChainedHT::BenchmarkConcByteArrayChainedHT(
    uint64_t size, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new ConcurrentByteArrayChainedHT(size, bin_size);
}

uint8_t BenchmarkConcByteArrayChainedHT::Insert(uint64_t key, uint64_t value) {
    return tab->Insert(key, value);
}

uint64_t BenchmarkConcByteArrayChainedHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkConcByteArrayChainedHT::Update(uint64_t key, uint8_t ptr,
                                             uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkConcByteArrayChainedHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

void BenchmarkConcByteArrayChainedHT::YCSBFill(std::vector<uint64_t>& keys,
                                               int num_threads) {
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

void BenchmarkConcByteArrayChainedHT::YCSBRun(
    std::vector<std::pair<uint64_t, uint64_t>>& ops, int num_threads) {
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

}  // namespace tinyptr
