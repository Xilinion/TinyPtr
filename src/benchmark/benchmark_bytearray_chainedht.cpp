#include "benchmark_bytearray_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkByteArrayChained::TYPE =
    BenchmarkObjectType::BYTEARRAYCHAINEDHT;

BenchmarkByteArrayChained::BenchmarkByteArrayChained(
    int n, uint8_t quotienting_tail_length, uint16_t bin_size)
    : BenchmarkChained(TYPE) {
    tab = new ByteArrayChainedHT(n, quotienting_tail_length, bin_size);
}

uint8_t BenchmarkByteArrayChained::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkByteArrayChained::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

bool BenchmarkByteArrayChained::Query(uint64_t key, uint64_t* value_ptr) {
    return tab->Query(key, value_ptr);
}

void BenchmarkByteArrayChained::Update(uint64_t key, uint8_t ptr,
                                       uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkByteArrayChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

void BenchmarkByteArrayChained::ConcurrentRun(
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
                    Insert(std::get<1>(ops[j]), std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::QUERY) {
                    Query(std::get<1>(ops[j]), uint8_t(0));
                } else if (std::get<0>(ops[j]) == ConcOptType::UPDATE) {
                    Update(std::get<1>(ops[j]), 0, std::get<2>(ops[j]));
                } else if (std::get<0>(ops[j]) == ConcOptType::ERASE) {
                    Erase(std::get<1>(ops[j]), 0);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

double BenchmarkByteArrayChained::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkByteArrayChained::MaxChainLength() {
    return tab->MaxChainLength();
}

uint64_t* BenchmarkByteArrayChained::ChainLengthHistogram() {
    return tab->ChainLengthHistogram();
}

void BenchmarkByteArrayChained::FillChainLength(uint8_t chain_length) {
    tab->FillChainLength(chain_length);
}

void BenchmarkByteArrayChained::set_chain_length(uint64_t chain_length) {
    tab->set_chain_length(chain_length);
}

bool BenchmarkByteArrayChained::QueryNoMem(uint64_t key, uint64_t* value_ptr) {
    return tab->QueryNoMem(key, value_ptr);
}

uint64_t BenchmarkByteArrayChained::QueryEntryCnt() {
    return tab->QueryEntryCnt();
}

}  // namespace tinyptr