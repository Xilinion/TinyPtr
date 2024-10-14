#include "benchmark_same_bin_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkSameBinChained::TYPE =
    BenchmarkObjectType::SAMEBINCHAINEDHT;

BenchmarkSameBinChained::BenchmarkSameBinChained(int n, uint16_t bin_size)
    : BenchmarkChained(TYPE) {
    tab = new SameBinChainedHT(n, bin_size);
}

uint8_t BenchmarkSameBinChained::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkSameBinChained::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkSameBinChained::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkSameBinChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

double BenchmarkSameBinChained::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkSameBinChained::MaxChainLength() {
    return tab->MaxChainLength();
}

uint64_t* BenchmarkSameBinChained::ChainLengthHistogram() {
    return tab->ChainLengthHistogram();
}

}  // namespace tinyptr