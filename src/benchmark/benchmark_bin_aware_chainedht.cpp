#include "benchmark_bin_aware_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkBinAwareChained::TYPE =
    BenchmarkObjectType::BINAWARECHAINEDHT;

BenchmarkBinAwareChained::BenchmarkBinAwareChained(int n, uint16_t bin_size,
                                           uint8_t double_slot_num)
    : BenchmarkChained(TYPE) {
    tab = new BinAwareChainedHT(n, bin_size, double_slot_num);
}

uint8_t BenchmarkBinAwareChained::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkBinAwareChained::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkBinAwareChained::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkBinAwareChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

double BenchmarkBinAwareChained::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkBinAwareChained::MaxChainLength() {
    return tab->MaxChainLength();
}

uint64_t* BenchmarkBinAwareChained::ChainLengthHistogram() {
    return tab->ChainLengthHistogram();
}

uint64_t* BenchmarkBinAwareChained::DoubleSlotStatistics() {
    return tab->DoubleSlotStatistics();
}

}  // namespace tinyptr