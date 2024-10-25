#include "benchmark_yarded_tp_ht.h"
#include <cstdint>

namespace tinyptr {

const BenchmarkObjectType BenchmarkYardedTPHT::TYPE =
    BenchmarkObjectType::YARDEDTTPHT;

BenchmarkYardedTPHT::BenchmarkYardedTPHT(int n, uint16_t bin_size)
    : BenchmarkChained(TYPE) {
    tab = new YardedTPHT(n, bin_size);
}

uint8_t BenchmarkYardedTPHT::Insert(uint64_t key, uint64_t value) {
    return tab->Insert(key, value);
}

uint64_t BenchmarkYardedTPHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkYardedTPHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkYardedTPHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

double BenchmarkYardedTPHT::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkYardedTPHT::MaxChainLength() {
    return tab->MaxChainLength();
}

uint64_t* BenchmarkYardedTPHT::ChainLengthHistogram() {
    return tab->ChainLengthHistogram();
}

}  // namespace tinyptr