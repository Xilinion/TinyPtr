#include "benchmark_fp_tp_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkFPTPChained::TYPE =
    BenchmarkObjectType::FPTPCHAINEDHT;

BenchmarkFPTPChained::BenchmarkFPTPChained(int n, uint16_t bin_size,
                                           uint8_t double_slot_num)
    : BenchmarkObject64(TYPE) {
    tab = new FPTPChainedHT(n, bin_size, double_slot_num);
}

uint8_t BenchmarkFPTPChained::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkFPTPChained::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkFPTPChained::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkFPTPChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

double BenchmarkFPTPChained::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkFPTPChained::MaxChainLength() {
    return tab->MaxChainLength();
}

uint64_t* BenchmarkFPTPChained::ChainLengthHistogram() {
    return tab->ChainLengthHistogram();
}

void BenchmarkFPTPChained::FillChainLength(uint8_t chain_length) {
    tab->FillChainLength(chain_length);
}

void BenchmarkFPTPChained::set_chain_length(uint64_t chain_length) {
    tab->set_chain_length(chain_length);
}

bool BenchmarkFPTPChained::QueryNoMem(uint64_t key, uint64_t* value_ptr) {
    return tab->QueryNoMem(key, value_ptr);
}

}  // namespace tinyptr