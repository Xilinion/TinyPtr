#include "benchmark_skulkerht.h"
#include "skulker_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkSkulkerHT::TYPE =
    BenchmarkObjectType::SKULKERHT;

BenchmarkSkulkerHT::BenchmarkSkulkerHT(uint64_t size, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new SkulkerHT(size, bin_size);
}

uint8_t BenchmarkSkulkerHT::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkSkulkerHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkSkulkerHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkSkulkerHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

uint64_t BenchmarkSkulkerHT::QueryEntryCnt() {
    return tab->QueryEntryCnt();
}

}  // namespace tinyptr
