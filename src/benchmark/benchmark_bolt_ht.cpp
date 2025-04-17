#include "benchmark_bolt_ht.h"
#include "bolt_ht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkBoltHT::TYPE = BenchmarkObjectType::BOLT;

BenchmarkBoltHT::BenchmarkBoltHT(uint64_t size, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new BoltHT(size, bin_size);
}

uint8_t BenchmarkBoltHT::Insert(uint64_t key, uint64_t value) {
    if (tab->Insert(key, value)) {
        return 1;
    }
    return ~0;
}

uint64_t BenchmarkBoltHT::Query(uint64_t key, uint8_t ptr) {
    uint64_t value;
    tab->Query(key, &value);
    return value;
}

void BenchmarkBoltHT::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkBoltHT::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

}  // namespace tinyptr
