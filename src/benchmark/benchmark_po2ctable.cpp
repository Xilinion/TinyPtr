#include "benchmark_po2ctable.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkPo2CTable::TYPE =
    BenchmarkObjectType::PO2C_TABLE;

BenchmarkPo2CTable::BenchmarkPo2CTable(int n) : BenchmarkObject64(TYPE) {
    tab = new Po2CTable(n);
}

uint8_t BenchmarkPo2CTable::Insert(uint64_t key, uint64_t value) {
    return tab->Allocate(key, value);
}

uint64_t BenchmarkPo2CTable::Query(uint64_t key, uint8_t ptr) {
    uint64_t result;
    tab->Query(key, ptr, &result);
    return result;
}

void BenchmarkPo2CTable::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, ptr, value);
}

void BenchmarkPo2CTable::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key, ptr);
}

}  // namespace tinyptr
