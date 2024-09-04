#include "benchmark_bytearray_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkByteArrayChained::TYPE =
    BenchmarkObjectType::BYTEARRAYCHAINEDHT;

BenchmarkByteArrayChained::BenchmarkByteArrayChained(int n) : BenchmarkObject64(TYPE) {
    tab = new ByteArrayChainedHT(n);
}

uint8_t BenchmarkByteArrayChained::Insert(uint64_t key, uint64_t value) {
    tab->Insert(key, value);
    return 0;
}

uint64_t BenchmarkByteArrayChained::Query(uint64_t key, uint8_t ptr) {
    return tab->Query(key);
}

void BenchmarkByteArrayChained::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkByteArrayChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Erase(key);
}

}  // namespace tinyptr