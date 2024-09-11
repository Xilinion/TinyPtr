#include "benchmark_bytearray_chainedht.h"

namespace tinyptr {

const BenchmarkObjectType BenchmarkByteArrayChained::TYPE =
    BenchmarkObjectType::BYTEARRAYCHAINEDHT;

BenchmarkByteArrayChained::BenchmarkByteArrayChained(
    int n, uint8_t quotiented_tail_length, uint16_t bin_size)
    : BenchmarkObject64(TYPE) {
    tab = new ByteArrayChainedHT(n, quotiented_tail_length, bin_size);
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

void BenchmarkByteArrayChained::Update(uint64_t key, uint8_t ptr,
                                       uint64_t value) {
    tab->Update(key, value);
}

void BenchmarkByteArrayChained::Erase(uint64_t key, uint8_t ptr) {
    tab->Free(key);
}

double BenchmarkByteArrayChained::AvgChainLength() {
    return tab->AvgChainLength();
}

uint32_t BenchmarkByteArrayChained::MaxChainLength() {
    return tab->MaxChainLength();
}

}  // namespace tinyptr