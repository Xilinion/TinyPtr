#include "o2v_po2c_table.h"

namespace tinyptr {

O2VPo2CTable::Bin::Bin() {
    for (int i = 0; i < O2VDereferenceTable64::kBinSize; ++i)
        bin[i].value_1 = i + 2;
}

bool O2VPo2CTable::Bin::full() {
    return cnt == O2VDereferenceTable64::kBinSize;
}

uint8_t O2VPo2CTable::Bin::count() {
    return cnt;
}

void O2VPo2CTable::Bin::query_first(uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    *value_ptr = bin[ptr].value_1;
}

void O2VPo2CTable::Bin::query_second(uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    *value_ptr = bin[ptr].value_2;
}

// ~0 for full bin
// i+1 for position i
uint8_t O2VPo2CTable::Bin::insert(uint64_t value_1, uint64_t value_2) {
    if (this->full())
        return ~0;

    ++cnt;
    auto tmp = head;
    head = bin[head - 1].value_1;
    bin[tmp - 1].value_1 = value_1;
    bin[tmp - 1].value_2 = value_2;

    return tmp;
}

void O2VPo2CTable::Bin::update_first(uint8_t ptr, uint64_t value) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    bin[ptr].value_1 = value;
}

void O2VPo2CTable::Bin::update_second(uint8_t ptr, uint64_t value) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    bin[ptr].value_2 = value;
}

void O2VPo2CTable::Bin::free(uint8_t ptr) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    bin[ptr].value_1 = head;
    head = ptr + 1;
    --cnt;
}

O2VPo2CTable::O2VPo2CTable(int n) {
    bin_num =
        (n + O2VDereferenceTable64::kBinSize - 1) / O2VDereferenceTable64::kBinSize;
    tab = new Bin[bin_num];
    srand(time(0));
    int hash_seed[2] = {rand(), rand()};
    while (hash_seed[1] == hash_seed[0])
        hash_seed[1] = rand();
    for (int i = 0; i < 2; ++i)
        HashBin[i] =
            std::function<uint64_t(uint64_t)>([=](uint64_t key) -> uint64_t {
                return XXHash64::hash(&key, sizeof(uint64_t), hash_seed[i]) %
                       bin_num;
            });
}

uint8_t O2VPo2CTable::Allocate(uint64_t key, uint64_t value_1, uint64_t value_2) {
    uint64_t hashbin[2];
    for (int i = 0; i < 2; ++i) {
        hashbin[i] = HashBin[i](key);
    }

    uint8_t flag = tab[hashbin[0]].count() > tab[hashbin[1]].count();

    uint8_t ptr = tab[hashbin[flag]].insert(value_1, value_2);
    
    assert(ptr);

    ptr ^=
        flag * (ptr != O2VDereferenceTable64::kOverflowTinyPtr) * ((1 << 8) - 1);
    return ptr;
}

void O2VPo2CTable::UpdateFirst(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr != O2VDereferenceTable64::kOverflowTinyPtr);
    assert(ptr != O2VDereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].update_first(ptr, value);
}

void O2VPo2CTable::UpdateSecond(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr != O2VDereferenceTable64::kOverflowTinyPtr);
    assert(ptr != O2VDereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].update_second(ptr, value);
}

void O2VPo2CTable::QueryFirst(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr != O2VDereferenceTable64::kOverflowTinyPtr);
    assert(ptr != O2VDereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].query_first(ptr, value_ptr);
}

void O2VPo2CTable::QuerySecond(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr != O2VDereferenceTable64::kOverflowTinyPtr);
    assert(ptr != O2VDereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].query_second(ptr, value_ptr);
}

void O2VPo2CTable::Free(uint64_t key, uint8_t ptr) {
    assert(ptr != O2VDereferenceTable64::kOverflowTinyPtr);
    assert(ptr != O2VDereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].free(ptr);
}

}  // namespace tinyptr