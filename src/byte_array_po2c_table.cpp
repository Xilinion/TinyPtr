#include "byte_array_po2c_table.h"
#include <cstdint>

namespace tinyptr {

uint64_t ByteArrayPo2CTable::KPV::get_quotiented_key() {
    return *((uint64_t*)data) << 16 >> 16;
}

uint8_t ByteArrayPo2CTable::KPV::get_tiny_ptr() {
    return data[6];
}

uint64_t ByteArrayPo2CTable::KPV::get_value() {
    return *((uint64_t*)(data + 7));
}

void ByteArrayPo2CTable::KPV::set_quotiented_key(uint64_t key) {
    uint16_t tmp = *((uint16_t*)(data + 5));
    *((uint64_t*)data) = key;
    *((uint16_t*)(data + 5)) = tmp;
}

void ByteArrayPo2CTable::KPV::set_tiny_ptr(uint8_t ptr) {
    data[6] = ptr;
}

void ByteArrayPo2CTable::KPV::set_value(uint64_t value) {
    *((uint64_t*)(data + 7)) = value;
}

ByteArrayPo2CTable::Bin::Bin() {
    for (int i = 0; i < ByteArrayDereferenceTable::kBinSize; ++i)
        bin[i].set_tiny_ptr(i + 2);
}

bool ByteArrayPo2CTable::Bin::full() {
    return cnt == ByteArrayDereferenceTable::kBinSize;
}

uint8_t ByteArrayPo2CTable::Bin::count() {
    return cnt;
}

void ByteArrayPo2CTable::Bin::query_quotiented_key(uint8_t ptr,
                                                   uint64_t* value_ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    *value_ptr = bin[ptr].get_quotiented_key();
}

uint64_t ByteArrayPo2CTable::Bin::query_quotiented_key(uint8_t ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    return bin[ptr].get_quotiented_key();
}

void ByteArrayPo2CTable::Bin::query_tiny_ptr(uint8_t ptr, uint8_t* value_ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    *value_ptr = bin[ptr].get_tiny_ptr();
}

uint8_t ByteArrayPo2CTable::Bin::query_tiny_ptr(uint8_t ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    return bin[ptr].get_tiny_ptr();
}

void ByteArrayPo2CTable::Bin::query_value(uint8_t ptr, uint64_t* value_ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    *value_ptr = bin[ptr].get_value();
}

uint64_t ByteArrayPo2CTable::Bin::query_value(uint8_t ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    return bin[ptr].get_value();
}

// ~0 for full bin
// i+1 for position i
uint8_t ByteArrayPo2CTable::Bin::insert(uint64_t quotiented_key, uint8_t ptr,
                                        uint64_t value) {
    if (this->full())
        return ~0;

    ++cnt;
    auto tmp = head;
    head = bin[head - 1].get_tiny_ptr();
    bin[tmp - 1].set_quotiented_key(quotiented_key);
    bin[tmp - 1].set_tiny_ptr(ptr);
    bin[tmp - 1].set_value(value);

    return tmp;
}

void ByteArrayPo2CTable::Bin::update_quotiented_key(uint8_t ptr,
                                                    uint64_t quotiented_key) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    bin[ptr].set_quotiented_key(quotiented_key);
}

void ByteArrayPo2CTable::Bin::update_tiny_ptr(uint8_t ptr, uint8_t tiny_ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    bin[ptr].set_tiny_ptr(tiny_ptr);
}

void ByteArrayPo2CTable::Bin::update_value(uint8_t ptr, uint64_t value) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    bin[ptr].set_value(value);
}

void ByteArrayPo2CTable::Bin::free(uint8_t ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    bin[ptr].set_tiny_ptr(head);
    head = ptr + 1;
    --cnt;
}

ByteArrayPo2CTable::ByteArrayPo2CTable(int n) {
    bin_num = (n + ByteArrayDereferenceTable::kBinSize - 1) /
              ByteArrayDereferenceTable::kBinSize;
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

uint8_t ByteArrayPo2CTable::Allocate(uint64_t key, uint64_t quotiented_key,
                                     uint8_t tiny_ptr, uint64_t value) {
    uint64_t hashbin[2];
    for (int i = 0; i < 2; ++i) {
        hashbin[i] = HashBin[i](key);
    }

    uint8_t flag = tab[hashbin[0]].count() > tab[hashbin[1]].count();

    uint8_t ptr = tab[hashbin[flag]].insert(quotiented_key, tiny_ptr, value);

    // assert(ptr);

    ptr ^= flag * (ptr != ByteArrayDereferenceTable::kOverflowTinyPtr) *
           ((1 << 8) - 1);
    return ptr;
}

void ByteArrayPo2CTable::UpdateQuotientedKey(uint64_t key, uint8_t ptr,
                                             uint64_t quotiented_key) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].update_quotiented_key(ptr, quotiented_key);
}

void ByteArrayPo2CTable::UpdateTinyptr(uint64_t key, uint8_t ptr,
                                       uint8_t tiny_ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].update_tiny_ptr(ptr, tiny_ptr);
}

void ByteArrayPo2CTable::UpdateValue(uint64_t key, uint8_t ptr,
                                     uint64_t value) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].update_value(ptr, value);
}

uint64_t ByteArrayPo2CTable::QueryDataAddress(uint64_t key, uint8_t ptr){
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return uint64_t(tab + HashBin[flag](key));
}

void ByteArrayPo2CTable::QueryQuotientedKey(uint64_t key, uint8_t ptr,
                                            uint64_t* value_ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].query_quotiented_key(ptr, value_ptr);
}

uint64_t ByteArrayPo2CTable::QueryQuotientedKey(uint64_t key, uint8_t ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].query_quotiented_key(ptr);
}

void ByteArrayPo2CTable::QueryTinyPtr(uint64_t key, uint8_t ptr,
                                      uint8_t* value_ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].query_tiny_ptr(ptr, value_ptr);
}

uint8_t ByteArrayPo2CTable::QueryTinyPtr(uint64_t key, uint8_t ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].query_tiny_ptr(ptr);
}

void ByteArrayPo2CTable::QueryValue(uint64_t key, uint8_t ptr,
                                    uint64_t* value_ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].query_value(ptr, value_ptr);
}

uint64_t ByteArrayPo2CTable::QueryValue(uint64_t key, uint8_t ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].query_value(ptr);
}

void ByteArrayPo2CTable::Free(uint64_t key, uint8_t ptr) {
    // assert(ptr != ByteArrayDereferenceTable::kOverflowTinyPtr);
    // assert(ptr != ByteArrayDereferenceTable::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    tab[HashBin[flag](key)].free(ptr);
}

}  // namespace tinyptr