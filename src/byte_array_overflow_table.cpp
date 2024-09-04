#include "byte_array_overflow_table.h"

namespace tinyptr {

uint8_t ByteArrayOverflowTable::Allocate(uint64_t key, uint64_t value) {
    tab[key] = value;
    return ~0;
}

void ByteArrayOverflowTable::Update(uint64_t key, uint64_t value) {
    tab[key] = value;
}

void ByteArrayOverflowTable::Query(uint64_t key, uint64_t* value_ptr) {
    *value_ptr = tab[key];
}

uint64_t ByteArrayOverflowTable::Query(uint64_t key) {
    return tab[key];
}

void ByteArrayOverflowTable::Free(uint64_t key) {
    tab.erase(key);
}
}  // namespace tinyptr