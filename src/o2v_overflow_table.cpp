#include "o2v_overflow_table.h"

namespace tinyptr {

uint8_t O2VOverflowTable::Allocate(uint64_t key, uint64_t value_1,
                                   uint64_t value_2) {
    tab[key] = VV{value_1, value_2};
    return ~0;
}

void O2VOverflowTable::UpdateFirst(uint64_t key, uint64_t value) {
    tab[key].value_1 = value;
}

void O2VOverflowTable::UpdateSecond(uint64_t key, uint64_t value) {
    tab[key].value_2 = value;
}

void O2VOverflowTable::QueryFirst(uint64_t key, uint64_t* value_ptr) {
    *value_ptr = tab[key].value_1;
}

uint64_t O2VOverflowTable::QueryFirst(uint64_t key) {
    return tab[key].value_1;
}

void O2VOverflowTable::QuerySecond(uint64_t key, uint64_t* value_ptr) {
    *value_ptr = tab[key].value_2;
}

uint64_t O2VOverflowTable::QuerySecond(uint64_t key) {
    return tab[key].value_2;
}

void O2VOverflowTable::Free(uint64_t key) {
    tab.erase(key);
}
}  // namespace tinyptr