#include "overflow_table.h"

namespace tinyptr {

uint8_t OverflowTable::Allocate(uint64_t key, uint64_t value) {
    if (tab.find(key) == tab.end()) {
        tab[key] = value;
        return ~0;
    }
    return 0;
}

bool OverflowTable::Update(uint64_t key, uint64_t value) {
    if (tab.find(key) != tab.end()) {
        tab[key] = value;
        return 1;
    }
    return 0;
}

bool OverflowTable::Query(uint64_t key, uint64_t* value_ptr) {
    if (tab.find(key) != tab.end()) {
        *value_ptr = tab[key];
        return 1;
    }
    return 0;
}

bool OverflowTable::Free(uint64_t key) {
    if (tab.find(key) != tab.end()) {
        tab.erase(key);
        return 1;
    }
    return 0;
}
}  // namespace tinyptr