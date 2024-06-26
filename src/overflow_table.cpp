#include "overflow_table.h"

namespace tinyptr {

bool OverflowTable::allocation_check(uint64_t key) {
#ifdef TINYPTR_DEREFTAB64_KEY_UNIQUENESS_CHECK
    return tab.find(key) == tab.end();
#else
    return 1;
#endif
}

// We leave the sanity check of keys' uniqueness to users and promise allocation will success
uint8_t OverflowTable::Allocate(uint64_t key, uint64_t value) {
    if (allocation_check(key)) {
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