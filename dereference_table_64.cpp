#include "dereference_table_64.h"
#include "overflow_table.h"
#include "po2c_table.h"

namespace tinyptr {

DeferenceTable64::DeferenceTable64(int n) {
    p_tab = new Po2CTable(n);
    o_tab = new OverflowTable;
}

uint8_t DeferenceTable64::Allocate(uint64_t key, uint64_t value) {
    if (uint8_t ptr = p_tab->Allocate(key, value)) {
        if (ptr == kOverflowTinyPtr)
            return o_tab->Allocate(key, value);
        return ptr;
    }
    return 0;
}

bool DeferenceTable64::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->Update(key, value);
    return p_tab->Update(key, ptr, value);
}

bool DeferenceTable64::Query(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->Query(key, value_ptr);
    return p_tab->Query(key, ptr, value_ptr);
}

bool DeferenceTable64::Free(uint64_t key, uint8_t ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->Free(key);
    return p_tab->Free(key, ptr);
}

}  // namespace tinyptr