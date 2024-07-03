#include "o2v_dereference_table_64.h"
#include "o2v_overflow_table.h"
#include "o2v_po2c_table.h"

namespace tinyptr {

O2VDereferenceTable64::O2VDereferenceTable64(int n) {
    p_tab = new O2VPo2CTable(n);
    o_tab = new O2VOverflowTable;
}

uint8_t O2VDereferenceTable64::Allocate(uint64_t key, uint64_t value_1,
                                        uint64_t value_2) {
    if (uint8_t ptr = p_tab->Allocate(key, value_1, value_2)) {
        if (ptr == kOverflowTinyPtr)
            return o_tab->Allocate(key, value_1, value_2);
        return ptr;
    }
    return 0;
}

void O2VDereferenceTable64::UpdateFirst(uint64_t key, uint8_t ptr,
                                        uint64_t value) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->UpdateFirst(key, value);
    else
        p_tab->UpdateFirst(key, ptr, value);
}

void O2VDereferenceTable64::UpdateSecond(uint64_t key, uint8_t ptr,
                                         uint64_t value) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->UpdateSecond(key, value);
    else
        p_tab->UpdateSecond(key, ptr, value);
}

void O2VDereferenceTable64::QueryFirst(uint64_t key, uint8_t ptr,
                                       uint64_t* value_ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->QueryFirst(key, value_ptr);
    else
        p_tab->QueryFirst(key, ptr, value_ptr);
}

uint64_t O2VDereferenceTable64::QueryFirst(uint64_t key, uint8_t ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->QueryFirst(key);
    return p_tab->QueryFirst(key, ptr);
}

void O2VDereferenceTable64::QuerySecond(uint64_t key, uint8_t ptr,
                                        uint64_t* value_ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->QuerySecond(key, value_ptr);
    else
        p_tab->QuerySecond(key, ptr, value_ptr);
}

uint64_t O2VDereferenceTable64::QuerySecond(uint64_t key, uint8_t ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->QuerySecond(key);
    return p_tab->QuerySecond(key, ptr);
}

void O2VDereferenceTable64::Free(uint64_t key, uint8_t ptr) {
    assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->Free(key);
    else
        p_tab->Free(key, ptr);
}

}  // namespace tinyptr