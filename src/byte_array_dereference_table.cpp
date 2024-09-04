#include "byte_array_dereference_table.h"

#include <cassert>
#include "byte_array_overflow_table.h"
#include "byte_array_po2c_table.h"

namespace tinyptr {

ByteArrayDereferenceTable::ByteArrayDereferenceTable(int n) {
    p_tab = new ByteArrayPo2CTable(n);
    o_tab = new ByteArrayOverflowTable;
}

uint8_t ByteArrayDereferenceTable::Allocate(uint64_t key,
                                            uint64_t quotiented_key,
                                            uint8_t tiny_ptr, uint64_t value) {
    if (uint8_t ptr = p_tab->Allocate(key, quotiented_key, tiny_ptr, value)) {
        if (ptr == kOverflowTinyPtr)
            return o_tab->Allocate(key, value);
        return ptr;
    }
    return 0;
}

void ByteArrayDereferenceTable::UpdateQuotientedKey(uint64_t key, uint8_t ptr,
                                                    uint64_t quotiented_key) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    p_tab->UpdateQuotientedKey(key, ptr, quotiented_key);
}

void ByteArrayDereferenceTable::UpdateTinyptr(uint64_t key, uint8_t ptr,
                                              uint8_t tiny_ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    p_tab->UpdateTinyptr(key, ptr, tiny_ptr);
}

void ByteArrayDereferenceTable::UpdateValue(uint64_t key, uint8_t ptr,
                                            uint64_t value) {
    // assert(ptr);
    if (ptr == kOverflowTinyPtr) {
        o_tab->Update(key, value);
    } else {
        p_tab->UpdateTinyptr(key, ptr, value);
    }
}

uint64_t ByteArrayDereferenceTable::QueryDataAddress(uint64_t key,
                                                     uint8_t ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);

    return p_tab->QueryDataAddress(key, ptr);
}

void ByteArrayDereferenceTable::QueryQuotientedKey(uint64_t key, uint8_t ptr,
                                                   uint64_t* value_ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    p_tab->QueryQuotientedKey(key, ptr, value_ptr);
}

uint64_t ByteArrayDereferenceTable::QueryQuotientedKey(uint64_t key,
                                                       uint8_t ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    return p_tab->QueryQuotientedKey(key, ptr);
}

void ByteArrayDereferenceTable::QueryTinyPtr(uint64_t key, uint8_t ptr,
                                             uint8_t* value_ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    p_tab->QueryTinyPtr(key, ptr, value_ptr);
}

uint8_t ByteArrayDereferenceTable::QueryTinyPtr(uint64_t key, uint8_t ptr) {
    // assert(ptr);
    // assert (ptr == kOverflowTinyPtr);
    return p_tab->QueryTinyPtr(key, ptr);
}

uint64_t ByteArrayDereferenceTable::QueryValue(uint64_t key, uint8_t ptr) {
    // assert(ptr);

    if (ptr == kOverflowTinyPtr)
        return o_tab->Query(key);
    return p_tab->QueryValue(key, ptr);
}

void ByteArrayDereferenceTable::QueryValue(uint64_t key, uint8_t ptr,
                                           uint64_t* value_ptr) {
    // assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->Query(key, value_ptr);
    else
        p_tab->QueryValue(key, ptr, value_ptr);
}

void ByteArrayDereferenceTable::Free(uint64_t key, uint8_t ptr) {
    // assert(ptr);

    if (ptr == kOverflowTinyPtr)
        o_tab->Free(key);
    else
        p_tab->Free(key, ptr);
}

}  // namespace tinyptr