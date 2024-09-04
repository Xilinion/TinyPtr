#pragma once

#include <cstddef>
#include <cstdint>

#include "common.h"
#include "dereference_table.h"

namespace tinyptr {

class ByteArrayPo2CTable;
class ByteArrayOverflowTable;

class ByteArrayDereferenceTable : DereferenceTable {

   public:
    static constexpr size_t kBinSize = 127;
    static constexpr uint8_t kNullTinyPtr = 0;
    static constexpr uint8_t kOverflowTinyPtr = ((1 << 8) - 1);

   public:
    ByteArrayDereferenceTable() = delete;
    ByteArrayDereferenceTable(int n);
    ~ByteArrayDereferenceTable() = default;
    
    uint8_t Allocate(uint64_t key, uint64_t quotiented_key, uint8_t tiny_ptr,
                     uint64_t value);
    void UpdateQuotientedKey(uint64_t key, uint8_t ptr, uint64_t quotiented_key);
    void UpdateTinyptr(uint64_t key, uint8_t ptr, uint8_t tiny_ptr);
    void UpdateValue(uint64_t key, uint8_t ptr, uint64_t value);
    uint64_t QueryDataAddress(uint64_t key, uint8_t ptr);
    void QueryQuotientedKey(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QueryQuotientedKey(uint64_t key, uint8_t ptr);
    void QueryTinyPtr(uint64_t key, uint8_t ptr, uint8_t* value_ptr);
    uint8_t QueryTinyPtr(uint64_t key, uint8_t ptr);
    void QueryValue(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QueryValue(uint64_t key, uint8_t ptr);
    void Free(uint64_t key, uint8_t ptr);

   private:
    ByteArrayPo2CTable* p_tab;
    ByteArrayOverflowTable* o_tab;
};

}  // namespace tinyptr