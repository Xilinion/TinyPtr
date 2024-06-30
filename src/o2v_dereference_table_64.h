#pragma once

#include <cstddef>
#include <cstdint>


#include "common.h"
#include "dereference_table.h"

namespace tinyptr {

class O2VPo2CTable;
class O2VOverflowTable;

class O2VDereferenceTable64 : DereferenceTable {

   public:
    static constexpr size_t kBinSize = 127;
    static constexpr uint8_t kNullTinyPtr = 0;
    static constexpr uint8_t kOverflowTinyPtr = ((1 << 8) - 1);

   public:
    O2VDereferenceTable64() = delete;
    O2VDereferenceTable64(int n);
    ~O2VDereferenceTable64() = default;

    uint8_t Allocate(uint64_t key, uint64_t value_1, uint64_t value_2);
    void UpdateFirst(uint64_t key, uint8_t ptr, uint64_t value);
    void UpdateSecond(uint64_t key, uint8_t ptr, uint64_t value);
    void QueryFirst(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    void QuerySecond(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    void Free(uint64_t key, uint8_t ptr);

   private:
    O2VPo2CTable* p_tab;
    O2VOverflowTable* o_tab;
};

}  // namespace tinyptr