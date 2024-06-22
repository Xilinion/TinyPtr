#pragma once

#include <cstdint>

#include "common.h"
#include "overflow_table.h"
#include "po2c_table.h"
#include "dereference_table.h"

namespace tinyptr {


class DeferenceTable64 : DeferenceTable{

    public:
    
    static constexpr size_t kBinSize = 127;
    static constexpr uint8_t kNullTinyPtr = 0;
    static constexpr uint8_t kOverflowTinyPtr = ((1 << 8) - 1);
   public:
    uint8_t Allocate(uint64_t key, uint64_t value);
    bool Update(uint64_t key, uint8_t ptr, uint64_t value);
    bool Query(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    bool Free(uint64_t key, uint8_t ptr);

   private:
    Po2CTable* p_tab;
    OverflowTable* o_tab;
};

}  // namespace tinyptr