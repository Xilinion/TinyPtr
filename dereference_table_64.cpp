#include "dereference_table_64.h"

namespace tinyptr {

class {

   public:
    static constexpr size_t kBinSize = 127;
    static constexpr uint8_t kNullTinyPtr = 0;
    static constexpr uint8_t kOverflowTinyPtr = ((1 << 8) - 1);

   public:
   private:
    Po2CTable* p_tab;
    OverflowTable* o_tab;

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

    bool DeferenceTable64::Query(uint64_t key, uint8_t ptr,
                                 uint64_t* value_ptr) {
        assert(ptr);

        if (ptr == kOverflowTinyPtr)
            return o_tab->Query(key, value_ptr);
        return p_tab->Query(key, ptr, value_ptr);
    }

    bool DeferenceTable64::Free(uint64_t key, uint8_t ptr) {
        assert(ptr);

        if (ptr == kOverflowTinyPtr)
            return o_tab->Free(key, value);
        return p_tab->Free(key, ptr, value);
    }
};

}  // namespace tinyptr