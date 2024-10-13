#pragma once

#include <cstdint>
#include "byte_array_chained_ht.h"

namespace tinyptr {

class BinAwareChainedHT : public ByteArrayChainedHT {
   public:
    const uint8_t kDoubleSlotSize;
    const uint8_t kInBinTinyPtrMask = 0x7f;
    const uint8_t kInDoubleSlotMask = 0x80;
    const uint8_t kSecondHashMask = 0x80;

   public:
    BinAwareChainedHT(uint64_t size, uint16_t bin_size,
                  uint8_t double_slot_num = 32);

    ~BinAwareChainedHT() = default;

   private:
    uint8_t* ptab_insert_entry_address(uint64_t key, uint8_t pre_tiny_ptr);
    void bin_prefetch(uintptr_t key, uint8_t ptr);
    uint8_t& bin_head_double_slot(uint64_t bin_id);

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t key, uint64_t value);
    void Free(uint64_t key);

   private:
    uint8_t* head_double_slot;
};

}  // namespace tinyptr
