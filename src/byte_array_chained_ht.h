#pragma once

#include <cstdint>
#include <functional>
#include "common.h"

namespace tinyptr {

class ByteArrayChainedHT {
   public:
    const uint64_t kHashSeed1;
    const uint64_t kHashSeed2;
    const uint8_t kQuotientingTailLength;
    const uint64_t kQuotientingTailMask;
    const uint64_t kBaseTabSize;
    const uint16_t kBinSize;
    const uint64_t kBinNum;
    const uint8_t kTinyPtrOffset;
    const uint8_t kValueOffset;
    const uint8_t kQuotKeyByteLength;
    const uint8_t kEntryByteLength;
    const uint16_t kBinByteLength;

   protected:
    uint8_t AutoQuotTailLength(uint64_t size);

   public:
    ByteArrayChainedHT(uint64_t size, uint8_t quotienting_tail_length,
                       uint16_t bin_size);
    ByteArrayChainedHT(uint64_t size, uint16_t bin_size);

   protected:
    uint64_t hash_1(uint64_t key);
    uint64_t hash_1_bin(uint64_t key);
    uint64_t hash_1_base_id(uint64_t key);
    uint64_t hash_2(uint64_t key);
    uint64_t hash_2_bin(uint64_t key);

    uint8_t& bin_cnt(uint64_t bin_id);
    uint8_t& bin_head(uint64_t bin_id);
    uint8_t& base_tab_ptr(uint64_t base_id);

    uint8_t* ptab_query_entry_address(uint64_t key, uint8_t ptr);
    uint8_t* ptab_insert_entry_address(uint64_t key);

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t key, uint64_t value);
    void Free(uint64_t key);

    // Experimental Utility Functions
   public:
    double AvgChainLength();
    uint32_t MaxChainLength();
    uint64_t* ChainLengthHistogram();
    void FillChainLength(uint8_t chain_lenght);

   protected:
    uint8_t* byte_array;
    uint8_t* base_tab;
    uint8_t* bin_cnt_head;

   protected:
    uint8_t* play_entry;
    uint64_t chain_length;

   public:
    bool QueryNoMem(uint64_t key, uint64_t* value_ptr);
    void set_chain_length(uint64_t chain_length);
};

}  // namespace tinyptr