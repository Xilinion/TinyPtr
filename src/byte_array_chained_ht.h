#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <cstdint>
#include <functional>
#include "common.h"
#include "utils/cache_line_size.h"

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
    const uintptr_t kPtr16BAlignMask = ~0xF;
    const uintptr_t kPtr16BBufferOffsetMask = 0xF;
    const uint8_t kPtr16BBufferSecondLoadOffset = 16;
    const uintptr_t kPtrCacheLineOffsetMask = utils::kCacheLineSize - 1;
    const uintptr_t kPtrCacheLineAlignMask =
        ~((uintptr_t)(utils::kCacheLineSize - 1));

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

   protected:
    void random_base_entry_prefetch();
    uint8_t* non_temporal_load_single_entry(uint8_t* entry);
    void evict_entry_cache_line(uint8_t* entry);

   protected:
    uint64_t limited_base_id(uint64_t key);

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
    uint64_t QueryEntryCnt();

   protected:
    uint8_t* byte_array;
    uint8_t* base_tab;
    uint8_t* bin_cnt_head;

   protected:
    uint8_t non_temporal_load_entry_buffer[64];

   protected:
    uint64_t query_entry_cnt = 0;
    uint8_t* play_entry;
    uint64_t chain_length;

    uint64_t limited_base_entry_num;
    uint64_t limited_base_cnt = 0;

   public:
    bool QueryNoMem(uint64_t key, uint64_t* value_ptr);
    void set_chain_length(uint64_t chain_length);
};

}  // namespace tinyptr