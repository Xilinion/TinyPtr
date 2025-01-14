#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <sys/cdefs.h>
#include <sys/types.h>
#include <cstdint>
#include <functional>
#include "common.h"
#include "utils/cache_line_size.h"

namespace tinyptr {

class ConcurrentByteArrayChainedHT {
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
    ConcurrentByteArrayChainedHT(uint64_t size, uint8_t quotienting_tail_length,
                       uint16_t bin_size);
    ConcurrentByteArrayChainedHT(uint64_t size, uint16_t bin_size);

   protected:
    __attribute__((always_inline)) inline uint64_t hash_1(uint64_t key) {
        return XXH64(&key, sizeof(uint64_t), kHashSeed1);
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin(uint64_t key) {
        return (XXH64(&key, sizeof(uint64_t), kHashSeed1)) % kBinNum;
        // return 0;
    }

    __attribute__((always_inline)) inline uint64_t hash_1_base_id(
        uint64_t key) {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
               kQuotientingTailMask;
    }

    __attribute__((always_inline)) inline uint64_t hash_2(uint64_t key) {
        return XXH64(&key, sizeof(uint64_t), kHashSeed2);
    }

    __attribute__((always_inline)) inline uint64_t hash_2_bin(uint64_t key) {
        return (XXH64(&key, sizeof(uint64_t), kHashSeed2)) % kBinNum;
        // return 0;
    }

    __attribute__((always_inline)) inline uint8_t& bin_cnt(uint64_t bin_id) {
        return bin_cnt_head[bin_id << 1];
    }

    __attribute__((always_inline)) inline uint8_t& bin_head(uint64_t bin_id) {
        return bin_cnt_head[(bin_id << 1) | 1];
    }

    __attribute__((always_inline)) inline uint8_t& base_tab_ptr(
        uint64_t base_id) {
        return base_tab[base_id];
    }

    __attribute__((always_inline)) inline uint8_t* ptab_query_entry_address(
        uint64_t key, uint8_t ptr) {
        uint8_t flag = (ptr >= (1 << 7));
        ptr = ptr & ((1 << 7) - 1);
        if (flag) {
            return byte_array +
                   (hash_2_bin(key) * kBinSize + ptr - 1) * kEntryByteLength;
        } else {
            return byte_array +
                   (hash_1_bin(key) * kBinSize + ptr - 1) * kEntryByteLength;
        }
    }

    __attribute__((always_inline)) inline uint8_t* ptab_insert_entry_address(
        uint64_t key) {
        uint64_t bin1 = hash_1_bin(key);
        uint64_t bin2 = hash_2_bin(key);
        uint8_t flag = bin_cnt(bin1) > bin_cnt(bin2);
        uint64_t bin_id = flag ? bin2 : bin1;

        uint8_t& head = bin_head(bin_id);
        uint8_t& cnt = bin_cnt(bin_id);

        if (head) {
            uint8_t* entry =
                byte_array + (bin_id * kBinSize + head - 1) * kEntryByteLength;
            *entry = head | (flag << 7);
            head = entry[kTinyPtrOffset];
            cnt++;
            return entry;
        } else {
            return nullptr;
        }
    }

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