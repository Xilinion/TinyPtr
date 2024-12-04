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

class SkulkerHT {
    friend struct SkulkerHTBushLookupInitializer;

   public:
    static uint8_t kBushLookup[256];

   public:
    const uint64_t kHashSeed1;
    const uint64_t kHashSeed2;
    const uint8_t kQuotientingTailLength;
    const uint64_t kQuotientingTailMask;
    const uint8_t kQuotKeyByteLength;
    const uint8_t kEntryByteLength;

    // const uint8_t kBushByteLength = utils::kCacheLineSize;
    const uint8_t kBushByteLength = 64;
    const uint8_t kBushIdShiftOffset = 6;
    // expected ratio of used quotienting slots
    const double kBushRatio;
    const double kBushOverflowBound;
    // expected ratio of skulkers over data size
    const double kSkulkerRatio;
    const uint8_t kInitExhibitorNum;
    const uint8_t kInitSkulkerNum;
    const uint8_t kBushCapacity;

    // control byte and skulkers grow from the end of the bush
    const uint64_t kBushNum;
    const uint8_t kControlByteOffset;
    const uint8_t kSkulkerOffset;

    const uint16_t kBinSize;
    const uint64_t kBinNum;
    const uint8_t kTinyPtrOffset;
    const uint8_t kKeyOffset;
    const uint8_t kValueOffset;
    const uint16_t kBinByteLength;
    const uintptr_t kPtr16BAlignMask = ~0xF;
    const uintptr_t kPtr16BBufferOffsetMask = 0xF;
    const uint8_t kPtr16BBufferSecondLoadOffset = 16;
    const uintptr_t kPtrCacheLineOffsetMask = utils::kCacheLineSize - 1;
    const uintptr_t kPtrCacheLineAlignMask =
        ~((uintptr_t)(utils::kCacheLineSize - 1));

    const uint64_t kFastDivisionShift = 36;
    const uint64_t kFastDivisionUpperBound = 1ULL << 32;
    const uint64_t kFastDivisionBase = 1ULL << kFastDivisionShift;
    const uint64_t kFastDivisionReciprocal;

   protected:
    uint8_t AutoQuotTailLength(uint64_t size);

   public:
    SkulkerHT(uint64_t size, uint8_t quotienting_tail_length,
              uint16_t bin_size);
    SkulkerHT(uint64_t size, uint16_t bin_size);

   protected:
    __attribute__((always_inline)) inline uint64_t hash_1(uint64_t key) {
        return XXH64(&key, sizeof(uint64_t), kHashSeed1);
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin(uint64_t key) {
        return (XXH64(&key, sizeof(uint64_t), kHashSeed1)) % kBinNum;
    }

    __attribute__((always_inline)) inline uint64_t hash_1_base_id(
        uint64_t key) {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
               kQuotientingTailMask;
    }

    __attribute__((always_inline)) inline uint64_t hash_1_key_rebuild(
        uint64_t quotiented_key, uint64_t base_id) {
        uint64_t tmp = (quotiented_key << kQuotientingTailLength) >>
                       kQuotientingTailLength;
        return ((XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ base_id) &
                kQuotientingTailMask) |
               (tmp << kQuotientingTailLength);
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
            uint8_t new_pre_tiny_ptr = head | (flag << 7);
            head = entry[kTinyPtrOffset];
            *entry = new_pre_tiny_ptr;
            cnt++;
            return entry;
        } else {
            return nullptr;
        }
    }

    __attribute__((always_inline)) inline bool ptab_insert(
        uint8_t* pre_tiny_ptr, uintptr_t pre_deref_key, uint64_t key,
        uint64_t value) {
        while (*pre_tiny_ptr != 0) {
            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);
        }

        uint8_t* entry = ptab_insert_entry_address(pre_deref_key);

        if (entry != nullptr) {
            *pre_tiny_ptr = *entry;
            // assuming little endian
            *reinterpret_cast<uint64_t*>(entry + kKeyOffset) =
                key >> kQuotientingTailLength;
            entry[kTinyPtrOffset] = 0;
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        } else {
            return false;
        }
    }

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t key, uint64_t value);
    void Free(uint64_t key);

    uint64_t QueryEntryCnt();

   protected:
    uint8_t* bush_tab;
    uint8_t* byte_array;
    uint8_t* base_tab;
    uint8_t* bin_cnt_head;

    uint8_t* play_entry;

    uint64_t query_entry_cnt = 0;
};

struct SkulkerHTBushLookupInitializer {
    SkulkerHTBushLookupInitializer() {
        for (int i = 0; i < 256; ++i) {
            SkulkerHT::kBushLookup[i] = 0;
            int tmp = i;
            while (tmp) {
                SkulkerHT::kBushLookup[i]++;
                tmp -= tmp & (-tmp);
            }
        }
    };
};

}  // namespace tinyptr