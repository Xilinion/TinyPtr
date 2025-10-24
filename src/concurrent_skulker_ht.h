#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <pthread.h>
#include <sys/cdefs.h>
#include <sys/types.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>  // Include for std::unique_ptr
#include <mutex>   // Include for concurrency
#include <queue>
#include <thread>
#include <utility>
#include <vector>
#include "common.h"
#include "utils/cache_line_size.h"

namespace tinyptr {

class ConcurrentSkulkerHT {
    friend struct ConcurrentSkulkerHTBushLookupInitializer;

   public:
    static uint8_t kBushLookup[256];
    static constexpr uint8_t kByteMask = 0xFF;
    static constexpr uint8_t kByteShift = 8;

   public:
    const uint64_t kHashSeed1;
    const uint64_t kHashSeed2;
    const uint8_t kQuotientingTailLength;
    const uint64_t kQuotientingTailMask;
    const uint8_t kQuotKeyByteLength;
    const uint8_t kEntryByteLength;

    // layout
    // {4*{TP,K,V},{Skulkers},{Control}}

    // const uint8_t kBushByteLength = utils::kCacheLineSize;
    static constexpr uint8_t kBushByteLength = 64;
    static constexpr uint8_t kBushIdShiftOffset = 6;
    // control byte and skulkers grow from the end of the bush
    static constexpr uint8_t kConcurrentVersionByteLength = 1;
    static constexpr uint8_t kControlByteLength = 2;
    const uint8_t kConcurrentVersionOffset;
    const uint8_t kControlOffset;
    const uint8_t kSkulkerOffset;

    // expected ratio of used quotienting slots
    const double kBushRatio;
    const double kBushOverflowBound;
    // expected ratio of skulkers over data size
    const double kSkulkerRatio;
    const uint8_t kInitExhibitorNum;
    const uint8_t kInitSkulkerNum;
    const uint8_t kBushCapacity;
    const uint64_t kBushNum;

    const uint16_t kBinSize;
    const uint64_t kBinNum;
    const uint8_t kTinyPtrOffset;
    const uint8_t kKeyOffset;
    const uint8_t kValueOffset;
    const uint16_t kBinByteLength;
    static constexpr uintptr_t kPtr16BAlignMask = ~0xF;
    static constexpr uintptr_t kPtr16BBufferOffsetMask = 0xF;
    static constexpr uint8_t kPtr16BBufferSecondLoadOffset = 16;
    // const uintptr_t kPtrCacheLineOffsetMask = utils::kCacheLineSize - 1;
    static constexpr uintptr_t kPtrCacheLineOffsetMask = kBushByteLength - 1;
    static constexpr uintptr_t kPtrCacheLineAlignMask =
        ~((uintptr_t)(kBushByteLength - 1));

    // base hash, spread the base_id
    const uint64_t kBaseHashFactor;
    const uint64_t kBaseHashInverse;

    static constexpr uint8_t kFastDivisionUpperBoundLog = 31;
    const uint8_t kFastDivisionShift[2];
    const uint64_t kFastDivisionUpperBound = 1ULL << kFastDivisionUpperBoundLog;
    const uint64_t kFastDivisionReciprocal[2];

   protected:
    uint8_t AutoQuotTailLength(uint64_t size);
    uint64_t GenBaseHashFactor(uint64_t min_gap, uint64_t mod_mask);
    uint64_t GenBaseHashInverse(uint64_t base_hash_factor, uint64_t mod_mask,
                                uint64_t mod_bit_length);
    uint8_t AutoLockNum(uint64_t thread_num_supported);
    uint8_t AutoFastDivisionInnerShift(uint64_t divisor);

   public:
    ConcurrentSkulkerHT(uint64_t size, uint8_t quotienting_tail_length,
                        uint16_t bin_size, bool if_resize = false, double resize_threshold = 1.0);
    ConcurrentSkulkerHT(uint64_t size, uint16_t bin_size,
                        bool if_resize = false, double resize_threshold = 1.0);
    ConcurrentSkulkerHT(uint64_t size, bool if_resize = false, double resize_threshold = 1.0);

    ~ConcurrentSkulkerHT();

    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t key, uint64_t value);
    void Free(uint64_t key);

    void SetResizeStride(uint64_t stride_num);
    bool ResizeMoveStride(uint64_t stride_id, ConcurrentSkulkerHT* new_ht);

    uint64_t GetTableSize() const { return kBushNum * 4; }

   protected:
    uint8_t* bush_tab;
    uint8_t* byte_array;
    uint8_t* bin_cnt_head;

    std::unique_ptr<std::atomic_flag[]> bin_locks;

    size_t bin_locks_size;

    uint64_t resize_stride_size;

   protected:
    __attribute__((always_inline)) inline uint64_t hash_1(uint64_t key) {
        return XXH64(&key, sizeof(uint64_t), kHashSeed1);
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin(uint64_t key) {
        uint64_t hash = XXH64(&key, sizeof(uint64_t), kHashSeed1) >> 33;

        return hash -
               ((hash * kFastDivisionReciprocal[1]) >> kFastDivisionShift[1]) *
                   kBinNum;
    }

    __attribute__((always_inline)) inline uint64_t hash_base_id(uint64_t key) {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (((XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
                 kQuotientingTailMask) *
                kBaseHashFactor) &
               kQuotientingTailMask;
    }

    __attribute__((always_inline)) inline uint64_t hash_key_rebuild(
        uint64_t quotiented_key, uint64_t base_id) {
        uint64_t tmp = (quotiented_key << kQuotientingTailLength) >>
                       kQuotientingTailLength;
        return ((XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^
                 ((base_id * kBaseHashInverse) & kQuotientingTailMask)) &
                kQuotientingTailMask) |
               (tmp << kQuotientingTailLength);
    }

    __attribute__((always_inline)) inline uint64_t hash_2(uint64_t key) {
        return XXH64(&key, sizeof(uint64_t), kHashSeed2);
    }

    __attribute__((always_inline)) inline uint64_t hash_2_bin(uint64_t key) {
        uint64_t hash = XXH64(&key, sizeof(uint64_t), kHashSeed2) >> 33;
        return hash -
               ((hash * kFastDivisionReciprocal[1]) >> kFastDivisionShift[1]) *
                   kBinNum;
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

        if (bin1 == bin2) {
            // If bin1 and bin2 are the same, lock only once
            while (bin_locks[bin1].test_and_set(std::memory_order_acquire))
                ;

            uint8_t& head = bin_head(bin1);
            uint8_t& cnt = bin_cnt(bin1);

            if (head < kBinSize) {
                uint8_t* entry =
                    byte_array + (bin1 * kBinSize + head) * kEntryByteLength;
                uint8_t new_pre_tiny_ptr = head + 1;
                head = head + 1 + entry[kTinyPtrOffset];
                if (head > kBinSize) {
                    head -= (kBinSize + 1);
                }
                *entry = new_pre_tiny_ptr;
                cnt++;
                bin_locks[bin1].clear(std::memory_order_release);
                return entry;
            } else {
                bin_locks[bin1].clear(std::memory_order_release);
                return nullptr;
            }
        } else {
            uint64_t lock_bin1 = bin1;
            uint64_t lock_bin2 = bin2;

            bool lock_flag = lock_bin1 > lock_bin2;
            if (lock_flag) {
                std::swap(lock_bin1, lock_bin2);
            }

            // Lock lock_bin1 first, then lock_bin2
            while (bin_locks[lock_bin1].test_and_set(std::memory_order_acquire))
                ;
            while (bin_locks[lock_bin2].test_and_set(std::memory_order_acquire))
                ;

            uint8_t flag = bin_cnt(bin1) > bin_cnt(bin2);
            uint64_t bin_id = flag ? bin2 : bin1;

            // Unlock the mutex of the bin not being used
            if (flag ^ lock_flag) {
                bin_locks[lock_bin1].clear(std::memory_order_release);
            } else {
                bin_locks[lock_bin2].clear(std::memory_order_release);
            }

            uint8_t& head = bin_head(bin_id);
            uint8_t& cnt = bin_cnt(bin_id);

            if (head < kBinSize) {
                uint8_t* entry =
                    byte_array + (bin_id * kBinSize + head) * kEntryByteLength;
                uint8_t new_pre_tiny_ptr = (head + 1) | (flag << 7);
                head = head + 1 + entry[kTinyPtrOffset];
                if (head > kBinSize) {
                    head -= (kBinSize + 1);
                }
                *entry = new_pre_tiny_ptr;
                cnt++;
                bin_locks[bin_id].clear(std::memory_order_release);
                return entry;
            } else {
                bin_locks[bin_id].clear(std::memory_order_release);
                return nullptr;
            }
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

    __attribute__((always_inline)) inline void ptab_free(
        uint8_t* pre_tiny_ptr, uintptr_t pre_deref_key,
        uint64_t quotiented_N_reshifted_key) {

        uint8_t* cur_tiny_ptr = nullptr;
        uint8_t* cur_entry = nullptr;
        uint8_t* aiming_entry = nullptr;

        if (*pre_tiny_ptr != 0) {
            cur_entry = ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(cur_entry + kKeyOffset)
                 << kQuotientingTailLength) == quotiented_N_reshifted_key) {
                aiming_entry = cur_entry;
            }
            cur_tiny_ptr = cur_entry + kTinyPtrOffset;
        } else {
            return;
        }

        while (*cur_tiny_ptr != 0) {
            pre_tiny_ptr = cur_tiny_ptr;

            cur_entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(cur_tiny_ptr), *cur_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(cur_entry + kKeyOffset)
                 << kQuotientingTailLength) == quotiented_N_reshifted_key) {
                aiming_entry = cur_entry;
            }
            cur_tiny_ptr = cur_entry + kTinyPtrOffset;
        }

        if (aiming_entry == nullptr) {
            return;
        }

        uint8_t tmp = aiming_entry[kTinyPtrOffset];
        memcpy(aiming_entry, cur_entry, kEntryByteLength);
        aiming_entry[kTinyPtrOffset] = tmp;

        uint64_t bin_id = (cur_entry - byte_array) / kBinByteLength;

        while (bin_locks[bin_id].test_and_set(std::memory_order_acquire))
            ;

        bin_cnt(bin_id)--;
        uint8_t& head = bin_head(bin_id);
        uint8_t cur_in_bin_pos = ((uint8_t)((*pre_tiny_ptr) << 1) >> 1) - 1;
        cur_entry[kTinyPtrOffset] = head + kBinSize - cur_in_bin_pos;
        if (cur_entry[kTinyPtrOffset] > kBinSize) {
            cur_entry[kTinyPtrOffset] -= (kBinSize + 1);
        }
        head = cur_in_bin_pos;
        *pre_tiny_ptr = 0;

        bin_locks[bin_id].clear(std::memory_order_release);
    }

    __attribute__((always_inline)) inline void ptab_lift_to_bush(
        uint8_t* pre_tiny_ptr, uintptr_t pre_deref_key,
        uint8_t* exhibitor_ptr) {

        uint8_t* cur_tiny_ptr = nullptr;
        uint8_t* cur_entry = nullptr;

        if (*pre_tiny_ptr != 0) {
            cur_entry = ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            cur_tiny_ptr = cur_entry + kTinyPtrOffset;
        } else {
            return;
        }

        while (*cur_tiny_ptr != 0) {
            pre_tiny_ptr = cur_tiny_ptr;

            cur_entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(cur_tiny_ptr), *cur_tiny_ptr);
            cur_tiny_ptr = cur_entry + kTinyPtrOffset;
        }

        uint8_t tmp = exhibitor_ptr[kTinyPtrOffset];
        memcpy(exhibitor_ptr, cur_entry, kEntryByteLength);
        exhibitor_ptr[kTinyPtrOffset] = tmp;

        uint64_t bin_id = (cur_entry - byte_array) / kBinByteLength;

        while (bin_locks[bin_id].test_and_set(std::memory_order_acquire))
            ;

        bin_cnt(bin_id)--;
        uint8_t& head = bin_head(bin_id);
        uint8_t cur_in_bin_pos = ((uint8_t)((*pre_tiny_ptr) << 1) >> 1) - 1;
        cur_entry[kTinyPtrOffset] = head + kBinSize - cur_in_bin_pos;
        if (cur_entry[kTinyPtrOffset] > kBinSize) {
            cur_entry[kTinyPtrOffset] -= (kBinSize + 1);
        }
        head = cur_in_bin_pos;
        *pre_tiny_ptr = 0;

        bin_locks[bin_id].clear(std::memory_order_release);
    }

    __attribute__((always_inline)) inline bool bush_exhibitor_hide(
        uint8_t* bush, uint64_t bush_offset, uint16_t& control_info,
        uint8_t exhibitor_num, uint8_t item_cnt) {

        thread_local static uint8_t* play_entry = new uint8_t[kEntryByteLength];
        thread_local static uint64_t* play_1 =
            reinterpret_cast<uint64_t*>(play_entry);
        thread_local static uint64_t* play_2 =
            reinterpret_cast<uint64_t*>(play_entry + kValueOffset);

        // convert the last exhibitor to the first skulker
        memcpy(play_entry, bush + (exhibitor_num - 1) * kEntryByteLength,
               kEntryByteLength);
        // *play_1 = *(uint64_t*)(bush + (exhibitor_num - 1) * kEntryByteLength);
        // *play_2 = *(uint64_t*)(bush + (exhibitor_num - 1) * kEntryByteLength +
        //                        kValueOffset);

        // exhibitor spills and converts the last exhibitor to the first skulker
        for (uint8_t* i = bush + kSkulkerOffset - (item_cnt - exhibitor_num);
             i < bush + kSkulkerOffset; i++) {
            *i = *(i + 1);
        }

        // get the base_id of the last exhibitor

        uint32_t hide_in_bush_offset = _tzcnt_u32(
            _pdep_u32(1u << (item_cnt - exhibitor_num), control_info));

        /*
        uint8_t hide_in_bush_offset = 0;
        uint16_t control_info_before_spilled =
            control_info >> (hide_in_bush_offset + 1);

        while (kBushLookup[control_info_before_spilled & kByteMask] +
                   kBushLookup[(control_info_before_spilled >> kByteShift)] >=
               exhibitor_num) {
            hide_in_bush_offset++;
            control_info_before_spilled =
                control_info >> (hide_in_bush_offset + 1);
        }
        */

        uintptr_t spilled_base_id = bush_offset + hide_in_bush_offset;

        bush[kSkulkerOffset] = play_entry[kTinyPtrOffset];

        if (!ptab_insert(bush + kSkulkerOffset, spilled_base_id,
                         hash_key_rebuild(*(uint64_t*)(play_entry + kKeyOffset),
                                          spilled_base_id),
                         *(uint64_t*)(play_entry + kValueOffset))) {

            // recover the bush
            for (uint8_t* i = bush + kSkulkerOffset;
                 i > bush + kSkulkerOffset - (item_cnt - exhibitor_num); i--) {
                *i = *(i - 1);
            }

            memcpy(bush + (exhibitor_num - 1) * kEntryByteLength, play_entry,
                   kEntryByteLength);
            // *(uint64_t*)(bush + (exhibitor_num - 1) * kEntryByteLength) =
            //     *play_1;
            // *(uint64_t*)(bush + (exhibitor_num - 1) * kEntryByteLength +
            //              kValueOffset) = *play_2;

            return false;
        }

        return true;
    }

    // exhibitor_num is the # we currently have in the bush
    __attribute__((always_inline)) inline void bush_skulker_raid(
        uint8_t* bush, uint64_t bush_offset, uint16_t& control_info,
        uint8_t exhibitor_num, uint8_t item_cnt) {
        uint8_t* exhibitor_ptr = bush + (exhibitor_num)*kEntryByteLength;
        exhibitor_ptr[kTinyPtrOffset] = bush[kSkulkerOffset];

        for (uint8_t* i = bush + kSkulkerOffset;
             i > bush + kSkulkerOffset - (item_cnt - exhibitor_num - 1); i--) {
            *i = *(i - 1);
        }

        // get the base_id of the first skulker

        uint32_t raid_in_bush_offset = _tzcnt_u32(
            _pdep_u32(1u << (item_cnt - exhibitor_num - 1), control_info));

        /*
        uint8_t raid_in_bush_offset = 0;
        uint16_t control_info_before_spilled =
            control_info >> (raid_in_bush_offset + 1);

        while (kBushLookup[control_info_before_spilled & kByteMask] +
                   kBushLookup[(control_info_before_spilled >> kByteShift)] >
               exhibitor_num) {
            raid_in_bush_offset++;
            control_info_before_spilled =
                control_info >> (raid_in_bush_offset + 1);
        }
        */

        uint8_t* pre_tiny_ptr = exhibitor_ptr + kTinyPtrOffset;
        uintptr_t pre_deref_key = bush_offset + raid_in_bush_offset;

        ptab_lift_to_bush(pre_tiny_ptr, pre_deref_key, exhibitor_ptr);
    }

   public:
    __attribute__((always_inline)) inline void prefetch_key(uint64_t key) {
        uint64_t base_id = hash_base_id(key);
        // do fast division
        uint64_t bush_id;
        if (base_id > kFastDivisionUpperBound) {
            bush_id = base_id / kBushCapacity;
        } else {
            bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                      kFastDivisionShift[0];
        }
        __builtin_prefetch(
            (const void*)(bush_tab + (bush_id << kBushIdShiftOffset)));
    }
};

struct ConcurrentSkulkerHTBushLookupInitializer {
    ConcurrentSkulkerHTBushLookupInitializer() {
        for (int i = 0; i < 256; ++i) {
            ConcurrentSkulkerHT::kBushLookup[i] = 0;
            int tmp = i;
            while (tmp) {
                ConcurrentSkulkerHT::kBushLookup[i]++;
                tmp -= tmp & (-tmp);
            }
        }
    };
};

}  // namespace tinyptr