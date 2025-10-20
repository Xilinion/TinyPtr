#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <pthread.h>
#include <sys/cdefs.h>
#include <sys/types.h>
#include <atomic>
#include <bitset>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>
#include "common.h"
#include "utils/cache_line_size.h"

namespace tinyptr {

class BlastHT {
   public:
    static uint8_t kCloudLookup[256];
    static constexpr uint8_t kByteMask = 0xFF;
    static constexpr uint8_t kByteShift = 8;

   public:
    const uint64_t kHashSeed1;
    const uint64_t kHashSeed2;
    const uint32_t kCloudQuotientingLength;
    const uint32_t kBlastQuotientingLength;
    const uint64_t kBlastQuotientingMask;
    const uint64_t kQuotientingTailMask;
    const uint32_t kQuotKeyByteLength;
    const uint8_t kEntryByteLength;
    const uint32_t kCrystalOffset;

    // layout
    // {4*{TP,K,V},{Bolts},{Control}}

    // const uint8_t kCloudByteLength = utils::kCacheLineSize;
    static constexpr uint32_t kCloudByteLength = 64;
    static constexpr uint32_t kCloudIdShiftOffset = 6;
    // control byte and bolts grow from the end of the cloud
    static constexpr uint32_t kConcurrentVersionByteLength = 1;
    static constexpr uint32_t kConcurrentVersionOffset =
        kCloudByteLength - kConcurrentVersionByteLength;
    static constexpr uint32_t kControlByteLength = 1;
    static constexpr uint32_t kControlOffset =
        kConcurrentVersionOffset - kControlByteLength;
    // static constexpr uint32_t kBoltByteLength = 2;
    // static constexpr uint32_t kBoltByteLengthShift = 1;
    // static constexpr uint32_t kBoltOffset = kControlOffset;

    static constexpr uint32_t kControlCrystalMask = (1 << 3) - 1;
    static constexpr uint32_t kControlTinyPtrShift = 3;

    static constexpr double kCloudOverflowBound = 0.23;
    // expected ratio of used quotienting slots
    const uint64_t kCloudNum;

    const uint16_t kBinSize;
    const uint64_t kBinNum;
    static constexpr uint32_t kTinyPtrOffset = 0;
    static constexpr uint32_t kFingerprintOffset = 0;
    static constexpr uint32_t kFingerprintShift = 3;
    static constexpr uint32_t kKeyOffset = 0;
    const uint32_t kValueOffset;
    const uint16_t kBinByteLength;
    static constexpr uintptr_t kPtr16BAlignMask = ~0xF;
    static constexpr uintptr_t kPtr16BBufferOffsetMask = 0xF;
    static constexpr uint32_t kPtr16BBufferSecondLoadOffset = 16;
    // const uintptr_t kPtrCacheLineOffsetMask = utils::kCacheLineSize - 1;
    static constexpr uintptr_t kPtrCacheLineOffsetMask = kCloudByteLength - 1;
    static constexpr uintptr_t kPtrCacheLineAlignMask =
        ~((uintptr_t)(kCloudByteLength - 1));

    // cloud hash, spread the cloud_id
    // const uint64_t kBaseHashFactor;
    // const uint64_t kBaseHashInverse;

    static constexpr uint32_t kFastDivisionUpperBoundLog = 31;
    const uint32_t kFastDivisionShift[2];
    static constexpr uint64_t kFastDivisionUpperBound =
        1ULL << kFastDivisionUpperBoundLog;
    const uint64_t kFastDivisionReciprocal[2];

   protected:
    uint8_t AutoQuotTailLength(uint64_t size);
    uint64_t GenBaseHashFactor(uint64_t min_gap, uint64_t mod_mask);
    // uint64_t GenBaseHashInverse(uint64_t cloud_hash_factor, uint64_t mod_mask,
    //                             uint64_t mod_bit_length);
    uint8_t AutoLockNum(uint64_t thread_num_supported);
    uint8_t AutoFastDivisionInnerShift(uint64_t divisor);

   public:
    BlastHT(uint64_t size, uint8_t quotienting_tail_length, uint16_t bin_size,
            bool if_resize);
    BlastHT(uint64_t size, uint16_t bin_size, bool if_resize);
    BlastHT(uint64_t size, bool if_resize);

    ~BlastHT();

    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t key, uint64_t value);
    void Free(uint64_t key);

    void SetResizeStride(uint64_t stride_num);
    bool ResizeMoveStride(uint64_t stride_id, BlastHT* new_ht);

    void Scan4Stats();

   protected:
    void* combined_mem;
    uint8_t* cloud_tab;
    uint8_t* byte_array;
    uint8_t* bin_cnt_head;

    std::unique_ptr<std::atomic_flag[]> bin_locks;

    size_t bin_locks_size;

    uint64_t resize_stride_size;

   protected:
    __attribute__((always_inline)) inline uint64_t hash_1(uint64_t key) {
        return HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed1);
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin(uint64_t key) {
        // uint64_t hash = HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed1) >>
        //                 (8 * sizeof(uint64_t) - kFastDivisionUpperBoundLog);
        uint64_t hash = HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed1) >> 33;

        return hash -
               ((hash * kFastDivisionReciprocal[1]) >> kFastDivisionShift[1]) *
                   kBinNum;
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin_from_hash(
        uint64_t hash) {
        // hash >>= (8 * sizeof(uint64_t) - kFastDivisionUpperBoundLog);
        hash >>= 33;
        return hash -
               ((hash * kFastDivisionReciprocal[1]) >> kFastDivisionShift[1]) *
                   kBinNum;
    }

    __attribute__((always_inline)) inline uint64_t hash_cloud_id(uint64_t key) {

        uint64_t tmp = key >> kCloudQuotientingLength;

        // return (((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
        //          kQuotientingTailMask) *
        //         kBaseHashFactor) &
        //        kQuotientingTailMask;
        return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
                kQuotientingTailMask);
    }

    __attribute__((always_inline)) inline uint64_t cloud_id_from_hash(
        uint64_t hash) {
        return (hash & kQuotientingTailMask);
    }

    __attribute__((always_inline)) inline uint64_t hash_key_rebuild(
        uint64_t quotiented_key, uint64_t cloud_id) {
        uint64_t tmp = (quotiented_key << kCloudQuotientingLength) >>
                       kCloudQuotientingLength;

        // return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^
        //          ((cloud_id * kBaseHashInverse) & kQuotientingTailMask)) &
        //         kQuotientingTailMask) |
        //        (tmp << kQuotientingTailLength);
        return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ cloud_id) &
                kQuotientingTailMask) |
               (tmp << kCloudQuotientingLength);
    }

    __attribute__((always_inline)) inline uint64_t hash_key_rebuild(
        uint64_t quotiented_key, uint64_t cloud_id, uint8_t fp) {
        uint64_t tmp = (quotiented_key << kBlastQuotientingLength) >>
                       kBlastQuotientingLength;

        uint64_t fp_64 = fp;
        fp_64 <<= kCloudQuotientingLength;
        fp_64 |= cloud_id;

        // return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^
        //          ((cloud_id * kBaseHashInverse) & kQuotientingTailMask)) &
        //         kQuotientingTailMask) |
        //        (tmp << kQuotientingTailLength);
        return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ fp_64) &
                kBlastQuotientingMask) |
               (tmp << kBlastQuotientingLength);
    }

    __attribute__((always_inline)) inline uint64_t hash_2(uint64_t key) {
        return HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed2);
    }

    __attribute__((always_inline)) inline uint64_t hash_2_bin(uint64_t key) {
        uint64_t hash = HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed2) >> 33;

        // std::cout << "hash_2_bin: "
        //           << hash - ((hash * kFastDivisionReciprocal[1]) >>
        //                      kFastDivisionShift[1]) *
        //                         kBinNum
        //           << std::endl;
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

        uint8_t* entry = ptab_insert_entry_address(pre_deref_key);

        if (entry != nullptr) {
            *pre_tiny_ptr = *entry;
            // assuming little endian
            *reinterpret_cast<uint64_t*>(entry + kKeyOffset) = key;
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        } else {
            return false;
        }
    }

    __attribute__((always_inline)) inline void ptab_free(
        uint8_t* pre_tiny_ptr, uintptr_t pre_deref_key) {

        uint8_t* entry = ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);

        uint64_t bin_id = (entry - byte_array) / kBinByteLength;

        while (bin_locks[bin_id].test_and_set(std::memory_order_acquire))
            ;

        bin_cnt(bin_id)--;
        uint8_t& head = bin_head(bin_id);
        uint8_t cur_in_bin_pos = ((uint8_t)((*pre_tiny_ptr) << 1) >> 1) - 1;
        entry[kTinyPtrOffset] = head + kBinSize - cur_in_bin_pos;
        if (entry[kTinyPtrOffset] > kBinSize) {
            entry[kTinyPtrOffset] -= (kBinSize + 1);
        }
        head = cur_in_bin_pos;
        *pre_tiny_ptr = 0;

        bin_locks[bin_id].clear(std::memory_order_release);
    }

   public:
    __attribute__((always_inline)) inline void prefetch_key(uint64_t key) {
        uint64_t truncated_key = key >> kBlastQuotientingLength;
        uint64_t cloud_id =
            ((HASH_FUNCTION(&truncated_key, sizeof(uint64_t), kHashSeed1) ^ key) &
             kBlastQuotientingMask);
        cloud_id = cloud_id & kQuotientingTailMask;

        // do fast division
        // uint64_t cloud_id;
        // if (cloud_id > kFastDivisionUpperBound) {
        //     cloud_id = cloud_id / kCloudCapacity;
        // } else {
        //     cloud_id = (1ULL * cloud_id * kFastDivisionReciprocal[0]) >>
        //                kFastDivisionShift[0];
        // }
        __builtin_prefetch(
            (const void*)(cloud_tab + (cloud_id << kCloudIdShiftOffset)), 1, 3);
    }
};

}  // namespace tinyptr
