#pragma once

#include <emmintrin.h>
#include <immintrin.h>
#include <nmmintrin.h>
#include <sys/cdefs.h>
#include <sys/types.h>
#include <atomic>
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
                                 uint16_t bin_size, bool if_resize = false);
    ConcurrentByteArrayChainedHT(uint64_t size, uint16_t bin_size,
                                 bool if_resize = false);
    ConcurrentByteArrayChainedHT(uint64_t size, bool if_resize = false);

    ~ConcurrentByteArrayChainedHT();

   protected:
    __attribute__((always_inline)) inline uint64_t hash_1(uint64_t key) {
        return HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed1);
    }

    __attribute__((always_inline)) inline uint64_t hash_1_bin(uint64_t key) {
        return (HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed1)) % kBinNum;
        // return 0;
    }

    __attribute__((always_inline)) inline uint64_t hash_1_base_id(
        uint64_t key) {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
               kQuotientingTailMask;
    }

    __attribute__((always_inline)) inline uint64_t hash_key_rebuild(
        uint64_t quotiented_key, uint64_t base_id) {
        uint64_t tmp = (quotiented_key << kQuotientingTailLength) >>
                       kQuotientingTailLength;
        return ((HASH_FUNCTION(&tmp, sizeof(uint64_t), kHashSeed1) ^ base_id) &
                kQuotientingTailMask) |
               (tmp << kQuotientingTailLength);
    }

    __attribute__((always_inline)) inline uint64_t hash_2(uint64_t key) {
        return HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed2);
    }

    __attribute__((always_inline)) inline uint64_t hash_2_bin(uint64_t key) {
        return (HASH_FUNCTION(&key, sizeof(uint64_t), kHashSeed2)) % kBinNum;
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
                uint8_t new_pre_tiny_ptr = head + 1;
                *entry = new_pre_tiny_ptr | (flag << 7);
                head = head + 1 + entry[kTinyPtrOffset];
                if (head > kBinSize) {
                    head -= (kBinSize + 1);
                }
                cnt++;
                bin_locks[bin_id].clear(std::memory_order_release);
                return entry;
            } else {
                bin_locks[bin_id].clear(std::memory_order_release);
                return nullptr;
            }
        }
    }

    __attribute__((always_inline)) inline uint64_t base_id_to_version_id(
        uint64_t base_id) {
        return base_id % base_tab_concurrent_version_size;
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

    void SetResizeStride(uint64_t stride_num);
    bool ResizeMoveStride(uint64_t stride_id,
                          ConcurrentByteArrayChainedHT* new_ht);

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

    std::unique_ptr<std::atomic_flag[]> bin_locks;
    std::unique_ptr<std::atomic_flag[]> base_tab_concurrent_version;

    size_t bin_locks_size;
    size_t base_tab_concurrent_version_size;

    uint64_t resize_stride_size;

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