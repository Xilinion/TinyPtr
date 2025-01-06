#include "concurrent_skulker_ht.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace tinyptr {

uint8_t ConcurrentSkulkerHT::kBushLookup[256];
struct ConcurrentSkulkerHTBushLookupInitializer
    concurrent_skulker_ht_bush_lookup_initializer;

std::mutex output_mutex;

// #define USE_CONCURRENT_VERSION_QUERY

uint8_t ConcurrentSkulkerHT::AutoQuotTailLength(uint64_t size) {
    uint8_t res = 16;
    // making 4 *size > 1 << res > 2 * size
    size >>= 15;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

uint64_t ConcurrentSkulkerHT::GenBaseHashFactor(uint64_t min_gap,
                                                uint64_t mod_mask) {
    // mod must be a power of 2
    // min_gap should be a relative small threshold
    uint64_t res = (rand() | 1) & mod_mask;
    while (res < min_gap) {
        res = (rand() | 1) & mod_mask;
    }
    return res;
}

uint64_t ConcurrentSkulkerHT::GenBaseHashInverse(uint64_t base_hash_factor,
                                                 uint64_t mod_mask,
                                                 uint64_t mod_bit_length) {
    uint64_t res = 1;
    while (--mod_bit_length) {
        res = (base_hash_factor * res) & mod_mask;
        base_hash_factor = (base_hash_factor * base_hash_factor) & mod_mask;
    }
    return res;
}

uint8_t ConcurrentSkulkerHT::AutoLockNum(uint64_t thread_num_supported) {
    uint8_t res = 0;
    thread_num_supported = thread_num_supported * thread_num_supported;
    while (thread_num_supported) {
        thread_num_supported >>= 1;
        res++;
    }
    return 1 << res;
}

ConcurrentSkulkerHT::ConcurrentSkulkerHT(uint64_t size,
                                         uint8_t quotienting_tail_length,
                                         uint16_t bin_size)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kQuotientingTailLength(quotienting_tail_length
                                 ? quotienting_tail_length
                                 : AutoQuotTailLength(size)),
      kQuotientingTailMask((1ULL << kQuotientingTailLength) - 1),
      kQuotKeyByteLength((64 + 7 - kQuotientingTailLength) >> 3),
      kEntryByteLength(kQuotKeyByteLength + 1 + 8),
      kBinByteLength(kBinSize * kEntryByteLength),
      kBushRatio(1 -
                 std::exp(size / (-1.0 * (1ULL << kQuotientingTailLength)))),
      kBushOverflowBound(0.21),
      kSkulkerRatio((1 - kBushRatio * (1ULL << kQuotientingTailLength) / size) +
                    kBushOverflowBound),
      kInitExhibitorNum(kBushByteLength / kEntryByteLength),
      kInitSkulkerNum(kBushByteLength - kInitExhibitorNum * kEntryByteLength -
                      kControlByteLength - kConcurrentVersionByteLength),
      kBushCapacity(std::min(
          16, static_cast<int>(floor(kInitExhibitorNum / kBushRatio)))),
      kBushNum(((1ULL << kQuotientingTailLength) + kBushCapacity - 1) /
               kBushCapacity),
      kConcurrentVersionOffset(kBushByteLength - kConcurrentVersionByteLength),
      kControlOffset(kConcurrentVersionOffset - kControlByteLength),
      kSkulkerOffset(kControlOffset - 1),
      kBinSize(bin_size),
      kBinNum((size * kSkulkerRatio + kBinSize - 1) / kBinSize),
      kTinyPtrOffset(0),
      kKeyOffset(1),
      kValueOffset(kKeyOffset + kQuotKeyByteLength),
      kBaseHashFactor(GenBaseHashFactor(kBushCapacity, kQuotientingTailMask)),
      kBaseHashInverse(GenBaseHashInverse(kBaseHashFactor, kQuotientingTailMask,
                                          kQuotientingTailLength)),
      kFastDivisionReciprocal(kFastDivisionBase / kBushCapacity + 1) {

    assert(4 * size >= (1ULL << (kQuotientingTailLength)));
    assert(bin_size < 128);

    // Determine the number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads =
            4;  // Fallback to a default value if hardware_concurrency is not available
    }

    bush_locks_size = num_threads * num_threads * 10;
    bin_locks_size = kBinNum;

    bush_locks = std::make_unique<std::atomic_flag[]>(bush_locks_size);
    bin_locks = std::make_unique<std::atomic_flag[]>(bin_locks_size);

    // Initialize all atomic flags to clear
    for (size_t i = 0; i < bush_locks_size; ++i) {
        bush_locks[i].clear();
    }
    for (size_t i = 0; i < bin_locks_size; ++i) {
        bin_locks[i].clear();
    }

    (void)posix_memalign(reinterpret_cast<void**>(&bush_tab), 64,
                         kBushNum * kBushByteLength);
    memset(bush_tab, 0, kBushNum * kBushByteLength);

    (void)posix_memalign(reinterpret_cast<void**>(&byte_array), 64,
                         kBinNum * kBinSize * kEntryByteLength);
    memset(byte_array, 0, kBinNum * kBinSize * kEntryByteLength);

    (void)posix_memalign(reinterpret_cast<void**>(&bin_cnt_head), 64,
                         kBinNum << 1);

    for (uint64_t i = 0, ptr_offset = kTinyPtrOffset; i < kBinNum; i++) {
        for (uint8_t j = 0; j < kBinSize - 1; j++) {
            byte_array[ptr_offset] = j + 2;
            ptr_offset += kEntryByteLength;
        }
        // the last entry in the bin points to null
        byte_array[ptr_offset] = 0;
        ptr_offset += kEntryByteLength;

        bin_cnt_head[i << 1] = 0;
        bin_cnt_head[(i << 1) | 1] = 1;
    }

    // freopen("lalala.txt", "w", stdout);
}

ConcurrentSkulkerHT::ConcurrentSkulkerHT(uint64_t size, uint16_t bin_size)
    : ConcurrentSkulkerHT(size, 0, bin_size) {}

bool ConcurrentSkulkerHT::Insert(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t> &concurrent_version = *reinterpret_cast<std::atomic<uint8_t>*>(&bush_tab[(bush_id << kBushIdShiftOffset) + kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) || !concurrent_version.compare_exchange_weak(expected_version, expected_version + 1));

    // {
    //     std::lock_guard<std::mutex> lock(output_mutex);
        // std::cout << "thread_id: " << std::this_thread::get_id() << " bush_id: " << bush_id <<   " concurrent_version before: " << (int)concurrent_version << std::endl;
    // }

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint16_t& control_info = *reinterpret_cast<uint16_t*>(
        bush_tab + (bush_id << kBushIdShiftOffset) + kControlOffset);
    uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                       kBushLookup[(control_info >> kByteShift)];
    uint16_t control_info_before_item = control_info >> in_bush_offset;
    uint8_t before_item_cnt =
        kBushLookup[control_info_before_item & kByteMask] +
        kBushLookup[(control_info_before_item >> kByteShift)];

    if ((control_info >> in_bush_offset) & 1) {

        uint8_t overload_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);
        uint8_t* pre_tiny_ptr =
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      (before_item_cnt > kInitExhibitorNum - overload_flag
                           ? kSkulkerOffset -
                                 (before_item_cnt - 1 -
                                  (kInitExhibitorNum - overload_flag))
                           : (before_item_cnt - 1) * kEntryByteLength)];

        uintptr_t pre_deref_key = base_id;

        bool result = ptab_insert(pre_tiny_ptr, pre_deref_key, key, value);

        // Release the lock
        concurrent_version.fetch_add(1);
        return result;

    } else {
        uint8_t pre_overload_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

        uint8_t overload_flag =
            item_cnt + 1 > (kInitSkulkerNum + kInitExhibitorNum);

        uint8_t exhibitor_num;

        bool new_overload_flag = pre_overload_flag == 0 && overload_flag == 1;
        // spill before inserting the new item
        if (new_overload_flag) {
            // spill the last exhibitor
            exhibitor_num = kInitExhibitorNum;
            if (!bush_exhibitor_hide(bush, base_id - in_bush_offset,
                                     control_info, exhibitor_num, item_cnt)) {
                concurrent_version.fetch_add(1);
                return false;
            }
        }

        exhibitor_num = kInitExhibitorNum - overload_flag;

        if (before_item_cnt >= exhibitor_num) {
            // move skulkers after it one byte backwards
            for (uint8_t* i =
                     bush + kSkulkerOffset - (item_cnt - exhibitor_num);
                 i < bush + kSkulkerOffset - (before_item_cnt - exhibitor_num);
                 i++) {
                *i = *(i + 1);
            }

            uint8_t* pre_tiny_ptr =
                bush + kSkulkerOffset - (before_item_cnt - exhibitor_num);
            *pre_tiny_ptr = 0;

            uintptr_t pre_deref_key = base_id;

            if (!ptab_insert(pre_tiny_ptr, pre_deref_key, key, value)) {
                // recover the bush
                for (uint8_t* i = bush + kSkulkerOffset -
                                  (before_item_cnt - exhibitor_num);
                     i > bush + kSkulkerOffset - (item_cnt - exhibitor_num);
                     i--) {
                    *i = *(i - 1);
                }

                if (new_overload_flag) {
                    bush_skulker_raid(bush, base_id - in_bush_offset,
                                      control_info, exhibitor_num, item_cnt);
                }
                concurrent_version.fetch_add(1);
                return false;
            }

        } else {

            if (item_cnt >= exhibitor_num) {
                if (!bush_exhibitor_hide(bush, base_id - in_bush_offset,
                                         control_info, exhibitor_num,
                                         item_cnt)) {
                    if (new_overload_flag) {
                        bush_skulker_raid(bush, base_id - in_bush_offset,
                                          control_info, exhibitor_num,
                                          item_cnt);
                    }
                    concurrent_version.fetch_add(1);
                    return false;
                }
            }

            memmove(bush + (before_item_cnt + 1) * kEntryByteLength,
                    bush + before_item_cnt * kEntryByteLength,
                    kEntryByteLength * (exhibitor_num - before_item_cnt - 1));

            uint8_t* new_entry = bush + before_item_cnt * kEntryByteLength;
            new_entry[kTinyPtrOffset] = 0;
            *(uint64_t*)(new_entry + kKeyOffset) =
                key >> kQuotientingTailLength;
            *(uint64_t*)(new_entry + kValueOffset) = value;
        }

        control_info |= (1u << in_bush_offset);
        concurrent_version.fetch_add(1);
        return true;
    }
}

bool ConcurrentSkulkerHT::Query(uint64_t key, uint64_t* value_ptr) {
#ifdef USE_CONCURRENT_VERSION_QUERY
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t> &concurrent_version = *reinterpret_cast<std::atomic<uint8_t>*>(&bush_tab[(bush_id << kBushIdShiftOffset) + kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while (expected_version % 2 != 0 || !concurrent_version.compare_exchange_weak(expected_version, expected_version + 1));

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint16_t& control_info = *reinterpret_cast<uint16_t*>(
        bush_tab + (bush_id << kBushIdShiftOffset) + kControlOffset);
    uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                       kBushLookup[(control_info >> kByteShift)];
    uint16_t control_info_before_item = control_info >> in_bush_offset;
    uint8_t before_item_cnt =
        kBushLookup[control_info_before_item & kByteMask] +
        kBushLookup[(control_info_before_item >> kByteShift)];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    if ((control_info >> in_bush_offset) & 1) {

        key = key & (~kQuotientingTailMask);

        if (before_item_cnt <= exhibitor_num) {
            uint8_t* exhibitor_ptr =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            if ((*reinterpret_cast<uint64_t*>(exhibitor_ptr + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr =
                    *reinterpret_cast<uint64_t*>(exhibitor_ptr + kValueOffset);
                concurrent_version.fetch_add(1);
                return true;
            }
        }

        uint8_t* pre_tiny_ptr =
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      (before_item_cnt > exhibitor_num
                           ? kSkulkerOffset -
                                 (before_item_cnt - 1 - exhibitor_num)
                           : (before_item_cnt - 1) * kEntryByteLength)];

        uintptr_t pre_deref_key = base_id;

        while (*pre_tiny_ptr != 0) {

            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(entry + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
                concurrent_version.fetch_add(1);
                return true;
            }
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);
        }
    }

    concurrent_version.fetch_add(1);
    return false;
#else
    // Current version without locking
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t> &concurrent_version = *reinterpret_cast<std::atomic<uint8_t>*>(&bush_tab[(bush_id << kBushIdShiftOffset) + kConcurrentVersionOffset]);

query_again:

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1));

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint16_t& control_info = *reinterpret_cast<uint16_t*>(
        bush_tab + (bush_id << kBushIdShiftOffset) + kControlOffset);
    uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                       kBushLookup[(control_info >> kByteShift)];
    uint16_t control_info_before_item = control_info >> in_bush_offset;
    uint8_t before_item_cnt =
        kBushLookup[control_info_before_item & kByteMask] +
        kBushLookup[(control_info_before_item >> kByteShift)];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    if ((control_info >> in_bush_offset) & 1) {

        key = key & (~kQuotientingTailMask);

        if (before_item_cnt <= exhibitor_num) {
            uint8_t* exhibitor_ptr =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            if ((*reinterpret_cast<uint64_t*>(exhibitor_ptr + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr =
                    *reinterpret_cast<uint64_t*>(exhibitor_ptr + kValueOffset);

                if (concurrent_version.load() != expected_version) {
                    goto query_again;
                }
                return true;
            }
        }

        uint8_t* pre_tiny_ptr =
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      (before_item_cnt > exhibitor_num
                           ? kSkulkerOffset -
                                 (before_item_cnt - 1 - exhibitor_num)
                           : (before_item_cnt - 1) * kEntryByteLength)];

        uintptr_t pre_deref_key = base_id;

        while (*pre_tiny_ptr != 0) {

            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(entry + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);

                if (concurrent_version.load() != expected_version) {
                    goto query_again;
                }
                return true;
            }
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);

            if (concurrent_version.load() != expected_version) {
                goto query_again;
            }
        }
    }

    if (concurrent_version.load() != expected_version) {
        goto query_again;
    }
    return false;
#endif
}

bool ConcurrentSkulkerHT::Update(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t> &concurrent_version = *reinterpret_cast<std::atomic<uint8_t>*>(&bush_tab[(bush_id << kBushIdShiftOffset) + kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) || !concurrent_version.compare_exchange_weak(expected_version, expected_version + 1));

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint16_t& control_info = *reinterpret_cast<uint16_t*>(
        bush_tab + (bush_id << kBushIdShiftOffset) + kControlOffset);
    uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                       kBushLookup[(control_info >> kByteShift)];
    uint16_t control_info_before_item = control_info >> in_bush_offset;
    uint8_t before_item_cnt =
        kBushLookup[control_info_before_item & kByteMask] +
        kBushLookup[(control_info_before_item >> kByteShift)];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    if ((control_info >> in_bush_offset) & 1) {

        key = key & (~kQuotientingTailMask);

        if (before_item_cnt <= exhibitor_num) {
            uint8_t* exhibitor_ptr =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            if ((*reinterpret_cast<uint64_t*>(exhibitor_ptr + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *reinterpret_cast<uint64_t*>(exhibitor_ptr + kValueOffset) =
                    value;
                concurrent_version.fetch_add(1);
                return true;
            }
        }

        uint8_t* pre_tiny_ptr =
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      (before_item_cnt > exhibitor_num
                           ? kSkulkerOffset -
                                 (before_item_cnt - 1 - exhibitor_num)
                           : (before_item_cnt - 1) * kEntryByteLength)];

        uintptr_t pre_deref_key = base_id;

        while (*pre_tiny_ptr != 0) {
            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(entry + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
                concurrent_version.fetch_add(1);
                return true;
            }
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);
        }
    }

    concurrent_version.fetch_add(1);
    return false;
}

void ConcurrentSkulkerHT::Free(uint64_t key) {
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t> &concurrent_version = *reinterpret_cast<std::atomic<uint8_t>*>(&bush_tab[(bush_id << kBushIdShiftOffset) + kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) || !concurrent_version.compare_exchange_weak(expected_version, expected_version + 1));

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint16_t& control_info = *reinterpret_cast<uint16_t*>(
        bush_tab + (bush_id << kBushIdShiftOffset) + kControlOffset);
    uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                       kBushLookup[(control_info >> kByteShift)];
    uint16_t control_info_before_item = control_info >> in_bush_offset;
    uint8_t before_item_cnt =
        kBushLookup[control_info_before_item & kByteMask] +
        kBushLookup[(control_info_before_item >> kByteShift)];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    if ((control_info >> in_bush_offset) & 1) {

        key = key & (~kQuotientingTailMask);

        if (before_item_cnt <= exhibitor_num) {

            uint8_t* exhibitor_ptr =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            if ((*reinterpret_cast<uint64_t*>(exhibitor_ptr + kKeyOffset)
                 << kQuotientingTailLength) == key) {

                if (exhibitor_ptr[kTinyPtrOffset] == 0) {

                    memmove(
                        bush + (before_item_cnt - 1) * kEntryByteLength,
                        bush + before_item_cnt * kEntryByteLength,
                        kEntryByteLength * (exhibitor_num - before_item_cnt));

                    control_info &= ~(1u << in_bush_offset);
                    item_cnt--;
                    before_item_cnt--;

                    if (item_cnt >= exhibitor_num) {
                        bush_skulker_raid(bush, base_id - in_bush_offset,
                                          control_info, exhibitor_num - 1,
                                          item_cnt);

                        uint8_t overflow_flag_after =
                            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

                        if (overload_flag && !overflow_flag_after) {
                            bush_skulker_raid(bush, base_id - in_bush_offset,
                                              control_info, exhibitor_num,
                                              item_cnt);
                        }
                    }

                } else {
                    uint8_t* pre_tiny_ptr = exhibitor_ptr + kTinyPtrOffset;
                    uintptr_t pre_deref_key = base_id;

                    ptab_lift_to_bush(pre_tiny_ptr, pre_deref_key,
                                      exhibitor_ptr);
                }
                concurrent_version.fetch_add(1);
                return;
            }
        }

        uint8_t* pre_tiny_ptr =
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      (before_item_cnt > exhibitor_num
                           ? kSkulkerOffset -
                                 (before_item_cnt - 1 - exhibitor_num)
                           : (before_item_cnt - 1) * kEntryByteLength)];

        uintptr_t pre_deref_key = base_id;

        ptab_free(pre_tiny_ptr, pre_deref_key, key);

        if (*pre_tiny_ptr == 0 && before_item_cnt > exhibitor_num) {
            control_info &= ~(1u << in_bush_offset);
            item_cnt--;
            before_item_cnt--;

            for (uint8_t* i =
                     bush + kSkulkerOffset - (before_item_cnt - exhibitor_num);
                 i > bush + kSkulkerOffset - (item_cnt - exhibitor_num); i--) {
                *i = *(i - 1);
            }

            uint8_t overflow_flag_after =
                item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

            if (overload_flag && !overflow_flag_after) {
                bush_skulker_raid(bush, base_id - in_bush_offset, control_info,
                                  exhibitor_num, item_cnt);
            }
        }
    }

    concurrent_version.fetch_add(1);
}

}  // namespace tinyptr
