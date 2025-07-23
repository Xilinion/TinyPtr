#include "concurrent_skulker_ht.h"
#include <sys/mman.h>
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

// #define USE_CONCURRENT_VERSION_QUERY

uint8_t ConcurrentSkulkerHT::AutoFastDivisionInnerShift(uint64_t divisor) {
    uint8_t res = 0;
    while ((1ULL << res) < divisor) {
        res++;
    }
    return res;
}

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
                                         uint16_t bin_size, bool if_resize)
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
      kFastDivisionShift{
          static_cast<uint8_t>(kFastDivisionUpperBoundLog +
                               AutoFastDivisionInnerShift(kBushCapacity)),
          static_cast<uint8_t>(kFastDivisionUpperBoundLog +
                               AutoFastDivisionInnerShift(kBinNum))},
      kFastDivisionReciprocal{
          (1ULL << kFastDivisionShift[0]) / kBushCapacity + 1,
          (1ULL << kFastDivisionShift[1]) / kBinNum + 1} {

    assert(4 * size >= (1ULL << (kQuotientingTailLength)));
    assert(bin_size < 128);

    // Determine the number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads =
            4;  // Fallback to a default value if hardware_concurrency is not available
    }

    bin_locks_size = kBinNum;

    bin_locks = std::make_unique<std::atomic_flag[]>(bin_locks_size);

    for (size_t i = 0; i < bin_locks_size; ++i) {
        bin_locks[i].clear();
    }

    // auto start = std::chrono::high_resolution_clock::now();

    /*
        (void)posix_memalign(reinterpret_cast<void**>(&bush_tab), 64,
                         kBushNum * kBushByteLength);
    memset(bush_tab, 0, kBushNum * kBushByteLength);

    (void)posix_memalign(reinterpret_cast<void**>(&byte_array), 64,
                         kBinNum * kBinSize * kEntryByteLength);
    memset(byte_array, 0, kBinNum * kBinSize * kEntryByteLength);

    (void)posix_memalign(reinterpret_cast<void**>(&bin_cnt_head), 64,
                         kBinNum << 1);
    memset(bin_cnt_head, 0, kBinNum << 1);
    */

    // Calculate individual sizes
    uint64_t bush_size = kBushNum * kBushByteLength;
    uint64_t byte_array_size = kBinNum * kBinSize * kEntryByteLength;
    uint64_t bin_cnt_size = kBinNum << 1;

    // Align each section to 64 bytes
    uint64_t bush_size_aligned = (bush_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t byte_array_size_aligned =
        (byte_array_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t bin_cnt_size_aligned =
        (bin_cnt_size + 63) & ~static_cast<uint64_t>(63);

    // Total size for combined allocation
    uint64_t total_size =
        bush_size_aligned + byte_array_size_aligned + bin_cnt_size_aligned;

    // Allocate a single aligned block
    void* combined_mem;
    // if (posix_memalign(&combined_mem, 64, total_size) != 0) {
    //     // Handle allocation failure
    //     abort();
    // }

    // auto start = std::chrono::high_resolution_clock::now();

    if (if_resize) {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    } else {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
    }

    // auto end = chrono::high_resolution_clock::now();
    // std::cout
    //     << "mmap time: "
    //     << chrono::duration_cast<chrono::milliseconds>(end - start).count()
    //     << "ms" << std::endl;

    // combined_mem = mmap(nullptr, total_size, PROT_READ | PROT_WRITE,
    //                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);

    // combined_mem = calloc(1, total_size);

    // madvise(combined_mem, total_size, MADV_WILLNEED);
    // Zero the entire block in one operation
    // memset(combined_mem, 0, total_size);

    // Assign pointers to their respective regions
    uint8_t* base =
        (uint8_t*)((uint64_t)(combined_mem + 63) & ~static_cast<uint64_t>(63));
    bush_tab = base;
    base += bush_size_aligned;

    byte_array = base;
    base += byte_array_size_aligned;

    bin_cnt_head = base;

    // auto end = std::chrono::high_resolution_clock::now();

    // static auto initialization_time =
    //     std::chrono::duration_cast<std::chrono::milliseconds>(end - end)
    //         .count();

    // initialization_time +=
    //     std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
    //         .count();

    // std::cerr << "Initialization time: " << initialization_time << "ms"
    //           << std::endl;

    /*
    {
        size_t total_bytes = kBushNum * kBushByteLength;
        bush_tab = static_cast<uint8_t*>(calloc(1, total_bytes));
        if (!bush_tab) {
            perror("calloc(bush_tab)");
            exit(EXIT_FAILURE);
        }
    }

    {
        size_t total_bytes = kBinNum * kBinSize * kEntryByteLength;
        byte_array = static_cast<uint8_t*>(calloc(1, total_bytes));
        if (!byte_array) {
            perror("calloc(byte_array)");
            exit(EXIT_FAILURE);
        }
    }

    {
        size_t cnt_bytes = kBinNum << 1;
        bin_cnt_head = static_cast<uint8_t*>(calloc(1, cnt_bytes));
        if (!bin_cnt_head) {
            perror("calloc(bin_cnt_head)");
            exit(EXIT_FAILURE);
        }
    }
    */

    /*
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
    */

    // freopen("lalala.txt", "w", stdout);
}

ConcurrentSkulkerHT::ConcurrentSkulkerHT(uint64_t size, uint16_t bin_size,
                                         bool if_resize)
    : ConcurrentSkulkerHT(size, 0, bin_size, if_resize) {}

ConcurrentSkulkerHT::ConcurrentSkulkerHT(uint64_t size, bool if_resize)
    : ConcurrentSkulkerHT(size, 0, 127, if_resize) {}

ConcurrentSkulkerHT::~ConcurrentSkulkerHT() {
    munmap(bush_tab, kBushNum * kBushByteLength);
    munmap(byte_array, kBinNum * kBinSize * kEntryByteLength);
    munmap(bin_cnt_head, kBinNum << 1);
}

bool ConcurrentSkulkerHT::Insert(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                  kFastDivisionShift[0];
    }

    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &bush[kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    // {
    //     std::lock_guard<std::mutex> lock(output_mutex);
    // std::cout << "thread_id: " << std::this_thread::get_id() << " bush_id: " << bush_id <<   " concurrent_version before: " << (int)concurrent_version << std::endl;
    // }

    uint16_t& control_info =
        *reinterpret_cast<uint16_t*>(bush + kControlOffset);
    uint16_t control_info_before_item = control_info >> in_bush_offset;

    uint8_t item_cnt = uint8_t(_popcnt32(control_info));
    uint8_t before_item_cnt = uint8_t(_popcnt32(control_info_before_item));

    // uint8_t before_item_cnt =
    //     kBushLookup[control_info_before_item & kByteMask] +
    //     kBushLookup[(control_info_before_item >> kByteShift)];

    // uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
    //                    kBushLookup[(control_info >> kByteShift)];

    if ((control_info >> in_bush_offset) & 1) {

        uint8_t overload_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);
        uint8_t* pre_tiny_ptr =
            bush + (before_item_cnt > kInitExhibitorNum - overload_flag
                        ? kSkulkerOffset - (before_item_cnt - 1 -
                                            (kInitExhibitorNum - overload_flag))
                        : (before_item_cnt - 1) * kEntryByteLength);

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

            // if (exhibitor_num - before_item_cnt < 5) {
            //     uint64_t* src = reinterpret_cast<uint64_t*>(
            //         bush + before_item_cnt * kEntryByteLength);
            //     src--;
            //     uint64_t* src_ind = reinterpret_cast<uint64_t*>(
            //         bush + before_item_cnt * kEntryByteLength +
            //         kEntryByteLength * (exhibitor_num - before_item_cnt - 1));
            //     src_ind--;
            //     uint64_t* dst_ind = reinterpret_cast<uint64_t*>(
            //         bush + (before_item_cnt + 1) * kEntryByteLength +
            //         kEntryByteLength * (exhibitor_num - before_item_cnt - 1));
            //     dst_ind--;
            //     while (src < src_ind) {
            //         *dst_ind = *src_ind;
            //         dst_ind--;
            //         src_ind--;
            //     }
            // }

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
#ifdef USE_LOCK_BASED_VERSION_QUERY
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                  kFastDivisionShift[0];
    }

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &bush_tab[(bush_id << kBushIdShiftOffset) +
                      kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while (expected_version % 2 != 0 ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

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
    // 2-3 ns
    // Current version without locking
    uint64_t base_id = hash_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                  kFastDivisionShift[0];
    }

    // 1 ns
    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];
    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    // 31 ns

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &bush[kConcurrentVersionOffset]);

query_again:

    // 3-4 ns
    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1));

    // 3 ns

    uint16_t& control_info =
        *reinterpret_cast<uint16_t*>(bush + kControlOffset);
    uint16_t control_info_before_item = control_info >> in_bush_offset;

    uint8_t item_cnt = uint8_t(_popcnt32(control_info));
    uint8_t before_item_cnt = uint8_t(_popcnt32(control_info_before_item));

    // uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
    //                    kBushLookup[(control_info >> kByteShift)];
    // uint8_t before_item_cnt =
    //     kBushLookup[control_info_before_item & kByteMask] +
    //     kBushLookup[(control_info_before_item >> kByteShift)];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    // *value_ptr = concurrent_version;
    // return true;

    static uint64_t hit_num = 0;

    if (control_info_before_item & 1) {

        key = key & (~kQuotientingTailMask);

        uint8_t* pre_tiny_ptr;
        uintptr_t pre_deref_key = base_id;

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
            pre_tiny_ptr = bush + (before_item_cnt - 1) * kEntryByteLength;
        } else {
            pre_tiny_ptr =
                bush + kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num);
        }

        // uint8_t* pre_tiny_ptr =
        //     bush + (before_item_cnt > exhibitor_num
        //                 ? kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num)
        //                 : (before_item_cnt - 1) * kEntryByteLength);

        // uintptr_t pre_deref_key = base_id;

        // *value_ptr = pre_deref_key;
        // return false;

        while (*pre_tiny_ptr != 0) {

            // hit_num++;
            // if (hit_num % 100000 == 0) {
            //     std::cout << "hit_num: " << hit_num << std::endl;
            // }
            // *value_ptr = *pre_tiny_ptr;
            // return false;

            // 5-10 ns
            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);

            uint64_t entry_key =
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)
                 << kQuotientingTailLength);
            uint64_t entry_value =
                *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            // *value_ptr = entry_key + entry_value;
            // return false;

            // if (0) {
            if (entry_key == key) {
                // uint64_t entry_value =
                //     *reinterpret_cast<uint64_t*>(entry + kValueOffset);
                *value_ptr = entry_value;

                if (concurrent_version.load() != expected_version) {
                    goto query_again;
                }
                return true;
            }
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);

            // *value_ptr = reinterpret_cast<uint64_t>(pre_tiny_ptr);
            // *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            // return false;
            // if (concurrent_version.load() != expected_version) {
            //     goto query_again;
            // }
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
        bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                  kFastDivisionShift[0];
    }
    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &bush[kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    uint16_t& control_info =
        *reinterpret_cast<uint16_t*>(bush + kControlOffset);
    uint16_t control_info_before_item = control_info >> in_bush_offset;

    uint8_t item_cnt = uint8_t(_popcnt32(control_info));
    uint8_t before_item_cnt = uint8_t(_popcnt32(control_info_before_item));

    // uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
    //                    kBushLookup[(control_info >> kByteShift)];
    // uint8_t before_item_cnt =
    //     kBushLookup[control_info_before_item & kByteMask] +
    //     kBushLookup[(control_info_before_item >> kByteShift)];

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
            bush + (before_item_cnt > exhibitor_num
                        ? kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num)
                        : (before_item_cnt - 1) * kEntryByteLength);

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
        bush_id = (1ULL * base_id * kFastDivisionReciprocal[0]) >>
                  kFastDivisionShift[0];
    }
    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &bush[kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    uint16_t& control_info =
        *reinterpret_cast<uint16_t*>(bush + kControlOffset);
    uint16_t control_info_before_item = control_info >> in_bush_offset;

    uint8_t item_cnt = uint8_t(_popcnt32(control_info));
    uint8_t before_item_cnt = uint8_t(_popcnt32(control_info_before_item));

    // uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
    //                    kBushLookup[(control_info >> kByteShift)];
    // uint8_t before_item_cnt =
    //     kBushLookup[control_info_before_item & kByteMask] +
    //     kBushLookup[(control_info_before_item >> kByteShift)];

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

                    // if (before_item_cnt < 5) {
                    //     uint64_t* src_end = reinterpret_cast<uint64_t*>(
                    //         bush + before_item_cnt * kEntryByteLength +
                    //         kEntryByteLength *
                    //             (exhibitor_num - before_item_cnt));
                    //     uint64_t* src_ind = reinterpret_cast<uint64_t*>(
                    //         bush + before_item_cnt * kEntryByteLength);
                    //     uint64_t* dst_ind = reinterpret_cast<uint64_t*>(
                    //         bush + (before_item_cnt - 1) * kEntryByteLength);
                    //     while (src_ind < src_end) {
                    //         *dst_ind = *src_ind;
                    //         dst_ind++;
                    //         src_ind++;
                    //     }
                    // }

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
            bush + (before_item_cnt > exhibitor_num
                        ? kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num)
                        : (before_item_cnt - 1) * kEntryByteLength);

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

void ConcurrentSkulkerHT::SetResizeStride(uint64_t stride_num) {
    resize_stride_size = ceil(1.0 * kBushNum / (stride_num));
}

/*
bool ConcurrentSkulkerHT::ResizeMoveStride(uint64_t stride_id,
                                           ConcurrentSkulkerHT* new_ht) {
    uint64_t stride_id_start = stride_id * resize_stride_size;
    uint64_t stride_id_end = stride_id_start + resize_stride_size;
    if (stride_id_end > kBushNum) {
        stride_id_end = kBushNum;
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    for (uint64_t bush_id = stride_id_start; bush_id < stride_id_end;
         bush_id++) {
        uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];
        uint16_t& control_info =
            *reinterpret_cast<uint16_t*>(bush + kControlOffset);
        uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                           kBushLookup[(control_info >> kByteShift)];
        uint8_t overflow_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

        uint8_t exhibitor_num = kInitExhibitorNum - overflow_flag;

        for (uint8_t in_bush_offset = 0, moved_cnt = 0;
             in_bush_offset < kBushCapacity; in_bush_offset++) {
            if ((control_info >> in_bush_offset) & 1) {

                if (item_cnt - moved_cnt <= exhibitor_num) {
                    uint64_t base_id = bush_id * kBushCapacity + in_bush_offset;

                    uint8_t* entry =
                        bush + (item_cnt - moved_cnt - 1) * kEntryByteLength;

                    if (!new_ht->Insert(
                            hash_key_rebuild((*reinterpret_cast<uint64_t*>(
                                                 entry + kKeyOffset)),
                                             base_id),
                            *reinterpret_cast<uint64_t*>(entry +
                                                         kValueOffset))) {
                        return false;
                    }

                    uintptr_t pre_deref_key = base_id;
                    uint8_t* pre_tiny_ptr = entry + kTinyPtrOffset;

                    while (*pre_tiny_ptr != 0) {
                        uint8_t* entry = ptab_query_entry_address(
                            pre_deref_key, *pre_tiny_ptr);

                        if (!new_ht->Insert(
                                hash_key_rebuild((*reinterpret_cast<uint64_t*>(
                                                     entry + kKeyOffset)),
                                                 base_id),
                                *reinterpret_cast<uint64_t*>(entry +
                                                             kValueOffset))) {
                            return false;
                        }
                        pre_tiny_ptr = entry + kTinyPtrOffset;
                        pre_deref_key = reinterpret_cast<uintptr_t>(entry);
                    }
                } else {
                    uint64_t base_id = bush_id * kBushCapacity + in_bush_offset;
                    uintptr_t pre_deref_key = base_id;
                    uint8_t* pre_tiny_ptr =
                        bush + kSkulkerOffset -
                        (item_cnt - exhibitor_num - moved_cnt - 1);

                    while (*pre_tiny_ptr != 0) {
                        uint8_t* entry = ptab_query_entry_address(
                            pre_deref_key, *pre_tiny_ptr);

                        if (!new_ht->Insert(
                                hash_key_rebuild((*reinterpret_cast<uint64_t*>(
                                                     entry + kKeyOffset)),
                                                 base_id),
                                *reinterpret_cast<uint64_t*>(entry +
                                                             kValueOffset))) {
                            return false;
                        }
                        pre_tiny_ptr = entry + kTinyPtrOffset;
                        pre_deref_key = reinterpret_cast<uintptr_t>(entry);
                    }
                }
                moved_cnt++;
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();

    static auto resize_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              end_time)
            .count();

    resize_time += std::chrono::duration_cast<std::chrono::milliseconds>(
                       end_time - start_time)
                       .count();

    std::cerr << "Resize time: " << resize_time << "ms" << std::endl;

    return true;
}
*/

bool ConcurrentSkulkerHT::ResizeMoveStride(uint64_t stride_id,
                                           ConcurrentSkulkerHT* new_ht) {
    // auto start_time = std::chrono::high_resolution_clock::now();

    uint64_t stride_id_start = stride_id * resize_stride_size;
    uint64_t stride_id_end = stride_id_start + resize_stride_size;
    if (stride_id_end > kBushNum) {
        stride_id_end = kBushNum;
    }

    std::queue<std::pair<uint64_t, uint64_t>> ins_queue;

    for (uint64_t bush_id = stride_id_start; bush_id < stride_id_end;
         bush_id++) {
        uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];
        uint16_t& control_info =
            *reinterpret_cast<uint16_t*>(bush + kControlOffset);
        uint8_t item_cnt = kBushLookup[control_info & kByteMask] +
                           kBushLookup[(control_info >> kByteShift)];
        uint8_t overflow_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

        uint8_t exhibitor_num = kInitExhibitorNum - overflow_flag;

        for (uint8_t in_bush_offset = 0, moved_cnt = 0;
             in_bush_offset < kBushCapacity; in_bush_offset++) {
            if ((control_info >> in_bush_offset) & 1) {

                if (item_cnt - moved_cnt <= exhibitor_num) {
                    uint64_t base_id = bush_id * kBushCapacity + in_bush_offset;

                    uint8_t* entry =
                        bush + (item_cnt - moved_cnt - 1) * kEntryByteLength;

                    uint64_t ins_key = hash_key_rebuild(
                        (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)),
                        base_id);
                    ins_queue.push(std::make_pair(
                        ins_key,
                        *reinterpret_cast<uint64_t*>(entry + kValueOffset)));
                    new_ht->prefetch_key(ins_key);

                    uintptr_t pre_deref_key = base_id;
                    uint8_t* pre_tiny_ptr = entry + kTinyPtrOffset;

                    while (*pre_tiny_ptr != 0) {
                        uint8_t* entry = ptab_query_entry_address(
                            pre_deref_key, *pre_tiny_ptr);

                        uint64_t ins_key = hash_key_rebuild(
                            (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)),
                            base_id);
                        ins_queue.push(std::make_pair(
                            ins_key, *reinterpret_cast<uint64_t*>(
                                         entry + kValueOffset)));
                        new_ht->prefetch_key(ins_key);

                        pre_tiny_ptr = entry + kTinyPtrOffset;
                        pre_deref_key = reinterpret_cast<uintptr_t>(entry);
                    }
                } else {
                    uint64_t base_id = bush_id * kBushCapacity + in_bush_offset;
                    uintptr_t pre_deref_key = base_id;
                    uint8_t* pre_tiny_ptr =
                        bush + kSkulkerOffset -
                        (item_cnt - exhibitor_num - moved_cnt - 1);

                    while (*pre_tiny_ptr != 0) {
                        uint8_t* entry = ptab_query_entry_address(
                            pre_deref_key, *pre_tiny_ptr);

                        uint64_t ins_key = hash_key_rebuild(
                            (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)),
                            base_id);
                        ins_queue.push(std::make_pair(
                            ins_key, *reinterpret_cast<uint64_t*>(
                                         entry + kValueOffset)));
                        new_ht->prefetch_key(ins_key);

                        pre_tiny_ptr = entry + kTinyPtrOffset;
                        pre_deref_key = reinterpret_cast<uintptr_t>(entry);
                    }
                }
                moved_cnt++;
            }
        }

        if (ins_queue.size() >= 10) {
            while (!ins_queue.empty()) {
                auto ins_pair = ins_queue.front();
                ins_queue.pop();
                if (!new_ht->Insert(ins_pair.first, ins_pair.second)) {
                    return false;
                }
            }
        }
    }

    while (!ins_queue.empty()) {
        auto ins_pair = ins_queue.front();
        ins_queue.pop();
        if (!new_ht->Insert(ins_pair.first, ins_pair.second)) {
            return false;
        }
    }

    // // asfasdfasdfasfasdfa
    //     auto end_time = std::chrono::high_resolution_clock::now();

    //     static auto resize_time =
    //         std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
    //                                                               end_time)
    //             .count();

    //     resize_time += std::chrono::duration_cast<std::chrono::milliseconds>(
    //                        end_time - start_time)
    //                        .count();

    //     std::cerr << "Resize time: " << resize_time << "ms" << std::endl;

    return true;
}

}  // namespace tinyptr
