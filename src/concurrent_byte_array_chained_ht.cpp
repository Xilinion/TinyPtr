#include "concurrent_byte_array_chained_ht.h"
#include <emmintrin.h>
#include <inttypes.h>
#include <sys/cdefs.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <regex>

namespace tinyptr {

uint8_t ConcurrentByteArrayChainedHT::AutoQuotTailLength(uint64_t size) {
    uint8_t res = 16;
    size >>= 16;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

ConcurrentByteArrayChainedHT::ConcurrentByteArrayChainedHT(
    uint64_t size, uint8_t quotienting_tail_length, uint16_t bin_size,
    bool if_resize)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kQuotientingTailLength(quotienting_tail_length
                                 ? quotienting_tail_length
                                 : AutoQuotTailLength(size)),
      kQuotientingTailMask((1ULL << kQuotientingTailLength) - 1),
      kBaseTabSize(1ULL << kQuotientingTailLength),
      kBinSize(bin_size),
      kBinNum((size + kBinSize - 1) / kBinSize),
      kTinyPtrOffset((64 + 7 - kQuotientingTailLength) >> 3),
      kValueOffset(kTinyPtrOffset + 1),
      kQuotKeyByteLength(kTinyPtrOffset),
      kEntryByteLength(kQuotKeyByteLength + 1 + 8),
      kBinByteLength(kBinSize * kEntryByteLength) {

    // Determine the number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads =
            4;  // Fallback to a default value if hardware_concurrency is not available
    }

    bin_locks_size = kBinNum;
    // currently use the sqaure of number of threads, can modify later
    base_tab_concurrent_version_size = num_threads * num_threads * num_threads;

    bin_locks = std::make_unique<std::atomic_flag[]>(bin_locks_size);
    base_tab_concurrent_version =
        std::make_unique<std::atomic_flag[]>(base_tab_concurrent_version_size);

    // Initialize all atomic flags to clear
    for (size_t i = 0; i < bin_locks_size; ++i) {
        bin_locks[i].clear();
    }
    for (size_t i = 0; i < base_tab_concurrent_version_size; ++i) {
        base_tab_concurrent_version[i].clear();
    }

    uint64_t base_tab_size = kBaseTabSize;
    uint64_t byte_array_size = kBinNum * kBinSize * kEntryByteLength;
    uint64_t bin_cnt_size = kBinNum << 1;

    // Align each section to 64 bytes
    uint64_t base_tab_size_aligned =
        (base_tab_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t byte_array_size_aligned =
        (byte_array_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t bin_cnt_size_aligned =
        (bin_cnt_size + 63) & ~static_cast<uint64_t>(63);

    // Total size for combined allocation
    uint64_t total_size =
        base_tab_size_aligned + byte_array_size_aligned + bin_cnt_size_aligned;

    // Allocate a single aligned block
    void* combined_mem;
    if (if_resize) {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    } else {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
    }

    // Assign pointers to their respective regions
    uint8_t* base =
        (uint8_t*)((uint64_t)(combined_mem + 63) & ~static_cast<uint64_t>(63));

    byte_array = base;
    base += byte_array_size_aligned;

    bin_cnt_head = base;
    base += bin_cnt_size_aligned;

    base_tab = base;

    /*
    {
        size_t total_bytes = kBinNum * kBinSize * kEntryByteLength;
        byte_array = static_cast<uint8_t*>(calloc(1, total_bytes));
        if (!byte_array) {
            perror("calloc(byte_array)");
            exit(EXIT_FAILURE);
        }
    }

    {
        base_tab = static_cast<uint8_t*>(calloc(1, kBaseTabSize));
        if (!base_tab) {
            perror("calloc(base_tab)");
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

    play_entry = new uint8_t[kEntryByteLength];
}

ConcurrentByteArrayChainedHT::ConcurrentByteArrayChainedHT(uint64_t size,
                                                           uint16_t bin_size,
                                                           bool if_resize)
    : ConcurrentByteArrayChainedHT(size, 0, bin_size, if_resize) {}

ConcurrentByteArrayChainedHT::ConcurrentByteArrayChainedHT(uint64_t size,
                                                           bool if_resize)
    : ConcurrentByteArrayChainedHT(size, 0, 127, if_resize) {}

ConcurrentByteArrayChainedHT::~ConcurrentByteArrayChainedHT() {
    munmap(byte_array, kBinNum * kBinSize * kEntryByteLength);
    munmap(base_tab, kBaseTabSize);
    munmap(bin_cnt_head, kBinNum << 1);
}

uint64_t ConcurrentByteArrayChainedHT::limited_base_id(uint64_t key) {
    if (limited_base_cnt < limited_base_entry_num) {
        return limited_base_cnt++;
    } else {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) %
               limited_base_entry_num;
    }
}

void ConcurrentByteArrayChainedHT::random_base_entry_prefetch() {
    static uint64_t prefetch_cnt = 0;
    for (int i = 0; i < 5; i++) {
        prefetch_cnt++;
        uint64_t base_id = hash_1(prefetch_cnt) % kBaseTabSize;
        __builtin_prefetch(base_tab + base_id, 0, 3);
    }
}

uint8_t* ConcurrentByteArrayChainedHT::non_temporal_load_single_entry(
    uint8_t* entry) {
    uintptr_t entry_intptr = (uintptr_t)entry;
#if defined(__SSE2__)
    __m128i* entry_ptr = reinterpret_cast<__m128i*>(entry_intptr);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(non_temporal_load_entry_buffer),
                     _mm_loadu_si128(entry_ptr));
    return non_temporal_load_entry_buffer;
    // __m128i* entry_ptr_first_align =
    //     reinterpret_cast<__m128i*>(entry_intptr & kPtr16BAlignMask);
    // __m128i val1 = _mm_stream_load_si128(entry_ptr_first_align);
    // _mm_storeu_si128(reinterpret_cast<__m128i*>(non_temporal_load_entry_buffer),
    //                  val1);
    // __m128i val2 = _mm_stream_load_si128(entry_ptr_first_align + 1);
    // _mm_storeu_si128(reinterpret_cast<__m128i*>(non_temporal_load_entry_buffer +
    //                                             kPtr16BBufferSecondLoadOffset),
    //                  val2);
    // return non_temporal_load_entry_buffer +
    //        (entry_intptr & kPtr16BBufferOffsetMask);
#else
    return entry;
#endif
}

void ConcurrentByteArrayChainedHT::evict_entry_cache_line(uint8_t* entry) {

    uintptr_t entry_intptr = (uintptr_t)entry;
    uintptr_t start_intptr = entry_intptr & kPtrCacheLineAlignMask;

    if ((entry_intptr & kPtrCacheLineOffsetMask) + kEntryByteLength >
        utils::kCacheLineSize) {
        _mm_clflushopt(
            reinterpret_cast<void*>(start_intptr + utils::kCacheLineSize));
    }
    _mm_clflushopt(reinterpret_cast<void*>(start_intptr));
}

bool ConcurrentByteArrayChainedHT::Insert(uint64_t key, uint64_t value) {

    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &base_tab_concurrent_version[base_id_to_version_id(base_id)]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    uint8_t* entry =
        ptab_insert_entry_address(reinterpret_cast<uint64_t>(pre_tiny_ptr));

    if (entry != nullptr) {
        *pre_tiny_ptr = *entry;
        // assuming little endian
        *reinterpret_cast<uint64_t*>(entry) = key >> kQuotientingTailLength;
        entry[kTinyPtrOffset] = 0;
        *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
        // Release the lock
        concurrent_version.fetch_add(1);
        return true;
    } else {
        // Release the lock
        concurrent_version.fetch_add(1);
        return false;
    }
}

bool ConcurrentByteArrayChainedHT::Query(uint64_t key, uint64_t* value_ptr) {
#ifdef USE_CONCURRENT_VERSION_QUERY
    uint64_t base_id = hash_1_base_id(key);

    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &base_tab_concurrent_version[base_id]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {

        // query_entry_cnt++;

        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            // evict_entry_cache_line(entry);
            concurrent_version.fetch_add(1);
            return true;
        }
        // evict_entry_cache_line(entry);
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    concurrent_version.fetch_add(1);
    return false;
#else
    uint64_t base_id = hash_1_base_id(key);

    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &base_tab_concurrent_version[base_id_to_version_id(base_id)]);

query_again:

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while (expected_version & 1);

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {

        // query_entry_cnt++;

        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            // evict_entry_cache_line(entry);
            if (concurrent_version.load() != expected_version) {
                goto query_again;
            }
            return true;
        }
        // evict_entry_cache_line(entry);
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    /*
    uint8_t pre_tiny_ptr_value = *pre_tiny_ptr;
    while (pre_tiny_ptr_value != 0) {
        uint64_t pre_tiny_ptr_int = reinterpret_cast<uint64_t>(pre_tiny_ptr);
        uint8_t* entry =
            ptab_query_entry_address(pre_tiny_ptr_int, pre_tiny_ptr_value);

        uint8_t* non_temporal_entry = non_temporal_load_single_entry(entry);
        if ((*reinterpret_cast<uint64_t*>(non_temporal_entry)
             << kQuotientingTailLength) == key) {
            *value_ptr =
                *reinterpret_cast<uint64_t*>(non_temporal_entry + kValueOffset);
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
        pre_tiny_ptr_value = non_temporal_entry[kTinyPtrOffset];
    }
    */
    if (concurrent_version.load() != expected_version) {
        goto query_again;
    }
    return false;
#endif
}

void ConcurrentByteArrayChainedHT::set_chain_length(uint64_t chain_length) {
    this->chain_length = chain_length;
}

bool ConcurrentByteArrayChainedHT::QueryNoMem(uint64_t key,
                                              uint64_t* value_ptr) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        // entry=play_entry;
        // if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) == key) {
        //     *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
        //     return true;
        // }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

bool ConcurrentByteArrayChainedHT::Update(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &base_tab_concurrent_version[base_id_to_version_id(base_id)]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    // quotienting
    key >>= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            concurrent_version.fetch_add(1);
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    concurrent_version.fetch_add(1);
    return false;
}

void ConcurrentByteArrayChainedHT::Free(uint64_t key) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* cur_tiny_ptr = nullptr;

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &base_tab_concurrent_version[base_id_to_version_id(base_id)]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    // quotienting
    key >>= kQuotientingTailLength;

    uint8_t* cur_entry = nullptr;
    uint8_t* aiming_entry = nullptr;

    if (*pre_tiny_ptr != 0) {
        cur_entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(cur_entry)
              << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            aiming_entry = cur_entry;
        }
        cur_tiny_ptr = cur_entry + kTinyPtrOffset;
    } else {
        concurrent_version.fetch_add(1);
        return;
    }

    while (*cur_tiny_ptr != 0) {
        pre_tiny_ptr = cur_tiny_ptr;

        cur_entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(cur_tiny_ptr), *cur_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(cur_entry)
              << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            aiming_entry = cur_entry;
        }
        cur_tiny_ptr = cur_entry + kTinyPtrOffset;
    }

    if (aiming_entry == nullptr) {
        concurrent_version.fetch_add(1);
        return;
    }

    uint8_t tmp = aiming_entry[kTinyPtrOffset];
    memcpy(aiming_entry, cur_entry, kEntryByteLength);
    aiming_entry[kTinyPtrOffset] = tmp;

    uint64_t bin_id = (cur_entry - byte_array) / kBinByteLength;
    bin_cnt(bin_id)--;
    uint8_t& head = bin_head(bin_id);
    uint8_t cur_in_bin_pos = ((uint8_t)((*pre_tiny_ptr) << 1) >> 1) - 1;
    cur_entry[kTinyPtrOffset] = head + kBinSize - cur_in_bin_pos;
    if (cur_entry[kTinyPtrOffset] > kBinSize) {
        cur_entry[kTinyPtrOffset] -= (kBinSize + 1);
    }
    head = cur_in_bin_pos;
    *pre_tiny_ptr = 0;
    concurrent_version.fetch_add(1);
}

double ConcurrentByteArrayChainedHT::AvgChainLength() {
    double sum = 0;
    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        while (*pre_tiny_ptr != 0) {
            sum++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
    }

    return sum / kBaseTabSize;
}

uint32_t ConcurrentByteArrayChainedHT::MaxChainLength() {
    uint32_t max = 0;
    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        uint32_t cnt = 0;
        while (*pre_tiny_ptr != 0) {
            cnt++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
        max = cnt > max ? cnt : max;
    }

    return max;
}

uint64_t* ConcurrentByteArrayChainedHT::ChainLengthHistogram() {
    uint64_t max_chain_length =
        std::max(static_cast<uint64_t>(kBaseTabSize), uint64_t(1000));
    uint64_t* res = new uint64_t[max_chain_length];
    memset(res, 0, max_chain_length * sizeof(uint64_t));

    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        uint32_t cnt = 0;
        while (*pre_tiny_ptr != 0) {
            cnt++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
        res[cnt]++;
    }

    return res;
}

void ConcurrentByteArrayChainedHT::FillChainLength(uint8_t chain_length) {
    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

        uint8_t tmp = chain_length;
        while (tmp--) {
            uint8_t* entry = ptab_insert_entry_address(
                reinterpret_cast<uint64_t>(pre_tiny_ptr));
            if (entry != nullptr) {
                *pre_tiny_ptr = *entry;
                entry[kTinyPtrOffset] = 0;
                pre_tiny_ptr = entry + kTinyPtrOffset;
            }
        }
    }
}

uint64_t ConcurrentByteArrayChainedHT::QueryEntryCnt() {
    return query_entry_cnt;
}

void ConcurrentByteArrayChainedHT::SetResizeStride(uint64_t stride_num) {
    resize_stride_size = ceil(1.0 * kBaseTabSize / (stride_num));
}

bool ConcurrentByteArrayChainedHT::ResizeMoveStride(
    uint64_t stride_id, ConcurrentByteArrayChainedHT* new_ht) {

    uint64_t start_base_id = stride_id * resize_stride_size;
    uint64_t end_base_id = start_base_id + resize_stride_size;
    if (end_base_id > kBaseTabSize) {
        end_base_id = kBaseTabSize;
    }
    for (uint64_t base_id = start_base_id; base_id < end_base_id; base_id++) {

        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        while (*pre_tiny_ptr != 0) {

            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);

            if (!new_ht->Insert(
                    hash_key_rebuild(*reinterpret_cast<uint64_t*>(entry),
                                     base_id),
                    *reinterpret_cast<uint64_t*>(entry + kValueOffset))) {
                return false;
            }

            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
    }
    return true;
}

}  // namespace tinyptr
