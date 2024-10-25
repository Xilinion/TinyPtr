#include "byte_array_chained_ht.h"
#include <emmintrin.h>
#include <inttypes.h>
#include <sys/types.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <regex>

namespace tinyptr {

uint8_t ByteArrayChainedHT::AutoQuotTailLength(uint64_t size) {
    uint8_t res = 16;
    size >>= 16;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

ByteArrayChainedHT::ByteArrayChainedHT(uint64_t size,
                                       uint8_t quotienting_tail_length,
                                       uint16_t bin_size)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kQuotientingTailLength(quotienting_tail_length
                                 ? quotienting_tail_length
                                 : AutoQuotTailLength(size)),
      kQuotientingTailMask((1ll << kQuotientingTailLength) - 1),
      kBaseTabSize(1 << kQuotientingTailLength),
      kBinSize(bin_size),
      kBinNum((size + kBinSize - 1) / kBinSize),
      kTinyPtrOffset((64 + 7 - kQuotientingTailLength) >> 3),
      kValueOffset(kTinyPtrOffset + 1),
      kQuotKeyByteLength(kTinyPtrOffset),
      kEntryByteLength(kQuotKeyByteLength + 1 + 8),
      kBinByteLength(kBinSize * kEntryByteLength) {
    (void)posix_memalign(reinterpret_cast<void**>(&byte_array), 64,
                         kBinNum * kBinSize * kEntryByteLength);
    memset(byte_array, 0, kBinNum * kBinSize * kEntryByteLength);

    (void)posix_memalign(reinterpret_cast<void**>(&base_tab), 64, kBaseTabSize);
    memset(base_tab, 0, kBaseTabSize);

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

    play_entry = new uint8_t[kEntryByteLength];
}

ByteArrayChainedHT::ByteArrayChainedHT(uint64_t size, uint16_t bin_size)
    : ByteArrayChainedHT(size, 0, bin_size) {}

uint64_t ByteArrayChainedHT::hash_1(uint64_t key) {
    return XXH64(&key, sizeof(uint64_t), kHashSeed1);
}

uint64_t ByteArrayChainedHT::hash_1_base_id(uint64_t key) {
    uint64_t tmp = key >> kQuotientingTailLength;
    return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
           kQuotientingTailMask;
}

uint64_t ByteArrayChainedHT::limited_base_id(uint64_t key) {
    if (limited_base_cnt < limited_base_entry_num) {
        return limited_base_cnt++;
    } else {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) %
               limited_base_entry_num;
    }
}

uint64_t ByteArrayChainedHT::hash_1_bin(uint64_t key) {
    return (XXH64(&key, sizeof(uint64_t), kHashSeed1)) % kBinNum;
    // return 0;
}

uint64_t ByteArrayChainedHT::hash_2(uint64_t key) {
    return XXH64(&key, sizeof(uint64_t), kHashSeed2);
}

uint64_t ByteArrayChainedHT::hash_2_bin(uint64_t key) {
    return (XXH64(&key, sizeof(uint64_t), kHashSeed2)) % kBinNum;
    // return 0;
}

uint8_t& ByteArrayChainedHT::bin_cnt(uint64_t bin_id) {
    return bin_cnt_head[bin_id << 1];
}

uint8_t& ByteArrayChainedHT::bin_head(uint64_t bin_id) {
    return bin_cnt_head[(bin_id << 1) | 1];
}

uint8_t& ByteArrayChainedHT::base_tab_ptr(uint64_t base_id) {
    return base_tab[base_id];
}

void ByteArrayChainedHT::random_base_entry_prefetch() {
    static uint64_t prefetch_cnt = 0;
    for (int i = 0; i < 5; i++) {
        prefetch_cnt++;
        uint64_t base_id = hash_1(prefetch_cnt) % kBaseTabSize;
        __builtin_prefetch(base_tab + base_id, 0, 3);
    }
}

uint8_t* ByteArrayChainedHT::non_temporal_load_single_entry(uint8_t* entry) {
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

void ByteArrayChainedHT::evict_entry_cache_line(uint8_t* entry) {

    uintptr_t entry_intptr = (uintptr_t)entry;
    uintptr_t start_intptr = entry_intptr & kPtrCacheLineAlignMask;

    if ((entry_intptr & kPtrCacheLineOffsetMask) + kEntryByteLength >
        utils::kCacheLineSize) {
        _mm_clflushopt(
            reinterpret_cast<void*>(start_intptr + utils::kCacheLineSize));
    }
    _mm_clflushopt(reinterpret_cast<void*>(start_intptr));
}

uint8_t* ByteArrayChainedHT::ptab_query_entry_address(uint64_t key,
                                                      uint8_t ptr) {
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

uint8_t* ByteArrayChainedHT::ptab_insert_entry_address(uint64_t key) {
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

bool ByteArrayChainedHT::Insert(uint64_t key, uint64_t value) {

    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

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
        return true;
    } else {
        return false;
    }
}

bool ByteArrayChainedHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t base_id = hash_1_base_id(key);

    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {

        query_entry_cnt++;

        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            // evict_entry_cache_line(entry);
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
    return false;
}

void ByteArrayChainedHT::set_chain_length(uint64_t chain_length) {
    this->chain_length = chain_length;
}

bool ByteArrayChainedHT::QueryNoMem(uint64_t key, uint64_t* value_ptr) {
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

bool ByteArrayChainedHT::Update(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);

    // quotienting
    key >>= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

void ByteArrayChainedHT::Free(uint64_t key) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* cur_tiny_ptr = nullptr;

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
        return;
    }

    uint8_t tmp = aiming_entry[kTinyPtrOffset];
    memcpy(aiming_entry, cur_entry, kEntryByteLength);
    aiming_entry[kTinyPtrOffset] = tmp;

    uint64_t bin_id = (cur_entry - byte_array) / kBinByteLength;
    bin_cnt(bin_id)--;
    uint8_t& head = bin_head(bin_id);
    cur_entry[kTinyPtrOffset] = head;
    head = ((uint8_t)((*pre_tiny_ptr) << 1) >> 1);
    *pre_tiny_ptr = 0;
}

double ByteArrayChainedHT::AvgChainLength() {
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

uint32_t ByteArrayChainedHT::MaxChainLength() {
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

uint64_t* ByteArrayChainedHT::ChainLengthHistogram() {
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

void ByteArrayChainedHT::FillChainLength(uint8_t chain_length) {
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

uint64_t ByteArrayChainedHT::QueryEntryCnt() {
    return query_entry_cnt;
}

}  // namespace tinyptr
