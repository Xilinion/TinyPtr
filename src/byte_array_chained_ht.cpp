#include "byte_array_chained_ht.h"
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
      kBinNum((size + kBinSize) / kBinSize),
      kTinyPtrOffset((64 + 7 - kQuotientingTailLength) >> 3),
      kValueOffset(kTinyPtrOffset + 1),
      kQuotKeyByteLength(kTinyPtrOffset),
      kEntryByteLength(kQuotKeyByteLength + 1 + 8),
      kBinByteLength(kBinSize * kEntryByteLength) {
    byte_array = new uint8_t[kBinNum * kBinSize * kEntryByteLength];
    memset(byte_array, 0, kBinNum * kBinSize * kEntryByteLength);

    base_tab = new uint8_t[kBaseTabSize];
    memset(base_tab, 0, kBaseTabSize);

    bin_cnt_head = new uint8_t[kBinNum << 1];
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
}

ByteArrayChainedHT::ByteArrayChainedHT(uint64_t size, uint16_t bin_size)
    : ByteArrayChainedHT(size, 0, bin_size) {}

uint64_t ByteArrayChainedHT::hash_1(uint64_t key) {
    return XXH64(&key, sizeof(uint64_t), kHashSeed1);
}

uint64_t ByteArrayChainedHT::hash_1_base_id(uint64_t key) {
    return (XXH64(&key, sizeof(uint64_t), kHashSeed1) ^ key) &
           kQuotientingTailMask;
}

uint64_t ByteArrayChainedHT::hash_1_bin(uint64_t key) {
    return (XXH64(&key, sizeof(uint64_t), kHashSeed1)) % kBinNum;
}

uint64_t ByteArrayChainedHT::hash_2(uint64_t key) {
    return XXH64(&key, sizeof(uint64_t), kHashSeed2);
}

uint64_t ByteArrayChainedHT::hash_2_bin(uint64_t key) {
    return (XXH64(&key, sizeof(uint64_t), kHashSeed2)) % kBinNum;
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

    // quotienting
    key >>= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            return true;
        }
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
    head = (((*pre_tiny_ptr) << 1) >> 1);
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
    uint64_t* res = new uint64_t[kBaseTabSize];
    memset(res, 0, kBaseTabSize * sizeof(uint64_t));

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

}  // namespace tinyptr