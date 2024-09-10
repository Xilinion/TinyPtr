#include "byte_array_chained_ht.h"
#include <sys/types.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <regex>

namespace tinyptr {
ByteArrayChainedHT::ByteArrayChainedHT(uint64_t size,
                                       uint8_t quotiented_tail_length,
                                       uint16_t bin_size)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kQuotientedTailLength(quotiented_tail_length),
      kQuotientedTailMask((1ll << quotiented_tail_length) - 1),
      kBaseTabSize(1 << quotiented_tail_length),
      kBinSize(bin_size),
      kBinNum((size + bin_size) / bin_size),
      kTinyPtrOffset((64 + 7 - quotiented_tail_length) >> 3),
      kValueOffset(kTinyPtrOffset + 1),
      kQuotKeyByteLength((64 + 7 - quotiented_tail_length) >> 3),
      kEntryByteLength(kQuotKeyByteLength + 1 + 8),
      kBinByteLength(bin_size * kEntryByteLength) {
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

uint64_t ByteArrayChainedHT::hash_1(uint64_t key) {
    uint64_t tmp;
    XXHash64::hash(&tmp, sizeof(uint64_t), kHashSeed1);
    return tmp;
}

uint64_t ByteArrayChainedHT::hash_1_base_id(uint64_t key) {
    uint64_t tmp;
    XXHash64::hash(&tmp, sizeof(uint64_t), kHashSeed1);
    return (tmp ^ key) & kQuotientedTailMask;
}

uint64_t ByteArrayChainedHT::hash_1_bin(uint64_t key) {
    uint64_t tmp;
    XXHash64::hash(&tmp, sizeof(uint64_t), kHashSeed1);
    return tmp % kBinNum;
}

uint64_t ByteArrayChainedHT::hash_2(uint64_t key) {
    uint64_t tmp;
    XXHash64::hash(&tmp, sizeof(uint64_t), kHashSeed2);
    return tmp;
}

uint64_t ByteArrayChainedHT::hash_2_bin(uint64_t key) {
    uint64_t tmp;
    XXHash64::hash(&tmp, sizeof(uint64_t), kHashSeed2);
    return tmp % kBinNum;
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
    ptr ^= flag * ((1 << 8) - 1);
    if (flag) {
        return byte_array +
               (hash_2_bin(key) * kBinSize + ptr - 1) * kEntryByteLength;
    } else {
        return byte_array +
               (hash_1_bin(key) * kBinSize + ptr - 1) * kEntryByteLength;
    }
}

uint8_t* ByteArrayChainedHT::ptab_insert_entry_address(uint64_t key) {
    uint8_t bin1 = hash_1_bin(key);
    uint8_t bin2 = hash_2_bin(key);
    uint8_t bin_id = bin_cnt(bin1) < bin_cnt(bin2) ? bin1 : bin2;

    uint8_t& head = bin_head(bin_id);
    uint8_t& cnt = bin_cnt(bin_id);

    if (head) {
        uint8_t* entry =
            byte_array + (bin_id * kBinSize + head - 1) * kEntryByteLength;
        *entry = head;
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

    if (entry) {
        *pre_tiny_ptr = *entry;
        // assuming little endian
        *reinterpret_cast<uint64_t*>(entry) = key >> kQuotientedTailLength;
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
    key >>= kQuotientedTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientedTailLength) >>
             kQuotientedTailLength) == key) {
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
    key >>= kQuotientedTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientedTailLength) >>
             kQuotientedTailLength) == key) {
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
    key >>= kQuotientedTailLength;

    uint8_t* cur_entry = nullptr;
    uint8_t* aiming_entry = nullptr;

    if (*pre_tiny_ptr != 0) {
        cur_entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(pre_tiny_ptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(cur_entry)
              << kQuotientedTailLength) >>
             kQuotientedTailLength) == key) {
            aiming_entry = cur_entry;
        }
        cur_tiny_ptr = cur_entry + kTinyPtrOffset;
    }
    else {
        return;
    }

    while (*cur_tiny_ptr != 0) {
        pre_tiny_ptr = cur_tiny_ptr;

        cur_entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(cur_tiny_ptr), *cur_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(cur_entry)
              << kQuotientedTailLength) >>
             kQuotientedTailLength) == key) {
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
    bin_cnt(bin_id)++;
    uint8_t& head = bin_head(bin_id);
    cur_entry[kTinyPtrOffset] = head;
    head = (((*pre_tiny_ptr) << 1) >> 1);
    *pre_tiny_ptr = 0;
}

}  // namespace tinyptr