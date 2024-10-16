#include "same_bin_chained_ht.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>

namespace tinyptr {

SameBinChainedHT::SameBinChainedHT(uint64_t size, uint16_t bin_size)
    : ByteArrayChainedHT(size, bin_size) {}

uint8_t* SameBinChainedHT::ptab_insert_entry_address(uint64_t key,
                                                     uint8_t pre_tiny_ptr) {
    uint64_t bin1 = hash_1_bin(key);
    uint64_t bin2 = hash_2_bin(key);
    uint8_t flag =
        pre_tiny_ptr ? (pre_tiny_ptr >> 7) : (bin_cnt(bin1) > bin_cnt(bin2));
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

uint8_t* SameBinChainedHT::ptab_query_entry_address(uint64_t key, uint8_t ptr) {
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

bool SameBinChainedHT::Insert(uint64_t key, uint64_t value) {

    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* base_tiny_ptr = pre_tiny_ptr;
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    uint8_t* entry = ptab_insert_entry_address(
        reinterpret_cast<uint64_t>(base_intptr), *base_tiny_ptr);

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

bool SameBinChainedHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

bool SameBinChainedHT::Update(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);
    // quotienting
    key >>= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

void SameBinChainedHT::Free(uint64_t key) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);
    uint8_t* cur_tiny_ptr = nullptr;

    // quotienting
    key >>= kQuotientingTailLength;

    uint8_t* cur_entry = nullptr;
    uint8_t* aiming_entry = nullptr;

    if (*pre_tiny_ptr != 0) {
        cur_entry = ptab_query_entry_address(
            reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
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
            reinterpret_cast<uint64_t>(base_intptr), *cur_tiny_ptr);
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

double SameBinChainedHT::AvgChainLength() {
    double sum = 0;
    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);
        while (*pre_tiny_ptr != 0) {
            sum++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
    }

    return sum / kBaseTabSize;
}

uint32_t SameBinChainedHT::MaxChainLength() {
    uint32_t max = 0;
    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);
        uint32_t cnt = 0;
        while (*pre_tiny_ptr != 0) {
            cnt++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
        max = cnt > max ? cnt : max;
    }

    return max;
}

uint64_t* SameBinChainedHT::ChainLengthHistogram() {
    uint64_t max_chain_length =
        std::max(static_cast<uint64_t>(kBaseTabSize), uint64_t(1000));
    uint64_t* res = new uint64_t[max_chain_length];
    memset(res, 0, max_chain_length * sizeof(uint64_t));

    for (int base_id = 0; base_id < kBaseTabSize; base_id++) {
        uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
        uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);
        uint32_t cnt = 0;

        uint8_t flag = (*pre_tiny_ptr) >> 7;
        while (*pre_tiny_ptr != 0) {
            assert(flag == (*pre_tiny_ptr >> 7));
            cnt++;
            uint8_t* entry = ptab_query_entry_address(
                reinterpret_cast<uint64_t>(base_intptr), *pre_tiny_ptr);
            pre_tiny_ptr = entry + kTinyPtrOffset;
        }
        res[cnt]++;
    }

    return res;
}

}  // namespace tinyptr
