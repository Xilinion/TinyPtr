#include "fp_tp_chained_ht.h"
#include <cstdint>
#include <cstring>

namespace tinyptr {

FPTPChainedHT::FPTPChainedHT(uint64_t size, uint16_t bin_size,
                             uint8_t double_slot_num)
    : ByteArrayChainedHT(size, bin_size), kDoubleSlotSize(double_slot_num) {
    head_double_slot = new uint8_t[kBinNum];
    memset(head_double_slot, 0, kBinNum);

    for (uint64_t i = 0, ptr_offset = kTinyPtrOffset; i < kBinNum;
         ptr_offset = kTinyPtrOffset + (++i) * kBinByteLength) {
        for (uint8_t j = 0; j < kDoubleSlotSize; j += 2) {
            byte_array[ptr_offset] = j + 3;
            ptr_offset += kEntryByteLength;
            // the second entry in the double slot points to nullptr
            // which eliminates the need of extra access when transfering to single slot
            // this requirement should hold for all double slots
            byte_array[ptr_offset] = 0;
            ptr_offset += kEntryByteLength;
        }
        // the last entry in the bin points to null
        byte_array[ptr_offset] = 0;
        ptr_offset += kEntryByteLength;
        byte_array[ptr_offset] = 0;

        head_double_slot[i] = 0;
        bin_cnt_head[(i << 1) | 1] = kDoubleSlotSize + 1;
    }
}

uint8_t& FPTPChainedHT::bin_head_double_slot(uint64_t bin_id) {
    return head_double_slot[bin_id];
}

uint8_t* FPTPChainedHT::ptab_insert_entry_address(uint64_t key,
                                                  uint8_t pre_tiny_ptr) {
    uint64_t bin1 = hash_1_bin(key);
    uint64_t bin2 = hash_2_bin(key);
    uint8_t flag =
        pre_tiny_ptr ? pre_tiny_ptr >> 7 : bin_cnt(bin1) > bin_cnt(bin2);
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

void FPTPChainedHT::bin_prefetch(uintptr_t key, uint8_t ptr) {
    uint8_t flag = (ptr >= (1 << 7));
    ptr = ptr & ((1 << 7) - 1);
    uint8_t* bin_front =
        byte_array + (flag ? hash_2_bin(key) : hash_1_bin(key)) * kBinSize *
                         kEntryByteLength;
    uint8_t* bin_back = bin_front + kBinSize * kEntryByteLength;
    for (uint8_t *p = bin_front, i = 0; i < 4; p += 64, i++) {
        _mm_prefetch(p, _MM_HINT_T0);
    }
}

bool FPTPChainedHT::Insert(uint64_t key, uint64_t value) {

    uint64_t base_id = hash_1_base_id(key);
    // leave the redundant variable for readibility
    uint8_t* base_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* pre_tiny_ptr = base_tiny_ptr;
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(base_tiny_ptr);

    uint8_t* aiming_entry = nullptr;

    if (0 == *base_tiny_ptr) {

        uint64_t bin1 = hash_1_bin(key);
        uint64_t bin2 = hash_2_bin(key);
        uint8_t flag = bin_cnt(bin1) > bin_cnt(bin2);
        uint64_t bin_id = flag ? bin2 : bin1;

        uint8_t& cnt = bin_cnt(bin_id);
        if (cnt == kBinSize) {
            return false;
        }

        uint8_t& head = bin_head(bin_id);
        if (head == 0) {
            uint8_t& head_double_slot = bin_head_double_slot(bin_id);
            head = head_double_slot + 1;

            aiming_entry =
                byte_array +
                (bin_id * kBinSize + head_double_slot - 1) * kEntryByteLength;
            // saving the tinyptr to itself at the beginning
            *pre_tiny_ptr = head_double_slot | (flag << 7);
            head_double_slot = aiming_entry[kTinyPtrOffset];
        } else {
            aiming_entry =
                byte_array + (bin_id * kBinSize + head - 1) * kEntryByteLength;
            // saving the tinyptr to itself at the beginning
            *pre_tiny_ptr = head | (flag << 7);
            head = aiming_entry[kTinyPtrOffset];
        }

        cnt++;

    } else {
        uint64_t bin1 = hash_1_bin(key);
        uint64_t bin2 = hash_2_bin(key);
        uint8_t flag = (*base_tiny_ptr) >> 7;
        uint64_t bin_id = flag ? bin2 : bin1;

        uint8_t& cnt = bin_cnt(bin_id);
        if (cnt == kBinSize) {
            return false;
        }

        uint8_t* bin_begin = byte_array + bin_id * kBinSize * kEntryByteLength;

        uint8_t odd_last = 0;
        uint8_t* pre_pre_tiny_ptr = nullptr;
        while ((kInBinTinyPtrMask & (*pre_tiny_ptr)) != 0) {
            odd_last ^= 1;
            pre_pre_tiny_ptr = pre_tiny_ptr;
            pre_tiny_ptr =
                bin_begin +
                kEntryByteLength * ((kInBinTinyPtrMask & (*pre_tiny_ptr)) - 1) +
                kTinyPtrOffset;
        }

        uint8_t& head = bin_head(bin_id);

        if (odd_last && bin_head_double_slot(bin_id)) {
            uint8_t& head_double_slot = bin_head_double_slot(bin_id);

            // extract original single slot to free list
            *pre_tiny_ptr = head;
            head = (*pre_pre_tiny_ptr) & kInBinTinyPtrMask;

            // set TP before the double slot
            *pre_pre_tiny_ptr =
                head_double_slot | ((*pre_pre_tiny_ptr) & kInDoubleSlotMask);

            uint8_t* double_slot_begin =
                byte_array +
                (bin_id * kBinSize + head_double_slot - 1) * kEntryByteLength;

            // assuming little endian
            // copy previous single slot to double slot
            uint64_t cp_tmp =
                *reinterpret_cast<uint64_t*>(pre_tiny_ptr - kTinyPtrOffset);
            *reinterpret_cast<uint64_t*>(double_slot_begin) = cp_tmp;

            cp_tmp = *reinterpret_cast<uint64_t*>(
                pre_tiny_ptr - kTinyPtrOffset + kValueOffset);
            *reinterpret_cast<uint64_t*>(double_slot_begin + kValueOffset) =
                cp_tmp;

            // set first entry of double slot
            pre_tiny_ptr = double_slot_begin + kTinyPtrOffset;
            *pre_tiny_ptr = (head_double_slot + 1) | kInDoubleSlotMask;

            aiming_entry = double_slot_begin + kEntryByteLength;

            // special setting of end because of kInDoubleSlotMask
            // assuming little endian
            *reinterpret_cast<uint64_t*>(aiming_entry) =
                key >> kQuotientingTailLength;
            aiming_entry[kTinyPtrOffset] = kInDoubleSlotMask;
            *reinterpret_cast<uint64_t*>(aiming_entry + kValueOffset) = value;

            return true;

        } else if (head == 0) {
            uint8_t& head_double_slot = bin_head_double_slot(bin_id);
            head = head_double_slot + 1;

            aiming_entry =
                byte_array +
                (bin_id * kBinSize + head_double_slot - 1) * kEntryByteLength;
            // saving the tinyptr to itself at the beginning
            *pre_tiny_ptr =
                head_double_slot | ((*pre_tiny_ptr) & kInDoubleSlotMask);
            head_double_slot = aiming_entry[kTinyPtrOffset];
        } else {
            aiming_entry =
                byte_array + (bin_id * kBinSize + head - 1) * kEntryByteLength;
            // saving the tinyptr to itself at the beginning
            *pre_tiny_ptr = head | ((*pre_tiny_ptr) & kInDoubleSlotMask);
            head = aiming_entry[kTinyPtrOffset];
        }

        cnt++;
    }

    // assuming little endian
    *reinterpret_cast<uint64_t*>(aiming_entry) = key >> kQuotientingTailLength;
    aiming_entry[kTinyPtrOffset] = 0;
    *reinterpret_cast<uint64_t*>(aiming_entry + kValueOffset) = value;

    return true;
}

bool FPTPChainedHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t base_id = hash_1_base_id(key);
    // leave the redundant variable for readibility
    uint8_t* base_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* pre_tiny_ptr = base_tiny_ptr;
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);

    if (*pre_tiny_ptr == 0) {
        return false;
    }

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    uint8_t* bin_begin =
        byte_array + (((*base_tiny_ptr) & kSecondHashMask) ? hash_2_bin(key)
                                                           : hash_1_bin(key)) *
                         kBinSize * kEntryByteLength;

    while ((kInBinTinyPtrMask & (*pre_tiny_ptr)) != 0) {
        uint8_t* entry =
            bin_begin +
            kEntryByteLength * (((*pre_tiny_ptr) & kInBinTinyPtrMask) - 1);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

bool FPTPChainedHT::Update(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);

    // quotienting
    key >>= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {
        uint8_t* entry = ptab_query_entry_address(base_intptr, *pre_tiny_ptr);
        if (((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) >>
             kQuotientingTailLength) == key) {
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
    }

    return false;
}

void FPTPChainedHT::Free(uint64_t key) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t* pre_tiny_ptr = &base_tab_ptr(base_id);
    uint8_t* cur_tiny_ptr = nullptr;
    uintptr_t base_intptr = reinterpret_cast<uintptr_t>(pre_tiny_ptr);

    // quotienting
    key >>= kQuotientingTailLength;

    uint8_t* cur_entry = nullptr;
    uint8_t* aiming_entry = nullptr;

    if (*pre_tiny_ptr != 0) {
        cur_entry = ptab_query_entry_address(base_intptr, *pre_tiny_ptr);
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

        cur_entry = ptab_query_entry_address(base_intptr, *cur_tiny_ptr);
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

}  // namespace tinyptr
