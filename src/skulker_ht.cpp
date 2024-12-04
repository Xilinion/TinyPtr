#include "skulker_ht.h"
#include <cstdint>
#include <cstring>

namespace tinyptr {

uint8_t SkulkerHT::kBushLookup[256];
struct SkulkerHTBushLookupInitializer skulker_ht_bush_lookup_initializer;

uint8_t SkulkerHT::AutoQuotTailLength(uint64_t size) {
    uint8_t res = 16;
    size >>= 16;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

SkulkerHT::SkulkerHT(uint64_t size, uint8_t quotienting_tail_length,
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
      kBushRatio(0.5 * size / (1.0 * (1ULL << kQuotientingTailLength)) + 0.15),
      kBushOverflowBound(0.11),
      kSkulkerRatio((1 - kBushRatio * (1ULL << kQuotientingTailLength) / size) +
                    kBushOverflowBound),
      kInitExhibitorNum(kBushByteLength / kEntryByteLength),
      kInitSkulkerNum(kBushByteLength - kInitExhibitorNum * kEntryByteLength -
                      1),
      kBushCapacity(
          std::min(8, static_cast<int>(ceil(kInitExhibitorNum / kBushRatio)))),
      kBushNum(((1ULL << kQuotientingTailLength) + kBushCapacity - 1) /
               kBushCapacity),
      kControlByteOffset(kBushByteLength - 1),
      kSkulkerOffset(kControlByteOffset - 1),
      kBinSize(bin_size),
      kBinNum((size * kSkulkerRatio + kBinSize - 1) / kBinSize),
      kTinyPtrOffset(0),
      kKeyOffset(1),
      kValueOffset(kKeyOffset + kQuotKeyByteLength),
      kFastDivisionReciprocal(kFastDivisionBase / kBushCapacity +
                              kBushCapacity) {

    assert(2 * size >= (1ULL << (kQuotientingTailLength)));
    assert(bin_size < 128);

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

    play_entry = new uint8_t[kEntryByteLength];
}

SkulkerHT::SkulkerHT(uint64_t size, uint16_t bin_size)
    : SkulkerHT(size, 0, bin_size) {}

bool SkulkerHT::Insert(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint64_t bush_id = base_id / kBushCapacity;
    uint64_t in_bush_offset = base_id % kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    uint8_t& control_byte =
        bush_tab[(bush_id << kBushIdShiftOffset) + kControlByteOffset];
    uint8_t item_cnt = kBushLookup[control_byte];
    uint8_t before_item_cnt = kBushLookup[control_byte >> in_bush_offset];

    if ((control_byte >> in_bush_offset) & 1) {

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

        return ptab_insert(pre_tiny_ptr, pre_deref_key, key, value);

    } else {
        uint8_t pre_overload_flag =
            item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

        uint8_t overload_flag =
            item_cnt + 1 > (kInitSkulkerNum + kInitExhibitorNum);

        // if (overload_flag) {
        //     return false;
        // }

        // spill before inserting the new item
        if (pre_overload_flag == 0 && overload_flag == 1) {
            uint8_t exhibitor_num = kInitExhibitorNum;

            // convert the last exhibitor to the first skulker
            memcpy(play_entry, bush + (exhibitor_num - 1) * kEntryByteLength,
                   kEntryByteLength);

            if (item_cnt > exhibitor_num) {
                // exhibitor spills and converts the last exhibitor to the first skulker
                for (uint8_t* i =
                         bush + kSkulkerOffset - (item_cnt - exhibitor_num - 1);
                     i < bush + kSkulkerOffset; i++) {
                    *i = *(i + 1);
                }

                bush[kSkulkerOffset] = play_entry[kTinyPtrOffset];
                // get the base_id of the last exhibitor
                uint8_t spilled_in_bush_offset = 0;
                while (
                    kBushLookup[control_byte >> (spilled_in_bush_offset + 1)] >
                    exhibitor_num - overload_flag) {
                    spilled_in_bush_offset++;
                }

                uintptr_t spilled_base_id =
                    base_id - in_bush_offset + spilled_in_bush_offset;

                bush[kSkulkerOffset] = 0;

                if (!ptab_insert(bush + kSkulkerOffset, spilled_base_id,
                                 hash_1_key_rebuild(
                                     *(uint64_t*)(play_entry + kKeyOffset),
                                     spilled_base_id),
                                 *(uint64_t*)(play_entry + kValueOffset))) {
                    return false;
                }
            }
        }

        control_byte |= (1 << in_bush_offset);
        item_cnt++;
        before_item_cnt++;

        uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;
        if (before_item_cnt > exhibitor_num) {
            // move skulkers after it one byte backwards
            for (uint8_t* i =
                     bush + kSkulkerOffset - (item_cnt - exhibitor_num - 1);
                 i <
                 bush + kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num);
                 i++) {
                *i = *(i + 1);
            }

            uint8_t* pre_tiny_ptr =
                bush + kSkulkerOffset - (before_item_cnt - 1 - exhibitor_num);
            *pre_tiny_ptr = 0;

            uintptr_t pre_deref_key = base_id;

            return ptab_insert(pre_tiny_ptr, pre_deref_key, key, value);

        } else {

            if (item_cnt > exhibitor_num) {
                memcpy(play_entry,
                       bush + (exhibitor_num - 1) * kEntryByteLength,
                       kEntryByteLength);

                // exhibitor spills and converts the last exhibitor to the first skulker
                for (uint8_t* i =
                         bush + kSkulkerOffset - (item_cnt - exhibitor_num - 1);
                     i < bush + kSkulkerOffset; i++) {
                    *i = *(i + 1);
                }

                bush[kSkulkerOffset] = play_entry[kTinyPtrOffset];
                // get the base_id of the last exhibitor
                uint8_t spilled_in_bush_offset = 0;
                while (
                    kBushLookup[control_byte >> (spilled_in_bush_offset + 1)] >
                    exhibitor_num - overload_flag) {
                    spilled_in_bush_offset++;
                }

                uintptr_t spilled_base_id =
                    base_id - in_bush_offset + spilled_in_bush_offset;

                if (!ptab_insert(bush + kSkulkerOffset, spilled_base_id,
                                 hash_1_key_rebuild(
                                     *(uint64_t*)(play_entry + kKeyOffset),
                                     spilled_base_id),
                                 *(uint64_t*)(play_entry + kValueOffset))) {
                    return false;
                }
            }

            memmove(bush + before_item_cnt * kEntryByteLength,
                    bush + (before_item_cnt - 1) * kEntryByteLength,
                    kEntryByteLength * (exhibitor_num - before_item_cnt));

            uint8_t* new_entry =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            new_entry[kTinyPtrOffset] = 0;
            *(uint64_t*)(new_entry + kKeyOffset) =
                key >> kQuotientingTailLength;
            *(uint64_t*)(new_entry + kValueOffset) = value;

            return true;
        }
    }
}

bool SkulkerHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t base_id = hash_1_base_id(key);

    // do fast division
    uint64_t bush_id;
    if (base_id > kFastDivisionUpperBound) {
        bush_id = base_id / kBushCapacity;
    } else {
        bush_id =
            (1ULL * base_id * kFastDivisionReciprocal) >> kFastDivisionShift;
    }
    uint64_t in_bush_offset = base_id - bush_id * kBushCapacity;

    uint8_t* bush = &bush_tab[(bush_id << kBushIdShiftOffset)];

    /*
    // cache simluate without calculation
    *value_ptr = *(uint64_t*)bush;

    uint64_t bin_id = hash_2_bin(key);
    uint64_t bin_offset = key % kBinSize;

    uint64_t rand_val = hash_1(key) % 100;
    
    if (40 > rand_val) {
        *value_ptr ^= 
        // 0;
        // bin_offset;
        //  ^ bin_id;
        *(uint64_t*)(byte_array + bin_id * kBinSize * kEntryByteLength +
         bin_offset * kEntryByteLength + kValueOffset);
    }

    return true;
    // simulation end

*/

    uint8_t& control_byte =
        bush_tab[(bush_id << kBushIdShiftOffset) + kControlByteOffset];
    uint8_t item_cnt = kBushLookup[control_byte];
    uint8_t before_item_cnt = kBushLookup[control_byte >> in_bush_offset];

    uint8_t overload_flag = item_cnt > (kInitSkulkerNum + kInitExhibitorNum);

    uint8_t exhibitor_num = kInitExhibitorNum - overload_flag;

    query_entry_cnt++;

    if ((control_byte >> in_bush_offset) & 1) {

        // key >>= kQuotientingTailLength;
        // key <<= kQuotientingTailLength;

        key = key & (~kQuotientingTailMask);

        if (before_item_cnt <= exhibitor_num) {
            uint8_t* exhibitor_ptr =
                bush + (before_item_cnt - 1) * kEntryByteLength;
            if ((*reinterpret_cast<uint64_t*>(exhibitor_ptr + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr =
                    *reinterpret_cast<uint64_t*>(exhibitor_ptr + kValueOffset);
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

            query_entry_cnt++;

            uint8_t* entry =
                ptab_query_entry_address(pre_deref_key, *pre_tiny_ptr);
            if ((*reinterpret_cast<uint64_t*>(entry + kKeyOffset)
                 << kQuotientingTailLength) == key) {
                *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
                return true;
            }
            pre_tiny_ptr = entry + kTinyPtrOffset;
            pre_deref_key = reinterpret_cast<uintptr_t>(entry);
        }
    }

    return false;
}

bool SkulkerHT::Update(uint64_t key, uint64_t value) {
    return false;
}

void SkulkerHT::Free(uint64_t key) {
    return;
}

uint64_t SkulkerHT::QueryEntryCnt() {
    return query_entry_cnt;
}

}  // namespace tinyptr
