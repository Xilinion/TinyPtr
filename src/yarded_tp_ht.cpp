#include "yarded_tp_ht.h"
#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include "cmath"
#include "utils/cache_line_size.h"

namespace tinyptr {

uint8_t YardedTPHT::AutoBaseBinsize() {
    double valid_base_entry_ratio =
        0.5 * kBinNum * kBinSize / kBaseTabSize + 0.15;

    // 113 when cache line size is 64
    uint8_t max_base_bin_size = (16 * utils::kCacheLineSize - 7) / 9;
    uint8_t res = std::min(
        max_base_bin_size,
        static_cast<uint8_t>(std::ceil(utils::kCacheLineSize /
                                       (1.0 / 8 + valid_base_entry_ratio))));
    return res;
}

YardedTPHT::YardedTPHT(uint64_t size, uint16_t bin_size)
    : ByteArrayChainedHT(size, bin_size),
      kBaseDuplexSize(AutoBaseBinsize()),
      kBaseDuplexEncodingBytes((kBaseDuplexSize + 7) / 8),
      kFrontyardSize(utils::kCacheLineSize - kBaseDuplexEncodingBytes),
      kBackyardSize(kBaseDuplexEncodingBytes - kFrontyardSize),
      kFrontyardOffset(kBaseDuplexEncodingBytes),
      kYardNum((kBaseTabSize + kBaseDuplexSize - 1) / kFrontyardSize) {

    (void)posix_memalign(reinterpret_cast<void**>(&frontyard_tab_ptr),
                         utils::kCacheLineSize,
                         (utils::kCacheLineSize + kBackyardSize) * kYardNum);
    memset(frontyard_tab_ptr, 0,
           (utils::kCacheLineSize + kBackyardSize) * kYardNum);
    backyard_tab_ptr = frontyard_tab_ptr + kYardNum * utils::kCacheLineSize;

    // freopen("/home/xt253/TinyPtr/results/yarded_tp_ht.txt", "w", stdout);

    // std::cout << "kBaseDuplexSize: " << int(kBaseDuplexSize) << std::endl;
    // exit(0);
}

void YardedTPHT::print_duplex(uint64_t base_id, char* msg) {

    return;

    uint64_t duplex_id = base_id / kBaseDuplexSize;
    uint64_t in_duplex_id = base_id % kBaseDuplexSize;
    uint8_t* frontyard_ptr =
        frontyard_tab_ptr + duplex_id * utils::kCacheLineSize;

    std::cout << msg << std::endl;
    std::cout << "base_id: " << base_id << std::endl;
    std::cout << "duplex_id: " << duplex_id << " in_duplex_id: " << in_duplex_id
              << std::endl;
    std::cout << "frontyard_meta: "
              << std::bitset<64>(*reinterpret_cast<uint64_t*>(frontyard_ptr))
              << " "
              << std::bitset<64>(
                     reinterpret_cast<uint64_t*>(frontyard_ptr + 8)[0] &
                     ((1ull << (kBaseDuplexSize - utils::kCacheLineSize)) - 1))
              << std::endl;
    std::cout << "frontyard_data: ";
    for (int i = kFrontyardOffset; i < kFrontyardOffset + kFrontyardSize; i++) {
        std::cout << int(frontyard_ptr[i]) << " ";
    }
    std::cout << std::endl;
    std::cout << "backyard_data: ";
    for (int i = 0; i < kBackyardSize; i++) {
        std::cout << int(backyard_tab_ptr[duplex_id * kBackyardSize + i])
                  << " ";
    }
    std::cout << std::endl;
}

uint64_t YardedTPHT::get_base_key(uint64_t base_id) {
    return reinterpret_cast<uint64_t>(frontyard_tab_ptr) + base_id;
}

uint8_t YardedTPHT::get_base_tab_ptr(uint64_t base_id) {

    print_duplex(base_id, "get_base_tab_ptr");

    uint64_t duplex_id = base_id / kBaseDuplexSize;
    uint64_t in_duplex_id = base_id % kBaseDuplexSize;
    uint8_t* frontyard_ptr =
        frontyard_tab_ptr + duplex_id * utils::kCacheLineSize;

    // if the base_tab entry exists
    uint8_t byte_index = in_duplex_id >> 3;
    uint8_t bit_index = in_duplex_id & 7;

    // Check if the ith bit is 1
    bool is_one = (frontyard_ptr[byte_index] >> bit_index) & 1;

    if (!is_one) {
        return 0;
    }

    uint8_t in_yard_pos = 0;
    uint64_t first_segment = *reinterpret_cast<uint64_t*>(frontyard_ptr);

    if (in_duplex_id < utils::kCacheLineSize) {
        in_yard_pos += _mm_popcnt_u64(first_segment << (utils::kCacheLineSize -
                                                        1 - in_duplex_id)) -
                       1;
    } else {
        uint64_t second_segment =
            *reinterpret_cast<uint64_t*>(frontyard_ptr + 8);
        in_yard_pos +=
            _mm_popcnt_u64(second_segment
                           << (2 * utils::kCacheLineSize - 1 - in_duplex_id)) +
            _mm_popcnt_u64(first_segment) - 1;
    }

    if (in_yard_pos < kFrontyardSize) {
        return frontyard_ptr[kFrontyardOffset + in_yard_pos];
    } else {
        return backyard_tab_ptr[duplex_id * kBackyardSize + in_yard_pos -
                                kFrontyardSize];
    }
}

void YardedTPHT::set_base_tab_ptr(uint64_t base_id, uint8_t new_ptr) {

    print_duplex(base_id, "set_base_tab_ptr");

    uint64_t duplex_id = base_id / kBaseDuplexSize;
    uint64_t in_duplex_id = base_id % kBaseDuplexSize;
    uint8_t* frontyard_ptr =
        frontyard_tab_ptr + duplex_id * utils::kCacheLineSize;

    uint8_t byte_index = in_duplex_id >> 3;
    uint8_t bit_index = in_duplex_id & 7;

    frontyard_ptr[byte_index] |= 1 << bit_index;

    uint8_t in_yard_pos = 0, in_yard_cnt = 0;
    uint64_t first_segment = *reinterpret_cast<uint64_t*>(frontyard_ptr);
    uint64_t second_segment = *reinterpret_cast<uint64_t*>(frontyard_ptr + 8);

    if (in_duplex_id < utils::kCacheLineSize) {
        in_yard_pos += _mm_popcnt_u64(first_segment << (utils::kCacheLineSize -
                                                        1 - in_duplex_id)) -
                       1;
    } else {
        in_yard_cnt += _mm_popcnt_u64(first_segment);
        in_yard_pos +=
            _mm_popcnt_u64(second_segment
                           << (2 * utils::kCacheLineSize - 1 - in_duplex_id)) +
            in_yard_cnt - 1;
    }

    if (in_yard_pos >= kFrontyardSize) {
        backyard_tab_ptr[duplex_id * kBackyardSize + in_yard_pos -
                         kFrontyardSize] = new_ptr;
        return;
    }

    uint8_t spill_ptr = frontyard_ptr[utils::kCacheLineSize - 1];

    uint8_t move_ind = utils::kCacheLineSize - 1;
    for (; move_ind - 8 >= in_yard_pos + kFrontyardOffset; move_ind -= 8) {
        *reinterpret_cast<uint64_t*>(frontyard_ptr + move_ind + 1 - 8) =
            *reinterpret_cast<uint64_t*>(frontyard_ptr + move_ind - 8);
    }

    // Move remaining bytes forward
    for (; move_ind >= in_yard_pos + kFrontyardOffset; move_ind--) {
        frontyard_ptr[move_ind + 1] = frontyard_ptr[move_ind];
    }

    frontyard_ptr[kFrontyardOffset + in_yard_pos] = new_ptr;

    if (spill_ptr == 0) {
        return;
    } else {
        in_yard_cnt +=
            _mm_popcnt_u64(second_segment << (2 * utils::kCacheLineSize - 1 -
                                              kBaseDuplexSize));
        uint64_t spill_ptr_id = reinterpret_cast<uint64_t*>(
            frontyard_ptr + kFrontyardOffset - 8)[0];

        in_yard_cnt--;
        while (in_yard_cnt > kFrontyardSize) {
            in_yard_cnt--;
            spill_ptr_id -= spill_ptr_id & (-spill_ptr_id);
        }
        spill_ptr_id = spill_ptr_id & (-spill_ptr_id);
        spill_ptr_id =
            _lzcnt_u64(spill_ptr_id) + kBaseDuplexSize - utils::kCacheLineSize;
        backyard_tab_ptr[spill_ptr_id] = spill_ptr;

        return;
    }
}

void YardedTPHT::free_base_tab_ptr(uint64_t base_id) {
    frontyard_tab_ptr[base_id] = 0;
}

bool YardedTPHT::Insert(uint64_t key, uint64_t value) {
    uint64_t base_id = hash_1_base_id(key);
    uint8_t base_tab_ptr = get_base_tab_ptr(base_id);
    uint64_t base_key = get_base_key(base_id);

    if (base_tab_ptr == 0) {
        uint8_t* entry = ptab_insert_entry_address(base_key);
        if (entry != nullptr) {
            set_base_tab_ptr(base_id, *entry);
            print_duplex(base_id, "Insert");
            // assuming little endian
            *reinterpret_cast<uint64_t*>(entry) = key >> kQuotientingTailLength;
            entry[kTinyPtrOffset] = 0;
            *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
            return true;
        } else {
            return false;
        }
    }

    // the base_tab_ptr is not null so will not be written later
    uint8_t* pre_tiny_ptr = &base_tab_ptr;
    uint64_t pre_tiny_ptr_key = base_key;

    int cnt = 0;

    while (*pre_tiny_ptr != 0) {
        // cnt++;
        // if (cnt > 10) {
        //     exit(0);
        // }
        // std::cout << "pre_tiny_ptr: " << int(*pre_tiny_ptr) << std::endl;
        // std::cout << "pre_tiny_ptr_key: " << pre_tiny_ptr_key << std::endl;

        uint8_t* entry =
            ptab_query_entry_address(pre_tiny_ptr_key, *pre_tiny_ptr);
        pre_tiny_ptr = entry + kTinyPtrOffset;
        pre_tiny_ptr_key = reinterpret_cast<uint64_t>(entry);
    }

    uint8_t* entry = ptab_insert_entry_address(pre_tiny_ptr_key);

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

bool YardedTPHT::Query(uint64_t key, uint64_t* value_ptr) {

    uint64_t base_id = hash_1_base_id(key);
    uint8_t base_tab_ptr = get_base_tab_ptr(base_id);

    if (base_tab_ptr == 0) {
        return false;
    }

    uint8_t* pre_tiny_ptr = &base_tab_ptr;
    uint64_t pre_tiny_ptr_key = get_base_key(base_id);

    // quotienting and shifting back
    key >>= kQuotientingTailLength;
    key <<= kQuotientingTailLength;

    while (*pre_tiny_ptr != 0) {

        query_entry_cnt++;

        uint8_t* entry =
            ptab_query_entry_address(pre_tiny_ptr_key, *pre_tiny_ptr);
        if ((*reinterpret_cast<uint64_t*>(entry) << kQuotientingTailLength) ==
            key) {
            *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);
            return true;
        }
        pre_tiny_ptr = entry + kTinyPtrOffset;
        pre_tiny_ptr_key = reinterpret_cast<uint64_t>(entry);
    }

    return false;
}

}  // namespace tinyptr
