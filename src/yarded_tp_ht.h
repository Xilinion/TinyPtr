#pragma once

#include <bitset>
#include <cstdint>
#include "byte_array_chained_ht.h"

namespace tinyptr {

class YardedTPHT : public ByteArrayChainedHT {

   public:
    const uint8_t kBaseDuplexSize;
    const uint8_t kBaseDuplexEncodingBytes;
    const uint8_t kFrontyardSize;
    const uint8_t kBackyardSize;
    const uint8_t kFrontyardOffset;
    const uint64_t kYardNum;

   protected:
    uint8_t AutoBaseBinsize();

   public:
    YardedTPHT(uint64_t size, uint16_t bin_size);
    ~YardedTPHT() = default;

   protected:
    __attribute__((always_inline)) inline void print_duplex(uint64_t base_id,
                                                            const char* msg) {

        return;

        uint64_t duplex_id = base_id / kBaseDuplexSize;
        uint64_t in_duplex_id = base_id % kBaseDuplexSize;
        uint8_t* frontyard_ptr =
            frontyard_tab_ptr + duplex_id * utils::kCacheLineSize;

        std::cout << msg << std::endl;
        std::cout << "base_id: " << base_id << std::endl;
        std::cout << "duplex_id: " << duplex_id
                  << " in_duplex_id: " << in_duplex_id << std::endl;
        std::cout
            << "frontyard_meta: "
            << std::bitset<64>(*reinterpret_cast<uint64_t*>(frontyard_ptr))
            << " "
            << std::bitset<64>(
                   reinterpret_cast<uint64_t*>(frontyard_ptr + 8)[0] &
                   ((1ull << (kBaseDuplexSize - utils::kCacheLineSize)) - 1))
            << std::endl;
        std::cout << "frontyard_data: ";
        for (int i = kFrontyardOffset; i < kFrontyardOffset + kFrontyardSize;
             i++) {
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

    __attribute__((always_inline)) inline uint64_t get_base_key(
        uint64_t base_id) {
        return reinterpret_cast<uint64_t>(frontyard_tab_ptr) + base_id;
    }

    __attribute__((always_inline)) inline uint8_t get_base_tab_ptr(
        uint64_t base_id) {

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
            in_yard_pos +=
                _mm_popcnt_u64(first_segment
                               << (utils::kCacheLineSize - 1 - in_duplex_id)) -
                1;
        } else {
            uint64_t second_segment =
                *reinterpret_cast<uint64_t*>(frontyard_ptr + 8);
            in_yard_pos +=
                _mm_popcnt_u64(second_segment << (2 * utils::kCacheLineSize -
                                                  1 - in_duplex_id)) +
                _mm_popcnt_u64(first_segment) - 1;
        }

        if (in_yard_pos < kFrontyardSize) {
            return frontyard_ptr[kFrontyardOffset + in_yard_pos];
        } else {
            return backyard_tab_ptr[duplex_id * kBackyardSize + in_yard_pos -
                                    kFrontyardSize];
        }
    }

    __attribute__((always_inline)) inline uint8_t
    get_base_tab_ptr_for_cachegrind(uint64_t base_id) {

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
            in_yard_pos +=
                _mm_popcnt_u64(first_segment
                               << (utils::kCacheLineSize - 1 - in_duplex_id)) -
                1;
        } else {
            uint64_t second_segment =
                *reinterpret_cast<uint64_t*>(frontyard_ptr + 8);
            in_yard_pos +=
                _mm_popcnt_u64(second_segment << (2 * utils::kCacheLineSize -
                                                  1 - in_duplex_id)) +
                _mm_popcnt_u64(first_segment) - 1;
        }

        if (in_yard_pos < kFrontyardSize) {
            return frontyard_ptr[kFrontyardOffset + in_yard_pos];
        } else {
            return backyard_tab_ptr[duplex_id * kBackyardSize + in_yard_pos -
                                    kFrontyardSize];
        }
    }

    __attribute__((always_inline)) inline void set_base_tab_ptr(
        uint64_t base_id, uint8_t new_ptr) {

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
        uint64_t second_segment =
            *reinterpret_cast<uint64_t*>(frontyard_ptr + 8);

        if (in_duplex_id < utils::kCacheLineSize) {
            in_yard_pos +=
                _mm_popcnt_u64(first_segment
                               << (utils::kCacheLineSize - 1 - in_duplex_id)) -
                1;
        } else {
            in_yard_cnt += _mm_popcnt_u64(first_segment);
            in_yard_pos +=
                _mm_popcnt_u64(second_segment << (2 * utils::kCacheLineSize -
                                                  1 - in_duplex_id)) +
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
                _mm_popcnt_u64(second_segment << (2 * utils::kCacheLineSize -
                                                  1 - kBaseDuplexSize));
            uint64_t spill_ptr_id = reinterpret_cast<uint64_t*>(
                frontyard_ptr + kFrontyardOffset - 8)[0];

            in_yard_cnt--;
            while (in_yard_cnt > kFrontyardSize) {
                in_yard_cnt--;
                spill_ptr_id -= spill_ptr_id & (-spill_ptr_id);
            }
            spill_ptr_id = spill_ptr_id & (-spill_ptr_id);
            spill_ptr_id = _lzcnt_u64(spill_ptr_id) + kBaseDuplexSize -
                           utils::kCacheLineSize;
            backyard_tab_ptr[spill_ptr_id] = spill_ptr;

            return;
        }
    }

    __attribute__((always_inline)) inline void free_base_tab_ptr(
        uint64_t base_id) {
        frontyard_tab_ptr[base_id] = 0;
    }

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);

   protected:
    uint8_t* frontyard_tab_ptr;
    uint8_t* backyard_tab_ptr;
};

}  // namespace tinyptr
