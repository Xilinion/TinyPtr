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

    int result = posix_memalign(reinterpret_cast<void**>(&frontyard_tab_ptr),
                                utils::kCacheLineSize,
                                (utils::kCacheLineSize + kBackyardSize) * kYardNum);
    if (result != 0) {
        throw std::runtime_error("posix_memalign failed");
    }
    memset(frontyard_tab_ptr, 0,
           (utils::kCacheLineSize + kBackyardSize) * kYardNum);
    backyard_tab_ptr = frontyard_tab_ptr + kYardNum * utils::kCacheLineSize;

    // freopen("/home/xt253/TinyPtr/results/yarded_tp_ht.txt", "w", stdout);

    // std::cout << "kBaseDuplexSize: " << int(kBaseDuplexSize) << std::endl;
    // exit(0);
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
    // uint8_t base_tab_ptr = get_base_tab_ptr(base_id);
    uint8_t base_tab_ptr = get_base_tab_ptr_for_cachegrind(base_id);

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
