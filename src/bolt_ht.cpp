#include "bolt_ht.h"
#include <sys/mman.h>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace tinyptr {

uint8_t BoltHT::kCloudLookup[256];
struct BoltHTCloudLookupInitializer bolt_ht_cloud_lookup_initializer;

std::mutex output_mutex;

// #define USE_CONCURRENT_VERSION_QUERY

uint8_t BoltHT::AutoFastDivisionInnerShift(uint64_t divisor) {
    uint8_t res = 0;
    while ((1ULL << res) < divisor) {
        res++;
    }
    return res;
}

uint8_t BoltHT::AutoQuotTailLength(uint64_t size) {
    uint8_t res = 8;
    // making size/4 <= 1 << res < size/2
    size >>= 10;
    while (size) {
        size >>= 1;
        res++;
    }
    return res;
}

// uint64_t BoltHT::GenCloudHashFactor(uint64_t min_gap, uint64_t mod_mask) {
//     // mod must be a power of 2
//     // min_gap should be a relative small threshold
//     uint64_t res = (rand() | 1) & mod_mask;
//     while (res < min_gap) {
//         res = (rand() | 1) & mod_mask;
//     }
//     return res;
// }

uint8_t BoltHT::AutoLockNum(uint64_t thread_num_supported) {
    uint8_t res = 0;
    thread_num_supported = thread_num_supported * thread_num_supported;
    while (thread_num_supported) {
        thread_num_supported >>= 1;
        res++;
    }
    return 1 << res;
}

BoltHT::BoltHT(uint64_t size, uint8_t quotienting_tail_length,
               uint16_t bin_size)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kQuotientingTailLength(quotienting_tail_length
                                 ? quotienting_tail_length
                                 : AutoQuotTailLength(size)),
      kBoltQuotientingLength(kQuotientingTailLength + kByteShift),
      kQuotientingTailMask((1ULL << kQuotientingTailLength) - 1),
      kQuotKeyByteLength((64 + 7 - kQuotientingTailLength) >> 3),
      kCrystalByteLength(kQuotKeyByteLength + 8),
      kDropletByteLength(kCrystalByteLength - 1),
      kBinByteLength(kBinSize * kDropletByteLength),
      kCloudNum(1ULL << kQuotientingTailLength),
      kBinSize(bin_size),
      kBinNum((size * kCloudOverflowBound + kBinSize - 1) / kBinSize),
      kValueOffset(kKeyOffset + kQuotKeyByteLength),
      kDropletValueOffset(kValueOffset - 1),
      kFastDivisionShift{
          static_cast<uint8_t>(
              kFastDivisionUpperBoundLog +
              AutoFastDivisionInnerShift(kDropletByteLength)) /*not used*/,
          static_cast<uint8_t>(kFastDivisionUpperBoundLog +
                               AutoFastDivisionInnerShift(kBinNum))},
      kFastDivisionReciprocal{
          (1ULL << kFastDivisionShift[0]) / kDropletByteLength + 1 /*not used*/,
          (1ULL << kFastDivisionShift[1]) / kBinNum + 1} {

    assert(size / 2 > (1ULL << (kQuotientingTailLength)));
    assert(bin_size < 128);

    // Determine the number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads =
            4;  // Fallback to a default value if hardware_concurrency is not available
    }

    bin_locks_size = kBinNum;

    bin_locks = std::make_unique<std::atomic_flag[]>(bin_locks_size);

    for (size_t i = 0; i < bin_locks_size; ++i) {
        bin_locks[i].clear();
    }

    // Calculate individual sizes
    uint64_t cloud_size = kCloudNum * kCloudByteLength;
    uint64_t byte_array_size = kBinNum * kBinSize * kDropletByteLength;
    uint64_t bin_cnt_size = kBinNum << 1;

    // Align each section to 64 bytes
    uint64_t cloud_size_aligned =
        (cloud_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t byte_array_size_aligned =
        (byte_array_size + 63) & ~static_cast<uint64_t>(63);
    uint64_t bin_cnt_size_aligned =
        (bin_cnt_size + 63) & ~static_cast<uint64_t>(63);

    // Total size for combined allocation
    uint64_t total_size =
        cloud_size_aligned + byte_array_size_aligned + bin_cnt_size_aligned;

    // Allocate a single aligned block
    void* combined_mem;
    // if (posix_memalign(&combined_mem, 64, total_size) != 0) {
    //     // Handle allocation failure
    //     abort();
    // }
    combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                        MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

    // Assign pointers to their respective regions
    uint8_t* cloud =
        (uint8_t*)((uint64_t)(combined_mem + 63) & ~static_cast<uint64_t>(63));
    cloud_tab = cloud;
    cloud += cloud_size_aligned;

    byte_array = cloud;
    cloud += byte_array_size_aligned;

    bin_cnt_head = cloud;
}

BoltHT::BoltHT(uint64_t size, uint16_t bin_size) : BoltHT(size, 0, bin_size) {}

BoltHT::BoltHT(uint64_t size) : BoltHT(size, 0, 127) {}

BoltHT::~BoltHT() {
    munmap(cloud_tab, kCloudNum * kCloudByteLength);
    munmap(byte_array, kBinNum * kBinSize * kDropletByteLength);
    munmap(bin_cnt_head, kBinNum << 1);
}

bool BoltHT::Insert(uint64_t key, uint64_t value) {

    uint64_t cloud_id = hash_cloud_id(key);

    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    // Use std::atomic for concurrent_version
    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &cloud[kConcurrentVersionOffset]);

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1) ||
             !concurrent_version.compare_exchange_weak(expected_version,
                                                       expected_version + 1));

    uint8_t& control_info = cloud[kControlOffset];
    uint8_t crystal_cnt = control_info & kControlCrystalMask;
    uint8_t bolt_cnt = (control_info >> kControlBoltShift);

    uint8_t crystal_end = kCrystalOffset + (crystal_cnt * kCrystalByteLength);
    uint8_t bolt_end = kBoltOffset - (bolt_cnt << kBoltByteLengthShift);

    uint64_t truncated_key = key >> kQuotientingTailLength;

    if (crystal_end + kCrystalByteLength <= bolt_end) {
        reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0] =
            truncated_key;
        reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0] =
            value;
        control_info++;
        concurrent_version++;
        return true;
    } else if (crystal_end + kBoltByteLength <= bolt_end) {

        uint8_t* bolt_ptr = cloud + bolt_end - kBoltByteLength;
        uint8_t* tiny_ptr = bolt_ptr + kTinyPtrOffset;
        uint8_t fingerprint = truncated_key & kByteMask;
        bolt_ptr[kFingerprintOffset] = fingerprint ^ bolt_cnt;

        bool result = ptab_insert(
            tiny_ptr, (cloud_id << kByteShift) | fingerprint, key, value);

        control_info += (result << kControlBoltShift);
        concurrent_version++;
        return result;
    } else {

        crystal_end -= kCrystalByteLength;

        uint64_t last_crystal_trunced_key =
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0];
        uint64_t last_crystal_masked_key =
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0]
            << kQuotientingTailLength;
        uint64_t last_crystal_value =
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0];

        uint8_t* bolt_ptr = cloud + bolt_end - kBoltByteLength;
        uint8_t* tiny_ptr = bolt_ptr + kTinyPtrOffset;
        uint8_t fingerprint = last_crystal_trunced_key & kByteMask;
        bolt_ptr[kFingerprintOffset] = fingerprint ^ bolt_cnt;

        if (ptab_insert(tiny_ptr, (cloud_id << kByteShift) | fingerprint,
                        last_crystal_masked_key, last_crystal_value)) {

            bolt_ptr -= kBoltByteLength;
            tiny_ptr -= kBoltByteLength;
            fingerprint = truncated_key & kByteMask;
            bolt_ptr[kFingerprintOffset] = fingerprint ^ (bolt_cnt + 1);

            if (ptab_insert(tiny_ptr, (cloud_id << kByteShift) | fingerprint,
                            key, value)) {

                control_info--;
                uint8_t inc_cnt = 2;
                control_info += (inc_cnt << kControlBoltShift);
                concurrent_version++;
                return true;
            } else {
                reinterpret_cast<uint64_t*>(
                    cloud + crystal_end + kValueOffset)[0] = last_crystal_value;
                concurrent_version++;
                return false;
            }

        } else {
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0] =
                last_crystal_value;
            concurrent_version++;
            return false;
        }
    }
}

bool BoltHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t cloud_id = hash_cloud_id(key);
    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    std::atomic<uint8_t>& concurrent_version =
        *reinterpret_cast<std::atomic<uint8_t>*>(
            &cloud[kConcurrentVersionOffset]);

query_again:

    uint8_t expected_version;
    do {
        expected_version = concurrent_version.load();
    } while ((expected_version & 1));

    uint8_t& control_info = cloud[kControlOffset];
    uint8_t crystal_cnt = control_info & kControlCrystalMask;
    uint8_t bolt_cnt = (control_info >> kControlBoltShift);

    uint64_t truncated_key = key >> kQuotientingTailLength;
    uint64_t masked_key = truncated_key << kQuotientingTailLength;

    uint8_t* crystal_ptr = cloud + kCrystalOffset;

    // Crystal search - using standard loop
    for (uint8_t i = 0; i < crystal_cnt; i++) {
        uint64_t crystal_masked_key =
            reinterpret_cast<uint64_t*>(crystal_ptr + kKeyOffset)[0]
            << kQuotientingTailLength;

        if (crystal_masked_key == masked_key) {
            *value_ptr =
                reinterpret_cast<uint64_t*>(crystal_ptr + kValueOffset)[0];

            if (concurrent_version.load() != expected_version) {
                goto query_again;
            }
            return true;
        }
        crystal_ptr += kCrystalByteLength;
    }

    // Bolt search - using standard loop
    uint8_t* bolt_ptr = cloud + kBoltOffset - kBoltByteLength;
    uint8_t query_fp = truncated_key & kByteMask;
    masked_key = (truncated_key >> kByteShift) << kBoltQuotientingLength;

    // Simple loop over all bolts
    for (uint8_t i = 0; i < bolt_cnt; i++) {
        uint8_t* tiny_ptr = bolt_ptr + kTinyPtrOffset;
        uint8_t fingerprint = bolt_ptr[kFingerprintOffset] ^ i;

        if (fingerprint == query_fp) {
            uint64_t deref_key = (cloud_id << kByteShift) | fingerprint;
            uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);
            uint64_t entry_masked_key =
                (*reinterpret_cast<uint64_t*>(entry + kDropletKeyOffset))
                << kBoltQuotientingLength;

            if (entry_masked_key == masked_key) {
                *value_ptr =
                    *reinterpret_cast<uint64_t*>(entry + kDropletValueOffset);

                if (concurrent_version.load() != expected_version) {
                    goto query_again;
                }
                return true;
            }
        }

        bolt_ptr -= kBoltByteLength;
    }

    if (concurrent_version.load() != expected_version) {
        goto query_again;
    }
    return false;
}

bool BoltHT::Update(uint64_t key, uint64_t value) {
    return 1;
}

void BoltHT::Free(uint64_t key) {}

}  // namespace tinyptr