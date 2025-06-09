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
                        MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);

    // Assign pointers to their respective regions
    uint8_t* base =
        (uint8_t*)((uint64_t)(combined_mem + 63) & ~static_cast<uint64_t>(63));
    cloud_tab = base;
    base += cloud_size_aligned;

    byte_array = base;
    base += byte_array_size_aligned;

    bin_cnt_head = base;
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

/*
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


*/

bool BoltHT::Query(uint64_t key, uint64_t* value_ptr) {
    //  ────────────────────────────────────────────
    //    ❶  One-time, per-thread constants
    // ────────────────────────────────────────────
    const uint32_t CBL = kCrystalByteLength;  //  1 … 16
    const uint32_t BBL = kBoltByteLength;     //  1 … 16
    const uint32_t KL = kQuotKeyByteLength;   //  1 …  8

    static thread_local struct {
        __m256i cry_off;       // {0, CBL, 2·CBL, 3·CBL}
        __m256i cry_mask_vec;  // mask real key bytes
        uint64_t cry_mask;
        __m128i rev_idx16;  // {7,6,5,4,3,2,1,0}
    } C = [&] {
        uint64_t offs[4] = {0, CBL, 2ULL * CBL, 3ULL * CBL};
        const uint64_t km = (KL == 8) ? ~0ULL : ((1ULL << (KL * 8)) - 1ULL);
        return decltype(C){
            _mm256_load_si256(reinterpret_cast<const __m256i*>(offs)),
            _mm256_set1_epi64x(km), km, _mm_set_epi16(0, 1, 2, 3, 4, 5, 6, 7)};
    }();

    // 4ns above

    //  ────────────────────────────────────────────
    //    ❷  Pre-compute query-key parts
    // ────────────────────────────────────────────
    const uint64_t truncated = key >> kQuotientingTailLength;
    const __m256i key_vec = _mm256_set1_epi64x(truncated & C.cry_mask);

    const uint8_t query_fp = static_cast<uint8_t>(truncated & kByteMask);
    const uint64_t bolt_masked_key = (truncated >> kByteShift)
                                     << kBoltQuotientingLength;

    uint64_t cloud_id = hash_cloud_id(key);

    uint8_t* cloud = cloud_tab + (cloud_id << kCloudIdShiftOffset);

    // 4ns above, hash is ~3ns

retry:
    // ===== version-controlled critical section ===========================

    auto* ver = reinterpret_cast<std::atomic<uint8_t>*>(
        cloud + kConcurrentVersionOffset);
    uint8_t v0;
    do {
        v0 = ver->load();
    } while (v0 & 1u);

    // 30ns for the check & load

    const uint8_t control = cloud[kControlOffset];
    const uint8_t crystal_cnt = control & kControlCrystalMask;  // 0–7
    const uint8_t bolt_cnt = control >> kControlBoltShift;      // 0–31

    // 2-3ns

    // 22ns for positive
    // 16ns for negative
    //  ───────────────────────────────────────── Crystal probe ────────────────
    {

        const uint8_t* base = cloud + kCrystalOffset;

        // __m256i cry_off_vec = _mm256_set_epi64x(3ULL * CBL, 2ULL * CBL, CBL, 0);
        // __m256i g = _mm256_i64gather_epi64(
        //     reinterpret_cast<const long long*>(base + kKeyOffset), cry_off_vec,
        //     1);

        __m256i g = _mm256_i64gather_epi64(
            reinterpret_cast<const long long*>(base + kKeyOffset), C.cry_off,
            1);
        // __m256i g = _mm256_load_si256(
        //     reinterpret_cast<const __m256i*>(base + kKeyOffset));

        // 5-6ns

        // auto cry_mask_vec =
        //     _mm256_set1_epi64x((KL == 8) ? ~0ULL : ((1ULL << (KL * 8)) - 1ULL));
        // g = _mm256_and_si256(g, cry_mask_vec);
        g = _mm256_and_si256(g, C.cry_mask_vec);

        // 0-1ns

        // *value_ptr = _mm256_extract_epi64(g, 0);
        // return false;

        uint32_t bm = _mm256_cmpeq_epi64_mask(g, key_vec);

        // 5ns

        bm &= (1u << (crystal_cnt)) - 1u;

        // 5ns

        // *value_ptr = bm;
        // return false;

        // 10ns for positive
        // 1ns for negative

        if (bm) {

            // int idx = _tzcnt_u32(bm) >> 3;
            int idx = _tzcnt_u32(bm);
            // idx &= 3;

            // *value_ptr= idx;

            const uint8_t* hit = base + idx * CBL;
            *value_ptr = *reinterpret_cast<const uint64_t*>(hit + kValueOffset);

            if ((ver->load() != v0))
                goto retry;
            return true;
        }
    }

    // *value_ptr = crystal_cnt + bolt_cnt;
    // return false;

    //  ───────────────────────── Bolt probe (2-byte bolts) ───────────────
    // 44ns for positive
    // 19ns for negative
    // if (1) {
    // 48ns for positive
    // 23ns for negative
    if (bolt_cnt) {
        uint8_t* fp_hi =
            cloud + kBoltOffset - kBoltByteLength + kFingerprintOffset;

        uint32_t remaining = bolt_cnt;  // bolts left to scan
        uint32_t base_idx = 0;          // index of fp_hi

        const __m128i even_mask16 = _mm_set1_epi16(0x00FF);
        const uint16_t query_fp16 = static_cast<uint16_t>(query_fp);
        const __m128i qfp16_vec = _mm_set1_epi16(query_fp16);

        // 0-2ns

        do {
            const uint32_t batch = remaining >= 8 ? 8 : remaining;

            uint8_t* fp_lo = fp_hi - 7 * kBoltByteLength;

            __m128i bytes =
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(fp_lo));

            __m128i fp16 = _mm_and_si128(bytes, even_mask16);

            // 0ns

            __m128i lala = _mm_set_epi16(0, 1, 2, 3, 4, 5, 6, 7);

            __m128i tmp = _mm_add_epi16(_mm_set1_epi16(base_idx), lala);
            // __m128i tmp = _mm_add_epi16(_mm_set1_epi16(base_idx), C.rev_idx16);

            // 9-10ns

            __m128i cand = _mm_xor_si128(fp16, tmp);
            // 0ns

            uint32_t mask = _mm_cmpeq_epi16_mask(cand, qfp16_vec);

            mask &= ~((1u << (8 - batch)) - 1u);

            // 0-1ns

            while (mask) {
                // passing this while
                // 7ns for positive
                // 5ns for negative

                int idx = 7 - _tzcnt_u32(mask);
                mask &= mask - 1u;

                // 0-1ns

                uint8_t* bolt_ptr =
                    fp_hi -
                    (static_cast<uint64_t>(idx) << kBoltByteLengthShift) -
                    kFingerprintOffset;
                uint8_t* tiny_ptr = bolt_ptr + kTinyPtrOffset;

                uint64_t deref_key = (cloud_id << kByteShift) | query_fp;

                // 1ns

                uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);

                // 1 ns

                uint64_t entry_mk =
                    (*reinterpret_cast<uint64_t*>(entry + kDropletKeyOffset))
                    << kBoltQuotientingLength;

                // 13ns for positive, makes sense, since around 20% items are bolts
                // 1ns for negative, it accesses some random place in the derefrence table, low lantency comes from the lower possibility of the getting into this branch

                if (entry_mk == bolt_masked_key) {
                    // adding the branch only
                    // 9ns for positive
                    // 3ns for negative

                    *value_ptr = *reinterpret_cast<uint64_t*>(
                        entry + kDropletValueOffset);

                    // 1-2ns

                    if ((ver->load() != v0))
                        goto retry;
                    // 0ns

                    return true;
                }
            }

            fp_hi -= kBoltByteLength * 8;
            base_idx += batch;
            remaining -= batch;
            // } while (0);
        } while (remaining);
        // 6ns for postive not using 0
        // 2ns for not using 0
    }

    //  ───────────────────────────────────────── Miss / version check ─────────
    if ((ver->load() != v0))
        goto retry;
    return false;
}



bool BoltHT::Update(uint64_t key, uint64_t value) {

    return 1;
}

void BoltHT::Free(uint64_t key) {}

void BoltHT::Scan4Stats() {
    uint64_t total_slots = kCloudNum * kCloudByteLength;
    uint64_t used_slots = 0;
    uint64_t empty_slots = 0;

    uint8_t* cloud = cloud_tab;
    uint8_t* byte_array = byte_array;
    uint8_t* bin_cnt_head = bin_cnt_head;

    uint64_t crystal_cnt_hist[128] = {0};
    uint64_t bolt_cnt_hist[128] = {0};
    uint64_t total_bolt_cnt = 0;
    uint64_t total_crystal_cnt = 0;

    for (uint64_t i = 0; i < kCloudNum; i++) {
        uint8_t control_info = cloud[kControlOffset];
        uint8_t crystal_cnt = control_info & kControlCrystalMask;
        uint8_t bolt_cnt = (control_info >> kControlBoltShift);

        crystal_cnt_hist[crystal_cnt]++;
        bolt_cnt_hist[bolt_cnt]++;
        total_bolt_cnt += bolt_cnt;
        total_crystal_cnt += crystal_cnt;

        cloud += kCloudByteLength;
    }

    std::cout << "kCloudNum: " << kCloudNum << std::endl;
    std::cout << "Total crystal cnt: " << total_crystal_cnt << std::endl;
    std::cout << "Total bolt cnt: " << total_bolt_cnt << std::endl;

    std::cout << "Crystal cnt hist: " << std::endl;
    for (uint64_t i = 0; i < 128; i++) {
        if (crystal_cnt_hist[i] > 0) {
            std::cout << "Crystal cnt: " << i
                      << " count: " << crystal_cnt_hist[i] << std::endl;
        }
    }
    std::cout << std::endl;

    std::cout << "Bolt cnt hist: " << std::endl;
    for (uint64_t i = 0; i < 128; i++) {
        if (bolt_cnt_hist[i] > 0) {
            std::cout << "Bolt cnt: " << i << " count: " << bolt_cnt_hist[i]
                      << std::endl;
        }
    }
    std::cout << std::endl;
}

}  // namespace tinyptr