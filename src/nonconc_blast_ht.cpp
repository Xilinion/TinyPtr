#include "nonconc_blast_ht.h"
#include <immintrin.h>
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

// #define USE_CONCURRENT_VERSION_QUERY

uint8_t NonConcBlastHT::AutoFastDivisionInnerShift(uint64_t divisor) {
    uint8_t res = 0;
    while ((1ULL << res) < divisor) {
        res++;
    }
    return res;
}

uint8_t NonConcBlastHT::AutoQuotTailLength(uint64_t size) {
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

NonConcBlastHT::NonConcBlastHT(uint64_t size, uint8_t quotienting_tail_length,
                 uint16_t bin_size, bool if_resize)
    : kHashSeed1(rand() & ((1 << 16) - 1)),
      kHashSeed2(65536 + rand()),
      kCloudQuotientingLength(quotienting_tail_length
                                  ? quotienting_tail_length
                                  : AutoQuotTailLength(size)),
      kBlastQuotientingLength(kCloudQuotientingLength + kByteShift),
      kBlastQuotientingMask((1ULL << kBlastQuotientingLength) - 1),
      kQuotientingTailMask((1ULL << kCloudQuotientingLength) - 1),
      kQuotKeyByteLength(((64 + 7 - kCloudQuotientingLength) >> 3) - 1),
      kEntryByteLength(kQuotKeyByteLength + 8),
      kCrystalOffset(kControlOffset - kEntryByteLength),
      kBinByteLength(kBinSize * kEntryByteLength),
      kCloudNum(1ULL << kCloudQuotientingLength),
      kBinSize(bin_size),
      kBinNum((size * kCloudOverflowBound + kBinSize - 1) / kBinSize),
      kValueOffset(kKeyOffset + kQuotKeyByteLength),
      kFastDivisionShift{
          static_cast<uint8_t>(
              kFastDivisionUpperBoundLog +
              AutoFastDivisionInnerShift(kEntryByteLength)) /*not used*/,
          static_cast<uint8_t>(kFastDivisionUpperBoundLog +
                               AutoFastDivisionInnerShift(kBinNum))},
      kFastDivisionReciprocal{
          (1ULL << kFastDivisionShift[0]) / kEntryByteLength + 1 /*not used*/,
          (1ULL << kFastDivisionShift[1]) / kBinNum + 1} {

    assert(size / 2 >= (1ULL << (kCloudQuotientingLength)));
    assert(bin_size < 128);

    // Determine the number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
        num_threads =
            4;  // Fallback to a default value if hardware_concurrency is not available
    }

    // Calculate individual sizes
    uint64_t cloud_size = kCloudNum * kCloudByteLength;
    uint64_t byte_array_size = kBinNum * kBinSize * kEntryByteLength;
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

    if (if_resize) {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    } else {
        combined_mem = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
                            MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
    }

    // Assign pointers to their respective regions
    uint8_t* base =
        (uint8_t*)((uint64_t)(combined_mem + 63) & ~static_cast<uint64_t>(63));
    cloud_tab = base;
    base += cloud_size_aligned;

    byte_array = base;
    base += byte_array_size_aligned;

    bin_cnt_head = base;
}

NonConcBlastHT::NonConcBlastHT(uint64_t size, uint16_t bin_size, bool if_resize)
    : NonConcBlastHT(size, 0, bin_size, if_resize) {}

NonConcBlastHT::NonConcBlastHT(uint64_t size, bool if_resize)
    : NonConcBlastHT(size, 0, 127, if_resize) {}

NonConcBlastHT::~NonConcBlastHT() {
    munmap(cloud_tab, kCloudNum * kCloudByteLength);
    munmap(byte_array, kBinNum * kBinSize * kEntryByteLength);
    munmap(bin_cnt_head, kBinNum << 1);
}

bool NonConcBlastHT::Insert(uint64_t key, uint64_t value) {

    uint64_t truncated_key = key >> kBlastQuotientingLength;
    uint64_t cloud_id =
        ((HASH_FUNCTION(&truncated_key, sizeof(uint64_t), kHashSeed1) ^ key) &
         kBlastQuotientingMask);
    uint8_t fp = cloud_id >> kCloudQuotientingLength;
    cloud_id = cloud_id & kQuotientingTailMask;

    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    uint8_t& control_info = cloud[kControlOffset];
    uint8_t crystal_cnt = control_info & kControlCrystalMask;
    uint8_t tp_cnt = (control_info >> kControlTinyPtrShift);
    uint8_t fp_cnt = crystal_cnt + tp_cnt;

    uint8_t crystal_end = kControlOffset - crystal_cnt * kEntryByteLength;

    if (crystal_end - kEntryByteLength >= fp_cnt + tp_cnt + 1) {

        crystal_end -= kEntryByteLength;

        cloud[kFingerprintOffset + fp_cnt] = fp;

        reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0] =
            truncated_key;
        reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0] =
            value;

        control_info++;
        return true;
    } else if (crystal_end >= fp_cnt + tp_cnt + 2) {

        uint8_t* tiny_ptr = cloud + crystal_end - tp_cnt - 1;
        cloud[kFingerprintOffset + fp_cnt] = fp;

        bool result = ptab_insert(tiny_ptr, (cloud_id << kByteShift) | fp,
                                  truncated_key, value);

        control_info += (result << kControlTinyPtrShift);
        return result;
    } else {

        if (__builtin_expect(crystal_cnt == 0, 0)) {
            return false;
        }

        uint64_t last_crystal_trunced_key =
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0];
        uint64_t last_crystal_value =
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0];

        memmove(cloud + crystal_end + kEntryByteLength - tp_cnt - 1,
                cloud + crystal_end - tp_cnt, tp_cnt);

        crystal_end += kEntryByteLength;

        uint8_t* tiny_ptr = cloud + crystal_end - 1;
        uint8_t last_crystal_fp = cloud[kFingerprintOffset + crystal_cnt - 1];

        if (ptab_insert(tiny_ptr, (cloud_id << kByteShift) | last_crystal_fp,
                        last_crystal_trunced_key, last_crystal_value)) {

            cloud[kFingerprintOffset + fp_cnt] = fp;
            tiny_ptr -= tp_cnt + 1;

            if (ptab_insert(tiny_ptr, (cloud_id << kByteShift) | fp,
                            truncated_key, value)) {

                control_info--;
                uint8_t inc_cnt = 2;
                control_info += (inc_cnt << kControlTinyPtrShift);
                return true;
            } else {
                memmove(cloud + crystal_end - kEntryByteLength - tp_cnt,
                        cloud + crystal_end - tp_cnt - 1,

                        tp_cnt);

                crystal_end -= kEntryByteLength;

                reinterpret_cast<uint64_t*>(cloud + crystal_end +
                                            kKeyOffset)[0] =
                    last_crystal_trunced_key;
                reinterpret_cast<uint64_t*>(
                    cloud + crystal_end + kValueOffset)[0] = last_crystal_value;
                return false;
            }

        } else {
            memmove(cloud + crystal_end - kEntryByteLength - tp_cnt,
                    cloud + crystal_end - tp_cnt - 1, tp_cnt);

            crystal_end -= kEntryByteLength;

            reinterpret_cast<uint64_t*>(cloud + crystal_end + kKeyOffset)[0] =
                last_crystal_trunced_key;
            reinterpret_cast<uint64_t*>(cloud + crystal_end + kValueOffset)[0] =
                last_crystal_value;

            return false;
        }
    }
}

bool NonConcBlastHT::Query(uint64_t key, uint64_t* value_ptr) {
    uint64_t truncated_key = key >> kBlastQuotientingLength;
    const uint64_t h =
        HASH_FUNCTION(&truncated_key, sizeof(uint64_t), kHashSeed1) ^ key;
    const uint64_t qbits = (h & kBlastQuotientingMask);
    const uint64_t cloud_id = qbits & kQuotientingTailMask;

    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    uint8_t& control_info = cloud[kControlOffset];
    uint32_t tp_cnt = (control_info >> kControlTinyPtrShift);
    uint32_t crystal_cnt = control_info & kControlCrystalMask;
    uint32_t fp_cnt = tp_cnt + crystal_cnt;

    const uint8_t fp = qbits >> kCloudQuotientingLength;
    __m256i fp_dup_vec = _mm256_set1_epi8(fp);
    __m256i fp_vec = _mm256_load_si256(
		    reinterpret_cast<__m256i*>(cloud + kFingerprintOffset));

    const __mmask32 kkeep = static_cast<__mmask32>(_bzhi_u32(0xFFFF'FFFFu, fp_cnt));

    __mmask32 mask = _mm256_mask_cmpeq_epi8_mask(kkeep, fp_vec, fp_dup_vec);

    uint32_t crystal_begin = kControlOffset - kEntryByteLength;

    uint64_t masked_key = truncated_key << kBlastQuotientingLength;
    while (mask) {

        uint8_t i = __builtin_ctz(mask);
        mask &= ~(1u << i);

        if (i < crystal_cnt) {
            if (reinterpret_cast<uint64_t*>(cloud + crystal_begin -
                                            i * kEntryByteLength +
                                            kKeyOffset)[0]
                    << kBlastQuotientingLength ==
                masked_key) {
                *value_ptr = reinterpret_cast<uint64_t*>(cloud + crystal_begin -
                                                         i * kEntryByteLength +
                                                         kValueOffset)[0];
                return true;
            }
        } else {
            uint8_t crystal_end =
                kControlOffset - kEntryByteLength * crystal_cnt;
            uint8_t* tiny_ptr = cloud + crystal_end - i + crystal_cnt - 1;
            uint64_t deref_key = (cloud_id << kByteShift) | fp;
            uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);

            uint64_t entry_masked_key =
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset))
                << kBlastQuotientingLength;

            if (entry_masked_key == masked_key) {
                *value_ptr = *reinterpret_cast<uint64_t*>(entry + kValueOffset);

                return true;
            }
        }
    }
    // } while (mask);

    return false;
}

bool NonConcBlastHT::Update(uint64_t key, uint64_t value) {

    uint64_t truncated_key = key >> kBlastQuotientingLength;
    uint64_t masked_key = truncated_key << kBlastQuotientingLength;
    uint64_t cloud_id =
        ((HASH_FUNCTION(&truncated_key, sizeof(uint64_t), kHashSeed1) ^ key) &
         kBlastQuotientingMask);
    uint8_t fp = cloud_id >> kCloudQuotientingLength;
    cloud_id = cloud_id & kQuotientingTailMask;

    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    uint8_t& control_info = cloud[kControlOffset];
    uint8_t crystal_cnt = control_info & kControlCrystalMask;
    uint8_t tp_cnt = (control_info >> kControlTinyPtrShift);
    uint8_t fp_cnt = crystal_cnt + tp_cnt;

    uint8_t crystal_begin = kControlOffset - kEntryByteLength;

    // __m256i revert_mask = _mm256_set_epi8(
    //     31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14,
    //     13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    __m256i fp_vec = _mm256_loadu_si256(
        reinterpret_cast<__m256i*>(cloud + kFingerprintOffset));

    // fp_vec = _mm256_xor_si256(fp_vec, revert_mask);
    __mmask32 mask = _mm256_cmpeq_epi8_mask(fp_vec, _mm256_set1_epi8(fp));

    mask &= ((1u << (fp_cnt)) - 1u);

    while (mask) {

        uint8_t i = __builtin_ctz(mask);
        mask &= ~(1u << i);

        if (i < crystal_cnt) {
            if (reinterpret_cast<uint64_t*>(cloud + crystal_begin -
                                            i * kEntryByteLength +
                                            kKeyOffset)[0]
                    << kBlastQuotientingLength ==
                masked_key) {
                reinterpret_cast<uint64_t*>(cloud + crystal_begin -
                                            i * kEntryByteLength +
                                            kValueOffset)[0] = value;
                return true;
            }
        } else {
            uint8_t crystal_end =
                kControlOffset - kEntryByteLength * crystal_cnt;
            uint8_t* tiny_ptr = cloud + crystal_end - i + crystal_cnt - 1;
            uint64_t deref_key = (cloud_id << kByteShift) | fp;
            uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);

            uint64_t entry_masked_key =
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset))
                << kBlastQuotientingLength;

            if (entry_masked_key == masked_key) {
                *reinterpret_cast<uint64_t*>(entry + kValueOffset) = value;
                return true;
            }
        }
    }

    return false;
}

void NonConcBlastHT::Free(uint64_t key) {

    uint64_t truncated_key = key >> kBlastQuotientingLength;
    uint64_t masked_key = truncated_key << kBlastQuotientingLength;
    uint64_t cloud_id =
        ((HASH_FUNCTION(&truncated_key, sizeof(uint64_t), kHashSeed1) ^ key) &
         kBlastQuotientingMask);
    uint8_t fp = cloud_id >> kCloudQuotientingLength;
    cloud_id = cloud_id & kQuotientingTailMask;

    uint8_t* cloud = &cloud_tab[(cloud_id << kCloudIdShiftOffset)];

    uint8_t& control_info = cloud[kControlOffset];
    uint8_t crystal_cnt = control_info & kControlCrystalMask;
    uint8_t tp_cnt = (control_info >> kControlTinyPtrShift);
    uint8_t fp_cnt = crystal_cnt + tp_cnt;

    uint8_t crystal_begin = kControlOffset - kEntryByteLength;
    uint8_t crystal_end = kControlOffset - kEntryByteLength * crystal_cnt;

    __m256i fp_vec = _mm256_loadu_si256(
        reinterpret_cast<__m256i*>(cloud + kFingerprintOffset));

    __mmask32 mask = _mm256_cmpeq_epi8_mask(fp_vec, _mm256_set1_epi8(fp));

    mask &= ((1u << (fp_cnt)) - 1u);

    while (mask) {

        uint8_t i = __builtin_ctz(mask);
        mask &= ~(1u << i);

        if (i < crystal_cnt) {
            if (reinterpret_cast<uint64_t*>(cloud + crystal_begin -
                                            i * kEntryByteLength +
                                            kKeyOffset)[0]
                    << kBlastQuotientingLength ==
                masked_key) {

                if (tp_cnt > 0) {

                    uint8_t j = fp_cnt - 1;
                    cloud[kFingerprintOffset + i] =
                        cloud[kFingerprintOffset + j];

                    uint64_t deref_key = (cloud_id << kByteShift) |
                                         cloud[kFingerprintOffset + j];

                    uint8_t* tiny_ptr = cloud + crystal_end - tp_cnt;
                    uint8_t* entry =
                        ptab_query_entry_address(deref_key, *tiny_ptr);
                    uint64_t entry_key_word =
                        (*reinterpret_cast<uint64_t*>(entry + kKeyOffset));
                    uint64_t entry_value_word =
                        *reinterpret_cast<uint64_t*>(entry + kValueOffset);

                    std::memcpy(cloud + crystal_begin - i * kEntryByteLength,
                                entry, kEntryByteLength);

                    ptab_free(tiny_ptr, deref_key);

                    control_info -= (1 << kControlTinyPtrShift);
                    tp_cnt--;
                    fp_cnt--;

                    if (crystal_end - kEntryByteLength >= fp_cnt + tp_cnt - 1 &&
                        tp_cnt > 0) {
                        memmove(cloud + crystal_end - kEntryByteLength -
                                    (tp_cnt - 1),
                                cloud + crystal_end - tp_cnt, tp_cnt - 1);

                        uint8_t* tiny_ptr = cloud + crystal_end - 1;
                        uint64_t deref_key =
                            (cloud_id << kByteShift) |
                            cloud[kFingerprintOffset + crystal_cnt];
                        uint8_t* entry =
                            ptab_query_entry_address(deref_key, *tiny_ptr);

                        uint8_t tmp_ptr = *tiny_ptr;

                        std::memcpy(cloud + crystal_end - kEntryByteLength,
                                    entry, kEntryByteLength);

                        ptab_free(&tmp_ptr, deref_key);

                        control_info++;
                        control_info -= (1 << kControlTinyPtrShift);
                    }

                    return;

                } else {
                    uint8_t j = crystal_cnt - 1;
                    std::memcpy(cloud + crystal_begin - i * kEntryByteLength,
                                cloud + crystal_begin - j * kEntryByteLength,
                                kEntryByteLength);

                    cloud[kFingerprintOffset + i] =
                        cloud[kFingerprintOffset + j];

                    control_info--;

                    return;
                }
            }
        } else {
            uint8_t crystal_end =
                kControlOffset - kEntryByteLength * crystal_cnt;
            uint8_t* tiny_ptr = cloud + crystal_end - i + crystal_cnt - 1;
            uint64_t deref_key = (cloud_id << kByteShift) | fp;
            uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);

            uint64_t entry_masked_key =
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset))
                << kBlastQuotientingLength;

            if (entry_masked_key == masked_key) {

                ptab_free(tiny_ptr, deref_key);

                uint8_t j = fp_cnt - 1;

                cloud[kFingerprintOffset + i] = cloud[kFingerprintOffset + j];
                *tiny_ptr = cloud[crystal_end - tp_cnt];

                control_info -= (1 << kControlTinyPtrShift);
                tp_cnt--;
                fp_cnt--;

                if (crystal_end - kEntryByteLength >= fp_cnt + tp_cnt - 1 &&
                    tp_cnt > 0) {
                    memmove(
                        cloud + crystal_end - kEntryByteLength - (tp_cnt - 1),
                        cloud + crystal_end - tp_cnt, tp_cnt - 1);

                    uint8_t* tiny_ptr = cloud + crystal_end - 1;
                    uint64_t deref_key =
                        (cloud_id << kByteShift) |
                        cloud[kFingerprintOffset + crystal_cnt];
                    uint8_t* entry =
                        ptab_query_entry_address(deref_key, *tiny_ptr);

                    uint8_t tmp_ptr = *tiny_ptr;

                    std::memcpy(cloud + crystal_end - kEntryByteLength, entry,
                                kEntryByteLength);

                    ptab_free(&tmp_ptr, deref_key);

                    control_info++;
                    control_info -= (1 << kControlTinyPtrShift);
                }

                return;
            }
        }
    }

}

void NonConcBlastHT::SetResizeStride(uint64_t stride_num) {
    resize_stride_size = ceil(1.0 * kCloudNum / (stride_num));
}

bool NonConcBlastHT::ResizeMoveStride(uint64_t stride_id, NonConcBlastHT* new_ht) {

    uint64_t stride_id_start = stride_id * resize_stride_size;
    uint64_t stride_id_end = stride_id_start + resize_stride_size;
    if (stride_id_end > kCloudNum) {
        stride_id_end = kCloudNum;
    }

    std::queue<std::pair<uint64_t, uint64_t>> ins_queue;

    for (uint64_t cloud_id = stride_id_start; cloud_id < stride_id_end;
         cloud_id++) {
        uint8_t* cloud = &cloud_tab[cloud_id << kCloudIdShiftOffset];
        uint8_t& control_info = cloud[kControlOffset];
        uint8_t crystal_cnt = control_info & kControlCrystalMask;
        uint8_t tp_cnt = (control_info >> kControlTinyPtrShift);
        uint8_t fp_cnt = crystal_cnt + tp_cnt;

        uint8_t crystal_end = kControlOffset - kEntryByteLength * crystal_cnt;

        for (uint8_t crystal_iter = 0, moved_cnt = 0;
             crystal_iter < crystal_cnt; crystal_iter++) {

            uint8_t fp = cloud[kFingerprintOffset + crystal_iter];

            uint8_t* entry =
                cloud + kControlOffset - crystal_iter * kEntryByteLength - kEntryByteLength;

            uint64_t ins_key = hash_key_rebuild(
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)), cloud_id,
                fp);

            ins_queue.push(std::make_pair(
                ins_key, *reinterpret_cast<uint64_t*>(entry + kValueOffset)));

            new_ht->prefetch_key(ins_key);
        }

        for (uint8_t tp_iter = 0; tp_iter < tp_cnt; tp_iter++) {

            uint8_t fp = cloud[kFingerprintOffset + tp_iter + crystal_cnt];

            uint64_t deref_key = (cloud_id << kByteShift) | fp;
            uint8_t* tiny_ptr = cloud + crystal_end - tp_iter - 1;

            uint8_t* entry = ptab_query_entry_address(deref_key, *tiny_ptr);

            uint64_t ins_key = hash_key_rebuild(
                (*reinterpret_cast<uint64_t*>(entry + kKeyOffset)), cloud_id,
                fp);
            ins_queue.push(std::make_pair(
                ins_key, *reinterpret_cast<uint64_t*>(entry + kValueOffset)));
            new_ht->prefetch_key(ins_key);
        }

        if (ins_queue.size() >= 10) {
            while (!ins_queue.empty()) {
                auto ins_pair = ins_queue.front();
                ins_queue.pop();
                if (!new_ht->Insert(ins_pair.first, ins_pair.second)) {
                    return false;
                }
            }
        }
    }

    while (!ins_queue.empty()) {
        auto ins_pair = ins_queue.front();
        ins_queue.pop();
        if (!new_ht->Insert(ins_pair.first, ins_pair.second)) {
            return false;
        }
    }

    // // asfasdfasdfasfasdfa
    //     auto end_time = std::chrono::high_resolution_clock::now();

    //     static auto resize_time =
    //         std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
    //                                                               end_time)
    //             .count();

    //     resize_time += std::chrono::duration_cast<std::chrono::milliseconds>(
    //                        end_time - start_time)
    //                        .count();

    //     std::cerr << "Resize time: " << resize_time << "ms" << std::endl;

    return true;
}

void NonConcBlastHT::Scan4Stats() {
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
        uint8_t bolt_cnt = (control_info >> kControlTinyPtrShift);

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
