#pragma once

#include <sys/types.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "blast_ht.h"
#include "concurrent_byte_array_chained_ht.h"
#include "concurrent_skulker_ht.h"

namespace tinyptr {

template <typename HTType>
class ResizableHT {
   public:
    const uint64_t kHashSeed;
    static constexpr uint64_t kCacheLineSize = 64;

    // Cache line calculations - must be compile-time constants
    static constexpr uint64_t kCacheLineUint64Count = 8;
    static constexpr uint64_t kCacheLineInt64Count = 8;
    static constexpr uint64_t kInt64toCacheLineShift = 3;

    // Dynamic hash multiplier - randomized per instance for attack resistance
    uint64_t partition_hash_multiplier;
    uint64_t partition_mask;  // part_num - 1, for fast bitwise AND

    // Compile-time assertions to ensure compatibility
    static_assert(
        kCacheLineSize == 64,
        "Cache line size must be 64 bytes for current implementation");
    static_assert(sizeof(uint64_t) == 8, "uint64_t must be 8 bytes");
    static_assert(sizeof(int64_t) == 8, "int64_t must be 8 bytes");
    static_assert((1ULL << kInt64toCacheLineShift) == kCacheLineUint64Count,
                  "Cache line shift must match uint64 count");

   public:
    ResizableHT(uint64_t initial_size_per_part = 40000, uint64_t part_num = 0,
                uint32_t thread_num = 0, double resize_threshold = 0.7,
                double resize_factor = 2.0);

   protected:
    HTType** partitions;
    HTType** partitions_new;
    uint64_t part_num;
    uint64_t initial_size_per_part;
    double resize_threshold;
    double resize_factor;
    uint64_t thread_num;
    uint64_t stride_num;
    uint64_t* part_size;
    int64_t* part_resize_threshold;
    std::atomic<int64_t>* part_cnt;
    std::atomic<uint64_t>* part_resizing_thread_num;
    std::atomic<uint64_t>* part_resizing_stage;
    std::atomic<uint64_t>* part_resizing_stride_done;
    std::atomic<uint64_t>* thread_working_lock;
    int64_t** thread_part_cnt;

   private:
    std::set<uint64_t> free_handle;
    std::mutex handle_mutex;

   public:
    bool Insert(uint64_t handle, uint64_t key, uint64_t value);
    bool Query(uint64_t handle, uint64_t key, uint64_t* value_ptr);
    bool Update(uint64_t handle, uint64_t key, uint64_t value);
    void Erase(uint64_t handle, uint64_t key);

    __attribute__((always_inline)) inline uint64_t GetHandle() {
        std::lock_guard<std::mutex> lock(handle_mutex);
        uint64_t handle = *free_handle.begin();
        free_handle.erase(handle);
        thread_part_cnt[handle] = new int64_t[part_num + kCacheLineInt64Count];
        memset(thread_part_cnt[handle], 0,
               (part_num + kCacheLineInt64Count) * sizeof(int64_t));
        return handle;
    }

    __attribute__((always_inline)) inline void FreeHandle(uint64_t handle) {

        for (uint64_t part_id = 0; part_id < part_num; part_id++) {
            uint64_t part_index = part_id << kInt64toCacheLineShift;
            part_cnt[part_index].fetch_add(thread_part_cnt[handle][part_id]);
        }

        delete[] thread_part_cnt[handle];
        thread_part_cnt[handle] = nullptr;
        std::lock_guard<std::mutex> lock(handle_mutex);
        free_handle.insert(handle);
    }

   private:
    __attribute__((always_inline)) inline uint64_t get_part_id(uint64_t key) {
        // return XXH64(&key, sizeof(uint64_t), kHashSeed) & (part_num - 1);
        key ^= kHashSeed;
        key *= partition_hash_multiplier;
        return (key >> 32) & partition_mask;
    }

    __attribute__((always_inline)) inline void check_join_resize(
        uint64_t handle, uint64_t part_id) {

        uint64_t part_index = part_id << kInt64toCacheLineShift;

        while (true) {
            uint64_t resizing_thread_num =
                part_resizing_thread_num[part_index].load();

            if (resizing_thread_num == 0) {
                return;
            } else if (part_resizing_thread_num[part_index]
                           .compare_exchange_weak(resizing_thread_num,
                                                  resizing_thread_num + 1)) {

                thread_working_lock[handle].store(uint64_t(-1));

                while (true) {

                    uint64_t stage = part_resizing_stage[part_index].load();

                    if (stage > 0 && stage < stride_num) {
                        if (part_resizing_stage[part_index]
                                .compare_exchange_weak(stage, stage + 1)) {

                            partitions[part_id]->ResizeMoveStride(
                                stage, partitions_new[part_id]);
                            part_resizing_stride_done[part_index]++;
                        }
                    } else if (stage >= stride_num) {
                        part_resizing_thread_num[part_index].fetch_sub(1);
                        while (part_resizing_thread_num[part_index].load() > 0)
                            ;
                        thread_working_lock[handle].store(part_id);
                        break;
                    }
                }
            }
        }
    }

    __attribute__((always_inline)) inline void check_start_resize(
        uint64_t handle, uint64_t part_id) {

        uint64_t part_index = part_id << kInt64toCacheLineShift;

        if (std::abs(thread_part_cnt[handle][part_id]) >
            thread_num * thread_num) {
            part_cnt[part_index].fetch_add(thread_part_cnt[handle][part_id]);
            thread_part_cnt[handle][part_id] = 0;
        }

        if (part_cnt[part_index].load() > part_resize_threshold[part_id]) {
            uint64_t expected = 0;
            // std::cerr << "start resize" << std::endl;

            if (part_resizing_thread_num[part_index].compare_exchange_weak(
                    expected, uint64_t(1))) {

                if (part_cnt[part_index].load() <=
                    part_resize_threshold[part_id]) {
                    part_resizing_stage[part_index].store(stride_num);

                    uint64_t expected = 1;
                    while (!part_resizing_thread_num[part_index]
                                .compare_exchange_weak(expected, 0)) {
                        expected = 1;
                    }

                    part_resizing_stage[part_index].store(0);
                    return;
                }

                // auto start_time = std::chrono::high_resolution_clock::now();

                thread_working_lock[handle].store(uint64_t(-1));

                for (uint64_t i = 0; i < thread_num; i++) {
                    while (thread_working_lock[i << kInt64toCacheLineShift]
                               .load() == part_id)
                        ;
                }

                // std::chrono::high_resolution_clock::time_point my_time[10];
                // my_time[0] = std::chrono::high_resolution_clock::now();

                partitions_new[part_id] = new HTType(
                    uint64_t(part_size[part_id] * resize_factor), true);

                // std::cerr << "allocated partitions_new[" << part_id
                //   << "]: " << partitions_new[part_id] << std::endl;

                partitions[part_id]->SetResizeStride(stride_num);

                uint64_t stage = part_resizing_stage[part_index].load();

                // my_time[1] = std::chrono::high_resolution_clock::now();

                while (stage < stride_num) {
                    if (part_resizing_stage[part_index].compare_exchange_weak(
                            stage, stage + 1)) {
                        partitions[part_id]->ResizeMoveStride(
                            stage, partitions_new[part_id]);
                        part_resizing_stride_done[part_index]++;
                    }
                }

                // my_time[2] = std::chrono::high_resolution_clock::now();

                while (part_resizing_stride_done[part_index].load() <
                       stride_num)
                    ;

                // my_time[3] = std::chrono::high_resolution_clock::now();

                HTType* tmp = partitions[part_id];
                partitions[part_id] = partitions_new[part_id];

                for (uint64_t i = 0; i < thread_num; i++) {
                    while (thread_working_lock[i << kInt64toCacheLineShift]
                               .load() == part_id)
                        ;
                }

                // std::cerr << "delete old partitions[" << part_id << "]: " << tmp
                //           << std::endl;

                delete tmp;
                partitions_new[part_id] = nullptr;
                part_size[part_id] =
                    uint64_t(part_size[part_id] * resize_factor);
                part_resize_threshold[part_id] =
                    uint64_t(part_size[part_id] * resize_threshold);

                // std::cerr << "part_cnt: " << part_cnt[part_index].load()
                //           << std::endl;
                // std::cerr << "part_size: " << part_size[part_id] << std::endl;
                // std::cerr << "part_resize_threshold: "
                //           << part_resize_threshold[part_id] << std::endl;

                part_resizing_stride_done[part_index].store(0);

                thread_working_lock[handle].store(part_id);

                // my_time[4] = std::chrono::high_resolution_clock::now();

                uint64_t expected = 1;
                while (
                    !part_resizing_thread_num[part_index].compare_exchange_weak(
                        expected, 0)) {
                    expected = 1;
                }

                part_resizing_stage[part_index].store(0);

                // my_time[5] = std::chrono::high_resolution_clock::now();

                // for (int i = 0; i < 5; i++) {
                //     std::cerr << "time " << i << ": "
                //               << std::chrono::duration_cast<
                //                      std::chrono::milliseconds>(my_time[i + 1] -
                //                                                 my_time[i])
                //                      .count()
                //               << "ms" << std::endl;
                // }
            }
        }
    };
};

template <typename HTType>
ResizableHT<HTType>::ResizableHT(uint64_t initial_size_per_part_,
                                 uint64_t part_num_, uint32_t thread_num_,
                                 double resize_threshold_,
                                 double resize_factor_)
    : initial_size_per_part(initial_size_per_part_),
      part_num(part_num_),
      thread_num(thread_num_),
      resize_threshold(resize_threshold_),
      resize_factor(resize_factor_),
      kHashSeed(rand() & ((1 << 16) - 1)) {

    if (thread_num == 0) {
        thread_num = std::max(uint32_t(4),
                              uint32_t(std::thread::hardware_concurrency()));
    }

    stride_num = thread_num * thread_num;
    // stride_num = thread_num;

    if (part_num == 0) {
        part_num = std::max(uint64_t(100), thread_num + 1);
    }

    uint64_t tmp_part_num = 1;
    while (tmp_part_num < part_num) {
        tmp_part_num <<= 1;
    }
    part_num = tmp_part_num;

    // Initialize hash parameters - randomized multiplier for attack resistance
    partition_hash_multiplier =
        kHashSeed | 1;  // Ensure odd number for good distribution
    partition_hash_multiplier = (partition_hash_multiplier << 32) |
                                (rand() | 1);  // 64-bit odd multiplier
    partition_mask = part_num - 1;  // Fast bitwise mask for power-of-2

    partitions = new HTType*[part_num];
    partitions_new = new HTType*[part_num];
    part_size = new uint64_t[part_num];
    part_resize_threshold = new int64_t[part_num];

    for (uint64_t i = 0; i < part_num; i++) {
        double frac = (part_num <= 1) ? 0.0 : (double)i / (double)(part_num - 1);
        // double sized = initial_size_per_part * (1 + resize_factor * frac);
        double sized = initial_size_per_part * std::pow(resize_factor, frac);
        uint64_t size_i = std::max<uint64_t>(1, static_cast<uint64_t>(sized));
        // size_i = initial_size_per_part;

        partitions[i] = new HTType(size_i, true);
        part_size[i] = size_i;
        part_resize_threshold[i] = static_cast<int64_t>(size_i * resize_threshold);
    }

    part_cnt = new std::atomic<int64_t>[part_num << kInt64toCacheLineShift];
    part_resizing_thread_num =
        new std::atomic<uint64_t>[part_num << kInt64toCacheLineShift];
    part_resizing_stage =
        new std::atomic<uint64_t>[part_num << kInt64toCacheLineShift];
    part_resizing_stride_done =
        new std::atomic<uint64_t>[part_num << kInt64toCacheLineShift];
    thread_working_lock =
        new std::atomic<uint64_t>[thread_num << kInt64toCacheLineShift];
    thread_part_cnt = new int64_t*[thread_num << kInt64toCacheLineShift];

    for (uint64_t i = 0; i < part_num; i++) {
        part_cnt[i << kInt64toCacheLineShift] = 0;
        part_resizing_thread_num[i << kInt64toCacheLineShift] = 0;
        part_resizing_stride_done[i << kInt64toCacheLineShift] = 0;
        part_resizing_stage[i << kInt64toCacheLineShift] = 0;
    }

    for (uint64_t i = 0; i < thread_num; i++) {
        thread_working_lock[i << kInt64toCacheLineShift] = uint64_t(-1);
        free_handle.insert(i << kInt64toCacheLineShift);
    }
}

template <typename HTType>
bool ResizableHT<HTType>::Insert(uint64_t handle, uint64_t key,
                                 uint64_t value) {
    uint64_t part_id = get_part_id(key);
    thread_working_lock[handle].store(part_id);

    check_join_resize(handle, part_id);
    check_start_resize(handle, part_id);

    auto res = partitions[part_id]->Insert(key, value);
    thread_part_cnt[handle][part_id] += res;

    thread_working_lock[handle].store(uint64_t(-1));
    return res;
}

template <typename HTType>
bool ResizableHT<HTType>::Query(uint64_t handle, uint64_t key,
                                uint64_t* value_ptr) {
    uint64_t part_id = get_part_id(key);
    thread_working_lock[handle].store(part_id);

    // check_join_resize(handle, part_id);

    auto res = partitions[part_id]->Query(key, value_ptr);

    thread_working_lock[handle].store(uint64_t(-1));
    return res;
}

template <typename HTType>
bool ResizableHT<HTType>::Update(uint64_t handle, uint64_t key,
                                 uint64_t value) {
    uint64_t part_id = get_part_id(key);
    thread_working_lock[handle].store(part_id);

    check_join_resize(handle, part_id);

    auto res = partitions[part_id]->Update(key, value);

    thread_working_lock[handle].store(uint64_t(-1));
    return res;
}

template <typename HTType>
void ResizableHT<HTType>::Erase(uint64_t handle, uint64_t key) {
    uint64_t part_id = get_part_id(key);
    thread_working_lock[handle].store(part_id);

    check_join_resize(handle, part_id);

    partitions[part_id]->Free(key);
    thread_part_cnt[handle][part_id]--;
    thread_working_lock[handle].store(uint64_t(-1));
}

using ResizableSkulkerHT = ResizableHT<ConcurrentSkulkerHT>;
using ResizableByteArrayChainedHT = ResizableHT<ConcurrentByteArrayChainedHT>;
using ResizableBlastHT = ResizableHT<BlastHT>;

}  // namespace tinyptr