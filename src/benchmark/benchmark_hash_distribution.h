#include "benchmark/benchmark_object_64.h"
#include "common.h"
#include <atomic>

namespace tinyptr {

class BenchmarkHashDistribution : public BenchmarkObject64 {
   public:
    static const BenchmarkObjectType TYPE;

   public:
    BenchmarkHashDistribution(uint64_t n);
    ~BenchmarkHashDistribution() = default;

   public:
    uint8_t Insert(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key, uint8_t ptr);
    void Update(uint64_t key, uint8_t ptr, uint64_t value);
    void Erase(uint64_t key, uint8_t ptr);

    void Concurrent_Simulation(int num_threads);
    std::vector<std::pair<uint64_t, uint64_t> > Occupancy_Distribution();

   private:
    __attribute__((always_inline)) inline uint64_t hash(
        uint64_t key) {
        uint64_t tmp = key >> kQuotientingTailLength;
        return (XXH64(&tmp, sizeof(uint64_t), kHashSeed1) ^ key) &
               kQuotientingTailMask;
    }
   private:
    uint64_t op_cnt;
    uint64_t kHashSeed1;
    uint8_t kQuotientingTailLength;
    uint64_t kQuotientingTailMask;
    std::vector<std::atomic<uint64_t> > bin_vec;
    std::vector<std::atomic<uint64_t> > cnt_vec;
    
};

}  // namespace tinyptr