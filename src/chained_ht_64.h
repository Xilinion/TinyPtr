#include <functional>
#include "common.h"
#include "dereference_table_64.h"

namespace tinyptr {

class ChainedHT64 {
   public:
    static constexpr uint64_t kQuotientingTailSize = 20;
    static constexpr uint64_t kQuotientingHeadSize = 44;

   public:
    ChainedHT64() = delete;
    ChainedHT64(int n);
    ~ChainedHT64() = default;

   private:
    uint32_t get_bin_num(uint64_t key);
    uint64_t encode_key(uint64_t key);
    uint64_t decode_key(uint64_t quot_key, uint32_t bin_num);

   public:
    bool ContainsKey(uint64_t key);

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Erase(uint64_t key);
    bool Update(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key);
    // TODO: or not todo? supporting iterator & key/value set & vectorized operators

   private:
    std::function<uint32_t(uint64_t)> quot_head_hash;
    DereferenceTable64* deref_tab;
    uint8_t* quot_tab;
};
}  // namespace tinyptr