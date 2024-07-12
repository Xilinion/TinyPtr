#pragma once

#include <cstdint>
#include <cstdlib>
#include <map>
#include <unordered_map>

namespace tinyptr {
class O2VOverflowTable {
   private:
    struct VV {
        uint64_t value_1, value_2;
    };

   public:
    O2VOverflowTable() = default;
    ~O2VOverflowTable() = default;

   public:
    uint8_t Allocate(uint64_t key, uint64_t value_1, uint64_t value_2);
    void UpdateFirst(uint64_t key, uint64_t value);
    void UpdateSecond(uint64_t key, uint64_t value);
    void QueryFirst(uint64_t key, uint64_t* value_ptr);
    uint64_t QueryFirst(uint64_t key);
    void QuerySecond(uint64_t key, uint64_t* value_ptr);
    uint64_t QuerySecond(uint64_t key);
    void Free(uint64_t key);

   private:
    std::unordered_map<uint64_t, VV> tab;
};
}  // namespace tinyptr