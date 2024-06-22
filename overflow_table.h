#pragma once

#include <map>

namespace tinyptr {
class OverflowTable {
   public:
    OverflowTable() = default;
    ~OverflowTable() = default;

    uint8_t Allocate(uint64_t key, uint64_t value);
    bool Update(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);
    bool Free(uint64_t key);

   private:
    std::map<uint64_t, uint64_t> tab;
}
}  // namespace tinyptr