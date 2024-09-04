#pragma once

#include <cstdint>
#include <cstdlib>
#include <map>
#include <unordered_map>

namespace tinyptr {
class ByteArrayOverflowTable {
    //    private:
    // struct KV {
    //     uint64_t key, value;
    // };

   public:
    ByteArrayOverflowTable() = default;
    ~ByteArrayOverflowTable() = default;

   public:
    uint8_t Allocate(uint64_t key, uint64_t value);
    void Update(uint64_t key, uint64_t value);
    void Query(uint64_t key, uint64_t* value_ptr);
    uint64_t Query(uint64_t key);
    void Free(uint64_t key);

   private:
    std::unordered_map<uint64_t, uint64_t> tab;
};
}  // namespace tinyptr