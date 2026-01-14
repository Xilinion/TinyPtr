#pragma once

#include <bitset>
#include <cassert>
#include <cstdlib>
#include <map>
#include "common.h"
#include "dereference_table_64.h"
#include "utils/xxhash64.h"

namespace tinyptr {
class Po2CTable {
   private:
    struct KV {
        uint64_t key, value;
    };

    class Bin {
       public:
        Bin();
        ~Bin() = default;

       private:
        friend class Po2CTable;

       public:
        bool full();
        uint8_t count();
        uint8_t find(uint64_t key);
        bool query(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
        bool insert_check(uint64_t key);
        uint8_t insert(uint64_t key, uint64_t value);
        bool update(uint64_t key, uint8_t ptr, uint64_t value);
        bool free(uint64_t key, uint8_t ptr);

       private:
        uint8_t cnt = 0;
        uint8_t head = 1;
        KV bin[DereferenceTable64::kBinSize];
    };

   public:
    Po2CTable() = delete;

    Po2CTable(int n);

    ~Po2CTable() = default;

    uint8_t Allocate(uint64_t key, uint64_t value);
    bool Update(uint64_t key, uint8_t ptr, uint64_t value);
    bool Query(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    bool Free(uint64_t key, uint8_t ptr);

   private:
    uint64_t hash_seed0;
    uint64_t hash_seed1;
    __attribute__((always_inline)) inline uint64_t HashBin0(
        uint64_t key) const {
        return HASH_FUNCTION(&key, sizeof(uint64_t), hash_seed0) % bin_num;
    }
    __attribute__((always_inline)) inline uint64_t HashBin1(
        uint64_t key) const {
        return HASH_FUNCTION(&key, sizeof(uint64_t), hash_seed1) % bin_num;
    }
    uint64_t bin_num;
    Bin* tab;
};
}  // namespace tinyptr