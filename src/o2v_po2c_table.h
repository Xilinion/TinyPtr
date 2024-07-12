#pragma once

#include <bitset>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <map>
#include "common.h"
#include "o2v_dereference_table_64.h"
#include "utils/xxhash64.h"

namespace tinyptr {
class O2VPo2CTable {
   private:
    struct VV {
        uint64_t value_1, value_2;
    };

    class Bin {
       public:
        Bin();
        ~Bin() = default;

       public:
        bool full();
        uint8_t count();
        void query_first(uint8_t ptr, uint64_t* value_ptr);
        uint64_t query_first(uint8_t ptr);
        void query_second(uint8_t ptr, uint64_t* value_ptr);
        uint64_t query_second(uint8_t ptr);
        uint8_t insert(uint64_t value_1, uint64_t value_2);
        void update_first(uint8_t ptr, uint64_t value);
        void update_second(uint8_t ptr, uint64_t value);
        void free(uint8_t ptr);

       private:
        uint8_t cnt = 0;
        uint8_t head = 1;
        VV bin[O2VDereferenceTable64::kBinSize];
    };

   public:
    O2VPo2CTable() = delete;

    O2VPo2CTable(int n);

    ~O2VPo2CTable() = default;

    uint8_t Allocate(uint64_t key, uint64_t value_1, uint64_t value_2);
    void UpdateFirst(uint64_t key, uint8_t ptr, uint64_t value);
    void UpdateSecond(uint64_t key, uint8_t ptr, uint64_t value);
    void QueryFirst(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QueryFirst(uint64_t key, uint8_t ptr);
    void QuerySecond(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QuerySecond(uint64_t key, uint8_t ptr);
    void Free(uint64_t key, uint8_t ptr);

   private:
    std::function<uint64_t(uint64_t)> HashBin[2];
    uint64_t bin_num;
    Bin* tab;
};
}  // namespace tinyptr