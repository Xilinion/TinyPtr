#pragma once

#include <sys/types.h>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include "byte_array_dereference_table.h"
#include "common.h"
#include "utils/xxhash64.h"

namespace tinyptr {
class ByteArrayPo2CTable {
   private:
    struct KPV {
        // Fix me: assuming little endian
        // 6 quotiented_key | 1 ptr | 8 value
        uint8_t data[15];

        uint64_t get_quotiented_key();
        uint8_t get_tiny_ptr();
        uint64_t get_value();
        void set_quotiented_key(uint64_t key);
        void set_tiny_ptr(uint8_t ptr);
        void set_value(uint64_t value);
    };

    class Bin {
       public:
        Bin();
        ~Bin() = default;

       public:
        bool full();
        uint8_t count();
        void query_quotiented_key(uint8_t ptr, uint64_t* value_ptr);
        uint64_t query_quotiented_key(uint8_t ptr);
        void query_tiny_ptr(uint8_t ptr, uint8_t* value_ptr);
        uint8_t query_tiny_ptr(uint8_t ptr);
        void query_value(uint8_t ptr, uint64_t* value_ptr);
        uint64_t query_value(uint8_t ptr);
        uint8_t insert(uint64_t quotiented_key, uint8_t ptr, uint64_t value);
        void update_quotiented_key(uint8_t ptr, uint64_t value);
        void update_tiny_ptr(uint8_t ptr, uint8_t value);
        void update_value(uint8_t ptr, uint64_t value);
        void free(uint8_t ptr);

       private:
        uint8_t cnt = 0;
        uint8_t head = 1;
        KPV bin[ByteArrayDereferenceTable::kBinSize];
    };

   public:
    ByteArrayPo2CTable() = delete;

    ByteArrayPo2CTable(int n);

    ~ByteArrayPo2CTable() = default;

    uint8_t Allocate(uint64_t key, uint64_t quotiented_key, uint8_t tiny_ptr,
                     uint64_t value);
    void UpdateQuotientedKey(uint64_t key, uint8_t ptr, uint64_t quotiented_key);
    void UpdateTinyptr(uint64_t key, uint8_t ptr, uint8_t tiny_ptr);
    void UpdateValue(uint64_t key, uint8_t ptr, uint64_t value);
    uint64_t QueryDataAddress(uint64_t key, uint8_t ptr);
    void QueryQuotientedKey(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QueryQuotientedKey(uint64_t key, uint8_t ptr);
    void QueryTinyPtr(uint64_t key, uint8_t ptr, uint8_t* value_ptr);
    uint8_t QueryTinyPtr(uint64_t key, uint8_t ptr);
    void QueryValue(uint64_t key, uint8_t ptr, uint64_t* value_ptr);
    uint64_t QueryValue(uint64_t key, uint8_t ptr);
    void Free(uint64_t key, uint8_t ptr);

   private:
    std::function<uint64_t(uint64_t)> HashBin[2];
    uint64_t bin_num;
    Bin* tab;
};
}  // namespace tinyptr