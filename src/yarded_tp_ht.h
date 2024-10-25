#pragma once

#include <cstdint>
#include "byte_array_chained_ht.h"

namespace tinyptr {

class YardedTPHT : public ByteArrayChainedHT {

   public:
    const uint8_t kBaseDuplexSize;
    const uint8_t kBaseDuplexEncodingBytes;
    const uint8_t kFrontyardSize;
    const uint8_t kBackyardSize;
    const uint8_t kFrontyardOffset;
    const uint64_t kYardNum;

   protected:
    uint8_t AutoBaseBinsize();

   public:
    YardedTPHT(uint64_t size, uint16_t bin_size);
    ~YardedTPHT() = default;

   protected:
    void print_duplex(uint64_t base_id, char* msg);
    uint64_t get_base_key(uint64_t base_id);
    uint8_t get_base_tab_ptr(uint64_t base_id);
    void set_base_tab_ptr(uint64_t base_id, uint8_t new_ptr);
    void free_base_tab_ptr(uint64_t base_id);

   public:
    bool Insert(uint64_t key, uint64_t value);
    bool Query(uint64_t key, uint64_t* value_ptr);

   protected:
    uint8_t* frontyard_tab_ptr;
    uint8_t* backyard_tab_ptr;
};

}  // namespace tinyptr
