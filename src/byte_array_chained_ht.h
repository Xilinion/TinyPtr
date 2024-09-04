#pragma once

#include <functional>
#include "byte_array_dereference_table.h"
#include "common.h"

namespace tinyptr {

class ByteArrayChainedHT {
   private:
    class ListIndicator {
       public:
        static constexpr uint64_t kBaseBitPos = 10 - 1;
        static constexpr uint64_t kPtrLength = 8;

       public:
        ListIndicator();
        ListIndicator(uint64_t bit_str_);
        ListIndicator(uint64_t quot_key, bool base_bit, uint8_t ptr);
        ~ListIndicator() = default;

        void set_base_bit();
        void set_base_bit(bool base_bit);
        void erase_base_bit();
        void set_quot_head(uint64_t quot_key);
        void set_ptr(uint8_t ptr);
        void set_bit_str(uint64_t bit_str_);
        bool get_base_bit();
        uint64_t get_quot_head();
        uint64_t get_quot_key();
        uint8_t get_ptr();
        uint64_t get_bit_str();

       private:
        uint64_t bit_str;
    };

   public:
    static constexpr uint64_t kQuotientingTailSize = 16;
    static constexpr uint64_t kQuotientingHeadSize = 48;

   public:
    ByteArrayChainedHT() = delete;
    ByteArrayChainedHT(int n);
    ~ByteArrayChainedHT() = default;

   private:
    uint32_t get_bin_num(uint64_t key);
    uint64_t encode_key(uint64_t key);
    uint64_t decode_key(uint64_t quot_key, uint32_t bin_num);

   public:
    bool ContainsKey(uint64_t key);
    void Insert(uint64_t key, uint64_t value);
    void Erase(uint64_t key);
    void Update(uint64_t key, uint64_t value);
    uint64_t Query(uint64_t key);
    // TODO: or not todo? supporting iterator & key/value set & vectorized operators

   private:
    std::function<uint32_t(uint64_t)> quot_head_hash;
    ByteArrayDereferenceTable* deref_tab;
    uint8_t* quot_tab;
};
}  // namespace tinyptr