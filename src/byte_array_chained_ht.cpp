#include "byte_array_chained_ht.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include "byte_array_dereference_table.h"

namespace tinyptr {
ByteArrayChainedHT::ListIndicator::ListIndicator() {
    bit_str = 0;
}

ByteArrayChainedHT::ListIndicator::ListIndicator(uint64_t bit_str_) {
    bit_str = bit_str_;
}

ByteArrayChainedHT::ListIndicator::ListIndicator(uint64_t quot_key,
                                                 bool base_bit, uint8_t ptr) {
    bit_str = 0;
    set_quot_head(quot_key);
    if (base_bit)
        set_base_bit();
    set_ptr(ptr);
}

void ByteArrayChainedHT::ListIndicator::set_base_bit() {
    bit_str |= (1 << kBaseBitPos);
}

void ByteArrayChainedHT::ListIndicator::set_base_bit(bool base_bit) {
    bit_str |= (1 << kBaseBitPos);
    bit_str ^= (static_cast<uint64_t>(base_bit ^ 1) << kBaseBitPos);
}

void ByteArrayChainedHT::ListIndicator::erase_base_bit() {
    bit_str ^=
        (((bit_str << (64 - kBaseBitPos - 1)) >> (64 - 1)) << kBaseBitPos);
}

void ByteArrayChainedHT::ListIndicator::set_quot_head(uint64_t quot_key) {
    bit_str = ((bit_str << kQuotientingHeadSize) >> kQuotientingHeadSize) |
              (quot_key << kQuotientingTailSize);
}

void ByteArrayChainedHT::ListIndicator::set_bit_str(uint64_t bit_str_) {
    bit_str = bit_str_;
}

void ByteArrayChainedHT::ListIndicator::set_ptr(uint8_t ptr) {
    bit_str = ((bit_str >> kQuotientingTailSize) << kQuotientingTailSize) | ptr;
}

bool ByteArrayChainedHT::ListIndicator::get_base_bit() {
    return bit_str & (1 << kBaseBitPos);
}

uint64_t ByteArrayChainedHT::ListIndicator::get_quot_head() {
    return (bit_str >> kQuotientingTailSize) << kQuotientingTailSize;
}

uint64_t ByteArrayChainedHT::ListIndicator::get_quot_key() {
    return (bit_str >> kQuotientingTailSize);
}

uint8_t ByteArrayChainedHT::ListIndicator::get_ptr() {
    // it's implicitly truncated
    return bit_str;
}

uint64_t ByteArrayChainedHT::ListIndicator::get_bit_str() {
    return bit_str;
}

ByteArrayChainedHT::ByteArrayChainedHT(int n) {
    deref_tab = new ByteArrayDereferenceTable(n);
    quot_tab = new uint8_t[1 << kQuotientingTailSize];
    memset(quot_tab, 0, sizeof(uint8_t) * (1 << kQuotientingTailSize));

    auto hash_seed = rand();

    quot_head_hash =
        std::function<uint32_t(uint64_t)>([=](uint64_t key) -> uint32_t {
            key = key >> kQuotientingTailSize << kQuotientingTailSize;
            return XXHash64::hash(&key, sizeof(uint64_t), hash_seed) &
                   ((1 << kQuotientingTailSize) - 1);
        });
}

uint32_t ByteArrayChainedHT::get_bin_num(uint64_t key) {
    return (quot_head_hash(key) ^ key) & ((1 << kQuotientingTailSize) - 1);
}

uint64_t ByteArrayChainedHT::encode_key(uint64_t key) {
    return key >> kQuotientingTailSize;
}

uint64_t ByteArrayChainedHT::decode_key(uint64_t quot_key, uint32_t bin_num) {
    uint64_t quot_head = quot_key << kQuotientingTailSize;
    return quot_head | (bin_num ^ quot_head_hash(quot_head));
}

/*
bool ByteArrayChainedHT::ContainsKey(uint64_t key) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t iter_ptr = quot_tab[bin_num];
    uint64_t iter_key = decode_key(kBaseDerefQuotKey, bin_num);

    if (iter_ptr) {
        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));

        if (key == iter_key)
            if (list_ind.get_base_bit())
                return 1;
            else
                return 0;

        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }
    while (iter_ptr) {
        if (key == iter_key)
            return 1;

        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));
        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }
    return 0;
}
*/

void ByteArrayChainedHT::Insert(uint64_t key, uint64_t value) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t ptr = quot_tab[bin_num];
    uint64_t address_key = bin_num;
    // decode_key(kBaseDerefQuotKey, bin_num);

    if (ptr) {
        // ListIndicator list_head(deref_tab->QueryFirst(base_key, ptr));
        uint64_t tmp = address_key;
        while (ptr) {
            tmp = address_key;
            address_key = deref_tab->QueryDataAddress(address_key, ptr);
            ptr = deref_tab->QueryTinyPtr(tmp, ptr);
        }
        uint8_t new_ptr =
            deref_tab->Allocate(address_key, encode_key(key), 0, value);
        deref_tab->UpdateTinyptr(tmp, ptr, new_ptr);
    } else {
        // actually, this new field for value_1 is meaningful iff key == base_key
        // whenever the ptr field of the indicator is 0, it implies the end of the list
        // ListIndicator new_entry_ind(encode_key(key), base_key == key, 0);

        quot_tab[bin_num] =
            deref_tab->Allocate(address_key, encode_key(key), 0, value);
    }
}

void ByteArrayChainedHT::Erase(uint64_t key) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t ptr = quot_tab[bin_num];
    uint64_t address_key = bin_num;

    uint64_t quotiented_key = encode_key(key);
    // decode_key(kBaseDerefQuotKey, bin_num);

    // ListIndicator list_head(deref_tab->QueryFirst(base_key, ptr));
    uint64_t tmp = address_key;
    while (ptr) {
        if (deref_tab->QueryQuotientedKey(address_key, ptr) == quotiented_key) {
            break;
        }

        tmp = address_key;
        address_key = deref_tab->QueryDataAddress(address_key, ptr);
        ptr = deref_tab->QueryTinyPtr(tmp, ptr);
    }

    while(ptr)
    {
        uint8_t tmp_ptr = deref_tab->QueryTinyPtr(address_key, ptr);
        if (!tmp_ptr)
        {
            deref_tab->Free(address_key, ptr);
            break;
        }

        tmp = address_key;
        address_key = deref_tab->QueryDataAddress(address_key, ptr);
        ptr = deref_tab->QueryTinyPtr(tmp, ptr);

        deref_tab->UpdateQuotientedKey(tmp, tmp_ptr, deref_tab->QueryQuotientedKey(address_key, ptr));
        deref_tab->UpdateTinyptr(tmp, tmp_ptr, deref_tab->QueryTinyPtr(address_key, ptr));
        deref_tab->UpdateValue(tmp, tmp_ptr, deref_tab->QueryValue(address_key, ptr));
    }
}

void ByteArrayChainedHT::Update(uint64_t key, uint64_t value) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t ptr = quot_tab[bin_num];
    uint64_t address_key = bin_num;

    uint64_t quotiented_key = encode_key(key);
    // decode_key(kBaseDerefQuotKey, bin_num);

    // ListIndicator list_head(deref_tab->QueryFirst(base_key, ptr));
    uint64_t tmp = address_key;
    while (ptr) {
        if (deref_tab->QueryQuotientedKey(address_key, ptr) == quotiented_key) {
            deref_tab->UpdateValue(address_key, ptr, value);
        }

        tmp = address_key;
        address_key = deref_tab->QueryDataAddress(address_key, ptr);
        ptr = deref_tab->QueryTinyPtr(tmp, ptr);
    }

    Insert(key, value);
}

uint64_t ByteArrayChainedHT::Query(uint64_t key) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t ptr = quot_tab[bin_num];
    uint64_t address_key = bin_num;

    uint64_t quotiented_key = encode_key(key);
    // decode_key(kBaseDerefQuotKey, bin_num);

    // ListIndicator list_head(deref_tab->QueryFirst(base_key, ptr));
    uint64_t tmp = address_key;
    while (ptr) {
        if (deref_tab->QueryQuotientedKey(address_key, ptr) == quotiented_key) {
            return deref_tab->QueryValue(address_key, ptr);
        }

        tmp = address_key;
        address_key = deref_tab->QueryDataAddress(address_key, ptr);
        ptr = deref_tab->QueryTinyPtr(tmp, ptr);
    }

    Insert(key, 0);
    return 0;
}

}  // namespace tinyptr