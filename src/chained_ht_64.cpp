#include <cstdlib>
#include <cstring>
#include "chained_ht_64.h"

namespace tinyptr {
ChainedHT64::ListIndicator::ListIndicator() {
    bit_str = 0;
}

ChainedHT64::ListIndicator::ListIndicator(uint64_t bit_str_) {
    bit_str = bit_str_;
}

ChainedHT64::ListIndicator::ListIndicator(uint64_t quot_key, bool base_bit,
                                          uint8_t ptr) {
    bit_str = 0;
    set_quot_head(quot_key);
    if (base_bit)
        set_base_bit();
    set_ptr(ptr);
}

void ChainedHT64::ListIndicator::set_base_bit() {
    bit_str |= (1 << kBaseBitPos);
}

void ChainedHT64::ListIndicator::set_base_bit(bool base_bit) {
    bit_str |= (1 << kBaseBitPos);
    bit_str ^= (static_cast<uint64_t>(base_bit ^ 1) << kBaseBitPos);
}

void ChainedHT64::ListIndicator::erase_base_bit() {
    bit_str ^=
        (((bit_str << (64 - kBaseBitPos - 1)) >> (64 - 1)) << kBaseBitPos);
}

void ChainedHT64::ListIndicator::set_quot_head(uint64_t quot_key) {
    bit_str = ((bit_str << kQuotientingHeadSize) >> kQuotientingHeadSize) |
              (quot_key << kQuotientingTailSize);
}

void ChainedHT64::ListIndicator::set_bit_str(uint64_t bit_str_) {
    bit_str = bit_str_;
}

void ChainedHT64::ListIndicator::set_ptr(uint8_t ptr) {
    bit_str = ((bit_str >> kQuotientingTailSize) << kQuotientingTailSize) | ptr;
}

bool ChainedHT64::ListIndicator::get_base_bit() {
    return bit_str & (1 << kBaseBitPos);
}

uint64_t ChainedHT64::ListIndicator::get_quot_head() {
    return (bit_str >> kQuotientingTailSize) << kQuotientingTailSize;
}

uint64_t ChainedHT64::ListIndicator::get_quot_key() {
    return (bit_str >> kQuotientingTailSize);
}

uint8_t ChainedHT64::ListIndicator::get_ptr() {
    // it's implicitly truncated
    return bit_str;
}

uint64_t ChainedHT64::ListIndicator::get_bit_str() {
    return bit_str;
}

ChainedHT64::ChainedHT64(int n) {
    deref_tab = new O2VDereferenceTable64(n);
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

uint32_t ChainedHT64::get_bin_num(uint64_t key) {
    return (quot_head_hash(key) ^ key) & ((1 << kQuotientingTailSize) - 1);
}

uint64_t ChainedHT64::encode_key(uint64_t key) {
    return key >> kQuotientingTailSize;
}

uint64_t ChainedHT64::decode_key(uint64_t quot_key, uint32_t bin_num) {
    uint64_t quot_head = quot_key << kQuotientingTailSize;
    return quot_head | (bin_num ^ quot_head_hash(quot_head));
}

bool ChainedHT64::ContainsKey(uint64_t key) {
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

void ChainedHT64::Insert(uint64_t key, uint64_t value) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t ptr = quot_tab[bin_num];
    uint64_t base_key = decode_key(kBaseDerefQuotKey, bin_num);

    if (ptr) {
        ListIndicator list_head(deref_tab->QueryFirst(base_key, ptr));
        if (key == base_key) {
            list_head.set_base_bit();
            deref_tab->UpdateFirst(key, ptr, list_head.get_bit_str());
            deref_tab->UpdateSecond(key, ptr, value);
        } else {
            list_head.set_ptr(
                deref_tab->Allocate(key, list_head.get_bit_str(), value));
            list_head.set_quot_head(encode_key(key));
            deref_tab->UpdateFirst(base_key, ptr, list_head.get_bit_str());
        }
    } else {
        // actually, this new field for value_1 is meaningful iff key == base_key
        // whenever the ptr field of the indicator is 0, it implies the end of the list
        ListIndicator new_entry_ind(encode_key(key), base_key == key, 0);
        if (base_key ^ key) {
            // reusing ptr
            ptr = deref_tab->Allocate(key, new_entry_ind.get_bit_str(), value);
            // reusing new_entry_ind
            new_entry_ind.set_ptr(ptr);
        }
        quot_tab[bin_num] =
            deref_tab->Allocate(base_key, new_entry_ind.get_bit_str(), value);
    }
}

void ChainedHT64::Erase(uint64_t key) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t pre_ptr = quot_tab[bin_num];
    uint64_t pre_key = decode_key(kBaseDerefQuotKey, bin_num);
    uint8_t iter_ptr;
    uint64_t iter_key;

    ListIndicator list_ind;

    if (pre_ptr) {
        list_ind.set_bit_str(deref_tab->QueryFirst(pre_key, pre_ptr));

        if (key == pre_key) {
            if (list_ind.get_base_bit()) {
                list_ind.erase_base_bit();
                deref_tab->UpdateFirst(pre_key, pre_ptr,
                                       list_ind.get_bit_str());
            }
            return;
        }

        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }

    while (iter_ptr) {

        list_ind.set_bit_str(deref_tab->QueryFirst(iter_key, iter_ptr));

        if (key == iter_key) {
            ListIndicator pre_ind(deref_tab->QueryFirst(pre_key, pre_ptr));
            // prevent covering base_bit
            pre_ind.set_quot_head(list_ind.get_quot_key());
            pre_ind.set_ptr(list_ind.get_ptr());
            deref_tab->UpdateFirst(pre_key, pre_ptr, pre_ind.get_bit_str());
            deref_tab->Free(iter_key, iter_ptr);
            return;
        }

        pre_key = iter_key;
        pre_ptr = iter_ptr;
        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }
}

void ChainedHT64::Update(uint64_t key, uint64_t value) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t iter_ptr = quot_tab[bin_num];
    uint64_t iter_key = decode_key(kBaseDerefQuotKey, bin_num);

    if (iter_ptr) {
        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));

        if (key == iter_key)
            if (list_ind.get_base_bit()) {
                deref_tab->UpdateSecond(iter_key, iter_ptr, value);
                return;
            } else
                goto failNinsert;

        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }
    while (iter_ptr) {
        if (key == iter_key) {
            deref_tab->UpdateSecond(iter_key, iter_ptr, value);
            return;
        }

        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));
        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }

failNinsert:
    // to comply with the behavior of std::map
    Insert(key, value);
}

uint64_t ChainedHT64::Query(uint64_t key) {
    uint32_t bin_num = get_bin_num(key);

    uint8_t iter_ptr = quot_tab[bin_num];
    uint64_t iter_key = decode_key(kBaseDerefQuotKey, bin_num);

    if (iter_ptr) {
        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));

        if (key == iter_key)
            if (list_ind.get_base_bit())
                return deref_tab->QuerySecond(iter_key, iter_ptr);
            else
                goto failNinsert;

        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }
    while (iter_ptr) {
        if (key == iter_key)
            return deref_tab->QuerySecond(iter_key, iter_ptr);

        ListIndicator list_ind(deref_tab->QueryFirst(iter_key, iter_ptr));
        iter_key = decode_key(list_ind.get_quot_key(), bin_num);
        iter_ptr = list_ind.get_ptr();
    }

failNinsert:
    // to comply with the behavior of std::map
    Insert(key, 0);
    return 0;
}

}  // namespace tinyptr