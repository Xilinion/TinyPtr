#include "po2c_table.h"

namespace tinyptr {

Po2CTable::Bin::Bin() {
    for (int i = 0; i < DereferenceTable64::kBinSize; ++i)
        bin[i].key = i + 2;
}

inline bool Po2CTable::Bin::full() {
    return cnt == DereferenceTable64::kBinSize;
}

inline uint8_t Po2CTable::Bin::count() {
    return cnt;
}

// 0 for null
// i+1 for position i
// there're at most 2 same value in the key field: 1) the pointer of free list 2) the key
inline uint8_t Po2CTable::Bin::find(uint64_t key) {
    uint8_t key_cnt = 0;
    uint8_t key_pos[2];
    // not duplicate key
    for (int i = 0; i < DereferenceTable64::kBinSize; ++i)
        if (bin[i].key == key)
            key_pos[key_cnt++] = i + 1;

    auto tmp = head;
    while (!(tmp & (1 << 7))) {
        if (bin[tmp - 1].key == key) {
            if (key_pos[0] == tmp) {
                key_pos[0] = key_pos[1];
                --key_cnt;
                break;
            }
        }
        tmp = bin[tmp - 1].key;
    }

    if (key_cnt)
        return key_pos[0];
    return 0;
}

inline bool Po2CTable::Bin::query(uint64_t key, uint8_t ptr,
                                  uint64_t* value_ptr) {
    // assert(ptr < (1 << 7));
    // assert(ptr);
    --ptr;

    if (bin[ptr].key == key) {
        // fixme: there's a hack. if bin[ptr] is in free list and bin[ptr].key happens to equal key, it will return false positive.
        *value_ptr = bin[ptr].value;
        return 1;
    }

    return 0;
}

bool Po2CTable::Bin::insert_check(uint64_t key) {
#ifdef TINYPTR_DEREFTAB64_KEY_UNIQUENESS_CHECK
    return !this->find(key);
#else
    return 1;
#endif
}

// 0 for null (duplicate key), note this should hold higher priority than full bin
// ~0 for full bin
// i+1 for position i
__attribute__((always_inline)) inline uint8_t Po2CTable::Bin::insert(
    uint64_t key, uint64_t value) {
    // if (!this->insert_check(key))
    //     return 0;

    if (cnt == DereferenceTable64::kBinSize)
        return ~0;

    ++cnt;
    auto tmp = head;
    head = bin[head - 1].key;
    bin[tmp - 1].key = key;
    bin[tmp - 1].value = value;

    return tmp;
}

__attribute__((always_inline)) inline bool Po2CTable::Bin::update(
    uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    if (bin[ptr].key == key) {
        bin[ptr].value = value;
        return 1;
    }

    return 0;
}

__attribute__((always_inline)) inline bool Po2CTable::Bin::free(uint64_t key,
                                                                uint8_t ptr) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    if (bin[ptr].key == key) {
        bin[ptr].key = head;
        head = ptr + 1;
        --cnt;
        return 1;
    }

    return 0;
}

Po2CTable::Po2CTable(int n) {
    bin_num =
        (n + DereferenceTable64::kBinSize - 1) / DereferenceTable64::kBinSize;
    tab = new Bin[bin_num];
    srand(time(0));
    hash_seed0 = rand();
    hash_seed1 = rand();
    while (hash_seed1 == hash_seed0)
        hash_seed1 = rand();
}

uint8_t Po2CTable::Allocate(uint64_t key, uint64_t value) {
    const uint64_t hashbin0 = HashBin0(key);
    const uint64_t hashbin1 = HashBin1(key);

    Bin* bin0 = tab + hashbin0;
    Bin* bin1 = tab + hashbin1;

    const bool bin0_full = bin0->cnt == DereferenceTable64::kBinSize;
    const bool bin1_full = bin1->cnt == DereferenceTable64::kBinSize;
    if (bin0_full && bin1_full) {
        return ~0;  // both bins full
    }

    Bin* target = bin0_full
                      ? bin1
                      : (bin1_full ? bin0
                                   : (bin0->cnt > bin1->cnt ? bin1 : bin0));
    const uint8_t flag = (target == bin1);

    const uint8_t head = target->head;
    KV* slot = target->bin + (head - 1);
    target->head = static_cast<uint8_t>(slot->key);
    slot->key = key;
    slot->value = value;
    ++target->cnt;

    return flag ? static_cast<uint8_t>(head ^ ((1 << 8) - 1)) : head;
}

bool Po2CTable::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[flag ? HashBin1(key) : HashBin0(key)].update(key, ptr, value);
}

bool Po2CTable::Query(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[flag ? HashBin1(key) : HashBin0(key)].query(key, ptr, value_ptr);
}

bool Po2CTable::Free(uint64_t key, uint8_t ptr) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[flag ? HashBin1(key) : HashBin0(key)].free(key, ptr);
}

}  // namespace tinyptr