#include "po2c_table.h"

namespace tinyptr {

Po2CTable::Bin::Bin() {
    for (int i = 0; i < DereferenceTable64::kBinSize; ++i)
        bin[i].key = i + 2;
}

bool Po2CTable::Bin::full() {
    return cnt == DereferenceTable64::kBinSize;
}

uint8_t Po2CTable::Bin::count() {
    return cnt;
}

// 0 for null
// i+1 for position i
// there're at most 2 same value in the key field: 1) the pointer of free list 2) the key
uint8_t Po2CTable::Bin::find(uint64_t key) {
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

bool Po2CTable::Bin::query(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr < (1 << 7));
    assert(ptr);
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
uint8_t Po2CTable::Bin::insert(uint64_t key, uint64_t value) {
    if (!this->insert_check(key))
        return 0;

    if (this->full())
        return ~0;

    ++cnt;
    auto tmp = head;
    head = bin[head - 1].key;
    bin[tmp - 1].key = key;
    bin[tmp - 1].value = value;

    return tmp;
}

bool Po2CTable::Bin::update(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr < (1 << 7));
    assert(ptr);
    --ptr;

    if (bin[ptr].key == key) {
        bin[ptr].value = value;
        return 1;
    }

    return 0;
}

bool Po2CTable::Bin::free(uint64_t key, uint8_t ptr) {
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
    int hash_seed[2] = {rand(), rand()};
    while (hash_seed[1] == hash_seed[0])
        hash_seed[1] = rand();
    for (int i = 0; i < 2; ++i)
        HashBin[i] =
            std::function<uint64_t(uint64_t)>([=](uint64_t key) -> uint64_t {
                return SlowXXHash64::hash(&key, sizeof(uint64_t), hash_seed[i]) %
                       bin_num;
            });
}

uint8_t Po2CTable::Allocate(uint64_t key, uint64_t value) {
    uint64_t hashbin[2];
    for (int i = 0; i < 2; ++i) {
        hashbin[i] = HashBin[i](key);
        if (!tab[hashbin[i]].insert_check(key))
            return 0;
    }

    uint8_t flag = tab[hashbin[0]].count() > tab[hashbin[1]].count();

    uint8_t ptr = tab[hashbin[flag]].insert(key, value);
    // ptr should not be 0 after the previous check in both bins
    assert(ptr);

    ptr ^=
        flag * (ptr != DereferenceTable64::kOverflowTinyPtr) * ((1 << 8) - 1);
    return ptr;
}

bool Po2CTable::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].update(key, ptr, value);
}

bool Po2CTable::Query(uint64_t key, uint8_t ptr, uint64_t* value_ptr) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].query(key, ptr, value_ptr);
}

bool Po2CTable::Free(uint64_t key, uint8_t ptr) {
    assert(ptr != DereferenceTable64::kOverflowTinyPtr);
    assert(ptr != DereferenceTable64::kNullTinyPtr);

    uint8_t flag = (ptr >= (1 << 7));
    ptr ^= flag * ((1 << 8) - 1);
    return tab[HashBin[flag](key)].free(key, ptr);
}

}  // namespace tinyptr