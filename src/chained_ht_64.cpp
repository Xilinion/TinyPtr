#include <cstdlib>
#include <cstring>
#include "chained_ht_64.h"

namespace tinyptr {

ChainedHT64::ChainedHT64(int n) {
    deref_tab = new DereferenceTable64(n);
    quot_tab = new uint8_t[1 << kQuotientingTailSize];
    memset(quot_tab, 0, sizeof(uint8_t) * (1 << kQuotientingTailSize));

    quot_head_hash =
        std::function<uint32_t(uint64_t)>([](uint64_t key) -> uint32_t {
            return XXHash64::hash(&key, sizeof(uint64_t), rand()) &
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
    // return deref_tab.Query(key, uint8_t ptr, uint64_t* value_ptr);
}

bool ChainedHT64::Insert(uint64_t key, uint64_t value) {
    uint32_t bin_num = get_bin_num(key);
    
    uint8_t ptr = quot_tab[bin_num];

    while(ptr)
    {

    }
}

bool ChainedHT64::Erase(uint64_t key) {}

bool ChainedHT64::Update(uint64_t key, uint64_t value) {}

uint64_t ChainedHT64::Query(uint64_t key) {}

}  // namespace tinyptr