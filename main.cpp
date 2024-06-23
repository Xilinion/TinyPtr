#pragma warning "Ignore '#pragma once' in main file"

#include <cstdlib>
#include "dereference_table_64.h"

int main() {
    int n = 1000;
    tinyptr::DeferenceTable64 a(n);

    auto p = new uint8_t[10001];
    int i;
    for (i = 1; i <= 10000; ++i) {
        uint8_t ptr = a.Allocate(i, i);
        p[i] = ptr;
        a.Update(i, ptr, i + 1);
        uint64_t ret;
        a.Query(i, ptr, &ret);

        assert(ret == i + 1);
    }

    for (i = 1; i <= 10000; ++i)
        a.Free(i, p[i]);

    for (i = 1; i <= 10000; ++i) {
        uint8_t ptr = a.Allocate(i, i);
        a.Update(i, ptr, i + 1);
        uint64_t ret;
        a.Query(i, ptr, &ret);

        assert(ret == i + 1);
    }
}