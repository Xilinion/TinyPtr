#pragma once

#include <cstdint>

namespace tinyptr {

struct ConcOptType {
    enum { INSERT = 0, QUERY = 1, UPDATE = 2, ERASE = 3, COUNT = 4 };

    ConcOptType() = default;
    ConcOptType(ConcOptType& b) = default;

    ConcOptType(int b) : val(b) {}

    int val;
};

}  // namespace tinyptr
