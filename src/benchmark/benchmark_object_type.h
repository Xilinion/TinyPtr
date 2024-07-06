#pragma once

#include <cstdint>

namespace tinyptr {

enum class BenchmarkObjectType : int8_t {
    INVALID = -1,
    DEREFTAB64 = 0,
    CHAINEDHT64,
    INTARRAY64,
    STDUNORDEREDMAP64,
    COUNT
};

}