#pragma once

namespace tinyptr {

enum class BenchmarkObjectType : uint8_t {
    DEREFTAB64 = 0,
    CHAINEDHT64,
    INTARRAY,
    STDUNORDEREDMAP,
    COUNT
};

}