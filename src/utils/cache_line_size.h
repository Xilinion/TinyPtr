#pragma once

#include <array>
#include <iostream>

#if defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>
#endif

namespace utils {

static uint32_t get_cache_line_size() {
    unsigned int eax, ebx, ecx, edx;

#if defined(__GNUC__) || defined(__clang__)
    // Use GCC/Clang built-in function to get CPUID information
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        return (ebx & 0xFF00) >> 5;  // Cache line size is in bits 15:8 of EBX
    }
#endif

    return 64;  // Return 0 if CPUID is not supported
}

static const uint32_t kCacheLineSize = get_cache_line_size();

}  // namespace utils