#pragma once

#include <cassert>
#include <cstdint>
#include "utils/cache_line_size.h"
#include "utils/rng.h"
#include "utils/xxhash64.h"

#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#define XXH_NAMESPACE
#include "utils/xxhash.h"

// General-purpose hash macro - you can change the implementation as needed
#define HASH_FUNCTION(input, length, seed) XXH3_64bits_withSeed(input, length, seed)
// #define HASH_FUNCTION(input, length, seed) XXH64(input, length, seed)