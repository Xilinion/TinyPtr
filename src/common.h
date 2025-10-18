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

inline uint64_t avalanche(uint64_t h) {
	h ^= h >> 33;
	h *= 0xff51afd7ed558ccd;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53;
	h ^= h >> 33;
	return h;
}

// General-purpose hash macro - you can change the implementation as needed
// #define HASH_FUNCTION(input, length, seed) XXH3_64bits_withSeed(input, length, seed)
#define HASH_FUNCTION(input, length, seed) avalanche(*input)
// #define HASH_FUNCTION(input, length, seed) XXH64(input, length, seed)
