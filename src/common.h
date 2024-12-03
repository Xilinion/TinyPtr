#pragma once

#include <cassert>
#include <cstdint>
#include "utils/cache_line_size.h"
#include "utils/rng.h"
#include "utils/xxhash64.h"

#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"