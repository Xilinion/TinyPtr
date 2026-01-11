#include "benchmark_third_party_tinypointers.h"

// Include the actual third-party TinyPointers headers
#include "tiny_ptr_unified.h"
#include "tiny_ptr_simple.h"

#include <memory>
#include <cassert>
#include <stdexcept>

namespace tinyptr {

// Type constants
const BenchmarkObjectType BenchmarkThirdPartyTinyPointersSimple::TYPE =
    BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_SIMPLE;

const BenchmarkObjectType BenchmarkThirdPartyTinyPointersFixed::TYPE =
    BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_FIXED;

const BenchmarkObjectType BenchmarkThirdPartyTinyPointersVariable::TYPE =
    BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_VARIABLE;

// PIMPL implementation to hide third-party details
struct BenchmarkThirdPartyTinyPointersBase::Impl {
    tiny_ptr_table_t* unified_table;  // For unified interface variants
    SimpleTable* simple_table;        // For direct simple interface
    TinyPtrVariant variant;
    
    Impl(int n, TinyPtrVariant var) : unified_table(nullptr), simple_table(nullptr), variant(var) {
        if (variant == TINY_PTR_SIMPLE) {
            // For simple variant, we can use either the unified interface or direct interface
            // Let's use the direct interface for potentially better performance
            simple_table = simple_create_ex(static_cast<size_t>(n), 0.75);
            if (!simple_table) {
                throw std::runtime_error("Failed to create third-party TinyPointers simple table");
            }
        } else {
            // For fixed and variable variants, use the unified interface
            unified_table = tiny_ptr_create(static_cast<size_t>(n), variant, 0.75);
            if (!unified_table) {
                throw std::runtime_error("Failed to create third-party TinyPointers unified table");
            }
        }
    }
    
    ~Impl() {
        if (unified_table) {
            tiny_ptr_destroy(unified_table);
        }
        if (simple_table) {
            simple_destroy(simple_table);
        }
    }
};

BenchmarkThirdPartyTinyPointersBase::BenchmarkThirdPartyTinyPointersBase(BenchmarkObjectType type, int n) 
    : BenchmarkObject64(type) {
    
    TinyPtrVariant variant;
    switch (type.val) {
        case BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_SIMPLE:
            variant = TINY_PTR_SIMPLE;
            break;
        case BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_FIXED:
            variant = TINY_PTR_FIXED;
            break;
        case BenchmarkObjectType::THIRD_PARTY_TINYPOINTERS_VARIABLE:
            variant = TINY_PTR_VARIABLE;
            break;
        default:
            throw std::runtime_error("Invalid third-party TinyPointers variant");
    }
    
    impl_ = std::make_unique<Impl>(n, variant);
}

BenchmarkThirdPartyTinyPointersBase::~BenchmarkThirdPartyTinyPointersBase() = default;

uint8_t BenchmarkThirdPartyTinyPointersBase::Insert(uint64_t key, uint64_t value) {
    // Convert uint64_t to int for third-party API (assuming it fits)
    int tiny_ptr;
    
    if (impl_->simple_table) {
        // Use direct simple interface
        tiny_ptr = simple_allocate(impl_->simple_table, static_cast<int>(key), static_cast<int>(value));
    } else {
        // Use unified interface
        tiny_ptr = tiny_ptr_allocate(impl_->unified_table, static_cast<int>(key), static_cast<int>(value));
    }
    
    return static_cast<uint8_t>(tiny_ptr);
}

uint64_t BenchmarkThirdPartyTinyPointersBase::Query(uint64_t key, uint8_t ptr) {
    int result;
    
    if (impl_->simple_table) {
        // Use direct simple interface
        result = simple_dereference(impl_->simple_table, static_cast<int>(key), static_cast<int>(ptr));
    } else {
        // Use unified interface
        result = tiny_ptr_dereference(impl_->unified_table, static_cast<int>(key), static_cast<int>(ptr));
    }
    
    return static_cast<uint64_t>(result);
}

void BenchmarkThirdPartyTinyPointersBase::Update(uint64_t key, uint8_t ptr, uint64_t value) {
    // Third-party TinyPointers doesn't seem to have an update function
    // We'll need to free and then allocate again
    if (impl_->simple_table) {
        simple_free(impl_->simple_table, static_cast<int>(key), static_cast<int>(ptr));
        simple_allocate(impl_->simple_table, static_cast<int>(key), static_cast<int>(value));
    } else {
        tiny_ptr_free(impl_->unified_table, static_cast<int>(key), static_cast<int>(ptr));
        tiny_ptr_allocate(impl_->unified_table, static_cast<int>(key), static_cast<int>(value));
    }
}

void BenchmarkThirdPartyTinyPointersBase::Erase(uint64_t key, uint8_t ptr) {
    if (impl_->simple_table) {
        simple_free(impl_->simple_table, static_cast<int>(key), static_cast<int>(ptr));
    } else {
        tiny_ptr_free(impl_->unified_table, static_cast<int>(key), static_cast<int>(ptr));
    }
}

// Concrete class constructors
BenchmarkThirdPartyTinyPointersSimple::BenchmarkThirdPartyTinyPointersSimple(int n)
    : BenchmarkThirdPartyTinyPointersBase(TYPE, n) {
}

BenchmarkThirdPartyTinyPointersFixed::BenchmarkThirdPartyTinyPointersFixed(int n)
    : BenchmarkThirdPartyTinyPointersBase(TYPE, n) {
}

BenchmarkThirdPartyTinyPointersVariable::BenchmarkThirdPartyTinyPointersVariable(int n)
    : BenchmarkThirdPartyTinyPointersBase(TYPE, n) {
}

}  // namespace tinyptr
