#pragma once

#include <defines/defines.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>

namespace ker::mod::mm::dyn::slab {
struct Slab;  // forward declaration

struct FreeSlab {
    void* mem;
    FreeSlab* next;
    Slab* parent;
};

struct Slab {
    Slab* next;
    Slab* prev;
    FreeSlab* freelist;
    uint64_t refs;
};

struct SlabCache {
    uint64_t size;
    uint64_t object_size;
    const char* name;
    Slab* slabs;
};

bool cache_grow(SlabCache* cache, uint64_t count);
SlabCache* new_cache(uint64_t object_size, const char* name, uint64_t align);
}  // namespace ker::mod::mm::dyn::slab