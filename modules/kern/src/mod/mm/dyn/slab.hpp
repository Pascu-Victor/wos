#pragma once

#include <defines/defines.hpp>
#include <mod/mm/paging.hpp>
#include <mod/mm/phys.hpp>

namespace ker::mod::mm::dyn::slab
{
    struct Slab; //forward declaration
    
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
        uint64_t objectSize;
        const char* name;
        Slab* slabs;
    };

    bool cacheGrow(SlabCache* cache, uint64_t count);
    SlabCache* newCache(uint64_t objectSize, const char* name, uint64_t align);
}