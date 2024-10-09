#include "slab.hpp"

namespace ker::mod::mm::dyn::slab {
static Slab* createSlab(SlabCache* cache) {
    FreeSlab* freeSlab = phys::pageAlloc<FreeSlab>();
    if (!freeSlab) {
        return nullptr;
    }

    freeSlab->next = nullptr;

    Slab* slab = (Slab*)((uint64_t)freeSlab + paging::PAGE_SIZE - sizeof(Slab));  // get Slab* at the end of the page
    slab->next = nullptr;
    slab->refs = 0;
    slab->freelist = freeSlab;

    if (cache->slabs == nullptr) {
        cache->slabs = slab;
    } else {
        Slab* tail = cache->slabs;
        while (tail->next != nullptr) {
            tail = tail->next;
        }
        tail->next = slab;
    }
    return slab;
}

bool cacheGrow(SlabCache* cache, uint64_t count) {
    for (uint64_t i = 0; i < count; i++) {
        Slab* slab = createSlab(cache);
        if (!slab) {
            // TODO: probably should free the created but not added slabs
            return false;
        }
        FreeSlab* freeSlab = slab->freelist;

        size_t elementCount = cache->size / cache->objectSize;
        FreeSlab* tail = freeSlab;

        for (size_t j = 0; j < elementCount - 1; j++) {
            uint64_t* offset = (uint64_t*)freeSlab + cache->objectSize * j;
            FreeSlab* newSlab = (FreeSlab*)offset;
            newSlab->parent = slab;
            newSlab->mem = (void*)offset;

            if (!tail) {
                freeSlab = newSlab;
            } else {
                tail->next = newSlab;
            }
            tail = newSlab;
        }

        tail->next = nullptr;
    }
    return true;
}

}  // namespace ker::mod::mm::dyn::slab
