#include "slab.hpp"

#include <cstddef>
#include <cstdint>

#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"

namespace ker::mod::mm::dyn::slab {

namespace {

auto create_slab(SlabCache* cache) -> Slab* {
    auto* free_slab = static_cast<FreeSlab*>(phys::page_alloc());
    if (free_slab == nullptr) {
        return nullptr;
    }

    free_slab->next = nullptr;

    auto* slab = reinterpret_cast<Slab*>(reinterpret_cast<uint64_t>(free_slab) + paging::PAGE_SIZE - sizeof(Slab));
    slab->next = nullptr;
    slab->refs = 0;
    slab->freelist = free_slab;

    if (cache->slabs == nullptr) {
        cache->slabs = slab;
    } else {
        auto* tail = cache->slabs;
        while (tail->next != nullptr) {
            tail = tail->next;
        }
        tail->next = slab;
    }
    return slab;
}

}  // namespace

bool cache_grow(SlabCache* cache, uint64_t count) {
    for (uint64_t i = 0; i < count; i++) {
        auto* slab = create_slab(cache);
        if (slab == nullptr) {
            // TODO: probably should free the created but not added slabs
            return false;
        }
        auto* free_slab = slab->freelist;

        size_t const ELEMENT_COUNT = cache->size / cache->object_size;
        auto* tail = free_slab;

        for (size_t j = 0; j < ELEMENT_COUNT - 1; j++) {
            uint64_t* offset = reinterpret_cast<uint64_t*>(free_slab) + (cache->object_size * j);
            auto* new_slab = reinterpret_cast<FreeSlab*>(offset);
            new_slab->parent = slab;
            new_slab->mem = reinterpret_cast<void*>(offset);

            if (tail == nullptr) {
                free_slab = new_slab;
            } else {
                tail->next = new_slab;
            }
            tail = new_slab;
        }

        tail->next = nullptr;
    }
    return true;
}

}  // namespace ker::mod::mm::dyn::slab
