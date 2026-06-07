#include "slab.hpp"

#include <cstddef>
#include <cstdint>

#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"

namespace ker::mod::mm::dyn::slab {

namespace {

auto slab_page(Slab* slab) -> void* { return reinterpret_cast<void*>(page_align_down(reinterpret_cast<uint64_t>(slab))); }

auto create_slab() -> Slab* {
    auto* free_slab = static_cast<FreeSlab*>(phys::page_alloc());
    if (free_slab == nullptr) {
        return nullptr;
    }
    if (!phys::page_mark_kind(free_slab, PageKind::SLAB)) {
        phys::page_free(free_slab);
        return nullptr;
    }

    free_slab->next = nullptr;

    auto* slab = reinterpret_cast<Slab*>(reinterpret_cast<uint64_t>(free_slab) + paging::PAGE_SIZE - sizeof(Slab));
    slab->next = nullptr;
    slab->prev = nullptr;
    slab->refs = 0;
    slab->freelist = free_slab;
    free_slab->parent = slab;
    free_slab->mem = free_slab;

    return slab;
}

void free_slab(Slab* slab) { phys::page_free(slab_page(slab)); }

void append_slab_list(SlabCache* cache, Slab* head) {
    if (head == nullptr) {
        return;
    }

    if (cache->slabs == nullptr) {
        cache->slabs = head;
        return;
    }

    auto* tail = cache->slabs;
    while (tail->next != nullptr) {
        tail = tail->next;
    }
    tail->next = head;
    head->prev = tail;
}

void free_slab_list(Slab* head) {
    while (head != nullptr) {
        auto* next = head->next;
        free_slab(head);
        head = next;
    }
}

auto init_slab_freelist(Slab* slab, uint64_t object_size, uint64_t size) -> void {
    auto* free_slab = slab->freelist;
    auto* slab_base = reinterpret_cast<uint8_t*>(free_slab);
    size_t const ELEMENT_COUNT = size / object_size;
    auto* tail = free_slab;

    for (size_t j = 1; j < ELEMENT_COUNT; j++) {
        auto* offset = slab_base + (object_size * j);
        auto* new_slab = reinterpret_cast<FreeSlab*>(offset);
        new_slab->parent = slab;
        new_slab->mem = reinterpret_cast<void*>(offset);
        tail->next = new_slab;
        tail = new_slab;
    }
    tail->next = nullptr;
}

auto append_slab(Slab* tail, Slab* slab) -> Slab* {
    if (tail != nullptr) {
        tail->next = slab;
        slab->prev = tail;
    }
    return slab;
}

auto make_slab_list(SlabCache* cache, uint64_t count) -> Slab* {
    Slab* head = nullptr;
    Slab* tail = nullptr;

    for (uint64_t i = 0; i < count; i++) {
        auto* slab = create_slab();
        if (slab == nullptr) {
            free_slab_list(head);
            return nullptr;
        }

        init_slab_freelist(slab, cache->object_size, cache->size);

        if (head == nullptr) {
            head = slab;
        }
        tail = append_slab(tail, slab);
    }
    return head;
}

}  // namespace

bool cache_grow(SlabCache* cache, uint64_t count) {
    auto* new_slabs = make_slab_list(cache, count);
    if (new_slabs == nullptr && count != 0) {
        return false;
    }

    append_slab_list(cache, new_slabs);
    return true;
}

}  // namespace ker::mod::mm::dyn::slab
