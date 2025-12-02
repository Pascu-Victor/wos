#include "phys.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>

#include "buddy_alloc/buddy_alloc.hpp"
#include "limine.h"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace ker::mod::mm::phys {

namespace {
sys::Spinlock memlock;
__attribute__((section(".data"))) paging::PageZone* zones = nullptr;
}  // namespace

auto getZones() -> paging::PageZone* { return zones; }

namespace {
auto initPageZone(uint64_t base, uint64_t len, int zoneNum) -> paging::PageZone* {
    auto* zone = (paging::PageZone*)base;

    base = PAGE_ALIGN_UP(base + sizeof(paging::PageZone));
    len -= paging::PAGE_SIZE;

    zone->start = base;
    zone->len = len;
    zone->name = "Physical Memory";
    zone->pageCount = len / paging::PAGE_SIZE;
    zone->zoneNum = zoneNum;
    zone->buddyInstance = buddy_embed((uint8_t*)base, len);

    return zone;
}

auto findFreeBlock(uint64_t size) -> void* {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < size) {
            continue;
        }
        void* const block = buddy_malloc(zone->buddyInstance, size);
        if (block == nullptr) {
            [[unlikely]] continue;
        }
        return block;
    }

    return nullptr;
}

}  // namespace

void init(limine_memmap_response* memmapResponse) {
    if (memmapResponse == nullptr) {
        // TODO: logging
        hcf();
    }
    limine_memmap_response memmap = *(memmapResponse);

    paging::PageZone* zones_tail = nullptr;
    size_t zoneNum = 0;
    for (size_t i = 0; i < memmap.entry_count; i++) {
        if (memmap.entries[i]->type != LIMINE_MEMMAP_USABLE || memmap.entries[i]->length == paging::PAGE_SIZE) {
            continue;
        }

        paging::PageZone* zone =
            initPageZone((uint64_t)addr::getVirtPointer(memmap.entries[i]->base), memmap.entries[i]->length - 1, zoneNum++);

        if (zones_tail == nullptr) {
            zones = zone;  // set the head
        } else {
            zones_tail->next = zone;
        }
        zones_tail = zone;
    }

    if (zones_tail == nullptr) {
        hcf();  // no usable memory???
    }

    zones_tail->next = nullptr;
}

auto pageAlloc(uint64_t size) -> void* {
    memlock.lock();
    void* block = findFreeBlock(size);

    if (block == nullptr) {
        [[unlikely]] memlock.unlock();
        // OOM condition - dump allocation info for debugging
        io::serial::write("OOM: pageAlloc failed for size ");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        dumpPageAllocationsOOM();
        return nullptr;
    }

    memset(block, 0, size);

    memlock.unlock();
    return block;
}

void pageFree(void* page) {
    memlock.lock();
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if ((uint64_t)page < zone->start || (uint64_t)page > zone->start + zone->len) {
            continue;
        }

        // Get the buddy instance dynamically since it's in RELATIVE_MODE
        buddy* buddyPtr = buddy_get_embed_at((uint8_t*)zone->start, zone->len);
        if (buddyPtr != nullptr) {
            buddy_free(buddyPtr, page);
        }
        break;
    }
    memlock.unlock();
}

}  // namespace ker::mod::mm::phys
