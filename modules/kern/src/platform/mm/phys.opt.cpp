#include "phys.hpp"

#include <mod/io/serial/serial.hpp>

namespace ker::mod::mm::phys {
static sys::Spinlock memlock;

paging::PageZone* initPageZone(uint64_t base, uint64_t len, int zoneNum) {
    paging::PageZone* zone = (paging::PageZone*)base;

    base = PAGE_ALIGN_UP(base + sizeof(paging::PageZone));
    len -= paging::PAGE_SIZE;

    zone->start = base;
    zone->len = len;
    zone->name = "Physical Memory";
    zone->pageCount = len / paging::PAGE_SIZE;
    zone->zoneNum = zoneNum;
    zone->buddy = buddy_embed((uint8_t*)base, len);

    return zone;
}

static paging::PageZone* zones = nullptr;

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
    uint64_t totalSize = 0;
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        totalSize += zone->len;
    }

    mod::io::serial::write("Memory pool size: ");
    mod::io::serial::write(totalSize);
    mod::io::serial::write("B\n");
}

void* findFreeBlock(uint64_t size) {
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if (zone->len < size) {
            continue;
        }
        void* const block = buddy_malloc(zone->buddy, size);
        if (!block) {
            [[unlikely]] continue;
        }
        return block;
    }

    return nullptr;
}

void* pageAlloc(uint64_t size) {
    memlock.lock();
    void* block = findFreeBlock(size);

    if (block == nullptr) {
        [[unlikely]] memlock.unlock();
        return nullptr;
    }

    // memset(block, 0, size);

    memlock.unlock();
    return block;
}

void pageFree(void* page) {
    memlock.lock();
    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        if ((uint64_t)page < zone->start || (uint64_t)page > zone->start + zone->len) {
            continue;
        }

        buddy_free(zone->buddy, page);
        break;
    }
    memlock.unlock();
}

}  // namespace ker::mod::mm::phys
