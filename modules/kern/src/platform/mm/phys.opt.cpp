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
#include "platform/mm/virt.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace {
// Forward declaration - we'll get kernel pagemap physical address once during init
static uint64_t kernelCr3 = 0;
}  // anonymous namespace

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
    zone->buddyInstance = buddy_embed_alignment((uint8_t*)base, len, paging::PAGE_SIZE);

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
            initPageZone((uint64_t)addr::getVirtPointer(memmap.entries[i]->base), memmap.entries[i]->length, zoneNum++);

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

void setKernelCr3(uint64_t cr3) { kernelCr3 = cr3; }

auto pageAlloc(uint64_t size) -> void* {
    memlock.lock();
    void* block = findFreeBlock(size);

    if (block != nullptr) {
        // dbg::log("pageAlloc: size=%x returned=%p", size, block);
    }

    if (block == nullptr) {
        [[unlikely]] memlock.unlock();
        // OOM condition - dump allocation info for debugging
        io::serial::write("OOM: pageAlloc failed for size ");
        io::serial::writeHex(size);
        io::serial::write(" bytes\n");
        dumpPageAllocationsOOM();
        return nullptr;
    }

    // Validate the returned address is in a reasonable HHDM range
    auto blockAddr = (uint64_t)block;
    uint64_t hhdmBase = 0xffff800000000000ULL;
    uint64_t hhdmEnd = 0xffff808000000000ULL;  // ~512GB max physical
    if (blockAddr < hhdmBase || blockAddr >= hhdmEnd) {
        io::serial::write("FATAL: buddy returned invalid HHDM addr: ");
        io::serial::writeHex(blockAddr);
        io::serial::write("\n");
        memlock.unlock();
        hcf();
    }

    // If we're not using kernel CR3, temporarily switch for memset
    // This handles the case where userspace pagemaps don't have complete HHDM
    uint64_t savedCr3 = 0;
    if (kernelCr3 != 0) {
        uint64_t currentCr3 = rdcr3();
        if (currentCr3 != kernelCr3) {
            savedCr3 = currentCr3;
            wrcr3(kernelCr3);
        }
    }

    memset(block, 0, size);

    if (savedCr3 != 0) {
        wrcr3(savedCr3);
    }

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

void dumpMiniMallocStats() { ::mini_dump_stats(); }

void dumpKmallocTrackedAllocs() { ker::mod::mm::dyn::kmalloc::dumpTrackedAllocations(); }

}  // namespace ker::mod::mm::phys
