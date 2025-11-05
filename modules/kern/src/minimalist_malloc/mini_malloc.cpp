#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

#include "slab_allocator.hpp"

const size_t PAGE_SIZE = 0x2000;
namespace {
Slab<0x010, PAGE_SIZE> slab_0x10;
Slab<0x020, PAGE_SIZE> slab_0x20;
Slab<0x040, PAGE_SIZE> slab_0x40;
Slab<0x080, PAGE_SIZE> slab_0x80;
Slab<0x100, PAGE_SIZE> slab_0x100;
Slab<0x200, PAGE_SIZE> slab_0x200;
Slab<0x300, PAGE_SIZE> slab_0x300;
Slab<0x400, PAGE_SIZE> slab_0x400;
Slab<0x800, PAGE_SIZE> slab_0x800;

struct LargeAllocation {
    void* virt_addr;
    void* phys_addr;
    size_t size;
    bool in_use;
};

constexpr size_t LARGE_ALLOC_THRESHOLD = 0x1000;  // 4KB
constexpr size_t MAX_LARGE_ALLOCS = 128;

// Large allocation tracking
std::array<LargeAllocation, MAX_LARGE_ALLOCS> large_allocs;
bool large_allocs_initialized = false;

void free_from_slab(SlabHeader<1, 1>& generic_slab, void* address) {
    switch (generic_slab.size) {
        case 0x10: {
            auto* slab = reinterpret_cast<Slab<0x10, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x20: {
            auto* slab = reinterpret_cast<Slab<0x20, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x40: {
            auto* slab = reinterpret_cast<Slab<0x40, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x80: {
            auto* slab = reinterpret_cast<Slab<0x80, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x100: {
            auto* slab = reinterpret_cast<Slab<0x100, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x200: {
            auto* slab = reinterpret_cast<Slab<0x200, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x300: {
            auto* slab = reinterpret_cast<Slab<0x300, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x400: {
            auto* slab = reinterpret_cast<Slab<0x400, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case 0x800: {
            auto* slab = reinterpret_cast<Slab<0x800, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        default:
            assert(false);
    }
}

}  // namespace

void mini_malloc_init() {
    slab_0x10.init();
    slab_0x20.init();
    slab_0x40.init();
    slab_0x80.init();
    slab_0x100.init();
    slab_0x200.init();
    slab_0x300.init();
    slab_0x400.init();
    slab_0x800.init();

    // Initialize large allocation tracking
    if (!large_allocs_initialized) {
        for (size_t i = 0; i < MAX_LARGE_ALLOCS; ++i) {
            large_allocs[i].virt_addr = nullptr;
            large_allocs[i].phys_addr = nullptr;
            large_allocs[i].size = 0;
            large_allocs[i].in_use = false;
        }
        large_allocs_initialized = true;
    }
}

auto mini_malloc(size_t size) -> void* {
    if (!size) {
        return nullptr;
    }

    // Handle large allocations (> 0x1000 bytes)
    if (size > LARGE_ALLOC_THRESHOLD) {
        // Round up size to page boundary (0x1000 = 4KB)
        size_t alloc_size = (size + 0xFFF) & ~0xFFF;

        // Find a free slot in the large allocation tracker
        size_t slot = MAX_LARGE_ALLOCS;
        for (size_t i = 0; i < MAX_LARGE_ALLOCS; ++i) {
            if (!large_allocs[i].in_use) {
                slot = i;
                break;
            }
        }

        if (slot == MAX_LARGE_ALLOCS) {
            ker::mod::io::serial::write("mini_malloc: no free slots for large allocation\n");
            return nullptr;
        }

        // Allocate physical pages
        void* phys_addr = ker::mod::mm::phys::pageAlloc(alloc_size);
        if (phys_addr == nullptr) {
            ker::mod::io::serial::write("mini_malloc: physical page allocation failed for size 0x");
            ker::mod::io::serial::writeHex(alloc_size);
            ker::mod::io::serial::write("\n");
            return nullptr;
        }

        // pageAlloc returns a virtual address already (HHDM mapped), not a physical address
        // So we can use it directly - no need to map since it's already in HHDM
        void* virt_addr = phys_addr;

        // Track the allocation
        large_allocs[slot].virt_addr = virt_addr;
        large_allocs[slot].phys_addr = virt_addr;  // Store the same address since pageAlloc returns virtual
        large_allocs[slot].size = alloc_size;
        large_allocs[slot].in_use = true;

        return virt_addr;
    }

    // Handle small allocations via slab allocator
    if (size <= 0x10) {
        return slab_0x10.alloc();
    } else if (size <= 0x20) {
        return slab_0x20.alloc();
    } else if (size <= 0x40) {
        return slab_0x40.alloc();
    } else if (size <= 0x80) {
        return slab_0x80.alloc();
    } else if (size <= 0x100) {
        return slab_0x100.alloc();
    } else if (size <= 0x200) {
        return slab_0x200.alloc();
    } else if (size <= 0x300) {
        return slab_0x300.alloc();
    } else if (size <= 0x400) {
        return slab_0x400.alloc();
    } else if (size <= 0x800) {
        return slab_0x800.alloc();
    }

    return nullptr;
}

void mini_free(void* address) {
    if (address == nullptr) {
        return;
    }

    // Check if this is a large allocation
    for (auto& large_alloc : large_allocs) {
        if (large_alloc.in_use && large_alloc.virt_addr == address) {
            // Free the physical pages
            ker::mod::mm::phys::pageFree(large_alloc.phys_addr);

            // Mark slot as free
            large_alloc.virt_addr = nullptr;
            large_alloc.phys_addr = nullptr;
            large_alloc.size = 0;
            large_alloc.in_use = false;
            return;
        }
    }

    // Not a large allocation, must be from slab allocator
    auto* block = reinterpret_cast<MemoryBlock<0>*>(uintptr_t(address) - sizeof(MemoryBlock<0>::slab_ptr));
    assert(block != nullptr);
    assert(block->slab_ptr);
    auto* generic_slab = reinterpret_cast<SlabHeader<1, 1>*>(block->slab_ptr);
    assert(generic_slab->magic == MAGIC);
    free_from_slab(*generic_slab, address);
}
