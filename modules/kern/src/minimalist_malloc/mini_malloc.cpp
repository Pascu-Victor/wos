#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/spinlock.hpp>

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

// Spinlock for large allocation tracking
ker::mod::sys::Spinlock largeAllocLock;

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

uint64_t mini_get_total_slab_bytes() {
    // Compute total slab bytes across types
    uint64_t totalSlabBytes = 0;
    auto addSlab = [&](auto& slabInstance) {
        uint64_t sCount = 0, tBlocks = 0, fBlocks = 0;
        slabInstance.collectStats(sCount, tBlocks, fBlocks);
        totalSlabBytes += sCount * PAGE_SIZE;
    };
    addSlab(slab_0x10);
    addSlab(slab_0x20);
    addSlab(slab_0x40);
    addSlab(slab_0x80);
    addSlab(slab_0x100);
    addSlab(slab_0x200);
    addSlab(slab_0x300);
    addSlab(slab_0x400);
    addSlab(slab_0x800);
    return totalSlabBytes;
}

void mini_dump_stats() {
    // Print large allocation table
    largeAllocLock.lock();
    uint64_t totalLarge = 0;
    uint64_t countLarge = 0;
    ker::mod::io::serial::write("mini_malloc: Large allocations:\n");
    for (size_t i = 0; i < MAX_LARGE_ALLOCS; ++i) {
        if (large_allocs[i].in_use) {
            countLarge++;
            totalLarge += large_allocs[i].size;
            ker::mod::io::serial::write("  Slot ");
            ker::mod::io::serial::writeHex(i);
            ker::mod::io::serial::write(": addr=0x");
            ker::mod::io::serial::writeHex((uint64_t)large_allocs[i].virt_addr);
            ker::mod::io::serial::write(" size=");
            ker::mod::io::serial::writeHex(large_allocs[i].size);
            ker::mod::io::serial::write("\n");
        }
    }
    ker::mod::io::serial::write("  Total large allocations: ");
    ker::mod::io::serial::writeHex(countLarge);
    ker::mod::io::serial::write(" entries, ");
    ker::mod::io::serial::writeHex(totalLarge);
    ker::mod::io::serial::write(" bytes\n");
    largeAllocLock.unlock();

    // Print slab usage for each slab type
    struct SlabTypeInfo {
        const char* name;
        uint64_t slabCount;
        uint64_t totalBlocks;
        uint64_t freeBlocks;
        uint64_t pageBytes;
    } infos[9] = {};

    // helper to fill info
    auto fillInfo = [&](const char* name, auto& slabInstance, uint64_t pageSize) {
        uint64_t sCount = 0, tBlocks = 0, fBlocks = 0;
        slabInstance.collectStats(sCount, tBlocks, fBlocks);
        return SlabTypeInfo{name, sCount, tBlocks, fBlocks, sCount * pageSize};
    };

    infos[0] = fillInfo("0x10", slab_0x10, PAGE_SIZE);
    infos[1] = fillInfo("0x20", slab_0x20, PAGE_SIZE);
    infos[2] = fillInfo("0x40", slab_0x40, PAGE_SIZE);
    infos[3] = fillInfo("0x80", slab_0x80, PAGE_SIZE);
    infos[4] = fillInfo("0x100", slab_0x100, PAGE_SIZE);
    infos[5] = fillInfo("0x200", slab_0x200, PAGE_SIZE);
    infos[6] = fillInfo("0x300", slab_0x300, PAGE_SIZE);
    infos[7] = fillInfo("0x400", slab_0x400, PAGE_SIZE);
    infos[8] = fillInfo("0x800", slab_0x800, PAGE_SIZE);

    ker::mod::io::serial::write("mini_malloc: Slab usage:\n");
    uint64_t totalSlabBytes = 0;
    for (auto& info : infos) {
        ker::mod::io::serial::write("  Slab ");
        ker::mod::io::serial::write(info.name);
        ker::mod::io::serial::write(": slabs=");
        ker::mod::io::serial::writeHex(info.slabCount);
        ker::mod::io::serial::write(" blocks_total=");
        ker::mod::io::serial::writeHex(info.totalBlocks);
        ker::mod::io::serial::write(" free_blocks=");
        ker::mod::io::serial::writeHex(info.freeBlocks);
        ker::mod::io::serial::write(" mem_bytes=");
        ker::mod::io::serial::writeHex(info.pageBytes);
        ker::mod::io::serial::write("\n");
        totalSlabBytes += info.pageBytes;
    }
    ker::mod::io::serial::write("  Total slab memory: ");
    ker::mod::io::serial::writeHex(totalSlabBytes);
    ker::mod::io::serial::write(" bytes\n");
}

auto mini_malloc(size_t size) -> void* {
    if (!size) {
        return nullptr;
    }

    // Handle large allocations (> 0x1000 bytes)
    if (size > LARGE_ALLOC_THRESHOLD) {
        largeAllocLock.lock();

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
            largeAllocLock.unlock();
            ker::mod::io::serial::write("mini_malloc: no free slots for large allocation\n");
            return nullptr;
        }

        // Allocate physical pages (pageAlloc has its own lock)
        largeAllocLock.unlock();  // Release lock during potentially slow pageAlloc
        void* phys_addr = ker::mod::mm::phys::pageAlloc(alloc_size);
        if (phys_addr == nullptr) {
            ker::mod::io::serial::write("mini_malloc: physical page allocation failed for size 0x");
            ker::mod::io::serial::writeHex(alloc_size);
            ker::mod::io::serial::write("\n");
            return nullptr;
        }
        largeAllocLock.lock();  // Re-acquire lock to update tracking

        // Re-check slot is still free (another thread might have taken it)
        if (large_allocs[slot].in_use) {
            // Find another slot
            slot = MAX_LARGE_ALLOCS;
            for (size_t i = 0; i < MAX_LARGE_ALLOCS; ++i) {
                if (!large_allocs[i].in_use) {
                    slot = i;
                    break;
                }
            }
            if (slot == MAX_LARGE_ALLOCS) {
                largeAllocLock.unlock();
                ker::mod::mm::phys::pageFree(phys_addr);
                ker::mod::io::serial::write("mini_malloc: no free slots after pageAlloc\n");
                return nullptr;
            }
        }

        // pageAlloc returns a virtual address already (HHDM mapped), not a physical address
        // So we can use it directly - no need to map since it's already in HHDM
        void* virt_addr = phys_addr;

        // Track the allocation
        large_allocs[slot].virt_addr = virt_addr;
        large_allocs[slot].phys_addr = virt_addr;  // Store the same address since pageAlloc returns virtual
        large_allocs[slot].size = alloc_size;
        large_allocs[slot].in_use = true;

        largeAllocLock.unlock();
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

    // Defensive: ensure address is in valid kernel memory range before touching it.
    // Valid ranges: HHDM (0xffff800000000000-0xffff900000000000) or kernel static (0xffffffff80000000-0xffffffffc0000000)
    uintptr_t addr_val = reinterpret_cast<uintptr_t>(address);
    bool inHHDM = (addr_val >= 0xffff800000000000ULL && addr_val < 0xffff900000000000ULL);
    bool inKernelStatic = (addr_val >= 0xffffffff80000000ULL && addr_val < 0xffffffffc0000000ULL);
    if (!inHHDM && !inKernelStatic) {
        ker::mod::dbg::log("mini_free: address %p outside valid kernel range (caller=%p); skipping free\n", address,
                           __builtin_return_address(0));
        return;
    }

    // Check if this is a large allocation
    largeAllocLock.lock();
    for (auto& large_alloc : large_allocs) {
        if (large_alloc.in_use && large_alloc.virt_addr == address) {
            // Mark slot as free first
            void* phys_to_free = large_alloc.phys_addr;
            large_alloc.virt_addr = nullptr;
            large_alloc.phys_addr = nullptr;
            large_alloc.size = 0;
            large_alloc.in_use = false;
            largeAllocLock.unlock();

            // Free the physical pages (pageFree has its own lock)
            ker::mod::mm::phys::pageFree(phys_to_free);
            return;
        }
    }
    largeAllocLock.unlock();

    // Not a large allocation, must be from slab allocator
    auto* block = reinterpret_cast<MemoryBlock<0>*>(uintptr_t(address) - sizeof(MemoryBlock<0>::slab_ptr));

    // Defensive checks: if block or slab_ptr are invalid, log diagnostics and skip freeing
    if (block == nullptr) {
        ker::mod::dbg::log("mini_free: computed block is NULL for address %p (caller=%p). Skipping free", address,
                           __builtin_return_address(0));
        return;
    }

    if (block->slab_ptr == 0) {
        ker::mod::dbg::log("mini_free: block %p (addr=%p) has NULL slab_ptr (caller=%p, caller2=%p). Skipping free", block, address,
                           __builtin_return_address(0), __builtin_return_address(1));
        return;
    }

    // Validate slab_ptr is in valid kernel memory range before dereferencing
    uintptr_t slab_ptr_val = block->slab_ptr;
    bool slabInHHDM = (slab_ptr_val >= 0xffff800000000000ULL && slab_ptr_val < 0xffff900000000000ULL);
    bool slabInKernelStatic = (slab_ptr_val >= 0xffffffff80000000ULL && slab_ptr_val < 0xffffffffc0000000ULL);
    if (!slabInHHDM && !slabInKernelStatic) {
        ker::mod::dbg::log("mini_free: block %p (addr=%p) has invalid slab_ptr %p (caller=%p, caller2=%p). Skipping free", block, address,
                           (void*)slab_ptr_val, __builtin_return_address(0), __builtin_return_address(1));
        return;
    }

    // Verify pointer aligns to block start (prevent interior pointer frees)
    void* expected_block_start = reinterpret_cast<void*>(reinterpret_cast<char*>(block) + sizeof(block->slab_ptr));
    if (expected_block_start != address) {
        ker::mod::dbg::log("mini_free: pointer %p is not aligned to block start %p (caller=%p); skipping free", address,
                           expected_block_start, __builtin_return_address(0));
        return;
    }

    auto* generic_slab = reinterpret_cast<SlabHeader<1, 1>*>(block->slab_ptr);
    if (generic_slab->magic != MAGIC) {
        // Log a small diagnostic snapshot to help root-cause the corruption
        uint64_t prefix = 0;
        // Attempt to read first 8 bytes at the address if readable
        prefix = *(reinterpret_cast<uint64_t*>(address));
        ker::mod::dbg::log("mini_free: invalid slab magic at slab_ptr=%p magic=0x%x addr=%p caller=%p prefix=0x%llx", generic_slab,
                           generic_slab->magic, address, __builtin_return_address(0), (unsigned long long)prefix);
        return;
    }

    free_from_slab(*generic_slab, address);
}
