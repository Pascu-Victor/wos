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

    // mini_malloc only handles slab allocations up to 0x800 bytes
    // Larger allocations should go through kmalloc
    if (size > 0x800) {
        return nullptr;  // Caller should use kmalloc for larger sizes
    }

    // Handle allocations via slab allocator
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

    // mini_free only handles slab allocations
    // All allocations > 0x800 should be freed through kmalloc
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
