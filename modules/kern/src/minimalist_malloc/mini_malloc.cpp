#include "minimalist_malloc/mini_malloc.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "slab_allocator.hpp"
namespace ker::mod::mm::mini_malloc {
namespace {
namespace emergency_serial = ker::mod::dbg::emergency_serial;
constexpr size_t PAGE_SIZE = 0x2000;

Slab<SLAB_SIZE10, PAGE_SIZE> slab_0x10;
Slab<SLAB_SIZE20, PAGE_SIZE> slab_0x20;
Slab<SLAB_SIZE40, PAGE_SIZE> slab_0x40;
Slab<SLAB_SIZE80, PAGE_SIZE> slab_0x80;
Slab<SLAB_SIZE100, PAGE_SIZE> slab_0x100;
Slab<SLAB_SIZE200, PAGE_SIZE> slab_0x200;
Slab<SLAB_SIZE300, PAGE_SIZE> slab_0x300;
Slab<SLAB_SIZE400, PAGE_SIZE> slab_0x400;
Slab<SLAB_SIZE800, PAGE_SIZE> slab_0x800;

void free_from_slab(SlabHeader<1, 1>& generic_slab, void* address) {
    switch (generic_slab.size) {
        case SLAB_SIZE10: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE10, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE20: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE20, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE40: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE40, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE80: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE80, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE100: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE100, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE200: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE200, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE300: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE300, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE400: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE400, PAGE_SIZE>*>(&generic_slab);
            slab->free(address);
            break;
        }
        case SLAB_SIZE800: {
            auto* slab = reinterpret_cast<Slab<SLAB_SIZE800, PAGE_SIZE>*>(&generic_slab);
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
    uint64_t total_slab_bytes = 0;
    auto add_slab = [&](auto& slab_instance) {
        uint64_t slab_count = 0;
        uint64_t total_blocks = 0;
        uint64_t free_blocks = 0;
        slab_instance.collect_stats(slab_count, total_blocks, free_blocks);
        total_slab_bytes += slab_count * PAGE_SIZE;
    };
    add_slab(slab_0x10);
    add_slab(slab_0x20);
    add_slab(slab_0x40);
    add_slab(slab_0x80);
    add_slab(slab_0x100);
    add_slab(slab_0x200);
    add_slab(slab_0x300);
    add_slab(slab_0x400);
    add_slab(slab_0x800);
    return total_slab_bytes;
}

auto mini_collect_slab_stats(MiniSlabStats* out, size_t max_rows) -> size_t {
    if (out == nullptr || max_rows == 0) {
        return 0;
    }

    size_t rows = 0;
    auto add_slab = [&](const char* name, uint64_t object_size, auto& slab_instance) {
        if (rows >= max_rows) {
            return;
        }
        uint64_t slab_count = 0;
        uint64_t total_blocks = 0;
        uint64_t free_blocks = 0;
        slab_instance.collect_stats(slab_count, total_blocks, free_blocks);
        out[rows++] = MiniSlabStats{.name = name,
                                    .object_size = object_size,
                                    .slab_count = slab_count,
                                    .total_blocks = total_blocks,
                                    .free_blocks = free_blocks,
                                    .page_bytes = slab_count * PAGE_SIZE};
    };

    add_slab("0x10", SLAB_SIZE10, slab_0x10);
    add_slab("0x20", SLAB_SIZE20, slab_0x20);
    add_slab("0x40", SLAB_SIZE40, slab_0x40);
    add_slab("0x80", SLAB_SIZE80, slab_0x80);
    add_slab("0x100", SLAB_SIZE100, slab_0x100);
    add_slab("0x200", SLAB_SIZE200, slab_0x200);
    add_slab("0x300", SLAB_SIZE300, slab_0x300);
    add_slab("0x400", SLAB_SIZE400, slab_0x400);
    add_slab("0x800", SLAB_SIZE800, slab_0x800);
    return rows;
}

void mini_iter_live_debug_slots(void* userdata, void (*fn)(void* ud, const void* user_ptr, size_t block_size, uintptr_t debug_ref)) {
    slab_0x10.iter_live_blocks_unlocked(userdata, fn);
    slab_0x20.iter_live_blocks_unlocked(userdata, fn);
    slab_0x40.iter_live_blocks_unlocked(userdata, fn);
    slab_0x80.iter_live_blocks_unlocked(userdata, fn);
    slab_0x100.iter_live_blocks_unlocked(userdata, fn);
    slab_0x200.iter_live_blocks_unlocked(userdata, fn);
    slab_0x300.iter_live_blocks_unlocked(userdata, fn);
    slab_0x400.iter_live_blocks_unlocked(userdata, fn);
    slab_0x800.iter_live_blocks_unlocked(userdata, fn);
}

void mini_dump_stats() {
    // Print slab usage for each slab type
    struct SlabTypeInfo {
        const char* name;
        uint64_t slab_count;
        uint64_t total_blocks;
        uint64_t free_blocks;
        uint64_t page_bytes;
    };

    // helper to fill info
    auto fill_info = [&](const char* name, auto& slab_instance, uint64_t page_size) {
        uint64_t slab_count = 0;
        uint64_t total_blocks = 0;
        uint64_t free_blocks = 0;
        slab_instance.collect_stats(slab_count, total_blocks, free_blocks);
        return SlabTypeInfo{.name = name,
                            .slab_count = slab_count,
                            .total_blocks = total_blocks,
                            .free_blocks = free_blocks,
                            .page_bytes = slab_count * page_size};
    };

    const std::array INFOS{
        fill_info("0x10", slab_0x10, PAGE_SIZE),   fill_info("0x20", slab_0x20, PAGE_SIZE),   fill_info("0x40", slab_0x40, PAGE_SIZE),
        fill_info("0x80", slab_0x80, PAGE_SIZE),   fill_info("0x100", slab_0x100, PAGE_SIZE), fill_info("0x200", slab_0x200, PAGE_SIZE),
        fill_info("0x300", slab_0x300, PAGE_SIZE), fill_info("0x400", slab_0x400, PAGE_SIZE), fill_info("0x800", slab_0x800, PAGE_SIZE)};

    emergency_serial::write("mini_malloc: Slab usage:\n");
    uint64_t total_slab_bytes = 0;
    for (const auto& info : INFOS) {
        emergency_serial::write("  Slab ");
        emergency_serial::write(info.name);
        emergency_serial::write(": slabs=0x");
        emergency_serial::write_hex(info.slab_count);
        emergency_serial::write(" blocks_total=0x");
        emergency_serial::write_hex(info.total_blocks);
        emergency_serial::write(" free_blocks=0x");
        emergency_serial::write_hex(info.free_blocks);
        emergency_serial::write(" mem_bytes=0x");
        emergency_serial::write_hex(info.page_bytes);
        emergency_serial::write("\n");
        total_slab_bytes += info.page_bytes;
    }
    emergency_serial::write("  Total slab memory: 0x");
    emergency_serial::write_hex(total_slab_bytes);
    emergency_serial::write(" bytes\n");
}

auto mini_malloc(size_t size) -> void* {
    if (size == 0U) {
        return nullptr;
    }

    // mini_malloc only handles slab allocations up to SLAB_SIZE800 bytes
    // Larger allocations should go through kmalloc
    if (size > SLAB_SIZE800) {
        return nullptr;  // Caller should use kmalloc for larger sizes
    }

    // Handle allocations via slab allocator
    if (size <= SLAB_SIZE10) {
        return slab_0x10.alloc();
    }
    if (size <= SLAB_SIZE20) {
        return slab_0x20.alloc();
    }
    if (size <= SLAB_SIZE40) {
        return slab_0x40.alloc();
    }
    if (size <= SLAB_SIZE80) {
        return slab_0x80.alloc();
    }
    if (size <= SLAB_SIZE100) {
        return slab_0x100.alloc();
    }
    if (size <= SLAB_SIZE200) {
        return slab_0x200.alloc();
    }
    if (size <= SLAB_SIZE300) {
        return slab_0x300.alloc();
    }
    if (size <= SLAB_SIZE400) {
        return slab_0x400.alloc();
    }
    if (size <= SLAB_SIZE800) {
        return slab_0x800.alloc();
    }

    return nullptr;
}

size_t mini_get_slab_size(void* ptr) {
    if (ptr == nullptr) {
        return 0;
    }
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    bool const VALID =
        (addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL) || (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL);
    if (!VALID) {
        return 0;
    }
    auto* block = reinterpret_cast<MemoryBlock<0>*>(addr - MemoryBlock<0>::DATA_OFFSET);
    if (block->slab_ptr == 0) {
        return 0;
    }
    uintptr_t const SP = block->slab_ptr;
    bool const SP_VALID =
        (SP >= 0xffff800000000000ULL && SP < 0xffff900000000000ULL) || (SP >= 0xffffffff80000000ULL && SP < 0xffffffffc0000000ULL);
    if (!SP_VALID) {
        return 0;
    }
    auto* hdr = reinterpret_cast<SlabHeader<1, 1>*>(SP);
    if (hdr->magic != MAGIC) {
        return 0;
    }
    return hdr->size;
}

void mini_free(void* address) {
    if (address == nullptr) {
        return;
    }

    // Defensive: ensure address is in valid kernel memory range before touching it.
    // Valid ranges: HHDM (0xffff800000000000-0xffff900000000000) or kernel static (0xffffffff80000000-0xffffffffc0000000)
    auto addr_val = reinterpret_cast<uintptr_t>(address);
    bool const IN_HHDM = (addr_val >= 0xffff800000000000ULL && addr_val < 0xffff900000000000ULL);
    bool const IN_KERNEL_STATIC = (addr_val >= 0xffffffff80000000ULL && addr_val < 0xffffffffc0000000ULL);
    if (!IN_HHDM && !IN_KERNEL_STATIC) {
        ker::mod::dbg::log("mini_free: address %p outside valid kernel range (caller=%p); skipping free\n", address,
                           __builtin_return_address(0));
        return;
    }

    // mini_free only handles slab allocations
    // All allocations > 0x800 should be freed through kmalloc
    auto* block = reinterpret_cast<MemoryBlock<0>*>(reinterpret_cast<uintptr_t>(address) - MemoryBlock<0>::DATA_OFFSET);

    // Defensive checks: if block or slab_ptr are invalid, log diagnostics and skip freeing
    if (block == nullptr) {
        ker::mod::dbg::log("mini_free: computed block is NULL for address %p (caller=%p). Skipping free", address,
                           __builtin_return_address(0));
        return;
    }

    if (block->slab_ptr == 0) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wframe-address"
        ker::mod::dbg::log("mini_free: block %p (addr=%p) has NULL slab_ptr (caller=%p, caller2=%p). Skipping free", block, address,
                           __builtin_return_address(0), __builtin_return_address(1));
#pragma clang diagnostic pop
        return;
    }

    // Validate slab_ptr is in valid kernel memory range before dereferencing
    uintptr_t const SLAB_PTR_VAL = block->slab_ptr;
    bool const SLAB_IN_HHDM = (SLAB_PTR_VAL >= 0xffff800000000000ULL && SLAB_PTR_VAL < 0xffff900000000000ULL);
    bool const SLAB_IN_KERNEL_STATIC = (SLAB_PTR_VAL >= 0xffffffff80000000ULL && SLAB_PTR_VAL < 0xffffffffc0000000ULL);
    if (!SLAB_IN_HHDM && !SLAB_IN_KERNEL_STATIC) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wframe-address"
        ker::mod::dbg::log("mini_free: block %p (addr=%p) has invalid slab_ptr %p (caller=%p, caller2=%p). Skipping free", block, address,
                           reinterpret_cast<void*>(SLAB_PTR_VAL), __builtin_return_address(0), __builtin_return_address(1));
#pragma clang diagnostic pop
        return;
    }

    // Verify pointer aligns to block start (prevent interior pointer frees)
    void* expected_block_start = reinterpret_cast<void*>(reinterpret_cast<char*>(block) + MemoryBlock<0>::DATA_OFFSET);
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
                           generic_slab->magic, address, __builtin_return_address(0), static_cast<unsigned long long>(prefix));
        return;
    }

    free_from_slab(*generic_slab, address);
}

}  // namespace ker::mod::mm::mini_malloc
