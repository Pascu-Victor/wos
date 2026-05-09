#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>

#include "bitmap.hpp"

const static uint32_t MAGIC = 0x8CBEEFC8;

template <size_t slab_size, size_t memory_size>
class Slab;
template <size_t slab_size, size_t memory_size, size_t max_blocks = memory_size / slab_size>
struct SlabHeader {
    uint32_t magic;
    uint32_t size;
    size_t free_blocks;
    size_t next_fit_block;
    Slab<slab_size, memory_size>*prev, *next;
    Bitmap<max_blocks> mem_map;
    // Diagnostic tracking per block: last caller address that freed the block and free count
    uintptr_t* last_free_caller;
    unsigned int* free_count;
};

template <size_t size>
struct alignas(16) MemoryBlock {
    uintptr_t slab_ptr;
    uintptr_t _align_pad;  // padding so data starts at offset 16 (16-byte aligned)
    std::array<char, size> data;

    // Offset from block start to user data (must match actual layout)
    static constexpr size_t DATA_OFFSET = 16;
};

template <size_t slab_size, size_t memory_size>
class Slab {
   private:
    const static size_t MAX_HEADER_SIZE = sizeof(SlabHeader<slab_size, memory_size>);
    const static size_t MAX_BLOCKS = (memory_size - MAX_HEADER_SIZE) / sizeof(MemoryBlock<slab_size>);
    static_assert(memory_size > MAX_HEADER_SIZE);
    static_assert((sizeof(MemoryBlock<slab_size>) + MAX_HEADER_SIZE) <= memory_size);

    SlabHeader<slab_size, memory_size, MAX_BLOCKS> header;
    std::array<MemoryBlock<slab_size>, MAX_BLOCKS> blocks;

    // Static spinlock shared across all slabs of the same size for thread safety
    static inline ker::mod::sys::Spinlock slab_lock;

    bool is_address_in_slab(void* address);
    void* alloc_in_current_slab(size_t block_index);
    void* alloc_in_new_slab();
    void free_from_current_slab(size_t block_index);
    void* request_memory_from_os(size_t size);
    void free_memory_to_os(void* addrss, size_t size);

    // Internal unlocked versions for use when lock is already held
    void* alloc_unlocked();
    void free_unlocked(void* address);

   public:
    void init(Slab* prev = nullptr);
    void* alloc();
    void free(void* address);

    // Collect statistics about the slab chain starting at this slab.
    // Outputs (by reference): number of slab pages, total blocks across all slabs,
    // and total free blocks across all slabs. This function does not allocate.
    void collect_stats(uint64_t& out_slab_count, uint64_t& out_total_blocks, uint64_t& out_free_blocks) const;

    // Walk every live (allocated) block in this slab chain.
    // For each live block, fn receives: userdata, pointer to user data, block size, and the
    // debug_idx stored in the _align_pad field (lower 32 bits).  Safe to call without the
    // slab_lock (caller must ensure quiescence, e.g. other CPUs halted during OOM dump).
    void iter_live_blocks_unlocked(void* userdata, void (*fn)(void* ud, const void* user_ptr, size_t block_size, uint32_t debug_idx)) const;
};

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::init(Slab* prev) {
    header.magic = MAGIC;
    header.size = slab_size;
    header.prev = prev;
    header.next = nullptr;
    header.free_blocks = MAX_BLOCKS;
    header.next_fit_block = 0;
    header.mem_map.init();
    // lazily allocate diagnostic arrays to avoid blowing up header size
    header.last_free_caller = nullptr;
    header.free_count = nullptr;
    // allocate arrays on first use to reduce memory overhead
    // (done below if needed)
}

template <size_t slab_size, size_t memory_size>
void* Slab<slab_size, memory_size>::alloc_unlocked() {
    assert(header.magic == MAGIC);
    assert(header.size == slab_size);

    size_t block_index = -1;
    if (header.free_blocks) {
        block_index = header.mem_map.find_unused(header.next_fit_block);
        if (BITMAP_NO_BITS_LEFT != block_index) {
            return alloc_in_current_slab(block_index);
        }
    }
    if (header.next) {
        // Validate before following the pointer — misaligned means page-reuse UAF.
        auto next_addr = reinterpret_cast<uintptr_t>(header.next);
        if ((next_addr & 0xfULL) != 0 || ((next_addr < 0xffff800000000000ULL || next_addr >= 0xffff900000000000ULL) &&
                                          (next_addr < 0xffffffff80000000ULL || next_addr >= 0xffffffffc0000000ULL))) {
            ker::mod::dbg::log("slab UAF: header.next=0x%llx slab=%p free_blocks=%zu magic=0x%x size=%u", (unsigned long long)next_addr,
                               this, header.free_blocks, header.magic, header.size);
            ker::mod::dbg::panic_handler("slab: corrupt header.next — freed slab page reused");
        }
        return header.next->alloc_unlocked();
    }
    return alloc_in_new_slab();
}

template <size_t slab_size, size_t memory_size>
void* Slab<slab_size, memory_size>::alloc() {
    slab_lock.lock();
    void* result = alloc_unlocked();
    slab_lock.unlock();
    return result;
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::free_unlocked(void* address) {
    assert(header.magic == MAGIC);
    assert(header.size == slab_size);
    assert(is_address_in_slab(address));

    size_t block_index = (uintptr_t(address) - uintptr_t(blocks.data())) / sizeof(MemoryBlock<slab_size>);

    // Defensive checks: ensure computed index is in range and belongs to this slab.
    if (block_index >= MAX_BLOCKS || blocks[block_index].slab_ptr != uintptr_t(this) || !header.mem_map.check_used(block_index)) {
        // Try a linear scan to find the matching block as a fallback (handles rare corruption/aliasing cases)
        size_t found = BITMAP_NO_BITS_LEFT;
        for (size_t i = 0; i < MAX_BLOCKS; ++i) {
            if (header.mem_map.check_used(i)) {
                if (static_cast<void*>(blocks[i].data.data()) == address) {
                    found = i;
                    break;
                }
            }
        }
        if (found == BITMAP_NO_BITS_LEFT) {
            // Not found: this is an invalid free or double free. Dump diagnostics to help root cause.
            ker::mod::dbg::log("slab: invalid free or double free detected for addr %p (slab=%p, computed_index=%d)", address, this,
                               (unsigned long)block_index);
            ker::mod::dbg::log("slab header: magic=0x%x size=%d free_blocks=%d next_fit=%d prev=%p next=%p", header.magic,
                               (unsigned long)header.size, (unsigned long)header.free_blocks, (unsigned long)header.next_fit_block,
                               header.prev, header.next);
            // Dump block table summary (only first 64 entries to avoid huge logs)
            auto limit = MAX_BLOCKS;
            limit = std::min<unsigned int>(limit, 64);
            for (unsigned i = 0; i < limit; ++i) {
                // Read a prefix of the data to help diagnose buffer overrun corruption
                uint64_t prefix = 0;
                memcpy(&prefix, &blocks[i].data, sizeof(prefix));
                ker::mod::dbg::log("  block[%d]=%p slab_ptr=%p used=%d prefix=0x%x", (unsigned long)i, &blocks[i].data,
                                   (void*)blocks[i].slab_ptr, (int)header.mem_map.check_used(i), (unsigned long long)prefix);
                if (header.free_count[i] > 0) {
                    ker::mod::dbg::log("    last_free: caller=%p count=%d", (void*)header.last_free_caller[i], (int)header.free_count[i]);
                }
            }
            // Search neighboring slabs in the chain for this address
            Slab* s = header.prev;
            while (s) {
                if ((address >= &s->blocks[0].data) && (address <= &s->blocks[MAX_BLOCKS - 1].data[slab_size - 1])) {
                    ker::mod::dbg::log("  address belongs to prev slab %p (size=%d)", s, (unsigned long)s->header.size);
                }
                s = s->header.prev;
            }
            s = header.next;
            while (s) {
                if ((address >= &s->blocks[0].data) && (address <= &s->blocks[MAX_BLOCKS - 1].data[slab_size - 1])) {
                    ker::mod::dbg::log("  address belongs to next slab %p (size=%d)", s, (unsigned long)s->header.size);
                }
                s = s->header.next;
            }

            // Invalid free detected: skip actual free to avoid halting kernel; diagnostics already logged.
            ker::mod::dbg::log("slab: invalid free/double free detected - skipping actual free");
            return;
        }
        block_index = found;
    }

    free_from_current_slab(block_index);
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::free(void* address) {
    slab_lock.lock();
    free_unlocked(address);
    slab_lock.unlock();
}

template <size_t slab_size, size_t memory_size>
bool Slab<slab_size, memory_size>::is_address_in_slab(void* address) {
    return static_cast<bool>((address >= blocks.data()) && (address <= &blocks[MAX_BLOCKS - 1].data[slab_size - 1]));
    return true;
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::collect_stats(uint64_t& out_slab_count, uint64_t& out_total_blocks, uint64_t& out_free_blocks) const {
    const Slab* s = this;
    while (s != nullptr) {
        out_slab_count++;
        out_total_blocks += MAX_BLOCKS;
        out_free_blocks += s->header.free_blocks;
        s = s->header.next;
    }
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::iter_live_blocks_unlocked(void* userdata, void (*fn)(void* ud, const void* user_ptr, size_t block_size,
                                                                                        uint32_t debug_idx)) const {
    const Slab* s = this;
    while (s != nullptr) {
        for (size_t i = 0; i < MAX_BLOCKS; ++i) {
            if (!s->header.mem_map.check_used(i)) {
                continue;
            }
            // debug_idx is stored in the lower 32 bits of _align_pad, which sits
            // sizeof(uintptr_t) bytes before the user data pointer.
            uint32_t debug_idx = *reinterpret_cast<const uint32_t*>(s->blocks[i].data.data() - sizeof(uintptr_t));
            fn(userdata, s->blocks[i].data.data(), slab_size, debug_idx);
        }
        s = s->header.next;
    }
}

template <size_t slab_size, size_t memory_size>
void* Slab<slab_size, memory_size>::alloc_in_new_slab() {
    Slab* new_slab = static_cast<Slab*>(request_memory_from_os(sizeof(Slab)));
    if (new_slab == nullptr) {
        return nullptr;
    }
    new_slab->init(this);
    header.next = new_slab;
    // Call alloc_unlocked since we already hold the lock
    return new_slab->alloc_unlocked();
}

template <size_t slab_size, size_t memory_size>
void* Slab<slab_size, memory_size>::alloc_in_current_slab(size_t block_index) {
    header.mem_map.set_used(block_index);
    header.next_fit_block = (block_index + 1) % MAX_BLOCKS;
    header.free_blocks--;
    blocks[block_index].slab_ptr = uintptr_t(this);
    return static_cast<void*>(blocks[block_index].data.data());
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::free_from_current_slab(size_t block_index) {
    header.mem_map.set_unused(block_index);
    // lazily allocate diagnostic arrays if not present
    if (!header.last_free_caller) {
        header.last_free_caller = static_cast<uintptr_t*>(request_memory_from_os(sizeof(uintptr_t) * MAX_BLOCKS));
        if (header.last_free_caller) {
            memset(header.last_free_caller, 0, sizeof(uintptr_t) * MAX_BLOCKS);
        }
    }
    if (!header.free_count) {
        header.free_count = static_cast<unsigned int*>(request_memory_from_os(sizeof(unsigned int) * MAX_BLOCKS));
        if (header.free_count) {
            memset(header.free_count, 0, sizeof(unsigned int) * MAX_BLOCKS);
        }
    }
    if (header.last_free_caller && header.free_count) {
        // Record the external caller (skip one more frame) so we can see who invoked free()
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wframe-address"
        header.last_free_caller[block_index] = reinterpret_cast<uintptr_t>(__builtin_return_address(2));
#pragma clang diagnostic pop
    }

    header.next_fit_block = block_index;
    header.free_blocks++;

    // If a 0x100 slab is completely free, keep it linked instead of returning
    // the page to the OS. Repeated stale frees in fork/pipe stress are currently
    // turning into retired-page reuse panics before the first producer is visible.
    // Quarantining this size class keeps the slab metadata intact long enough to
    // surface the real invalid-free site.
    if constexpr (slab_size == 0x100) {
        return;
    }

    // If slab is completely free and it's not the first slab, return it to OS
    if ((header.free_blocks == MAX_BLOCKS) && (header.prev)) {
        // detach from list and free pages
        if (header.next) {
            // Unlink from middle
            header.next->header.prev = header.prev;
            header.prev->header.next = header.next;
        } else {
            // Tail slab, simple unlink
            header.prev->header.next = nullptr;
        }
#ifdef SLAB_DEBUG
        ker::mod::dbg::log("slab: freeing empty slab %p (size=%d)", this, (unsigned long)slab_size);
#endif
        // free diagnostic arrays if allocated
        if (header.last_free_caller) {
            free_memory_to_os(header.last_free_caller, sizeof(uintptr_t) * MAX_BLOCKS);
            header.last_free_caller = nullptr;
        }
        if (header.free_count) {
            free_memory_to_os(header.free_count, sizeof(unsigned int) * MAX_BLOCKS);
            header.free_count = nullptr;
        }
        free_memory_to_os(this, sizeof(Slab));
        // The slab committed suicide, don't ever use it again!
    }
}

template <size_t slab_size, size_t memory_size>
void* Slab<slab_size, memory_size>::request_memory_from_os(size_t size) {
    // system dependent function, returns aligned memory region.
    void* address = ker::mod::mm::phys::page_alloc(size);
    if (address == nullptr) {
        ker::mod::dbg::log("Malloc memory expansion failed halting.");
        assert(false);
    }
    return address;
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::free_memory_to_os(void* addrss, size_t size) {
    // system dependent function, returns aligned memory region.
    (void)size;
    ker::mod::mm::phys::page_free(addrss);
}
