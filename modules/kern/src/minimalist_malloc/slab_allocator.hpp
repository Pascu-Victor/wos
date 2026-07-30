#pragma once
#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>

#include "bitmap.hpp"

inline constexpr uint32_t MAGIC = 0x8CBEEFC8;
inline constexpr size_t MEMORY_ALIGNMENT = 16;

template <size_t slab_size, size_t memory_size>
class Slab;
template <size_t slab_size, size_t memory_size, size_t max_blocks = memory_size / slab_size>
struct SlabHeader {
    uint32_t magic;
    uint32_t size;
    size_t free_blocks;
    size_t next_fit_block;
    Slab<slab_size, memory_size>* prev;
    Slab<slab_size, memory_size>* next;
    Slab<slab_size, memory_size>* nonfull_prev;
    Slab<slab_size, memory_size>* nonfull_next;
    bool on_nonfull_list;
    Bitmap<max_blocks> mem_map;
#ifdef WOS_KMALLOC_DEBUG_INFO
    // Diagnostic tracking per block: last caller address that freed the block and free count
    uintptr_t* last_free_caller;
    unsigned int* free_count;
#endif
};

template <size_t size>
struct alignas(MEMORY_ALIGNMENT) MemoryBlock {
    uintptr_t slab_ptr;
    uintptr_t align_pad;  // padding so data starts at offset 16 (16-byte aligned)
    std::array<char, size> data;

    // Offset from block start to user data (must match actual layout)
    static constexpr size_t DATA_OFFSET = 16;
};

template <size_t slab_size, size_t memory_size>
class Slab {
   private:
    static constexpr size_t MAX_HEADER_SIZE = sizeof(SlabHeader<slab_size, memory_size>);
    static constexpr size_t MAX_BLOCKS = (memory_size - MAX_HEADER_SIZE) / sizeof(MemoryBlock<slab_size>);
    static_assert(memory_size > MAX_HEADER_SIZE);
    static_assert((sizeof(MemoryBlock<slab_size>) + MAX_HEADER_SIZE) <= memory_size);

    SlabHeader<slab_size, memory_size, MAX_BLOCKS> header;
    std::array<MemoryBlock<slab_size>, MAX_BLOCKS> blocks;

    // Static spinlock shared across all slabs of the same size for thread safety
    static inline ker::mod::sys::Spinlock slab_lock;
    // Slabs stay on the ownership chain for diagnostics and safe magazine
    // reuse, but allocation must not scan that ever-growing chain. Keep a
    // separate intrusive list containing only slabs with reusable blocks.
    static inline Slab* nonfull_head;
    static inline Slab* ownership_tail;

    auto is_address_in_slab(void* address) -> bool;
    auto alloc_in_current_slab(size_t block_index) -> void*;
    void free_from_current_slab(size_t block_index);
    void link_nonfull_unlocked();
    void unlink_nonfull_unlocked();
    auto request_untracked_memory_from_os(size_t size) -> void*;
    auto request_memory_from_os(size_t size) -> void*;
    auto free_memory_to_os(void* address, size_t size) -> void;
    void mark_memory_as_slab(void* address);

    // Internal unlocked versions for use when lock is already held
    auto alloc_unlocked() -> void*;
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
    // debug ref stored in the _align_pad field.  Safe to call without the
    // slab_lock (caller must ensure quiescence, e.g. other CPUs halted during OOM dump).
    void iter_live_blocks_unlocked(void* userdata,
                                   void (*fn)(void* ud, const void* user_ptr, size_t block_size, uintptr_t debug_ref)) const;
};

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::init(Slab* prev) {
    header.magic = MAGIC;
    header.size = slab_size;
    header.prev = prev;
    header.next = nullptr;
    header.nonfull_prev = nullptr;
    header.nonfull_next = nullptr;
    header.on_nonfull_list = false;
    header.free_blocks = MAX_BLOCKS;
    header.next_fit_block = 0;
    header.mem_map.init();
#ifdef WOS_KMALLOC_DEBUG_INFO
    // lazily allocate diagnostic arrays to avoid blowing up header size
    header.last_free_caller = nullptr;
    header.free_count = nullptr;
    // allocate arrays on first use to reduce memory overhead
    // (done below if needed)
#endif
    if (prev == nullptr) {
        nonfull_head = this;
        ownership_tail = this;
        header.on_nonfull_list = true;
    } else {
        link_nonfull_unlocked();
    }
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::alloc_unlocked() -> void* {
    Slab* slab = nonfull_head;
    if (slab == nullptr) {
        return nullptr;
    }

    if (slab->header.magic != MAGIC || slab->header.size != slab_size || !slab->header.on_nonfull_list || slab->header.free_blocks == 0) {
        ker::mod::dbg::log(
            "slab: corrupt non-full head slab=%p magic=0x%x size=%u expected_size=%zu free_blocks=%zu listed=%u prev=%p next=%p", slab,
            slab->header.magic, slab->header.size, slab_size, slab->header.free_blocks, static_cast<unsigned>(slab->header.on_nonfull_list),
            slab->header.prev, slab->header.next);
        ker::mod::dbg::panic_handler("slab: corrupt non-full list");
    }

    size_t block_index = slab->header.mem_map.find_unused(slab->header.next_fit_block);
    if (block_index == BITMAP_NO_BITS_LEFT && slab->header.next_fit_block != 0) {
        block_index = slab->header.mem_map.find_unused();
    }
    if (block_index == BITMAP_NO_BITS_LEFT) {
        ker::mod::dbg::log("slab: non-full slab has no reusable block slab=%p size=%u free_blocks=%zu", slab, slab->header.size,
                           slab->header.free_blocks);
        ker::mod::dbg::panic_handler("slab: inconsistent free-block bitmap");
    }
    return slab->alloc_in_current_slab(block_index);
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::alloc() -> void* {
    Slab* new_slab = nullptr;

    slab_lock.lock();
    void* result = alloc_unlocked();
    if (result != nullptr) {
        slab_lock.unlock();
        return result;
    }
    slab_lock.unlock();

    new_slab = static_cast<Slab*>(request_untracked_memory_from_os(sizeof(Slab)));
    if (new_slab == nullptr) {
        return nullptr;
    }

    slab_lock.lock();
    result = alloc_unlocked();
    if (result != nullptr) {
        slab_lock.unlock();
        free_memory_to_os(new_slab, sizeof(Slab));
        return result;
    }

    Slab* tail = ownership_tail;
    if (tail == nullptr || tail->header.magic != MAGIC || tail->header.size != slab_size || tail->header.next != nullptr) {
        ker::mod::dbg::log("slab: corrupt ownership tail tail=%p magic=0x%x size=%u expected_size=%zu next=%p", tail,
                           tail != nullptr ? tail->header.magic : 0, tail != nullptr ? tail->header.size : 0, slab_size,
                           tail != nullptr ? tail->header.next : nullptr);
        ker::mod::dbg::panic_handler("slab: corrupt ownership tail");
    }

    new_slab->init(tail);
    mark_memory_as_slab(new_slab);
    tail->header.next = new_slab;
    ownership_tail = new_slab;
    result = new_slab->alloc_in_current_slab(0);
    slab_lock.unlock();
    return result;
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::free_unlocked(void* address) -> void {
    assert(header.magic == MAGIC);
    assert(header.size == slab_size);
    assert(is_address_in_slab(address));

    size_t block_index =
        (reinterpret_cast<uintptr_t>(address) - reinterpret_cast<uintptr_t>(blocks.data())) / sizeof(MemoryBlock<slab_size>);

    // Defensive checks: ensure computed index is in range and belongs to this slab.
    if (block_index >= MAX_BLOCKS || blocks.at(block_index).slab_ptr != reinterpret_cast<uintptr_t>(this) ||
        !header.mem_map.check_used(block_index)) {
        // Try a linear scan to find the matching block as a fallback (handles rare corruption/aliasing cases)
        size_t found = BITMAP_NO_BITS_LEFT;
        for (size_t i = 0; i < MAX_BLOCKS; ++i) {
            if (header.mem_map.check_used(i)) {
                if (static_cast<void*>(blocks.at(i).data.data()) == address) {
                    found = i;
                    break;
                }
            }
        }
        if (found == BITMAP_NO_BITS_LEFT) {
            // Not found: this is an invalid free or double free. Dump diagnostics to help root cause.
            ker::mod::dbg::log("slab: invalid free or double free detected for addr %p (slab=%p, computed_index=%d)", address, this,
                               static_cast<unsigned long>(block_index));
            ker::mod::dbg::log("slab header: magic=0x%x size=%d free_blocks=%d next_fit=%d prev=%p next=%p", header.magic,
                               static_cast<unsigned long>(header.size), static_cast<unsigned long>(header.free_blocks),
                               static_cast<unsigned long>(header.next_fit_block), header.prev, header.next);
            // Dump block table summary (only first 64 entries to avoid huge logs)
            size_t const LIMIT = std::min<size_t>(MAX_BLOCKS, 64);
            for (size_t i = 0; i < LIMIT; ++i) {
                // Read a prefix of the data to help diagnose buffer overrun corruption
                uint64_t prefix = 0;
                std::memcpy(&prefix, blocks.at(i).data.data(), sizeof(prefix));
                ker::mod::dbg::log("  block[%d]=%p slab_ptr=%p used=%d prefix=0x%x", static_cast<unsigned long>(i), &blocks.at(i).data,
                                   reinterpret_cast<void*>(blocks.at(i).slab_ptr), static_cast<int>(header.mem_map.check_used(i)),
                                   static_cast<unsigned long long>(prefix));
#ifdef WOS_KMALLOC_DEBUG_INFO
                if (header.free_count != nullptr && header.last_free_caller != nullptr && header.free_count[i] > 0) {
                    ker::mod::dbg::log("    last_free: caller=%p count=%d", reinterpret_cast<void*>(header.last_free_caller[i]),
                                       static_cast<int>(header.free_count[i]));
                }
#endif
            }
            // Search neighboring slabs in the chain for this address
            Slab* s = header.prev;
            while (s) {
                if ((address >= &s->blocks.at(0).data) && (address <= &s->blocks.at(MAX_BLOCKS - 1).data.at(slab_size - 1))) {
                    ker::mod::dbg::log("  address belongs to prev slab %p (size=%d)", s, static_cast<unsigned long>(s->header.size));
                }
                s = s->header.prev;
            }
            s = header.next;
            while (s) {
                if ((address >= &s->blocks.at(0).data) && (address <= &s->blocks.at(MAX_BLOCKS - 1).data.at(slab_size - 1))) {
                    ker::mod::dbg::log("  address belongs to next slab %p (size=%d)", s, static_cast<unsigned long>(s->header.size));
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
auto Slab<slab_size, memory_size>::free(void* address) -> void {
    Slab* retired = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
    uintptr_t* retired_last_free_caller = nullptr;
    unsigned int* retired_free_count = nullptr;
#endif
    slab_lock.lock();
    free_unlocked(address);
    if (header.free_blocks == MAX_BLOCKS && header.prev != nullptr) {
        if (header.on_nonfull_list) {
            unlink_nonfull_unlocked();
        }
        header.prev->header.next = header.next;
        if (header.next != nullptr) {
            header.next->header.prev = header.prev;
        } else {
            ownership_tail = header.prev;
        }
        header.prev = nullptr;
        header.next = nullptr;
#ifdef WOS_KMALLOC_DEBUG_INFO
        retired_last_free_caller = header.last_free_caller;
        retired_free_count = header.free_count;
        header.last_free_caller = nullptr;
        header.free_count = nullptr;
#endif
        header.magic = 0;
        retired = this;
    }
    slab_lock.unlock();

    if (retired != nullptr) {
        // The bitmap can be completely empty only when no per-CPU magazine
        // retains a block from this slab: magazine-held blocks remain marked
        // allocated until their batch is flushed through mini_free().
        (void)ker::mod::mm::phys::page_mark_kind(retired, ker::mod::mm::PageKind::NORMAL);
        free_memory_to_os(retired, sizeof(Slab));
#ifdef WOS_KMALLOC_DEBUG_INFO
        if (retired_last_free_caller != nullptr) {
            (void)ker::mod::mm::phys::page_mark_kind(retired_last_free_caller, ker::mod::mm::PageKind::NORMAL);
            free_memory_to_os(retired_last_free_caller, sizeof(uintptr_t) * MAX_BLOCKS);
        }
        if (retired_free_count != nullptr) {
            (void)ker::mod::mm::phys::page_mark_kind(retired_free_count, ker::mod::mm::PageKind::NORMAL);
            free_memory_to_os(retired_free_count, sizeof(unsigned int) * MAX_BLOCKS);
        }
#endif
    }
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::is_address_in_slab(void* address) -> bool {
    return static_cast<bool>((address >= blocks.data()) && (address <= &blocks.at(MAX_BLOCKS - 1).data[slab_size - 1]));
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::link_nonfull_unlocked() {
    assert(!header.on_nonfull_list);
    assert(header.free_blocks > 0);

    header.nonfull_prev = nullptr;
    header.nonfull_next = nonfull_head;
    if (nonfull_head != nullptr) {
        nonfull_head->header.nonfull_prev = this;
    }
    nonfull_head = this;
    header.on_nonfull_list = true;
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::unlink_nonfull_unlocked() {
    assert(header.on_nonfull_list);

    if (header.nonfull_prev != nullptr) {
        header.nonfull_prev->header.nonfull_next = header.nonfull_next;
    } else {
        nonfull_head = header.nonfull_next;
    }
    if (header.nonfull_next != nullptr) {
        header.nonfull_next->header.nonfull_prev = header.nonfull_prev;
    }
    header.nonfull_prev = nullptr;
    header.nonfull_next = nullptr;
    header.on_nonfull_list = false;
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::collect_stats(uint64_t& out_slab_count, uint64_t& out_total_blocks, uint64_t& out_free_blocks) const
    -> void {
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
                                                                                        uintptr_t debug_ref)) const {
    const Slab* s = this;
    while (s != nullptr) {
        for (size_t i = 0; i < MAX_BLOCKS; ++i) {
            if (!s->header.mem_map.check_used(i)) {
                continue;
            }
            // debug_ref is stored in _align_pad, which sits sizeof(uintptr_t)
            // bytes before the user data pointer.
            uintptr_t const DEBUG_REF = *reinterpret_cast<const uintptr_t*>(s->blocks.at(i).data.data() - sizeof(uintptr_t));
            fn(userdata, s->blocks.at(i).data.data(), slab_size, DEBUG_REF);
        }
        s = s->header.next;
    }
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::alloc_in_current_slab(size_t block_index) -> void* {
    assert(header.on_nonfull_list);
    assert(header.free_blocks > 0);
    header.mem_map.set_used(block_index);
    header.next_fit_block = (block_index + 1) % MAX_BLOCKS;
    header.free_blocks--;
    if (header.free_blocks == 0) {
        unlink_nonfull_unlocked();
    }
    blocks.at(block_index).slab_ptr = reinterpret_cast<uintptr_t>(this);
    return static_cast<void*>(blocks.at(block_index).data.data());
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::free_from_current_slab(size_t block_index) {
    bool const WAS_FULL = header.free_blocks == 0;
    header.mem_map.set_unused(block_index);
#ifdef WOS_KMALLOC_DEBUG_INFO
    // lazily allocate diagnostic arrays if not present
    if (!header.last_free_caller) {
        header.last_free_caller = static_cast<uintptr_t*>(request_memory_from_os(sizeof(uintptr_t) * MAX_BLOCKS));
        if (header.last_free_caller) {
            std::memset(header.last_free_caller, 0, sizeof(uintptr_t) * MAX_BLOCKS);
        }
    }
    if (!header.free_count) {
        header.free_count = static_cast<unsigned int*>(request_memory_from_os(sizeof(unsigned int) * MAX_BLOCKS));
        if (header.free_count) {
            std::memset(header.free_count, 0, sizeof(unsigned int) * MAX_BLOCKS);
        }
    }
    if (header.last_free_caller && header.free_count) {
        // Record the external caller (skip one more frame) so we can see who invoked free()
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wframe-address"
        header.last_free_caller[block_index] = reinterpret_cast<uintptr_t>(__builtin_return_address(2));
#pragma clang diagnostic pop
        header.free_count[block_index]++;
    }
#endif

    header.next_fit_block = block_index;
    header.free_blocks++;
    if (WAS_FULL) {
        link_nonfull_unlocked();
    }

    // A magazine-held block remains set in this bitmap. Therefore a non-root
    // slab that reaches MAX_BLOCKS after this transition has no deferred
    // pointers on any CPU and can be detached safely by free().
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::request_memory_from_os(size_t size) -> void* {
    // system dependent function, returns aligned memory region.
    void* address = request_untracked_memory_from_os(size);
    mark_memory_as_slab(address);
    return address;
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::request_untracked_memory_from_os(size_t size) -> void* {
    void* address = ker::mod::mm::phys::page_alloc(ker::mod::mm::PhysicalPageOwner::KMALLOC_SLAB, size, "kmalloc_slab");
    if (address == nullptr) {
        ker::mod::dbg::log("Malloc memory expansion failed halting.");
        assert(false);
    }
    return address;
}

template <size_t slab_size, size_t memory_size>
void Slab<slab_size, memory_size>::mark_memory_as_slab(void* address) {
    if (address != nullptr) {
        (void)ker::mod::mm::phys::page_mark_kind(address, ker::mod::mm::PageKind::SLAB);
    }
}

template <size_t slab_size, size_t memory_size>
auto Slab<slab_size, memory_size>::free_memory_to_os(void* address, size_t size) -> void {
    // system dependent function, returns aligned memory region.
    (void)size;
    ker::mod::mm::phys::page_free(address);
}
