#pragma once

#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>

namespace ker::mod::mm::mini_malloc {

constexpr size_t SLAB_SIZE10 = 0x10;
constexpr size_t SLAB_SIZE20 = 0x20;
constexpr size_t SLAB_SIZE40 = 0x40;
constexpr size_t SLAB_SIZE80 = 0x80;
constexpr size_t SLAB_SIZE100 = 0x100;
constexpr size_t SLAB_SIZE200 = 0x200;
constexpr size_t SLAB_SIZE300 = 0x300;
constexpr size_t SLAB_SIZE400 = 0x400;
constexpr size_t SLAB_SIZE800 = 0x800;

struct MiniSlabStats {
    const char* name;
    uint64_t object_size;
    uint64_t slab_count;
    uint64_t total_blocks;
    uint64_t free_blocks;
    uint64_t page_bytes;
};

void mini_malloc_init();
auto mini_malloc(size_t size) -> void*;
auto mini_free(void* address) -> void;

// Dump mini-malloc internal stats (large allocations + slab usage). Safe to call during OOM.
void mini_dump_stats();

// Returns total bytes committed to slab pages across all slab sizes (no allocations)
auto mini_get_total_slab_bytes() -> uint64_t;

// Copies per-size-class slab statistics into out. Returns rows written.
auto mini_collect_slab_stats(MiniSlabStats* out, size_t max_rows) -> size_t;

// Returns the slab object size (one of 0x10/0x20/.../0x800) for a slab-allocated pointer,
// or 0 if the pointer is not a slab allocation or fails validation.
auto mini_get_slab_size(void* ptr) -> size_t;

// Walk every live slab block across all size classes and invoke fn for each.
// fn receives: userdata, pointer to user data, block size, and the debug ref stored in
// _align_pad.  Safe to call without locks when other CPUs are halted
// (e.g. during OOM dump).  Only compiled/linked when called.
void mini_iter_live_debug_slots(void* userdata, void (*fn)(void* ud, const void* user_ptr, size_t block_size, uintptr_t debug_ref));

}  // namespace ker::mod::mm::mini_malloc
