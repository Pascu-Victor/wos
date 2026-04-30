#pragma once
#include <defines/defines.hpp>

void mini_malloc_init();
void* mini_malloc(size_t size);
void mini_free(void* address);

// Dump mini-malloc internal stats (large allocations + slab usage). Safe to call during OOM.
void mini_dump_stats();

// Returns total bytes committed to slab pages across all slab sizes (no allocations)
uint64_t mini_get_total_slab_bytes();

// Returns the slab object size (one of 0x10/0x20/.../0x800) for a slab-allocated pointer,
// or 0 if the pointer is not a slab allocation or fails validation.
size_t mini_get_slab_size(void* ptr);

// Walk every live slab block across all size classes and invoke fn for each.
// fn receives: userdata, pointer to user data, block size, and the debug_idx stored in
// _align_pad (lower 32 bits).  Safe to call without locks when other CPUs are halted
// (e.g. during OOM dump).  Only compiled/linked when called.
void mini_iter_live_debug_slots(
    void* userdata,
    void (*fn)(void* ud, const void* user_ptr, size_t block_size, uint32_t debug_idx));
