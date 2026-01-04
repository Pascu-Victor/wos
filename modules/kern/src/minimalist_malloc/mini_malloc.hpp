#pragma once
#include <defines/defines.hpp>

void mini_malloc_init();
void* mini_malloc(size_t size);
void mini_free(void* address);

// Dump mini-malloc internal stats (large allocations + slab usage). Safe to call during OOM.
void mini_dump_stats();

// Returns total bytes committed to slab pages across all slab sizes (no allocations)
uint64_t mini_get_total_slab_bytes();
