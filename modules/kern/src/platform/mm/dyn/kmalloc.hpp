#pragma once
#include <cstddef>
#include <cstdint>
#include <minimalist_malloc/mini_malloc.hpp>
#include <new>

namespace ker::mod::mm::dyn::kmalloc {
struct KmallocTrackedTotals {
    uint64_t medium_count;
    uint64_t medium_bytes;
    uint64_t large_count;
    uint64_t large_bytes;
};

struct KmallocLiveAlloc {
    const char* tier;
    uint64_t addr;
    uint64_t size;
    uint64_t caller;
    const char* tag;
    bool has_debug;
};

struct KmallocCallerStat {
    const char* tier;
    uint64_t caller;
    const char* tag;
    uint64_t count;
    uint64_t bytes;
    bool has_debug;
};

struct KmallocDebugStats {
    uint64_t block_count;
    uint64_t block_bytes;
    uint64_t capacity;
    uint64_t active;
    uint64_t high_water;
    uint64_t dropped;
};

void init();
void enable_per_cpu_allocations();  // Call after cpuParamInit to enable per-CPU allocators

// Dump tracked large allocations for kmalloc (safe to call from OOM)
void dump_tracked_allocations();

// Fill outCount and outBytes with totals for tracked large allocations
void get_tracked_alloc_totals(uint64_t& out_count, uint64_t& out_bytes);
void get_tracked_alloc_breakdown(KmallocTrackedTotals& out);
auto snapshot_live_allocs(KmallocLiveAlloc* out, size_t max_rows, size_t& total_rows) -> size_t;
auto snapshot_caller_stats(KmallocCallerStat* out, size_t max_rows, size_t& total_rows) -> size_t;

auto debug_info_available() -> bool;
auto debug_info_enabled() -> bool;
void debug_info_set_enabled(bool enabled);
void debug_info_reset();
auto debug_info_generation() -> uint64_t;
auto debug_info_default_enabled() -> bool;
auto debug_info_stats() -> KmallocDebugStats;

auto malloc(uint64_t size) -> void*;
void free(void* ptr);
auto realloc(void* ptr, size_t size) -> void*;
auto calloc(size_t nmemb, size_t size) -> void*;

#ifdef WOS_KMALLOC_DEBUG_INFO
// Typed allocation: captures call-site address and a compile-time type string for OOM dumps.
// Called by the malloc<T>() template; may also be called directly with a custom tag.
auto malloc_tagged(uint64_t size, uintptr_t caller, const char* tag) -> void*;
#endif

template <typename T>
inline static auto malloc() -> T* {
#ifdef WOS_KMALLOC_DEBUG_INFO
    return static_cast<T*>(malloc_tagged(sizeof(T), reinterpret_cast<uintptr_t>(__builtin_return_address(0)), __PRETTY_FUNCTION__));
#else
    return static_cast<T*>(malloc(sizeof(T)));
#endif
}

template <typename T, int = sizeof(T)>
inline static void free(T* ptr) {
    ::ker::mod::mm::dyn::kmalloc::free(static_cast<void*>(ptr));
}
}  // namespace ker::mod::mm::dyn::kmalloc
