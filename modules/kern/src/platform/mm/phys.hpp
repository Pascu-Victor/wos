#pragma once

#include <limine.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/hcf.hpp>

namespace ker::mod::mm::phys {
void init(limine_memmap_response* memmapResponse);
void setKernelCr3(uint64_t cr3);  // Call after initPagemap to set kernel CR3 for safe memset
void initHugePageZoneDeferred();  // Call after initPagemap to initialize huge page zone
void enablePerCpuAllocations();   // Call after cpuParamInit to enable per-CPU page caches
auto pageAlloc(uint64_t size = ker::mod::mm::paging::PAGE_SIZE) -> void*;
auto pageAllocHuge(uint64_t size) -> void*;  // Allocate from huge page zone
void pageFree(void* page);

// --- Frame reference counting (for COW fork) ---
// Increment the refcount for a physical page (HHDM pointer).
// Pages start at refcount 1 after pageAlloc().
void pageRefInc(void* page);
// Decrement the refcount. When it reaches 0 the page is freed.
// Returns the new refcount (0 = freed).
uint32_t pageRefDec(void* page);
// Get current refcount for a physical page. Returns 0 for unknown/free pages.
uint32_t pageRefGet(void* page);

// Get the head of the memory zones list (for OOM diagnostics)
auto getZones() -> paging::PageZone*;
auto getHugePageZone() -> paging::PageZone*;

// Dump page allocation status when OOM - uses NO dynamic allocations
// Call this when out of memory to get diagnostic information
void dumpPageAllocationsOOM();

// Additional OOM helpers
void dumpMiniMallocStats();
void dumpKmallocTrackedAllocs();
void dumpAllocStats();  // Dump allocation/free counters for debugging
void enable_stack_overlap_check();

uint64_t get_free_mem_bytes();

template <typename T>
inline static void pageFree(T* page) {
    pageFree((void*)page);
}
}  // namespace ker::mod::mm::phys
