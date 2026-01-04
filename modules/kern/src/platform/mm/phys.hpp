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
auto pageAlloc(uint64_t size = ker::mod::mm::paging::PAGE_SIZE) -> void*;
void pageFree(void* page);

// Get the head of the memory zones list (for OOM diagnostics)
auto getZones() -> paging::PageZone*;

// Dump page allocation status when OOM - uses NO dynamic allocations
// Call this when out of memory to get diagnostic information
void dumpPageAllocationsOOM();

// Additional OOM helpers
void dumpMiniMallocStats();
void dumpKmallocTrackedAllocs();

template <typename T>
inline static void pageFree(T* page) {
    pageFree((void*)page);
}
}  // namespace ker::mod::mm::phys
