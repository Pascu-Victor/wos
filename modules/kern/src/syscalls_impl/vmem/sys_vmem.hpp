#pragma once

#include <cstdint>
#include <platform/mm/paging.hpp>

namespace ker::mod::sched::task {
struct LazyVmemRange;
struct Task;
}  // namespace ker::mod::sched::task

namespace ker::syscall::vmem {
class SharedVmemPublicationGuard {
   public:
    SharedVmemPublicationGuard();
    ~SharedVmemPublicationGuard();

    SharedVmemPublicationGuard(const SharedVmemPublicationGuard&) = delete;
    SharedVmemPublicationGuard(SharedVmemPublicationGuard&&) = delete;
    auto operator=(const SharedVmemPublicationGuard&) -> SharedVmemPublicationGuard& = delete;
    auto operator=(SharedVmemPublicationGuard&&) -> SharedVmemPublicationGuard& = delete;
};

struct FileMmapCacheStats {
    uint64_t pages;
    uint64_t bytes;
    uint64_t capacity_pages;
};

// Virtual memory syscall handler
// op: operation code (anon_allocate, anon_free)
// a1: hint address (for allocate) or address to free
// a2: size in bytes
// a3: protection flags (PROT_READ | PROT_WRITE | PROT_EXEC)
// a4: mapping flags (MAP_PRIVATE | MAP_ANONYMOUS, etc.)
auto sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t;
// Virtual memory mapping syscall handler
// hint: hint address
// size: size in bytes
// prot: protection flags
// flags: mapping flags
// fd: file descriptor (for file-backed mappings) or -1 for anonymous
// offset: offset in file (for file-backed mappings)
// addr: address to map (for MAP_FIXED) or 0
auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t;
auto file_mmap_cache_stats() -> FileMmapCacheStats;
auto materialize_lazy_file_page(ker::mod::sched::task::Task* task, const ker::mod::sched::task::LazyVmemRange& range, uint64_t page_vaddr,
                                const ker::mod::mm::paging::PageFault& fault) -> bool;
auto clone_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* src, ker::mod::mm::paging::PageTable* dst) -> bool;
void release_file_mmap_ranges_for_pagemap(ker::mod::mm::paging::PageTable* pagemap);

}  // namespace ker::syscall::vmem
