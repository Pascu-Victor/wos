#include "sys_vmem.hpp"

#include <abi/callnums/vmem.h>
#include <bits/off_t.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <util/hcf.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vmem {
using log = ker::mod::dbg::logger<"vmem">;

// Linux x86_64 user space address range
// User space: 0x0000000000000000 - 0x00007FFFFFFFFFFF (128TB)
// Kernel space: 0xFFFF800000000000 - 0xFFFFFFFFFFFFFFFF
constexpr uint64_t USER_SPACE_START = 0x0000000000400000ULL;  // Start after first 4MB (for NULL protection and low mem)
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;    // Linux canonical address limit
constexpr uint64_t MMAP_START = 0x0000100000000000ULL;        // mmap base - avoid collision with ELF debug info at 0x700000000000

namespace {
// Get the current task
inline auto get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

constexpr bool ENABLE_WATCHED_MMAP_LOGS = false;
constexpr uint64_t WATCH_MMAP_VADDR = 0x00001000007da000ULL;

inline auto is_watched_mmap_vaddr(uint64_t vaddr) -> bool { return ENABLE_WATCHED_MMAP_LOGS && vaddr == WATCH_MMAP_VADDR; }

// Find a free virtual address range of the given size
// Uses a simple linear search through allocated regions
auto find_free_range(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint) -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr) {
        return 0;
    }

    // Align size to page boundary
    size = page_align_up(size);

    // If hint is provided and valid, try to use it
    if (hint >= USER_SPACE_START && hint + size <= USER_SPACE_END) {
        // Check if the hint range is free
        bool is_free = true;
        for (uint64_t addr = hint; addr < hint + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::is_page_mapped(task->pagemap, addr)) {
                is_free = false;
                break;
            }
        }
        if (is_free) {
            return hint;
        }
    }

    // Simple linear search for free space starting from MMAP_START
    // TODO: Replace with a proper VMA allocator
    uint64_t const SEARCH_START = MMAP_START;
    uint64_t current_addr = SEARCH_START;

    while (current_addr + size <= USER_SPACE_END) {
        bool range_is_free = true;

        // Check if the entire range is unmapped
        for (uint64_t addr = current_addr; addr < current_addr + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::is_page_mapped(task->pagemap, addr)) {
                range_is_free = false;
                // Skip past this mapped page and align to next page
                current_addr = page_align_up(addr + 1);
                break;
            }
        }

        if (range_is_free) {
            return current_addr;
        }

        // Move to next potential range
        current_addr += ker::mod::mm::paging::PAGE_SIZE;
    }

    return 0;  // No free range found
}

// Convert protection flags to page table flags
auto prot_to_page_flags(uint64_t prot) -> uint64_t {
    uint64_t flags = ker::mod::mm::paging::PAGE_PRESENT | ker::mod::mm::paging::PAGE_USER;

    if ((prot & ker::abi::vmem::PROT_WRITE) != 0) {
        flags |= ker::mod::mm::paging::PAGE_WRITE;
    }

    // If not executable, set NX bit
    if ((prot & ker::abi::vmem::PROT_EXEC) == 0) {
        flags |= ker::mod::mm::paging::PAGE_NX;
    }

    return flags;
}

// Allocate anonymous memory
auto anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags) -> uint64_t {
    // Get current task
    auto* task = get_current_task();
    if (task == nullptr) {
        ker::mod::dbg::error("vmem: no current task");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        ker::mod::dbg::error("vmem: task has no pagemap");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate pagemap pointer is in valid HHDM range (not kernel static range)
    auto pm_addr = reinterpret_cast<uintptr_t>(task->pagemap);
    if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
        log::error("task PID %x has corrupted pagemap ptr 0x%x", task->pid, pm_addr);
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate size
    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check for size overflow
    if (size > USER_SPACE_END - USER_SPACE_START) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    // Align size to page boundary
    size = page_align_up(size);

    // Find a free virtual address range
    uint64_t vaddr = 0;

    if (((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0) {
        // MAP_FIXED: use exact address
        if (hint < USER_SPACE_START || hint + size > USER_SPACE_END) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        // Find a suitable range
        vaddr = find_free_range(task, size, hint);
        if (vaddr == 0) {
            log::warn("no free range found for size %x", size);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    }

    // Convert protection flags to page flags
    auto page_flags = prot_to_page_flags(prot);

    // Allocate and map pages
    auto num_pages = size / ker::mod::mm::paging::PAGE_SIZE;

    // Allocate all physical pages at once for efficiency
    void* phys_pages = ker::mod::mm::phys::page_alloc(size);
    if (phys_pages == nullptr) {
        log::error("out of physical memory for %llu pages", num_pages);
        ker::mod::mm::phys::dump_page_allocations_oom();
        // TODO: implement process termination on OOM here for now dump will HCF
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    for (uint64_t i = 0; i < num_pages; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        auto* phys_page = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(phys_pages) + (i * ker::mod::mm::paging::PAGE_SIZE));
        // Get physical address
        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(phys_page)));

        // Zero out the page for security
        memset(phys_page, 0, ker::mod::mm::paging::PAGE_SIZE);

        // Map the page
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, paddr, page_flags);

        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=anon vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(paddr), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot));
        }
    }

    if (!ker::mod::mm::phys::page_split_to_order0(phys_pages)) {
        log::error("failed to split anon backing block for leaf reclaim");
        hcf();
    }

    return vaddr;
}

// Free anonymous memory
auto anon_free(uint64_t addr, uint64_t size) -> uint64_t {
    // Get current task
    auto* task = get_current_task();
    if (task == nullptr) {
        log::error("vmem: no current task for free");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        log::error("vmem: task has no pagemap for free");
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate address
    if (addr == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    if (addr < USER_SPACE_START || addr >= USER_SPACE_END) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Validate size
    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check alignment
    if (addr % ker::mod::mm::paging::PAGE_SIZE != 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Align size to page boundary
    size = page_align_up(size);

    // Check bounds
    if (addr + size > USER_SPACE_END || addr + size < addr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Unmap pages
    uint64_t const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        uint64_t const CURRENT_VADDR = addr + (i * ker::mod::mm::paging::PAGE_SIZE);

        // Check if page is mapped
        if (ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR)) {
            if (is_watched_mmap_vaddr(CURRENT_VADDR)) {
                const auto PHYS = ker::mod::mm::virt::translate(task->pagemap, CURRENT_VADDR);
                log::warn("watch mmap-unmap: pid=%lu name=%s pagemap=%p vaddr=0x%llx phys=0x%llx size=0x%llx", task->pid, task->name,
                          static_cast<void*>(task->pagemap), static_cast<unsigned long long>(CURRENT_VADDR),
                          static_cast<unsigned long long>(PHYS), static_cast<unsigned long long>(size));
            }
            // Unmap the page (this also frees the physical page)
            ker::mod::mm::virt::unmap_page(task->pagemap, CURRENT_VADDR);
        }
    }
#ifdef VMEM_DEBUG
    log::debug("freed %x bytes at %p", size, addr);
#endif
    return 0;  // Success
}

// Allocate file-backed (MAP_PRIVATE) memory
auto file_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, int fd, uint64_t offset) -> uint64_t {
    auto* task = get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (size == 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    ker::vfs::Stat st{};
    if (ker::vfs::vfs_fstat(fd, &st) < 0) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    size = page_align_up(size);

    uint64_t vaddr = 0;
    if (((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0) {
        if (hint < USER_SPACE_START || hint + size > USER_SPACE_END) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        vaddr = find_free_range(task, size, hint);
        if (vaddr == 0) {
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }
    }

    auto page_flags = prot_to_page_flags(prot);
    auto num_pages = size / ker::mod::mm::paging::PAGE_SIZE;

    void* phys_pages = ker::mod::mm::phys::page_alloc(size);
    if (phys_pages == nullptr) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }
    memset(phys_pages, 0, size);

    // Read file content into the backing pages
    auto file_size = static_cast<uint64_t>(st.st_size);
    if (offset < file_size) {
        uint64_t read_size = file_size - offset;
        read_size = std::min(read_size, size);
        ker::vfs::vfs_lseek(fd, static_cast<off_t>(offset), 0 /* SEEK_SET */);
        ker::vfs::vfs_read(fd, phys_pages, static_cast<size_t>(read_size));
    }

    for (uint64_t i = 0; i < num_pages; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        auto* phys_page = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(phys_pages) + (i * ker::mod::mm::paging::PAGE_SIZE));
        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(phys_page)));
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, paddr, page_flags);
        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=file vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx fd=%d off=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(paddr), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot), fd, static_cast<unsigned long long>(offset));
        }
    }

    if (!ker::mod::mm::phys::page_split_to_order0(phys_pages)) {
        log::error("vmem: failed to split file backing block for leaf reclaim");
        hcf();
    }

    return vaddr;
}

}  // anonymous namespace

// Main syscall handler
auto sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t {
    switch (static_cast<ker::abi::vmem::ops>(op)) {
        case ker::abi::vmem::ops::ANON_ALLOCATE: {
            // a1: hint address
            // a2: size
            // a3: protection flags
            // a4: mapping flags
            return anon_allocate(a1, a2, a3, a4);
        }

        case ker::abi::vmem::ops::ANON_FREE: {
            // a1: address
            // a2: size
            return anon_free(a1, a2);
        }

        case ker::abi::vmem::ops::PROTECT: {
            // a1: address
            // a2: size
            // a3: new protection flags
            auto* task = get_current_task();
            if (task == nullptr || task->pagemap == nullptr) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EFAULT);
            }

            uint64_t const ADDR = a1;
            uint64_t size = a2;
            uint64_t const PROT = a3;

            if (size == 0) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }
            if (ADDR % ker::mod::mm::paging::PAGE_SIZE != 0) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            size = page_align_up(size);

            if (ADDR + size > USER_SPACE_END || ADDR + size < ADDR) {
                return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
            }

            auto page_flags = prot_to_page_flags(PROT);
            uint64_t const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;

            for (uint64_t i = 0; i < NUM_PAGES; i++) {
                uint64_t const CURRENT_VADDR = ADDR + (i * ker::mod::mm::paging::PAGE_SIZE);
                if (ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR)) {
                    ker::mod::mm::virt::unify_page_flags(task->pagemap, CURRENT_VADDR, page_flags);
                }
            }

            return 0;
        }

        default:
            log::warn("invalid operation %llu", op);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }
}

auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t {
    if ((flags & ker::abi::vmem::MAP_ANONYMOUS) != 0) {
        return anon_allocate(hint, size, prot, flags);
    }

    if (fd > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_EINVAL);
    }

    return file_allocate(hint, size, prot, flags, static_cast<int>(fd), offset);
}

}  // namespace ker::syscall::vmem
