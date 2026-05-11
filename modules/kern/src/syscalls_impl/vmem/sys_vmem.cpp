#include "sys_vmem.hpp"

#include <abi/callnums/vmem.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
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

void rollback_mapped_pages(ker::mod::sched::task::Task* task, uint64_t vaddr, uint64_t mapped_pages) {
    if (task == nullptr || task->pagemap == nullptr) {
        return;
    }

    for (uint64_t i = 0; i < mapped_pages; i++) {
        uint64_t const CURRENT_VADDR = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        if (ker::mod::mm::virt::is_page_mapped(task->pagemap, CURRENT_VADDR)) {
            ker::mod::mm::virt::unmap_page(task->pagemap, CURRENT_VADDR);
        }
    }
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
    auto const PAGE_FLAGS = prot_to_page_flags(prot);

    // Virtual mappings do not require physically contiguous backing. Allocate
    // page-by-page so mmap can use fragmented free memory instead of needing a
    // high-order buddy block for every large arena.
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;
    uint64_t mapped_pages = 0;
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        const void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-anon");
        if (PHYS_PAGE == nullptr) {
            log::error("out of physical memory after mapping %llu/%llu anon pages", static_cast<unsigned long long>(mapped_pages),
                       static_cast<unsigned long long>(NUM_PAGES));
            rollback_mapped_pages(task, vaddr, mapped_pages);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }

        // Get physical address
        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));

        // Map the page
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, paddr, PAGE_FLAGS);
        mapped_pages++;

        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=anon vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(paddr), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot));
        }
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

    auto const PAGE_FLAGS = prot_to_page_flags(prot);
    auto const NUM_PAGES = size / ker::mod::mm::paging::PAGE_SIZE;

    // Read file content into the backing pages
    auto const FILE_SIZE = static_cast<uint64_t>(st.st_size);
    uint64_t file_bytes_remaining = 0;
    if (offset < FILE_SIZE) {
        file_bytes_remaining = std::min(FILE_SIZE - offset, size);
        ker::vfs::vfs_lseek(fd, static_cast<off_t>(offset), 0 /* SEEK_SET */);
    }

    uint64_t mapped_pages = 0;
    for (uint64_t i = 0; i < NUM_PAGES; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        void* const PHYS_PAGE = ker::mod::mm::phys::page_alloc(ker::mod::mm::paging::PAGE_SIZE, "vmem-file");
        if (PHYS_PAGE == nullptr) {
            rollback_mapped_pages(task, vaddr, mapped_pages);
            return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
        }

        if (file_bytes_remaining > 0) {
            uint64_t const READ_SIZE = std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, file_bytes_remaining);
            ssize_t const READ_RET = ker::vfs::vfs_read(fd, PHYS_PAGE, static_cast<size_t>(READ_SIZE));
            if (READ_RET > 0) {
                auto const BYTES_READ = static_cast<uint64_t>(READ_RET);
                file_bytes_remaining -= std::min(BYTES_READ, file_bytes_remaining);
                if (BYTES_READ < READ_SIZE) {
                    file_bytes_remaining = 0;
                }
            } else {
                file_bytes_remaining = 0;
            }
        }

        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PHYS_PAGE)));
        ker::mod::mm::virt::map_page(task->pagemap, current_vaddr, paddr, PAGE_FLAGS);
        mapped_pages++;
        if (is_watched_mmap_vaddr(current_vaddr)) {
            log::warn(
                "watch mmap-map: pid=%lu name=%s pagemap=%p kind=file vaddr=0x%llx phys=0x%llx hint=0x%llx size=0x%llx flags=0x%llx "
                "prot=0x%llx fd=%d off=0x%llx",
                task->pid, task->name, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(current_vaddr),
                static_cast<unsigned long long>(paddr), static_cast<unsigned long long>(hint), static_cast<unsigned long long>(size),
                static_cast<unsigned long long>(flags), static_cast<unsigned long long>(prot), fd, static_cast<unsigned long long>(offset));
        }
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
