#include "sys_vmem.hpp"

#include <abi/callnums/vmem.h>
#include <assert.h>

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::syscall::vmem {

// Linux x86_64 user space address range
// User space: 0x0000000000000000 - 0x00007FFFFFFFFFFF (128TB)
// Kernel space: 0xFFFF800000000000 - 0xFFFFFFFFFFFFFFFF
constexpr uint64_t USER_SPACE_START = 0x0000000000400000ULL;  // Start after first 4MB (for NULL protection and low mem)
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL;    // Linux canonical address limit
constexpr uint64_t MMAP_START = 0x0000100000000000ULL;        // mmap base - avoid collision with ELF debug info at 0x700000000000

namespace {
// Get the current task
inline auto getCurrentTask() -> ker::mod::sched::task::Task* { return ker::mod::sched::getCurrentTask(); }

// Find a free virtual address range of the given size
// Uses a simple linear search through allocated regions
auto findFreeRange(ker::mod::sched::task::Task* task, uint64_t size, uint64_t hint) -> uint64_t {
    if (task == nullptr || task->pagemap == nullptr) {
        return 0;
    }

    // Align size to page boundary
    size = PAGE_ALIGN_UP(size);

    // If hint is provided and valid, try to use it
    if (hint >= USER_SPACE_START && hint + size <= USER_SPACE_END) {
        // Check if the hint range is free
        bool isFree = true;
        for (uint64_t addr = hint; addr < hint + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::isPageMapped(task->pagemap, addr)) {
                isFree = false;
                break;
            }
        }
        if (isFree) {
            return hint;
        }
    }

    // Simple linear search for free space starting from MMAP_START
    // TODO: Replace with a proper VMA allocator
    uint64_t searchStart = MMAP_START;
    uint64_t currentAddr = searchStart;

    while (currentAddr + size <= USER_SPACE_END) {
        bool rangeIsFree = true;

        // Check if the entire range is unmapped
        for (uint64_t addr = currentAddr; addr < currentAddr + size; addr += ker::mod::mm::paging::PAGE_SIZE) {
            if (ker::mod::mm::virt::isPageMapped(task->pagemap, addr)) {
                rangeIsFree = false;
                // Skip past this mapped page and align to next page
                currentAddr = PAGE_ALIGN_UP(addr + 1);
                break;
            }
        }

        if (rangeIsFree) {
            return currentAddr;
        }

        // Move to next potential range
        currentAddr += ker::mod::mm::paging::PAGE_SIZE;
    }

    return 0;  // No free range found
}

// Convert protection flags to page table flags
auto protToPageFlags(uint64_t prot) -> uint64_t {
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
auto anonAllocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags) -> uint64_t {
    // Get current task
    auto* task = getCurrentTask();
    if (task == nullptr) {
        ker::mod::dbg::error("vmem: no current task");
        return (uint64_t)(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        ker::mod::dbg::error("vmem: task has no pagemap");
        return (uint64_t)(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate pagemap pointer is in valid HHDM range (not kernel static range)
    auto pm_addr = reinterpret_cast<uintptr_t>(task->pagemap);
    if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
        ker::mod::dbg::log("vmem: task PID %x has corrupted pagemap ptr 0x%x", task->pid, pm_addr);
        return (uint64_t)(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate size
    if (size == 0) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check for size overflow
    if (size > USER_SPACE_END - USER_SPACE_START) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EOVERFLOW);
    }

    // Align size to page boundary
    size = PAGE_ALIGN_UP(size);

    // Find a free virtual address range
    uint64_t vaddr = 0;

    if (((flags & ker::abi::vmem::MAP_FIXED) != 0) && hint != 0) {
        // MAP_FIXED: use exact address
        if (hint < USER_SPACE_START || hint + size > USER_SPACE_END) {
            return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
        }
        if (hint % ker::mod::mm::paging::PAGE_SIZE != 0) {
            return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
        }
        vaddr = hint;
    } else {
        // Find a suitable range
        vaddr = findFreeRange(task, size, hint);
        if (vaddr == 0) {
            ker::mod::dbg::log("vmem: no free range found for size %x", size);
            return (uint64_t)(-ker::abi::vmem::VMEM_ENOMEM);
        }
    }

    // Convert protection flags to page flags
    auto page_flags = protToPageFlags(prot);

    // Allocate and map pages
    auto num_pages = size / ker::mod::mm::paging::PAGE_SIZE;

    // Allocate all physical pages at once for efficiency
    void* phys_pages = ker::mod::mm::phys::pageAlloc(size);
    if (phys_pages == nullptr) {
        ker::mod::dbg::log("vmem: out of physical memory for %llu pages", num_pages);
        ker::mod::mm::phys::dumpPageAllocationsOOM();
        // TODO: implement process termination on OOM here for now dump will HCF
        return static_cast<uint64_t>(-ker::abi::vmem::VMEM_ENOMEM);
    }

    for (uint64_t i = 0; i < num_pages; i++) {
        auto current_vaddr = vaddr + (i * ker::mod::mm::paging::PAGE_SIZE);
        auto* phys_page = reinterpret_cast<void*>(reinterpret_cast<uint64_t>(phys_pages) + (i * ker::mod::mm::paging::PAGE_SIZE));

        // Get physical address
        auto paddr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(reinterpret_cast<uint64_t>(phys_page)));

        // Zero out the page for security
        memset(phys_page, 0, ker::mod::mm::paging::PAGE_SIZE);

        // Map the page
        ker::mod::mm::virt::mapPage(task->pagemap, current_vaddr, paddr, page_flags);
    }

    return vaddr;
}

// Free anonymous memory
auto anonFree(uint64_t addr, uint64_t size) -> uint64_t {
    // Get current task
    auto* task = getCurrentTask();
    if (task == nullptr) {
        ker::mod::dbg::error("vmem: no current task for free");
        return (uint64_t)(-ker::abi::vmem::VMEM_EFAULT);
    }

    if (task->pagemap == nullptr) {
        ker::mod::dbg::error("vmem: task has no pagemap for free");
        return (uint64_t)(-ker::abi::vmem::VMEM_EFAULT);
    }

    // Validate address
    if (addr == 0) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    if (addr < USER_SPACE_START || addr >= USER_SPACE_END) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Validate size
    if (size == 0) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Check alignment
    if (addr % ker::mod::mm::paging::PAGE_SIZE != 0) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Align size to page boundary
    size = PAGE_ALIGN_UP(size);

    // Check bounds
    if (addr + size > USER_SPACE_END || addr + size < addr) {
        return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }

    // Unmap pages
    uint64_t numPages = size / ker::mod::mm::paging::PAGE_SIZE;
    for (uint64_t i = 0; i < numPages; i++) {
        uint64_t currentVaddr = addr + (i * ker::mod::mm::paging::PAGE_SIZE);

        // Check if page is mapped
        if (ker::mod::mm::virt::isPageMapped(task->pagemap, currentVaddr)) {
            // Unmap the page (this also frees the physical page)
            ker::mod::mm::virt::unmapPage(task->pagemap, currentVaddr);
        }
    }
#ifdef VMEM_DEBUG
    ker::mod::dbg::log("vmem: freed %x bytes at %p", size, addr);
#endif
    return 0;  // Success
}

}  // anonymous namespace

// Main syscall handler
auto sys_vmem(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t {
    switch (static_cast<ker::abi::vmem::ops>(op)) {
        case ker::abi::vmem::ops::anon_allocate: {
            // a1: hint address
            // a2: size
            // a3: protection flags
            // a4: mapping flags
            return anonAllocate(a1, a2, a3, a4);
        }

        case ker::abi::vmem::ops::anon_free: {
            // a1: address
            // a2: size
            return anonFree(a1, a2);
        }

        default:
            ker::mod::dbg::log("vmem: invalid operation %llu", op);
            return (uint64_t)(-ker::abi::vmem::VMEM_EINVAL);
    }
}

auto sys_vmem_map(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags, uint64_t fd, uint64_t offset) -> uint64_t {
    // For now, only support anonymous mappings
    if (fd != (uint64_t)(-1)) {
        ker::mod::dbg::log("vmem_map: only anonymous mappings supported");
        return (uint64_t)(-ker::abi::vmem::VMEM_ENOSYS);
    }

    // Use anonAllocate for anonymous mappings
    return anonAllocate(hint, size, prot, flags);
}

}  // namespace ker::syscall::vmem
