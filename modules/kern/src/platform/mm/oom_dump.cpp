#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <span>

#include "minimalist_malloc/mini_malloc.hpp"
#include "phys.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/smt/smt.hpp"
#include "util/hcf.hpp"

// Atomic flag to ensure only one CPU enters OOM dump.
// Keep this lock-free and allocation-free for OOM paths.
namespace {
std::atomic<uint64_t> oom_dump_in_progress{0};
}
// ============================================================================
// OOM DUMP PRE-ALLOCATED MEMORY
// All buffers are reserved at compile time to avoid any allocations during OOM
// ============================================================================

// Buffer for number-to-string conversions

namespace ker::mod::mm::phys {
namespace emergency_serial = ker::mod::dbg::emergency_serial_unlocked;
namespace io::serial {
using emergency_serial::write;
}

namespace {

constexpr size_t OOM_DUMP_BUFFER_SIZE = 256;
__attribute__((section(".bss"))) std::array<char, OOM_DUMP_BUFFER_SIZE> oom_dump_buffer;

// Maximum number of tasks we can track during OOM dump
constexpr size_t MAX_OOM_TRACKED_TASKS = 128;

// Pre-allocated structure to store task memory info during OOM dump
struct TaskMemoryInfo {
    const char* name;
    uint64_t pid;
    uint64_t page_count;         // Number of user-space pages mapped
    uint64_t page_table_count;   // Number of page table pages
    paging::PageTable* pagemap;  // Pointer to the pagemap
    bool is_active;              // Is this task in an active/wait queue?
    bool has_exited;             // Has this task exited?
    // Memory region breakdown
    uint64_t code_pages;   // Code/ELF pages
    uint64_t heap_pages;   // Heap pages
    uint64_t mmap_pages;   // mmap region (mlibc arenas)
    uint64_t stack_pages;  // Stack pages
    uint64_t rw_pages;     // Read-write pages
    uint64_t rx_pages;     // Read-execute pages
};
__attribute__((section(".bss"))) std::array<TaskMemoryInfo, MAX_OOM_TRACKED_TASKS> oom_task_info;
__attribute__((section(".bss"))) size_t oom_task_count;

// Pre-allocated array to track all known pagemaps for orphan detection
constexpr size_t MAX_OOM_TRACKED_PAGEMAPS = 256;
__attribute__((section(".bss"))) std::array<paging::PageTable*, MAX_OOM_TRACKED_PAGEMAPS> oom_known_pagemaps;
__attribute__((section(".bss"))) size_t oom_known_pagemap_count;

// Memory region tracking for per-process analysis
// Categorize memory by address ranges
struct MemoryRegionStats {
    uint64_t code_pages;   // Low addresses (typically 0x400000 - 0x1000000) - ELF code/data
    uint64_t heap_pages;   // Heap region (brk area, typically after code)
    uint64_t mmap_pages;   // mmap region where DSOs and mlibc arenas live
    uint64_t stack_pages;  // Stack region (high addresses near 0x7FFFFFFFFFFF)
    uint64_t other_pages;  // Other mapped pages
    uint64_t total_pages;
    uint64_t rw_pages;  // Read-write pages (likely heap/data)
    uint64_t rx_pages;  // Read-execute pages (likely code)
    uint64_t ro_pages;  // Read-only pages
};

// Address range constants for categorization
constexpr uint64_t CODE_REGION_START = 0x400000ULL;
constexpr uint64_t CODE_REGION_END = 0x10000000ULL;  // 256MB for code/data
constexpr uint64_t HEAP_REGION_START = 0x10000000ULL;
constexpr uint64_t HEAP_REGION_END = 0x200000000000ULL;    // Up to mmap region
constexpr uint64_t MMAP_REGION_START = 0x200000000000ULL;  // Above ASAN shadow, below ELF debug info
constexpr uint64_t MMAP_REGION_END = 0x700000000000ULL;    // Up to ELF debug info region
constexpr uint64_t STACK_REGION_START = 0x7F0000000000ULL;
constexpr uint64_t STACK_REGION_END = 0x800000000000ULL;  // End of canonical user space

// ============================================================================
// NUMBER CONVERSION HELPERS (no allocations)
// ============================================================================

// Helper to convert uint64_t to hex string without allocations
__attribute__((no_sanitize("address"))) auto u64_to_hex_no_alloc(uint64_t val, char* buf, size_t buf_size) -> char* {
    constexpr size_t HEX_CHARS_MAX = 17;  // 16 hex chars + null
    if (buf_size < HEX_CHARS_MAX) {
        buf[0] = '\0';
        return buf;
    }

    char* ptr = buf + buf_size - 1;
    *ptr = '\0';

    if (val == 0) {
        --ptr;
        *ptr = '0';
        return ptr;
    }

    constexpr uint8_t HEX_DIGIT_MASK = 0xF;
    constexpr uint8_t DECIMAL_BASE = 10;
    while (val > 0 && ptr > buf) {
        --ptr;
        uint8_t const DIGIT = val & HEX_DIGIT_MASK;
        *ptr = static_cast<char>((DIGIT < DECIMAL_BASE) ? ('0' + DIGIT) : ('a' + DIGIT - DECIMAL_BASE));
        val >>= 4;
    }

    return ptr;
}

// Helper to convert uint64_t to decimal string without allocations
__attribute__((no_sanitize("address"))) auto u64_to_dec_no_alloc(uint64_t val, char* buf, size_t buf_size) -> char* {
    constexpr size_t DEC_CHARS_MAX = 21;  // 20 decimal chars + null for max uint64
    if (buf_size < DEC_CHARS_MAX) {
        buf[0] = '\0';
        return buf;
    }

    char* ptr = buf + buf_size - 1;
    *ptr = '\0';

    if (val == 0) {
        --ptr;
        *ptr = '0';
        return ptr;
    }

    constexpr uint64_t DECIMAL_BASE = 10;
    while (val > 0 && ptr > buf) {
        --ptr;
        *ptr = static_cast<char>('0' + (val % DECIMAL_BASE));
        val /= DECIMAL_BASE;
    }

    return ptr;
}

// ============================================================================
// PAGE TABLE WALKING (no allocations)
// ============================================================================

// Count mapped pages in a page table (user-space only, first 256 entries of PML4)
// Also counts page table pages used
// Returns: user pages in first element, page table pages in second
struct PageCountResult {
    uint64_t user_pages;
    uint64_t page_table_pages;
    bool valid;  // Whether we could safely walk the page table
};

// Page table constants
constexpr size_t USER_SPACE_PML4_ENTRIES = 256;      // First 256 entries are user space
constexpr uint64_t PAGES_PER_1GB_HUGEPAGE = 262144;  // 1GB / 4KB
constexpr uint64_t PAGES_PER_2MB_HUGEPAGE = 512;     // 2MB / 4KB
constexpr uint64_t FRAME_SHIFT = 12;                 // Page table entry frame field shift

// Virtual address calculation bit shifts
constexpr uint64_t PML4_SHIFT = 39;
constexpr uint64_t PML3_SHIFT = 30;
constexpr uint64_t PML2_SHIFT = 21;
constexpr uint64_t PML1_SHIFT = 12;

// HHDM range constants - cached at runtime
uint64_t cached_hhdm_offset = 0;

// Helper to safely check if a virtual address is in HHDM range
__attribute__((no_sanitize("address"))) auto is_in_hhdm_range(uint64_t addr) -> bool {
    if (cached_hhdm_offset == 0) {
        cached_hhdm_offset = addr::get_hhdm_offset();
    }
    // HHDM typically covers up to 256TB of physical memory
    constexpr uint64_t MAX_HHDM_SIZE = 0x100000000000ULL;  // 1TB reasonable limit
    return addr >= cached_hhdm_offset && addr < (cached_hhdm_offset + MAX_HHDM_SIZE);
}

// Helper to check if a physical address falls within one of our known memory zones
// This is the SAFEST check - if it passes, we know the memory is accessible
__attribute__((no_sanitize("address"))) auto is_phys_addr_in_zone(uint64_t phys_addr) -> bool {
    for (paging::PageZone const* zone = get_zones(); zone != nullptr; zone = zone->next) {
        // Zone stores virtual addresses (HHDM mapped), convert back to physical
        uint64_t const ZONE_PHYS_START = zone->start - cached_hhdm_offset;
        uint64_t const ZONE_PHYS_END = ZONE_PHYS_START + zone->len;
        if (phys_addr >= ZONE_PHYS_START && phys_addr < ZONE_PHYS_END) {
            return true;
        }
    }
    return false;
}

// Convert physical address to virtual address in HHDM range
// Returns 0 if address cannot be safely accessed
__attribute__((no_sanitize("address"))) auto phys_to_virt_safe(uint64_t phys_addr) -> uint64_t {
    if (cached_hhdm_offset == 0) {
        cached_hhdm_offset = addr::get_hhdm_offset();
    }

    // Basic sanity checks
    if (phys_addr == 0) {
        return 0;
    }

    // Must be page aligned for page table entries
    if ((phys_addr & (paging::PAGE_SIZE - 1)) != 0) {
        return 0;
    }

    // Check against reasonable physical memory size (16TB max)
    constexpr uint64_t MAX_REASONABLE_PHYS = 0x100000000000ULL;
    if (phys_addr >= MAX_REASONABLE_PHYS) {
        return 0;
    }

    // The safest check: verify this physical address is in a known zone
    // This guarantees the HHDM mapping exists and is valid
    if (!is_phys_addr_in_zone(phys_addr)) {
        return 0;  // Not in any valid zone - don't access
    }

    return cached_hhdm_offset + phys_addr;
}

// Safely dump the buddy allocator free-block histogram for one zone.
// Scans pageFlags[] linearly, never follows any FreeBlock* next pointer.
__attribute__((no_sanitize("address"))) void dump_buddy_free_histogram(const paging::PageZone* zone) {
    if (zone == nullptr || zone->allocator == nullptr) {
        return;
    }
    const auto* alloc = zone->allocator;
    if (alloc->base == 0 || (alloc->base & (paging::PAGE_SIZE - 1)) != 0 || !is_in_hhdm_range(alloc->base)) {
        io::serial::write("  [buddy] base invalid, skipping histogram\n");
        return;
    }
    const auto* expected_flags = reinterpret_cast<const uint8_t*>(alloc->base + sizeof(PageAllocator));
    if (alloc->page_flags != expected_flags) {
        io::serial::write("  [buddy] pageFlags at unexpected offset, skipping histogram\n");
        return;
    }
    // totalPages must cover at least the metadata; usable + metadata = total
    if (alloc->total_pages == 0 || alloc->metadata_pages >= alloc->total_pages) {
        io::serial::write("  [buddy] totalPages/metadataPages inconsistent, skipping histogram\n");
        return;
    }

    constexpr uint8_t FLAG_KIND_MASK = 0xC0U;
    constexpr uint8_t FLAG_ORDER_MASK = 0x1FU;
    std::array<uint32_t, PageAllocator::MAX_ORDER + 1> order_counts{};
    uint32_t free_pages_found = 0;
    bool bad_order = false;

    for (uint32_t i = 0; i < alloc->total_pages; i++) {
        uint8_t const FLAG = alloc->page_flags[i];
        if ((FLAG & FLAG_KIND_MASK) != PageAllocator::FLAG_FREE_HEAD) {
            continue;
        }
        uint8_t const ORD = FLAG & FLAG_ORDER_MASK;
        if (ORD > PageAllocator::MAX_ORDER) {
            bad_order = true;
            continue;
        }
        *std::next(order_counts.begin(), ORD) += 1;
        free_pages_found += (1U << ORD);
    }

    io::serial::write("  Buddy free histogram:\n");
    bool any_free = false;
    uint8_t ord = 0;
    for (uint32_t const ORDER_COUNT : order_counts) {
        if (ORDER_COUNT == 0) {
            ord++;
            continue;
        }
        any_free = true;
        io::serial::write("    order ");
        io::serial::write(u64_to_dec_no_alloc(static_cast<uint64_t>(ord), oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(": ");
        io::serial::write(u64_to_dec_no_alloc(static_cast<uint64_t>(ORDER_COUNT), oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" block(s) = ");
        io::serial::write(u64_to_dec_no_alloc(static_cast<uint64_t>(ORDER_COUNT) << ord, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" pages\n");
        ord++;
    }
    if (!any_free) {
        io::serial::write("    (no free blocks)\n");
    }
    if (bad_order) {
        io::serial::write("  [buddy] WARNING: FLAG_FREE_HEAD with order > MAX_ORDER (corruption?)\n");
    }
    if (free_pages_found != alloc->free_count) {
        io::serial::write("  [buddy] WARNING: scan=");
        io::serial::write(u64_to_dec_no_alloc(static_cast<uint64_t>(free_pages_found), oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" freeCount=");
        io::serial::write(u64_to_dec_no_alloc(static_cast<uint64_t>(alloc->free_count), oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" (inconsistency - possible mid-op corruption)\n");
    }
}

// Get physical address from page table entry
__attribute__((no_sanitize("address"))) auto get_frame_addr(const paging::PageTableEntry& entry) -> uint64_t {
    return static_cast<uint64_t>(entry.frame) << FRAME_SHIFT;
}

// Safely walk page tables and count mapped pages
// Uses multiple layers of validation to avoid page faults
__attribute__((no_sanitize("address"))) auto count_mapped_pages_no_alloc(paging::PageTable* pml4) -> PageCountResult {
    PageCountResult result = {.user_pages = 0, .page_table_pages = 0, .valid = false};

    // Initialize HHDM offset cache if needed
    if (cached_hhdm_offset == 0) {
        cached_hhdm_offset = addr::get_hhdm_offset();
        if (cached_hhdm_offset == 0) {
            return result;  // HHDM not initialized
        }
    }

    if (pml4 == nullptr) {
        return result;
    }

    // Validate pml4 pointer itself - should be in HHDM range
    auto pml4_addr = reinterpret_cast<uint64_t>(pml4);
    if (!is_in_hhdm_range(pml4_addr)) {
        return result;  // Invalid pagemap pointer
    }

    // Mark as valid since we can at least access the PML4
    result.valid = true;

    // Only process user-space entries (first 256 entries of PML4)
    // Entries 256-511 are kernel space
    for (const auto& pml4_entry : std::span(pml4->entries.data(), USER_SPACE_PML4_ENTRIES)) {
        if (pml4_entry.present == 0) {
            continue;
        }

        // Get physical address of PML3 table
        uint64_t const PML3_PHYS_ADDR = get_frame_addr(pml4_entry);
        uint64_t const PML3_VIRT_ADDR = phys_to_virt_safe(PML3_PHYS_ADDR);
        if (PML3_VIRT_ADDR == 0) {
            // Can't safely access this PML3 - count the PML4 entry but skip
            result.page_table_pages++;
            continue;
        }

        auto* pml3 = reinterpret_cast<paging::PageTable*>(PML3_VIRT_ADDR);
        result.page_table_pages++;  // Count the PML3 page

        for (auto& entry : pml3->entries) {
            if (entry.present == 0) {
                continue;
            }

            // Check for 1GB huge page
            if (entry.pagesize != 0) {
                // 1GB page - counts as 262144 4KB pages
                result.user_pages += PAGES_PER_1GB_HUGEPAGE;
                continue;
            }

            // Get physical address of PML2 table
            uint64_t const PML2_PHYS_ADDR = get_frame_addr(entry);
            uint64_t const PML2_VIRT_ADDR = phys_to_virt_safe(PML2_PHYS_ADDR);
            if (PML2_VIRT_ADDR == 0) {
                result.page_table_pages++;
                continue;
            }

            auto* pml2 = reinterpret_cast<paging::PageTable*>(PML2_VIRT_ADDR);
            result.page_table_pages++;  // Count the PML2 page

            for (auto& entry : pml2->entries) {
                if (entry.present == 0) {
                    continue;
                }

                // Check for 2MB huge page
                if (entry.pagesize != 0) {
                    // 2MB page - counts as 512 4KB pages
                    result.user_pages += PAGES_PER_2MB_HUGEPAGE;
                    continue;
                }

                // Get physical address of PML1 table
                uint64_t const PML1_PHYS_ADDR = get_frame_addr(entry);
                uint64_t const PML1_VIRT_ADDR = phys_to_virt_safe(PML1_PHYS_ADDR);
                if (PML1_VIRT_ADDR == 0) {
                    result.page_table_pages++;
                    continue;
                }

                auto* pml1 = reinterpret_cast<paging::PageTable*>(PML1_VIRT_ADDR);
                result.page_table_pages++;  // Count the PML1 page

                // Count present entries in PML1 (actual 4KB pages)
                for (auto& entry : pml1->entries) {
                    if (entry.present != 0) {
                        result.user_pages++;
                    }
                }
            }
        }
    }

    return result;
}

// Analyze memory regions in detail by walking page tables
// Categorizes pages by virtual address range and permissions
// This gives insight into mlibc's mmap arena usage
__attribute__((no_sanitize("address"))) void analyze_memory_regions(paging::PageTable* pml4, MemoryRegionStats& stats) {
    // Zero out stats
    stats = {};

    if (cached_hhdm_offset == 0) {
        cached_hhdm_offset = addr::get_hhdm_offset();
        if (cached_hhdm_offset == 0) {
            return;
        }
    }

    if (pml4 == nullptr) {
        return;
    }

    auto pml4_addr = reinterpret_cast<uint64_t>(pml4);
    if (!is_in_hhdm_range(pml4_addr)) {
        return;
    }

    // Walk all user-space PML4 entries
    size_t i4 = 0;
    for (const auto& pml4_entry : std::span(pml4->entries.data(), USER_SPACE_PML4_ENTRIES)) {
        size_t const PML4_INDEX = i4++;
        if (pml4_entry.present == 0) {
            continue;
        }

        uint64_t const PML3_PHYS_ADDR = get_frame_addr(pml4_entry);
        uint64_t const PML3_VIRT_ADDR = phys_to_virt_safe(PML3_PHYS_ADDR);
        if (PML3_VIRT_ADDR == 0) {
            continue;
        }

        auto* pml3 = reinterpret_cast<paging::PageTable*>(PML3_VIRT_ADDR);

        size_t i3 = 0;
        for (const auto& pml3_entry : pml3->entries) {
            size_t const PML3_INDEX = i3++;
            if (pml3_entry.present == 0) {
                continue;
            }

            // Check for 1GB huge page
            if (pml3_entry.pagesize != 0) {
                // Calculate virtual address for this 1GB region
                uint64_t const VADDR = (PML4_INDEX << PML4_SHIFT) | (PML3_INDEX << PML3_SHIFT);
                uint64_t const PAGE_COUNT = PAGES_PER_1GB_HUGEPAGE;

                // Categorize by address
                if (VADDR >= MMAP_REGION_START && VADDR < MMAP_REGION_END) {
                    stats.mmap_pages += PAGE_COUNT;
                } else if (VADDR >= STACK_REGION_START && VADDR < STACK_REGION_END) {
                    stats.stack_pages += PAGE_COUNT;
                } else if (VADDR >= CODE_REGION_START && VADDR < CODE_REGION_END) {
                    stats.code_pages += PAGE_COUNT;
                } else if (VADDR >= HEAP_REGION_START && VADDR < HEAP_REGION_END) {
                    stats.heap_pages += PAGE_COUNT;
                } else {
                    stats.other_pages += PAGE_COUNT;
                }
                stats.total_pages += PAGE_COUNT;

                // Categorize by permissions
                if (pml3_entry.writable != 0) {
                    stats.rw_pages += PAGE_COUNT;
                } else if (pml3_entry.no_execute == 0) {
                    stats.rx_pages += PAGE_COUNT;
                } else {
                    stats.ro_pages += PAGE_COUNT;
                }
                continue;
            }

            uint64_t const PML2_PHYS_ADDR = get_frame_addr(pml3_entry);
            uint64_t const PML2_VIRT_ADDR = phys_to_virt_safe(PML2_PHYS_ADDR);
            if (PML2_VIRT_ADDR == 0) {
                continue;
            }

            auto* pml2 = reinterpret_cast<paging::PageTable*>(PML2_VIRT_ADDR);

            size_t i2 = 0;
            for (const auto& pml2_entry : pml2->entries) {
                size_t const PML2_INDEX = i2++;
                if (pml2_entry.present == 0) {
                    continue;
                }

                // Check for 2MB huge page
                if (pml2_entry.pagesize != 0) {
                    uint64_t const VADDR = (PML4_INDEX << PML4_SHIFT) | (PML3_INDEX << PML3_SHIFT) | (PML2_INDEX << PML2_SHIFT);
                    uint64_t const PAGE_COUNT = PAGES_PER_2MB_HUGEPAGE;

                    if (VADDR >= MMAP_REGION_START && VADDR < MMAP_REGION_END) {
                        stats.mmap_pages += PAGE_COUNT;
                    } else if (VADDR >= STACK_REGION_START && VADDR < STACK_REGION_END) {
                        stats.stack_pages += PAGE_COUNT;
                    } else if (VADDR >= CODE_REGION_START && VADDR < CODE_REGION_END) {
                        stats.code_pages += PAGE_COUNT;
                    } else if (VADDR >= HEAP_REGION_START && VADDR < HEAP_REGION_END) {
                        stats.heap_pages += PAGE_COUNT;
                    } else {
                        stats.other_pages += PAGE_COUNT;
                    }
                    stats.total_pages += PAGE_COUNT;

                    if (pml2_entry.writable != 0) {
                        stats.rw_pages += PAGE_COUNT;
                    } else if (pml2_entry.no_execute == 0) {
                        stats.rx_pages += PAGE_COUNT;
                    } else {
                        stats.ro_pages += PAGE_COUNT;
                    }
                    continue;
                }

                uint64_t const PML1_PHYS_ADDR = get_frame_addr(pml2_entry);
                uint64_t const PML1_VIRT_ADDR = phys_to_virt_safe(PML1_PHYS_ADDR);
                if (PML1_VIRT_ADDR == 0) {
                    continue;
                }

                auto* pml1 = reinterpret_cast<paging::PageTable*>(PML1_VIRT_ADDR);

                size_t i1 = 0;
                for (const auto& pml1_entry : pml1->entries) {
                    size_t const PML1_INDEX = i1++;
                    if (pml1_entry.present == 0) {
                        continue;
                    }

                    // Calculate full virtual address
                    uint64_t const VADDR =
                        (PML4_INDEX << PML4_SHIFT) | (PML3_INDEX << PML3_SHIFT) | (PML2_INDEX << PML2_SHIFT) | (PML1_INDEX << PML1_SHIFT);

                    // Categorize by address range
                    if (VADDR >= MMAP_REGION_START && VADDR < MMAP_REGION_END) {
                        stats.mmap_pages++;
                    } else if (VADDR >= STACK_REGION_START && VADDR < STACK_REGION_END) {
                        stats.stack_pages++;
                    } else if (VADDR >= CODE_REGION_START && VADDR < CODE_REGION_END) {
                        stats.code_pages++;
                    } else if (VADDR >= HEAP_REGION_START && VADDR < HEAP_REGION_END) {
                        stats.heap_pages++;
                    } else {
                        stats.other_pages++;
                    }
                    stats.total_pages++;

                    // Categorize by permissions
                    if (pml1_entry.writable != 0) {
                        stats.rw_pages++;
                    } else if (pml1_entry.no_execute == 0) {
                        stats.rx_pages++;
                    } else {
                        stats.ro_pages++;
                    }
                }
            }
        }
    }
}

// Check if a pagemap is in our known list
__attribute__((no_sanitize("address"))) auto is_pagemap_known(paging::PageTable* pagemap) -> bool {
    auto const KNOWN_PAGEMAPS = std::span(oom_known_pagemaps.data(), oom_known_pagemap_count);
    return std::ranges::find(KNOWN_PAGEMAPS, pagemap) != KNOWN_PAGEMAPS.end();
}

// Add a pagemap to our known list
__attribute__((no_sanitize("address"))) void add_known_pagemap(paging::PageTable* pagemap) {
    if (pagemap == nullptr || oom_known_pagemap_count >= MAX_OOM_TRACKED_PAGEMAPS) {
        return;
    }
    if (!is_pagemap_known(pagemap)) {
        *std::next(oom_known_pagemaps.begin(), static_cast<ptrdiff_t>(oom_known_pagemap_count++)) = pagemap;
    }
}

// ============================================================================
// TASK COLLECTION (no allocations)
// ============================================================================

// Collect info about a single task - VERY DEFENSIVE
// We can't trust any pointers from the task structure as they may be corrupted
__attribute__((no_sanitize("address"))) void collect_task_info(sched::task::Task* task, bool is_active) {
    if (task == nullptr || oom_task_count >= MAX_OOM_TRACKED_TASKS) {
        return;
    }

    // First, validate that the task pointer itself is in kernel space
    auto task_addr = reinterpret_cast<uint64_t>(task);
    if (!is_in_hhdm_range(task_addr)) {
        // Task pointer is not in valid kernel memory range
        return;
    }

    // Check if we already have this task (by PID, not pagemap - pagemap might be shared)
    for (const auto& tracked_task : std::span(oom_task_info.data(), oom_task_count)) {
        if (tracked_task.pid == task->pid) {
            return;  // Already tracked
        }
    }

    // Validate pagemap pointer
    paging::PageTable* pagemap = task->pagemap;
    TaskMemoryInfo& info = *std::next(oom_task_info.begin(), static_cast<ptrdiff_t>(oom_task_count));
    info.pid = task->pid;
    info.name = task->name;
    info.is_active = is_active;
    info.has_exited = task->has_exited;
    if (pagemap == nullptr) {
        // No pagemap - still record the task but with no memory info
        info.pagemap = nullptr;
        info.page_count = 0;
        info.page_table_count = 0;
        oom_task_count++;
        return;
    }

    // Check pagemap is in valid HHDM range
    auto pagemap_addr = reinterpret_cast<uint64_t>(pagemap);
    if (!is_in_hhdm_range(pagemap_addr)) {
        // Invalid pagemap pointer - record task but skip memory counting
        info.pagemap = pagemap;  // Store the bad address for debugging
        info.page_count = 0;
        info.page_table_count = 0;
        oom_task_count++;
        return;
    }

    info.pagemap = pagemap;

    // Count pages for this task - this should be safe now since pagemap is validated
    PageCountResult const COUNTS = count_mapped_pages_no_alloc(pagemap);
    info.page_count = COUNTS.user_pages;
    info.page_table_count = COUNTS.page_table_pages;

    // Analyze memory regions to get breakdown by address range
    // This shows where mlibc arenas (mmap region) and other memory is allocated
    MemoryRegionStats region_stats = {};
    analyze_memory_regions(pagemap, region_stats);
    info.code_pages = region_stats.code_pages;
    info.heap_pages = region_stats.heap_pages;
    info.mmap_pages = region_stats.mmap_pages;
    info.stack_pages = region_stats.stack_pages;
    info.rw_pages = region_stats.rw_pages;
    info.rx_pages = region_stats.rx_pages;

    oom_task_count++;

    // Track this pagemap
    add_known_pagemap(pagemap);
}

}  // namespace

// Dump page allocation status when OOM - uses NO dynamic allocations
// This function uses only pre-allocated static buffers and serial output
__attribute__((no_sanitize("address"))) void dump_page_allocations_oom() {
    asm volatile("cli");  // Disable interrupts to avoid concurrency issues during OOM dump

    // Atomically try to claim the OOM dump - only one CPU can proceed
    // Other CPUs that hit OOM will just halt immediately
    uint64_t expected = 0;
    if (!oom_dump_in_progress.compare_exchange_strong(expected, 1, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
        // Another CPU is already doing the OOM dump - just halt
        hcf();
    }

    // Halt all other CPUs immediately so they won't modify global state
    // while we perform a defensive OOM analysis on this core.
    ker::mod::smt::halt_other_cores();

    io::serial::write("\n");
    io::serial::write("╔══════════════════════════════════════════════════════════════════════╗\n");
    io::serial::write("║                    OOM PAGE ALLOCATION DUMP                          ║\n");
    io::serial::write("╚══════════════════════════════════════════════════════════════════════╝\n\n");

    // We'll print allocator stats later after the MEMORY SUMMARY so we can
    // use the computed totals there to refine the unaccounted memory estimate.
    // For now just call the raw dumps so they appear early in the log for visibility.
    mini_malloc::mini_dump_stats();
    ker::mod::mm::dyn::kmalloc::dump_tracked_allocations();
    dump_alloc_stats();  // Dump page allocation counters

    // Reset tracking arrays
    oom_task_count = 0;
    oom_known_pagemap_count = 0;
    std::memset(oom_task_info.data(), 0, sizeof(oom_task_info));
    std::memset(static_cast<void*>(oom_known_pagemaps.data()), 0, sizeof(oom_known_pagemaps));

    // ========================================================================
    // SECTION 1: Physical Memory Zones
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ PHYSICAL MEMORY ZONES                                               │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    paging::PageZone const* zones = get_zones();
    if (zones == nullptr) {
        io::serial::write("ERROR: No memory zones initialized!\n");
        return;
    }

    size_t zone_count = 0;
    uint64_t total_memory = 0;

    for (paging::PageZone const* zone = zones; zone != nullptr; zone = zone->next) {
        zone_count++;

        io::serial::write("Zone ");
        io::serial::write(u64_to_dec_no_alloc(zone->zone_num, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(": ");
        if (zone->name != nullptr) {
            io::serial::write(zone->name);
        }
        io::serial::write("\n");

        io::serial::write("  Start: 0x");
        io::serial::write(u64_to_hex_no_alloc(zone->start, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write("\n");

        io::serial::write("  Length: ");
        io::serial::write(u64_to_dec_no_alloc(zone->len, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" bytes (");
        constexpr uint64_t BYTES_PER_MB_ZONE = 1024ULL * 1024ULL;
        io::serial::write(u64_to_dec_no_alloc(zone->len / BYTES_PER_MB_ZONE, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" MB)\n");

        io::serial::write("  Page count: ");
        io::serial::write(u64_to_dec_no_alloc(zone->page_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write("\n");

        total_memory += zone->len;

        // Report free/usable page counts from the allocator (O(1), no tree walk).
        if (zone->allocator != nullptr) {
            io::serial::write("  Free pages: ");
            io::serial::write(u64_to_dec_no_alloc(zone->allocator->get_free_pages(), oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" / ");
            io::serial::write(u64_to_dec_no_alloc(zone->allocator->get_usable_pages(), oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write("\n");
            dump_buddy_free_histogram(zone);
        }

        io::serial::write("\n");
    }

    // ========================================================================
    // SECTION 2: Process Memory Usage
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ PROCESS MEMORY USAGE                                                │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    // Collect tasks from all CPUs
    uint64_t const CORE_COUNT = smt::get_core_count();
    uint64_t total_task_pages = 0;
    uint64_t total_page_table_pages = 0;
    uint64_t exited_task_pages = 0;
    uint64_t total_tasks_found = 0;

    io::serial::write("Scanning ");
    io::serial::write(u64_to_dec_no_alloc(CORE_COUNT, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" CPU(s) for tasks...\n\n");

    // Note: We access scheduler internals directly here since we're in OOM state
    // This is safe because we're just reading, not modifying
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; cpu_no++) {
        io::serial::write("CPU ");
        io::serial::write(u64_to_dec_no_alloc(cpu_no, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(":\n");
    }

    // Try to find tasks by scanning known PID range
    // Process in batches of MAX_OOM_TRACKED_TASKS to handle more tasks than our buffer
    io::serial::write("\nScanning for active processes...\n");
    constexpr uint64_t MAX_PID_SCAN = 4096;  // Scan more PIDs
    constexpr uint64_t BYTES_PER_PAGE = 4096;
    constexpr uint64_t BYTES_PER_KB = 1024;

    uint64_t current_pid = 1;
    uint64_t batch_number = 0;

    while (current_pid <= MAX_PID_SCAN) {
        // Reset batch tracking
        oom_task_count = 0;

        // Collect up to MAX_OOM_TRACKED_TASKS tasks in this batch
        while (current_pid <= MAX_PID_SCAN && oom_task_count < MAX_OOM_TRACKED_TASKS) {
            sched::task::Task* task = sched::find_task_by_pid(current_pid);
            if (task != nullptr) {
                collect_task_info(task, !task->has_exited);
            }
            current_pid++;
        }

        // If no tasks found in this batch, we're done
        if (oom_task_count == 0) {
            break;
        }

        total_tasks_found += oom_task_count;
        batch_number++;

        // Print batch header
        io::serial::write("\n--- Batch ");
        io::serial::write(u64_to_dec_no_alloc(batch_number, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" (");
        io::serial::write(u64_to_dec_no_alloc(oom_task_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" tasks) ---\n\n");

        // Print task information for this batch
        for (TaskMemoryInfo const& info : std::span(oom_task_info.data(), oom_task_count)) {
            io::serial::write("  PID ");
            io::serial::write(u64_to_dec_no_alloc(info.pid, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(": ");
            if (info.name != nullptr && is_in_hhdm_range(reinterpret_cast<uint64_t>(info.name))) {
                io::serial::write(info.name);
            } else {
                io::serial::write("<name inaccessible>");
            }

            // Status indicator
            if (info.has_exited) {
                io::serial::write(" [EXITED/ZOMBIE]");
                exited_task_pages += info.page_count;
            } else if (info.is_active) {
                io::serial::write(" [ACTIVE]");
            } else {
                io::serial::write(" [WAITING]");
            }
            io::serial::write("\n");

            io::serial::write("    Pagemap: 0x");
            io::serial::write(
                u64_to_hex_no_alloc(reinterpret_cast<uint64_t>(info.pagemap), oom_dump_buffer.data(), oom_dump_buffer.size()));

            // Check if pagemap is valid
            if (info.pagemap == nullptr) {
                io::serial::write(" (NULL)\n");
            } else if (!is_in_hhdm_range(reinterpret_cast<uint64_t>(info.pagemap))) {
                io::serial::write(" (INVALID - not in HHDM range)\n");
            } else {
                io::serial::write(" (valid)\n");
            }

            // Show detailed memory usage
            io::serial::write("    User pages: ");
            io::serial::write(u64_to_dec_no_alloc(info.page_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" (");
            io::serial::write(
                u64_to_dec_no_alloc((info.page_count * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" KB)\n");

            io::serial::write("    Page table pages: ");
            io::serial::write(u64_to_dec_no_alloc(info.page_table_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" (");
            io::serial::write(u64_to_dec_no_alloc((info.page_table_count * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(),
                                                  oom_dump_buffer.size()));
            io::serial::write(" KB)\n");

            // Memory region breakdown - shows mlibc arena usage
            io::serial::write("    Memory Regions:\n");
            io::serial::write("      Code/ELF:     ");
            io::serial::write(u64_to_dec_no_alloc(info.code_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages (");
            io::serial::write(
                u64_to_dec_no_alloc((info.code_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" KB)\n");

            io::serial::write("      Heap:         ");
            io::serial::write(u64_to_dec_no_alloc(info.heap_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages (");
            io::serial::write(
                u64_to_dec_no_alloc((info.heap_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" KB)\n");

            io::serial::write("      mmap (mlibc): ");
            io::serial::write(u64_to_dec_no_alloc(info.mmap_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages (");
            io::serial::write(
                u64_to_dec_no_alloc((info.mmap_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" KB) <- mlibc slab arenas\n");

            io::serial::write("      Stack:        ");
            io::serial::write(u64_to_dec_no_alloc(info.stack_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages (");
            io::serial::write(
                u64_to_dec_no_alloc((info.stack_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" KB)\n");

            io::serial::write("    Permissions:\n");
            io::serial::write("      RW (data/heap): ");
            io::serial::write(u64_to_dec_no_alloc(info.rw_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages\n");

            io::serial::write("      RX (code):      ");
            io::serial::write(u64_to_dec_no_alloc(info.rx_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
            io::serial::write(" pages\n\n");

            total_task_pages += info.page_count;
            total_page_table_pages += info.page_table_count;
        }
    }

    // Print total tasks found
    io::serial::write("\nTotal tasks found: ");
    io::serial::write(u64_to_dec_no_alloc(total_tasks_found, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" (scanned PIDs 1-");
    io::serial::write(u64_to_dec_no_alloc(MAX_PID_SCAN, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(")\n");

    // ========================================================================
    // SECTION 2.5: Kernel Dynamic Buffers
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ KERNEL DYNAMIC BUFFERS                                              │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    io::serial::write("\nScheduler Run Queues (per-CPU, EEVDF - zero dynamic allocations):\n");

    uint64_t total_runnable_count = 0;
    uint64_t total_dead_count = 0;
    uint64_t total_wait_count = 0;

    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; cpu_no++) {
        sched::RunQueueStats const RQ_STATS = sched::get_run_queue_stats(cpu_no);

        io::serial::write("  CPU ");
        io::serial::write(u64_to_dec_no_alloc(cpu_no, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(":\n");

        io::serial::write("    runnable_heap:  ");
        io::serial::write(u64_to_dec_no_alloc(RQ_STATS.active_task_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" tasks\n");

        io::serial::write("    deadList:      ");
        io::serial::write(u64_to_dec_no_alloc(RQ_STATS.expired_task_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" tasks\n");

        io::serial::write("    waitList:      ");
        io::serial::write(u64_to_dec_no_alloc(RQ_STATS.wait_queue_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" tasks\n");

        total_runnable_count += RQ_STATS.active_task_count;
        total_dead_count += RQ_STATS.expired_task_count;
        total_wait_count += RQ_STATS.wait_queue_count;
    }

    io::serial::write("\n  Totals across all CPUs:\n");
    io::serial::write("    Total runnable (heap): ");
    io::serial::write(u64_to_dec_no_alloc(total_runnable_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write("\n");

    io::serial::write("    Total dead (GC):       ");
    io::serial::write(u64_to_dec_no_alloc(total_dead_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write("\n");

    // Print dead tasks with their refcounts (useful for debugging leaks).
    io::serial::write("\nDead tasks (PID : refcount) per CPU:\n");
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; cpu_no++) {
        constexpr size_t BLOCK_SIZE = 128;
        std::array<uint64_t, BLOCK_SIZE> pids{};
        std::array<uint32_t, BLOCK_SIZE> refs{};

        io::serial::write("  CPU ");
        io::serial::write(u64_to_dec_no_alloc(cpu_no, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(": ");

        size_t start_index = 0;
        bool printed_any = false;
        while (true) {
            size_t const N = ker::mod::sched::get_expired_task_refcounts(cpu_no, pids.data(), refs.data(), BLOCK_SIZE, start_index);
            if (N == 0) {
                if (!printed_any) {
                    io::serial::write("(none)");
                }
                break;
            }

            // Print this block
            auto* ref = refs.data();
            for (uint64_t const PID : std::span(pids.data(), N)) {
                printed_any = true;
                io::serial::write("PID=");
                io::serial::write(u64_to_dec_no_alloc(PID, oom_dump_buffer.data(), oom_dump_buffer.size()));
                io::serial::write(" ref=");
                io::serial::write(u64_to_dec_no_alloc(*ref, oom_dump_buffer.data(), oom_dump_buffer.size()));
                io::serial::write("  ");
                ref++;
            }

            // Advance start index; if fewer than BLOCK returned, we've reached the end
            start_index += N;
            if (N < BLOCK_SIZE) {
                break;
            }
        }
        io::serial::write("\n");
    }

    io::serial::write("    Total waitList nodes:     ");
    io::serial::write(u64_to_dec_no_alloc(total_wait_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write("\n");

    // Scheduler queues use zero dynamic allocations (array-backed heap + intrusive lists)
    uint64_t const TOTAL_SCHED_LIST_BYTES = 0;
    io::serial::write("    Scheduler list memory: 0 bytes (zero-alloc EEVDF)\n");

    io::serial::write("\nThread Tracking:\n");
    uint64_t const ACTIVE_THREAD_COUNT = sched::threading::get_active_thread_count();
    // std::list<Thread*> node: prev + next + data = 24 bytes each
    constexpr uint64_t STD_LIST_NODE_SIZE = 24;
    io::serial::write("  activeThreads list: ");
    io::serial::write(u64_to_dec_no_alloc(ACTIVE_THREAD_COUNT, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" nodes (");
    io::serial::write(u64_to_dec_no_alloc(ACTIVE_THREAD_COUNT * STD_LIST_NODE_SIZE, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes)\n");

    // Estimate Task structure size - includes all fields
    constexpr uint64_t TASK_STRUCT_SIZE_ESTIMATE = sizeof(sched::task::Task) + sizeof(sched::threading::Thread) +
                                                   sizeof(paging::PageTable) + 512;  // Add padding for other fields and overhead
    constexpr uint64_t THREAD_STRUCT_SIZE = sizeof(sched::threading::Thread);

    io::serial::write("\nEstimated Kernel Object Memory:\n");

    uint64_t const TASK_OBJECTS_MEMORY = total_tasks_found * TASK_STRUCT_SIZE_ESTIMATE;
    io::serial::write("  Task objects (~");
    io::serial::write(u64_to_dec_no_alloc(TASK_STRUCT_SIZE_ESTIMATE, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes each): ");
    io::serial::write(u64_to_dec_no_alloc(total_tasks_found, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" x ");
    io::serial::write(u64_to_dec_no_alloc(TASK_STRUCT_SIZE_ESTIMATE, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" = ");
    io::serial::write(u64_to_dec_no_alloc(TASK_OBJECTS_MEMORY / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB\n");

    uint64_t const THREAD_OBJECTS_MEMORY = ACTIVE_THREAD_COUNT * THREAD_STRUCT_SIZE;
    io::serial::write("  Thread objects (");
    io::serial::write(u64_to_dec_no_alloc(THREAD_STRUCT_SIZE, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes each): ");
    io::serial::write(u64_to_dec_no_alloc(ACTIVE_THREAD_COUNT, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" x ");
    io::serial::write(u64_to_dec_no_alloc(THREAD_STRUCT_SIZE, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" = ");
    io::serial::write(u64_to_dec_no_alloc(THREAD_OBJECTS_MEMORY, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes\n");

    uint64_t const TOTAL_KERNEL_DYNAMIC = TOTAL_SCHED_LIST_BYTES + TASK_OBJECTS_MEMORY + THREAD_OBJECTS_MEMORY;
    io::serial::write("\n  Total estimated kernel dynamic allocations: ");
    io::serial::write(u64_to_dec_no_alloc(TOTAL_KERNEL_DYNAMIC / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB\n\n");

    // ========================================================================
    // SECTION 3: Memory Summary
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ MEMORY SUMMARY                                                      │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    constexpr uint64_t BYTES_PER_MB = 1024ULL * 1024ULL;

    io::serial::write("Physical Memory:\n");
    io::serial::write("  Total zones: ");
    io::serial::write(u64_to_dec_no_alloc(zone_count, oom_dump_buffer.data(), oom_dump_buffer.size()));

    io::serial::write("  Total memory: ");
    io::serial::write(u64_to_dec_no_alloc(total_memory / BYTES_PER_MB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" MB (");
    io::serial::write(u64_to_dec_no_alloc(total_memory, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes)\n");

    // Sum free pages across all zones
    uint64_t total_free_pages = 0;
    for (paging::PageZone const* z = get_zones(); z != nullptr; z = z->next) {
        if (z->allocator != nullptr) {
            total_free_pages += z->allocator->get_free_pages();
        }
    }
    io::serial::write("  Free memory: ");
    io::serial::write(
        u64_to_dec_no_alloc(total_free_pages * paging::PAGE_SIZE / BYTES_PER_MB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" MB (");
    io::serial::write(u64_to_dec_no_alloc(total_free_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" pages)\n");
    io::serial::write("  Used memory: ");
    io::serial::write(u64_to_dec_no_alloc((total_memory - (total_free_pages * paging::PAGE_SIZE)) / BYTES_PER_MB, oom_dump_buffer.data(),
                                          oom_dump_buffer.size()));
    io::serial::write(" MB\n\n");

    io::serial::write("Process Memory:\n");
    io::serial::write("  Total tasks tracked: ");
    io::serial::write(u64_to_dec_no_alloc(total_tasks_found, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write("\n");

    io::serial::write("  User pages: ");
    io::serial::write(u64_to_dec_no_alloc(total_task_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" (");
    io::serial::write(
        u64_to_dec_no_alloc((total_task_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB)\n");

    io::serial::write("  Page table pages: ");
    io::serial::write(u64_to_dec_no_alloc(total_page_table_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" (");
    io::serial::write(
        u64_to_dec_no_alloc((total_page_table_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB)\n");

    uint64_t const TOTAL_PROCESS_MEM = (total_task_pages + total_page_table_pages) * BYTES_PER_PAGE;
    io::serial::write("  Total process memory: ");
    io::serial::write(u64_to_dec_no_alloc(TOTAL_PROCESS_MEM / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB\n\n");

    // ========================================================================
    // SECTION 4: Dead/Zombie Memory (potential leaks)
    // ========================================================================
    if (exited_task_pages > 0) {
        io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
        io::serial::write("│    ZOMBIE/DEAD MEMORY DETECTED                                     │\n");
        io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

        io::serial::write("Memory held by exited processes: ");
        io::serial::write(
            u64_to_dec_no_alloc((exited_task_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" KB (");
        io::serial::write(u64_to_dec_no_alloc(exited_task_pages, oom_dump_buffer.data(), oom_dump_buffer.size()));
        io::serial::write(" pages)\n");
        io::serial::write("This memory can be reclaimed by reaping zombie processes.\n\n");
    }

    // ========================================================================
    // SECTION 5: Unaccounted Memory Analysis
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ MEMORY ACCOUNTING                                                   │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    io::serial::write("Total physical memory: ");
    io::serial::write(u64_to_dec_no_alloc(total_memory / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB\n");

    io::serial::write("Accounted process memory: ");
    io::serial::write(u64_to_dec_no_alloc(TOTAL_PROCESS_MEM / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB\n");

    // Show allocator-level accounting
    uint64_t kmalloc_count = 0;
    uint64_t kmalloc_bytes = 0;
    ker::mod::mm::dyn::kmalloc::get_tracked_alloc_totals(kmalloc_count, kmalloc_bytes);
    uint64_t const SLAB_BYTES = mini_malloc::mini_get_total_slab_bytes();

    io::serial::write("\n");
    phys::dump_caller_page_stats();

    io::serial::write("\nAllocator accounting:\n");
    io::serial::write("  kmalloc tracked large allocations: ");
    io::serial::write(u64_to_dec_no_alloc(kmalloc_count, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" entries, ");
    io::serial::write(u64_to_dec_no_alloc(kmalloc_bytes, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes (");
    io::serial::write(u64_to_dec_no_alloc(kmalloc_bytes / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB)\n");
    io::serial::write("  total slab memory (mini): ");
    io::serial::write(u64_to_dec_no_alloc(SLAB_BYTES, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" bytes (");
    io::serial::write(u64_to_dec_no_alloc(SLAB_BYTES / BYTES_PER_KB, oom_dump_buffer.data(), oom_dump_buffer.size()));
    io::serial::write(" KB)\n");

    io::serial::write("\n");
    io::serial::write("╔══════════════════════════════════════════════════════════════════════╗\n");
    io::serial::write("║                       END OOM DUMP                                   ║\n");
    io::serial::write("╚══════════════════════════════════════════════════════════════════════╝\n\n");
    hcf();
}
}  // namespace ker::mod::mm::phys
