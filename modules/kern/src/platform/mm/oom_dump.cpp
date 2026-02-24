#include <cstddef>
#include <cstdint>
#include <cstring>

#include "mod/io/serial/serial.hpp"
#include "phys.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/smt/smt.hpp"
#include "util/hcf.hpp"

// Atomic flag to ensure only one CPU enters OOM dump
// Uses __atomic builtins to safely coordinate between CPUs
namespace {
volatile uint64_t oomDumpInProgress = 0;
}
// ============================================================================
// OOM DUMP PRE-ALLOCATED MEMORY
// All buffers are reserved at compile time to avoid any allocations during OOM
// ============================================================================

// Buffer for number-to-string conversions

namespace ker::mod::mm::phys {

namespace {

constexpr size_t OOM_DUMP_BUFFER_SIZE = 256;
__attribute__((section(".bss"))) char oomDumpBuffer[OOM_DUMP_BUFFER_SIZE];

// Maximum number of tasks we can track during OOM dump
constexpr size_t MAX_OOM_TRACKED_TASKS = 128;

// Pre-allocated structure to store task memory info during OOM dump
struct TaskMemoryInfo {
    const char* name;
    uint64_t pid;
    uint64_t pageCount;          // Number of user-space pages mapped
    uint64_t pageTableCount;     // Number of page table pages
    paging::PageTable* pagemap;  // Pointer to the pagemap
    bool isActive;               // Is this task in an active/wait queue?
    bool hasExited;              // Has this task exited?
    // Memory region breakdown
    uint64_t codePages;   // Code/ELF pages
    uint64_t heapPages;   // Heap pages
    uint64_t mmapPages;   // mmap region (mlibc arenas)
    uint64_t stackPages;  // Stack pages
    uint64_t rwPages;     // Read-write pages
    uint64_t rxPages;     // Read-execute pages
};
__attribute__((section(".bss"))) TaskMemoryInfo oomTaskInfo[MAX_OOM_TRACKED_TASKS];
__attribute__((section(".bss"))) size_t oomTaskCount;

// Pre-allocated array to track all known pagemaps for orphan detection
constexpr size_t MAX_OOM_TRACKED_PAGEMAPS = 256;
__attribute__((section(".bss"))) paging::PageTable* oomKnownPagemaps[MAX_OOM_TRACKED_PAGEMAPS];
__attribute__((section(".bss"))) size_t oomKnownPagemapCount;

// Memory region tracking for per-process analysis
// Categorize memory by address ranges
struct MemoryRegionStats {
    uint64_t codePages;   // Low addresses (typically 0x400000 - 0x1000000) - ELF code/data
    uint64_t heapPages;   // Heap region (brk area, typically after code)
    uint64_t mmapPages;   // mmap region (0x100000000000+) - where mlibc arenas live
    uint64_t stackPages;  // Stack region (high addresses near 0x7FFFFFFFFFFF)
    uint64_t otherPages;  // Other mapped pages
    uint64_t totalPages;
    uint64_t rwPages;  // Read-write pages (likely heap/data)
    uint64_t rxPages;  // Read-execute pages (likely code)
    uint64_t roPages;  // Read-only pages
};

// Address range constants for categorization
constexpr uint64_t CODE_REGION_START = 0x400000ULL;
constexpr uint64_t CODE_REGION_END = 0x10000000ULL;  // 256MB for code/data
constexpr uint64_t HEAP_REGION_START = 0x10000000ULL;
constexpr uint64_t HEAP_REGION_END = 0x100000000000ULL;    // Up to mmap region
constexpr uint64_t MMAP_REGION_START = 0x100000000000ULL;  // mlibc mmap base (changed from 0x700000000000)
constexpr uint64_t MMAP_REGION_END = 0x700000000000ULL;    // Up to ELF debug info region
constexpr uint64_t STACK_REGION_START = 0x7F0000000000ULL;
constexpr uint64_t STACK_REGION_END = 0x800000000000ULL;  // End of canonical user space

// ============================================================================
// NUMBER CONVERSION HELPERS (no allocations)
// ============================================================================

// Helper to convert uint64_t to hex string without allocations
auto u64ToHexNoAlloc(uint64_t val, char* buf, size_t bufSize) -> char* {
    constexpr size_t HEX_CHARS_MAX = 17;  // 16 hex chars + null
    if (bufSize < HEX_CHARS_MAX) {
        buf[0] = '\0';
        return buf;
    }

    char* ptr = buf + bufSize - 1;
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
        uint8_t digit = val & HEX_DIGIT_MASK;
        *ptr = static_cast<char>((digit < DECIMAL_BASE) ? ('0' + digit) : ('a' + digit - DECIMAL_BASE));
        val >>= 4;
    }

    return ptr;
}

// Helper to convert uint64_t to decimal string without allocations
auto u64ToDecNoAlloc(uint64_t val, char* buf, size_t bufSize) -> char* {
    constexpr size_t DEC_CHARS_MAX = 21;  // 20 decimal chars + null for max uint64
    if (bufSize < DEC_CHARS_MAX) {
        buf[0] = '\0';
        return buf;
    }

    char* ptr = buf + bufSize - 1;
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
    uint64_t userPages;
    uint64_t pageTablePages;
    bool valid;  // Whether we could safely walk the page table
};

// Page table constants
constexpr size_t PAGE_TABLE_ENTRIES = 512;
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
uint64_t cachedHHDMOffset = 0;

// Helper to safely check if a virtual address is in HHDM range
auto isInHHDMRange(uint64_t addr) -> bool {
    if (cachedHHDMOffset == 0) {
        cachedHHDMOffset = addr::getHHDMOffset();
    }
    // HHDM typically covers up to 256TB of physical memory
    constexpr uint64_t MAX_HHDM_SIZE = 0x100000000000ULL;  // 1TB reasonable limit
    return addr >= cachedHHDMOffset && addr < (cachedHHDMOffset + MAX_HHDM_SIZE);
}

// Helper to check if a physical address falls within one of our known memory zones
// This is the SAFEST check - if it passes, we know the memory is accessible
auto isPhysAddrInZone(uint64_t physAddr) -> bool {
    for (paging::PageZone* zone = getZones(); zone != nullptr; zone = zone->next) {
        // Zone stores virtual addresses (HHDM mapped), convert back to physical
        uint64_t zonePhysStart = zone->start - cachedHHDMOffset;
        uint64_t zonePhysEnd = zonePhysStart + zone->len;
        if (physAddr >= zonePhysStart && physAddr < zonePhysEnd) {
            return true;
        }
    }
    return false;
}

// Convert physical address to virtual address in HHDM range
// Returns 0 if address cannot be safely accessed
auto physToVirtSafe(uint64_t physAddr) -> uint64_t {
    if (cachedHHDMOffset == 0) {
        cachedHHDMOffset = addr::getHHDMOffset();
    }

    // Basic sanity checks
    if (physAddr == 0) {
        return 0;
    }

    // Must be page aligned for page table entries
    if ((physAddr & (paging::PAGE_SIZE - 1)) != 0) {
        return 0;
    }

    // Check against reasonable physical memory size (16TB max)
    constexpr uint64_t MAX_REASONABLE_PHYS = 0x100000000000ULL;
    if (physAddr >= MAX_REASONABLE_PHYS) {
        return 0;
    }

    // The safest check: verify this physical address is in a known zone
    // This guarantees the HHDM mapping exists and is valid
    if (!isPhysAddrInZone(physAddr)) {
        return 0;  // Not in any valid zone - don't access
    }

    return cachedHHDMOffset + physAddr;
}

// Get physical address from page table entry
auto getFrameAddr(const paging::PageTableEntry& entry) -> uint64_t { return static_cast<uint64_t>(entry.frame) << FRAME_SHIFT; }

// Safely walk page tables and count mapped pages
// Uses multiple layers of validation to avoid page faults
auto countMappedPagesNoAlloc(paging::PageTable* pml4) -> PageCountResult {
    PageCountResult result = {0, 0, false};

    // Initialize HHDM offset cache if needed
    if (cachedHHDMOffset == 0) {
        cachedHHDMOffset = addr::getHHDMOffset();
        if (cachedHHDMOffset == 0) {
            return result;  // HHDM not initialized
        }
    }

    if (pml4 == nullptr) {
        return result;
    }

    // Validate pml4 pointer itself - should be in HHDM range
    uint64_t pml4Addr = reinterpret_cast<uint64_t>(pml4);
    if (!isInHHDMRange(pml4Addr)) {
        return result;  // Invalid pagemap pointer
    }

    // Mark as valid since we can at least access the PML4
    result.valid = true;

    // Only process user-space entries (first 256 entries of PML4)
    // Entries 256-511 are kernel space
    for (size_t i4 = 0; i4 < USER_SPACE_PML4_ENTRIES; i4++) {
        if (pml4->entries[i4].present == 0) {
            continue;
        }

        // Get physical address of PML3 table
        uint64_t pml3PhysAddr = getFrameAddr(pml4->entries[i4]);
        uint64_t pml3VirtAddr = physToVirtSafe(pml3PhysAddr);
        if (pml3VirtAddr == 0) {
            // Can't safely access this PML3 - count the PML4 entry but skip
            result.pageTablePages++;
            continue;
        }

        paging::PageTable* pml3 = reinterpret_cast<paging::PageTable*>(pml3VirtAddr);
        result.pageTablePages++;  // Count the PML3 page

        for (size_t i3 = 0; i3 < PAGE_TABLE_ENTRIES; i3++) {
            if (pml3->entries[i3].present == 0) {
                continue;
            }

            // Check for 1GB huge page
            if (pml3->entries[i3].pagesize != 0) {
                // 1GB page - counts as 262144 4KB pages
                result.userPages += PAGES_PER_1GB_HUGEPAGE;
                continue;
            }

            // Get physical address of PML2 table
            uint64_t pml2PhysAddr = getFrameAddr(pml3->entries[i3]);
            uint64_t pml2VirtAddr = physToVirtSafe(pml2PhysAddr);
            if (pml2VirtAddr == 0) {
                result.pageTablePages++;
                continue;
            }

            paging::PageTable* pml2 = reinterpret_cast<paging::PageTable*>(pml2VirtAddr);
            result.pageTablePages++;  // Count the PML2 page

            for (size_t i2 = 0; i2 < PAGE_TABLE_ENTRIES; i2++) {
                if (pml2->entries[i2].present == 0) {
                    continue;
                }

                // Check for 2MB huge page
                if (pml2->entries[i2].pagesize != 0) {
                    // 2MB page - counts as 512 4KB pages
                    result.userPages += PAGES_PER_2MB_HUGEPAGE;
                    continue;
                }

                // Get physical address of PML1 table
                uint64_t pml1PhysAddr = getFrameAddr(pml2->entries[i2]);
                uint64_t pml1VirtAddr = physToVirtSafe(pml1PhysAddr);
                if (pml1VirtAddr == 0) {
                    result.pageTablePages++;
                    continue;
                }

                paging::PageTable* pml1 = reinterpret_cast<paging::PageTable*>(pml1VirtAddr);
                result.pageTablePages++;  // Count the PML1 page

                // Count present entries in PML1 (actual 4KB pages)
                for (size_t i1 = 0; i1 < PAGE_TABLE_ENTRIES; i1++) {
                    if (pml1->entries[i1].present != 0) {
                        result.userPages++;
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
void analyzeMemoryRegions(paging::PageTable* pml4, MemoryRegionStats& stats) {
    // Zero out stats
    stats = {};

    if (cachedHHDMOffset == 0) {
        cachedHHDMOffset = addr::getHHDMOffset();
        if (cachedHHDMOffset == 0) {
            return;
        }
    }

    if (pml4 == nullptr) {
        return;
    }

    uint64_t pml4Addr = reinterpret_cast<uint64_t>(pml4);
    if (!isInHHDMRange(pml4Addr)) {
        return;
    }

    // Walk all user-space PML4 entries
    for (size_t i4 = 0; i4 < USER_SPACE_PML4_ENTRIES; i4++) {
        if (pml4->entries[i4].present == 0) {
            continue;
        }

        uint64_t pml3PhysAddr = getFrameAddr(pml4->entries[i4]);
        uint64_t pml3VirtAddr = physToVirtSafe(pml3PhysAddr);
        if (pml3VirtAddr == 0) {
            continue;
        }

        paging::PageTable* pml3 = reinterpret_cast<paging::PageTable*>(pml3VirtAddr);

        for (size_t i3 = 0; i3 < PAGE_TABLE_ENTRIES; i3++) {
            if (pml3->entries[i3].present == 0) {
                continue;
            }

            // Check for 1GB huge page
            if (pml3->entries[i3].pagesize != 0) {
                // Calculate virtual address for this 1GB region
                uint64_t vaddr = (i4 << PML4_SHIFT) | (i3 << PML3_SHIFT);
                uint64_t pageCount = PAGES_PER_1GB_HUGEPAGE;

                // Categorize by address
                if (vaddr >= MMAP_REGION_START && vaddr < MMAP_REGION_END) {
                    stats.mmapPages += pageCount;
                } else if (vaddr >= STACK_REGION_START && vaddr < STACK_REGION_END) {
                    stats.stackPages += pageCount;
                } else if (vaddr >= CODE_REGION_START && vaddr < CODE_REGION_END) {
                    stats.codePages += pageCount;
                } else if (vaddr >= HEAP_REGION_START && vaddr < HEAP_REGION_END) {
                    stats.heapPages += pageCount;
                } else {
                    stats.otherPages += pageCount;
                }
                stats.totalPages += pageCount;

                // Categorize by permissions
                if (pml3->entries[i3].writable != 0) {
                    stats.rwPages += pageCount;
                } else if (pml3->entries[i3].noExecute == 0) {
                    stats.rxPages += pageCount;
                } else {
                    stats.roPages += pageCount;
                }
                continue;
            }

            uint64_t pml2PhysAddr = getFrameAddr(pml3->entries[i3]);
            uint64_t pml2VirtAddr = physToVirtSafe(pml2PhysAddr);
            if (pml2VirtAddr == 0) {
                continue;
            }

            paging::PageTable* pml2 = reinterpret_cast<paging::PageTable*>(pml2VirtAddr);

            for (size_t i2 = 0; i2 < PAGE_TABLE_ENTRIES; i2++) {
                if (pml2->entries[i2].present == 0) {
                    continue;
                }

                // Check for 2MB huge page
                if (pml2->entries[i2].pagesize != 0) {
                    uint64_t vaddr = (i4 << PML4_SHIFT) | (i3 << PML3_SHIFT) | (i2 << PML2_SHIFT);
                    uint64_t pageCount = PAGES_PER_2MB_HUGEPAGE;

                    if (vaddr >= MMAP_REGION_START && vaddr < MMAP_REGION_END) {
                        stats.mmapPages += pageCount;
                    } else if (vaddr >= STACK_REGION_START && vaddr < STACK_REGION_END) {
                        stats.stackPages += pageCount;
                    } else if (vaddr >= CODE_REGION_START && vaddr < CODE_REGION_END) {
                        stats.codePages += pageCount;
                    } else if (vaddr >= HEAP_REGION_START && vaddr < HEAP_REGION_END) {
                        stats.heapPages += pageCount;
                    } else {
                        stats.otherPages += pageCount;
                    }
                    stats.totalPages += pageCount;

                    if (pml2->entries[i2].writable != 0) {
                        stats.rwPages += pageCount;
                    } else if (pml2->entries[i2].noExecute == 0) {
                        stats.rxPages += pageCount;
                    } else {
                        stats.roPages += pageCount;
                    }
                    continue;
                }

                uint64_t pml1PhysAddr = getFrameAddr(pml2->entries[i2]);
                uint64_t pml1VirtAddr = physToVirtSafe(pml1PhysAddr);
                if (pml1VirtAddr == 0) {
                    continue;
                }

                paging::PageTable* pml1 = reinterpret_cast<paging::PageTable*>(pml1VirtAddr);

                for (size_t i1 = 0; i1 < PAGE_TABLE_ENTRIES; i1++) {
                    if (pml1->entries[i1].present == 0) {
                        continue;
                    }

                    // Calculate full virtual address
                    uint64_t vaddr = (i4 << PML4_SHIFT) | (i3 << PML3_SHIFT) | (i2 << PML2_SHIFT) | (i1 << PML1_SHIFT);

                    // Categorize by address range
                    if (vaddr >= MMAP_REGION_START && vaddr < MMAP_REGION_END) {
                        stats.mmapPages++;
                    } else if (vaddr >= STACK_REGION_START && vaddr < STACK_REGION_END) {
                        stats.stackPages++;
                    } else if (vaddr >= CODE_REGION_START && vaddr < CODE_REGION_END) {
                        stats.codePages++;
                    } else if (vaddr >= HEAP_REGION_START && vaddr < HEAP_REGION_END) {
                        stats.heapPages++;
                    } else {
                        stats.otherPages++;
                    }
                    stats.totalPages++;

                    // Categorize by permissions
                    if (pml1->entries[i1].writable != 0) {
                        stats.rwPages++;
                    } else if (pml1->entries[i1].noExecute == 0) {
                        stats.rxPages++;
                    } else {
                        stats.roPages++;
                    }
                }
            }
        }
    }
}

// Check if a pagemap is in our known list
auto isPagemapKnown(paging::PageTable* pagemap) -> bool {
    for (size_t i = 0; i < oomKnownPagemapCount; i++) {
        if (oomKnownPagemaps[i] == pagemap) {
            return true;
        }
    }
    return false;
}

// Add a pagemap to our known list
void addKnownPagemap(paging::PageTable* pagemap) {
    if (pagemap == nullptr || oomKnownPagemapCount >= MAX_OOM_TRACKED_PAGEMAPS) {
        return;
    }
    if (!isPagemapKnown(pagemap)) {
        oomKnownPagemaps[oomKnownPagemapCount++] = pagemap;
    }
}

// ============================================================================
// TASK COLLECTION (no allocations)
// ============================================================================

// Collect info about a single task - VERY DEFENSIVE
// We can't trust any pointers from the task structure as they may be corrupted
void collectTaskInfo(sched::task::Task* task, bool isActive) {
    if (task == nullptr || oomTaskCount >= MAX_OOM_TRACKED_TASKS) {
        return;
    }

    // First, validate that the task pointer itself is in kernel space
    uint64_t taskAddr = reinterpret_cast<uint64_t>(task);
    if (!isInHHDMRange(taskAddr)) {
        // Task pointer is not in valid kernel memory range
        return;
    }

    // Now try to read basic task info - be very careful
    // The task structure should be in kernel heap which is in HHDM range

    // Store info BEFORE trying to access pagemap
    TaskMemoryInfo& info = oomTaskInfo[oomTaskCount];
    info.pid = task->pid;
    info.name = task->name;  // Just store the pointer, don't dereference
    info.isActive = isActive;
    info.hasExited = task->hasExited;

    // Validate pagemap pointer
    paging::PageTable* pagemap = task->pagemap;
    if (pagemap == nullptr) {
        // No pagemap - still record the task but with no memory info
        info.pagemap = nullptr;
        info.pageCount = 0;
        info.pageTableCount = 0;
        oomTaskCount++;
        return;
    }

    // Check pagemap is in valid HHDM range
    uint64_t pagemapAddr = reinterpret_cast<uint64_t>(pagemap);
    if (!isInHHDMRange(pagemapAddr)) {
        // Invalid pagemap pointer - record task but skip memory counting
        info.pagemap = pagemap;  // Store the bad address for debugging
        info.pageCount = 0;
        info.pageTableCount = 0;
        oomTaskCount++;
        return;
    }

    // Check if we already have this task (by PID, not pagemap - pagemap might be shared)
    for (size_t i = 0; i < oomTaskCount; i++) {
        if (oomTaskInfo[i].pid == task->pid) {
            return;  // Already tracked
        }
    }

    info.pagemap = pagemap;

    // Count pages for this task - this should be safe now since pagemap is validated
    PageCountResult counts = countMappedPagesNoAlloc(pagemap);
    info.pageCount = counts.userPages;
    info.pageTableCount = counts.pageTablePages;

    // Analyze memory regions to get breakdown by address range
    // This shows where mlibc arenas (mmap region) and other memory is allocated
    MemoryRegionStats regionStats = {};
    analyzeMemoryRegions(pagemap, regionStats);
    info.codePages = regionStats.codePages;
    info.heapPages = regionStats.heapPages;
    info.mmapPages = regionStats.mmapPages;
    info.stackPages = regionStats.stackPages;
    info.rwPages = regionStats.rwPages;
    info.rxPages = regionStats.rxPages;

    oomTaskCount++;

    // Track this pagemap
    addKnownPagemap(pagemap);
}

}  // namespace

// Dump page allocation status when OOM - uses NO dynamic allocations
// This function uses only pre-allocated static buffers and serial output
void dumpPageAllocationsOOM() {
    asm volatile("cli");  // Disable interrupts to avoid concurrency issues during OOM dump

    // Atomically try to claim the OOM dump - only one CPU can proceed
    // Other CPUs that hit OOM will just halt immediately
    uint64_t expected = 0;
    if (!__atomic_compare_exchange_n(&oomDumpInProgress, &expected, 1, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        // Another CPU is already doing the OOM dump - just halt
        hcf();
    }

    // Halt all other CPUs immediately so they won't modify global state
    // while we perform a defensive OOM analysis on this core.
    ker::mod::smt::haltOtherCores();

    io::serial::write("\n");
    io::serial::write("╔══════════════════════════════════════════════════════════════════════╗\n");
    io::serial::write("║                    OOM PAGE ALLOCATION DUMP                          ║\n");
    io::serial::write("╚══════════════════════════════════════════════════════════════════════╝\n\n");

    // We'll print allocator stats later after the MEMORY SUMMARY so we can
    // use the computed totals there to refine the unaccounted memory estimate.
    // For now just call the raw dumps so they appear early in the log for visibility.
    mini_dump_stats();
    ker::mod::mm::dyn::kmalloc::dumpTrackedAllocations();
    dumpAllocStats();  // Dump page allocation counters

    // Reset tracking arrays
    oomTaskCount = 0;
    oomKnownPagemapCount = 0;
    memset(oomTaskInfo, 0, sizeof(oomTaskInfo));
    memset(static_cast<void*>(oomKnownPagemaps), 0, sizeof(oomKnownPagemaps));

    // ========================================================================
    // SECTION 1: Physical Memory Zones
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ PHYSICAL MEMORY ZONES                                               │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    paging::PageZone* zones = getZones();
    if (zones == nullptr) {
        io::serial::write("ERROR: No memory zones initialized!\n");
        return;
    }

    size_t zoneCount = 0;
    uint64_t totalMemory = 0;

    for (paging::PageZone* zone = zones; zone != nullptr; zone = zone->next) {
        zoneCount++;

        io::serial::write("Zone ");
        io::serial::write(u64ToDecNoAlloc(zone->zoneNum, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(": ");
        if (zone->name != nullptr) {
            io::serial::write(zone->name);
        }
        io::serial::write("\n");

        io::serial::write("  Start: 0x");
        io::serial::write(u64ToHexNoAlloc(zone->start, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write("\n");

        io::serial::write("  Length: ");
        io::serial::write(u64ToDecNoAlloc(zone->len, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" bytes (");
        constexpr uint64_t BYTES_PER_MB_ZONE = 1024ULL * 1024ULL;
        io::serial::write(u64ToDecNoAlloc(zone->len / BYTES_PER_MB_ZONE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" MB)\n");

        io::serial::write("  Page count: ");
        io::serial::write(u64ToDecNoAlloc(zone->pageCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write("\n");

        totalMemory += zone->len;

        // Report free/usable page counts from the allocator (O(1), no tree walk).
        if (zone->allocator != nullptr) {
            io::serial::write("  Free pages: ");
            io::serial::write(u64ToDecNoAlloc(zone->allocator->getFreePages(), oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" / ");
            io::serial::write(u64ToDecNoAlloc(zone->allocator->getUsablePages(), oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write("\n");
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
    uint64_t core_count = smt::getCoreCount();
    uint64_t total_task_pages = 0;
    uint64_t total_page_table_pages = 0;
    uint64_t exited_task_pages = 0;
    uint64_t total_tasks_found = 0;

    io::serial::write("Scanning ");
    io::serial::write(u64ToDecNoAlloc(core_count, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" CPU(s) for tasks...\n\n");

    // Note: We access scheduler internals directly here since we're in OOM state
    // This is safe because we're just reading, not modifying
    for (uint64_t cpuNo = 0; cpuNo < core_count; cpuNo++) {
        io::serial::write("CPU ");
        io::serial::write(u64ToDecNoAlloc(cpuNo, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(":\n");
    }

    // Try to find tasks by scanning known PID range
    // Process in batches of MAX_OOM_TRACKED_TASKS to handle more tasks than our buffer
    io::serial::write("\nScanning for active processes...\n");
    constexpr uint64_t MAX_PID_SCAN = 4096;  // Scan more PIDs
    constexpr uint64_t BYTES_PER_PAGE = 4096;
    constexpr uint64_t BYTES_PER_KB = 1024;

    uint64_t currentPid = 1;
    uint64_t batchNumber = 0;

    while (currentPid <= MAX_PID_SCAN) {
        // Reset batch tracking
        oomTaskCount = 0;

        // Collect up to MAX_OOM_TRACKED_TASKS tasks in this batch
        while (currentPid <= MAX_PID_SCAN && oomTaskCount < MAX_OOM_TRACKED_TASKS) {
            sched::task::Task* task = sched::find_task_by_pid(currentPid);
            if (task != nullptr) {
                collectTaskInfo(task, !task->hasExited);
            }
            currentPid++;
        }

        // If no tasks found in this batch, we're done
        if (oomTaskCount == 0) {
            break;
        }

        total_tasks_found += oomTaskCount;
        batchNumber++;

        // Print batch header
        io::serial::write("\n--- Batch ");
        io::serial::write(u64ToDecNoAlloc(batchNumber, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" (");
        io::serial::write(u64ToDecNoAlloc(oomTaskCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" tasks) ---\n\n");

        // Print task information for this batch
        for (size_t i = 0; i < oomTaskCount; i++) {
            TaskMemoryInfo& info = oomTaskInfo[i];

            io::serial::write("  PID ");
            io::serial::write(u64ToDecNoAlloc(info.pid, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(": ");
            if (info.name != nullptr && isInHHDMRange(reinterpret_cast<uint64_t>(info.name))) {
                io::serial::write(info.name);
            } else {
                io::serial::write("<name inaccessible>");
            }

            // Status indicator
            if (info.hasExited) {
                io::serial::write(" [EXITED/ZOMBIE]");
                exited_task_pages += info.pageCount;
            } else if (info.isActive) {
                io::serial::write(" [ACTIVE]");
            } else {
                io::serial::write(" [WAITING]");
            }
            io::serial::write("\n");

            io::serial::write("    Pagemap: 0x");
            io::serial::write(u64ToHexNoAlloc(reinterpret_cast<uint64_t>(info.pagemap), oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));

            // Check if pagemap is valid
            if (info.pagemap == nullptr) {
                io::serial::write(" (NULL)\n");
            } else if (!isInHHDMRange(reinterpret_cast<uint64_t>(info.pagemap))) {
                io::serial::write(" (INVALID - not in HHDM range)\n");
            } else {
                io::serial::write(" (valid)\n");
            }

            // Show detailed memory usage
            io::serial::write("    User pages: ");
            io::serial::write(u64ToDecNoAlloc(info.pageCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" (");
            io::serial::write(u64ToDecNoAlloc((info.pageCount * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB)\n");

            io::serial::write("    Page table pages: ");
            io::serial::write(u64ToDecNoAlloc(info.pageTableCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" (");
            io::serial::write(u64ToDecNoAlloc((info.pageTableCount * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB)\n");

            // Memory region breakdown - shows mlibc arena usage
            io::serial::write("    Memory Regions:\n");
            io::serial::write("      Code/ELF:     ");
            io::serial::write(u64ToDecNoAlloc(info.codePages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages (");
            io::serial::write(u64ToDecNoAlloc((info.codePages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB)\n");

            io::serial::write("      Heap:         ");
            io::serial::write(u64ToDecNoAlloc(info.heapPages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages (");
            io::serial::write(u64ToDecNoAlloc((info.heapPages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB)\n");

            io::serial::write("      mmap (mlibc): ");
            io::serial::write(u64ToDecNoAlloc(info.mmapPages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages (");
            io::serial::write(u64ToDecNoAlloc((info.mmapPages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB) <- mlibc slab arenas\n");

            io::serial::write("      Stack:        ");
            io::serial::write(u64ToDecNoAlloc(info.stackPages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages (");
            io::serial::write(u64ToDecNoAlloc((info.stackPages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" KB)\n");

            io::serial::write("    Permissions:\n");
            io::serial::write("      RW (data/heap): ");
            io::serial::write(u64ToDecNoAlloc(info.rwPages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages\n");

            io::serial::write("      RX (code):      ");
            io::serial::write(u64ToDecNoAlloc(info.rxPages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
            io::serial::write(" pages\n\n");

            total_task_pages += info.pageCount;
            total_page_table_pages += info.pageTableCount;
        }
    }

    // Print total tasks found
    io::serial::write("\nTotal tasks found: ");
    io::serial::write(u64ToDecNoAlloc(total_tasks_found, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" (scanned PIDs 1-");
    io::serial::write(u64ToDecNoAlloc(MAX_PID_SCAN, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(")\n");

    // ========================================================================
    // SECTION 2.5: Kernel Dynamic Buffers
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ KERNEL DYNAMIC BUFFERS                                              │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    io::serial::write("\nScheduler Run Queues (per-CPU, EEVDF — zero dynamic allocations):\n");

    uint64_t totalRunnableCount = 0;
    uint64_t totalDeadCount = 0;
    uint64_t totalWaitCount = 0;

    for (uint64_t cpuNo = 0; cpuNo < core_count; cpuNo++) {
        sched::RunQueueStats rqStats = sched::get_run_queue_stats(cpuNo);

        io::serial::write("  CPU ");
        io::serial::write(u64ToDecNoAlloc(cpuNo, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(":\n");

        io::serial::write("    runnableHeap:  ");
        io::serial::write(u64ToDecNoAlloc(rqStats.active_task_count, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" tasks\n");

        io::serial::write("    deadList:      ");
        io::serial::write(u64ToDecNoAlloc(rqStats.expired_task_count, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" tasks\n");

        io::serial::write("    waitList:      ");
        io::serial::write(u64ToDecNoAlloc(rqStats.wait_queue_count, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" tasks\n");

        totalRunnableCount += rqStats.active_task_count;
        totalDeadCount += rqStats.expired_task_count;
        totalWaitCount += rqStats.wait_queue_count;
    }

    io::serial::write("\n  Totals across all CPUs:\n");
    io::serial::write("    Total runnable (heap): ");
    io::serial::write(u64ToDecNoAlloc(totalRunnableCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write("\n");

    io::serial::write("    Total dead (GC):       ");
    io::serial::write(u64ToDecNoAlloc(totalDeadCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write("\n");

    // Print dead tasks with their refcounts (useful for debugging leaks).
    io::serial::write("\nDead tasks (PID : refcount) per CPU:\n");
    for (uint64_t cpuNo = 0; cpuNo < core_count; cpuNo++) {
        constexpr size_t BLOCK = 128;
        uint64_t pids[BLOCK];
        uint32_t refs[BLOCK];

        io::serial::write("  CPU ");
        io::serial::write(u64ToDecNoAlloc(cpuNo, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(": ");

        size_t startIndex = 0;
        bool printedAny = false;
        while (true) {
            size_t n = ker::mod::sched::get_expired_task_refcounts(cpuNo, pids, refs, BLOCK, startIndex);
            if (n == 0) {
                if (!printedAny) {
                    io::serial::write("(none)");
                }
                break;
            }

            // Print this block
            for (size_t i = 0; i < n; ++i) {
                printedAny = true;
                io::serial::write("PID=");
                io::serial::write(u64ToDecNoAlloc(pids[i], oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
                io::serial::write(" ref=");
                io::serial::write(u64ToDecNoAlloc(refs[i], oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
                io::serial::write("  ");
            }

            // Advance start index; if fewer than BLOCK returned, we've reached the end
            startIndex += n;
            if (n < BLOCK) break;
        }
        io::serial::write("\n");
    }

    io::serial::write("    Total waitList nodes:     ");
    io::serial::write(u64ToDecNoAlloc(totalWaitCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write("\n");

    // Scheduler queues use zero dynamic allocations (array-backed heap + intrusive lists)
    uint64_t totalSchedListBytes = 0;
    io::serial::write("    Scheduler list memory: 0 bytes (zero-alloc EEVDF)\n");

    io::serial::write("\nThread Tracking:\n");
    uint64_t activeThreadCount = sched::threading::getActiveThreadCount();
    // std::list<Thread*> node: prev + next + data = 24 bytes each
    constexpr uint64_t STD_LIST_NODE_SIZE = 24;
    io::serial::write("  activeThreads list: ");
    io::serial::write(u64ToDecNoAlloc(activeThreadCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" nodes (");
    io::serial::write(u64ToDecNoAlloc(activeThreadCount * STD_LIST_NODE_SIZE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes)\n");

    // Estimate Task structure size - includes all fields
    constexpr uint64_t TASK_STRUCT_SIZE_ESTIMATE = 512;  // Conservative estimate
    constexpr uint64_t THREAD_STRUCT_SIZE = sizeof(sched::threading::Thread);

    io::serial::write("\nEstimated Kernel Object Memory:\n");

    uint64_t taskObjectsMemory = total_tasks_found * TASK_STRUCT_SIZE_ESTIMATE;
    io::serial::write("  Task objects (~");
    io::serial::write(u64ToDecNoAlloc(TASK_STRUCT_SIZE_ESTIMATE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes each): ");
    io::serial::write(u64ToDecNoAlloc(total_tasks_found, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" x ");
    io::serial::write(u64ToDecNoAlloc(TASK_STRUCT_SIZE_ESTIMATE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" = ");
    io::serial::write(u64ToDecNoAlloc(taskObjectsMemory / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB\n");

    uint64_t threadObjectsMemory = activeThreadCount * THREAD_STRUCT_SIZE;
    io::serial::write("  Thread objects (");
    io::serial::write(u64ToDecNoAlloc(THREAD_STRUCT_SIZE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes each): ");
    io::serial::write(u64ToDecNoAlloc(activeThreadCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" x ");
    io::serial::write(u64ToDecNoAlloc(THREAD_STRUCT_SIZE, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" = ");
    io::serial::write(u64ToDecNoAlloc(threadObjectsMemory, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes\n");

    uint64_t totalKernelDynamic = totalSchedListBytes + taskObjectsMemory + threadObjectsMemory;
    io::serial::write("\n  Total estimated kernel dynamic allocations: ");
    io::serial::write(u64ToDecNoAlloc(totalKernelDynamic / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
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
    io::serial::write(u64ToDecNoAlloc(zoneCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));

    io::serial::write("  Total memory: ");
    io::serial::write(u64ToDecNoAlloc(totalMemory / BYTES_PER_MB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" MB (");
    io::serial::write(u64ToDecNoAlloc(totalMemory, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes)\n");

    // Sum free pages across all zones
    uint64_t totalFreePages = 0;
    for (paging::PageZone* z = getZones(); z != nullptr; z = z->next) {
        if (z->allocator != nullptr) totalFreePages += z->allocator->getFreePages();
    }
    io::serial::write("  Free memory: ");
    io::serial::write(u64ToDecNoAlloc(totalFreePages * paging::PAGE_SIZE / BYTES_PER_MB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" MB (");
    io::serial::write(u64ToDecNoAlloc(totalFreePages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" pages)\n");
    io::serial::write("  Used memory: ");
    io::serial::write(
        u64ToDecNoAlloc((totalMemory - totalFreePages * paging::PAGE_SIZE) / BYTES_PER_MB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" MB\n\n");

    io::serial::write("Process Memory:\n");
    io::serial::write("  Total tasks tracked: ");
    io::serial::write(u64ToDecNoAlloc(total_tasks_found, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write("\n");

    io::serial::write("  User pages: ");
    io::serial::write(u64ToDecNoAlloc(total_task_pages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" (");
    io::serial::write(u64ToDecNoAlloc((total_task_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB)\n");

    io::serial::write("  Page table pages: ");
    io::serial::write(u64ToDecNoAlloc(total_page_table_pages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" (");
    io::serial::write(u64ToDecNoAlloc((total_page_table_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB)\n");

    uint64_t totalProcessMem = (total_task_pages + total_page_table_pages) * BYTES_PER_PAGE;
    io::serial::write("  Total process memory: ");
    io::serial::write(u64ToDecNoAlloc(totalProcessMem / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB\n\n");

    // ========================================================================
    // SECTION 4: Dead/Zombie Memory (potential leaks)
    // ========================================================================
    if (exited_task_pages > 0) {
        io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
        io::serial::write("│    ZOMBIE/DEAD MEMORY DETECTED                                     │\n");
        io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

        io::serial::write("Memory held by exited processes: ");
        io::serial::write(u64ToDecNoAlloc((exited_task_pages * BYTES_PER_PAGE) / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" KB (");
        io::serial::write(u64ToDecNoAlloc(exited_task_pages, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
        io::serial::write(" pages)\n");
        io::serial::write("This memory can be reclaimed by reaping zombie processes.\n\n");
    }

    // ========================================================================
    // SECTION 5: Unaccounted Memory Analysis
    // ========================================================================
    io::serial::write("┌─────────────────────────────────────────────────────────────────────┐\n");
    io::serial::write("│ MEMORY ACCOUNTING                                                   │\n");
    io::serial::write("└─────────────────────────────────────────────────────────────────────┘\n");

    io::serial::write("(Buddy tree walk skipped - unsafe during OOM condition)\n");
    io::serial::write("Total physical memory: ");
    io::serial::write(u64ToDecNoAlloc(totalMemory / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB\n");

    io::serial::write("Accounted process memory: ");
    io::serial::write(u64ToDecNoAlloc(totalProcessMem / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB\n");

    // Show allocator-level accounting
    uint64_t kmallocCount = 0, kmallocBytes = 0;
    ker::mod::mm::dyn::kmalloc::getTrackedAllocTotals(kmallocCount, kmallocBytes);
    uint64_t slabBytes = mini_get_total_slab_bytes();

    io::serial::write("\nAllocator accounting:\n");
    io::serial::write("  kmalloc tracked large allocations: ");
    io::serial::write(u64ToDecNoAlloc(kmallocCount, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" entries, ");
    io::serial::write(u64ToDecNoAlloc(kmallocBytes, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes (");
    io::serial::write(u64ToDecNoAlloc(kmallocBytes / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB)\n");
    io::serial::write("  total slab memory (mini): ");
    io::serial::write(u64ToDecNoAlloc(slabBytes, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" bytes (");
    io::serial::write(u64ToDecNoAlloc(slabBytes / BYTES_PER_KB, oomDumpBuffer, OOM_DUMP_BUFFER_SIZE));
    io::serial::write(" KB)\n");

    io::serial::write("\n");
    io::serial::write("╔══════════════════════════════════════════════════════════════════════╗\n");
    io::serial::write("║                       END OOM DUMP                                   ║\n");
    io::serial::write("╚══════════════════════════════════════════════════════════════════════╝\n\n");
    hcf();
}
}  // namespace ker::mod::mm::phys
