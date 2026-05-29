#include "threading.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/list.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"

namespace ker::mod::sched::threading {
namespace {
using log = ker::mod::dbg::logger<"thread">;

util::List<Thread*> active_threads;
sys::Spinlock active_threads_lock;
constexpr uint64_t STACK_INITIAL_BACKING_BYTES = 256ULL * 1024ULL;
constexpr uint64_t STACK_GROW_NORMAL_BYTES = 256ULL * 1024ULL;
constexpr uint64_t STACK_GROW_MEDIUM_BYTES = 512ULL * 1024ULL;
constexpr uint64_t STACK_GROW_LARGE_BYTES = 1024ULL * 1024ULL;
constexpr uint64_t STACK_GROW_NEAR_BYTES = 64ULL * 1024ULL;
constexpr uint64_t STACK_GROW_RSP_FAR_BYTES = 512ULL * 1024ULL;

auto min_u64(uint64_t a, uint64_t b) -> uint64_t { return a < b ? a : b; }

auto max_u64(uint64_t a, uint64_t b) -> uint64_t { return a > b ? a : b; }

auto buddy_backing_size(uint64_t size) -> uint64_t {
    if (size == 0) {
        return 0;
    }

    uint64_t const PAGES = (size + mm::paging::PAGE_SIZE - 1) / mm::paging::PAGE_SIZE;
    uint64_t rounded_pages = 1;
    while (rounded_pages < PAGES && rounded_pages < (uint64_t{1} << mm::PageAllocator::MAX_ORDER)) {
        rounded_pages <<= 1;
    }
    return rounded_pages * mm::paging::PAGE_SIZE;
}

auto stack_top(const Thread* thread) -> uint64_t {
    if (thread == nullptr || thread->stack_size == 0) {
        return 0;
    }
    uint64_t const TOP = thread->stack_base_virt + thread->stack_size;
    return TOP < thread->stack_base_virt ? 0 : TOP;
}

void free_mapped_user_range(mm::paging::PageTable* page_table, uint64_t start, uint64_t end) {
    if (page_table == nullptr || start >= end) {
        return;
    }
    uint64_t const LOW = page_align_down(start);
    uint64_t const HIGH = page_align_up(end);
    for (uint64_t addr = LOW; addr < HIGH; addr += mm::paging::PAGE_SIZE) {
        if (mm::virt::translate(page_table, addr) == mm::virt::PADDR_INVALID) {
            continue;
        }
        mm::virt::unmap_page(page_table, addr);
    }
}
}  // namespace

void init_threading() {}

bool ensure_stack_backing(Thread* thread, mm::paging::PageTable* page_table, uint64_t start, uint64_t end) {
    if (thread == nullptr || page_table == nullptr || start > end) {
        return false;
    }
    if (start == end) {
        return true;
    }

    uint64_t const BASE = thread->stack_base_virt;
    uint64_t const TOP = stack_top(thread);
    if (BASE == 0 || TOP == 0 || start < BASE || end > TOP) {
        return false;
    }

    uint64_t const LOW = max_u64(BASE, page_align_down(start));
    uint64_t const HIGH = min_u64(TOP, page_align_up(end));
    for (uint64_t addr = LOW; addr < HIGH; addr += mm::paging::PAGE_SIZE) {
        if (mm::virt::translate(page_table, addr) != mm::virt::PADDR_INVALID) {
            continue;
        }

        void* page = mm::phys::page_alloc(mm::paging::PAGE_SIZE, "thread_stack_lazy");
        if (page == nullptr) {
            log::error("ensure_stack_backing: OOM backing stack page vaddr=0x%llx range=[0x%llx,0x%llx)",
                       static_cast<unsigned long long>(addr), static_cast<unsigned long long>(LOW), static_cast<unsigned long long>(HIGH));
            return false;
        }

        auto const PHYS = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        mm::virt::map_page(page_table, addr, PHYS, mm::paging::page_types::USER);
    }

    if (thread->stack_lowest_backed == 0 || LOW < thread->stack_lowest_backed) {
        thread->stack_lowest_backed = LOW;
    }
    return true;
}

bool handle_lazy_stack_fault(Thread* thread, mm::paging::PageTable* page_table, uint64_t fault_addr, uint64_t rsp) {
    if (thread == nullptr || page_table == nullptr) {
        return false;
    }

    uint64_t const BASE = thread->stack_base_virt;
    uint64_t const TOP = stack_top(thread);
    if (BASE == 0 || TOP == 0 || fault_addr < BASE || fault_addr >= TOP) {
        return false;
    }

    uint64_t const FAULT_PAGE = page_align_down(fault_addr);
    uint64_t const RSP_PAGE = (rsp >= BASE && rsp < TOP) ? page_align_down(rsp) : FAULT_PAGE;
    uint64_t const LOWEST_BACKED = thread->stack_lowest_backed != 0 ? thread->stack_lowest_backed : TOP;
    uint64_t const GAP_FROM_HINT = LOWEST_BACKED > FAULT_PAGE ? LOWEST_BACKED - FAULT_PAGE : 0;
    uint64_t const RSP_DISTANCE = RSP_PAGE > FAULT_PAGE ? RSP_PAGE - FAULT_PAGE : FAULT_PAGE - RSP_PAGE;

    uint64_t batch = STACK_GROW_NORMAL_BYTES;
    if (GAP_FROM_HINT > STACK_GROW_LARGE_BYTES || RSP_DISTANCE > STACK_GROW_RSP_FAR_BYTES) {
        batch = STACK_GROW_LARGE_BYTES;
    } else if (GAP_FROM_HINT > STACK_GROW_NEAR_BYTES) {
        batch = STACK_GROW_MEDIUM_BYTES;
    }

    uint64_t const ABOVE = batch / 4;
    uint64_t const BELOW = batch - ABOVE;
    uint64_t low = FAULT_PAGE > BASE + BELOW ? FAULT_PAGE - BELOW : BASE;
    uint64_t high = min_u64(TOP, FAULT_PAGE + mm::paging::PAGE_SIZE + ABOVE);

    // If the fault is just below the hot stack slice, fill the short gap so
    // normal downward growth stays mostly fault-free.
    if (LOWEST_BACKED > high && LOWEST_BACKED - high <= STACK_GROW_LARGE_BYTES) {
        high = LOWEST_BACKED;
    }

    return ensure_stack_backing(thread, page_table, low, high);
}

Thread* create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table,
                      const ker::loader::elf::TlsModule& tls_info) {
    auto* thread = new Thread();
    if (thread == nullptr) {
        log::error("create_thread: failed to allocate Thread object");
        return nullptr;
    }
    thread->stack_size = stack_size;
    thread->tls_size = tls_size;

    // Use the actual TLS size from PT_TLS segment if available, otherwise use provided size
    uint64_t const ACTUAL_TLS_SIZE = (tls_info.tls_size > 0) ? tls_info.tls_size : tls_size;

    // Allocate memory for TLS + TCB + SafeStack
    // TCB structure is ~136 bytes + stack canary + padding
    uint64_t const TCB_SIZE = 256;          // Extra space for mlibc's Tcb structure
    uint64_t const SAFESTACK_SIZE = 65536;  // 64KB for SafeStack unsafe stack
    uint64_t const TOTAL_TLS_SIZE = ACTUAL_TLS_SIZE + TCB_SIZE + SAFESTACK_SIZE;
    uint64_t const ALIGNED_TOTAL_SIZE = page_align_up(TOTAL_TLS_SIZE);

    void* tls = mm::phys::page_alloc(ALIGNED_TOTAL_SIZE, "thread_tls");
    if (tls == nullptr) {
        log::error("create_thread: failed to allocate TLS memory (%llu bytes)", static_cast<unsigned long long>(ALIGNED_TOTAL_SIZE));
        delete thread;
        return nullptr;
    }

    // TLS is reclaimed one 4 KiB leaf at a time during destroy_user_space() /
    // COW teardown, so split the bulk allocation before exposing it through
    // page tables. The user stack is backed lazily with single-page leaves.
    bool const TLS_SPLIT = mm::phys::page_split_to_order0(tls);
    if (!TLS_SPLIT) {
        log::error("create_thread: failed to split TLS backing pages tls=%p tls_size=%llu", tls,
                   static_cast<unsigned long long>(ALIGNED_TOTAL_SIZE));
        mm::phys::page_free(tls);
        delete thread;
        return nullptr;
    }
    // page_alloc() returns buddy-sized backing; only the mapped TLS range is
    // later reclaimed through destroy_user_space(), so return the tail now.
    uint64_t const TLS_BACKING_SIZE = buddy_backing_size(ALIGNED_TOTAL_SIZE);
    for (uint64_t offset = ALIGNED_TOTAL_SIZE; offset < TLS_BACKING_SIZE; offset += mm::paging::PAGE_SIZE) {
        auto* tail_page = reinterpret_cast<uint8_t*>(tls) + offset;
        std::memset(tail_page, 0, mm::paging::PAGE_SIZE);
        mm::phys::page_free(tail_page);
    }

    // CRITICAL FIX: Use page-aligned size for virtual address calculation too
    uint64_t const TLS_VIRT_ADDR = 0x7FFF00000000ULL - ALIGNED_TOTAL_SIZE;
    uint64_t const STACK_VIRT_ADDR = TLS_VIRT_ADDR - stack_size;
    thread->magic = 0xDEADBEEF;

    // Map all pages for TLS
    for (uint64_t offset = 0; offset < ALIGNED_TOTAL_SIZE; offset += mm::paging::PAGE_SIZE) {
        auto const TLS_PHYS = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(tls) + offset));
        mm::virt::map_page(page_table, TLS_VIRT_ADDR + offset, TLS_PHYS, mm::paging::page_types::USER);
    }

    // TCB goes at the TOP of the TLS area (highest address)
    void* tcb = reinterpret_cast<uint8_t*>(tls) + ACTUAL_TLS_SIZE;
    uint64_t const TCB_VIRT_ADDR = TLS_VIRT_ADDR + ACTUAL_TLS_SIZE;

    // SafeStack area goes after the TCB
    void* safestack_area = reinterpret_cast<uint8_t*>(tls) + ACTUAL_TLS_SIZE + TCB_SIZE;
    uint64_t const SAFESTACK_VIRT_ADDR = TLS_VIRT_ADDR + ACTUAL_TLS_SIZE + TCB_SIZE;

    // Initialize the TLS area (clear it first)
    std::memset(tls, 0, ACTUAL_TLS_SIZE);

    // Initialize the TCB according to mlibc's Tcb structure
    // Zero out the entire TCB area
    std::memset(tcb, 0, TCB_SIZE);

    // Set up minimal TCB structure for mlibc
    // TCB layout for x86_64 (from mlibc/tcb.hpp):
    // +0x00: selfPointer (Tcb *) - points to TCB itself
    // +0x08: dtvSize
    // +0x10: dtvPointers
    // +0x18: tid
    // +0x1C: didExit
    // +0x20: padding (8 bytes on x86_64)
    // +0x28: stackCanary
    // +0x30: cancelBits

    auto* tcb_ptr = static_cast<uint64_t*>(tcb);
    tcb_ptr[0] = TCB_VIRT_ADDR;  // selfPointer points to TCB itself
    tcb_ptr[1] = 1;              // dtvSize
    tcb_ptr[2] = 0;              // dtvPointers (can be null for now)

    auto* tcb_i32 = static_cast<uint32_t*>(tcb);
    tcb_i32[6] = 0;  // tid (will be set later)
    tcb_i32[7] = 0;  // didExit

    // CRITICAL: Set the stack canary at offset 0x28 (40 decimal)
    tcb_ptr[5] = 0x3000000018ULL;  // stackCanary at fs:[0x28]

    tcb_i32[12] = 0;  // cancelBits at offset 0x30

    // SafeStack support: The TLS area needs to have space for __safestack_unsafe_stack_ptr
    // This is a thread-local variable that the compiler uses for SafeStack
    // According to the relocation, it should be at TLS offset -8 from the TCB

    // Set up TLS variables area RIGHT BEFORE the TCB (negative offsets from fs register)
    // Use physical address since the page table hasn't been switched yet

    // Initialize the entire TLS area with zeros first
    std::memset(tls, 0, ACTUAL_TLS_SIZE);

    // For mlibc TLS variables, we need to set up the proper layout:
    // The linker expects TLS variables at specific offsets from the TCB
    // Based on binary analysis, errno (__mlibc_errno) should be accessible at fs:[-176]

    // Set up key TLS variables:
    // 1. SafeStack pointer at the standard location (offset 0 in TLS segment)
    auto* safestack_ptr = static_cast<uint64_t*>(tls);  // At TLS offset 0
    uint64_t const SAFESTACK_TOP = SAFESTACK_VIRT_ADDR + SAFESTACK_SIZE;

    uint64_t const SAFESTACK_PTR_VALUE = SAFESTACK_TOP - 512;  // Leave 512 bytes safety margin from top
    *safestack_ptr = SAFESTACK_PTR_VALUE;                      // SafeStack grows downward

    auto* errno_ptr = reinterpret_cast<uint32_t*>(static_cast<uint8_t*>(tls) + 0xa0);  // TLS offset 160
    *errno_ptr = 0;                                                                    // Initialize errno to 0

    // Initialize the SafeStack area
    std::memset(safestack_area, 0, SAFESTACK_SIZE);

    // Save TLS mapping info into the Thread object for later initialization
    thread->tls_size = ALIGNED_TOTAL_SIZE;
    thread->tls_base_virt = TLS_VIRT_ADDR;
    thread->safestack_ptr_value = SAFESTACK_PTR_VALUE;

    // Stack grows downward, so thread->stack points near the TOP of the reserved stack.
    // The actual syscall scratch area lives in kernel memory on Task::context.
    uint64_t const SCRATCH_AREA_SIZE = sizeof(cpu::PerCpu);
    thread->stack = STACK_VIRT_ADDR + stack_size - SCRATCH_AREA_SIZE;  // Stack starts above scratch area
    thread->stack_base_virt = STACK_VIRT_ADDR;
    thread->stack_lowest_backed = STACK_VIRT_ADDR + stack_size;
    thread->fsbase = TCB_VIRT_ADDR;
    // User GS_BASE points at the base of the reserved stack range.
    thread->gsbase = STACK_VIRT_ADDR;

    // Store the physical (HHDM) pointers for cleanup
    thread->tls_phys_ptr = reinterpret_cast<uint64_t>(tls);
    thread->stack_phys_ptr = 0;

    uint64_t const STACK_TOP = STACK_VIRT_ADDR + stack_size;
    uint64_t const INITIAL_STACK_BYTES = min_u64(STACK_INITIAL_BACKING_BYTES, stack_size);
    bool const INITIAL_STACK_OK = ensure_stack_backing(thread, page_table, STACK_TOP - INITIAL_STACK_BYTES, STACK_TOP) &&
                                  ensure_stack_backing(thread, page_table, STACK_VIRT_ADDR, STACK_VIRT_ADDR + mm::paging::PAGE_SIZE);
    if (!INITIAL_STACK_OK) {
        log::error("create_thread: failed to lazily back initial stack windows stack=0x%llx size=%llu initial=%llu",
                   static_cast<unsigned long long>(STACK_VIRT_ADDR), static_cast<unsigned long long>(stack_size),
                   static_cast<unsigned long long>(INITIAL_STACK_BYTES));
        free_mapped_user_range(page_table, TLS_VIRT_ADDR, TLS_VIRT_ADDR + ALIGNED_TOTAL_SIZE);
        free_mapped_user_range(page_table, STACK_TOP - INITIAL_STACK_BYTES, STACK_TOP);
        free_mapped_user_range(page_table, STACK_VIRT_ADDR, STACK_VIRT_ADDR + mm::paging::PAGE_SIZE);
        thread->tls_phys_ptr = 0;
        delete thread;
        return nullptr;
    }
    thread->stack_lowest_backed = STACK_TOP - INITIAL_STACK_BYTES;

    active_threads_lock.lock();
    active_threads.push_back(thread);
    active_threads_lock.unlock();
    return thread;
}

void destroy_thread(Thread* thread) {
    if (thread == nullptr) {
        return;
    }

    active_threads_lock.lock();
    active_threads.remove(thread);
    active_threads_lock.unlock();

    // Free the actual physical allocations using the stored HHDM pointers
    if (thread->tls_phys_ptr != 0) {
        mm::phys::page_free(reinterpret_cast<void*>(thread->tls_phys_ptr));
    }
    if (thread->stack_phys_ptr != 0) {
        mm::phys::page_free(reinterpret_cast<void*>(thread->stack_phys_ptr));
    }

    delete thread;
}

auto get_active_thread_count() -> uint64_t {
    active_threads_lock.lock();
    uint64_t const COUNT = active_threads.size();
    active_threads_lock.unlock();
    return COUNT;
}
}  // namespace ker::mod::sched::threading
