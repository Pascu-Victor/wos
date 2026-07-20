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

auto write_mapped_user_bytes(mm::paging::PageTable* page_table, uint64_t vaddr, const void* source, size_t size) -> bool {
    const auto* source_bytes = static_cast<const uint8_t*>(source);
    while (size != 0) {
        uint64_t const PADDR = mm::virt::translate(page_table, vaddr);
        if (PADDR == mm::virt::PADDR_INVALID) {
            return false;
        }

        size_t const PAGE_REMAINING = mm::paging::PAGE_SIZE - (vaddr & (mm::paging::PAGE_SIZE - 1));
        size_t const CHUNK_SIZE = size < PAGE_REMAINING ? size : PAGE_REMAINING;
        std::memcpy(mm::addr::get_virt_pointer(PADDR), source_bytes, CHUNK_SIZE);
        source_bytes += CHUNK_SIZE;
        vaddr += CHUNK_SIZE;
        size -= CHUNK_SIZE;
    }
    return true;
}

template <typename T>
auto write_mapped_user_value(mm::paging::PageTable* page_table, uint64_t vaddr, const T& value) -> bool {
    return write_mapped_user_bytes(page_table, vaddr, &value, sizeof(value));
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

        void* page = mm::phys::page_alloc_with_reclaim_may_fail(mm::paging::PAGE_SIZE, "thread_stack_lazy");
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

Thread* create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table, uint64_t initial_tid,
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

    // Keep the user ABI contiguous while backing the mapping with independent
    // order-0 pages. Process creation only needs a contiguous virtual TLS
    // range; requiring one physical run makes fork/exec fail under harmless
    // buddy fragmentation.
    uint64_t const TLS_VIRT_ADDR = 0x7FFF00000000ULL - ALIGNED_TOTAL_SIZE;
    uint64_t const STACK_VIRT_ADDR = TLS_VIRT_ADDR - stack_size;
    thread->magic = 0xDEADBEEF;

    for (uint64_t offset = 0; offset < ALIGNED_TOTAL_SIZE; offset += mm::paging::PAGE_SIZE) {
        void* tls_page = mm::phys::page_alloc_with_reclaim_may_fail(mm::paging::PAGE_SIZE, "thread_tls_page");
        if (tls_page == nullptr) {
            log::error("create_thread: failed to allocate TLS page offset=%llu total=%llu", static_cast<unsigned long long>(offset),
                       static_cast<unsigned long long>(ALIGNED_TOTAL_SIZE));
            free_mapped_user_range(page_table, TLS_VIRT_ADDR, TLS_VIRT_ADDR + offset);
            delete thread;
            return nullptr;
        }
        std::memset(tls_page, 0, mm::paging::PAGE_SIZE);
        auto const TLS_PHYS = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(tls_page)));
        mm::virt::map_page(page_table, TLS_VIRT_ADDR + offset, TLS_PHYS, mm::paging::page_types::USER);
    }

    // TCB goes at the TOP of the TLS area (highest address)
    uint64_t const TCB_VIRT_ADDR = TLS_VIRT_ADDR + ACTUAL_TLS_SIZE;

    // SafeStack area goes after the TCB
    uint64_t const SAFESTACK_VIRT_ADDR = TLS_VIRT_ADDR + ACTUAL_TLS_SIZE + TCB_SIZE;

    // Initialize the TCB according to mlibc's Tcb structure
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

    uint64_t const SELF_POINTER = TCB_VIRT_ADDR;
    uint64_t const DTV_SIZE = 1;
    uint64_t const DTV_POINTERS = 0;
    auto const INITIAL_TID = static_cast<uint32_t>(initial_tid);
    uint32_t const DID_EXIT = 0;
    uint64_t const STACK_CANARY = 0x3000000018ULL;
    uint32_t const CANCEL_BITS = 0;
    bool const TCB_INITIALIZED = write_mapped_user_value(page_table, TCB_VIRT_ADDR, SELF_POINTER) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x08, DTV_SIZE) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x10, DTV_POINTERS) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x18, INITIAL_TID) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x1C, DID_EXIT) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x28, STACK_CANARY) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x30, CANCEL_BITS);
    if (!TCB_INITIALIZED) {
        log::error("create_thread: failed to initialize mapped TCB");
        free_mapped_user_range(page_table, TLS_VIRT_ADDR, TLS_VIRT_ADDR + ALIGNED_TOTAL_SIZE);
        delete thread;
        return nullptr;
    }

    // SafeStack support: The TLS area needs to have space for __safestack_unsafe_stack_ptr
    // This is a thread-local variable that the compiler uses for SafeStack
    // According to the relocation, it should be at TLS offset -8 from the TCB

    // For mlibc TLS variables, we need to set up the proper layout:
    // The linker expects TLS variables at specific offsets from the TCB
    // Based on binary analysis, errno (__mlibc_errno) should be accessible at fs:[-176]

    // Set up key TLS variables:
    // 1. SafeStack pointer at the standard location (offset 0 in TLS segment)
    uint64_t const SAFESTACK_TOP = SAFESTACK_VIRT_ADDR + SAFESTACK_SIZE;

    uint64_t const SAFESTACK_PTR_VALUE = SAFESTACK_TOP - 512;  // Leave 512 bytes safety margin from top
    if (!write_mapped_user_value(page_table, TLS_VIRT_ADDR, SAFESTACK_PTR_VALUE)) {
        log::error("create_thread: failed to initialize SafeStack pointer");
        free_mapped_user_range(page_table, TLS_VIRT_ADDR, TLS_VIRT_ADDR + ALIGNED_TOTAL_SIZE);
        delete thread;
        return nullptr;
    }

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

    // TLS pages are owned and reclaimed individually through the user pagemap.
    thread->tls_phys_ptr = 0;
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
