#include "threading.hpp"

#include <cstdint>
#include <cstring>
#include <limits>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/user_layout.hpp>
#include <platform/mm/virt.hpp>
#include <platform/random/entropy.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/scheduler.hpp>
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
// Keep startup residency small; exec backs argument pages explicitly and the
// existing fault path grows the full reserved stack in larger batches.
constexpr uint64_t STACK_INITIAL_BACKING_BYTES = 32ULL * 1024ULL;
constexpr uint64_t STACK_GROW_NORMAL_BYTES = 256ULL * 1024ULL;
constexpr uint64_t STACK_GROW_MEDIUM_BYTES = 512ULL * 1024ULL;
constexpr uint64_t STACK_GROW_LARGE_BYTES = 1024ULL * 1024ULL;
constexpr uint64_t STACK_GROW_NEAR_BYTES = 64ULL * 1024ULL;
constexpr uint64_t STACK_GROW_RSP_FAR_BYTES = 512ULL * 1024ULL;

auto min_u64(uint64_t a, uint64_t b) -> uint64_t { return a < b ? a : b; }

auto max_u64(uint64_t a, uint64_t b) -> uint64_t { return a > b ? a : b; }

auto checked_add(uint64_t lhs, uint64_t rhs, uint64_t& result) -> bool {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return false;
    }
    result = lhs + rhs;
    return true;
}

auto checked_page_align_up(uint64_t value, uint64_t& result) -> bool {
    constexpr uint64_t MASK = mm::paging::PAGE_SIZE - 1;
    if (value > std::numeric_limits<uint64_t>::max() - MASK) {
        return false;
    }
    result = (value + MASK) & ~MASK;
    return true;
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
        if (!mm::virt::is_page_mapped_or_reserved(page_table, addr)) {
            continue;
        }
        mm::virt::unmap_page(page_table, addr);
    }
}

auto map_zeroed_user_range(mm::paging::PageTable* page_table, uint64_t start, uint64_t size, const char* allocation_name) -> bool {
    for (uint64_t offset = 0; offset < size; offset += mm::paging::PAGE_SIZE) {
        // Fully clear private backing before publishing its user PTE. Avoid
        // an allocator zero/CR3 round trip followed by the same full clear.
        void* page =
            mm::phys::page_alloc_full_overwrite_page_with_reclaim_may_fail(mm::PhysicalPageOwner::USER_THREAD_TLS, allocation_name);
        if (page == nullptr) {
            return false;
        }
        std::memset(page, 0, mm::paging::PAGE_SIZE);
        auto const PHYS = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        mm::virt::map_page(page_table, start + offset, PHYS, mm::paging::page_types::USER | mm::paging::PAGE_NX);
    }
    return true;
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

auto stack_budget(task::Task* subject, uint64_t capacity) -> uint64_t {
    resource::Owner owner(subject);
    if (owner.get() == nullptr) {
        return 0;
    }
    uint64_t const FLAGS = owner.get()->resource_lock.lock_irqsave();
    uint64_t const SOFT = owner.get()->resources.entries[static_cast<unsigned>(resource::Id::STACK)].limit.soft;
    owner.get()->resource_lock.unlock_irqrestore(FLAGS);
    return resource::stack_backing_budget(SOFT, capacity, mm::paging::PAGE_SIZE);
}

void deny_stack(task::Task* subject) {
    resource::Owner owner(subject);
    if (owner.get() != nullptr) {
        uint64_t const FLAGS = owner.get()->resource_lock.lock_irqsave();
        owner.get()->resources.deny(static_cast<unsigned>(resource::Id::STACK), resource::Denial::SOFT_LIMIT);
        owner.get()->resource_lock.unlock_irqrestore(FLAGS);
    }
}

// No reclaim/yield while pages are temporarily outside pagemap ownership.
// Every PTE in this kernel-managed stack range was reserved at construction.
auto back_stack_pages(mm::paging::PageTable* page_table, uint64_t low, uint64_t high
#ifdef WOS_SELFTEST
                      ,
                      int fail_after = -1
#endif
                      ) -> bool {
    PreemptGuard guard;
    struct PendingPage {
        PendingPage* next;
        uint64_t vaddr;
    };
    PendingPage* pending = nullptr;
    for (uint64_t addr = low; addr < high; addr += mm::paging::PAGE_SIZE) {
        if (mm::virt::translate(page_table, addr) != mm::virt::PADDR_INVALID) {
            continue;
        }
#ifdef WOS_SELFTEST
        bool const INJECT_FAILURE = fail_after == 0;
        if (fail_after > 0) {
            --fail_after;
        }
#else
        constexpr bool INJECT_FAILURE = false;
#endif
        auto* page = INJECT_FAILURE ? nullptr
                                    : static_cast<PendingPage*>(mm::phys::page_alloc_full_overwrite_may_fail(
                                          mm::PhysicalPageOwner::USER_THREAD_STACK, mm::paging::PAGE_SIZE, "thread_stack_lazy"));
        if (page == nullptr) {
            while (pending != nullptr) {
                auto* next = pending->next;
                mm::phys::page_free(pending);
                pending = next;
            }
            return false;
        }
        page->next = pending;
        page->vaddr = addr;
        pending = page;
    }
    while (pending != nullptr) {
        auto* page = pending;
        pending = page->next;
        uint64_t const VADDR = page->vaddr;
        std::memset(page, 0, mm::paging::PAGE_SIZE);
        auto const PHYS = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        mm::virt::map_page(page_table, VADDR, PHYS, mm::paging::page_types::USER | mm::paging::PAGE_NX);
    }
    return true;
}

template <typename T>
auto write_mapped_user_value(mm::paging::PageTable* page_table, uint64_t vaddr, const T& value) -> bool {
    return write_mapped_user_bytes(page_table, vaddr, &value, sizeof(value));
}
}  // namespace

void init_threading() {}

#ifdef WOS_SELFTEST
auto selftest_back_stack_pages(mm::paging::PageTable* page_table, uint64_t low, uint64_t high, int fail_after) -> bool {
    return back_stack_pages(page_table, low, high, fail_after);
}
#endif

auto has_automatic_stack(const Thread* thread) -> bool {
    return thread != nullptr && thread->stack_size != 0 && thread->layout_size != 0 &&
           thread->stack_base_virt == thread->layout_base_virt + mm::paging::PAGE_SIZE && thread->stack_size < thread->layout_size;
}

void record_stack_usage(task::Task& subject) {
    resource::Owner owner(&subject);
    if (owner.get() == nullptr) {
        return;
    }
    auto& process = *owner.get();
    uint64_t const FLAGS = process.resource_lock.lock_irqsave();
    auto const* thread = process.thread;
    uint64_t const EXTENT =
        has_automatic_stack(thread) && thread->stack_lowest_backed != 0 ? stack_top(thread) - thread->stack_lowest_backed : 0;
    process.resources.commit_extent(static_cast<unsigned>(resource::Id::STACK), EXTENT);
    process.resource_lock.unlock_irqrestore(FLAGS);
}

bool ensure_stack_backing(Thread* thread, mm::paging::PageTable* page_table, uint64_t start, uint64_t end, task::Task* policy_owner) {
    if (!has_automatic_stack(thread) || page_table == nullptr || start > end) {
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
    uint64_t const FLOOR = TOP - stack_budget(policy_owner, thread->stack_size);
    // A policy snapshot admits this complete batch. Lowering later leaves
    // already mapped pages and this admitted operation intact.
    for (uint64_t addr = LOW; addr < min_u64(HIGH, FLOOR); addr += mm::paging::PAGE_SIZE) {
        if (mm::virt::translate(page_table, addr) == mm::virt::PADDR_INVALID) {
            deny_stack(policy_owner);
            return false;
        }
    }
    if (!back_stack_pages(page_table, LOW, HIGH)) {
        return false;
    }
    if (thread->stack_lowest_backed == 0 || LOW < thread->stack_lowest_backed) {
        thread->stack_lowest_backed = LOW;
    }
    if (policy_owner != nullptr && policy_owner->thread == thread && policy_owner->pagemap == page_table) {
        record_stack_usage(*policy_owner);
    }
    return true;
}

bool handle_lazy_stack_fault(Thread* thread, mm::paging::PageTable* page_table, uint64_t fault_addr, uint64_t rsp,
                             task::Task* policy_owner) {
    if (!has_automatic_stack(thread) || page_table == nullptr) {
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

    uint64_t const FLOOR = TOP - stack_budget(policy_owner, thread->stack_size);
    if (FAULT_PAGE < FLOOR) {
        deny_stack(policy_owner);
        return false;
    }
    low = max_u64(low, FLOOR);
    return ensure_stack_backing(thread, page_table, low, high, policy_owner);
}

Thread* create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table, uint64_t initial_tid,
                      const ker::loader::elf::TlsModule& tls_info, task::Task* policy_owner) {
    if (page_table == nullptr) {
        return nullptr;
    }

    uint64_t const BACKING_BUDGET = stack_budget(policy_owner, stack_size);
    if (BACKING_BUDGET < mm::paging::PAGE_SIZE) {
        deny_stack(policy_owner);
        return nullptr;
    }
    auto* thread = new Thread();
    if (thread == nullptr) {
        log::error("create_thread: failed to allocate Thread object");
        return nullptr;
    }

    // Use the actual TLS size from PT_TLS segment if available, otherwise use provided size
    uint64_t const ACTUAL_TLS_SIZE = (tls_info.tls_size > 0) ? tls_info.tls_size : tls_size;

    // TCB structure is ~136 bytes + stack canary + padding
    uint64_t const TCB_SIZE = 256;          // Extra space for mlibc's Tcb structure
    uint64_t const SAFESTACK_SIZE = 65536;  // 64KB for SafeStack unsafe stack
    uint64_t const GUARD_SIZE = mm::paging::PAGE_SIZE;
    uint64_t aligned_stack_size = 0;
    uint64_t tls_and_tcb_size = 0;
    uint64_t aligned_tls_size = 0;
    uint64_t layout_size = 0;
    bool const SIZES_VALID = stack_size != 0 && checked_page_align_up(stack_size, aligned_stack_size) &&
                             checked_add(ACTUAL_TLS_SIZE, TCB_SIZE, tls_and_tcb_size) &&
                             checked_page_align_up(tls_and_tcb_size, aligned_tls_size) &&
                             checked_add(GUARD_SIZE, aligned_stack_size, layout_size) &&
                             checked_add(layout_size, GUARD_SIZE, layout_size) && checked_add(layout_size, aligned_tls_size, layout_size) &&
                             checked_add(layout_size, GUARD_SIZE, layout_size) && checked_add(layout_size, SAFESTACK_SIZE, layout_size) &&
                             checked_add(layout_size, GUARD_SIZE, layout_size) && aligned_stack_size > sizeof(cpu::PerCpu);
    if (!SIZES_VALID) {
        log::error("create_thread: invalid stack/TLS layout sizes");
        delete thread;
        return nullptr;
    }

    if (!resource::admit_address_space(policy_owner, page_table, layout_size)) {
        delete thread;
        return nullptr;
    }

    uint64_t layout_base = 0;
    if (!mm::user_layout::choose_thread_region(page_table, layout_size, layout_base)) {
        log::error("create_thread: failed to select a collision-free user layout");
        delete thread;
        return nullptr;
    }

    uint64_t const STACK_VIRT_ADDR = layout_base + GUARD_SIZE;
    uint64_t const STACK_TOP = STACK_VIRT_ADDR + aligned_stack_size;
    uint64_t const TLS_VIRT_ADDR = STACK_TOP + GUARD_SIZE;
    uint64_t const TLS_END = TLS_VIRT_ADDR + aligned_tls_size;
    uint64_t const SAFESTACK_VIRT_ADDR = TLS_END + GUARD_SIZE;
    uint64_t const SAFESTACK_END = SAFESTACK_VIRT_ADDR + SAFESTACK_SIZE;

    uint64_t stack_canary = 0;
    if (!random::entropy::get_bytes(&stack_canary, sizeof(stack_canary))) {
        log::error("create_thread: entropy unavailable for stack canary");
        delete thread;
        return nullptr;
    }
    // A NUL low byte preserves the conventional string-overflow tripwire
    // without exposing or logging any of the remaining random canary bytes.
    stack_canary &= ~0xFFULL;
    if (stack_canary == 0) {
        stack_canary = 0x100;
    }

    // Reserve the whole region before backing it. Interior non-present stack
    // pages remain occupied address space, while the four designated guard
    // pages stay permanently non-present.
    mm::virt::reserve_page_range(page_table, layout_base, layout_size / mm::paging::PAGE_SIZE);
    if (!map_zeroed_user_range(page_table, TLS_VIRT_ADDR, aligned_tls_size, "thread_tls_page") ||
        !map_zeroed_user_range(page_table, SAFESTACK_VIRT_ADDR, SAFESTACK_SIZE, "thread_safestack_page")) {
        log::error("create_thread: failed to allocate TLS/SafeStack pages");
        free_mapped_user_range(page_table, layout_base, layout_base + layout_size);
        delete thread;
        return nullptr;
    }

    thread->magic = 0xDEADBEEF;

    // TCB goes at the TOP of the TLS area (highest address)
    uint64_t const TCB_VIRT_ADDR = TLS_VIRT_ADDR + ACTUAL_TLS_SIZE;

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
    uint32_t const CANCEL_BITS = 0;
    bool const TCB_INITIALIZED = write_mapped_user_value(page_table, TCB_VIRT_ADDR, SELF_POINTER) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x08, DTV_SIZE) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x10, DTV_POINTERS) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x18, INITIAL_TID) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x1C, DID_EXIT) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x28, stack_canary) &&
                                 write_mapped_user_value(page_table, TCB_VIRT_ADDR + 0x30, CANCEL_BITS);
    if (!TCB_INITIALIZED) {
        log::error("create_thread: failed to initialize mapped TCB");
        free_mapped_user_range(page_table, layout_base, layout_base + layout_size);
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
    uint64_t const SAFESTACK_PTR_VALUE = SAFESTACK_END - 512;  // Leave 512 bytes safety margin from top
    if (!write_mapped_user_value(page_table, TLS_VIRT_ADDR, SAFESTACK_PTR_VALUE)) {
        log::error("create_thread: failed to initialize SafeStack pointer");
        free_mapped_user_range(page_table, layout_base, layout_base + layout_size);
        delete thread;
        return nullptr;
    }

    // Save TLS mapping info into the Thread object for later initialization
    // Preserve the existing diagnostic extent semantics: tls_size covers the
    // complete TLS/TCB/SafeStack area (now including its interior guard).
    thread->tls_size = SAFESTACK_END - TLS_VIRT_ADDR;
    thread->tls_base_virt = TLS_VIRT_ADDR;
    thread->safestack_ptr_value = SAFESTACK_PTR_VALUE;
    thread->layout_base_virt = layout_base;
    thread->layout_size = layout_size;

    // Stack grows downward, so thread->stack points near the TOP of the reserved stack.
    // The actual syscall scratch area lives in kernel memory on Task::context.
    uint64_t const SCRATCH_AREA_SIZE = sizeof(cpu::PerCpu);
    thread->stack = STACK_TOP - SCRATCH_AREA_SIZE;
    thread->stack_size = aligned_stack_size;
    thread->stack_base_virt = STACK_VIRT_ADDR;
    thread->stack_lowest_backed = STACK_TOP;
    thread->fsbase = TCB_VIRT_ADDR;
    // User GS_BASE points at the base of the reserved stack range.
    thread->gsbase = STACK_VIRT_ADDR;

    // TLS pages are owned and reclaimed individually through the user pagemap.
    thread->tls_phys_ptr = 0;
    thread->stack_phys_ptr = 0;

    uint64_t const INITIAL_STACK_BYTES = min_u64(STACK_INITIAL_BACKING_BYTES, BACKING_BUDGET);
    bool const INITIAL_STACK_OK = ensure_stack_backing(thread, page_table, STACK_TOP - INITIAL_STACK_BYTES, STACK_TOP, policy_owner) &&
                                  back_stack_pages(page_table, STACK_VIRT_ADDR, STACK_VIRT_ADDR + mm::paging::PAGE_SIZE);
    if (!INITIAL_STACK_OK) {
        log::error("create_thread: failed to lazily back initial stack windows stack=0x%llx size=%llu initial=%llu",
                   static_cast<unsigned long long>(STACK_VIRT_ADDR), static_cast<unsigned long long>(stack_size),
                   static_cast<unsigned long long>(INITIAL_STACK_BYTES));
        free_mapped_user_range(page_table, layout_base, layout_base + layout_size);
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

auto range_overlaps_initial_layout(const Thread* thread, uint64_t start, uint64_t size) -> bool {
    if (thread == nullptr || thread->layout_size == 0 || size == 0) {
        return false;
    }

    uint64_t end = 0;
    uint64_t layout_end = 0;
    if (!checked_add(start, size, end) || !checked_add(thread->layout_base_virt, thread->layout_size, layout_end)) {
        return true;
    }
    return start < layout_end && thread->layout_base_virt < end;
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
