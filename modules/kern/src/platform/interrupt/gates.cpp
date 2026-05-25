#include "gates.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/coredump.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/debug/ptrace.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

#ifdef WOS_KCOV_PANIC_TRACE
#include <sanitizer/kcov.hpp>
#endif

#include <syscalls_impl/process/exit.hpp>

#include "abi/ptrace.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/sched/scheduler.hpp"
#include "util/hcf.hpp"

namespace ker::mod::gates {
namespace {
using journal = dbg::logger<"kernel">;
constexpr size_t INTERRUPT_VECTOR_COUNT = 256;
constexpr uint8_t DYNAMIC_VECTOR_BEGIN = 48;
constexpr uint64_t HHDM_BEGIN = 0xffff800000000000ULL;
constexpr uint64_t HHDM_END = 0xffff900000000000ULL;
constexpr uint64_t KERNEL_STATIC_BEGIN = 0xffffffff80000000ULL;
constexpr uint64_t KERNEL_STATIC_END = 0xffffffffc0000000ULL;
constexpr uint64_t PAGE_TABLE_BASE_MASK = ~((uint64_t{1} << ker::mod::mm::paging::PAGE_SHIFT) - 1U);
constexpr uint32_t IRQ_SLOW_TRACE_US = 30;
constexpr uint16_t IRQ_KIND_CONTEXT = 1;
constexpr uint16_t IRQ_KIND_LEGACY = 2;
constexpr uint16_t IRQ_KIND_UNHANDLED = 3;

std::array<interruptHandler_t, INTERRUPT_VECTOR_COUNT> interrupt_handlers{};
std::atomic<bool> panic_lock{false};
std::atomic<int64_t> panic_lock_owner{-1};

// Context-based IRQ handlers (parallel array)
struct IrqContext {
    irq_handler_fn handler = nullptr;
    void* data = nullptr;
    const char* name = nullptr;
};
std::array<IrqContext, INTERRUPT_VECTOR_COUNT> irq_contexts{};

// Next vector to try for allocation (48+ to avoid legacy ISA range)
uint8_t next_alloc_vector = DYNAMIC_VECTOR_BEGIN;

[[nodiscard]] constexpr auto is_kernel_pointer_range(uintptr_t addr) -> bool {
    return (addr >= HHDM_BEGIN && addr < HHDM_END) || (addr >= KERNEL_STATIC_BEGIN && addr < KERNEL_STATIC_END);
}

[[nodiscard]] constexpr auto page_table_index(uint64_t vaddr, uint8_t shift) -> size_t {
    return static_cast<size_t>((vaddr >> shift) & 0x1FFU);
}

auto load_u64_unaligned(const void* ptr) -> uint64_t {
    uint64_t value = 0;
    __builtin_memcpy(&value, ptr, sizeof(value));
    return value;
}

auto clamp_irq_latency_us(uint64_t latency_us) -> uint32_t {
    return latency_us > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(latency_us);
}

auto current_perf_pid() -> uint64_t {
    if (!sched::can_query_current_task()) {
        return 0;
    }
    auto* task = sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

void record_irq_perf(uint8_t vector, uint16_t kind, int32_t status, uint64_t started_us) {
    if (started_us == 0) {
        return;
    }

    uint64_t const NOW_US = time::get_us();
    uint64_t const ELAPSED_US = NOW_US >= started_us ? NOW_US - started_us : 0;
    uint32_t const LATENCY_US = clamp_irq_latency_us(ELAPSED_US);
    auto const CPU_ID = static_cast<uint32_t>(cpu::get_current_cpu_id_safe());
    perf::record_local_irq_summary(perf::WkiPerfLocalIrqOp::HANDLER, vector, kind, status, LATENCY_US, true);
    if (status >= 0 && ELAPSED_US < IRQ_SLOW_TRACE_US) {
        return;
    }

    perf::record_wki_event(CPU_ID, current_perf_pid(), perf::WkiPerfScope::LOCAL_IRQ,
                           static_cast<uint8_t>(perf::WkiPerfLocalIrqOp::HANDLER), perf::WkiPerfPhase::END, vector, kind,
                           perf::next_wki_trace_correlation(), status, LATENCY_US, WOS_PERF_CALLSITE());
}

void dump_stack_owner(const char* label, uint64_t rsp) {
    auto* active_owner = sched::debug_find_task_by_kernel_stack(rsp);
    if (active_owner != nullptr) {
        journal::panic("%s active owner: pid=%lu name=%s task=0x%lx stack=0x%lx state=%u queue=%u", label, active_owner->pid,
                       active_owner->name != nullptr ? active_owner->name : "?", reinterpret_cast<uint64_t>(active_owner),
                       active_owner->context.syscall_kernel_stack,
                       static_cast<unsigned>(active_owner->state.load(std::memory_order_relaxed)),
                       static_cast<unsigned>(active_owner->sched_queue));
    } else {
        journal::panic("%s active owner: <none>", label);
    }

    auto* dead_owner = sched::debug_find_dead_task_by_kernel_stack(rsp);
    if (dead_owner != nullptr) {
        journal::panic("%s dead owner: pid=%lu name=%s task=0x%lx stack=0x%lx state=%u queue=%u", label, dead_owner->pid,
                       dead_owner->name != nullptr ? dead_owner->name : "?", reinterpret_cast<uint64_t>(dead_owner),
                       dead_owner->context.syscall_kernel_stack, static_cast<unsigned>(dead_owner->state.load(std::memory_order_relaxed)),
                       static_cast<unsigned>(dead_owner->sched_queue));
    } else {
        journal::panic("%s dead owner: <none>", label);
    }
}

auto lookup_user_pte(ker::mod::mm::paging::PageTable* pagemap, uint64_t vaddr) -> const ker::mod::mm::paging::PageTableEntry* {
    if (pagemap == nullptr || vaddr >= 0x0000800000000000ULL) {
        return nullptr;
    }

    const auto IDX4 = page_table_index(vaddr, 39);
    const auto IDX3 = page_table_index(vaddr, 30);
    const auto IDX2 = page_table_index(vaddr, 21);
    const auto IDX1 = page_table_index(vaddr, 12);

    if (!pagemap->entries.at(IDX4).present) {
        return nullptr;
    }
    auto* pml3 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pagemap->entries.at(IDX4).frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml3->entries.at(IDX3).present) {
        return nullptr;
    }
    auto* pml2 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pml3->entries.at(IDX3).frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml2->entries.at(IDX2).present) {
        return nullptr;
    }
    auto* pml1 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
        ker::mod::mm::addr::get_virt_pointer(pml2->entries.at(IDX2).frame << ker::mod::mm::paging::PAGE_SHIFT));
    if (!pml1->entries.at(IDX1).present) {
        return nullptr;
    }

    return &pml1->entries.at(IDX1);
}

auto exception_handler(cpu::GPRegs& gpr, InterruptFrame& frame) -> void {
    // Page fault handler - handles COW faults and user-space segfaults.
    // This MUST run before the panic ownership CAS below, because page faults
    // (especially COW resolution) are normal concurrent events. If two CPUs
    // hit a COW fault at the same time the old code would make the second CPU
    // think it was a nested panic and halt.
    if (frame.int_num == 14) {
        // NOLINTNEXTLINE(misc-const-correctness)
        uint64_t cr2{};
        asm volatile("mov %%cr2, %0" : "=r"(cr2));

        // If pagefault_handler resolved a COW fault, we're done.
        // Otherwise fall through to the normal exception path which will
        // crash the userspace process or kernel-panic as appropriate.
        if (mm::virt::pagefault_handler(cr2, frame, gpr)) {
            return;
        }
    }
    // NOLINTBEGIN(misc-const-correctness)
    uint64_t cr0{};
    uint64_t cr2{};
    uint64_t cr3{};
    uint64_t cr4{};
    uint64_t cr8{};
    // NOLINTEND(misc-const-correctness)
    asm volatile("mov %%cr0, %0" : "=r"(cr0));
    asm volatile("mov %%cr2, %0" : "=r"(cr2));
    asm volatile("mov %%cr3, %0" : "=r"(cr3));
    asm volatile("mov %%cr4, %0" : "=r"(cr4));
    asm volatile("mov %%cr8, %0" : "=r"(cr8));

    // If an exception occurs while returning to or running in userspace,
    // it must not take down the whole kernel. Treat it as a process crash.
    // NOTE: userspace faults are handled BEFORE the panic-ownership CAS below
    // because they are normal concurrent events (multiple threads can crash
    // simultaneously) and must never be mistaken for nested kernel panics.
    uint64_t const CPL = frame.cs & 0x3;
    if (CPL == 3) {
        if (frame.int_num == 1) {
            uint64_t dr6 = 0;
            asm volatile("mov %%dr6, %0" : "=r"(dr6));
            if (ker::mod::debug::ptrace::report_user_stop(gpr, frame, ker::abi::ptrace::stop_reason::TRACE, 5, dr6)) {
                const uint64_t DR6_CLEAR = 0xFFFF0FF0;
                asm volatile("mov %0, %%dr6" ::"r"(DR6_CLEAR));
                return;
            }
        }
        if (frame.int_num == 3 &&
            ker::mod::debug::ptrace::report_user_stop(gpr, frame, ker::abi::ptrace::stop_reason::BREAKPOINT, 5, frame.rip)) {
            return;
        }

        auto* current_task_for_dump = ker::mod::sched::get_current_task();

        // DEBUG: Log detailed info about the mismatch between currentTask and CR3
        uint32_t const APIC_ID = apic::get_apic_id();
        uint64_t const CPU_ID_FROM_APIC = smt::get_cpu_index_from_apic_id(APIC_ID);
        journal::warn("USERFAULT DEBUG: apicId=%d cpuFromApic=%d task=%p taskPid=%d cr3=0x%lx taskPagemap=%p", APIC_ID, CPU_ID_FROM_APIC,
                      current_task_for_dump, (current_task_for_dump != nullptr) ? current_task_for_dump->pid : 0xDEAD, cr3,
                      (current_task_for_dump != nullptr) ? reinterpret_cast<void*>(current_task_for_dump->pagemap) : nullptr);

        ker::mod::dbg::coredump::try_write_for_task(current_task_for_dump, gpr, frame, cr2, cr3, apic::get_apic_id());

        if (frame.int_num == 14) {
            journal::warn("Userspace page fault: cr2=0x%lx err=%d rip=0x%lx rsp=0x%lx pid=%d", cr2, frame.err_code, frame.rip, frame.rsp,
                          (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
            if (current_task_for_dump != nullptr) {
                bool ret_slot_matches_rip = false;
                journal::warn(" task_ctx: saved_rip=0x%lx saved_rsp=0x%lx entry=0x%lx thread=%p scratch=%p",
                              current_task_for_dump->context.frame.rip, current_task_for_dump->context.frame.rsp,
                              current_task_for_dump->entry, current_task_for_dump->thread,
                              reinterpret_cast<void*>(current_task_for_dump->context.syscall_scratch_area));
                if (current_task_for_dump->pagemap != nullptr) {
                    const uint64_t RSP_PHYS = ker::mod::mm::virt::translate(current_task_for_dump->pagemap, frame.rsp);
                    const uint64_t RET_SLOT_VA = (frame.rsp >= sizeof(uint64_t)) ? (frame.rsp - sizeof(uint64_t)) : frame.rsp;
                    const uint64_t RET_SLOT_PHYS = ker::mod::mm::virt::translate(current_task_for_dump->pagemap, RET_SLOT_VA);

                    if (RET_SLOT_PHYS != ker::mod::mm::virt::PADDR_INVALID && RET_SLOT_PHYS != 0) {
                        const auto* ret_slot = reinterpret_cast<const uint64_t*>(ker::mod::mm::addr::get_virt_pointer(RET_SLOT_PHYS));
                        ret_slot_matches_rip = ret_slot[0] == frame.rip;
                        journal::warn(" fault_ret_slot_va=0x%lx phys=0x%lx q_m1=0x%lx rip_match=%d", RET_SLOT_VA, RET_SLOT_PHYS,
                                      ret_slot[0], ret_slot_matches_rip ? 1 : 0);
                    } else {
                        journal::warn(" fault_ret_slot_va=0x%lx phys=0x0 (unmapped)", RET_SLOT_VA);
                    }

                    if (RSP_PHYS != ker::mod::mm::virt::PADDR_INVALID && RSP_PHYS != 0) {
                        const auto* stack_words = reinterpret_cast<const uint64_t*>(ker::mod::mm::addr::get_virt_pointer(RSP_PHYS));
                        const uint64_t PAGE_OFF = frame.rsp & (ker::mod::mm::paging::PAGE_SIZE - 1);
                        size_t words_available = (ker::mod::mm::paging::PAGE_SIZE - PAGE_OFF) / sizeof(uint64_t);
                        words_available = std::min<size_t>(words_available, 4);
                        const uint64_t Q0 = (words_available > 0) ? stack_words[0] : 0;
                        const uint64_t Q1 = (words_available > 1) ? stack_words[1] : 0;
                        const uint64_t Q2 = (words_available > 2) ? stack_words[2] : 0;
                        const uint64_t Q3 = (words_available > 3) ? stack_words[3] : 0;
                        journal::warn(" fault_rsp_phys=0x%lx stack_qwords=%zu q0=0x%lx q1=0x%lx q2=0x%lx q3=0x%lx", RSP_PHYS,
                                      words_available, Q0, Q1, Q2, Q3);
                    } else {
                        journal::warn(" fault_rsp_phys=0x0 (unmapped)");
                    }

                    if (const auto* stack_pte = lookup_user_pte(current_task_for_dump->pagemap, frame.rsp); stack_pte != nullptr) {
                        const uint64_t STACK_PAGE_PHYS = static_cast<uint64_t>(stack_pte->frame) << ker::mod::mm::paging::PAGE_SHIFT;
                        uint64_t stack_pte_raw = 0;
                        __builtin_memcpy(&stack_pte_raw, stack_pte, sizeof(stack_pte_raw));
                        const bool STACK_COW = (stack_pte_raw & ker::mod::mm::paging::PAGE_COW) != 0U;
                        const auto STACK_REF = ker::mod::mm::phys::page_ref_get(
                            reinterpret_cast<void*>(ker::mod::mm::addr::get_virt_pointer(STACK_PAGE_PHYS)));
                        journal::warn(" stack_pte: vaddr=0x%lx phys=0x%lx user=%u rw=%u nx=%u cow=%u ref=%llu",
                                      frame.rsp & ~(ker::mod::mm::paging::PAGE_SIZE - 1), STACK_PAGE_PHYS, stack_pte->user,
                                      stack_pte->writable, stack_pte->no_execute, STACK_COW ? 1U : 0U,
                                      static_cast<unsigned long long>(STACK_REF));

                        if (ret_slot_matches_rip) {
                            ker::mod::mm::virt::debug_log_user_phys_mappings(STACK_PAGE_PHYS, "userfault-stack-ret",
                                                                             current_task_for_dump->pid, current_task_for_dump->name, true);
                        }
                    } else {
                        journal::warn(" stack_pte: vaddr=0x%lx unmapped", frame.rsp & ~(ker::mod::mm::paging::PAGE_SIZE - 1));
                    }
                }
            }

            auto* pml4 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(ker::mod::mm::addr::get_virt_pointer(cr3 & ~0xFFF));
            const auto IDX4 = page_table_index(cr2, 39);
            const auto IDX3 = page_table_index(cr2, 30);
            const auto IDX2 = page_table_index(cr2, 21);
            const auto IDX1 = page_table_index(cr2, 12);

            const auto& pml4e = pml4->entries.at(IDX4);
            journal::warn("PML4[%d]: present=%d frame=0x%lx", IDX4, pml4e.present, pml4e.frame);
            if (pml4e.present) {
                auto* pml3 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
                    ker::mod::mm::addr::get_virt_pointer(pml4e.frame << ker::mod::mm::paging::PAGE_SHIFT));
                const auto& pml3e = pml3->entries.at(IDX3);
                journal::warn(" PML3[%d]: present=%d frame=0x%lx", IDX3, pml3e.present, pml3e.frame);
                if (pml3e.present) {
                    auto* pml2 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
                        ker::mod::mm::addr::get_virt_pointer(pml3e.frame << ker::mod::mm::paging::PAGE_SHIFT));
                    const auto& pml2e = pml2->entries.at(IDX2);
                    journal::warn("  PML2[%d]: present=%d frame=0x%lx", IDX2, pml2e.present, pml2e.frame);
                    if (pml2e.present) {
                        auto* pml1 = reinterpret_cast<ker::mod::mm::paging::PageTable*>(
                            ker::mod::mm::addr::get_virt_pointer(pml2e.frame << ker::mod::mm::paging::PAGE_SHIFT));
                        const auto& pml1e = pml1->entries.at(IDX1);
                        journal::warn("   PML1[%d]: present=%d frame=0x%lx user=%d rw=%d nx=%d", IDX1, pml1e.present, pml1e.frame,
                                      pml1e.user, pml1e.writable, pml1e.no_execute);
                    }
                }
            }

            // Conventional exit code for a segfault-like crash.
            ker::syscall::process::wos_proc_exit(139);
            __builtin_unreachable();
        }

        if (frame.int_num == 1) {
            // Debug exception (hardware breakpoint or TF single-step).
            const uint64_t DR7_CLEAR = 0x400;       // bit 10 reserved=1, all enable bits=0
            const uint64_t DR6_CLEAR = 0xFFFF0FF0;  // reserved bits set, all status bits cleared
            asm volatile("mov %0, %%dr7" ::"r"(DR7_CLEAR));
            asm volatile("mov %0, %%dr6" ::"r"(DR6_CLEAR));
            journal::debug("Userspace debug trap (INT 1): rip=0x%lx pid=%x", frame.rip,
                           (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
            ker::syscall::process::wos_proc_exit(128 + 5);  // 133 = SIGTRAP
            __builtin_unreachable();
        }

        journal::warn("Userspace exception: int=%d err=%d rip=0x%lx pid=%x", frame.int_num, frame.err_code, frame.rip,
                      (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
        ker::syscall::process::wos_proc_exit(128 + static_cast<int>(frame.int_num & 0x7f));
        __builtin_unreachable();
    }

    // Kernel-mode exception - this is a kernel bug, don't try to write coredumps
    // as that would require memory allocation which could cause deadlocks if we
    // faulted while holding a spinlock (e.g., in pageAlloc).
    // Coredumps are only for userspace crashes, handled above.
    asm volatile("cli" ::: "memory");
    apic::one_shot_timer(0);

    // Serialize all panicking CPUs: the first CPU owns the dump and halts the
    // rest.  If this CPU already owns the lock, this is a nested fault during
    // panic reporting; halt immediately.
    {
        auto my_apic_id = static_cast<int64_t>(apic::get_apic_id());
        if (panic_lock_owner.load(std::memory_order_acquire) == my_apic_id) {
            hcf();
        }
        while (panic_lock.exchange(true, std::memory_order_acquire)) {
            asm volatile("pause");
        }
        panic_lock_owner.store(my_apic_id, std::memory_order_release);
    }

    ker::mod::smt::halt_other_cores();

    // CRITICAL: Enter epoch critical section to prevent GC from freeing currentTask
    // or its fields while we're printing panic info.
    ker::mod::sched::EpochManager::enter_critical_apic();

    // Acquire the serial lock BEFORE entering panic mode so other CPUs still
    // respect it. Once enterPanicMode() is called, all serial locking becomes
    // a no-op for other CPUs - they cannot deadlock waiting for our lock.
    // Other CPUs have been halted, but keep the serial panic path in its
    // unlocked mode so a nested panic on this CPU cannot deadlock on logging.
    io::serial::acquire_lock();
    io::serial::enter_panic_mode();

    journal::panic("PANIC!");

    // stack trace - validate RSP before dereferencing
    journal::panic("Stack trace:");
    auto* rsp = reinterpret_cast<uint64_t*>(frame.rsp);
    auto rsp_addr = reinterpret_cast<uintptr_t>(rsp);
    const bool RSP_VALID = is_kernel_pointer_range(rsp_addr);

    if (RSP_VALID) {
        constexpr uint64_t MAX_STACK_TRACE = 64;
        for (uint64_t i = 0; i < MAX_STACK_TRACE; i++) {
            journal::panic("%d: 0x%lx", i, rsp[i]);
        }
    } else {
        journal::panic("Invalid RSP: 0x%lx - skipping stack trace", rsp_addr);
    }

#ifdef WOS_KCOV_PANIC_TRACE
    ker::sanitizer::kcov::dump_panic_trace_for_cpu(apic::get_apic_id());
#endif

    // Dump current task information - use getCurrentTask() instead of hardcoded address
    // The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
    uint64_t const CPU_ID = apic::get_apic_id();
    sched::task::Task const* current_task = nullptr;

    if (!sched::has_run_queues()) {
        journal::panic("WARNING: RunQueues not initialized OR runQueue not set - cannot get current task!");
        goto skip_task_dump;  // NOLINT
    }

    current_task = sched::get_current_task();

    journal::panic("=== Current Task Info ===");
    journal::panic("debug_task_ptrs[%d] = 0x%lx", CPU_ID, reinterpret_cast<uint64_t>(current_task));

    if (current_task != nullptr) {
        // CRITICAL: Validate task pointer is in valid memory range before dereferencing
        // This prevents nested faults if GC is freeing the task concurrently
        auto task_addr = reinterpret_cast<uintptr_t>(current_task);

        if (!is_kernel_pointer_range(task_addr)) {
            journal::panic("currentTask pointer 0x%lx is out of valid kernel range!", reinterpret_cast<uint64_t>(current_task));
            journal::panic("=========================");
            goto skip_task_dump;  // NOLINT
        }

        // Use volatile access to detect if memory is being freed (may contain garbage)
        journal::panic("Task address: 0x%lx", reinterpret_cast<uint64_t>(current_task));

        // Validate and print name with extreme caution
        const char* name_ptr = current_task->name;
        if (name_ptr != nullptr) {
            auto name_addr = reinterpret_cast<uintptr_t>(name_ptr);

            if (is_kernel_pointer_range(name_addr)) {
                // Check first byte without assuming string is valid
                volatile const char* volatile_name_ptr = name_ptr;
                volatile char const FIRST_CHAR = volatile_name_ptr[0];
                if (FIRST_CHAR >= 0x20 && FIRST_CHAR <= 0x7e) {
                    journal::panic("Task name: %s", name_ptr);
                } else {
                    journal::panic("Task name: <invalid: first byte 0x%x>", static_cast<unsigned char>(FIRST_CHAR));
                }
            } else {
                journal::panic("Task name ptr: 0x%lx <out of range>", name_addr);
            }
        } else {
            journal::panic("Task name: <null>");
        }

        // Print primitive fields (safe - no pointer dereference)
        journal::panic("PID: 0x%x", current_task->pid);
        journal::panic("Type: 0x%x", static_cast<uint64_t>(current_task->type));
        journal::panic("Entry: 0x%lx", current_task->entry);
        journal::panic("kthread_entry: 0x%lx", reinterpret_cast<uint64_t>(current_task->kthread_entry));
        journal::panic("Pagemap: 0x%lx", reinterpret_cast<uint64_t>(current_task->pagemap));
        journal::panic("Thread: 0x%lx", reinterpret_cast<uint64_t>(current_task->thread));

        journal::panic("Task Context Frame:");
        journal::panic("  frame.rip: 0x%lx", current_task->context.frame.rip);
        journal::panic("  frame.cs: 0x%x", current_task->context.frame.cs);
        journal::panic("  frame.rsp: 0x%lx", current_task->context.frame.rsp);
        journal::panic("  frame.ss: 0x%x", current_task->context.frame.ss);
        journal::panic("  frame.flags: 0x%lx", current_task->context.frame.flags);

        journal::panic("Task Context:");
        journal::panic("  syscall_kernel_stack: 0x%lx", current_task->context.syscall_kernel_stack);
        journal::panic("  syscall_scratch_area: 0x%lx", current_task->context.syscall_scratch_area);
        journal::panic("Task Scheduler State:");
        journal::panic("  cpu=%lu queue=%u voluntary=%u wants_block=%u deferred=%u yield=%u", current_task->cpu,
                       static_cast<unsigned>(current_task->sched_queue), current_task->voluntary_block ? 1U : 0U,
                       current_task->wants_block ? 1U : 0U, current_task->deferred_task_switch ? 1U : 0U,
                       current_task->yield_switch ? 1U : 0U);
        journal::panic("  preemptDepth=%u preempt_pending=%u preemptMaxUs=%lu", current_task->preempt_disable_depth,
                       current_task->preempt_pending ? 1U : 0U, current_task->preempt_disable_max_us);
        dump_stack_owner("  frame.rsp", current_task->context.frame.rsp);
    } else {
        journal::panic("WARNING: currentTask is NULL!");
    }
    journal::panic("=========================");

skip_task_dump:

    // Check if page 0 is mapped (it should NOT be - null derefs should crash)
    uint64_t const CR3_BASE = cr3 & PAGE_TABLE_BASE_MASK;
    const auto* pml4 = reinterpret_cast<const uint64_t*>(CR3_BASE + HHDM_BEGIN);
    uint64_t const PML4E = load_u64_unaligned(&pml4[0]);  // Entry for VA 0x0
    journal::panic("Page 0 check: PML4[0] = 0x%lx (Present=%d)", PML4E, ((PML4E & 1) != 0U) ? 1 : 0);
    if ((PML4E & 1) != 0U) {
        journal::panic("WARNING: Page 0 is mapped! NULL derefs won't crash!");
    }

    // print frame info
    journal::panic("CPU: %d", CPU_ID);
    journal::panic("Interrupt number: %d", frame.int_num);
    journal::panic("Error code: %d", frame.err_code);
    journal::panic("RIP: 0x%lx", frame.rip);
    journal::panic("CS: 0x%x", frame.cs);
    journal::panic("CALCULATED PRIVILEGE LEVEL: %d", frame.cs & 0x3);
    journal::panic("RFLAGS: 0x%lx", frame.flags);
    journal::panic("RSP: 0x%lx", frame.rsp);
    journal::panic("SS: 0x%x", frame.ss);
    dump_stack_owner("fault rsp", frame.rsp);
    journal::panic("CR0: 0x%lx", cr0);
    journal::panic("CR2: 0x%lx", cr2);
    journal::panic("CR3: 0x%lx", cr3);
    journal::panic("CR4: 0x%lx", cr4);
    journal::panic("CR8: 0x%lx", cr8);

    // print general purpose registers
    journal::panic("General purpose registers:");
    journal::panic("RAX: 0x%lx", gpr.rax);
    journal::panic("RBX: 0x%lx", gpr.rbx);
    journal::panic("RCX: 0x%lx", gpr.rcx);
    journal::panic("RDX: 0x%lx", gpr.rdx);
    journal::panic("RDI: 0x%lx", gpr.rdi);
    journal::panic("RSI: 0x%lx", gpr.rsi);
    journal::panic("RBP: 0x%lx", gpr.rbp);
    journal::panic("R8: 0x%lx", gpr.r8);
    journal::panic("R9: 0x%lx", gpr.r9);
    journal::panic("R10: 0x%lx", gpr.r10);
    journal::panic("R11: 0x%lx", gpr.r11);
    journal::panic("R12: 0x%lx", gpr.r12);
    journal::panic("R13: 0x%lx", gpr.r13);
    journal::panic("R14: 0x%lx", gpr.r14);
    journal::panic("R15: 0x%lx", gpr.r15);

    // print segment selectors and descriptors
    journal::panic("Segment selectors and descriptors:");
    struct GDTR {
        uint16_t limit;
        uint64_t base;
    } __attribute__((packed)) gdtr{};
    asm volatile("sgdt %0" : "=m"(gdtr));

    // Validate GDTR before using it
    const bool GDTR_VALID = is_kernel_pointer_range(static_cast<uintptr_t>(gdtr.base));
    journal::panic("GDTR: base=0x%lx, limit=0x%x, valid=%d", gdtr.base, gdtr.limit, GDTR_VALID ? 1 : 0);

    // NOLINTBEGIN(misc-const-correctness)
    uint16_t sel_cs{};
    uint16_t sel_ds{};
    uint16_t sel_es{};
    uint16_t sel_fs{};
    uint16_t sel_gs{};
    uint16_t sel_ss{};
    uint16_t sel_tr{};
    // NOLINTEND(misc-const-correctness)
    asm volatile("mov %%cs, %0" : "=r"(sel_cs));
    asm volatile("mov %%ds, %0" : "=r"(sel_ds));
    asm volatile("mov %%es, %0" : "=r"(sel_es));
    asm volatile("mov %%fs, %0" : "=r"(sel_fs));
    asm volatile("mov %%gs, %0" : "=r"(sel_gs));
    asm volatile("mov %%ss, %0" : "=r"(sel_ss));
    asm volatile("str %0" : "=r"(sel_tr));

    auto print_selector = [&](const char* name, uint16_t sel) -> void {
        journal::panic("%s: 0x%x (index=%d, rpl=%d)", name, sel, static_cast<uint64_t>(sel >> 3), static_cast<uint64_t>(sel & 0x3));

        if (sel == 0) {
            journal::panic("  NULL selector");
            return;
        }

        if (!GDTR_VALID) {
            journal::panic("  Skipping descriptor dump (invalid GDTR)");
            return;
        }

        uint64_t const GDT_BASE = gdtr.base;
        uint64_t const INDEX = (sel >> 3);
        uint64_t const DESC_ADDR = GDT_BASE + (INDEX * 8);

        // Validate descriptor address is in valid kernel memory
        const bool DESC_ADDR_VALID = is_kernel_pointer_range(static_cast<uintptr_t>(DESC_ADDR));

        if (!DESC_ADDR_VALID) {
            journal::panic("  Invalid descriptor address: 0x%lx", DESC_ADDR);
            return;
        }

        uint64_t const DESC = load_u64_unaligned(reinterpret_cast<const void*>(DESC_ADDR));

        journal::panic("  Raw descriptor: 0x%lx", DESC);

        const uint64_t LIMIT_LOW = DESC & 0xFFFF;
        const uint64_t BASE_0_15 = (DESC >> 16) & 0xFFFF;
        const uint64_t BASE_16_23 = (DESC >> 32) & 0xFF;
        const uint64_t ACCESS = (DESC >> 40) & 0xFF;
        const uint64_t LIMIT_16_19 = (DESC >> 48) & 0xF;
        const uint64_t FLAGS = (DESC >> 52) & 0xF;
        const uint64_t BASE_24_31 = (DESC >> 56) & 0xFF;

        uint64_t limit = LIMIT_LOW | (LIMIT_16_19 << 16);
        bool const GRAN = (FLAGS >> 3) & 1;
        if (GRAN) {
            limit = (limit << 12) | 0xFFF;
        }

        const uint64_t BASE = BASE_0_15 | (BASE_16_23 << 16) | (BASE_24_31 << 24);

        bool const S_BIT = (ACCESS >> 4) & 1;  // 0 = system, 1 = code/data
        if (!S_BIT) {
            // system descriptor (e.g., TSS) -- read second qword for full base
            const uint64_t DESC_ADDR_HIGH = DESC_ADDR + 8;
            const bool DESC_ADDR_HIGH_VALID = is_kernel_pointer_range(static_cast<uintptr_t>(DESC_ADDR_HIGH));

            if (DESC_ADDR_HIGH_VALID) {
                const uint64_t DESC_HIGH = load_u64_unaligned(reinterpret_cast<const void*>(DESC_ADDR_HIGH));
                const uint64_t BASE_HIGH = DESC_HIGH & 0xFFFFFFFF;
                const uint64_t FULL_BASE = BASE | (BASE_HIGH << 32);
                journal::panic("  System descriptor (likely TSS). Base: 0x%lx, Limit: 0x%lx", FULL_BASE, limit);
                journal::panic("  Access: 0x%x, Flags: 0x%x", ACCESS, FLAGS);
                journal::panic("  Raw high: 0x%lx", DESC_HIGH);
            } else {
                journal::panic("  System descriptor (TSS). Base (partial): 0x%lx, Limit: 0x%lx", BASE, limit);
                journal::panic("  Access: 0x%x, Flags: 0x%x (high descriptor invalid)", ACCESS, FLAGS);
            }
        } else {
            // code/data descriptor
            journal::panic("  Code/Data descriptor. Base: 0x%lx, Limit: 0x%lx", BASE, limit);
            journal::panic("  Access: 0x%x, Flags: 0x%x", ACCESS, FLAGS);
        }
    };

    print_selector("CS", sel_cs);
    print_selector("DS", sel_ds);
    print_selector("ES", sel_es);
    print_selector("FS", sel_fs);
    print_selector("GS", sel_gs);
    print_selector("SS", sel_ss);
    print_selector("TR", sel_tr);

    // print common MSRs
    journal::panic("Common MSRs:");
    struct MsrInfo {
        const char* name;
        uint32_t id;
    };
    const std::array<MsrInfo, 10> MSRS = {{{.name = "IA32_EFER", .id = 0xC0000080},
                                           {.name = "IA32_STAR", .id = 0xC0000081},
                                           {.name = "IA32_LSTAR", .id = 0xC0000082},
                                           {.name = "IA32_FMASK", .id = 0xC0000084},
                                           {.name = "IA32_APIC_BASE", .id = 0x1B},
                                           {.name = "IA32_PAT", .id = 0x277},
                                           {.name = "IA32_MISC_ENABLE", .id = 0x1A0},
                                           {.name = "IA32_FS_BASE", .id = IA32_FS_BASE},
                                           {.name = "IA32_GS_BASE", .id = IA32_GS_BASE},
                                           {.name = "IA32_KERNEL_GS_BASE", .id = 0xC0000102}}};
    for (const auto& m : MSRS) {
        uint64_t val{};
        cpu_get_msr(m.id, &val);
        journal::panic("%s: 0x%lx", m.name, val);
    }

    journal::panic("Halting");
    apic::one_shot_timer(0);
    hcf();
}

}  // namespace

extern "C" void iterrupt_handler(cpu::GPRegs gpr, InterruptFrame frame) {
    if (frame.err_code != UINT64_MAX) {
        exception_handler(gpr, frame);
        return;
    }

    // Check context-based handler first (for device drivers)
    if (frame.int_num >= interrupt_handlers.size()) {
        exception_handler(gpr, frame);
        return;
    }
    const auto VECTOR = static_cast<size_t>(frame.int_num);
    bool const TRACE_IRQ = perf::is_local_irq_recording_enabled();
    uint64_t const IRQ_STARTED_US = TRACE_IRQ ? time::get_us() : 0;

    if (irq_contexts.at(VECTOR).handler != nullptr) {
        irq_contexts.at(VECTOR).handler(static_cast<uint8_t>(frame.int_num), irq_contexts.at(VECTOR).data);
        ker::mod::apic::eoi();
        record_irq_perf(static_cast<uint8_t>(frame.int_num), IRQ_KIND_CONTEXT, 0, IRQ_STARTED_US);
        return;
    }

    uint16_t irq_kind = IRQ_KIND_LEGACY;
    int32_t irq_status = 0;
    if (interrupt_handlers.at(VECTOR) != nullptr) {
        interrupt_handlers.at(VECTOR)(gpr, frame);
    } else if (is_irq(frame.int_num)) {
        // Unexpected hardware IRQ with no handler - log and ignore.
        ker::mod::io::serial::write("UNHANDLED IRQ: vector=");
        ker::mod::io::serial::write_hex(static_cast<uint8_t>(frame.int_num));
        ker::mod::io::serial::write("\n");
        irq_kind = IRQ_KIND_UNHANDLED;
        irq_status = -1;
    } else {
        exception_handler(gpr, frame);
        return;
    }
    ker::mod::apic::eoi();
    record_irq_perf(static_cast<uint8_t>(frame.int_num), irq_kind, irq_status, IRQ_STARTED_US);
}

// Vector 32 (0x20) is the timer interrupt, hardcoded in gates.asm (isr32 -> task_switch_handler).
// It must NEVER be assigned to any other handler.
static constexpr uint8_t TIMER_VECTOR = 32;

void set_interrupt_handler(uint8_t int_num, interruptHandler_t handler) {
    if (int_num == TIMER_VECTOR) {
        ker::mod::io::serial::write("setInterruptHandler: vector 32 is reserved for timer\n");
        return;
    }
    if (interrupt_handlers.at(int_num) != nullptr) {
        ker::mod::io::serial::write("Handler already set\n");
        return;
    }
    interrupt_handlers.at(int_num) = handler;
}

void remove_interrupt_handler(uint8_t int_num) { interrupt_handlers.at(int_num) = nullptr; }

auto is_interrupt_handler_set(uint8_t int_num) -> bool { return interrupt_handlers.at(int_num) != nullptr; }

auto request_irq(uint8_t vector, irq_handler_fn handler, void* data, const char* name) -> int {
    if (vector == TIMER_VECTOR) {
        journal::error("requestIrq: vector 32 is reserved for timer");
        return -1;
    }
    if (interrupt_handlers.at(vector) != nullptr || irq_contexts.at(vector).handler != nullptr) {
        journal::error("requestIrq: vector %d already in use (handler=%p context_handler=%p)", vector, interrupt_handlers.at(vector),
                       irq_contexts.at(vector).handler);
        return -1;
    }
    journal::info("requestIrq: vector=%d name=%s", vector, name);
    irq_contexts.at(vector).handler = handler;
    irq_contexts.at(vector).data = data;
    irq_contexts.at(vector).name = name;
    return 0;
}

void free_irq(uint8_t vector) {
    irq_contexts.at(vector).handler = nullptr;
    irq_contexts.at(vector).data = nullptr;
    irq_contexts.at(vector).name = nullptr;
}

auto allocate_vector() -> uint8_t {
    // Find a free vector starting from 48 (above legacy IRQ range).
    // Vectors 0-31 are CPU exceptions.
    // Vector 32 (0x20) is the timer interrupt - hardcoded in gates.asm
    // (isr32 -> task_switch_handler). NEVER allocate it.
    // Vectors 33-47 are reserved for legacy ISA IRQs.
    // Vectors 48-255 are available for MSI/dynamic allocation.
    for (size_t v = next_alloc_vector; v < interrupt_handlers.size(); v++) {
        if (interrupt_handlers.at(v) == nullptr && irq_contexts.at(v).handler == nullptr) {
            next_alloc_vector = static_cast<uint8_t>(v + 1);
            return static_cast<uint8_t>(v);
        }
    }
    // Wrap around and search from 48
    for (uint16_t v = DYNAMIC_VECTOR_BEGIN; v < next_alloc_vector; v++) {
        if (interrupt_handlers.at(v) == nullptr && irq_contexts.at(v).handler == nullptr) {
            return static_cast<uint8_t>(v);
        }
    }
    return 0;  // No free vector found
}

}  // namespace ker::mod::gates
