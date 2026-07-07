#include "gates.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/coredump.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/debug/ptrace.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

#ifdef WOS_KCOV_PANIC_TRACE
#include <sanitizer/kcov.hpp>
#endif

#include <syscalls_impl/process/exit.hpp>

#include "abi/ptrace.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sys/signal.hpp"
#include "platform/sys/usercopy.hpp"
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

void log_userfault_lazy_vmem_ranges(ker::mod::sched::task::Task* task, uint64_t cr2) {
    if (task == nullptr) {
        return;
    }

    uint64_t shadow_app = 0;
    bool const HAS_SHADOW_APP = cr2 >= 0x000000007fff8000ULL && cr2 < 0x0000800000000000ULL;
    if (HAS_SHADOW_APP) {
        shadow_app = (cr2 - 0x000000007fff8000ULL) << 3U;
    }

    ker::mod::sched::task::LazyVmemRange hit{};
    bool found_hit = false;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    size_t const RANGE_COUNT = task->lazy_vmem_ranges.size();
    for (size_t i = 0; i < RANGE_COUNT; ++i) {
        auto const& range = task->lazy_vmem_ranges.at(i);
        if (cr2 >= range.start && cr2 < range.end) {
            hit = range;
            found_hit = true;
            break;
        }
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);

    journal::warn(" userfault lazy_vmem: ranges=%llu cr2=0x%lx shadow_app=0x%lx", static_cast<unsigned long long>(RANGE_COUNT), cr2,
                  HAS_SHADOW_APP ? shadow_app : 0UL);

    if (found_hit) {
        journal::warn("  lazy_hit: [0x%lx, 0x%lx) prot=0x%lx flags=0x%lx", hit.start, hit.end, hit.prot, hit.flags);
        return;
    }

    journal::warn("  lazy_hit: <none>");
}

void log_user_exception_words(ker::mod::sched::task::Task* task, const char* label, uint64_t addr) {
    if (task == nullptr) {
        return;
    }

    std::array<uint64_t, 8> words{};
    constexpr size_t WORD_BYTES = sizeof(words[0]) * 8;
    if (!ker::mod::sys::usercopy::range_valid(addr, WORD_BYTES)) {
        journal::warn(" userfault %s: addr=0x%lx outside canonical user range", label, addr);
        return;
    }

    if (!ker::mod::sys::usercopy::copy_from_task(*task, addr, words.data(), WORD_BYTES)) {
        journal::warn(" userfault %s: unable to copy 64 bytes at addr=0x%lx", label, addr);
        return;
    }

    journal::warn(" userfault %s0: [0]=0x%lx [1]=0x%lx [2]=0x%lx [3]=0x%lx", label, words[0], words[1], words[2], words[3]);
    journal::warn(" userfault %s1: [4]=0x%lx [5]=0x%lx [6]=0x%lx [7]=0x%lx", label, words[4], words[5], words[6], words[7]);
}

void log_user_exception_registers(ker::mod::sched::task::Task* task, const cpu::GPRegs& gpr, const InterruptFrame& frame, uint64_t cr2) {
    journal::warn("Userspace exception detail: int=%d err=%d rip=0x%lx cs=0x%lx rsp=0x%lx ss=0x%lx rbp=0x%lx flags=0x%lx cr2=0x%lx pid=%d",
                  frame.int_num, frame.err_code, frame.rip, frame.cs, frame.rsp, frame.ss, gpr.rbp, frame.flags, cr2,
                  task != nullptr ? task->pid : 0);
    journal::warn(" userfault gpr0: rax=0x%lx rbx=0x%lx rcx=0x%lx rdx=0x%lx rsi=0x%lx rdi=0x%lx", gpr.rax, gpr.rbx, gpr.rcx, gpr.rdx,
                  gpr.rsi, gpr.rdi);
    journal::warn(" userfault gpr1: r8=0x%lx r9=0x%lx r10=0x%lx r11=0x%lx r12=0x%lx r13=0x%lx r14=0x%lx r15=0x%lx", gpr.r8, gpr.r9, gpr.r10,
                  gpr.r11, gpr.r12, gpr.r13, gpr.r14, gpr.r15);

    log_user_exception_words(task, "rsp", frame.rsp);
    log_user_exception_words(task, "rbp", gpr.rbp);
    log_user_exception_words(task, "rip", frame.rip >= 16 ? frame.rip - 16 : frame.rip);
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

auto signal_for_user_exception(uint64_t vector) -> int {
    switch (vector) {
        case 0:
            return 8;  // SIGFPE
        case 1:
        case 3:
            return 5;  // SIGTRAP
        case 6:
            return 4;  // SIGILL
        case 13:
        case 14:
            return 11;  // SIGSEGV
        default: {
            int const SIGNO = static_cast<int>(vector & 0x7f);
            return SIGNO != 0 ? SIGNO : 6;  // SIGABRT fallback
        }
    }
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

        auto const FATAL_SIGNAL = static_cast<uint32_t>(signal_for_user_exception(frame.int_num));
        uint64_t const FATAL_ADDRESS = frame.int_num == 14 ? cr2 : frame.rip;
        if (ker::mod::debug::ptrace::report_user_exception_stop(gpr, frame, FATAL_SIGNAL, FATAL_ADDRESS, frame.int_num)) {
            return;
        }
        if (ker::mod::sys::signal::deliver_synchronous_signal_interrupt(gpr, frame, static_cast<int>(FATAL_SIGNAL))) {
            return;
        }

        auto* current_task_for_dump = ker::mod::sched::get_current_task();

        // DEBUG: Log detailed info about the mismatch between currentTask and CR3
        uint32_t const APIC_ID = apic::get_apic_id();
        uint64_t const CPU_ID_FROM_APIC = smt::get_cpu_index_from_apic_id(APIC_ID);
        journal::warn("USERFAULT DEBUG: apicId=%d cpuFromApic=%d task=%p taskPid=%d cr3=0x%lx taskPagemap=%p", APIC_ID, CPU_ID_FROM_APIC,
                      current_task_for_dump, (current_task_for_dump != nullptr) ? current_task_for_dump->pid : 0xDEAD, cr3,
                      (current_task_for_dump != nullptr) ? reinterpret_cast<void*>(current_task_for_dump->pagemap) : nullptr);
        log_user_exception_registers(current_task_for_dump, gpr, frame, cr2);

        if (frame.int_num == 14) {
            log_userfault_lazy_vmem_ranges(current_task_for_dump, cr2);
            journal::warn("Userspace page fault: cr2=0x%lx err=%d rip=0x%lx rsp=0x%lx pid=%d", cr2, frame.err_code, frame.rip, frame.rsp,
                          (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
            journal::warn(" userfault gpr0: rax=0x%lx rbx=0x%lx rcx=0x%lx rdx=0x%lx rsi=0x%lx rdi=0x%lx rbp=0x%lx", gpr.rax, gpr.rbx,
                          gpr.rcx, gpr.rdx, gpr.rsi, gpr.rdi, gpr.rbp);
            journal::warn(" userfault gpr1: r8=0x%lx r9=0x%lx r10=0x%lx r11=0x%lx r12=0x%lx r13=0x%lx r14=0x%lx r15=0x%lx", gpr.r8, gpr.r9,
                          gpr.r10, gpr.r11, gpr.r12, gpr.r13, gpr.r14, gpr.r15);
            if (current_task_for_dump != nullptr) {
                std::array<uint64_t, 8> stack_words{};
                if (ker::mod::sys::usercopy::copy_from_task(*current_task_for_dump, frame.rsp, stack_words.data(),
                                                            stack_words.size() * sizeof(stack_words[0]))) {
                    journal::warn(" userfault stack0: [0]=0x%lx [1]=0x%lx [2]=0x%lx [3]=0x%lx", stack_words[0], stack_words[1],
                                  stack_words[2], stack_words[3]);
                    journal::warn(" userfault stack1: [4]=0x%lx [5]=0x%lx [6]=0x%lx [7]=0x%lx", stack_words[4], stack_words[5],
                                  stack_words[6], stack_words[7]);
                } else {
                    journal::warn(" userfault stack: unable to copy 64 bytes at rsp=0x%lx", frame.rsp);
                }
                journal::warn(" task_ctx: saved_rip=0x%lx saved_rsp=0x%lx entry=0x%lx thread=%p scratch=%p",
                              current_task_for_dump->context.frame.rip, current_task_for_dump->context.frame.rsp,
                              current_task_for_dump->entry, current_task_for_dump->thread,
                              reinterpret_cast<void*>(current_task_for_dump->context.syscall_scratch_area));
            }
        }

        ker::mod::dbg::coredump::try_write_for_task(current_task_for_dump, gpr, frame, cr2, cr3, apic::get_apic_id());

        if (frame.int_num == 14) {
            ker::syscall::process::wos_proc_exit_signal(11);
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
            ker::syscall::process::wos_proc_exit_signal(5);
            __builtin_unreachable();
        }

        journal::warn("Userspace exception: int=%d err=%d rip=0x%lx pid=%x", frame.int_num, frame.err_code, frame.rip,
                      (ker::mod::sched::get_current_task() != nullptr) ? ker::mod::sched::get_current_task()->pid : 0);
        ker::syscall::process::wos_proc_exit_signal(signal_for_user_exception(frame.int_num));
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

    if (!sched::can_query_current_task()) {
        journal::panic("WARNING: current task is not queryable in this context - skipping task dump!");
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
                       static_cast<unsigned>(current_task->sched_queue), current_task->is_voluntary_blocked() ? 1U : 0U,
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
    uint64_t const IRQ_ACCOUNT_STARTED_US = time::get_us();
    uint64_t const IRQ_STARTED_US = TRACE_IRQ ? IRQ_ACCOUNT_STARTED_US : 0;

    if (irq_contexts.at(VECTOR).handler != nullptr) {
        irq_contexts.at(VECTOR).handler(static_cast<uint8_t>(frame.int_num), irq_contexts.at(VECTOR).data);
        ker::mod::apic::eoi();
        uint64_t const IRQ_ACCOUNT_FINISHED_US = time::get_us();
        sched::account_irq_time_us(IRQ_ACCOUNT_FINISHED_US >= IRQ_ACCOUNT_STARTED_US ? IRQ_ACCOUNT_FINISHED_US - IRQ_ACCOUNT_STARTED_US
                                                                                     : 0);
        record_irq_perf(static_cast<uint8_t>(frame.int_num), IRQ_KIND_CONTEXT, 0, IRQ_STARTED_US);
        return;
    }

    uint16_t irq_kind = IRQ_KIND_LEGACY;
    int32_t irq_status = 0;
    if (interrupt_handlers.at(VECTOR) != nullptr) {
        interrupt_handlers.at(VECTOR)(gpr, frame);
    } else if (is_irq(frame.int_num)) {
        // Unexpected hardware IRQ with no handler - log and ignore.
        journal::warn("UNHANDLED IRQ: vector=0x%x", static_cast<unsigned>(frame.int_num));
        irq_kind = IRQ_KIND_UNHANDLED;
        irq_status = -1;
    } else {
        exception_handler(gpr, frame);
        return;
    }
    ker::mod::apic::eoi();
    uint64_t const IRQ_ACCOUNT_FINISHED_US = time::get_us();
    sched::account_irq_time_us(IRQ_ACCOUNT_FINISHED_US >= IRQ_ACCOUNT_STARTED_US ? IRQ_ACCOUNT_FINISHED_US - IRQ_ACCOUNT_STARTED_US : 0);
    record_irq_perf(static_cast<uint8_t>(frame.int_num), irq_kind, irq_status, IRQ_STARTED_US);
}

// Vector 32 (0x20) is the timer interrupt, hardcoded in gates.asm (isr32 -> task_switch_handler).
// It must NEVER be assigned to any other handler.
static constexpr uint8_t TIMER_VECTOR = 32;

void set_interrupt_handler(uint8_t int_num, interruptHandler_t handler) {
    if (int_num == TIMER_VECTOR) {
        journal::error("setInterruptHandler: vector 32 is reserved for timer");
        return;
    }
    if (interrupt_handlers.at(int_num) != nullptr) {
        journal::error("setInterruptHandler: vector %u already set", static_cast<unsigned>(int_num));
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
