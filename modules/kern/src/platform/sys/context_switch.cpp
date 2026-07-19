#include "context_switch.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <util/hcf.hpp>
#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/power/power.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/signal.hpp"

namespace ker::mod::sys::context_switch {
extern "C" void wos_kernel_idle_loop();                               // NOLINT(readability-identifier-naming)
extern "C" void wos_kernel_thread_trampoline();                       // NOLINT(readability-identifier-naming)
extern "C" [[noreturn]] void wos_enterIdleStack(uint64_t stack_top);  // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_start[];                                // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_end[];                                  // NOLINT(readability-identifier-naming)

namespace {
// Enables deeper scheduler/context validation such as stack-owner checks on
// every timer interrupt.  Keep off in production: the cheap selector/rip/rsp
// guards below still catch obviously corrupt return frames.
#ifdef SCHED_VALIDATE_CONTEXT
constexpr bool K_ENABLE_SCHED_VALIDATE_CONTEXT = true;
#else
constexpr bool K_ENABLE_SCHED_VALIDATE_CONTEXT = false;
#endif

constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;
constexpr uint64_t USER_RFLAGS_FIXED_ONE = 1ULL << 1;
constexpr uint64_t USER_RFLAGS_INTERRUPT_ENABLE = 1ULL << 9;
constexpr uint64_t USER_RFLAGS_REQUIRED_MASK = USER_RFLAGS_FIXED_ONE | USER_RFLAGS_INTERRUPT_ENABLE;
constexpr uint64_t USER_PROT_WRITE = 0x2ULL;
constexpr uint64_t USER_PROT_EXEC = 0x4ULL;

inline auto valid_kernel_stack(uint64_t rsp) -> bool { return rsp >= 0xffff800000000000ULL && rsp < 0xffff900000000000ULL; }

inline auto is_kernel_text_pointer(uint64_t rip) -> bool {
    auto const TEXT_START = reinterpret_cast<uint64_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uint64_t>(__kernel_text_end);
    return rip >= TEXT_START && rip < TEXT_END;
}

inline auto stack_belongs_to_task(sched::task::Task* task, uint64_t rsp) -> bool {
    if (task == nullptr || !valid_kernel_stack(task->context.syscall_kernel_stack)) {
        return false;
    }
    uint64_t const STACK_TOP = task->context.syscall_kernel_stack;
    return rsp > STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE && rsp <= STACK_TOP;
}

inline auto valid_user_resume_scalar(uint64_t value) -> bool { return value != 0 && value < USER_ADDR_LIMIT; }

inline auto user_rflags_have_fixed_bit(uint64_t flags) -> bool { return (flags & USER_RFLAGS_FIXED_ONE) != 0; }

inline auto valid_user_rflags(uint64_t flags) -> bool { return (flags & USER_RFLAGS_REQUIRED_MASK) == USER_RFLAGS_REQUIRED_MASK; }

enum class LazyUserAccess : uint8_t {
    EXECUTE,
    WRITE_STACK,
};

inline auto lazy_user_range_allows(sched::task::Task* task, uint64_t addr, LazyUserAccess access) -> bool {
    if (task == nullptr || addr == 0 || addr >= USER_ADDR_LIMIT) {
        return false;
    }

    uint64_t const PAGE_ADDR = addr & ~(mm::paging::PAGE_SIZE - 1);
    bool allowed = false;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    for (const auto& range : task->lazy_vmem_ranges) {
        if (PAGE_ADDR < range.start || PAGE_ADDR >= range.end) {
            continue;
        }

        uint64_t const REQUIRED_PROT = access == LazyUserAccess::EXECUTE ? USER_PROT_EXEC : USER_PROT_WRITE;
        allowed = (range.prot & REQUIRED_PROT) != 0;
        break;
    }
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    return allowed;
}

inline auto stack_region_contains(sched::task::Task* task, uint64_t addr, size_t size) -> bool {
    if (task == nullptr || !valid_kernel_stack(task->context.syscall_kernel_stack) || size == 0) {
        return false;
    }
    uint64_t const STACK_TOP = task->context.syscall_kernel_stack;
    uint64_t const STACK_BASE = STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE;
    uint64_t const END = addr + static_cast<uint64_t>(size);
    return END >= addr && addr >= STACK_BASE && END <= STACK_TOP;
}

inline void panic_bad_handoff_stack(sched::task::Task* task, const char* reason) {
    dbg::logger<"ctxswitch">::error(
        "bad user handoff stack: reason=%s pid=%lu name=%s type=%u stack=0x%llx task=%p current=%p return=%p",
        reason != nullptr ? reason : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
        task != nullptr ? static_cast<unsigned>(task->type) : 0U,
        task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL, static_cast<void*>(task),
        static_cast<void*>(sched::get_current_task()), static_cast<void*>(sched::get_return_task()));
    hcf();
}

inline void validate_kernel_frame(const gates::InterruptFrame& frame, sched::task::Task* task, const char* path) {
    if (frame.cs == desc::gdt::GDT_USER_CS) {
        if (task != nullptr && task->type == sched::task::TaskType::PROCESS) {
            return;
        }
        dbg::logger<"ctxswitch">::error(
            "user return frame for non-process: path=%s pid=%lu name=%s type=%u ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx task=%p",
            path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
            task != nullptr ? static_cast<unsigned>(task->type) : 0U, static_cast<unsigned long long>(frame.ss),
            static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
            static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
        hcf();
    }

    if (frame.cs != desc::gdt::GDT_KERN_CS) {
        dbg::logger<"ctxswitch">::error(
            "bad return frame selector: path=%s pid=%lu name=%s cs=0x%llx ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx task=%p",
            path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
            static_cast<unsigned long long>(frame.cs), static_cast<unsigned long long>(frame.ss),
            static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
            static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
        hcf();
    }

    const bool RIP_BAD = !is_kernel_text_pointer(frame.rip);
    const bool RSP_BAD = !valid_kernel_stack(frame.rsp);
    const bool STACK_OWNER_BAD = K_ENABLE_SCHED_VALIDATE_CONTEXT && task != nullptr && task->type != sched::task::TaskType::IDLE &&
                                 !stack_belongs_to_task(task, frame.rsp);
    const bool FLAGS_BAD = (frame.flags & 0x2ULL) == 0;
    const bool SS_BAD = frame.ss != desc::gdt::GDT_KERN_DS;
    if (!RIP_BAD && !RSP_BAD && !STACK_OWNER_BAD && !FLAGS_BAD && !SS_BAD) {
        return;
    }

    auto* owner = K_ENABLE_SCHED_VALIDATE_CONTEXT ? sched::debug_find_task_by_kernel_stack(frame.rsp) : nullptr;
    dbg::logger<"ctxswitch">::error(
        "bad kernel return frame: path=%s pid=%lu name=%s rip=0x%llx rsp=0x%llx stack=0x%llx owner=%lu(%s) cs=0x%llx ss=0x%llx "
        "flags=0x%llx task=%p",
        path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
        static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
        task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL, owner != nullptr ? owner->pid : 0UL,
        (owner != nullptr && owner->name != nullptr) ? owner->name : "?", static_cast<unsigned long long>(frame.cs),
        static_cast<unsigned long long>(frame.ss), static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
    hcf();
}

inline void validate_user_frame(const gates::InterruptFrame& frame, sched::task::Task* task, const char* path) {
    const bool TASK_BAD = task == nullptr || task->type != sched::task::TaskType::PROCESS || task->pagemap == nullptr;
    const bool CS_BAD = frame.cs != desc::gdt::GDT_USER_CS;
    const bool SS_BAD = frame.ss != desc::gdt::GDT_USER_DS;
    const bool RIP_BAD = frame.rip >= 0x800000000000ULL;
    const bool RSP_BAD = frame.rsp >= 0x800000000000ULL;
    const bool FLAGS_BAD = !valid_user_rflags(frame.flags);
    const uint64_t RIP_PHYS = !TASK_BAD && !RIP_BAD ? mm::virt::translate(task->pagemap, frame.rip) : mm::virt::PADDR_INVALID;
    const uint64_t RSP_PHYS = !TASK_BAD && !RSP_BAD ? mm::virt::translate(task->pagemap, frame.rsp) : mm::virt::PADDR_INVALID;
    const bool RIP_UNMAPPED = RIP_PHYS == mm::virt::PADDR_INVALID || RIP_PHYS == 0;
    const bool RSP_UNMAPPED = RSP_PHYS == mm::virt::PADDR_INVALID || RSP_PHYS == 0;
    const bool RIP_LAZY_BACKED = RIP_UNMAPPED && !TASK_BAD && !RIP_BAD && lazy_user_range_allows(task, frame.rip, LazyUserAccess::EXECUTE);
    const bool RSP_LAZY_BACKED =
        RSP_UNMAPPED && !TASK_BAD && !RSP_BAD && lazy_user_range_allows(task, frame.rsp, LazyUserAccess::WRITE_STACK);
    if (!TASK_BAD && !CS_BAD && !SS_BAD && !RIP_BAD && !RSP_BAD && !FLAGS_BAD && (!RIP_UNMAPPED || RIP_LAZY_BACKED) &&
        (!RSP_UNMAPPED || RSP_LAZY_BACKED)) {
        return;
    }

    dbg::logger<"ctxswitch">::error(
        "bad user return frame: path=%s pid=%lu name=%s type=%u rip=0x%llx rsp=0x%llx cs=0x%llx ss=0x%llx flags=0x%llx "
        "rip_phys=0x%llx rsp_phys=0x%llx task=%p",
        path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
        task != nullptr ? static_cast<unsigned>(task->type) : 0U, static_cast<unsigned long long>(frame.rip),
        static_cast<unsigned long long>(frame.rsp), static_cast<unsigned long long>(frame.cs), static_cast<unsigned long long>(frame.ss),
        static_cast<unsigned long long>(frame.flags), static_cast<unsigned long long>(RIP_PHYS), static_cast<unsigned long long>(RSP_PHYS),
        static_cast<void*>(task));
    hcf();
}

inline auto is_idle_return_frame(const gates::InterruptFrame& frame, sched::task::Task* task) -> bool {
    return task != nullptr && task->type == sched::task::TaskType::IDLE && frame.cs == desc::gdt::GDT_KERN_CS &&
           frame.rip == reinterpret_cast<uint64_t>(wos_kernel_idle_loop);
}

inline auto is_user_return_frame(const gates::InterruptFrame& frame) -> bool { return (frame.cs & 0x3ULL) == 0x3ULL; }

inline auto task_name_looks_like_clang(const sched::task::Task* task) -> bool {
    if (task == nullptr || task->name == nullptr) {
        return false;
    }

    char const* name = task->name;
    return name[0] == 'c' && name[1] == 'l' && name[2] == 'a' && name[3] == 'n' && name[4] == 'g';
}

void debug_validate_clang_return(const char* path, sched::task::Task* task, const cpu::GPRegs& gpr, const gates::InterruptFrame& frame,
                                 uint64_t syscall_signal_mode = UINT64_MAX, uint64_t live_rip = 0, uint64_t live_rsp = 0,
                                 uint64_t live_flags = 0) {
    constexpr uint64_t COMPLEX_MID_INSN_RIP = 0x2dcbb0fULL;
    constexpr uint64_t OMP_R13_READY_BEGIN = 0x2ecce6fULL;
    constexpr uint64_t OMP_SCOPE_END = 0x2eccee3ULL;
    constexpr uint64_t OMP_MID_CALL_RIP = 0x2eccee0ULL;
    constexpr uint64_t LOW_USER_POINTER_LIMIT = 0x100000000ULL;

    if (!task_name_looks_like_clang(task) || frame.cs != desc::gdt::GDT_USER_CS) {
        return;
    }

    bool const IN_WATCHED_RANGE = frame.rip >= OMP_R13_READY_BEGIN && frame.rip < OMP_SCOPE_END;
    bool const MID_CALL_RIP = frame.rip == OMP_MID_CALL_RIP;
    bool const COMPLEX_MID_RIP = frame.rip == COMPLEX_MID_INSN_RIP;
    bool const LOW_R13 = gpr.r13 != 0 && gpr.r13 < LOW_USER_POINTER_LIMIT;
    bool const LOW_R15 = gpr.r15 != 0 && gpr.r15 < LOW_USER_POINTER_LIMIT;
    if ((!IN_WATCHED_RANGE && !MID_CALL_RIP && !COMPLEX_MID_RIP) || (!LOW_R13 && !LOW_R15 && !MID_CALL_RIP && !COMPLEX_MID_RIP)) {
        return;
    }

    uint64_t const EXPECTED_R13 = gpr.rbp >= 0x168 ? gpr.rbp - 0x168 : 0;
    static std::atomic<uint64_t> log_count{0};
    uint64_t const N = log_count.fetch_add(1, std::memory_order_relaxed);
    if (N >= 32) {
        return;
    }

    dbg::logger<"ctxswitch">::warn(
        "clang suspicious return: path=%s mode=%llu pid=%lu name=%s rip=0x%llx rsp=0x%llx flags=0x%llx live_rip=0x%llx "
        "live_rsp=0x%llx live_flags=0x%llx r13=0x%llx expected_r13=0x%llx rbp=0x%llx r12=0x%llx r14=0x%llx r15=0x%llx "
        "rcx=0x%llx sig=%u do_sig=%u pending=0x%llx",
        path != nullptr ? path : "?", static_cast<unsigned long long>(syscall_signal_mode), task->pid,
        task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
        static_cast<unsigned long long>(frame.flags), static_cast<unsigned long long>(live_rip), static_cast<unsigned long long>(live_rsp),
        static_cast<unsigned long long>(live_flags), static_cast<unsigned long long>(gpr.r13),
        static_cast<unsigned long long>(EXPECTED_R13), static_cast<unsigned long long>(gpr.rbp), static_cast<unsigned long long>(gpr.r12),
        static_cast<unsigned long long>(gpr.r14), static_cast<unsigned long long>(gpr.r15), static_cast<unsigned long long>(gpr.rcx),
        task->in_signal_handler ? 1U : 0U, task->do_sigreturn ? 1U : 0U,
        static_cast<unsigned long long>(task->sig_pending.load(std::memory_order_acquire)));
}

inline void check_pending_signals_for_return(cpu::GPRegs& gpr, gates::InterruptFrame& frame) {
    if (!is_user_return_frame(frame)) {
        return;
    }

    auto* return_task = sched::get_return_task();
    if (return_task == sched::get_current_task()) {
        sys::signal::check_pending_signals_interrupt(gpr, frame);
    } else {
        sys::signal::check_pending_signals_handoff(return_task, gpr, frame);
    }
}

inline auto is_first_run_kernel_thread_frame(const gates::InterruptFrame& frame, const cpu::GPRegs& gpr, sched::task::Task* task) -> bool {
    return task != nullptr && task->type == sched::task::TaskType::DAEMON && task->kthread_entry != nullptr &&
           frame.cs == desc::gdt::GDT_KERN_CS && frame.ss == desc::gdt::GDT_KERN_DS &&
           frame.rip == reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline) &&
           gpr.rdi == reinterpret_cast<uint64_t>(task->kthread_entry) && frame.rsp == task->context.syscall_kernel_stack &&
           valid_kernel_stack(frame.rsp);
}

auto interrupted_caller_from_rbp(uint64_t rbp) -> uint64_t {
    if (!valid_kernel_stack(rbp)) {
        return 0;
    }
    auto* frame = reinterpret_cast<uint64_t*>(rbp);
    return frame[1];
}

void validate_timer_stack(const gates::InterruptFrame& frame, const cpu::GPRegs& gpr, sched::task::Task* task) {
    if constexpr (!K_ENABLE_SCHED_VALIDATE_CONTEXT) {
        (void)frame;
        (void)gpr;
        (void)task;
        return;
    }

    if (task == nullptr || frame.cs != desc::gdt::GDT_KERN_CS || task->type == sched::task::TaskType::IDLE) {
        return;
    }
    if (stack_belongs_to_task(task, frame.rsp)) {
        return;
    }

    auto* owner = sched::debug_find_task_by_kernel_stack(frame.rsp);
    uint64_t const CALLER = interrupted_caller_from_rbp(gpr.rbp);
    dbg::logger<"ctxswitch">::error(
        "timer on non-task stack: current=%lu(%s) rip=0x%llx caller=0x%llx rsp=0x%llx stack=0x%llx type=%u hasRun=%u owner=%lu(%s) "
        "owner_stack=0x%llx owner_cpu=%llu owner_q=%u owner_heap=%d",
        task->pid, task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(frame.rip),
        static_cast<unsigned long long>(CALLER), static_cast<unsigned long long>(frame.rsp),
        static_cast<unsigned long long>(task->context.syscall_kernel_stack), static_cast<unsigned>(task->type), task->has_run ? 1U : 0U,
        owner != nullptr ? owner->pid : 0UL, (owner != nullptr && owner->name != nullptr) ? owner->name : "?",
        owner != nullptr ? static_cast<unsigned long long>(owner->context.syscall_kernel_stack) : 0ULL,
        owner != nullptr ? static_cast<unsigned long long>(owner->cpu) : 0ULL,
        owner != nullptr ? static_cast<unsigned>(owner->sched_queue) : 0U, owner != nullptr ? owner->heap_index : -1);
    hcf();
}

auto current_stack_allows_local_reschedule() -> bool {
    if (!sched::can_query_current_task()) {
        return true;
    }
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type == sched::task::TaskType::IDLE) {
        return true;
    }
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp)::"memory");
    if (stack_belongs_to_task(task, rsp)) {
        return true;
    }

    // User-mode interrupts enter on the CPU's TSS/RSP0 stack, which is not
    // owned by any Task. That path is safe to use for merely arming the local
    // APIC timer. The dangerous case is a real task stack owned by someone
    // other than current_task, which can happen during the handoff window after
    // current_task is changed but before the actual RSP switch.
    return sched::debug_find_task_by_kernel_stack(rsp) == nullptr;
}

auto defer_process_reschedule_to_syscall_exit() -> bool {
    if (!sched::can_query_current_task()) {
        return false;
    }
    if (power::shutdown_in_progress()) {
        return false;
    }

    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->deferred_task_switch || task->is_voluntary_blocked() ||
        task->wants_block) {
        return false;
    }

    // Same-CPU event wakes from syscalls only need to yield at the syscall
    // return boundary. Avoid arming a one-tick APIC timer while the process is
    // deep on its syscall stack; daemon hlt/yield paths still use the timer.
    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp)::"memory");
    if (!stack_belongs_to_task(task, rsp)) {
        return false;
    }

    task->yield_switch = true;
    task->deferred_task_switch = true;
    task->clear_wait_channel();
    return true;
}

[[noreturn]] void enter_idle_from_timer(sched::task::Task* task, const gates::InterruptFrame& frame) {
    uint64_t const STACK = task != nullptr ? task->context.syscall_kernel_stack : 0;
    if (!valid_kernel_stack(STACK)) {
        dbg::logger<"ctxswitch">::error("timer idle return bad rsp: pid=%lu frame_rsp=0x%llx stack=0x%llx", task != nullptr ? task->pid : 0,
                                        static_cast<unsigned long long>(frame.rsp), static_cast<unsigned long long>(STACK));
        hcf();
    }

    mm::virt::switch_to_kernel_pagemap();
    auto* scratch = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    scratch->cpu_id = cpu::current_cpu();
    cpu::wrgsbase(task->context.syscall_scratch_area);
    cpu_set_msr(IA32_KERNEL_GS_BASE, task->context.syscall_scratch_area);

    wos_enterIdleStack(STACK);
}
}  // namespace

auto can_request_local_reschedule() -> bool { return current_stack_allows_local_reschedule(); }

auto normalize_user_return_flags(uint64_t flags) -> uint64_t { return flags | USER_RFLAGS_REQUIRED_MASK; }

auto valid_user_return_flags(uint64_t flags) -> bool { return valid_user_rflags(flags); }

void normalize_process_user_return_state(sched::task::Task* task) {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->is_voluntary_blocked()) {
        return;
    }

    if (task->context.frame.cs == desc::gdt::GDT_USER_CS && task->context.frame.ss == desc::gdt::GDT_USER_DS &&
        user_rflags_have_fixed_bit(task->context.frame.flags)) {
        task->context.frame.flags = normalize_user_return_flags(task->context.frame.flags);
    }

    if (task->context.syscall_scratch_area == 0) {
        return;
    }

    auto* scratch = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    if (user_rflags_have_fixed_bit(scratch->syscall_ret_flags)) {
        scratch->syscall_ret_flags = normalize_user_return_flags(scratch->syscall_ret_flags);
    }
}

auto repair_stale_process_syscall_resume(sched::task::Task* task) -> bool {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->is_voluntary_blocked() ||
        task->context.syscall_scratch_area == 0 || task->deferred_task_switch || task->wants_block || task->wait_channel != nullptr) {
        return false;
    }

    bool const HAS_VALID_USER_FRAME = task->context.frame.cs == desc::gdt::GDT_USER_CS &&
                                      task->context.frame.ss == desc::gdt::GDT_USER_DS &&
                                      valid_user_resume_scalar(task->context.frame.rip) &&
                                      valid_user_resume_scalar(task->context.frame.rsp) && valid_user_rflags(task->context.frame.flags);
    if (HAS_VALID_USER_FRAME) {
        return false;
    }

    auto* scratch = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    if (scratch->syscall_stack != task->context.syscall_kernel_stack || !valid_kernel_stack(scratch->syscall_stack) ||
        !valid_user_resume_scalar(scratch->syscall_ret_rip) || !valid_user_resume_scalar(scratch->user_rsp) ||
        !user_rflags_have_fixed_bit(scratch->syscall_ret_flags)) {
        return false;
    }
    uint64_t const NORMALIZED_FLAGS = normalize_user_return_flags(scratch->syscall_ret_flags);

    uint64_t const REGS_ADDR = scratch->syscall_stack - sizeof(uint64_t) - sizeof(cpu::GPRegs);
    uint64_t const RETVAL_ADDR = scratch->syscall_stack - sizeof(uint64_t);
    if (!stack_region_contains(task, REGS_ADDR, sizeof(cpu::GPRegs)) || !stack_region_contains(task, RETVAL_ADDR, sizeof(uint64_t))) {
        return false;
    }

    uint64_t const OLD_CS = task->context.frame.cs;
    auto const* saved_regs = reinterpret_cast<const cpu::GPRegs*>(REGS_ADDR);
    auto const* return_value = reinterpret_cast<const uint64_t*>(RETVAL_ADDR);
    task->context.regs = *saved_regs;
    task->context.regs.rax = *return_value;
    task->context.regs.rcx = scratch->syscall_ret_rip;
    task->context.regs.r11 = NORMALIZED_FLAGS;

    task->context.frame.int_num = 0;
    task->context.frame.err_code = 0;
    task->context.frame.rip = scratch->syscall_ret_rip;
    task->context.frame.cs = desc::gdt::GDT_USER_CS;
    task->context.frame.flags = NORMALIZED_FLAGS;
    task->context.frame.rsp = scratch->user_rsp;
    task->context.frame.ss = desc::gdt::GDT_USER_DS;
    scratch->syscall_ret_flags = NORMALIZED_FLAGS;
    task->set_voluntary_blocked(false);

    static std::atomic<uint64_t> repair_count{0};
    uint64_t const N = repair_count.fetch_add(1, std::memory_order_relaxed);
    if (N < 16) {
        dbg::logger<"ctxswitch">::warn(
            "repaired process syscall resume frame: pid=%lu name=%s old_cs=0x%llx rip=0x%llx rsp=0x%llx retval=0x%llx", task->pid,
            task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(OLD_CS),
            static_cast<unsigned long long>(task->context.frame.rip), static_cast<unsigned long long>(task->context.frame.rsp),
            static_cast<unsigned long long>(*return_value));
    }

    return true;
}

#ifdef WOS_SELFTEST
auto context_switch_selftest_repair_stale_process_syscall_resume() -> bool {
    auto* stack = static_cast<uint8_t*>(mm::phys::kernel_stack_alloc("ctxswitch_stale_syscall_ktest"));
    if (stack == nullptr) {
        return false;
    }

    uint64_t const STACK_TOP = reinterpret_cast<uint64_t>(stack) + mm::KERNEL_STACK_SIZE;
    uint64_t const REGS_ADDR = STACK_TOP - sizeof(uint64_t) - sizeof(cpu::GPRegs);
    uint64_t const RETVAL_ADDR = STACK_TOP - sizeof(uint64_t);

    auto* saved_regs = reinterpret_cast<cpu::GPRegs*>(REGS_ADDR);
    *saved_regs = {};
    saved_regs->rdi = 0x1111;
    saved_regs->rsi = 0x2222;
    saved_regs->rax = 0x3333;

    auto* return_value = reinterpret_cast<uint64_t*>(RETVAL_ADDR);
    *return_value = 0x12345678ULL;

    cpu::PerCpu scratch{};
    scratch.syscall_stack = STACK_TOP;
    scratch.user_rsp = 0x00007fff'ffffe000ULL;
    scratch.syscall_ret_rip = 0x00000000'00401000ULL;
    scratch.syscall_ret_flags = 0x246;

    sched::task::Task task{};
    task.name = "ctxswitch-ktest";
    task.pid = 42;
    task.type = sched::task::TaskType::PROCESS;
    task.context.syscall_kernel_stack = STACK_TOP;
    task.context.syscall_scratch_area = reinterpret_cast<uint64_t>(&scratch);
    task.context.frame.cs = desc::gdt::GDT_KERN_CS;
    task.context.frame.ss = desc::gdt::GDT_KERN_DS;
    task.context.regs.rdi = 0xaaaa;
    task.set_voluntary_blocked(false);

    bool const STALE_REPAIRED = repair_stale_process_syscall_resume(&task);
    bool const STALE_OK = STALE_REPAIRED && task.context.frame.cs == desc::gdt::GDT_USER_CS &&
                          task.context.frame.ss == desc::gdt::GDT_USER_DS && task.context.frame.rip == scratch.syscall_ret_rip &&
                          task.context.frame.rsp == scratch.user_rsp && task.context.frame.flags == scratch.syscall_ret_flags &&
                          task.context.regs.rdi == saved_regs->rdi && task.context.regs.rsi == saved_regs->rsi &&
                          task.context.regs.rax == *return_value && task.context.regs.rcx == scratch.syscall_ret_rip &&
                          task.context.regs.r11 == scratch.syscall_ret_flags && !task.is_voluntary_blocked();

    saved_regs->rdi = 0x4444;
    saved_regs->rsi = 0x5555;
    saved_regs->rax = 0x6666;
    *return_value = 0x87654321ULL;
    scratch.user_rsp = 0x00007fff'ffffd000ULL;
    scratch.syscall_ret_rip = 0x00000000'00402000ULL;
    scratch.syscall_ret_flags = 0x246;

    task.context.frame.cs = desc::gdt::GDT_USER_CS;
    task.context.frame.ss = desc::gdt::GDT_USER_DS;
    task.context.frame.rip = 0;
    task.context.frame.rsp = scratch.user_rsp;
    task.context.frame.flags = scratch.syscall_ret_flags;
    task.context.regs.rdi = 0xbbbb;

    bool const INVALID_REPAIRED = repair_stale_process_syscall_resume(&task);
    bool const INVALID_OK = INVALID_REPAIRED && task.context.frame.rip == scratch.syscall_ret_rip &&
                            task.context.frame.rsp == scratch.user_rsp && task.context.regs.rdi == saved_regs->rdi &&
                            task.context.regs.rsi == saved_regs->rsi && task.context.regs.rax == *return_value &&
                            task.context.regs.rcx == scratch.syscall_ret_rip && task.context.regs.r11 == scratch.syscall_ret_flags &&
                            !task.is_voluntary_blocked();

    mm::phys::page_free(stack);
    return STALE_OK && INVALID_OK;
}
#endif

// Debug helper: update per-CPU task pointer for panic inspection
// NOTE: The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
// Now we just use the scheduler's getCurrentTask() instead of a separate debug array.
// This function is now a no-op since the scheduler tracks currentTask internally.
namespace {
inline void update_debug_task_ptr([[maybe_unused]] sched::task::Task* task, [[maybe_unused]] uint64_t cpu_id) {
    // No-op: scheduler's runQueues->thisCpu()->currentTask is the authoritative source
}

inline void kasan_unpoison_irq_save_area([[maybe_unused]] void* stack_ptr) {
#ifdef WOS_KASAN
    if (ker::mod::kasan::is_enabled()) {
        // Built by hardware/assembly, then patched by C++ before iretq.
        ker::mod::kasan::unpoison_range(stack_ptr, sizeof(cpu::GPRegs) + sizeof(gates::InterruptFrame));
    }
#endif
}
}  // namespace

namespace {

alignas(64) std::array<uint8_t, cpu::XSAVE_STATIC_AREA_SIZE> initial_fpu_state{};
std::atomic<int> initial_fpu_state_status{0};
std::array<std::atomic<sched::task::Task*>, desc::gdt::MAX_CPUS> fpu_owner{};
std::array<std::atomic<bool>, desc::gdt::MAX_CPUS> timer_fpu_restore_suppressed{};

auto cmdline_has_token(const char* cmdline, const char* token) -> bool;

auto timer_fpu_restore_slot() -> std::atomic<bool>* {
    uint64_t const CPU_ID = cpu::current_cpu();
    if (CPU_ID >= timer_fpu_restore_suppressed.size()) {
        return nullptr;
    }
    return &timer_fpu_restore_suppressed[static_cast<size_t>(CPU_ID)];
}

void set_timer_fpu_restore_suppressed(bool suppressed) {
    auto* slot = timer_fpu_restore_slot();
    if (slot == nullptr) {
        return;
    }
    slot->store(suppressed, std::memory_order_release);
}

auto consume_timer_fpu_restore_suppressed() -> bool {
    auto* slot = timer_fpu_restore_slot();
    if (slot == nullptr) {
        return false;
    }
    return slot->exchange(false, std::memory_order_acq_rel);
}

auto local_fpu_owner_slot() -> std::atomic<sched::task::Task*>* {
    uint64_t const CPU_ID = cpu::current_cpu();
    if (CPU_ID >= fpu_owner.size()) {
        return nullptr;
    }
    return &fpu_owner[static_cast<size_t>(CPU_ID)];
}

auto local_fpu_owner() -> sched::task::Task* {
    auto* slot = local_fpu_owner_slot();
    if (slot == nullptr) {
        return nullptr;
    }
    return slot->load(std::memory_order_acquire);
}

void set_local_fpu_owner(sched::task::Task* task) {
    auto* slot = local_fpu_owner_slot();
    if (slot == nullptr) {
        return;
    }

    if (task != nullptr) {
        for (auto& owner_slot : fpu_owner) {
            if (&owner_slot == slot) {
                continue;
            }
            sched::task::Task* expected = task;
            static_cast<void>(owner_slot.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel, std::memory_order_acquire));
        }
    }

    slot->store(task, std::memory_order_release);
}

void forget_fpu_owner(sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    for (auto& slot : fpu_owner) {
        sched::task::Task* expected = task;
        static_cast<void>(slot.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel, std::memory_order_acquire));
    }
}

auto initial_fpu_state_buffer() -> const uint8_t* {
    int expected = 0;
    if (initial_fpu_state_status.compare_exchange_strong(expected, 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
        initial_fpu_state.fill(0);

        // FXSAVE legacy-region defaults. For XRSTOR, XSTATE_BV below names only
        // x87/SSE as explicit; other enabled components are restored to their
        // architectural initial state.
        *reinterpret_cast<uint16_t*>(initial_fpu_state.data()) = 0x037F;
        *reinterpret_cast<uint32_t*>(initial_fpu_state.data() + 24) = 0x1F80;
        *reinterpret_cast<uint32_t*>(initial_fpu_state.data() + 28) = 0xFFFF;
        *reinterpret_cast<uint64_t*>(initial_fpu_state.data() + 512) = cpu::XSAVE_LEGACY_MASK;

        initial_fpu_state_status.store(2, std::memory_order_release);
    } else {
        while (initial_fpu_state_status.load(std::memory_order_acquire) != 2) {
            asm volatile("pause" ::: "memory");
        }
    }

    return initial_fpu_state.data();
}

// NOLINTNEXTLINE(readability-non-const-parameter): xsave writes through this pointer.
inline void xsave_to_buffer(uint8_t* buf) {
    uint64_t const MASK = cpu::xsave_feature_mask;
    auto const EAX = static_cast<uint32_t>(MASK);
    auto const EDX = static_cast<uint32_t>(MASK >> 32U);
    asm volatile("xsave64 (%0)" : : "r"(buf), "a"(EAX), "d"(EDX) : "memory");
}

inline void xrstor_from_buffer(const uint8_t* buf) {
    uint64_t const MASK = cpu::xsave_feature_mask;
    auto const EAX = static_cast<uint32_t>(MASK);
    auto const EDX = static_cast<uint32_t>(MASK >> 32U);
    asm volatile("xrstor64 (%0)" : : "r"(buf), "a"(EAX), "d"(EDX) : "memory");
}

void save_live_fpu_state(sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    // NOLINTNEXTLINE(misc-const-correctness)
    auto* buf = task->fx_state.aligned();
    if (cpu::xsave_feature_mask != 0) {
        xsave_to_buffer(buf);
    } else {
        asm volatile("fxsave64 (%0)" : : "r"(buf) : "memory");
    }
    task->fx_state.saved = true;
    task->fx_state.live_saved = true;
    task->fx_state.initialized = true;
    set_local_fpu_owner(nullptr);
}

}  // namespace

// Save FPU/SSE/AVX state of a task. Uses xsave if available, fxsave otherwise.
// The memory operand must be 64-byte aligned for xsave, 16-byte for fxsave.
// fx_state::aligned() guarantees 64-byte alignment regardless of Task placement.
void save_fpu_state(sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    if (task->fx_state.live_saved && local_fpu_owner() != task) {
        return;
    }

    save_live_fpu_state(task);
}

// Restore FPU/SSE/AVX state of a task. Uses xrstor if available, fxrstor otherwise.
void restore_fpu_state(sched::task::Task* task) {
    // NOLINTNEXTLINE(misc-const-correctness)
    auto* buf = task->fx_state.aligned();
    if (cpu::xsave_feature_mask != 0) {
        xrstor_from_buffer(buf);
    } else {
        asm volatile("fxrstor64 (%0)" : : "r"(buf) : "memory");
    }
    set_local_fpu_owner(task);
    task->fx_state.live_saved = false;
    task->fx_state.initialized = true;
}

void restore_or_init_fpu_state(sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    if (task->fx_state.saved) {
        restore_fpu_state(task);
        return;
    }

    const uint8_t* const BUF = initial_fpu_state_buffer();
    if (cpu::xsave_feature_mask != 0) {
        xrstor_from_buffer(BUF);
    } else {
        asm volatile("fxrstor64 (%0)" : : "r"(BUF) : "memory");
    }
    set_local_fpu_owner(task);
    task->fx_state.live_saved = false;
    task->fx_state.initialized = true;
}

void reset_fpu_state(sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    task->fx_state.saved = false;
    task->fx_state.live_saved = false;
    task->fx_state.initialized = false;
    forget_fpu_owner(task);
    restore_or_init_fpu_state(task);
}

extern "C" void wos_syscall_save_current_fpu() {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }

    save_live_fpu_state(task);
}

extern "C" void wos_syscall_restore_current_fpu() {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }

    restore_or_init_fpu_state(task);
}

extern "C" auto wos_user_interrupt_save_fpu() -> sched::task::Task* {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return nullptr;
    }

    save_live_fpu_state(task);
    return task;
}

extern "C" void wos_user_interrupt_restore_fpu(sched::task::Task* saved_task) {
    if (saved_task == nullptr || saved_task->type != sched::task::TaskType::PROCESS) {
        return;
    }
    if (sched::get_return_task() != saved_task) {
        return;
    }

    restore_or_init_fpu_state(saved_task);
}

extern "C" void wos_restore_return_task_fpu() {
    if (consume_timer_fpu_restore_suppressed()) {
        return;
    }

    auto* task = sched::get_return_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }
    if (local_fpu_owner() == task && !task->fx_state.live_saved) {
        return;
    }

    restore_or_init_fpu_state(task);
}

namespace {
struct DebugRegisterCpuState {
    bool initialized{false};
    bool armed{false};
};

std::array<DebugRegisterCpuState, desc::gdt::MAX_CPUS> debug_register_state{};

inline void clear_debug_registers() {
    uint64_t constexpr DR7_CLEAR = 0x400;
    uint64_t constexpr DR6_CLEAR = 0xFFFF0FF0;
    asm volatile("mov %0, %%dr7" ::"r"(DR7_CLEAR) : "memory");
    asm volatile("mov %0, %%dr6" ::"r"(DR6_CLEAR) : "memory");
}

inline auto task_has_hardware_debug_state(const sched::task::Task* task) -> bool {
    return task != nullptr && task->ptrace_traced && task->ptrace_dr7 != 0;
}

__attribute__((noinline, no_sanitize("address", "undefined", "coverage"))) void restore_debug_registers(sched::task::Task* task) {
    bool const NEEDS_DEBUG_REGISTERS = task_has_hardware_debug_state(task);
    uint64_t const CPU_ID = cpu::current_cpu();

    if (CPU_ID >= debug_register_state.size()) {
        clear_debug_registers();
        if (!NEEDS_DEBUG_REGISTERS) {
            return;
        }
    } else {
        auto& state = debug_register_state[static_cast<size_t>(CPU_ID)];
        if (!state.initialized) {
            clear_debug_registers();
            state.initialized = true;
            state.armed = false;
        }

        if (!NEEDS_DEBUG_REGISTERS) {
            if (state.armed) {
                clear_debug_registers();
                state.armed = false;
            }
            return;
        }

        // Disable breakpoints while rewriting DR0-DR3 so stale slots cannot
        // fire for the incoming traced task during partial register restore.
        clear_debug_registers();
        state.armed = true;
    }

    asm volatile("mov %0, %%dr0" ::"r"(task->ptrace_dr_addr.at(0)) : "memory");
    asm volatile("mov %0, %%dr1" ::"r"(task->ptrace_dr_addr.at(1)) : "memory");
    asm volatile("mov %0, %%dr2" ::"r"(task->ptrace_dr_addr.at(2)) : "memory");
    asm volatile("mov %0, %%dr3" ::"r"(task->ptrace_dr_addr.at(3)) : "memory");
    asm volatile("mov %0, %%dr7" ::"r"(task->ptrace_dr7) : "memory");
}
}  // namespace

void restore_debug_registers_for_task(sched::task::Task* task) { restore_debug_registers(task); }

void install_task_cpu_bases(sched::task::Task* next_task, uint64_t cpu_id) {
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(next_task->context.syscall_scratch_area);
    scratch_area->cpu_id = cpu_id;

    // These bases are part of the syscall/interrupt ABI.  Do not infer the
    // actual CPU state from current_task here: entry/return assembly can change
    // GS state independently of scheduler metadata.
    cpu::wrgsbase(next_task->context.syscall_scratch_area);
    if (next_task->thread != nullptr) {
        cpu_set_msr(IA32_KERNEL_GS_BASE, next_task->thread->gsbase);
        cpu::wrfsbase(next_task->thread->fsbase);
    } else {
        cpu_set_msr(IA32_KERNEL_GS_BASE, next_task->context.syscall_scratch_area);
    }
}

auto switch_to(cpu::GPRegs& gpr, gates::InterruptFrame& frame, sched::task::Task* next_task) -> bool {
    // CRITICAL: We must do ALL validation BEFORE modifying any state.
    // If we modify gpr/frame but fail to switch pagemap, the iretq will use
    // the new task's RIP/RSP with the wrong pagemap - causing a crash.

    // First state check - bail out immediately if task is not ACTIVE
    if (next_task->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL early - PID %x not ACTIVE (state=%d)", next_task->pid,
                 static_cast<int>(next_task->state.load(std::memory_order_relaxed)));
        return false;
    }

    // Capture all values we need from the task BEFORE modifying anything.
    // This creates a consistent snapshot even if task starts exiting mid-switch.
    // Note: thread and pagemap pointers could become stale, but we defer their
    // freeing to GC so they remain valid during this epoch critical section.

    // Validate task has required resources for a user task
    // IDLE and DAEMON tasks use the kernel pagemap and have no user thread
    if (next_task->type == sched::task::TaskType::PROCESS) {
        // User process must have pagemap
        if (next_task->pagemap == nullptr) {
            dbg::log("switchTo: FAIL - PID %x pagemap==nullptr", next_task->pid);
            // Task is being torn down, don't switch to it
            return false;
        }

        // Validate pagemap pointer is in valid HHDM range (not kernel static range)
        auto pm_addr = reinterpret_cast<uintptr_t>(next_task->pagemap);
        if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
            dbg::log("switchTo: FAIL - PID %x pagemap ptr invalid (pmAddr=0x%x)", next_task->pid, pm_addr);
            // Corrupted pagemap pointer - task struct was freed/corrupted
            return false;
        }

        // Validate thread pointer if present
        if (next_task->thread != nullptr) {
            auto thread_addr = reinterpret_cast<uintptr_t>(next_task->thread);
            // Accept either HHDM or kernel-static addresses as valid
            bool const TH_IN_HHDM = (thread_addr >= 0xffff800000000000ULL && thread_addr < 0xffff900000000000ULL);
            bool const TH_IN_KERNEL_STATIC = (thread_addr >= 0xffffffff80000000ULL && thread_addr < 0xffffffffc0000000ULL);
            if (!TH_IN_HHDM && !TH_IN_KERNEL_STATIC) {
                dbg::log("switchTo: FAIL - PID %x thread ptr invalid (thread=0x%lx)", next_task->pid, thread_addr);
                // Corrupted thread pointer - task struct was freed/corrupted
                return false;
            }
        }
    }

    // Second state check after reading resources - task might have started exiting
    if (next_task->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL 2nd check - PID %x changed state (state=%d)", next_task->pid,
                 static_cast<int>(next_task->state.load(std::memory_order_relaxed)));
        return false;
    }

    if (!valid_kernel_stack(next_task->context.syscall_kernel_stack)) {
        dbg::log("switchTo: FAIL - PID %x invalid kernel stack 0x%lx", next_task->pid, next_task->context.syscall_kernel_stack);
        return false;
    }

    // === POINT OF NO RETURN ===
    // After this point, we MUST complete the context switch.
    // The epoch guard in processTasks ensures the task struct and its resources
    // (thread, pagemap) won't be freed until after we exit the critical section.

    // Get real CPU ID from GS (no MSR / VM exit)
    uint64_t const REAL_CPU_ID = cpu::current_cpu();

    // Update debug task pointer for panic inspection
    update_debug_task_ptr(next_task, REAL_CPU_ID);
    desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);

    // Now safe to modify interrupt frame and registers.
    if (next_task->type == sched::task::TaskType::DAEMON && !valid_kernel_stack(next_task->context.frame.rsp) &&
        valid_kernel_stack(next_task->context.syscall_kernel_stack)) {
        dbg::logger<"ctxswitch">::warn("repairing daemon rsp before switch: pid=%lu name=%s frame_rsp=0x%llx stack=0x%llx", next_task->pid,
                                       next_task->name != nullptr ? next_task->name : "?",
                                       static_cast<unsigned long long>(next_task->context.frame.rsp),
                                       static_cast<unsigned long long>(next_task->context.syscall_kernel_stack));
        next_task->context.frame.rsp = next_task->context.syscall_kernel_stack;
    }

    // The live GPRegs block sits immediately before the live InterruptFrame on
    // the timer stack. Copy registers first so the return frame is the last
    // thing written before validation and return assembly consumes it.
    if (next_task->type == sched::task::TaskType::PROCESS && !next_task->is_voluntary_blocked()) {
        static_cast<void>(repair_stale_process_syscall_resume(next_task));
        static_cast<void>(sys::signal::restore_deferred_sigreturn(next_task));
        normalize_process_user_return_state(next_task);
    }
    gpr = next_task->context.regs;

    frame.rip = next_task->context.frame.rip;
    frame.rsp = next_task->context.frame.rsp;
    frame.cs = next_task->context.frame.cs;
    frame.ss = next_task->context.frame.ss;
    frame.flags = next_task->context.frame.flags;
    validate_kernel_frame(frame, next_task, "switchTo");

    // Validate context before restoring - catch corruption before it causes a crash
    // in userspace where debugging is much harder.
    // Only validate user-mode context for PROCESS tasks (IDLE/DAEMON run in ring 0)
    // Skip validation when voluntary_block is set - the saved context is legitimately
    // kernel-mode (task was preempted at a safe blocking point like sti;hlt in a syscall).
    if (next_task->type == sched::task::TaskType::PROCESS && !next_task->is_voluntary_blocked()) {
        if (frame.cs != desc::gdt::GDT_USER_CS) {
            dbg::log("switchTo: CORRUPT cs=0x%x (expected 0x%x) PID %x", frame.cs, desc::gdt::GDT_USER_CS, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.ss != desc::gdt::GDT_USER_DS) {
            dbg::log("switchTo: CORRUPT ss=0x%x (expected 0x%x) PID %x", frame.ss, desc::gdt::GDT_USER_DS, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.rip >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rip=0x%x (kernel addr?) PID %x", frame.rip, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.rsp >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rsp=0x%x (kernel addr?) PID %x", frame.rsp, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (!valid_user_rflags(frame.flags)) {
            dbg::log("switchTo: CORRUPT flags=0x%x PID %x", frame.flags, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
    }

    install_task_cpu_bases(next_task, REAL_CPU_ID);

    // Switch pagemap for user tasks.  Keep the unconditional CR3 reload inside
    // mm::virt::switch_pagemap(): it also refreshes kernel mappings and owns
    // the stale-TLB/address-space-reuse guard.
    if (next_task->pagemap != nullptr) {
        mm::virt::switch_pagemap(next_task);
    }

    // FPU/SIMD state is restored only at the final userspace boundary.  A
    // process can be resumed here into a kernel syscall/blocking frame; making
    // its user FPU image live before that frame reaches userspace lets a later
    // kernel-mode preemption overwrite the hardware state without saving it.
    restore_debug_registers_for_task(next_task);

    return true;
}

namespace {
long timer_quantum;
constexpr uint64_t SCHED_TIMER_QUANTUM_US = 4000;
uint64_t timer_quantum_us = SCHED_TIMER_QUANTUM_US;
constexpr uint64_t APIC_TIMER_MAX_COUNT = 0xFFFFFFFFULL;

// Tick counter for periodic epoch advancement and garbage collection
std::atomic<uint64_t> timer_tick_count{0};

constexpr bool K_ENABLE_SCHED_HOT_LOGGING = false;

constexpr auto TIMER_FRAME_USER_CS = desc::gdt::GDT_USER_CS;
constexpr auto TIMER_FRAME_KERNEL_CS = desc::gdt::GDT_KERN_CS;

[[maybe_unused]]
constexpr uint64_t HOT_TASK_STREAK_TICKS = 250;
[[maybe_unused]]
constexpr uint64_t SCHED_CPU_DUMP_PERIOD_TICKS = 1000;
constexpr uint64_t SCHED_CPU_LOCAL_DUMP_PERIOD_TICKS = 1000;

struct HotTaskTracker {
    uint64_t last_pid{0};
    uint64_t streak{0};
};

[[maybe_unused]]
std::array<HotTaskTracker, 16> hot_task_trackers;

std::atomic<int> sched_cpu_dump_enabled{-1};
std::atomic<int> sched_cpu_local_dump_enabled{-1};
std::atomic<int> sched_timer_bypass_inflate_enabled{-1};
std::array<std::atomic<uint64_t>, desc::gdt::MAX_CPUS> sched_cpu_local_dump_ticks{};

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') {
            cursor++;
        }

        const char* const START = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\n') {
            cursor++;
        }

        auto const ARG_LEN = static_cast<size_t>(cursor - START);
        if (ARG_LEN == TOKEN_LEN && std::memcmp(START, token, TOKEN_LEN) == 0) {
            return true;
        }
    }

    return false;
}

auto cmdline_token_uint_value(const char* cmdline, const char* token, uint64_t& out_value) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') {
            cursor++;
        }

        const char* const START = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\n') {
            cursor++;
        }

        auto const ARG_LEN = static_cast<size_t>(cursor - START);
        if (ARG_LEN <= TOKEN_LEN || std::memcmp(START, token, TOKEN_LEN) != 0 || START[TOKEN_LEN] != '=') {
            continue;
        }

        uint64_t value = 0;
        bool any = false;
        for (const char* p = START + TOKEN_LEN + 1; p < cursor; ++p) {
            if (*p < '0' || *p > '9') {
                return false;
            }
            any = true;
            value = (value * 10U) + static_cast<uint64_t>(*p - '0');
        }
        if (!any) {
            return false;
        }
        out_value = value;
        return true;
    }

    return false;
}

auto scheduler_cpu_dump_enabled() -> bool {
    int const CACHED = sched_cpu_dump_enabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "sched.cpu_dump");
    sched_cpu_dump_enabled.store(ENABLED ? 1 : 0, std::memory_order_release);
    return ENABLED;
}

auto scheduler_cpu_local_dump_enabled() -> bool {
    int const CACHED = sched_cpu_local_dump_enabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "sched.cpu_dump_local");
    sched_cpu_local_dump_enabled.store(ENABLED ? 1 : 0, std::memory_order_release);
    return ENABLED;
}

auto scheduler_timer_bypass_inflate_enabled() -> bool {
    int const CACHED = sched_timer_bypass_inflate_enabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "sched.timer_bypass_inflate");
    sched_timer_bypass_inflate_enabled.store(ENABLED ? 1 : 0, std::memory_order_release);
    return ENABLED;
}

auto task_name_looks_like_inflate_diag(const sched::task::Task* task) -> bool {
    if (task == nullptr || task->name == nullptr) {
        return false;
    }

    char const* name = task->name;
    bool const INFLATE_PREFIX = name[0] == 'i' && name[1] == 'n' && name[2] == 'f' && name[3] == 'l' && name[4] == 'a' && name[5] == 't' &&
                                name[6] == 'e' && name[7] == '_';
    bool const WOS_INFLATE_PREFIX = name[0] == 'w' && name[1] == 'o' && name[2] == 's' && name[3] == '_' && name[4] == 'i' &&
                                    name[5] == 'n' && name[6] == 'f' && name[7] == 'l' && name[8] == 'a' && name[9] == 't' &&
                                    name[10] == 'e';
    return INFLATE_PREFIX || WOS_INFLATE_PREFIX;
}

auto scheduler_timer_should_bypass_for_inflate_diag(const gates::InterruptFrame& frame) -> bool {
    if (!scheduler_timer_bypass_inflate_enabled() || !is_user_return_frame(frame)) {
        return false;
    }
    return task_name_looks_like_inflate_diag(sched::get_current_task());
}

[[maybe_unused]]
inline auto hot_task_tracker_slot(uint64_t cpu_no) -> HotTaskTracker& {
    // CPU_NO is checked against hot_task_trackers.size() before access.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return hot_task_trackers[static_cast<size_t>(cpu_no)];
}

auto scheduler_timer_ticks_for_delta_us(uint64_t delta_us) -> uint64_t {
    if (delta_us == 0 || timer_quantum <= 0) {
        return 1;
    }

    auto const QUANTUM_TICKS = static_cast<uint64_t>(timer_quantum);
    uint64_t const MAX_DELTA_US = (APIC_TIMER_MAX_COUNT / QUANTUM_TICKS) * timer_quantum_us;
    if (MAX_DELTA_US == 0) {
        return APIC_TIMER_MAX_COUNT;
    }
    uint64_t const CLAMPED_DELTA_US = (MAX_DELTA_US != 0 && delta_us > MAX_DELTA_US) ? MAX_DELTA_US : delta_us;
    uint64_t ticks = ((QUANTUM_TICKS * CLAMPED_DELTA_US) + (timer_quantum_us - 1)) / timer_quantum_us;
    if (ticks == 0) {
        ticks = 1;
    }
    return std::min(ticks, APIC_TIMER_MAX_COUNT);
}
}  // namespace

extern "C" __attribute__((no_sanitize("address", "undefined", "coverage"))) void wos_repair_timer_return_frame(void* stack_ptr) {
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = stack_ptr != nullptr
                          ? reinterpret_cast<gates::InterruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs))
                          : nullptr;
    auto* task = sched::get_current_task();

    uint64_t const BAD_CS = frame_ptr != nullptr ? frame_ptr->cs : 0;
    static std::atomic<uint64_t> repair_count{0};

    if (task != nullptr && gpr_ptr != nullptr && frame_ptr != nullptr &&
        (task->context.frame.cs == TIMER_FRAME_USER_CS || task->context.frame.cs == TIMER_FRAME_KERNEL_CS)) {
        repair_count.fetch_add(1, std::memory_order_relaxed);
        *gpr_ptr = task->context.regs;
        *frame_ptr = task->context.frame;
        return;
    }

    dbg::logger<"ctxswitch">::error(
        "bad timer return frame: cs=0x%llx rip=0x%llx rsp=0x%llx task=%p pid=%lu saved_cs=0x%llx saved_rip=0x%llx saved_rsp=0x%llx",
        static_cast<unsigned long long>(BAD_CS), frame_ptr != nullptr ? static_cast<unsigned long long>(frame_ptr->rip) : 0ULL,
        frame_ptr != nullptr ? static_cast<unsigned long long>(frame_ptr->rsp) : 0ULL, static_cast<void*>(task),
        task != nullptr ? task->pid : 0, task != nullptr ? static_cast<unsigned long long>(task->context.frame.cs) : 0ULL,
        task != nullptr ? static_cast<unsigned long long>(task->context.frame.rip) : 0ULL,
        task != nullptr ? static_cast<unsigned long long>(task->context.frame.rsp) : 0ULL);
    hcf();
}

extern "C" __attribute__((no_sanitize("address", "undefined", "coverage"))) void wos_validate_deferred_return_frame(
    cpu::GPRegs* gpr_ptr, gates::InterruptFrame* frame_ptr) {
    auto* task = sched::get_return_task();
    if (frame_ptr == nullptr) {
        dbg::logger<"ctxswitch">::error("deferred return without frame: task=%p pid=%lu", static_cast<void*>(task),
                                        task != nullptr ? task->pid : 0);
        hcf();
    }

    if (frame_ptr->cs == desc::gdt::GDT_USER_CS) {
        if (gpr_ptr != nullptr) {
            debug_validate_clang_return("deferred-final", task, *gpr_ptr, *frame_ptr);
        }
        validate_user_frame(*frame_ptr, task, "deferred-final");
        return;
    }
    if (frame_ptr->cs == desc::gdt::GDT_KERN_CS) {
        validate_kernel_frame(*frame_ptr, task, "deferred-final");
        return;
    }

    dbg::logger<"ctxswitch">::error(
        "bad deferred return selector: pid=%lu name=%s cs=0x%llx ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx task=%p",
        task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
        static_cast<unsigned long long>(frame_ptr->cs), static_cast<unsigned long long>(frame_ptr->ss),
        static_cast<unsigned long long>(frame_ptr->rip), static_cast<unsigned long long>(frame_ptr->rsp),
        static_cast<unsigned long long>(frame_ptr->flags), static_cast<void*>(task));
    hcf();
}

extern "C" __attribute__((no_sanitize("address", "undefined", "coverage"))) void wos_validate_kernel_thread_start(uint64_t stack_top,
                                                                                                                  void (*entry)()) {
    auto* task = sched::get_return_task();
    const bool STACK_BAD = !valid_kernel_stack(stack_top);
    const bool ENTRY_BAD = entry == nullptr || !is_kernel_text_pointer(reinterpret_cast<uint64_t>(entry));
    const bool TASK_BAD = task == nullptr || task->type != sched::task::TaskType::DAEMON;
    const bool OWNER_BAD = task != nullptr && stack_top != task->context.syscall_kernel_stack;
    if (!STACK_BAD && !ENTRY_BAD && !TASK_BAD && !OWNER_BAD) {
        return;
    }

    dbg::logger<"ctxswitch">::error(
        "bad kernel thread start: pid=%lu name=%s stack=0x%llx task_stack=0x%llx entry=%p task=%p type=%u", task != nullptr ? task->pid : 0,
        (task != nullptr && task->name != nullptr) ? task->name : "?", static_cast<unsigned long long>(stack_top),
        task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL, reinterpret_cast<void*>(entry),
        static_cast<void*>(task), task != nullptr ? static_cast<unsigned>(task->type) : 0U);
    hcf();
}

extern "C" __attribute__((no_sanitize("address", "undefined", "coverage"))) auto wos_user_handoff_stack_top() -> uint64_t {
    auto* return_task = sched::get_return_task();
    auto* current_task = sched::get_current_task();
    if (return_task == nullptr || return_task == current_task) {
        return 0;
    }
    if (return_task->type != sched::task::TaskType::PROCESS || return_task->pagemap == nullptr || return_task->thread == nullptr) {
        panic_bad_handoff_stack(return_task, "bad-return-task");
    }

    uint64_t const STACK_TOP = return_task->context.syscall_kernel_stack;
    if (!valid_kernel_stack(STACK_TOP)) {
        panic_bad_handoff_stack(return_task, "bad-stack-top");
    }
    if (current_task != nullptr && STACK_TOP == current_task->context.syscall_kernel_stack) {
        return 0;
    }
    return STACK_TOP;
}

#ifdef SCHED_DEBUG
// Per-CPU interrupt timing stats (indexed by CPU id, max 16 CPUs)
struct IrqStats {
    std::atomic<uint64_t> total_ns{0};      // total ns spent inside wos_sched_timer
    std::atomic<uint64_t> count{0};         // number of timer interrupts
    std::atomic<uint64_t> ctx_switches{0};  // number of context switches performed
    std::atomic<uint64_t> xsave_ns{0};      // total ns spent in save_fpu_state
};
static std::array<IrqStats, 16> irqStats;

void dump_irq_stats() {
    uint64_t grand_total_ns = 0;
    uint64_t grand_count = 0;
    uint64_t grand_switches = 0;
    uint64_t grand_xsave_ns = 0;
    for (uint64_t i = 0; i < 16; i++) {
        uint64_t ns = irqStats[i].total_ns.load(std::memory_order_relaxed);
        uint64_t cnt = irqStats[i].count.load(std::memory_order_relaxed);
        uint64_t sw = irqStats[i].ctx_switches.load(std::memory_order_relaxed);
        uint64_t xns = irqStats[i].xsave_ns.load(std::memory_order_relaxed);
        if (cnt == 0) continue;
        dbg::log("irqstats: cpu%lu ticks=%lu avg_ns=%lu switches=%lu xsave_ms=%lu", static_cast<unsigned long>(i),
                 static_cast<unsigned long>(cnt), static_cast<unsigned long>(ns / cnt), static_cast<unsigned long>(sw),
                 static_cast<unsigned long>(xns / 1000000ULL));
        grand_total_ns += ns;
        grand_count += cnt;
        grand_switches += sw;
        grand_xsave_ns += xns;
    }
    if (grand_count > 0) {
        dbg::log("irqstats: TOTAL ticks=%lu switches=%lu total_irq_ms=%lu xsave_ms=%lu avg_ns=%lu", static_cast<unsigned long>(grand_count),
                 static_cast<unsigned long>(grand_switches), static_cast<unsigned long>(grand_total_ns / 1000000ULL),
                 static_cast<unsigned long>(grand_xsave_ns / 1000000ULL), static_cast<unsigned long>(grand_total_ns / grand_count));
    }
}
#endif  // SCHED_DEBUG

extern "C" void wos_sched_timer(void* stack_ptr) {
    [[maybe_unused]] uint64_t const IRQ_ACCOUNT_STARTED_US = K_ENABLE_SCHED_HOT_LOGGING ? ker::mod::time::get_us() : 0;
    apic::eoi();
    sched::note_scheduler_timer_interrupt();
    mm::virt::service_pending_tlb_shootdowns();
    set_timer_fpu_restore_suppressed(false);

    // Advance epoch and request garbage collection periodically on CPU 0 only.
    // Task reclamation itself runs in the scheduler GC daemon, never in IRQ.
    uint64_t const TICKS = timer_tick_count.fetch_add(1, std::memory_order_relaxed);
    if (cpu::current_cpu() == 0 && (TICKS % 10) == 0) {
        sched::EpochManager::advance_epoch();
        sched::request_gc();
    }

    kasan_unpoison_irq_save_area(stack_ptr);

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::InterruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));
    validate_timer_stack(*frame_ptr, *gpr_ptr, sched::get_current_task());
    if (scheduler_timer_should_bypass_for_inflate_diag(*frame_ptr)) {
        sched::note_scheduler_timer_arm();
        apic::one_shot_timer(timer_quantum);
        return;
    }
    auto* const interrupted_task = is_user_return_frame(*frame_ptr) ? sched::get_current_task() : nullptr;
    bool const INTERRUPTED_USER_PROCESS = interrupted_task != nullptr && interrupted_task->type == sched::task::TaskType::PROCESS;

#ifdef SCHED_DEBUG
    uint64_t t0 = rdtsc();
    auto* task_before = sched::get_current_task();
#endif

    sched::process_tasks(*gpr_ptr, *frame_ptr);

    check_pending_signals_for_return(*gpr_ptr, *frame_ptr);
    auto* return_task = sched::get_return_task();
    debug_validate_clang_return("timer", return_task, *gpr_ptr, *frame_ptr);
    validate_kernel_frame(*frame_ptr, return_task, "timer-return");
    if (INTERRUPTED_USER_PROCESS && is_user_return_frame(*frame_ptr) && return_task == interrupted_task &&
        local_fpu_owner() == return_task) {
        set_timer_fpu_restore_suppressed(true);
    }

    if (is_idle_return_frame(*frame_ptr, return_task)) {
        sched::arm_idle_timer_for_this_cpu();
        enter_idle_from_timer(return_task, *frame_ptr);
    }

    auto const TIMER_DECISION = sched::get_scheduler_timer_decision_for_this_cpu(ker::mod::time::get_us());
    if (TIMER_DECISION.arm) {
        sched::note_scheduler_timer_arm();
        uint64_t const TICKS = TIMER_DECISION.use_deadline_delta ? scheduler_timer_ticks_for_delta_us(TIMER_DECISION.deadline_delta_us)
                                                                 : static_cast<uint64_t>(timer_quantum);
        apic::one_shot_timer(TICKS);
    } else {
        sched::note_scheduler_timer_disarm();
    }

    if constexpr (K_ENABLE_SCHED_HOT_LOGGING) {
        uint64_t const CPU_NO = cpu::current_cpu();
        if (CPU_NO < hot_task_trackers.size()) {
            auto& tracker = hot_task_tracker_slot(CPU_NO);
            uint64_t const CURRENT_PID = return_task != nullptr ? return_task->pid : 0;

            if (CURRENT_PID != 0 && CURRENT_PID == tracker.last_pid) {
                tracker.streak++;
            } else {
                tracker.last_pid = CURRENT_PID;
                tracker.streak = CURRENT_PID != 0 ? 1 : 0;
            }

            if (return_task != nullptr && return_task->type != sched::task::TaskType::IDLE && tracker.streak != 0 &&
                (tracker.streak % HOT_TASK_STREAK_TICKS) == 0) {
                auto stats = sched::get_run_queue_stats(CPU_NO);
                dbg::log("schedhot: cpu%lu pid=%lu(%s) streak=%lu type=%u vblk=%u wblk=%u pinned=%u runq=%lu waitq=%lu cs=0x%x",
                         static_cast<unsigned long>(CPU_NO), static_cast<unsigned long>(return_task->pid),
                         return_task->name != nullptr ? return_task->name : "?", static_cast<unsigned long>(tracker.streak),
                         static_cast<unsigned>(return_task->type), return_task->is_voluntary_blocked() ? 1U : 0U,
                         return_task->wants_block ? 1U : 0U, return_task->cpu_pinned ? 1U : 0U,
                         static_cast<unsigned long>(stats.active_task_count), static_cast<unsigned long>(stats.wait_queue_count),
                         static_cast<unsigned>(frame_ptr->cs));
            }

            uint64_t const IRQ_ACCOUNT_FINISHED_US = ker::mod::time::get_us();
            sched::account_irq_time_us(IRQ_ACCOUNT_FINISHED_US >= IRQ_ACCOUNT_STARTED_US ? IRQ_ACCOUNT_FINISHED_US - IRQ_ACCOUNT_STARTED_US
                                                                                         : 0);
        }
    }

    if (scheduler_cpu_dump_enabled() && cpu::current_cpu() == 0 &&
        (TICKS % SCHED_CPU_DUMP_PERIOD_TICKS) == (SCHED_CPU_DUMP_PERIOD_TICKS - 1)) {
        sched::dump_scheduler_cpu_states();
    }
    if (scheduler_cpu_local_dump_enabled()) {
        uint64_t const CPU_NO = cpu::current_cpu();
        if (CPU_NO < sched_cpu_local_dump_ticks.size()) {
            uint64_t const LOCAL_TICK = sched_cpu_local_dump_ticks[CPU_NO].fetch_add(1, std::memory_order_relaxed) + 1;
            if ((LOCAL_TICK % SCHED_CPU_LOCAL_DUMP_PERIOD_TICKS) == 0) {
                sched::dump_scheduler_current_cpu_state(*frame_ptr);
            }
        }
    }

#ifdef SCHED_DEBUG
    auto* task_after = sched::get_current_task();
    uint64_t elapsed_ns = tsc::ticks_to_ns(rdtsc() - t0);
    uint64_t cpu = CPU_NO;
    if (cpu < 16) {
        irqStats[cpu].total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        irqStats[cpu].count.fetch_add(1, std::memory_order_relaxed);
        if (task_before != task_after) {
            irqStats[cpu].ctx_switches.fetch_add(1, std::memory_order_relaxed);
        }
    }

    static std::atomic<uint64_t> switch_log_count{0};
    if (cpu == 0 && task_before != task_after && TICKS > 25000) {
        uint64_t n = switch_log_count.fetch_add(1, std::memory_order_relaxed);
        if (n < 32) {
            const char* name_before = (task_before != nullptr && task_before->name != nullptr) ? task_before->name : "?";
            const char* name_after = (task_after != nullptr && task_after->name != nullptr) ? task_after->name : "?";
            dbg::log("sw[%lu] t=%lu: %lu(%s)->%lu(%s) cs=0x%x", static_cast<unsigned long>(n), static_cast<unsigned long>(TICKS),
                     task_before != nullptr ? static_cast<unsigned long>(task_before->pid) : 0UL, name_before,
                     task_after != nullptr ? static_cast<unsigned long>(task_after->pid) : 0UL, name_after,
                     static_cast<unsigned>(frame_ptr->cs));
        }
    }

    if (cpu == 0 && (TICKS % 500) == 499) {
        dump_irq_stats();
        sched::dump_scheduler_trace_stats();
    }
#endif  // SCHED_DEBUG

    if (is_first_run_kernel_thread_frame(*frame_ptr, *gpr_ptr, return_task)) {
        wos_start_kernel_thread(frame_ptr->rsp, return_task->kthread_entry);
        __builtin_unreachable();
    }
}

extern "C" __attribute__((no_sanitize("address", "undefined", "coverage"))) void wos_debug_validate_syscall_user_return(
    cpu::GPRegs* gpr_ptr, uint64_t signal_return_mode) {
    if (gpr_ptr == nullptr) {
        return;
    }

    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->context.syscall_scratch_area == 0) {
        return;
    }

    auto* scratch = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    uint64_t live_rip = 0;
    uint64_t live_rsp = 0;
    uint64_t live_flags = 0;
    asm volatile("movq %%gs:0x28, %0" : "=r"(live_rip)::"memory");
    asm volatile("movq %%gs:0x08, %0" : "=r"(live_rsp)::"memory");
    asm volatile("movq %%gs:0x30, %0" : "=r"(live_flags)::"memory");

    gates::InterruptFrame frame{};
    frame.rip = scratch->syscall_ret_rip;
    frame.cs = desc::gdt::GDT_USER_CS;
    frame.flags = scratch->syscall_ret_flags;
    frame.rsp = scratch->user_rsp;
    frame.ss = desc::gdt::GDT_USER_DS;
    debug_validate_clang_return("syscall", task, *gpr_ptr, frame, signal_return_mode, live_rip, live_rsp, live_flags);

    if (live_rip == scratch->syscall_ret_rip && live_rsp == scratch->user_rsp && live_flags == scratch->syscall_ret_flags) {
        return;
    }

    frame.rip = live_rip;
    frame.flags = live_flags;
    frame.rsp = live_rsp;
    debug_validate_clang_return("syscall-live", task, *gpr_ptr, frame, signal_return_mode, live_rip, live_rsp, live_flags);
}

extern "C" void wos_jump_to_next_task_no_save(void* stack_ptr) {
    kasan_unpoison_irq_save_area(stack_ptr);

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::InterruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jump_to_next_task(*gpr_ptr, *frame_ptr);
    check_pending_signals_for_return(*gpr_ptr, *frame_ptr);
    auto* return_task = sched::get_return_task();
    validate_kernel_frame(*frame_ptr, return_task, "exit-return");
}

void start_sched_timer() {
    uint64_t requested_quantum_us = SCHED_TIMER_QUANTUM_US;
    if (cmdline_token_uint_value(ker::init::get_kernel_cmdline(), "sched.timer_quantum_us", requested_quantum_us) &&
        requested_quantum_us != 0) {
        timer_quantum_us = requested_quantum_us;
    } else {
        timer_quantum_us = SCHED_TIMER_QUANTUM_US;
    }
    timer_quantum = apic::calibrate_timer(timer_quantum_us);  // default 4ms (matches Linux CFS typical quantum)
    sched::note_scheduler_timer_arm();
    apic::one_shot_timer(timer_quantum);
}

auto request_reschedule() -> bool {
    if (defer_process_reschedule_to_syscall_exit()) {
        return true;
    }

    if (!current_stack_allows_local_reschedule()) {
        if (!sched::interrupts_enabled()) {
            return false;
        }
        static std::atomic<uint64_t> skipped_mismatch_logs{0};
        uint64_t const N = skipped_mismatch_logs.fetch_add(1, std::memory_order_relaxed);
        if (N < 8) {
            auto* task = sched::get_current_task();
            // NOLINTNEXTLINE(misc-const-correctness)
            uint64_t rsp = 0;
            asm volatile("mov %%rsp, %0" : "=r"(rsp)::"memory");
            auto* owner = sched::debug_find_task_by_kernel_stack(rsp);
            dbg::logger<"ctxswitch">::warn(
                "skip local resched on non-current task stack: current=%lu(%s) owner=%lu(%s) rsp=0x%llx stack=0x%llx",
                task != nullptr ? task->pid : 0UL, (task != nullptr && task->name != nullptr) ? task->name : "?",
                owner != nullptr ? owner->pid : 0UL, (owner != nullptr && owner->name != nullptr) ? owner->name : "?",
                static_cast<unsigned long long>(rsp),
                task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL);
        }
        if (timer_quantum != 0) {
            sched::note_scheduler_timer_arm();
            apic::one_shot_timer(timer_quantum);
            return true;
        }
        return false;
    }
    sched::note_local_reschedule_timer_poke();
    apic::one_shot_timer(1);
    return true;
}
}  // namespace ker::mod::sys::context_switch
