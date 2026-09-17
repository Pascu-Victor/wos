#include "signal.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/sched/resource.hpp>
#include <platform/sched/signal_pending.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/frame_class.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/sys/context_switch.hpp"
#include "platform/sys/usercopy.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"

namespace {

using Task = ker::mod::sched::task::Task;

// Signal constants (matching Linux ABI)
constexpr uint64_t WOS_SIG_DFL = 0;
constexpr uint64_t WOS_SIG_IGN = 1;
constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGSEGV = 11;
constexpr int WOS_SIGHUP = 1;
constexpr int WOS_SIGINT = 2;
constexpr int WOS_SIGQUIT = 3;
constexpr int WOS_SIGTERM = 15;
constexpr int WOS_SIGSTOP = 19;
constexpr auto sanitized_signal_mask(uint64_t mask) -> uint64_t {
    return mask & ~((uint64_t{1} << (WOS_SIGKILL - 1)) | (uint64_t{1} << (WOS_SIGSTOP - 1)));
}
constexpr int WOS_SIGCHLD = 17;
constexpr int WOS_SIGURG = 23;
constexpr int WOS_SIGWINCH = 28;
constexpr int WOS_SIGCONT = 18;
constexpr int WOS_SIGTSTP = 20;
constexpr int WOS_SIGTTIN = 21;
constexpr int WOS_SIGTTOU = 22;

// Stack offsets for the pushed GP registers in syscall.asm (pushq macro).
// After pushq, RSP points to r15. Offsets from RSP:
//   0x00=r15, 0x08=r14, 0x10=r13, 0x18=r12, 0x20=r11, 0x28=r10,
//   0x30=r9,  0x38=r8,  0x40=rbp, 0x48=rdi, 0x50=rsi, 0x58=rdx,
//   0x60=rcx, 0x68=rbx, 0x70=rax, 0x78=return_value
constexpr auto STACK_OFF_RDI = 0x48;
constexpr auto STACK_OFF_RCX = 0x60;
constexpr auto STACK_OFF_RAX = 0x70;
constexpr auto STACK_OFF_RETVAL = 0x78;
constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;
constexpr uint64_t USER_RFLAGS_CF = 1ULL << 0;
constexpr uint64_t USER_RFLAGS_FIXED_ONE = 1ULL << 1;
constexpr uint64_t USER_RFLAGS_PF = 1ULL << 2;
constexpr uint64_t USER_RFLAGS_AF = 1ULL << 4;
constexpr uint64_t USER_RFLAGS_ZF = 1ULL << 6;
constexpr uint64_t USER_RFLAGS_SF = 1ULL << 7;
constexpr uint64_t USER_RFLAGS_TF = 1ULL << 8;
constexpr uint64_t USER_RFLAGS_IF = 1ULL << 9;
constexpr uint64_t USER_RFLAGS_DF = 1ULL << 10;
constexpr uint64_t USER_RFLAGS_OF = 1ULL << 11;
constexpr uint64_t USER_RFLAGS_RF = 1ULL << 16;
constexpr uint64_t USER_RFLAGS_AC = 1ULL << 18;
constexpr uint64_t USER_RFLAGS_ID = 1ULL << 21;
constexpr uint64_t USER_RFLAGS_ALLOWED_MASK = USER_RFLAGS_CF | USER_RFLAGS_FIXED_ONE | USER_RFLAGS_PF | USER_RFLAGS_AF | USER_RFLAGS_ZF |
                                              USER_RFLAGS_SF | USER_RFLAGS_TF | USER_RFLAGS_IF | USER_RFLAGS_DF | USER_RFLAGS_OF |
                                              USER_RFLAGS_RF | USER_RFLAGS_AC | USER_RFLAGS_ID;
constexpr uint64_t USER_RFLAGS_REQUIRED_MASK = USER_RFLAGS_FIXED_ONE | USER_RFLAGS_IF;
constexpr uint64_t WOS_TCB_SIGNAL_MASK_OFFSET = 0x38;
constexpr uint64_t WOS_TCB_SIGNAL_MASK_SEQ_OFFSET = 0x40;
constexpr uint64_t WOS_TCB_SIGNAL_MASK_VALID_OFFSET = 0x48;

// Helper: read a uint64_t from a stack slot
auto stack_read(const uint8_t* base, int offset) -> uint64_t { return *reinterpret_cast<const uint64_t*>(base + offset); }

// Helper: write a uint64_t to a stack slot
void stack_write(uint8_t* base, int offset, uint64_t value) { *reinterpret_cast<uint64_t*>(base + offset) = value; }

inline auto is_user_signal_handler(const ker::mod::sched::task::Task::SigHandler& handler) -> bool {
    return handler.handler != WOS_SIG_DFL && handler.handler != WOS_SIG_IGN;
}

auto user_pointer_valid(uint64_t ptr) -> bool { return ptr != 0 && ptr < USER_ADDR_LIMIT; }

auto user_signal_target_valid(const ker::mod::sched::task::Task::SigHandler& handler) -> bool {
    return user_pointer_valid(handler.handler) && user_pointer_valid(handler.restorer);
}

auto user_rflags_valid(uint64_t flags) -> bool {
    return (flags & USER_RFLAGS_REQUIRED_MASK) == USER_RFLAGS_REQUIRED_MASK && (flags & ~USER_RFLAGS_ALLOWED_MASK) == 0;
}

auto signal_return_frame_valid(const ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return user_pointer_valid(frame.saved_rip) && user_pointer_valid(frame.saved_rsp) && user_rflags_valid(frame.saved_rflags);
}

auto live_syscall_user_rsp() -> uint64_t {
    uint64_t value = 0;
    asm volatile("movq %%gs:0x08, %0" : "=r"(value)::"memory");
    return value;
}

auto live_syscall_return_rip() -> uint64_t {
    uint64_t value = 0;
    asm volatile("movq %%gs:0x28, %0" : "=r"(value)::"memory");
    return value;
}

auto live_syscall_return_flags() -> uint64_t {
    uint64_t value = 0;
    asm volatile("movq %%gs:0x30, %0" : "=r"(value)::"memory");
    return value;
}

void write_live_syscall_return(uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags) {
    asm volatile("movq %0, %%gs:0x08" ::"r"(user_rsp) : "memory");
    asm volatile("movq %0, %%gs:0x28" ::"r"(user_rip) : "memory");
    asm volatile("movq %0, %%gs:0x30" ::"r"(user_flags) : "memory");
}

void write_task_syscall_return(Task& task, uint64_t user_rsp, uint64_t user_rip, uint64_t user_flags) {
    if (task.context.syscall_scratch_area == 0) {
        return;
    }
    auto* per_cpu = reinterpret_cast<ker::mod::cpu::PerCpu*>(task.context.syscall_scratch_area);
    per_cpu->user_rsp = user_rsp;
    per_cpu->syscall_ret_rip = user_rip;
    per_cpu->syscall_ret_flags = user_flags;
}

auto task_stack_contains(const Task& task, uint64_t start, uint64_t end) -> bool {
    if (task.thread == nullptr || task.thread->stack_size == 0) {
        return false;
    }
    uint64_t const STACK_BASE = task.thread->stack_base_virt;
    uint64_t const STACK_TOP = STACK_BASE + task.thread->stack_size;
    return STACK_TOP >= STACK_BASE && start >= STACK_BASE && end <= STACK_TOP;
}

auto ensure_signal_frame_destination(Task& task, uint64_t frame_addr) -> bool {
    if (task.pagemap == nullptr || !ker::mod::sys::usercopy::range_valid(frame_addr, sizeof(ker::mod::sys::signal::SignalFrame))) {
        return false;
    }

    uint64_t const FRAME_END = frame_addr + sizeof(ker::mod::sys::signal::SignalFrame);
    if (ker::mod::sched::threading::has_automatic_stack(task.thread) && task_stack_contains(task, frame_addr, FRAME_END) &&
        !ker::mod::sched::threading::ensure_stack_backing(task.thread, task.pagemap, frame_addr, FRAME_END, &task)) {
        return false;
    }

    return ker::mod::sys::usercopy::ensure_writable(task, frame_addr, sizeof(ker::mod::sys::signal::SignalFrame));
}

auto copy_from_task_user(Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    return ker::mod::sys::usercopy::copy_from_task(task, user_addr, dst, size);
}

auto copy_to_task_user(Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return ker::mod::sys::usercopy::copy_to_task(task, user_addr, src, size);
}

auto copy_from_task_user_mapped(Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    return ker::mod::sys::usercopy::copy_from_task_mapped(task, user_addr, dst, size);
}

auto copy_to_task_user_mapped(Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return ker::mod::sys::usercopy::copy_to_task_mapped(task, user_addr, src, size);
}

auto write_signal_frame(Task& task, uint64_t frame_addr, const ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return ensure_signal_frame_destination(task, frame_addr) && copy_to_task_user(task, frame_addr, &frame, sizeof(frame));
}

auto write_signal_frame_mapped(Task& task, uint64_t frame_addr, const ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return ker::mod::sys::usercopy::range_valid(frame_addr, sizeof(frame)) &&
           copy_to_task_user_mapped(task, frame_addr, &frame, sizeof(frame));
}

auto read_signal_frame(Task& task, uint64_t frame_addr, ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return copy_from_task_user(task, frame_addr, &frame, sizeof(frame));
}

auto read_signal_frame_mapped(Task& task, uint64_t frame_addr, ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return copy_from_task_user_mapped(task, frame_addr, &frame, sizeof(frame));
}

auto signal_frame_saved_mask_value(const Task& task) -> uint64_t {
    return task.sigsuspend_active ? task.sigsuspend_saved_mask : task.signal_mask_bits();
}

void consume_signal_frame_saved_mask(Task& task) {
    if (!task.sigsuspend_active) {
        return;
    }
    task.sigsuspend_active = false;
    task.sigsuspend_saved_mask = 0;
}

void handle_signal_frame_fault(Task* task) {
    if (task == ker::mod::sched::get_current_task()) {
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }
}

}  // namespace

namespace ker::mod::sys::signal {

namespace {

auto sync_task_signal_mask_cache_impl(sched::task::Task* task, uint64_t tcb, bool mapped_only) -> bool {
    if (task == nullptr || task->pagemap == nullptr || tcb == 0) {
        return false;
    }

    static_assert(WOS_TCB_SIGNAL_CACHE_BYTES == WOS_TCB_SIGNAL_MASK_VALID_OFFSET + sizeof(uint32_t));
    if (!ker::mod::sys::usercopy::range_valid(tcb, WOS_TCB_SIGNAL_CACHE_BYTES)) {
        return false;
    }
    uint32_t invalid = 0;
    uint32_t valid = 1;
    uint64_t const MASK = task->signal_mask_bits();
    uint64_t const SEQ = task->signal_mask_next_seq();

    auto copy_cache_field = [&](uint64_t addr, const void* value, size_t size) -> bool {
        return mapped_only ? copy_to_task_user_mapped(*task, addr, value, size) : copy_to_task_user(*task, addr, value, size);
    };

    if (!copy_cache_field(tcb + WOS_TCB_SIGNAL_MASK_VALID_OFFSET, &invalid, sizeof(invalid))) {
        return false;
    }
    if (!copy_cache_field(tcb + WOS_TCB_SIGNAL_MASK_OFFSET, &MASK, sizeof(MASK))) {
        return false;
    }
    if (!copy_cache_field(tcb + WOS_TCB_SIGNAL_MASK_SEQ_OFFSET, &SEQ, sizeof(SEQ))) {
        return false;
    }
    return copy_cache_field(tcb + WOS_TCB_SIGNAL_MASK_VALID_OFFSET, &valid, sizeof(valid));
}

}  // namespace

void sync_task_signal_mask_cache(sched::task::Task* task) {
    if (task == nullptr || task->thread == nullptr) {
        return;
    }
    (void)sync_task_signal_mask_cache_impl(task, task->thread->fsbase, false);
}

auto sync_task_signal_mask_cache_at(sched::task::Task* task, uint64_t tcb) -> bool {
    return sync_task_signal_mask_cache_impl(task, tcb, false);
}

void sync_task_signal_mask_cache_mapped(sched::task::Task* task) {
    if (task == nullptr || task->thread == nullptr) {
        return;
    }
    (void)sync_task_signal_mask_cache_impl(task, task->thread->fsbase, true);
}

auto resume_job_control_for_pending_signal(sched::task::Task* task) -> bool {
    if (task == nullptr) {
        return true;
    }
    if (auto managed = sched::job_control::resume(*task); managed.has_value()) return *managed;
    if (!task->jobctl_stopped.load(std::memory_order_acquire)) return true;

    constexpr uint64_t SIGCONT_MASK = 1ULL << (WOS_SIGCONT - 1);
    constexpr uint64_t SIGKILL_MASK = 1ULL << (WOS_SIGKILL - 1);
    bool const KILLED = (task->signal_pending_bits() & SIGKILL_MASK) != 0;
    uint32_t const STOP_SIGNAL = task->jobctl_stop_signal;
    if (!KILLED && task->jobctl_stop_publish_deferred.load(std::memory_order_acquire) &&
        !task->jobctl_stop_pending.load(std::memory_order_acquire) && STOP_SIGNAL > 0 && STOP_SIGNAL <= 64) {
        // The deferred event belongs to the stopped epoch even if a newer
        // CONT has already canceled the pending STOP bit at generation time.
        task->jobctl_stop_pending.store(true, std::memory_order_release);
        if (!ker::syscall::process::child_events::publish_job_control_stop(*task, STOP_SIGNAL)) {
            task->jobctl_stop_pending.store(false, std::memory_order_release);
            return false;
        }
        task->jobctl_stop_publish_deferred.store(false, std::memory_order_release);
        task->signal_clear_pending_mask(1ULL << (STOP_SIGNAL - 1U));
    }

    bool const CONTINUED = (task->signal_pending_bits() & SIGCONT_MASK) != 0 || sched::job_control::legacy_continue_requested(*task);
    if (!CONTINUED && !KILLED) {
        return false;
    }

    // A continued transition is observable even when SIGCONT is blocked or
    // ignored. Keep the task stopped if its embedded continued-event slot is
    // still occupied; this preserves transition order without allocating in a
    // scheduler/interrupt signal-generation context. SIGKILL resumes without
    // producing a continued event.
    if (!KILLED && CONTINUED && !ker::syscall::process::child_events::publish_continued(*task)) {
        return false;
    }

    task->jobctl_stopped.store(false, std::memory_order_release);
    task->jobctl_stop_publish_deferred.store(false, std::memory_order_release);
    task->set_voluntary_blocked(false);
    task->wants_block = false;
    task->wake_at_us = 0;
    task->clear_wait_channel();
    sched::job_control::complete_legacy_continue(*task);
    return true;
}

auto restore_deferred_sigreturn(sched::task::Task* task) -> DeferredSigreturnResult {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || !task->do_sigreturn ||
        !sched::task::saved_frame_restore_policy(task->context.saved_frame_class).validate_as_user ||
        !context_switch::saved_frame_class_is_valid(task, task->context.frame)) {
        return DeferredSigreturnResult::NONE;
    }

    task->do_sigreturn = false;

    uint64_t const USER_RSP = task->context.frame.rsp;
    if (USER_RSP < sizeof(uint64_t)) {
        handle_signal_frame_fault(task);
        return DeferredSigreturnResult::FAULT;
    }

    uint64_t const FRAME_START = USER_RSP - sizeof(uint64_t);
    SignalFrame frame{};
    if (!read_signal_frame_mapped(*task, FRAME_START, frame) || !signal_return_frame_valid(frame)) {
        handle_signal_frame_fault(task);
        return DeferredSigreturnResult::FAULT;
    }

    task->signal_mask_store(sanitized_signal_mask(frame.saved_mask));
    sync_task_signal_mask_cache_mapped(task);

    static_assert(sizeof(task->context.regs) == sizeof(frame.saved_regs));
    std::memcpy(&task->context.regs, frame.saved_regs.data(), sizeof(task->context.regs));
    task->context.regs.rax = frame.saved_retval;

    task->context.frame.int_num = 0;
    task->context.frame.err_code = 0;
    task->context.frame.rip = frame.saved_rip;
    task->context.frame.cs = desc::gdt::GDT_USER_CS;
    task->context.frame.flags = frame.saved_rflags;
    task->context.frame.rsp = frame.saved_rsp;
    task->context.frame.ss = desc::gdt::GDT_USER_DS;
    ker::mod::sys::context_switch::record_saved_frame_class(task, task->context.frame,
                                                            sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);
    write_task_syscall_return(*task, frame.saved_rsp, frame.saved_rip, frame.saved_rflags);

    task->in_signal_handler = false;
    return DeferredSigreturnResult::RESTORED;
}

namespace {

auto interrupt_frame_is_user_return(sched::task::Task* task, const gates::InterruptFrame& frame) -> bool {
    if (task == nullptr) {
        return false;
    }
    auto const FRAME_CLASS =
        context_switch::classify_saved_frame(task, frame, sched::task::SavedFrameOrigin::INTERRUPT, task->is_voluntary_blocked());
    return sched::task::saved_frame_restore_policy(FRAME_CLASS).deliver_signals;
}

auto synchronous_signal_can_recover_on_alt_stack(const sched::task::Task& task, const gates::InterruptFrame& frame,
                                                 const sched::task::Task::SigHandler& handler) -> bool {
    // A synchronous fault can itself make the interrupted RSP invalid.  That
    // is precisely the condition SA_ONSTACK is meant to recover from, so
    // validate every other user-return field and let the mapped alternate
    // stack provide the signal-frame destination.
    return frame.cs == desc::gdt::GDT_USER_CS && frame.ss == desc::gdt::GDT_USER_DS && user_pointer_valid(frame.rip) &&
           user_rflags_valid(frame.flags) && (handler.flags & WOS_SA_ONSTACK) != 0 && (task.sigaltstack_flags & WOS_SS_DISABLE) == 0 &&
           !is_on_alt_stack(task, frame.rsp);
}

auto is_job_control_stop_signal(int signo) -> bool {
    return signo == WOS_SIGSTOP || signo == WOS_SIGTSTP || signo == WOS_SIGTTIN || signo == WOS_SIGTTOU;
}

auto default_signal_is_ignored(int signo) -> bool {
    return signo == WOS_SIGCHLD || signo == WOS_SIGURG || signo == WOS_SIGWINCH || signo == WOS_SIGCONT;
}

auto task_owns_current_kernel_stack(Task* task) -> bool {
    if (task == nullptr || task->context.syscall_kernel_stack < ker::mod::mm::KERNEL_STACK_SIZE) {
        return false;
    }

    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp));

    uint64_t const STACK_TOP = task->context.syscall_kernel_stack;
    uint64_t const STACK_BASE = STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE;
    return rsp > STACK_BASE && rsp <= STACK_TOP;
}

void park_current_job_control_stop(Task& task) {
    while (task.jobctl_stopped.load(std::memory_order_acquire) && !task.has_exited) {
        if (resume_job_control_for_pending_signal(&task)) {
            continue;
        }
        sched::preemptible_syscall_park("job_stop", sched::task::WaitChannelKind::GENERIC);
    }
}

auto apply_job_control_stop(Task* task, int signo, bool park_current, sched::SignalPendingClaim* claim = nullptr) -> bool {
    if (task == nullptr) {
        return true;
    }
    auto const DELIVERY = sched::job_control::begin_stop(*task, signo, claim);
    if (DELIVERY == sched::job_control::StopDelivery::CANCELED) return true;
    bool const MANAGED = DELIVERY == sched::job_control::StopDelivery::MANAGED;
    if (!MANAGED && task->jobctl_stopped.load(std::memory_order_acquire)) {
        return true;
    }

    if (!MANAGED) {
        task->jobctl_stop_signal = static_cast<uint32_t>(signo);
        task->jobctl_stopped.store(true, std::memory_order_release);
        task->jobctl_stop_pending.store(true, std::memory_order_release);
        task->jobctl_stop_publish_deferred.store(false, std::memory_order_release);
    }
    bool const PUBLISHED = MANAGED || ker::syscall::process::child_events::publish_job_control_stop(*task, static_cast<uint32_t>(signo));
    if (!PUBLISHED) {
        task->jobctl_stop_pending.store(false, std::memory_order_release);
        task->jobctl_stop_publish_deferred.store(true, std::memory_order_release);
        task->signal_add_pending_mask(1ULL << (signo - 1));
    }
    if (!MANAGED) {
        task->set_wait_channel("job_stop", sched::task::WaitChannelKind::GENERIC);
        task->set_voluntary_blocked(true);
    }

    if (park_current && sched::get_current_task() == task) {
        if (MANAGED) {
            sched::job_control::acknowledge_stopped(*task);
            sched::job_control::drain();
        }
        park_current_job_control_stop(*task);
    } else if (MANAGED) {
        // A concurrent CONT may already have released the local gate. A
        // generic signal wake lets runqueue selection test the current gate;
        // a blind debug-stop here could park an already continued task.
        sched::wake_task_for_signal(task);
    } else {
        // IRQ/handoff delivery is already past the syscall deferred-switch
        // gate. A deferred flag here would strand a USER_RETURN task behind
        // the preemption policy's return-transition guard. Request a timer
        // switch instead; scheduler admission enforces the stopped state.
        static_cast<void>(sched::debug_stop_task(task));
    }
    return PUBLISHED;
}

auto handle_handoff_non_user_signal_action(Task* task, int signo, unsigned idx, const Task::SigHandler& handler) -> bool {
    if (handler.handler == WOS_SIG_DFL) {
        if (default_signal_is_ignored(signo)) {
            if (!task->consume_signal_for_delivery(1ULL << idx)) {
                return true;
            }
            if (task->sigsuspend_active) {
                task->signal_mask_store(task->signal_frame_saved_mask());
                sync_task_signal_mask_cache_mapped(task);
            }
            return true;
        }
        if (is_job_control_stop_signal(signo)) {
            auto claim = task->claim_signal_for_delivery(1ULL << idx);
            if (claim) {
                static_cast<void>(apply_job_control_stop(task, signo, false, &claim));
            }
            return true;
        }

        // Handoff return can run while current_task is still the outgoing stack
        // owner. Leave fatal default signals pending so the task exits from its
        // own current-task context instead of terminating the outgoing/idle task.
        return true;
    }

    if (handler.handler == WOS_SIG_IGN) {
        if (!task->consume_signal_for_delivery(1ULL << idx)) {
            return true;
        }
        if (task->sigsuspend_active) {
            task->signal_mask_store(task->signal_frame_saved_mask());
            sync_task_signal_mask_cache_mapped(task);
        }
        return true;
    }

    return false;
}

}  // namespace

namespace {
// Call only after completed syscall work or validated USER_RETURN admission.
// A userspace handler cannot defer SIGKILL, and lower pending signal numbers
// cannot hide it. Incoming-frame preparation must leave it pending until the
// task owns the live kernel stack; returning true suppresses other delivery.
auto handle_pending_sigkill_at_user_return(Task* task) -> bool {
    constexpr uint64_t KILL_MASK = uint64_t{1} << (WOS_SIGKILL - 1);
    if ((task->signal_pending_bits() & KILL_MASK) == 0) return false;
    if (task != sched::get_current_task() || !task_owns_current_kernel_stack(task)) return true;
    // Keep fatal admission visible until exit cleanup publishes/observes
    // exit_in_progress. Clearing here could let a concurrent ptrace stop park
    // this stack between signal selection and entry into the exit path.
    ker::syscall::process::wos_proc_exit_signal(WOS_SIGKILL);
    __builtin_unreachable();
}

auto pending_signals_for_user_delivery(Task* task) -> uint64_t {
    // STOP does not build a nested handler frame. Its uncatchable action must
    // precede handler deferral and lower-numbered catchable pending signals.
    constexpr uint64_t STOP_MASK = uint64_t{1} << (WOS_SIGSTOP - 1);
    if ((task->signal_pending_bits() & STOP_MASK) != 0) return STOP_MASK;
    return task->in_signal_handler ? 0 : task->signal_deliverable_bits();
}

void deliver_cpu_limits_at_user_return(Task* task) {
    if (task != sched::get_current_task()) {
        // The incoming USER_RETURN frame is already validated. Prepare a soft
        // handler on that frame now; hard expiry must wait until assembly has
        // moved to this task's own kernel stack and committed ownership.
        constexpr uint64_t SOFT_MASK = uint64_t{1} << 23;
        task->signal_add_pending_mask(task->pending_cpu_signals.fetch_and(~SOFT_MASK, std::memory_order_acq_rel) & SOFT_MASK);
        return;
    }
    uint64_t const PENDING = task->pending_cpu_signals.exchange(0, std::memory_order_acq_rel);
    if ((PENDING & (uint64_t{1} << (WOS_SIGKILL - 1))) != 0) {
        // This gate is reached only after completed syscall work or with a
        // validated user-return frame. Hard CPU expiry is uncatchable even
        // when userspace is currently inside a looping SIGXCPU handler.
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGKILL);
        __builtin_unreachable();
    }
    task->signal_add_pending_mask(PENDING);
}
}  // namespace

void exit_current_on_cpu_limit_user_handoff() {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || !task_owns_current_kernel_stack(task) ||
        !sched::task::saved_frame_restore_policy(task->context.saved_frame_class).deliver_signals ||
        !context_switch::saved_frame_class_is_valid(task, task->context.frame)) {
        return;
    }
    deliver_cpu_limits_at_user_return(task);
}

void exit_current_on_pending_fatal_default_signal() {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->exit_in_progress || task->has_exited ||
        task->unpublished_teardown_in_progress.load(std::memory_order_acquire) ||
        task->exec_preparation_teardown_in_progress.load(std::memory_order_acquire) ||
        task->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE || !task_owns_current_kernel_stack(task)) {
        return;
    }
    // A handoff can resume an interrupted or parked syscall with scoped File,
    // reservation and lock owners still on its kernel stack. Noreturn teardown
    // here would abandon those owners. Leave fatal signals/exit requests pending
    // until the syscall unwinds and a validated userspace return is reached.
    if (!sched::task::saved_frame_restore_policy(task->context.saved_frame_class).deliver_signals ||
        !context_switch::saved_frame_class_is_valid(task, task->context.frame)) {
        return;
    }
    context_switch::UserReturnFpuAuthorization return_fpu(*task);
    if (!resume_job_control_for_pending_signal(task)) {
        return;
    }

    sched::resource::wait_for_exec_quiescence(*task);
    ker::syscall::process::exit_current_if_process_exit_requested();
    if (handle_pending_sigkill_at_user_return(task)) return;
    ker::syscall::process::service_pending_exec_handoff();
    if (task->in_signal_handler) return;

    uint64_t const DELIVERABLE = task->signal_deliverable_bits();
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);
    auto const HANDLER = task->signal_handler_snapshot(IDX);
    if (HANDLER.handler != WOS_SIG_DFL || default_signal_is_ignored(SIGNO) || is_job_control_stop_signal(SIGNO)) {
        return;
    }

    if (!task->consume_signal_for_delivery(1ULL << IDX)) {
        return;
    }
    ker::syscall::process::wos_proc_exit_signal(SIGNO);
    __builtin_unreachable();
}

void service_retired_main_signals() {
    auto* owner = sched::get_current_task();
    uint64_t pending = owner->signal_pending_bits();
    constexpr uint64_t KILL = uint64_t{1} << (WOS_SIGKILL - 1);
    constexpr uint64_t STOP = uint64_t{1} << (WOS_SIGSTOP - 1);
    if ((pending & KILL) != 0) {
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGKILL);
    }
    if ((pending & STOP) != 0) {
        owner->signal_clear_pending_mask(STOP);
        static_cast<void>(apply_job_control_stop(owner, WOS_SIGSTOP, true));
        if ((owner->signal_pending_bits() & KILL) != 0) {
            ker::syscall::process::wos_proc_exit_signal(WOS_SIGKILL);
        }
        return;
    }
    struct Recipient {
        Task* owner = nullptr;
        uint64_t mask = 0;
    } request{.owner = owner};
    auto eligible = [](Task* candidate, void* opaque) -> bool {
        auto const& requested = *static_cast<Recipient*>(opaque);
        return candidate->is_thread && candidate->credential_owner() == requested.owner &&
               candidate->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
               !candidate->exit_in_progress.load(std::memory_order_acquire) && (candidate->signal_mask_bits() & requested.mask) == 0;
    };
    // A retired thread's mask and handler frame cannot receive process signals.
    // Keep a bit pending if all remaining threads block it; retry after a park.
    while (pending != 0) {
        auto const INDEX = static_cast<unsigned>(__builtin_ctzll(pending));
        request.mask = uint64_t{1} << INDEX;
        pending &= ~request.mask;
        auto* recipient = sched::find_active_task_lifetime_ref_if(eligible, &request);
        if (recipient == nullptr) {
            continue;
        }
        // This is only a readiness wake. A worker claims the owner's pending
        // occurrence at its own delivery boundary; retirement cannot lose it.
        sched::wake_task_for_signal(recipient);
        recipient->release();
    }
}

// Called from syscall.asm after the deferred task switch check, before returning
// to userspace. Handles both sigreturn (context restore) and signal delivery.
//
// stack_base: pointer to RSP at the bottom of pushed GP registers (from pushq).
extern "C" auto check_pending_signals(uint8_t* stack_base) -> uint64_t {
    sched::job_control::drain();
    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return 0;
    }

    deliver_cpu_limits_at_user_return(task);
    if (!resume_job_control_for_pending_signal(task)) {
        park_current_job_control_stop(*task);
    }

    sched::resource::wait_for_exec_quiescence(*task);
    ker::syscall::process::exit_current_if_process_exit_requested();

    if (handle_pending_sigkill_at_user_return(task)) return 0;
    ker::syscall::process::service_pending_exec_handoff();

    // Keep the live syscall return frame on this stack while stopped, including
    // a pending sigreturn. CONT resumes that frame; KILL must terminate before
    // any restoration or return to the interrupted handler.
    constexpr uint64_t STOP_MASK = uint64_t{1} << (WOS_SIGSTOP - 1);
    if ((task->signal_pending_bits() & STOP_MASK) != 0) {
        task->signal_clear_pending_mask(STOP_MASK);
        static_cast<void>(apply_job_control_stop(task, WOS_SIGSTOP, true));
        ker::syscall::process::exit_current_if_process_exit_requested();
        if (handle_pending_sigkill_at_user_return(task)) return 0;
    }

    // --- Handle sigreturn first ---
    if (task->do_sigreturn) {
        task->do_sigreturn = false;

        // User RSP at sigreturn syscall entry points to &frame.signo
        // (restorer's `ret` already popped pretcode, so RSP = pretcode + 8 = &signo)
        uint64_t const USER_RSP = live_syscall_user_rsp();
        uint64_t const FRAME_START = USER_RSP - 8;  // back up to pretcode
        SignalFrame frame{};
        if (!read_signal_frame(*task, FRAME_START, frame)) {
            ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
            __builtin_unreachable();
        }
        if (!signal_return_frame_valid(frame)) {
            ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
            __builtin_unreachable();
        }

        // Restore signal mask
        task->signal_mask_store(sanitized_signal_mask(frame.saved_mask));
        sync_task_signal_mask_cache(task);

        // Restore GP registers to the on-stack positions
        for (int i = 0; i < 15; i++) {
            stack_write(stack_base, i * 8, frame.saved_regs.at(static_cast<size_t>(i)));
        }

        // Restore the return value slot
        stack_write(stack_base, STACK_OFF_RETVAL, frame.saved_retval);
        stack_write(stack_base, STACK_OFF_RAX, frame.saved_retval);

        // Restore user RIP, RSP, and RFLAGS through the same live GS scratch
        // that syscall.asm will consume for the imminent return.
        write_task_syscall_return(*task, frame.saved_rsp, frame.saved_rip, frame.saved_rflags);
        write_live_syscall_return(frame.saved_rsp, frame.saved_rip, frame.saved_rflags);

        task->in_signal_handler = false;
        return 1;  // Return via iretq so RCX/R11 are restored as user GPRs.
    }

    // --- Check for deliverable signals ---
    // The task model tracks one active userspace signal frame. Match the
    // interrupt/deferred paths and defer nested handlers until sigreturn.
    if (task->in_signal_handler) {
        return 0;
    }

    uint64_t const DELIVERABLE = task->signal_deliverable_bits();
    if (DELIVERABLE == 0) {
        return 0;
    }

    // Find the first pending signal (1-based)
    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);

    auto const HANDLER = task->signal_handler_snapshot(IDX);

    // Handle SIG_DFL
    if (HANDLER.handler == WOS_SIG_DFL) {
        if (is_job_control_stop_signal(SIGNO)) {
            auto claim = task->claim_signal_for_delivery(1ULL << IDX);
            if (claim) {
                static_cast<void>(apply_job_control_stop(task, SIGNO, true, &claim));
            }
            return 0;
        }
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return 0;
        }
        // Default action depends on signal:
        // SIGCHLD, SIGURG, SIGWINCH, SIGCONT: ignore
        // SIGTSTP(20), SIGTTIN(21), SIGTTOU(22): stop (for job control)
        // SIGKILL, SIGTERM, SIGINT, etc: terminate
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->signal_mask_store(task->signal_frame_saved_mask());
                sync_task_signal_mask_cache(task);
            }
            return 0;  // Ignore
        }
        // Default terminate semantics for normal fatal signals.
        if (SIGNO == WOS_SIGHUP || SIGNO == WOS_SIGINT || SIGNO == WOS_SIGQUIT || SIGNO == WOS_SIGTERM || SIGNO == WOS_SIGKILL ||
            SIGNO == 13 || SIGNO == 11 || SIGNO == 6 || SIGNO == 8 || SIGNO == 4 || SIGNO == 7) {
            ker::syscall::process::wos_proc_exit_signal(SIGNO);
            __builtin_unreachable();
        }

        // Conservative fallback: terminate by signal number if action is unknown.
        ker::syscall::process::wos_proc_exit_signal(SIGNO);
        __builtin_unreachable();
    }

    // Handle SIG_IGN
    if (HANDLER.handler == WOS_SIG_IGN) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return 0;
        }
        if (task->sigsuspend_active) {
            task->signal_mask_store(task->signal_frame_saved_mask());
            sync_task_signal_mask_cache(task);
        }
        return 0;
    }

    if (!user_signal_target_valid(HANDLER)) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return 0;
        }
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    auto claim = task->claim_signal_for_delivery(1ULL << IDX);
    if (!claim) {
        return 0;
    }

    // --- Deliver the signal: set up signal frame on user stack ---
    uint64_t const USER_RSP = live_syscall_user_rsp();
    uint64_t const USER_RIP = live_syscall_return_rip();
    uint64_t const USER_RFLAGS = live_syscall_return_flags();

    // Preserve the SysV user red zone; mlibc and clang-generated leaf code may
    // keep live locals in the 128 bytes below RSP across syscalls.
    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, USER_RSP, HANDLER.flags);
    SignalFrame frame{};

    frame.pretcode = HANDLER.restorer;  // sa_restorer trampoline
    frame.signo = static_cast<uint64_t>(SIGNO);
    frame.saved_mask = signal_frame_saved_mask_value(*task);
    frame.saved_rip = USER_RIP;
    frame.saved_rsp = USER_RSP;
    frame.saved_rflags = USER_RFLAGS;
    frame.saved_retval = stack_read(stack_base, STACK_OFF_RETVAL);

    // Save all 15 GP registers from the stack
    for (int i = 0; i < 15; i++) {
        frame.saved_regs.at(static_cast<size_t>(i)) = stack_read(stack_base, i * 8);
    }

    if (!write_signal_frame(*task, FRAME_ADDR, frame)) {
        if (!claim.commit()) {
            return 0;
        }
        handle_signal_frame_fault(task);
        return 0;
    }
    if (!claim.commit()) {
        return 0;
    }
    consume_signal_frame_saved_mask(*task);

    // Modify the on-stack registers to redirect to the signal handler
    stack_write(stack_base, STACK_OFF_RCX, HANDLER.handler);               // sysret target = HANDLER
    stack_write(stack_base, STACK_OFF_RDI, static_cast<uint64_t>(SIGNO));  // arg1 = signo

    // Update the stored task scratch and the live GS scratch.  The latter is
    // what syscall.asm checks immediately before SYSRET.
    write_task_syscall_return(*task, FRAME_ADDR, HANDLER.handler, USER_RFLAGS);
    write_live_syscall_return(FRAME_ADDR, HANDLER.handler, USER_RFLAGS);

    // Block additional signals during handler execution
    // Block the handler's sa_mask + the signal itself (unless SA_NODEFER)
    task->signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask));
    if ((HANDLER.flags & 0x40000000ULL) == 0U) {  // SA_NODEFER = 0x40000000
        task->signal_add_mask_bits(1ULL << IDX);
    }
    sync_task_signal_mask_cache(task);

    task->in_signal_handler = true;
    return 1;  // Return via iretq so HANDLER entry uses the full GP restore path.
}

void check_pending_signals_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame) {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }
    if (!resume_job_control_for_pending_signal(task)) {
        return;
    }

    // Only USER_RETURN frames may construct a userspace signal frame.
    if (!interrupt_frame_is_user_return(task, frame)) {
        return;
    }

    ker::syscall::process::exit_current_if_process_exit_requested();

    deliver_cpu_limits_at_user_return(task);
    if (handle_pending_sigkill_at_user_return(task)) return;

    uint64_t const DELIVERABLE = pending_signals_for_user_delivery(task);
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);

    auto const HANDLER = task->signal_handler_snapshot(IDX);

    if (HANDLER.handler == WOS_SIG_DFL) {
        if (is_job_control_stop_signal(SIGNO)) {
            auto claim = task->claim_signal_for_delivery(1ULL << IDX);
            if (claim) {
                static_cast<void>(apply_job_control_stop(task, SIGNO, false, &claim));
            }
            return;
        }
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->signal_mask_store(task->signal_frame_saved_mask());
                sync_task_signal_mask_cache_mapped(task);
            }
            return;
        }
        if (SIGNO == WOS_SIGHUP || SIGNO == WOS_SIGINT || SIGNO == WOS_SIGQUIT || SIGNO == WOS_SIGTERM || SIGNO == WOS_SIGKILL ||
            SIGNO == 13 || SIGNO == 11 || SIGNO == 6 || SIGNO == 8 || SIGNO == 4 || SIGNO == 7) {
            ker::syscall::process::wos_proc_exit_signal(SIGNO);
            __builtin_unreachable();
        }

        ker::syscall::process::wos_proc_exit_signal(SIGNO);
        __builtin_unreachable();
    }

    if (HANDLER.handler == WOS_SIG_IGN) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        if (task->sigsuspend_active) {
            task->signal_mask_store(task->signal_frame_saved_mask());
            sync_task_signal_mask_cache_mapped(task);
        }
        return;
    }

    if (!user_signal_target_valid(HANDLER)) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    auto claim = task->claim_signal_for_delivery(1ULL << IDX);
    if (!claim) {
        return;
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, HANDLER.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = HANDLER.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    static_assert(sizeof(gpr) == sizeof(sigframe.saved_regs));
    std::memcpy(sigframe.saved_regs.data(), &gpr, sizeof(gpr));

    // Interrupt return cannot fault in a lazy/COW stack page.  Leave an
    // asynchronous signal pending so a later safe syscall-return path can
    // construct the frame; a stable mapped-only copy still covers resident
    // pages without dereferencing the userspace address directly.
    if (!write_signal_frame_mapped(*task, FRAME_ADDR, sigframe)) {
        return;
    }
    if (!claim.commit()) {
        return;
    }
    consume_signal_frame_saved_mask(*task);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = HANDLER.handler;
    frame.rsp = FRAME_ADDR;

    task->signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask));
    if ((HANDLER.flags & 0x40000000ULL) == 0U) {
        task->signal_add_mask_bits(1ULL << IDX);
    }
    sync_task_signal_mask_cache_mapped(task);
    task->in_signal_handler = true;
}

auto deliver_synchronous_signal_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame, int signo) -> bool {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return false;
    }

    if (task->in_signal_handler) {
        return false;
    }
    if (signo <= 0) {
        return false;
    }

    auto const SIGNO = static_cast<unsigned>(signo);
    if (SIGNO > sched::task::Task::MAX_SIGNALS) {
        return false;
    }

    auto const IDX = SIGNO - 1;
    auto const HANDLER = task->signal_handler_snapshot(IDX);
    if (!is_user_signal_handler(HANDLER) || (task->signal_mask_bits() & (1ULL << IDX)) != 0) {
        return false;
    }
    if (!interrupt_frame_is_user_return(task, frame) && !synchronous_signal_can_recover_on_alt_stack(*task, frame, HANDLER)) {
        return false;
    }
    if (!user_signal_target_valid(HANDLER)) {
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, HANDLER.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = HANDLER.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    static_assert(sizeof(gpr) == sizeof(sigframe.saved_regs));
    std::memcpy(sigframe.saved_regs.data(), &gpr, sizeof(gpr));

    if (!write_signal_frame_mapped(*task, FRAME_ADDR, sigframe)) {
        handle_signal_frame_fault(task);
        return false;
    }
    consume_signal_frame_saved_mask(*task);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = HANDLER.handler;
    frame.rsp = FRAME_ADDR;

    task->signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask));
    if ((HANDLER.flags & 0x40000000ULL) == 0U) {
        task->signal_add_mask_bits(1ULL << IDX);
    }
    sync_task_signal_mask_cache_mapped(task);
    task->in_signal_handler = true;
    return true;
}

void check_pending_signals_handoff(sched::task::Task* task, cpu::GPRegs& gpr, gates::InterruptFrame& frame) {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }
    if (!resume_job_control_for_pending_signal(task)) {
        return;
    }

    if (!interrupt_frame_is_user_return(task, frame)) {
        return;
    }

    deliver_cpu_limits_at_user_return(task);
    if (handle_pending_sigkill_at_user_return(task)) return;

    uint64_t const DELIVERABLE = pending_signals_for_user_delivery(task);
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);
    auto const HANDLER = task->signal_handler_snapshot(IDX);

    if (handle_handoff_non_user_signal_action(task, SIGNO, IDX, HANDLER)) {
        return;
    }
    if (!user_signal_target_valid(HANDLER)) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        task->signal_add_pending_mask(1ULL << (WOS_SIGSEGV - 1));
        return;
    }

    auto claim = task->claim_signal_for_delivery(1ULL << IDX);
    if (!claim) {
        return;
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, HANDLER.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = HANDLER.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    static_assert(sizeof(gpr) == sizeof(sigframe.saved_regs));
    std::memcpy(sigframe.saved_regs.data(), &gpr, sizeof(gpr));

    if (!write_signal_frame_mapped(*task, FRAME_ADDR, sigframe)) {
        return;
    }
    if (!claim.commit()) {
        return;
    }
    consume_signal_frame_saved_mask(*task);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = HANDLER.handler;
    frame.rsp = FRAME_ADDR;

    write_task_syscall_return(*task, FRAME_ADDR, HANDLER.handler, frame.flags);

    task->signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask));
    if ((HANDLER.flags & 0x40000000ULL) == 0U) {
        task->signal_add_mask_bits(1ULL << IDX);
    }
    sync_task_signal_mask_cache_mapped(task);
    task->in_signal_handler = true;
    task->context.regs = gpr;
    task->context.frame = frame;
    ker::mod::sys::context_switch::record_saved_frame_class(task, task->context.frame, sched::task::SavedFrameOrigin::INTERRUPT);
}

void check_pending_signals_deferred(sched::task::Task* task, DeferredSignalDelivery delivery) {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }
    if (!resume_job_control_for_pending_signal(task)) {
        return;
    }

    // Signal frames must only be built from a validated USER_RETURN context.
    if (!sched::task::saved_frame_restore_policy(task->context.saved_frame_class).deliver_signals ||
        !context_switch::saved_frame_class_is_valid(task, task->context.frame)) {
        return;
    }

    if (task == sched::get_current_task()) {
        ker::syscall::process::exit_current_if_process_exit_requested();
    }

    deliver_cpu_limits_at_user_return(task);
    if (handle_pending_sigkill_at_user_return(task)) return;

    uint64_t const DELIVERABLE = pending_signals_for_user_delivery(task);
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);
    auto const HANDLER = task->signal_handler_snapshot(IDX);

    if (delivery == DeferredSignalDelivery::USER_HANDLERS_ONLY && !is_user_signal_handler(HANDLER)) {
        return;
    }
    if (is_user_signal_handler(HANDLER) && !user_signal_target_valid(HANDLER)) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    if (HANDLER.handler == WOS_SIG_DFL) {
        if (is_job_control_stop_signal(SIGNO)) {
            auto claim = task->claim_signal_for_delivery(1ULL << IDX);
            if (claim) {
                static_cast<void>(apply_job_control_stop(task, SIGNO, false, &claim));
            }
            return;
        }
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->signal_mask_store(task->signal_frame_saved_mask());
                sync_task_signal_mask_cache_mapped(task);
            }
            return;
        }

        ker::syscall::process::wos_proc_exit_signal(SIGNO);
        __builtin_unreachable();
    }

    if (HANDLER.handler == WOS_SIG_IGN) {
        if (!task->consume_signal_for_delivery(1ULL << IDX)) {
            return;
        }
        if (task->sigsuspend_active) {
            task->signal_mask_store(task->signal_frame_saved_mask());
            sync_task_signal_mask_cache_mapped(task);
        }
        return;
    }

    auto& gpr = task->context.regs;
    auto& frame = task->context.frame;
    auto claim = task->claim_signal_for_delivery(1ULL << IDX);
    if (!claim) {
        return;
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, HANDLER.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = HANDLER.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    static_assert(sizeof(gpr) == sizeof(sigframe.saved_regs));
    std::memcpy(sigframe.saved_regs.data(), &gpr, sizeof(gpr));

    if (!write_signal_frame_mapped(*task, FRAME_ADDR, sigframe)) {
        return;
    }
    if (!claim.commit()) {
        return;
    }
    consume_signal_frame_saved_mask(*task);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = HANDLER.handler;
    frame.rsp = FRAME_ADDR;
    write_task_syscall_return(*task, FRAME_ADDR, HANDLER.handler, frame.flags);

    task->signal_add_mask_bits(sanitized_signal_mask(HANDLER.mask));
    if ((HANDLER.flags & 0x40000000ULL) == 0U) {
        task->signal_add_mask_bits(1ULL << IDX);
    }
    sync_task_signal_mask_cache_mapped(task);
    task->in_signal_handler = true;
}

}  // namespace ker::mod::sys::signal
