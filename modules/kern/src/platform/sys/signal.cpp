#include "signal.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
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
constexpr int WOS_SIGCHLD = 17;
constexpr int WOS_SIGURG = 23;
constexpr int WOS_SIGWINCH = 28;
constexpr int WOS_SIGCONT = 18;
constexpr int WOS_SIGTSTP = 20;
constexpr int WOS_SIGTTIN = 21;
constexpr int WOS_SIGTTOU = 22;
constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
constexpr int WOS_WSTOPPED = 2;
constexpr int STOP_STATUS_LOW = 0x7f;

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

auto user_range_valid(uint64_t start, size_t size) -> bool {
    uint64_t const END = start + static_cast<uint64_t>(size);
    return size != 0 && END >= start && END <= USER_ADDR_LIMIT;
}

auto user_copy_chunk(size_t remaining, uint64_t user_addr) -> size_t {
    uint64_t const PAGE_OFFSET = user_addr & (ker::mod::mm::paging::PAGE_SIZE - 1);
    uint64_t const PAGE_REMAINING = ker::mod::mm::paging::PAGE_SIZE - PAGE_OFFSET;
    return remaining < PAGE_REMAINING ? remaining : static_cast<size_t>(PAGE_REMAINING);
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
    if (task.pagemap == nullptr || !user_range_valid(frame_addr, sizeof(ker::mod::sys::signal::SignalFrame))) {
        return false;
    }

    uint64_t const FRAME_END = frame_addr + sizeof(ker::mod::sys::signal::SignalFrame);
    if (task_stack_contains(task, frame_addr, FRAME_END) &&
        !ker::mod::sched::threading::ensure_stack_backing(task.thread, task.pagemap, frame_addr, FRAME_END)) {
        return false;
    }

    uint64_t const PAGE_END = page_align_up(FRAME_END);
    for (uint64_t page = page_align_down(frame_addr); page < PAGE_END; page += ker::mod::mm::paging::PAGE_SIZE) {
        if (!ker::mod::mm::virt::ensure_user_page_writable(&task, page)) {
            return false;
        }
    }
    return true;
}

auto copy_from_task_user(Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    if (task.pagemap == nullptr || dst == nullptr || !user_range_valid(user_addr, size)) {
        return false;
    }

    auto* out = static_cast<uint8_t*>(dst);
    size_t copied = 0;
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        uint64_t const PHYS = ker::mod::mm::virt::translate(task.pagemap, CUR);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            return false;
        }
        size_t const CHUNK = user_copy_chunk(size - copied, CUR);
        auto const* src = reinterpret_cast<const uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
        std::memcpy(out + copied, src, CHUNK);
        copied += CHUNK;
    }
    return true;
}

auto copy_to_task_user(Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    if (task.pagemap == nullptr || src == nullptr || !user_range_valid(user_addr, size)) {
        return false;
    }

    auto const* in = static_cast<const uint8_t*>(src);
    size_t copied = 0;
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        uint64_t const PHYS = ker::mod::mm::virt::translate(task.pagemap, CUR);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            return false;
        }
        size_t const CHUNK = user_copy_chunk(size - copied, CUR);
        auto* dst = reinterpret_cast<uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
        std::memcpy(dst, in + copied, CHUNK);
        copied += CHUNK;
    }
    return true;
}

auto write_signal_frame(Task& task, uint64_t frame_addr, const ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return ensure_signal_frame_destination(task, frame_addr) && copy_to_task_user(task, frame_addr, &frame, sizeof(frame));
}

auto read_signal_frame(Task& task, uint64_t frame_addr, ker::mod::sys::signal::SignalFrame& frame) -> bool {
    return copy_from_task_user(task, frame_addr, &frame, sizeof(frame));
}

auto signal_frame_saved_mask_value(const Task& task) -> uint64_t {
    return task.sigsuspend_active ? task.sigsuspend_saved_mask : task.sig_mask;
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

void sync_task_signal_mask_cache(sched::task::Task* task) {
    if (task == nullptr || task->pagemap == nullptr || task->thread == nullptr || task->thread->fsbase == 0) {
        return;
    }

    uint64_t const TCB = task->thread->fsbase;
    uint32_t invalid = 0;
    uint32_t valid = 1;
    uint64_t const MASK = task->sig_mask;
    uint64_t const SEQ = ++task->sig_mask_seq;

    if (!copy_to_task_user(*task, TCB + WOS_TCB_SIGNAL_MASK_VALID_OFFSET, &invalid, sizeof(invalid))) {
        return;
    }
    if (!copy_to_task_user(*task, TCB + WOS_TCB_SIGNAL_MASK_OFFSET, &MASK, sizeof(MASK))) {
        return;
    }
    if (!copy_to_task_user(*task, TCB + WOS_TCB_SIGNAL_MASK_SEQ_OFFSET, &SEQ, sizeof(SEQ))) {
        return;
    }
    (void)copy_to_task_user(*task, TCB + WOS_TCB_SIGNAL_MASK_VALID_OFFSET, &valid, sizeof(valid));
}

namespace {

auto is_job_control_stop_signal(int signo) -> bool {
    return signo == WOS_SIGSTOP || signo == WOS_SIGTSTP || signo == WOS_SIGTTIN || signo == WOS_SIGTTOU;
}

auto job_control_stop_status(int signo) -> int32_t { return static_cast<int32_t>((static_cast<uint32_t>(signo) << 8U) | STOP_STATUS_LOW); }

auto waitpid_waiter_matches_stopped_child(Task& waiter, Task& stopped) -> bool {
    return waiter.waiting_for_pid == stopped.pid || waiter.waiting_for_pid == WAIT_ANY_CHILD;
}

auto waitpid_waiter_context_can_be_completed(Task& waiter) -> bool {
    return !waiter.deferred_task_switch && !waiter.waitpid_publish_pending.load(std::memory_order_acquire);
}

void clear_waitpid_wait_state(Task& waiter) {
    waiter.waitpid_publish_pending.store(false, std::memory_order_release);
    ker::mod::sched::task::task_clear_waitpid_block_state(waiter);
}

auto complete_waitpid_stop_waiter(Task& waiter, Task& stopped, int signo) -> bool {
    if (!waitpid_waiter_matches_stopped_child(waiter, stopped)) {
        return false;
    }
    if ((waiter.wait_options & WOS_WSTOPPED) == 0 || !waitpid_waiter_context_can_be_completed(waiter)) {
        return false;
    }

    if (waiter.wait_status_user_addr != 0 && waiter.pagemap != nullptr) {
        uint64_t const PHYS = ker::mod::mm::virt::translate(waiter.pagemap, waiter.wait_status_user_addr);
        if (PHYS != 0 && PHYS != ker::mod::mm::virt::PADDR_INVALID) {
            auto* status = reinterpret_cast<int32_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
            *status = job_control_stop_status(signo);
        }
    }

    uint64_t const WAITER_LOCK_FLAGS = stopped.exit_waiters_lock.lock_irqsave();
    (void)stopped.awaitee_on_exit.remove(waiter.pid);
    stopped.exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);

    waiter.context.regs.rax = stopped.pid;
    clear_waitpid_wait_state(waiter);
    waiter.deferred_task_switch = false;
    waiter.set_voluntary_blocked(false);
    waiter.wants_block = false;
    stopped.jobctl_stop_pending.store(false, std::memory_order_release);
    ker::mod::sched::reschedule_task_for_cpu(waiter.cpu, &waiter);
    return true;
}

void notify_parent_of_job_control_stop(Task& stopped, int signo) {
    if (stopped.parent_pid == 0) {
        return;
    }

    auto* parent = ker::mod::sched::find_task_by_pid_safe(stopped.parent_pid);
    if (parent == nullptr) {
        return;
    }

    parent->sig_pending |= (1ULL << (WOS_SIGCHLD - 1));
    bool const COMPLETED_WAIT = complete_waitpid_stop_waiter(*parent, stopped, signo);
    if (!COMPLETED_WAIT) {
        ker::mod::sched::wake_task_for_signal(parent);
    }
    parent->release();
}

void park_current_job_control_stop(Task& task) {
    while (task.jobctl_stopped.load(std::memory_order_acquire) && !task.has_exited) {
        sched::preemptible_syscall_park("job_stop", sched::task::WaitChannelKind::GENERIC);
    }
}

auto apply_job_control_stop(Task* task, int signo, bool park_current) -> bool {
    if (task == nullptr) {
        return true;
    }

    task->jobctl_stop_signal = static_cast<uint32_t>(signo);
    task->jobctl_stopped.store(true, std::memory_order_release);
    task->jobctl_stop_pending.store(true, std::memory_order_release);
    task->set_wait_channel("job_stop", sched::task::WaitChannelKind::GENERIC);
    task->set_voluntary_blocked(true);
    notify_parent_of_job_control_stop(*task, signo);

    if (park_current && sched::get_current_task() == task) {
        park_current_job_control_stop(*task);
    } else if (sched::get_current_task() == task) {
        task->deferred_task_switch = true;
    } else {
        static_cast<void>(sched::debug_stop_task(task));
    }
    return true;
}

auto handle_non_user_signal_action(Task* task, int signo, unsigned idx, const Task::SigHandler& handler, bool park_current) -> bool {
    if (handler.handler == WOS_SIG_DFL) {
        task->sig_pending &= ~(1ULL << idx);
        if (signo == WOS_SIGCHLD || signo == WOS_SIGURG || signo == WOS_SIGWINCH || signo == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
                sync_task_signal_mask_cache(task);
            }
            return true;
        }
        if (is_job_control_stop_signal(signo)) {
            return apply_job_control_stop(task, signo, park_current);
        }

        ker::syscall::process::wos_proc_exit_signal(signo);
        __builtin_unreachable();
    }

    if (handler.handler == WOS_SIG_IGN) {
        task->sig_pending &= ~(1ULL << idx);
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
            sync_task_signal_mask_cache(task);
        }
        return true;
    }

    return false;
}

}  // namespace

// Called from syscall.asm after the deferred task switch check, before returning
// to userspace. Handles both sigreturn (context restore) and signal delivery.
//
// stack_base: pointer to RSP at the bottom of pushed GP registers (from pushq).
extern "C" auto check_pending_signals(uint8_t* stack_base) -> uint64_t {
    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return 0;
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
        task->sig_mask = frame.saved_mask;
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
    uint64_t const DELIVERABLE = task->sig_pending & ~task->sig_mask;
    if (DELIVERABLE == 0) {
        return 0;
    }

    // Find the first pending signal (1-based)
    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);

    auto& handler = task->sig_handlers.at(IDX);

    // Handle SIG_DFL
    if (handler.handler == WOS_SIG_DFL) {
        task->sig_pending &= ~(1ULL << IDX);
        // Default action depends on signal:
        // SIGCHLD, SIGURG, SIGWINCH, SIGCONT: ignore
        // SIGTSTP(20), SIGTTIN(21), SIGTTOU(22): stop (for job control)
        // SIGKILL, SIGTERM, SIGINT, etc: terminate
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
                sync_task_signal_mask_cache(task);
            }
            return 0;  // Ignore
        }
        // Uncatchable stop signal
        if (is_job_control_stop_signal(SIGNO)) {
            static_cast<void>(apply_job_control_stop(task, SIGNO, true));
            return 0;
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
    if (handler.handler == WOS_SIG_IGN) {
        task->sig_pending &= ~(1ULL << IDX);
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
            sync_task_signal_mask_cache(task);
        }
        return 0;
    }

    if (!user_signal_target_valid(handler)) {
        task->sig_pending &= ~(1ULL << IDX);
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    // --- Deliver the signal: set up signal frame on user stack ---
    uint64_t const USER_RSP = live_syscall_user_rsp();
    uint64_t const USER_RIP = live_syscall_return_rip();
    uint64_t const USER_RFLAGS = live_syscall_return_flags();

    // Preserve the SysV user red zone; mlibc and clang-generated leaf code may
    // keep live locals in the 128 bytes below RSP across syscalls.
    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, USER_RSP, handler.flags);
    SignalFrame frame{};

    frame.pretcode = handler.restorer;  // sa_restorer trampoline
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
        handle_signal_frame_fault(task);
        return 0;
    }
    consume_signal_frame_saved_mask(*task);
    task->sig_pending &= ~(1ULL << IDX);

    // Modify the on-stack registers to redirect to the signal handler
    stack_write(stack_base, STACK_OFF_RCX, handler.handler);               // sysret target = handler
    stack_write(stack_base, STACK_OFF_RDI, static_cast<uint64_t>(SIGNO));  // arg1 = signo

    // Update the stored task scratch and the live GS scratch.  The latter is
    // what syscall.asm checks immediately before SYSRET.
    write_task_syscall_return(*task, FRAME_ADDR, handler.handler, USER_RFLAGS);
    write_live_syscall_return(FRAME_ADDR, handler.handler, USER_RFLAGS);

    // Block additional signals during handler execution
    // Block the handler's sa_mask + the signal itself (unless SA_NODEFER)
    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {  // SA_NODEFER = 0x40000000
        task->sig_mask |= (1ULL << IDX);
    }
    sync_task_signal_mask_cache(task);

    task->in_signal_handler = true;
    return 0;
}

void check_pending_signals_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame) {
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }

    // Only deliver on a direct return to userspace. Voluntary-blocked tasks
    // can carry a kernel-mode context in frame/gpr and must use the existing
    // deferred resume path instead.
    if ((frame.cs & 0x3) != 0x3 || task->is_voluntary_blocked()) {
        return;
    }

    if (task->in_signal_handler) {
        return;
    }

    uint64_t const DELIVERABLE = task->sig_pending & ~task->sig_mask;
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);

    auto& handler = task->sig_handlers.at(IDX);

    if (handler.handler == WOS_SIG_DFL) {
        task->sig_pending &= ~(1ULL << IDX);
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
                sync_task_signal_mask_cache(task);
            }
            return;
        }
        if (is_job_control_stop_signal(SIGNO)) {
            static_cast<void>(apply_job_control_stop(task, SIGNO, false));
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

    if (handler.handler == WOS_SIG_IGN) {
        task->sig_pending &= ~(1ULL << IDX);
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
            sync_task_signal_mask_cache(task);
        }
        return;
    }

    if (!user_signal_target_valid(handler)) {
        task->sig_pending &= ~(1ULL << IDX);
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, handler.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = handler.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    const auto* regs_arr = reinterpret_cast<const uint64_t*>(&gpr);
    for (int i = 0; i < 15; i++) {
        sigframe.saved_regs.at(static_cast<size_t>(i)) = regs_arr[i];
    }

    if (!write_signal_frame(*task, FRAME_ADDR, sigframe)) {
        handle_signal_frame_fault(task);
        return;
    }
    consume_signal_frame_saved_mask(*task);
    task->sig_pending &= ~(1ULL << IDX);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = handler.handler;
    frame.rsp = FRAME_ADDR;

    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {
        task->sig_mask |= (1ULL << IDX);
    }
    sync_task_signal_mask_cache(task);
    task->in_signal_handler = true;
}

void check_pending_signals_handoff(sched::task::Task* task, cpu::GPRegs& gpr, gates::InterruptFrame& frame) {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }

    if ((frame.cs & 0x3) != 0x3 || task->is_voluntary_blocked()) {
        return;
    }

    if (task->in_signal_handler) {
        return;
    }

    uint64_t const DELIVERABLE = task->sig_pending & ~task->sig_mask;
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);
    auto& handler = task->sig_handlers.at(IDX);

    if (handle_non_user_signal_action(task, SIGNO, IDX, handler, false)) {
        return;
    }
    if (!user_signal_target_valid(handler)) {
        task->sig_pending &= ~(1ULL << IDX);
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, handler.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = handler.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    const auto* regs_arr = reinterpret_cast<const uint64_t*>(&gpr);
    for (int i = 0; i < 15; i++) {
        sigframe.saved_regs.at(static_cast<size_t>(i)) = regs_arr[i];
    }

    if (!write_signal_frame(*task, FRAME_ADDR, sigframe)) {
        handle_signal_frame_fault(task);
        return;
    }
    consume_signal_frame_saved_mask(*task);
    task->sig_pending &= ~(1ULL << IDX);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = handler.handler;
    frame.rsp = FRAME_ADDR;

    write_task_syscall_return(*task, FRAME_ADDR, handler.handler, frame.flags);

    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {
        task->sig_mask |= (1ULL << IDX);
    }
    sync_task_signal_mask_cache(task);
    task->in_signal_handler = true;
    task->context.regs = gpr;
    task->context.frame = frame;
}

void check_pending_signals_deferred(sched::task::Task* task, DeferredSignalDelivery delivery) {
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS) {
        return;
    }

    // A PROCESS can be resumed here with a saved kernel frame when it was
    // preempted at a voluntary syscall wait point. Signal frames must only be
    // built on a real user stack; the syscall/interrupt return paths will
    // deliver the signal once the task reaches a user-mode return boundary.
    if ((task->context.frame.cs & 0x3) != 0x3 || task->is_voluntary_blocked()) {
        return;
    }

    if (task->in_signal_handler) {
        return;
    }

    uint64_t const DELIVERABLE = task->sig_pending & ~task->sig_mask;
    if (DELIVERABLE == 0) {
        return;
    }

    int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
    auto const IDX = static_cast<unsigned>(SIGNO - 1);
    auto& handler = task->sig_handlers.at(IDX);

    if (delivery == DeferredSignalDelivery::USER_HANDLERS_ONLY && !is_user_signal_handler(handler)) {
        return;
    }
    if (is_user_signal_handler(handler) && !user_signal_target_valid(handler)) {
        task->sig_pending &= ~(1ULL << IDX);
        ker::syscall::process::wos_proc_exit_signal(WOS_SIGSEGV);
        __builtin_unreachable();
    }

    if (handler.handler == WOS_SIG_DFL) {
        task->sig_pending &= ~(1ULL << IDX);
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
                sync_task_signal_mask_cache(task);
            }
            return;
        }
        if (is_job_control_stop_signal(SIGNO)) {
            static_cast<void>(apply_job_control_stop(task, SIGNO, false));
            return;
        }

        ker::syscall::process::wos_proc_exit_signal(SIGNO);
        __builtin_unreachable();
    }

    if (handler.handler == WOS_SIG_IGN) {
        task->sig_pending &= ~(1ULL << IDX);
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
            sync_task_signal_mask_cache(task);
        }
        return;
    }

    auto& gpr = task->context.regs;
    auto& frame = task->context.frame;
    uint64_t const FRAME_ADDR = signal_frame_address_for_task(*task, frame.rsp, handler.flags);
    SignalFrame sigframe{};

    sigframe.pretcode = handler.restorer;
    sigframe.signo = static_cast<uint64_t>(SIGNO);
    sigframe.saved_mask = signal_frame_saved_mask_value(*task);
    sigframe.saved_rip = frame.rip;
    sigframe.saved_rsp = frame.rsp;
    sigframe.saved_rflags = frame.flags;
    sigframe.saved_retval = gpr.rax;

    const auto* regs_arr = reinterpret_cast<const uint64_t*>(&gpr);
    for (int i = 0; i < 15; i++) {
        sigframe.saved_regs.at(static_cast<size_t>(i)) = regs_arr[i];
    }

    if (!write_signal_frame(*task, FRAME_ADDR, sigframe)) {
        handle_signal_frame_fault(task);
        return;
    }
    consume_signal_frame_saved_mask(*task);
    task->sig_pending &= ~(1ULL << IDX);

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = handler.handler;
    frame.rsp = FRAME_ADDR;
    write_task_syscall_return(*task, FRAME_ADDR, handler.handler, frame.flags);

    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {
        task->sig_mask |= (1ULL << IDX);
    }
    sync_task_signal_mask_cache(task);
    task->in_signal_handler = true;
}

}  // namespace ker::mod::sys::signal
