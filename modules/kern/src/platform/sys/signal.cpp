#include "signal.hpp"

#include <cstdint>
#include <cstring>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "syscalls_impl/process/exit.hpp"

namespace {

// Signal constants (matching Linux ABI)
constexpr uint64_t WOS_SIG_DFL = 0;
constexpr uint64_t WOS_SIG_IGN = 1;
constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGHUP = 1;
constexpr int WOS_SIGINT = 2;
constexpr int WOS_SIGQUIT = 3;
constexpr int WOS_SIGTERM = 15;
constexpr int WOS_SIGSTOP = 19;
constexpr int WOS_SIGCHLD = 17;
constexpr int WOS_SIGURG = 23;
constexpr int WOS_SIGWINCH = 28;
constexpr int WOS_SIGCONT = 18;

// Stack offsets for the pushed GP registers in syscall.asm (pushq macro).
// After pushq, RSP points to r15. Offsets from RSP:
//   0x00=r15, 0x08=r14, 0x10=r13, 0x18=r12, 0x20=r11, 0x28=r10,
//   0x30=r9,  0x38=r8,  0x40=rbp, 0x48=rdi, 0x50=rsi, 0x58=rdx,
//   0x60=rcx, 0x68=rbx, 0x70=rax, 0x78=return_value
constexpr auto STACK_OFF_RDI = 0x48;
constexpr auto STACK_OFF_RCX = 0x60;
constexpr auto STACK_OFF_RAX = 0x70;
constexpr auto STACK_OFF_RETVAL = 0x78;

// Helper: read a uint64_t from a stack slot
auto stack_read(const uint8_t* base, int offset) -> uint64_t { return *reinterpret_cast<const uint64_t*>(base + offset); }

// Helper: write a uint64_t to a stack slot
void stack_write(uint8_t* base, int offset, uint64_t value) { *reinterpret_cast<uint64_t*>(base + offset) = value; }

}  // namespace

namespace ker::mod::sys::signal {

// Called from syscall.asm after the deferred task switch check, before returning
// to userspace. Handles both sigreturn (context restore) and signal delivery.
//
// stack_base: pointer to RSP at the bottom of pushed GP registers (from pushq).
extern "C" auto check_pending_signals(uint8_t* stack_base) -> uint64_t {
    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return 0;
    }

    // Get PerCpu struct to read/write user RSP, RIP, and RFLAGS
    auto* per_cpu = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);

    // --- Handle sigreturn first ---
    if (task->do_sigreturn) {
        task->do_sigreturn = false;

        // User RSP at sigreturn syscall entry points to &frame.signo
        // (restorer's `ret` already popped pretcode, so RSP = pretcode + 8 = &signo)
        uint64_t const USER_RSP = per_cpu->user_rsp;
        auto* frame_start = reinterpret_cast<uint8_t*>(USER_RSP - 8);  // back up to pretcode
        auto* frame = reinterpret_cast<SignalFrame*>(frame_start);

        // Restore signal mask
        task->sig_mask = frame->saved_mask;

        // Restore GP registers to the on-stack positions
        for (int i = 0; i < 15; i++) {
            stack_write(stack_base, i * 8, frame->saved_regs.at(static_cast<size_t>(i)));
        }

        // Restore the return value slot
        stack_write(stack_base, STACK_OFF_RETVAL, frame->saved_retval);
        stack_write(stack_base, STACK_OFF_RAX, frame->saved_retval);

        // Restore user RIP, RSP, and RFLAGS through PerCpu
        per_cpu->user_rsp = frame->saved_rsp;
        per_cpu->syscall_ret_rip = frame->saved_rip;
        per_cpu->syscall_ret_flags = frame->saved_rflags;

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

    // Clear the pending bit
    task->sig_pending &= ~(1ULL << IDX);

    auto& handler = task->sig_handlers.at(IDX);

    // Handle SIG_DFL
    if (handler.handler == WOS_SIG_DFL) {
        // Default action depends on signal:
        // SIGCHLD, SIGURG, SIGWINCH, SIGCONT: ignore
        // SIGTSTP(20), SIGTTIN(21), SIGTTOU(22): stop (for job control)
        // SIGKILL, SIGTERM, SIGINT, etc: terminate
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
            }
            return 0;  // Ignore
        }
        // Uncatchable stop signal
        if (SIGNO == WOS_SIGSTOP) {
            task->voluntary_block = true;
            return 0;
        }
        // Job control stop signals: default action is to stop the process
        if (SIGNO == 20 || SIGNO == 21 || SIGNO == 22) {  // SIGTSTP, SIGTTIN, SIGTTOU
            // Block the task so the scheduler won't run it until SIGCONT
            task->voluntary_block = true;
            return 0;
        }
        // Default terminate semantics for normal fatal signals.
        // Shells expect status 128 + signo for signal-terminated tasks.
        if (SIGNO == WOS_SIGHUP || SIGNO == WOS_SIGINT || SIGNO == WOS_SIGQUIT || SIGNO == WOS_SIGTERM || SIGNO == WOS_SIGKILL ||
            SIGNO == 13 || SIGNO == 11 || SIGNO == 6 || SIGNO == 8 || SIGNO == 4 || SIGNO == 7) {
            ker::syscall::process::wos_proc_exit(128 + SIGNO);
            __builtin_unreachable();
        }

        // Conservative fallback: terminate by signal number if action is unknown.
        ker::syscall::process::wos_proc_exit(128 + SIGNO);
        __builtin_unreachable();
    }

    // Handle SIG_IGN
    if (handler.handler == WOS_SIG_IGN) {
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
        }
        return 0;
    }

    // --- Deliver the signal: set up signal frame on user stack ---
    uint64_t const USER_RSP = per_cpu->user_rsp;
    uint64_t const USER_RIP = per_cpu->syscall_ret_rip;
    uint64_t const USER_RFLAGS = per_cpu->syscall_ret_flags;

    // Preserve the SysV user red zone; mlibc and clang-generated leaf code may
    // keep live locals in the 128 bytes below RSP across syscalls.
    uint64_t const FRAME_ADDR = signal_frame_address(USER_RSP);
    auto* frame = reinterpret_cast<SignalFrame*>(FRAME_ADDR);

    // Write the signal frame to user-space stack
    // (We're still in the task's pagemap during syscall, so direct writes work)
    frame->pretcode = handler.restorer;  // sa_restorer trampoline
    frame->signo = static_cast<uint64_t>(SIGNO);
    frame->saved_mask = task->signal_frame_saved_mask();
    frame->saved_rip = USER_RIP;
    frame->saved_rsp = USER_RSP;
    frame->saved_rflags = USER_RFLAGS;
    frame->saved_retval = stack_read(stack_base, STACK_OFF_RETVAL);

    // Save all 15 GP registers from the stack
    for (int i = 0; i < 15; i++) {
        frame->saved_regs.at(static_cast<size_t>(i)) = stack_read(stack_base, i * 8);
    }

    // Modify the on-stack registers to redirect to the signal handler
    stack_write(stack_base, STACK_OFF_RCX, handler.handler);               // sysret target = handler
    stack_write(stack_base, STACK_OFF_RDI, static_cast<uint64_t>(SIGNO));  // arg1 = signo

    // Update PerCpu for the modified return path
    per_cpu->user_rsp = FRAME_ADDR;              // new user stack (with frame)
    per_cpu->syscall_ret_rip = handler.handler;  // update diagnostic check value

    // Block additional signals during handler execution
    // Block the handler's sa_mask + the signal itself (unless SA_NODEFER)
    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {  // SA_NODEFER = 0x40000000
        task->sig_mask |= (1ULL << IDX);
    }

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
    if ((frame.cs & 0x3) != 0x3 || task->voluntary_block) {
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
    task->sig_pending &= ~(1ULL << IDX);

    auto& handler = task->sig_handlers.at(IDX);

    if (handler.handler == WOS_SIG_DFL) {
        if (SIGNO == WOS_SIGCHLD || SIGNO == WOS_SIGURG || SIGNO == WOS_SIGWINCH || SIGNO == WOS_SIGCONT) {
            if (task->sigsuspend_active) {
                task->sig_mask = task->signal_frame_saved_mask();
            }
            return;
        }
        if (SIGNO == WOS_SIGSTOP) {
            task->voluntary_block = true;
            return;
        }
        if (SIGNO == 20 || SIGNO == 21 || SIGNO == 22) {
            task->voluntary_block = true;
            return;
        }
        if (SIGNO == WOS_SIGHUP || SIGNO == WOS_SIGINT || SIGNO == WOS_SIGQUIT || SIGNO == WOS_SIGTERM || SIGNO == WOS_SIGKILL ||
            SIGNO == 13 || SIGNO == 11 || SIGNO == 6 || SIGNO == 8 || SIGNO == 4 || SIGNO == 7) {
            ker::syscall::process::wos_proc_exit(128 + SIGNO);
            __builtin_unreachable();
        }

        ker::syscall::process::wos_proc_exit(128 + SIGNO);
        __builtin_unreachable();
    }

    if (handler.handler == WOS_SIG_IGN) {
        if (task->sigsuspend_active) {
            task->sig_mask = task->signal_frame_saved_mask();
        }
        return;
    }

    uint64_t const FRAME_ADDR = signal_frame_address(frame.rsp);
    auto* sigframe = reinterpret_cast<SignalFrame*>(FRAME_ADDR);

    sigframe->pretcode = handler.restorer;
    sigframe->signo = static_cast<uint64_t>(SIGNO);
    sigframe->saved_mask = task->signal_frame_saved_mask();
    sigframe->saved_rip = frame.rip;
    sigframe->saved_rsp = frame.rsp;
    sigframe->saved_rflags = frame.flags;
    sigframe->saved_retval = gpr.rax;

    const auto* regs_arr = reinterpret_cast<const uint64_t*>(&gpr);
    for (int i = 0; i < 15; i++) {
        sigframe->saved_regs.at(static_cast<size_t>(i)) = regs_arr[i];
    }

    gpr.rdi = static_cast<uint64_t>(SIGNO);
    frame.rip = handler.handler;
    frame.rsp = FRAME_ADDR;

    task->sig_mask |= handler.mask;
    if ((handler.flags & 0x40000000ULL) == 0U) {
        task->sig_mask |= (1ULL << IDX);
    }
    task->in_signal_handler = true;
}

}  // namespace ker::mod::sys::signal
