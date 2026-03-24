#include "signal.hpp"

#include <cstring>

#include "platform/asm/cpu.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "syscalls_impl/process/exit.hpp"

// Signal constants (matching Linux ABI)
static constexpr uint64_t WOS_SIG_DFL = 0;
static constexpr uint64_t WOS_SIG_IGN = 1;
static constexpr int WOS_SIGKILL = 9;
static constexpr int WOS_SIGHUP = 1;
static constexpr int WOS_SIGINT = 2;
static constexpr int WOS_SIGQUIT = 3;
static constexpr int WOS_SIGTERM = 15;
static constexpr int WOS_SIGSTOP = 19;
static constexpr int WOS_SIGCHLD = 17;
static constexpr int WOS_SIGURG = 23;
static constexpr int WOS_SIGWINCH = 28;
static constexpr int WOS_SIGCONT = 18;

// Stack offsets for the pushed GP registers in syscall.asm (pushq macro).
// After pushq, RSP points to r15. Offsets from RSP:
//   0x00=r15, 0x08=r14, 0x10=r13, 0x18=r12, 0x20=r11, 0x28=r10,
//   0x30=r9,  0x38=r8,  0x40=rbp, 0x48=rdi, 0x50=rsi, 0x58=rdx,
//   0x60=rcx, 0x68=rbx, 0x70=rax, 0x78=return_value
constexpr auto STACK_OFF_R15 = 0x00;
constexpr auto STACK_OFF_R14 = 0x08;
constexpr auto STACK_OFF_R13 = 0x10;
constexpr auto STACK_OFF_R12 = 0x18;
constexpr auto STACK_OFF_R11 = 0x20;
constexpr auto STACK_OFF_R10 = 0x28;
constexpr auto STACK_OFF_R9 = 0x30;
constexpr auto STACK_OFF_R8 = 0x38;
constexpr auto STACK_OFF_RBP = 0x40;
constexpr auto STACK_OFF_RDI = 0x48;
constexpr auto STACK_OFF_RSI = 0x50;
constexpr auto STACK_OFF_RDX = 0x58;
constexpr auto STACK_OFF_RCX = 0x60;
constexpr auto STACK_OFF_RBX = 0x68;
constexpr auto STACK_OFF_RAX = 0x70;
constexpr auto STACK_OFF_RETVAL = 0x78;

namespace ker::mod::sys::signal {

// Helper: read a uint64_t from a stack slot
static inline uint64_t stack_read(uint8_t* base, int offset) { return *reinterpret_cast<uint64_t*>(base + offset); }

// Helper: write a uint64_t to a stack slot
static inline void stack_write(uint8_t* base, int offset, uint64_t value) { *reinterpret_cast<uint64_t*>(base + offset) = value; }

// Called from syscall.asm after the deferred task switch check, before returning
// to userspace. Handles both sigreturn (context restore) and signal delivery.
//
// stack_base: pointer to RSP at the bottom of pushed GP registers (from pushq).
extern "C" void check_pending_signals(uint8_t* stack_base) {
    auto* task = sched::get_current_task();
    if (task == nullptr) return;

    // Get PerCpu struct to read/write user RSP, RIP, and RFLAGS
    auto* perCpu = reinterpret_cast<cpu::PerCpu*>(task->context.syscallScratchArea);

    // --- Handle sigreturn first ---
    if (task->doSigreturn) {
        task->doSigreturn = false;

        // User RSP at sigreturn syscall entry points to &frame.signo
        // (restorer's `ret` already popped pretcode, so RSP = pretcode + 8 = &signo)
        uint64_t userRsp = perCpu->userRsp;
        auto* frameStart = reinterpret_cast<uint8_t*>(userRsp - 8);  // back up to pretcode
        auto* frame = reinterpret_cast<SignalFrame*>(frameStart);

        // Restore signal mask
        task->sigMask = frame->saved_mask;

        // Restore GP registers to the on-stack positions
        for (int i = 0; i < 15; i++) {
            stack_write(stack_base, i * 8, frame->saved_regs[i]);
        }

        // Restore the return value slot
        stack_write(stack_base, STACK_OFF_RETVAL, frame->saved_retval);

        // Restore user RIP, RSP, and RFLAGS through PerCpu
        perCpu->userRsp = frame->saved_rsp;
        perCpu->syscallRetRip = frame->saved_rip;
        perCpu->syscallRetFlags = frame->saved_rflags;

        // Also update the on-stack RCX (used by sysret) and R11 (RFLAGS)
        stack_write(stack_base, STACK_OFF_RCX, frame->saved_rip);
        stack_write(stack_base, STACK_OFF_R11, frame->saved_rflags);

        task->inSignalHandler = false;
        return;  // Don't deliver new signals right after sigreturn
    }

    // --- Check for deliverable signals ---
    uint64_t deliverable = task->sigPending & ~task->sigMask;
    if (deliverable == 0) return;

    // Find the first pending signal (1-based)
    int signo = __builtin_ctzll(deliverable) + 1;
    unsigned idx = static_cast<unsigned>(signo - 1);

    // Clear the pending bit
    task->sigPending &= ~(1ULL << idx);

    auto& handler = task->sigHandlers[idx];

    // Handle SIG_DFL
    if (handler.handler == WOS_SIG_DFL) {
        // Default action depends on signal:
        // SIGCHLD, SIGURG, SIGWINCH, SIGCONT: ignore
        // SIGTSTP(20), SIGTTIN(21), SIGTTOU(22): stop (for job control)
        // SIGKILL, SIGTERM, SIGINT, etc: terminate
        if (signo == WOS_SIGCHLD || signo == WOS_SIGURG || signo == WOS_SIGWINCH || signo == WOS_SIGCONT) {
            return;  // Ignore
        }
        // Uncatchable stop signal
        if (signo == WOS_SIGSTOP) {
            task->voluntaryBlock = true;
            return;
        }
        // Job control stop signals: default action is to stop the process
        if (signo == 20 || signo == 21 || signo == 22) {  // SIGTSTP, SIGTTIN, SIGTTOU
            // Block the task so the scheduler won't run it until SIGCONT
            task->voluntaryBlock = true;
            return;
        }
        // Default terminate semantics for normal fatal signals.
        // Shells expect status 128 + signo for signal-terminated tasks.
        if (signo == WOS_SIGHUP || signo == WOS_SIGINT || signo == WOS_SIGQUIT || signo == WOS_SIGTERM || signo == WOS_SIGKILL ||
            signo == 13 || signo == 11 || signo == 6 || signo == 8 || signo == 4 || signo == 7) {
            ker::syscall::process::wos_proc_exit(128 + signo);
            __builtin_unreachable();
        }

        // Conservative fallback: terminate by signal number if action is unknown.
        ker::syscall::process::wos_proc_exit(128 + signo);
        __builtin_unreachable();
    }

    // Handle SIG_IGN
    if (handler.handler == WOS_SIG_IGN) {
        return;
    }

    // --- Deliver the signal: set up signal frame on user stack ---
    uint64_t user_rsp = perCpu->userRsp;
    uint64_t user_rip = perCpu->syscallRetRip;
    uint64_t user_rflags = perCpu->syscallRetFlags;

    // Compute frame location on user stack. make sure it is 16 byte aligned
    // for system V compat
    uint64_t frame_addr = ((user_rsp - sizeof(SignalFrame)) & ~0xFULL) - 8;
    auto* frame = reinterpret_cast<SignalFrame*>(frame_addr);

    // Write the signal frame to user-space stack
    // (We're still in the task's pagemap during syscall, so direct writes work)
    frame->pretcode = handler.restorer;  // sa_restorer trampoline
    frame->signo = static_cast<uint64_t>(signo);
    frame->saved_mask = task->sigMask;
    frame->saved_rip = user_rip;
    frame->saved_rsp = user_rsp;
    frame->saved_rflags = user_rflags;
    frame->saved_retval = stack_read(stack_base, STACK_OFF_RETVAL);

    // Save all 15 GP registers from the stack
    for (int i = 0; i < 15; i++) {
        frame->saved_regs[i] = stack_read(stack_base, i * 8);
    }

    // Modify the on-stack registers to redirect to the signal handler
    stack_write(stack_base, STACK_OFF_RCX, handler.handler);               // sysret target = handler
    stack_write(stack_base, STACK_OFF_RDI, static_cast<uint64_t>(signo));  // arg1 = signo

    // Update PerCpu for the modified return path
    perCpu->userRsp = frame_addr;             // new user stack (with frame)
    perCpu->syscallRetRip = handler.handler;  // update diagnostic check value

    // Block additional signals during handler execution
    // Block the handler's sa_mask + the signal itself (unless SA_NODEFER)
    task->sigMask |= handler.mask;
    if (!(handler.flags & 0x40000000ULL)) {  // SA_NODEFER = 0x40000000
        task->sigMask |= (1ULL << idx);
    }

    task->inSignalHandler = true;
}

}  // namespace ker::mod::sys::signal
