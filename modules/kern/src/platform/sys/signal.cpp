#include "signal.hpp"

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

        // Restore signal mask
        task->sig_mask = frame.saved_mask;

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
            }
            return 0;  // Ignore
        }
        // Uncatchable stop signal
        if (SIGNO == WOS_SIGSTOP) {
            task->set_voluntary_blocked(true);
            return 0;
        }
        // Job control stop signals: default action is to stop the process
        if (SIGNO == 20 || SIGNO == 21 || SIGNO == 22) {  // SIGTSTP, SIGTTIN, SIGTTOU
            // Block the task so the scheduler won't run it until SIGCONT
            task->set_voluntary_blocked(true);
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
            }
            return;
        }
        if (SIGNO == WOS_SIGSTOP) {
            task->set_voluntary_blocked(true);
            return;
        }
        if (SIGNO == 20 || SIGNO == 21 || SIGNO == 22) {
            task->set_voluntary_blocked(true);
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

    if (!is_user_signal_handler(handler)) {
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
    task->in_signal_handler = true;
    task->context.regs = gpr;
    task->context.frame = frame;
}

}  // namespace ker::mod::sys::signal
