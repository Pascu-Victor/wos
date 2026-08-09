#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::sys::signal {

// Signal frame pushed onto user stack during signal delivery.
// Layout must be kept in sync with check_pending_signals / sigreturn.
struct SignalFrame {
    uint64_t pretcode{};                    // restorer address (handler's return address)
    uint64_t signo{};                       // signal number (1-based)
    uint64_t saved_mask{};                  // previous signal mask
    uint64_t saved_rip{};                   // original user RIP
    uint64_t saved_rsp{};                   // original user RSP
    uint64_t saved_rflags{};                // original RFLAGS
    uint64_t saved_retval{};                // original syscall return value (rax)
    std::array<uint64_t, 15> saved_regs{};  // raw GP register save (r15..rax, same order as stack)
};
// Total: 7*8 + 15*8 = 176 bytes
static_assert(sizeof(SignalFrame) == 176, "SignalFrame layout is userspace ABI");
static_assert(offsetof(SignalFrame, saved_regs) == 56, "SignalFrame saved_regs offset is userspace ABI");

constexpr uint64_t USER_RED_ZONE_SIZE = 128;
constexpr uint64_t WOS_SA_ONSTACK = 0x08000000;
constexpr uint32_t WOS_SS_ONSTACK = 1;
constexpr uint32_t WOS_SS_DISABLE = 2;
// mlibc's TCB signal-cache valid word ends at offset 0x4c.  Callers that
// publish a new thread under a subsystem mutex preflight this whole prefix so
// the in-lock refresh can remain mapped-only.
constexpr size_t WOS_TCB_SIGNAL_CACHE_BYTES = 0x4c;

enum class DeferredSignalDelivery : uint8_t {
    FULL,
    USER_HANDLERS_ONLY,
};

enum class DeferredSigreturnResult : uint8_t {
    NONE,
    RESTORED,
    FAULT,
};

constexpr auto signal_frame_address(uint64_t user_rsp) -> uint64_t {
    return ((user_rsp - USER_RED_ZONE_SIZE - sizeof(SignalFrame)) & ~0xFULL) - 8;
}

inline auto is_on_alt_stack(const sched::task::Task& task, uint64_t user_rsp) -> bool {
    uint64_t const START = task.sigaltstack_sp;
    uint64_t const END = START + task.sigaltstack_size;
    return START != 0 && END > START && user_rsp >= START && user_rsp < END;
}

inline auto signal_frame_address_for_task(const sched::task::Task& task, uint64_t user_rsp, uint64_t handler_flags) -> uint64_t {
    if ((handler_flags & WOS_SA_ONSTACK) == 0 || (task.sigaltstack_flags & WOS_SS_DISABLE) != 0 || is_on_alt_stack(task, user_rsp)) {
        return signal_frame_address(user_rsp);
    }

    uint64_t const ALT_TOP = task.sigaltstack_sp + task.sigaltstack_size;
    if (ALT_TOP <= task.sigaltstack_sp) {
        return signal_frame_address(user_rsp);
    }
    return signal_frame_address(ALT_TOP);
}

// Deliver one pending signal when an interrupt/scheduler path is about to
// return directly to userspace via iretq.
void check_pending_signals_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame);
auto deliver_synchronous_signal_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame, int signo) -> bool;
void check_pending_signals_handoff(sched::task::Task* task, cpu::GPRegs& gpr, gates::InterruptFrame& frame);
void check_pending_signals_deferred(sched::task::Task* task, DeferredSignalDelivery delivery);
auto restore_deferred_sigreturn(sched::task::Task* task) -> DeferredSigreturnResult;
void exit_current_on_pending_fatal_default_signal();
void sync_task_signal_mask_cache(sched::task::Task* task);
// Normal-context publication helper. It may fault in/COW the supplied TCB
// pages and reports failure so callers can avoid publishing an unusable base.
[[nodiscard]] auto sync_task_signal_mask_cache_at(sched::task::Task* task, uint64_t tcb) -> bool;
// Scheduler/interrupt/publication contexts must not fault in TCB pages.  A
// failed mapped-only refresh leaves the userspace cache invalid or stale; the
// authoritative mask remains in Task and a later safe refresh repairs it.
void sync_task_signal_mask_cache_mapped(sched::task::Task* task);

}  // namespace ker::mod::sys::signal
