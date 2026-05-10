#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"

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

// Deliver one pending signal when an interrupt/scheduler path is about to
// return directly to userspace via iretq.
void check_pending_signals_interrupt(cpu::GPRegs& gpr, gates::InterruptFrame& frame);

}  // namespace ker::mod::sys::signal
