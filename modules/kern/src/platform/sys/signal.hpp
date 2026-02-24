#pragma once

#include <cstdint>

namespace ker::mod::sys::signal {

// Signal frame pushed onto user stack during signal delivery.
// Layout must be kept in sync with check_pending_signals / sigreturn.
struct SignalFrame {
    uint64_t pretcode;        // restorer address (handler's return address)
    uint64_t signo;           // signal number (1-based)
    uint64_t saved_mask;      // previous signal mask
    uint64_t saved_rip;       // original user RIP
    uint64_t saved_rsp;       // original user RSP
    uint64_t saved_rflags;    // original RFLAGS
    uint64_t saved_retval;    // original syscall return value (rax)
    uint64_t saved_regs[15];  // raw GP register save (r15..rax, same order as stack)
};
// Total: 7*8 + 15*8 = 176 bytes

}  // namespace ker::mod::sys::signal
