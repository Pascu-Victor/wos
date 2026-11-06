#pragma once

#include <cstdint>

namespace ker::mod::gates {
struct InterruptFrame;
}

namespace ker::mod::cpu {

// Read an optional register after this CPU has loaded the WOS IDT. Absence of
// CPUID.MSR, an unready local IDT, or #GP(0) leaves output unchanged. This grants
// no authority to write MSRs or to recover any other faulting instruction.
[[nodiscard]] auto try_read_msr(uint32_t index, uint64_t& output) -> bool;

// The only recoverable instruction is the single out-of-line probe RDMSR.
// Interrupt entry passes the actual saved return RIP, never a by-value copy.
[[nodiscard]] auto try_recover_msr_read(uint64_t vector, uint64_t error, uint64_t cs, uint64_t& rip) -> bool;

#ifdef WOS_SELFTEST
// Explicit destructive fixture after CPUID.MSR/x2APIC, the local IDT and an
// actual APIC_BASE read prove x2APIC mode. Reading write-only SELF_IPI at this
// separate instruction must reach ordinary fatal #GP handling. A VMM ignoring
// that invalid access instead reaches UD2 and fails the qualification.
[[noreturn]] void selftest_unmatched_msr_fault();
#endif

}  // namespace ker::mod::cpu

extern "C" [[nodiscard]] auto wos_try_recover_msr_frame(ker::mod::gates::InterruptFrame* frame) -> bool;
