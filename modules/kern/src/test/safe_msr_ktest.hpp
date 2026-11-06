#pragma once

namespace ker::test {
// Isolated BSP fixture after local IDT/APIC setup and selftest AP parking.
// Invokes the same two registered KTEST bodies; returns only when both pass.
void run_safe_msr_selftest();
}  // namespace ker::test
