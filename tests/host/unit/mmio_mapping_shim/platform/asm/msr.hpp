#pragma once

#include <cstdint>

inline constexpr uint32_t IA32_APIC_BASE = 0x1b;

// Finite instruction input for the actual cache-policy collector. The composed
// hardware-port TU provides this definition; no host RDMSR is compiled here.
void cpu_get_msr(uint32_t index, uint64_t* output);
