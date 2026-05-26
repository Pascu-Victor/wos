#pragma once

#include <cstdint>
#include <defines/defines.hpp>

inline constexpr uint32_t CPUID_FEAT_EDX_FPU = uint32_t{1} << 0;
inline constexpr uint32_t CPUID_FEAT_EDX_VME = uint32_t{1} << 1;
inline constexpr uint32_t CPUID_FEAT_EDX_DE = uint32_t{1} << 2;
inline constexpr uint32_t CPUID_FEAT_EDX_PSE = uint32_t{1} << 3;
inline constexpr uint32_t CPUID_FEAT_EDX_TSC = uint32_t{1} << 4;
inline constexpr uint32_t CPUID_FEAT_EDX_MSR = uint32_t{1} << 5;
inline constexpr uint32_t CPUID_FEAT_EDX_PAE = uint32_t{1} << 6;
inline constexpr uint32_t CPUID_FEAT_EDX_MCE = uint32_t{1} << 7;
inline constexpr uint32_t CPUID_FEAT_EDX_CX8 = uint32_t{1} << 8;
inline constexpr uint32_t CPUID_FEAT_EDX_APIC = uint32_t{1} << 9;
inline constexpr uint32_t CPUID_FEAT_EDX_SEP = uint32_t{1} << 11;
inline constexpr uint32_t CPUID_FEAT_EDX_MTRR = uint32_t{1} << 12;
inline constexpr uint32_t CPUID_FEAT_EDX_PGE = uint32_t{1} << 13;
inline constexpr uint32_t CPUID_FEAT_EDX_MCA = uint32_t{1} << 14;
inline constexpr uint32_t CPUID_FEAT_EDX_CMOV = uint32_t{1} << 15;
inline constexpr uint32_t CPUID_FEAT_EDX_PAT = uint32_t{1} << 16;
inline constexpr uint32_t CPUID_FEAT_EDX_PSE36 = uint32_t{1} << 17;
inline constexpr uint32_t CPUID_FEAT_EDX_PSN = uint32_t{1} << 18;
inline constexpr uint32_t CPUID_FEAT_EDX_CLF = uint32_t{1} << 19;
inline constexpr uint32_t CPUID_FEAT_EDX_DTES = uint32_t{1} << 21;
inline constexpr uint32_t CPUID_FEAT_EDX_ACPI = uint32_t{1} << 22;
inline constexpr uint32_t CPUID_FEAT_EDX_MMX = uint32_t{1} << 23;
inline constexpr uint32_t CPUID_FEAT_EDX_FXSR = uint32_t{1} << 24;
inline constexpr uint32_t CPUID_FEAT_EDX_SSE = uint32_t{1} << 25;
inline constexpr uint32_t CPUID_FEAT_EDX_SSE2 = uint32_t{1} << 26;
inline constexpr uint32_t CPUID_FEAT_EDX_SS = uint32_t{1} << 27;
inline constexpr uint32_t CPUID_FEAT_EDX_HTT = uint32_t{1} << 28;
inline constexpr uint32_t CPUID_FEAT_EDX_TM = uint32_t{1} << 29;
inline constexpr uint32_t CPUID_FEAT_EDX_PBE = uint32_t{1} << 31;

inline constexpr uint32_t IA32_KERNEL_GS_BASE = 0xC0000102;
inline constexpr uint32_t IA32_GS_BASE = 0xC0000101;
inline constexpr uint32_t IA32_FS_BASE = 0xC0000100;
inline constexpr uint32_t IA32_EFER = 0xC0000080;
inline constexpr uint32_t IA32_STAR = 0xC0000081;
inline constexpr uint32_t IA32_LSTAR = 0xC0000082;
inline constexpr uint32_t IA32_FMASK = 0xC0000084;
inline constexpr uint32_t IA32_CSTAR = 0xC0000083;

inline constexpr uint32_t IA32_APIC_BASE = 0x1B;
// NOLINTBEGIN(readability-non-const-parameter)
static inline void cpuid(uint32_t eax, uint32_t* eax_out, uint32_t* edx_out) {
    asm volatile("cpuid" : "=a"(*eax_out), "=d"(*edx_out) : "a"(eax) : "ebx", "ecx");
}

static inline void cpu_set_msr(uint32_t msr, uint32_t eax, uint32_t edx) { asm volatile("wrmsr" : : "c"(msr), "a"(eax), "d"(edx)); }

static inline void cpu_set_msr(uint32_t msr, uint64_t value) { cpu_set_msr(msr, value & 0xFFFFFFFF, value >> 32); }

static inline void cpu_get_msr(uint32_t msr, uint32_t* rax, uint32_t* rdx) { asm volatile("rdmsr" : "=a"(*rax), "=d"(*rdx) : "c"(msr)); }

static inline void cpu_get_msr(uint32_t msr, uint64_t* out) {
    uint32_t eax = 0;
    uint32_t edx = 0;
    cpu_get_msr(msr, &eax, &edx);
    *out = (static_cast<uint64_t>(edx) << 32) | eax;
}

static inline uint64_t rdtsc() {
    // Written by inline asm output constraints.
    // NOLINTBEGIN(misc-const-correctness)
    uint32_t lo = 0;
    uint32_t hi = 0;
    // NOLINTEND(misc-const-correctness)
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<uint64_t>(hi) << 32) | lo;
}
// NOLINTEND(readability-non-const-parameter)
