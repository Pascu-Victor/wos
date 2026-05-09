#pragma once

#include <cstdint>
#include <defines/defines.hpp>

enum {
    CPUID_FEAT_EDX_FPU = (1 << 0),
    CPUID_FEAT_EDX_VME = (1 << 1),
    CPUID_FEAT_EDX_DE = (1 << 2),
    CPUID_FEAT_EDX_PSE = (1 << 3),
    CPUID_FEAT_EDX_TSC = (1 << 4),
    CPUID_FEAT_EDX_MSR = (1 << 5),
    CPUID_FEAT_EDX_PAE = (1 << 6),
    CPUID_FEAT_EDX_MCE = (1 << 7),
    CPUID_FEAT_EDX_CX8 = (1 << 8),
    CPUID_FEAT_EDX_APIC = (1 << 9),
    CPUID_FEAT_EDX_SEP = (1 << 11),
    CPUID_FEAT_EDX_MTRR = (1 << 12),
    CPUID_FEAT_EDX_PGE = (1 << 13),
    CPUID_FEAT_EDX_MCA = (1 << 14),
    CPUID_FEAT_EDX_CMOV = (1 << 15),
    CPUID_FEAT_EDX_PAT = (1 << 16),
    CPUID_FEAT_EDX_PSE36 = (1 << 17),
    CPUID_FEAT_EDX_PSN = (1 << 18),
    CPUID_FEAT_EDX_CLF = (1 << 19),
    CPUID_FEAT_EDX_DTES = (1 << 21),
    CPUID_FEAT_EDX_ACPI = (1 << 22),
    CPUID_FEAT_EDX_MMX = (1 << 23),
    CPUID_FEAT_EDX_FXSR = (1 << 24),
    CPUID_FEAT_EDX_SSE = (1 << 25),
    CPUID_FEAT_EDX_SSE2 = (1 << 26),
    CPUID_FEAT_EDX_SS = (1 << 27),
    CPUID_FEAT_EDX_HTT = (1 << 28),
    CPUID_FEAT_EDX_TM = (1 << 29),
    CPUID_FEAT_EDX_PBE = (1 << 31)
};

enum {
    IA32_KERNEL_GS_BASE = 0xC0000102,
    IA32_GS_BASE = 0xC0000101,
    IA32_FS_BASE = 0xC0000100,
    IA32_EFER = 0xC0000080,
    IA32_STAR = 0xC0000081,
    IA32_LSTAR = 0xC0000082,
    IA32_FMASK = 0xC0000084,
    IA32_CSTAR = 0xC0000083
};

enum { IA32_APIC_BASE = 0x1B };
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
    uint32_t lo = 0;
    uint32_t hi = 0;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return (static_cast<uint64_t>(hi) << 32) | lo;
}
// NOLINTEND(readability-non-const-parameter)
