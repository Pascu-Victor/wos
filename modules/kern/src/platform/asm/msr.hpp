#pragma once

#include <defines/defines.hpp>

#define CPUID_FEAT_EDX_FPU (1 << 0)
#define CPUID_FEAT_EDX_VME (1 << 1)
#define CPUID_FEAT_EDX_DE (1 << 2)
#define CPUID_FEAT_EDX_PSE (1 << 3)
#define CPUID_FEAT_EDX_TSC (1 << 4)
#define CPUID_FEAT_EDX_MSR (1 << 5)
#define CPUID_FEAT_EDX_PAE (1 << 6)
#define CPUID_FEAT_EDX_MCE (1 << 7)
#define CPUID_FEAT_EDX_CX8 (1 << 8)
#define CPUID_FEAT_EDX_APIC (1 << 9)
#define CPUID_FEAT_EDX_SEP (1 << 11)
#define CPUID_FEAT_EDX_MTRR (1 << 12)
#define CPUID_FEAT_EDX_PGE (1 << 13)
#define CPUID_FEAT_EDX_MCA (1 << 14)
#define CPUID_FEAT_EDX_CMOV (1 << 15)
#define CPUID_FEAT_EDX_PAT (1 << 16)
#define CPUID_FEAT_EDX_PSE36 (1 << 17)
#define CPUID_FEAT_EDX_PSN (1 << 18)
#define CPUID_FEAT_EDX_CLF (1 << 19)
#define CPUID_FEAT_EDX_DTES (1 << 21)
#define CPUID_FEAT_EDX_ACPI (1 << 22)
#define CPUID_FEAT_EDX_MMX (1 << 23)
#define CPUID_FEAT_EDX_FXSR (1 << 24)
#define CPUID_FEAT_EDX_SSE (1 << 25)
#define CPUID_FEAT_EDX_SSE2 (1 << 26)
#define CPUID_FEAT_EDX_SS (1 << 27)
#define CPUID_FEAT_EDX_HTT (1 << 28)
#define CPUID_FEAT_EDX_TM (1 << 29)
#define CPUID_FEAT_EDX_PBE (1 << 31)

#define IA32_KERNEL_GS_BASE 0xC0000102
#define IA32_GS_BASE 0xC0000101
#define IA32_FS_BASE 0xC0000100
#define IA32_EFER 0xC0000080
#define IA32_STAR 0xC0000081
#define IA32_LSTAR 0xC0000082
#define IA32_FMASK 0xC0000084
#define IA32_CSTAR 0xC0000083

#define GS_BASE 0xC0000101

static inline void cpuid(uint32_t eax, uint32_t *eax_out, uint32_t *edx_out) {
    asm volatile("cpuid" : "=a"(*eax_out), "=d"(*edx_out) : "a"(eax) : "ebx", "ecx");
}

static inline void cpuSetMSR(uint32_t msr, uint32_t eax, uint32_t edx) { asm volatile("wrmsr" : : "c"(msr), "a"(eax), "d"(edx)); }

static inline void cpuSetMSR(uint32_t msr, uint64_t value) { cpuSetMSR(msr, value & 0xFFFFFFFF, value >> 32); }

static inline void cpuGetMSR(uint64_t msr, uint32_t *rax, uint32_t *rdx) { asm volatile("rdmsr" : "=a"(*rax), "=d"(*rdx) : "c"(msr)); }

static inline void cpuGetMSR(uint64_t msr, uint64_t *out) {
    uint32_t eax, edx;
    cpuGetMSR(msr, &eax, &edx);
    *out = ((uint64_t)edx << 32) | eax;
}

static inline uint64_t rdtsc(void) {
    uint32_t lo, hi;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}
