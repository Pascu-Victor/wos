#pragma once

#include <defines/defines.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/msr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::mod::cpu {
struct CpuidContext {
    int eax;
    int ebx;
    int ecx;
    int edx;
    int function;
};

void cpuid(struct CpuidContext *cpuid_context);
uint64_t currentCpu(void);

struct GPRegs {
    uint64_t r15;
    uint64_t r14;
    uint64_t r13;
    uint64_t r12;
    uint64_t r11;
    uint64_t r10;
    uint64_t r9;
    uint64_t r8;
    uint64_t rbp;
    uint64_t rdi;
    uint64_t rsi;
    uint64_t rdx;
    uint64_t rcx;
    uint64_t rbx;
    uint64_t rax;
} __attribute__((packed));

struct PerCpu {
    uint64_t syscallStack;
    uint64_t kernelStack;
} __attribute__((packed));

#define CURRENT_CPU \
    { (PerCpu *)cpuGetMSR(GS_BASE); }

static __always_inline uint64_t rdfsbase(void) {
    uint64_t fsbase;

    asm volatile("rdfsbase %0" : "=r"(fsbase)::"memory");

    return fsbase;
}

static __always_inline uint64_t rdgsbase(void) {
    uint64_t gsbase;

    asm volatile("rdgsbase %0" : "=r"(gsbase)::"memory");

    return gsbase;
}

static __always_inline void wrfsbase(uint64_t fsbase) { asm volatile("wrfsbase %0" ::"r"(fsbase) : "memory"); }

static __always_inline void wrgsbase(uint64_t gsbase) { asm volatile("wrgsbase %0" ::"r"(gsbase) : "memory"); }

static __always_inline void wrcr4(uint64_t val) { asm volatile("mov %0, %%cr4\n" ::"r"(val) : "memory"); }

static __always_inline void rdcr4(uint64_t *val) { asm volatile("mov %%cr4, %0" : "=r"(*val) : : "memory"); }

void enablePAE(void);
void enablePSE(void);

#define savesegment(seg, value) asm("movq %%" #seg ",%0" : "=r"(value) : : "memory")
}  // namespace ker::mod::cpu
