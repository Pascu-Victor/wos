#pragma once

#include <cstdint>
#include <defines/defines.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/msr.hpp>

namespace ker::mod::cpu {
struct CpuidContext {
    int eax;
    int ebx;
    int ecx;
    int edx;
    int function;
};

void cpuid(struct CpuidContext* cpuid_context);
auto current_cpu() -> uint64_t;
void set_current_cpuid(uint64_t id);

// Safe CPU ID getter - falls back to APIC ID during early boot before per-CPU
// structures are initialized. Call notifyPerCpuReady() to enable the fast path.
auto get_current_cpu_id_safe() -> uint64_t;
void notify_per_cpu_ready();

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
    uint64_t syscall_stack;      // 0x00
    uint64_t user_rsp;           // 0x08 - saved user RSP at syscall entry
    uint64_t cpu_id;             // 0x10
    uint64_t saved_ds;           // 0x18 - saved DS segment
    uint64_t saved_es;           // 0x20 - saved ES segment
    uint64_t syscall_ret_rip;    // 0x28 - RCX at syscall entry (return RIP)
    uint64_t syscall_ret_flags;  // 0x30 - R11 at syscall entry (RFLAGS)
} __attribute__((packed));

static ALWAYS_INLINE uint64_t rdfsbase() {
    uint64_t fsbase = 0;

    asm volatile("rdfsbase %0" : "=r"(fsbase)::"memory");

    return fsbase;
}

static ALWAYS_INLINE uint64_t rdgsbase() {
    uint64_t gsbase = 0;

    asm volatile("rdgsbase %0" : "=r"(gsbase)::"memory");

    return gsbase;
}

static ALWAYS_INLINE void wrfsbase(uint64_t fsbase) { asm volatile("wrfsbase %0" ::"r"(fsbase) : "memory"); }

static ALWAYS_INLINE void wrgsbase(uint64_t gsbase) { asm volatile("wrgsbase %0" ::"r"(gsbase) : "memory"); }

static ALWAYS_INLINE void wrcr4(uint64_t val) { asm volatile("mov %0, %%cr4\n" ::"r"(val) : "memory"); }
// NOLINTNEXTLINE(readability-non-const-parameter)
static ALWAYS_INLINE void rdcr4(uint64_t* val) { asm volatile("mov %%cr4, %0" : "=r"(*val) : : "memory"); }

void enable_pae();
void enable_pse();
void enable_sse();
void enable_fsgsbase();

// Enable XSAVE/XRSTOR and configure XCR0 for x87+SSE+AVX.
// Must be called after enableSSE(). Called on each CPU.
void enable_xsave();

// Size in bytes of the xsave area (set after enableXSave). 0 = XSAVE not supported, use fxsave.
extern uint64_t xsave_area_size;

#define SAVESEGMENT(seg, value) asm("movq %%" #seg ",%0" : "=r"(value) : : "memory")
}  // namespace ker::mod::cpu
