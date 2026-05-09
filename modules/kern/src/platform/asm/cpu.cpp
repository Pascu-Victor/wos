#include "cpu.hpp"

#include <atomic>
#include <cstdint>
#include <platform/acpi/apic/apic.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::cpu {

namespace {
std::atomic<bool> s_per_cpu_ready{false};
}  // namespace

auto get_current_cpu_id_safe() -> uint64_t {
    if (s_per_cpu_ready.load(std::memory_order_acquire)) {
        return current_cpu();
    }
    // Early boot: use APIC ID
    uint32_t apic_id = apic::get_apic_id();
    if (smt::has_cpu_data()) {
        return smt::get_cpu_index_from_apic_id(apic_id);
    }
    return 0;  // BSP during very early init
}

void notify_per_cpu_ready() { s_per_cpu_ready.store(true, std::memory_order_release); }
void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

uint64_t current_cpu() {
    // After swapgs in syscall/interrupt handler, GS_BASE points to the per-task
    // scratch area (PerCpu structure). cpuId is at offset 0x10 in PerCpu.
    // We must read via gs: segment, NOT from KERNEL_GS_BASE (which holds user's TLS after swapgs).
    uint64_t cpuId;
    asm volatile("mov %%gs:0x10, %0" : "=r"(cpuId)::"memory");
    return cpuId;
}

void set_current_cpuid(uint64_t id) {
    // Write cpuId to gs:0x10 (offset of cpuId in PerCpu structure)
    asm volatile("mov %0, %%gs:0x10" ::"r"(id) : "memory");
}

void enable_pae() {
    uint64_t cr4;
    rdcr4(&cr4);
    cr4 |= 1 << 5;  // PAE
    wrcr4(cr4);
}

void enable_pse() {
    uint64_t cr4;
    rdcr4(&cr4);
    cr4 |= 1 << 4;  // PSE
    wrcr4(cr4);
}

void enable_fsgsbase() {
    uint64_t cr4;
    rdcr4(&cr4);
    cr4 |= 1 << 16;  // FSGSBASE
    wrcr4(cr4);
}

extern "C" void _wOS_enableSSE_asm(void);
extern "C" uint64_t _wOS_enableXSave_asm(void);

void enable_sse() { _wOS_enableSSE_asm(); }

uint64_t xsave_area_size = 0;

void enable_xsave() {
    uint64_t size = _wOS_enableXSave_asm();
    if (size > 0 && (xsave_area_size == 0 || size == xsave_area_size)) {
        xsave_area_size = size;
    }
}

}  // namespace ker::mod::cpu
