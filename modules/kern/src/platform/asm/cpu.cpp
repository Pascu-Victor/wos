#include "cpu.hpp"

#include <cstdint>
#include <platform/acpi/apic/apic.hpp>
#include <platform/smt/smt.hpp>

extern "C" uint8_t wos_per_cpu_ready_state = 0;

namespace ker::mod::cpu {

[[clang::no_sanitize("coverage")]] auto get_current_cpu_id_safe() -> uint64_t {
    if (per_cpu_ready_flag_acquire()) {
        return current_cpu_fast();
    }
    // Early boot: use APIC ID
    uint32_t const APIC_ID = apic::get_apic_id();
    if (smt::has_cpu_data()) {
        return smt::get_cpu_index_from_apic_id(APIC_ID);
    }
    return 0;  // BSP during very early init
}

[[clang::no_sanitize("coverage")]] auto is_per_cpu_ready() -> bool { return per_cpu_ready_flag_acquire(); }

void notify_per_cpu_ready() { __atomic_store_n(&wos_per_cpu_ready_state, static_cast<uint8_t>(1), __ATOMIC_RELEASE); }
void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

[[clang::no_sanitize("coverage")]] uint64_t current_cpu() { return current_cpu_fast(); }

[[clang::no_sanitize("coverage")]] void set_current_cpuid(uint64_t id) {
    // Write cpuId to gs:0x10 (offset of cpuId in PerCpu structure)
    asm volatile("mov %0, %%gs:0x10" ::"r"(id) : "memory");
}

void enable_pae() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 5;  // PAE
    wrcr4(cr4);
}

void enable_pse() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 4;  // PSE
    wrcr4(cr4);
}

void enable_fsgsbase() {
    uint64_t cr4 = 0;
    rdcr4(&cr4);
    cr4 |= 1 << 16;  // FSGSBASE
    wrcr4(cr4);
}

extern "C" void wos_enable_sse_asm(void);
extern "C" auto wos_enable_xsave_asm(void) -> uint64_t;

void enable_sse() { wos_enable_sse_asm(); }

uint64_t xsave_area_size = 0;

void enable_xsave() {
    uint64_t const SIZE = wos_enable_xsave_asm();
    if (SIZE > 0 && (xsave_area_size == 0 || SIZE == xsave_area_size)) {
        xsave_area_size = SIZE;
    }
}

}  // namespace ker::mod::cpu
