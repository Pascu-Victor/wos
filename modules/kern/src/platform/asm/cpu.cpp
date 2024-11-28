#include "cpu.hpp"

namespace ker::mod::cpu {
void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

uint64_t currentCpu(void) { return apic::cpuid(); }

void enablePAE(void) {
    uint64_t cr4;
    rdcr4(&cr4);
    cr4 |= 1 << 5;  // PAE
    wrcr4(cr4);
}

void enablePSE(void) {
    uint64_t cr4;
    rdcr4(&cr4);
    cr4 |= 1 << 4;  // PSE
    wrcr4(cr4);
}

}  // namespace ker::mod::cpu
