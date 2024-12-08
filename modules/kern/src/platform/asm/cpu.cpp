#include "cpu.hpp"

namespace ker::mod::cpu {
void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

uint64_t currentCpu(void) {
    PerCpu* cpuPtr;
    cpuGetMSR(IA32_KERNEL_GS_BASE, (uint64_t*)&cpuPtr);
    return cpuPtr->cpuId;
}

void setCurrentCpuid(uint64_t id) {
    PerCpu* cpuPtr;
    cpuGetMSR(IA32_KERNEL_GS_BASE, (uint64_t*)&cpuPtr);
    cpuPtr->cpuId = id;
}

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
