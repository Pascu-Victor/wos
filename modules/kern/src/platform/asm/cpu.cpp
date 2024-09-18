#include "cpu.hpp"

namespace ker::mod::cpu {
void initPerCpu(uint64_t current_stack) {
    PerCpu* per_cpu = mm::dyn::kmalloc::kmalloc<PerCpu>();

    per_cpu->syscallStack = current_stack;
    per_cpu->kernelStack = 0;
    // TODO: logging here

    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)per_cpu);
    cpuSetMSR(GS_BASE, (uint64_t)per_cpu);

    apic::init();
}

void cpuid(struct CpuidContext* cpuid_context) {
    asm volatile("cpuid"
                 : "=a"(cpuid_context->eax), "=b"(cpuid_context->ebx), "=c"(cpuid_context->ecx), "=d"(cpuid_context->edx)
                 : "a"(cpuid_context->function));
}

uint64_t currentCpu(void) { return apic::cpuid(); }

}  // namespace ker::mod::cpu
