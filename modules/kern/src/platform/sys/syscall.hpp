#pragma once

#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>
#include <platform/interrupt/gdt.hpp>
#include <std/hcf.hpp>

namespace ker::mod::sys {

extern "C" void _wOS_asm_syscallHandler(void);

void init() {
    uint64_t efer = 0;
    cpuGetMSR(IA32_EFER, &efer);
    cpuSetMSR(IA32_EFER, efer | (1 << 0));
    cpuSetMSR(IA32_FMASK, (1 << 9) | (1 << 8));  // IF and TF
    cpuSetMSR(IA32_STAR, (desc::gdt::GDT_KERN_CS << 32) | (desc::gdt::GDT_KERN_DS | 3) << 48);
    cpuSetMSR(IA32_LSTAR, (uint64_t)&_wOS_asm_syscallHandler);
}

extern "C" void syscallHandler(cpu::GPRegs regs);

}  // namespace ker::mod::sys
