#include "syscall.hpp"

namespace ker::mod::sys {

extern "C" void syscallHandler(cpu::GPRegs regs) {
    uint64_t callnum = regs.r10;
    uint64_t a1 = regs.rdi;
    uint64_t a2 = regs.rsi;
    uint64_t a3 = regs.rdx;
    uint64_t a4 = regs.rcx;
    uint64_t a5 = regs.r8;
    uint64_t a6 = regs.r9;

    io::serial::write("Syscall\n");
    io::serial::write("Callnum: ");
    io::serial::write(callnum);
    io::serial::write("\n");
    io::serial::write("a1: ");
    io::serial::write(a1);
    io::serial::write("\n");
    io::serial::write("a2: ");
    io::serial::write(a2);
    io::serial::write("\n");
    io::serial::write("a3: ");
    io::serial::write(a3);
    io::serial::write("\n");
    io::serial::write("a4: ");
    io::serial::write(a4);
    io::serial::write("\n");
    io::serial::write("a5: ");
    io::serial::write(a5);
    io::serial::write("\n");
    io::serial::write("a6: ");
    io::serial::write(a6);
    io::serial::write("\n");

    io::serial::write("Halting\n");
    hcf();
}

void init() {
    uint64_t efer = 0;
    cpuGetMSR(IA32_EFER, &efer);
    cpuSetMSR(IA32_EFER, efer | (1 << 0));       // Enable syscall/sysret
    cpuSetMSR(IA32_FMASK, (1 << 9) | (1 << 8));  // IF and TF
    cpuSetMSR(IA32_STAR, (desc::gdt::GDT_KERN_CS << 32));
    cpuSetMSR(IA32_LSTAR, (uint64_t)&_wOS_asm_syscallHandler);
}
}  // namespace ker::mod::sys
