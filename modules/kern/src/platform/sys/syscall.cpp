#include "syscall.hpp"

namespace ker::mod::sys {

extern "C" uint64_t syscallHandler(cpu::GPRegs regs) {
    auto callnum = static_cast<abi::callnums>(regs.rax);
    uint64_t a1 = regs.rdi;
    uint64_t a2 = regs.rsi;
    uint64_t a3 = regs.rdx;
    uint64_t a4 = regs.r8;
    uint64_t a5 = regs.r9;
    uint64_t a6 = regs.r10;

    switch (callnum) {
        case abi::callnums::sysLog:
            return ker::syscall::log::sysLog(static_cast<abi::inter::sysLog::sys_log_ops>(a1), (const char*)a2, a3,
                                             static_cast<abi::inter::sysLog::sys_log_device>(a4));
        case abi::callnums::threadInfo:
            return ker::syscall::multiproc::threadInfo(static_cast<abi::inter::multiproc::threadInfoOps>(a1));
        default:
            io::serial::write("Syscall undefined\n");
            io::serial::write("Callnum: ");
            io::serial::write((uint64_t)callnum);
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
            break;
    }
}

void init() {
    uint64_t efer = 0;
    cpuGetMSR(IA32_EFER, &efer);
    cpuSetMSR(IA32_EFER, efer | (1 << 0));                                                       // Enable syscall/sysret
    cpuSetMSR(IA32_FMASK, (1 << 9) | (1 << 8));                                                  // clear IF and TF
    const uint64_t kernCS = (desc::gdt::GDT_ENTRY_KERNEL_CODE) * 8;                              // Kernel CS
    const uint64_t userCS = ((desc::gdt::GDT_ENTRY_USER_CODE) * 8 | desc::gdt::GDT_RING3) - 16;  // -16 since 64 bit sysret offsets
    const uint64_t* STARValue = (uint64_t*)((kernCS | (userCS << 16)) << 32);                    // kern/user CS stored in high half of STAR
    cpuSetMSR(IA32_STAR, (uint64_t)STARValue);
    cpuSetMSR(IA32_LSTAR, (uint64_t)&_wOS_asm_syscallHandler);
    uint32_t eax = 0;
    uint32_t edx = 0;
    cpuGetMSR(IA32_STAR, &eax, &edx);
}
}  // namespace ker::mod::sys
