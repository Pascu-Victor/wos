#include "syscall.hpp"

namespace ker::mod::sys {

extern "C" void syscallHandler(cpu::GPRegs regs) {
    io::serial::write("Syscall\n");
    io::serial::write("R8: ");
    io::serial::writeHex(regs.r8);
    io::serial::write("\n");
    io::serial::write("R9: ");
    io::serial::writeHex(regs.r9);
    io::serial::write("\n");
    io::serial::write("R10: ");
    io::serial::writeHex(regs.r10);
    io::serial::write("\n");
    io::serial::write("R11: ");
    io::serial::writeHex(regs.r11);
    io::serial::write("\n");
    io::serial::write("R12: ");
    io::serial::writeHex(regs.r12);
    io::serial::write("\n");
    io::serial::write("R13: ");
    io::serial::writeHex(regs.r13);
    io::serial::write("\n");
    io::serial::write("R14: ");
    io::serial::writeHex(regs.r14);
    io::serial::write("\n");
    io::serial::write("R15: ");
    io::serial::writeHex(regs.r15);
    io::serial::write("\n");

    io::serial::write("Halting\n");
    apic::eoi();
    hcf();
}
}  // namespace ker::mod::sys
