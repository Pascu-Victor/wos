#include "gates.hpp"

#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::gates {
smt::PerCpuVar<interruptHandler_t> interruptHandlers[256](nullptr);

void exception_handler(cpu::GPRegs &gpr, interruptFrame &frame) {
    if (frame.intNum == 14) {
        uint64_t cr2;
        asm volatile("mov %%cr2, %0" : "=r"(cr2));
        // dbg::log("Page fault at address %x with error code %b  rip: 0x%x\n", cr2, frame.errCode, frame.rip);

        mm::virt::pagefaultHandler(cr2, frame.errCode);
        return;
    }

    uint64_t cr0;
    uint64_t cr2;
    uint64_t cr3;
    uint64_t cr4;
    uint64_t cr8;
    asm volatile("mov %%cr0, %0" : "=r"(cr0));
    asm volatile("mov %%cr2, %0" : "=r"(cr2));
    asm volatile("mov %%cr3, %0" : "=r"(cr3));
    asm volatile("mov %%cr4, %0" : "=r"(cr4));
    asm volatile("mov %%cr8, %0" : "=r"(cr8));
    ker::mod::io::serial::write("PANIC!\n");
    // print frame info
    ker::mod::io::serial::write("CPU: ");
    ker::mod::io::serial::write((uint64_t)apic::getApicId());
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("Interrupt number: ");
    ker::mod::io::serial::write(frame.intNum);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("Error code: ");
    ker::mod::io::serial::write(frame.errCode);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RIP: ");
    ker::mod::io::serial::writeHex(frame.rip);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("CS: ");
    ker::mod::io::serial::write(frame.cs);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("CALCULATED PRIVILEGE LEVEL: ");
    ker::mod::io::serial::write(frame.cs & 0x3);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RFLAGS: ");
    ker::mod::io::serial::writeHex(frame.flags);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RSP: ");
    ker::mod::io::serial::writeHex(frame.rsp);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("SS: ");
    ker::mod::io::serial::writeHex(frame.ss);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("CR0: ");
    ker::mod::io::serial::writeBin(cr0);
    ker::mod::io::serial::write(" (0x");
    ker::mod::io::serial::writeHex(cr0);
    ker::mod::io::serial::write(")\n");
    ker::mod::io::serial::write("CR2: ");
    ker::mod::io::serial::writeBin(cr2);
    ker::mod::io::serial::write(" (0x");
    ker::mod::io::serial::writeHex(cr2);
    ker::mod::io::serial::write(")\n");
    ker::mod::io::serial::write("CR3: ");
    ker::mod::io::serial::writeBin(cr3);
    ker::mod::io::serial::write(" (0x");
    ker::mod::io::serial::writeHex(cr3);
    ker::mod::io::serial::write(")\n");
    ker::mod::io::serial::write("CR4: ");
    ker::mod::io::serial::writeBin(cr4);
    ker::mod::io::serial::write(" (0x");
    ker::mod::io::serial::writeHex(cr4);
    ker::mod::io::serial::write(")\n");
    ker::mod::io::serial::write("CR8: ");
    ker::mod::io::serial::writeBin(cr8);
    ker::mod::io::serial::write(" (0x");
    ker::mod::io::serial::writeHex(cr8);
    ker::mod::io::serial::write(")\n");

    // print general purpose registers
    ker::mod::io::serial::write("\nGeneral purpose registers:\n");
    ker::mod::io::serial::write("RAX: ");
    ker::mod::io::serial::writeHex(gpr.rax);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RBX: ");
    ker::mod::io::serial::writeHex(gpr.rbx);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RCX: ");
    ker::mod::io::serial::writeHex(gpr.rcx);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RDX: ");
    ker::mod::io::serial::writeHex(gpr.rdx);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RDI: ");
    ker::mod::io::serial::writeHex(gpr.rdi);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RSI: ");
    ker::mod::io::serial::writeHex(gpr.rsi);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("RBP: ");
    ker::mod::io::serial::writeHex(gpr.rbp);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R8: ");
    ker::mod::io::serial::writeHex(gpr.r8);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R9: ");
    ker::mod::io::serial::writeHex(gpr.r9);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R10: ");
    ker::mod::io::serial::writeHex(gpr.r10);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R11: ");
    ker::mod::io::serial::writeHex(gpr.r11);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R12: ");
    ker::mod::io::serial::writeHex(gpr.r12);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R13: ");
    ker::mod::io::serial::writeHex(gpr.r13);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R14: ");
    ker::mod::io::serial::writeHex(gpr.r14);
    ker::mod::io::serial::write("\n");
    ker::mod::io::serial::write("R15: ");
    ker::mod::io::serial::writeHex(gpr.r15);
    ker::mod::io::serial::write("\n");

    // stack trace
    ker::mod::io::serial::write("\nStack trace:\n");
    uint64_t *rsp = (uint64_t *)frame.rsp;
    for (uint64_t i = 0; i < 30; i++) {
        ker::mod::io::serial::write(i);
        ker::mod::io::serial::write(":");
        ker::mod::io::serial::write(" 0x");
        ker::mod::io::serial::writeHex(rsp[i]);
        ker::mod::io::serial::write("\n");
    }

    ker::mod::io::serial::write("Halting\n");
    // ker::mod::apic::eoi();
    hcf();
}

extern "C" void task_switch_handler(interruptFrame frame);
extern "C" void iterrupt_handler(cpu::GPRegs gpr, interruptFrame frame) {
    if (frame.errCode != UINT64_MAX) {
        exception_handler(gpr, frame);
        return;
    }
    if (frame.intNum == 0x20) {
        task_switch_handler(frame);
    } else if (interruptHandlers[frame.intNum].get() != nullptr) {
        interruptHandlers[frame.intNum].get()(gpr, frame);
    } else {
        if (!isIrq(frame.intNum)) {
            // ker::mod::apic::eoi();
            ker::mod::io::serial::write("No handler for interrupt");
            ker::mod::io::serial::write(frame.intNum);
            ker::mod::io::serial::write("\n");
            hcf();
        }
    }
    // ker::mod::apic::eoi();
}

void setInterruptHandler(uint8_t intNum, interruptHandler_t handler) {
    if (interruptHandlers[intNum].get() != nullptr) {
        ker::mod::io::serial::write("Handler already set\n");
        return;
    }
    interruptHandlers[intNum] = handler;
}

void removeInterruptHandler(uint8_t intNum) { interruptHandlers[intNum] = nullptr; }

bool isInterruptHandlerSet(uint8_t intNum) { return interruptHandlers[intNum].get() != nullptr; }
}  // namespace ker::mod::gates
