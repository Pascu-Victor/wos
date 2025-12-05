#include "gates.hpp"

#include <cstdint>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>

#include "mod/io/serial/serial.hpp"

namespace ker::mod::gates {
namespace {
interruptHandler_t interruptHandlers[256] = {nullptr};
}

void exception_handler(cpu::GPRegs& gpr, interruptFrame& frame) {
    // FIXME: disabled direct map page fault handling for now
    // TODO: implement proper page fault handling
    //  if (frame.intNum == 14) {
    //      uint64_t cr2;
    //      asm volatile("mov %%cr2, %0" : "=r"(cr2));
    //      // dbg::log("Page fault at address %x with error code %b  rip: 0x%x\n", cr2, frame.errCode, frame.rip);

    //     mm::virt::pagefaultHandler(cr2, frame.errCode);
    //     return;
    // }

    uint64_t cr0{};
    uint64_t cr2{};
    uint64_t cr3{};
    uint64_t cr4{};
    uint64_t cr8{};
    asm volatile("mov %%cr0, %0" : "=r"(cr0));
    asm volatile("mov %%cr2, %0" : "=r"(cr2));
    asm volatile("mov %%cr3, %0" : "=r"(cr3));
    asm volatile("mov %%cr4, %0" : "=r"(cr4));
    asm volatile("mov %%cr8, %0" : "=r"(cr8));
    {
        ker::mod::io::serial::ScopedLock lock;

        ker::mod::io::serial::write("PANIC!\n");

        // stack trace
        ker::mod::io::serial::write("\nStack trace:\n");
        auto* rsp = (uint64_t*)frame.rsp;
        constexpr uint64_t MAX_STACK_TRACE = 32;
        for (uint64_t i = 0; i < MAX_STACK_TRACE; i++) {
            ker::mod::io::serial::write(i);
            ker::mod::io::serial::write(":");
            ker::mod::io::serial::write(" 0x");
            ker::mod::io::serial::writeHex(rsp[i]);
            ker::mod::io::serial::write("\n");
        }

        // Dump current task information
        uint64_t cpuId = apic::getApicId();
        constexpr uint64_t DEBUG_TASK_PTR_BASE = 0xffff800000500000ULL;
        auto** debug_task_ptrs = reinterpret_cast<sched::task::Task**>(DEBUG_TASK_PTR_BASE);
        sched::task::Task* currentTask = debug_task_ptrs[cpuId];

        if (false && currentTask != nullptr) {
            ker::mod::io::serial::write("\n=== Current Task Info ===\n");
            ker::mod::io::serial::write("Task address: 0x");
            ker::mod::io::serial::writeHex((uint64_t)currentTask);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("Task name ptr: 0x");
            ker::mod::io::serial::writeHex((uint64_t)currentTask->name);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("PID: 0x");
            ker::mod::io::serial::writeHex(currentTask->pid);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("Type: ");
            ker::mod::io::serial::write(currentTask->type == sched::task::TaskType::PROCESS ? "PROCESS" : "IDLE");
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("Entry: 0x");
            ker::mod::io::serial::writeHex(currentTask->entry);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("Pagemap: 0x");
            ker::mod::io::serial::writeHex((uint64_t)currentTask->pagemap);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("Thread: 0x");
            ker::mod::io::serial::writeHex((uint64_t)currentTask->thread);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("\nTask Context Frame:\n");
            ker::mod::io::serial::write("  frame.rip: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.frame.rip);
            ker::mod::io::serial::write("\n");
            ker::mod::io::serial::write("  frame.cs: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.frame.cs);
            ker::mod::io::serial::write("\n");
            ker::mod::io::serial::write("  frame.rsp: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.frame.rsp);
            ker::mod::io::serial::write("\n");
            ker::mod::io::serial::write("  frame.ss: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.frame.ss);
            ker::mod::io::serial::write("\n");
            ker::mod::io::serial::write("  frame.flags: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.frame.flags);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("\nTask Context:\n");
            ker::mod::io::serial::write("  syscallKernelStack: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.syscallKernelStack);
            ker::mod::io::serial::write("\n");
            ker::mod::io::serial::write("  syscallScratchArea: 0x");
            ker::mod::io::serial::writeHex(currentTask->context.syscallScratchArea);
            ker::mod::io::serial::write("\n");

            ker::mod::io::serial::write("=========================\n\n");
        }

        // print frame info
        ker::mod::io::serial::write("CPU: ");
        ker::mod::io::serial::write((uint64_t)cpuId);
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
        ker::mod::io::serial::writeHex(frame.cs);
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

        // print segment selectors and descriptors
        ker::mod::io::serial::write("\nSegment selectors and descriptors:\n");
        struct GDTR {
            uint16_t limit;
            uint64_t base;
        } __attribute__((packed)) gdtr;
        asm volatile("sgdt %0" : "=m"(gdtr));

        auto rdmsr = [](uint32_t msr) -> uint64_t {
            uint32_t lo, hi;
            asm volatile("rdmsr" : "=a"(lo), "=d"(hi) : "c"(msr));
            return ((uint64_t)hi << 32) | lo;
        };

        uint16_t sel_cs, sel_ds, sel_es, sel_fs, sel_gs, sel_ss, sel_tr;
        asm volatile("mov %%cs, %0" : "=r"(sel_cs));
        asm volatile("mov %%ds, %0" : "=r"(sel_ds));
        asm volatile("mov %%es, %0" : "=r"(sel_es));
        asm volatile("mov %%fs, %0" : "=r"(sel_fs));
        asm volatile("mov %%gs, %0" : "=r"(sel_gs));
        asm volatile("mov %%ss, %0" : "=r"(sel_ss));
        asm volatile("str %0" : "=r"(sel_tr));

        auto printSelector = [&](const char* name, uint16_t sel) {
            ker::mod::io::serial::write(name);
            ker::mod::io::serial::write(": 0x");
            ker::mod::io::serial::writeHex(sel);
            ker::mod::io::serial::write(" (index=");
            ker::mod::io::serial::write((uint64_t)(sel >> 3));
            ker::mod::io::serial::write(", rpl=");
            ker::mod::io::serial::write((uint64_t)(sel & 0x3));
            ker::mod::io::serial::write(")\n");

            if (sel == 0) {
                ker::mod::io::serial::write("  NULL selector\n");
                return;
            }

            uint64_t gdt_base = gdtr.base;
            uint64_t index = (sel >> 3);
            uint64_t desc_addr = gdt_base + index * 8;
            uint64_t desc = *(uint64_t*)(desc_addr);

            ker::mod::io::serial::write("  Raw descriptor: 0x");
            ker::mod::io::serial::writeHex(desc);
            ker::mod::io::serial::write("\n");

            uint64_t limit_low = desc & 0xFFFF;
            uint64_t base_0_15 = (desc >> 16) & 0xFFFF;
            uint64_t base_16_23 = (desc >> 32) & 0xFF;
            uint64_t access = (desc >> 40) & 0xFF;
            uint64_t limit_16_19 = (desc >> 48) & 0xF;
            uint64_t flags = (desc >> 52) & 0xF;
            uint64_t base_24_31 = (desc >> 56) & 0xFF;

            uint64_t limit = limit_low | (limit_16_19 << 16);
            bool gran = (flags >> 3) & 1;
            if (gran) {
                limit = (limit << 12) | 0xFFF;
            }

            uint64_t base = base_0_15 | (base_16_23 << 16) | (base_24_31 << 24);

            bool s_bit = (access >> 4) & 1;  // 0 = system, 1 = code/data
            if (!s_bit) {
                // system descriptor (e.g., TSS) -- read second qword for full base
                uint64_t desc_high = *(uint64_t*)(desc_addr + 8);
                uint64_t base_high = desc_high & 0xFFFFFFFF;
                uint64_t full_base = base | (base_high << 32);
                ker::mod::io::serial::write("  System descriptor (likely TSS). Base: 0x");
                ker::mod::io::serial::writeHex(full_base);
                ker::mod::io::serial::write(", Limit: 0x");
                ker::mod::io::serial::writeHex(limit);
                ker::mod::io::serial::write("\n");
                ker::mod::io::serial::write("  Access: 0x");
                ker::mod::io::serial::writeHex(access);
                ker::mod::io::serial::write(", Flags: 0x");
                ker::mod::io::serial::writeHex(flags);
                ker::mod::io::serial::write("\n");
                ker::mod::io::serial::write("  Raw high: 0x");
                ker::mod::io::serial::writeHex(desc_high);
                ker::mod::io::serial::write("\n");
            } else {
                // code/data descriptor
                ker::mod::io::serial::write("  Code/Data descriptor. Base: 0x");
                ker::mod::io::serial::writeHex(base);
                ker::mod::io::serial::write(", Limit: 0x");
                ker::mod::io::serial::writeHex(limit);
                ker::mod::io::serial::write("\n");
                ker::mod::io::serial::write("  Access: 0x");
                ker::mod::io::serial::writeHex(access);
                ker::mod::io::serial::write(", Flags: 0x");
                ker::mod::io::serial::writeHex(flags);
                ker::mod::io::serial::write("\n");
            }
        };

        printSelector("CS", sel_cs);
        printSelector("DS", sel_ds);
        printSelector("ES", sel_es);
        printSelector("FS", sel_fs);
        printSelector("GS", sel_gs);
        printSelector("SS", sel_ss);
        printSelector("TR", sel_tr);

        // print common MSRs
        ker::mod::io::serial::write("\nCommon MSRs:\n");
        struct MsrInfo {
            const char* name;
            uint32_t id;
        };
        MsrInfo msrs[] = {{"IA32_EFER", 0xC0000080},          {"IA32_STAR", 0xC0000081},      {"IA32_LSTAR", 0xC0000082},
                          {"IA32_FMASK", 0xC0000084},         {"IA32_APIC_BASE", 0x1B},       {"IA32_PAT", 0x277},
                          {"IA32_MISC_ENABLE", 0x1A0},        {"IA32_FS_BASE", IA32_FS_BASE}, {"IA32_GS_BASE", IA32_GS_BASE},
                          {"IA32_KERNEL_GS_BASE", 0xC0000102}};
        for (auto& m : msrs) {
            uint64_t val = rdmsr(m.id);
            ker::mod::io::serial::write(m.name);
            ker::mod::io::serial::write(": 0x");
            ker::mod::io::serial::writeHex(val);
            ker::mod::io::serial::write("\n");
        }

        ker::mod::io::serial::write("Halting\n");
    }
    hcf();
}

extern "C" void iterrupt_handler(cpu::GPRegs gpr, interruptFrame frame) {
    if (frame.errCode != UINT64_MAX) {
        exception_handler(gpr, frame);
        return;
    }
    if (interruptHandlers[frame.intNum] != nullptr) {
        interruptHandlers[frame.intNum](gpr, frame);
    } else {
        if (!isIrq(frame.intNum)) {
            exception_handler(gpr, frame);
            ker::mod::apic::eoi();
            ker::mod::io::serial::write("No handler for interrupt ");
            ker::mod::io::serial::write(frame.intNum);
            ker::mod::io::serial::write("\n");
            hcf();
        }
    }
    ker::mod::apic::eoi();
}

void setInterruptHandler(uint8_t intNum, interruptHandler_t handler) {
    if (interruptHandlers[intNum] != nullptr) {
        ker::mod::io::serial::write("Handler already set\n");
        return;
    }
    interruptHandlers[intNum] = handler;
}

void removeInterruptHandler(uint8_t intNum) { interruptHandlers[intNum] = nullptr; }

bool isInterruptHandlerSet(uint8_t intNum) { return interruptHandlers[intNum] != nullptr; }
}  // namespace ker::mod::gates
