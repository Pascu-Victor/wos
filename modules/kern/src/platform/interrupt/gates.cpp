#include "gates.hpp"

#include <atomic>
#include <cstdint>
#include <platform/dbg/coredump.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <syscalls_impl/process/exit.hpp>

#include "mod/io/serial/serial.hpp"
#include "platform/sched/scheduler.hpp"

namespace ker::mod::gates {
namespace {
interruptHandler_t interruptHandlers[256] = {nullptr};

// Context-based IRQ handlers (parallel array)
struct IrqContext {
    irq_handler_fn handler = nullptr;
    void* data = nullptr;
    const char* name = nullptr;
};
IrqContext irq_contexts[256] = {};

// Next vector to try for allocation (48+ to avoid legacy ISA range)
uint8_t next_alloc_vector = 48;
}  // namespace

void exception_handler(cpu::GPRegs& gpr, interruptFrame& frame) {
    // CRITICAL: Prevent nested panics by detecting recursion
    // Use atomic int to track which CPU owns the panic handler (-1 = none)
    static std::atomic<int64_t> panicOwnerCpu{-1};

    int64_t myApicId = apic::getApicId();
    int64_t expectedOwner = -1;

    // Try to claim ownership of the panic handler
    if (!panicOwnerCpu.compare_exchange_strong(expectedOwner, myApicId, std::memory_order_acq_rel)) {
        // Another CPU already owns the panic handler
        if (expectedOwner == myApicId) {
            // We already own it - this is a nested panic on the same CPU
            io::serial::enterPanicMode();
            dbg::log(
                "CPU %d: NESTED PANIC DETECTED! Halting immediately. Frame info: intNum=%d, errCode=%d, rip=0x%x, cs=0x%x, flags=0x%x, "
                "rsp=0x%x, ss=0x%x. GPRegs: rax=0x%x, rbx=0x%x, rcx=0x%x, rdx=0x%x, rdi=0x%x, rsi=0x%x, rbp=0x%x, r8=0x%x, r9=0x%x, "
                "r10=0x%x, r11=0x%x, r12=0x%x, r13=0x%x, r14=0x%x, r15=0x%x",
                myApicId, frame.intNum, frame.errCode, frame.rip, frame.cs, frame.flags, frame.rsp, frame.ss, gpr.rax, gpr.rbx, gpr.rcx,
                gpr.rdx, gpr.rdi, gpr.rsi, gpr.rbp, gpr.r8, gpr.r9, gpr.r10, gpr.r11, gpr.r12, gpr.r13, gpr.r14, gpr.r15);
        } else {
            // Different CPU owns it - just print minimal info and halt
            io::serial::enterPanicMode();
            dbg::log(
                "CPU %d: FAULT while CPU %d is handling panic. Halting. Frame info: intNum=%d, errCode=%d, rip=0x%x, cs=0x%x, flags=0x%x, "
                "rsp=0x%x, ss=0x%x. GPRegs: rax=0x%x, rbx=0x%x, rcx=0x%x, rdx=0x%x, rdi=0x%x, rsi=0x%x, rbp=0x%x, r8=0x%x, r9=0x%x, "
                "r10=0x%x, r11=0x%x, r12=0x%x, r13=0x%x, r14=0x%x, r15=0x%x",
                myApicId, expectedOwner, frame.intNum, frame.errCode, frame.rip, frame.cs, frame.flags, frame.rsp, frame.ss, gpr.rax,
                gpr.rbx, gpr.rcx, gpr.rdx, gpr.rdi, gpr.rsi, gpr.rbp, gpr.r8, gpr.r9, gpr.r10, gpr.r11, gpr.r12, gpr.r13, gpr.r14, gpr.r15);
        }
        asm volatile("cli; hlt");
        __builtin_unreachable();
    }

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

    // If an exception occurs while returning to or running in userspace,
    // it must not take down the whole kernel. Treat it as a process crash.
    uint64_t cpl = frame.cs & 0x3;
    if (cpl == 3) {
        auto* currentTaskForDump = ker::mod::sched::get_current_task();

        // DEBUG: Log detailed info about the mismatch between currentTask and CR3
        uint32_t apicId = apic::getApicId();
        uint64_t cpuIdFromApic = smt::getCpuIndexFromApicId(apicId);
        dbg::log("USERFAULT DEBUG: apicId=%d cpuFromApic=%d task=%p taskPid=%x cr3=0x%x taskPagemap=%p", apicId, cpuIdFromApic,
                 currentTaskForDump, (currentTaskForDump != nullptr) ? currentTaskForDump->pid : 0xDEAD, cr3,
                 (currentTaskForDump != nullptr) ? (void*)currentTaskForDump->pagemap : nullptr);

        ker::mod::dbg::coredump::tryWriteForTask(currentTaskForDump, gpr, frame, cr2, cr3, apic::getApicId());

        if (frame.intNum == 14) {
            dbg::log("Userspace page fault: cr2=0x%x err=%d rip=0x%x pid=%x", cr2, frame.errCode, frame.rip,
                     ker::mod::sched::get_current_task() ? ker::mod::sched::get_current_task()->pid : 0);

            auto* pml4 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer(cr3 & ~0xFFF);
            uint64_t idx4 = (cr2 >> 39) & 0x1FF;
            uint64_t idx3 = (cr2 >> 30) & 0x1FF;
            uint64_t idx2 = (cr2 >> 21) & 0x1FF;
            uint64_t idx1 = (cr2 >> 12) & 0x1FF;

            dbg::log("PML4[%d]: present=%d frame=0x%x", idx4, pml4->entries[idx4].present, pml4->entries[idx4].frame);
            if (pml4->entries[idx4].present) {
                auto* pml3 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer(pml4->entries[idx4].frame << 12);
                dbg::log(" PML3[%d]: present=%d frame=0x%x", idx3, pml3->entries[idx3].present, pml3->entries[idx3].frame);
                if (pml3->entries[idx3].present) {
                    auto* pml2 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer(pml3->entries[idx3].frame << 12);
                    dbg::log("  PML2[%d]: present=%d frame=0x%x", idx2, pml2->entries[idx2].present, pml2->entries[idx2].frame);
                    if (pml2->entries[idx2].present) {
                        auto* pml1 = (ker::mod::mm::paging::PageTable*)ker::mod::mm::addr::getVirtPointer(pml2->entries[idx2].frame << 12);
                        dbg::log("   PML1[%d]: present=%d frame=0x%x user=%d rw=%d nx=%d", idx1, pml1->entries[idx1].present,
                                 pml1->entries[idx1].frame, pml1->entries[idx1].user, pml1->entries[idx1].writable,
                                 pml1->entries[idx1].noExecute);
                    }
                }
            }

            // Conventional exit code for a segfault-like crash.
            ker::syscall::process::wos_proc_exit(139);
            __builtin_unreachable();
        }

        dbg::log("Userspace exception: int=%d err=%d rip=0x%x pid=%x", frame.intNum, frame.errCode, frame.rip,
                 ker::mod::sched::get_current_task() ? ker::mod::sched::get_current_task()->pid : 0);
        ker::syscall::process::wos_proc_exit(128 + (int)(frame.intNum & 0x7f));
        __builtin_unreachable();
    }

    // Kernel-mode exception - this is a kernel bug, don't try to write coredumps
    // as that would require memory allocation which could cause deadlocks if we
    // faulted while holding a spinlock (e.g., in pageAlloc).
    // Coredumps are only for userspace crashes, handled above.

    // We already claimed ownership via CAS at the top of this function.

    // CRITICAL: Enter epoch critical section to prevent GC from freeing currentTask
    // or its fields while we're printing panic info. We do this BEFORE acquiring
    // the serial lock to minimize the window where GC can corrupt task data.
    // If epoch management is broken, this is a no-op (safe).
    ker::mod::sched::EpochManager::enterCriticalAPIC();

    // Enter panic mode for serial output. This disables the serial lock entirely
    // to prevent deadlocks when CPU ID detection is unreliable during panic.
    // All subsequent serial writes (including from dbg::log) will proceed without locking.
    io::serial::enterPanicMode();

    dbg::log("PANIC!");

    // stack trace - validate RSP before dereferencing
    dbg::log("Stack trace:");
    auto* rsp = (uint64_t*)frame.rsp;
    uintptr_t rspAddr = reinterpret_cast<uintptr_t>(rsp);
    bool rspValid = (rspAddr >= 0xffff800000000000ULL && rspAddr < 0xffff900000000000ULL) ||
                    (rspAddr >= 0xffffffff80000000ULL && rspAddr < 0xffffffffc0000000ULL);

    if (rspValid) {
        constexpr uint64_t MAX_STACK_TRACE = 64;
        for (uint64_t i = 0; i < MAX_STACK_TRACE; i++) {
            dbg::log("%d: 0x%x", i, rsp[i]);
        }
    } else {
        dbg::log("Invalid RSP: 0x%x - skipping stack trace", rspAddr);
    }

    // Dump current task information - use getCurrentTask() instead of hardcoded address
    // The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
    uint64_t cpuId = apic::getApicId();
    sched::task::Task* currentTask = nullptr;

    if (!sched::has_run_queues()) {
        dbg::log("WARNING: RunQueues not initialized OR runQueue not set - cannot get current task!");
        goto skip_task_dump;  // NOLINT
    }

    currentTask = sched::get_current_task();

    dbg::log("=== Current Task Info ===");
    dbg::log("debug_task_ptrs[%d] = 0x%x", cpuId, (uint64_t)currentTask);

    if (currentTask != nullptr) {
        // CRITICAL: Validate task pointer is in valid memory range before dereferencing
        // This prevents nested faults if GC is freeing the task concurrently
        uintptr_t taskAddr = reinterpret_cast<uintptr_t>(currentTask);
        bool inHHDM = (taskAddr >= 0xffff800000000000ULL && taskAddr < 0xffff900000000000ULL);
        bool inKernelStatic = (taskAddr >= 0xffffffff80000000ULL && taskAddr < 0xffffffffc0000000ULL);

        if (!inHHDM && !inKernelStatic) {
            dbg::log("WARNING: currentTask pointer 0x%x is out of valid kernel range!", (uint64_t)currentTask);
            dbg::log("=========================");
            goto skip_task_dump;  // NOLINT
        }

        // Use volatile access to detect if memory is being freed (may contain garbage)
        dbg::log("Task address: 0x%x", (uint64_t)currentTask);

        // Validate and print name with extreme caution
        const char* namePtr = currentTask->name;
        if (namePtr != nullptr) {
            uintptr_t nameAddr = reinterpret_cast<uintptr_t>(namePtr);
            bool nameInHHDM = (nameAddr >= 0xffff800000000000ULL && nameAddr < 0xffff900000000000ULL);
            bool nameInKStatic = (nameAddr >= 0xffffffff80000000ULL && nameAddr < 0xffffffffc0000000ULL);

            if (nameInHHDM || nameInKStatic) {
                // Check first byte without assuming string is valid
                volatile const char* volatileNamePtr = namePtr;
                volatile char firstChar = volatileNamePtr[0];
                if (firstChar >= 0x20 && firstChar <= 0x7e) {
                    dbg::log("Task name: %s", namePtr);
                } else {
                    dbg::log("Task name: <invalid: first byte 0x%x>", (unsigned char)firstChar);
                }
            } else {
                dbg::log("Task name ptr: 0x%x <out of range>", nameAddr);
            }
        } else {
            dbg::log("Task name: <null>");
        }

        // Print primitive fields (safe - no pointer dereference)
        dbg::log("PID: 0x%x", currentTask->pid);
        dbg::log("Type: 0x%x", (uint64_t)currentTask->type);
        dbg::log("Entry: 0x%x", currentTask->entry);
        dbg::log("Pagemap: 0x%x", (uint64_t)currentTask->pagemap);
        dbg::log("Thread: 0x%x", (uint64_t)currentTask->thread);

        dbg::log("Task Context Frame:");
        dbg::log("  frame.rip: 0x%x", currentTask->context.frame.rip);
        dbg::log("  frame.cs: 0x%x", currentTask->context.frame.cs);
        dbg::log("  frame.rsp: 0x%x", currentTask->context.frame.rsp);
        dbg::log("  frame.ss: 0x%x", currentTask->context.frame.ss);
        dbg::log("  frame.flags: 0x%x", currentTask->context.frame.flags);

        dbg::log("Task Context:");
        dbg::log("  syscallKernelStack: 0x%x", currentTask->context.syscallKernelStack);
        dbg::log("  syscallScratchArea: 0x%x", currentTask->context.syscallScratchArea);
    } else {
        dbg::log("WARNING: currentTask is NULL!");
    }
    dbg::log("=========================");

skip_task_dump:

    // Check if page 0 is mapped (it should NOT be - null derefs should crash)
    uint64_t* pml4 = (uint64_t*)(cr3 + 0xffff800000000000ULL);  // HHDM offset
    uint64_t pml4e = pml4[0];                                   // Entry for VA 0x0
    dbg::log("Page 0 check: PML4[0] = 0x%x (Present=%d)", pml4e, (pml4e & 1) ? 1 : 0);
    if (pml4e & 1) {
        dbg::log("WARNING: Page 0 is mapped! NULL derefs won't crash!");
    }

    // print frame info
    dbg::log("CPU: %d", (uint64_t)cpuId);
    dbg::log("Interrupt number: %d", frame.intNum);
    dbg::log("Error code: %d", frame.errCode);
    dbg::log("RIP: 0x%x", frame.rip);
    dbg::log("CS: 0x%x", frame.cs);
    dbg::log("CALCULATED PRIVILEGE LEVEL: %d", frame.cs & 0x3);
    dbg::log("RFLAGS: 0x%x", frame.flags);
    dbg::log("RSP: 0x%x", frame.rsp);
    dbg::log("SS: 0x%x", frame.ss);
    dbg::log("CR0: 0x%x", cr0);
    dbg::log("CR2: 0x%x", cr2);
    dbg::log("CR3: 0x%x", cr3);
    dbg::log("CR4: 0x%x", cr4);
    dbg::log("CR8: 0x%x", cr8);

    // print general purpose registers
    dbg::log("General purpose registers:");
    dbg::log("RAX: 0x%x", gpr.rax);
    dbg::log("RBX: 0x%x", gpr.rbx);
    dbg::log("RCX: 0x%x", gpr.rcx);
    dbg::log("RDX: 0x%x", gpr.rdx);
    dbg::log("RDI: 0x%x", gpr.rdi);
    dbg::log("RSI: 0x%x", gpr.rsi);
    dbg::log("RBP: 0x%x", gpr.rbp);
    dbg::log("R8: 0x%x", gpr.r8);
    dbg::log("R9: 0x%x", gpr.r9);
    dbg::log("R10: 0x%x", gpr.r10);
    dbg::log("R11: 0x%x", gpr.r11);
    dbg::log("R12: 0x%x", gpr.r12);
    dbg::log("R13: 0x%x", gpr.r13);
    dbg::log("R14: 0x%x", gpr.r14);
    dbg::log("R15: 0x%x", gpr.r15);

    // print segment selectors and descriptors
    dbg::log("Segment selectors and descriptors:");
    struct GDTR {
        uint16_t limit;
        uint64_t base;
    } __attribute__((packed)) gdtr;
    asm volatile("sgdt %0" : "=m"(gdtr));

    // Validate GDTR before using it
    bool gdtr_valid = (gdtr.base >= 0xffff800000000000ULL || gdtr.base >= 0xffffffff80000000ULL);
    dbg::log("GDTR: base=0x%x, limit=0x%x, valid=%d", gdtr.base, gdtr.limit, gdtr_valid ? 1 : 0);

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
        dbg::log("%s: 0x%x (index=%d, rpl=%d)", name, sel, (uint64_t)(sel >> 3), (uint64_t)(sel & 0x3));

        if (sel == 0) {
            dbg::log("  NULL selector");
            return;
        }

        if (!gdtr_valid) {
            dbg::log("  Skipping descriptor dump (invalid GDTR)");
            return;
        }

        uint64_t gdt_base = gdtr.base;
        uint64_t index = (sel >> 3);
        uint64_t desc_addr = gdt_base + index * 8;

        // Validate descriptor address is in valid kernel memory
        bool desc_addr_valid = (desc_addr >= 0xffff800000000000ULL && desc_addr < 0xffff900000000000ULL) ||
                               (desc_addr >= 0xffffffff80000000ULL && desc_addr < 0xffffffffc0000000ULL);

        if (!desc_addr_valid) {
            dbg::log("  Invalid descriptor address: 0x%x", desc_addr);
            return;
        }

        uint64_t desc = *(uint64_t*)(desc_addr);

        dbg::log("  Raw descriptor: 0x%x", desc);

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
            uint64_t desc_addr_high = desc_addr + 8;
            bool desc_addr_high_valid = (desc_addr_high >= 0xffff800000000000ULL && desc_addr_high < 0xffff900000000000ULL) ||
                                        (desc_addr_high >= 0xffffffff80000000ULL && desc_addr_high < 0xffffffffc0000000ULL);

            if (desc_addr_high_valid) {
                uint64_t desc_high = *(uint64_t*)(desc_addr_high);
                uint64_t base_high = desc_high & 0xFFFFFFFF;
                uint64_t full_base = base | (base_high << 32);
                dbg::log("  System descriptor (likely TSS). Base: 0x%x, Limit: 0x%x", full_base, limit);
                dbg::log("  Access: 0x%x, Flags: 0x%x", access, flags);
                dbg::log("  Raw high: 0x%x", desc_high);
            } else {
                dbg::log("  System descriptor (TSS). Base (partial): 0x%x, Limit: 0x%x", base, limit);
                dbg::log("  Access: 0x%x, Flags: 0x%x (high descriptor invalid)", access, flags);
            }
        } else {
            // code/data descriptor
            dbg::log("  Code/Data descriptor. Base: 0x%x, Limit: 0x%x", base, limit);
            dbg::log("  Access: 0x%x, Flags: 0x%x", access, flags);
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
    dbg::log("Common MSRs:");
    struct MsrInfo {
        const char* name;
        uint32_t id;
    };
    MsrInfo msrs[] = {{.name = "IA32_EFER", .id = 0xC0000080},      {.name = "IA32_STAR", .id = 0xC0000081},
                      {.name = "IA32_LSTAR", .id = 0xC0000082},     {.name = "IA32_FMASK", .id = 0xC0000084},
                      {.name = "IA32_APIC_BASE", .id = 0x1B},       {.name = "IA32_PAT", .id = 0x277},
                      {.name = "IA32_MISC_ENABLE", .id = 0x1A0},    {.name = "IA32_FS_BASE", .id = IA32_FS_BASE},
                      {.name = "IA32_GS_BASE", .id = IA32_GS_BASE}, {.name = "IA32_KERNEL_GS_BASE", .id = 0xC0000102}};
    for (auto& m : msrs) {
        uint64_t val = rdmsr(m.id);
        dbg::log("%s: 0x%x", m.name, val);
    }

    dbg::log("Halting");
    hcf();
}

extern "C" void iterrupt_handler(cpu::GPRegs gpr, interruptFrame frame) {
    if (frame.errCode != UINT64_MAX) {
        exception_handler(gpr, frame);
        return;
    }

    // Check context-based handler first (for device drivers)
    if (irq_contexts[frame.intNum].handler != nullptr) {
        irq_contexts[frame.intNum].handler(static_cast<uint8_t>(frame.intNum), irq_contexts[frame.intNum].data);
        ker::mod::apic::eoi();
        return;
    }

    if (interruptHandlers[frame.intNum] != nullptr) {
        interruptHandlers[frame.intNum](gpr, frame);
    } else {
        // No handler registered - log and handle appropriately
        ker::mod::io::serial::write("UNHANDLED INT: vector=");
        ker::mod::io::serial::writeHex(static_cast<uint8_t>(frame.intNum));
        ker::mod::io::serial::write(" rip=");
        ker::mod::io::serial::writeHex(frame.rip);
        ker::mod::io::serial::write("\n");

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

// Vector 32 (0x20) is the timer interrupt, hardcoded in gates.asm (isr32 -> task_switch_handler).
// It must NEVER be assigned to any other handler.
static constexpr uint8_t TIMER_VECTOR = 32;

void setInterruptHandler(uint8_t intNum, interruptHandler_t handler) {
    if (intNum == TIMER_VECTOR) {
        ker::mod::io::serial::write("setInterruptHandler: vector 32 is reserved for timer\n");
        return;
    }
    if (interruptHandlers[intNum] != nullptr) {
        ker::mod::io::serial::write("Handler already set\n");
        return;
    }
    interruptHandlers[intNum] = handler;
}

void removeInterruptHandler(uint8_t intNum) { interruptHandlers[intNum] = nullptr; }

bool isInterruptHandlerSet(uint8_t intNum) { return interruptHandlers[intNum] != nullptr; }

auto requestIrq(uint8_t vector, irq_handler_fn handler, void* data, const char* name) -> int {
    if (vector == TIMER_VECTOR) {
        ker::mod::io::serial::write("requestIrq: vector 32 is reserved for timer\n");
        return -1;
    }
    if (interruptHandlers[vector] != nullptr || irq_contexts[vector].handler != nullptr) {
        ker::mod::io::serial::write("requestIrq: vector already in use\n");
        return -1;
    }
    ker::mod::io::serial::write("requestIrq: allocated vector ");
    ker::mod::io::serial::writeHex(vector);
    ker::mod::io::serial::write(" for ");
    ker::mod::io::serial::write(name);
    ker::mod::io::serial::write("\n");
    irq_contexts[vector].handler = handler;
    irq_contexts[vector].data = data;
    irq_contexts[vector].name = name;
    return 0;
}

void freeIrq(uint8_t vector) {
    irq_contexts[vector].handler = nullptr;
    irq_contexts[vector].data = nullptr;
    irq_contexts[vector].name = nullptr;
}

auto allocateVector() -> uint8_t {
    // Find a free vector starting from 48 (above legacy IRQ range).
    // Vectors 0-31 are CPU exceptions.
    // Vector 32 (0x20) is the timer interrupt — hardcoded in gates.asm
    // (isr32 -> task_switch_handler). NEVER allocate it.
    // Vectors 33-47 are reserved for legacy ISA IRQs.
    // Vectors 48-255 are available for MSI/dynamic allocation.
    for (uint16_t v = next_alloc_vector; v < 256; v++) {
        if (interruptHandlers[v] == nullptr && irq_contexts[v].handler == nullptr) {
            next_alloc_vector = static_cast<uint8_t>(v + 1);
            return static_cast<uint8_t>(v);
        }
    }
    // Wrap around and search from 48
    for (uint16_t v = 48; v < next_alloc_vector; v++) {
        if (interruptHandlers[v] == nullptr && irq_contexts[v].handler == nullptr) {
            return static_cast<uint8_t>(v);
        }
    }
    return 0;  // No free vector found
}

}  // namespace ker::mod::gates
