#include "context_switch.hpp"

#include <cstdint>
#include <platform/sched/scheduler.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::sys::context_switch {

void switchTo(cpu::GPRegs& gpr, gates::interruptFrame& frame, sched::task::Task* nextTask) {
    frame.rip = nextTask->context.frame.rip;
    frame.rsp = nextTask->context.frame.rsp;
    frame.cs = nextTask->context.frame.cs;
    frame.ss = nextTask->context.frame.ss;
    frame.flags = nextTask->context.frame.flags;

    gpr = nextTask->context.regs;

    mm::virt::switchPagemap(nextTask);
}

static long timerQuantum;

extern "C" void _wOS_schedTimer(void* stack_ptr) {
    // The stack layout at this point (from low to high address):
    // [GPRegs] <- stack_ptr points here (15 registers)
    // [intNum]
    // [errCode]
    // [RIP]
    // [CS]
    // [RFLAGS]
    // [RSP]
    // [SS]

    apic::eoi();
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    apic::oneShotTimer(timerQuantum);

    // This may not return if we switch contexts
    sched::processTasks(*gpr_ptr, *frame_ptr);
}

extern "C" void _wOS_jumpToNextTaskNoSave(void* stack_ptr) {
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jumpToNextTask(*gpr_ptr, *frame_ptr);
}

void startSchedTimer() {
    timerQuantum = apic::calibrateTimer(10000);  // 10ms
    apic::oneShotTimer(timerQuantum);
}
}  // namespace ker::mod::sys::context_switch
