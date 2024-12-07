#include "context_switch.hpp"

#include <platform/sched/scheduler.hpp>

namespace ker::mod::sys::context_switch {

void switchTo(cpu::GPRegs& gpr, gates::interruptFrame& frame, sched::task::Task* nextTask) {
    frame.rip = nextTask->context.frame.rip;
    frame.rsp = nextTask->context.frame.rsp;
    gpr = nextTask->context.regs;

    mm::virt::switchPagemap(nextTask);
}

static long timerQuantum;

extern "C" void _wOS_schedTimer(cpu::GPRegs gpr, gates::interruptFrame frame) {
    sched::processTasks(gpr, frame);
    apic::oneShotTimer(timerQuantum);
}

void startSchedTimer() {
    timerQuantum = apic::calibrateTimer(200);
    apic::oneShotTimer(timerQuantum);
}
}  // namespace ker::mod::sys::context_switch
