#include "scheduler.hpp"

#include <platform/sys/userspace.hpp>

namespace ker::mod::sched {
// One run queue per cpu
smt::PerCpuCrossAccess<RunQueue> *runQueues;

bool postTask(task::Task *task) {
    runQueues->thisCpu()->activeTasks.push_back(task);
    return true;
}

task::Task *getCurrentTask() {
    task::Task *task = runQueues->thisCpu()->activeTasks.front();
    return task;
}
void processTasks(ker::mod::cpu::GPRegs *gpr, ker::mod::gates::interruptFrame *frame) {
    dbg::log("HIT:%x", cpu::currentCpu());
    // save current task
    apic::eoi();
    auto currentTask = getCurrentTask();
    {
        auto rqRef = runQueues->thisCpu();
        currentTask->regs.regs = *gpr;
        currentTask->regs.ip = frame->rip;
        currentTask->regs.rsp = frame->rsp;

        rqRef->activeTasks.push_back(currentTask);
        task::Task *nextTask = rqRef->activeTasks.pop_front();
        sys::context_switch::switchTo(gpr, frame, nextTask);
    }
}

void percpuInit() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());
    runQueues->thisCpu()->activeTasks = std::list<task::Task *>();
    runQueues->thisCpu()->expiredTasks = std::list<task::Task *>();
}

void startScheduler() {
    time::sleep(1000);
    auto firstTask = runQueues->thisCpu()->activeTasks.front();
    mm::virt::switchPagemap(firstTask);
    sys::context_switch::startSchedTimer();
    _wOS_asm_enterUsermode(firstTask->entry, firstTask->regs.rsp);
}

void init() { runQueues = new smt::PerCpuCrossAccess<RunQueue>(); }

}  // namespace ker::mod::sched
