#include "scheduler.hpp"

#include <platform/sys/userspace.hpp>

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuVar<RunQueue> *runQueues;

bool postTask(task::Task *task) {
    runQueues->get().activeTasks.push_back(task);
    return true;
}

task::Task *getCurrentTask() {
    task::Task *task = runQueues->get().activeTasks.front();
    return task;
}
void processTasks(ker::mod::cpu::GPRegs &gpr, ker::mod::gates::interruptFrame &frame) {
    // save current task
    apic::eoi();
    auto currentTask = getCurrentTask();
    {
        currentTask->context.regs = gpr;
        currentTask->context.frame = frame;
        runQueues->get().activeTasks.push_back(currentTask);
        task::Task *nextTask = runQueues->get().activeTasks.pop_front();
        sys::context_switch::switchTo(gpr, frame, nextTask);
    }
}

void percpuInit() {
    auto cpu = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu);
    runQueues->get().activeTasks = std::list<task::Task *>();
    runQueues->get().expiredTasks = std::list<task::Task *>();
}

void startScheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());
    // time::sleep(1000);
    auto firstTask = runQueues->get().activeTasks.front();
    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)firstTask->context.syscallKernelStack - KERNEL_STACK_SIZE);
    cpuSetMSR(IA32_GS_BASE, (uint64_t)firstTask->context.syscallUserStack - USER_STACK_SIZE);
    mm::virt::switchPagemap(firstTask);
    sys::context_switch::startSchedTimer();
    _wOS_asm_enterUsermode(firstTask->entry, firstTask->context.frame.rsp);
}

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuVar<RunQueue>();
}

}  // namespace ker::mod::sched
