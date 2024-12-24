#include "scheduler.hpp"

#include <platform/sys/userspace.hpp>

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue> *runQueues;

// TODO: may be unique_ptr
bool postTask(task::Task *task) {
    runQueues->thisCpu()->activeTasks.push_back(task);
    return true;
}

bool postTaskForCpu(uint64_t cpuNo, task::Task *task) {
    runQueues->thatCpu(cpuNo)->activeTasks.push_back(task);
    return true;
}

task::Task *getCurrentTask() {
    task::Task *task = runQueues->thisCpu()->activeTasks.front();
    return task;
}
void processTasks(ker::mod::cpu::GPRegs &gpr, ker::mod::gates::interruptFrame &frame) {
    apic::eoi();
    auto currentTask = getCurrentTask();
    currentTask->context.regs = gpr;
    currentTask->context.frame = frame;
    runQueues->thisCpu()->activeTasks.push_back(currentTask);
    task::Task *nextTask = runQueues->thisCpu()->activeTasks.pop_front();
    sys::context_switch::switchTo(gpr, frame, nextTask);
}

void percpuInit() {
    auto cpu = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu);
    runQueues->thisCpu()->activeTasks = std::list<task::Task *>();
    runQueues->thisCpu()->expiredTasks = std::list<task::Task *>();
}

void startScheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());
    auto firstTask = runQueues->thisCpu()->activeTasks.front();
    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)firstTask->context.syscallKernelStack - KERNEL_STACK_SIZE);
    cpuSetMSR(IA32_GS_BASE, (uint64_t)firstTask->context.syscallUserStack - USER_STACK_SIZE);
    mm::virt::switchPagemap(firstTask);
    sys::context_switch::startSchedTimer();
    _wOS_asm_enterUsermode(firstTask->entry, firstTask->context.frame.rsp);
}

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();
}

}  // namespace ker::mod::sched
