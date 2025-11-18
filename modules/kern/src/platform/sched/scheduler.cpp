#include "scheduler.hpp"

#include <platform/asm/segment.hpp>
#include <platform/sys/userspace.hpp>
// Debug helpers
#include <platform/loader/debug_info.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/mm/mm.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/context_switch.hpp"

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue>* runQueues;

// TODO: may be unique_ptr
bool postTask(task::Task* task) {
    runQueues->thisCpu()->activeTasks.push_back(task);
    return true;
}

bool postTaskForCpu(uint64_t cpuNo, task::Task* task) {
    runQueues->thatCpu(cpuNo)->activeTasks.push_back(task);
    return true;
}

task::Task* getCurrentTask() {
    task::Task* task = runQueues->thisCpu()->activeTasks.front();
    return task;
}

void removeCurrentTask() {
    runQueues->thisCpu()->activeTasks.pop_front();

    if (runQueues->thisCpu()->activeTasks.size() == 0) {
        dbg::log("No more tasks in runqueue, halting CPU");
        for (;;) {
            asm volatile("hlt");
        }
    }
}

void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    apic::eoi();
    auto* currentTask = getCurrentTask();

    if (currentTask->hasRun) {
        currentTask->context.regs = gpr;
        currentTask->context.frame = frame;
    }

    runQueues->thisCpu()->activeTasks.pop_front();
    runQueues->thisCpu()->activeTasks.push_back(currentTask);
    task::Task* nextTask = runQueues->thisCpu()->activeTasks.front();

    // Mark the next task as having run
    nextTask->hasRun = true;

    sys::context_switch::switchTo(gpr, frame, nextTask);
}

// Jump to the next task without saving the current task's state
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    apic::eoi();
    runQueues->thisCpu()->activeTasks.pop_front();
    task::Task* nextTask = runQueues->thisCpu()->activeTasks.front();

    // Mark the next task as having run
    nextTask->hasRun = true;

    sys::context_switch::switchTo(gpr, frame, nextTask);
}

void percpuInit() {
    auto cpu = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu);
    runQueues->thisCpu()->activeTasks = std::list<task::Task*>();
    runQueues->thisCpu()->expiredTasks = std::list<task::Task*>();
}

void startScheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());
    auto* firstTask = runQueues->thisCpu()->activeTasks.front();

    // Mark the first task as having run since we're entering it directly
    firstTask->hasRun = true;

    cpuSetMSR(IA32_KERNEL_GS_BASE, firstTask->context.syscallKernelStack - KERNEL_STACK_SIZE);
    cpuSetMSR(IA32_GS_BASE, firstTask->context.syscallUserStack - USER_STACK_SIZE);
    cpuSetMSR(IA32_FS_BASE, firstTask->thread->fsbase);
    wrgsbase(firstTask->context.syscallKernelStack - KERNEL_STACK_SIZE);

    mm::virt::switchPagemap(firstTask);

    // Write TLS self-pointer after switching pagemaps so it goes to the correct user-mapped physical memory
    *((uint64_t*)firstTask->thread->fsbase) = firstTask->thread->fsbase;
    sys::context_switch::startSchedTimer();
    for (;;) {
        _wOS_asm_enterUsermode(firstTask->entry, firstTask->context.frame.rsp);
    }
}

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();
}

}  // namespace ker::mod::sched
