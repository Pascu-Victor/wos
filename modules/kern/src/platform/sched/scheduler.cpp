#include "scheduler.hpp"

#include <cstddef>
#include <cstdint>
#include <platform/asm/segment.hpp>
#include <platform/sys/userspace.hpp>
// Debug helpers
#include <platform/loader/debug_info.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/mm/mm.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/context_switch.hpp"

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue>* runQueues;

// TODO: may be unique_ptr
auto postTask(task::Task* task) -> bool {
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
    runQueues->thisCpu()->expiredTasks.push_back(getCurrentTask());
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
    cpuSetMSR(IA32_GS_BASE, firstTask->context.syscallScratchArea);  // Scratch area base, not end
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

auto findTaskByPid(uint64_t pid) -> task::Task* {
    // Search all CPUs for a task with the given PID
    for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
        auto* runQueue = runQueues->thatCpu(cpuNo);

        // Search wait queue
        for (auto* node = runQueue->waitQueue.getHead(); node != nullptr; node = node->next) {
            if (node->data != nullptr && node->data->pid == pid) {
                return node->data;
            }
        }

        // Search active tasks
        for (auto* node = runQueue->activeTasks.getHead(); node != nullptr; node = node->next) {
            if (node->data != nullptr && node->data->pid == pid) {
                return node->data;
            }
        }

        // Search expired tasks
        for (auto* node = runQueue->expiredTasks.getHead(); node != nullptr; node = node->next) {
            if (node->data != nullptr && node->data->pid == pid) {
                return node->data;
            }
        }
    }
    return nullptr;
}

void rescheduleTaskForCpu(uint64_t cpuNo, task::Task* task) {
    // Add the task to the active tasks list for the specified CPU
    runQueues->thatCpu(cpuNo)->activeTasks.push_back(task);
}

void placeTaskInWaitQueue(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    // Save the current task's context
    auto* currentTask = getCurrentTask();
    if (currentTask == nullptr) {
        return;
    }

    // Save the context before removing from active queue
    currentTask->context.regs = gpr;
    currentTask->context.frame = frame;

    // Remove from active tasks and add to wait queue
    runQueues->thisCpu()->activeTasks.pop_front();
    runQueues->thisCpu()->waitQueue.push_back(currentTask);

    dbg::log("placeTaskInWaitQueue: Moved PID %x to wait queue", currentTask->pid);

    // Jump to the next task in the active queue
    if (runQueues->thisCpu()->activeTasks.size() > 0) {
        task::Task* nextTask = runQueues->thisCpu()->activeTasks.front();
        nextTask->hasRun = true;
        sys::context_switch::switchTo(gpr, frame, nextTask);
    } else {
        // No more active tasks, but we can't halt yet because wait queue has tasks
        // This is an error condition - we should have at least one task (idle)
        dbg::log("placeTaskInWaitQueue: ERROR - No active tasks left!");
        for (;;) {
            asm volatile("hlt");
        }
    }
}

extern "C" void deferredTaskSwitch(ker::mod::cpu::GPRegs* gpr_ptr, [[maybe_unused]] ker::mod::gates::interruptFrame* frame_ptr) {
    // This is called from syscall.asm AFTER the syscall has been processed
    // but BEFORE returning to userspace, allowing us to switch tasks properly

    if (gpr_ptr == nullptr) {
        return;
    }

    auto* currentTask = getCurrentTask();
    if (currentTask == nullptr) {
        return;
    }

    // For syscall context, we need to BUILD the interrupt frame
    // because syscall doesn't push one. The syscall ABI stores:
    // - Return RIP in RCX (saved to gs:0x28 at syscall entry)
    // - RFLAGS in R11 (saved to gs:0x30 at syscall entry)
    // - User RSP was saved at gs:0x08
    uint64_t returnRip = returnRip;
    uint64_t returnFlags = returnFlags;
    uint64_t userRsp = userRsp;
    asm volatile("movq %%gs:0x28, %0" : "=r"(returnRip));
    asm volatile("movq %%gs:0x30, %0" : "=r"(returnFlags));
    asm volatile("movq %%gs:0x08, %0" : "=r"(userRsp));

    // The syscall handler clobbered these, but we saved the originals at entry
    currentTask->context.regs.rcx = returnRip;    // RCX had return RIP
    currentTask->context.regs.r11 = returnFlags;  // R11 had RFLAGS

#ifdef SCHED_DEBUG
    dbg::log("deferredTaskSwitch: Saving context - RIP=%x, FLAGS=%x, userRSP=%x", returnRip, returnFlags, userRsp);
#endif

    currentTask->context.frame.intNum = 0;  // Not from an interrupt
    currentTask->context.frame.errCode = 0;
    currentTask->context.frame.rip = returnRip;              // From gs:0x28 (preserved RCX)
    currentTask->context.frame.cs = desc::gdt::GDT_USER_CS;  // User code segment
    currentTask->context.frame.flags = returnFlags;          // From gs:0x30 (preserved R11)
    currentTask->context.frame.rsp = userRsp;                // User stack pointer
    currentTask->context.frame.ss = desc::gdt::GDT_USER_DS;  // User data segment

    // Remove from active tasks and add to wait queue
    runQueues->thisCpu()->activeTasks.pop_front();
    runQueues->thisCpu()->waitQueue.push_back(currentTask);

#ifdef SCHED_DEBUG
    dbg::log("deferredTaskSwitch: Moved PID %x to wait queue", currentTask->pid);
#endif

    // Get the next task in the active queue
    if (runQueues->thisCpu()->activeTasks.size() > 0) {
        task::Task* nextTask = runQueues->thisCpu()->activeTasks.front();
        nextTask->hasRun = true;

        mm::virt::switchPagemap(nextTask);

        // Pass pointers to the next task's saved context for restoration
        _wOS_deferredTaskSwitchReturn(&nextTask->context.regs, &nextTask->context.frame);
        // This function does not return - it jumps directly to userspace
        __builtin_unreachable();
    } else {
        // No more active tasks, but we can't halt yet because wait queue has tasks
        // This is an error condition - we should have at least one task (idle)
        dbg::log("deferredTaskSwitch: ERROR - No active tasks left!");
        for (;;) {
            asm volatile("hlt");
        }
    }
}

auto getRunQueueStats(uint64_t cpuNo) -> RunQueueStats {
    RunQueueStats stats = {0, 0, 0};
    if (runQueues == nullptr) {
        return stats;
    }
    auto* runQueue = runQueues->thatCpu(cpuNo);
    if (runQueue == nullptr) {
        return stats;
    }
    stats.activeTaskCount = runQueue->activeTasks.size();
    stats.expiredTaskCount = runQueue->expiredTasks.size();
    stats.waitQueueCount = runQueue->waitQueue.size();
    return stats;
}

}  // namespace ker::mod::sched

// Export offset of deferredTaskSwitch for assembly code
extern "C" {
const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET = offsetof(ker::mod::sched::task::Task, deferredTaskSwitch);
}

extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task* { return ker::mod::sched::getCurrentTask(); }
