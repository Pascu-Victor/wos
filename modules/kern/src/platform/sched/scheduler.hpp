#pragma once

#include <atomic>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/list.hpp>

namespace ker::mod::sched {
struct RunQueue {
    std::list<task::Task*> activeTasks;   // TODO: replace with fast priority queue should also be fast to search
    std::list<task::Task*> expiredTasks;  // TODO: replace with fast priority queue should also be fast to search
    std::list<task::Task*> waitQueue;     // Tasks waiting on I/O or other events (e.g., waitpid)
    task::Task* currentTask;
    std::atomic<bool> isIdle;  // True when CPU is halted waiting for work (only send IPI when idle)
    [[nodiscard]]
    RunQueue()
        : activeTasks(), expiredTasks(), waitQueue(), currentTask(nullptr), isIdle(false) {}
};

struct SchedEntry {
    uint32_t weight;
    uint32_t inverseWeight;
};

void init();
auto postTask(task::Task* task) -> bool;
auto postTaskForCpu(uint64_t cpuNo, task::Task* task) -> bool;
auto postTaskBalanced(task::Task* task) -> bool;  // Post to least loaded CPU
auto getLeastLoadedCpu() -> uint64_t;             // Get CPU with least tasks
auto getCurrentTask() -> task::Task*;
void removeCurrentTask();                                     // Remove current task from runqueue (for exit)
auto findTaskByPid(uint64_t pid) -> task::Task*;              // Find a task by PID across all CPUs
void rescheduleTaskForCpu(uint64_t cpuNo, task::Task* task);  // Reschedule a specific task on a specific CPU
void placeTaskInWaitQueue(ker::mod::cpu::GPRegs& gpr,
                          ker::mod::gates::interruptFrame& frame);  // Move current task to wait queue with context saved
extern "C" void deferredTaskSwitch(ker::mod::cpu::GPRegs* gpr_ptr,
                                   ker::mod::gates::interruptFrame* frame_ptr);  // Called from syscall.asm to switch tasks after syscall
void startScheduler();
void percpuInit();
void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);

// OOM diagnostics - get queue sizes for a specific CPU
struct RunQueueStats {
    uint64_t activeTaskCount;
    uint64_t expiredTaskCount;
    uint64_t waitQueueCount;
};
auto getRunQueueStats(uint64_t cpuNo) -> RunQueueStats;

}  // namespace ker::mod::sched
extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task*;
extern "C" const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void _wOS_deferredTaskSwitchReturn(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::interruptFrame* frame_ptr);
