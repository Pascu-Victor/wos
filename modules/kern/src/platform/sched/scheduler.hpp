#pragma once

#include <atomic>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/run_heap.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::sched {

// Maximum PIDs system-wide (2^24)
static constexpr uint32_t MAX_PIDS = (1 << 24);

struct RunQueue {
    RunHeap runnableHeap;        // EEVDF min-heap of runnable tasks (keyed on vdeadline)
    IntrusiveTaskList waitList;  // Tasks blocked on I/O or events (waitpid, socket recv)
    IntrusiveTaskList deadList;  // Dead tasks awaiting epoch-based GC

    task::Task* currentTask;   // Currently executing task on this CPU
    task::Task* idleTask;      // Idle task for this CPU (never in heap)
    std::atomic<bool> isIdle;  // True when CPU is halted waiting for work

    // EEVDF bookkeeping (updated under per-CPU lock)
    int64_t totalWeightedVruntime;  // sum(vruntime_i * weight_i) for tasks in heap
    int64_t totalWeight;            // sum(weight_i) for tasks in heap
    int64_t minVruntime;            // Monotonic floor — new/waking tasks start at least here

    // Last timer timestamp (microseconds from HPET) for computing delta
    uint64_t lastTickUs;

    RunQueue()
        : currentTask(nullptr), idleTask(nullptr), isIdle(false), totalWeightedVruntime(0), totalWeight(0), minVruntime(0), lastTickUs(0) {
        runnableHeap.init();
        waitList.init();
        deadList.init();
    }
};

void init();
auto postTask(task::Task* task) -> bool;
auto postTaskForCpu(uint64_t cpuNo, task::Task* task) -> bool;
auto postTaskBalanced(task::Task* task) -> bool;  // Post to least loaded CPU
auto getLeastLoadedCpu() -> uint64_t;             // Get CPU with least tasks
auto getCurrentTask() -> task::Task*;
void removeCurrentTask();                                     // Remove current task from runqueue (for exit)
auto findTaskByPid(uint64_t pid) -> task::Task*;              // Find a task by PID (O(1) via PID registry)
auto findTaskByPidSafe(uint64_t pid) -> task::Task*;          // Find task by PID with refcount (caller must release!)
void rescheduleTaskForCpu(uint64_t cpuNo, task::Task* task);  // Reschedule a specific task on a specific CPU
void gcExpiredTasks();                                        // Garbage collect dead tasks from dead lists
void placeTaskInWaitQueue(ker::mod::cpu::GPRegs& gpr,
                          ker::mod::gates::interruptFrame& frame);  // Move current task to wait queue with context saved
extern "C" void deferredTaskSwitch(ker::mod::cpu::GPRegs* gpr_ptr,
                                   ker::mod::gates::interruptFrame* frame_ptr);  // Called from syscall.asm to switch tasks after syscall
void startScheduler();
void percpuInit();
void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
bool hasRunQueues();
// OOM diagnostics - get queue sizes for a specific CPU
struct RunQueueStats {
    uint64_t activeTaskCount;   // runnableHeap.size
    uint64_t expiredTaskCount;  // deadList.count
    uint64_t waitQueueCount;    // waitList.count
};
auto getRunQueueStats(uint64_t cpuNo) -> RunQueueStats;

// Return up to maxEntries dead task PIDs and their refcounts for diagnostics,
// starting at `startIndex` into the dead list. Returns the number of entries written.
size_t getExpiredTaskRefcounts(uint64_t cpuNo, uint64_t* pids, uint32_t* refcounts, size_t maxEntries, size_t startIndex = 0);

}  // namespace ker::mod::sched
extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task*;
extern "C" const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void _wOS_deferredTaskSwitchReturn(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::interruptFrame* frame_ptr);
