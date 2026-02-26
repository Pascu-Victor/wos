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
void setup_queues();  // Called separately after EpochManager::init() for dependency system
auto post_task(task::Task* task) -> bool;
auto post_task_for_cpu(uint64_t cpu_no, task::Task* task) -> bool;
auto post_task_balanced(task::Task* task) -> bool;  // Post to least loaded CPU
auto get_least_loaded_cpu() -> uint64_t;            // Get CPU with least tasks
auto get_current_task() -> task::Task*;
void remove_current_task();                                       // Remove current task from runqueue (for exit)
auto find_task_by_pid(uint64_t pid) -> task::Task*;               // Find a task by PID (O(1) via PID registry)
auto find_task_by_pid_safe(uint64_t pid) -> task::Task*;          // Find task by PID with refcount (caller must release!)
void signal_process_group(uint64_t pgid, int sig);                // Send signal to all tasks in a process group
void reschedule_task_for_cpu(uint64_t cpu_no, task::Task* task);  // Reschedule a specific task on a specific CPU
void wake_cpu(uint64_t cpu_no);                                   // Send wake IPI to a CPU (unconditional, for hlt wakeup)
void insert_into_dead_list(task::Task* task);                     // Place a task into CPU 0's dead list for GC
void gc_expired_tasks();                                          // Garbage collect dead tasks from dead lists
void place_task_in_wait_queue(ker::mod::cpu::GPRegs& gpr,
                              ker::mod::gates::interruptFrame& frame);  // Move current task to wait queue with context saved
extern "C" void deferred_task_switch(ker::mod::cpu::GPRegs* gpr_ptr,
                                     ker::mod::gates::interruptFrame* frame_ptr);  // Called from syscall.asm to switch tasks after syscall
void start_scheduler();
void percpu_init();
void process_tasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
void jump_to_next_task(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
bool has_run_queues();
// OOM diagnostics - get queue sizes for a specific CPU
struct RunQueueStats {
    uint64_t active_task_count;   // runnableHeap.size
    uint64_t expired_task_count;  // deadList.count
    uint64_t wait_queue_count;    // waitList.count
};
auto get_run_queue_stats(uint64_t cpu_no) -> RunQueueStats;

// Active task enumeration (for procfs)
auto get_active_task_count() -> uint32_t;
auto get_active_task_at(uint32_t index) -> task::Task*;

// Return up to maxEntries dead task PIDs and their refcounts for diagnostics,
// starting at `startIndex` into the dead list. Returns the number of entries written.
size_t get_expired_task_refcounts(uint64_t cpu_no, uint64_t* pids, uint32_t* refcounts, size_t max_entries, size_t start_index = 0);

// D17: Function pointer for WKI remote task placement.
// When non-null, postTaskBalanced() calls this before local placement.
// If it returns true, the task was submitted remotely and will not be scheduled locally.
// Set by WKI subsystem init; nullptr when WKI is not active.
extern bool (*wki_try_remote_placement_fn)(task::Task* task);  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// kern_yield() — voluntary preemption point for kernel blocking loops
// ============================================================================
//
// Use instead of bare `asm volatile("sti\nhlt")` in any kernel code that
// busy-waits for an event (socket data, connect completion, timer tick,
// etc.).  For PROCESS tasks executing inside a syscall, this sets the
// voluntaryBlock flag so the scheduler knows it may safely preempt the
// task and save/restore its kernel-mode context.  For DAEMON tasks the
// flag is a no-op since they are always preemptible.
//
// This function requires the scheduler to be initialised.  Do NOT use it
// in pre-scheduler code paths or in panic/halt loops.
//
// Typical pattern:
//   while (!condition) { kern_yield(); }
//
inline void kern_yield() {
    auto* task = get_current_task();
    if (task != nullptr) {
        task->voluntaryBlock = true;
    }
    asm volatile("sti\n\thlt" ::: "memory");
    if (task != nullptr) {
        task->voluntaryBlock = false;
    }
}

}  // namespace ker::mod::sched
extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task*;
extern "C" const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void _wOS_deferredTaskSwitchReturn(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::interruptFrame* frame_ptr);
