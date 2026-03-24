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

    // Lightweight per-CPU scheduler tracing counters.
    std::atomic<uint64_t> idleTimerArms;
    std::atomic<uint64_t> idleTimerDisarms;
    std::atomic<uint64_t> idleTimerWakeups;
    std::atomic<uint64_t> wakeIpisSent;
    std::atomic<uint64_t> localRescheduleRequests;
    std::atomic<uint64_t> slowRescheduleScans;
    std::atomic<uint64_t> loadBalancePushes;
    std::atomic<uint32_t> placementReservations;

    RunQueue()
        : currentTask(nullptr),
          idleTask(nullptr),
          isIdle(false),
          totalWeightedVruntime(0),
          totalWeight(0),
          minVruntime(0),
          lastTickUs(0),
          idleTimerArms(0),
          idleTimerDisarms(0),
          idleTimerWakeups(0),
          wakeIpisSent(0),
          localRescheduleRequests(0),
          slowRescheduleScans(0),
          loadBalancePushes(0),
          placementReservations(0) {
        runnableHeap.init();
        waitList.init();
        deadList.init();
    }
};

struct SchedulerTraceStats {
    uint64_t idle_timer_arms;
    uint64_t idle_timer_disarms;
    uint64_t idle_timer_wakeups;
    uint64_t wake_ipis_sent;
    uint64_t local_reschedule_requests;
    uint64_t slow_reschedule_scans;
    uint64_t load_balance_pushes;
};

struct SchedulerCpuState {
    uint64_t cpu_no;
    uint64_t current_pid;
    const char* current_name;
    uint8_t current_type;
    bool current_voluntary_block;
    bool current_wants_block;
    bool current_cpu_pinned;
    bool is_idle;
    uint64_t runnable_count;
    uint64_t wait_queue_count;
};

void init();
void setup_queues();  // Called separately after EpochManager::init() for dependency system
auto post_task(task::Task* task) -> bool;
auto post_task_for_cpu(uint64_t cpu_no, task::Task* task) -> bool;
// Like post_task_for_cpu but marks the task as CPU-pinned: the scheduler will
// never migrate it to another CPU via load-balancing.
auto post_task_pinned_cpu(uint64_t cpu_no, task::Task* task) -> bool;
auto post_task_balanced(task::Task* task) -> bool;  // Post to least loaded CPU
auto get_least_loaded_cpu() -> uint64_t;            // Get CPU with least tasks
auto get_current_task() -> task::Task*;
void remove_current_task();                                       // Remove current task from runqueue (for exit)
auto find_task_by_pid(uint64_t pid) -> task::Task*;               // Find a task by PID (O(1) via PID registry)
auto find_task_by_pid_safe(uint64_t pid) -> task::Task*;          // Find task by PID with refcount (caller must release!)
void signal_process_group(uint64_t pgid, int sig);                // Send signal to all tasks in a process group
void wake_task_for_signal(task::Task* task);                      // Make a signaled blocked task runnable so signal delivery can occur
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
void arm_idle_timer_for_this_cpu();
void note_scheduler_timer_interrupt();
bool has_run_queues();
// OOM diagnostics - get queue sizes for a specific CPU
struct RunQueueStats {
    uint64_t active_task_count;   // runnableHeap.size
    uint64_t expired_task_count;  // deadList.count
    uint64_t wait_queue_count;    // waitList.count
};
auto get_run_queue_stats(uint64_t cpu_no) -> RunQueueStats;
auto get_scheduler_trace_stats(uint64_t cpu_no) -> SchedulerTraceStats;
auto get_scheduler_cpu_state(uint64_t cpu_no) -> SchedulerCpuState;
void dump_scheduler_trace_stats();
void dump_scheduler_cpu_states();

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

// kern_block() — request that the current DAEMON task be truly blocked
// (moved to wait list) on the next timer tick, then hlt to give up the CPU.
// The timer interrupt handler sees wantsBlock=true and moves the task to the
// wait list before picking the next runnable task — safe because it runs
// under the run queue lock with interrupts disabled.
// Must only be called from DAEMON kernel threads.
inline void kern_block() {
    auto* task = get_current_task();
    if (task != nullptr) {
        task->wantsBlock = true;
        task->voluntaryBlock = true;
    }
    asm volatile("sti\n\thlt" ::: "memory");
    if (task != nullptr) {
        task->voluntaryBlock = false;
        // wantsBlock is cleared by the scheduler when it does the block
    }
}

// kern_sleep_us() -- block the current DAEMON task until the timer wake scan
// reaches the requested deadline. This avoids keeping timer/worker threads as
// the current task on otherwise idle CPUs.
inline void kern_sleep_us(uint64_t sleep_us) {
    if (sleep_us == 0) {
        kern_yield();
        return;
    }

    auto* task = get_current_task();
    if (task != nullptr) {
        task->wakeAtUs = ker::mod::time::getUs() + sleep_us;
        task->wantsBlock = true;
        task->voluntaryBlock = true;
    }
    asm volatile("sti\n\thlt" ::: "memory");
    if (task != nullptr) {
        task->voluntaryBlock = false;
    }
}

// kern_wake() — move a blocked DAEMON task back to the runnable heap.
// Safe to call from interrupt context or any CPU.
// No-op if the task is already runnable.
void kern_wake(task::Task* task);

}  // namespace ker::mod::sched
extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task*;
extern "C" const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void _wOS_deferredTaskSwitchReturn(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::interruptFrame* frame_ptr);
