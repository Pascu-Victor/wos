#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/run_heap.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::sched {

// Maximum PIDs system-wide (2^24)
static constexpr uint32_t MAX_PIDS = (1 << 24);

struct RunQueue {
    RunHeap runnable_heap{};        // EEVDF min-heap of runnable tasks (keyed on vdeadline)
    IntrusiveTaskList wait_list{};  // Tasks blocked on I/O or events (waitpid, socket recv)
    IntrusiveTaskList dead_list{};  // Dead tasks awaiting epoch-based GC

    task::Task* current_task{nullptr};  // Currently executing task on this CPU
    task::Task* idle_task{nullptr};     // Idle task for this CPU (never in heap)
    std::atomic<bool> is_idle;          // True when CPU is halted waiting for work

    // EEVDF bookkeeping (updated under per-CPU lock)
    int64_t total_weighted_vruntime{0};  // sum(vruntime_i * weight_i) for tasks in heap
    int64_t total_weight{0};             // sum(weight_i) for tasks in heap
    int64_t min_vruntime{0};             // Monotonic floor - new/waking tasks start at least here

    // Last timer timestamp (microseconds from HPET) for computing delta
    uint64_t last_tick_us{0};

    // Timestamp (µs) when currentTask most recently started running on this CPU.
    // Used by the wakeup min-granularity guard in process_tasks: a justWoke task
    // cannot preempt the running task until it has been running for at least
    // SCHED_MIN_GRANULARITY_US continuously.  Set to 0 at init (means "no data").
    uint64_t current_task_start_us = 0;

    // Lightweight per-CPU scheduler tracing counters.
    std::atomic<uint64_t> idle_timer_arms;
    std::atomic<uint64_t> idle_timer_disarms;
    std::atomic<uint64_t> idle_timer_wakeups;
    std::atomic<uint64_t> wake_ipis_sent;
    std::atomic<uint64_t> local_reschedule_requests;
    std::atomic<uint64_t> slow_reschedule_scans;
    std::atomic<uint64_t> load_balance_pushes;
    std::atomic<uint32_t> placement_reservations;

    // CPU domain fields (Phase 1)
    // domain_id: which CpuDomain this RunQueue belongs to (0 = root)
    // daemon_load_penalty: added to reported load when incoming task is PROCESS type.
    // Set to 56 for soft-exclusive daemon CPUs (makes compute load appear as 64=full).
    uint32_t domain_id = 0;
    uint32_t daemon_load_penalty = 0;

    RunQueue()
        : is_idle(false),

          idle_timer_arms(0),
          idle_timer_disarms(0),
          idle_timer_wakeups(0),
          wake_ipis_sent(0),
          local_reschedule_requests(0),
          slow_reschedule_scans(0),
          load_balance_pushes(0),
          placement_reservations(0) {
        runnable_heap.init();
        wait_list.init();
        dead_list.init();
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
    uint32_t current_preempt_depth;
    bool current_preempt_pending;
    uint64_t current_preempt_max_us;
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
auto post_task_waiting(task::Task* task) -> bool;                // Register a blocked task without making it runnable
auto post_task_balanced(task::Task* task) -> bool;               // Post to least loaded CPU
auto get_least_loaded_cpu() -> uint64_t;                         // Get CPU with least tasks
auto get_least_loaded_cpu_in_mask(uint64_t mask) -> uint64_t;    // Get least-loaded CPU within mask
void set_cpu_daemon_penalty(uint64_t cpu_no, uint32_t penalty);  // Set daemon_load_penalty for a CPU
void set_cpu_domain_id(uint64_t cpu_no, uint32_t domain_id);     // Set domain_id on a CPU's RunQueue (Phase 8)
auto get_cpu_load(uint64_t cpu_no) -> uint32_t;                  // Raw load for a CPU (for queryDomain)
auto get_current_task() -> task::Task*;
bool can_query_current_task();
void remove_current_task();                                       // Remove current task from runqueue (for exit)
auto find_task_by_pid(uint64_t pid) -> task::Task*;               // Find a task by PID (O(1) via PID registry)
auto find_task_by_pid_safe(uint64_t pid) -> task::Task*;          // Find task by PID with refcount (caller must release!)
void set_task_nice(task::Task* task, int nice);                   // Update task weight safely on its run queue
void signal_process_group(uint64_t pgid, int sig);                // Send signal to all tasks in a process group
void wake_task_for_signal(task::Task* task);                      // Make a signaled blocked task runnable so signal delivery can occur
void reschedule_task_for_cpu(uint64_t cpu_no, task::Task* task);  // Reschedule a specific task on a specific CPU
void wake_cpu(uint64_t cpu_no);                                   // Send wake IPI to a CPU (unconditional, for hlt wakeup)
void insert_into_dead_list(task::Task* task);                     // Place a task into CPU 0's dead list for GC
void gc_expired_tasks();                                          // Garbage collect dead tasks from dead lists
void place_task_in_wait_queue(ker::mod::cpu::GPRegs& gpr,
                              ker::mod::gates::InterruptFrame& frame);  // Move current task to wait queue with context saved
extern "C" void deferred_task_switch(ker::mod::cpu::GPRegs* gpr_ptr,
                                     ker::mod::gates::InterruptFrame* frame_ptr);  // Called from syscall.asm to switch tasks after syscall
void start_scheduler();
void percpu_init();
void process_tasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame);
void jump_to_next_task(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame);
void arm_idle_timer_for_this_cpu();
void note_scheduler_timer_interrupt();
bool has_run_queues();

void preempt_disable();
void preempt_disable_at(uint64_t caller);
void preempt_enable();
void preempt_enable_at(uint64_t caller);
auto preempt_count() -> uint32_t;
bool preemptible();
void note_preempt_disabled_block(const char* op, uint64_t perf_callsite);

struct PreemptGuard {
    PreemptGuard() { preempt_disable(); }
    ~PreemptGuard() { preempt_enable(); }
    PreemptGuard(const PreemptGuard&) = delete;
    auto operator=(const PreemptGuard&) -> PreemptGuard& = delete;
};

inline auto interrupts_enabled() -> bool {
    uint64_t flags = 0;
    asm volatile("pushfq\n\tpop %0" : "=r"(flags)::"memory");
    return (flags & cpu::GATE_IF_MASK) != 0;
}

inline void halt_once_preserving_interrupt_state(bool const INTERRUPTS_WERE_ENABLED) {
    if (INTERRUPTS_WERE_ENABLED) {
        asm volatile("sti\n\thlt" ::: "memory");
    } else {
        asm volatile("sti\n\thlt\n\tcli" ::: "memory");
    }
}

// OOM diagnostics - get queue sizes for a specific CPU
struct RunQueueStats {
    uint64_t active_task_count;   // runnable_heap.size
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
auto get_active_task_at_safe(uint32_t index) -> task::Task*;
auto debug_find_task_by_kernel_stack(uint64_t rsp) -> task::Task*;
auto get_dead_task_count(uint64_t cpu_no) -> size_t;
auto get_dead_task_at_safe(uint64_t cpu_no, size_t index) -> task::Task*;

// Return up to maxEntries dead task PIDs and their refcounts for diagnostics,
// starting at `startIndex` into the dead list. Returns the number of entries written.
size_t get_expired_task_refcounts(uint64_t cpu_no, uint64_t* pids, uint32_t* refcounts, size_t max_entries, size_t start_index = 0);

// D17: Function pointer for WKI remote task placement.
// When non-null, postTaskBalanced() calls this before local placement.
// If it returns true, the task was submitted remotely and will not be scheduled locally.
// Set by WKI subsystem init; nullptr when WKI is not active.
extern bool (*wki_try_remote_placement_fn)(task::Task* task);  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// kern_yield() - voluntary preemption point for kernel blocking loops
// ============================================================================
//
// Use instead of bare `asm volatile("sti\nhlt")` in any kernel code that
// busy-waits for an event (socket data, connect completion, timer tick,
// etc.).  For PROCESS tasks executing inside a syscall, this sets the
// voluntary_block flag so the scheduler knows it may safely preempt the
// task and save/restore its kernel-mode context.  For DAEMON tasks the
// flag is a no-op since they are always preemptible.
//
// This function requires the scheduler to be initialised.  Do NOT use it
// in pre-scheduler code paths or in panic/halt loops.
//
// Typical pattern:
//   while (!condition) { kern_yield(); }
//
inline void kern_yield_impl(uint64_t perf_callsite) {
    if (preempt_count() != 0) {
        note_preempt_disabled_block("kern_yield", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->wait_channel = "kern_yield";
        task->voluntary_block = true;
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->voluntary_block = false;
            task->wait_channel = nullptr;
            return;
        }
    }
    halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED);
    if (task != nullptr) {
        (void)task->wakeup_pending.exchange(false, std::memory_order_acquire);
        task->voluntary_block = false;
        task->wait_channel = nullptr;
    }
}

// kern_block() - request that the current DAEMON task be truly blocked
// (moved to wait list) on the next timer tick, then hlt to give up the CPU.
// The timer interrupt handler sees wants_block=true and moves the task to the
// wait list before picking the next runnable task - safe because it runs
// under the run queue lock with interrupts disabled.
// Must only be called from DAEMON kernel threads.
inline void kern_block_impl(uint64_t perf_callsite) {
    if (preempt_count() != 0) {
        note_preempt_disabled_block("kern_block", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->wait_channel = "kern_block";
        task->wants_block = true;
        task->voluntary_block = true;
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wants_block = false;
            task->voluntary_block = false;
            task->wait_channel = nullptr;
            return;
        }
    }
    for (;;) {
        halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED);
        if (task == nullptr) {
            break;
        }
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wake_at_us = 0;
            task->wants_block = false;
            break;
        }
        if (!task->wants_block && task->wake_at_us == 0) {
            break;
        }
    }
    if (task != nullptr) {
        task->wake_at_us = 0;
        task->wants_block = false;
        task->voluntary_block = false;
        task->wait_channel = nullptr;
    }
}

// kern_sleep_us() -- block the current DAEMON task until the timer wake scan
// reaches the requested deadline. This avoids keeping timer/worker threads as
// the current task on otherwise idle CPUs.
inline void kern_sleep_us_impl(uint64_t sleep_us, uint64_t perf_callsite) {
    if (sleep_us == 0) {
        kern_yield_impl(perf_callsite);
        return;
    }

    if (preempt_count() != 0) {
        note_preempt_disabled_block("kern_sleep", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->wait_channel = "kern_sleep";
        task->wake_at_us = ker::mod::time::get_us() + sleep_us;
        task->wants_block = true;
        task->voluntary_block = true;
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wake_at_us = 0;
            task->wants_block = false;
            task->voluntary_block = false;
            task->wait_channel = nullptr;
            return;
        }
    }
    for (;;) {
        halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED);
        if (task == nullptr) {
            break;
        }
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wake_at_us = 0;
            task->wants_block = false;
            break;
        }
        if (!task->wants_block && task->wake_at_us == 0) {
            break;
        }
    }
    if (task != nullptr) {
        task->wake_at_us = 0;
        task->wants_block = false;
        task->voluntary_block = false;
        task->wait_channel = nullptr;
    }
}

// kern_wake() - move a blocked DAEMON task back to the runnable heap.
// Safe to call from interrupt context or any CPU.
// No-op if the task is already runnable.
void kern_wake(task::Task* task);

constexpr auto kern_yield() { ker::mod::sched::kern_yield_impl(WOS_PERF_CALLSITE()); }
constexpr auto kern_block() { ker::mod::sched::kern_block_impl(WOS_PERF_CALLSITE()); }
constexpr auto kern_sleep_us(uint64_t sleep_us) { ker::mod::sched::kern_sleep_us_impl(sleep_us, WOS_PERF_CALLSITE()); }

}  // namespace ker::mod::sched
extern "C" auto wos_get_current_task() -> ker::mod::sched::task::Task*;
extern "C" const uint64_t WOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void wos_deferred_task_switch_return(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::InterruptFrame* frame_ptr);
extern "C" [[noreturn]] void wos_start_kernel_thread(uint64_t stack_top, void (*entry)());
