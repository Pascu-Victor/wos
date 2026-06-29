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

[[nodiscard]] constexpr auto saturating_deadline_us(uint64_t base_us, uint64_t delta_us) -> uint64_t {
    if (UINT64_MAX - base_us < delta_us) {
        return UINT64_MAX;
    }
    return base_us + delta_us;
}

struct RunQueue {
    RunHeap runnable_heap{};        // EEVDF min-heap of runnable tasks (keyed on vdeadline)
    IntrusiveTaskList wait_list{};  // Tasks blocked on I/O or events (waitpid, socket recv)
    IntrusiveTaskList dead_list{};  // Dead tasks awaiting epoch-based GC

    task::Task* current_task{nullptr};  // Task whose stack is currently executing on this CPU
    task::Task* handoff_task{nullptr};  // Picked next task reserved for an in-progress stack/frame handoff
    task::Task* idle_task{nullptr};     // Idle task for this CPU (never in heap)
    std::atomic<bool> is_idle;          // True when CPU is halted waiting for work

    // EEVDF bookkeeping (updated under per-CPU lock)
    int64_t total_weighted_vruntime{0};  // sum(vruntime_i * weight_i) for tasks in heap
    int64_t total_weight{0};             // sum(weight_i) for tasks in heap
    int64_t min_vruntime{0};             // Monotonic floor - new/waking tasks start at least here

    // Last timer timestamp (microseconds from HPET) for computing delta
    uint64_t last_tick_us{0};
    uint64_t next_wait_deadline_us{0};

    // Linux-style CPU accounting buckets, stored in microseconds. These are
    // updated from timer/IRQ paths, so keep them allocation-free and lock-free.
    std::atomic<uint64_t> cpu_user_us;
    std::atomic<uint64_t> cpu_nice_us;
    std::atomic<uint64_t> cpu_system_us;
    std::atomic<uint64_t> cpu_idle_us;
    std::atomic<uint64_t> cpu_iowait_us;
    std::atomic<uint64_t> cpu_irq_us;
    std::atomic<uint64_t> cpu_softirq_us;
    std::atomic<uint64_t> cpu_steal_us;
    std::atomic<uint64_t> irq_uncharged_us;

    // Timestamp (µs) when currentTask most recently started running on this CPU.
    // Used by the wakeup min-granularity guard in process_tasks: a justWoke task
    // cannot preempt the running task until it has been running for at least
    // SCHED_MIN_GRANULARITY_US continuously.  Set to 0 at init (means "no data").
    uint64_t current_task_start_us = 0;

    // Lightweight per-CPU scheduler tracing counters.
    std::atomic<uint64_t> idle_timer_arms;
    std::atomic<uint64_t> idle_timer_disarms;
    std::atomic<uint64_t> idle_timer_wakeups;
    std::atomic<uint64_t> scheduler_timer_interrupts;
    std::atomic<uint64_t> scheduler_timer_arms;
    std::atomic<uint64_t> scheduler_timer_disarms;
    std::atomic<uint64_t> scheduler_timer_arm_wait_deadline;
    std::atomic<uint64_t> scheduler_timer_arm_itimer;
    std::atomic<uint64_t> scheduler_timer_arm_voluntary;
    std::atomic<uint64_t> scheduler_timer_arm_idle_work;
    std::atomic<uint64_t> scheduler_timer_arm_runqueue;
    std::atomic<uint64_t> scheduler_timer_arm_competitor;
    std::atomic<uint64_t> wake_ipis_sent;
    std::atomic<uint64_t> wake_ipis_coalesced;
    std::atomic<uint64_t> local_reschedule_requests;
    std::atomic<uint64_t> local_reschedule_timer_pokes;
    std::atomic<uint64_t> slow_reschedule_scans;
    std::atomic<uint64_t> wait_list_scan_iterations;
    std::atomic<uint64_t> wait_list_scan_passes;
    std::atomic<uint64_t> wait_list_scan_max;
    std::atomic<uint64_t> timer_expired_wakeups;
    std::atomic<uint64_t> gc_passes_triggered;
    std::atomic<uint64_t> gc_tasks_reclaimed;
    std::atomic<uint64_t> gc_work_us_total;
    std::atomic<uint64_t> gc_work_us_max;
    std::atomic<uint64_t> gc_task_us_total;
    std::atomic<uint64_t> gc_task_us_max;
    std::atomic<uint64_t> gc_detach_us_total;
    std::atomic<uint64_t> gc_detach_us_max;
    std::atomic<uint64_t> gc_pagemap_us_total;
    std::atomic<uint64_t> gc_pagemap_us_max;
    std::atomic<uint64_t> gc_thread_us_total;
    std::atomic<uint64_t> gc_thread_us_max;
    std::atomic<uint64_t> gc_misc_us_total;
    std::atomic<uint64_t> gc_misc_us_max;
    std::atomic<uint64_t> gc_debug_us_total;
    std::atomic<uint64_t> gc_debug_us_max;
    std::atomic<uint64_t> load_balance_pushes;
    std::atomic<uint32_t> cached_load_default;
    std::atomic<uint32_t> cached_load_process;
    std::atomic<uint32_t> cached_current_load_default;
    std::atomic<uint32_t> cached_current_load_process;
    std::atomic<uint32_t> placement_reservations;
    std::atomic<bool> resched_timer_pending;

    // CPU domain fields (Phase 1)
    // domain_id: which CpuDomain this RunQueue belongs to (0 = root)
    // daemon_load_penalty: added to reported load when incoming task is PROCESS type.
    // Positive values make soft-exclusive daemon CPUs less attractive to PROCESS placement.
    uint32_t domain_id = 0;
    std::atomic<uint32_t> daemon_load_penalty;

    RunQueue()
        : is_idle(false),
          cpu_user_us(0),
          cpu_nice_us(0),
          cpu_system_us(0),
          cpu_idle_us(0),
          cpu_iowait_us(0),
          cpu_irq_us(0),
          cpu_softirq_us(0),
          cpu_steal_us(0),
          irq_uncharged_us(0),
          idle_timer_arms(0),
          idle_timer_disarms(0),
          idle_timer_wakeups(0),
          scheduler_timer_interrupts(0),
          scheduler_timer_arms(0),
          scheduler_timer_disarms(0),
          scheduler_timer_arm_wait_deadline(0),
          scheduler_timer_arm_itimer(0),
          scheduler_timer_arm_voluntary(0),
          scheduler_timer_arm_idle_work(0),
          scheduler_timer_arm_runqueue(0),
          scheduler_timer_arm_competitor(0),
          wake_ipis_sent(0),
          wake_ipis_coalesced(0),
          local_reschedule_requests(0),
          local_reschedule_timer_pokes(0),
          slow_reschedule_scans(0),
          wait_list_scan_iterations(0),
          wait_list_scan_passes(0),
          wait_list_scan_max(0),
          timer_expired_wakeups(0),
          gc_passes_triggered(0),
          gc_tasks_reclaimed(0),
          gc_work_us_total(0),
          gc_work_us_max(0),
          gc_task_us_total(0),
          gc_task_us_max(0),
          gc_detach_us_total(0),
          gc_detach_us_max(0),
          gc_pagemap_us_total(0),
          gc_pagemap_us_max(0),
          gc_thread_us_total(0),
          gc_thread_us_max(0),
          gc_misc_us_total(0),
          gc_misc_us_max(0),
          gc_debug_us_total(0),
          gc_debug_us_max(0),
          load_balance_pushes(0),
          cached_load_default(0),
          cached_load_process(0),
          cached_current_load_default(0),
          cached_current_load_process(0),
          placement_reservations(0),
          resched_timer_pending(false),
          daemon_load_penalty(0) {
        runnable_heap.init();
        wait_list.init();
        dead_list.init();
    }
};

struct SchedulerTraceStats {
    uint64_t idle_timer_arms;
    uint64_t idle_timer_disarms;
    uint64_t idle_timer_wakeups;
    uint64_t scheduler_timer_interrupts;
    uint64_t scheduler_timer_arms;
    uint64_t scheduler_timer_disarms;
    uint64_t scheduler_timer_arm_wait_deadline;
    uint64_t scheduler_timer_arm_itimer;
    uint64_t scheduler_timer_arm_voluntary;
    uint64_t scheduler_timer_arm_idle_work;
    uint64_t scheduler_timer_arm_runqueue;
    uint64_t scheduler_timer_arm_competitor;
    uint64_t wake_ipis_sent;
    uint64_t wake_ipis_coalesced;
    uint64_t local_reschedule_requests;
    uint64_t local_reschedule_timer_pokes;
    uint64_t slow_reschedule_scans;
    uint64_t wait_list_scan_iterations;
    uint64_t wait_list_scan_passes;
    uint64_t wait_list_scan_max;
    uint64_t timer_expired_wakeups;
    uint64_t gc_passes_triggered;
    uint64_t gc_tasks_reclaimed;
    uint64_t gc_work_us_total;
    uint64_t gc_work_us_max;
    uint64_t gc_task_us_total;
    uint64_t gc_task_us_max;
    uint64_t gc_detach_us_total;
    uint64_t gc_detach_us_max;
    uint64_t gc_pagemap_us_total;
    uint64_t gc_pagemap_us_max;
    uint64_t gc_thread_us_total;
    uint64_t gc_thread_us_max;
    uint64_t gc_misc_us_total;
    uint64_t gc_misc_us_max;
    uint64_t gc_debug_us_total;
    uint64_t gc_debug_us_max;
    uint64_t gc_deferred_queued;
    uint64_t gc_deferred_completed;
    uint64_t gc_deferred_depth;
    uint64_t gc_deferred_depth_max;
    uint64_t gc_deferred_slices;
    uint64_t gc_deferred_slices_completed;
    uint64_t gc_deferred_oldest_wait_us_max;
    uint64_t gc_idle_boost_passes;
    uint64_t gc_foreground_passes;
    uint64_t load_balance_pushes;
};

struct SchedulerTimerDecision {
    bool arm;
    bool use_deadline_delta;
    uint64_t deadline_delta_us;
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
    bool resched_timer_pending;
    bool is_idle;
    uint64_t runnable_count;
    uint64_t wait_queue_count;
    uint64_t scheduler_timer_interrupts;
    uint64_t scheduler_timer_arms;
    uint64_t scheduler_timer_disarms;
    uint64_t wake_ipis_sent;
    uint64_t wake_ipis_coalesced;
    uint64_t local_reschedule_requests;
    uint64_t local_reschedule_timer_pokes;
    uint64_t last_tick_us;
    uint64_t next_wait_deadline_us;
};

struct CpuAccountingSnapshot {
    uint64_t user_us;
    uint64_t nice_us;
    uint64_t system_us;
    uint64_t idle_us;
    uint64_t iowait_us;
    uint64_t irq_us;
    uint64_t softirq_us;
    uint64_t steal_us;
};

struct KernelThreadShutdownResult {
    size_t requested{};
    size_t remaining{};
};

struct LoadAverageSnapshot {
    uint64_t load1_milli;
    uint64_t load5_milli;
    uint64_t load15_milli;
    uint32_t runnable_tasks;
    uint32_t uninterruptible_tasks;
    uint32_t total_tasks;
    uint64_t last_pid;
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
auto get_return_task() -> task::Task*;
auto current_cpu_for_task(task::Task* task) -> uint64_t;
auto owner_cpu_for_task(task::Task* task) -> uint64_t;
auto migrate_task_to_cpu(task::Task* task, uint64_t target_cpu) -> bool;
auto pin_task_to_cpu(task::Task* task, uint64_t target_cpu) -> bool;
bool can_query_current_task();
void remove_current_task();                               // Remove current task from runqueue (for exit)
auto find_task_by_pid(uint64_t pid) -> task::Task*;       // Find a task by PID (O(1) via PID registry)
auto find_task_by_pid_safe(uint64_t pid) -> task::Task*;  // Find task by PID with refcount (caller must release!)
auto task_has_live_pagemap_sibling(task::Task* subject) -> bool;
void set_task_nice(task::Task* task, int nice);               // Update task weight safely on its run queue
auto signal_process_group(uint64_t pgid, int sig) -> size_t;  // Send signal to all live tasks in a process group
auto signal_controlling_tty(int controlling_tty, int sig) -> size_t;
auto signal_controlling_tty_wki_proxies(int controlling_tty, int sig) -> size_t;
auto signal_visible_processes_except(uint64_t excluded_pid, uint64_t excluded_owner_pid, int sig) -> size_t;
void wake_task_for_signal(task::Task* task);  // Make a signaled blocked task runnable so signal delivery can occur
[[nodiscard]] auto kernel_threads_shutdown_requested() -> bool;
void maybe_exit_current_kernel_thread_for_shutdown();
auto request_kernel_threads_shutdown(uint64_t timeout_us) -> KernelThreadShutdownResult;

// Event-driven waiters should resume on the CPU that observed the event when
// they are truly parked and migratable.  Keeping this policy central prevents
// sockets, PTYs, pipes, and similar subsystems from drifting apart.
enum class EventWakeDeferredSwitch : uint8_t {
    PRESERVE,
    CANCEL,
};

[[nodiscard]] constexpr auto event_wake_prefers_waker_cpu(bool cpu_pinned, bool waiting, bool voluntary_block) -> bool {
    return !cpu_pinned && (waiting || voluntary_block);
}
[[nodiscard]] constexpr auto event_wake_cancels_deferred_switch(EventWakeDeferredSwitch deferred_switch) -> bool {
    return deferred_switch == EventWakeDeferredSwitch::CANCEL;
}
[[nodiscard]] auto event_wake_target_cpu(const task::Task* task, uint64_t waker_cpu) -> uint64_t;
void wake_task_from_event(task::Task* task, EventWakeDeferredSwitch deferred_switch = EventWakeDeferredSwitch::PRESERVE);
void wake_task_from_event_on_cpu(task::Task* task, uint64_t target_cpu,
                                 EventWakeDeferredSwitch deferred_switch = EventWakeDeferredSwitch::PRESERVE);
[[nodiscard]] auto wake_task_by_pid_from_event(uint64_t pid, EventWakeDeferredSwitch deferred_switch = EventWakeDeferredSwitch::PRESERVE)
    -> bool;
[[nodiscard]] auto wake_task_by_pid_from_event_on_cpu(uint64_t pid, uint64_t target_cpu,
                                                      EventWakeDeferredSwitch deferred_switch = EventWakeDeferredSwitch::PRESERVE) -> bool;
void reschedule_task_for_cpu(uint64_t cpu_no, task::Task* task);  // Reschedule a specific task on a specific CPU
auto debug_stop_task(task::Task* task) -> bool;                   // Move a traced task out of runnable scheduling if possible
enum class WakeCpuMode : uint8_t {
    COALESCE,
    FORCE,
};

void wake_cpu(uint64_t cpu_no, WakeCpuMode mode = WakeCpuMode::COALESCE);  // Send wake IPI to a CPU
void insert_into_dead_list(task::Task* task);                              // Place a task into CPU 0's dead list for GC
void gc_expired_tasks();                                                   // Garbage collect dead tasks from dead lists
auto gc_expired_tasks_budgeted(uint32_t max_tasks) -> uint32_t;            // Budgeted task GC for scheduler GC daemon
void request_gc();                                                         // Request deferred scheduler GC from IRQ-safe paths
void request_gc_memory_pressure();                                         // Request aggressive GC from allocation pressure paths
auto reclaim_memory_pressure() -> uint32_t;                                // Synchronously run one exclusive pressure-GC pass when safe
void start_gc_worker();                                                    // Start scheduler GC daemon after init task creation
void request_local_timer_recheck();                                        // Arm a local timer tick after a new current-task deadline
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
void note_scheduler_timer_arm();
void note_scheduler_timer_disarm();
void note_local_reschedule_timer_poke();
auto get_scheduler_timer_decision_for_this_cpu(uint64_t now_us) -> SchedulerTimerDecision;
void account_irq_time_us(uint64_t elapsed_us);
auto get_cpu_accounting_snapshot(uint64_t cpu_no) -> CpuAccountingSnapshot;
auto get_load_average_snapshot() -> LoadAverageSnapshot;
bool has_run_queues();
void begin_syscall_accounting();
void finish_syscall_accounting();
[[nodiscard]] auto pause_syscall_accounting() -> bool;
void resume_syscall_accounting();

void preempt_disable();
void preempt_disable_at(uint64_t caller);
[[nodiscard]] auto preempt_disable_token_at(uint64_t caller) -> task::Task*;
void preempt_enable();
void preempt_enable_at(uint64_t caller);
void preempt_enable_token_at(task::Task* task, uint64_t caller);
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
    if (INTERRUPTS_WERE_ENABLED) {  // NOLINT(bugprone-branch-clone)
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

// Task enumeration helpers. Returned lifetime refs must be released by callers.
using ActiveTaskPredicate = bool (*)(task::Task* task, void* context);
using DeadTaskPredicate = bool (*)(task::Task* task, void* context);
auto get_active_task_count() -> uint32_t;
auto get_active_task_at(uint32_t index) -> task::Task*;
auto get_active_task_at_safe(uint32_t index) -> task::Task*;
auto find_active_task_lifetime_ref_if(ActiveTaskPredicate predicate, void* context) -> task::Task*;
auto debug_find_task_by_kernel_stack(uint64_t rsp) -> task::Task*;
auto debug_find_dead_task_by_kernel_stack(uint64_t rsp) -> task::Task*;
auto get_dead_task_count(uint64_t cpu_no) -> size_t;
auto get_dead_task_at_safe(uint64_t cpu_no, size_t index) -> task::Task*;
auto find_dead_task_lifetime_ref_if(DeadTaskPredicate predicate, void* context) -> task::Task*;

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
    maybe_exit_current_kernel_thread_for_shutdown();
    if (preempt_count() != 0) {
        note_preempt_disabled_block("kern_yield", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->set_wait_channel("kern_yield");
        task->set_voluntary_blocked(true);
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->set_voluntary_blocked(false);
            task->clear_wait_channel();
            return;
        }
        request_local_timer_recheck();
    }
    bool const PAUSED_SYSCALL_ACCOUNTING = pause_syscall_accounting();
    halt_once_preserving_interrupt_state(INTERRUPTS_WERE_ENABLED);
    if (task != nullptr) {
        (void)task->wakeup_pending.exchange(false, std::memory_order_acquire);
        task->set_voluntary_blocked(false);
        task->clear_wait_channel();
    }
    if (PAUSED_SYSCALL_ACCOUNTING) {
        resume_syscall_accounting();
    }
    maybe_exit_current_kernel_thread_for_shutdown();
}

// kern_block() - request that the current DAEMON task be truly blocked
// (moved to wait list) on the next timer tick, then hlt to give up the CPU.
// The timer interrupt handler sees wants_block=true and moves the task to the
// wait list before picking the next runnable task - safe because it runs
// under the run queue lock with interrupts disabled.
// Must only be called from DAEMON kernel threads.
inline void kern_block_impl(uint64_t perf_callsite) {
    maybe_exit_current_kernel_thread_for_shutdown();
    if (preempt_count() != 0) {
        note_preempt_disabled_block("kern_block", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->set_wait_channel("kern_block");
        task->wants_block = true;
        task->set_voluntary_blocked(true);
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wants_block = false;
            task->set_voluntary_blocked(false);
            task->clear_wait_channel();
            return;
        }
        request_local_timer_recheck();
    }
    bool const PAUSED_SYSCALL_ACCOUNTING = pause_syscall_accounting();
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
        task->set_voluntary_blocked(false);
        task->clear_wait_channel();
    }
    if (PAUSED_SYSCALL_ACCOUNTING) {
        resume_syscall_accounting();
    }
    maybe_exit_current_kernel_thread_for_shutdown();
}

// kern_sleep_us() -- block the current DAEMON task until the timer wake scan
// reaches the requested deadline. This avoids keeping timer/worker threads as
// the current task on otherwise idle CPUs.
inline void kern_sleep_us_impl(uint64_t sleep_us, uint64_t perf_callsite) {
    maybe_exit_current_kernel_thread_for_shutdown();
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
        task->set_wait_channel("kern_sleep");
        task->wake_at_us = saturating_deadline_us(ker::mod::time::get_us(), sleep_us);
        task->wants_block = true;
        task->set_voluntary_blocked(true);
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wake_at_us = 0;
            task->wants_block = false;
            task->set_voluntary_blocked(false);
            task->clear_wait_channel();
            return;
        }
        request_local_timer_recheck();
    }
    bool const PAUSED_SYSCALL_ACCOUNTING = pause_syscall_accounting();
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
        task->set_voluntary_blocked(false);
        task->clear_wait_channel();
    }
    if (PAUSED_SYSCALL_ACCOUNTING) {
        resume_syscall_accounting();
    }
    maybe_exit_current_kernel_thread_for_shutdown();
}

// preemptible_syscall_park() -- park a PROCESS syscall wait loop at a scheduler
// safe point, then return to the same in-kernel loop after an event, signal, or
// timeout wake. Unlike deferred_task_switch, this never returns to userspace
// with a private retry code.
inline void preemptible_syscall_park_impl(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us,
                                          uint64_t perf_callsite) {
    if (preempt_count() != 0) {
        note_preempt_disabled_block("preemptible_syscall_park", perf_callsite);
    }
    bool const INTERRUPTS_WERE_ENABLED = interrupts_enabled();
    auto* task = get_current_task();
    if (task != nullptr) {
        task->perf_wait_callsite = perf_callsite;
        task->set_wait_channel(wait_channel, wait_kind);
        task->wake_at_us = deadline_us;
        task->wants_block = true;
        task->set_voluntary_blocked(true);
        if (task->wakeup_pending.exchange(false, std::memory_order_acquire)) {
            task->wake_at_us = 0;
            task->wants_block = false;
            task->set_voluntary_blocked(false);
            task->clear_wait_channel();
            return;
        }
        if (task->has_interrupting_signal_pending()) {
            task->wake_at_us = 0;
            task->wants_block = false;
            task->set_voluntary_blocked(false);
            task->clear_wait_channel();
            return;
        }
        request_local_timer_recheck();
    }
    bool const PAUSED_SYSCALL_ACCOUNTING = pause_syscall_accounting();
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
        task->set_voluntary_blocked(false);
        task->clear_wait_channel();
    }
    if (PAUSED_SYSCALL_ACCOUNTING) {
        resume_syscall_accounting();
    }
}

inline void preemptible_syscall_park_impl(const char* wait_channel, uint64_t deadline_us, uint64_t perf_callsite) {
    preemptible_syscall_park_impl(wait_channel, task::WaitChannelKind::GENERIC, deadline_us, perf_callsite);
}

inline void preemptible_syscall_park_impl(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t perf_callsite) {
    preemptible_syscall_park_impl(wait_channel, wait_kind, 0, perf_callsite);
}

inline void preemptible_syscall_park_impl(const char* wait_channel, uint64_t perf_callsite) {
    preemptible_syscall_park_impl(wait_channel, 0, perf_callsite);
}

// kern_wake() - move a blocked DAEMON task back to the runnable heap.
// Safe to call from interrupt context or any CPU.
// No-op if the task is already runnable.
void kern_wake(task::Task* task);

constexpr auto kern_yield() { ker::mod::sched::kern_yield_impl(WOS_PERF_CALLSITE()); }
constexpr auto kern_block() { ker::mod::sched::kern_block_impl(WOS_PERF_CALLSITE()); }
constexpr auto kern_sleep_us(uint64_t sleep_us) { ker::mod::sched::kern_sleep_us_impl(sleep_us, WOS_PERF_CALLSITE()); }
constexpr auto preemptible_syscall_park(const char* wait_channel, uint64_t deadline_us = 0) {
    ker::mod::sched::preemptible_syscall_park_impl(wait_channel, deadline_us, WOS_PERF_CALLSITE());
}
constexpr auto preemptible_syscall_park(const char* wait_channel, task::WaitChannelKind wait_kind, uint64_t deadline_us = 0) {
    ker::mod::sched::preemptible_syscall_park_impl(wait_channel, wait_kind, deadline_us, WOS_PERF_CALLSITE());
}

}  // namespace ker::mod::sched

// ker::mod::sched::kern_yield() valid after macro expansion.
// NOLINTBEGIN(cppcoreguidelines-macro-usage, readability-identifier-naming)
#define kern_yield() kern_yield_impl(WOS_PERF_CALLSITE())
#define kern_block() kern_block_impl(WOS_PERF_CALLSITE())
#define kern_sleep_us(sleep_us) kern_sleep_us_impl((sleep_us), WOS_PERF_CALLSITE())
#define preemptible_syscall_park(...) preemptible_syscall_park_impl(__VA_ARGS__, WOS_PERF_CALLSITE())
// NOLINTEND(cppcoreguidelines-macro-usage, readability-identifier-naming)

extern "C" auto wos_get_current_task() -> ker::mod::sched::task::Task*;
extern "C" void wos_commit_handoff_task();
extern "C" const uint64_t WOS_DEFERRED_TASK_SWITCH_OFFSET;
extern "C" void wos_deferred_task_switch_return(ker::mod::cpu::GPRegs* gpr_ptr, ker::mod::gates::InterruptFrame* frame_ptr);
extern "C" [[noreturn]] void wos_start_kernel_thread(uint64_t stack_top, void (*entry)());
