#include "scheduler.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/asm/segment.hpp>
#include <platform/sys/userspace.hpp>
// Debug helpers
#include <platform/loader/debug_info.hpp>
#include <platform/loader/gdb_interface.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>

#include "epoch.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/sys/context_switch.hpp"
#include "platform/sys/signal.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "util/hcf.hpp"

// Kernel idle loop - defined in context_switch.asm
extern "C" void _wOS_kernel_idle_loop();  // NOLINT(readability-identifier-naming)

namespace ker::mod::sched {

// D17: WKI remote placement hook (nullptr when WKI not active)
bool (*wki_try_remote_placement_fn)(task::Task* task) = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// Global state
// ============================================================================
// PID hash table for O(1) amortized lookups.
// Open-addressing with linear probing, backward-shift deletion.
// PIDs are 64-bit monotonically increasing; the table supports up to MAX_PIDS
// concurrent entries. Hash spreads sequential PIDs across the table.
struct PidHashEntry {
    uint64_t pid;      // 0 = empty slot
    task::Task* task;  // nullptr when slot empty
};
namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::array<PidHashEntry, MAX_PIDS> pid_table = {PidHashEntry{.pid = 0, .task = nullptr}};

// Active PID tracking for fast process-group iteration (avoids scanning 16M-entry hash table)
constexpr uint32_t MAX_ACTIVE_TASKS = 2048;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::array<task::Task*, MAX_ACTIVE_TASKS> active_task_list = {nullptr};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t active_task_count = 0;

void active_list_insert(task::Task* t) {
    if (active_task_count < MAX_ACTIVE_TASKS) {
        active_task_list[active_task_count++] = t;
    }
}

void active_list_remove(uint64_t pid) {
    for (uint32_t i = 0; i < active_task_count; i++) {
        if (active_task_list[i] != nullptr && active_task_list[i]->pid == pid) {
            active_task_list[i] = active_task_list[--active_task_count];
            active_task_list[active_task_count] = nullptr;
            return;
        }
    }
}

inline uint32_t pid_hash(uint64_t pid) {
    // Knuth multiplicative hash — spreads sequential PIDs well
    return (uint32_t)((pid * 11400714819323198485ULL) >> 40) & (MAX_PIDS - 1);
}

bool pid_table_insert(task::Task* t) {
    uint32_t slot = pid_hash(t->pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t idx = (slot + i) & (MAX_PIDS - 1);
        if (pid_table[idx].pid == 0) {
            pid_table[idx].pid = t->pid;
            pid_table[idx].task = t;
            return true;
        }
        if (pid_table[idx].pid == t->pid) {
            // Slot already taken by same PID — update pointer (shouldn't happen)
            pid_table[idx].task = t;
            return true;
        }
    }
    return false;  // Table full — MAX_PIDS concurrent processes exceeded
}

task::Task* pid_table_find(uint64_t pid) {
    if (pid == 0) {
        return nullptr;
    }
    uint32_t slot = pid_hash(pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t idx = (slot + i) & (MAX_PIDS - 1);
        if (pid_table[idx].pid == 0) {
            return nullptr;  // Empty slot — not found
        }
        if (pid_table[idx].pid == pid) {
            return pid_table[idx].task;
        }
    }
    return nullptr;
}

void pid_table_remove(uint64_t pid) {
    if (pid == 0) {
        return;
    }
    uint32_t slot = pid_hash(pid);
    uint32_t idx = 0;
    bool found = false;

    // Find the entry
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        idx = (slot + i) & (MAX_PIDS - 1);
        if (pid_table[idx].pid == 0) {
            return;  // Not in table
        }
        if (pid_table[idx].pid == pid) {
            found = true;
            break;
        }
    }
    if (!found) {
        return;
    }

    // Remove and backward-shift to maintain probe chains
    pid_table[idx].pid = 0;
    pid_table[idx].task = nullptr;

    uint32_t next = (idx + 1) & (MAX_PIDS - 1);
    while (pid_table[next].pid != 0) {
        uint32_t natural = pid_hash(pid_table[next].pid);
        // Check if 'next' would benefit from being moved to 'idx'.
        // Condition: natural slot of 'next' is in the wrapped range (natural, idx].
        bool shift = false;
        if (idx < next) {
            shift = (natural <= idx || natural > next);
        } else {
            shift = (natural <= idx && natural > next);
        }
        if (shift) {
            pid_table[idx] = pid_table[next];
            pid_table[next].pid = 0;
            pid_table[next].task = nullptr;
            idx = next;
        }
        next = (next + 1) & (MAX_PIDS - 1);
    }
}

// Per-CPU run queues with spinlocks for cross-CPU access
smt::PerCpuCrossAccess<RunQueue>* run_queues;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Debug: Per-CPU current task pointers for panic inspection
std::array<task::Task*, desc::gdt::MAX_CPUS> debug_task_ptrs = {nullptr};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Vector allocated at init time for scheduler wake IPIs.
// Must not conflict with device driver IRQ allocations.
uint8_t wake_ipi_vector = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// Internal helpers
// ============================================================================

void request_local_reschedule();

// Restore kernel GS_BASE before entering idle loop.
// Ensures cpu::currentCpu() returns the correct CPU index when
// timer interrupts wake us from idle and call processTasks.
inline void restore_kernel_gs_for_idle() {
    uint32_t apic_id = apic::getApicId();
    uint64_t cpu_idx = smt::getCpuIndexFromApicId(apic_id);
    cpu::PerCpu* kernel_per_cpu = smt::getKernelPerCpu(cpu_idx);
    if (kernel_per_cpu != nullptr) {
        cpu::wrgsbase(reinterpret_cast<uint64_t>(kernel_per_cpu));
    }
}

// Compute EEVDF weighted average vruntime using relative-key representation.
// avg = minVruntime + totalWeightedVruntime / totalWeight
// The relative-key approach keeps totalWeightedVruntime bounded by the spread
// of vruntimes (not their absolute magnitude), preventing int64 overflow.
inline int64_t compute_avg_vruntime(RunQueue* rq) {
    if (rq->totalWeight <= 0) {
        return rq->minVruntime;
    }
    return rq->minVruntime + (rq->totalWeightedVruntime / rq->totalWeight);
}

// Add a task's EEVDF contribution to the run queue aggregate sums.
// Call AFTER setting task->vruntime and inserting into the heap.
inline void add_to_sums(RunQueue* rq, task::Task* t) {
    rq->totalWeight += (int64_t)t->schedWeight;
    rq->totalWeightedVruntime += (t->vruntime - rq->minVruntime) * (int64_t)t->schedWeight;
}

// Remove a task's EEVDF contribution from the run queue aggregate sums.
// Call when removing a task from the runnable heap.
inline void remove_from_sums(RunQueue* rq, task::Task* t) {
    rq->totalWeight -= (int64_t)t->schedWeight;
    rq->totalWeightedVruntime -= (t->vruntime - rq->minVruntime) * (int64_t)t->schedWeight;
}

inline uint32_t compute_effective_load_locked(RunQueue* rq, task::TaskType incoming_type = task::TaskType::DAEMON) {
    constexpr uint32_t full_load = 8;
    constexpr uint32_t daemon_load = 1;

    uint32_t load = rq->runnableHeap.size * full_load;
    auto* cur = rq->currentTask;
    bool cur_in_heap = cur != nullptr && rq->runnableHeap.contains(cur);

    if (cur != nullptr && cur->voluntaryBlock && cur_in_heap && load >= full_load) {
        load -= full_load;
    }

    bool has_active = (cur != nullptr && cur->type != task::TaskType::IDLE && !cur->voluntaryBlock);
    if (has_active && !cur_in_heap) {
        bool daemon_is_background = (incoming_type == task::TaskType::PROCESS && cur->type == task::TaskType::DAEMON);
        load += daemon_is_background ? daemon_load : full_load;
    }

    load += rq->placementReservations.load(std::memory_order_relaxed) * full_load;
    return load;
}

uint64_t get_least_loaded_cpu_for_task(task::Task* incoming_task) {
    if (run_queues == nullptr) {
        return 0;
    }

    uint64_t cpu_count = smt::getCoreCount();
    if (cpu_count <= 1) {
        return 0;
    }

    task::TaskType incoming_type = incoming_task != nullptr ? incoming_task->type : task::TaskType::DAEMON;

    static std::atomic<uint64_t> rr_seed{0};
    uint64_t start = rr_seed.fetch_add(1, std::memory_order_relaxed) % cpu_count;

    uint64_t best_cpu = start;
    uint32_t best_load = UINT32_MAX;

    for (uint64_t off = 0; off < cpu_count; ++off) {
        uint64_t cpu_no = (start + off) % cpu_count;
        uint32_t load = run_queues->withLock(
            cpu_no, [incoming_type](RunQueue* rq) -> uint32_t { return compute_effective_load_locked(rq, incoming_type); });

        if (load < best_load) {
            best_load = load;
            best_cpu = cpu_no;
        }
        if (best_load == 0) {
            return best_cpu;
        }
    }

    return best_cpu;
}

uint64_t reserve_least_loaded_cpu(task::Task* incoming_task) {
    uint64_t best_cpu = get_least_loaded_cpu_for_task(incoming_task);
    auto* rq = run_queues != nullptr ? run_queues->thatCpu(best_cpu) : nullptr;
    if (rq != nullptr) {
        rq->placementReservations.fetch_add(1, std::memory_order_relaxed);
    }
    return best_cpu;
}

void release_cpu_reservation(uint64_t cpu_no) {
    auto* rq = run_queues != nullptr ? run_queues->thatCpu(cpu_no) : nullptr;
    if (rq != nullptr) {
        rq->placementReservations.fetch_sub(1, std::memory_order_relaxed);
    }
}

bool post_task_for_cpu_impl(uint64_t cpu_no, task::Task* task, bool release_reservation) {
#ifdef SCHED_DEBUG
    dbg::log("POST: PID %x '%s' -> CPU %d (heapIdx=%d, from CPU %d)", task->pid, (task->name != nullptr) ? task->name : "?", (int)cpu_no,
             task->heapIndex, (int)cpu::currentCpu());
#endif
    task->cpu = cpu_no;

    if (task->start_time_us == 0) {
        task->start_time_us = time::getUs();
    }

    __atomic_thread_fence(__ATOMIC_RELEASE);

    if (task->type == task::TaskType::IDLE) {
        run_queues->withLockVoid(cpu_no, [task](RunQueue* rq) {
            rq->idleTask = task;
            task->schedQueue = task::Task::SchedQueue::NONE;
        });
        if (release_reservation) {
            release_cpu_reservation(cpu_no);
        }
        return true;
    }

    if (task->pid > 0) {
        pid_table_insert(task);
        active_list_insert(task);
    }

    run_queues->withLockVoid(cpu_no, [task](RunQueue* rq) {
        task->vruntime = (rq->minVruntime > 0) ? rq->minVruntime : 0;
        task->vdeadline = task->vruntime + (((int64_t)task->sliceNs * 1024) / (int64_t)task->schedWeight);
        task->sliceUsedNs = 0;
        task->schedQueue = task::Task::SchedQueue::RUNNABLE;

        rq->runnableHeap.insert(task);
        add_to_sums(rq, task);
    });

    if (release_reservation) {
        release_cpu_reservation(cpu_no);
    }

    if (cpu_no == cpu::currentCpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(cpu_no);
    }

    return true;
}

// Send a scheduler wake IPI to a specific CPU if it's currently idle.
// This ensures idle CPUs don't sleep up to 10ms before noticing new work.
static void request_local_reschedule() {
    auto* rq = run_queues->thisCpu();
    if (rq != nullptr) {
        rq->localRescheduleRequests.fetch_add(1, std::memory_order_relaxed);
    }
    sys::context_switch::request_reschedule();
}

static void arm_idle_timer_locked(RunQueue* rq) {
    rq->idleTimerArms.fetch_add(1, std::memory_order_relaxed);
    apic::oneShotTimer(apic::calibrateTimer(10000));
}

// Enter the kernel idle loop on the idle task's stack. Does NOT return.
[[noreturn]] void enter_idle_loop(RunQueue* rq) {
    rq->isIdle.store(true, std::memory_order_release);
    arm_idle_timer_for_this_cpu();

    uint64_t idle_stack =
        (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;

    // CRITICAL: Switch CR3 to the kernel pagemap before entering idle.
    // When a user task exits and we transition to idle, CR3 still points to the
    // exited task's pagemap. If GC later frees that pagemap's PML4 page and another
    // CPU reuses it, our HHDM mappings break (the kernel half of the PML4 gets
    // overwritten). This caused stack corruption crashes under stress.
    mm::virt::switchToKernelPagemap();

    restore_kernel_gs_for_idle();

    asm volatile(
        "mov %0, %%rsp\n"
        "sti\n"
        "jmp _wOS_kernel_idle_loop"
        :
        : "r"(idle_stack)
        : "memory");
    __builtin_unreachable();
}

// IPI handler for scheduler wake-up.
// NOTE: Do NOT call apic::eoi() here — the generic interrupt_handler already
// sends EOI after dispatching interruptHandlers[].
void scheduler_wake_handler([[maybe_unused]] cpu::GPRegs gpr, [[maybe_unused]] gates::interruptFrame frame) {
    auto* rq = run_queues->thisCpu();
    if (rq->runnableHeap.size > 0) {
        rq->isIdle.store(false, std::memory_order_release);
        // Trigger an immediate scheduling pass via the timer path.
        request_local_reschedule();
    }
}

}  // namespace
// Unconditional wake IPI — breaks a CPU out of hlt regardless of scheduler
// idle state.  Used by NAPI to wake worker threads that sleep via sti;hlt
// as the currentTask (so isIdle is false, and wake_idle_cpu would skip them).
void wake_cpu(uint64_t cpu_no) {
    if (wake_ipi_vector == 0) {
        return;
    }
    if (cpu_no == cpu::currentCpu()) {
        return;
    }

    apic::IPIConfig ipi{
        .vector = wake_ipi_vector,
        .deliveryMode = apic::IPIDeliveryMode::FIXED,
        .destinationMode = apic::IPIDestinationMode::PHYSICAL,
        .level = apic::IPILevel::ASSERT,
        .triggerMode = apic::IPITriggerMode::EDGE,
        .destinationShorthand = apic::IPIDestinationShorthand::NONE,
    };

    auto* rq = run_queues != nullptr ? run_queues->thatCpu(cpu_no) : nullptr;
    if (rq != nullptr) {
        rq->wakeIpisSent.fetch_add(1, std::memory_order_relaxed);
    }

    uint32_t lapic_id = smt::getCpu(cpu_no).lapic_id;
    apic::sendIpi(ipi, lapic_id);
}

// ============================================================================
// Initialization
// ============================================================================

void init() {
    ker::mod::smt::init();
    run_queues = new smt::PerCpuCrossAccess<RunQueue>();

    // Initialize epoch-based reclamation system
    EpochManager::init();

    // Allocate a dedicated vector for scheduler wake IPIs.
    // Must use allocateVector() rather than a hardcoded vector because device
    // drivers (virtio-net, e1000e, xhci, ivshmem) initialize before the
    // scheduler and consume vectors starting from 0x30 via allocateVector().
    wake_ipi_vector = gates::allocateVector();
    if (wake_ipi_vector != 0) {
        gates::setInterruptHandler(wake_ipi_vector, scheduler_wake_handler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void setup_queues() {
    // This is the portion of init() after smt::init() and EpochManager::init()
    // Used by the init dependency system for finer-grained control
    run_queues = new smt::PerCpuCrossAccess<RunQueue>();

    // Allocate a dedicated vector for scheduler wake IPIs.
    wake_ipi_vector = gates::allocateVector();
    if (wake_ipi_vector != 0) {
        gates::setInterruptHandler(wake_ipi_vector, scheduler_wake_handler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void percpu_init() {
    auto cpu_no = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu_no);
    // RunQueue constructor already initializes all fields (heap, lists, counters).
    // Set the initial HPET timestamp so the first timer tick has a valid delta.
    run_queues->thisCpu()->lastTickUs = time::getUs();
}

// ============================================================================
// Task posting
// ============================================================================

auto post_task(task::Task* task) -> bool { return post_task_for_cpu_impl(cpu::currentCpu(), task, false); }

bool post_task_for_cpu(uint64_t cpu_no, task::Task* task) { return post_task_for_cpu_impl(cpu_no, task, false); }

auto post_task_pinned_cpu(uint64_t cpu_no, task::Task* task) -> bool {
    task->cpu_pinned = true;
    return post_task_for_cpu(cpu_no, task);
}

auto post_task_balanced(task::Task* task) -> bool {
    // D17: Try remote placement if WKI is active and task is a user process
    if (wki_try_remote_placement_fn != nullptr && task->type == task::TaskType::PROCESS) {
        if (wki_try_remote_placement_fn(task)) {
            return true;  // Successfully submitted remotely
        }
    }

    uint64_t target_cpu = reserve_least_loaded_cpu(task);
    task->cpu = target_cpu;
    return post_task_for_cpu_impl(target_cpu, task, true);
}

// ============================================================================
// Current task access
// ============================================================================

task::Task* get_current_task() { return run_queues->thisCpu()->currentTask; }

bool has_run_queues() { return run_queues != nullptr; }

void remove_current_task() {
    run_queues->thisCpuLockedVoid([](RunQueue* rq) {
        auto* task = rq->currentTask;
        if (task == nullptr) {
            return;
        }

        // Remove from heap if present
        if (task->schedQueue == task::Task::SchedQueue::RUNNABLE && rq->runnableHeap.contains(task)) {
            remove_from_sums(rq, task);
            rq->runnableHeap.remove(task);
        }

        // Move to dead list for GC
        task->schedQueue = task::Task::SchedQueue::DEAD_GC;
        rq->deadList.push(task);
        rq->currentTask = nullptr;
    });
}

// ============================================================================
// processTasks — timer interrupt hot path (EEVDF)
// ============================================================================

void kern_wake(task::Task* task) {
    if (task == nullptr) {
        return;
    }

    // Always go through reschedule_task_for_cpu — it handles all cases safely
    // under locks: task RUNNABLE (no-op if currentTask), task WAITING (move to heap),
    // or task migrated to another CPU.
    // The fast path inside reschedule_task_for_cpu checks task->cpu first (O(1) common case).
    uint64_t preferred_cpu = task->cpu;
    uint64_t ncpus = smt::getCoreCount();
    if (preferred_cpu >= ncpus) {
        preferred_cpu = get_least_loaded_cpu();
    }
    reschedule_task_for_cpu(preferred_cpu, task);
}

void process_tasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    // Enter epoch critical section — protects task pointers from GC
    EpochGuard epoch_guard;

    // Wake any tasks sleeping via nanosleep whose deadline has passed.
    // Scan this CPU's wait list; tasks with wakeAtUs != 0 are timer sleeps.
    {
        uint64_t now_us = time::getUs();
        run_queues->thisCpuLockedVoid([now_us](RunQueue* rq) {
            // Collect tasks to wake (can't modify list while iterating)
            task::Task* to_wake[16];
            uint32_t wake_count = 0;
            task::Task* t = rq->waitList.head;
            while (t != nullptr && wake_count < 16) {
                if (t->wakeAtUs != 0 && now_us >= t->wakeAtUs) {
                    to_wake[wake_count++] = t;
                }
                t = t->schedNext;
            }
            for (uint32_t i = 0; i < wake_count; i++) {
                task::Task* w = to_wake[i];
                rq->waitList.remove(w);
                int64_t avg = compute_avg_vruntime(rq);
                int64_t fair = avg > rq->minVruntime ? avg : (int64_t)rq->minVruntime;
                w->wakeAtUs = 0;
                w->wantsBlock = false;  // Clear any pending block from kern_block()
                w->vruntime = w->vruntime > fair ? w->vruntime : fair;
                w->vdeadline = w->vruntime + (((int64_t)w->sliceNs * 1024) / (int64_t)w->schedWeight);
                w->sliceUsedNs = 0;
                w->schedQueue = task::Task::SchedQueue::RUNNABLE;
                rq->runnableHeap.insert(w);
                add_to_sums(rq, w);
            }
        });
    }

    auto* rq = run_queues->thisCpu();
    auto* current_task = rq->currentTask;

    // ---- Idle path: no running task, check heap for work ----
    if (current_task == nullptr || current_task->type == task::TaskType::IDLE) {
        task::Task* next_task = run_queues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
            if (rq->runnableHeap.size == 0) {
                return nullptr;
            }
            int64_t avg = compute_avg_vruntime(rq);
            auto* t = rq->runnableHeap.pickBestEligible(avg);
            if (t == nullptr) {
                return nullptr;
            }
            // Validate inside lock before committing
            if (t->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                return nullptr;
            }
            if (t->type == task::TaskType::PROCESS && (t->thread == nullptr || t->pagemap == nullptr)) {
                return nullptr;
            }
            if (t->type == task::TaskType::IDLE) {
                return nullptr;  // Don't switch idle→idle
            }
            // CRITICAL: Set currentTask inside lock to prevent double-scheduling.
            // rescheduleTaskForCpu checks currentTask under this same lock;
            // without this, there's a window where the task can be moved to
            // another CPU's heap between pickBestEligible and the assignment.
            rq->currentTask = t;
            // (runnableHeap.size already decremented by remove above)
            return t;
        });

        if (next_task == nullptr) {
            return;  // Stay idle
        }
#ifdef SCHED_DEBUG
        dbg::log("PICK-IDLE: CPU %d picked PID %x (heapIdx=%d)", (int)cpu::currentCpu(), nextTask->pid, nextTask->heapIndex);
#endif
        rq->isIdle.store(false, std::memory_order_release);
        debug_task_ptrs[cpu::currentCpu()] = next_task;
        next_task->hasRun = true;
        rq->lastTickUs = time::getUs();

        if (!sys::context_switch::switchTo(gpr, frame, next_task)) {
#ifdef SCHED_DEBUG
            dbg::log("PICK-IDLE: CPU %d switchTo FAILED for PID %x", (int)cpu::currentCpu(), nextTask->pid);
#endif
            rq->currentTask = rq->idleTask;
            debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
            rq->isIdle.store(true, std::memory_order_release);
            arm_idle_timer_for_this_cpu();
        }
        return;
    }

    // ---- Running task path: update EEVDF bookkeeping, maybe preempt ----

    // CRITICAL: If the timer fired while a PROCESS task was in kernel mode (e.g.
    // during a syscall), the interrupt frame and GPRegs contain kernel-mode values.
    // We must NOT save these as the task's user-mode context, and we must NOT
    // preempt — the kernel is non-preemptive for PROCESS tasks.
    // DAEMON (kernel thread) tasks are always in kernel mode but MUST be preemptible.
    bool in_kernel_mode = (frame.cs != desc::gdt::GDT_USER_CS);
    bool is_daemon = (current_task->type == task::TaskType::DAEMON);

    // A PROCESS task that set voluntaryBlock is at a safe preemption point
    // (e.g. sti;hlt wait loop in a syscall).  Treat it like a DAEMON for
    // context-save and preemption purposes.
    bool can_preempt_kernel = is_daemon || current_task->voluntaryBlock;

    // Save GPR/frame context: user-mode PROCESS tasks, or DAEMON/voluntaryBlock tasks (always kernel mode but preemptible)
    // NOTE: FPU save is deferred until we know a context switch is actually needed,
    // to avoid the expensive xsave on every timer tick when no switch occurs.
    if (current_task->hasRun && current_task->type != task::TaskType::IDLE) {
        if (can_preempt_kernel || !in_kernel_mode) {
            current_task->context.regs = gpr;
            current_task->context.frame = frame;
        }
    }

    bool blocked_current_task = false;
    task::Task* next_task =
        run_queues->thisCpuLocked([current_task, in_kernel_mode, can_preempt_kernel, &blocked_current_task](RunQueue* rq) -> task::Task* {
            // Compute time delta since last tick
            uint64_t now_us = time::getUs();
            auto delta_us = (int64_t)(now_us - rq->lastTickUs);
            rq->lastTickUs = now_us;
            if (delta_us <= 0) {
                delta_us = 1;
            }
            int64_t delta_ns = delta_us * 1000;

            // Process time accounting: attribute delta to user or system time
            if (current_task->type != task::TaskType::IDLE) {
                if (in_kernel_mode) {
                    current_task->system_time_us += (uint64_t)delta_us;
                } else {
                    current_task->user_time_us += (uint64_t)delta_us;
                }
            }

            // ITIMER_REAL expiry check — fire SIGALRM and reload or disarm
            if (current_task->itimer_real_expire_us != 0 && now_us >= current_task->itimer_real_expire_us) {
                current_task->sigPending |= (1ULL << (14 - 1));  // SIGALRM = 14
                if (current_task->itimer_real_interval_us != 0) {
                    current_task->itimer_real_expire_us = now_us + current_task->itimer_real_interval_us;
                } else {
                    current_task->itimer_real_expire_us = 0;
                }
            }

            // Update vruntime if task is in the heap
            if (rq->runnableHeap.contains(current_task)) {
                int64_t vruntime_delta = (delta_ns * 1024) / (int64_t)current_task->schedWeight;
                current_task->vruntime += vruntime_delta;
                current_task->sliceUsedNs += (uint32_t)delta_ns;

                // Track weighted sum: delta_v * weight = deltaNs * 1024 always
                rq->totalWeightedVruntime += vruntime_delta * (int64_t)current_task->schedWeight;

                // Slice exhausted — reset and recalculate deadline
                if (current_task->sliceUsedNs >= current_task->sliceNs) {
                    current_task->sliceUsedNs = 0;
                    current_task->vdeadline =
                        current_task->vruntime + (((int64_t)current_task->sliceNs * 1024) / (int64_t)current_task->schedWeight);
                }

                // Re-sift in heap after vruntime/vdeadline change
                rq->runnableHeap.update(current_task);
            }

            // Advance minVruntime to weighted average (prevents int64 overflow
            // in totalWeightedVruntime by keeping relative keys small)
            int64_t avg = compute_avg_vruntime(rq);
            if (avg > rq->minVruntime) {
                int64_t delta_min = avg - rq->minVruntime;
                rq->minVruntime = avg;
                rq->totalWeightedVruntime -= delta_min * rq->totalWeight;
            }

            // Don't preempt PROCESS tasks in kernel mode (they're mid-syscall)
            // unless they set voluntaryBlock (safe blocking point).
            // DAEMON tasks are always in kernel mode but must be preemptible.
            if (in_kernel_mode && !can_preempt_kernel) {
                return nullptr;
            }

            // kern_block(): if the current task requested a true block (wantsBlock),
            // move it from the runnable heap to the wait list now, under the lock.
            // This is safe because we're in the timer interrupt with interrupts disabled.
            if (current_task->wantsBlock && rq->runnableHeap.contains(current_task)) {
                current_task->wantsBlock = false;
                remove_from_sums(rq, current_task);
                rq->runnableHeap.remove(current_task);
                current_task->schedQueue = task::Task::SchedQueue::WAITING;
                rq->waitList.push(current_task);
                rq->currentTask = nullptr;
                blocked_current_task = true;
            }

            // Pick best eligible task
            if (rq->runnableHeap.size == 0) {
                return nullptr;
            }
            auto* next = rq->runnableHeap.pickBestEligible(avg);
            if (next == nullptr || next == current_task) {
                return nullptr;
            }

            // EEVDF preemption guard: only preempt the current task if the candidate
            // has a strictly earlier virtual deadline. This lets the current task run
            // its full slice (~10ms) instead of being preempted every timer tick.
            // Without this, equal-weight compute threads ping-pong on every 4ms tick.
            //
            // EXCEPTION: If the current task is at a voluntary block point (hlt in
            // kern_yield), always allow preemption. The task isn't computing — it's
            // sleeping. Without this, halted daemon threads waste their entire 10ms
            // slice doing nothing, starving compute threads on the same CPU.
            if (!current_task->voluntaryBlock && current_task->sliceUsedNs < current_task->sliceNs &&
                rq->runnableHeap.contains(current_task) && next->vdeadline >= current_task->vdeadline) {
                return nullptr;
            }

            // Validate next task state — it might have started exiting
            if (next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                return nullptr;
            }
            if (next->type == task::TaskType::PROCESS && (next->thread == nullptr || next->pagemap == nullptr)) {
                return nullptr;
            }
            if (next->type == task::TaskType::IDLE) {
                return nullptr;
            }

            // CRITICAL: Set currentTask inside lock to prevent double-scheduling.
            // rescheduleTaskForCpu checks currentTask under this same lock;
            // without this, there's a window where the picked task can be
            // moved to another CPU between pickBestEligible and the assignment.
            rq->currentTask = next;
            return next;
        });

    // No switch needed (same task, invalid, kernel mode, or no candidate)
    if (next_task == nullptr) {
        if (blocked_current_task) {
            auto* idle_rq = run_queues->thisCpu();
            idle_rq->currentTask = idle_rq->idleTask;
            idle_rq->isIdle.store(true, std::memory_order_release);
            debug_task_ptrs[cpu::currentCpu()] = idle_rq->idleTask;
            restore_kernel_gs_for_idle();

            frame.rip = reinterpret_cast<uint64_t>(_wOS_kernel_idle_loop);
            frame.cs = 0x08;
            frame.ss = 0x10;
            frame.rsp = (idle_rq->idleTask != nullptr) ? idle_rq->idleTask->context.syscallKernelStack
                                                       : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;
            frame.flags = 0x202;
            gpr = cpu::GPRegs();
        }
        return;
    }
#ifdef SCHED_DEBUG
    dbg::log("PICK-PREEMPT: CPU %d switching PID %x -> PID %x (heapIdx=%d)", (int)cpu::currentCpu(), currentTask->pid, nextTask->pid,
             nextTask->heapIndex);
#endif
    // Perform context switch — currentTask already set inside lock
    // Save FPU state now that we know a switch is needed. This avoids the
    // expensive xsave on every timer tick when the same task continues.
    if (current_task->type == task::TaskType::PROCESS) {
        sys::context_switch::saveFpuState(current_task);
    }

    task::Task* original_task = current_task;
    debug_task_ptrs[cpu::currentCpu()] = next_task;
    rq->isIdle.store(false, std::memory_order_release);
    next_task->hasRun = true;

    if (!sys::context_switch::switchTo(gpr, frame, next_task)) {
        rq->currentTask = original_task;
        debug_task_ptrs[cpu::currentCpu()] = original_task;
    }
}

// ============================================================================
// jumpToNextTask — called after task exit to switch to next task
// ============================================================================

void jump_to_next_task(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    apic::eoi();

    // Manually enter epoch critical section. This function doesn't return
    // normally (goes through switchTo→iretq or idle loop), so the guard
    // would never destruct. The critical section is "leaked" but OK:
    // the next processTasks call will create a new EpochGuard.
    EpochManager::enterCritical();

    task::Task* exiting_task = get_current_task();
#ifdef SCHED_DEBUG
    dbg::log("jumpToNextTask: CPU %d exitingTask=%p pid=%x", cpu::currentCpu(), exitingTask, exitingTask ? exitingTask->pid : 0);
#endif

    // Under lock: ensure exiting task is removed, pick next from heap
    task::Task* next_task = run_queues->thisCpuLocked([exiting_task](RunQueue* rq) -> task::Task* {
        // Remove exiting task from wherever it is
        if (exiting_task != nullptr) {
            if (exiting_task->schedQueue == task::Task::SchedQueue::RUNNABLE && rq->runnableHeap.contains(exiting_task)) {
                remove_from_sums(rq, exiting_task);
                rq->runnableHeap.remove(exiting_task);
            }
            // Also try removing from waitList in case of race
            if (exiting_task->schedQueue == task::Task::SchedQueue::WAITING) {
                rq->waitList.remove(exiting_task);
            }
            // Move to dead list if not already there
            if (exiting_task->schedQueue != task::Task::SchedQueue::DEAD_GC) {
                exiting_task->schedQueue = task::Task::SchedQueue::DEAD_GC;
                rq->deadList.push(exiting_task);
            }
        }

        // Pick next task from heap
        if (rq->runnableHeap.size == 0) {
            rq->currentTask = nullptr;
            return nullptr;
        }
        int64_t avg = compute_avg_vruntime(rq);
        auto* next = rq->runnableHeap.pickBestEligible(avg);
        // CRITICAL: Set currentTask inside lock to prevent double-scheduling.
        // rescheduleTaskForCpu checks currentTask under this same lock.
        rq->currentTask = next;
        return next;
    });

    if (next_task == nullptr) {
        // No runnable tasks — enter idle loop
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: CPU %d: No ready tasks, entering idle", cpu::currentCpu());
#endif
        auto* rq = run_queues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enter_idle_loop(rq);
    }

    // Validate task
    if (next_task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        auto* rq = run_queues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enter_idle_loop(rq);
    }

    if (next_task->type == task::TaskType::PROCESS && (next_task->thread == nullptr || next_task->pagemap == nullptr)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: PID %x missing resources, entering idle", nextTask->pid);
#endif
        auto* rq = run_queues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enter_idle_loop(rq);
    }

    // If idle task: enter idle loop directly
    if (next_task->type == task::TaskType::IDLE) {
        auto* rq = run_queues->thisCpu();
        rq->currentTask = next_task;
        debug_task_ptrs[cpu::currentCpu()] = next_task;
        rq->isIdle.store(true, std::memory_order_release);
        EpochManager::exitCritical();
        enter_idle_loop(rq);
    }

    // Switch to real task
    auto* rq = run_queues->thisCpu();
    rq->isIdle.store(false, std::memory_order_release);
    rq->currentTask = next_task;
    debug_task_ptrs[cpu::currentCpu()] = next_task;
    next_task->hasRun = true;

    if (!sys::context_switch::switchTo(gpr, frame, next_task)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: switchTo FAILED for PID %x", nextTask->pid);
#endif
        rq->currentTask = rq->idleTask;
        rq->isIdle.store(true, std::memory_order_release);
        restore_kernel_gs_for_idle();

        frame.rip = reinterpret_cast<uint64_t>(_wOS_kernel_idle_loop);
        frame.cs = 0x08;
        frame.ss = 0x10;
        frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack
                                              : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;
        frame.flags = 0x202;
        gpr = cpu::GPRegs();
    }
}

// ============================================================================
// startScheduler — initial task entry on each CPU
// ============================================================================

void start_scheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());

    auto* rq = run_queues->thisCpu();

    // Wait for a real (non-idle) task in the heap.
    // CRITICAL: Use lock for the initial check to prevent double-scheduling.
    // A lockless peek creates a race window where rescheduleTaskForCpu can
    // move the task to another CPU between the peek and the currentTask assignment.
    task::Task* first_task = run_queues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
        if (rq->runnableHeap.size == 0) {
            return nullptr;
        }
        int64_t avg = compute_avg_vruntime(rq);
        auto* t = rq->runnableHeap.pickBestEligible(avg);
        if (t != nullptr && t->type != task::TaskType::IDLE && (t->type == task::TaskType::DAEMON || t->thread != nullptr)) {
            rq->currentTask = t;
        }
        return t;
    });

    if (first_task == nullptr || first_task->type == task::TaskType::IDLE) {
        // Set idle task as current while waiting
        rq->currentTask = rq->idleTask;

        for (;;) {
            rq->isIdle.store(true, std::memory_order_release);
            arm_idle_timer_for_this_cpu();
            asm volatile("sti");
            asm volatile("hlt");
            asm volatile("cli");
            rq->isIdle.store(false, std::memory_order_release);

            // Check under lock if we have a non-idle task
            first_task = run_queues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
                if (rq->runnableHeap.size == 0) {
                    return nullptr;
                }

                // Scan heap for a non-idle, ready task
                int64_t avg = compute_avg_vruntime(rq);
                task::Task* candidate = rq->runnableHeap.pickBestEligible(avg);
                if (candidate == nullptr) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::IDLE) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::PROCESS && candidate->thread == nullptr) {
                    return nullptr;
                }

                rq->currentTask = candidate;
                return candidate;
            });

            if (first_task != nullptr) {
                dbg::log("CPU %d: Found task PID %x, starting", cpu::currentCpu(), first_task->pid);
                break;
            }
        }
    }

    // Check if task already ran before marking it — we need to know whether
    // to resume from saved context or start fresh at the entry point.
    bool already_ran = first_task->hasRun;
    first_task->hasRun = true;
    rq->currentTask = first_task;

    // Set up GS/FS MSRs for the first task
    uint64_t real_cpu_id = cpu::currentCpu();
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(first_task->context.syscallScratchArea);
    scratch_area->cpuId = real_cpu_id;
    if (first_task->thread != nullptr) {
#ifdef SCHED_DEBUG
        dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", first_task->thread->fsbase, first_task->thread->gsbase,
                 first_task->context.syscallScratchArea);
#endif
        cpuSetMSR(IA32_GS_BASE, first_task->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, first_task->thread->gsbase);
        cpuSetMSR(IA32_FS_BASE, first_task->thread->fsbase);
    } else {
        // DAEMON or thread-less task: use scratch area for both GS bases
        cpuSetMSR(IA32_GS_BASE, first_task->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, first_task->context.syscallScratchArea);
    }

    mm::virt::switchPagemap(first_task);

    // Update debug task pointer
    debug_task_ptrs[cpu::currentCpu()] = first_task;

    // Initialize HPET timestamp for first tick
    rq->lastTickUs = time::getUs();

    // Start the scheduler timer
    sys::context_switch::startSchedTimer();

    // Restore FPU/SSE/AVX state (PROCESS tasks only, and only if previously saved).
    if (first_task->type == task::TaskType::PROCESS && first_task->fxState.saved) {
        sys::context_switch::restoreFpuState(first_task);
    }

    if (already_ran) {
// Task was already running on another CPU and was migrated here.
// Resume from its saved context instead of restarting at the entry point.
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Resuming PID %x from saved context (rip=0x%x)", cpu::currentCpu(), first_task->pid,
                 first_task->context.frame.rip);
#endif
        _wOS_deferredTaskSwitchReturn(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // DAEMON kernel thread — enter at kthreadEntry in ring 0 via iretq
    if (first_task->type == task::TaskType::DAEMON) {
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Starting kernel thread PID %x '%s' (rip=0x%x)", cpu::currentCpu(), first_task->pid,
                 first_task->name ? first_task->name : "?", first_task->context.frame.rip);
#endif
        _wOS_deferredTaskSwitchReturn(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // Brand new user task — write TLS self-pointer and enter at ELF entry point
    if (first_task->thread != nullptr) {
        *(reinterpret_cast<uint64_t*>(first_task->thread->fsbase)) = first_task->thread->fsbase;
    }

    for (;;) {
        _wOS_asm_enterUsermode(first_task->entry, first_task->context.frame.rsp);
    }
}

// ============================================================================
// deferredTaskSwitch — called from syscall path for yield/block
// ============================================================================

// Check pending signals before restoring a task on the deferred-resume path.
// _wOS_deferredTaskSwitchReturn bypasses syscall.asm's check_pending_signals,
// so we must handle signals here for tasks woken from the wait queue.
// Called with GS and pagemap already switched to `task`.
static void check_pending_signals_deferred(task::Task* task) {
    if (task->type != task::TaskType::PROCESS) {
        return;
    }

    uint64_t deliverable = task->sigPending & ~task->sigMask;
    if (deliverable == 0) {
        return;
    }

    int signo = __builtin_ctzll(deliverable) + 1;
    unsigned idx = static_cast<unsigned>(signo - 1);
    task->sigPending &= ~(1ULL << idx);

    auto& handler = task->sigHandlers[idx];

    if (handler.handler == 0 /*SIG_DFL*/) {
        // Signals with default-ignore action: SIGCHLD, SIGURG, SIGWINCH, SIGCONT
        if (signo == 17 || signo == 23 || signo == 28 || signo == 18) {
            return;
        }
        // Stop signals
        if (signo == 19 || signo == 20 || signo == 21 || signo == 22) {
            task->voluntaryBlock = true;
            return;
        }
        // All other SIG_DFL: fatal terminate.
        // GS/pagemap/rq->currentTask are all set to `task` at this call site.
        ker::syscall::process::wos_proc_exit(128 + signo);
        __builtin_unreachable();
    }

    if (handler.handler == 1 /*SIG_IGN*/) {
        return;
    }

    // User signal handler: set up signal frame on the user stack and redirect context.
    uint64_t userRsp = task->context.frame.rsp;
    uint64_t userRip = task->context.frame.rip;
    uint64_t userRflags = task->context.frame.flags;

    uint64_t frameAddr = (userRsp - sizeof(sys::signal::SignalFrame)) & ~0xFULL;
    auto* sigframe = reinterpret_cast<sys::signal::SignalFrame*>(frameAddr);

    sigframe->pretcode = handler.restorer;
    sigframe->signo = static_cast<uint64_t>(signo);
    sigframe->saved_mask = task->sigMask;
    sigframe->saved_rip = userRip;
    sigframe->saved_rsp = userRsp;
    sigframe->saved_rflags = userRflags;
    sigframe->saved_retval = task->context.regs.rax;

    // GPRs in GPRegs struct are r15..rax (15 fields, same order as saved_regs)
    const uint64_t* regs_arr = reinterpret_cast<const uint64_t*>(&task->context.regs);
    for (int i = 0; i < 15; i++) {
        sigframe->saved_regs[i] = regs_arr[i];
    }

    // Redirect task context to signal handler
    task->context.regs.rdi = static_cast<uint64_t>(signo);  // arg1
    task->context.frame.rip = handler.handler;
    task->context.frame.rsp = frameAddr;

    // Update PerCpu scratch area (GS already points to task's PerCpu)
    auto* perCpu = reinterpret_cast<cpu::PerCpu*>(task->context.syscallScratchArea);
    perCpu->userRsp = frameAddr;
    perCpu->syscallRetRip = handler.handler;

    // Block additional signals for duration of handler
    task->sigMask |= handler.mask;
    if (!(handler.flags & 0x40000000ULL)) {  // !SA_NODEFER
        task->sigMask |= (1ULL << idx);
    }
    task->inSignalHandler = true;
}

extern "C" void deferred_task_switch(ker::mod::cpu::GPRegs* gpr_ptr, [[maybe_unused]] ker::mod::gates::interruptFrame* frame_ptr) {
    if (gpr_ptr == nullptr) {
        return;
    }

    // Epoch guard protects task pointers from GC during switch
    EpochGuard epoch_guard;

    auto* current_task = get_current_task();
    if (current_task == nullptr) {
        return;
    }

    // Build interrupt frame from syscall scratch area (syscall doesn't push one).
    // gs:0x28 = saved RCX (return RIP), gs:0x30 = saved R11 (RFLAGS), gs:0x08 = user RSP
    uint64_t return_rip = 0;
    uint64_t return_flags = 0;
    uint64_t user_rsp = 0;
    asm volatile("movq %%gs:0x28, %0" : "=r"(return_rip));
    asm volatile("movq %%gs:0x30, %0" : "=r"(return_flags));
    asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));

    // Save all GPRs from the syscall stack.
    // syscall.asm passes rsp+8 in RDI; actual GPRegs block starts 8 bytes earlier.
    auto* stack_regs = reinterpret_cast<cpu::GPRegs*>(reinterpret_cast<uint8_t*>(gpr_ptr) - 8);
    current_task->context.regs = *stack_regs;

    // Fix up RAX: syscall return value is stored in the slot past GPRegs
    auto* return_value_slot = reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(stack_regs) + sizeof(cpu::GPRegs));
    current_task->context.regs.rax = *return_value_slot;

    // Restore clobbered RCX/R11
    current_task->context.regs.rcx = return_rip;
    current_task->context.regs.r11 = return_flags;

    current_task->context.frame.intNum = 0;
    current_task->context.frame.errCode = 0;
    current_task->context.frame.rip = return_rip;
    current_task->context.frame.cs = desc::gdt::GDT_USER_CS;
    current_task->context.frame.flags = return_flags;
    current_task->context.frame.rsp = user_rsp;
    current_task->context.frame.ss = desc::gdt::GDT_USER_DS;

    // Save outgoing task's FPU/SSE/AVX state
    sys::context_switch::saveFpuState(current_task);

    bool is_yield = current_task->yieldSwitch;
    current_task->yieldSwitch = false;
    current_task->deferredTaskSwitch = false;

    // Signal race check: if a deliverable signal is already pending, do not
    // move this task to wait queue; resume userspace with EINTR.
    bool skip_wait_queue = false;
    if (!is_yield) {
        uint64_t deliverable = current_task->sigPending & ~current_task->sigMask;
        if (deliverable != 0) {
            skip_wait_queue = true;
            current_task->context.regs.rax = static_cast<uint64_t>(-EINTR);
        }
    }

    // Race check: for blocking waits, verify target hasn't already exited
    static constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
    if (!is_yield && current_task->waitingForPid != 0) {
        if (current_task->waitingForPid == WAIT_ANY_CHILD) {
            // Wait-for-any-child: scan for an exited child of this task
            uint32_t count = get_active_task_count();
            for (uint32_t i = 0; i < count; i++) {
                auto* child = get_active_task_at(i);
                if (child != nullptr && child->parentPid == current_task->pid && child->hasExited && !child->waitedOn) {
                    skip_wait_queue = true;
                    current_task->context.regs.rax = child->pid;
                    if (current_task->waitStatusPhysAddr != 0) {
                        auto* status_ptr = reinterpret_cast<int32_t*>(mm::addr::getVirtPointer(current_task->waitStatusPhysAddr));
                        *status_ptr = child->exitStatus;
                    }
                    if (current_task->waitRusagePhysAddr != 0) {
                        auto* ru =
                            reinterpret_cast<syscall::process::KernRusage*>(mm::addr::getVirtPointer(current_task->waitRusagePhysAddr));
                        ru->ru_utime_sec = (int64_t)(child->user_time_us / 1000000ULL);
                        ru->ru_utime_usec = (int64_t)(child->user_time_us % 1000000ULL);
                        ru->ru_stime_sec = (int64_t)(child->system_time_us / 1000000ULL);
                        ru->ru_stime_usec = (int64_t)(child->system_time_us % 1000000ULL);
                        current_task->waitRusagePhysAddr = 0;
                    }
                    current_task->waitingForPid = 0;
                    child->waitedOn = true;
                    break;
                }
            }
        } else {
            // Wait-for-specific-PID
            auto* target = find_task_by_pid(current_task->waitingForPid);
            if (target != nullptr && target->hasExited) {
                skip_wait_queue = true;
                current_task->context.regs.rax = target->pid;
                if (current_task->waitStatusPhysAddr != 0) {
                    auto* status_ptr = reinterpret_cast<int32_t*>(mm::addr::getVirtPointer(current_task->waitStatusPhysAddr));
                    *status_ptr = target->exitStatus;
                }
                if (current_task->waitRusagePhysAddr != 0) {
                    auto* ru = reinterpret_cast<syscall::process::KernRusage*>(mm::addr::getVirtPointer(current_task->waitRusagePhysAddr));
                    ru->ru_utime_sec = (int64_t)(target->user_time_us / 1000000ULL);
                    ru->ru_utime_usec = (int64_t)(target->user_time_us % 1000000ULL);
                    ru->ru_stime_sec = (int64_t)(target->system_time_us / 1000000ULL);
                    ru->ru_stime_usec = (int64_t)(target->system_time_us % 1000000ULL);
                    current_task->waitRusagePhysAddr = 0;
                }
                current_task->waitingForPid = 0;
                // Mark that parent has retrieved exit status (zombie can now be reaped)
                target->waitedOn = true;
            }
        }
    }

    // Under lock: update EEVDF state and pick next task
    task::Task* next_task = run_queues->thisCpuLocked([current_task, is_yield, skip_wait_queue](RunQueue* rq) -> task::Task* {
        // Account for time used during syscall
        uint64_t now_us = time::getUs();
        auto delta_us = (int64_t)(now_us - rq->lastTickUs);
        rq->lastTickUs = now_us;
        if (delta_us <= 0) {
            delta_us = 1;
        }
        int64_t delta_ns = delta_us * 1000;

        // Deferred switch is always from syscall context (kernel mode) — attribute to system time
        if (current_task->type != task::TaskType::IDLE) {
            current_task->system_time_us += (uint64_t)delta_us;
        }

        if (rq->runnableHeap.contains(current_task)) {
            int64_t vruntime_delta = (delta_ns * 1024) / (int64_t)current_task->schedWeight;
            current_task->vruntime += vruntime_delta;
            rq->totalWeightedVruntime += vruntime_delta * (int64_t)current_task->schedWeight;
        }

        if (is_yield || skip_wait_queue) {
            // Yield / target already exited: task stays in heap with fresh deadline
            if (rq->runnableHeap.contains(current_task)) {
                current_task->sliceUsedNs = 0;
                current_task->vdeadline =
                    current_task->vruntime + (((int64_t)current_task->sliceNs * 1024) / (int64_t)current_task->schedWeight);
                rq->runnableHeap.update(current_task);
            }
        } else {
            // Block: remove from heap, add to wait list
            if (rq->runnableHeap.contains(current_task)) {
                remove_from_sums(rq, current_task);
                rq->runnableHeap.remove(current_task);
            }
            current_task->schedQueue = task::Task::SchedQueue::WAITING;
            rq->waitList.push(current_task);
        }

        // Advance minVruntime
        int64_t avg = compute_avg_vruntime(rq);
        if (avg > rq->minVruntime) {
            int64_t delta_min = avg - rq->minVruntime;
            rq->minVruntime = avg;
            rq->totalWeightedVruntime -= delta_min * rq->totalWeight;
        }

        // Pick next task
        if (rq->runnableHeap.size == 0) {
            rq->currentTask = nullptr;
            return nullptr;
        }
        task::Task* next = rq->runnableHeap.pickBestEligible(avg);
        rq->currentTask = next;
        return next;
    });

#ifdef SCHED_DEBUG
    dbg::log("deferredTaskSwitch: Moved PID %x to %s", currentTask->pid,
             isYield ? "yield (heap)" : (skipWaitQueue ? "skip-wait (heap)" : "wait queue"));
#endif

    if (next_task == nullptr || next_task->type == task::TaskType::IDLE) {
        // Enter idle loop
        auto* rq = run_queues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        rq->isIdle.store(true, std::memory_order_release);
        arm_idle_timer_for_this_cpu();

        uint64_t idle_stack = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack
                                                        : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;

        restore_kernel_gs_for_idle();

        asm volatile(
            "mov %0, %%rsp\n"
            "sti\n"
            "jmp _wOS_kernel_idle_loop"
            :
            : "r"(idle_stack)
            : "memory");
        __builtin_unreachable();
    }

    next_task->hasRun = true;

    // Set up GS/FS for next task
    uint64_t real_cpu_id = cpu::currentCpu();
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(next_task->context.syscallScratchArea);
    scratch_area->cpuId = real_cpu_id;

    if (next_task->thread != nullptr) {
        cpu::wrgsbase(next_task->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, next_task->thread->gsbase);
        cpu::wrfsbase(next_task->thread->fsbase);
    } else {
        cpu::wrgsbase(next_task->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, next_task->context.syscallScratchArea);
    }

    mm::virt::switchPagemap(next_task);

    // Validate context before restoring (only for PROCESS tasks — DAEMON uses kernel addresses)
    // Skip validation when voluntaryBlock is set — kernel-mode context is legitimate.
    if (next_task->type == task::TaskType::PROCESS && !next_task->voluntaryBlock) {
        if (next_task->context.frame.cs != desc::gdt::GDT_USER_CS) {
            dbg::log("deferredTaskSwitch: CORRUPT cs=0x%x (expected 0x%x) PID %x", next_task->context.frame.cs, desc::gdt::GDT_USER_CS,
                     next_task->pid);
            hcf();
        }
        if (next_task->context.frame.ss != desc::gdt::GDT_USER_DS) {
            dbg::log("deferredTaskSwitch: CORRUPT ss=0x%x (expected 0x%x) PID %x", next_task->context.frame.ss, desc::gdt::GDT_USER_DS,
                     next_task->pid);
            hcf();
        }
        if (next_task->context.frame.rip >= 0x800000000000ULL) {
            dbg::log("deferredTaskSwitch: CORRUPT rip=0x%x PID %x", next_task->context.frame.rip, next_task->pid);
            hcf();
        }
        if (next_task->context.frame.rsp >= 0x800000000000ULL) {
            dbg::log("deferredTaskSwitch: CORRUPT rsp=0x%x PID %x", next_task->context.frame.rsp, next_task->pid);
            hcf();
        }
    }

    debug_task_ptrs[cpu::currentCpu()] = next_task;

    check_pending_signals_deferred(next_task);

    // Restore incoming task's FPU/SSE/AVX state (PROCESS tasks only).
    if (next_task->type == task::TaskType::PROCESS && next_task->fxState.saved) {
        sys::context_switch::restoreFpuState(next_task);
    }

    _wOS_deferredTaskSwitchReturn(&next_task->context.regs, &next_task->context.frame);
    __builtin_unreachable();
}

// ============================================================================
// placeTaskInWaitQueue — block current task on I/O
// ============================================================================

void place_task_in_wait_queue(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    auto* current_task = get_current_task();
    if (current_task == nullptr) {
        return;
    }

    // Save context
    current_task->context.regs = gpr;
    current_task->context.frame = frame;
    if (current_task->type == task::TaskType::PROCESS) {
        sys::context_switch::saveFpuState(current_task);
    }

    // Under lock: remove from heap, push to wait list, pick next
    task::Task* next_task = run_queues->thisCpuLocked([current_task](RunQueue* rq) -> task::Task* {
        // Remove from heap
        if (rq->runnableHeap.contains(current_task)) {
            remove_from_sums(rq, current_task);
            rq->runnableHeap.remove(current_task);
        }
        current_task->schedQueue = task::Task::SchedQueue::WAITING;
        rq->waitList.push(current_task);

        // Pick next
        if (rq->runnableHeap.size == 0) {
            rq->currentTask = nullptr;
            return nullptr;
        }
        int64_t avg = compute_avg_vruntime(rq);
        task::Task* next = rq->runnableHeap.pickBestEligible(avg);
        rq->currentTask = next;
        return next;
    });
#ifdef SCHED_DEBUG
    dbg::log("placeTaskInWaitQueue: Moved PID %x to wait queue", currentTask->pid);
#endif
    if (next_task != nullptr && next_task->type != task::TaskType::IDLE) {
        next_task->hasRun = true;
        if (!sys::context_switch::switchTo(gpr, frame, next_task)) {
            dbg::log("placeTaskInWaitQueue: switchTo failed, entering idle");
            run_queues->thisCpu()->isIdle.store(true, std::memory_order_release);
            restore_kernel_gs_for_idle();

            frame.rip = reinterpret_cast<uint64_t>(_wOS_kernel_idle_loop);
            frame.cs = 0x08;
            frame.ss = 0x10;
            auto* rq = run_queues->thisCpu();
            frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack
                                                  : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;
            frame.flags = 0x202;
            gpr = cpu::GPRegs();
        }
    } else {
        // Only idle or no tasks — enter idle via iretq
        auto* rq = run_queues->thisCpu();
        rq->currentTask = rq->idleTask;
        rq->isIdle.store(true, std::memory_order_release);
        arm_idle_timer_for_this_cpu();
        restore_kernel_gs_for_idle();

        frame.rip = reinterpret_cast<uint64_t>(_wOS_kernel_idle_loop);
        frame.cs = 0x08;
        frame.ss = 0x10;
        frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack
                                              : reinterpret_cast<uint64_t>(mm::phys::pageAlloc(4096)) + 4096;
        frame.flags = 0x202;
        gpr = cpu::GPRegs();
    }
}

// ============================================================================
// rescheduleTaskForCpu — wake task from wait queue onto target CPU
// ============================================================================

void reschedule_task_for_cpu(uint64_t cpu_no, task::Task* task) {
#ifdef SCHED_DEBUG
    // DIAGNOSTIC: Always log reschedule attempts
    dbg::log("RESCHED: PID %x -> CPU %d (heapIdx=%d, schedQ=%d, curCpu=%d)", task->pid, (int)cpuNo, task->heapIndex, (int)task->schedQueue,
             (int)cpu::currentCpu());
#endif
    // Don't reschedule tasks that are exiting or dead
    auto state = task->state.load(std::memory_order_acquire);
    if (state != task::TaskState::ACTIVE) {
        dbg::log("RESCHED: PID %x SKIP - not ACTIVE (state=%d)", task->pid, (int)state);
        return;
    }

    // Remove from whatever queue the task is in.
    // Optimization: check the task's last known CPU first (O(1) common case).
    // Only fall back to scanning all CPUs if not found there (handles races).
    // CRITICAL: If the task is currentTask on some CPU, that CPU is actively running
    // it (possibly in a syscall). Don't move it — let the timer preempt it naturally.
    bool is_current_on_some_cpu = false;
    uint64_t current_cpu_of_task = UINT64_MAX;
    bool found_and_removed = false;

    // Fast path: task's last CPU
    uint64_t last_cpu = task->cpu;
    uint64_t ncpus_resched = smt::getCoreCount();
    if (last_cpu < ncpus_resched) {
        run_queues->withLockVoid(last_cpu,
                                 [task, &is_current_on_some_cpu, &current_cpu_of_task, &found_and_removed, last_cpu](RunQueue* rq) {
                                     if (rq->currentTask == task) {
                                         is_current_on_some_cpu = true;
                                         current_cpu_of_task = last_cpu;
                                         found_and_removed = true;
                                         return;
                                     }
                                     if (task->schedQueue == task::Task::SchedQueue::WAITING) {
                                         if (rq->waitList.remove(task)) {
                                             found_and_removed = true;
                                         }
                                     }
                                     if (rq->runnableHeap.contains(task)) {
                                         remove_from_sums(rq, task);
                                         rq->runnableHeap.remove(task);
                                         found_and_removed = true;
                                     }
                                 });
    }

    // Slow path: scan all CPUs only if not found on last known CPU
    if (!found_and_removed && !is_current_on_some_cpu) {
        run_queues->thisCpu()->slowRescheduleScans.fetch_add(1, std::memory_order_relaxed);
        for (uint64_t search_cpu = 0; search_cpu < ncpus_resched; ++search_cpu) {
            if (search_cpu == last_cpu) continue;  // already checked
            run_queues->withLockVoid(
                search_cpu, [task, &is_current_on_some_cpu, &current_cpu_of_task, &found_and_removed, search_cpu](RunQueue* rq) {
                    if (found_and_removed || is_current_on_some_cpu) return;
                    if (rq->currentTask == task) {
                        is_current_on_some_cpu = true;
                        current_cpu_of_task = search_cpu;
#ifdef SCHED_DEBUG
                        dbg::log("RESCHED: PID %x is currentTask on CPU %d", task->pid, (int)search_cpu);
#endif
                        return;
                    }
                    if (task->schedQueue == task::Task::SchedQueue::WAITING) {
                        if (rq->waitList.remove(task)) {
                            found_and_removed = true;
#ifdef SCHED_DEBUG
                            dbg::log("RESCHED: PID %x removed from CPU %d waitList", task->pid, (int)search_cpu);
#endif
                        }
                    }
                    if (rq->runnableHeap.contains(task)) {
#ifdef SCHED_DEBUG
                        dbg::log("RESCHED: PID %x found in CPU %d heap (idx=%d), removing", task->pid, (int)search_cpu, task->heapIndex);
#endif
                        remove_from_sums(rq, task);
                        rq->runnableHeap.remove(task);
                        found_and_removed = true;
                    }
                });
        }
    }

    // If the task is currently executing on a CPU, don't re-insert it.
    // It will be picked up by the scheduler on its current CPU when it
    // yields or its syscall returns.
    if (is_current_on_some_cpu) {
#ifdef SCHED_DEBUG
        dbg::log("RESCHED: PID %x ABORT - is currentTask somewhere", task->pid);
#endif
        // If the task is at a voluntary yield point (hlt), wake its CPU so it
        // can check its condition promptly rather than waiting for the next
        // timer tick (up to 1ms).  Use request_reschedule() for same-CPU since
        // wake_cpu() is a no-op for the current CPU.
        if (task->voluntaryBlock && current_cpu_of_task != UINT64_MAX) {
            if (current_cpu_of_task == cpu::currentCpu()) {
                request_local_reschedule();
            } else {
                wake_cpu(current_cpu_of_task);
            }
        }
        return;
    }

// Insert into target CPU's heap with updated vruntime.
// For pinned tasks, always re-insert on the task's own CPU, not the requested cpu_no.
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x INSERT -> CPU %d (heapIdx=%d before insert)", task->pid, (int)cpu_no, task->heapIndex);
#endif
    if (task->cpu_pinned) {
        cpu_no = task->cpu;  // Ignore requested CPU — keep on pinned CPU
    } else {
        task->cpu = cpu_no;
    }
    run_queues->withLockVoid(cpu_no, [task](RunQueue* rq) {
        // Clamp vruntime to [minVruntime, avg_vruntime] on wakeup.
        // Using minVruntime gives the task an artificially early vdeadline
        // that immediately preempts active compute threads — causing thrashing
        // when daemons wake up frequently (e.g. every 4ms for netd/httpd).
        // Using avg_vruntime places the task at the current fair position,
        // so it doesn't cut ahead of tasks that have been running continuously.
        int64_t avg = compute_avg_vruntime(rq);
        int64_t fair_vruntime = std::max((int64_t)rq->minVruntime, avg);
        task->vruntime = std::max(task->vruntime, fair_vruntime);
        task->vdeadline = task->vruntime + ((((int64_t)task->sliceNs * 1024) / (int64_t)task->schedWeight));
        task->sliceUsedNs = 0;
        task->schedQueue = task::Task::SchedQueue::RUNNABLE;
        // Clear any pending block request — we're explicitly waking this task.
        // If wantsBlock is left true, the next timer tick would immediately re-block it.
        task->wantsBlock = false;

        rq->runnableHeap.insert(task);
        add_to_sums(rq, task);
    });

    // Poke the target CPU so the newly-rescheduled task runs promptly.
    // wake_cpu() is a no-op for the current CPU (can't self-IPI), so use
    // request_reschedule() to arm the APIC timer with count=1 instead.
    // This avoids up to 1ms of scheduling latency on same-CPU wakes.
    if (cpu_no == cpu::currentCpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(cpu_no);
    }
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x DONE -> CPU %d (heapIdx=%d)", task->pid, (int)cpuNo, task->heapIndex);
#endif
}

// ============================================================================
// PID lookup (O(1) via registry)
// ============================================================================

auto find_task_by_pid(uint64_t pid) -> task::Task* { return pid_table_find(pid); }

auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {
    auto* task = pid_table_find(pid);
    if (task != nullptr && task->tryAcquire()) {
        return task;
    }
    return nullptr;
}

auto get_active_task_count() -> uint32_t { return active_task_count; }

auto get_active_task_at(uint32_t index) -> task::Task* {
    if (index >= active_task_count) {
        return nullptr;
    }
    return active_task_list[index];
}

void wake_task_for_signal(task::Task* task) {
    if (task == nullptr) {
        return;
    }
    if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE || task->hasExited) {
        return;
    }

    // Nudge the task's current CPU immediately so hlt/deferred paths react fast.
    // This is cheap and avoids latency when signal delivery races scheduler state.
    wake_cpu(task->cpu);

    // If the task is blocked or sleeping in a syscall path, put it back on
    // a runnable queue so it can process pending signals promptly.
    if (task->schedQueue == task::Task::SchedQueue::WAITING || task->deferredTaskSwitch || task->voluntaryBlock) {
        // Use the least-loaded CPU to avoid piling daemon wakeups onto CPUs
        // that are busy running compute threads.
        uint64_t cpu = get_least_loaded_cpu();
        reschedule_task_for_cpu(cpu, task);
    }
}

void signal_process_group(uint64_t pgid, int sig) {
    if (pgid == 0 || sig <= 0 || sig > static_cast<int>(task::Task::MAX_SIGNALS)) {
        return;
    }
    uint64_t mask = 1ULL << (sig - 1);
    for (uint32_t i = 0; i < active_task_count; i++) {
        auto* t = active_task_list[i];
        if (t != nullptr && t->pgid == pgid && !t->hasExited) {
            t->sigPending |= mask;
            wake_task_for_signal(t);
        }
    }
}

// ============================================================================
// Garbage collection
// ============================================================================

void insert_into_dead_list(task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->schedQueue = task::Task::SchedQueue::DEAD_GC;
    run_queues->withLockVoid(0, [task](RunQueue* rq) { rq->deadList.push(task); });
}

void gc_expired_tasks() {
    for (uint64_t cpu_no = 0; cpu_no < smt::getCoreCount(); ++cpu_no) {
        run_queues->withLockVoid(cpu_no, [
#ifdef SCHED_DEBUG
                                             cpu_no
#endif
        ](RunQueue* rq) {
            // Walk dead list, reclaiming tasks whose epoch grace period has elapsed.
            // Because IntrusiveTaskList::remove() only removes one node (not all
            // occurrences), we can safely walk with a restart-on-remove pattern.
            bool made_progress = true;
            while (made_progress) {
                made_progress = false;
                task::Task* cur = rq->deadList.head;
                while (cur != nullptr) {
                    task::Task* next = cur->schedNext;

                    if (cur->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
                        cur = next;
                        continue;
                    }

                    uint64_t death_epoch = cur->deathEpoch.load(std::memory_order_acquire);
                    if (!EpochManager::isSafeToReclaim(death_epoch)) {
#ifdef SCHED_DEBUG
                        static uint64_t epochSkipCount = 0;
                        if (++epochSkipCount % 1000 == 1) {
                            dbg::log("GC: PID %x deathEpoch=%d not safe yet", cur->pid, death_epoch);
                        }
#endif
                        cur = next;
                        continue;
                    }

                    // Check if any CPU still has this task as currentTask
                    bool still_in_use = false;
                    for (uint64_t check_cpu = 0; check_cpu < smt::getCoreCount(); ++check_cpu) {
                        if (run_queues->thatCpu(check_cpu)->currentTask == cur) {
                            still_in_use = true;
#ifdef SCHED_DEBUG
                            dbg::log("GC: PID %x still currentTask on CPU %d", cur->pid, check_cpu);
#endif
                            break;
                        }
                    }
                    if (still_in_use) {
                        cur = next;
                        continue;
                    }

                    uint32_t rc = cur->refCount.load(std::memory_order_acquire);
                    if (rc != 1) {
                        cur = next;
                        continue;
                    }

                    // LINUX-STYLE ZOMBIE BEHAVIOR: Don't reclaim until parent has called waitpid
                    // OR the parent is dead. This keeps the exit status available for waitpid.
                    // EXCEPTION: Threads (isThread) are joined via futex, not waitpid.
                    // They never get waitedOn set, so without this exception they'd
                    // accumulate as zombies forever, bloating the dead list.
                    if (cur->hasExited && !cur->waitedOn && !cur->isThread) {
                        // Check if parent is still alive
                        if (cur->parentPid != 0) {
                            auto* parent = find_task_by_pid(cur->parentPid);
                            if (parent != nullptr && parent->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE) {
                                // Parent is alive and hasn't called waitpid yet - keep zombie
#ifdef SCHED_DEBUG
                                static uint64_t zombieSkipCount = 0;
                                if (++zombieSkipCount % 1000 == 1) {
                                    dbg::log("GC: PID %x is zombie, waiting for parent PID %x to call waitpid", cur->pid, cur->parentPid);
                                }
#endif
                                cur = next;
                                continue;
                            }
                            // Parent is dead - orphaned zombie can be reaped immediately
#ifdef SCHED_DEBUG
                            dbg::log("GC: PID %x is orphaned zombie (parent PID %x dead), reaping", cur->pid, cur->parentPid);
#endif
                        }
                        // No parent (init) or parent is dead - safe to reclaim
                    }

#ifdef SCHED_DEBUG
                    dbg::log("GC: Reclaiming PID %x from CPU %d", cur->pid, cpuNo);
#endif

                    // Validate task struct before freeing
                    bool task_looks_valid = true;

                    if (cur->thread != nullptr) {
                        auto thread_addr = reinterpret_cast<uintptr_t>(cur->thread);
                        if (thread_addr < 0xffff800000000000ULL) {
                            dbg::log("GC: Task %p (PID %x) has invalid thread ptr %p, skipping", cur, cur->pid, cur->thread);
                            task_looks_valid = false;
                        }
                    }
                    if (cur->pagemap != nullptr) {
                        auto pm_addr = reinterpret_cast<uintptr_t>(cur->pagemap);
                        if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
                            dbg::log("GC: Task %p (PID %x) has invalid pagemap ptr %p, skipping", cur, cur->pid, cur->pagemap);
                            task_looks_valid = false;
                        }
                    }

                    if (!task_looks_valid) {
                        rq->deadList.remove(cur);
                        dbg::log("GC: Leaking corrupted task %p to avoid crash", cur);
                        made_progress = true;
                        break;
                    }

                    // Remove from dead list
                    rq->deadList.remove(cur);

                    // Clear PID hash table entry
                    if (cur->pid > 0) {
                        pid_table_remove(cur->pid);
                        active_list_remove(cur->pid);
                    }

                    // Free pagemap.
                    // - DAEMON tasks use the kernel pagemap — must NOT free it.
                    // - Thread tasks (isThread == true) share the owner process's pagemap —
                    //   must NOT free it here; we only free when the last user is gone.
                    // - For process tasks (isThread == false): only free if no sibling
                    //   threads sharing the same pagemap are still alive (not yet DEAD).
                    if (cur->pagemap != nullptr && cur->type != task::TaskType::DAEMON && !cur->isThread) {
                        bool sibling_alive = false;
                        for (uint32_t ai = 0; ai < active_task_count; ai++) {
                            auto* other = active_task_list[ai];
                            if (other != nullptr && other != cur && other->pagemap == cur->pagemap &&
                                other->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
                                sibling_alive = true;
                                break;
                            }
                        }
                        // Also scan the dead lists of all CPUs for threads sharing this
                        // pagemap that haven't been reclaimed yet (still in grace period).
                        if (!sibling_alive) {
                            for (uint64_t scan_cpu = 0; scan_cpu < smt::getCoreCount() && !sibling_alive; scan_cpu++) {
                                auto* dl = run_queues->thatCpu(scan_cpu)->deadList.head;
                                while (dl != nullptr) {
                                    if (dl != cur && dl->pagemap == cur->pagemap) {
                                        sibling_alive = true;
                                        break;
                                    }
                                    dl = dl->schedNext;
                                }
                            }
                        }
                        if (!sibling_alive) {
                            mm::virt::destroyUserSpace(cur->pagemap);
                            mm::phys::pageFree(cur->pagemap);
                        }
                    }

                    // Free thread
                    if (cur->thread != nullptr) {
                        auto* thread_ptr = cur->thread;
                        auto thread_addr = reinterpret_cast<uintptr_t>(thread_ptr);

                        bool thread_in_hhdm = (thread_addr >= 0xffff800000000000ULL && thread_addr < 0xffff900000000000ULL);
                        bool thread_in_kernel_static = (thread_addr >= 0xffffffff80000000ULL && thread_addr < 0xffffffffc0000000ULL);
                        if (!thread_in_hhdm && !thread_in_kernel_static) {
                            dbg::log("GC: Task %p (PID %x) thread ptr %p out of range; skipping", cur, cur->pid, thread_ptr);
                            cur->thread = nullptr;
                        } else {
                            if (thread_ptr->magic != 0xDEADBEEF) {
                                dbg::log("GC: Task %p (PID %x) thread bad magic 0x%x", cur, cur->pid, thread_ptr->magic);
                            } else {
                                thread_ptr->tlsPhysPtr = 0;
                                thread_ptr->stackPhysPtr = 0;
                                threading::destroyThread(thread_ptr);
                            }
                        }
                    }

                    // Free kernel stack
                    if (cur->context.syscallKernelStack != 0) {
                        uint64_t top = cur->context.syscallKernelStack;
                        uint64_t base = 0;
                        if (top > KERNEL_STACK_SIZE) {
                            base = top - KERNEL_STACK_SIZE;
                        }
                        if (base != 0) {
                            mm::phys::pageFree(reinterpret_cast<void*>(base));
                        }
                        cur->context.syscallKernelStack = 0;
                    }

                    // Free scratch area
                    if (cur->context.syscallScratchArea != 0) {
                        auto* sa = reinterpret_cast<cpu::PerCpu*>(cur->context.syscallScratchArea);
                        auto sa_addr = reinterpret_cast<uintptr_t>(sa);
                        bool in_hhdm = (sa_addr >= 0xffff800000000000ULL && sa_addr < 0xffff900000000000ULL);
                        bool in_kernel_static = (sa_addr >= 0xffffffff80000000ULL && sa_addr < 0xffffffffc0000000ULL);
                        if (in_hhdm || in_kernel_static) {
                            delete sa;
                        }
                        cur->context.syscallScratchArea = 0;
                    }

                    // Free name string
                    if (cur->name != nullptr) {
                        const auto* nm = cur->name;
                        auto nm_addr = reinterpret_cast<uintptr_t>(nm);
                        bool in_hhdm = (nm_addr >= 0xffff800000000000ULL && nm_addr < 0xffff900000000000ULL);
                        bool in_kernel_static = (nm_addr >= 0xffffffff80000000ULL && nm_addr < 0xffffffffc0000000ULL);
                        if (in_hhdm || in_kernel_static) {
                            const size_t MAX_NAME_LEN = 1024;
                            bool found_null = false;
                            for (size_t i = 0; i < MAX_NAME_LEN; ++i) {
                                volatile char c = nm[i];
                                if (c == '\0') {
                                    found_null = true;
                                    break;
                                }
                            }
                            if (found_null) {
                                delete[] nm;
                            }
                        }
                        cur->name = nullptr;
                    }

                    // Clean up debug info
                    loader::debug::unregisterProcess(cur->pid);
                    loader::debug::removeGdbDebugInfo(cur->pid);

                    // Free task struct
                    delete cur;

                    made_progress = true;
                    break;  // Restart from head after removal
                }
            }
        });
    }
}

// ============================================================================
// Diagnostics / stats
// ============================================================================

auto get_run_queue_stats(uint64_t cpu_no) -> RunQueueStats {
    RunQueueStats stats = {.active_task_count = 0, .expired_task_count = 0, .wait_queue_count = 0};
    if (run_queues == nullptr) {
        return stats;
    }
    return run_queues->withLock(cpu_no, [](RunQueue* rq) -> RunQueueStats {
        if (rq == nullptr) {
            return {.active_task_count = 0, .expired_task_count = 0, .wait_queue_count = 0};
        }
        return {
            .active_task_count = rq->runnableHeap.size, .expired_task_count = rq->deadList.count, .wait_queue_count = rq->waitList.count};
    });
}

void arm_idle_timer_for_this_cpu() {
    if (run_queues == nullptr) {
        return;
    }
    run_queues->thisCpuLockedVoid([](RunQueue* rq) { arm_idle_timer_locked(rq); });
}

void note_scheduler_timer_interrupt() {
    if (run_queues == nullptr) {
        return;
    }
    auto* rq = run_queues->thisCpu();
    auto* current = rq != nullptr ? rq->currentTask : nullptr;
    if (current == nullptr || current->type == task::TaskType::IDLE) {
        rq->idleTimerWakeups.fetch_add(1, std::memory_order_relaxed);
    }
}

auto get_scheduler_trace_stats(uint64_t cpu_no) -> SchedulerTraceStats {
    if (run_queues == nullptr) {
        return SchedulerTraceStats{};
    }

    return run_queues->withLock(cpu_no, [](RunQueue* rq) -> SchedulerTraceStats {
        if (rq == nullptr) {
            return SchedulerTraceStats{};
        }

        return SchedulerTraceStats{
            .idle_timer_arms = rq->idleTimerArms.load(std::memory_order_relaxed),
            .idle_timer_disarms = rq->idleTimerDisarms.load(std::memory_order_relaxed),
            .idle_timer_wakeups = rq->idleTimerWakeups.load(std::memory_order_relaxed),
            .wake_ipis_sent = rq->wakeIpisSent.load(std::memory_order_relaxed),
            .local_reschedule_requests = rq->localRescheduleRequests.load(std::memory_order_relaxed),
            .slow_reschedule_scans = rq->slowRescheduleScans.load(std::memory_order_relaxed),
            .load_balance_pushes = rq->loadBalancePushes.load(std::memory_order_relaxed),
        };
    });
}

auto get_scheduler_cpu_state(uint64_t cpu_no) -> SchedulerCpuState {
    SchedulerCpuState state{};
    state.cpu_no = cpu_no;
    state.current_name = "?";

    if (run_queues == nullptr) {
        return state;
    }

    return run_queues->withLock(cpu_no, [cpu_no](RunQueue* rq) -> SchedulerCpuState {
        SchedulerCpuState state{};
        state.cpu_no = cpu_no;
        state.current_name = "?";

        if (rq == nullptr) {
            return state;
        }

        state.is_idle = rq->isIdle.load(std::memory_order_acquire);
        state.runnable_count = rq->runnableHeap.size;
        state.wait_queue_count = rq->waitList.count;

        auto* current = rq->currentTask;
        if (current != nullptr) {
            state.current_pid = current->pid;
            state.current_name = current->name != nullptr ? current->name : "?";
            state.current_type = static_cast<uint8_t>(current->type);
            state.current_voluntary_block = current->voluntaryBlock;
            state.current_wants_block = current->wantsBlock;
            state.current_cpu_pinned = current->cpu_pinned;
        }

        return state;
    });
}

void dump_scheduler_trace_stats() {
    if (run_queues == nullptr) {
        return;
    }

    uint64_t cpu_count = smt::getCoreCount();
    for (uint64_t cpu_no = 0; cpu_no < cpu_count; ++cpu_no) {
        auto stats = get_scheduler_trace_stats(cpu_no);
        if (stats.idle_timer_arms == 0 && stats.idle_timer_disarms == 0 && stats.idle_timer_wakeups == 0 && stats.wake_ipis_sent == 0 &&
            stats.local_reschedule_requests == 0 && stats.slow_reschedule_scans == 0 && stats.load_balance_pushes == 0) {
            continue;
        }

        dbg::log("schedstats: cpu%lu idle_arm=%lu idle_disarm=%lu idle_wake=%lu wake_ipi=%lu local_resched=%lu slow_scan=%lu lb_push=%lu",
                 (unsigned long)cpu_no, (unsigned long)stats.idle_timer_arms, (unsigned long)stats.idle_timer_disarms,
                 (unsigned long)stats.idle_timer_wakeups, (unsigned long)stats.wake_ipis_sent,
                 (unsigned long)stats.local_reschedule_requests, (unsigned long)stats.slow_reschedule_scans,
                 (unsigned long)stats.load_balance_pushes);
    }
}

void dump_scheduler_cpu_states() {
    if (run_queues == nullptr) {
        return;
    }

    uint64_t cpu_count = smt::getCoreCount();
    for (uint64_t cpu_no = 0; cpu_no < cpu_count; ++cpu_no) {
        auto state = get_scheduler_cpu_state(cpu_no);
        dbg::log("schedcpu: cpu%lu idle=%u runq=%lu waitq=%lu cur=%lu(%s) type=%u vblk=%u wblk=%u pinned=%u", (unsigned long)state.cpu_no,
                 state.is_idle ? 1U : 0U, (unsigned long)state.runnable_count, (unsigned long)state.wait_queue_count,
                 (unsigned long)state.current_pid, state.current_name, (unsigned)state.current_type,
                 state.current_voluntary_block ? 1U : 0U, state.current_wants_block ? 1U : 0U, state.current_cpu_pinned ? 1U : 0U);
    }
}

auto get_least_loaded_cpu() -> uint64_t {
    if (run_queues == nullptr) {
        return 0;
    }

    uint64_t cpu_count = smt::getCoreCount();
    if (cpu_count <= 1) {
        return 0;
    }

    // Simple load metric: runnableHeap.size plus a running non-idle task.
    // Discount a current task that is sitting at a voluntary blocking point,
    // since it can be preempted immediately and should not make the CPU look
    // busy for fresh thread placement. Also include in-flight placement
    // reservations so burst thread creation does not keep choosing the same CPU
    // before earlier insertions become visible in the steady-state load metric.
    static std::atomic<uint64_t> rr_seed{0};
    uint64_t start = rr_seed.fetch_add(1, std::memory_order_relaxed) % cpu_count;

    uint64_t best_cpu = start;
    uint32_t best_load = UINT32_MAX;

    for (uint64_t off = 0; off < cpu_count; off++) {
        uint64_t i = (start + off) % cpu_count;
        uint32_t load = run_queues->withLock(i, [](RunQueue* rq) -> uint32_t { return compute_effective_load_locked(rq); });

        if (load < best_load) {
            best_load = load;
            best_cpu = i;
        }
        // Short-circuit: load=0 means truly empty CPU
        if (best_load == 0) {
            return best_cpu;
        }
    }
    return best_cpu;
}

size_t get_expired_task_refcounts(uint64_t cpu_no, uint64_t* pids, uint32_t* refcounts, size_t max_entries, size_t start_index) {
    if (run_queues == nullptr || pids == nullptr || refcounts == nullptr || max_entries == 0) {
        return 0;
    }
    return run_queues->withLock(cpu_no, [pids, refcounts, max_entries, start_index](RunQueue* rq) -> size_t {
        size_t count = 0;
        if (rq == nullptr) {
            return 0;
        }
        size_t idx = 0;
        task::Task* cur = rq->deadList.head;
        while (cur != nullptr) {
            if (idx < start_index) {
                idx++;
                cur = cur->schedNext;
                continue;
            }
            if (count >= max_entries) {
                break;
            }
            pids[count] = cur->pid;
            refcounts[count] = cur->refCount.load(std::memory_order_acquire);
            count++;
            idx++;
            cur = cur->schedNext;
        }
        return count;
    });
}

}  // namespace ker::mod::sched

// ============================================================================
// Extern "C" exports for assembly code
// ============================================================================

extern "C" {
const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET = offsetof(ker::mod::sched::task::Task, deferredTaskSwitch);
}

extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

extern "C" auto _wOS_getCurrentPagemap() -> ker::mod::mm::paging::PageTable* {  // NOLINT
    auto* t = ker::mod::sched::get_current_task();
    return (t != nullptr) ? t->pagemap : nullptr;
}
