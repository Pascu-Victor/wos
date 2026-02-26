#include "scheduler.hpp"

#include <algorithm>
#include <atomic>
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
#include "util/hcf.hpp"

// Kernel idle loop - defined in context_switch.asm
extern "C" void _wOS_kernel_idle_loop();

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
static PidHashEntry pidTable[MAX_PIDS];  // BSS, zero-initialized

// Active PID tracking for fast process-group iteration (avoids scanning 16M-entry hash table)
static constexpr uint32_t MAX_ACTIVE_TASKS = 2048;
static task::Task* activeTaskList[MAX_ACTIVE_TASKS];  // BSS, zero-initialized
static uint32_t activeTaskCount = 0;

static void activeListInsert(task::Task* t) {
    if (activeTaskCount < MAX_ACTIVE_TASKS) {
        activeTaskList[activeTaskCount++] = t;
    }
}

static void activeListRemove(uint64_t pid) {
    for (uint32_t i = 0; i < activeTaskCount; i++) {
        if (activeTaskList[i] != nullptr && activeTaskList[i]->pid == pid) {
            activeTaskList[i] = activeTaskList[--activeTaskCount];
            activeTaskList[activeTaskCount] = nullptr;
            return;
        }
    }
}

static inline uint32_t pidHash(uint64_t pid) {
    // Knuth multiplicative hash — spreads sequential PIDs well
    return (uint32_t)((pid * 11400714819323198485ULL) >> 40) & (MAX_PIDS - 1);
}

static bool pidTableInsert(task::Task* t) {
    uint32_t slot = pidHash(t->pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t idx = (slot + i) & (MAX_PIDS - 1);
        if (pidTable[idx].pid == 0) {
            pidTable[idx].pid = t->pid;
            pidTable[idx].task = t;
            return true;
        }
        if (pidTable[idx].pid == t->pid) {
            // Slot already taken by same PID — update pointer (shouldn't happen)
            pidTable[idx].task = t;
            return true;
        }
    }
    return false;  // Table full — MAX_PIDS concurrent processes exceeded
}

static task::Task* pidTableFind(uint64_t pid) {
    if (pid == 0) return nullptr;
    uint32_t slot = pidHash(pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t idx = (slot + i) & (MAX_PIDS - 1);
        if (pidTable[idx].pid == 0) return nullptr;  // Empty slot — not found
        if (pidTable[idx].pid == pid) return pidTable[idx].task;
    }
    return nullptr;
}

static void pidTableRemove(uint64_t pid) {
    if (pid == 0) return;
    uint32_t slot = pidHash(pid);
    uint32_t idx = 0;
    bool found = false;

    // Find the entry
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        idx = (slot + i) & (MAX_PIDS - 1);
        if (pidTable[idx].pid == 0) return;  // Not in table
        if (pidTable[idx].pid == pid) {
            found = true;
            break;
        }
    }
    if (!found) return;

    // Remove and backward-shift to maintain probe chains
    pidTable[idx].pid = 0;
    pidTable[idx].task = nullptr;

    uint32_t next = (idx + 1) & (MAX_PIDS - 1);
    while (pidTable[next].pid != 0) {
        uint32_t natural = pidHash(pidTable[next].pid);
        // Check if 'next' would benefit from being moved to 'idx'.
        // Condition: natural slot of 'next' is in the wrapped range (natural, idx].
        bool shift;
        if (idx < next) {
            shift = (natural <= idx || natural > next);
        } else {
            shift = (natural <= idx && natural > next);
        }
        if (shift) {
            pidTable[idx] = pidTable[next];
            pidTable[next].pid = 0;
            pidTable[next].task = nullptr;
            idx = next;
        }
        next = (next + 1) & (MAX_PIDS - 1);
    }
}

// Per-CPU run queues with spinlocks for cross-CPU access
static smt::PerCpuCrossAccess<RunQueue>* runQueues;

// Debug: Per-CPU current task pointers for panic inspection
static task::Task* debug_task_ptrs[256] = {nullptr};

// ============================================================================
// Internal helpers
// ============================================================================

// Restore kernel GS_BASE before entering idle loop.
// Ensures cpu::currentCpu() returns the correct CPU index when
// timer interrupts wake us from idle and call processTasks.
static inline void restoreKernelGsForIdle() {
    uint32_t apicId = apic::getApicId();
    uint64_t cpuIdx = smt::getCpuIndexFromApicId(apicId);
    cpu::PerCpu* kernelPerCpu = smt::getKernelPerCpu(cpuIdx);
    if (kernelPerCpu != nullptr) {
        cpu::wrgsbase((uint64_t)kernelPerCpu);
    }
}

// Compute EEVDF weighted average vruntime using relative-key representation.
// avg = minVruntime + totalWeightedVruntime / totalWeight
// The relative-key approach keeps totalWeightedVruntime bounded by the spread
// of vruntimes (not their absolute magnitude), preventing int64 overflow.
static inline int64_t computeAvgVruntime(RunQueue* rq) {
    if (rq->totalWeight <= 0) return rq->minVruntime;
    return rq->minVruntime + (rq->totalWeightedVruntime / rq->totalWeight);
}

// Add a task's EEVDF contribution to the run queue aggregate sums.
// Call AFTER setting task->vruntime and inserting into the heap.
static inline void addToSums(RunQueue* rq, task::Task* t) {
    rq->totalWeight += (int64_t)t->schedWeight;
    rq->totalWeightedVruntime += (t->vruntime - rq->minVruntime) * (int64_t)t->schedWeight;
}

// Remove a task's EEVDF contribution from the run queue aggregate sums.
// Call when removing a task from the runnable heap.
static inline void removeFromSums(RunQueue* rq, task::Task* t) {
    rq->totalWeight -= (int64_t)t->schedWeight;
    rq->totalWeightedVruntime -= (t->vruntime - rq->minVruntime) * (int64_t)t->schedWeight;
}

// Vector allocated at init time for scheduler wake IPIs.
// Must not conflict with device driver IRQ allocations.
static uint8_t wake_ipi_vector = 0;

// Send a scheduler wake IPI to a specific CPU if it's currently idle.
// This ensures idle CPUs don't sleep up to 10ms before noticing new work.
static void wakeIdleCpu(uint64_t cpuNo) {
    if (wake_ipi_vector == 0) return;        // No vector allocated
    if (cpuNo == cpu::currentCpu()) return;  // Don't IPI ourselves
    auto* rq = runQueues->thatCpu(cpuNo);
    if (!rq->isIdle.load(std::memory_order_acquire)) return;  // Already running

    apic::IPIConfig ipi{};
    ipi.vector = wake_ipi_vector;
    ipi.deliveryMode = apic::IPIDeliveryMode::FIXED;
    ipi.destinationMode = apic::IPIDestinationMode::PHYSICAL;
    ipi.level = apic::IPILevel::ASSERT;
    ipi.triggerMode = apic::IPITriggerMode::EDGE;
    ipi.destinationShorthand = apic::IPIDestinationShorthand::NONE;

    uint32_t lapicId = smt::getCpu(cpuNo).lapic_id;
    apic::sendIpi(ipi, lapicId);
}

// Unconditional wake IPI — breaks a CPU out of hlt regardless of scheduler
// idle state.  Used by NAPI to wake worker threads that sleep via sti;hlt
// as the currentTask (so isIdle is false, and wakeIdleCpu would skip them).
void wake_cpu(uint64_t cpuNo) {
    if (wake_ipi_vector == 0) return;
    if (cpuNo == cpu::currentCpu()) return;

    apic::IPIConfig ipi{};
    ipi.vector = wake_ipi_vector;
    ipi.deliveryMode = apic::IPIDeliveryMode::FIXED;
    ipi.destinationMode = apic::IPIDestinationMode::PHYSICAL;
    ipi.level = apic::IPILevel::ASSERT;
    ipi.triggerMode = apic::IPITriggerMode::EDGE;
    ipi.destinationShorthand = apic::IPIDestinationShorthand::NONE;

    uint32_t lapicId = smt::getCpu(cpuNo).lapic_id;
    apic::sendIpi(ipi, lapicId);
}

// Enter the kernel idle loop on the idle task's stack. Does NOT return.
[[noreturn]] static void enterIdleLoop(RunQueue* rq) {
    rq->isIdle.store(true, std::memory_order_release);
    apic::oneShotTimer(apic::calibrateTimer(10000));  // 10ms timer

    uint64_t idleStack = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : (uint64_t)mm::phys::pageAlloc(4096) + 4096;

    // CRITICAL: Switch CR3 to the kernel pagemap before entering idle.
    // When a user task exits and we transition to idle, CR3 still points to the
    // exited task's pagemap. If GC later frees that pagemap's PML4 page and another
    // CPU reuses it, our HHDM mappings break (the kernel half of the PML4 gets
    // overwritten). This caused stack corruption crashes under stress.
    mm::virt::switchToKernelPagemap();

    restoreKernelGsForIdle();

    asm volatile(
        "mov %0, %%rsp\n"
        "sti\n"
        "jmp _wOS_kernel_idle_loop"
        :
        : "r"(idleStack)
        : "memory");
    __builtin_unreachable();
}

// IPI handler for scheduler wake-up.
// NOTE: Do NOT call apic::eoi() here — the generic iterrupt_handler already
// sends EOI after dispatching interruptHandlers[].
static void schedulerWakeHandler([[maybe_unused]] cpu::GPRegs gpr, [[maybe_unused]] gates::interruptFrame frame) {
    auto* rq = runQueues->thisCpu();
    if (rq->runnableHeap.size > 0) {
        rq->isIdle.store(false, std::memory_order_release);
    }
}

// ============================================================================
// Initialization
// ============================================================================

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();

    // Initialize epoch-based reclamation system
    EpochManager::init();

    // Allocate a dedicated vector for scheduler wake IPIs.
    // Must use allocateVector() rather than a hardcoded vector because device
    // drivers (virtio-net, e1000e, xhci, ivshmem) initialize before the
    // scheduler and consume vectors starting from 0x30 via allocateVector().
    wake_ipi_vector = gates::allocateVector();
    if (wake_ipi_vector != 0) {
        gates::setInterruptHandler(wake_ipi_vector, schedulerWakeHandler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void setup_queues() {
    // This is the portion of init() after smt::init() and EpochManager::init()
    // Used by the init dependency system for finer-grained control
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();

    // Allocate a dedicated vector for scheduler wake IPIs.
    wake_ipi_vector = gates::allocateVector();
    if (wake_ipi_vector != 0) {
        gates::setInterruptHandler(wake_ipi_vector, schedulerWakeHandler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void percpu_init() {
    auto cpuNo = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpuNo);
    // RunQueue constructor already initializes all fields (heap, lists, counters).
    // Set the initial HPET timestamp so the first timer tick has a valid delta.
    runQueues->thisCpu()->lastTickUs = time::getUs();
}

// ============================================================================
// Task posting
// ============================================================================

auto post_task(task::Task* task) -> bool { return post_task_for_cpu(cpu::currentCpu(), task); }

bool post_task_for_cpu(uint64_t cpuNo, task::Task* task) {
#ifdef SCHED_DEBUG
    dbg::log("POST: PID %x '%s' -> CPU %d (heapIdx=%d, from CPU %d)", task->pid, (task->name != nullptr) ? task->name : "?", (int)cpuNo,
             task->heapIndex, (int)cpu::currentCpu());
#endif
    task->cpu = cpuNo;

    // Set start time if not already set
    if (task->start_time_us == 0) {
        task->start_time_us = time::getUs();
    }

    // Memory barrier to ensure all task fields are visible to other CPUs
    __atomic_thread_fence(__ATOMIC_RELEASE);

    // Idle tasks are stored separately — never in the heap
    if (task->type == task::TaskType::IDLE) {
        runQueues->withLockVoid(cpuNo, [task](RunQueue* rq) {
            rq->idleTask = task;
            task->schedQueue = task::Task::SchedQueue::NONE;
        });
        return true;
    }

    // Register in PID hash table for O(1) lookups
    if (task->pid > 0) {
        pidTableInsert(task);
        activeListInsert(task);
    }

    // Insert into target CPU's runnable heap with EEVDF initialization
    runQueues->withLockVoid(cpuNo, [task](RunQueue* rq) {
        // New task starts at minVruntime — fair position relative to existing tasks.
        // This ensures it doesn't get a huge head start (starting at 0) or penalty.
        task->vruntime = (rq->minVruntime > 0) ? rq->minVruntime : 0;
        task->vdeadline = task->vruntime + ((int64_t)task->sliceNs * 1024) / (int64_t)task->schedWeight;
        task->sliceUsedNs = 0;
        task->schedQueue = task::Task::SchedQueue::RUNNABLE;

        rq->runnableHeap.insert(task);
        addToSums(rq, task);
    });

    // Wake the target CPU if it's idle so it picks up the new task immediately
    wakeIdleCpu(cpuNo);

    return true;
}

auto post_task_balanced(task::Task* task) -> bool {
    // D17: Try remote placement if WKI is active and task is a user process
    if (wki_try_remote_placement_fn != nullptr && task->type == task::TaskType::PROCESS) {
        if (wki_try_remote_placement_fn(task)) {
            return true;  // Successfully submitted remotely
        }
    }

    uint64_t targetCpu = get_least_loaded_cpu();
    task->cpu = targetCpu;
    return post_task_for_cpu(targetCpu, task);
}

// ============================================================================
// Current task access
// ============================================================================

task::Task* get_current_task() { return runQueues->thisCpu()->currentTask; }

bool has_run_queues() { return runQueues != nullptr; }

void remove_current_task() {
    runQueues->thisCpuLockedVoid([](RunQueue* rq) {
        auto* task = rq->currentTask;
        if (task == nullptr) return;

        // Remove from heap if present
        if (task->schedQueue == task::Task::SchedQueue::RUNNABLE && rq->runnableHeap.contains(task)) {
            removeFromSums(rq, task);
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

void process_tasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    // Enter epoch critical section — protects task pointers from GC
    EpochGuard epochGuard;

    auto* rq = runQueues->thisCpu();
    auto* currentTask = rq->currentTask;

    // ---- Idle path: no running task, check heap for work ----
    if (currentTask == nullptr || currentTask->type == task::TaskType::IDLE) {
        task::Task* nextTask = runQueues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
            if (rq->runnableHeap.size == 0) return nullptr;
            int64_t avg = computeAvgVruntime(rq);
            auto* t = rq->runnableHeap.pickBestEligible(avg);
            if (t == nullptr) return nullptr;
            // Validate inside lock before committing
            if (t->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) return nullptr;
            if (t->type == task::TaskType::PROCESS && (t->thread == nullptr || t->pagemap == nullptr)) return nullptr;
            if (t->type == task::TaskType::IDLE) return nullptr;  // Don't switch idle→idle
            // CRITICAL: Set currentTask inside lock to prevent double-scheduling.
            // rescheduleTaskForCpu checks currentTask under this same lock;
            // without this, there's a window where the task can be moved to
            // another CPU's heap between pickBestEligible and the assignment.
            rq->currentTask = t;
            return t;
        });

        if (nextTask == nullptr) return;  // Stay idle
#ifdef SCHED_DEBUG
        dbg::log("PICK-IDLE: CPU %d picked PID %x (heapIdx=%d)", (int)cpu::currentCpu(), nextTask->pid, nextTask->heapIndex);
#endif
        rq->isIdle.store(false, std::memory_order_release);
        debug_task_ptrs[cpu::currentCpu()] = nextTask;
        nextTask->hasRun = true;
        rq->lastTickUs = time::getUs();

        if (!sys::context_switch::switchTo(gpr, frame, nextTask)) {
#ifdef SCHED_DEBUG
            dbg::log("PICK-IDLE: CPU %d switchTo FAILED for PID %x", (int)cpu::currentCpu(), nextTask->pid);
#endif
            rq->currentTask = rq->idleTask;
            debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
            rq->isIdle.store(true, std::memory_order_release);
        }
        return;
    }

    // ---- Running task path: update EEVDF bookkeeping, maybe preempt ----

    // CRITICAL: If the timer fired while a PROCESS task was in kernel mode (e.g.
    // during a syscall), the interrupt frame and GPRegs contain kernel-mode values.
    // We must NOT save these as the task's user-mode context, and we must NOT
    // preempt — the kernel is non-preemptive for PROCESS tasks.
    // DAEMON (kernel thread) tasks are always in kernel mode but MUST be preemptible.
    bool inKernelMode = (frame.cs != desc::gdt::GDT_USER_CS);
    bool isDaemon = (currentTask->type == task::TaskType::DAEMON);

    // A PROCESS task that set voluntaryBlock is at a safe preemption point
    // (e.g. sti;hlt wait loop in a syscall).  Treat it like a DAEMON for
    // context-save and preemption purposes.
    bool canPreemptKernel = isDaemon || currentTask->voluntaryBlock;

    // Save context: user-mode PROCESS tasks, or DAEMON/voluntaryBlock tasks (always kernel mode but preemptible)
    if (currentTask->hasRun && currentTask->type != task::TaskType::IDLE) {
        if (canPreemptKernel || !inKernelMode) {
            currentTask->context.regs = gpr;
            currentTask->context.frame = frame;
        }
    }

    task::Task* nextTask = runQueues->thisCpuLocked([currentTask, inKernelMode, canPreemptKernel](RunQueue* rq) -> task::Task* {
        // Compute time delta since last tick
        uint64_t now_us = time::getUs();
        auto delta_us = (int64_t)(now_us - rq->lastTickUs);
        rq->lastTickUs = now_us;
        if (delta_us <= 0) {
            delta_us = 1;
        }
        int64_t delta_ns = delta_us * 1000;

        // Process time accounting: attribute delta to user or system time
        if (currentTask->type != task::TaskType::IDLE) {
            if (inKernelMode) {
                currentTask->system_time_us += (uint64_t)delta_us;
            } else {
                currentTask->user_time_us += (uint64_t)delta_us;
            }
        }

        // Update vruntime if task is in the heap
        if (rq->runnableHeap.contains(currentTask)) {
            int64_t vruntime_delta = (delta_ns * 1024) / (int64_t)currentTask->schedWeight;
            currentTask->vruntime += vruntime_delta;
            currentTask->sliceUsedNs += (uint32_t)delta_ns;

            // Track weighted sum: delta_v * weight = deltaNs * 1024 always
            rq->totalWeightedVruntime += vruntime_delta * (int64_t)currentTask->schedWeight;

            // Slice exhausted — reset and recalculate deadline
            if (currentTask->sliceUsedNs >= currentTask->sliceNs) {
                currentTask->sliceUsedNs = 0;
                currentTask->vdeadline =
                    currentTask->vruntime + (((int64_t)currentTask->sliceNs * 1024) / (int64_t)currentTask->schedWeight);
            }

            // Re-sift in heap after vruntime/vdeadline change
            rq->runnableHeap.update(currentTask);
        }

        // Advance minVruntime to weighted average (prevents int64 overflow
        // in totalWeightedVruntime by keeping relative keys small)
        int64_t avg = computeAvgVruntime(rq);
        if (avg > rq->minVruntime) {
            int64_t delta_min = avg - rq->minVruntime;
            rq->minVruntime = avg;
            rq->totalWeightedVruntime -= delta_min * rq->totalWeight;
        }

        // Don't preempt PROCESS tasks in kernel mode (they're mid-syscall)
        // unless they set voluntaryBlock (safe blocking point).
        // DAEMON tasks are always in kernel mode but must be preemptible.
        if (inKernelMode && !canPreemptKernel) {
            return nullptr;
        }

        // Pick best eligible task
        if (rq->runnableHeap.size == 0) {
            return nullptr;
        }
        auto* next = rq->runnableHeap.pickBestEligible(avg);
        if (next == nullptr || next == currentTask) {
            return nullptr;
        }

        // Validate next task state — it might have started exiting
        if (next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
            return nullptr;
        }
        if (next->type == task::TaskType::PROCESS && (next->thread == nullptr || next->pagemap == nullptr)) return nullptr;
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
    if (nextTask == nullptr) {
        return;
    }
#ifdef SCHED_DEBUG
    dbg::log("PICK-PREEMPT: CPU %d switching PID %x -> PID %x (heapIdx=%d)", (int)cpu::currentCpu(), currentTask->pid, nextTask->pid,
             nextTask->heapIndex);
#endif
    // Perform context switch — currentTask already set inside lock
    task::Task* original_task = currentTask;
    debug_task_ptrs[cpu::currentCpu()] = nextTask;
    rq->isIdle.store(false, std::memory_order_release);
    nextTask->hasRun = true;

    if (!sys::context_switch::switchTo(gpr, frame, nextTask)) {
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
    task::Task* next_task = runQueues->thisCpuLocked([exiting_task](RunQueue* rq) -> task::Task* {
        // Remove exiting task from wherever it is
        if (exiting_task != nullptr) {
            if (exiting_task->schedQueue == task::Task::SchedQueue::RUNNABLE && rq->runnableHeap.contains(exiting_task)) {
                removeFromSums(rq, exiting_task);
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
        int64_t avg = computeAvgVruntime(rq);
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
        auto* rq = runQueues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enterIdleLoop(rq);
    }

    // Validate task
    if (next_task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        auto* rq = runQueues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enterIdleLoop(rq);
    }

    if (next_task->type == task::TaskType::PROCESS && (next_task->thread == nullptr || next_task->pagemap == nullptr)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: PID %x missing resources, entering idle", nextTask->pid);
#endif
        auto* rq = runQueues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        EpochManager::exitCritical();
        enterIdleLoop(rq);
    }

    // If idle task: enter idle loop directly
    if (next_task->type == task::TaskType::IDLE) {
        auto* rq = runQueues->thisCpu();
        rq->currentTask = next_task;
        debug_task_ptrs[cpu::currentCpu()] = next_task;
        rq->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));
        EpochManager::exitCritical();
        enterIdleLoop(rq);
    }

    // Switch to real task
    auto* rq = runQueues->thisCpu();
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
        restoreKernelGsForIdle();

        frame.rip = (uint64_t)_wOS_kernel_idle_loop;
        frame.cs = 0x08;
        frame.ss = 0x10;
        frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : (uint64_t)mm::phys::pageAlloc(4096) + 4096;
        frame.flags = 0x202;
        gpr = cpu::GPRegs();
    }
}

// ============================================================================
// startScheduler — initial task entry on each CPU
// ============================================================================

void start_scheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());

    auto* rq = runQueues->thisCpu();

    // Wait for a real (non-idle) task in the heap.
    // CRITICAL: Use lock for the initial check to prevent double-scheduling.
    // A lockless peek creates a race window where rescheduleTaskForCpu can
    // move the task to another CPU between the peek and the currentTask assignment.
    task::Task* first_task = runQueues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
        if (rq->runnableHeap.size == 0) {
            return nullptr;
        }
        int64_t avg = computeAvgVruntime(rq);
        auto* t = rq->runnableHeap.pickBestEligible(avg);
        if (t != nullptr && t->type != task::TaskType::IDLE && (t->type == task::TaskType::DAEMON || t->thread != nullptr)) {
            rq->currentTask = t;
        }
        return t;
    });

    if (first_task == nullptr || first_task->type == task::TaskType::IDLE) {
        dbg::log("CPU %d: Only idle task, waiting for work...", cpu::currentCpu());

        // Set idle task as current while waiting
        rq->currentTask = rq->idleTask;

        for (;;) {
            rq->isIdle.store(true, std::memory_order_release);
            apic::oneShotTimer(apic::calibrateTimer(1000));  // Wake every 1ms
            asm volatile("sti");
            asm volatile("hlt");
            asm volatile("cli");
            rq->isIdle.store(false, std::memory_order_release);

            // Check under lock if we have a non-idle task
            first_task = runQueues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
                if (rq->runnableHeap.size == 0) {
                    return nullptr;
                }

                // Scan heap for a non-idle, ready task
                int64_t avg = computeAvgVruntime(rq);
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
    uint32_t apic_id = apic::getApicId();
    uint64_t real_cpu_id = smt::getCpuIndexFromApicId(apic_id);
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(first_task->context.syscallScratchArea);
    scratch_area->cpuId = real_cpu_id;
    if (first_task->thread != nullptr) {
#ifdef SCHED_DEBUG
        dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", firstTask->thread->fsbase, firstTask->thread->gsbase,
                 firstTask->context.syscallScratchArea);
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

    if (already_ran) {
// Task was already running on another CPU and was migrated here.
// Resume from its saved context instead of restarting at the entry point.
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Resuming PID %x from saved context (rip=0x%x)", cpu::currentCpu(), firstTask->pid, firstTask->context.frame.rip);
#endif
        _wOS_deferredTaskSwitchReturn(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // DAEMON kernel thread — enter at kthreadEntry in ring 0 via iretq
    if (first_task->type == task::TaskType::DAEMON) {
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Starting kernel thread PID %x '%s' (rip=0x%x)", cpu::currentCpu(), firstTask->pid,
                 firstTask->name ? firstTask->name : "?", firstTask->context.frame.rip);
#endif
        _wOS_deferredTaskSwitchReturn(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // Brand new user task — write TLS self-pointer and enter at ELF entry point
    if (first_task->thread != nullptr) {
        *((uint64_t*)first_task->thread->fsbase) = first_task->thread->fsbase;
    }

    for (;;) {
        _wOS_asm_enterUsermode(first_task->entry, first_task->context.frame.rsp);
    }
}

// ============================================================================
// deferredTaskSwitch — called from syscall path for yield/block
// ============================================================================

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

    bool is_yield = current_task->yieldSwitch;
    current_task->yieldSwitch = false;
    current_task->deferredTaskSwitch = false;

    // Race check: for blocking waits, verify target hasn't already exited
    static constexpr uint64_t WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
    bool skip_wait_queue = false;
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
                current_task->waitingForPid = 0;
                // Mark that parent has retrieved exit status (zombie can now be reaped)
                target->waitedOn = true;
            }
        }
    }

    // Under lock: update EEVDF state and pick next task
    task::Task* next_task = runQueues->thisCpuLocked([current_task, is_yield, skip_wait_queue](RunQueue* rq) -> task::Task* {
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
                removeFromSums(rq, current_task);
                rq->runnableHeap.remove(current_task);
            }
            current_task->schedQueue = task::Task::SchedQueue::WAITING;
            rq->waitList.push(current_task);
        }

        // Advance minVruntime
        int64_t avg = computeAvgVruntime(rq);
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
        auto* rq = runQueues->thisCpu();
        rq->currentTask = rq->idleTask;
        debug_task_ptrs[cpu::currentCpu()] = rq->idleTask;
        rq->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));

        uint64_t idle_stack =
            (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : (uint64_t)mm::phys::pageAlloc(4096) + 4096;

        restoreKernelGsForIdle();

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
    uint32_t apic_id = apic::getApicId();
    uint64_t real_cpu_id = smt::getCpuIndexFromApicId(apic_id);
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

    // Under lock: remove from heap, push to wait list, pick next
    task::Task* next_task = runQueues->thisCpuLocked([current_task](RunQueue* rq) -> task::Task* {
        // Remove from heap
        if (rq->runnableHeap.contains(current_task)) {
            removeFromSums(rq, current_task);
            rq->runnableHeap.remove(current_task);
        }
        current_task->schedQueue = task::Task::SchedQueue::WAITING;
        rq->waitList.push(current_task);

        // Pick next
        if (rq->runnableHeap.size == 0) {
            rq->currentTask = nullptr;
            return nullptr;
        }
        int64_t avg = computeAvgVruntime(rq);
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
            runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
            restoreKernelGsForIdle();

            frame.rip = (uint64_t)_wOS_kernel_idle_loop;
            frame.cs = 0x08;
            frame.ss = 0x10;
            auto* rq = runQueues->thisCpu();
            frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : (uint64_t)mm::phys::pageAlloc(4096) + 4096;
            frame.flags = 0x202;
            gpr = cpu::GPRegs();
        }
    } else {
        // Only idle or no tasks — enter idle via iretq
        auto* rq = runQueues->thisCpu();
        rq->currentTask = rq->idleTask;
        rq->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));
        restoreKernelGsForIdle();

        frame.rip = (uint64_t)_wOS_kernel_idle_loop;
        frame.cs = 0x08;
        frame.ss = 0x10;
        frame.rsp = (rq->idleTask != nullptr) ? rq->idleTask->context.syscallKernelStack : (uint64_t)mm::phys::pageAlloc(4096) + 4096;
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

    // Remove from whatever queue the task is in on ALL CPUs.
    // The task could be on any CPU's waitList (or even in a heap if there's a race).
    // CRITICAL: If the task is currentTask on some CPU, that CPU is actively running
    // it (possibly in a syscall). Don't move it — let the timer preempt it naturally.
    bool is_current_on_some_cpu = false;
    for (uint64_t search_cpu = 0; search_cpu < smt::getCoreCount(); ++search_cpu) {
        runQueues->withLockVoid(search_cpu, [task, &is_current_on_some_cpu, search_cpu](RunQueue* rq) {
            if (rq->currentTask == task) {
                is_current_on_some_cpu = true;
#ifdef SCHED_DEBUG
                dbg::log("RESCHED: PID %x is currentTask on CPU %d", task->pid, (int)search_cpu);
#endif
                return;
            }
            // Remove from wait list
            if (task->schedQueue == task::Task::SchedQueue::WAITING) {
                if (rq->waitList.remove(task)) {
#ifdef SCHED_DEBUG
                    dbg::log("RESCHED: PID %x removed from CPU %d waitList", task->pid, (int)searchCpu);
#endif
                }
            }
            // Also try heap removal in case of races
            if (rq->runnableHeap.contains(task)) {
#ifdef SCHED_DEBUG
                dbg::log("RESCHED: PID %x found in CPU %d heap (idx=%d), removing", task->pid, (int)searchCpu, task->heapIndex);
#endif
                removeFromSums(rq, task);
                rq->runnableHeap.remove(task);
            }
        });
    }

    // If the task is currently executing on a CPU, don't re-insert it.
    // It will be picked up by the scheduler on its current CPU when it
    // yields or its syscall returns.
    if (is_current_on_some_cpu) {
#ifdef SCHED_DEBUG
        dbg::log("RESCHED: PID %x ABORT - is currentTask somewhere", task->pid);
#endif
        return;
    }

// Insert into target CPU's heap with updated vruntime
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x INSERT -> CPU %d (heapIdx=%d before insert)", task->pid, (int)cpuNo, task->heapIndex);
#endif
    task->cpu = cpu_no;
    runQueues->withLockVoid(cpu_no, [task](RunQueue* rq) {
        // Boost vruntime to at least minVruntime so the task doesn't
        // monopolize the CPU after waking from a long sleep
        task->vruntime = std::max(task->vruntime, rq->minVruntime);
        task->vdeadline = task->vruntime + ((((int64_t)task->sliceNs * 1024) / (int64_t)task->schedWeight));
        task->sliceUsedNs = 0;
        task->schedQueue = task::Task::SchedQueue::RUNNABLE;

        rq->runnableHeap.insert(task);
        addToSums(rq, task);
    });

    // Wake the target CPU if it's idle
    wakeIdleCpu(cpu_no);
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x DONE -> CPU %d (heapIdx=%d)", task->pid, (int)cpuNo, task->heapIndex);
#endif
}

// ============================================================================
// PID lookup (O(1) via registry)
// ============================================================================

auto find_task_by_pid(uint64_t pid) -> task::Task* { return pidTableFind(pid); }

auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {
    auto* task = pidTableFind(pid);
    if (task != nullptr && task->tryAcquire()) {
        return task;
    }
    return nullptr;
}

auto get_active_task_count() -> uint32_t { return activeTaskCount; }

auto get_active_task_at(uint32_t index) -> task::Task* {
    if (index >= activeTaskCount) return nullptr;
    return activeTaskList[index];
}

void signal_process_group(uint64_t pgid, int sig) {
    if (pgid == 0 || sig <= 0 || sig > static_cast<int>(task::Task::MAX_SIGNALS)) return;
    uint64_t mask = 1ULL << (sig - 1);
    for (uint32_t i = 0; i < activeTaskCount; i++) {
        auto* t = activeTaskList[i];
        if (t != nullptr && t->pgid == pgid && !t->hasExited) {
            t->sigPending |= mask;
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
    runQueues->withLockVoid(0, [task](RunQueue* rq) { rq->deadList.push(task); });
}

void gc_expired_tasks() {
    for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
        runQueues->withLockVoid(cpuNo, [cpuNo](RunQueue* rq) {
            // Walk dead list, reclaiming tasks whose epoch grace period has elapsed.
            // Because IntrusiveTaskList::remove() only removes one node (not all
            // occurrences), we can safely walk with a restart-on-remove pattern.
            bool madeProgress = true;
            while (madeProgress) {
                madeProgress = false;
                task::Task* cur = rq->deadList.head;
                while (cur != nullptr) {
                    task::Task* next = cur->schedNext;

                    if (cur->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
                        cur = next;
                        continue;
                    }

                    uint64_t deathEpoch = cur->deathEpoch.load(std::memory_order_acquire);
                    if (!EpochManager::isSafeToReclaim(deathEpoch)) {
#ifdef SCHED_DEBUG
                        static uint64_t epochSkipCount = 0;
                        if (++epochSkipCount % 1000 == 1) {
                            dbg::log("GC: PID %x deathEpoch=%d not safe yet", cur->pid, deathEpoch);
                        }
#endif
                        cur = next;
                        continue;
                    }

                    // Check if any CPU still has this task as currentTask
                    bool stillInUse = false;
                    for (uint64_t checkCpu = 0; checkCpu < smt::getCoreCount(); ++checkCpu) {
                        if (runQueues->thatCpu(checkCpu)->currentTask == cur) {
                            stillInUse = true;
#ifdef SCHED_DEBUG
                            dbg::log("GC: PID %x still currentTask on CPU %d", cur->pid, checkCpu);
#endif
                            break;
                        }
                    }
                    if (stillInUse) {
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
                    if (cur->hasExited && !cur->waitedOn) {
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
                    bool taskLooksValid = true;

                    if (cur->thread != nullptr) {
                        auto threadAddr = reinterpret_cast<uintptr_t>(cur->thread);
                        if (threadAddr < 0xffff800000000000ULL) {
                            dbg::log("GC: Task %p (PID %x) has invalid thread ptr %p, skipping", cur, cur->pid, cur->thread);
                            taskLooksValid = false;
                        }
                    }
                    if (cur->pagemap != nullptr) {
                        auto pmAddr = reinterpret_cast<uintptr_t>(cur->pagemap);
                        if (pmAddr >= 0xffffffff80000000ULL || pmAddr < 0xffff800000000000ULL) {
                            dbg::log("GC: Task %p (PID %x) has invalid pagemap ptr %p, skipping", cur, cur->pid, cur->pagemap);
                            taskLooksValid = false;
                        }
                    }

                    if (!taskLooksValid) {
                        rq->deadList.remove(cur);
                        dbg::log("GC: Leaking corrupted task %p to avoid crash", cur);
                        madeProgress = true;
                        break;
                    }

                    // Remove from dead list
                    rq->deadList.remove(cur);

                    // Clear PID hash table entry
                    if (cur->pid > 0) {
                        pidTableRemove(cur->pid);
                        activeListRemove(cur->pid);
                    }

                    // Free pagemap (DAEMON tasks use the kernel pagemap — must NOT free it)
                    if (cur->pagemap != nullptr && cur->type != task::TaskType::DAEMON) {
                        mm::virt::destroyUserSpace(cur->pagemap);
                        mm::phys::pageFree(cur->pagemap);
                    }

                    // Free thread
                    if (cur->thread != nullptr) {
                        auto* th = cur->thread;
                        uintptr_t thAddr = reinterpret_cast<uintptr_t>(th);

                        bool thInHHDM = (thAddr >= 0xffff800000000000ULL && thAddr < 0xffff900000000000ULL);
                        bool thInKernelStatic = (thAddr >= 0xffffffff80000000ULL && thAddr < 0xffffffffc0000000ULL);
                        if (!thInHHDM && !thInKernelStatic) {
                            dbg::log("GC: Task %p (PID %x) thread ptr %p out of range; skipping", cur, cur->pid, th);
                            cur->thread = nullptr;
                        } else {
                            if (th->magic != 0xDEADBEEF) {
                                dbg::log("GC: Task %p (PID %x) thread bad magic 0x%x", cur, cur->pid, th->magic);
                            } else {
                                th->tlsPhysPtr = 0;
                                th->stackPhysPtr = 0;
                                threading::destroyThread(th);
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
                        uintptr_t sa_addr = reinterpret_cast<uintptr_t>(sa);
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

                    madeProgress = true;
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
    if (runQueues == nullptr) {
        return stats;
    }
    return runQueues->withLock(cpu_no, [](RunQueue* rq) -> RunQueueStats {
        if (rq == nullptr) {
            return {.active_task_count = 0, .expired_task_count = 0, .wait_queue_count = 0};
        }
        return {
            .active_task_count = rq->runnableHeap.size, .expired_task_count = rq->deadList.count, .wait_queue_count = rq->waitList.count};
    });
}

auto get_least_loaded_cpu() -> uint64_t {
    if (runQueues == nullptr) {
        return 0;
    }

    uint64_t cpu_count = smt::getCoreCount();
    if (cpu_count <= 1) {
        return 0;
    }

    // First pass: check for idle CPUs without taking locks (racy but cheap).
    // Prefer spreading across idle cores before piling onto busy ones.
    // Use a simple round-robin seed to avoid always picking the same idle CPU.
    static uint64_t rr_seed = 0;
    uint64_t start = rr_seed++ % cpu_count;
    for (uint64_t off = 0; off < cpu_count; off++) {
        uint64_t i = (start + off) % cpu_count;
        auto* rq = runQueues->thatCpu(i);
        if (rq->isIdle.load(std::memory_order_acquire) && rq->runnableHeap.size == 0) {
            return i;
        }
    }

    // No idle CPU — fall back to least total load (runnable + waiting)
    uint64_t least_loaded_cpu = 0;
    uint64_t min_load = UINT64_MAX;

    for (uint64_t i = 0; i < cpu_count; i++) {
        auto* rq = runQueues->thatCpu(i);
        uint64_t load = rq->runnableHeap.size + rq->waitList.count;
        if (load < min_load) {
            min_load = load;
            least_loaded_cpu = i;
        }
    }
    return least_loaded_cpu;
}

size_t get_expired_task_refcounts(uint64_t cpu_no, uint64_t* pids, uint32_t* refcounts, size_t max_entries, size_t start_index) {
    if (runQueues == nullptr || pids == nullptr || refcounts == nullptr || max_entries == 0) {
        return 0;
    }
    return runQueues->withLock(cpu_no, [pids, refcounts, max_entries, start_index](RunQueue* rq) -> size_t {
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
