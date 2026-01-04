#include "scheduler.hpp"

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
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/sys/context_switch.hpp"

// Kernel idle loop - defined in context_switch.asm
extern "C" void _wOS_kernel_idle_loop();

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue>* runQueues;

// Debug: Per-CPU current task pointers at fixed address for panic inspection
// Per-CPU debug task pointers for panic handler inspection
// CRITICAL: Do NOT use hardcoded physical addresses - they conflict with page table allocations!
// Instead, use a statically allocated array in kernel BSS.
static task::Task* debug_task_ptrs[256] = {nullptr};  // Support up to 256 CPUs

// TODO: may be unique_ptr
auto postTask(task::Task* task) -> bool {
    // Must use locked access because timer interrupts can modify the queue
    // and std::list is not thread-safe for concurrent modifications
    runQueues->thisCpuLockedVoid([task](RunQueue* rq) { rq->activeTasks.push_back(task); });
    return true;
}

bool postTaskForCpu(uint64_t cpuNo, task::Task* task) {
#ifdef SCHED_DEBUG
    dbg::log("postTaskForCpu: posting task '%s' (PID %x) to CPU %d (current CPU: %d)", task->name, task->pid, cpuNo, cpu::currentCpu());
#endif

    task->cpu = cpuNo;

    // Memory barrier to ensure all task fields are visible to other CPUs before posting
    // This is critical for cross-CPU task posting to prevent other CPUs from seeing
    // partially-initialized task objects
    __atomic_thread_fence(__ATOMIC_RELEASE);

    // Use PerCpuCrossAccess locking for cross-CPU access to add task
    runQueues->withLockVoid(cpuNo, [task](RunQueue* rq) { rq->activeTasks.push_back(task); });
#ifdef SCHED_DEBUG
    // Log whether the target CPU was idle and the resulting activeTasks size
    bool wasIdle = runQueues->thatCpu(cpuNo)->isIdle.load(std::memory_order_acquire);
    size_t activeCount = runQueues->withLock(cpuNo, [](RunQueue* rq) -> size_t { return rq->activeTasks.size(); });
    dbg::log("postTaskForCpu: posted PID %x to CPU %d: wasIdle=%d activeCount=%d", task->pid, cpuNo, wasIdle, (int)activeCount);
#endif

    // TODO: IPIs disabled temporarily - they cause race conditions with idle loop
    // For now, idle CPUs will wake on timer and check for work
    // Send IPI to wake up the target CPU ONLY if it's halted (waiting for work)
    /*
    if (cpuNo != cpu::currentCpu()) {
        std::atomic<bool>& isIdleAtomic = runQueues->thatCpu(cpuNo)->isIdle;
        bool expected = true;
        if (isIdleAtomic.compare_exchange_strong(expected, false, std::memory_order_acq_rel, std::memory_order_acquire)) {
            uint32_t targetLapicId = smt::getCpu(cpuNo).lapic_id;
            apic::IPIConfig ipi = {};
            ipi.vector = 0x30;
            ipi.deliveryMode = apic::IPIDeliveryMode::FIXED;
            ipi.destinationMode = apic::IPIDestinationMode::PHYSICAL;
            ipi.level = apic::IPILevel::ASSERT;
            ipi.triggerMode = apic::IPITriggerMode::EDGE;
            ipi.destinationShorthand = apic::IPIDestinationShorthand::NONE;
            apic::sendIpi(ipi, targetLapicId);
        }
    }
    */

    return true;
}
task::Task* getCurrentTask() {
    // Return the currentTask pointer which tracks which task is actually running.
    // This is separate from activeTasks.front() because timer interrupts can
    // rotate the queue while a syscall is in progress. The currentTask field
    // is only updated when we actually switch to a new task.
    // No lock needed - currentTask is only modified by the timer handler on this CPU
    // and we're either in timer context or in syscall context (interrupts can't
    // preempt us while reading a single pointer).
    return runQueues->thisCpu()->currentTask;
}

void removeCurrentTask() {
    // Must use locked access because other CPUs can post tasks to our queue
    bool isEmpty = runQueues->thisCpuLocked([](RunQueue* rq) -> bool {
        rq->activeTasks.pop_front();
        if (rq->activeTasks.size() == 0) {
            rq->currentTask = nullptr;
            return true;
        }
        // Don't set currentTask here - jumpToNextTask will do it
        // after selecting the appropriate next task
        return false;
    });

    if (isEmpty) {
#ifdef SCHED_DEBUG
        dbg::log("No more tasks in runqueue, halting CPU");
#endif
        for (;;) {
            asm volatile("hlt");
        }
    }
}

void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    // Enter epoch critical section - protects task pointers from being freed
    // while we're using them outside of locks
    EpochGuard epochGuard;

    // Note: EOI is called by _wOS_schedTimer before calling this function
    auto* currentTask = getCurrentTask();

    // If currentTask is null, we are in the idle loop.
    // We should check if there are any tasks to run.
    if (currentTask == nullptr) {
        // Check if we have any active tasks
        bool hasWork = runQueues->thisCpuLocked([](RunQueue* rq) -> bool { return rq->activeTasks.size() > 0; });

        if (hasWork) {
            // We have work! Switch to the next task.
            // jumpToNextTask will handle picking the task and setting up the frame.
            jumpToNextTask(gpr, frame);
        }
        return;
    }

    // Only save context for non-idle tasks that have run
    if (currentTask->hasRun && currentTask->type != task::TaskType::IDLE) {
        currentTask->context.regs = gpr;
        currentTask->context.frame = frame;
    }

    // Use locked access since other CPUs can post tasks to our queue
    // IMPORTANT: Don't set currentTask inside the lambda! We must only update it
    // after we're certain we're going to switch. Otherwise, if we bail out early,
    // currentTask would be wrong and subsequent timer interrupts would corrupt context.
    task::Task* nextTask = runQueues->thisCpuLocked([currentTask](RunQueue* rq) -> task::Task* {
        // CRITICAL FIX: Remove currentTask from wherever it is in the queue (not just front!)
        // This is necessary because new tasks can be posted to the queue while currentTask
        // is running a syscall, causing currentTask to no longer be at the front.
        rq->activeTasks.remove(currentTask);
        rq->activeTasks.push_back(currentTask);
        task::Task* next = rq->activeTasks.front();

        // Skip idle tasks, non-ACTIVE tasks (EXITING/DEAD), and tasks not yet initialized
        size_t taskCount = rq->activeTasks.size();
        size_t checked = 0;
        while (checked < taskCount) {
            // Skip idle tasks
            bool isIdle = (next->type == task::TaskType::IDLE);
            // Skip tasks that are exiting or dead
            bool isNotActive = (next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE);
            // Skip tasks that are not fully initialized (missing thread or pagemap for user tasks)
            bool notReady = false;
            if (!isIdle) {
                if (next->thread == nullptr || next->pagemap == nullptr) {
#ifdef SCHED_DEBUG
                    dbg::log("processTasks: Skipping PID %x not ready (thread=%p pagemap=%p)", next->pid, next->thread, next->pagemap);
#endif
                    notReady = true;
                }
            }

            if ((isIdle || isNotActive || notReady) && taskCount > 1) {
                rq->activeTasks.pop_front();
                rq->activeTasks.push_back(next);
                next = rq->activeTasks.front();
                checked++;
            } else {
                break;
            }
        }
        // DON'T update currentTask here - do it after we're sure we're switching
        return next;
    });

    // DEBUG: Periodically log init (PID 2) status - check ALL CPUs
    // Note: PID 1 is CPU 1's idle task, PID 2 is init
    static uint64_t debugCounter = 0;
    if (cpu::currentCpu() == 0 && (++debugCounter % 1000) == 0) {
        bool foundInit = false;
        for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
            uint64_t activeCount = 0;
            for (auto* node = runQueues->thatCpu(cpuNo)->activeTasks.getHead(); node != nullptr; node = node->next) {
                activeCount++;
                if (node->data && node->data->pid == 2) {
                    foundInit = true;
                    dbg::log("DEBUG: init (PID 2) in CPU %d activeTasks, state=%d, activeCount=%d", cpuNo,
                             (int)node->data->state.load(std::memory_order_acquire), activeCount);
                    break;
                }
            }
            if (foundInit) break;
            for (auto* node = runQueues->thatCpu(cpuNo)->waitQueue.getHead(); node != nullptr; node = node->next) {
                if (node->data && node->data->pid == 2) {
                    foundInit = true;
                    dbg::log("DEBUG: init (PID 2) in CPU %d waitQueue!", cpuNo);
                    break;
                }
            }
            if (foundInit) break;
            for (auto* node = runQueues->thatCpu(cpuNo)->expiredTasks.getHead(); node != nullptr; node = node->next) {
                if (node->data && node->data->pid == 2) {
                    foundInit = true;
                    dbg::log("DEBUG: init (PID 2) in CPU %d expiredTasks!", cpuNo);
                    break;
                }
            }
            if (foundInit) break;
        }
        if (!foundInit) {
            dbg::log("DEBUG: init (PID 2) NOT FOUND on ANY CPU!");
        }
    }

    // Validate task state before use - task might have started exiting after we got it
    if (nextTask->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        // Task is exiting/dead, just return to let timer fire again
        // currentTask stays pointing to the original task (which is still running)
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        return;
    }

    // If the task hasn't finished initialization (missing thread or pagemap), don't try to switch to it
    if (nextTask->type != task::TaskType::IDLE && (nextTask->thread == nullptr || nextTask->pagemap == nullptr)) {
        dbg::log("processTasks: nextTask PID %x missing resources (thread=%p pagemap=%p), marking CPU idle", nextTask->pid,
                 nextTask->thread, nextTask->pagemap);
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        return;
    }

    // If we only have idle task, just return to the interrupt handler.
    // The handler will do iretq which returns to wherever we were (likely hlt loop).
    // The timer is already armed by _wOS_schedTimer, so we'll get another interrupt.
    // This avoids nested interrupts from halting inside this function.
    if (nextTask->type == task::TaskType::IDLE) {
        // Mark CPU as idle so other CPUs know to send IPI when posting tasks
        // currentTask stays pointing to the original task (which is still running)
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        return;
    }

    // Save the original task so we can restore currentTask if switch fails
    task::Task* originalTask = currentTask;

    // NOW we're certain we're going to switch - update currentTask
    runQueues->thisCpu()->currentTask = nextTask;

    // Update debug task pointer for panic inspection
    debug_task_ptrs[cpu::currentCpu()] = nextTask;

    // Mark CPU as not idle since we have a real task to run
    runQueues->thisCpu()->isIdle.store(false, std::memory_order_release);

    // Mark the next task as having run
    nextTask->hasRun = true;

    // Try to switch - if it fails, restore currentTask to the original task
    // so the next timer interrupt saves context to the correct task
    if (!sys::context_switch::switchTo(gpr, frame, nextTask)) {
        // switchTo failed - reset currentTask to the original task that's still running
        runQueues->thisCpu()->currentTask = originalTask;
        debug_task_ptrs[cpu::currentCpu()] = originalTask;
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
    }
}

// Jump to the next task without saving the current task's state
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    apic::eoi();

    // Manually enter epoch critical section to protect task pointers.
    // We can't use EpochGuard RAII because this function doesn't return normally
    // (it goes through switchTo -> iretq). The critical section will be "leaked"
    // but that's OK: when the new task runs and processTasks is eventually called,
    // a new EpochGuard will be created which updates the epoch state. When that
    // processTasks returns (or the next timer tick), exitCritical will be called.
    // The important thing is that GC won't free tasks while we're accessing them.
    EpochManager::enterCritical();

    // Get the exiting task - this is the task that called exit(), not necessarily front of queue
    task::Task* exitingTask = getCurrentTask();
#ifdef SCHED_DEBUG
    dbg::log("jumpToNextTask: CPU %d called - exitingTask=%p pid=%x", cpu::currentCpu(), exitingTask, exitingTask ? exitingTask->pid : 0);
#endif

    // Use locked access since other CPUs can post tasks to our queue
    task::Task* nextTask = runQueues->thisCpuLocked([exitingTask](RunQueue* rq) -> task::Task* {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask(lambda): CPU %d active=%d expired=%d (before remove), exitingPID=%x", cpu::currentCpu(),
                 (unsigned)rq->activeTasks.size(), (unsigned)rq->expiredTasks.size(), exitingTask ? exitingTask->pid : 0);
#endif
        // CRITICAL FIX: Remove the exiting task from wherever it is in the queue (not just front!)
        // This is necessary because new tasks can be posted to the queue while the task
        // is in the exit syscall, causing it to no longer be at the front.
        rq->activeTasks.remove(exitingTask);
        rq->expiredTasks.push_back(exitingTask);

        // Check if we have any tasks left
        if (rq->activeTasks.size() == 0) {
            rq->currentTask = nullptr;
            return nullptr;
        }

        // Skip EXITING/DEAD tasks and tasks that are not fully initialized - find an ACTIVE or IDLE task to run
        // This prevents us from returning a task that switchTo will reject
        task::Task* next = rq->activeTasks.front();
        size_t taskCount = rq->activeTasks.size();
        size_t checked = 0;
        while (checked < taskCount) {
            // Idle tasks are always valid to switch to (they run in kernel mode)
            if (next->type == task::TaskType::IDLE) {
                break;
            }

            // Check if task is ACTIVE
            if (next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                // Task is EXITING/DEAD - rotate to next task and move to expired
                rq->activeTasks.pop_front();
                rq->expiredTasks.push_back(next);  // Move to expired for GC
                next = rq->activeTasks.front();
                checked++;
                continue;
            }

            // Task could be ACTIVE but not yet fully initialized (thread/pagemap may be null)
            if (next->thread == nullptr || (next->type != task::TaskType::IDLE && next->pagemap == nullptr)) {
#ifdef SCHED_DEBUG
                dbg::log("jumpToNextTask(lambda): skipping PID %x - not initialized (thread=%p pagemap=%p)", next->pid, next->thread,
                         next->pagemap);
#endif
                rq->activeTasks.pop_front();
                rq->activeTasks.push_back(next);
                next = rq->activeTasks.front();
                checked++;
                continue;
            }

            // Task is good to run
            break;
        }
        // DON'T update currentTask here - do it after we're sure we're switching
        return next;
    });

#ifdef SCHED_DEBUG
    dbg::log("jumpToNextTask: CPU %d selected nextTask=%p pid=%x", cpu::currentCpu(), nextTask, nextTask ? nextTask->pid : 0);
    uint64_t aCount = runQueues->thisCpuLocked([](RunQueue* rq) -> uint64_t { return rq->activeTasks.size(); });
    uint64_t eCount = runQueues->thisCpuLocked([](RunQueue* rq) -> uint64_t { return rq->expiredTasks.size(); });
    dbg::log("jumpToNextTask: CPU %d queue sizes: active=%d expired=%d", cpu::currentCpu(), aCount, eCount);
    // Print first few active PIDs for diagnostics
    runQueues->thisCpuLockedVoid([](RunQueue* rq) {
        auto* node = rq->activeTasks.getHead();
        int i = 0;
        while (node != nullptr && i < 8) {
            dbg::log("jumpToNextTask: active[%d]=PID %x", i, node->data ? node->data->pid : 0);
            node = node->next;
            ++i;
        }
    });
#endif

    if (nextTask == nullptr) {
        // No more tasks or only non-ready task - fall back to idle
        dbg::log("jumpToNextTask: CPU %d: No ready tasks found after exit, entering idle", cpu::currentCpu());
        // No more tasks - should not happen if idle task exists
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));

        // CRITICAL FIX: We cannot return to the idle loop using iretq because
        // iretq to Ring 0 does not switch the stack. We would continue running
        // on the dead task's stack, which is unsafe.
        // Instead, we manually switch to a fresh stack and jump to the idle loop.

        uint64_t idleStack = (uint64_t)mm::phys::pageAlloc(4096) + 4096;

        // Exit critical section before idling to allow GC to proceed
        EpochManager::exitCritical();

        asm volatile(
            "mov %0, %%rsp\n"
            "sti\n"
            "jmp _wOS_kernel_idle_loop"
            :
            : "r"(idleStack)
            : "memory");
        __builtin_unreachable();
    }

    // If the selected task is missing required resources, don't attempt to switch
    if (nextTask->type != task::TaskType::IDLE && (nextTask->thread == nullptr || nextTask->pagemap == nullptr)) {
        dbg::log("jumpToNextTask: selected PID %x missing resources (thread=%p pagemap=%p), entering idle", nextTask->pid, nextTask->thread,
                 nextTask->pagemap);
        runQueues->thisCpu()->currentTask = nullptr;
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));

        uint64_t idleStack = (uint64_t)mm::phys::pageAlloc(4096) + 4096;
        EpochManager::exitCritical();
        asm volatile(
            "mov %0, %%rsp\n"
            "sti\n"
            "jmp _wOS_kernel_idle_loop"
            :
            : "r"(idleStack)
            : "memory");
        __builtin_unreachable();
    }
    // If only idle task remains, we CANNOT switch to it because idle tasks
    // don't have valid usermode context. We need to enter a kernel idle loop
    // that properly cleans up the stack.
    //
    // The trick: set up a frame that returns to a kernel-mode halt routine.
    // We'll create a simple idle loop in kernel space.
    if (nextTask->type == task::TaskType::IDLE) {
#ifdef SCHED_DEBUG
        uint64_t queueSize = runQueues->thisCpuLocked([](RunQueue* rq) -> uint64_t { return rq->activeTasks.size(); });
        dbg::log("jumpToNextTask: CPU %d entering idle, queue size = %d", cpu::currentCpu(), queueSize);
#endif
        // Set currentTask to the idle task
        runQueues->thisCpu()->currentTask = nextTask;
        debug_task_ptrs[cpu::currentCpu()] = nextTask;
        // Mark CPU as idle so other CPUs know to send IPI when posting tasks
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);

        // Arm the timer so we wake up periodically to check for new tasks
        apic::oneShotTimer(apic::calibrateTimer(10000));  // 10ms timer

        // CRITICAL FIX: Manually switch stack and jump to idle loop
        uint64_t idleStack = (uint64_t)mm::phys::pageAlloc(4096) + 4096;

        // Exit critical section before idling
        EpochManager::exitCritical();

        asm volatile(
            "mov %0, %%rsp\n"
            "sti\n"
            "jmp _wOS_kernel_idle_loop"
            :
            : "r"(idleStack)
            : "memory");
        __builtin_unreachable();
    }

    // CRITICAL: Final state check before calling switchTo
    // The task could have transitioned to EXITING/DEAD after we released the lock.
    // If we call switchTo with a dead task, it returns without modifying frame,
    // and then iretq would use the dummy zeros we pushed, causing a GPF.
    if (nextTask->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        // Task is no longer valid - fall back to idle loop
        // Set currentTask to nullptr since we're not actually running any task
        runQueues->thisCpu()->currentTask = nullptr;
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));  // 10ms timer

        uint64_t idleStack = (uint64_t)mm::phys::pageAlloc(4096) + 4096;
        EpochManager::exitCritical();
        asm volatile(
            "mov %0, %%rsp\n"
            "sti\n"
            "jmp _wOS_kernel_idle_loop"
            :
            : "r"(idleStack)
            : "memory");
        __builtin_unreachable();
    }

    // Mark CPU as not idle since we have a real task to run
    runQueues->thisCpu()->isIdle = false;

    // Mark the next task as having run
    nextTask->hasRun = true;

    // NOW we're certain we're going to switch - update currentTask
    runQueues->thisCpu()->currentTask = nextTask;

    // Update debug task pointer for panic inspection
    debug_task_ptrs[cpu::currentCpu()] = nextTask;

#ifdef SCHED_DEBUG
    dbg::log("jumpToNextTask: attempting switchTo PID %x (state=%d)", nextTask->pid, (int)nextTask->state.load(std::memory_order_acquire));
#endif

    // Try to switch to the next task - if it fails (pagemap/thread validation failed),
    // fall back to kernel idle loop to avoid iretq with uninitialized frame
    if (!sys::context_switch::switchTo(gpr, frame, nextTask)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: switchTo FAILED for PID %x, entering idle", nextTask->pid);
#endif
        // switchTo failed - reset currentTask since we're not actually running this task
        runQueues->thisCpu()->currentTask = nullptr;
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        apic::oneShotTimer(apic::calibrateTimer(10000));
        frame.rip = (uint64_t)_wOS_kernel_idle_loop;
        frame.cs = 0x08;
        frame.ss = 0x10;
        frame.rsp = (uint64_t)mm::phys::pageAlloc(4096) + 4096;
        frame.flags = 0x202;
        gpr = cpu::GPRegs();
    }
}

void percpuInit() {
    auto cpu = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu);
    runQueues->thisCpu()->activeTasks = std::list<task::Task*>();
    runQueues->thisCpu()->expiredTasks = std::list<task::Task*>();
}

void startScheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());

    // Check if we only have idle task(s) - must use locked access
    auto* firstTask = runQueues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
        task::Task* first = rq->activeTasks.front();
        // Set currentTask immediately so timer interrupts don't crash
        // This will be the idle task initially, updated when we find a real task
        rq->currentTask = first;
        return first;
    });

    // If the first task is an idle task (no entry point), wait for real tasks
    if (firstTask->type == task::TaskType::IDLE || firstTask->entry == 0) {
        dbg::log("CPU %d: Only idle task, waiting for work...", cpu::currentCpu());
        // Enable interrupts and wait - arm a timer so we wake periodically to check for tasks
        for (;;) {
            // Mark CPU as idle before halting so other CPUs know to send IPI
            runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);

            // Arm a short timer to wake us up periodically to check for new tasks
            // This replaces the need for IPIs while we debug the race conditions
            apic::oneShotTimer(apic::calibrateTimer(1000));  // Wake every 1ms

            asm volatile("sti");
            asm volatile("hlt");  // Wait for interrupt (timer will wake us)
            asm volatile("cli");  // Disable interrupts before checking
            // Mark CPU as no longer idle after waking
            runQueues->thisCpu()->isIdle.store(false, std::memory_order_release);

            // Check if we have any non-idle tasks now - use locked access since
            // other CPUs can post tasks to our queue via postTaskForCpu
            bool foundTask = runQueues->thisCpuLocked([&firstTask](RunQueue* rq) -> bool {
                uint64_t taskCount = rq->activeTasks.size();
                if (taskCount > 1) {
                    // Save the idle task before we move it
                    task::Task* idleTask = firstTask;

                    // Move idle to back, get the real task from front
                    rq->activeTasks.pop_front();          // Remove idle from front
                    rq->activeTasks.push_back(idleTask);  // Put idle at back
                    firstTask = rq->activeTasks.front();  // Get the real task

                    // Validate the task before accepting it - protect against race conditions
                    // where task is posted but not fully initialized
                    if (firstTask != nullptr && firstTask->thread != nullptr) {
                        // Update currentTask to the new real task
                        rq->currentTask = firstTask;
                        return true;
                    }

                    // Task not ready yet, restore idle to front and wait more
                    rq->activeTasks.pop_back();            // Remove idle from back
                    rq->activeTasks.push_front(idleTask);  // Put idle back at front
                    firstTask = idleTask;                  // Reset to idle
                    // currentTask stays as idle task
                    return false;
                }
                return false;
            });

            if (foundTask) {
                dbg::log("CPU %d: Found task, switching to real task", cpu::currentCpu());
                break;  // Exit with interrupts disabled
            }
        }
    }

    // Mark the first task as having run since we're entering it directly
    firstTask->hasRun = true;

    // Set currentTask so getCurrentTask() returns the correct task during syscalls
    runQueues->thisCpu()->currentTask = firstTask;

    // NOTE: IA32_KERNEL_GS_BASE was already set by cpuParamInit to this CPU's kernel stack.
    // We must NOT overwrite it with the task's stored value, because that value was set
    // when the task was created (possibly on a different CPU).
    // The kernel GS base is per-CPU, not per-task.

    // Validate task before entering
    if (firstTask == nullptr || firstTask->thread == nullptr) {
        dbg::log("CPU %d: ERROR - Invalid task or thread pointer!", cpu::currentCpu());
        for (;;) {
            asm volatile("hlt");
        }
    }

    // User GS_BASE = TLS/stack base (user-accessible), KERNEL_GS_BASE = scratch area location
    // After swapgs in syscall: GS swaps with KERNEL_GS, so kernel gets scratch area
    //
    // IMPORTANT: We set GS_BASE to scratch area NOW (so currentCpu() works in kernel code),
    // and KERNEL_GS_BASE to user TLS. When _wOS_asm_enterUsermode does swapgs, these will
    // be swapped so user gets GS_BASE=TLS and kernel (after next swapgs) gets GS_BASE=scratch.
    //
    // CRITICAL: Update cpuId in the scratch area to reflect THIS CPU!
    // The scratch area was created when the task was spawned (possibly on a different CPU).
    auto* scratchArea = reinterpret_cast<cpu::PerCpu*>(firstTask->context.syscallScratchArea);
    scratchArea->cpuId = cpu::currentCpu();

    dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", firstTask->thread->fsbase, firstTask->thread->gsbase,
             firstTask->context.syscallScratchArea);
    cpuSetMSR(IA32_GS_BASE, firstTask->context.syscallScratchArea);  // Scratch area - will be swapped to KERNEL_GS_BASE by swapgs
    cpuSetMSR(IA32_KERNEL_GS_BASE, firstTask->thread->gsbase);       // User's TLS - will be swapped to GS_BASE by swapgs
    cpuSetMSR(IA32_FS_BASE, firstTask->thread->fsbase);

    mm::virt::switchPagemap(firstTask);

    // Write TLS self-pointer after switching pagemaps so it goes to the correct user-mapped physical memory
    *((uint64_t*)firstTask->thread->fsbase) = firstTask->thread->fsbase;

    // Update debug task pointer for panic inspection
    debug_task_ptrs[cpu::currentCpu()] = firstTask;

    // Start the scheduler timer now that everything is set up
    sys::context_switch::startSchedTimer();

    for (;;) {
        _wOS_asm_enterUsermode(firstTask->entry, firstTask->context.frame.rsp);
    }
}

// Garbage collect dead tasks from expiredTasks lists across all CPUs.
// Called periodically (e.g., every N timer ticks on CPU 0) to reclaim memory
// from tasks that have exited and whose epoch grace period has elapsed.
void gcExpiredTasks() {
    for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
        runQueues->withLockVoid(cpuNo, [](RunQueue* rq) {
            // Use restart-from-head approach because list::remove() now removes ALL
            // occurrences, which could invalidate our saved nextNode pointer.
            bool madeProgress = true;
            while (madeProgress) {
                madeProgress = false;
                auto* node = rq->expiredTasks.getHead();
                while (node != nullptr) {
                    auto* taskToCheck = node->data;
                    auto* nextNode = node->next;

                    if (taskToCheck == nullptr) {
                        node = nextNode;
                        continue;
                    }

                    // Check if task is DEAD and epoch is safe to reclaim
                    if (taskToCheck->state.load(std::memory_order_acquire) == task::TaskState::DEAD) {
                        uint64_t deathEpoch = taskToCheck->deathEpoch.load(std::memory_order_acquire);

                        // Check if enough epochs have passed for safe reclamation
                        if (EpochManager::isSafeToReclaim(deathEpoch)) {
                            // CRITICAL: Check if ANY CPU still has this task as currentTask
                            // This prevents freeing a task that a CPU is still using (e.g., during panic)
                            bool stillInUse = false;
                            for (uint64_t checkCpu = 0; checkCpu < smt::getCoreCount(); ++checkCpu) {
                                if (runQueues->thatCpu(checkCpu)->currentTask == taskToCheck) {
                                    stillInUse = true;
#ifdef SCHED_DEBUG
                                    dbg::log("GC: Task %p (PID %x) still currentTask on CPU %d, skipping", taskToCheck, taskToCheck->pid,
                                             checkCpu);
#endif
                                    break;
                                }
                            }

                            if (stillInUse) {
                                node = nextNode;
                                continue;
                            }

                            // Check refcount is 1 (only scheduler owns it)
                            uint32_t rc = taskToCheck->refCount.load(std::memory_order_acquire);
                            if (rc == 1) {
                                // Sanity check: validate task struct looks valid before freeing
                                // A corrupted task would have garbage pointers that could crash destroyUserSpace
                                bool taskLooksValid = true;

                                // Check if thread pointer is in valid heap range (0xffff8000... not 0xffffffff80...)
                                // Idle tasks have null thread, that's OK
                                if (taskToCheck->thread != nullptr) {
                                    uintptr_t threadAddr = reinterpret_cast<uintptr_t>(taskToCheck->thread);
                                    // Valid heap addresses are in HHDM range 0xffff800000000000 - 0xffff8fffffffffff
                                    // Kernel static is 0xffffffff80000000 - 0xffffffffffffffff
                                    // Allow kernel static because mini_malloc uses static slabs for small allocations
                                    if (threadAddr < 0xffff800000000000ULL) {
                                        dbg::log("GC: Task %p (PID %x) has invalid thread ptr %p, skipping", taskToCheck, taskToCheck->pid,
                                                 taskToCheck->thread);
                                        taskLooksValid = false;
                                    }
                                }

                                // Check if pagemap pointer is in valid HHDM range
                                if (taskToCheck->pagemap != nullptr) {
                                    uintptr_t pmAddr = reinterpret_cast<uintptr_t>(taskToCheck->pagemap);
                                    if (pmAddr >= 0xffffffff80000000ULL || pmAddr < 0xffff800000000000ULL) {
                                        dbg::log("GC: Task %p (PID %x) has invalid pagemap ptr %p, skipping", taskToCheck, taskToCheck->pid,
                                                 taskToCheck->pagemap);
                                        taskLooksValid = false;
                                    }
                                }

                                if (!taskLooksValid) {
                                    // Task struct is corrupted - don't try to free its resources
                                    // Just remove from list and leak to avoid crashes
                                    rq->expiredTasks.remove(taskToCheck);
                                    dbg::log("GC: Leaking corrupted task %p to avoid crash", taskToCheck);
                                    madeProgress = true;  // Restart from head after removal
                                    break;
                                }

                                // Safe to free! Remove from list first (removes ALL occurrences)
                                rq->expiredTasks.remove(taskToCheck);

                                // Free pagemap if allocated
                                // This was deferred from exit.cpp because other CPUs might
                                // have been about to load it into CR3 in switchTo()
                                // Now that the epoch grace period has passed, no CPU is using it
                                if (taskToCheck->pagemap != nullptr) {
                                    // First destroy user space (frees user pages and intermediate tables)
                                    mm::virt::destroyUserSpace(taskToCheck->pagemap);
                                    // Then free the PML4 itself
                                    mm::phys::pageFree(taskToCheck->pagemap);
                                }

                                // Prevent double-free: destroyUserSpace frees per-task user pages
                                // (including TLS and user stack) by calling pageFree on mapped pages.
                                // Validate and destroy thread safely to avoid double-destroy or use-after-free.
                                if (taskToCheck->thread != nullptr) {
                                    auto* th = taskToCheck->thread;
                                    uintptr_t thAddr = reinterpret_cast<uintptr_t>(th);

                                    // Validate thread pointer is in valid kernel memory before dereferencing
                                    bool thInHHDM = (thAddr >= 0xffff800000000000ULL && thAddr < 0xffff900000000000ULL);
                                    bool thInKernelStatic = (thAddr >= 0xffffffff80000000ULL && thAddr < 0xffffffffc0000000ULL);
                                    if (!thInHHDM && !thInKernelStatic) {
                                        dbg::log("GC: Task %p (PID %x) thread ptr %p out of kernel range; skipping destroy", taskToCheck,
                                                 taskToCheck->pid, th);
                                        taskToCheck->thread = nullptr;
                                    } else {
                                        // Basic magic sanity check to detect previously-freed or corrupted Thread
                                        // If magic is invalid, skip destroying it and log for diagnostics
                                        if (th->magic != 0xDEADBEEF) {
                                            dbg::log("GC: Task %p (PID %x) thread ptr %p has invalid magic 0x%x, skipping destroy",
                                                     taskToCheck, taskToCheck->pid, th, th->magic);
                                        } else {
                                            // Clear phys pointers so destroyThread doesn't double-free pages
                                            th->tlsPhysPtr = 0;
                                            th->stackPhysPtr = 0;

                                            // Now safely destroy the Thread object
                                            threading::destroyThread(th);
                                        }
                                        // DON'T null the thread pointer! Other CPUs might still have references
                                        // to this task struct (e.g., in currentTask, debug_task_ptrs, or local
                                        // variables during exception handling). Nulling the field would corrupt
                                        // the task data while it's still being used. The entire task struct will
                                        // be freed shortly anyway, so there's no risk of double-destroy.
                                    }
                                }

                                // Free kernel stack if allocated
                                if (taskToCheck->context.syscallKernelStack != 0) {
                                    // syscallKernelStack stores the top-of-stack (kernelRsp). Compute the base
                                    // and free that pointer (pageFree expects the base returned by pageAlloc).
                                    uint64_t top = taskToCheck->context.syscallKernelStack;
                                    uint64_t base = 0;
                                    if (top > KERNEL_STACK_SIZE) {
                                        base = top - KERNEL_STACK_SIZE;
                                    }
                                    if (base != 0) {
#ifdef SCHED_DEBUG
                                        dbg::log("GC: Freeing kernel stack for PID %x (top=0x%x base=0x%x)", taskToCheck->pid, top, base);
#endif
                                        mm::phys::pageFree(reinterpret_cast<void*>(base));
                                    } else {
                                        dbg::log("GC: Kernel stack top 0x%x is too small, skipping free for PID %x", top, taskToCheck->pid);
                                    }
                                    taskToCheck->context.syscallKernelStack = 0;
                                }

                                // Free syscallScratchArea if allocated (it's allocated via new cpu::PerCpu() in task.cpp)
                                if (taskToCheck->context.syscallScratchArea != 0) {
                                    auto* sa = reinterpret_cast<cpu::PerCpu*>(taskToCheck->context.syscallScratchArea);
                                    uintptr_t saAddr = reinterpret_cast<uintptr_t>(sa);

                                    // Validate the pointer is in valid kernel range before deleting
                                    bool inHHDM = (saAddr >= 0xffff800000000000ULL && saAddr < 0xffff900000000000ULL);
                                    bool inKernelStatic = (saAddr >= 0xffffffff80000000ULL && saAddr < 0xffffffffc0000000ULL);
                                    if (inHHDM || inKernelStatic) {
                                        delete sa;
                                    }
                                    taskToCheck->context.syscallScratchArea = 0;
                                }

                                // Free name string if allocated. Be defensive: ensure pointer is in valid kernel
                                // memory and appears to be a valid C string before deleting to avoid double-free.
                                if (taskToCheck->name != nullptr) {
                                    auto* nm = taskToCheck->name;
                                    uintptr_t nmAddr = reinterpret_cast<uintptr_t>(nm);

                                    // Valid kernel memory: HHDM or kernel static region
                                    bool inHHDM = (nmAddr >= 0xffff800000000000ULL && nmAddr < 0xffff900000000000ULL);
                                    bool inKernelStatic = (nmAddr >= 0xffffffff80000000ULL && nmAddr < 0xffffffffc0000000ULL);
                                    if (inHHDM || inKernelStatic) {
                                        // Check for a NUL terminator within a reasonable bound to avoid reading bad memory
                                        const size_t maxNameLen = 1024;  // arbitrary safety cap
                                        bool foundNul = false;
                                        for (size_t i = 0; i < maxNameLen; ++i) {
                                            // Use volatile access to avoid compiler optimizations
                                            volatile char c = nm[i];
                                            if (c == '\0') {
                                                foundNul = true;
                                                break;
                                            }
                                        }

                                        if (foundNul) {
                                            delete[] nm;
                                        } else {
                                            dbg::log("GC: Task %p (PID %x) name %p has no NUL in %d bytes; skipping delete", taskToCheck,
                                                     taskToCheck->pid, nm, (unsigned)maxNameLen);
                                        }
                                    } else {
                                        dbg::log("GC: Task %p (PID %x) name %p out of kernel range; skipping delete", taskToCheck,
                                                 taskToCheck->pid, nm);
                                    }

                                    // Clear pointer to avoid future double-delete attempts
                                    taskToCheck->name = nullptr;
                                }

                                // Clean up debug info registries for this process
                                // These are per-process allocations that grow indefinitely if not cleaned
                                loader::debug::unregisterProcess(taskToCheck->pid);
                                loader::debug::removeGdbDebugInfo(taskToCheck->pid);

                                // Finally free the task structure itself
                                delete taskToCheck;

                                // Restart from head since remove() may have invalidated nextNode
                                madeProgress = true;
                                break;
                            }
                        }
                    }

                    node = nextNode;
                }
            }
        });
    }
}

// IPI handler for scheduler wake-up (vector 0x30)
// When a CPU is halted waiting for work, another CPU can send this IPI to wake it.
// We must immediately check for tasks and context switch if available, because:
// 1. The CPU might be in _wOS_kernel_idle_loop with no valid userspace context
// 2. We can't just return and wait for the timer - the kernel idle loop doesn't set up proper frames
// 3. If we have a task to run, we must switch to it NOW, not wait for the next timer tick
static void schedulerWakeHandler([[maybe_unused]] cpu::GPRegs gpr, [[maybe_unused]] gates::interruptFrame frame) {
    // Send EOI immediately so APIC can accept more interrupts
    apic::eoi();

#ifdef SCHED_DEBUG
    dbg::log("CPU %d: Received scheduler wake IPI, checking for tasks", cpu::currentCpu());
#endif

    // Just check if we have work - don't manipulate the queue!
    // The idle loop in startScheduler() will handle task switching properly
    bool hasWork = runQueues->thisCpuLocked([](RunQueue* rq) -> bool {
        // Check if we have more than just the idle task
        return rq->activeTasks.size() > 1;
    });

    if (hasWork) {
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: IPI detected work in queue, waking CPU", cpu::currentCpu());
#endif
        // Mark CPU as not idle so idle loop knows to check for real tasks
        runQueues->thisCpu()->isIdle.store(false, std::memory_order_release);
    }

    // Return to wherever we were - idle loop will check queue and switch if needed
}

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();

    // Initialize epoch-based reclamation system for lock-free task lifecycle management
    EpochManager::init();

    // Register the scheduler wake IPI handler
    gates::setInterruptHandler(0x30, schedulerWakeHandler);
    dbg::log("Registered scheduler wake IPI handler for vector 0x30");
}
auto findTaskByPid(uint64_t pid) -> task::Task* {
    // Search all CPUs for a task with the given PID
    for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
        task::Task* found = runQueues->withLock(cpuNo, [pid](RunQueue* runQueue) -> task::Task* {
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
            return nullptr;
        });
        if (found != nullptr) {
            return found;
        }
    }
    return nullptr;
}

// Safe version of findTaskByPid that acquires a reference before returning.
// This prevents use-after-free when the task is freed by another CPU.
// IMPORTANT: Caller MUST call task->release() when done with the task pointer!
auto findTaskByPidSafe(uint64_t pid) -> task::Task* {
    for (uint64_t cpuNo = 0; cpuNo < smt::getCoreCount(); ++cpuNo) {
        task::Task* found = runQueues->withLock(cpuNo, [pid](RunQueue* runQueue) -> task::Task* {
            // Search wait queue
            for (auto* node = runQueue->waitQueue.getHead(); node != nullptr; node = node->next) {
                if (node->data != nullptr && node->data->pid == pid) {
                    // Try to acquire a reference while holding the lock
                    if (node->data->tryAcquire()) {
                        return node->data;
                    }
                    // Task is exiting/dead, don't return it
                    return nullptr;
                }
            }

            // Search active tasks
            for (auto* node = runQueue->activeTasks.getHead(); node != nullptr; node = node->next) {
                if (node->data != nullptr && node->data->pid == pid) {
                    if (node->data->tryAcquire()) {
                        return node->data;
                    }
                    return nullptr;
                }
            }

            // Search expired tasks
            for (auto* node = runQueue->expiredTasks.getHead(); node != nullptr; node = node->next) {
                if (node->data != nullptr && node->data->pid == pid) {
                    if (node->data->tryAcquire()) {
                        return node->data;
                    }
                    return nullptr;
                }
            }
            return nullptr;
        });
        if (found != nullptr) {
            return found;
        }
    }
    return nullptr;
}

void rescheduleTaskForCpu(uint64_t cpuNo, task::Task* task) {
    dbg::log("rescheduleTaskForCpu: Rescheduling PID %x on CPU %d", task->pid, cpuNo);

    // Remove from wait queue (if present) and add to active tasks list for the specified CPU
    // The task might be on a different CPU's wait queue than the target CPU, so we need to
    // search all CPUs' wait queues to find and remove it.
    for (uint64_t searchCpu = 0; searchCpu < smt::getCoreCount(); ++searchCpu) {
        runQueues->withLockVoid(searchCpu, [task](RunQueue* rq) { rq->waitQueue.remove(task); });
    }

    // Now add to the target CPU's active tasks
    runQueues->withLockVoid(cpuNo, [task](RunQueue* rq) { rq->activeTasks.push_back(task); });
    dbg::log("rescheduleTaskForCpu: Done rescheduling PID %x", task->pid);
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

    // Remove from active tasks and add to wait queue (must be locked)
    task::Task* nextTask = runQueues->thisCpuLocked([currentTask](RunQueue* rq) -> task::Task* {
        rq->activeTasks.remove(currentTask);
        rq->waitQueue.push_back(currentTask);
        if (rq->activeTasks.size() > 0) {
            task::Task* next = rq->activeTasks.front();
            // Update currentTask to track which task is actually running
            rq->currentTask = next;
            return next;
        }
        rq->currentTask = nullptr;
        return nullptr;
    });

    dbg::log("placeTaskInWaitQueue: Moved PID %x to wait queue", currentTask->pid);

    // Jump to the next task in the active queue
    if (nextTask != nullptr) {
        nextTask->hasRun = true;
        if (!sys::context_switch::switchTo(gpr, frame, nextTask)) {
            // switchTo failed - fall back to idle loop
            dbg::log("placeTaskInWaitQueue: switchTo failed, entering idle");
            runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
            apic::oneShotTimer(apic::calibrateTimer(10000));
            frame.rip = (uint64_t)_wOS_kernel_idle_loop;
            frame.cs = 0x08;
            frame.ss = 0x10;
            frame.rsp = (uint64_t)mm::phys::pageAlloc(4096) + 4096;
            frame.flags = 0x202;
            gpr = cpu::GPRegs();
        }
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
    uint64_t returnRip = 0;
    uint64_t returnFlags = 0;
    uint64_t userRsp = 0;
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

    // Remove from active tasks and add to wait queue (must be locked)
    task::Task* nextTask = runQueues->thisCpuLocked([currentTask](RunQueue* rq) -> task::Task* {
        rq->activeTasks.remove(currentTask);
        rq->waitQueue.push_back(currentTask);
        if (rq->activeTasks.size() > 0) {
            task::Task* next = rq->activeTasks.front();
            // Update currentTask to track which task is actually running
            rq->currentTask = next;
            return next;
        }
        rq->currentTask = nullptr;
        return nullptr;
    });

#ifdef SCHED_DEBUG
    dbg::log("deferredTaskSwitch: Moved PID %x to wait queue", currentTask->pid);
#endif

    // Get the next task in the active queue
    if (nextTask != nullptr) {
        nextTask->hasRun = true;

        // Set up GS for the next task before switching pagemap
        // We're in kernel mode (after swapgs from syscall entry), so:
        // GS_BASE should point to next task's scratch area (for kernel use)
        // KERNEL_GS_BASE should point to next task's user TLS (will be swapped to GS on swapgs at return)
        //
        // CRITICAL: Update cpuId in the scratch area before switching to it!
        // The scratch area was created when the task was spawned (possibly on a different CPU),
        // so we must update cpuId to reflect which CPU is actually running this task now.
        auto* scratchArea = reinterpret_cast<cpu::PerCpu*>(nextTask->context.syscallScratchArea);
        scratchArea->cpuId = cpu::currentCpu();

        if (nextTask->thread) {
            cpu::wrgsbase(nextTask->context.syscallScratchArea);
            cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->thread->gsbase);
            cpu::wrfsbase(nextTask->thread->fsbase);
        } else {
            cpu::wrgsbase(nextTask->context.syscallScratchArea);
            cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->context.syscallScratchArea);
        }

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
    return runQueues->withLock(cpuNo, [](RunQueue* runQueue) -> RunQueueStats {
        RunQueueStats s = {0, 0, 0};
        if (runQueue == nullptr) {
            return s;
        }
        s.activeTaskCount = runQueue->activeTasks.size();
        s.expiredTaskCount = runQueue->expiredTasks.size();
        s.waitQueueCount = runQueue->waitQueue.size();
        return s;
    });
}

// Find the CPU with the least number of active tasks for load balancing
auto getLeastLoadedCpu() -> uint64_t {
    if (runQueues == nullptr) {
        return 0;
    }

    uint64_t cpuCount = smt::getCoreCount();
    if (cpuCount <= 1) {
        return 0;
    }

    uint64_t leastLoadedCpu = 0;
    uint64_t minTasks = UINT64_MAX;

    for (uint64_t i = 0; i < cpuCount; i++) {
        uint64_t taskCount = runQueues->withLock(i, [](RunQueue* runQueue) -> uint64_t {
            if (runQueue == nullptr) {
                return UINT64_MAX;
            }
            return runQueue->activeTasks.size();
        });
        if (taskCount < minTasks) {
            minTasks = taskCount;
            leastLoadedCpu = i;
        }
    }

    return leastLoadedCpu;
}

// Diagnostic helper: return up to maxEntries (pid, refcount) from expiredTasks for a CPU
size_t getExpiredTaskRefcounts(uint64_t cpuNo, uint64_t* pids, uint32_t* refcounts, size_t maxEntries, size_t startIndex) {
    if (runQueues == nullptr || pids == nullptr || refcounts == nullptr || maxEntries == 0) {
        return 0;
    }
    return runQueues->withLock(cpuNo, [pids, refcounts, maxEntries, startIndex](RunQueue* rq) -> size_t {
        size_t count = 0;
        if (rq == nullptr) return 0;
        size_t idx = 0;
        for (auto* node = rq->expiredTasks.getHead(); node != nullptr; node = node->next) {
            if (node->data == nullptr) continue;
            if (idx < startIndex) {
                idx++;
                continue;
            }
            if (count >= maxEntries) break;
            pids[count] = node->data->pid;
            refcounts[count] = node->data->refCount.load(std::memory_order_acquire);
            count++;
            idx++;
        }
        return count;
    });
}

// Post a task to the least loaded CPU for load balancing
auto postTaskBalanced(task::Task* task) -> bool {
    uint64_t targetCpu = getLeastLoadedCpu();
    task->cpu = targetCpu;
    return postTaskForCpu(targetCpu, task);
}

}  // namespace ker::mod::sched

// Export offset of deferredTaskSwitch for assembly code
extern "C" {
const uint64_t _wOS_DEFERRED_TASK_SWITCH_OFFSET = offsetof(ker::mod::sched::task::Task, deferredTaskSwitch);
}

extern "C" auto _wOS_getCurrentTask() -> ker::mod::sched::task::Task* { return ker::mod::sched::getCurrentTask(); }

extern "C" auto _wOS_getCurrentPagemap() -> ker::mod::mm::paging::PageTable* {
    auto* t = ker::mod::sched::getCurrentTask();
    return t ? t->pagemap : nullptr;
}
