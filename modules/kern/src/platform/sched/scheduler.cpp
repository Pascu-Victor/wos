#include "scheduler.hpp"

#include <atomic>
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

// Kernel idle loop - defined in context_switch.asm
extern "C" void _wOS_kernel_idle_loop();

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue>* runQueues;

// Debug: Per-CPU current task pointers at fixed address for panic inspection
// Located at physical address 0x500000 (5 MB mark - safe area in our memory layout)
// This allows debugger/panic handler to examine current task state on each CPU
// Access via: x/2xg 0xffff800000500000  (for 2 CPUs)
// Then: p *(Task*)0x<address> to see task details
static constexpr uint64_t DEBUG_TASK_PTR_BASE = 0xffff800000500000ULL;  // Higher half mapping

// TODO: may be unique_ptr
auto postTask(task::Task* task) -> bool {
    // thisCpu access doesn't need locking - only current CPU accesses it
    auto* rq = runQueues->thisCpu();
    rq->activeTasks.push_back(task);
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
    task::Task* task = runQueues->thisCpu()->activeTasks.front();
    return task;
}

void removeCurrentTask() {
    runQueues->thisCpu()->activeTasks.pop_front();

    if (runQueues->thisCpu()->activeTasks.size() == 0) {
#ifdef SCHED_DEBUG
        dbg::log("No more tasks in runqueue, halting CPU");
#endif
        for (;;) {
            asm volatile("hlt");
        }
    }
}

void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    // Note: EOI is called by _wOS_schedTimer before calling this function
    auto* currentTask = getCurrentTask();

    // Only save context for non-idle tasks that have run
    if (currentTask->hasRun && currentTask->type != task::TaskType::IDLE) {
        currentTask->context.regs = gpr;
        currentTask->context.frame = frame;
    }

    // Use locked access since other CPUs can post tasks to our queue
    task::Task* nextTask = runQueues->thisCpuLocked([currentTask](RunQueue* rq) -> task::Task* {
        rq->activeTasks.pop_front();
        rq->activeTasks.push_back(currentTask);
        task::Task* next = rq->activeTasks.front();

        // Skip idle tasks if there are other tasks available
        size_t taskCount = rq->activeTasks.size();
        size_t checked = 0;
        while (next->type == task::TaskType::IDLE && taskCount > 1 && checked < taskCount) {
            rq->activeTasks.pop_front();
            rq->activeTasks.push_back(next);
            next = rq->activeTasks.front();
            checked++;
        }
        return next;
    });

    // If we only have idle task, just return to the interrupt handler.
    // The handler will do iretq which returns to wherever we were (likely hlt loop).
    // The timer is already armed by _wOS_schedTimer, so we'll get another interrupt.
    // This avoids nested interrupts from halting inside this function.
    if (nextTask->type == task::TaskType::IDLE) {
        // Mark CPU as idle so other CPUs know to send IPI when posting tasks
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        return;
    }

    // Mark CPU as not idle since we have a real task to run
    runQueues->thisCpu()->isIdle.store(false, std::memory_order_release);

    // Mark the next task as having run
    nextTask->hasRun = true;

    sys::context_switch::switchTo(gpr, frame, nextTask);
}

// Jump to the next task without saving the current task's state
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame) {
    apic::eoi();

    // Use locked access since other CPUs can post tasks to our queue
    task::Task* nextTask = runQueues->thisCpuLocked([](RunQueue* rq) -> task::Task* {
        task::Task* current = rq->activeTasks.front();
        rq->expiredTasks.push_back(current);
        rq->activeTasks.pop_front();

        // Check if we have any tasks left
        if (rq->activeTasks.size() == 0) {
            return nullptr;
        }
        return rq->activeTasks.front();
    });

    if (nextTask == nullptr) {
        // No more tasks - should not happen if idle task exists
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: No tasks left after exit, halting forever", cpu::currentCpu());
#endif
        // Just halt forever - no tasks means system is essentially dead
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);
        for (;;) {
            asm volatile("sti");
            asm volatile("hlt");
        }
    }

    // If only idle task remains, we CANNOT switch to it because idle tasks
    // don't have valid usermode context. We need to enter a kernel idle loop
    // that properly cleans up the stack.
    //
    // The trick: set up a frame that returns to a kernel-mode halt routine.
    // We'll create a simple idle loop in kernel space.
    if (nextTask->type == task::TaskType::IDLE) {
        // Mark CPU as idle so other CPUs know to send IPI when posting tasks
        runQueues->thisCpu()->isIdle.store(true, std::memory_order_release);

        // Arm the timer so we wake up periodically to check for new tasks
        apic::oneShotTimer(apic::calibrateTimer(10000));  // 10ms timer

        // Instead of returning to userspace (which idle task doesn't have),
        // we'll modify the frame to do an infinite halt loop when iretq runs.
        // We set CS to kernel mode (0x8) and set up a simple hlt loop.
        frame.rip = (uint64_t)_wOS_kernel_idle_loop;
        frame.cs = 0x08;                                         // Kernel code segment
        frame.ss = 0x10;                                         // Kernel data segment
        frame.rsp = (uint64_t)mm::phys::pageAlloc(4096) + 4096;  // Allocate a small stack for idle
        frame.flags = 0x202;                                     // IF=1, reserved=1

        // Clear GPRegs
        gpr = cpu::GPRegs();
        return;
    }

    // Mark CPU as not idle since we have a real task to run
    runQueues->thisCpu()->isIdle = false;

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

    // Check if we only have idle task(s)
    auto* firstTask = runQueues->thisCpu()->activeTasks.front();

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
                        return true;
                    }

                    // Task not ready yet, restore idle to front and wait more
                    rq->activeTasks.pop_back();            // Remove idle from back
                    rq->activeTasks.push_front(idleTask);  // Put idle back at front
                    firstTask = idleTask;                  // Reset to idle
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
    dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", 
             firstTask->thread->fsbase, firstTask->thread->gsbase, firstTask->context.syscallScratchArea);
    cpuSetMSR(IA32_GS_BASE, firstTask->thread->gsbase);  // User's TLS/stack base
    cpuSetMSR(IA32_KERNEL_GS_BASE, firstTask->context.syscallScratchArea);  // Scratch area for kernel
    cpuSetMSR(IA32_FS_BASE, firstTask->thread->fsbase);

    mm::virt::switchPagemap(firstTask);

    // Write TLS self-pointer after switching pagemaps so it goes to the correct user-mapped physical memory
    *((uint64_t*)firstTask->thread->fsbase) = firstTask->thread->fsbase;

    // Update debug task pointer for panic inspection
    auto** debug_ptrs = reinterpret_cast<task::Task**>(DEBUG_TASK_PTR_BASE);
    debug_ptrs[cpu::currentCpu()] = firstTask;

    // Start the scheduler timer now that everything is set up
    sys::context_switch::startSchedTimer();

    for (;;) {
        _wOS_asm_enterUsermode(firstTask->entry, firstTask->context.frame.rsp);
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

void rescheduleTaskForCpu(uint64_t cpuNo, task::Task* task) {
    // Add the task to the active tasks list for the specified CPU with proper locking
    runQueues->withLockVoid(cpuNo, [task](RunQueue* rq) { rq->activeTasks.push_back(task); });
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
