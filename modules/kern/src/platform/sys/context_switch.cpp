#include "context_switch.hpp"

#include <atomic>
#include <cstdint>
#include <platform/asm/msr.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::sys::context_switch {

// Debug helper: update per-CPU task pointer for panic inspection
// NOTE: The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
// Now we just use the scheduler's getCurrentTask() instead of a separate debug array.
// This function is now a no-op since the scheduler tracks currentTask internally.
static inline void updateDebugTaskPtr([[maybe_unused]] sched::task::Task* task, [[maybe_unused]] uint64_t cpuId) {
    // No-op: scheduler's runQueues->thisCpu()->currentTask is the authoritative source
}

bool switchTo(cpu::GPRegs& gpr, gates::interruptFrame& frame, sched::task::Task* nextTask) {
    // CRITICAL: We must do ALL validation BEFORE modifying any state.
    // If we modify gpr/frame but fail to switch pagemap, the iretq will use
    // the new task's RIP/RSP with the wrong pagemap - causing a crash.

    // First state check - bail out immediately if task is not ACTIVE
    if (nextTask->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL early - PID %x not ACTIVE (state=%d)", nextTask->pid,
                 (int)nextTask->state.load(std::memory_order_relaxed));
        return false;
    }

    // Capture all values we need from the task BEFORE modifying anything.
    // This creates a consistent snapshot even if task starts exiting mid-switch.
    // Note: thread and pagemap pointers could become stale, but we defer their
    // freeing to GC so they remain valid during this epoch critical section.

    // Validate task has required resources for a user task
    // IDLE and DAEMON tasks use the kernel pagemap and have no user thread
    if (nextTask->type == sched::task::TaskType::PROCESS) {
        // User process must have pagemap
        if (nextTask->pagemap == nullptr) {
            dbg::log("switchTo: FAIL - PID %x pagemap==nullptr", nextTask->pid);
            // Task is being torn down, don't switch to it
            return false;
        }

        // Validate pagemap pointer is in valid HHDM range (not kernel static range)
        uintptr_t pmAddr = reinterpret_cast<uintptr_t>(nextTask->pagemap);
        if (pmAddr >= 0xffffffff80000000ULL || pmAddr < 0xffff800000000000ULL) {
            dbg::log("switchTo: FAIL - PID %x pagemap ptr invalid (pmAddr=0x%x)", nextTask->pid, pmAddr);
            // Corrupted pagemap pointer - task struct was freed/corrupted
            return false;
        }

        // Validate thread pointer if present
        if (nextTask->thread != nullptr) {
            uintptr_t threadAddr = reinterpret_cast<uintptr_t>(nextTask->thread);
            // Accept either HHDM or kernel-static addresses as valid
            bool thInHHDM = (threadAddr >= 0xffff800000000000ULL && threadAddr < 0xffff900000000000ULL);
            bool thInKernelStatic = (threadAddr >= 0xffffffff80000000ULL && threadAddr < 0xffffffffc0000000ULL);
            if (!thInHHDM && !thInKernelStatic) {
                dbg::log("switchTo: FAIL - PID %x thread ptr invalid (thread=0x%lx)", nextTask->pid, threadAddr);
                // Corrupted thread pointer - task struct was freed/corrupted
                return false;
            }
        }
    }

    // Second state check after reading resources - task might have started exiting
    if (nextTask->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL 2nd check - PID %x changed state (state=%d)", nextTask->pid,
                 (int)nextTask->state.load(std::memory_order_relaxed));
        return false;
    }

    // === POINT OF NO RETURN ===
    // After this point, we MUST complete the context switch.
    // The epoch guard in processTasks ensures the task struct and its resources
    // (thread, pagemap) won't be freed until after we exit the critical section.

    // Get real CPU ID from APIC
    uint32_t apicId = apic::getApicId();
    uint64_t realCpuId = smt::getCpuIndexFromApicId(apicId);

    // Update debug task pointer for panic inspection
    updateDebugTaskPtr(nextTask, realCpuId);

    // Now safe to modify interrupt frame and registers
    frame.rip = nextTask->context.frame.rip;
    frame.rsp = nextTask->context.frame.rsp;
    frame.cs = nextTask->context.frame.cs;
    frame.ss = nextTask->context.frame.ss;
    frame.flags = nextTask->context.frame.flags;

    gpr = nextTask->context.regs;

    // Validate context before restoring — catch corruption before it causes a crash
    // in userspace where debugging is much harder.
    // Only validate user-mode context for PROCESS tasks (IDLE/DAEMON run in ring 0)
    // Skip validation when voluntaryBlock is set — the saved context is legitimately
    // kernel-mode (task was preempted at a safe blocking point like sti;hlt in a syscall).
    if (nextTask->type == sched::task::TaskType::PROCESS && !nextTask->voluntaryBlock) {
        if (frame.cs != desc::gdt::GDT_USER_CS) {
            dbg::log("switchTo: CORRUPT cs=0x%x (expected 0x%x) PID %x", frame.cs, desc::gdt::GDT_USER_CS, nextTask->pid);
            for (;;) asm volatile("hlt");
        }
        if (frame.ss != desc::gdt::GDT_USER_DS) {
            dbg::log("switchTo: CORRUPT ss=0x%x (expected 0x%x) PID %x", frame.ss, desc::gdt::GDT_USER_DS, nextTask->pid);
            for (;;) asm volatile("hlt");
        }
        if (frame.rip >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rip=0x%x (kernel addr?) PID %x", frame.rip, nextTask->pid);
            for (;;) asm volatile("hlt");
        }
        if (frame.rsp >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rsp=0x%x (kernel addr?) PID %x", frame.rsp, nextTask->pid);
            for (;;) asm volatile("hlt");
        }
    }

    // Update scratch area cpuId
    auto* scratchArea = reinterpret_cast<cpu::PerCpu*>(nextTask->context.syscallScratchArea);
    scratchArea->cpuId = realCpuId;

    // Set up GS/FS bases
    if (nextTask->thread) {
        cpu::wrgsbase(nextTask->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->thread->gsbase);
        cpu::wrfsbase(nextTask->thread->fsbase);
    } else {
        // Idle task uses kernel-allocated scratch area for both
        cpu::wrgsbase(nextTask->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->context.syscallScratchArea);
    }

    // Switch pagemap for user tasks
    // We've already validated pagemap != nullptr for user tasks above
    if (nextTask->pagemap != nullptr) {
        mm::virt::switchPagemap(nextTask);
    }

    return true;
}

static long timerQuantum;

// Tick counter for periodic epoch advancement and garbage collection
static std::atomic<uint64_t> timerTickCount{0};

extern "C" void _wOS_schedTimer(void* stack_ptr) {
    // The stack layout at this point (from low to high address):
    // [GPRegs] <- stack_ptr points here (15 registers)
    // [intNum]
    // [errCode]
    // [RIP]
    // [CS]
    // [RFLAGS]
    // [RSP]
    // [SS]

    apic::eoi();

    // Advance epoch and run garbage collection periodically on CPU 0 only.
    // This ensures consistent epoch advancement and avoids contention.
    // Every 10 timer ticks (~100ms at 10ms timer) we advance the epoch
    // and collect dead tasks whose grace period has elapsed.
    // Increased frequency (from 100 to 10) to prevent memory buildup when
    // processes exit faster than GC can reclaim.
    uint64_t ticks = timerTickCount.fetch_add(1, std::memory_order_relaxed);
    if (cpu::currentCpu() == 0 && (ticks % 10) == 0) {
        sched::EpochManager::advanceEpoch();
        sched::gc_expired_tasks();
    }

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    // This may not return if we switch contexts
    // Note: processTasks will arm the timer when appropriate
    sched::process_tasks(*gpr_ptr, *frame_ptr);

    // Re-arm timer for next quantum (only reached if processTasks returned,
    // meaning we're continuing with idle task or same task)
    apic::oneShotTimer(timerQuantum);
}

extern "C" void _wOS_jumpToNextTaskNoSave(void* stack_ptr) {
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jump_to_next_task(*gpr_ptr, *frame_ptr);
}

void startSchedTimer() {
    timerQuantum = apic::calibrateTimer(10000);  // 10ms
    apic::oneShotTimer(timerQuantum);
}
}  // namespace ker::mod::sys::context_switch
