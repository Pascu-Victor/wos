#include "context_switch.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <platform/tsc/tsc.hpp>

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

// Save FPU/SSE/AVX state of a task. Uses xsave if available, fxsave otherwise.
// The memory operand must be 64-byte aligned for xsave, 16-byte for fxsave.
// FxState::aligned() guarantees 64-byte alignment regardless of Task placement.
void saveFpuState(sched::task::Task* task) {
    auto* buf = task->fxState.aligned();
    if (cpu::xsaveAreaSize > 0) {
        asm volatile("xsave64 (%0)" : : "r"(buf), "a"(0x7), "d"(0) : "memory");
    } else {
        asm volatile("fxsave64 (%0)" : : "r"(buf) : "memory");
    }
    task->fxState.saved = true;
}

// Restore FPU/SSE/AVX state of a task. Uses xrstor if available, fxrstor otherwise.
void restoreFpuState(sched::task::Task* task) {
    auto* buf = task->fxState.aligned();
    if (cpu::xsaveAreaSize > 0) {
        asm volatile("xrstor64 (%0)" : : "r"(buf), "a"(0x7), "d"(0) : "memory");
    } else {
        asm volatile("fxrstor64 (%0)" : : "r"(buf) : "memory");
    }
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

    // Get real CPU ID from GS (no MSR / VM exit)
    uint64_t realCpuId = cpu::currentCpu();

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

    // Restore incoming task's FPU/SSE/AVX state (PROCESS tasks only).
    if (nextTask->type == sched::task::TaskType::PROCESS && nextTask->fxState.saved) {
        restoreFpuState(nextTask);
    }

    return true;
}

static long timerQuantum;

// Tick counter for periodic epoch advancement and garbage collection
static std::atomic<uint64_t> timerTickCount{0};

namespace {
constexpr bool kEnableSchedHotLogging = false;
constexpr bool kEnableSchedCpuDump = false;

[[maybe_unused]]
constexpr uint64_t HOT_TASK_STREAK_TICKS = 250;
[[maybe_unused]]
constexpr uint64_t SCHED_CPU_DUMP_PERIOD_TICKS = 1000;

struct HotTaskTracker {
    uint64_t last_pid{0};
    uint64_t streak{0};
};

[[maybe_unused]]
std::array<HotTaskTracker, 16> hotTaskTrackers;
}  // namespace

#ifdef SCHED_DEBUG
// Per-CPU interrupt timing stats (indexed by CPU id, max 16 CPUs)
struct IrqStats {
    std::atomic<uint64_t> total_ns{0};      // total ns spent inside _wOS_schedTimer
    std::atomic<uint64_t> count{0};         // number of timer interrupts
    std::atomic<uint64_t> ctx_switches{0};  // number of context switches performed
    std::atomic<uint64_t> xsave_ns{0};      // total ns spent in saveFpuState
};
static std::array<IrqStats, 16> irqStats;

void dump_irq_stats() {
    uint64_t grand_total_ns = 0;
    uint64_t grand_count = 0;
    uint64_t grand_switches = 0;
    uint64_t grand_xsave_ns = 0;
    for (uint64_t i = 0; i < 16; i++) {
        uint64_t ns = irqStats[i].total_ns.load(std::memory_order_relaxed);
        uint64_t cnt = irqStats[i].count.load(std::memory_order_relaxed);
        uint64_t sw = irqStats[i].ctx_switches.load(std::memory_order_relaxed);
        uint64_t xns = irqStats[i].xsave_ns.load(std::memory_order_relaxed);
        if (cnt == 0) continue;
        dbg::log("irqstats: cpu%lu ticks=%lu avg_ns=%lu switches=%lu xsave_ms=%lu", (unsigned long)i, (unsigned long)cnt,
                 (unsigned long)(ns / cnt), (unsigned long)sw, (unsigned long)(xns / 1000000ULL));
        grand_total_ns += ns;
        grand_count += cnt;
        grand_switches += sw;
        grand_xsave_ns += xns;
    }
    if (grand_count > 0) {
        dbg::log("irqstats: TOTAL ticks=%lu switches=%lu total_irq_ms=%lu xsave_ms=%lu avg_ns=%lu", (unsigned long)grand_count,
                 (unsigned long)grand_switches, (unsigned long)(grand_total_ns / 1000000ULL), (unsigned long)(grand_xsave_ns / 1000000ULL),
                 (unsigned long)(grand_total_ns / grand_count));
    }
}
#endif  // SCHED_DEBUG

extern "C" void _wOS_schedTimer(void* stack_ptr) {
    apic::eoi();

    // Advance epoch and run garbage collection periodically on CPU 0 only.
    uint64_t ticks = timerTickCount.fetch_add(1, std::memory_order_relaxed);
    if (cpu::currentCpu() == 0 && (ticks % 10) == 0) {
        sched::EpochManager::advanceEpoch();
        sched::gc_expired_tasks();
    }

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

#ifdef SCHED_DEBUG
    uint64_t t0 = rdtsc();
    auto* task_before = sched::get_current_task();
#endif

    sched::process_tasks(*gpr_ptr, *frame_ptr);

    apic::oneShotTimer(timerQuantum);

    auto* current_task = sched::get_current_task();
    uint64_t cpu_no = cpu::currentCpu();
    if constexpr (kEnableSchedHotLogging) {
        if (cpu_no < hotTaskTrackers.size()) {
            auto& tracker = hotTaskTrackers[cpu_no];
            uint64_t current_pid = current_task != nullptr ? current_task->pid : 0;

            if (current_pid != 0 && current_pid == tracker.last_pid) {
                tracker.streak++;
            } else {
                tracker.last_pid = current_pid;
                tracker.streak = current_pid != 0 ? 1 : 0;
            }

            if (current_task != nullptr && current_task->type != sched::task::TaskType::IDLE && tracker.streak != 0 &&
                (tracker.streak % HOT_TASK_STREAK_TICKS) == 0) {
                auto stats = sched::get_run_queue_stats(cpu_no);
                dbg::log("schedhot: cpu%lu pid=%lu(%s) streak=%lu type=%u vblk=%u wblk=%u pinned=%u runq=%lu waitq=%lu cs=0x%x",
                         (unsigned long)cpu_no, (unsigned long)current_task->pid, current_task->name != nullptr ? current_task->name : "?",
                         (unsigned long)tracker.streak, (unsigned)current_task->type, current_task->voluntaryBlock ? 1U : 0U,
                         current_task->wantsBlock ? 1U : 0U, current_task->cpu_pinned ? 1U : 0U, (unsigned long)stats.active_task_count,
                         (unsigned long)stats.wait_queue_count, (unsigned)frame_ptr->cs);
            }
        }
    }

    if constexpr (kEnableSchedCpuDump) {
        if (cpu_no == 0 && (ticks % SCHED_CPU_DUMP_PERIOD_TICKS) == (SCHED_CPU_DUMP_PERIOD_TICKS - 1)) {
            sched::dump_scheduler_cpu_states();
        }
    }

#ifdef SCHED_DEBUG
    auto* task_after = sched::get_current_task();
    uint64_t elapsed_ns = tsc::ticksToNs(rdtsc() - t0);
    uint64_t cpu = cpu_no;
    if (cpu < 16) {
        irqStats[cpu].total_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        irqStats[cpu].count.fetch_add(1, std::memory_order_relaxed);
        if (task_before != task_after) {
            irqStats[cpu].ctx_switches.fetch_add(1, std::memory_order_relaxed);
        }
    }

    static std::atomic<uint64_t> switch_log_count{0};
    if (cpu == 0 && task_before != task_after && ticks > 25000) {
        uint64_t n = switch_log_count.fetch_add(1, std::memory_order_relaxed);
        if (n < 32) {
            const char* name_before = (task_before != nullptr && task_before->name != nullptr) ? task_before->name : "?";
            const char* name_after = (task_after != nullptr && task_after->name != nullptr) ? task_after->name : "?";
            dbg::log("sw[%lu] t=%lu: %lu(%s)->%lu(%s) cs=0x%x", (unsigned long)n, (unsigned long)ticks,
                     task_before != nullptr ? (unsigned long)task_before->pid : 0UL, name_before,
                     task_after != nullptr ? (unsigned long)task_after->pid : 0UL, name_after, (unsigned)(frame_ptr->cs));
        }
    }

    if (cpu == 0 && (ticks % 500) == 499) {
        dump_irq_stats();
        sched::dump_scheduler_trace_stats();
    }
#endif  // SCHED_DEBUG
}

extern "C" void _wOS_jumpToNextTaskNoSave(void* stack_ptr) {
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jump_to_next_task(*gpr_ptr, *frame_ptr);
}

void startSchedTimer() {
    timerQuantum = apic::calibrateTimer(4000);  // 4ms (matches Linux CFS typical quantum)
    apic::oneShotTimer(timerQuantum);
}

void request_reschedule() {
    // Fires the scheduler timer on the next CPU cycle
    apic::oneShotTimer(1);
}
}  // namespace ker::mod::sys::context_switch
