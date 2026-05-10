#include "context_switch.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <util/hcf.hpp>
#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/signal.hpp"

namespace ker::mod::sys::context_switch {
extern "C" void wos_kernel_thread_trampoline();                       // NOLINT(readability-identifier-naming)
extern "C" void wos_kernel_idle_loop();                               // NOLINT(readability-identifier-naming)
extern "C" [[noreturn]] void wos_enterIdleStack(uint64_t stack_top);  // NOLINT(readability-identifier-naming)

namespace {
inline auto valid_kernel_stack(uint64_t rsp) -> bool { return rsp >= 0xffff800000000000ULL && rsp < 0xffff900000000000ULL; }

inline auto stack_belongs_to_task(sched::task::Task* task, uint64_t rsp) -> bool {
    if (task == nullptr || !valid_kernel_stack(task->context.syscall_kernel_stack)) {
        return false;
    }
    uint64_t const STACK_TOP = task->context.syscall_kernel_stack;
    return rsp > STACK_TOP - KERNEL_STACK_SIZE && rsp <= STACK_TOP;
}

inline void validate_kernel_frame(const gates::InterruptFrame& frame, sched::task::Task* task, const char* path) {
    if (frame.cs == desc::gdt::GDT_USER_CS) {
        if (task != nullptr && task->type == sched::task::TaskType::PROCESS) {
            return;
        }
        dbg::logger<"ctxswitch">::error(
            "user return frame for non-process: path=%s pid=%lu name=%s type=%u ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx task=%p",
            path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
            task != nullptr ? static_cast<unsigned>(task->type) : 0U, static_cast<unsigned long long>(frame.ss),
            static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
            static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
        hcf();
    }

    if (frame.cs != desc::gdt::GDT_KERN_CS) {
        dbg::logger<"ctxswitch">::error(
            "bad return frame selector: path=%s pid=%lu name=%s cs=0x%llx ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx task=%p",
            path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
            static_cast<unsigned long long>(frame.cs), static_cast<unsigned long long>(frame.ss),
            static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
            static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
        hcf();
    }

    const bool RIP_BAD = frame.rip < 0xffffffff80000000ULL || frame.rip >= 0xffffffffc0000000ULL;
    const bool RSP_BAD = !valid_kernel_stack(frame.rsp);
    const bool STACK_OWNER_BAD = task != nullptr && task->type != sched::task::TaskType::IDLE && !stack_belongs_to_task(task, frame.rsp);
    const bool FLAGS_BAD = (frame.flags & 0x2ULL) == 0;
    const bool SS_BAD = frame.ss != desc::gdt::GDT_KERN_DS;
    if (!RIP_BAD && !RSP_BAD && !STACK_OWNER_BAD && !FLAGS_BAD && !SS_BAD) {
        return;
    }

    auto* owner = sched::debug_find_task_by_kernel_stack(frame.rsp);
    dbg::logger<"ctxswitch">::error(
        "bad kernel return frame: path=%s pid=%lu name=%s rip=0x%llx rsp=0x%llx stack=0x%llx owner=%lu(%s) cs=0x%llx ss=0x%llx "
        "flags=0x%llx task=%p",
        path != nullptr ? path : "?", task != nullptr ? task->pid : 0, (task != nullptr && task->name != nullptr) ? task->name : "?",
        static_cast<unsigned long long>(frame.rip), static_cast<unsigned long long>(frame.rsp),
        task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL, owner != nullptr ? owner->pid : 0UL,
        (owner != nullptr && owner->name != nullptr) ? owner->name : "?", static_cast<unsigned long long>(frame.cs),
        static_cast<unsigned long long>(frame.ss), static_cast<unsigned long long>(frame.flags), static_cast<void*>(task));
    hcf();
}

inline auto is_kernel_thread_trampoline_frame(const gates::InterruptFrame& frame, const cpu::GPRegs& gpr, sched::task::Task* task) -> bool {
    return task != nullptr && task->type == sched::task::TaskType::DAEMON && task->kthread_entry != nullptr &&
           frame.cs == desc::gdt::GDT_KERN_CS && frame.rip == reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline) &&
           gpr.rdi == reinterpret_cast<uint64_t>(task->kthread_entry);
}

inline auto is_idle_return_frame(const gates::InterruptFrame& frame, sched::task::Task* task) -> bool {
    return task != nullptr && task->type == sched::task::TaskType::IDLE && frame.cs == desc::gdt::GDT_KERN_CS &&
           frame.rip == reinterpret_cast<uint64_t>(wos_kernel_idle_loop);
}

auto interrupted_caller_from_rbp(uint64_t rbp) -> uint64_t {
    if (!valid_kernel_stack(rbp)) {
        return 0;
    }
    auto* frame = reinterpret_cast<uint64_t*>(rbp);
    return frame[1];
}

void validate_timer_stack(const gates::InterruptFrame& frame, const cpu::GPRegs& gpr, sched::task::Task* task) {
    if (task == nullptr || frame.cs != desc::gdt::GDT_KERN_CS || task->type == sched::task::TaskType::IDLE) {
        return;
    }
    if (stack_belongs_to_task(task, frame.rsp)) {
        return;
    }

    auto* owner = sched::debug_find_task_by_kernel_stack(frame.rsp);
    uint64_t const CALLER = interrupted_caller_from_rbp(gpr.rbp);
    dbg::logger<"ctxswitch">::error(
        "timer on non-task stack: current=%lu(%s) rip=0x%llx caller=0x%llx rsp=0x%llx stack=0x%llx type=%u hasRun=%u owner=%lu(%s) "
        "owner_stack=0x%llx owner_cpu=%llu owner_q=%u owner_heap=%d",
        task->pid, task->name != nullptr ? task->name : "?", static_cast<unsigned long long>(frame.rip),
        static_cast<unsigned long long>(CALLER), static_cast<unsigned long long>(frame.rsp),
        static_cast<unsigned long long>(task->context.syscall_kernel_stack), static_cast<unsigned>(task->type), task->has_run ? 1U : 0U,
        owner != nullptr ? owner->pid : 0UL, (owner != nullptr && owner->name != nullptr) ? owner->name : "?",
        owner != nullptr ? static_cast<unsigned long long>(owner->context.syscall_kernel_stack) : 0ULL,
        owner != nullptr ? static_cast<unsigned long long>(owner->cpu) : 0ULL,
        owner != nullptr ? static_cast<unsigned>(owner->sched_queue) : 0U, owner != nullptr ? owner->heap_index : -1);
    hcf();
}

auto current_stack_matches_current_task() -> bool {
    if (!sched::can_query_current_task()) {
        return true;
    }
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type == sched::task::TaskType::IDLE) {
        return true;
    }
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp)::"memory");
    return stack_belongs_to_task(task, rsp);
}

[[noreturn]] void enter_idle_from_timer(sched::task::Task* task, const gates::InterruptFrame& frame) {
    uint64_t const STACK = task != nullptr ? task->context.syscall_kernel_stack : 0;
    if (!valid_kernel_stack(STACK)) {
        dbg::logger<"ctxswitch">::error("timer idle return bad rsp: pid=%lu frame_rsp=0x%llx stack=0x%llx", task != nullptr ? task->pid : 0,
                                        static_cast<unsigned long long>(frame.rsp), static_cast<unsigned long long>(STACK));
        hcf();
    }

    mm::virt::switch_to_kernel_pagemap();
    auto* scratch = reinterpret_cast<cpu::PerCpu*>(task->context.syscall_scratch_area);
    scratch->cpu_id = cpu::current_cpu();
    cpu::wrgsbase(task->context.syscall_scratch_area);
    cpu_set_msr(IA32_KERNEL_GS_BASE, task->context.syscall_scratch_area);

    wos_enterIdleStack(STACK);
}
}  // namespace

// Debug helper: update per-CPU task pointer for panic inspection
// NOTE: The old DEBUG_TASK_PTR_BASE (0xffff800000500000) conflicted with kernel page tables!
// Now we just use the scheduler's getCurrentTask() instead of a separate debug array.
// This function is now a no-op since the scheduler tracks currentTask internally.
static inline void update_debug_task_ptr([[maybe_unused]] sched::task::Task* task, [[maybe_unused]] uint64_t cpu_id) {
    // No-op: scheduler's runQueues->thisCpu()->currentTask is the authoritative source
}

static inline void kasan_unpoison_irq_save_area([[maybe_unused]] void* stack_ptr) {
#ifdef WOS_KASAN
    if (ker::mod::kasan::is_enabled()) {
        // Built by hardware/assembly, then patched by C++ before iretq.
        ker::mod::kasan::unpoison_range(stack_ptr, sizeof(cpu::GPRegs) + sizeof(gates::InterruptFrame));
    }
#endif
}

// Save FPU/SSE/AVX state of a task. Uses xsave if available, fxsave otherwise.
// The memory operand must be 64-byte aligned for xsave, 16-byte for fxsave.
// fx_state::aligned() guarantees 64-byte alignment regardless of Task placement.
void save_fpu_state(sched::task::Task* task) {
    // NOLINTNEXTLINE(misc-const-correctness)
    auto* buf = task->fx_state.aligned();
    if (cpu::xsave_area_size > 0) {
        asm volatile("xsave64 (%0)" : : "r"(buf), "a"(0xE7), "d"(0) : "memory");
    } else {
        asm volatile("fxsave64 (%0)" : : "r"(buf) : "memory");
    }
    task->fx_state.saved = true;
}

// Restore FPU/SSE/AVX state of a task. Uses xrstor if available, fxrstor otherwise.
void restore_fpu_state(sched::task::Task* task) {
    // NOLINTNEXTLINE(misc-const-correctness)
    auto* buf = task->fx_state.aligned();
    if (cpu::xsave_area_size > 0) {
        asm volatile("xrstor64 (%0)" : : "r"(buf), "a"(0xE7), "d"(0) : "memory");
    } else {
        asm volatile("fxrstor64 (%0)" : : "r"(buf) : "memory");
    }
}

auto switch_to(cpu::GPRegs& gpr, gates::InterruptFrame& frame, sched::task::Task* next_task) -> bool {
    // CRITICAL: We must do ALL validation BEFORE modifying any state.
    // If we modify gpr/frame but fail to switch pagemap, the iretq will use
    // the new task's RIP/RSP with the wrong pagemap - causing a crash.

    // First state check - bail out immediately if task is not ACTIVE
    if (next_task->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL early - PID %x not ACTIVE (state=%d)", next_task->pid,
                 static_cast<int>(next_task->state.load(std::memory_order_relaxed)));
        return false;
    }

    // Capture all values we need from the task BEFORE modifying anything.
    // This creates a consistent snapshot even if task starts exiting mid-switch.
    // Note: thread and pagemap pointers could become stale, but we defer their
    // freeing to GC so they remain valid during this epoch critical section.

    // Validate task has required resources for a user task
    // IDLE and DAEMON tasks use the kernel pagemap and have no user thread
    if (next_task->type == sched::task::TaskType::PROCESS) {
        // User process must have pagemap
        if (next_task->pagemap == nullptr) {
            dbg::log("switchTo: FAIL - PID %x pagemap==nullptr", next_task->pid);
            // Task is being torn down, don't switch to it
            return false;
        }

        // Validate pagemap pointer is in valid HHDM range (not kernel static range)
        auto pm_addr = reinterpret_cast<uintptr_t>(next_task->pagemap);
        if (pm_addr >= 0xffffffff80000000ULL || pm_addr < 0xffff800000000000ULL) {
            dbg::log("switchTo: FAIL - PID %x pagemap ptr invalid (pmAddr=0x%x)", next_task->pid, pm_addr);
            // Corrupted pagemap pointer - task struct was freed/corrupted
            return false;
        }

        // Validate thread pointer if present
        if (next_task->thread != nullptr) {
            auto thread_addr = reinterpret_cast<uintptr_t>(next_task->thread);
            // Accept either HHDM or kernel-static addresses as valid
            bool const TH_IN_HHDM = (thread_addr >= 0xffff800000000000ULL && thread_addr < 0xffff900000000000ULL);
            bool const TH_IN_KERNEL_STATIC = (thread_addr >= 0xffffffff80000000ULL && thread_addr < 0xffffffffc0000000ULL);
            if (!TH_IN_HHDM && !TH_IN_KERNEL_STATIC) {
                dbg::log("switchTo: FAIL - PID %x thread ptr invalid (thread=0x%lx)", next_task->pid, thread_addr);
                // Corrupted thread pointer - task struct was freed/corrupted
                return false;
            }
        }
    }

    // Second state check after reading resources - task might have started exiting
    if (next_task->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE) {
        dbg::log("switchTo: FAIL 2nd check - PID %x changed state (state=%d)", next_task->pid,
                 static_cast<int>(next_task->state.load(std::memory_order_relaxed)));
        return false;
    }

    // === POINT OF NO RETURN ===
    // After this point, we MUST complete the context switch.
    // The epoch guard in processTasks ensures the task struct and its resources
    // (thread, pagemap) won't be freed until after we exit the critical section.

    // Get real CPU ID from GS (no MSR / VM exit)
    uint64_t const REAL_CPU_ID = cpu::current_cpu();

    // Update debug task pointer for panic inspection
    update_debug_task_ptr(next_task, REAL_CPU_ID);

    // Now safe to modify interrupt frame and registers.
    if (next_task->type == sched::task::TaskType::DAEMON && !valid_kernel_stack(next_task->context.frame.rsp) &&
        valid_kernel_stack(next_task->context.syscall_kernel_stack)) {
        dbg::logger<"ctxswitch">::warn("repairing daemon rsp before switch: pid=%lu name=%s frame_rsp=0x%llx stack=0x%llx", next_task->pid,
                                       next_task->name != nullptr ? next_task->name : "?",
                                       static_cast<unsigned long long>(next_task->context.frame.rsp),
                                       static_cast<unsigned long long>(next_task->context.syscall_kernel_stack));
        next_task->context.frame.rsp = next_task->context.syscall_kernel_stack;
    }

    frame.rip = next_task->context.frame.rip;
    frame.rsp = next_task->context.frame.rsp;
    frame.cs = next_task->context.frame.cs;
    frame.ss = next_task->context.frame.ss;
    frame.flags = next_task->context.frame.flags;

    gpr = next_task->context.regs;
    validate_kernel_frame(frame, next_task, "switchTo");

    // Validate context before restoring - catch corruption before it causes a crash
    // in userspace where debugging is much harder.
    // Only validate user-mode context for PROCESS tasks (IDLE/DAEMON run in ring 0)
    // Skip validation when voluntary_block is set - the saved context is legitimately
    // kernel-mode (task was preempted at a safe blocking point like sti;hlt in a syscall).
    if (next_task->type == sched::task::TaskType::PROCESS && !next_task->voluntary_block) {
        if (frame.cs != desc::gdt::GDT_USER_CS) {
            dbg::log("switchTo: CORRUPT cs=0x%x (expected 0x%x) PID %x", frame.cs, desc::gdt::GDT_USER_CS, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.ss != desc::gdt::GDT_USER_DS) {
            dbg::log("switchTo: CORRUPT ss=0x%x (expected 0x%x) PID %x", frame.ss, desc::gdt::GDT_USER_DS, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.rip >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rip=0x%x (kernel addr?) PID %x", frame.rip, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
        if (frame.rsp >= 0x800000000000ULL) {
            dbg::log("switchTo: CORRUPT rsp=0x%x (kernel addr?) PID %x", frame.rsp, next_task->pid);
            for (;;) {
                asm volatile("hlt");
            };
        }
    }

    // Update scratch area cpu_id
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(next_task->context.syscall_scratch_area);
    scratch_area->cpu_id = REAL_CPU_ID;

    // Set up GS/FS bases
    if (next_task->thread != nullptr) {
        cpu::wrgsbase(next_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, next_task->thread->gsbase);
        cpu::wrfsbase(next_task->thread->fsbase);
    } else {
        // Idle task uses kernel-allocated scratch area for both
        cpu::wrgsbase(next_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, next_task->context.syscall_scratch_area);
    }

    // Switch pagemap for user tasks
    // We've already validated pagemap != nullptr for user tasks above
    if (next_task->pagemap != nullptr) {
        mm::virt::switch_pagemap(next_task);
    }

    // Restore incoming task's FPU/SSE/AVX state (PROCESS tasks only).
    if (next_task->type == sched::task::TaskType::PROCESS && next_task->fx_state.saved) {
        restore_fpu_state(next_task);
    }

    return true;
}

static long timer_quantum;

// Tick counter for periodic epoch advancement and garbage collection
static std::atomic<uint64_t> timer_tick_count{0};

namespace {
constexpr bool K_ENABLE_SCHED_HOT_LOGGING = false;
constexpr bool K_ENABLE_SCHED_CPU_DUMP = false;

[[maybe_unused]]
constexpr uint64_t HOT_TASK_STREAK_TICKS = 250;
[[maybe_unused]]
constexpr uint64_t SCHED_CPU_DUMP_PERIOD_TICKS = 1000;

struct HotTaskTracker {
    uint64_t last_pid{0};
    uint64_t streak{0};
};

[[maybe_unused]]
std::array<HotTaskTracker, 16> hot_task_trackers;
}  // namespace

#ifdef SCHED_DEBUG
// Per-CPU interrupt timing stats (indexed by CPU id, max 16 CPUs)
struct IrqStats {
    std::atomic<uint64_t> total_ns{0};      // total ns spent inside wos_sched_timer
    std::atomic<uint64_t> count{0};         // number of timer interrupts
    std::atomic<uint64_t> ctx_switches{0};  // number of context switches performed
    std::atomic<uint64_t> xsave_ns{0};      // total ns spent in save_fpu_state
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

extern "C" void wos_sched_timer(void* stack_ptr) {
    apic::eoi();

    // Advance epoch and run garbage collection periodically on CPU 0 only.
    uint64_t const TICKS = timer_tick_count.fetch_add(1, std::memory_order_relaxed);
    if (cpu::current_cpu() == 0 && (TICKS % 10) == 0) {
        sched::EpochManager::advance_epoch();
        sched::gc_expired_tasks();
    }

    kasan_unpoison_irq_save_area(stack_ptr);

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::InterruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));
    validate_timer_stack(*frame_ptr, *gpr_ptr, sched::get_current_task());

#ifdef SCHED_DEBUG
    uint64_t t0 = rdtsc();
    auto* task_before = sched::get_current_task();
#endif

    sched::process_tasks(*gpr_ptr, *frame_ptr);

    sys::signal::check_pending_signals_interrupt(*gpr_ptr, *frame_ptr);
    validate_kernel_frame(*frame_ptr, sched::get_current_task(), "timer-return");

    auto* current_task = sched::get_current_task();
    if (is_idle_return_frame(*frame_ptr, current_task)) {
        sched::arm_idle_timer_for_this_cpu();
        enter_idle_from_timer(current_task, *frame_ptr);
    }

    apic::one_shot_timer(timer_quantum);

    if (is_kernel_thread_trampoline_frame(*frame_ptr, *gpr_ptr, current_task)) {
        uint64_t stack = frame_ptr->rsp;
        if (!valid_kernel_stack(stack) && current_task != nullptr && valid_kernel_stack(current_task->context.syscall_kernel_stack)) {
            dbg::logger<"ctxswitch">::warn("direct daemon start repaired rsp: pid=%lu name=%s frame_rsp=0x%llx stack=0x%llx",
                                           current_task->pid, current_task->name != nullptr ? current_task->name : "?",
                                           (unsigned long long)frame_ptr->rsp,
                                           (unsigned long long)current_task->context.syscall_kernel_stack);
            stack = current_task->context.syscall_kernel_stack;
        }
        if (!valid_kernel_stack(stack)) {
            dbg::logger<"ctxswitch">::error(
                "direct daemon start bad rsp: pid=%lu name=%s frame_rsp=0x%llx stack=0x%llx",
                current_task != nullptr ? current_task->pid : 0,
                (current_task != nullptr && current_task->name != nullptr) ? current_task->name : "?", (unsigned long long)frame_ptr->rsp,
                current_task != nullptr ? (unsigned long long)current_task->context.syscall_kernel_stack : 0ULL);
            hcf();
        }
        wos_start_kernel_thread(stack, current_task->kthread_entry);
        __builtin_unreachable();
    }

    uint64_t const CPU_NO = cpu::current_cpu();
    if constexpr (K_ENABLE_SCHED_HOT_LOGGING) {
        if (CPU_NO < hot_task_trackers.size()) {
            auto& tracker = hot_task_trackers[CPU_NO];
            uint64_t const CURRENT_PID = current_task != nullptr ? current_task->pid : 0;

            if (CURRENT_PID != 0 && CURRENT_PID == tracker.last_pid) {
                tracker.streak++;
            } else {
                tracker.last_pid = CURRENT_PID;
                tracker.streak = CURRENT_PID != 0 ? 1 : 0;
            }

            if (current_task != nullptr && current_task->type != sched::task::TaskType::IDLE && tracker.streak != 0 &&
                (tracker.streak % HOT_TASK_STREAK_TICKS) == 0) {
                auto stats = sched::get_run_queue_stats(CPU_NO);
                dbg::log("schedhot: cpu%lu pid=%lu(%s) streak=%lu type=%u vblk=%u wblk=%u pinned=%u runq=%lu waitq=%lu cs=0x%x",
                         (unsigned long)CPU_NO, (unsigned long)current_task->pid, current_task->name != nullptr ? current_task->name : "?",
                         (unsigned long)tracker.streak, (unsigned)current_task->type, current_task->voluntary_block ? 1U : 0U,
                         current_task->wants_block ? 1U : 0U, current_task->cpu_pinned ? 1U : 0U, (unsigned long)stats.active_task_count,
                         (unsigned long)stats.wait_queue_count, (unsigned)frame_ptr->cs);
            }
        }
    }

    if constexpr (K_ENABLE_SCHED_CPU_DUMP) {
        if (CPU_NO == 0 && (TICKS % SCHED_CPU_DUMP_PERIOD_TICKS) == (SCHED_CPU_DUMP_PERIOD_TICKS - 1)) {
            sched::dump_scheduler_cpu_states();
        }
    }

#ifdef SCHED_DEBUG
    auto* task_after = sched::get_current_task();
    uint64_t elapsed_ns = tsc::ticks_to_ns(rdtsc() - t0);
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

extern "C" void wos_jump_to_next_task_no_save(void* stack_ptr) {
    kasan_unpoison_irq_save_area(stack_ptr);

    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::InterruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jump_to_next_task(*gpr_ptr, *frame_ptr);
    sys::signal::check_pending_signals_interrupt(*gpr_ptr, *frame_ptr);
    validate_kernel_frame(*frame_ptr, sched::get_current_task(), "exit-return");
}

void start_sched_timer() {
    timer_quantum = apic::calibrate_timer(4000);  // 4ms (matches Linux CFS typical quantum)
    apic::one_shot_timer(timer_quantum);
}

void request_reschedule() {
    // Fires the scheduler timer on the next CPU cycle
    if (!current_stack_matches_current_task()) {
        static std::atomic<uint64_t> skipped_mismatch_logs{0};
        uint64_t const N = skipped_mismatch_logs.fetch_add(1, std::memory_order_relaxed);
        if (N < 8) {
            auto* task = sched::get_current_task();
            // NOLINTNEXTLINE(misc-const-correctness)
            uint64_t rsp = 0;
            asm volatile("mov %%rsp, %0" : "=r"(rsp)::"memory");
            dbg::logger<"ctxswitch">::warn("skip local resched on non-current stack: current=%lu(%s) rsp=0x%llx stack=0x%llx",
                                           task != nullptr ? task->pid : 0UL, (task != nullptr && task->name != nullptr) ? task->name : "?",
                                           static_cast<unsigned long long>(rsp),
                                           task != nullptr ? static_cast<unsigned long long>(task->context.syscall_kernel_stack) : 0ULL);
        }
        return;
    }
    apic::one_shot_timer(1);
}
}  // namespace ker::mod::sys::context_switch
