#include "scheduler.hpp"

#include <abi/ptrace.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/userspace.hpp>
#include <utility>
// Debug helpers
#include <net/wki/remote_compute.hpp>
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
#include "platform/interrupt/idt.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sched/run_heap.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/smt/smt.hpp"
#include "platform/sys/context_switch.hpp"
#include "platform/sys/signal.hpp"
#include "syscalls_impl/futex/futex.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "util/hcf.hpp"

extern "C" const uint64_t WOS_DEFERRED_TASK_SWITCH_OFFSET = offsetof(ker::mod::sched::task::Task, deferred_task_switch);

// Kernel idle loop - defined in context_switch.asm
extern "C" void wos_kernel_idle_loop();                               // NOLINT(readability-identifier-naming)
extern "C" void wos_kernel_thread_trampoline();                       // NOLINT(readability-identifier-naming)
extern "C" [[noreturn]] void wos_enterIdleStack(uint64_t stack_top);  // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_start[];                                // NOLINT(readability-identifier-naming)
extern "C" char __kernel_text_end[];                                  // NOLINT(readability-identifier-naming)

extern "C" [[noreturn]] void wos_kernel_thread_returned() {  // NOLINT(readability-identifier-naming)
    auto* task = ker::mod::sched::get_current_task();
    ker::mod::dbg::logger<"sched">::error("kernel thread returned: pid=%lu name=%s", task != nullptr ? task->pid : 0,
                                          (task != nullptr && task->name != nullptr) ? task->name : "?");
    ker::syscall::process::wos_proc_exit(0);
    for (;;) {
        asm volatile("cli\n\thlt" ::: "memory");
    }
}

namespace ker::mod::sched {

// D17: WKI remote placement hook (nullptr when WKI not active)
bool (*wki_try_remote_placement_fn)(task::Task* task) = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ============================================================================
// Global state
// ============================================================================
namespace {
using wait_log = ker::mod::dbg::logger<"wait">;
using resume_log = ker::mod::dbg::logger<"resume">;
using preempt_log = ker::mod::dbg::logger<"preempt">;

// PID hash table for O(1) amortized lookups.
// Open-addressing with linear probing, backward-shift deletion.
// PIDs are 64-bit monotonically increasing; the table supports up to MAX_PIDS
// concurrent entries. Hash spreads sequential PIDs across the table.
struct PidHashEntry {
    uint64_t pid;      // 0 = empty slot
    task::Task* task;  // nullptr when slot empty
};

std::atomic<uint64_t> g_preempt_block_warnings{0};
sys::Spinlock g_placement_lock;
constexpr size_t PENDING_WAKE_LIMIT = 16;
using PendingWakeList = std::array<task::Task*, PENDING_WAKE_LIMIT>;

inline auto pending_wake_slot(PendingWakeList& tasks, uint32_t index) -> task::Task*& {
    // Wake collectors cap index by PENDING_WAKE_LIMIT before accessing.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return tasks[static_cast<size_t>(index)];
}

inline auto is_valid_kernel_stack(uint64_t rsp) -> bool { return rsp >= 0xffff800000000000ULL && rsp < 0xffff900000000000ULL; }

inline auto is_kernel_text_pointer(uint64_t rip) -> bool {
    auto const TEXT_START = reinterpret_cast<uint64_t>(__kernel_text_start);
    auto const TEXT_END = reinterpret_cast<uint64_t>(__kernel_text_end);
    return rip >= TEXT_START && rip < TEXT_END;
}

inline auto task_type_for_log(const task::Task* task) -> uint32_t {
    return task != nullptr ? static_cast<uint32_t>(task->type) : UINT32_MAX;
}

inline auto task_state_for_log(const task::Task* task) -> uint32_t {
    return task != nullptr ? static_cast<uint32_t>(task->state.load(std::memory_order_acquire)) : UINT32_MAX;
}

inline auto task_queue_for_log(const task::Task* task) -> int32_t { return task != nullptr ? static_cast<int32_t>(task->sched_queue) : -1; }

inline auto task_name_for_log(const task::Task* task) -> const char* {
    return (task != nullptr && task->name != nullptr) ? task->name : "?";
}

inline auto has_kernel_scheduler_context(const task::Task* task) -> bool {
    return task != nullptr && task->context.syscall_kernel_stack != 0 && task->context.syscall_scratch_area != 0;
}

inline auto is_publishable_idle_task(const task::Task* task) -> bool {
    return task != nullptr && task->type == task::TaskType::IDLE && task->pid == 0 && task->thread == nullptr &&
           task->pagemap == mm::virt::get_kernel_pagemap() && has_kernel_scheduler_context(task);
}

inline auto is_gc_protected_idle_task(const task::Task* task) -> bool {
    return task != nullptr && task->type == task::TaskType::IDLE && task->pid == 0;
}

inline auto is_dead_gc_candidate_task(const task::Task* task) -> bool { return task != nullptr && !is_gc_protected_idle_task(task); }

inline auto restore_gc_protected_idle_task(task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    bool const NEEDS_RESTORE = task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE ||
                               task->death_epoch.load(std::memory_order_acquire) != 0 || task->gc_queued.load(std::memory_order_acquire) ||
                               task->sched_queue == task::Task::sched_queue::DEAD_GC;
    if (!NEEDS_RESTORE) {
        return false;
    }

    task->state.store(task::TaskState::ACTIVE, std::memory_order_release);
    task->death_epoch.store(0, std::memory_order_release);
    task->gc_queued.store(false, std::memory_order_release);
    task->sched_queue = task::Task::sched_queue::NONE;
    return true;
}

inline auto is_publishable_runnable_task(const task::Task* task) -> bool {
    if (task == nullptr || task->type == task::TaskType::IDLE || !has_kernel_scheduler_context(task)) {
        return false;
    }

    task::TaskState const STATE = task->state.load(std::memory_order_acquire);
    if (STATE == task::TaskState::DEAD || STATE == task::TaskState::EXITING || task->gc_queued.load(std::memory_order_acquire)) {
        return false;
    }

    if (task->type == task::TaskType::PROCESS) {
        return task->pid != 0 && task->thread != nullptr && task->pagemap != nullptr;
    }

    if (task->type == task::TaskType::DAEMON) {
        return task->pid != 0 && task->pagemap == mm::virt::get_kernel_pagemap();
    }

    return false;
}

void log_rejected_task_publication(const char* reason, uint64_t cpu_no, const task::Task* task) {
    dbg::logger<"sched">::error(
        "rejecting task publication: reason=%s cpu=%lu task=%p name=%s pid=%lu type=%u state=%u queue=%d stack=0x%lx scratch=0x%lx "
        "thread=%p pagemap=%p",
        reason != nullptr ? reason : "?", cpu_no, static_cast<const void*>(task), task_name_for_log(task), task != nullptr ? task->pid : 0,
        task_type_for_log(task), task_state_for_log(task), task_queue_for_log(task),
        task != nullptr ? task->context.syscall_kernel_stack : 0, task != nullptr ? task->context.syscall_scratch_area : 0,
        task != nullptr ? static_cast<void*>(task->thread) : nullptr, task != nullptr ? static_cast<void*>(task->pagemap) : nullptr);
}

inline void validate_kernel_resume_target(task::Task* task, const char* path) {
    if (task == nullptr) {
        return;
    }
    if (task->context.frame.cs == desc::gdt::GDT_USER_CS && task->type == task::TaskType::PROCESS) {
        return;
    }

    const uint64_t RIP = task->context.frame.rip;
    const uint64_t RSP = task->context.frame.rsp;
    const bool CS_BAD = task->context.frame.cs != desc::gdt::GDT_KERN_CS;
    const bool SS_BAD = task->context.frame.ss != desc::gdt::GDT_KERN_DS;
    const bool RIP_BAD = !is_kernel_text_pointer(RIP);
    const bool RSP_BAD = !is_valid_kernel_stack(RSP);
    const bool FLAGS_BAD = (task->context.frame.flags & 0x2ULL) == 0;
    if (!CS_BAD && !SS_BAD && !RIP_BAD && !RSP_BAD && !FLAGS_BAD) {
        return;
    }

    resume_log::error(
        "kernel resume anomaly: path=%s pid=%lu name=%s cs=0x%llx ss=0x%llx rip=0x%llx rsp=0x%llx flags=0x%llx entry=0x%llx kentry=%p "
        "scratch=%p",
        path != nullptr ? path : "?", task->pid, task->name != nullptr ? task->name : "?",
        static_cast<unsigned long long>(task->context.frame.cs), static_cast<unsigned long long>(task->context.frame.ss),
        static_cast<unsigned long long>(RIP), static_cast<unsigned long long>(RSP),
        static_cast<unsigned long long>(task->context.frame.flags), static_cast<unsigned long long>(task->entry),
        reinterpret_cast<void*>(task->kthread_entry), reinterpret_cast<void*>(task->context.syscall_scratch_area));
    hcf();
}

inline void prepare_first_run_daemon(task::Task* task, const char* path) {
    if (task == nullptr || task->type != task::TaskType::DAEMON || task->has_run) {
        return;
    }

    if (task->kthread_entry == nullptr) {
        resume_log::error("first-run daemon without entry: path=%s pid=%lu name=%s", path != nullptr ? path : "?", task->pid,
                          task->name != nullptr ? task->name : "?");
        hcf();
    }

    uint64_t const STACK = task->context.syscall_kernel_stack;
    if (!is_valid_kernel_stack(STACK)) {
        resume_log::error("first-run daemon bad syscall stack: path=%s pid=%lu name=%s stack=0x%llx frame_rsp=0x%llx",
                          path != nullptr ? path : "?", task->pid, task->name != nullptr ? task->name : "?",
                          static_cast<unsigned long long>(STACK), static_cast<unsigned long long>(task->context.frame.rsp));
        hcf();
    }

    if (!is_valid_kernel_stack(task->context.frame.rsp)) {
        resume_log::warn("first-run daemon stack repair: path=%s pid=%lu name=%s old_rsp=0x%llx stack=0x%llx entry=%p",
                         path != nullptr ? path : "?", task->pid, task->name != nullptr ? task->name : "?",
                         static_cast<unsigned long long>(task->context.frame.rsp), static_cast<unsigned long long>(STACK),
                         reinterpret_cast<void*>(task->kthread_entry));
    }

    task->context.regs = cpu::GPRegs();
    task->context.regs.rdi = reinterpret_cast<uint64_t>(task->kthread_entry);
    task->context.frame.rip = reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline);
    task->context.frame.cs = desc::gdt::GDT_KERN_CS;
    task->context.frame.ss = desc::gdt::GDT_KERN_DS;
    task->context.frame.flags = 0x202;
    task->context.frame.rsp = STACK;
    task->context.frame.int_num = 0;
    task->context.frame.err_code = 0;
}

inline void record_local_proc_first_run(task::Task* task, uint64_t callsite) {
    if (task == nullptr || task->type != task::TaskType::PROCESS || task->has_run) {
        return;
    }

    perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), task->pid, perf::WkiPerfScope::LOCAL_PROC,
                           static_cast<uint8_t>(perf::WkiPerfLocalProcOp::FIRST_RUN), perf::WkiPerfPhase::POINT, 0, 0,
                           perf::next_wki_trace_correlation(), 0, static_cast<uint32_t>(cpu::current_cpu()), callsite);
}

inline void validate_user_resume_target(task::Task* task, const char* path) {
    if (task == nullptr || task->pagemap == nullptr || task->type != task::TaskType::PROCESS || task->is_voluntary_blocked()) {
        return;
    }

    const uint64_t RIP = task->context.frame.rip;
    const uint64_t RSP = task->context.frame.rsp;
    const uint64_t RIP_PHYS = mm::virt::translate(task->pagemap, RIP);
    const uint64_t RSP_PHYS = mm::virt::translate(task->pagemap, RSP);
    const bool RIP_BAD = RIP == 0 || RIP >= 0x0000800000000000ULL || RIP_PHYS == mm::virt::PADDR_INVALID;
    const bool RSP_BAD = RSP == 0 || RSP >= 0x0000800000000000ULL || RSP_PHYS == mm::virt::PADDR_INVALID;
    if (!RIP_BAD && !RSP_BAD) {
        return;
    }

    resume_log::warn(
        "resume anomaly: path=%s pid=%lu pagemap=%p rip=0x%llx rip_phys=0x%llx rsp=0x%llx rsp_phys=0x%llx cs=0x%llx ss=0x%llx "
        "entry=0x%llx thread=%p scratch=%p",
        path != nullptr ? path : "?", task->pid, static_cast<void*>(task->pagemap), static_cast<unsigned long long>(RIP),
        static_cast<unsigned long long>((RIP_PHYS == mm::virt::PADDR_INVALID) ? 0 : RIP_PHYS), static_cast<unsigned long long>(RSP),
        static_cast<unsigned long long>((RSP_PHYS == mm::virt::PADDR_INVALID) ? 0 : RSP_PHYS),
        static_cast<unsigned long long>(task->context.frame.cs), static_cast<unsigned long long>(task->context.frame.ss),
        static_cast<unsigned long long>(task->entry), static_cast<void*>(task->thread),
        reinterpret_cast<void*>(task->context.syscall_scratch_area));
}

inline void validate_wait_resume_mapping(task::Task* waiter, task::Task* child, const char* path) {
    if (waiter == nullptr || waiter->pagemap == nullptr) {
        return;
    }

    if (waiter->wait_resume_rip_user_addr != 0) {
        uint64_t const RIP_PHYS = mm::virt::translate(waiter->pagemap, waiter->wait_resume_rip_user_addr);
        if (RIP_PHYS == mm::virt::PADDR_INVALID || RIP_PHYS == 0 ||
            (waiter->wait_resume_rip_phys_addr != 0 && waiter->wait_resume_rip_phys_addr != RIP_PHYS)) {
            wait_log::warn(
                "waitpid-resume drift: waiter=%lu child=%lu path=%s rip_va=0x%llx old_phys=0x%llx new_phys=0x%llx rsp_va=0x%llx pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                static_cast<unsigned long long>(waiter->wait_resume_rip_user_addr),
                static_cast<unsigned long long>(waiter->wait_resume_rip_phys_addr),
                static_cast<unsigned long long>((RIP_PHYS == mm::virt::PADDR_INVALID) ? 0 : RIP_PHYS),
                static_cast<unsigned long long>(waiter->wait_resume_rsp_user_addr), static_cast<void*>(waiter->pagemap));
        }
        waiter->wait_resume_rip_phys_addr = (RIP_PHYS != mm::virt::PADDR_INVALID) ? RIP_PHYS : 0;
    }

    if (waiter->wait_resume_rsp_user_addr != 0) {
        uint64_t const RSP_PHYS = mm::virt::translate(waiter->pagemap, waiter->wait_resume_rsp_user_addr);
        if (RSP_PHYS == mm::virt::PADDR_INVALID || RSP_PHYS == 0) {
            wait_log::warn(
                "waitpid-stack unmapped: waiter=%lu child=%lu path=%s rsp_va=0x%llx old_phys=0x%llx new_phys=0x%llx rip_va=0x%llx "
                "pagemap=%p",
                waiter->pid, child != nullptr ? child->pid : 0, path != nullptr ? path : "?",
                static_cast<unsigned long long>(waiter->wait_resume_rsp_user_addr),
                static_cast<unsigned long long>(waiter->wait_resume_rsp_phys_addr),
                static_cast<unsigned long long>((RSP_PHYS == mm::virt::PADDR_INVALID) ? 0 : RSP_PHYS),
                static_cast<unsigned long long>(waiter->wait_resume_rip_user_addr), static_cast<void*>(waiter->pagemap));
        }
        waiter->wait_resume_rsp_phys_addr = (RSP_PHYS != mm::virt::PADDR_INVALID) ? RSP_PHYS : 0;
    }
}
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::array<PidHashEntry, MAX_PIDS> pid_table = {PidHashEntry{.pid = 0, .task = nullptr}};
std::atomic<uint64_t> scheduler_task_context_ready_mask{0};

constexpr std::array<uint32_t, 40> K_NICE_TO_WEIGHT = {
    88761, 71755, 56483, 46273, 36291, 29154, 23254, 18705, 14949, 11916, 9548, 7620, 6100, 4904, 3906, 3121, 2501, 1991, 1586, 1277,
    1024,  820,   655,   526,   423,   335,   272,   215,   172,   137,   110,  87,   70,   56,   45,   36,   29,   23,   18,   15,
};

constexpr int SCHED_NICE_MIN = -20;
constexpr int SCHED_NICE_MAX = 19;
constexpr uint64_t LOADAVG_SCALE = 1000000;
constexpr uint64_t LOADAVG_EXP_1MIN = 983471;
constexpr uint64_t LOADAVG_EXP_5MIN = 996672;
constexpr uint64_t LOADAVG_EXP_15MIN = 998889;

ker::mod::sys::Spinlock load_average_lock;
uint64_t load1_milli = 0;
uint64_t load5_milli = 0;
uint64_t load15_milli = 0;
uint64_t load_average_last_update_us = 0;

inline auto clamp_nice(int nice) -> int {
    if (nice < SCHED_NICE_MIN) {
        return SCHED_NICE_MIN;
    }
    if (nice > SCHED_NICE_MAX) {
        return SCHED_NICE_MAX;
    }
    return nice;
}

inline auto nice_weight_slot(size_t index) -> const uint32_t& {
    // Index is derived from clamp_nice(), keeping it within K_NICE_TO_WEIGHT.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return K_NICE_TO_WEIGHT[index];
}

inline auto nice_to_weight(int nice) -> uint32_t {
    auto const INDEX = static_cast<size_t>(clamp_nice(nice) - SCHED_NICE_MIN);
    return nice_weight_slot(INDEX);
}

inline bool has_short_burst_history(const task::Task* task) { return task->last_run_us >= 250U && task->last_run_us <= 8'000U; }

inline bool has_short_sleep_history(const task::Task* task) { return task->last_sleep_us != 0 && task->last_sleep_us <= 20'000U; }

inline bool has_recent_sleep_window(const task::Task* task, uint64_t now_us) {
    return task->last_sleep_start_us != 0 && now_us > task->last_sleep_start_us && (now_us - task->last_sleep_start_us) <= 20'000ULL;
}

inline uint32_t observed_sleep_us(const task::Task* task, uint64_t now_us) {
    if (task->last_sleep_start_us == 0 || now_us <= task->last_sleep_start_us) {
        return 0;
    }
    return static_cast<uint32_t>(std::min<uint64_t>(now_us - task->last_sleep_start_us, UINT32_MAX));
}

inline auto runtime_delta_ns_from_us(uint64_t delta_us) -> uint64_t {
    if (delta_us > UINT64_MAX / 1000ULL) {
        return UINT64_MAX;
    }
    return delta_us * 1000ULL;
}

inline auto vruntime_delta_from_runtime_ns(uint64_t delta_ns, uint32_t sched_weight) -> int64_t {
    uint64_t const WEIGHT = std::max<uint32_t>(sched_weight, 1U);
    uint64_t const WHOLE = delta_ns / WEIGHT;
    uint64_t const REMAINDER = delta_ns % WEIGHT;
    auto const MAX_I64 = static_cast<uint64_t>(INT64_MAX);
    uint64_t const SCALED_WHOLE = WHOLE > MAX_I64 / 1024ULL ? MAX_I64 : WHOLE * 1024ULL;
    uint64_t const SCALED_REMAINDER = (REMAINDER * 1024ULL) / WEIGHT;
    if (SCALED_WHOLE > MAX_I64 - SCALED_REMAINDER) {
        return INT64_MAX;
    }
    return static_cast<int64_t>(SCALED_WHOLE + SCALED_REMAINDER);
}

inline auto vdeadline_from_slice(int64_t vruntime, uint32_t slice_ns, uint32_t sched_weight) -> int64_t {
    int64_t const DELTA = vruntime_delta_from_runtime_ns(slice_ns, sched_weight);
    if (DELTA > 0 && vruntime > INT64_MAX - DELTA) {
        return INT64_MAX;
    }
    return vruntime + DELTA;
}

inline auto saturating_i64_mul(int64_t lhs, uint32_t rhs) -> int64_t {
    if (lhs <= 0 || rhs == 0) {
        return 0;
    }
    auto const RHS = static_cast<int64_t>(rhs);
    if (lhs > INT64_MAX / RHS) {
        return INT64_MAX;
    }
    return lhs * RHS;
}

inline void add_weighted_vruntime_delta(RunQueue* rq, int64_t vruntime_delta, uint32_t sched_weight) {
    int64_t const WEIGHTED_DELTA = saturating_i64_mul(vruntime_delta, sched_weight);
    if (rq->total_weighted_vruntime > INT64_MAX - WEIGHTED_DELTA) {
        rq->total_weighted_vruntime = INT64_MAX;
        return;
    }
    rq->total_weighted_vruntime += WEIGHTED_DELTA;
}

inline auto accumulate_slice_used_ns(uint32_t used_ns, uint64_t delta_ns, uint32_t slice_ns) -> uint32_t {
    if (slice_ns == 0) {
        return 0;
    }
    uint64_t const USED = used_ns;
    if (delta_ns >= static_cast<uint64_t>(slice_ns) || USED > static_cast<uint64_t>(slice_ns) - delta_ns) {
        return slice_ns;
    }
    return static_cast<uint32_t>(USED + delta_ns);
}

inline void note_perf_wait_callsite(task::Task* task, uint64_t fallback_rip) {
    if (task == nullptr) {
        return;
    }
    if (task->perf_wait_callsite == 0) {
        task->perf_wait_callsite = fallback_rip;
    }
}

inline uint64_t perf_wait_callsite(const task::Task* task) {
    if (task == nullptr) {
        return 0;
    }
    if (task->perf_wait_callsite != 0) {
        return task->perf_wait_callsite;
    }
    return task->context.frame.rip;
}

inline uint8_t perf_sleep_flags(uint64_t wake_at_us) { return perf::PERF_FLAG_BLOCK | (wake_at_us != 0 ? perf::PERF_FLAG_TIMED : 0U); }

inline uint8_t perf_wake_flags(uint64_t wake_at_us, bool explicit_wake, bool wake_current) {
    uint8_t flags = wake_at_us != 0 ? perf::PERF_FLAG_TIMED : 0U;
    if (explicit_wake) {
        flags |= perf::PERF_FLAG_EXPLICIT_WAKE;
    }
    if (wake_current) {
        flags |= perf::PERF_FLAG_WAKE_CURRENT;
    }
    return flags;
}

inline bool is_low_latency_handoff_wait_channel(task::WaitChannelKind wait_channel) {
    return wait_channel == task::WaitChannelKind::LOCAL_PIPE || wait_channel == task::WaitChannelKind::LOCAL_PTY ||
           wait_channel == task::WaitChannelKind::WAITPID;
}

inline bool is_futex_wait_channel(task::WaitChannelKind wait_channel) { return wait_channel == task::WaitChannelKind::FUTEX; }

inline bool is_waitpid_wait_channel(task::WaitChannelKind wait_channel) { return wait_channel == task::WaitChannelKind::WAITPID; }

inline auto effective_process_group_id(const task::Task& task) -> uint64_t { return task.pgid != 0 ? task.pgid : task::process_pid(task); }

inline constexpr auto WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
inline constexpr int WOS_WSTOPPED = 2;
inline constexpr int STOP_STATUS_LOW = 0x7f;
inline constexpr uint64_t SIGCHLD_MASK = 1ULL << (17 - 1);
inline constexpr uint64_t WAITPID_REPAIR_FALLBACK_MIN_US = 50'000ULL;

inline auto job_control_stop_status(const task::Task& task) -> int32_t {
    uint32_t const SIGNAL = task.jobctl_stop_signal != 0 ? task.jobctl_stop_signal : 19;
    return static_cast<int32_t>((SIGNAL << 8U) | STOP_STATUS_LOW);
}

inline auto active_waitpid_unwaited_child(task::Task* child, void* raw_waiter) -> bool;
inline auto active_waitpid_waitable_exit_child(task::Task* child, void* raw_waiter) -> bool;
inline auto active_waitpid_job_stop_child(task::Task* child, void* raw_waiter) -> bool;

inline auto waitpid_job_stop_waitable(const task::Task* waiter, const task::Task* target) -> bool {
    return waiter != nullptr && target != nullptr && (waiter->wait_options & WOS_WSTOPPED) != 0 && target->parent_pid == waiter->pid &&
           !target->is_thread && !target->has_exited && target->jobctl_stopped.load(std::memory_order_acquire) &&
           target->jobctl_stop_pending.load(std::memory_order_acquire) &&
           (waiter->waiting_for_pid == WAIT_ANY_CHILD || waiter->waiting_for_pid == target->pid);
}

inline auto complete_waitpid_job_stop_if_waitable(task::Task* waiter, task::Task* target) -> bool {
    if (waiter == nullptr || !task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (!waitpid_job_stop_waitable(waiter, target)) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }

    waiter->context.regs.rax = target->pid;
    if (waiter->wait_status_user_addr != 0 && waiter->pagemap != nullptr) {
        uint64_t const STATUS_PHYS = mm::virt::translate(waiter->pagemap, waiter->wait_status_user_addr);
        if (STATUS_PHYS != mm::virt::PADDR_INVALID && STATUS_PHYS != 0) {
            auto* status_ptr = reinterpret_cast<int32_t*>(mm::addr::get_virt_pointer(STATUS_PHYS));
            *status_ptr = job_control_stop_status(*target);
        }
    }
    target->jobctl_stop_pending.store(false, std::memory_order_release);
    waiter->waitpid_publish_pending.store(false, std::memory_order_release);
    task::task_clear_waitpid_block_state(*waiter);
    return true;
}

inline auto waitpid_has_job_stop_ready(task::Task* waiter) -> bool {
    if (waiter == nullptr || waiter->waiting_for_pid == 0 || (waiter->wait_options & WOS_WSTOPPED) == 0) {
        return false;
    }

    if (waiter->waiting_for_pid != WAIT_ANY_CHILD) {
        auto* target = find_task_by_pid(waiter->waiting_for_pid);
        return waitpid_job_stop_waitable(waiter, target);
    }

    auto* child = find_active_task_lifetime_ref_if(active_waitpid_job_stop_child, waiter);
    if (child != nullptr) {
        child->release();
        return true;
    }
    return false;
}

inline void interrupt_waitpid_block_for_signal(task::Task* task);

inline auto waitpid_child_matches_waiter(const task::Task* waiter, const task::Task* child) -> bool {
    if (waiter == nullptr || child == nullptr || child->is_thread) {
        return false;
    }
    if (waiter->waiting_for_pid != WAIT_ANY_CHILD && waiter->waiting_for_pid != child->pid) {
        return false;
    }
    return child->parent_pid == waiter->pid || (child->ptrace_traced && child->ptrace_tracer_pid == waiter->pid);
}

inline auto waitpid_child_is_unwaited(const task::Task* waiter, const task::Task* child) -> bool {
    return waitpid_child_matches_waiter(waiter, child) && !task::task_waited_on(*child);
}

inline auto waitpid_child_is_waitable_exit(const task::Task* child) -> bool {
    if (child == nullptr) {
        return false;
    }

    // exit_notify_ready publishes the normal waitable-exit point. A task that
    // has reached the final DEAD state is also waitable: preserving a waitpid
    // block on a dead-listed direct child can strand the parent forever.
    bool const EXIT_READY = child->exit_notify_ready.load(std::memory_order_acquire);
    task::TaskState const STATE = child->state.load(std::memory_order_acquire);
    return EXIT_READY || STATE == task::TaskState::DEAD;
}

inline auto waitpid_repair_due(const task::Task* waiter, uint64_t now_us) -> bool {
    if (waiter == nullptr || !waiter->wait_channel_is(task::WaitChannelKind::WAITPID) || waiter->waiting_for_pid == 0) {
        return false;
    }
    bool const SIGCHLD_PENDING = (waiter->sig_pending & SIGCHLD_MASK) != 0;
    if (SIGCHLD_PENDING) {
        return true;
    }
    uint64_t const LAST_REPAIR_US = waiter->waitpid_last_repair_us != 0 ? waiter->waitpid_last_repair_us : waiter->last_sleep_start_us;
    return LAST_REPAIR_US != 0 && now_us > LAST_REPAIR_US && now_us - LAST_REPAIR_US >= WAITPID_REPAIR_FALLBACK_MIN_US;
}

inline auto active_waitpid_unwaited_child(task::Task* child, void* raw_waiter) -> bool {
    return waitpid_child_is_unwaited(static_cast<task::Task*>(raw_waiter), child);
}

inline auto active_waitpid_waitable_exit_child(task::Task* child, void* raw_waiter) -> bool {
    auto* waiter = static_cast<task::Task*>(raw_waiter);
    return waitpid_child_is_unwaited(waiter, child) && waitpid_child_is_waitable_exit(child);
}

inline auto active_waitpid_job_stop_child(task::Task* child, void* raw_waiter) -> bool {
    return waitpid_job_stop_waitable(static_cast<task::Task*>(raw_waiter), child);
}

inline auto dead_waitpid_specific_target(task::Task* child, void* raw_waiter) -> bool {
    auto* waiter = static_cast<task::Task*>(raw_waiter);
    return waiter != nullptr && child != nullptr && waiter->waiting_for_pid != 0 && waiter->waiting_for_pid != WAIT_ANY_CHILD &&
           child->pid == waiter->waiting_for_pid;
}

inline auto registered_waitpid_matches_child(const task::Task* waiter, const task::Task* child) -> bool {
    if (waiter == nullptr || child == nullptr || child->is_thread || waiter->waiting_for_pid == 0) {
        return false;
    }
    if (!waitpid_child_matches_waiter(waiter, child)) {
        return false;
    }
    return waiter->waiting_for_pid == WAIT_ANY_CHILD || waiter->waiting_for_pid == child->pid;
}

__attribute__((no_sanitize("address"))) auto pid_table_find_lifetime_ref(uint64_t pid) -> task::Task*;
inline auto waitpid_has_registered_unwaited_child(task::Task* waiter) -> bool;
inline auto waitpid_specific_target_registered(task::Task* waiter, uint64_t pid) -> bool;

inline void clear_waitpid_output_addrs(task::Task* waiter) {
    if (waiter == nullptr) {
        return;
    }
    waiter->wait_status_user_addr = 0;
    waiter->wait_status_phys_addr = 0;
    waiter->wait_rusage_user_addr = 0;
    waiter->wait_rusage_phys_addr = 0;
}

inline void write_waitpid_status(task::Task* waiter, int32_t status) {
    if (waiter == nullptr || waiter->wait_status_user_addr == 0 || waiter->pagemap == nullptr) {
        return;
    }
    uint64_t const STATUS_PHYS = mm::virt::translate(waiter->pagemap, waiter->wait_status_user_addr);
    if (STATUS_PHYS == mm::virt::PADDR_INVALID || STATUS_PHYS == 0) {
        return;
    }
    auto* status_ptr = reinterpret_cast<int32_t*>(mm::addr::get_virt_pointer(STATUS_PHYS));
    *status_ptr = status;
}

inline void fill_waitpid_rusage(task::Task* waiter, const task::Task* child) {
    if (waiter == nullptr || child == nullptr || waiter->wait_rusage_user_addr == 0 || waiter->pagemap == nullptr) {
        return;
    }
    uint64_t const RUSAGE_PHYS = mm::virt::translate(waiter->pagemap, waiter->wait_rusage_user_addr);
    if (RUSAGE_PHYS == mm::virt::PADDR_INVALID || RUSAGE_PHYS == 0) {
        return;
    }
    auto* ru = reinterpret_cast<syscall::process::KernRusage*>(mm::addr::get_virt_pointer(RUSAGE_PHYS));
    uint64_t const USER_TIME_US = task::task_rusage_user_time_us(*child);
    uint64_t const SYSTEM_TIME_US = task::task_rusage_system_time_us(*child);
    ru->ru_utime_sec = static_cast<int64_t>(USER_TIME_US / 1000000ULL);
    ru->ru_utime_usec = static_cast<int64_t>(USER_TIME_US % 1000000ULL);
    ru->ru_stime_sec = static_cast<int64_t>(SYSTEM_TIME_US / 1000000ULL);
    ru->ru_stime_usec = static_cast<int64_t>(SYSTEM_TIME_US % 1000000ULL);
}

inline void finish_waitpid_scheduler_result(task::Task* waiter) {
    if (waiter == nullptr) {
        return;
    }
    waiter->waitpid_publish_pending.store(false, std::memory_order_release);
    waiter->deferred_task_switch = false;
    waiter->set_voluntary_blocked(false);
    waiter->wants_block = false;
    waiter->wake_at_us = 0;
    task::task_clear_waitpid_block_state(*waiter);
}

inline auto complete_waitpid_exit_for_scheduler(task::Task* waiter, task::Task* child, const char* path) -> bool {
    if (waiter == nullptr || !task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (!waitpid_child_is_unwaited(waiter, child) || !waitpid_child_is_waitable_exit(child)) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    if (!task::task_try_mark_waited_on(*child)) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    task::task_accumulate_waited_child_times(*waiter, *child);
    waiter->context.regs.rax = child->pid;
    validate_wait_resume_mapping(waiter, child, path);
    write_waitpid_status(waiter, child->exit_status);
    fill_waitpid_rusage(waiter, child);
    clear_waitpid_output_addrs(waiter);
    finish_waitpid_scheduler_result(waiter);
    return true;
}

inline auto complete_registered_waitpid_exit_for_scheduler(task::Task* waiter, task::Task* child, const char* path) -> bool {
    if (waiter == nullptr || !task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (!registered_waitpid_matches_child(waiter, child) || task::task_waited_on(*child) || !waitpid_child_is_waitable_exit(child)) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    if (!task::task_try_mark_waited_on(*child)) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    task::task_accumulate_waited_child_times(*waiter, *child);
    waiter->context.regs.rax = child->pid;
    validate_wait_resume_mapping(waiter, child, path);
    write_waitpid_status(waiter, child->exit_status);
    fill_waitpid_rusage(waiter, child);
    clear_waitpid_output_addrs(waiter);
    finish_waitpid_scheduler_result(waiter);
    return true;
}

inline auto complete_waitpid_ptrace_stop_for_scheduler(task::Task* waiter, task::Task* target) -> bool {
    if (waiter == nullptr || !task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (target == nullptr || !target->ptrace_traced || target->ptrace_tracer_pid != waiter->pid || !target->ptrace_stopped ||
        !target->ptrace_stop_pending) {
        task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }

    uint32_t signal = target->ptrace_stop_signal != 0 ? target->ptrace_stop_signal : 5;
    if ((target->ptrace_stop_reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER ||
         target->ptrace_stop_reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) &&
        (target->ptrace_options & 0x00000001U) != 0) {
        signal |= 0x80U;
    }

    waiter->context.regs.rax = target->pid;
    write_waitpid_status(waiter, static_cast<int32_t>((signal << 8U) | STOP_STATUS_LOW));
    target->ptrace_stop_pending = false;
    clear_waitpid_output_addrs(waiter);
    finish_waitpid_scheduler_result(waiter);
    return true;
}

inline auto waitpid_has_unwaited_child_for_scheduler(task::Task* waiter) -> bool {
    if (waiter == nullptr) {
        return false;
    }

    auto* active_child = find_active_task_lifetime_ref_if(active_waitpid_unwaited_child, waiter);
    if (active_child != nullptr) {
        active_child->release();
        return true;
    }

    auto* dead_child = find_dead_task_lifetime_ref_if(active_waitpid_unwaited_child, waiter);
    if (dead_child != nullptr) {
        dead_child->release();
        return true;
    }
    return false;
}

inline auto complete_waitpid_any_for_scheduler(task::Task* waiter) -> bool {
    for (;;) {
        auto* child = find_active_task_lifetime_ref_if(active_waitpid_waitable_exit_child, waiter);
        if (child == nullptr) {
            break;
        }
        bool const COMPLETED = complete_waitpid_exit_for_scheduler(waiter, child, "wake-any-active");
        bool const CLAIM_IN_PROGRESS =
            !COMPLETED && waiter->waiting_for_pid != 0 && waiter->waitpid_completion_claimed.load(std::memory_order_acquire);
        child->release();
        if (COMPLETED) {
            return true;
        }
        if (CLAIM_IN_PROGRESS) {
            return false;
        }
    }

    for (;;) {
        auto* child = find_dead_task_lifetime_ref_if(active_waitpid_waitable_exit_child, waiter);
        if (child == nullptr) {
            break;
        }
        bool const COMPLETED = complete_waitpid_exit_for_scheduler(waiter, child, "wake-any-dead");
        bool const WAITER_DONE = waiter->waiting_for_pid == 0;
        bool const CLAIM_IN_PROGRESS = !COMPLETED && !WAITER_DONE && waiter->waitpid_completion_claimed.load(std::memory_order_acquire);
        child->release();
        if (COMPLETED || WAITER_DONE) {
            return true;
        }
        if (CLAIM_IN_PROGRESS) {
            return false;
        }
    }

    if (!waitpid_has_unwaited_child_for_scheduler(waiter) && !waitpid_has_registered_unwaited_child(waiter)) {
        if (!task::task_try_claim_waitpid_completion(*waiter)) {
            return waiter->waiting_for_pid == 0;
        }
        if (waiter->waiting_for_pid != WAIT_ANY_CHILD || waitpid_has_unwaited_child_for_scheduler(waiter) ||
            waitpid_has_registered_unwaited_child(waiter)) {
            task::task_release_waitpid_completion_claim(*waiter);
            return waiter->waiting_for_pid == 0;
        }
        waiter->context.regs.rax = static_cast<uint64_t>(-ECHILD);
        clear_waitpid_output_addrs(waiter);
        finish_waitpid_scheduler_result(waiter);
        return true;
    }
    return false;
}

inline auto complete_waitpid_specific_for_scheduler(task::Task* waiter) -> bool {
    uint64_t const WAIT_TARGET = waiter != nullptr ? waiter->waiting_for_pid : 0;
    if (WAIT_TARGET == 0 || WAIT_TARGET == WAIT_ANY_CHILD) {
        return false;
    }

    auto complete_missing_child = [waiter, WAIT_TARGET]() -> bool {
        if (!task::task_try_claim_waitpid_completion(*waiter)) {
            return waiter->waiting_for_pid == 0;
        }
        if (waiter->waiting_for_pid != WAIT_TARGET) {
            task::task_release_waitpid_completion_claim(*waiter);
            return waiter->waiting_for_pid == 0;
        }
        waiter->context.regs.rax = static_cast<uint64_t>(-ECHILD);
        clear_waitpid_output_addrs(waiter);
        finish_waitpid_scheduler_result(waiter);
        return true;
    };

    if (auto* target = find_task_by_pid_safe(WAIT_TARGET); target != nullptr) {
        if (complete_registered_waitpid_exit_for_scheduler(waiter, target, "wake-specific-registered-active")) {
            target->release();
            return true;
        }
        if (!waitpid_child_matches_waiter(waiter, target) || task::task_waited_on(*target)) {
            target->release();
            return complete_missing_child();
        }
        if (complete_waitpid_ptrace_stop_for_scheduler(waiter, target) ||
            complete_waitpid_exit_for_scheduler(waiter, target, "wake-specific-active") ||
            complete_waitpid_job_stop_if_waitable(waiter, target)) {
            target->release();
            return true;
        }
        target->release();
        return false;
    }

    if (auto* target = pid_table_find_lifetime_ref(WAIT_TARGET); target != nullptr) {
        if (complete_registered_waitpid_exit_for_scheduler(waiter, target, "wake-specific-registered-lifetime")) {
            target->release();
            return true;
        }
        if (!waitpid_child_matches_waiter(waiter, target) || task::task_waited_on(*target)) {
            target->release();
            return complete_missing_child();
        }
        if (complete_waitpid_exit_for_scheduler(waiter, target, "wake-specific-lifetime")) {
            target->release();
            return true;
        }
        target->release();
        return false;
    }

    auto* dead_target = find_dead_task_lifetime_ref_if(dead_waitpid_specific_target, waiter);
    if (dead_target != nullptr) {
        bool const MATCH = waitpid_child_matches_waiter(waiter, dead_target);
        bool const COMPLETED = complete_registered_waitpid_exit_for_scheduler(waiter, dead_target, "wake-specific-registered-dead") ||
                               complete_waitpid_exit_for_scheduler(waiter, dead_target, "wake-specific-dead");
        bool const ALREADY_WAITED = task::task_waited_on(*dead_target);
        bool const WAITER_DONE = waiter->waiting_for_pid == 0;
        bool const CLAIM_IN_PROGRESS = !COMPLETED && !WAITER_DONE && waiter->waitpid_completion_claimed.load(std::memory_order_acquire);
        dead_target->release();
        if (COMPLETED || WAITER_DONE) {
            return true;
        }
        if (CLAIM_IN_PROGRESS) {
            return false;
        }
        if (!MATCH || ALREADY_WAITED) {
            return complete_missing_child();
        }
        return false;
    }

    if (waitpid_specific_target_registered(waiter, WAIT_TARGET)) {
        return false;
    }

    return complete_missing_child();
}

inline auto complete_or_preserve_waitpid_block(task::Task* waiter) -> bool {
    if (waiter == nullptr || !waiter->wait_channel_is(task::WaitChannelKind::WAITPID) || waiter->waiting_for_pid == 0) {
        return true;
    }
    if (waiter->waitpid_completion_claimed.load(std::memory_order_acquire)) {
        return waiter->waiting_for_pid == 0;
    }

    if (waitpid_has_job_stop_ready(waiter)) {
        if (waiter->waiting_for_pid == WAIT_ANY_CHILD) {
            for (;;) {
                auto* child = find_active_task_lifetime_ref_if(active_waitpid_job_stop_child, waiter);
                if (child == nullptr) {
                    break;
                }
                bool const COMPLETED = complete_waitpid_job_stop_if_waitable(waiter, child);
                child->release();
                if (COMPLETED) {
                    return true;
                }
            }
        } else if (auto* target = find_task_by_pid_safe(waiter->waiting_for_pid); target != nullptr) {
            bool const COMPLETED = complete_waitpid_job_stop_if_waitable(waiter, target);
            target->release();
            if (COMPLETED) {
                return true;
            }
        }
    }

    if (waiter->has_interrupting_signal_pending()) {
        interrupt_waitpid_block_for_signal(waiter);
        return true;
    }

    if (waiter->waiting_for_pid == WAIT_ANY_CHILD) {
        return complete_waitpid_any_for_scheduler(waiter);
    }
    return complete_waitpid_specific_for_scheduler(waiter);
}

inline void complete_parent_waitpid_after_dead_enqueue(task::Task* child) {
    if (child == nullptr || child->is_thread) {
        return;
    }

    auto nudge_waiter = [child](task::Task* waiter, const char* path) -> bool {
        if (waiter == nullptr || waiter->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE ||
            !waiter->wait_channel_is(task::WaitChannelKind::WAITPID) || waiter->waiting_for_pid == 0 ||
            !registered_waitpid_matches_child(waiter, child)) {
            return false;
        }

        if (waiter->sched_queue == task::Task::sched_queue::WAITING && !waiter->deferred_task_switch) {
            (void)complete_registered_waitpid_exit_for_scheduler(waiter, child, path);
        }

        uint64_t target_cpu = waiter->cpu;
        if (target_cpu >= smt::get_core_count()) {
            target_cpu = get_least_loaded_cpu();
        }
        reschedule_task_for_cpu(target_cpu, waiter);
        return true;
    };

    if (child->parent_pid != 0) {
        auto* parent = find_task_by_pid_safe(child->parent_pid);
        if (parent != nullptr) {
            bool const NUDGED = nudge_waiter(parent, "dead-list-parent-waitpid");
            parent->release();
            if (NUDGED) {
                return;
            }
        }
    }

    uint32_t const ACTIVE_COUNT = get_active_task_count();
    for (uint32_t i = 0; i < ACTIVE_COUNT; ++i) {
        auto* waiter = get_active_task_at_safe(i);
        if (waiter == nullptr) {
            continue;
        }
        bool const NUDGED = nudge_waiter(waiter, "dead-list-registered-waitpid");
        waiter->release();
        if (NUDGED) {
            return;
        }
    }
}

inline void unlink_specific_waitpid_waiter(task::Task* waiter) {
    if (waiter == nullptr) {
        return;
    }

    uint64_t const WAIT_TARGET = waiter->waiting_for_pid;
    if (WAIT_TARGET == 0 || WAIT_TARGET == WAIT_ANY_CHILD) {
        return;
    }

    auto* target = find_task_by_pid_safe(WAIT_TARGET);
    if (target == nullptr) {
        return;
    }

    uint64_t const WAITER_LOCK_FLAGS = target->exit_waiters_lock.lock_irqsave();
    (void)target->awaitee_on_exit.remove(waiter->pid);
    target->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);
    target->release();
}

inline void interrupt_waitpid_block_for_signal(task::Task* task) {
    if (task == nullptr) {
        return;
    }

    task->context.regs.rax = static_cast<uint64_t>(-EINTR);
    unlink_specific_waitpid_waiter(task);
    task::task_clear_waitpid_block_state(*task);
}

inline void note_task_wakeup(task::Task* task, uint64_t now_us, task::WaitChannelKind wait_channel) {
    if (task->last_sleep_start_us != 0 && now_us > task->last_sleep_start_us) {
        uint64_t const SLEEP_US = now_us - task->last_sleep_start_us;
        task->last_sleep_us = static_cast<uint32_t>(std::min<uint64_t>(SLEEP_US, UINT32_MAX));
    } else {
        task->last_sleep_us = 0;
    }
    task->last_wake_us = is_low_latency_handoff_wait_channel(wait_channel) ? 0 : now_us;
    task->last_sleep_start_us = 0;
    task->perf_wait_callsite = 0;
}

inline bool is_bursty_wakeup_contender(const task::Task* task, uint64_t now_us) {
    return task->last_wake_us != 0 && now_us >= task->last_wake_us && (now_us - task->last_wake_us) <= 20'000ULL &&
           has_short_burst_history(task) && has_short_sleep_history(task);
}

inline bool is_bursty_voluntary_block_contender(const task::Task* task, uint64_t now_us) {
    return task->is_voluntary_blocked() && has_short_burst_history(task) && has_recent_sleep_window(task, now_us);
}

inline bool is_lower_weight_contender(const task::Task* current, const task::Task* contender) {
    return contender->sched_weight < current->sched_weight;
}

inline bool is_higher_priority_process_contender(const task::Task* current, const task::Task* contender) {
    return current != nullptr && contender != nullptr && current->type == task::TaskType::PROCESS &&
           contender->type == task::TaskType::PROCESS && contender->sched_nice < current->sched_nice;
}

inline uint64_t compute_lower_weight_preempt_guard_us(const task::Task* current, const task::Task* contender) {
    static constexpr uint64_t SCHED_LOWER_WEIGHT_PREEMPT_BASE_US = 12000ULL;
    static constexpr uint64_t SCHED_LOWER_WEIGHT_PREEMPT_STEP_US = 4000ULL;
    static constexpr uint64_t SCHED_LOWER_WEIGHT_PREEMPT_MAX_US = 48000ULL;

    uint64_t const CONTENDER_WEIGHT = std::max<uint64_t>(1ULL, static_cast<uint64_t>(contender->sched_weight));
    uint64_t weight_ratio = static_cast<uint64_t>(current->sched_weight) / CONTENDER_WEIGHT;
    weight_ratio = std::max<uint64_t>(weight_ratio, 1ULL);
    uint64_t const SCALED_GUARD = SCHED_LOWER_WEIGHT_PREEMPT_BASE_US + ((weight_ratio - 1ULL) * SCHED_LOWER_WEIGHT_PREEMPT_STEP_US);
    return std::min<uint64_t>(SCALED_GUARD, SCHED_LOWER_WEIGHT_PREEMPT_MAX_US);
}

inline uint64_t compute_bursty_preempt_guard_us(const task::Task* task) {
    static constexpr uint64_t SCHED_BURSTY_PREEMPT_BASE_US = 24'000ULL;
    static constexpr uint64_t SCHED_BURSTY_PREEMPT_MAX_US = 40'000ULL;
    uint64_t const SCALED_GUARD = static_cast<uint64_t>(task->last_run_us) * 4ULL;
    return std::clamp<uint64_t>(SCALED_GUARD, SCHED_BURSTY_PREEMPT_BASE_US, SCHED_BURSTY_PREEMPT_MAX_US);
}

// Active PID tracking for fast process-group and wait-any iteration (avoids scanning 16M-entry hash table).
// Large self-host builds can keep thousands of waited zombies visible until the GC worker catches up.
constexpr uint32_t MAX_ACTIVE_TASKS = 65536;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::array<task::Task*, MAX_ACTIVE_TASKS> active_task_list = {nullptr};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t active_task_count = 0;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> kernel_thread_shutdown_requested{false};

ker::mod::sys::Spinlock global_task_registry_lock;

inline auto active_task_slot(uint32_t index) -> task::Task*& {
    // Callers hold global_task_registry_lock and bound index by active_task_count.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return active_task_list[static_cast<size_t>(index)];
}

auto is_kernel_thread_shutdown_target(task::Task* task, task::Task* current) -> bool {
    return task != nullptr && task != current && task->type == task::TaskType::DAEMON &&
           task->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !task->has_exited && task->kthread_entry != nullptr &&
           task->context.syscall_kernel_stack != 0;
}

auto count_kernel_thread_shutdown_targets(task::Task* current) -> size_t {
    size_t remaining = 0;
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* task = get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (is_kernel_thread_shutdown_target(task, current)) {
            ++remaining;
        }
        task->release();
    }
    return remaining;
}

inline auto pid_slot(uint32_t index) -> PidHashEntry& {
    // PID indexes are masked with MAX_PIDS - 1 before reaching this helper.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return pid_table[static_cast<size_t>(index)];
}

auto active_list_insert(task::Task* t) -> bool {
    if (t == nullptr) {
        return false;
    }
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    for (uint32_t i = 0; i < active_task_count; ++i) {
        auto* existing = active_task_slot(i);
        if (existing == t || (existing != nullptr && existing->pid == t->pid)) {
            active_task_slot(i) = t;
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    if (active_task_count < MAX_ACTIVE_TASKS) {
        active_task_slot(active_task_count++) = t;
        global_task_registry_lock.unlock_irqrestore(FLAGS);
        return true;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return false;
}

void active_list_remove(uint64_t pid) {
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t i = 0;
    while (i < active_task_count) {
        if (active_task_slot(i) != nullptr && active_task_slot(i)->pid == pid) {
            active_task_slot(i) = active_task_slot(--active_task_count);
            active_task_slot(active_task_count) = nullptr;
            continue;
        }
        ++i;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
}

inline auto pid_hash(uint64_t pid) -> uint32_t {
    // Knuth multiplicative hash - spreads sequential PIDs well
    // NOLINTBEGIN(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)
    return static_cast<uint32_t>((pid * 11400714819323198485ULL) >> 40) & (MAX_PIDS - 1);
    // NOLINTEND(readability-magic-numbers, cppcoreguidelines-avoid-magic-numbers)
}

auto pid_table_insert(task::Task* t) -> bool {
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const SLOT = pid_hash(t->pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t const IDX = (SLOT + i) & (MAX_PIDS - 1);
        auto& entry = pid_slot(IDX);
        if (entry.pid == 0) {
            entry.pid = t->pid;
            entry.task = t;
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return true;
        }
        if (entry.pid == t->pid) {
            // Slot already taken by same PID - update pointer (shouldn't happen)
            entry.task = t;
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return true;
        }
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return false;  // Table full - MAX_PIDS concurrent processes exceeded
}

inline auto waitpid_has_registered_unwaited_child(task::Task* waiter) -> bool {
    if (waiter == nullptr) {
        return false;
    }

    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    for (uint32_t i = 0; i < active_task_count; ++i) {
        auto* child = active_task_slot(i);
        if (waitpid_child_is_unwaited(waiter, child)) {
            if (child != nullptr && child->try_acquire_lifetime_ref()) {
                child->release();
                global_task_registry_lock.unlock_irqrestore(FLAGS);
                return true;
            }
            continue;
        }
        if (waitpid_job_stop_waitable(waiter, child)) {
            if (child != nullptr && child->try_acquire_lifetime_ref()) {
                child->release();
                global_task_registry_lock.unlock_irqrestore(FLAGS);
                return true;
            }
            continue;
        }
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return false;
}

inline auto waitpid_specific_target_registered(task::Task* waiter, uint64_t pid) -> bool {
    if (waiter == nullptr || pid == 0) {
        return false;
    }

    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const SLOT = pid_hash(pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t const IDX = (SLOT + i) & (MAX_PIDS - 1);
        auto& entry = pid_slot(IDX);
        if (entry.pid == 0) {
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return false;
        }
        if (entry.pid != pid) {
            continue;
        }

        bool const PRESENT =
            waitpid_child_is_unwaited(waiter, entry.task) && entry.task != nullptr && entry.task->try_acquire_lifetime_ref();
        if (PRESENT) {
            entry.task->release();
        }
        global_task_registry_lock.unlock_irqrestore(FLAGS);
        return PRESENT;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return false;
}

__attribute__((no_sanitize("address"))) auto pid_table_find_internal(uint64_t pid, bool acquire) -> task::Task* {
    if (pid == 0) {
        return nullptr;
    }
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const SLOT = pid_hash(pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t const IDX = (SLOT + i) & (MAX_PIDS - 1);
        auto& entry = pid_slot(IDX);
        if (entry.pid == 0) {
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return nullptr;  // Empty slot - not found
        }
        if (entry.pid == pid) {
            task::Task* t = entry.task;
            if (acquire) {
                if (!t->try_acquire()) {
                    t = nullptr;
                }
            }
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return t;
        }
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return nullptr;
}

__attribute__((no_sanitize("address"))) auto pid_table_find(uint64_t pid) -> task::Task* { return pid_table_find_internal(pid, false); }

__attribute__((no_sanitize("address"))) auto pid_table_find_lifetime_ref(uint64_t pid) -> task::Task* {
    if (pid == 0) {
        return nullptr;
    }
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const SLOT = pid_hash(pid);
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        uint32_t const IDX = (SLOT + i) & (MAX_PIDS - 1);
        auto& entry = pid_slot(IDX);
        if (entry.pid == 0) {
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return nullptr;
        }
        if (entry.pid == pid) {
            task::Task* t = entry.task;
            if (t != nullptr && !t->try_acquire_lifetime_ref()) {
                t = nullptr;
            }
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return t;
        }
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return nullptr;
}

void pid_table_remove(uint64_t pid) {
    if (pid == 0) {
        return;
    }
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const SLOT = pid_hash(pid);
    uint32_t idx = 0;
    bool found = false;

    // Find the entry
    for (uint32_t i = 0; i < MAX_PIDS; i++) {
        idx = (SLOT + i) & (MAX_PIDS - 1);
        auto& entry = pid_slot(idx);
        if (entry.pid == 0) {
            global_task_registry_lock.unlock_irqrestore(FLAGS);
            return;  // Not in table
        }
        if (entry.pid == pid) {
            found = true;
            break;
        }
    }
    if (!found) {
        global_task_registry_lock.unlock_irqrestore(FLAGS);
        return;
    }

    // Remove and backward-shift to maintain probe chains
    pid_slot(idx).pid = 0;
    pid_slot(idx).task = nullptr;

    uint32_t next = (idx + 1) & (MAX_PIDS - 1);
    while (pid_slot(next).pid != 0) {
        uint32_t const NATURAL = pid_hash(pid_slot(next).pid);
        // Check if 'next' would benefit from being moved to 'idx'.
        // Condition: natural slot of 'next' is in the wrapped range (natural, idx].
        bool shift = false;
        if (idx < next) {
            shift = (NATURAL <= idx || NATURAL > next);
        } else {
            shift = (NATURAL <= idx && NATURAL > next);
        }
        if (shift) {
            pid_slot(idx) = pid_slot(next);
            pid_slot(next).pid = 0;
            pid_slot(next).task = nullptr;
            idx = next;
        }
        next = (next + 1) & (MAX_PIDS - 1);
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
}

// Per-CPU run queues with spinlocks for cross-CPU access
smt::PerCpuCrossAccess<RunQueue>* run_queues;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Debug: Per-CPU current task pointers for panic inspection
std::array<task::Task*, desc::gdt::MAX_CPUS> debug_task_ptrs = {nullptr};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

inline auto debug_task_slot(uint64_t cpu_no) -> task::Task*& {
    // CPU ids come from smt/cpu topology and are bounded by desc::gdt::MAX_CPUS.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return debug_task_ptrs[static_cast<size_t>(cpu_no)];
}

inline auto run_heap_entry(RunHeap& heap, uint32_t index) -> task::Task*& {
    // Scans bound index by heap.size before peeking into the fixed heap storage.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return heap.entries[static_cast<size_t>(index)];
}

// Vector allocated at init time for scheduler wake IPIs.
// Must not conflict with device driver IRQ allocations.
uint8_t wake_ipi_vector = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Cached APIC timer tick count for the idle timer.
uint32_t idle_timer_ticks = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint64_t IDLE_TIMER_CALIBRATION_US = 10'000;
constexpr uint64_t IDLE_REBALANCE_PROBE_US = 2'000;
constexpr uint32_t SCHED_GC_RECLAIM_BUDGET = 32;
constexpr uint64_t SCHED_GC_WORK_BUDGET_US = 500;
// Keep each deferred pagemap reclaim slice small enough that scheduler GC does
// not monopolize a CPU while process-per-core workloads are launching bursts of
// short-lived helpers. Throughput still comes from repeated self-wake passes.
constexpr uint32_t SCHED_GC_PAGEMAP_STEP_BUDGET = 64;
// When deferred pagemap cleanup is already queued, foreground variance is often
// dominated by repeatedly re-entering tiny reclaim slices. Use a middle budget:
// large enough to finish old address spaces in far fewer passes, but still much
// smaller than the idle-only drain budget.
constexpr uint32_t SCHED_GC_DEFERRED_RECLAIM_BUDGET = 128;
constexpr uint64_t SCHED_GC_DEFERRED_WORK_BUDGET_US = 2'000;
constexpr uint32_t SCHED_GC_DEFERRED_PAGEMAP_STEP_BUDGET = 1024;
constexpr uint32_t SCHED_GC_PRESSURE_RECLAIM_BUDGET = 512;
constexpr uint64_t SCHED_GC_PRESSURE_WORK_BUDGET_US = 50'000;
constexpr uint32_t SCHED_GC_PRESSURE_PAGEMAP_STEP_BUDGET = 65'536;
constexpr uint32_t SCHED_GC_IDLE_RECLAIM_BUDGET = 256;
constexpr uint64_t SCHED_GC_IDLE_WORK_BUDGET_US = 10'000;
constexpr uint32_t SCHED_GC_IDLE_PAGEMAP_STEP_BUDGET = 4096;
std::atomic<bool> scheduler_gc_requested{false};                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> scheduler_gc_worker_started{false};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<task::Task*> scheduler_gc_task{nullptr};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> scheduler_gc_memory_pressure_requested{false};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> scheduler_gc_reclaim_active{false};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint8_t PERF_TIMER_VECTOR = 32;
constexpr uint16_t PERF_IRQ_KIND_TIMER = 4;
constexpr uint32_t PERF_IRQ_SLOW_TRACE_US = 30;
constexpr bool K_ENABLE_SCHED_OWNER_SCAN_FALLBACK = true;

// ============================================================================
// Internal helpers
// ============================================================================

void request_local_reschedule();

[[clang::no_sanitize("kernel-address")]] inline auto current_task_for_preempt() -> task::Task* {
    if (!can_query_current_task()) {
        return nullptr;
    }
    return get_current_task();
}

auto perf_current_pid() -> uint64_t {
    auto* task = current_task_for_preempt();
    return task != nullptr ? task->pid : 0;
}

auto perf_clamp_irq_latency_us(uint64_t latency_us) -> uint32_t {
    return latency_us > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(latency_us);
}

class TimerIrqPerfScope {
   public:
    TimerIrqPerfScope() : started_us(perf::is_local_irq_recording_enabled() ? time::get_us() : 0) {}

    ~TimerIrqPerfScope() {
        if (started_us == 0) {
            return;
        }

        uint64_t const NOW_US = time::get_us();
        uint64_t const ELAPSED_US = NOW_US >= started_us ? NOW_US - started_us : 0;
        uint32_t const LATENCY_US = perf_clamp_irq_latency_us(ELAPSED_US);
        auto const CPU_ID = static_cast<uint32_t>(cpu::current_cpu());
        perf::record_local_irq_summary(perf::WkiPerfLocalIrqOp::HANDLER, PERF_TIMER_VECTOR, PERF_IRQ_KIND_TIMER, 0, LATENCY_US, true);
        if (ELAPSED_US < PERF_IRQ_SLOW_TRACE_US) {
            return;
        }

        perf::record_wki_event(CPU_ID, perf_current_pid(), perf::WkiPerfScope::LOCAL_IRQ,
                               static_cast<uint8_t>(perf::WkiPerfLocalIrqOp::HANDLER), perf::WkiPerfPhase::END, PERF_TIMER_VECTOR,
                               PERF_IRQ_KIND_TIMER, perf::next_wki_trace_correlation(), 0, LATENCY_US, WOS_PERF_CALLSITE());
    }

   private:
    uint64_t started_us;
};

// Restore kernel GS_BASE before entering idle loop.
// Ensures cpu::current_cpu() returns the correct CPU index when
// timer interrupts wake us from idle and call processTasks.
inline void restore_kernel_gs_for_idle() {
    uint32_t const APIC_ID = apic::get_apic_id();
    uint64_t const CPU_IDX = smt::get_cpu_index_from_apic_id(APIC_ID);
    cpu::PerCpu const* kernel_per_cpu = smt::get_kernel_per_cpu(CPU_IDX);
    if (kernel_per_cpu != nullptr) {
        cpu::wrgsbase(reinterpret_cast<uint64_t>(kernel_per_cpu));
    }
}

// Compute EEVDF weighted average vruntime using relative-key representation.
// avg = minVruntime + totalWeightedVruntime / totalWeight
// The relative-key approach keeps totalWeightedVruntime bounded by the spread
// of vruntimes (not their absolute magnitude), preventing int64 overflow.
inline int64_t compute_avg_vruntime(RunQueue* rq) {
    if (rq->total_weight <= 0) {
        return rq->min_vruntime;
    }
    return rq->min_vruntime + (rq->total_weighted_vruntime / rq->total_weight);
}

inline uint32_t task_load_for_incoming(task::Task const* task, task::TaskType incoming_type) {
    constexpr uint32_t FULL_LOAD = 8;
    constexpr uint32_t DAEMON_LOAD = 1;

    if (task == nullptr || task->type == task::TaskType::IDLE) {
        return 0;
    }
    if (incoming_type == task::TaskType::PROCESS && task->type == task::TaskType::DAEMON) {
        return DAEMON_LOAD;
    }
    return FULL_LOAD;
}

inline uint32_t current_task_load_for_incoming(task::Task const* task, task::TaskType incoming_type) {
    if (task == nullptr || task->type == task::TaskType::IDLE || task->is_voluntary_blocked() || task->wants_block) {
        return 0;
    }
    if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return 0;
    }
    return task_load_for_incoming(task, incoming_type);
}

inline void update_current_load_cache(RunQueue* rq, task::Task* task) {
    if (rq == nullptr) {
        return;
    }
    if (task != nullptr && rq->runnable_heap.contains(task)) {
        rq->cached_current_load_default.store(0, std::memory_order_relaxed);
        rq->cached_current_load_process.store(0, std::memory_order_relaxed);
        return;
    }
    rq->cached_current_load_default.store(current_task_load_for_incoming(task, task::TaskType::DAEMON), std::memory_order_relaxed);
    rq->cached_current_load_process.store(current_task_load_for_incoming(task, task::TaskType::PROCESS), std::memory_order_relaxed);
}

inline void publish_current_task(RunQueue* rq, task::Task* task) {
    if (rq == nullptr) {
        return;
    }
    rq->current_task = task;
    update_current_load_cache(rq, task);
}

// Add a task's EEVDF contribution to the run queue aggregate sums.
// Call AFTER setting task->vruntime and inserting into the heap.
inline void add_to_sums(RunQueue* rq, task::Task* t) {
    rq->total_weight += static_cast<int64_t>(t->sched_weight);
    rq->total_weighted_vruntime += (t->vruntime - rq->min_vruntime) * static_cast<int64_t>(t->sched_weight);
    rq->cached_load_default.fetch_add(task_load_for_incoming(t, task::TaskType::DAEMON), std::memory_order_relaxed);
    rq->cached_load_process.fetch_add(task_load_for_incoming(t, task::TaskType::PROCESS), std::memory_order_relaxed);
}

// Remove a task's EEVDF contribution from the run queue aggregate sums.
// Call when removing a task from the runnable heap.
inline void remove_from_sums(RunQueue* rq, task::Task* t) {
    rq->total_weight -= static_cast<int64_t>(t->sched_weight);
    rq->total_weighted_vruntime -= (t->vruntime - rq->min_vruntime) * static_cast<int64_t>(t->sched_weight);
    rq->cached_load_default.fetch_sub(task_load_for_incoming(t, task::TaskType::DAEMON), std::memory_order_relaxed);
    rq->cached_load_process.fetch_sub(task_load_for_incoming(t, task::TaskType::PROCESS), std::memory_order_relaxed);
}

inline auto remove_from_heap_by_scan_locked(RunQueue* rq, task::Task* task) -> bool {
    if (rq == nullptr || task == nullptr) {
        return false;
    }

    for (uint32_t idx = 0; idx < rq->runnable_heap.size; ++idx) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        if (rq->runnable_heap.entries[static_cast<size_t>(idx)] != task) {
            continue;
        }

        task->heap_index = static_cast<int32_t>(idx);
        remove_from_sums(rq, task);
        return rq->runnable_heap.remove(task);
    }

    return false;
}

inline auto valid_exit_switch_candidate(task::Task const* candidate, task::Task const* exiting_task) -> bool {
    if (candidate == nullptr || candidate == exiting_task) {
        return false;
    }
    if (candidate->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return false;
    }
    if (candidate->type == task::TaskType::IDLE) {
        return false;
    }
    return candidate->type != task::TaskType::PROCESS || (candidate->thread != nullptr && candidate->pagemap != nullptr);
}

inline auto pick_best_eligible_excluding_locked(RunQueue* rq, int64_t avg_vruntime, task::Task const* excluded) -> task::Task* {
    if (rq == nullptr || excluded == nullptr || rq->runnable_heap.size == 0) {
        return nullptr;
    }

    task::Task* best_eligible = nullptr;
    int64_t best_eligible_deadline = 0;
    task::Task* best_any = nullptr;
    int64_t best_any_deadline = 0;

    for (uint32_t idx = 0; idx < rq->runnable_heap.size; ++idx) {
        task::Task* candidate = run_heap_entry(rq->runnable_heap, idx);
        if (candidate == nullptr || candidate == excluded) {
            continue;
        }

        if (best_any == nullptr || candidate->vdeadline < best_any_deadline) {
            best_any = candidate;
            best_any_deadline = candidate->vdeadline;
        }

        int64_t const LAG = avg_vruntime - candidate->vruntime;
        if (LAG >= 0 && (best_eligible == nullptr || candidate->vdeadline < best_eligible_deadline)) {
            best_eligible = candidate;
            best_eligible_deadline = candidate->vdeadline;
        }
    }

    return best_eligible != nullptr ? best_eligible : best_any;
}

inline auto pick_best_eligible_for_switch_locked(RunQueue* rq, int64_t avg_vruntime, task::Task const* current_task) -> task::Task* {
    if (rq == nullptr || rq->runnable_heap.size == 0) {
        return nullptr;
    }

    task::Task* next = rq->runnable_heap.pick_best_eligible(avg_vruntime);
    if (next == current_task && current_task != nullptr && current_task->is_voluntary_blocked() && rq->runnable_heap.size > 1) {
        task::Task* alternate = pick_best_eligible_excluding_locked(rq, avg_vruntime, current_task);
        if (alternate != nullptr) {
            return alternate;
        }
    }
    return next;
}

inline auto pick_exit_switch_candidate_locked(RunQueue* rq, task::Task* exiting_task) -> task::Task* {
    if (rq == nullptr) {
        return nullptr;
    }

    while (rq->runnable_heap.size != 0) {
        int64_t const AVG = compute_avg_vruntime(rq);
        auto* candidate = rq->runnable_heap.pick_best_eligible(AVG);
        if (valid_exit_switch_candidate(candidate, exiting_task)) {
            return candidate;
        }

        if (!remove_from_heap_by_scan_locked(rq, candidate)) {
            return nullptr;
        }
        if (candidate != nullptr) {
            candidate->sched_queue = task::Task::sched_queue::NONE;
        }
    }

    return nullptr;
}

inline bool process_has_kernel_resume_frame(const task::Task* t) {
    return t != nullptr && t->type == task::TaskType::PROCESS && (t->context.frame.cs & 0x3ULL) == 0;
}

inline void finish_wait_metadata_for_runqueue(task::Task* t) {
    if (process_has_kernel_resume_frame(t)) {
        t->set_voluntary_blocked(true);
        return;
    }
    t->set_voluntary_blocked(false);
    t->clear_wait_channel();
}

inline auto min_nonzero_deadline(uint64_t lhs, uint64_t rhs) -> uint64_t {
    if (lhs == 0) {
        return rhs;
    }
    if (rhs == 0) {
        return lhs;
    }
    return std::min(lhs, rhs);
}

inline void update_relaxed_max(std::atomic<uint64_t>& slot, uint64_t value) {
    uint64_t observed = slot.load(std::memory_order_relaxed);
    while (value > observed && !slot.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

inline auto task_wait_deadline_us(task::Task const* t) -> uint64_t {
    if (t == nullptr) {
        return 0;
    }

    uint64_t waitpid_repair_deadline_us = 0;
    if (t->wait_channel_is(task::WaitChannelKind::WAITPID) && t->waiting_for_pid != 0) {
        uint64_t const LAST_REPAIR_US = t->waitpid_last_repair_us != 0 ? t->waitpid_last_repair_us : t->last_sleep_start_us;
        if (LAST_REPAIR_US != 0) {
            waitpid_repair_deadline_us =
                LAST_REPAIR_US > UINT64_MAX - WAITPID_REPAIR_FALLBACK_MIN_US ? UINT64_MAX : LAST_REPAIR_US + WAITPID_REPAIR_FALLBACK_MIN_US;
        }
    }

    return min_nonzero_deadline(min_nonzero_deadline(t->wake_at_us, t->itimer_real_expire_us), waitpid_repair_deadline_us);
}

inline auto wait_list_contains_locked(RunQueue* rq, task::Task const* t) -> bool {
    if (rq == nullptr || t == nullptr) {
        return false;
    }
    for (task::Task const* cur = rq->wait_list.head; cur != nullptr; cur = cur->sched_next) {
        if (cur == t) {
            return true;
        }
    }
    return false;
}

inline void note_wait_deadline_locked(RunQueue* rq, task::Task const* t) {
    uint64_t const DEADLINE_US = task_wait_deadline_us(t);
    if (rq == nullptr || DEADLINE_US == 0) {
        return;
    }
    if (rq->next_wait_deadline_us == 0 || DEADLINE_US < rq->next_wait_deadline_us) {
        rq->next_wait_deadline_us = DEADLINE_US;
    }
}

inline void recompute_wait_deadline_locked(RunQueue* rq) {
    if (rq == nullptr) {
        return;
    }
    uint64_t next_deadline = 0;
    for (task::Task const* cur = rq->wait_list.head; cur != nullptr; cur = cur->sched_next) {
        next_deadline = min_nonzero_deadline(next_deadline, task_wait_deadline_us(cur));
    }
    rq->next_wait_deadline_us = next_deadline;
}

inline void wait_list_push_locked(RunQueue* rq, task::Task* t) {
    if (rq == nullptr || t == nullptr) {
        return;
    }
    rq->wait_list.push(t);
    note_wait_deadline_locked(rq, t);
}

inline auto wait_list_remove_locked(RunQueue* rq, task::Task* t) -> bool {
    if (rq == nullptr || t == nullptr) {
        return false;
    }
    uint64_t const OLD_DEADLINE_US = task_wait_deadline_us(t);
    bool const REMOVED = rq->wait_list.remove(t);
    if (REMOVED && OLD_DEADLINE_US != 0 && OLD_DEADLINE_US <= rq->next_wait_deadline_us) {
        recompute_wait_deadline_locked(rq);
    }
    return REMOVED;
}

inline void wait_list_remove_all_locked(RunQueue* rq, task::Task* t) {
    while (wait_list_remove_locked(rq, t)) {
    }
}

inline void repair_stale_wait_membership_locked(RunQueue* rq, task::Task* t) {
    if (rq == nullptr || t == nullptr || !rq->runnable_heap.contains(t)) {
        return;
    }

    wait_list_remove_all_locked(rq, t);
    t->sched_queue = task::Task::sched_queue::RUNNABLE;
    t->wants_block = false;
    t->wake_at_us = 0;
    finish_wait_metadata_for_runqueue(t);
}

[[nodiscard]] inline auto publish_runnable_task_locked(RunQueue* rq, task::Task* t, const char* reason) -> bool {
    if (rq == nullptr || t == nullptr) {
        return false;
    }

    if (rq->runnable_heap.contains(t)) {
        repair_stale_wait_membership_locked(rq, t);
        if (rq->current_task == t) {
            update_current_load_cache(rq, t);
        }
        return true;
    }

    if (t->heap_index >= 0) {
        dbg::logger<"sched">::error("runnable publish refused: reason=%s pid=%lu name=%s cpu=%lu heap_index=%d queue=%d",
                                    reason != nullptr ? reason : "?", t->pid, task_name_for_log(t), t->cpu, t->heap_index,
                                    static_cast<int>(t->sched_queue));
        dbg::panic_handler("scheduler: runnable publish refused with stale heap index");
        return false;
    }

    if (!rq->runnable_heap.insert(t)) {
        dbg::logger<"sched">::error("runnable heap full: reason=%s pid=%lu name=%s cpu=%lu size=%u cap=%u",
                                    reason != nullptr ? reason : "?", t->pid, task_name_for_log(t), t->cpu, rq->runnable_heap.size,
                                    PER_CPU_HEAP_CAP);
        dbg::panic_handler("scheduler: runnable heap full");
        return false;
    }

    add_to_sums(rq, t);
    t->sched_queue = task::Task::sched_queue::RUNNABLE;
    if (rq->current_task == t) {
        update_current_load_cache(rq, t);
    }
    return true;
}

inline auto runqueue_task_is_reserved_locked(RunQueue* rq, task::Task const* task) -> bool {
    return rq != nullptr && task != nullptr && (rq->current_task == task || rq->handoff_task == task);
}

inline auto runqueue_owns_task_locked(RunQueue* rq, task::Task* task) -> bool {
    return runqueue_task_is_reserved_locked(rq, task) || (rq != nullptr && rq->runnable_heap.contains(task)) ||
           wait_list_contains_locked(rq, task);
}

inline void reserve_handoff_task_locked(RunQueue* rq, task::Task* task, uint64_t start_us) {
    task->cpu = cpu::current_cpu();
    rq->handoff_task = task;
    rq->current_task_start_us = start_us;
}

inline auto compute_wakeup_floor_vruntime(RunQueue* rq, task::Task* t, uint64_t now_us, task::WaitChannelKind wait_channel) -> int64_t;

void requeue_woken_outgoing_task_locked(RunQueue* rq, task::Task* outgoing, uint64_t now_us, task::Task*& waitpid_repair_task) {
    if (rq == nullptr || outgoing == nullptr || outgoing->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return;
    }
    if (!outgoing->wakeup_pending.exchange(false, std::memory_order_acquire)) {
        return;
    }
    if (outgoing->sched_queue != task::Task::sched_queue::WAITING) {
        return;
    }

    task::WaitChannelKind const WAIT_CHANNEL = outgoing->wait_channel_kind;
    if (is_waitpid_wait_channel(WAIT_CHANNEL) && outgoing->waiting_for_pid != 0) {
        if (waitpid_repair_task == nullptr && outgoing->try_acquire()) {
            waitpid_repair_task = outgoing;
        }
        return;
    }

    wait_list_remove_all_locked(rq, outgoing);
    outgoing->wants_block = false;
    outgoing->wake_at_us = 0;
    finish_wait_metadata_for_runqueue(outgoing);
    outgoing->just_woke = !is_low_latency_handoff_wait_channel(WAIT_CHANNEL);

    if (rq->runnable_heap.contains(outgoing)) {
        repair_stale_wait_membership_locked(rq, outgoing);
        return;
    }

    outgoing->vruntime = std::max(outgoing->vruntime, compute_wakeup_floor_vruntime(rq, outgoing, now_us, WAIT_CHANNEL));
    outgoing->vdeadline = vdeadline_from_slice(outgoing->vruntime, outgoing->slice_ns, outgoing->sched_weight);
    outgoing->slice_used_ns = 0;
    (void)publish_runnable_task_locked(rq, outgoing, "requeue-woken-outgoing");
}

void commit_handoff_task_at_return_boundary() {
    if (run_queues == nullptr) {
        return;
    }

    task::Task* outgoing = nullptr;
    task::Task* task = nullptr;
    task::Task* waitpid_repair_task = nullptr;
    uint64_t const NOW_US = time::get_us();
    run_queues->this_cpu_locked_void([&](RunQueue* rq) {
        task = rq->handoff_task;
        if (task == nullptr) {
            return;
        }

        outgoing = rq->current_task;
        task->cpu = cpu::current_cpu();
        publish_current_task(rq, task);
        debug_task_slot(cpu::current_cpu()) = task;
        rq->handoff_task = nullptr;
        requeue_woken_outgoing_task_locked(rq, outgoing, NOW_US, waitpid_repair_task);
    });

    if (task == nullptr) {
        if (waitpid_repair_task != nullptr) {
            waitpid_repair_task->release();
        }
        return;
    }

    if (waitpid_repair_task != nullptr) {
        uint64_t target_cpu = waitpid_repair_task->cpu;
        if (target_cpu >= smt::get_core_count()) {
            target_cpu = get_least_loaded_cpu();
        }
        reschedule_task_for_cpu(target_cpu, waitpid_repair_task);
        waitpid_repair_task->release();
    }

    if (is_dead_gc_candidate_task(outgoing) && outgoing != task &&
        outgoing->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE &&
        !outgoing->gc_queued.load(std::memory_order_acquire)) {
        insert_into_dead_list(outgoing);
    }

    sys::signal::exit_current_on_pending_fatal_default_signal();
}

void clear_handoff_task(task::Task* task) {
    run_queues->this_cpu_locked_void([task](RunQueue* rq) {
        if (rq->handoff_task == task) {
            rq->handoff_task = nullptr;
        }
    });
}

void set_task_nice_impl(task::Task* task, int nice) {
    if (task == nullptr || run_queues == nullptr) {
        return;
    }

    int const CLAMPED_NICE = clamp_nice(nice);
    uint32_t const NEW_WEIGHT = nice_to_weight(CLAMPED_NICE);
    uint64_t cpu_no = task->cpu;
    uint64_t const CORE_COUNT = smt::get_core_count();
    if (cpu_no >= CORE_COUNT) {
        cpu_no = cpu::current_cpu();
    }

    bool updated = false;
    run_queues->with_lock_void(cpu_no, [task, CLAMPED_NICE, NEW_WEIGHT, &updated](RunQueue* rq) {
        if (!runqueue_task_is_reserved_locked(rq, task) && !rq->runnable_heap.contains(task) &&
            task->sched_queue != task::Task::sched_queue::WAITING) {
            return;
        }

        bool const IN_HEAP = rq->runnable_heap.contains(task);
        if (IN_HEAP) {
            remove_from_sums(rq, task);
        }

        task->sched_nice = static_cast<int8_t>(CLAMPED_NICE);
        task->sched_weight = NEW_WEIGHT;
        task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);

        if (IN_HEAP) {
            add_to_sums(rq, task);
            rq->runnable_heap.update(task);
        }
        updated = true;
    });

    if (!updated) {
        task->sched_nice = static_cast<int8_t>(CLAMPED_NICE);
        task->sched_weight = NEW_WEIGHT;
        task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);
    }

    if (cpu_no == cpu::current_cpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(cpu_no);
    }
}

// Bursty short-sleep tasks (dropbear/httpd/netpoll style) should stay responsive,
// but they should not alternate 50/50 with CPU-bound workers just because sleep
// time freezes vruntime. Charge a small wakeup tax only for short-run + short-sleep
// wakeups; long-idle interactive wakes keep zero penalty.
inline auto compute_wakeup_floor_vruntime(RunQueue* rq, task::Task* t, uint64_t now_us, task::WaitChannelKind wait_channel) -> int64_t {
    int64_t const AVG = compute_avg_vruntime(rq);
    int64_t const FAIR_VRUNTIME = std::max(rq->min_vruntime, AVG);

    if (is_low_latency_handoff_wait_channel(wait_channel)) {
        return FAIR_VRUNTIME;
    }

    if (t->last_sleep_start_us == 0 || t->last_run_us == 0 || now_us <= t->last_sleep_start_us) {
        return FAIR_VRUNTIME;
    }

    uint64_t const SLEEP_US = now_us - t->last_sleep_start_us;
    if (SLEEP_US > 20'000ULL || t->last_run_us < 250U || t->last_run_us > 8'000U) {
        return FAIR_VRUNTIME;
    }

    uint64_t const PENALTY_US = std::clamp<uint64_t>(t->last_run_us, 4'000ULL, 8'000ULL);
    auto const PENALTY_VRUNTIME = vruntime_delta_from_runtime_ns(PENALTY_US * 1000ULL, t->sched_weight);
    return FAIR_VRUNTIME + PENALTY_VRUNTIME;
}

// ============================================================================
// Work stealing (Phase 2)
//
// Called with OUR per-CPU lock held (inside thisCpuLocked / thisCpuLockedVoid).
// Uses tryWithLock (non-blocking) for victim CPUs to avoid deadlock.
// Steals the task with the highest vdeadline (least urgent) from the most-loaded
// peer CPU in our GROUP domain, subject to:
//   • victim must have >= 2 full tasks (leaves victim at least 1)
//   • load differential >= 2 × full_load (prevents thrashing)
//   • task must not be cpu_pinned or domain_hard
//
// Returns true if a task was stolen and inserted into our heap.
// ============================================================================
inline auto cpu_mask_contains(uint64_t mask, uint64_t cpu_no) -> bool { return cpu_no < 64 && (mask & (1ULL << cpu_no)) != 0U; }

inline auto task_can_run_on_cpu(task::Task const* task, uint64_t cpu_no, uint64_t core_count) -> bool {
    if (task == nullptr || cpu_no >= core_count) {
        return false;
    }

    uint64_t const ALL_MASK = (core_count >= 64) ? UINT64_MAX : ((1ULL << core_count) - 1ULL);
    uint64_t allowed_mask = task->domain_mask & ALL_MASK;
    if (allowed_mask == 0) {
        allowed_mask = ALL_MASK;
    }

    return cpu_mask_contains(allowed_mask, cpu_no);
}

inline auto user_context_is_canonical(task::Task const* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    return task->context.frame.cs == desc::gdt::GDT_USER_CS && task->context.frame.ss == desc::gdt::GDT_USER_DS &&
           task->context.frame.rip < 0x0000800000000000ULL && task->context.frame.rsp < 0x0000800000000000ULL;
}

inline auto process_task_can_idle_steal(task::Task const* task) -> bool {
    if (task == nullptr || task->type != task::TaskType::PROCESS) {
        return true;
    }
    if (task->thread == nullptr || task->pagemap == nullptr || task->wki_proxy_task_id != 0) {
        return false;
    }
    if (task->preempt_disable_depth != 0 || task->deferred_task_switch || task->wants_block) {
        return false;
    }
    if (task->is_voluntary_blocked()) {
        return task->context.frame.cs == desc::gdt::GDT_KERN_CS && task->context.frame.ss == desc::gdt::GDT_KERN_DS &&
               is_valid_kernel_stack(task->context.frame.rsp);
    }

    return user_context_is_canonical(task);
}

inline auto idle_rebalance_probe_needed_for_idle(uint64_t idle_cpu) -> bool {
    if (run_queues == nullptr) {
        return false;
    }

    uint64_t const N = smt::get_core_count();
    if (N <= 1 || idle_cpu >= N) {
        return false;
    }

    uint32_t const OUR_GROUP = smt::find_group_for_cpu(idle_cpu);
    auto* group = smt::get_cpu_domain(OUR_GROUP);
    uint64_t const GROUP_MASK = (group != nullptr) ? group->cpu_mask : ~0ULL;
    constexpr uint32_t FULL_LOAD = 8;

    for (uint64_t off = 1; off <= N; ++off) {
        uint64_t const VICTIM_CPU = (idle_cpu + off) % N;
        if (VICTIM_CPU == idle_cpu || !cpu_mask_contains(GROUP_MASK, VICTIM_CPU)) {
            continue;
        }

        auto* victim_rq = run_queues->that_cpu(VICTIM_CPU);
        if (victim_rq == nullptr) {
            continue;
        }

        uint32_t const VICTIM_DOM_ID = victim_rq->domain_id;
        if (VICTIM_DOM_ID != 0) {
            auto* victim_dom = smt::get_cpu_domain(VICTIM_DOM_ID);
            if (victim_dom != nullptr && victim_dom->hard && !cpu_mask_contains(victim_dom->cpu_mask, idle_cpu)) {
                continue;
            }
        }

        uint32_t const VICTIM_HEAP_SIZE = victim_rq->runnable_heap.size;
        if (VICTIM_HEAP_SIZE == 0) {
            continue;
        }

        uint32_t const VICTIM_CURRENT_LOAD = victim_rq->cached_current_load_process.load(std::memory_order_relaxed);
        if (VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2) {
            continue;
        }

        uint32_t const VICTIM_LOAD = victim_rq->cached_load_process.load(std::memory_order_relaxed) + VICTIM_CURRENT_LOAD;
        uint32_t const MIN_STEAL_LOAD =
            (victim_rq->daemon_load_penalty.load(std::memory_order_relaxed) > 0) ? FULL_LOAD * 4 : FULL_LOAD * 2;
        if (VICTIM_LOAD >= MIN_STEAL_LOAD) {
            return true;
        }
    }

    return false;
}

auto try_steal_from_peers(uint64_t stealing_cpu, RunQueue* our_rq) -> bool {
    if (run_queues == nullptr) {
        return false;
    }
    if (our_rq == nullptr || our_rq->runnable_heap.size >= PER_CPU_HEAP_CAP) {
        return false;
    }
    const uint64_t N = smt::get_core_count();
    if (N <= 1) {
        return false;
    }

    // Restrict stealing to our GROUP domain
    uint32_t const OUR_GROUP = smt::find_group_for_cpu(stealing_cpu);
    auto* grp = smt::get_cpu_domain(OUR_GROUP);
    uint64_t const GROUP_MASK = (grp != nullptr) ? grp->cpu_mask : ~0ULL;

    constexpr uint32_t FULL_LOAD = 8;

    // Round-robin start point for fairness among victims
    static std::atomic<uint64_t> steal_seed{0};
    uint64_t const START = steal_seed.fetch_add(1, std::memory_order_relaxed) % N;

    for (uint64_t off = 1; off <= N; ++off) {
        uint64_t const VICTIM_CPU = (START + off) % N;
        if (VICTIM_CPU == stealing_cpu) {
            continue;
        }
        if (!cpu_mask_contains(GROUP_MASK, VICTIM_CPU)) {
            continue;
        }

        // Quick racy load peek (no lock needed - heuristic, not authoritative)
        auto* victim_rq_raw = run_queues->that_cpu(VICTIM_CPU);

        // Phase 8a: hard-domain boundary - never steal from a CPU in a hard leaf domain
        // into a CPU outside that domain. Racy read is intentional (heuristic guard).
        {
            uint32_t const VICTIM_DOM_ID = victim_rq_raw->domain_id;
            if (VICTIM_DOM_ID != 0) {
                auto* victim_dom = smt::get_cpu_domain(VICTIM_DOM_ID);
                if (victim_dom != nullptr && victim_dom->hard && !cpu_mask_contains(victim_dom->cpu_mask, stealing_cpu)) {
                    continue;  // Do not steal across hard-domain boundary
                }
            }
        }

        uint32_t const VICTIM_HEAP_SIZE = victim_rq_raw->runnable_heap.size;
        uint32_t const VICTIM_CURRENT_LOAD = victim_rq_raw->cached_current_load_process.load(std::memory_order_relaxed);
        uint32_t const VLOAD = victim_rq_raw->cached_load_process.load(std::memory_order_relaxed) + VICTIM_CURRENT_LOAD;
        // Phase 8b: soft-exclusive CPUs (daemon_load_penalty > 0) are only stolen from
        // if the load differential is 2× the normal threshold, preserving their intent.
        uint32_t const MIN_STEAL_LOAD =
            (victim_rq_raw->daemon_load_penalty.load(std::memory_order_relaxed) > 0) ? FULL_LOAD * 4 : FULL_LOAD * 2;
        if (VICTIM_HEAP_SIZE == 0 || (VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2)) {
            continue;
        }
        if (VLOAD < MIN_STEAL_LOAD) {
            continue;
        }

        // Phase 1: under victim's lock, find candidate, update its cpu field,
        // then remove from victim heap.
        //
        // CRITICAL: stolen->cpu must be updated while STILL holding the victim
        // lock, before removing it from the heap.  If we release the lock
        // first, reschedule_task_for_cpu on another CPU can see the task as
        // "not found anywhere" (heapIndex=-1, cpu=victim) and re-insert it
        // back into the victim's heap with heapIndex≥0.  Our Phase 2 insert
        // would then be silently refused (double-insert guard), leaving the
        // task stuck on the victim CPU with a stale cpu field.
        task::Task* stolen = nullptr;
        run_queues->try_with_lock(VICTIM_CPU, [stealing_cpu, &stolen, N](RunQueue* victim_rq) {
            // Authoritative check under lock
            uint32_t const VICTIM_HEAP_SIZE = victim_rq->runnable_heap.size;
            if (VICTIM_HEAP_SIZE == 0) {
                return;
            }

            uint32_t const VICTIM_CURRENT_LOAD = current_task_load_for_incoming(victim_rq->current_task, task::TaskType::PROCESS);
            if (VICTIM_CURRENT_LOAD == 0 && VICTIM_HEAP_SIZE < 2) {
                return;
            }

            constexpr uint32_t LOCKED_FULL_LOAD = 8;
            uint32_t const VICTIM_LOAD = victim_rq->cached_load_process.load(std::memory_order_relaxed) + VICTIM_CURRENT_LOAD;
            uint32_t const MIN_STEAL_LOAD =
                (victim_rq->daemon_load_penalty.load(std::memory_order_relaxed) > 0) ? LOCKED_FULL_LOAD * 4 : LOCKED_FULL_LOAD * 2;
            if (VICTIM_LOAD < MIN_STEAL_LOAD) {
                return;
            }

            // Never steal the task currently executing on the victim CPU.
            task::Task const* victim_current = victim_rq->current_task;
            task::Task const* victim_handoff = victim_rq->handoff_task;

            // Scan for highest-vdeadline stealable task (least urgent)
            task::Task* best = nullptr;
            int64_t best_vd = INT64_MIN;
            uint32_t scan = victim_rq->runnable_heap.size;
            scan = std::min<uint32_t>(scan, 16);  // Bound scan in interrupt context
            for (uint32_t i = 0; i < scan; ++i) {
                task::Task* t = run_heap_entry(victim_rq->runnable_heap, i);
                if (t == nullptr) {
                    continue;
                }
                if (t == victim_current || t == victim_handoff) {
                    continue;  // Never steal currently-executing or reserved-for-handoff task
                }
                if (t->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                    continue;
                }
                if (t->cpu_pinned || t->domain_hard) {
                    continue;
                }
                if (!task_can_run_on_cpu(t, stealing_cpu, N)) {
                    continue;
                }
                if (t->type == task::TaskType::IDLE) {
                    continue;
                }
                if (!t->has_run) {
                    continue;
                }
                if (!process_task_can_idle_steal(t)) {
                    continue;
                }
                if (t->vdeadline > best_vd) {
                    best_vd = t->vdeadline;
                    best = t;
                }
            }
            if (best == nullptr) {
                return;
            }

            // Claim ownership BEFORE removing from heap - closes the window
            // where reschedule_task_for_cpu could re-insert into victim queue.
            best->cpu = stealing_cpu;

            // Remove from victim (victim lock still held)
            remove_from_sums(victim_rq, best);
            victim_rq->runnable_heap.remove(best);
            stolen = best;
        });

        if (stolen != nullptr) {
            // Phase 2: insert into our heap. Victim lock is now released.
            // Our lock is already held by the outer thisCpuLocked - safe to write.
            if (stolen->vruntime < our_rq->min_vruntime) {
                int64_t const DIFF = our_rq->min_vruntime - stolen->vruntime;
                stolen->vruntime = our_rq->min_vruntime;
                stolen->vdeadline += DIFF;
            }
            if (!publish_runnable_task_locked(our_rq, stolen, "work-steal")) {
                // Insert refused (double-insert guard fired) - task is already
                // in some heap due to a concurrent reschedule.  Don't count
                // this as a successful steal; let the normal scheduler pick it.
                return false;
            }
            return true;
        }
    }
    return false;
}

inline uint32_t compute_heap_load_locked(RunQueue* rq, task::Task const* current, task::TaskType incoming_type) {
    uint32_t load = 0;
    for (uint32_t i = 0; i < rq->runnable_heap.size; ++i) {
        task::Task const* task = run_heap_entry(rq->runnable_heap, i);
        if (task == current && task->is_voluntary_blocked()) {
            continue;
        }
        load += task_load_for_incoming(task, incoming_type);
    }
    return load;
}

inline uint32_t compute_effective_load_locked(RunQueue* rq, task::TaskType incoming_type = task::TaskType::DAEMON) {
    uint32_t load = compute_heap_load_locked(rq, rq->current_task, incoming_type);
    auto* cur = rq->current_task;
    bool const CUR_IN_HEAP = cur != nullptr && rq->runnable_heap.contains(cur);

    bool const HAS_ACTIVE = (cur != nullptr && cur->type != task::TaskType::IDLE && !cur->is_voluntary_blocked());
    if (HAS_ACTIVE && !CUR_IN_HEAP) {
        load += task_load_for_incoming(cur, incoming_type);
    }

    constexpr uint32_t FULL_LOAD = 8;
    load += rq->placement_reservations.load(std::memory_order_relaxed) * FULL_LOAD;

    // Soft-exclusive daemon CPU penalty (Phase 1): if this RunQueue is marked as a
    // soft-exclusive daemon CPU and the incoming task is a PROCESS, inflate the
    // reported load so compute tasks prefer other CPUs.
    uint32_t const DAEMON_PENALTY = rq->daemon_load_penalty.load(std::memory_order_relaxed);
    if (DAEMON_PENALTY > 0 && incoming_type == task::TaskType::PROCESS) {
        load += DAEMON_PENALTY;
    }

    return load;
}

inline uint32_t cached_effective_load_for_cpu(uint64_t cpu_no, task::TaskType incoming_type) {
    constexpr uint32_t FULL_LOAD = 8;

    auto* rq = run_queues != nullptr ? run_queues->that_cpu(cpu_no) : nullptr;
    if (rq == nullptr) {
        return UINT32_MAX;
    }

    uint32_t load = (incoming_type == task::TaskType::PROCESS ? rq->cached_load_process.load(std::memory_order_relaxed)
                                                              : rq->cached_load_default.load(std::memory_order_relaxed));
    load += (incoming_type == task::TaskType::PROCESS ? rq->cached_current_load_process.load(std::memory_order_relaxed)
                                                      : rq->cached_current_load_default.load(std::memory_order_relaxed));
    load += rq->placement_reservations.load(std::memory_order_relaxed) * FULL_LOAD;

    if (incoming_type == task::TaskType::PROCESS) {
        load += rq->daemon_load_penalty.load(std::memory_order_relaxed);
    }

    return load;
}

uint64_t get_least_loaded_cpu_for_task(task::Task const* incoming_task) {
    if (run_queues == nullptr) {
        return 0;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    if (CORE_COUNT <= 1) {
        return 0;
    }

    task::TaskType const INCOMING_TYPE = incoming_task != nullptr ? incoming_task->type : task::TaskType::DAEMON;

    // Respect domain_mask: if the task has a restricted mask, only consider those CPUs.
    uint64_t allowed_mask = (incoming_task != nullptr) ? incoming_task->domain_mask : ~0ULL;
    uint64_t const ALL_MASK = (CORE_COUNT >= 64) ? UINT64_MAX : ((1ULL << CORE_COUNT) - 1ULL);
    allowed_mask &= ALL_MASK;
    if (allowed_mask == 0) {
        allowed_mask = ALL_MASK;  // fallback: allow all
    }

    static std::atomic<uint64_t> rr_seed{0};
    uint64_t const START = rr_seed.fetch_add(1, std::memory_order_relaxed) % CORE_COUNT;

    auto best_cpu = static_cast<uint64_t>(__builtin_ctzll(allowed_mask));  // safe default
    uint32_t best_load = UINT32_MAX;
    uint64_t const PLACEMENT_SAMPLES = INCOMING_TYPE == task::TaskType::PROCESS && CORE_COUNT <= 64 ? CORE_COUNT : 4;
    uint64_t samples = 0;

    for (uint64_t off = 0; off < CORE_COUNT && samples < PLACEMENT_SAMPLES; ++off) {
        uint64_t const CPU_NO = (START + off) % CORE_COUNT;
        if (!cpu_mask_contains(allowed_mask, CPU_NO)) {
            continue;
        }
        samples++;
        uint32_t const LOAD = cached_effective_load_for_cpu(CPU_NO, INCOMING_TYPE);

        if (LOAD < best_load) {
            best_load = LOAD;
            best_cpu = CPU_NO;
        }
        if (best_load == 0) {
            return best_cpu;
        }
    }

    return best_cpu;
}

uint64_t reserve_least_loaded_cpu(task::Task* incoming_task) {
    uint64_t const FLAGS = g_placement_lock.lock_irqsave();
    uint64_t const BEST_CPU = get_least_loaded_cpu_for_task(incoming_task);
    auto* rq = run_queues != nullptr ? run_queues->that_cpu(BEST_CPU) : nullptr;
    if (rq != nullptr) {
        rq->placement_reservations.fetch_add(1, std::memory_order_relaxed);
    }
    g_placement_lock.unlock_irqrestore(FLAGS);
    return BEST_CPU;
}

void release_cpu_reservation(uint64_t cpu_no) {
    auto* rq = run_queues != nullptr ? run_queues->that_cpu(cpu_no) : nullptr;
    if (rq != nullptr) {
        rq->placement_reservations.fetch_sub(1, std::memory_order_relaxed);
    }
}

bool post_task_for_cpu_impl(uint64_t cpu_no, task::Task* task, bool release_reservation) {
    auto release_reservation_if_needed = [&]() {
        if (release_reservation && run_queues != nullptr && cpu_no < smt::get_core_count()) {
            release_cpu_reservation(cpu_no);
        }
    };

    if (task == nullptr) {
        log_rejected_task_publication("null task", cpu_no, task);
        release_reservation_if_needed();
        return false;
    }

    if (run_queues == nullptr || cpu_no >= smt::get_core_count()) {
        log_rejected_task_publication("invalid target cpu", cpu_no, task);
        release_reservation_if_needed();
        return false;
    }

    if (task->type == task::TaskType::IDLE) {
        if (!is_publishable_idle_task(task)) {
            log_rejected_task_publication("invalid idle task", cpu_no, task);
            release_reservation_if_needed();
            return false;
        }

        task->cpu = cpu_no;
        if (task->start_time_us == 0) {
            task->start_time_us = time::get_us();
        }
        __atomic_thread_fence(__ATOMIC_RELEASE);

        task::Task* existing_idle = nullptr;
        run_queues->with_lock_void(cpu_no, [task, &existing_idle](RunQueue* rq) {
            existing_idle = rq->idle_task;
            if (existing_idle == nullptr || existing_idle == task) {
                rq->idle_task = task;
                task->sched_queue = task::Task::sched_queue::NONE;
            }
        });

        if (existing_idle != nullptr && existing_idle != task) {
            dbg::logger<"sched">::error("refusing to replace idle task: cpu=%lu existing=%p new=%p", cpu_no,
                                        static_cast<void*>(existing_idle), static_cast<void*>(task));
            release_reservation_if_needed();
            return false;
        }

        release_reservation_if_needed();
        return true;
    }

    if (!is_publishable_runnable_task(task)) {
        log_rejected_task_publication("invalid runnable task", cpu_no, task);
        release_reservation_if_needed();
        return false;
    }

#ifdef SCHED_DEBUG
    dbg::log("POST: PID %x '%s' -> CPU %d (heapIdx=%d, from CPU %d)", task->pid, (task->name != nullptr) ? task->name : "?",
             static_cast<int>(cpu_no), task->heap_index, static_cast<int>(cpu::current_cpu()));
#endif
    if (task->type == task::TaskType::DAEMON && kernel_threads_shutdown_requested()) {
        task->kernel_shutdown_requested.store(true, std::memory_order_release);
    }
    task->cpu = cpu_no;

    if (task->start_time_us == 0) {
        task->start_time_us = time::get_us();
    }

    __atomic_thread_fence(__ATOMIC_RELEASE);

    if (task->pid > 0) {
        if (!pid_table_insert(task)) {
            log_rejected_task_publication("pid registry full", cpu_no, task);
            release_reservation_if_needed();
            return false;
        }
        if (!active_list_insert(task)) {
            pid_table_remove(task->pid);
            log_rejected_task_publication("active registry full", cpu_no, task);
            release_reservation_if_needed();
            return false;
        }
    }

    bool posted = false;
    run_queues->with_lock_void(cpu_no, [task, &posted](RunQueue* rq) {
        task->vruntime = (rq->min_vruntime > 0) ? rq->min_vruntime : 0;
        task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);
        task->slice_used_ns = 0;
        posted = publish_runnable_task_locked(rq, task, "post-task");
    });

    release_reservation_if_needed();

    if (!posted) {
        if (task->pid > 0) {
            active_list_remove(task->pid);
            pid_table_remove(task->pid);
        }
        log_rejected_task_publication("runnable heap insert failed", cpu_no, task);
        return false;
    }

    if (cpu_no == cpu::current_cpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(cpu_no);
    }

    return true;
}

auto local_reschedule_requires_fresh_timer(RunQueue* rq) -> bool {
    if (rq == nullptr) {
        return false;
    }

    auto* current = rq->current_task;
    return rq->is_idle.load(std::memory_order_acquire) || current == nullptr || current->type == task::TaskType::IDLE ||
           current->is_voluntary_blocked() || current->wants_block;
}

// Send a scheduler wake IPI to a specific CPU if it's currently idle.
// This ensures idle CPUs don't sleep up to 10ms before noticing new work.
void request_local_reschedule() {
    auto* rq = run_queues->this_cpu();
    bool fresh_timer_required = false;
    if (rq != nullptr) {
        fresh_timer_required = local_reschedule_requires_fresh_timer(rq);
        rq->local_reschedule_requests.fetch_add(1, std::memory_order_relaxed);
        if (rq->resched_timer_pending.exchange(true, std::memory_order_acq_rel) && !fresh_timer_required) {
            return;
        }
    }
    if (!sys::context_switch::request_reschedule() && rq != nullptr) {
        rq->resched_timer_pending.store(false, std::memory_order_release);
    }
}

auto deferred_return_needs_local_timer_locked(RunQueue* rq, task::Task* return_task) -> bool {
    if (rq == nullptr) {
        return false;
    }

    bool const RETURN_IS_IDLE = return_task == nullptr || return_task->type == task::TaskType::IDLE;
    if (RETURN_IS_IDLE) {
        return rq->runnable_heap.size != 0 || rq->next_wait_deadline_us != 0;
    }

    if (return_task->is_voluntary_blocked() || return_task->wants_block) {
        return true;
    }

    if (rq->runnable_heap.size > 1) {
        return true;
    }

    if (rq->runnable_heap.size == 1 && !rq->runnable_heap.contains(return_task)) {
        return true;
    }

    return rq->next_wait_deadline_us != 0 || return_task->itimer_real_expire_us != 0;
}

void arm_local_timer_after_deferred_switch(task::Task* return_task) {
    if (run_queues == nullptr) {
        return;
    }

    bool arm_timer = false;
    run_queues->this_cpu_locked_void([return_task, &arm_timer](RunQueue* rq) {
        if (rq == nullptr) {
            return;
        }

        // A syscall-exit deferred switch consumes the local reschedule request
        // without taking the timer IRQ path that normally clears this bit.
        rq->resched_timer_pending.store(false, std::memory_order_release);
        arm_timer = deferred_return_needs_local_timer_locked(rq, return_task);
        if (arm_timer) {
            rq->resched_timer_pending.store(true, std::memory_order_release);
        }
    });

    if (!arm_timer) {
        return;
    }

    note_local_reschedule_timer_poke();
    apic::one_shot_timer(1);
}

auto idle_timer_ticks_for_delta_us(uint64_t delta_us) -> uint64_t {
    if (idle_timer_ticks == 0) {
        idle_timer_ticks = apic::calibrate_timer(IDLE_TIMER_CALIBRATION_US);
    }

    delta_us = std::min<uint64_t>(delta_us, IDLE_TIMER_CALIBRATION_US);
    uint64_t ticks = (static_cast<uint64_t>(idle_timer_ticks) * delta_us) / IDLE_TIMER_CALIBRATION_US;
    if (ticks == 0) {
        ticks = 1;
    }
    return ticks;
}

void arm_idle_timer_locked(RunQueue* rq) {
    if (rq->runnable_heap.size != 0) {
        rq->idle_timer_arms.fetch_add(1, std::memory_order_relaxed);
        apic::one_shot_timer(1);
        return;
    }

    uint64_t const DEADLINE_US = rq->next_wait_deadline_us;
    if (DEADLINE_US == 0) {
        if (idle_rebalance_probe_needed_for_idle(cpu::current_cpu())) {
            rq->idle_timer_arms.fetch_add(1, std::memory_order_relaxed);
            apic::one_shot_timer(idle_timer_ticks_for_delta_us(IDLE_REBALANCE_PROBE_US));
            return;
        }
        rq->idle_timer_disarms.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    uint64_t const NOW_US = time::get_us();
    uint64_t const DELTA_US = DEADLINE_US > NOW_US ? DEADLINE_US - NOW_US : 1;

    rq->idle_timer_arms.fetch_add(1, std::memory_order_relaxed);
    apic::one_shot_timer(idle_timer_ticks_for_delta_us(DELTA_US));
}

// Enter the kernel idle loop on the idle task's stack. Does NOT return.
[[noreturn]] void enter_idle_loop(RunQueue* rq) {
    if (rq == nullptr) {
        dbg::logger<"sched">::error("enter_idle_loop without runqueue");
        hcf();
    }

    task::Task* idle_task = nullptr;
    task::Task* current_task = nullptr;
    task::Task* handoff_task = nullptr;
    run_queues->this_cpu_locked_void([&](RunQueue* locked_rq) {
        idle_task = locked_rq->idle_task;
        current_task = locked_rq->current_task;
        handoff_task = locked_rq->handoff_task;
        if (is_publishable_idle_task(idle_task)) {
            reserve_handoff_task_locked(locked_rq, idle_task, time::get_us());
        }
    });

    if (!is_publishable_idle_task(idle_task)) {
        dbg::logger<"sched">::error(
            "enter_idle_loop without valid idle task: rq=%p idle=%p name=%s pid=%lu type=%u state=%u queue=%d stack=0x%lx "
            "scratch=0x%lx thread=%p pagemap=%p current=%p handoff=%p",
            static_cast<void*>(rq), static_cast<void*>(idle_task), task_name_for_log(idle_task), idle_task != nullptr ? idle_task->pid : 0,
            task_type_for_log(idle_task), task_state_for_log(idle_task), task_queue_for_log(idle_task),
            idle_task != nullptr ? idle_task->context.syscall_kernel_stack : 0,
            idle_task != nullptr ? idle_task->context.syscall_scratch_area : 0,
            idle_task != nullptr ? static_cast<void*>(idle_task->thread) : nullptr,
            idle_task != nullptr ? static_cast<void*>(idle_task->pagemap) : nullptr, static_cast<void*>(current_task),
            static_cast<void*>(handoff_task));
        hcf();
    }

    rq->is_idle.store(true, std::memory_order_release);
    arm_idle_timer_for_this_cpu();

    uint64_t const IDLE_STACK = idle_task->context.syscall_kernel_stack;

    // CRITICAL: Switch CR3 to the kernel pagemap before entering idle.
    // When a user task exits and we transition to idle, CR3 still points to the
    // exited task's pagemap. If GC later frees that pagemap's PML4 page and another
    // CPU reuses it, our HHDM mappings break (the kernel half of the PML4 gets
    // overwritten). This caused stack corruption crashes under stress.
    mm::virt::switch_to_kernel_pagemap();

    restore_kernel_gs_for_idle();

    wos_enterIdleStack(IDLE_STACK);
}

// IPI handler for scheduler wake-up.
// NOTE: Do NOT call apic::eoi() here - the generic interrupt_handler already
// sends EOI after dispatching interruptHandlers[].
void scheduler_wake_handler([[maybe_unused]] cpu::GPRegs gpr, [[maybe_unused]] gates::InterruptFrame frame) {
    auto* rq = run_queues->this_cpu();
    // Consume the coalescing bit before looking at the run queue.  If this IPI
    // was stale/empty, a racing later wake_cpu() must be able to send a fresh
    // IPI instead of being suppressed by our old pending flag.
    rq->resched_timer_pending.store(false, std::memory_order_release);
    if (rq->runnable_heap.size > 0) {
        rq->is_idle.store(false, std::memory_order_release);
        // Wake IPIs are the rescue path for CPUs halted in the idle loop.  The
        // idle loop does not inspect the runqueue after an interrupt returns;
        // it immediately executes hlt again.  Program the local APIC timer
        // directly here instead of going through the coalesced same-CPU poke
        // path, because a stale pending bit would otherwise strand runnable
        // work on a tickless idle CPU.
        rq->resched_timer_pending.store(true, std::memory_order_release);
        note_local_reschedule_timer_poke();
        apic::one_shot_timer(1);
    }
}

auto consume_precharged_syscall_time(task::Task* task, uint64_t delta_us) -> uint64_t {
    if (task == nullptr || task->precharged_syscall_time_us == 0 || delta_us == 0) {
        return 0;
    }

    uint64_t const CONSUMED_US = std::min(task->precharged_syscall_time_us, delta_us);
    task->precharged_syscall_time_us -= CONSUMED_US;
    return CONSUMED_US;
}

auto consume_irq_time_for_runtime(RunQueue* rq, uint64_t delta_us) -> uint64_t {
    if (rq == nullptr || delta_us == 0) {
        return 0;
    }

    uint64_t const IRQ_US = rq->irq_uncharged_us.exchange(0, std::memory_order_acq_rel);
    uint64_t const CONSUMED_US = std::min(IRQ_US, delta_us);
    if (IRQ_US > CONSUMED_US) {
        rq->irq_uncharged_us.fetch_add(IRQ_US - CONSUMED_US, std::memory_order_release);
    }
    return CONSUMED_US;
}

void account_cpu_runtime_delta(RunQueue* rq, task::Task* task, uint64_t delta_us, bool in_kernel_mode, bool charge_running_time) {
    if (rq == nullptr || delta_us == 0) {
        return;
    }

    if (task == nullptr || task->type == task::TaskType::IDLE || !charge_running_time) {
        rq->cpu_idle_us.fetch_add(delta_us, std::memory_order_relaxed);
        return;
    }

    if (in_kernel_mode) {
        rq->cpu_system_us.fetch_add(delta_us, std::memory_order_relaxed);
        return;
    }

    if (task->sched_nice > 0) {
        rq->cpu_nice_us.fetch_add(delta_us, std::memory_order_relaxed);
    } else {
        rq->cpu_user_us.fetch_add(delta_us, std::memory_order_relaxed);
    }
}

void add_runtime_delta_to_cpu_snapshot(CpuAccountingSnapshot& snapshot, task::Task const* task, uint64_t delta_us, bool in_kernel_mode,
                                       bool charge_running_time) {
    if (delta_us == 0) {
        return;
    }

    if (task == nullptr || task->type == task::TaskType::IDLE || !charge_running_time) {
        snapshot.idle_us += delta_us;
        return;
    }

    if (in_kernel_mode) {
        snapshot.system_us += delta_us;
        return;
    }

    if (task->sched_nice > 0) {
        snapshot.nice_us += delta_us;
    } else {
        snapshot.user_us += delta_us;
    }
}

auto task_in_kernel_mode_for_cpu_snapshot(task::Task const* task) -> bool {
    if (task == nullptr || task->type != task::TaskType::PROCESS || task->is_voluntary_blocked()) {
        return true;
    }
    return task->context.frame.cs != desc::gdt::GDT_USER_CS;
}

auto cpu_accounting_snapshot_locked(RunQueue* rq, uint64_t now_us) -> CpuAccountingSnapshot {
    if (rq == nullptr) {
        return CpuAccountingSnapshot{};
    }

    CpuAccountingSnapshot snapshot{
        .user_us = rq->cpu_user_us.load(std::memory_order_relaxed),
        .nice_us = rq->cpu_nice_us.load(std::memory_order_relaxed),
        .system_us = rq->cpu_system_us.load(std::memory_order_relaxed),
        .idle_us = rq->cpu_idle_us.load(std::memory_order_relaxed),
        .iowait_us = rq->cpu_iowait_us.load(std::memory_order_relaxed),
        .irq_us = rq->cpu_irq_us.load(std::memory_order_relaxed),
        .softirq_us = rq->cpu_softirq_us.load(std::memory_order_relaxed),
        .steal_us = rq->cpu_steal_us.load(std::memory_order_relaxed),
    };

    if (now_us <= rq->last_tick_us) {
        return snapshot;
    }

    auto const* current = rq->current_task;
    uint64_t const LIVE_DELTA_US = now_us - rq->last_tick_us;
    bool const CHARGE_RUNNING_TIME = current != nullptr && current->type != task::TaskType::IDLE && !current->is_voluntary_blocked();
    add_runtime_delta_to_cpu_snapshot(snapshot, current, LIVE_DELTA_US, task_in_kernel_mode_for_cpu_snapshot(current), CHARGE_RUNNING_TIME);
    return snapshot;
}

void account_task_runtime_delta(task::Task* task, uint64_t now_us, uint64_t delta_us, bool in_kernel_mode, bool charge_running_time);

auto account_runtime_slice(RunQueue* rq, task::Task* task, uint64_t now_us, uint64_t delta_us, bool in_kernel_mode,
                           bool charge_running_time) -> uint64_t {
    uint64_t const IRQ_US = consume_irq_time_for_runtime(rq, delta_us);
    uint64_t const CHARGEABLE_US = delta_us - IRQ_US;
    account_cpu_runtime_delta(rq, task, CHARGEABLE_US, in_kernel_mode, charge_running_time);
    account_task_runtime_delta(task, now_us, CHARGEABLE_US, in_kernel_mode, charge_running_time);
    return CHARGEABLE_US;
}

auto charge_syscall_time_until(task::Task* task, uint64_t now_us, bool finish) -> bool {
    if (task == nullptr || task->type != task::TaskType::PROCESS || task->syscall_account_start_us == 0) {
        return false;
    }

    uint64_t const START_US = task->syscall_account_start_us;
    if (now_us > START_US) {
        uint64_t const ELAPSED_US = now_us - START_US;
        task->system_time_us += ELAPSED_US;
        task->precharged_syscall_time_us += ELAPSED_US;
    }
    task->syscall_account_start_us = finish ? 0 : now_us;
    return true;
}

void account_task_runtime_delta(task::Task* task, uint64_t now_us, uint64_t delta_us, bool in_kernel_mode, bool charge_running_time) {
    if (task == nullptr || task->type == task::TaskType::IDLE || delta_us == 0) {
        return;
    }

    uint64_t const PRECHARGED_SYSTEM_US = consume_precharged_syscall_time(task, delta_us);
    uint64_t const UNCHARGED_US = delta_us - PRECHARGED_SYSTEM_US;
    if (!charge_running_time || UNCHARGED_US == 0) {
        return;
    }

    if (!in_kernel_mode) {
        task->user_time_us += UNCHARGED_US;
        return;
    }

    if (task->type == task::TaskType::PROCESS && task->syscall_account_start_us != 0) {
        uint64_t active_syscall_us = 0;
        if (now_us > task->syscall_account_start_us) {
            active_syscall_us = std::min(now_us - task->syscall_account_start_us, UNCHARGED_US);
        }
        if (active_syscall_us != 0) {
            task->system_time_us += active_syscall_us;
            task->syscall_account_start_us = now_us;
        }
        if (UNCHARGED_US > active_syscall_us) {
            task->user_time_us += UNCHARGED_US - active_syscall_us;
        }
        return;
    }

    task->system_time_us += UNCHARGED_US;
}

}  // namespace

void request_local_timer_recheck() { request_local_reschedule(); }

void set_task_nice(task::Task* task, int nice) { set_task_nice_impl(task, nice); }

// Wake IPI - breaks a CPU out of hlt regardless of scheduler idle state.
// Used by NAPI to wake worker threads that sleep via sti;hlt as the
// current_task (so is_idle is false, and wake_idle_cpu would skip them).
void wake_cpu(uint64_t cpu_no, WakeCpuMode mode) {
    if (wake_ipi_vector == 0) {
        return;
    }
    if (cpu_no >= smt::get_core_count()) {
        return;
    }
    if (cpu_no == cpu::current_cpu()) {
        return;
    }

    apic::IPIConfig const IPI{
        .vector = wake_ipi_vector,
        .delivery_mode = apic::IPIDeliveryMode::FIXED,
        .destination_mode = apic::IPIDestinationMode::PHYSICAL,
        .level = apic::IPILevel::ASSERT,
        .trigger_mode = apic::IPITriggerMode::EDGE,
        .destination_shorthand = apic::IPIDestinationShorthand::NONE,
    };

    auto* rq = run_queues != nullptr ? run_queues->that_cpu(cpu_no) : nullptr;
    if (rq != nullptr) {
        // The wake IPI's handler only arms a near scheduler timer, so repeated
        // remote wakeups before that timer fires can normally be coalesced.
        // Do not coalesce against a stale pending bit when the target is halted:
        // an idle CPU or a current task in a voluntary hlt needs the IPI itself
        // to escape the wait point if the earlier timer/IPI was lost or stale.
        auto* current = rq->current_task;
        bool const HALTED_TARGET = rq->is_idle.load(std::memory_order_acquire) ||
                                   (current != nullptr && (current->is_voluntary_blocked() || current->wants_block));
        if (mode == WakeCpuMode::COALESCE && rq->resched_timer_pending.exchange(true, std::memory_order_acq_rel)) {
            if (!HALTED_TARGET) {
                rq->wake_ipis_coalesced.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        } else if (mode == WakeCpuMode::FORCE) {
            rq->resched_timer_pending.store(true, std::memory_order_release);
        }
        rq->wake_ipis_sent.fetch_add(1, std::memory_order_relaxed);
    }

    uint32_t const LAPIC_ID = smt::get_cpu(cpu_no).lapic_id;
    apic::send_ipi(IPI, LAPIC_ID);
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
    wake_ipi_vector = gates::allocate_vector();
    if (wake_ipi_vector != 0) {
        gates::set_interrupt_handler(wake_ipi_vector, scheduler_wake_handler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void setup_queues() {
    // This is the portion of init() after smt::init() and EpochManager::init()
    // Used by the init dependency system for finer-grained control
    run_queues = new smt::PerCpuCrossAccess<RunQueue>();
    scheduler_task_context_ready_mask.store(0, std::memory_order_release);

    // Allocate a dedicated vector for scheduler wake IPIs.
    wake_ipi_vector = gates::allocate_vector();
    if (wake_ipi_vector != 0) {
        gates::set_interrupt_handler(wake_ipi_vector, scheduler_wake_handler);
        dbg::log("Registered scheduler wake IPI handler at vector 0x%x", wake_ipi_vector);
    } else {
        dbg::log("WARNING: No free interrupt vector for scheduler wake IPI");
    }
}

void percpu_init() {
    auto cpu_no = cpu::current_cpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu_no);
    // RunQueue constructor already initializes all fields (heap, lists, counters).
    // Set the initial HPET timestamp so the first timer tick has a valid delta.
    run_queues->this_cpu()->last_tick_us = time::get_us();
}

// ============================================================================
// Task posting
// ============================================================================

auto post_task(task::Task* task) -> bool { return post_task_for_cpu_impl(cpu::current_cpu(), task, false); }

bool post_task_for_cpu(uint64_t cpu_no, task::Task* task) { return post_task_for_cpu_impl(cpu_no, task, false); }

auto post_task_pinned_cpu(uint64_t cpu_no, task::Task* task) -> bool {
    task->cpu_pinned = true;
    return post_task_for_cpu(cpu_no, task);
}

auto post_task_waiting(task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }
    auto task_state = task->state.load(std::memory_order_acquire);
    if (task_state == task::TaskState::DEAD || task_state == task::TaskState::EXITING || task->gc_queued.load(std::memory_order_acquire)) {
        return false;
    }

    uint64_t target_cpu = task->cpu;
    if (target_cpu >= smt::get_core_count()) {
        target_cpu = cpu::current_cpu();
    }
    task->cpu = target_cpu;

    if (task->start_time_us == 0) {
        task->start_time_us = time::get_us();
    }

    bool const NEEDS_REGISTRATION = task->sched_queue == task::Task::sched_queue::NONE;
    __atomic_thread_fence(__ATOMIC_RELEASE);

    if (NEEDS_REGISTRATION && task->pid > 0) {
        if (!pid_table_insert(task)) {
            return false;
        }
        if (!active_list_insert(task)) {
            pid_table_remove(task->pid);
            return false;
        }
    }

    bool parked = false;
    run_queues->with_lock_void(target_cpu, [task, &parked, target_cpu](RunQueue* rq) {
        uint64_t const NOW_US = time::get_us();
        if (task->sched_queue == task::Task::sched_queue::WAITING) {
            parked = true;
            return;
        }

        if (task->sched_queue == task::Task::sched_queue::NONE) {
            task->last_sleep_start_us = NOW_US;
            task->sched_queue = task::Task::sched_queue::WAITING;
            wait_list_push_locked(rq, task);
            parked = true;
            return;
        }

        // Remote exec proxy setup can hand us a task that is already runnable
        // but has not actually run its deferred wait path yet. Parking that
        // task here avoids waiting for it to get a full timeslice just to
        // enter wki_execve_proxy.
        if (!rq->runnable_heap.contains(task) || runqueue_task_is_reserved_locked(rq, task)) {
            return;
        }

        task->last_run_us = task->slice_used_ns / 1000U;
        note_perf_wait_callsite(task, task->context.frame.rip);
        task->last_sleep_start_us = NOW_US;
        uint64_t const WAKE_AT_US = task->wake_at_us;
        remove_from_sums(rq, task);
        rq->runnable_heap.remove(task);
        task->sched_queue = task::Task::sched_queue::WAITING;
        wait_list_push_locked(rq, task);
        perf::record_sleep(static_cast<uint32_t>(target_cpu), task->pid, WAKE_AT_US, perf_sleep_flags(WAKE_AT_US), task->last_run_us,
                           perf_wait_callsite(task), task->wait_channel);
        parked = true;
    });

    return parked;
}

auto post_task_balanced(task::Task* task) -> bool {
    if (task == nullptr) {
        log_rejected_task_publication("null balanced task", cpu::current_cpu(), task);
        return false;
    }

    // D17: Try remote placement if WKI is active and task is a user process
    if (wki_try_remote_placement_fn != nullptr && task->type == task::TaskType::PROCESS) {
        if (wki_try_remote_placement_fn(task)) {
            return true;  // Successfully submitted remotely
        }
    }

    uint64_t const TARGET_CPU = reserve_least_loaded_cpu(task);
    return post_task_for_cpu_impl(TARGET_CPU, task, true);
}

// ============================================================================
// Current task access
// ============================================================================

auto get_current_task() -> task::Task* { return run_queues->this_cpu()->current_task; }

auto get_return_task() -> task::Task* {
    auto* rq = run_queues->this_cpu();
    return rq->handoff_task != nullptr ? rq->handoff_task : rq->current_task;
}

auto current_cpu_for_task(task::Task* task) -> uint64_t {
    if (task == nullptr || run_queues == nullptr) {
        return UINT64_MAX;
    }

    // Query-only helper: do not repair task->cpu here.  Owner repairs must stay
    // explicit in migration/wakeup paths so stale-owner bugs remain visible.
    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        bool found = false;
        run_queues->with_lock_void(cpu_no, [task, &found](RunQueue* rq) {
            if (runqueue_task_is_reserved_locked(rq, task)) {
                found = true;
            }
        });
        if (found) {
            return cpu_no;
        }
    }
    return UINT64_MAX;
}

auto owner_cpu_for_task(task::Task* task) -> uint64_t {
    if (task == nullptr || run_queues == nullptr) {
        return UINT64_MAX;
    }

    // Query-only helper: callers may use this for diagnostics or policy
    // decisions, but it must not silently rewrite scheduler ownership.
    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        bool found = false;
        run_queues->with_lock_void(cpu_no, [task, &found](RunQueue* rq) {
            if (runqueue_task_is_reserved_locked(rq, task) || rq->runnable_heap.contains(task)) {
                found = true;
                return;
            }
            for (task::Task* cur = rq->wait_list.head; cur != nullptr; cur = cur->sched_next) {
                if (cur == task) {
                    found = true;
                    return;
                }
            }
        });
        if (found) {
            return cpu_no;
        }
    }
    return UINT64_MAX;
}

namespace {
struct TaskOwnerMoveResult {
    bool moved{false};
    bool runnable{false};
};

auto hinted_or_scanned_owner_cpu(task::Task* task) -> uint64_t {
    uint64_t const CORE_COUNT = smt::get_core_count();
    uint64_t const HINTED_CPU = task != nullptr ? task->cpu : UINT64_MAX;
    if (task == nullptr || run_queues == nullptr) {
        return UINT64_MAX;
    }
    if (HINTED_CPU < CORE_COUNT) {
        bool hinted_owner = false;
        run_queues->with_lock_void(HINTED_CPU, [task, &hinted_owner](RunQueue* rq) { hinted_owner = runqueue_owns_task_locked(rq, task); });
        if (hinted_owner) {
            return HINTED_CPU;
        }
    }
    return owner_cpu_for_task(task);
}

auto move_task_owner_to_cpu(task::Task* task, uint64_t target_cpu, bool pin_to_target) -> TaskOwnerMoveResult {
    TaskOwnerMoveResult result{};
    if (task == nullptr || run_queues == nullptr || target_cpu >= smt::get_core_count()) {
        return result;
    }
    if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return result;
    }

    uint64_t const OWNER_CPU = hinted_or_scanned_owner_cpu(task);
    if (OWNER_CPU == UINT64_MAX) {
        return result;
    }

    bool const WAS_PINNED = task->cpu_pinned;
    run_queues->with_two_locks_void(
        OWNER_CPU, target_cpu, [task, OWNER_CPU, target_cpu, pin_to_target, WAS_PINNED, &result](RunQueue* owner_rq, RunQueue* target_rq) {
            if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                return;
            }

            if (runqueue_task_is_reserved_locked(owner_rq, task)) {
                if (owner_rq != target_rq) {
                    return;
                }
                task->cpu = target_cpu;
                if (pin_to_target) {
                    task->cpu_pinned = true;
                }
                result.moved = true;
                return;
            }

            bool const IN_HEAP = owner_rq->runnable_heap.contains(task);
            bool const IN_WAIT = wait_list_contains_locked(owner_rq, task);
            if (!IN_HEAP && !IN_WAIT) {
                return;
            }

            if (owner_rq == target_rq) {
                task->cpu = target_cpu;
                if (pin_to_target) {
                    task->cpu_pinned = true;
                }
                result.moved = true;
                result.runnable = IN_HEAP;
                return;
            }

            bool moved_waiting = false;
            bool moved_runnable = false;
            if (IN_WAIT) {
                moved_waiting = wait_list_remove_locked(owner_rq, task);
            }
            if (IN_HEAP) {
                moved_runnable = owner_rq->runnable_heap.remove(task);
                if (moved_runnable) {
                    remove_from_sums(owner_rq, task);
                    moved_waiting = false;
                }
            }
            if (!moved_waiting && !moved_runnable) {
                return;
            }

            task->cpu = target_cpu;
            if (pin_to_target) {
                task->cpu_pinned = true;
            }

            if (moved_waiting) {
                task->sched_queue = task::Task::sched_queue::WAITING;
                wait_list_push_locked(target_rq, task);
                result.moved = true;
                return;
            }

            int64_t const FAIR_VRUNTIME = std::max(target_rq->min_vruntime, compute_avg_vruntime(target_rq));
            task->vruntime = std::max(task->vruntime, FAIR_VRUNTIME);
            task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);
            if (publish_runnable_task_locked(target_rq, task, "move-target")) {
                result.moved = true;
                result.runnable = true;
                return;
            }

            task->cpu = OWNER_CPU;
            task->cpu_pinned = WAS_PINNED;
            (void)publish_runnable_task_locked(owner_rq, task, "move-rollback");
        });

    if (result.moved && result.runnable) {
        if (target_cpu == cpu::current_cpu()) {
            request_local_reschedule();
        } else {
            wake_cpu(target_cpu);
        }
    }
    return result;
}
}  // namespace

auto migrate_task_to_cpu(task::Task* task, uint64_t target_cpu) -> bool { return move_task_owner_to_cpu(task, target_cpu, false).moved; }

auto pin_task_to_cpu(task::Task* task, uint64_t target_cpu) -> bool { return move_task_owner_to_cpu(task, target_cpu, true).moved; }

[[clang::no_sanitize("kernel-address")]] auto can_query_current_task() -> bool {
    if (run_queues == nullptr) {
        return false;
    }
    uint64_t const CPU_NO = cpu::get_current_cpu_id_safe();
    if (CPU_NO >= 64) {
        return false;
    }
    return (scheduler_task_context_ready_mask.load(std::memory_order_acquire) & (1ULL << CPU_NO)) != 0;
}

auto has_run_queues() -> bool { return run_queues != nullptr; }

void begin_syscall_accounting() {
    if (!can_query_current_task()) {
        return;
    }
    auto* task = get_current_task();
    if (task == nullptr || task->type != task::TaskType::PROCESS) {
        return;
    }
    task->syscall_account_start_us = time::get_us();
}

void finish_syscall_accounting() {
    if (!can_query_current_task()) {
        return;
    }
    charge_syscall_time_until(get_current_task(), time::get_us(), true);
}

auto pause_syscall_accounting() -> bool {
    if (!can_query_current_task()) {
        return false;
    }
    return charge_syscall_time_until(get_current_task(), time::get_us(), true);
}

void resume_syscall_accounting() {
    if (!can_query_current_task()) {
        return;
    }
    auto* task = get_current_task();
    if (task == nullptr || task->type != task::TaskType::PROCESS || task->syscall_account_start_us != 0) {
        return;
    }
    task->syscall_account_start_us = time::get_us();
}

[[clang::no_sanitize("kernel-address")]] auto preempt_disable_token_at(uint64_t caller) -> task::Task* {
    auto* task = current_task_for_preempt();
    if (task == nullptr) {
        return nullptr;
    }

    if (task->preempt_disable_depth == 0) {
        task->preempt_disable_start_us = time::get_us();
        task->preempt_disable_owner = caller;
    }
    task->preempt_disable_depth++;
    return task;
}

[[clang::no_sanitize("kernel-address")]] void preempt_disable_at(uint64_t caller) { (void)preempt_disable_token_at(caller); }

[[clang::no_sanitize("kernel-address")]] void preempt_disable() {
    preempt_disable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
}

[[clang::no_sanitize("kernel-address")]] void preempt_enable_token_at(task::Task* task, uint64_t caller) {
    if (task == nullptr) {
        return;
    }

    if (task->preempt_disable_depth == 0) {
        if (desc::idt::is_idt_ready()) {
            uint64_t const COUNT = g_preempt_block_warnings.fetch_add(1, std::memory_order_relaxed);
            if ((COUNT & 0xffULL) == 0) {
                preempt_log::warn("preempt_enable without disable: pid=%lu caller=0x%llx rip=0x%llx", task->pid,
                                  static_cast<unsigned long long>(caller), static_cast<unsigned long long>(task->context.frame.rip));
            }
        }
        return;
    }

    task->preempt_disable_depth--;
    if (task->preempt_disable_depth != 0) {
        return;
    }

    if (task->preempt_disable_start_us != 0) {
        uint64_t const NOW_US = time::get_us();
        if (NOW_US >= task->preempt_disable_start_us) {
            uint64_t const HELD_US = NOW_US - task->preempt_disable_start_us;
            task->preempt_disable_max_us = std::max(HELD_US, task->preempt_disable_max_us);
        }
        task->preempt_disable_start_us = 0;
    }
    task->preempt_disable_owner = 0;

    if (task->preempt_pending) {
        task->preempt_pending = false;
        if (run_queues != nullptr) {
            request_local_reschedule();
        }
    }
}

[[clang::no_sanitize("kernel-address")]] void preempt_enable_at(uint64_t caller) {
    preempt_enable_token_at(current_task_for_preempt(), caller);
}

[[clang::no_sanitize("kernel-address")]] void preempt_enable() {
    preempt_enable_at(reinterpret_cast<uint64_t>(__builtin_return_address(0)));
}

[[clang::no_sanitize("kernel-address")]] auto preempt_count() -> uint32_t {
    auto* task = current_task_for_preempt();
    return task != nullptr ? task->preempt_disable_depth : 0;
}

[[clang::no_sanitize("kernel-address")]] auto preemptible() -> bool { return preempt_count() == 0; }

[[clang::no_sanitize("kernel-address")]] void note_preempt_disabled_block(const char* op, uint64_t perf_callsite) {
    auto* task = current_task_for_preempt();
    if (task == nullptr || task->preempt_disable_depth == 0) {
        return;
    }

    if (!desc::idt::is_idt_ready()) {
        return;
    }

    uint64_t const COUNT = g_preempt_block_warnings.fetch_add(1, std::memory_order_relaxed);
    if ((COUNT & 0x3fULL) == 0) {
        preempt_log::warn("blocking with preemption disabled: op=%s pid=%lu depth=%u owner=0x%llx callsite=0x%llx",
                          op != nullptr ? op : "?", task->pid, task->preempt_disable_depth,
                          static_cast<unsigned long long>(task->preempt_disable_owner), static_cast<unsigned long long>(perf_callsite));
    }
}

void remove_current_task() {
    task::Task* task_to_gc = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
        auto* task = rq->current_task;
        if (task == nullptr) {
            return nullptr;
        }

        // Remove from heap if present
        if (task->sched_queue == task::Task::sched_queue::RUNNABLE && rq->runnable_heap.contains(task)) {
            remove_from_sums(rq, task);
            rq->runnable_heap.remove(task);
        }

        // Keep current_task pointing at the live stack owner until the final
        // return boundary moves the CPU away from this task's stack.
        task->sched_queue = task::Task::sched_queue::NONE;
        return task;
    });

    if (is_dead_gc_candidate_task(task_to_gc)) {
        insert_into_dead_list(task_to_gc);
    }
}

// ============================================================================
// processTasks - timer interrupt hot path (EEVDF)
// ============================================================================

void kern_wake(task::Task* task) {
    if (task == nullptr) {
        return;
    }

    // Always go through reschedule_task_for_cpu - it handles all cases safely
    // under locks: task RUNNABLE (no-op if currentTask), task WAITING (move to heap),
    // or task migrated to another CPU.
    // The fast path inside reschedule_task_for_cpu checks task->cpu first (O(1) common case).
    uint64_t preferred_cpu = task->cpu;
    uint64_t const NCPUS = smt::get_core_count();
    if (preferred_cpu >= NCPUS) {
        preferred_cpu = get_least_loaded_cpu();
    }
    reschedule_task_for_cpu(preferred_cpu, task);
}

auto kernel_threads_shutdown_requested() -> bool { return kernel_thread_shutdown_requested.load(std::memory_order_acquire); }

void maybe_exit_current_kernel_thread_for_shutdown() {
    if (!kernel_threads_shutdown_requested()) {
        return;
    }

    auto* task = get_current_task();
    if (task == nullptr || task->type != task::TaskType::DAEMON || !task->kernel_shutdown_requested.load(std::memory_order_acquire)) {
        return;
    }

    if (task->preempt_disable_depth != 0 || !interrupts_enabled()) {
        task->preempt_pending = true;
        return;
    }

    task->wakeup_pending.store(false, std::memory_order_release);
    task->deferred_task_switch = false;
    task->yield_switch = false;
    task->wants_block = false;
    task->wake_at_us = 0;
    task->set_voluntary_blocked(false);
    task->clear_wait_channel();
    ker::syscall::process::wos_proc_exit(0);
    for (;;) {
        asm volatile("hlt" ::: "memory");
    }
}

namespace {
void wait_for_kernel_shutdown_progress(task::Task* current) {
    (void)current;
    kern_yield();
}

void wake_kernel_thread_for_shutdown(task::Task* task) {
    if (task == nullptr || task->type != task::TaskType::DAEMON) {
        return;
    }

    uint64_t const TARGET_CPU = task->cpu;
    if (TARGET_CPU >= smt::get_core_count()) {
        return;
    }

    bool should_poke = false;
    run_queues->with_lock_void(TARGET_CPU, [task, &should_poke](RunQueue* rq) {
        task->wakeup_pending.store(true, std::memory_order_release);
        task->wants_block = false;
        task->wake_at_us = 0;
        task->set_voluntary_blocked(false);
        task->clear_wait_channel();

        if (runqueue_task_is_reserved_locked(rq, task)) {
            should_poke = true;
            return;
        }

        if (rq->runnable_heap.contains(task)) {
            repair_stale_wait_membership_locked(rq, task);
            should_poke = true;
            return;
        }

        bool removed_wait = false;
        while (wait_list_remove_locked(rq, task)) {
            removed_wait = true;
        }
        if (!removed_wait && task->sched_queue != task::Task::sched_queue::WAITING) {
            return;
        }

        finish_wait_metadata_for_runqueue(task);
        task->vruntime = std::max(task->vruntime, compute_wakeup_floor_vruntime(rq, task, time::get_us(), task::WaitChannelKind::GENERIC));
        task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);
        task->slice_used_ns = 0;
        if (publish_runnable_task_locked(rq, task, "kernel-thread-shutdown")) {
            should_poke = true;
        }
    });

    if (!should_poke) {
        return;
    }
    if (TARGET_CPU == cpu::current_cpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(TARGET_CPU, WakeCpuMode::FORCE);
    }
}
}  // namespace

auto request_kernel_threads_shutdown(uint64_t timeout_us) -> KernelThreadShutdownResult {
    kernel_thread_shutdown_requested.store(true, std::memory_order_release);

    auto* current = get_current_task();
    bool const WAKE_TARGETS = timeout_us != 0;
    KernelThreadShutdownResult result{};
    if (!WAKE_TARGETS) {
        return result;
    }

    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* task = get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }

        if (is_kernel_thread_shutdown_target(task, current)) {
            bool const WAS_REQUESTED = task->kernel_shutdown_requested.exchange(true, std::memory_order_acq_rel);
            if (!WAS_REQUESTED) {
                ++result.requested;
            }
            task->wakeup_pending.store(true, std::memory_order_release);
            wake_kernel_thread_for_shutdown(task);
        }
        task->release();
    }

    uint64_t const START_US = time::get_us();
    result.remaining = count_kernel_thread_shutdown_targets(current);
    while (result.remaining != 0 && time::get_us() - START_US < timeout_us) {
        wait_for_kernel_shutdown_progress(current);
        result.remaining = count_kernel_thread_shutdown_targets(current);
    }
    return result;
}

namespace {
constexpr uint32_t EVENT_WAKE_REBALANCE_LOAD_GAP = 16;

inline auto task_allowed_cpu_mask(task::Task const* task, uint64_t core_count) -> uint64_t {
    uint64_t const ALL_MASK = (core_count >= 64) ? UINT64_MAX : ((1ULL << core_count) - 1ULL);
    uint64_t allowed_mask = task != nullptr ? task->domain_mask : ALL_MASK;
    allowed_mask &= ALL_MASK;
    return allowed_mask != 0 ? allowed_mask : ALL_MASK;
}

inline auto event_wake_can_rebalance_process(task::Task const* task) -> bool {
    if (task == nullptr || task->type != task::TaskType::PROCESS) {
        return false;
    }
    if (task->cpu_pinned || task->domain_hard) {
        return false;
    }
    if (task->thread == nullptr || task->pagemap == nullptr) {
        return false;
    }
    if (task->sched_queue != task::Task::sched_queue::WAITING) {
        return false;
    }
    return !task->wait_channel_is(task::WaitChannelKind::WKI_EXECVE_PROXY);
}

inline auto event_wake_rebalance_target_cpu(task::Task const* task, uint64_t preferred_cpu) -> uint64_t {
    if (!event_wake_can_rebalance_process(task) || run_queues == nullptr) {
        return preferred_cpu;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    if (CORE_COUNT <= 1) {
        return preferred_cpu;
    }

    uint64_t const ALLOWED_MASK = task_allowed_cpu_mask(task, CORE_COUNT);
    if (preferred_cpu >= CORE_COUNT || !cpu_mask_contains(ALLOWED_MASK, preferred_cpu)) {
        return get_least_loaded_cpu_for_task(task);
    }

    uint64_t const BEST_CPU = get_least_loaded_cpu_for_task(task);
    if (BEST_CPU >= CORE_COUNT || BEST_CPU == preferred_cpu) {
        return preferred_cpu;
    }

    uint32_t const PREFERRED_LOAD = cached_effective_load_for_cpu(preferred_cpu, task::TaskType::PROCESS);
    uint32_t const BEST_LOAD = cached_effective_load_for_cpu(BEST_CPU, task::TaskType::PROCESS);
    if (PREFERRED_LOAD == UINT32_MAX || BEST_LOAD == UINT32_MAX) {
        return preferred_cpu;
    }
    if (PREFERRED_LOAD > BEST_LOAD && PREFERRED_LOAD - BEST_LOAD > EVENT_WAKE_REBALANCE_LOAD_GAP) {
        return BEST_CPU;
    }
    return preferred_cpu;
}
}  // namespace

auto event_wake_target_cpu(const task::Task* task, uint64_t waker_cpu) -> uint64_t {
    if (task == nullptr) {
        return waker_cpu;
    }

    bool const WAITING = task->sched_queue == task::Task::sched_queue::WAITING;
    bool const VOLUNTARY_BLOCK = task->is_voluntary_blocked();
    if (event_wake_prefers_waker_cpu(task->cpu_pinned, WAITING, VOLUNTARY_BLOCK)) {
        return event_wake_rebalance_target_cpu(task, waker_cpu);
    }
    return task->cpu;
}

void wake_task_from_event_on_cpu(task::Task* task, uint64_t target_cpu, EventWakeDeferredSwitch deferred_switch) {
    if (task == nullptr) {
        return;
    }
    bool const CANCEL_DEFERRED_SWITCH = event_wake_cancels_deferred_switch(deferred_switch);
    if (CANCEL_DEFERRED_SWITCH) {
        task->deferred_task_switch = false;
    }
    // Preserve event-before-park wakeups even if the task is runnable/current
    // when reschedule observes it. Wait loops tolerate this as a spurious wake
    // and recheck readiness before sleeping again.
    task->wakeup_pending.store(true, std::memory_order_release);
    uint64_t cpu = target_cpu;
    if (cpu >= smt::get_core_count()) {
        cpu = get_least_loaded_cpu();
    }
    reschedule_task_for_cpu(cpu, task);
}

void wake_task_from_event(task::Task* task, EventWakeDeferredSwitch deferred_switch) {
    if (task == nullptr) {
        return;
    }
    wake_task_from_event_on_cpu(task, event_wake_target_cpu(task, cpu::current_cpu()), deferred_switch);
}

auto wake_task_by_pid_from_event_on_cpu(uint64_t pid, uint64_t target_cpu, EventWakeDeferredSwitch deferred_switch) -> bool {
    if (pid == 0) {
        return false;
    }

    auto* task = find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return false;
    }

    wake_task_from_event_on_cpu(task, target_cpu, deferred_switch);
    task->release();
    return true;
}

auto wake_task_by_pid_from_event(uint64_t pid, EventWakeDeferredSwitch deferred_switch) -> bool {
    if (pid == 0) {
        return false;
    }

    auto* task = find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return false;
    }

    wake_task_from_event(task, deferred_switch);
    task->release();
    return true;
}

auto debug_stop_task(task::Task* task) -> bool {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return false;
    }

    bool found = false;
    bool current_on_cpu = false;
    uint64_t current_cpu_of_task = UINT64_MAX;

    auto try_stop_on_cpu = [task, &found, &current_on_cpu, &current_cpu_of_task](uint64_t cpu_no) {
        run_queues->with_lock_void(cpu_no, [task, &found, &current_on_cpu, &current_cpu_of_task, cpu_no](RunQueue* rq) {
            if (found || current_on_cpu) {
                return;
            }
            if (runqueue_task_is_reserved_locked(rq, task)) {
                current_on_cpu = true;
                current_cpu_of_task = cpu_no;
                return;
            }
            if (rq->runnable_heap.contains(task)) {
                remove_from_sums(rq, task);
                rq->runnable_heap.remove(task);
                task->last_sleep_start_us = time::get_us();
                task->sched_queue = task::Task::sched_queue::WAITING;
                task->set_wait_channel("ptrace", task::WaitChannelKind::PTRACE);
                wait_list_push_locked(rq, task);
                found = true;
                return;
            }
            if (wait_list_remove_locked(rq, task)) {
                task->sched_queue = task::Task::sched_queue::WAITING;
                task->set_wait_channel("ptrace", task::WaitChannelKind::PTRACE);
                wait_list_push_locked(rq, task);
                found = true;
            }
        });
    };

    uint64_t const NCPUS = smt::get_core_count();
    if (task->cpu < NCPUS) {
        try_stop_on_cpu(task->cpu);
    }
    for (uint64_t cpu_no = 0; cpu_no < NCPUS && !found && !current_on_cpu; ++cpu_no) {
        if (cpu_no != task->cpu) {
            try_stop_on_cpu(cpu_no);
        }
    }

    if (current_on_cpu && current_cpu_of_task != UINT64_MAX) {
        if (current_cpu_of_task == cpu::current_cpu()) {
            request_local_reschedule();
        } else {
            wake_cpu(current_cpu_of_task);
        }
        return true;
    }
    return found;
}

void process_tasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame) {
    TimerIrqPerfScope const TIMER_IRQ_PERF;

    // Enter epoch critical section - protects task pointers from GC
    EpochGuard const EPOCH_GUARD;

    // Wake any tasks sleeping via nanosleep whose deadline has passed.
    // Scan this CPU's wait list; tasks with wakeAtUs != 0 are timer sleeps.
    // Also arm ITIMER_REAL for blocked tasks so signal-driven interfaces
    // like ping can wake from a blocking recvfrom().
    PendingWakeList signal_wake{};
    PendingWakeList futex_timeout_cleanup{};
    PendingWakeList waitpid_repair{};
    uint32_t signal_wake_count = 0;
    uint32_t futex_timeout_cleanup_count = 0;
    uint32_t waitpid_repair_count = 0;
    uint64_t const WAIT_SCAN_NOW_US = time::get_us();
    {
        run_queues->this_cpu_locked_void([WAIT_SCAN_NOW_US, &signal_wake, &signal_wake_count, &futex_timeout_cleanup,
                                          &futex_timeout_cleanup_count, &waitpid_repair, &waitpid_repair_count](RunQueue* rq) {
            bool const TIMED_SCAN = rq->next_wait_deadline_us != 0 && WAIT_SCAN_NOW_US >= rq->next_wait_deadline_us;
            if (!TIMED_SCAN && rq->wait_list.head == nullptr) {
                return;
            }

            // Collect tasks to wake (can't modify list while iterating)
            PendingWakeList to_wake{};
            uint32_t wake_count = 0;
            uint64_t scan_iterations = 0;
            task::Task* t = rq->wait_list.head;
            while (t != nullptr) {
                scan_iterations++;
                if (TIMED_SCAN && wake_count < PENDING_WAKE_LIMIT && t->wake_at_us != 0 && WAIT_SCAN_NOW_US >= t->wake_at_us) {
                    pending_wake_slot(to_wake, wake_count++) = t;
                } else if (TIMED_SCAN && t->itimer_real_expire_us != 0 && WAIT_SCAN_NOW_US >= t->itimer_real_expire_us &&
                           signal_wake_count < PENDING_WAKE_LIMIT) {
                    t->sig_pending |= (1ULL << (14 - 1));  // SIGALRM = 14
                    if (t->itimer_real_interval_us != 0) {
                        t->itimer_real_expire_us = saturating_deadline_us(WAIT_SCAN_NOW_US, t->itimer_real_interval_us);
                    } else {
                        t->itimer_real_expire_us = 0;
                    }
                    pending_wake_slot(signal_wake, signal_wake_count++) = t;
                }
                if (waitpid_repair_count < PENDING_WAKE_LIMIT && waitpid_repair_due(t, WAIT_SCAN_NOW_US) && t->try_acquire()) {
                    t->waitpid_last_repair_us = WAIT_SCAN_NOW_US;
                    pending_wake_slot(waitpid_repair, waitpid_repair_count++) = t;
                }
                t = t->sched_next;
            }
            rq->wait_list_scan_iterations.fetch_add(scan_iterations, std::memory_order_relaxed);
            rq->wait_list_scan_passes.fetch_add(1, std::memory_order_relaxed);
            update_relaxed_max(rq->wait_list_scan_max, scan_iterations);
            for (uint32_t i = 0; i < wake_count; i++) {
                task::Task* w = pending_wake_slot(to_wake, i);
                wait_list_remove_locked(rq, w);
                // Mark freshly woken: inhibits immediate wakeup-preemption (see process_tasks guard).
                bool const LOW_LATENCY_HANDOFF = is_low_latency_handoff_wait_channel(w->wait_channel_kind);
                w->just_woke = !LOW_LATENCY_HANDOFF;
                // Perf: record wakeup before clearing wakeAtUs
                uint64_t const WAKE_AT_US = w->wake_at_us;
                bool const FUTEX_TIMEOUT =
                    w->wait_channel_is(task::WaitChannelKind::FUTEX) && w->futex_waiter.load(std::memory_order_acquire) != nullptr;
                if (FUTEX_TIMEOUT) {
                    w->context.regs.rax = static_cast<uint64_t>(-ETIMEDOUT);
                    pending_wake_slot(futex_timeout_cleanup, futex_timeout_cleanup_count++) = w;
                }
                perf::record_wake(static_cast<uint32_t>(cpu::current_cpu()), w->pid, WAKE_AT_US, perf_wake_flags(WAKE_AT_US, false, false),
                                  observed_sleep_us(w, WAIT_SCAN_NOW_US), perf_wait_callsite(w), w->wait_channel);
                w->wake_at_us = 0;
                w->wants_block = false;  // Clear any pending block from kern_block()
                int64_t const WAKE_FLOOR = compute_wakeup_floor_vruntime(rq, w, WAIT_SCAN_NOW_US, w->wait_channel_kind);
                note_task_wakeup(w, WAIT_SCAN_NOW_US, w->wait_channel_kind);
                if (rq->runnable_heap.contains(w)) {
                    repair_stale_wait_membership_locked(rq, w);
                    continue;
                }
                w->vruntime = w->vruntime > WAKE_FLOOR ? w->vruntime : WAKE_FLOOR;
                w->vdeadline = vdeadline_from_slice(w->vruntime, w->slice_ns, w->sched_weight);
                w->slice_used_ns = 0;
                (void)publish_runnable_task_locked(rq, w, "timer-wake");
            }
            if (wake_count != 0) {
                rq->timer_expired_wakeups.fetch_add(wake_count, std::memory_order_relaxed);
            }
            recompute_wait_deadline_locked(rq);
        });
    }
    for (uint32_t i = 0; i < futex_timeout_cleanup_count; ++i) {
        ker::syscall::futex::futex_wait_cleanup_for_task(pending_wake_slot(futex_timeout_cleanup, i));
    }
    for (uint32_t i = 0; i < waitpid_repair_count; ++i) {
        task::Task* waiter = pending_wake_slot(waitpid_repair, i);
        if (complete_or_preserve_waitpid_block(waiter)) {
            uint64_t target_cpu = waiter->cpu;
            if (target_cpu >= smt::get_core_count()) {
                target_cpu = get_least_loaded_cpu();
            }
            reschedule_task_for_cpu(target_cpu, waiter);
        }
        waiter->release();
    }
    for (uint32_t i = 0; i < signal_wake_count; ++i) {
        wake_task_for_signal(pending_wake_slot(signal_wake, i));
    }

    auto* rq = run_queues->this_cpu();
    auto* current_task = rq->current_task;

    // ---- Idle path: no running task, check heap for work ----
    if (current_task == nullptr || current_task->type == task::TaskType::IDLE) {
        {
            uint64_t const NOW_US = time::get_us();
            auto delta_us = static_cast<int64_t>(NOW_US - rq->last_tick_us);
            rq->last_tick_us = NOW_US;
            if (delta_us <= 0) {
                delta_us = 1;
            }
            (void)account_runtime_slice(rq, current_task, NOW_US, static_cast<uint64_t>(delta_us), true, false);
        }

        task::Task* next_task = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
            if (rq->runnable_heap.size == 0) {
                // Phase 2: attempt work stealing from a peer CPU before going idle.
                // try_steal_from_peers is called with our lock held; it uses
                // tryWithLock (non-blocking) for victim CPUs - no deadlock risk.
                try_steal_from_peers(cpu::current_cpu(), rq);
                if (rq->runnable_heap.size == 0) {
                    return nullptr;
                }
            }
            int64_t const AVG = compute_avg_vruntime(rq);
            auto* t = rq->runnable_heap.pick_best_eligible(AVG);
            if (t == nullptr) {
                return nullptr;
            }
            // Validate inside lock before committing
            if (t->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
                return nullptr;
            }
            if (t->ptrace_stopped) {
                remove_from_sums(rq, t);
                rq->runnable_heap.remove(t);
                t->sched_queue = task::Task::sched_queue::WAITING;
                t->set_wait_channel("ptrace", task::WaitChannelKind::PTRACE);
                wait_list_push_locked(rq, t);
                return nullptr;
            }
            if (t->type == task::TaskType::PROCESS && (t->thread == nullptr || t->pagemap == nullptr)) {
                return nullptr;
            }
            if (t->type == task::TaskType::IDLE) {
                return nullptr;  // Don't switch idle->idle
            }
            // Reserve the picked task without publishing it as current yet.
            // current_task must continue to match the live stack until
            // switch_to() has patched the return frame for this timer tick.
            reserve_handoff_task_locked(rq, t, time::get_us());
            // (runnable_heap.size already decremented by remove above)
            return t;
        });

        if (next_task == nullptr) {
            return;  // Stay idle
        }
#ifdef SCHED_DEBUG
        dbg::log("PICK-IDLE: CPU %d picked PID %x (heapIdx=%d)", static_cast<int>(cpu::current_cpu()), next_task->pid,
                 next_task->heap_index);
#endif
        rq->is_idle.store(false, std::memory_order_release);
        prepare_first_run_daemon(next_task, "idle-switch");
        rq->last_tick_us = time::get_us();

        if (!sys::context_switch::switch_to(gpr, frame, next_task)) {
#ifdef SCHED_DEBUG
            dbg::log("PICK-IDLE: CPU %d switchTo FAILED for PID %x", static_cast<int>(cpu::current_cpu()), next_task->pid);
#endif
            clear_handoff_task(next_task);
            if (rq->idle_task != nullptr) {
                run_queues->this_cpu_locked_void([](RunQueue* rq) { reserve_handoff_task_locked(rq, rq->idle_task, time::get_us()); });
            }
            rq->is_idle.store(true, std::memory_order_release);
            arm_idle_timer_for_this_cpu();
        } else {
            record_local_proc_first_run(next_task, WOS_PERF_CALLSITE());
            next_task->has_run = true;
        }
        return;
    }

    // ---- Running task path: update EEVDF bookkeeping, maybe preempt ----

    // CRITICAL: If the timer fired while a PROCESS task was in kernel mode (e.g.
    // during a syscall), the interrupt frame and GPRegs contain kernel-mode values.
    // We must NOT save these as the task's user-mode context, and we must NOT
    // preempt - the kernel is non-preemptive for PROCESS tasks.
    // DAEMON (kernel thread) tasks are always in kernel mode but MUST be preemptible.
    bool const IN_KERNEL_MODE = (frame.cs != desc::gdt::GDT_USER_CS);
    bool const IS_DAEMON = (current_task->type == task::TaskType::DAEMON);

    // A PROCESS task that set voluntary_block is at a safe preemption point
    // (e.g. sti;hlt wait loop in a syscall).  Treat it like a DAEMON for
    // context-save and preemption purposes.
    bool const KERNEL_PREEMPT_SAFE = IS_DAEMON || current_task->is_voluntary_blocked();
    bool const PREEMPT_DISABLED = current_task->preempt_disable_depth != 0;
    bool const CAN_PREEMPT_KERNEL = KERNEL_PREEMPT_SAFE && !PREEMPT_DISABLED;

    // Save GPR/frame context: user-mode PROCESS tasks, or DAEMON/voluntary_block tasks (always kernel mode but preemptible)
    // NOTE: FPU save is deferred until we know a context switch is actually needed,
    // to avoid the expensive xsave on every timer tick when no switch occurs.
    if (current_task->has_run && current_task->type != task::TaskType::IDLE) {
        if (CAN_PREEMPT_KERNEL || !IN_KERNEL_MODE) {
            current_task->context.regs = gpr;
            current_task->context.frame = frame;
        }
    }

    bool blocked_current_task = false;
    int64_t perf_lag_out = 0;
    task::Task* next_task = run_queues->this_cpu_locked([current_task, IN_KERNEL_MODE, KERNEL_PREEMPT_SAFE, CAN_PREEMPT_KERNEL,
                                                         &blocked_current_task, &perf_lag_out](RunQueue* rq) -> task::Task* {
        // Compute time delta since last tick
        uint64_t const NOW_US = time::get_us();
        auto delta_us = static_cast<int64_t>(NOW_US - rq->last_tick_us);
        rq->last_tick_us = NOW_US;
        if (delta_us <= 0) {
            delta_us = 1;
        }

        // Process time accounting: attribute delta to user or system time.
        // voluntary_block means the task executed sti;hlt and the tick is
        // arriving out of a halted wait point, not from CPU it consumed.
        auto chargeable_us = static_cast<uint64_t>(delta_us);
        if (current_task->type != task::TaskType::IDLE) {
            chargeable_us = account_runtime_slice(rq, current_task, NOW_US, static_cast<uint64_t>(delta_us), IN_KERNEL_MODE,
                                                  !current_task->is_voluntary_blocked());

            // kern_yield() sleepers stay on the runnable heap, so record a
            // pseudo-sleep interval here. That lets the preemption guard treat
            // them like bursty wakeups instead of runnable compute peers.
            if (current_task->is_voluntary_blocked()) {
                if (current_task->last_sleep_start_us == 0) {
                    current_task->last_run_us = current_task->slice_used_ns / 1000U;
                    note_perf_wait_callsite(current_task, current_task->context.frame.rip);
                    current_task->last_sleep_start_us = NOW_US;
                }
            } else if (current_task->last_sleep_start_us != 0 && current_task->last_wake_us < current_task->last_sleep_start_us) {
                note_task_wakeup(current_task, NOW_US, current_task->wait_channel_kind);
                current_task->just_woke = true;
            }

            // Clear justWoke: the task has now run for at least one tick.
            // This ensures the wakeup-preemption guard only applies during the
            // first scheduling window after a sleep->wake transition.
            current_task->just_woke = false;
        }
        uint64_t const DELTA_NS = runtime_delta_ns_from_us(chargeable_us);

        // Perf: periodic CPU sample (~100 Hz sub-sampled from 1 kHz timer)
        if (!current_task->is_voluntary_blocked()) {
            perf::record_sample(static_cast<uint32_t>(cpu::current_cpu()), current_task->pid, current_task->context.frame.rip,
                                !IN_KERNEL_MODE, compute_avg_vruntime(rq) - current_task->vruntime);
        }

        // ITIMER_REAL expiry check - fire SIGALRM and reload or disarm
        if (current_task->itimer_real_expire_us != 0 && NOW_US >= current_task->itimer_real_expire_us) {
            current_task->sig_pending |= (1ULL << (14 - 1));  // SIGALRM = 14
            if (current_task->itimer_real_interval_us != 0) {
                current_task->itimer_real_expire_us = saturating_deadline_us(NOW_US, current_task->itimer_real_interval_us);
            } else {
                current_task->itimer_real_expire_us = 0;
            }
        }

        // Update vruntime if task is in the heap
        if (rq->runnable_heap.contains(current_task)) {
            int64_t const VRUNTIME_DELTA = vruntime_delta_from_runtime_ns(DELTA_NS, current_task->sched_weight);
            current_task->vruntime += VRUNTIME_DELTA;
            current_task->slice_used_ns = accumulate_slice_used_ns(current_task->slice_used_ns, DELTA_NS, current_task->slice_ns);

            // Track weighted sum: delta_v * weight = deltaNs * 1024 always
            add_weighted_vruntime_delta(rq, VRUNTIME_DELTA, current_task->sched_weight);

            // Slice exhausted - reset and recalculate deadline
            if (current_task->slice_used_ns >= current_task->slice_ns) {
                current_task->slice_used_ns = 0;
                current_task->vdeadline = vdeadline_from_slice(current_task->vruntime, current_task->slice_ns, current_task->sched_weight);
            }

            // Re-sift in heap after vruntime/vdeadline change
            rq->runnable_heap.update(current_task);
        }

        // Advance minVruntime to weighted average (prevents int64 overflow
        // in totalWeightedVruntime by keeping relative keys small)
        int64_t const AVG = compute_avg_vruntime(rq);
        if (AVG > rq->min_vruntime) {
            int64_t const DELTA_MIN = AVG - rq->min_vruntime;
            rq->min_vruntime = AVG;
            rq->total_weighted_vruntime -= DELTA_MIN * rq->total_weight;
        }

        // Don't preempt PROCESS tasks in kernel mode (they're mid-syscall)
        // unless they set voluntary_block (safe blocking point).
        // DAEMON tasks are always in kernel mode but must be preemptible.
        if (IN_KERNEL_MODE && !CAN_PREEMPT_KERNEL) {
            if (KERNEL_PREEMPT_SAFE && current_task->preempt_disable_depth != 0) {
                current_task->preempt_pending = true;
            }
            return nullptr;
        }

        // kern_block()/preemptible_syscall_park(): if the current task requested
        // a true block, move it to the wait list now, under the lock.  The
        // normal case has the current task in the runnable heap.  If a
        // handoff/current-task repair path left the live task current but not
        // heap-contained, it still must be parked; otherwise it can keep
        // halting with wants_block set until an unrelated wake happens.
        if (current_task->wants_block) {
            current_task->wants_block = false;
            current_task->last_run_us = current_task->slice_used_ns / 1000U;
            note_perf_wait_callsite(current_task, current_task->context.frame.rip);
            current_task->last_sleep_start_us = NOW_US;
            uint64_t const WAKE_AT_US = current_task->wake_at_us;
            if (rq->runnable_heap.contains(current_task)) {
                remove_from_sums(rq, current_task);
                rq->runnable_heap.remove(current_task);
            }
            wait_list_remove_all_locked(rq, current_task);
            current_task->sched_queue = task::Task::sched_queue::WAITING;
            wait_list_push_locked(rq, current_task);
            // Perf: task going to sleep (wants_block)
            perf::record_sleep(static_cast<uint32_t>(cpu::current_cpu()), current_task->pid, WAKE_AT_US, perf_sleep_flags(WAKE_AT_US),
                               current_task->last_run_us, perf_wait_callsite(current_task), current_task->wait_channel);
            blocked_current_task = true;
        }

        if (current_task->ptrace_stopped && rq->runnable_heap.contains(current_task)) {
            current_task->last_run_us = current_task->slice_used_ns / 1000U;
            note_perf_wait_callsite(current_task, current_task->context.frame.rip);
            current_task->last_sleep_start_us = NOW_US;
            remove_from_sums(rq, current_task);
            rq->runnable_heap.remove(current_task);
            current_task->sched_queue = task::Task::sched_queue::WAITING;
            current_task->set_wait_channel("ptrace", task::WaitChannelKind::PTRACE);
            wait_list_push_locked(rq, current_task);
            blocked_current_task = true;
        }

        // Pick best eligible task
        if (rq->runnable_heap.size == 0) {
            return nullptr;
        }
        auto* next = pick_best_eligible_for_switch_locked(rq, AVG, current_task);
        if (next == nullptr || next == current_task) {
            return nullptr;
        }
        if (next->ptrace_stopped) {
            remove_from_sums(rq, next);
            rq->runnable_heap.remove(next);
            next->sched_queue = task::Task::sched_queue::WAITING;
            next->set_wait_channel("ptrace", task::WaitChannelKind::PTRACE);
            wait_list_push_locked(rq, next);
            return nullptr;
        }

        // EEVDF preemption guard: only preempt the current task if the candidate
        // has a strictly earlier virtual deadline. This lets the current task run
        // its full slice (~10ms) instead of being preempted every timer tick.
        // Without this, equal-weight compute threads ping-pong on every 4ms tick.
        //
        // EXCEPTION: If the current task is at a voluntary block point (hlt in
        // kern_yield), always allow preemption. The task isn't computing - it's
        // sleeping. Without this, halted daemon threads waste their entire 10ms
        // slice doing nothing, starving compute threads on the same CPU.
        //
        // WAKEUP MIN-GRANULARITY (Linux sched_wakeup_granularity equivalent):
        // A freshly-woken task (justWoke=true) is not allowed to preempt the
        // currently running task until the current task has been running
        // continuously for at least SCHED_MIN_GRANULARITY_US.  This prevents
        // the I/O-bound 50/50 thrash where short-sleeping tasks (dropbear,
        // netpoll, etc.) win vdeadline comparisons on every 4ms tick purely
        // because they slept and were placed at avg_vruntime while the compute
        // task accumulated vruntime above avg.
        static constexpr uint64_t SCHED_MIN_GRANULARITY_US = 1000ULL;  // 1ms
        static constexpr uint64_t SCHED_BACKGROUND_PREEMPT_BASE_US = 12000ULL;
        static constexpr uint64_t SCHED_BACKGROUND_PREEMPT_PER_NICE_US = 2500ULL;
        static constexpr uint64_t SCHED_BACKGROUND_PREEMPT_MAX_US = 48000ULL;
        uint64_t const RUN_DURATION_US = (rq->current_task_start_us != 0 && NOW_US >= rq->current_task_start_us)
                                             ? (NOW_US - rq->current_task_start_us)
                                             : UINT64_MAX;  // 0 = unset: allow preemption
        bool const LOWER_PRIORITY_PROCESS_PREEMPT = current_task->type == task::TaskType::PROCESS &&
                                                    next->type == task::TaskType::PROCESS && next->sched_nice > current_task->sched_nice;
        bool const HIGHER_PRIORITY_PREEMPT = is_higher_priority_process_contender(current_task, next);
        bool const LOWER_WEIGHT_PREEMPT = current_task->type == task::TaskType::PROCESS && is_lower_weight_contender(current_task, next);
        bool const BURSTY_PROCESS_WAKEUP_PREEMPT =
            current_task->type == task::TaskType::PROCESS &&
            (is_bursty_wakeup_contender(next, NOW_US) || is_bursty_voluntary_block_contender(next, NOW_US));
        uint64_t background_preempt_granularity_us = SCHED_MIN_GRANULARITY_US;
        if (LOWER_PRIORITY_PROCESS_PREEMPT) {
            auto const NICE_DELTA = static_cast<uint64_t>(next->sched_nice - current_task->sched_nice);
            uint64_t const SCALED_GUARD = SCHED_BACKGROUND_PREEMPT_BASE_US + (NICE_DELTA * SCHED_BACKGROUND_PREEMPT_PER_NICE_US);
            background_preempt_granularity_us = std::min<uint64_t>(SCALED_GUARD, SCHED_BACKGROUND_PREEMPT_MAX_US);
        }
        if (LOWER_WEIGHT_PREEMPT) {
            background_preempt_granularity_us =
                std::max<uint64_t>(background_preempt_granularity_us, compute_lower_weight_preempt_guard_us(current_task, next));
        }
        if (BURSTY_PROCESS_WAKEUP_PREEMPT) {
            background_preempt_granularity_us =
                std::max<uint64_t>(background_preempt_granularity_us, compute_bursty_preempt_guard_us(next));
        }
        if (!current_task->is_voluntary_blocked() && !HIGHER_PRIORITY_PREEMPT &&
            (LOWER_PRIORITY_PROCESS_PREEMPT || LOWER_WEIGHT_PREEMPT || BURSTY_PROCESS_WAKEUP_PREEMPT) &&
            rq->runnable_heap.contains(current_task) && RUN_DURATION_US < background_preempt_granularity_us) {
            return nullptr;
        }
        if (!current_task->is_voluntary_blocked() && current_task->slice_used_ns < current_task->slice_ns &&
            rq->runnable_heap.contains(current_task) &&
            ((next->vdeadline >= current_task->vdeadline && !HIGHER_PRIORITY_PREEMPT) ||
             (next->just_woke && !HIGHER_PRIORITY_PREEMPT && RUN_DURATION_US < SCHED_MIN_GRANULARITY_US))) {
            return nullptr;
        }

        // Validate next task state - it might have started exiting
        if (next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
            return nullptr;
        }
        if (next->type == task::TaskType::PROCESS && (next->thread == nullptr || next->pagemap == nullptr)) {
            return nullptr;
        }
        if (next->type == task::TaskType::IDLE) {
            return nullptr;
        }

        // Reserve the picked task without publishing it as current yet.
        // current_task must continue to match the live interrupted stack until
        // switch_to() has patched the timer return frame.
        reserve_handoff_task_locked(rq, next, NOW_US);
        // Capture lag for the perf SWITCH event recorded outside the lock.
        perf_lag_out = AVG - current_task->vruntime;
        return next;
    });

    // No switch needed (same task, invalid, kernel mode, or no candidate)
    if (next_task == nullptr) {
        if (blocked_current_task) {
            auto* idle_rq = run_queues->this_cpu();
            enter_idle_loop(idle_rq);
        }
        return;
    }
#ifdef SCHED_DEBUG
    dbg::log("PICK-PREEMPT: CPU %d switching PID %x -> PID %x (heapIdx=%d)", static_cast<int>(cpu::current_cpu()), current_task->pid,
             next_task->pid, next_task->heap_index);
#endif
    // Perf: record preemption context switch
    {
        uint8_t const SW_FLAGS = current_task->is_voluntary_blocked() ? perf::PERF_FLAG_BLOCK : perf::PERF_FLAG_PREEMPT;
        perf::record_switch(static_cast<uint32_t>(cpu::current_cpu()), current_task->pid, next_task->pid, SW_FLAGS, perf_lag_out,
                            current_task->slice_used_ns / 1000U,
                            current_task->is_voluntary_blocked() ? perf_wait_callsite(current_task) : 0);
    }
    // Perform context switch. The next task is only reserved under the lock;
    // publish it as current after the live timer frame has been changed.
    // Save FPU state now that we know a switch is needed. This avoids the
    // expensive xsave on every timer tick when the same task continues.
    if (current_task->type == task::TaskType::PROCESS) {
        sys::context_switch::save_fpu_state(current_task);
    }

    task::Task* original_task = current_task;
    rq->is_idle.store(false, std::memory_order_release);
    prepare_first_run_daemon(next_task, "timer-switch");

    if (!sys::context_switch::switch_to(gpr, frame, next_task)) {
        clear_handoff_task(next_task);
        publish_current_task(rq, original_task);
        debug_task_slot(cpu::current_cpu()) = original_task;
    } else {
        record_local_proc_first_run(next_task, WOS_PERF_CALLSITE());
        next_task->has_run = true;
    }
}

// ============================================================================
// jumpToNextTask - called after task exit to switch to next task
// ============================================================================

void jump_to_next_task(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame) {
    apic::eoi();

    // Manually enter epoch critical section. This function doesn't return
    // normally (goes through switchTo->iretq or idle loop), so the guard
    // would never destruct. The critical section is "leaked" but OK:
    // the next processTasks call will create a new EpochGuard.
    uint64_t const EPOCH_CPU = EpochManager::enter_critical();

    task::Task* exiting_task = get_current_task();
#ifdef SCHED_DEBUG
    dbg::log("jumpToNextTask: CPU %d exitingTask=%p pid=%x", cpu::current_cpu(), exitingTask, exitingTask ? exitingTask->pid : 0);
#endif

    // Under lock: ensure exiting task is removed, pick next from heap
    task::Task* next_task = run_queues->this_cpu_locked([exiting_task](RunQueue* rq) -> task::Task* {
        // Remove exiting task from wherever it is
        if (exiting_task != nullptr) {
            while (remove_from_heap_by_scan_locked(rq, exiting_task)) {
                // Scrub stale duplicate heap membership before picking a successor.
            }
            // The intrusive wait list owns schedNext, so detach by actual
            // membership rather than trusting sched_queue to still be accurate.
            wait_list_remove_locked(rq, exiting_task);
            exiting_task->sched_queue = task::Task::sched_queue::NONE;
        }

        // Pick next task from heap
        auto* next = pick_exit_switch_candidate_locked(rq, exiting_task);
        if (next == nullptr) {
            return nullptr;
        }
        reserve_handoff_task_locked(rq, next, time::get_us());
        return next;
    });

    if (is_dead_gc_candidate_task(exiting_task)) {
        insert_into_dead_list(exiting_task);
    }

    if (next_task == nullptr) {
        // No runnable tasks - enter idle loop
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: CPU %d: No ready tasks, entering idle", cpu::current_cpu());
#endif
        auto* rq = run_queues->this_cpu();
        EpochManager::exit_critical_for_cpu(EPOCH_CPU);
        enter_idle_loop(rq);
    }

    // Validate task
    if (next_task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        auto* rq = run_queues->this_cpu();
        clear_handoff_task(next_task);
        EpochManager::exit_critical_for_cpu(EPOCH_CPU);
        enter_idle_loop(rq);
    }

    if (next_task->type == task::TaskType::PROCESS && (next_task->thread == nullptr || next_task->pagemap == nullptr)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: PID %x missing resources, entering idle", next_task->pid);
#endif
        auto* rq = run_queues->this_cpu();
        clear_handoff_task(next_task);
        EpochManager::exit_critical_for_cpu(EPOCH_CPU);
        enter_idle_loop(rq);
    }

    // If idle task: enter idle loop directly
    if (next_task->type == task::TaskType::IDLE) {
        auto* rq = run_queues->this_cpu();
        EpochManager::exit_critical_for_cpu(EPOCH_CPU);
        enter_idle_loop(rq);
    }

    // Switch to real task
    auto* rq = run_queues->this_cpu();
    rq->is_idle.store(false, std::memory_order_release);
    prepare_first_run_daemon(next_task, "exit-switch");

    if (!sys::context_switch::switch_to(gpr, frame, next_task)) {
#ifdef SCHED_DEBUG
        dbg::log("jumpToNextTask: switchTo FAILED for PID %x", next_task->pid);
#endif
        clear_handoff_task(next_task);
        EpochManager::exit_critical_for_cpu(EPOCH_CPU);
        enter_idle_loop(rq);
    } else {
        record_local_proc_first_run(next_task, WOS_PERF_CALLSITE());
        next_task->has_run = true;
    }
}

// ============================================================================
// startScheduler - initial task entry on each CPU
// ============================================================================

void start_scheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::current_cpu());

    auto* rq = run_queues->this_cpu();

    // Wait for a real (non-idle) task in the heap.
    // CRITICAL: Use lock for the initial check to prevent double-scheduling.
    // A lockless peek creates a race window where rescheduleTaskForCpu can
    // move the task to another CPU between the peek and the currentTask assignment.
    task::Task* first_task = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
        if (rq->runnable_heap.size == 0) {
            return nullptr;
        }
        int64_t const AVG = compute_avg_vruntime(rq);
        auto* t = rq->runnable_heap.pick_best_eligible(AVG);
        if (t != nullptr && t->type != task::TaskType::IDLE && (t->type == task::TaskType::DAEMON || t->thread != nullptr)) {
            t->cpu = cpu::current_cpu();
            publish_current_task(rq, t);
        }
        return t;
    });

    if (first_task == nullptr || first_task->type == task::TaskType::IDLE) {
        // Set idle task as current while waiting
        publish_current_task(rq, rq->idle_task);
        scheduler_task_context_ready_mask.fetch_or(1ULL << cpu::current_cpu(), std::memory_order_acq_rel);

        for (;;) {
            rq->is_idle.store(true, std::memory_order_release);
            arm_idle_timer_for_this_cpu();
            asm volatile("sti");
            asm volatile("hlt");
            asm volatile("cli");
            rq->is_idle.store(false, std::memory_order_release);

            // Check under lock if we have a non-idle task
            first_task = run_queues->this_cpu_locked([](RunQueue* rq) -> task::Task* {
                if (rq->runnable_heap.size == 0) {
                    return nullptr;
                }

                // Scan heap for a non-idle, ready task
                int64_t const AVG = compute_avg_vruntime(rq);
                task::Task* candidate = rq->runnable_heap.pick_best_eligible(AVG);
                if (candidate == nullptr) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::IDLE) {
                    return nullptr;
                }
                if (candidate->type == task::TaskType::PROCESS && candidate->thread == nullptr) {
                    return nullptr;
                }

                candidate->cpu = cpu::current_cpu();
                publish_current_task(rq, candidate);
                return candidate;
            });

            if (first_task != nullptr) {
                dbg::log("CPU %d: Found task PID %x, starting", cpu::current_cpu(), first_task->pid);
                break;
            }
        }
    } else {
        scheduler_task_context_ready_mask.fetch_or(1ULL << cpu::current_cpu(), std::memory_order_acq_rel);
    }

    bool const ALREADY_RAN = first_task->has_run;
    prepare_first_run_daemon(first_task, "start-first");
    record_local_proc_first_run(first_task, WOS_PERF_CALLSITE());
    first_task->has_run = true;
    first_task->cpu = cpu::current_cpu();
    publish_current_task(rq, first_task);

    // Set up GS/FS MSRs for the first task
    uint64_t const REAL_CPU_ID = cpu::current_cpu();
    auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(first_task->context.syscall_scratch_area);
    scratch_area->cpu_id = REAL_CPU_ID;
    if (first_task->thread != nullptr) {
#ifdef SCHED_DEBUG
        dbg::log("Setting MSRs: fsbase=0x%x, gsbase=0x%x, scratchArea=0x%x", first_task->thread->fsbase, first_task->thread->gsbase,
                 first_task->context.syscall_scratch_area);
#endif
        cpu_set_msr(IA32_GS_BASE, first_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, first_task->thread->gsbase);
        cpu_set_msr(IA32_FS_BASE, first_task->thread->fsbase);
    } else {
        // DAEMON or thread-less task: use scratch area for both GS bases
        cpu_set_msr(IA32_GS_BASE, first_task->context.syscall_scratch_area);
        cpu_set_msr(IA32_KERNEL_GS_BASE, first_task->context.syscall_scratch_area);
    }

    mm::virt::switch_pagemap(first_task);

    // Update debug task pointer
    debug_task_slot(cpu::current_cpu()) = first_task;

    // Initialize HPET timestamp for first tick
    rq->last_tick_us = time::get_us();

    // Keep interrupts masked until the final handoff has installed the target
    // task stack. Otherwise an APIC timer can save `first_task` while we are
    // still executing start_scheduler() on the CPU bootstrap stack.
    asm volatile("cli" ::: "memory");

    // Start the scheduler timer. The eventual enter path restores IF from the
    // target frame or enables it after switching to the kernel-thread stack.
    sys::context_switch::start_sched_timer();

    // Restore FPU/SSE/AVX state (PROCESS tasks only, and only if previously saved).
    if (first_task->type == task::TaskType::PROCESS && first_task->fx_state.saved) {
        sys::context_switch::restore_fpu_state(first_task);
    }

    if (ALREADY_RAN) {
// Task was already running on another CPU and was migrated here.
// Resume from its saved context instead of restarting at the entry point.
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Resuming PID %x from saved context (rip=0x%x)", cpu::current_cpu(), first_task->pid,
                 first_task->context.frame.rip);
#endif
        validate_kernel_resume_target(first_task, "start-resume");
        wos_deferred_task_switch_return(&first_task->context.regs, &first_task->context.frame);
        __builtin_unreachable();
    }

    // Brand-new DAEMON kernel thread: there is no interrupted kernel frame to
    // resume yet, so install the initial stack directly and enter the thread
    // trampoline. Preempted DAEMONs still use the generic resume path above.
    if (first_task->type == task::TaskType::DAEMON) {
#ifdef SCHED_DEBUG
        dbg::log("CPU %d: Starting kernel thread PID %x '%s' (rip=0x%x)", cpu::current_cpu(), first_task->pid,
                 first_task->name ? first_task->name : "?", first_task->context.frame.rip);
#endif
        validate_kernel_resume_target(first_task, "start-daemon");
        if (first_task->kthread_entry == nullptr) {
            resume_log::error("kernel thread without entry: pid=%lu name=%s", first_task->pid,
                              first_task->name != nullptr ? first_task->name : "?");
            hcf();
        }
        wos_start_kernel_thread(first_task->context.frame.rsp, first_task->kthread_entry);
        __builtin_unreachable();
    }

    // Brand new user task - write TLS self-pointer and enter at ELF entry point
    if (first_task->thread != nullptr) {
        *(reinterpret_cast<uint64_t*>(first_task->thread->fsbase)) = first_task->thread->fsbase;
    }

    for (;;) {
        wos_asm_enter_usermode(first_task->entry, first_task->context.frame.rsp);
    }
}

// ============================================================================
// deferred_task_switch - called from syscall path for yield/block
// ============================================================================

extern "C" void deferred_task_switch(ker::mod::cpu::GPRegs* gpr_ptr, [[maybe_unused]] ker::mod::gates::InterruptFrame* frame_ptr) {
    if (gpr_ptr == nullptr) {
        return;
    }

    // Epoch guard protects task pointers from GC during switch
    EpochGuard const EPOCH_GUARD;

    auto* current_task = get_current_task();
    if (current_task == nullptr) {
        return;
    }
    if (current_task->preempt_disable_depth != 0) {
        note_preempt_disabled_block("deferred_task_switch", current_task->perf_wait_callsite);
    }

    // Build interrupt frame from syscall scratch area (syscall doesn't push one).
    // gs:0x28 = saved RCX (return RIP), gs:0x30 = saved R11 (RFLAGS), gs:0x08 = user RSP
    // NOLINTBEGIN(misc-const-correctness)
    uint64_t return_rip = 0;
    uint64_t return_flags = 0;
    uint64_t user_rsp = 0;
    // NOLINTEND(misc-const-correctness)
    asm volatile("movq %%gs:0x28, %0" : "=r"(return_rip));
    asm volatile("movq %%gs:0x30, %0" : "=r"(return_flags));
    asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));

    // Save all GPRs from the syscall stack.
    current_task->context.regs = *gpr_ptr;

    // Fix up RAX: syscall return value is stored in the slot past GPRegs
    auto* return_value_slot = reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(gpr_ptr) + sizeof(cpu::GPRegs));
    current_task->context.regs.rax = *return_value_slot;

    // Restore clobbered RCX/R11
    current_task->context.regs.rcx = return_rip;
    current_task->context.regs.r11 = return_flags;

    current_task->context.frame.int_num = 0;
    current_task->context.frame.err_code = 0;
    current_task->context.frame.rip = return_rip;
    current_task->context.frame.cs = desc::gdt::GDT_USER_CS;
    current_task->context.frame.flags = return_flags;
    current_task->context.frame.rsp = user_rsp;
    current_task->context.frame.ss = desc::gdt::GDT_USER_DS;
    validate_user_resume_target(current_task, "deferred-save-current");
    current_task->waitpid_publish_pending.store(false, std::memory_order_release);

    // Save outgoing task's FPU/SSE/AVX state
    sys::context_switch::save_fpu_state(current_task);

    bool const IS_YIELD = current_task->yield_switch;
    current_task->yield_switch = false;

    // Signal race check: if a deliverable signal is already pending, do not
    // move this task to wait queue; resume userspace with EINTR.
    bool skip_wait_queue = false;
    if (!IS_YIELD && current_task->wki_proxy_task_id == 0) {
        bool const WAITPID_STOP_READY =
            is_waitpid_wait_channel(current_task->wait_channel_kind) && waitpid_has_job_stop_ready(current_task);
        if (current_task->has_interrupting_signal_pending() && !WAITPID_STOP_READY) {
            skip_wait_queue = true;
            interrupt_waitpid_block_for_signal(current_task);
        }
    }
    current_task->deferred_task_switch = false;

    if (!IS_YIELD && !skip_wait_queue && is_waitpid_wait_channel(current_task->wait_channel_kind) && current_task->waiting_for_pid != 0) {
        (void)current_task->wakeup_pending.exchange(false, std::memory_order_acquire);
        skip_wait_queue = complete_or_preserve_waitpid_block(current_task);
    }

    bool notify_wki_proxy_blocked = false;
    task::Task* futex_abort_cleanup_task = nullptr;

    // Under lock: update EEVDF state and pick next task
    task::Task* next_task = run_queues->this_cpu_locked([current_task, IS_YIELD, skip_wait_queue, &notify_wki_proxy_blocked,
                                                         &futex_abort_cleanup_task](RunQueue* rq) -> task::Task* {
        // Account for time used during syscall
        uint64_t const NOW_US = time::get_us();
        auto delta_us = static_cast<int64_t>(NOW_US - rq->last_tick_us);
        rq->last_tick_us = NOW_US;
        if (delta_us <= 0) {
            delta_us = 1;
        }

        auto chargeable_us = static_cast<uint64_t>(delta_us);
        if (current_task->type != task::TaskType::IDLE) {
            chargeable_us = account_runtime_slice(rq, current_task, NOW_US, static_cast<uint64_t>(delta_us), false, true);
        }
        uint64_t const DELTA_NS = runtime_delta_ns_from_us(chargeable_us);

        if (rq->runnable_heap.contains(current_task)) {
            int64_t const VRUNTIME_DELTA = vruntime_delta_from_runtime_ns(DELTA_NS, current_task->sched_weight);
            current_task->vruntime += VRUNTIME_DELTA;
            add_weighted_vruntime_delta(rq, VRUNTIME_DELTA, current_task->sched_weight);
        }

        // A concurrent wakeup may have seen this task as rq->currentTask and
        // set wakeupPending instead of moving it. Blocking now would lose that
        // wake and leave the task stuck, so treat it as a yield instead.
        //
        // Remote execve handoff is the exception: once execve has been
        // accepted as a proxy task, a signal wake must not turn the handoff
        // back into a normal syscall return or userspace observes a spurious
        // successful execve() return.
        bool woke = current_task->wakeup_pending.exchange(false, std::memory_order_acquire);
        if (current_task->wki_proxy_task_id != 0 && current_task->wait_channel_is(task::WaitChannelKind::WKI_EXECVE_PROXY)) {
            woke = false;
        }
        if (woke && !skip_wait_queue && is_waitpid_wait_channel(current_task->wait_channel_kind) && current_task->waiting_for_pid != 0) {
            // A stale or unrelated wake must not make blocking waitpid() return
            // the syscall placeholder 0. Real wait completions clear
            // waiting_for_pid or set skip_wait_queue before this point.
            woke = false;
        }

        if (IS_YIELD || skip_wait_queue || woke) {
            if (is_futex_wait_channel(current_task->wait_channel_kind) && current_task->has_interrupting_signal_pending()) {
                current_task->context.regs.rax = static_cast<uint64_t>(-EINTR);
            }
            if (is_waitpid_wait_channel(current_task->wait_channel_kind) && current_task->has_interrupting_signal_pending() &&
                !waitpid_has_job_stop_ready(current_task)) {
                interrupt_waitpid_block_for_signal(current_task);
            }
            current_task->wants_block = false;
            current_task->wake_at_us = 0;
            finish_wait_metadata_for_runqueue(current_task);

            // A signal or concurrent wake can abort a futex block after the
            // waiter node has already been published. Defer detaching it
            // until after the runqueue lock drops; cleanup takes futex
            // bucket locks and must not run inside scheduler locking.
            futex_abort_cleanup_task = current_task;

            // Yield / target already exited / concurrent wakeup: task stays in heap with fresh deadline
            if (rq->runnable_heap.contains(current_task)) {
                note_perf_wait_callsite(current_task, current_task->context.frame.rip);
                current_task->slice_used_ns = 0;
                current_task->vdeadline = vdeadline_from_slice(current_task->vruntime, current_task->slice_ns, current_task->sched_weight);
                rq->runnable_heap.update(current_task);
            }
        } else {
            // Block: remove from heap, add to wait list
            if (rq->runnable_heap.contains(current_task)) {
                remove_from_sums(rq, current_task);
                rq->runnable_heap.remove(current_task);
            }
            current_task->last_run_us = current_task->slice_used_ns / 1000U;
            note_perf_wait_callsite(current_task, current_task->context.frame.rip);
            current_task->last_sleep_start_us = NOW_US;
            uint64_t const WAKE_AT_US = current_task->wake_at_us;
            current_task->sched_queue = task::Task::sched_queue::WAITING;
            wait_list_push_locked(rq, current_task);
            perf::record_sleep(static_cast<uint32_t>(cpu::current_cpu()), current_task->pid, WAKE_AT_US, perf_sleep_flags(WAKE_AT_US),
                               current_task->last_run_us, perf_wait_callsite(current_task), current_task->wait_channel);
            notify_wki_proxy_blocked = current_task->wki_proxy_task_id != 0;

            if (notify_wki_proxy_blocked) {
                // wki_proxy_task_blocked() can synchronously finalize proxy tasks
                // and wake waiters.  Keep current_task pointing at the stack we
                // are still running on until that callout is done.
                return nullptr;
            }
        }

        // Advance minVruntime
        int64_t const AVG = compute_avg_vruntime(rq);
        if (AVG > rq->min_vruntime) {
            int64_t const DELTA_MIN = AVG - rq->min_vruntime;
            rq->min_vruntime = AVG;
            rq->total_weighted_vruntime -= DELTA_MIN * rq->total_weight;
        }

        // Pick next task
        if (rq->runnable_heap.size == 0) {
            return nullptr;
        }
        task::Task* next = rq->runnable_heap.pick_best_eligible(AVG);
        reserve_handoff_task_locked(rq, next, time::get_us());
        return next;
    });

    if (futex_abort_cleanup_task != nullptr) {
        ker::syscall::futex::futex_wait_cleanup_for_task(futex_abort_cleanup_task);
    }

    if (notify_wki_proxy_blocked) {
        ker::net::wki::wki_proxy_task_blocked(current_task);

        next_task = run_queues->this_cpu_locked([current_task](RunQueue* rq) -> task::Task* {
            if (current_task->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE) {
                bool woke = current_task->wakeup_pending.exchange(false, std::memory_order_acquire);
                if (current_task->wki_proxy_task_id != 0 && current_task->wait_channel_is(task::WaitChannelKind::WKI_EXECVE_PROXY)) {
                    woke = false;
                }

                if (woke && current_task->sched_queue == task::Task::sched_queue::WAITING) {
                    task::WaitChannelKind const WAIT_KIND = current_task->wait_channel_kind;
                    wait_list_remove_locked(rq, current_task);
                    current_task->wants_block = false;
                    current_task->wake_at_us = 0;
                    finish_wait_metadata_for_runqueue(current_task);
                    current_task->just_woke = !is_low_latency_handoff_wait_channel(WAIT_KIND);
                    if (rq->runnable_heap.contains(current_task)) {
                        repair_stale_wait_membership_locked(rq, current_task);
                    } else {
                        current_task->vruntime =
                            std::max(current_task->vruntime, compute_wakeup_floor_vruntime(rq, current_task, time::get_us(), WAIT_KIND));
                        current_task->vdeadline =
                            vdeadline_from_slice(current_task->vruntime, current_task->slice_ns, current_task->sched_weight);
                        current_task->slice_used_ns = 0;
                        (void)publish_runnable_task_locked(rq, current_task, "proxy-block-wake");
                    }
                }
            }

            if (rq->runnable_heap.size == 0) {
                return nullptr;
            }

            int64_t const AVG = compute_avg_vruntime(rq);
            if (AVG > rq->min_vruntime) {
                int64_t const DELTA_MIN = AVG - rq->min_vruntime;
                rq->min_vruntime = AVG;
                rq->total_weighted_vruntime -= DELTA_MIN * rq->total_weight;
            }

            task::Task* next = rq->runnable_heap.pick_best_eligible(AVG);
            if (next == nullptr || next->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE ||
                (next->type == task::TaskType::PROCESS && (next->thread == nullptr || next->pagemap == nullptr)) ||
                next->type == task::TaskType::IDLE) {
                return nullptr;
            }

            reserve_handoff_task_locked(rq, next, time::get_us());
            return next;
        });

        if (is_dead_gc_candidate_task(current_task) && current_task->state.load(std::memory_order_acquire) == task::TaskState::DEAD &&
            !current_task->gc_queued.load(std::memory_order_acquire)) {
            insert_into_dead_list(current_task);
        }
    }

#ifdef SCHED_DEBUG
    char const* switch_reason = "wait queue";
    if (IS_YIELD) {
        switch_reason = "yield (heap)";
    } else if (skip_wait_queue) {
        switch_reason = "skip-wait (heap)";
    }
    dbg::log("deferred_task_switch: Moved PID %x to %s", current_task->pid, switch_reason);
#endif

    // Perf: deferred context switch (yield or block path)
    if (next_task != nullptr && next_task != current_task) {
        uint8_t const SW_FLAGS = (IS_YIELD || skip_wait_queue) ? perf::PERF_FLAG_YIELD : perf::PERF_FLAG_BLOCK;
        perf::record_switch(static_cast<uint32_t>(cpu::current_cpu()), current_task->pid, next_task->pid, SW_FLAGS, 0,
                            current_task->slice_used_ns / 1000U, (IS_YIELD || !skip_wait_queue) ? perf_wait_callsite(current_task) : 0);
    }

    if (next_task == current_task) {
        // We are returning through syscall.asm's normal epilogue, not through
        // wos_deferred_task_switch_return(), so publish deferred wait results
        // back to the live syscall stack as well as Task::context.
        clear_handoff_task(current_task);
        arm_local_timer_after_deferred_switch(current_task);
        *gpr_ptr = current_task->context.regs;
        *return_value_slot = current_task->context.regs.rax;
        record_local_proc_first_run(current_task, WOS_PERF_CALLSITE());
        current_task->has_run = true;
        debug_task_slot(cpu::current_cpu()) = current_task;
        return;
    }

    if (next_task == nullptr || next_task->type == task::TaskType::IDLE) {
        // Enter idle loop
        asm volatile("cli" ::: "memory");
        auto* rq = run_queues->this_cpu();
        enter_idle_loop(rq);
    }

    prepare_first_run_daemon(next_task, "deferred-switch");
    record_local_proc_first_run(next_task, WOS_PERF_CALLSITE());
    next_task->has_run = true;

    bool restored_deferred_sigreturn = false;
    if (next_task->type == task::TaskType::PROCESS && !next_task->is_voluntary_blocked()) {
        static_cast<void>(sys::context_switch::repair_stale_process_syscall_resume(next_task));
        restored_deferred_sigreturn = sys::signal::restore_deferred_sigreturn(next_task) == sys::signal::DeferredSigreturnResult::RESTORED;
    }

    // Set up GS/FS for next task
    uint64_t const REAL_CPU_ID = cpu::current_cpu();
    sys::context_switch::install_task_cpu_bases(next_task, REAL_CPU_ID);
    desc::gdt::set_rsp0(reinterpret_cast<uint64_t*>(next_task->context.syscall_kernel_stack), REAL_CPU_ID);

    mm::virt::switch_pagemap(next_task);

    // Validate context before restoring (only for PROCESS tasks - DAEMON uses kernel addresses)
    // Skip validation when voluntary_block is set - kernel-mode context is legitimate.
    if (next_task->type == task::TaskType::PROCESS && !next_task->is_voluntary_blocked()) {
        if (next_task->context.frame.cs != desc::gdt::GDT_USER_CS) {
            dbg::log("deferred_task_switch: CORRUPT cs=0x%x (expected 0x%x) PID %x", next_task->context.frame.cs, desc::gdt::GDT_USER_CS,
                     next_task->pid);
            hcf();
        }
        if (next_task->context.frame.ss != desc::gdt::GDT_USER_DS) {
            dbg::log("deferred_task_switch: CORRUPT ss=0x%x (expected 0x%x) PID %x", next_task->context.frame.ss, desc::gdt::GDT_USER_DS,
                     next_task->pid);
            hcf();
        }
        if (next_task->context.frame.rip >= 0x800000000000ULL) {
            dbg::log("deferred_task_switch: CORRUPT rip=0x%x PID %x", next_task->context.frame.rip, next_task->pid);
            hcf();
        }
        if (next_task->context.frame.rsp >= 0x800000000000ULL) {
            dbg::log("deferred_task_switch: CORRUPT rsp=0x%x PID %x", next_task->context.frame.rsp, next_task->pid);
            hcf();
        }
    }
    validate_user_resume_target(next_task, "deferred-resume-next");

    asm volatile("cli" ::: "memory");

    validate_kernel_resume_target(next_task, "deferred-resume-next");
    if (!restored_deferred_sigreturn) {
        if (next_task == get_current_task()) {
            sys::signal::check_pending_signals_deferred(next_task, sys::signal::DeferredSignalDelivery::FULL);
        } else if (next_task == get_return_task()) {
            sys::signal::check_pending_signals_deferred(next_task, sys::signal::DeferredSignalDelivery::USER_HANDLERS_ONLY);
        }
    }

    // Restore incoming task's FPU/SSE/AVX state (PROCESS tasks only).
    if (next_task->type == task::TaskType::PROCESS && next_task->fx_state.saved) {
        sys::context_switch::restore_fpu_state(next_task);
    }
    sys::context_switch::restore_debug_registers_for_task(next_task);

    arm_local_timer_after_deferred_switch(next_task);
    wos_deferred_task_switch_return(&next_task->context.regs, &next_task->context.frame);
    __builtin_unreachable();
}

// ============================================================================
// placeTaskInWaitQueue - block current task on I/O
// ============================================================================

void place_task_in_wait_queue(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame) {
    auto* current_task = get_current_task();
    if (current_task == nullptr) {
        return;
    }

    // Save context
    current_task->context.regs = gpr;
    current_task->context.frame = frame;
    if (current_task->type == task::TaskType::PROCESS) {
        sys::context_switch::save_fpu_state(current_task);
    }

    // Under lock: remove from heap, push to wait list, pick next
    task::Task* next_task = run_queues->this_cpu_locked([current_task](RunQueue* rq) -> task::Task* {
        uint64_t const NOW_US = time::get_us();
        // Remove from heap
        if (rq->runnable_heap.contains(current_task)) {
            remove_from_sums(rq, current_task);
            rq->runnable_heap.remove(current_task);
        }
        current_task->last_run_us = current_task->slice_used_ns / 1000U;
        current_task->perf_wait_callsite = current_task->context.frame.rip;
        current_task->last_sleep_start_us = NOW_US;
        current_task->sched_queue = task::Task::sched_queue::WAITING;
        wait_list_push_locked(rq, current_task);

        // Pick next
        if (rq->runnable_heap.size == 0) {
            return nullptr;
        }
        int64_t const AVG = compute_avg_vruntime(rq);
        task::Task* next = rq->runnable_heap.pick_best_eligible(AVG);
        reserve_handoff_task_locked(rq, next, NOW_US);
        return next;
    });
#ifdef SCHED_DEBUG
    dbg::log("placeTaskInWaitQueue: Moved PID %x to wait queue", current_task->pid);
#endif
    if (next_task != nullptr && next_task->type != task::TaskType::IDLE) {
        prepare_first_run_daemon(next_task, "waitqueue-switch");
        if (!sys::context_switch::switch_to(gpr, frame, next_task)) {
            dbg::log("placeTaskInWaitQueue: switchTo failed, entering idle");
            auto* rq = run_queues->this_cpu();
            clear_handoff_task(next_task);
            enter_idle_loop(rq);
        } else {
            record_local_proc_first_run(next_task, WOS_PERF_CALLSITE());
            next_task->has_run = true;
        }
    } else {
        // Only idle or no tasks - enter idle on the per-CPU idle task stack.
        auto* rq = run_queues->this_cpu();
        enter_idle_loop(rq);
    }
}

// ============================================================================
// rescheduleTaskForCpu - wake task from wait queue onto target CPU
// ============================================================================

void reschedule_task_for_cpu(uint64_t cpu_no, task::Task* task) {
    if (task == nullptr || run_queues == nullptr) {
        return;
    }

    uint64_t const NCPUS_RESCHED = smt::get_core_count();
    if (NCPUS_RESCHED == 0) {
        return;
    }
    if (cpu_no >= NCPUS_RESCHED) {
        cpu_no = get_least_loaded_cpu();
        if (cpu_no >= NCPUS_RESCHED) {
            return;
        }
    }

#ifdef SCHED_DEBUG
    // DIAGNOSTIC: Always log reschedule attempts
    dbg::log("RESCHED: PID %x -> CPU %d (heapIdx=%d, schedQ=%d, curCpu=%d)", task->pid, static_cast<int>(cpu_no), task->heap_index,
             static_cast<int>(task->sched_queue), static_cast<int>(cpu::current_cpu()));
#endif
    // Don't reschedule tasks that are exiting or dead
    auto state = task->state.load(std::memory_order_acquire);
    if (state != task::TaskState::ACTIVE) {
#ifdef SCHED_DEBUG
        dbg::log("RESCHED: PID %x SKIP - not ACTIVE (state=%d)", task->pid, static_cast<int>(state));
#endif
        return;
    }

    // Remove from whatever queue the task is in.
    // Optimization: check the task's last known CPU first (O(1) common case).
    // Only fall back to scanning all CPUs if not found there (handles races).
    // CRITICAL: If the task is currentTask on some CPU, that CPU is actively running
    // it (possibly in a syscall). Don't move it - let the timer preempt it naturally.
    bool is_current_on_some_cpu = false;
    uint64_t current_cpu_of_task = UINT64_MAX;
    uint64_t found_owner_cpu = UINT64_MAX;
    bool found_and_removed = false;

    // Fast path: task's last CPU
    uint64_t const LAST_CPU = task->cpu;
    if (LAST_CPU < NCPUS_RESCHED) {
        run_queues->with_lock_void(
            LAST_CPU, [task, &is_current_on_some_cpu, &current_cpu_of_task, &found_owner_cpu, &found_and_removed, LAST_CPU](RunQueue* rq) {
                if (runqueue_task_is_reserved_locked(rq, task)) {
                    is_current_on_some_cpu = true;
                    current_cpu_of_task = LAST_CPU;
                    found_owner_cpu = LAST_CPU;
                    task->cpu = LAST_CPU;
                    found_and_removed = true;
                    return;
                }
                if (wait_list_remove_locked(rq, task)) {
                    found_owner_cpu = LAST_CPU;
                    found_and_removed = true;
                }
                if (rq->runnable_heap.contains(task)) {
                    remove_from_sums(rq, task);
                    rq->runnable_heap.remove(task);
                    found_owner_cpu = LAST_CPU;
                    found_and_removed = true;
                }
            });
    }

    // Rare recovery path: task->cpu is the authoritative owner for runnable,
    // waiting, current, and handoff tasks.  Normal wakeups must not reach this
    // path; if the owner is stale, keep correctness by scanning once and count
    // it so the invariant violation is visible in perf cpustat.
    if (!found_and_removed && !is_current_on_some_cpu) {
        if constexpr (K_ENABLE_SCHED_OWNER_SCAN_FALLBACK) {
            run_queues->this_cpu()->slow_reschedule_scans.fetch_add(1, std::memory_order_relaxed);
            for (uint64_t search_cpu = 0; search_cpu < NCPUS_RESCHED; ++search_cpu) {
                if (search_cpu == LAST_CPU) {
                    continue;  // already checked
                }
                run_queues->with_lock_void(search_cpu, [task, &is_current_on_some_cpu, &current_cpu_of_task, &found_owner_cpu,
                                                        &found_and_removed, search_cpu](RunQueue* rq) {
                    if (found_and_removed || is_current_on_some_cpu) {
                        return;
                    }
                    if (runqueue_task_is_reserved_locked(rq, task)) {
                        is_current_on_some_cpu = true;
                        current_cpu_of_task = search_cpu;
                        found_owner_cpu = search_cpu;
                        task->cpu = search_cpu;
#ifdef SCHED_DEBUG
                        dbg::log("RESCHED: PID %x is currentTask on CPU %d", task->pid, static_cast<int>(search_cpu));
#endif
                        return;
                    }
                    if (wait_list_remove_locked(rq, task)) {
                        found_owner_cpu = search_cpu;
                        found_and_removed = true;
#ifdef SCHED_DEBUG
                        dbg::log("RESCHED: PID %x removed from CPU %d wait_list", task->pid, static_cast<int>(search_cpu));
#endif
                    }
                    if (rq->runnable_heap.contains(task)) {
#ifdef SCHED_DEBUG
                        dbg::log("RESCHED: PID %x found in CPU %d heap (idx=%d), removing", task->pid, static_cast<int>(search_cpu),
                                 task->heap_index);
#endif
                        remove_from_sums(rq, task);
                        rq->runnable_heap.remove(task);
                        found_owner_cpu = search_cpu;
                        found_and_removed = true;
                    }
                });
            }
        }
    }

    // If the task is currently executing on a CPU, don't re-insert it.
    // It will be picked up by the scheduler on its current CPU when it
    // yields or its syscall returns.
    if (is_current_on_some_cpu) {
#ifdef SCHED_DEBUG
        dbg::log("RESCHED: PID %x ABORT - is currentTask somewhere", task->pid);
#endif
        // Signal to deferred_task_switch that a wakeup was attempted while the
        // task is still currentTask.  This remains required for CANCEL wakes:
        // syscall.asm may already have observed deferred_task_switch before the
        // waker cleared it, and deferred_task_switch must see a wake token
        // instead of parking after the event owner has removed its waiter.
        task->wakeup_pending.store(true, std::memory_order_release);

        uint64_t const NOW_US = time::get_us();
        uint64_t const WAKE_AT_US = task->wake_at_us;
        bool const WAKE_CURRENT = task->is_voluntary_blocked() || task->wants_block || task->last_sleep_start_us != 0;
        // If a wake races a deferred kern_block()/kern_sleep_us() request,
        // cancel the pending sleep/block before nudging the CPU. Otherwise the
        // next scheduler pass can still move the task to WAITING after the
        // event already arrived, effectively losing the wake.
        task->wants_block = false;
        task->wake_at_us = 0;
        if (WAKE_CURRENT && current_cpu_of_task < perf::get_num_perf_cpus()) {
            perf::record_wake(static_cast<uint32_t>(current_cpu_of_task), task->pid, WAKE_AT_US, perf_wake_flags(WAKE_AT_US, true, true),
                              observed_sleep_us(task, NOW_US), perf_wait_callsite(task), task->wait_channel);
        }

        // A wake can race with the task entering a preemptible syscall park:
        // the task is still the CPU's current task, but voluntary_block may not
        // be visible to this CPU yet. We already recorded wakeup_pending, so
        // always poke that CPU to make sure it observes the flag promptly.
        // Use request_reschedule() for same-CPU since wake_cpu() is a no-op
        // for the current CPU. request_reschedule() falls back to a delayed
        // timer if this runs during a stack handoff window.
        if (current_cpu_of_task != UINT64_MAX) {
            if (current_cpu_of_task == cpu::current_cpu()) {
                request_local_reschedule();
            } else {
                wake_cpu(current_cpu_of_task, WakeCpuMode::FORCE);
            }
        }
        return;
    }

    if (is_waitpid_wait_channel(task->wait_channel_kind) && task->waiting_for_pid != 0 && !complete_or_preserve_waitpid_block(task)) {
        if (found_and_removed) {
            uint64_t owner_cpu = found_owner_cpu;
            if (owner_cpu >= NCPUS_RESCHED) {
                owner_cpu = task->cpu < NCPUS_RESCHED ? task->cpu : cpu_no;
            }
            run_queues->with_lock_void(owner_cpu, [task](RunQueue* rq) {
                if (runqueue_task_is_reserved_locked(rq, task) || rq->runnable_heap.contains(task) || wait_list_contains_locked(rq, task)) {
                    return;
                }
                task->sched_queue = task::Task::sched_queue::WAITING;
                if (task->last_sleep_start_us == 0) {
                    task->last_sleep_start_us = time::get_us();
                }
                wait_list_push_locked(rq, task);
            });
        }
        return;
    }

// Insert into target CPU's heap with updated vruntime.
// For pinned tasks, always re-insert on the task's own CPU, not the requested cpu_no.
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x INSERT -> CPU %d (heapIdx=%d before insert)", task->pid, static_cast<int>(cpu_no), task->heap_index);
#endif

    // Guard against concurrent wakers: if another reschedule_task_for_cpu() already
    // inserted this task into a heap between our remove phase and now, don't
    // double-insert. Just poke the CPU it landed on.
    if (task->heap_index >= 0) {
        uint64_t const LANDED_CPU = task->cpu;
        if (LANDED_CPU < smt::get_core_count()) {
            run_queues->with_lock_void(LANDED_CPU, [task](RunQueue* rq) { repair_stale_wait_membership_locked(rq, task); });
            if (LANDED_CPU == cpu::current_cpu()) {
                request_local_reschedule();
            } else {
                wake_cpu(LANDED_CPU);
            }
        }
        return;
    }

    if (task->cpu_pinned) {
        uint64_t pinned_cpu = task->cpu;
        if (pinned_cpu >= NCPUS_RESCHED) {
            pinned_cpu = found_owner_cpu < NCPUS_RESCHED ? found_owner_cpu : cpu_no;
            task->cpu = pinned_cpu;
        }
        cpu_no = pinned_cpu;  // Ignore requested CPU - keep on pinned CPU
    } else {
        task->cpu = cpu_no;
    }
    run_queues->with_lock_void(cpu_no, [task, cpu_no](RunQueue* rq) {
        // Double-check under lock: another waker may have inserted between
        // the unlocked heapIndex check and acquiring this lock.
        if (task->heap_index >= 0) {
            repair_stale_wait_membership_locked(rq, task);
            return;
        }

        uint64_t const NOW_US = time::get_us();
        bool const WAS_WAITING = (task->sched_queue == task::Task::sched_queue::WAITING);
        uint64_t const WAKE_AT_US = task->wake_at_us;
        char const* const WAIT_CHANNEL = task->wait_channel;
        task::WaitChannelKind const WAIT_KIND = task->wait_channel_kind;
        if (!process_has_kernel_resume_frame(task)) {
            task->clear_wait_channel();
        } else {
            task->set_voluntary_blocked(true);
        }
        // Mark justWoke if this is a sleep->run transition (task was in WAITING state).
        // justWoke inhibits wakeup-preemption for SCHED_MIN_GRANULARITY_US; see process_tasks.
        task->just_woke = WAS_WAITING && !is_low_latency_handoff_wait_channel(WAIT_KIND);
        // Clamp vruntime to [minVruntime, avg_vruntime] on wakeup.
        // Using minVruntime gives the task an artificially early vdeadline
        // that immediately preempts active compute threads - causing thrashing
        // when daemons wake up frequently (e.g. every 4ms for netd/httpd).
        // Using avg_vruntime places the task at the current fair position,
        // so it doesn't cut ahead of tasks that have been running continuously.
        int64_t const FAIR_VRUNTIME =
            WAS_WAITING ? compute_wakeup_floor_vruntime(rq, task, NOW_US, WAIT_KIND) : std::max(rq->min_vruntime, compute_avg_vruntime(rq));
        task->vruntime = std::max(task->vruntime, FAIR_VRUNTIME);
        if (WAS_WAITING) {
            perf::record_wake(static_cast<uint32_t>(cpu_no), task->pid, WAKE_AT_US, perf_wake_flags(WAKE_AT_US, true, false),
                              observed_sleep_us(task, NOW_US), perf_wait_callsite(task), WAIT_CHANNEL);
            note_task_wakeup(task, NOW_US, WAIT_KIND);
        }
        task->vdeadline = vdeadline_from_slice(task->vruntime, task->slice_ns, task->sched_weight);
        task->slice_used_ns = 0;
        // Clear any pending block request and timeout deadline - we're explicitly
        // waking this task.  If wake_at_us is left set, a timed
        // preemptible_syscall_park() can resume after hlt, see the stale
        // deadline, and park again even though the event already arrived.
        task->wants_block = false;
        task->wake_at_us = 0;

        (void)publish_runnable_task_locked(rq, task, "reschedule");
    });

    // Poke the target CPU so the newly-rescheduled task runs promptly.
    // wake_cpu() is a no-op for the current CPU (can't self-IPI), so use
    // request_reschedule() to arm the APIC timer with count=1 instead.
    // This avoids up to 1ms of scheduling latency on same-CPU wakes.
    if (cpu_no == cpu::current_cpu()) {
        request_local_reschedule();
    } else {
        wake_cpu(cpu_no);
    }
#ifdef SCHED_DEBUG
    dbg::log("RESCHED: PID %x DONE -> CPU %d (heapIdx=%d)", task->pid, static_cast<int>(cpu_no), task->heap_index);
#endif
}

// ============================================================================
// PID lookup (O(1) via registry)
// ============================================================================

__attribute__((no_sanitize("address"))) auto find_task_by_pid(uint64_t pid) -> task::Task* { return pid_table_find(pid); }

__attribute__((no_sanitize("address"))) auto find_task_by_pid_safe(uint64_t pid) -> task::Task* {
    return pid_table_find_internal(pid, true);
}

auto task_has_live_pagemap_sibling(task::Task* subject) -> bool {
    if (subject == nullptr || subject->pagemap == nullptr) {
        return false;
    }

    auto* const PAGEMAP = subject->pagemap;
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    for (uint32_t i = 0; i < active_task_count; ++i) {
        auto* other = active_task_slot(i);
        if (other == nullptr || other == subject || other->pagemap != PAGEMAP) {
            continue;
        }
        global_task_registry_lock.unlock_irqrestore(FLAGS);
        return true;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);

    if (run_queues == nullptr) {
        return false;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        bool const FOUND = run_queues->with_lock(cpu_no, [subject, PAGEMAP](RunQueue* rq) -> bool {
            if (rq == nullptr) {
                return false;
            }
            for (task::Task* cur = rq->dead_list.head; cur != nullptr; cur = cur->sched_next) {
                if (cur != subject && cur->pagemap == PAGEMAP) {
                    return true;
                }
            }
            return false;
        });
        if (FOUND) {
            return true;
        }
    }
    return false;
}

auto get_active_task_count() -> uint32_t {
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    uint32_t const COUNT = active_task_count;
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

auto get_active_task_at(uint32_t index) -> task::Task* {
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    task::Task* task = (index < active_task_count) ? active_task_slot(index) : nullptr;
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return task;
}

auto get_active_task_at_safe(uint32_t index) -> task::Task* {
    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    task::Task* task = (index < active_task_count) ? active_task_slot(index) : nullptr;
    if (task != nullptr && !task->try_acquire()) {
        task = nullptr;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return task;
}

auto find_active_task_lifetime_ref_if(ActiveTaskPredicate predicate, void* context) -> task::Task* {
    if (predicate == nullptr) {
        return nullptr;
    }

    uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
    for (uint32_t i = 0; i < active_task_count; ++i) {
        task::Task* task = active_task_slot(i);
        if (task == nullptr || !predicate(task, context)) {
            continue;
        }
        if (!task->try_acquire_lifetime_ref()) {
            continue;
        }
        global_task_registry_lock.unlock_irqrestore(FLAGS);
        return task;
    }
    global_task_registry_lock.unlock_irqrestore(FLAGS);
    return nullptr;
}

auto find_dead_task_lifetime_ref_if(DeadTaskPredicate predicate, void* context) -> task::Task* {
    if (predicate == nullptr || run_queues == nullptr) {
        return nullptr;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        auto* found = run_queues->with_lock(cpu_no, [predicate, context](RunQueue* rq) -> task::Task* {
            if (rq == nullptr) {
                return nullptr;
            }
            for (task::Task* cur = rq->dead_list.head; cur != nullptr; cur = cur->sched_next) {
                if (!predicate(cur, context)) {
                    continue;
                }
                if (!cur->try_acquire_lifetime_ref()) {
                    continue;
                }
                return cur;
            }
            return nullptr;
        });
        if (found != nullptr) {
            return found;
        }
    }

    return nullptr;
}

auto debug_find_task_by_kernel_stack(uint64_t rsp) -> task::Task* {
    if (rsp < 0xffff800000000000ULL || rsp >= 0xffff900000000000ULL) {
        return nullptr;
    }
    uint32_t count = active_task_count;
    count = std::min(count, MAX_ACTIVE_TASKS);
    for (uint32_t i = 0; i < count; ++i) {
        auto* task = active_task_slot(i);
        if (task == nullptr || task->context.syscall_kernel_stack == 0) {
            continue;
        }
        uint64_t const STACK_TOP = task->context.syscall_kernel_stack;
        if (rsp > STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE && rsp <= STACK_TOP) {
            return task;
        }
    }
    return nullptr;
}

auto debug_find_dead_task_by_kernel_stack(uint64_t rsp) -> task::Task* {
    if (rsp < 0xffff800000000000ULL || rsp >= 0xffff900000000000ULL || run_queues == nullptr) {
        return nullptr;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        task::Task* owner = nullptr;
        run_queues->try_with_lock(cpu_no, [rsp, &owner](RunQueue* rq) {
            if (rq == nullptr) {
                return;
            }
            for (task::Task* cur = rq->dead_list.head; cur != nullptr; cur = cur->sched_next) {
                uint64_t const STACK_TOP = cur->context.syscall_kernel_stack;
                if (STACK_TOP == 0) {
                    continue;
                }
                if (rsp > STACK_TOP - ker::mod::mm::KERNEL_STACK_SIZE && rsp <= STACK_TOP) {
                    owner = cur;
                    return;
                }
            }
        });
        if (owner != nullptr) {
            return owner;
        }
    }

    return nullptr;
}

auto get_dead_task_count(uint64_t cpu_no) -> size_t {
    if (run_queues == nullptr || cpu_no >= smt::get_core_count()) {
        return 0;
    }
    return run_queues->with_lock(cpu_no, [](RunQueue* rq) -> size_t {
        size_t count = 0;
        if (rq == nullptr) {
            return 0;
        }
        for (task::Task const* cur = rq->dead_list.head; cur != nullptr; cur = cur->sched_next) {
            ++count;
        }
        return count;
    });
}

auto get_dead_task_at_safe(uint64_t cpu_no, size_t index) -> task::Task* {
    if (run_queues == nullptr || cpu_no >= smt::get_core_count()) {
        return nullptr;
    }
    return run_queues->with_lock(cpu_no, [index](RunQueue* rq) -> task::Task* {
        if (rq == nullptr) {
            return nullptr;
        }
        size_t cur_index = 0;
        for (task::Task* cur = rq->dead_list.head; cur != nullptr; cur = cur->sched_next, ++cur_index) {
            if (cur_index != index) {
                continue;
            }
            if (!cur->try_acquire_lifetime_ref()) {
                return nullptr;
            }
            return cur;
        }
        return nullptr;
    });
}

void wake_task_for_signal(task::Task* task) {
    if (task == nullptr) {
        return;
    }
    if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE || task->has_exited) {
        return;
    }

    constexpr uint64_t SIGCONT_MASK = 1ULL << (18 - 1);
    constexpr uint64_t SIGKILL_MASK = 1ULL << (9 - 1);
    uint64_t const PENDING = task->sig_pending;
    if ((PENDING & (SIGCONT_MASK | SIGKILL_MASK)) != 0U && task->jobctl_stopped.load(std::memory_order_acquire)) {
        task->jobctl_stopped.store(false, std::memory_order_release);
        if ((PENDING & SIGCONT_MASK) != 0U) {
            task->jobctl_stop_pending.store(false, std::memory_order_release);
        }
        task->set_voluntary_blocked(false);
        task->wants_block = false;
        task->wake_at_us = 0;
        task->clear_wait_channel();
    }

    // Nudge the task's current CPU immediately so hlt/deferred paths react fast.
    // For same-CPU delivery we cannot self-IPI, so arm the local timer instead.
    if (task->cpu == cpu::current_cpu()) {
        request_local_reschedule();
    } else {
        // Signal latency should not depend on an earlier coalesced wake timer.
        wake_cpu(task->cpu, WakeCpuMode::FORCE);
    }

    // If the task is blocked or sleeping in a syscall path, put it back on
    // a runnable queue so it can process pending signals promptly.
    if (task->sched_queue == task::Task::sched_queue::WAITING || task->deferred_task_switch || task->is_voluntary_blocked()) {
        bool const HAS_FUTEX_WAITER = task->futex_waiter.load(std::memory_order_acquire) != nullptr;
        bool const MAY_INTERRUPT_FUTEX = is_futex_wait_channel(task->wait_channel_kind) || HAS_FUTEX_WAITER;
        bool const MAY_INTERRUPT_WAITPID = is_waitpid_wait_channel(task->wait_channel_kind);
        bool const WAITPID_STOP_READY = MAY_INTERRUPT_WAITPID && waitpid_has_job_stop_ready(task);
        bool const HAS_INTERRUPTING_SIGNAL = (MAY_INTERRUPT_FUTEX || MAY_INTERRUPT_WAITPID) && task->has_interrupting_signal_pending();
        bool const INTERRUPTS_WAIT = task->wait_channel_is(task::WaitChannelKind::SIGSUSPEND) ||
                                     (MAY_INTERRUPT_FUTEX && HAS_INTERRUPTING_SIGNAL) || (MAY_INTERRUPT_WAITPID && HAS_INTERRUPTING_SIGNAL);
        if (MAY_INTERRUPT_WAITPID && HAS_INTERRUPTING_SIGNAL && !WAITPID_STOP_READY) {
            interrupt_waitpid_block_for_signal(task);
        } else if (INTERRUPTS_WAIT && !WAITPID_STOP_READY) {
            task->context.regs.rax = static_cast<uint64_t>(-EINTR);
        }
        if (HAS_FUTEX_WAITER && HAS_INTERRUPTING_SIGNAL) {
            ker::syscall::futex::futex_wait_cleanup_for_task(task);
        }

        // Use the least-loaded CPU to avoid piling daemon wakeups onto CPUs
        // that are busy running compute threads.
        uint64_t const CPU = get_least_loaded_cpu();
        reschedule_task_for_cpu(CPU, task);
    }
}

auto signal_process_group(uint64_t pgid, int sig) -> size_t {
    if (pgid == 0 || sig < 0 || std::cmp_greater(sig, task::Task::MAX_SIGNALS)) {
        return 0;
    }
    uint64_t const MASK = sig > 0 ? (1ULL << (sig - 1)) : 0;
    size_t matched = 0;
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* t = get_active_task_at_safe(i);
        if (t != nullptr) {
            bool const ALIVE = t->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !t->has_exited;
            if (effective_process_group_id(*t) == pgid && ALIVE) {
                ++matched;
                if (sig != 0) {
                    bool const FORWARDED = ker::net::wki::wki_proxy_task_forward_signal(t, sig);
                    if (!FORWARDED) {
                        t->sig_pending |= MASK;
                        wake_task_for_signal(t);
                    }
                }
            }
            t->release();
        }
    }
    return matched;
}

auto signal_controlling_tty(int controlling_tty, int sig) -> size_t {
    if (controlling_tty < 0 || sig < 0 || std::cmp_greater(sig, task::Task::MAX_SIGNALS)) {
        return 0;
    }
    uint64_t const MASK = sig > 0 ? (1ULL << (sig - 1)) : 0;
    size_t matched = 0;
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* t = get_active_task_at_safe(i);
        if (t == nullptr) {
            continue;
        }
        bool const ALIVE = t->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !t->has_exited;
        if (ALIVE && t->type == task::TaskType::PROCESS && t->controlling_tty == controlling_tty) {
            ++matched;
            if (sig != 0) {
                bool const FORWARDED = ker::net::wki::wki_proxy_task_forward_signal(t, sig);
                if (!FORWARDED) {
                    t->sig_pending |= MASK;
                    wake_task_for_signal(t);
                }
            }
        }
        t->release();
    }
    return matched;
}

auto signal_controlling_tty_wki_proxies(int controlling_tty, int sig) -> size_t {
    if (controlling_tty < 0 || sig < 0 || std::cmp_greater(sig, task::Task::MAX_SIGNALS)) {
        return 0;
    }

    size_t matched = 0;
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; i++) {
        auto* t = get_active_task_at_safe(i);
        if (t == nullptr) {
            continue;
        }
        bool const ALIVE = t->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !t->has_exited;
        if (ALIVE && t->type == task::TaskType::PROCESS && t->controlling_tty == controlling_tty && t->wki_proxy_task_id != 0) {
            if (sig == 0 || ker::net::wki::wki_proxy_task_forward_signal(t, sig)) {
                ++matched;
            }
        }
        t->release();
    }
    return matched;
}

auto signal_visible_processes_except(uint64_t excluded_pid, uint64_t excluded_owner_pid, int sig) -> size_t {
    if (sig < 0 || std::cmp_greater(sig, task::Task::MAX_SIGNALS)) {
        return 0;
    }
    uint64_t const MASK = sig > 0 ? (1ULL << (sig - 1)) : 0;
    size_t matched = 0;
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* t = get_active_task_at_safe(i);
        if (t == nullptr) {
            continue;
        }
        bool const ALIVE = t->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !t->has_exited;
        uint64_t const PROCESS_PID = task::process_pid(*t);
        bool const EXCLUDED = t->pid == excluded_pid || PROCESS_PID == excluded_owner_pid || PROCESS_PID == 1;
        if (ALIVE && t->type == task::TaskType::PROCESS && task::process_visible(*t) && !EXCLUDED) {
            ++matched;
            if (sig != 0) {
                t->sig_pending |= MASK;
                wake_task_for_signal(t);
            }
        }
        t->release();
    }
    return matched;
}

// ============================================================================
// Garbage collection
// ============================================================================

void insert_into_dead_list(task::Task* task) {
    if (task == nullptr) {
        return;
    }

    if (is_gc_protected_idle_task(task)) {
        task::TaskState const PRE_STATE = task->state.load(std::memory_order_acquire);
        uint64_t const PRE_DEATH_EPOCH = task->death_epoch.load(std::memory_order_acquire);
        bool const PRE_GC_QUEUED = task->gc_queued.load(std::memory_order_acquire);
        auto const PRE_QUEUE = task->sched_queue;
        if (restore_gc_protected_idle_task(task)) {
            dbg::logger<"sched">::error(
                "repaired idle task after dead GC enqueue attempt: task=%p name=%s cpu=%lu pre_state=%u pre_queue=%d "
                "pre_death_epoch=%lu pre_gc_queued=%u stack=0x%lx scratch=0x%lx thread=%p pagemap=%p",
                task, task_name_for_log(task), task->cpu, static_cast<unsigned>(PRE_STATE), static_cast<int>(PRE_QUEUE), PRE_DEATH_EPOCH,
                PRE_GC_QUEUED ? 1U : 0U, task->context.syscall_kernel_stack, task->context.syscall_scratch_area, task->thread,
                task->pagemap);
        }
        return;
    }

    if (task->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
        return;
    }

    if (task->sched_queue == task::Task::sched_queue::DEAD_GC || task->gc_queued.load(std::memory_order_acquire)) {
        return;
    }

    bool found_current = false;
    uint64_t const LAST_CPU = task->cpu;
    uint64_t const CORE_COUNT = smt::get_core_count();

    auto detach_from_cpu = [task, &found_current](RunQueue* rq) {
        if (runqueue_task_is_reserved_locked(rq, task)) {
            found_current = true;
        }
        wait_list_remove_all_locked(rq, task);
        while (rq->runnable_heap.contains(task)) {
            remove_from_sums(rq, task);
            rq->runnable_heap.remove(task);
        }
    };

    if (LAST_CPU < CORE_COUNT) {
        run_queues->with_lock_void(LAST_CPU, detach_from_cpu);
    }

    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        if (cpu_no == LAST_CPU) {
            continue;
        }
        run_queues->with_lock_void(cpu_no, detach_from_cpu);
    }

    if (found_current) {
        return;
    }

    bool expected_gc_queued = false;
    if (!task->gc_queued.compare_exchange_strong(expected_gc_queued, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    task->sched_queue = task::Task::sched_queue::DEAD_GC;
    run_queues->with_lock_void(0, [task](RunQueue* rq) { rq->dead_list.push(task); });
    complete_parent_waitpid_after_dead_enqueue(task);
}

namespace {

inline auto gc_time_budget_expired(uint64_t start_us, uint64_t budget_us) -> bool {
    if (budget_us == 0) {
        return false;
    }
    uint64_t const NOW_US = time::get_us();
    return NOW_US >= start_us && (NOW_US - start_us) >= budget_us;
}

inline auto elapsed_us_since(uint64_t start_us, uint64_t end_us) -> uint64_t { return end_us >= start_us ? end_us - start_us : 0; }

struct GcDetachedTask {
    task::Task* task = nullptr;
    bool should_free_pagemap = false;
    bool counted_without_cleanup = false;
    bool zombie_resources_only = false;
};

struct GcTaskTiming {
    uint64_t total_us = 0;
    uint64_t detach_us = 0;
    uint64_t pagemap_us = 0;
    uint64_t thread_us = 0;
    uint64_t misc_us = 0;
    uint64_t debug_us = 0;
};

struct GcDeferredCleanupItem {
    task::Task* task = nullptr;
    bool should_free_pagemap = false;
    bool zombie_resources_only = false;
    mm::virt::DestroyUserSpaceBudgetState* pagemap_state = nullptr;
    uint64_t detach_us = 0;
    uint64_t enqueue_us = 0;
    GcDeferredCleanupItem* next = nullptr;
};

GcDeferredCleanupItem* gc_deferred_cleanup_head = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
GcDeferredCleanupItem* gc_deferred_cleanup_tail = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_items_queued{0};        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_items_completed{0};     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_depth{0};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_depth_max{0};           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_slices{0};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_slices_completed{0};    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> gc_deferred_cleanup_oldest_wait_us_max{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> scheduler_gc_idle_boost_passes{0};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> scheduler_gc_foreground_passes{0};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto gc_deferred_cleanup_has_work() -> bool { return gc_deferred_cleanup_depth.load(std::memory_order_relaxed) != 0; }

void note_gc_deferred_cleanup_push() {
    gc_deferred_cleanup_items_queued.fetch_add(1, std::memory_order_relaxed);
    uint64_t const DEPTH = gc_deferred_cleanup_depth.fetch_add(1, std::memory_order_relaxed) + 1;
    update_relaxed_max(gc_deferred_cleanup_depth_max, DEPTH);
}

void note_gc_deferred_cleanup_complete(uint64_t wait_us) {
    gc_deferred_cleanup_items_completed.fetch_add(1, std::memory_order_relaxed);
    gc_deferred_cleanup_slices_completed.fetch_add(1, std::memory_order_relaxed);
    update_relaxed_max(gc_deferred_cleanup_oldest_wait_us_max, wait_us);

    uint64_t current = gc_deferred_cleanup_depth.load(std::memory_order_relaxed);
    while (current != 0 &&
           !gc_deferred_cleanup_depth.compare_exchange_weak(current, current - 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

void gc_deferred_cleanup_push(GcDeferredCleanupItem* item) {
    if (item == nullptr) {
        return;
    }
    item->next = nullptr;
    if (gc_deferred_cleanup_tail != nullptr) {
        gc_deferred_cleanup_tail->next = item;
    } else {
        gc_deferred_cleanup_head = item;
    }
    gc_deferred_cleanup_tail = item;
    note_gc_deferred_cleanup_push();
}

auto gc_deferred_cleanup_front() -> GcDeferredCleanupItem* { return gc_deferred_cleanup_head; }

void gc_deferred_cleanup_pop() {
    if (gc_deferred_cleanup_head == nullptr) {
        return;
    }
    gc_deferred_cleanup_head = gc_deferred_cleanup_head->next;
    if (gc_deferred_cleanup_head == nullptr) {
        gc_deferred_cleanup_tail = nullptr;
    }
}

inline void add_gc_phase_sample(std::atomic<uint64_t>& total, std::atomic<uint64_t>& max, uint64_t elapsed_us) {
    total.fetch_add(elapsed_us, std::memory_order_relaxed);
    update_relaxed_max(max, elapsed_us);
}

void note_gc_task_timing(GcTaskTiming const& timing) {
    if (run_queues == nullptr) {
        return;
    }

    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }

    add_gc_phase_sample(rq->gc_task_us_total, rq->gc_task_us_max, timing.total_us);
    add_gc_phase_sample(rq->gc_detach_us_total, rq->gc_detach_us_max, timing.detach_us);
    add_gc_phase_sample(rq->gc_pagemap_us_total, rq->gc_pagemap_us_max, timing.pagemap_us);
    add_gc_phase_sample(rq->gc_thread_us_total, rq->gc_thread_us_max, timing.thread_us);
    add_gc_phase_sample(rq->gc_misc_us_total, rq->gc_misc_us_max, timing.misc_us);
    add_gc_phase_sample(rq->gc_debug_us_total, rq->gc_debug_us_max, timing.debug_us);
}

auto gc_task_has_pagemap_sibling_locked(task::Task* cur) -> bool {
    if (cur == nullptr || cur->pagemap == nullptr) {
        return false;
    }

    uint32_t const ACTIVE_COUNT = get_active_task_count();
    for (uint32_t ai = 0; ai < ACTIVE_COUNT; ai++) {
        auto* other = get_active_task_at_safe(ai);
        if (other != nullptr) {
            if (other != cur && other->pagemap == cur->pagemap && other->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
                other->release();
                return true;
            }
            other->release();
        }
    }

    // This mirrors the existing conservative shared-pagemap check: a sibling
    // that is already DEAD but still queued for GC must keep the pagemap alive
    // until that sibling is detached and cleaned up too.
    for (uint64_t scan_cpu = 0; scan_cpu < smt::get_core_count(); scan_cpu++) {
        auto* dl = run_queues->that_cpu(scan_cpu)->dead_list.head;
        while (dl != nullptr) {
            if (dl != cur && dl->pagemap == cur->pagemap) {
                return true;
            }
            dl = dl->sched_next;
        }
    }

    return false;
}

auto detach_next_reclaimable_task_locked(RunQueue* rq, uint64_t cpu_no) -> GcDetachedTask {
    if (rq == nullptr) {
        return {};
    }
#ifndef SCHED_DEBUG
    (void)cpu_no;
#endif

    task::Task* cur = rq->dead_list.head;
    while (cur != nullptr) {
        task::Task* next = cur->sched_next;

        if (cur->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
            cur = next;
            continue;
        }

        uint64_t const DEATH_EPOCH = cur->death_epoch.load(std::memory_order_acquire);
        if (!EpochManager::is_safe_to_reclaim(DEATH_EPOCH)) {
#ifdef SCHED_DEBUG
            static uint64_t epoch_skip_count = 0;
            if (++epoch_skip_count % 1000 == 1) {
                dbg::log("GC: PID %x death_epoch=%lu not safe yet", cur->pid, DEATH_EPOCH);
            }
#endif
            cur = next;
            continue;
        }

        // Check if any CPU still has this task as currentTask
        bool still_in_use = false;
        for (uint64_t check_cpu = 0; check_cpu < smt::get_core_count(); ++check_cpu) {
            if (run_queues->that_cpu(check_cpu)->current_task == cur) {
                still_in_use = true;
#ifdef SCHED_DEBUG
                dbg::log("GC: PID %x still current_task on CPU %d", cur->pid, check_cpu);
#endif
                break;
            }
        }
        if (still_in_use) {
            cur = next;
            continue;
        }

        uint32_t const RC = cur->ref_count.load(std::memory_order_acquire);
        if (RC != 1) {
            cur = next;
            continue;
        }
        if (cur->zombie_resources_reclaiming.load(std::memory_order_acquire)) {
            cur = next;
            continue;
        }

        // ZOMBIE BEHAVIOR: Don't reclaim until parent has called waitpid OR
        // the parent is dead. Threads are joined via futex and do not waitpid.
        // A waitable zombie must keep its Task/PID/exit status, but it does not
        // need to pin the full address space or kernel stack while waiting for
        // waitpid. Reclaim those heavy resources once the epoch/current-task
        // guards above prove the task is no longer executing.
        if (cur->has_exited && !task::task_waited_on(*cur) && !cur->is_thread) {
            if (cur->parent_pid != 0) {
                auto* parent = find_task_by_pid(cur->parent_pid);
                if (parent != nullptr && parent->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE) {
                    bool expected = false;
                    if (!cur->zombie_resources_reclaimed.load(std::memory_order_acquire) &&
                        cur->zombie_resources_reclaiming.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                                                 std::memory_order_acquire)) {
                        bool should_free_pagemap = false;
                        if (cur->pagemap != nullptr && cur->type != task::TaskType::DAEMON) {
                            should_free_pagemap = !gc_task_has_pagemap_sibling_locked(cur);
                        }
                        return {.task = cur,
                                .should_free_pagemap = should_free_pagemap,
                                .counted_without_cleanup = false,
                                .zombie_resources_only = true};
                    }
#ifdef SCHED_DEBUG
                    static uint64_t zombie_skip_count = 0;
                    if (++zombie_skip_count % 1000 == 1) {
                        dbg::log("GC: PID %x is zombie, waiting for parent PID %x to call waitpid", cur->pid, cur->parent_pid);
                    }
#endif
                    cur = next;
                    continue;
                }
#ifdef SCHED_DEBUG
                dbg::log("GC: PID %x is orphaned zombie (parent PID %x dead), reaping", cur->pid, cur->parent_pid);
#endif
            }
        }

#ifdef SCHED_DEBUG
        dbg::log("GC: Reclaiming PID %x from CPU %d", cur->pid, cpu_no);
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
            while (rq->dead_list.remove(cur)) {
            }
            dbg::log("GC: Leaking corrupted task %p to avoid crash", cur);
            return {.counted_without_cleanup = true};
        }

        bool should_free_pagemap = false;
        if (cur->pagemap != nullptr && cur->type != task::TaskType::DAEMON && !cur->is_thread) {
            should_free_pagemap = !gc_task_has_pagemap_sibling_locked(cur);
        }

        while (rq->dead_list.remove(cur)) {
        }
        cur->sched_queue = task::Task::sched_queue::NONE;

        // Once the task is out of the registries, no new pid/active-list lookup
        // can acquire it while the heavy cleanup runs outside the runqueue lock.
        if (cur->pid > 0) {
            pid_table_remove(cur->pid);
            active_list_remove(cur->pid);
        }

        return {.task = cur, .should_free_pagemap = should_free_pagemap};
    }

    return {};
}

void cleanup_task_after_pagemap(task::Task* cur, GcTaskTiming& timing, uint64_t cleanup_start_us) {
    if (cur == nullptr) {
        return;
    }

    task::release_lazy_vmem_ranges(*cur);

    // Free thread
    if (cur->thread != nullptr) {
        uint64_t const START_US = time::get_us();
        auto* thread_ptr = cur->thread;
        auto thread_addr = reinterpret_cast<uintptr_t>(thread_ptr);

        bool const THREAD_IN_HHDM = (thread_addr >= 0xffff800000000000ULL && thread_addr < 0xffff900000000000ULL);
        bool const THREAD_IN_KERNEL_STATIC = (thread_addr >= 0xffffffff80000000ULL && thread_addr < 0xffffffffc0000000ULL);
        if (!THREAD_IN_HHDM && !THREAD_IN_KERNEL_STATIC) {
            dbg::log("GC: Task %p (PID %x) thread ptr %p out of range; skipping", cur, cur->pid, thread_ptr);
            cur->thread = nullptr;
        } else {
            if (thread_ptr->magic != 0xDEADBEEF) {
                dbg::log("GC: Task %p (PID %x) thread bad magic 0x%x", cur, cur->pid, thread_ptr->magic);
            } else {
                thread_ptr->tls_phys_ptr = 0;
                thread_ptr->stack_phys_ptr = 0;
                threading::destroy_thread(thread_ptr);
            }
            cur->thread = nullptr;
        }
        timing.thread_us = elapsed_us_since(START_US, time::get_us());
    }

    uint64_t misc_start_us = time::get_us();
    // Free kernel stack
    if (cur->context.syscall_kernel_stack != 0) {
        uint64_t const TOP = cur->context.syscall_kernel_stack;
        uint64_t base = 0;
        if (TOP > ker::mod::mm::KERNEL_STACK_SIZE) {
            base = TOP - ker::mod::mm::KERNEL_STACK_SIZE;
        }
        if (base != 0) {
            mm::phys::page_free(reinterpret_cast<void*>(base));
        }
        cur->context.syscall_kernel_stack = 0;
    }

    // Free scratch area
    if (cur->context.syscall_scratch_area != 0) {
        auto* sa = reinterpret_cast<cpu::PerCpu*>(cur->context.syscall_scratch_area);
        auto sa_addr = reinterpret_cast<uintptr_t>(sa);
        bool const IN_HHDM = (sa_addr >= 0xffff800000000000ULL && sa_addr < 0xffff900000000000ULL);
        bool const IN_KERNEL_STATIC = (sa_addr >= 0xffffffff80000000ULL && sa_addr < 0xffffffffc0000000ULL);
        if (IN_HHDM || IN_KERNEL_STATIC) {
            delete sa;
        }
        cur->context.syscall_scratch_area = 0;
    }

    // Free name string
    if (cur->name != nullptr) {
        const auto* nm = cur->name;
        auto nm_addr = reinterpret_cast<uintptr_t>(nm);
        bool const IN_HHDM = (nm_addr >= 0xffff800000000000ULL && nm_addr < 0xffff900000000000ULL);
        bool const IN_KERNEL_STATIC = (nm_addr >= 0xffffffff80000000ULL && nm_addr < 0xffffffffc0000000ULL);
        if (IN_HHDM || IN_KERNEL_STATIC) {
            const size_t MAX_NAME_LEN = 1024;
            bool found_null = false;
            for (size_t i = 0; i < MAX_NAME_LEN; ++i) {
                volatile char const C = nm[i];
                if (C == '\0') {
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
    timing.misc_us += elapsed_us_since(misc_start_us, time::get_us());

    uint64_t const DEBUG_START_US = time::get_us();
    loader::debug::unregister_process(cur->pid);
    loader::debug::remove_gdb_debug_info(cur->pid);
    timing.debug_us = elapsed_us_since(DEBUG_START_US, time::get_us());

    misc_start_us = time::get_us();
    delete cur;
    timing.misc_us += elapsed_us_since(misc_start_us, time::get_us());
    timing.total_us = timing.detach_us + elapsed_us_since(cleanup_start_us, time::get_us());
}

void finish_waitable_zombie_resource_cleanup(task::Task* cur, GcTaskTiming& timing, uint64_t cleanup_start_us) {
    if (cur == nullptr) {
        return;
    }

    task::release_lazy_vmem_ranges(*cur);

    if (cur->thread != nullptr) {
        uint64_t const START_US = time::get_us();
        auto* thread_ptr = cur->thread;
        auto thread_addr = reinterpret_cast<uintptr_t>(thread_ptr);
        bool const THREAD_IN_HHDM = (thread_addr >= 0xffff800000000000ULL && thread_addr < 0xffff900000000000ULL);
        bool const THREAD_IN_KERNEL_STATIC = (thread_addr >= 0xffffffff80000000ULL && thread_addr < 0xffffffffc0000000ULL);
        if ((THREAD_IN_HHDM || THREAD_IN_KERNEL_STATIC) && thread_ptr->magic == 0xDEADBEEF) {
            thread_ptr->tls_phys_ptr = 0;
            thread_ptr->stack_phys_ptr = 0;
            threading::destroy_thread(thread_ptr);
        }
        cur->thread = nullptr;
        timing.thread_us = elapsed_us_since(START_US, time::get_us());
    }

    uint64_t misc_start_us = time::get_us();
    if (cur->context.syscall_kernel_stack != 0) {
        uint64_t const TOP = cur->context.syscall_kernel_stack;
        uint64_t base = 0;
        if (TOP > ker::mod::mm::KERNEL_STACK_SIZE) {
            base = TOP - ker::mod::mm::KERNEL_STACK_SIZE;
        }
        if (base != 0) {
            mm::phys::page_free(reinterpret_cast<void*>(base));
        }
        cur->context.syscall_kernel_stack = 0;
    }

    if (cur->context.syscall_scratch_area != 0) {
        auto* sa = reinterpret_cast<cpu::PerCpu*>(cur->context.syscall_scratch_area);
        auto sa_addr = reinterpret_cast<uintptr_t>(sa);
        bool const IN_HHDM = (sa_addr >= 0xffff800000000000ULL && sa_addr < 0xffff900000000000ULL);
        bool const IN_KERNEL_STATIC = (sa_addr >= 0xffffffff80000000ULL && sa_addr < 0xffffffffc0000000ULL);
        if (IN_HHDM || IN_KERNEL_STATIC) {
            delete sa;
        }
        cur->context.syscall_scratch_area = 0;
    }
    timing.misc_us += elapsed_us_since(misc_start_us, time::get_us());

    cur->zombie_resources_reclaimed.store(true, std::memory_order_release);
    cur->zombie_resources_reclaiming.store(false, std::memory_order_release);
    timing.total_us = timing.detach_us + elapsed_us_since(cleanup_start_us, time::get_us());
}

void cleanup_waitable_zombie_resources(GcDetachedTask const& detached, GcTaskTiming& timing) {
    auto* cur = detached.task;
    if (cur == nullptr) {
        return;
    }

    uint64_t const CLEANUP_START_US = time::get_us();
    if (cur->pagemap != nullptr) {
        uint64_t const START_US = time::get_us();
        if (detached.should_free_pagemap) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(cur->pagemap);
            mm::virt::destroy_user_space(cur->pagemap, cur->pid, cur->name, "task-zombie-gc");
            mm::virt::release_pagemap(cur->pagemap);
            cur->pagemap = nullptr;
        }
        timing.pagemap_us = elapsed_us_since(START_US, time::get_us());
    }

    finish_waitable_zombie_resource_cleanup(cur, timing, CLEANUP_START_US);
}

auto queue_detached_gc_task_cleanup(GcDetachedTask const& detached, uint64_t detach_us) -> bool {
    auto* cur = detached.task;
    if (cur == nullptr || cur->pagemap == nullptr || !detached.should_free_pagemap) {
        return false;
    }

    auto* pagemap_state = mm::virt::create_destroy_user_space_budget_state(cur->pagemap, cur->pid, cur->name, "task-exit-gc");
    if (pagemap_state == nullptr) {
        return false;
    }

    auto* item = new GcDeferredCleanupItem{
        .task = cur,
        .should_free_pagemap = detached.should_free_pagemap,
        .zombie_resources_only = detached.zombie_resources_only,
        .pagemap_state = pagemap_state,
        .detach_us = detach_us,
        .enqueue_us = time::get_us(),
        .next = nullptr,
    };
    if (item == nullptr) {
        mm::virt::destroy_user_space_budget_state_destroy(pagemap_state);
        return false;
    }

    gc_deferred_cleanup_push(item);
    return true;
}

auto process_deferred_gc_cleanup_slice(GcTaskTiming& timing, uint32_t pagemap_step_budget) -> bool {
    auto* item = gc_deferred_cleanup_front();
    if (item == nullptr || item->task == nullptr) {
        return false;
    }

    pagemap_step_budget = std::max<uint32_t>(pagemap_step_budget, 1);

    auto* cur = item->task;
    timing.detach_us = item->detach_us;
    item->detach_us = 0;
    uint64_t const CLEANUP_START_US = time::get_us();
    gc_deferred_cleanup_slices.fetch_add(1, std::memory_order_relaxed);
    if (cur->pagemap != nullptr && item->should_free_pagemap && item->pagemap_state != nullptr) {
        uint64_t const START_US = time::get_us();
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(cur->pagemap);
        if (!mm::virt::destroy_user_space_budgeted(item->pagemap_state, pagemap_step_budget)) {
            timing.pagemap_us = elapsed_us_since(START_US, time::get_us());
            timing.total_us = timing.detach_us + timing.pagemap_us;
            return false;
        }

        mm::virt::destroy_user_space_budget_state_destroy(item->pagemap_state);
        item->pagemap_state = nullptr;
        mm::virt::release_pagemap(cur->pagemap);
        cur->pagemap = nullptr;
        timing.pagemap_us = elapsed_us_since(START_US, time::get_us());
    } else {
        cur->pagemap = nullptr;
    }

    if (item->zombie_resources_only) {
        finish_waitable_zombie_resource_cleanup(cur, timing, CLEANUP_START_US);
    } else {
        cleanup_task_after_pagemap(cur, timing, CLEANUP_START_US);
    }
    uint64_t const WAIT_US = elapsed_us_since(item->enqueue_us, time::get_us());
    gc_deferred_cleanup_pop();
    note_gc_deferred_cleanup_complete(WAIT_US);
    delete item;
    return true;
}

void cleanup_detached_gc_task(GcDetachedTask const& detached, GcTaskTiming& timing) {
    auto* cur = detached.task;
    if (cur == nullptr) {
        return;
    }

    uint64_t const CLEANUP_START_US = time::get_us();

    // Free pagemap.
    // - DAEMON tasks use the kernel pagemap - must NOT free it.
    // - Thread tasks share the owner process's pagemap - must NOT free it here.
    // - Process tasks free only after detach confirmed no alive/dead sibling still
    //   references the same pagemap.
    if (cur->pagemap != nullptr) {
        uint64_t const START_US = time::get_us();
        if (detached.should_free_pagemap) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(cur->pagemap);
            mm::virt::destroy_user_space(cur->pagemap, cur->pid, cur->name, "task-exit-gc");
            mm::virt::release_pagemap(cur->pagemap);
        }
        cur->pagemap = nullptr;
        timing.pagemap_us = elapsed_us_since(START_US, time::get_us());
    }

    cleanup_task_after_pagemap(cur, timing, CLEANUP_START_US);
}

auto gc_expired_tasks_budgeted_impl(uint32_t max_tasks, uint64_t max_work_us, uint32_t pagemap_step_budget, bool* time_budget_exhausted,
                                    bool* has_pending_work) -> uint32_t {
    if (max_tasks == 0 || run_queues == nullptr) {
        if (has_pending_work != nullptr) {
            *has_pending_work = gc_deferred_cleanup_has_work();
        }
        return 0;
    }

    pagemap_step_budget = std::max<uint32_t>(pagemap_step_budget, 1);

    uint64_t const START_US = max_work_us != 0 ? time::get_us() : 0;
    bool hit_time_budget = false;
    uint32_t reclaimed = 0;
    for (uint64_t cpu_no = 0; cpu_no < smt::get_core_count() && reclaimed < max_tasks && !hit_time_budget; ++cpu_no) {
        while (reclaimed < max_tasks && !hit_time_budget) {
            if (gc_time_budget_expired(START_US, max_work_us)) {
                hit_time_budget = true;
                break;
            }

            if (gc_deferred_cleanup_has_work()) {
                GcTaskTiming timing{};
                bool const COMPLETED = process_deferred_gc_cleanup_slice(timing, pagemap_step_budget);
                note_gc_task_timing(timing);
                if (COMPLETED) {
                    reclaimed++;
                }
                hit_time_budget = gc_time_budget_expired(START_US, max_work_us);
                if (hit_time_budget || gc_deferred_cleanup_has_work()) {
                    continue;
                }
            }

            GcDetachedTask detached{};
            uint64_t const DETACH_START_US = time::get_us();
            run_queues->with_lock_void(
                cpu_no, [&detached, cpu_no](RunQueue* rq) -> void { detached = detach_next_reclaimable_task_locked(rq, cpu_no); });

            GcTaskTiming timing{.detach_us = elapsed_us_since(DETACH_START_US, time::get_us())};
            if (detached.counted_without_cleanup) {
                timing.total_us = timing.detach_us;
                note_gc_task_timing(timing);
                reclaimed++;
                continue;
            }

            if (detached.task == nullptr) {
                break;
            }

            if (detached.zombie_resources_only) {
                if (queue_detached_gc_task_cleanup(detached, timing.detach_us)) {
                    hit_time_budget = gc_time_budget_expired(START_US, max_work_us);
                    continue;
                }
                cleanup_waitable_zombie_resources(detached, timing);
                note_gc_task_timing(timing);
                reclaimed++;
                hit_time_budget = gc_time_budget_expired(START_US, max_work_us);
                continue;
            }

            if (queue_detached_gc_task_cleanup(detached, timing.detach_us)) {
                hit_time_budget = gc_time_budget_expired(START_US, max_work_us);
                continue;
            }

            cleanup_detached_gc_task(detached, timing);
            note_gc_task_timing(timing);
            reclaimed++;
            hit_time_budget = gc_time_budget_expired(START_US, max_work_us);
        }
    }
    if (time_budget_exhausted != nullptr) {
        *time_budget_exhausted = hit_time_budget;
    }
    if (has_pending_work != nullptr) {
        *has_pending_work = gc_deferred_cleanup_has_work();
    }
    return reclaimed;
}

}  // namespace

auto gc_expired_tasks_budgeted(uint32_t max_tasks) -> uint32_t {
    return gc_expired_tasks_budgeted_impl(max_tasks, 0, SCHED_GC_PAGEMAP_STEP_BUDGET, nullptr, nullptr);
}

void gc_expired_tasks() { (void)gc_expired_tasks_budgeted(UINT32_MAX); }

namespace {

void note_gc_pass_result(uint32_t reclaimed, uint64_t elapsed_us) {
    if (run_queues == nullptr) {
        return;
    }

    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }

    rq->gc_passes_triggered.fetch_add(1, std::memory_order_relaxed);
    rq->gc_tasks_reclaimed.fetch_add(reclaimed, std::memory_order_relaxed);
    rq->gc_work_us_total.fetch_add(elapsed_us, std::memory_order_relaxed);

    uint64_t observed = rq->gc_work_us_max.load(std::memory_order_relaxed);
    while (elapsed_us > observed &&
           !rq->gc_work_us_max.compare_exchange_weak(observed, elapsed_us, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
}

struct GcPassResult {
    uint32_t reclaimed = 0;
    bool time_budget_exhausted = false;
    bool has_pending_work = false;
    bool ran = false;
};

auto gc_pass_needs_followup(GcPassResult const& result, uint32_t reclaim_budget) -> bool {
    return result.has_pending_work || result.reclaimed >= reclaim_budget || (result.time_budget_exhausted && result.reclaimed != 0);
}

auto run_gc_reclaim_pass_exclusive(uint32_t reclaim_budget, uint64_t work_budget_us, uint32_t pagemap_step_budget) -> GcPassResult {
    bool expected = false;
    if (!scheduler_gc_reclaim_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return {};
    }

    uint64_t const START_US = time::get_us();
    GcPassResult result{.ran = true};
    result.reclaimed = gc_expired_tasks_budgeted_impl(reclaim_budget, work_budget_us, pagemap_step_budget, &result.time_budget_exhausted,
                                                      &result.has_pending_work);
    uint64_t const END_US = time::get_us();
    note_gc_pass_result(result.reclaimed, END_US >= START_US ? END_US - START_US : 0);
    scheduler_gc_reclaim_active.store(false, std::memory_order_release);
    return result;
}

auto task_blocks_idle_gc_boost(task::Task const* task) -> bool {
    if (task == nullptr || task->type != task::TaskType::PROCESS || task->has_exited) {
        return false;
    }
    if (task->state.load(std::memory_order_acquire) != task::TaskState::ACTIVE) {
        return false;
    }
    return !task->is_voluntary_blocked();
}

auto runqueue_has_foreground_work_locked(RunQueue* rq) -> bool {
    if (rq == nullptr) {
        return false;
    }

    auto* const CURRENT = rq->handoff_task != nullptr ? rq->handoff_task : rq->current_task;
    if (task_blocks_idle_gc_boost(CURRENT)) {
        return true;
    }

    for (uint32_t i = 0; i < rq->runnable_heap.size; ++i) {
        if (task_blocks_idle_gc_boost(run_heap_entry(rq->runnable_heap, i))) {
            return true;
        }
    }

    return false;
}

auto scheduler_gc_should_boost_reclaim() -> bool {
    if (run_queues == nullptr) {
        return false;
    }

    for (uint64_t cpu_no = 0; cpu_no < smt::get_core_count(); ++cpu_no) {
        bool const HAS_FOREGROUND_WORK =
            run_queues->with_lock(cpu_no, [](RunQueue* rq) -> bool { return runqueue_has_foreground_work_locked(rq); });
        if (HAS_FOREGROUND_WORK) {
            return false;
        }
    }

    return true;
}

void scheduler_gc_thread_main() {
    for (;;) {
        if (!scheduler_gc_requested.exchange(false, std::memory_order_acq_rel)) {
            kern_block();
            continue;
        }

        bool const PRESSURE_RECLAIM = scheduler_gc_memory_pressure_requested.exchange(false, std::memory_order_acq_rel);
        bool const BOOST_RECLAIM = !PRESSURE_RECLAIM && scheduler_gc_should_boost_reclaim();
        if (BOOST_RECLAIM) {
            scheduler_gc_idle_boost_passes.fetch_add(1, std::memory_order_relaxed);
        } else {
            scheduler_gc_foreground_passes.fetch_add(1, std::memory_order_relaxed);
        }
        uint32_t reclaim_budget = SCHED_GC_RECLAIM_BUDGET;
        uint64_t work_budget_us = SCHED_GC_WORK_BUDGET_US;
        uint32_t pagemap_step_budget = SCHED_GC_PAGEMAP_STEP_BUDGET;
        if (PRESSURE_RECLAIM) {
            reclaim_budget = SCHED_GC_PRESSURE_RECLAIM_BUDGET;
            work_budget_us = SCHED_GC_PRESSURE_WORK_BUDGET_US;
            pagemap_step_budget = SCHED_GC_PRESSURE_PAGEMAP_STEP_BUDGET;
        } else if (BOOST_RECLAIM) {
            reclaim_budget = SCHED_GC_IDLE_RECLAIM_BUDGET;
            work_budget_us = SCHED_GC_IDLE_WORK_BUDGET_US;
            pagemap_step_budget = SCHED_GC_IDLE_PAGEMAP_STEP_BUDGET;
        } else if (gc_deferred_cleanup_has_work()) {
            reclaim_budget = SCHED_GC_DEFERRED_RECLAIM_BUDGET;
            work_budget_us = SCHED_GC_DEFERRED_WORK_BUDGET_US;
            pagemap_step_budget = SCHED_GC_DEFERRED_PAGEMAP_STEP_BUDGET;
        }

        GcPassResult const PASS = run_gc_reclaim_pass_exclusive(reclaim_budget, work_budget_us, pagemap_step_budget);
        if (!PASS.ran) {
            scheduler_gc_requested.store(true, std::memory_order_release);
            kern_yield();
            continue;
        }

        if (PRESSURE_RECLAIM && PASS.has_pending_work) {
            scheduler_gc_memory_pressure_requested.store(true, std::memory_order_release);
        }
        if (gc_pass_needs_followup(PASS, reclaim_budget)) {
            scheduler_gc_requested.store(true, std::memory_order_release);
            kern_yield();
        }
    }
}

}  // namespace

void request_gc() {
    scheduler_gc_requested.store(true, std::memory_order_release);

    auto* task = scheduler_gc_task.load(std::memory_order_acquire);
    if (task == nullptr) {
        return;
    }

    uint64_t target_cpu = task->cpu;
    if (target_cpu >= smt::get_core_count()) {
        target_cpu = 0;
    }
    wake_task_from_event_on_cpu(task, target_cpu, EventWakeDeferredSwitch::PRESERVE);
}

void request_gc_memory_pressure() {
    scheduler_gc_memory_pressure_requested.store(true, std::memory_order_release);
    request_gc();
}

auto reclaim_memory_pressure() -> uint32_t {
    GcPassResult const PASS = run_gc_reclaim_pass_exclusive(SCHED_GC_PRESSURE_RECLAIM_BUDGET, SCHED_GC_PRESSURE_WORK_BUDGET_US,
                                                            SCHED_GC_PRESSURE_PAGEMAP_STEP_BUDGET);
    if (!PASS.ran) {
        request_gc_memory_pressure();
        return 0;
    }

    if (PASS.has_pending_work) {
        scheduler_gc_memory_pressure_requested.store(true, std::memory_order_release);
    }
    if (gc_pass_needs_followup(PASS, SCHED_GC_PRESSURE_RECLAIM_BUDGET)) {
        request_gc_memory_pressure();
    }
    return PASS.reclaimed;
}

void start_gc_worker() {
    bool expected = false;
    if (!scheduler_gc_worker_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }

    auto* gc_task = task::Task::create_kernel_thread("sched_gc", scheduler_gc_thread_main);
    if (gc_task == nullptr) {
        scheduler_gc_worker_started.store(false, std::memory_order_release);
        return;
    }

    scheduler_gc_task.store(gc_task, std::memory_order_release);
    (void)post_task_for_cpu(0, gc_task);
}

// ============================================================================
// Diagnostics / stats
// ============================================================================

auto get_run_queue_stats(uint64_t cpu_no) -> RunQueueStats {
    RunQueueStats stats = {.active_task_count = 0, .expired_task_count = 0, .wait_queue_count = 0};
    if (run_queues == nullptr) {
        return stats;
    }
    return run_queues->with_lock(cpu_no, [](RunQueue* rq) -> RunQueueStats {
        if (rq == nullptr) {
            return {.active_task_count = 0, .expired_task_count = 0, .wait_queue_count = 0};
        }
        return {.active_task_count = rq->runnable_heap.size,
                .expired_task_count = rq->dead_list.count,
                .wait_queue_count = rq->wait_list.count};
    });
}

void arm_idle_timer_for_this_cpu() {
    if (run_queues == nullptr) {
        return;
    }
    run_queues->this_cpu_locked_void([](RunQueue* rq) { arm_idle_timer_locked(rq); });
}

void note_scheduler_timer_interrupt() {
    if (run_queues == nullptr) {
        return;
    }
    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }
    rq->scheduler_timer_interrupts.fetch_add(1, std::memory_order_relaxed);
    rq->resched_timer_pending.store(false, std::memory_order_release);
    auto* current = rq->current_task;
    if (current == nullptr || current->type == task::TaskType::IDLE) {
        rq->idle_timer_wakeups.fetch_add(1, std::memory_order_relaxed);
    }
}

void note_scheduler_timer_arm() {
    if (run_queues == nullptr) {
        return;
    }
    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }
    rq->scheduler_timer_arms.fetch_add(1, std::memory_order_relaxed);
}

void note_scheduler_timer_disarm() {
    if (run_queues == nullptr) {
        return;
    }
    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }
    rq->resched_timer_pending.store(false, std::memory_order_release);
    rq->scheduler_timer_disarms.fetch_add(1, std::memory_order_relaxed);
}

namespace {

enum class SchedulerTimerArmReason : uint8_t {
    WAIT_DEADLINE,
    ITIMER,
    VOLUNTARY_BLOCK,
    IDLE_WORK,
    RUNQUEUE,
    COMPETITOR,
};

void note_scheduler_timer_arm_reason_locked(RunQueue* rq, SchedulerTimerArmReason reason) {
    if (rq == nullptr) {
        return;
    }

    switch (reason) {
        case SchedulerTimerArmReason::WAIT_DEADLINE:
            rq->scheduler_timer_arm_wait_deadline.fetch_add(1, std::memory_order_relaxed);
            return;
        case SchedulerTimerArmReason::ITIMER:
            rq->scheduler_timer_arm_itimer.fetch_add(1, std::memory_order_relaxed);
            return;
        case SchedulerTimerArmReason::VOLUNTARY_BLOCK:
            rq->scheduler_timer_arm_voluntary.fetch_add(1, std::memory_order_relaxed);
            return;
        case SchedulerTimerArmReason::IDLE_WORK:
            rq->scheduler_timer_arm_idle_work.fetch_add(1, std::memory_order_relaxed);
            return;
        case SchedulerTimerArmReason::RUNQUEUE:
            rq->scheduler_timer_arm_runqueue.fetch_add(1, std::memory_order_relaxed);
            return;
        case SchedulerTimerArmReason::COMPETITOR:
            rq->scheduler_timer_arm_competitor.fetch_add(1, std::memory_order_relaxed);
            return;
    }
}

}  // namespace

auto get_scheduler_timer_decision_for_this_cpu(uint64_t now_us) -> SchedulerTimerDecision {
    SchedulerTimerDecision const FIXED_QUANTUM{
        .arm = true,
        .use_deadline_delta = false,
        .deadline_delta_us = 0,
    };
    SchedulerTimerDecision const DISARM{
        .arm = false,
        .use_deadline_delta = false,
        .deadline_delta_us = 0,
    };

    if (run_queues == nullptr) {
        return FIXED_QUANTUM;
    }

    return run_queues->this_cpu_locked([now_us, FIXED_QUANTUM, DISARM](RunQueue* rq) -> SchedulerTimerDecision {
        if (rq == nullptr) {
            return FIXED_QUANTUM;
        }

        // current_task tracks the live stack until the assembly return boundary
        // commits a switch.  Timer programming cares about the task we will
        // return to, so treat a reserved handoff as the effective running task.
        auto* const CURRENT = rq->handoff_task != nullptr ? rq->handoff_task : rq->current_task;
        bool const CURRENT_IS_IDLE = CURRENT == nullptr || CURRENT->type == task::TaskType::IDLE;

        // kern_yield-style wait loops remain runnable and halt voluntarily.
        // Keep a tick for those so condition polling cannot sleep forever.
        if (CURRENT != nullptr && CURRENT->is_voluntary_blocked()) {
            note_scheduler_timer_arm_reason_locked(rq, SchedulerTimerArmReason::VOLUNTARY_BLOCK);
            return FIXED_QUANTUM;
        }

        if (rq->runnable_heap.size != 0 && CURRENT_IS_IDLE) {
            note_scheduler_timer_arm_reason_locked(rq, SchedulerTimerArmReason::IDLE_WORK);
            return FIXED_QUANTUM;
        }

        if (rq->runnable_heap.size > 1) {
            note_scheduler_timer_arm_reason_locked(rq, SchedulerTimerArmReason::RUNQUEUE);
            return FIXED_QUANTUM;
        }

        if (rq->runnable_heap.size == 1 && !CURRENT_IS_IDLE && !rq->runnable_heap.contains(CURRENT)) {
            note_scheduler_timer_arm_reason_locked(rq, SchedulerTimerArmReason::COMPETITOR);
            return FIXED_QUANTUM;
        }

        uint64_t next_deadline_us = 0;
        SchedulerTimerArmReason deadline_reason = SchedulerTimerArmReason::WAIT_DEADLINE;
        if (rq->next_wait_deadline_us != 0) {
            next_deadline_us = rq->next_wait_deadline_us;
            deadline_reason = SchedulerTimerArmReason::WAIT_DEADLINE;
        }

        if (CURRENT != nullptr && CURRENT->itimer_real_expire_us != 0 &&
            (next_deadline_us == 0 || CURRENT->itimer_real_expire_us < next_deadline_us)) {
            next_deadline_us = CURRENT->itimer_real_expire_us;
            deadline_reason = SchedulerTimerArmReason::ITIMER;
        }

        if (next_deadline_us == 0) {
            return DISARM;
        }

        note_scheduler_timer_arm_reason_locked(rq, deadline_reason);
        return SchedulerTimerDecision{
            .arm = true,
            .use_deadline_delta = true,
            .deadline_delta_us = next_deadline_us > now_us ? next_deadline_us - now_us : 1,
        };
    });
}

void note_local_reschedule_timer_poke() {
    if (run_queues == nullptr) {
        return;
    }
    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }
    rq->local_reschedule_timer_pokes.fetch_add(1, std::memory_order_relaxed);
}

void account_irq_time_us(uint64_t elapsed_us) {
    if (run_queues == nullptr || elapsed_us == 0) {
        return;
    }

    auto* rq = run_queues->this_cpu();
    if (rq == nullptr) {
        return;
    }

    rq->cpu_irq_us.fetch_add(elapsed_us, std::memory_order_relaxed);
    rq->irq_uncharged_us.fetch_add(elapsed_us, std::memory_order_release);
}

auto get_cpu_accounting_snapshot(uint64_t cpu_no) -> CpuAccountingSnapshot {
    CpuAccountingSnapshot snapshot{};
    if (run_queues == nullptr) {
        return snapshot;
    }

    uint64_t const NOW_US = time::get_us();
    return run_queues->with_lock(cpu_no,
                                 [NOW_US](RunQueue* rq) -> CpuAccountingSnapshot { return cpu_accounting_snapshot_locked(rq, NOW_US); });
}

namespace {
auto decay_load_milli(uint64_t previous, uint64_t active_milli, uint64_t exp_scaled) -> uint64_t {
    return ((previous * exp_scaled) + (active_milli * (LOADAVG_SCALE - exp_scaled))) / LOADAVG_SCALE;
}
}  // namespace

auto get_load_average_snapshot() -> LoadAverageSnapshot {
    LoadAverageSnapshot snapshot{};
    uint32_t total = 0;
    uint32_t runnable = 0;
    uint32_t uninterruptible = 0;
    uint32_t active_for_load = 0;
    uint64_t last_pid = 0;

    {
        uint64_t const FLAGS = global_task_registry_lock.lock_irqsave();
        total = active_task_count;
        for (uint32_t i = 0; i < active_task_count; ++i) {
            auto* task = active_task_slot(i);
            if (task == nullptr) {
                continue;
            }
            last_pid = std::max<uint64_t>(last_pid, task->pid);
            auto const TASK_STATE = task->state.load(std::memory_order_acquire);
            bool const DEAD = TASK_STATE == task::TaskState::DEAD || TASK_STATE == task::TaskState::EXITING || task->has_exited;
            if (DEAD || task->ptrace_stopped) {
                continue;
            }
            bool const IS_RUNNABLE = task->sched_queue == task::Task::sched_queue::RUNNABLE && !task->is_voluntary_blocked();
            bool const IS_UNINTERRUPTIBLE =
                task->sched_queue == task::Task::sched_queue::WAITING && task->wait_channel != nullptr && !task->ptrace_stopped;
            if (IS_RUNNABLE) {
                runnable++;
            }
            if (IS_UNINTERRUPTIBLE) {
                uninterruptible++;
            }
            if (IS_RUNNABLE || IS_UNINTERRUPTIBLE) {
                active_for_load++;
            }
        }
        global_task_registry_lock.unlock_irqrestore(FLAGS);
    }

    uint64_t const NOW_US = time::get_us();
    uint64_t const FLAGS = load_average_lock.lock_irqsave();
    if (load_average_last_update_us == 0) {
        load1_milli = static_cast<uint64_t>(active_for_load) * 1000ULL;
        load5_milli = load1_milli;
        load15_milli = load1_milli;
        load_average_last_update_us = NOW_US;
    } else if (NOW_US > load_average_last_update_us) {
        uint64_t elapsed_s = (NOW_US - load_average_last_update_us) / 1000000ULL;
        elapsed_s = std::min<uint64_t>(elapsed_s, 60);
        uint64_t const ACTIVE_MILLI = static_cast<uint64_t>(active_for_load) * 1000ULL;
        for (uint64_t i = 0; i < elapsed_s; ++i) {
            load1_milli = decay_load_milli(load1_milli, ACTIVE_MILLI, LOADAVG_EXP_1MIN);
            load5_milli = decay_load_milli(load5_milli, ACTIVE_MILLI, LOADAVG_EXP_5MIN);
            load15_milli = decay_load_milli(load15_milli, ACTIVE_MILLI, LOADAVG_EXP_15MIN);
        }
        if (elapsed_s != 0) {
            load_average_last_update_us += elapsed_s * 1000000ULL;
        }
    }

    snapshot.load1_milli = load1_milli;
    snapshot.load5_milli = load5_milli;
    snapshot.load15_milli = load15_milli;
    snapshot.runnable_tasks = runnable;
    snapshot.uninterruptible_tasks = uninterruptible;
    snapshot.total_tasks = total;
    snapshot.last_pid = last_pid;
    load_average_lock.unlock_irqrestore(FLAGS);
    return snapshot;
}

auto get_scheduler_trace_stats(uint64_t cpu_no) -> SchedulerTraceStats {
    if (run_queues == nullptr) {
        return SchedulerTraceStats{};
    }

    return run_queues->with_lock(cpu_no, [cpu_no](RunQueue* rq) -> SchedulerTraceStats {
        if (rq == nullptr) {
            return SchedulerTraceStats{};
        }

        return SchedulerTraceStats{
            .idle_timer_arms = rq->idle_timer_arms.load(std::memory_order_relaxed),
            .idle_timer_disarms = rq->idle_timer_disarms.load(std::memory_order_relaxed),
            .idle_timer_wakeups = rq->idle_timer_wakeups.load(std::memory_order_relaxed),
            .scheduler_timer_interrupts = rq->scheduler_timer_interrupts.load(std::memory_order_relaxed),
            .scheduler_timer_arms = rq->scheduler_timer_arms.load(std::memory_order_relaxed),
            .scheduler_timer_disarms = rq->scheduler_timer_disarms.load(std::memory_order_relaxed),
            .scheduler_timer_arm_wait_deadline = rq->scheduler_timer_arm_wait_deadline.load(std::memory_order_relaxed),
            .scheduler_timer_arm_itimer = rq->scheduler_timer_arm_itimer.load(std::memory_order_relaxed),
            .scheduler_timer_arm_voluntary = rq->scheduler_timer_arm_voluntary.load(std::memory_order_relaxed),
            .scheduler_timer_arm_idle_work = rq->scheduler_timer_arm_idle_work.load(std::memory_order_relaxed),
            .scheduler_timer_arm_runqueue = rq->scheduler_timer_arm_runqueue.load(std::memory_order_relaxed),
            .scheduler_timer_arm_competitor = rq->scheduler_timer_arm_competitor.load(std::memory_order_relaxed),
            .wake_ipis_sent = rq->wake_ipis_sent.load(std::memory_order_relaxed),
            .wake_ipis_coalesced = rq->wake_ipis_coalesced.load(std::memory_order_relaxed),
            .local_reschedule_requests = rq->local_reschedule_requests.load(std::memory_order_relaxed),
            .local_reschedule_timer_pokes = rq->local_reschedule_timer_pokes.load(std::memory_order_relaxed),
            .slow_reschedule_scans = rq->slow_reschedule_scans.load(std::memory_order_relaxed),
            .wait_list_scan_iterations = rq->wait_list_scan_iterations.load(std::memory_order_relaxed),
            .wait_list_scan_passes = rq->wait_list_scan_passes.load(std::memory_order_relaxed),
            .wait_list_scan_max = rq->wait_list_scan_max.load(std::memory_order_relaxed),
            .timer_expired_wakeups = rq->timer_expired_wakeups.load(std::memory_order_relaxed),
            .gc_passes_triggered = rq->gc_passes_triggered.load(std::memory_order_relaxed),
            .gc_tasks_reclaimed = rq->gc_tasks_reclaimed.load(std::memory_order_relaxed),
            .gc_work_us_total = rq->gc_work_us_total.load(std::memory_order_relaxed),
            .gc_work_us_max = rq->gc_work_us_max.load(std::memory_order_relaxed),
            .gc_task_us_total = rq->gc_task_us_total.load(std::memory_order_relaxed),
            .gc_task_us_max = rq->gc_task_us_max.load(std::memory_order_relaxed),
            .gc_detach_us_total = rq->gc_detach_us_total.load(std::memory_order_relaxed),
            .gc_detach_us_max = rq->gc_detach_us_max.load(std::memory_order_relaxed),
            .gc_pagemap_us_total = rq->gc_pagemap_us_total.load(std::memory_order_relaxed),
            .gc_pagemap_us_max = rq->gc_pagemap_us_max.load(std::memory_order_relaxed),
            .gc_thread_us_total = rq->gc_thread_us_total.load(std::memory_order_relaxed),
            .gc_thread_us_max = rq->gc_thread_us_max.load(std::memory_order_relaxed),
            .gc_misc_us_total = rq->gc_misc_us_total.load(std::memory_order_relaxed),
            .gc_misc_us_max = rq->gc_misc_us_max.load(std::memory_order_relaxed),
            .gc_debug_us_total = rq->gc_debug_us_total.load(std::memory_order_relaxed),
            .gc_debug_us_max = rq->gc_debug_us_max.load(std::memory_order_relaxed),
            .gc_deferred_queued = cpu_no == 0 ? gc_deferred_cleanup_items_queued.load(std::memory_order_relaxed) : 0,
            .gc_deferred_completed = cpu_no == 0 ? gc_deferred_cleanup_items_completed.load(std::memory_order_relaxed) : 0,
            .gc_deferred_depth = cpu_no == 0 ? gc_deferred_cleanup_depth.load(std::memory_order_relaxed) : 0,
            .gc_deferred_depth_max = cpu_no == 0 ? gc_deferred_cleanup_depth_max.load(std::memory_order_relaxed) : 0,
            .gc_deferred_slices = cpu_no == 0 ? gc_deferred_cleanup_slices.load(std::memory_order_relaxed) : 0,
            .gc_deferred_slices_completed = cpu_no == 0 ? gc_deferred_cleanup_slices_completed.load(std::memory_order_relaxed) : 0,
            .gc_deferred_oldest_wait_us_max = cpu_no == 0 ? gc_deferred_cleanup_oldest_wait_us_max.load(std::memory_order_relaxed) : 0,
            .gc_idle_boost_passes = cpu_no == 0 ? scheduler_gc_idle_boost_passes.load(std::memory_order_relaxed) : 0,
            .gc_foreground_passes = cpu_no == 0 ? scheduler_gc_foreground_passes.load(std::memory_order_relaxed) : 0,
            .load_balance_pushes = rq->load_balance_pushes.load(std::memory_order_relaxed),
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

    return run_queues->with_lock(cpu_no, [cpu_no](RunQueue* rq) -> SchedulerCpuState {
        SchedulerCpuState state{};
        state.cpu_no = cpu_no;
        state.current_name = "?";

        if (rq == nullptr) {
            return state;
        }

        state.is_idle = rq->is_idle.load(std::memory_order_acquire);
        state.runnable_count = rq->runnable_heap.size;
        state.wait_queue_count = rq->wait_list.count;
        state.resched_timer_pending = rq->resched_timer_pending.load(std::memory_order_acquire);
        state.scheduler_timer_interrupts = rq->scheduler_timer_interrupts.load(std::memory_order_relaxed);
        state.scheduler_timer_arms = rq->scheduler_timer_arms.load(std::memory_order_relaxed);
        state.scheduler_timer_disarms = rq->scheduler_timer_disarms.load(std::memory_order_relaxed);
        state.wake_ipis_sent = rq->wake_ipis_sent.load(std::memory_order_relaxed);
        state.wake_ipis_coalesced = rq->wake_ipis_coalesced.load(std::memory_order_relaxed);
        state.local_reschedule_requests = rq->local_reschedule_requests.load(std::memory_order_relaxed);
        state.local_reschedule_timer_pokes = rq->local_reschedule_timer_pokes.load(std::memory_order_relaxed);
        state.last_tick_us = rq->last_tick_us;
        state.next_wait_deadline_us = rq->next_wait_deadline_us;

        auto* current = rq->current_task;
        if (current != nullptr) {
            state.current_pid = current->pid;
            state.current_name = current->name != nullptr ? current->name : "?";
            state.current_type = static_cast<uint8_t>(current->type);
            state.current_voluntary_block = current->is_voluntary_blocked();
            state.current_wants_block = current->wants_block;
            state.current_cpu_pinned = current->cpu_pinned;
            state.current_preempt_depth = current->preempt_disable_depth;
            state.current_preempt_pending = current->preempt_pending;
            state.current_preempt_max_us = current->preempt_disable_max_us;
        }

        return state;
    });
}

void dump_scheduler_trace_stats() {
    if (run_queues == nullptr) {
        return;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        auto stats = get_scheduler_trace_stats(cpu_no);
        if (stats.idle_timer_arms == 0 && stats.idle_timer_disarms == 0 && stats.idle_timer_wakeups == 0 && stats.wake_ipis_sent == 0 &&
            stats.wake_ipis_coalesced == 0 && stats.local_reschedule_requests == 0 && stats.slow_reschedule_scans == 0 &&
            stats.load_balance_pushes == 0 && stats.scheduler_timer_interrupts == 0 && stats.scheduler_timer_arms == 0 &&
            stats.scheduler_timer_disarms == 0 && stats.local_reschedule_timer_pokes == 0 && stats.wait_list_scan_iterations == 0 &&
            stats.wait_list_scan_passes == 0 && stats.wait_list_scan_max == 0 && stats.timer_expired_wakeups == 0 &&
            stats.gc_passes_triggered == 0 && stats.gc_tasks_reclaimed == 0 && stats.gc_task_us_total == 0) {
            continue;
        }

        dbg::log(
            "schedstats: cpu%lu timer_irq=%lu sched_arm=%lu sched_disarm=%lu idle_arm=%lu idle_disarm=%lu idle_wake=%lu wake_ipi=%lu "
            "wake_coal=%lu local_resched=%lu local_poke=%lu slow_scan=%lu wait_scan=%lu wait_pass=%lu wait_max=%lu timer_wake=%lu "
            "gc_pass=%lu gc_reclaim=%lu gc_us=%lu gc_max_us=%lu gc_task_us=%lu gc_task_max=%lu gc_pm_us=%lu gc_pm_max=%lu lb_push=%lu",
            static_cast<unsigned long>(cpu_no), static_cast<unsigned long>(stats.scheduler_timer_interrupts),
            static_cast<unsigned long>(stats.scheduler_timer_arms), static_cast<unsigned long>(stats.scheduler_timer_disarms),
            static_cast<unsigned long>(stats.idle_timer_arms), static_cast<unsigned long>(stats.idle_timer_disarms),
            static_cast<unsigned long>(stats.idle_timer_wakeups), static_cast<unsigned long>(stats.wake_ipis_sent),
            static_cast<unsigned long>(stats.wake_ipis_coalesced), static_cast<unsigned long>(stats.local_reschedule_requests),
            static_cast<unsigned long>(stats.local_reschedule_timer_pokes), static_cast<unsigned long>(stats.slow_reschedule_scans),
            static_cast<unsigned long>(stats.wait_list_scan_iterations), static_cast<unsigned long>(stats.wait_list_scan_passes),
            static_cast<unsigned long>(stats.wait_list_scan_max), static_cast<unsigned long>(stats.timer_expired_wakeups),
            static_cast<unsigned long>(stats.gc_passes_triggered), static_cast<unsigned long>(stats.gc_tasks_reclaimed),
            static_cast<unsigned long>(stats.gc_work_us_total), static_cast<unsigned long>(stats.gc_work_us_max),
            static_cast<unsigned long>(stats.gc_task_us_total), static_cast<unsigned long>(stats.gc_task_us_max),
            static_cast<unsigned long>(stats.gc_pagemap_us_total), static_cast<unsigned long>(stats.gc_pagemap_us_max),
            static_cast<unsigned long>(stats.load_balance_pushes));
    }
}

void dump_scheduler_cpu_states() {
    if (run_queues == nullptr) {
        return;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    for (uint64_t cpu_no = 0; cpu_no < CORE_COUNT; ++cpu_no) {
        auto state = get_scheduler_cpu_state(cpu_no);
        dbg::log(
            "schedcpu: cpu%lu idle=%u runq=%lu waitq=%lu cur=%lu(%s) type=%u vblk=%u wblk=%u pinned=%u preempt=%u/%u "
            "preempt_max_us=%lu pending=%u timer=%lu/%lu/%lu wake=%lu/%lu local=%lu/%lu last_tick=%lu wait_deadline=%lu",
            static_cast<unsigned long>(state.cpu_no), state.is_idle ? 1U : 0U, static_cast<unsigned long>(state.runnable_count),
            static_cast<unsigned long>(state.wait_queue_count), static_cast<unsigned long>(state.current_pid), state.current_name,
            static_cast<unsigned>(state.current_type), state.current_voluntary_block ? 1U : 0U, state.current_wants_block ? 1U : 0U,
            state.current_cpu_pinned ? 1U : 0U, state.current_preempt_depth, state.current_preempt_pending ? 1U : 0U,
            static_cast<unsigned long>(state.current_preempt_max_us), state.resched_timer_pending ? 1U : 0U,
            static_cast<unsigned long>(state.scheduler_timer_interrupts), static_cast<unsigned long>(state.scheduler_timer_arms),
            static_cast<unsigned long>(state.scheduler_timer_disarms), static_cast<unsigned long>(state.wake_ipis_sent),
            static_cast<unsigned long>(state.wake_ipis_coalesced), static_cast<unsigned long>(state.local_reschedule_requests),
            static_cast<unsigned long>(state.local_reschedule_timer_pokes), static_cast<unsigned long>(state.last_tick_us),
            static_cast<unsigned long>(state.next_wait_deadline_us));
    }
}

auto get_least_loaded_cpu() -> uint64_t {
    if (run_queues == nullptr) {
        return 0;
    }

    uint64_t const CORE_COUNT = smt::get_core_count();
    if (CORE_COUNT <= 1) {
        return 0;
    }

    static std::atomic<uint64_t> rr_seed{0};
    uint64_t const START = rr_seed.fetch_add(1, std::memory_order_relaxed) % CORE_COUNT;

    uint64_t best_cpu = START;
    uint32_t best_load = UINT32_MAX;
    constexpr uint64_t PLACEMENT_SAMPLES = 4;
    uint64_t samples = 0;

    for (uint64_t off = 0; off < CORE_COUNT && samples < PLACEMENT_SAMPLES; off++) {
        uint64_t const I = (START + off) % CORE_COUNT;
        samples++;
        uint32_t const LOAD = cached_effective_load_for_cpu(I, task::TaskType::DAEMON);

        if (LOAD < best_load) {
            best_load = LOAD;
            best_cpu = I;
        }
        // Short-circuit: load=0 means truly empty CPU
        if (best_load == 0) {
            return best_cpu;
        }
    }
    return best_cpu;
}

auto get_least_loaded_cpu_in_mask(uint64_t mask) -> uint64_t {
    if (run_queues == nullptr || mask == 0) {
        return 0;
    }
    uint64_t const CORE_COUNT = smt::get_core_count();
    uint64_t const ALL_MASK = (CORE_COUNT >= 64) ? UINT64_MAX : ((1ULL << CORE_COUNT) - 1ULL);
    mask &= ALL_MASK;
    if (mask == 0) {
        return 0;
    }

    auto best_cpu = static_cast<uint64_t>(__builtin_ctzll(mask));
    uint32_t best_load = UINT32_MAX;
    static std::atomic<uint64_t> rr_seed{0};
    uint64_t const START = rr_seed.fetch_add(1, std::memory_order_relaxed) % CORE_COUNT;
    constexpr uint64_t PLACEMENT_SAMPLES = 4;
    uint64_t samples = 0;
    for (uint64_t off = 0; off < CORE_COUNT && samples < PLACEMENT_SAMPLES; ++off) {
        uint64_t const CPU = (START + off) % CORE_COUNT;
        if (!cpu_mask_contains(mask, CPU)) {
            continue;
        }
        samples++;
        uint32_t const LOAD = cached_effective_load_for_cpu(CPU, task::TaskType::DAEMON);
        if (LOAD < best_load) {
            best_load = LOAD;
            best_cpu = CPU;
        }
        if (best_load == 0) {
            break;
        }
    }
    return best_cpu;
}

void set_cpu_daemon_penalty(uint64_t cpu_no, uint32_t penalty) {
    if (run_queues == nullptr) {
        return;
    }
    run_queues->with_lock_void(cpu_no, [penalty](RunQueue* rq) { rq->daemon_load_penalty.store(penalty, std::memory_order_relaxed); });
}

void set_cpu_domain_id(uint64_t cpu_no, uint32_t new_domain_id) {
    if (run_queues == nullptr) {
        return;
    }
    run_queues->with_lock_void(cpu_no, [new_domain_id](RunQueue* rq) { rq->domain_id = new_domain_id; });
}

auto get_cpu_load(uint64_t cpu_no) -> uint32_t {
    if (run_queues == nullptr) {
        return 0;
    }
    return run_queues->with_lock(cpu_no, [](RunQueue* rq) -> uint32_t { return compute_effective_load_locked(rq); });
}

auto get_expired_task_refcounts(uint64_t cpu_no, uint64_t* pids, uint32_t* refcounts, size_t max_entries, size_t start_index) -> size_t {
    if (run_queues == nullptr || pids == nullptr || refcounts == nullptr || max_entries == 0) {
        return 0;
    }
    return run_queues->with_lock(cpu_no, [pids, refcounts, max_entries, start_index](RunQueue* rq) -> size_t {
        size_t count = 0;
        if (rq == nullptr) {
            return 0;
        }
        size_t idx = 0;
        task::Task const* cur = rq->dead_list.head;
        while (cur != nullptr) {
            if (idx < start_index) {
                idx++;
                cur = cur->sched_next;
                continue;
            }
            if (count >= max_entries) {
                break;
            }
            pids[count] = cur->pid;
            refcounts[count] = cur->ref_count.load(std::memory_order_acquire);
            count++;
            idx++;
            cur = cur->sched_next;
        }
        return count;
    });
}

#ifdef WOS_SELFTEST
auto scheduler_selftest_runtime_delta_saturates() -> bool {
    bool const REGULAR_US_TO_NS = runtime_delta_ns_from_us(1234) == 1'234'000ULL;
    bool const HUGE_US_TO_NS_SATURATES = runtime_delta_ns_from_us(UINT64_MAX) == UINT64_MAX;
    bool const NICE0_VRUNTIME_MATCHES_NS = vruntime_delta_from_runtime_ns(1'000'000ULL, 1024) == 1'000'000;
    bool const ZERO_WEIGHT_IS_SAFE = vruntime_delta_from_runtime_ns(1024ULL, 0) == 1'048'576;
    bool const HUGE_VRUNTIME_SATURATES = vruntime_delta_from_runtime_ns(UINT64_MAX, 1) == INT64_MAX;
    bool const SLICE_ACCUMULATES = accumulate_slice_used_ns(100, 200, 1000) == 300;
    bool const SLICE_SATURATES = accumulate_slice_used_ns(900, 500, 1000) == 1000;
    bool const WEIGHTED_MUL_SATURATES = saturating_i64_mul(INT64_MAX, 2) == INT64_MAX;

    return REGULAR_US_TO_NS && HUGE_US_TO_NS_SATURATES && NICE0_VRUNTIME_MATCHES_NS && ZERO_WEIGHT_IS_SAFE && HUGE_VRUNTIME_SATURATES &&
           SLICE_ACCUMULATES && SLICE_SATURATES && WEIGHTED_MUL_SATURATES;
}
#endif

}  // namespace ker::mod::sched

// ============================================================================
// Extern "C" exports for assembly code
// ============================================================================

extern "C" auto wos_get_current_task() -> ker::mod::sched::task::Task* { return ker::mod::sched::get_current_task(); }

extern "C" void wos_commit_handoff_task() { ker::mod::sched::commit_handoff_task_at_return_boundary(); }

extern "C" auto wos_get_current_pagemap() -> ker::mod::mm::paging::PageTable* {  // NOLINT
    auto* t = ker::mod::sched::get_current_task();
    return (t != nullptr) ? t->pagemap : nullptr;
}
