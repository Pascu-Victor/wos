// Sleeping Mutex implementation.
//
// Fast path: atomic CAS on the `held_` flag - if uncontested, lock/unlock
// are a single atomic operation with no scheduler involvement.
//
// Slow path: when the lock is held, the caller parks briefly through the
// scheduler when that is safe, then retries. unlock() only clears `held_`; the
// timeout-based wait avoids coupling mutex wakeups to stack-local waiter nodes.
//
// Before the scheduler can identify the current task, contention falls back to
// a pause loop.  Real lock contention is not expected that early, but this keeps
// global/static mutexes boot-safe.

#include "mutex.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sys {

namespace {

std::atomic<int> mutex_stall_enabled_cache{-1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint64_t MUTEX_STALL_LOG_AFTER_US = 500'000;
constexpr uint64_t MUTEX_STALL_LOG_INTERVAL_US = 1'000'000;
constexpr uint64_t MUTEX_PARK_INTERVAL_US = 500;
constexpr size_t TASK_LABEL_MAX = 32;

struct OwnerTaskSnapshot {
    uint64_t pid = 0;
    uint64_t cpu = 0;
    uint64_t wait_callsite = 0;
    uint64_t saved_cs = 0;
    uint64_t saved_rip = 0;
    uint64_t saved_rsp = 0;
    uint64_t wake_at_us = 0;
    uint64_t preempt_max_us = 0;
    uint64_t preempt_owner = 0;
    uint64_t preempt_start_us = 0;
    uint32_t preempt_depth = 0;
    uint8_t state = 0;
    uint8_t type = 0;
    uint8_t wait_kind = 0;
    uint8_t sched_queue = 0;
    bool found = false;
    bool yield_switch = false;
    bool deferred_task_switch = false;
    bool voluntary_block = false;
    bool wants_block = false;
    bool wakeup_pending = false;
    std::array<char, TASK_LABEL_MAX> name{'?', '\0'};
    std::array<char, TASK_LABEL_MAX> wait_channel{'-', '\0'};
};

template <size_t N>
void copy_label(std::array<char, N>& dst, const char* value, const char* fallback) {
    const char* source = value != nullptr ? value : fallback;
    size_t i = 0;
    while (i + 1 < N && source[i] != '\0') {
        dst[i] = source[i];
        ++i;
    }
    dst[i] = '\0';
}

[[nodiscard]] auto task_wait_callsite(sched::task::Task const* task) -> uint64_t {
    if (task == nullptr) {
        return 0;
    }
    if (task->perf_wait_callsite != 0) {
        return task->perf_wait_callsite;
    }
    return task->context.frame.rip;
}

[[nodiscard]] auto snapshot_owner_task(uint64_t pid) -> OwnerTaskSnapshot {
    OwnerTaskSnapshot snapshot{};
    snapshot.pid = pid;
    if (pid == 0) {
        return snapshot;
    }

    auto* task = sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return snapshot;
    }

    snapshot.found = true;
    snapshot.pid = task->pid;
    snapshot.cpu = task->cpu;
    snapshot.wait_callsite = task_wait_callsite(task);
    snapshot.saved_cs = task->context.frame.cs;
    snapshot.saved_rip = task->context.frame.rip;
    snapshot.saved_rsp = task->context.frame.rsp;
    snapshot.wake_at_us = task->wake_at_us;
    snapshot.preempt_depth = task->preempt_disable_depth;
    snapshot.preempt_max_us = task->preempt_disable_max_us;
    snapshot.preempt_owner = task->preempt_disable_owner;
    snapshot.preempt_start_us = task->preempt_disable_start_us;
    snapshot.state = static_cast<uint8_t>(task->state.load(std::memory_order_acquire));
    snapshot.type = static_cast<uint8_t>(task->type);
    snapshot.wait_kind = static_cast<uint8_t>(task->wait_channel_kind);
    snapshot.sched_queue = static_cast<uint8_t>(task->sched_queue);
    snapshot.yield_switch = task->yield_switch;
    snapshot.deferred_task_switch = task->deferred_task_switch;
    snapshot.voluntary_block = task->is_voluntary_blocked();
    snapshot.wants_block = task->wants_block;
    snapshot.wakeup_pending = task->wakeup_pending.load(std::memory_order_acquire);
    copy_label(snapshot.name, task->name, "?");
    copy_label(snapshot.wait_channel, task->wait_channel, "-");
    task->release();
    return snapshot;
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') {
            ++cursor;
        }
        if (*cursor == '\0') {
            break;
        }

        const char* start = cursor;
        size_t segment_len = 0;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\n') {
            ++cursor;
            ++segment_len;
        }
        if (segment_len == TOKEN_LEN && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto mutex_stall_enabled() -> bool {
    int const CACHED = mutex_stall_enabled_cache.load(std::memory_order_relaxed);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    bool const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "mutex.stall");
    mutex_stall_enabled_cache.store(ENABLED ? 1 : 0, std::memory_order_relaxed);
    return ENABLED;
}

}  // namespace

void Mutex::record_owner(uint64_t acquire_site) {
    if (!mutex_stall_enabled()) {
        return;
    }

    owner_acquire_site.store(acquire_site, std::memory_order_relaxed);
    if (!sched::can_query_current_task()) {
        owner_name.store(nullptr, std::memory_order_relaxed);
        owner_pid.store(0, std::memory_order_release);
        return;
    }

    auto* task = sched::get_current_task();
    owner_pid.store(task != nullptr ? task->pid : 0, std::memory_order_relaxed);
    owner_name.store((task != nullptr && task->name != nullptr) ? task->name : nullptr, std::memory_order_relaxed);
    owner_acquire_site.store(acquire_site, std::memory_order_release);
}

void Mutex::clear_owner() {
    if (!mutex_stall_enabled()) {
        return;
    }

    owner_acquire_site.store(0, std::memory_order_relaxed);
    owner_name.store(nullptr, std::memory_order_relaxed);
    owner_pid.store(0, std::memory_order_release);
}

void Mutex::maybe_log_stall(uint64_t waiter_site, uint64_t wait_start_us, const SlowPathStats& stats) {
    if (!mutex_stall_enabled()) {
        return;
    }

    uint64_t const NOW_US = ker::mod::time::get_us();
    if (wait_start_us == 0 || NOW_US < wait_start_us || NOW_US - wait_start_us < MUTEX_STALL_LOG_AFTER_US) {
        return;
    }

    uint64_t const LAST_US = last_stall_log_us.load(std::memory_order_relaxed);
    if (LAST_US != 0 && NOW_US >= LAST_US && NOW_US - LAST_US < MUTEX_STALL_LOG_INTERVAL_US) {
        return;
    }
    last_stall_log_us.store(NOW_US, std::memory_order_relaxed);

    bool const CAN_QUERY_NOW = sched::can_query_current_task();
    auto* waiter = CAN_QUERY_NOW ? sched::get_current_task() : nullptr;
    uint32_t const PREEMPT_NOW = CAN_QUERY_NOW ? sched::preempt_count() : 0;
    uint64_t const OWNER_PID = owner_pid.load(std::memory_order_acquire);
    OwnerTaskSnapshot const OWNER_TASK = snapshot_owner_task(OWNER_PID);
    const char* owner_display_name = OWNER_TASK.name.data();
    if (!OWNER_TASK.found) {
        const char* owner_task_name = owner_name.load(std::memory_order_acquire);
        owner_display_name = owner_task_name != nullptr ? owner_task_name : "?";
    }
    dbg::logger<"mutex">::warn(
        "mutexstall: mutex=%p held=%u waiters=%u waiter=%lu(%s) waiter_site=0x%llx owner=%lu(%s) owner_site=0x%llx elapsed_us=%lu "
        "waits=%u parks=%u fallback=%u no_query=%u no_task=%u preempt_fallback=%u can_query_now=%u preempt_now=%u task_now=%p",
        static_cast<void*>(this), held.load(std::memory_order_relaxed) ? 1U : 0U, waiters.load(std::memory_order_relaxed),
        waiter != nullptr ? waiter->pid : 0, (waiter != nullptr && waiter->name != nullptr) ? waiter->name : "?",
        static_cast<unsigned long long>(waiter_site), static_cast<unsigned long>(OWNER_PID), owner_display_name,
        static_cast<unsigned long long>(owner_acquire_site.load(std::memory_order_acquire)),
        static_cast<unsigned long>(NOW_US - wait_start_us), stats.wait_count, stats.park_count, stats.fallback_count,
        stats.fallback_no_query, stats.fallback_no_task, stats.fallback_preempt, CAN_QUERY_NOW ? 1U : 0U, PREEMPT_NOW,
        static_cast<void*>(waiter));
    dbg::logger<"mutex">::warn(
        "mutexowner: mutex=%p found=%u owner=%lu(%s) state=%u type=%u cpu=%lu wait=%s kind=%u queue=%u wait_site=0x%llx",
        static_cast<void*>(this), OWNER_TASK.found ? 1U : 0U, static_cast<unsigned long>(OWNER_PID), owner_display_name,
        static_cast<unsigned>(OWNER_TASK.state), static_cast<unsigned>(OWNER_TASK.type), static_cast<unsigned long>(OWNER_TASK.cpu),
        OWNER_TASK.wait_channel.data(), static_cast<unsigned>(OWNER_TASK.wait_kind), static_cast<unsigned>(OWNER_TASK.sched_queue),
        static_cast<unsigned long long>(OWNER_TASK.wait_callsite));
    dbg::logger<"mutex">::warn(
        "mutexowner2: mutex=%p saved_cs=0x%llx saved_rip=0x%llx saved_rsp=0x%llx wake_at=%lu yield=%u deferred=%u vblk=%u wants=%u "
        "wakeup=%u preempt=%u/%lu preempt_owner=0x%llx preempt_start_us=%lu",
        static_cast<void*>(this), static_cast<unsigned long long>(OWNER_TASK.saved_cs),
        static_cast<unsigned long long>(OWNER_TASK.saved_rip), static_cast<unsigned long long>(OWNER_TASK.saved_rsp),
        static_cast<unsigned long>(OWNER_TASK.wake_at_us), OWNER_TASK.yield_switch ? 1U : 0U, OWNER_TASK.deferred_task_switch ? 1U : 0U,
        OWNER_TASK.voluntary_block ? 1U : 0U, OWNER_TASK.wants_block ? 1U : 0U, OWNER_TASK.wakeup_pending ? 1U : 0U,
        OWNER_TASK.preempt_depth, static_cast<unsigned long>(OWNER_TASK.preempt_max_us),
        static_cast<unsigned long long>(OWNER_TASK.preempt_owner), static_cast<unsigned long>(OWNER_TASK.preempt_start_us));
}

void Mutex::lock() {
    auto const CALLSITE = reinterpret_cast<uint64_t>(__builtin_return_address(0));

    // Fast path - try to acquire immediately
    bool expected = false;
    if (held.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        record_owner(CALLSITE);
        return;
    }

    // Slow path - contended
    waiters.fetch_add(1, std::memory_order_relaxed);

    // Brief spin before yielding (avoids a full context switch for very short
    // critical sections).
    constexpr int SPIN_LIMIT = 64;
    for (int i = 0; i < SPIN_LIMIT; i++) {
        expected = false;
        if (held.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            waiters.fetch_sub(1, std::memory_order_relaxed);
            record_owner(CALLSITE);
            return;
        }
        asm volatile("pause");
    }

    // Wait loop - park briefly when the scheduler can identify this task and
    // preemption is enabled. Otherwise keep the boot-safe fallback and record
    // exactly why it was needed.
    uint64_t const WAIT_START_US = ker::mod::time::get_us();
    SlowPathStats stats{};
    while (true) {
        bool const CAN_QUERY = sched::can_query_current_task();
        auto* current_task = CAN_QUERY ? sched::get_current_task() : nullptr;
        uint32_t const PREEMPT_DEPTH = current_task != nullptr ? sched::preempt_count() : 0;
        bool const CAN_PARK = CAN_QUERY && current_task != nullptr && PREEMPT_DEPTH == 0;
        if (CAN_PARK) {
            ++stats.park_count;
            if (current_task->type == sched::task::TaskType::PROCESS) {
                uint64_t const DEADLINE_US = sched::saturating_deadline_us(ker::mod::time::get_us(), MUTEX_PARK_INTERVAL_US);
                sched::preemptible_syscall_park_impl("mutex", sched::task::WaitChannelKind::GENERIC, DEADLINE_US, CALLSITE);
            } else {
                sched::kern_sleep_us_impl(MUTEX_PARK_INTERVAL_US, CALLSITE);
            }
        } else {
            ++stats.fallback_count;
            if (!CAN_QUERY) {
                ++stats.fallback_no_query;
            } else if (current_task == nullptr) {
                ++stats.fallback_no_task;
            } else if (PREEMPT_DEPTH != 0) {
                ++stats.fallback_preempt;
            }
            asm volatile("pause");
        }
        ++stats.wait_count;
        maybe_log_stall(CALLSITE, WAIT_START_US, stats);

        expected = false;
        if (held.compare_exchange_weak(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            waiters.fetch_sub(1, std::memory_order_relaxed);
            record_owner(CALLSITE);
            return;
        }
    }
}

auto Mutex::try_lock() -> bool {
    auto const CALLSITE = reinterpret_cast<uint64_t>(__builtin_return_address(0));
    bool expected = false;
    if (!held.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        return false;
    }
    record_owner(CALLSITE);
    return true;
}

void Mutex::unlock() {
    clear_owner();
    held.store(false, std::memory_order_release);
}

}  // namespace ker::mod::sys
