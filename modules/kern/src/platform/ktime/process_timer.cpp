#include "process_timer.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/ktime/timer_queue.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::time::process_timer {
namespace {
using Task = sched::task::Task;
using log = ker::mod::dbg::logger<"process_timer">;
constexpr size_t WORK_BUDGET = 32;
constexpr uint64_t POLL_US = 1000;
sys::Spinlock service_lock;
constinit TimerQueue real_queue{TimerDispatch::SYNCHRONOUS};
Task* wake_head{};
Task* wake_tail{};
uint64_t wake_depth{};
uint64_t max_late_us{};
std::atomic<uint64_t> expirations{0};
std::atomic<bool> worker_ready{false};
std::atomic<bool> worker_starting{false};

void unused_callback(void*) {}
void count_expirations(uint64_t count) {
    uint64_t previous = expirations.load(std::memory_order_relaxed);
    while (!expirations.compare_exchange_weak(previous, arithmetic::saturating_add(previous, count), std::memory_order_relaxed)) {
    }
}

auto retain_work(Task& owner) -> bool {
    if (!owner.try_acquire_lifetime_ref()) return false;
    owner.interval_work_refs.fetch_add(1, std::memory_order_release);
    return true;
}
void release_work(Task& owner) {
    owner.interval_work_refs.fetch_sub(1, std::memory_order_acq_rel);
    owner.release();
}

auto clocks_locked(const Task& owner) -> interval::Clocks {
    return {.monotonic_us = time::get_us(), .user_us = owner.resources.cpu_user_time_us, .total_us = owner.resources.cpu_time_us};
}

auto publish_locked(Task& owner, uint8_t domains, uint64_t monotonic_us = 0) -> bool {
    auto const DUE = owner.interval_timers.collect(
        {.monotonic_us = monotonic_us, .user_us = owner.resources.cpu_user_time_us, .total_us = owner.resources.cpu_time_us}, domains);
    uint64_t signals = 0;
    for (size_t i = 0; i < DUE.count; ++i) {
        uint64_t const MASK = owner.interval_timers.claim(DUE.tokens[i]);
        if (MASK != 0 && owner.signal_handler_snapshot(static_cast<unsigned>(__builtin_ctzll(MASK))).handler != 1) signals |= MASK;
    }
    if (DUE.count != 0) count_expirations(DUE.count);
    if (signals != 0) owner.timer_sig_pending.fetch_or(signals, std::memory_order_release);
    return signals != 0;
}

void detach_wake_locked(Task& owner) {
    if (owner.interval_wake_prev != nullptr)
        owner.interval_wake_prev->interval_wake_next = owner.interval_wake_next;
    else
        wake_head = owner.interval_wake_next;
    if (owner.interval_wake_next != nullptr)
        owner.interval_wake_next->interval_wake_prev = owner.interval_wake_prev;
    else
        wake_tail = owner.interval_wake_prev;
    owner.interval_wake_prev = owner.interval_wake_next = nullptr;
    owner.interval_wake_queued = false;
    if (wake_depth != 0) --wake_depth;
}

// The owner lifetime is held by the worker. Selection and wake happen outside
// both service/resource locks; shared signal claims select the eventual receiver.
void wake_recipients(Task& owner) {
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    bool const CLOSED = owner.interval_timers.closed() || owner.resource_cpu_admission_closed;
    uint64_t pending = owner.timer_sig_pending.load(std::memory_order_acquire) & Task::TIMER_SIGNALS;
    owner.resource_lock.unlock_irqrestore(FLAGS);
    if (CLOSED) return;
    while (pending != 0) {
        uint64_t const BIT = pending & (~pending + 1);
        pending &= ~BIT;
        struct Selection {
            Task* owner;
            uint64_t bit;
        } selection{&owner, BIT};
        auto eligible = [](Task* task, void* context) -> bool {
            auto const& request = *static_cast<Selection*>(context);
            return task->type == sched::task::TaskType::PROCESS && task->credential_owner() == request.owner &&
                   !task->main_thread_retired.load(std::memory_order_acquire) && !task->exit_in_progress && !task->has_exited &&
                   (task->signal_mask_bits() & request.bit) == 0;
        };
        if (eligible(&owner, &selection)) {
            sched::wake_task_for_signal(&owner);
        } else if (auto* target = sched::find_active_task_lifetime_ref_if(eligible, &selection); target != nullptr) {
            sched::wake_task_for_signal(target);
            target->release();
        }
    }
}

void dispatch_once(uint64_t now_us) {
    std::array<Task*, WORK_BUDGET * 2> work{};
    size_t count = 0;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    for (size_t i = 0; i < WORK_BUDGET; ++i) {
        TimerInvocation invocation{};
        uint64_t const NOW_US = now_us;
        if (!real_queue.pop_due(NOW_US, invocation)) break;
        auto& owner = *static_cast<Task*>(invocation.context);
        uint64_t const LATE_US = NOW_US - owner.interval_real_timer.deadline();
        if (LATE_US > max_late_us) max_late_us = LATE_US;
        // Transfer the popped queue's reference into this work batch.
        work[count++] = &owner;
        uint64_t const OWNER_FLAGS = owner.resource_lock.lock_irqsave();
        if (!owner.interval_timers.closed() && !owner.resource_cpu_admission_closed) {
            static_cast<void>(publish_locked(owner, 0b001, NOW_US));
            uint64_t deadline = 0;
            if (owner.interval_timers.next_deadline(interval::Kind::REAL, deadline) && retain_work(owner)) {
                if (!real_queue.arm_at(owner.interval_real_timer, deadline, unused_callback, &owner)) {
                    static_cast<void>(owner.interval_timers.cancel(interval::Kind::REAL));
                    // release() is atomic-only, but keep all service lifetime
                    // releases outside the ownership locks using a later row.
                    work[count++] = &owner;
                }
            }
        }
        owner.resource_lock.unlock_irqrestore(OWNER_FLAGS);
        // A failed rearm used two rows; reserve space for the next item.
        if (count + 2 > work.size()) break;
    }
    for (size_t i = 0; i < WORK_BUDGET && count < work.size() && wake_head != nullptr; ++i) {
        Task* owner = wake_head;
        detach_wake_locked(*owner);
        work[count++] = owner;  // Transfer queued wake reference.
    }
    service_lock.unlock_irqrestore(FLAGS);
    for (size_t i = 0; i < count; ++i) {
        wake_recipients(*work[i]);
        // A selected receiver can mask or retire before accepting the signal.
        // Retain bounded retry work until shared pending state is consumed or
        // closed, including while every thread masks the signal.
        if (work[i]->timer_sig_pending.load(std::memory_order_acquire) != 0) notify(*work[i]);
        release_work(*work[i]);
    }
}

void worker_main() {
    for (;;) {
        dispatch_once(time::get_us());
        sched::kern_sleep_us(POLL_US);
    }
}

}  // namespace

auto status() -> Status {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    Status const result{.real_depth = real_queue.size(),
                        .wake_depth = wake_depth,
                        .expirations = expirations.load(std::memory_order_relaxed),
                        .max_late_us = max_late_us,
                        .work_budget = WORK_BUDGET * 2};
    service_lock.unlock_irqrestore(FLAGS);
    return result;
}

auto init() -> bool {
    bool expected = false;
    if (!worker_starting.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        return worker_ready.load(std::memory_order_acquire);
    auto* worker = Task::create_kernel_thread("process_timer", worker_main);
    if (worker == nullptr) {
        worker_starting.store(false, std::memory_order_release);
        log::error("cannot create process timer worker");
        return false;
    }
    if (!sched::post_task_balanced(worker)) {
        log::error("cannot publish process timer worker");
        // Startup caller treats failure as fatal: never silently accept timers
        // without a published worker or retry an ambiguously published task.
        return false;
    }
    worker_ready.store(true, std::memory_order_release);
    return true;
}

auto set(Task& subject, int which, interval::Value requested, interval::Value* previous) -> int {
    if (which < 0 || which > 2) return -EINVAL;
    if (!worker_ready.load(std::memory_order_acquire)) return -EAGAIN;
    sched::resource::Owner retained(&subject);
    auto* owner = retained.get();
    if (owner == nullptr) return -ESRCH;
    auto const KIND = static_cast<interval::Kind>(which);
    bool release_previous = false;
    bool release_failed = false;
    int result = 0;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    uint64_t const OWNER_FLAGS = owner->resource_lock.lock_irqsave();
    if (owner->resource_cpu_admission_closed || owner->exit_in_progress || owner->interval_timers.closed()) {
        result = -ESRCH;
    } else if (owner->resource_exec_admission_closed) {
        result = -EAGAIN;
    } else if (KIND == interval::Kind::REAL && requested.remaining_us != 0 && owner->interval_real_timer.generation() == UINT64_MAX) {
        result = -EOVERFLOW;
    } else {
        auto const CONFIGURED = owner->interval_timers.configure(KIND, requested, clocks_locked(*owner), previous);
        if (CONFIGURED != interval::ConfigureResult::APPLIED) {
            result = CONFIGURED == interval::ConfigureResult::GENERATION_EXHAUSTED ? -EOVERFLOW : -ESRCH;
        } else if (KIND == interval::Kind::REAL) {
            if (owner->interval_real_timer.state() == TimerState::ARMED) {
                static_cast<void>(real_queue.cancel(owner->interval_real_timer));
                release_previous = true;
            }
            uint64_t deadline = 0;
            if (owner->interval_timers.next_deadline(KIND, deadline)) {
                if (!retain_work(*owner)) {
                    static_cast<void>(owner->interval_timers.cancel(KIND));
                    result = -ESRCH;
                } else if (!real_queue.arm_at(owner->interval_real_timer, deadline, unused_callback, owner)) {
                    static_cast<void>(owner->interval_timers.cancel(KIND));
                    release_failed = true;
                    result = -EOVERFLOW;
                }
            }
        }
    }
    owner->resource_lock.unlock_irqrestore(OWNER_FLAGS);
    service_lock.unlock_irqrestore(FLAGS);
    if (release_previous) release_work(*owner);
    if (release_failed) release_work(*owner);
    return result;
}

auto get(Task& subject, int which, interval::Value& value) -> int {
    if (which < 0 || which > 2) return -EINVAL;
    sched::resource::Owner retained(&subject);
    auto* owner = retained.get();
    if (owner == nullptr) return -ESRCH;
    uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
    bool const OK = owner->interval_timers.read(static_cast<interval::Kind>(which), clocks_locked(*owner), value);
    owner->resource_lock.unlock_irqrestore(FLAGS);
    return OK ? 0 : -EINVAL;
}

auto charge_locked(Task& owner) -> bool {
    return !owner.resource_cpu_admission_closed && !owner.interval_timers.closed() && publish_locked(owner, 0b110);
}

void notify(Task& owner) {
    uint64_t const FLAGS = service_lock.lock_irqsave();
    uint64_t const OWNER_FLAGS = owner.resource_lock.lock_irqsave();
    if (!owner.interval_timers.closed() && !owner.resource_cpu_admission_closed && !owner.interval_wake_queued &&
        (owner.timer_sig_pending.load(std::memory_order_acquire) & Task::TIMER_SIGNALS) != 0 && retain_work(owner)) {
        owner.interval_wake_prev = wake_tail;
        owner.interval_wake_next = nullptr;
        if (wake_tail != nullptr)
            wake_tail->interval_wake_next = &owner;
        else
            wake_head = &owner;
        wake_tail = &owner;
        owner.interval_wake_queued = true;
        wake_depth = arithmetic::saturating_add(wake_depth, 1);
    }
    owner.resource_lock.unlock_irqrestore(OWNER_FLAGS);
    service_lock.unlock_irqrestore(FLAGS);
}

void close(Task& owner) {
    unsigned releases = 0;
    uint64_t const FLAGS = service_lock.lock_irqsave();
    uint64_t const OWNER_FLAGS = owner.resource_lock.lock_irqsave();
    owner.interval_timers.close();
    owner.timer_sig_pending.store(0, std::memory_order_release);
    if (owner.interval_real_timer.state() == TimerState::ARMED) {
        static_cast<void>(real_queue.cancel(owner.interval_real_timer));
        ++releases;
    }
    if (owner.interval_wake_queued) {
        detach_wake_locked(owner);
        ++releases;
    }
    owner.resource_lock.unlock_irqrestore(OWNER_FLAGS);
    service_lock.unlock_irqrestore(FLAGS);
    while (releases-- != 0) release_work(owner);
}

auto has_active_locked(const Task& owner) -> bool {
    uint64_t deadline;
    return owner.interval_timers.next_deadline(interval::Kind::REAL, deadline) ||
           owner.interval_timers.next_deadline(interval::Kind::VIRTUAL, deadline) ||
           owner.interval_timers.next_deadline(interval::Kind::PROF, deadline) ||
           owner.timer_sig_pending.load(std::memory_order_acquire) != 0 || owner.timer_sig_claimed.load(std::memory_order_acquire) != 0 ||
           owner.interval_work_refs.load(std::memory_order_acquire) != 0;
}
#ifdef WOS_SELFTEST
auto selftest_service() -> bool {
    // KTEST runs before SMP/daemon startup. Exercise the production service
    // synchronously, with explicit REAL observation time and no scheduler park.
    if (worker_ready.load() || worker_starting.load() || real_queue.size() != 0 || wake_head != nullptr) return false;
    worker_ready.store(true);
    Task owner{};
    owner.type = sched::task::TaskType::PROCESS;
    owner.pid = 0x7B3191;
    owner.signal_mask_store(UINT64_MAX);
    Task thread{};
    thread.type = sched::task::TaskType::PROCESS;
    thread.is_thread = true;
    thread.owner_pid = owner.pid;
    thread.signal_mask_store(UINT64_MAX);
    bool ok = sched::resource::bind_thread_owner(thread, owner);
    interval::Value previous{};
    ok &= set(thread, 0, {1000, 0}, &previous) == 0 && previous.remaining_us == 0;
    dispatch_once(arithmetic::saturating_add(time::get_us(), 2000));
    ok &= (owner.timer_sig_pending.load() & interval::ProcessTimers::signal_mask(interval::Kind::REAL)) != 0;
    ok &= owner.interval_work_refs.load() == 1;  // Pending masked signal retains a retry.
    owner.timer_sig_pending.store(0);
    dispatch_once(time::get_us());
    ok &= owner.interval_work_refs.load() == 0;
    ok &= set(thread, 1, {200, 50}) == 0 && set(owner, 2, {300, 70}) == 0;
    sched::resource::charge_cpu(&thread, 400, 250);
    uint64_t const CPU_MASK =
        interval::ProcessTimers::signal_mask(interval::Kind::VIRTUAL) | interval::ProcessTimers::signal_mask(interval::Kind::PROF);
    ok &= (owner.timer_sig_pending.load() & CPU_MASK) == CPU_MASK && thread.timer_sig_pending.load() == 0;
    interval::Value current{};
    ok &= get(owner, 1, current) == 0 && current.remaining_us == 50 && current.interval_us == 50;
    ok &= get(thread, 2, current) == 0 && current.remaining_us == 40 && current.interval_us == 70;
    ok &= set(owner, 3, {1, 1}) == -EINVAL;
    owner.timer_sig_pending.store(0);
    dispatch_once(time::get_us());
    ok &= owner.interval_work_refs.load() == 0;
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    owner.resource_exec_admission_closed = true;
    owner.resource_lock.unlock_irqrestore(FLAGS);
    ok &= set(thread, 1, {1, 1}) == -EAGAIN;
    uint64_t const FLAGS2 = owner.resource_lock.lock_irqsave();
    owner.resource_exec_admission_closed = false;
    owner.resource_lock.unlock_irqrestore(FLAGS2);
    for (unsigned i = 0; i < 32; ++i) {
        ok &= set(owner, 0, {10000000, 0}) == 0;
        ok &= set(thread, 0, {0, 11}, &previous) == 0 && previous.remaining_us != 0;
    }
    // Simulate a worker's extracted independent reference across close.
    ok &= retain_work(owner);
    owner.timer_sig_pending.store(interval::ProcessTimers::signal_mask(interval::Kind::REAL));
    notify(owner);
    close(owner);
    ok &= owner.interval_work_refs.load() == 1 && owner.timer_sig_pending.load() == 0;
    release_work(owner);
    dispatch_once(time::get_us());
    ok &= owner.interval_work_refs.load() == 0 && owner.ref_count.load() == 2 && owner.interval_timers.closed();
    ok &= real_queue.size() == 0 && wake_head == nullptr && wake_depth == 0;
    worker_ready.store(false);
    return ok;
}
#endif
}  // namespace ker::mod::time::process_timer
