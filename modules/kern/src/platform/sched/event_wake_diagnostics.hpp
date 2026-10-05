#pragma once

#include <cstdint>
#include <platform/sched/scheduler_timer_trace.hpp>

namespace ker::mod::sched {

// Caller-owned elapsed observations; no task state or wake protocol lives here.
// Lock and poke spans are subsets of reschedule_us. Fixed start-CPU counters
// can include other tasks and exclude scheduler IRQ duration. Never subtract
// them to claim exclusive wake CPU or infer no intervening migration.
struct EventWakeDiagnostics {
    SchedulerTimerTraceWindow timer_trace{};
    uint64_t waker_pid = 0;
    uint8_t waker_type = 0;
    bool waker_pinned = false;
    bool waker_valid = false;
    uint64_t started_us = 0;
    uint64_t total_us = 0;
    uint64_t target_us = 0;
    uint64_t prepare_us = 0;
    uint64_t admission_us = 0;
    uint64_t reschedule_us = 0;
    uint64_t rq_wait_us = 0;
    uint64_t rq_body_us = 0;
    uint64_t rq_return_us = 0;
    uint64_t poke_us = 0;
    uint64_t irq_start_us = 0;
    uint64_t irq_end_us = 0;
    uint64_t timer_start = 0;
    uint64_t timer_end = 0;
    uint64_t cpu_start = 0;
    uint64_t cpu_end = 0;
    uint64_t target_cpu = 0;
    uint64_t passes = 0;
    uint64_t locks = 0;
    uint64_t scans = 0;
    uint64_t pokes = 0;
    bool recorded = false;
    bool leader = false;
    bool counters_valid = false;
};

}  // namespace ker::mod::sched
