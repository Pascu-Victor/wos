#pragma once

#include <cstdint>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::perf {

constexpr uint64_t PERF_CALLSITE_MAGIC = 0x5045524653495445ULL;

struct PerfCallsiteInfo {
    uint64_t magic;
    const char* file;
    const char* function;
    uint32_t line;
    uint32_t reserved;
};

// ===========================================================================
// Event types and flags
// ===========================================================================

enum class PerfEventType : uint8_t {
    SAMPLE = 0,  // Periodic timer-tick CPU sample (who is running, at what RIP)
    SWITCH = 1,  // Task context switch: prev task -> next task
    WAKE = 2,    // Task woken from blocking wait (timer expiry)
    SLEEP = 3,   // Task entering blocking wait (kern_sleep_us / kern_block)
};

// Flags for SAMPLE / SWITCH events
constexpr uint8_t PERF_FLAG_USER_MODE = 0x01;      // (SAMPLE) RIP was in userspace
constexpr uint8_t PERF_FLAG_PREEMPT = 0x02;        // (SWITCH) involuntary timer preemption
constexpr uint8_t PERF_FLAG_YIELD = 0x04;          // (SWITCH) voluntary sched_yield / kern_yield
constexpr uint8_t PERF_FLAG_BLOCK = 0x08;          // (SWITCH/SLEEP) blocking on I/O or timer
constexpr uint8_t PERF_FLAG_TIMED = 0x10;          // (WAKE/SLEEP) wakeAtUs carried a timer deadline
constexpr uint8_t PERF_FLAG_EXPLICIT_WAKE = 0x20;  // (WAKE) wake came from kern_wake/reschedule path
constexpr uint8_t PERF_FLAG_WAKE_CURRENT = 0x40;   // (WAKE) wake raced a task still current on some CPU

// ===========================================================================
// Event record — 48 bytes
// ===========================================================================

struct PerfEvent {
    uint64_t ts_ns;     // Monotonic timestamp (nanoseconds since boot, TSC-backed)
    uint64_t pid;       // Subject task PID.  For SWITCH: the outgoing (preempted) task.
    uint64_t data;      // SWITCH: next task PID;  SAMPLE: saved instruction pointer;
                        // WAKE/SLEEP: wakeAtUs (expected wake time, 0 if event-driven)
    uint64_t callsite;  // SWITCH/WAKE/SLEEP: PerfCallsiteInfo* or raw RIP fallback
    int64_t lag_v;      // EEVDF lag at event time: avg_vruntime − task->vruntime.
                        // Positive -> task is eligible (ahead of schedule).
                        // Negative -> task has used more than its fair share.
    uint16_t cpu;       // CPU number the event occurred on
    uint8_t type;       // PerfEventType cast to uint8_t
    uint8_t flags;      // PERF_FLAG_* bitmap
    uint32_t aux;       // SWITCH/SLEEP: run burst before switch/block (us)
                        // WAKE: observed sleep duration (us)
};

static_assert(sizeof(PerfEvent) == 48, "PerfEvent size mismatch");

// ===========================================================================
// Per-CPU aggregate statistics  (running counters, never reset)
// ===========================================================================

struct PerfCpuStats {
    uint64_t ctx_switches;  // Total context switches away from this CPU
    uint64_t preemptions;   // Timer-forced involuntary preemptions
    uint64_t yields;        // Voluntary yields (sched_yield / kern_yield)
    uint64_t sleeps;        // Transitions into blocking wait
    uint64_t wakes;         // Timer-expiry wakeups
    uint64_t samples;       // SAMPLE events recorded (sub-sampled ~100 Hz)
};

// ===========================================================================
// Per-CPU ring buffer  (2 048 events × 48 bytes = 96 KiB per CPU)
// ===========================================================================

constexpr size_t PERF_RING_SHIFT = 11;
constexpr size_t PERF_RING_ENTRIES = 1u << PERF_RING_SHIFT;  // 2048
constexpr size_t PERF_RING_MASK = PERF_RING_ENTRIES - 1;
constexpr size_t PERF_MAX_CPUS = 16;

struct PerfCpuRing {
    PerfCpuStats stats;
    uint64_t head;                 // Monotonically-increasing write index
    uint64_t drain;                // Consumer read index (advances on drain_events)
    ker::mod::sys::Spinlock lock;  // IRQ-safe spinlock protecting head/drain/events
    PerfEvent events[PERF_RING_ENTRIES];
};

// ===========================================================================
// Public API
// All record_* functions are IRQ-safe and callable from interrupt context.
// ===========================================================================

// Initialize ring buffers (recording starts disabled).  Call once during sched_init().
void init();

bool is_enabled();
void enable();
void disable();
// Reset all ring buffers (clear head/drain/events) without affecting stats counters.
// Called before enable() to ensure report only contains events from the new session.
void reset_rings();

// Record a timer-tick CPU sample.
// lag_v = compute_avg_vruntime(rq) - task->vruntime  (raw vruntime units)
void record_sample(uint32_t cpu, uint64_t pid, uint64_t rip, bool user_mode, int64_t lag_v);

// Record a context switch: prev_pid loses the CPU, next_pid gets it.
// flags:  PERF_FLAG_PREEMPT | PERF_FLAG_YIELD | PERF_FLAG_BLOCK
void record_switch(uint32_t cpu, uint64_t prev_pid, uint64_t next_pid, uint8_t flags, int64_t lag_v, uint32_t run_us, uint64_t callsite);

// Record a task being woken from the wait list (timer expiry).
void record_wake(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t sleep_us, uint64_t callsite);

// Record a task voluntarily entering the wait list.
void record_sleep(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t run_us, uint64_t callsite);

// Drain up to max_events events into dst.
// cpu_filter < PERF_MAX_CPUS -> drain only that CPU.
// cpu_filter == UINT32_MAX   -> drain all CPUs round-robin.
// Returns number of events written.  Advances each ring's drain cursor.
size_t drain_events(PerfEvent* dst, size_t max_events, uint32_t cpu_filter);

// Read per-CPU aggregate stats (non-destructive).
PerfCpuStats get_cpu_stats(uint32_t cpu);

}  // namespace ker::mod::perf
