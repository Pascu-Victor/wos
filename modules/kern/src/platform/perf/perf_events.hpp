#pragma once

#include <array>
#include <atomic>
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
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define WOS_PERF_CALLSITE()                                                                                                           \
    __extension__({                                                                                                                   \
        static const ::ker::mod::perf::PerfCallsiteInfo __wos_perf_site = {::ker::mod::perf::PERF_CALLSITE_MAGIC, __FILE__, __func__, \
                                                                           static_cast<uint32_t>(__LINE__), 0U};                      \
        reinterpret_cast<uint64_t>(&__wos_perf_site);                                                                                 \
    })
// NOLINTEND(cppcoreguidelines-macro-usage)

// ===========================================================================
// Event types and flags
// ===========================================================================

enum class PerfEventType : uint8_t {
    SAMPLE = 0,          // Periodic timer-tick CPU sample (who is running, at what RIP)
    SWITCH = 1,          // Task context switch: prev task -> next task
    WAKE = 2,            // Task woken from blocking wait (timer expiry)
    SLEEP = 3,           // Task entering blocking wait (kern_sleep_us / kern_block)
    CONTAINER_STAT = 4,  // Container/data structure operation (insert/remove/resize/OOM)
    WKI = 5,             // WKI transport or RPC trace event
};

constexpr size_t PERF_EVENT_TYPE_COUNT = 6;

// Bitmask for selective event recording
constexpr uint8_t PERF_MASK_SAMPLE = 1U << 0;
constexpr uint8_t PERF_MASK_SWITCH = 1U << 1;
constexpr uint8_t PERF_MASK_WAKE = 1U << 2;
constexpr uint8_t PERF_MASK_SLEEP = 1U << 3;
constexpr uint8_t PERF_MASK_CONTAINER = 1U << 4;
constexpr uint8_t PERF_MASK_WKI = 1U << 5;
constexpr uint8_t PERF_MASK_WKI_LAUNCH = 1U << 6;
constexpr uint8_t PERF_MASK_ALL = 0x7F;

// Flags for SAMPLE / SWITCH events
constexpr uint8_t PERF_FLAG_USER_MODE = 0x01;      // (SAMPLE) RIP was in userspace
constexpr uint8_t PERF_FLAG_PREEMPT = 0x02;        // (SWITCH) involuntary timer preemption
constexpr uint8_t PERF_FLAG_YIELD = 0x04;          // (SWITCH) voluntary sched_yield / kern_yield
constexpr uint8_t PERF_FLAG_BLOCK = 0x08;          // (SWITCH/SLEEP) blocking on I/O or timer
constexpr uint8_t PERF_FLAG_TIMED = 0x10;          // (WAKE/SLEEP) wakeAtUs carried a timer deadline
constexpr uint8_t PERF_FLAG_EXPLICIT_WAKE = 0x20;  // (WAKE) wake came from kern_wake/reschedule path
constexpr uint8_t PERF_FLAG_WAKE_CURRENT = 0x40;   // (WAKE) wake raced a task still current on some CPU

// Flags for CONTAINER_STAT events
constexpr uint8_t PERF_FLAG_CT_INSERT = 0x01;  // Element inserted
constexpr uint8_t PERF_FLAG_CT_REMOVE = 0x02;  // Element removed
constexpr uint8_t PERF_FLAG_CT_RESIZE = 0x04;  // Container resized (rehash, grow)
constexpr uint8_t PERF_FLAG_CT_OOM = 0x08;     // Allocation failure
constexpr uint8_t PERF_FLAG_CT_SPILL = 0x10;   // SmallVec inline->heap spill
constexpr uint8_t PERF_FLAG_CT_LOOKUP = 0x20;  // Slow-path lookup (long collision chain)

// Flags for WKI events
constexpr uint8_t PERF_FLAG_WKI_BEGIN = 0x01;
constexpr uint8_t PERF_FLAG_WKI_END = 0x02;
constexpr uint8_t PERF_FLAG_WKI_POINT = 0x04;

enum class WkiPerfScope : uint8_t {
    NONE = 0,
    TRANSPORT = 1,
    REMOTE_VFS = 2,
    REMOTE_COMPUTE = 3,
    EVENT_BUS = 4,
};

enum class WkiPerfPhase : uint8_t {
    BEGIN = 1,
    END = 2,
    POINT = 3,
};

enum class WkiPerfTransportOp : uint8_t {
    SEND = 1,
    ACK_RTT = 2,
    RETRANSMIT = 3,
    FAST_RETRANSMIT = 4,
    NO_CREDITS = 5,
    WAIT = 6,
};

enum class WkiPerfVfsOp : uint8_t {
    ATTACH_WAIT = 1,
    PROXY_WAIT = 2,
    OPEN = 3,
    STAT = 4,
    READ = 5,
    READDIR = 6,
    WRITE = 7,
    SEEK = 8,
    TRUNCATE = 9,
    READLINK = 10,
    CLOSE = 11,
    MKDIR = 12,
    UNLINK = 13,
    RMDIR = 14,
    RENAME = 15,
    RETRY = 16,
};

enum class WkiPerfComputeOp : uint8_t {
    SUBMIT_INLINE = 1,
    SUBMIT_VFS_REF = 2,
    COMPLETE_WAIT = 3,
    ACCEPT = 4,
    REJECT = 5,
    COMPLETE = 6,
    PROXY_READY = 7,
    DEFER_WAIT = 8,
    LOAD_ELF = 9,
    HANDLE_SUBMIT = 10,
    TASK_RUNTIME = 11,
    PROXY_READY_WAIT = 12,
    COMPLETE_HOLD = 13,
};

enum class WkiPerfEventOp : uint8_t {
    SUBSCRIBE = 1,
    UNSUBSCRIBE = 2,
    PUBLISH = 3,
    ACK = 4,
    RETRY = 5,
    REPLAY = 6,
};

const char* wki_scope_name(WkiPerfScope scope);
const char* wki_phase_name(WkiPerfPhase phase);
const char* wki_op_name(WkiPerfScope scope, uint8_t op);

constexpr size_t WKI_PERF_SUMMARY_BUCKETS = 256;
constexpr size_t WKI_PERF_HIST_BUCKETS = 32;

struct WkiPerfSummarySnapshot {
    uint8_t scope;
    uint8_t op;
    uint16_t peer;
    uint16_t channel;
    uint16_t reserved;
    uint64_t calls;
    uint64_t errors;
    uint64_t retries;
    uint64_t bytes;
    uint64_t total_latency_us;
    uint32_t max_latency_us;
    uint32_t p95_us;
    uint32_t p99_us;
    uint32_t p999_us;
    uint32_t p9999_us;
    uint32_t p99999_us;
};

// ===========================================================================
// Subsystem identifiers for CONTAINER_STAT events
// ===========================================================================

enum class PerfSubsystem : uint8_t {
    NONE = 0,
    FD_TABLE,
    FUTEX,
    DEVICE_REG,
    BLOCK_DEV,
    PTY_POOL,
    MOUNT_TABLE,
    PIPE_WAITQ,
    ACCEPT_Q,
    NAPI_REG,
    VFS_RULES,
    EXIT_WAITERS,
    PTY_WAITERS,
    COUNT  // sentinel - number of subsystems
};

constexpr size_t PERF_SUBSYSTEM_COUNT = static_cast<size_t>(PerfSubsystem::COUNT);

const char* subsystem_name(PerfSubsystem s);

// ===========================================================================
// Per-subsystem aggregate statistics (global, always-on, lock-free)
// ===========================================================================

struct PerfSubsystemStats {
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> removes{0};
    std::atomic<uint64_t> resizes{0};
    std::atomic<uint64_t> oom_failures{0};
    std::atomic<uint64_t> peak_count{0};
    std::atomic<uint64_t> current_count{0};
};

// Plain-old-data snapshot returned by get_subsystem_stats()
struct PerfSubsystemSnapshot {
    uint64_t inserts;
    uint64_t removes;
    uint64_t resizes;
    uint64_t oom_failures;
    uint64_t peak_count;
    uint64_t current_count;
};

// ===========================================================================
// Event record - 48 bytes
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
// Per-CPU ring buffer  (2 048 events * 48 bytes = 96 KiB per CPU)
// ===========================================================================

constexpr size_t PERF_RING_SHIFT = 11;
constexpr size_t PERF_RING_ENTRIES = 1U << PERF_RING_SHIFT;  // 2048
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
// cpu_filter < get_num_perf_cpus() -> drain only that CPU.
// cpu_filter == UINT32_MAX   -> drain all CPUs round-robin.
// Returns number of events written.  Advances each ring's drain cursor.
size_t drain_events(PerfEvent* dst, size_t max_events, uint32_t cpu_filter);

// Read per-CPU aggregate stats (non-destructive).
PerfCpuStats get_cpu_stats(uint32_t cpu);

// Return the number of per-CPU perf rings allocated (== online CPU count at init time).
size_t get_num_perf_cpus();

// Record a container/data structure operation.
// subsystem:     which subsystem this container belongs to
// instance_id:   per-instance identifier (e.g. PTY number, device ID)
// flags:         PERF_FLAG_CT_* bitmap
// element_count: current number of elements in the container
// capacity:      current capacity (e.g. bucket count after rehash), or 0
// callsite:      PerfCallsiteInfo* or raw RIP
void record_container_stat(uint32_t cpu, uint64_t pid, PerfSubsystem subsystem, uint32_t instance_id, uint8_t flags, int64_t element_count,
                           uint32_t capacity, uint64_t callsite);

// Update per-subsystem atomic counters (always-on, independent of recording).
void update_subsystem_stat(PerfSubsystem subsystem, uint8_t flags, uint64_t current_count);

// Read per-subsystem aggregate stats (non-destructive).
PerfSubsystemSnapshot get_subsystem_stats(PerfSubsystem subsystem);

// Event mask control: set which event types are recorded.
void set_event_mask(uint8_t mask);
uint8_t get_event_mask();

bool is_wki_recording_enabled();

uint64_t wki_pack_event_data(WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel);
void wki_unpack_event_data(uint64_t data, WkiPerfScope& scope, uint8_t& op, WkiPerfPhase& phase, uint16_t& peer, uint16_t& channel);
uint64_t wki_pack_trace_state(uint32_t correlation, int32_t status);
uint32_t wki_unpack_trace_correlation(int64_t packed);
int32_t wki_unpack_trace_status(int64_t packed);

uint32_t next_wki_trace_correlation();
void record_wki_event(uint32_t cpu, uint64_t pid, WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel,
                      uint32_t correlation, int32_t status, uint32_t aux, uint64_t callsite);
void record_wki_summary(WkiPerfScope scope, uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us,
                        bool has_latency, uint32_t retries, uint64_t bytes);
size_t get_wki_summary_snapshots(WkiPerfSummarySnapshot* dst, size_t max);

// Parse a comma-separated event type string into a mask.
// Recognized names: sample, switch, wake, sleep, container, wki, wki_launch
// Returns 0 on parse error.
uint8_t parse_event_mask(const char* str, size_t len);

}  // namespace ker::mod::perf
