#pragma once

#include <array>
#include <atomic>
#include <cstddef>
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

// Bitmask for selective event recording.
constexpr uint16_t PERF_MASK_SAMPLE = 1U << 0;
constexpr uint16_t PERF_MASK_SWITCH = 1U << 1;
constexpr uint16_t PERF_MASK_WAKE = 1U << 2;
constexpr uint16_t PERF_MASK_SLEEP = 1U << 3;
constexpr uint16_t PERF_MASK_CONTAINER = 1U << 4;
constexpr uint16_t PERF_MASK_WKI = 1U << 5;
constexpr uint16_t PERF_MASK_WKI_LAUNCH = 1U << 6;
constexpr uint16_t PERF_MASK_LOCAL_PIPE = 1U << 7;
constexpr uint16_t PERF_MASK_LOCAL_PROC = 1U << 8;
constexpr uint16_t PERF_MASK_LOCAL_VMEM = 1U << 9;
constexpr uint16_t PERF_MASK_LOCAL_LOADER = 1U << 10;
constexpr uint16_t PERF_MASK_LOCAL_XFS = 1U << 11;
constexpr uint16_t PERF_MASK_LOCAL_IRQ = 1U << 12;
constexpr uint16_t PERF_MASK_LOCAL =
    PERF_MASK_LOCAL_PIPE | PERF_MASK_LOCAL_PROC | PERF_MASK_LOCAL_VMEM | PERF_MASK_LOCAL_LOADER | PERF_MASK_LOCAL_XFS | PERF_MASK_LOCAL_IRQ;
constexpr uint16_t PERF_MASK_ALL = PERF_MASK_SAMPLE | PERF_MASK_SWITCH | PERF_MASK_WAKE | PERF_MASK_SLEEP | PERF_MASK_CONTAINER |
                                   PERF_MASK_WKI | PERF_MASK_WKI_LAUNCH | PERF_MASK_LOCAL;

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
    REMOTE_VFS_SERVER = 5,
    REMOTE_IPC = 6,
    LOCAL_PIPE = 7,
    LOCAL_PROC = 8,
    LOCAL_VMEM = 9,
    LOCAL_LOADER = 10,
    LOCAL_XFS = 11,
    LOCAL_IRQ = 12,
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
    STALL = 7,
    RDMA_WRITE = 8,
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
    CHMOD = 17,
};

enum class WkiPerfVfsServerOp : uint8_t {
    RX = 1,
    REPLY_SEND = 2,
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
    CHMOD = 16,
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

enum class WkiPerfIpcOp : uint8_t {
    PROXY_READ = 1,
    PROXY_WRITE = 2,
    PTY_IOCTL = 3,
    SOCK_CTRL = 4,
    PIPE_PUMP_READ = 5,
    PIPE_PUMP_SEND = 6,
    PIPE_DATA = 7,
    DEV_OP_QUEUE = 8,
    DEV_OP_HANDLE = 9,
    WAKE_READER = 10,
    POLL_WAKE = 11,
    EPOLL_CTL = 12,
};

enum class WkiPerfLocalPipeOp : uint8_t {
    READ = 1,
    WRITE = 2,
    BLOCK_READ = 3,
    BLOCK_WRITE = 4,
    WAKE_READERS = 5,
    WAKE_WRITERS = 6,
    DIRECT_RESERVE = 7,
    DIRECT_BEGIN = 8,
    DIRECT_READ = 9,
    DIRECT_COMMIT = 10,
};

enum class WkiPerfLocalProcOp : uint8_t {
    FORK = 1,
    EXECVE = 2,
    ELF_READ = 3,
    FIRST_RUN = 4,
    ARG_COPY = 5,
    OPEN_ACCESS = 6,
    REMOTE_SPAWN = 7,
    NEW_IMAGE = 8,
    LOAD_ELF = 9,
    LOAD_INTERP = 10,
    STACK_SETUP = 11,
    COMMIT = 12,
    DESTROY_OLD = 13,
    WAITPID = 14,
    EXIT = 15,
};

enum class WkiPerfLocalVmemOp : uint8_t {
    ANON_MMAP = 1,
    FILE_MMAP = 2,
    ZERO_PAGE_MAP = 3,
    FILE_CACHE_HIT = 4,
    FILE_CACHE_MISS = 5,
    FILE_CACHE_FILL = 6,
    FILE_CACHE_EVICT = 7,
    COW_ZERO = 8,
    COW_COPY = 9,
    COW_PROMOTE = 10,
};

enum class WkiPerfLocalLoaderOp : uint8_t {
    PT_LOAD_MAIN = 1,
    PT_LOAD_INTERP = 2,
    FINAL_PERMS_MAIN = 3,
    FINAL_PERMS_INTERP = 4,
};

enum class WkiPerfLocalXfsOp : uint8_t {
    READ = 1,
    WRITE = 2,
    READ_BMAP = 3,
    READ_IO = 4,
    WRITE_BMAP = 5,
    WRITE_ALLOC = 6,
    WRITE_IO = 7,
    WRITE_ILOG = 8,
    WRITE_HOLE_ITER = 9,
    WRITE_MAP_ITER = 10,
    DIRECT_READ = 11,
    DIRECT_WRITE = 12,
    BUFFERED_READ = 13,
    BUFFERED_WRITE = 14,
    BUF_READ_HIT = 15,
    BUF_READ_MISS = 16,
    BUF_GET_HIT = 17,
    BUF_GET_MISS = 18,
    BUF_DISK_READ = 19,
    BUF_DISK_WRITE = 20,
    BUF_DIRTY = 21,
    BUF_DISCARD = 22,
    INODE_FETCH = 23,
    INODE_CACHE_HIT = 24,
    INODE_CACHE_MISS = 25,
    INODE_UNAVAILABLE = 26,
    SYNC_BLOCKDEV = 27,
    BUF_FLUSH = 28,
    BUF_ALLOC = 29,
    READ_COPY = 30,
    READ_ZERO = 31,
    READ_GAP = 32,
    METADATA_LOCK_WAIT = 33,
    METADATA_LOCK_HOLD = 34,
    OPEN_CREATE = 35,
    CREATE_LOOKUP = 36,
    IALLOC = 37,
    DIR_ADD = 38,
    OPEN_COMMIT = 39,
    CREATE_TRANS_ALLOC = 40,
    CREATE_INODE_INIT = 41,
    CREATE_PATH_INVALIDATE = 42,
    CREATE_ICACHE = 43,
    LOG_WRITE = 44,
    LOG_BLOCKS = 45,
};

enum class WkiPerfLocalIrqOp : uint8_t {
    HANDLER = 1,
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
    uint64_t latency_samples;
    uint32_t max_latency_us;
    uint32_t p50_us;
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
    uint64_t fastpath_skips;
    uint64_t ring_writes;
};

struct PerfCpuStatsAtomic {
    std::atomic<uint64_t> ctx_switches{0};
    std::atomic<uint64_t> preemptions{0};
    std::atomic<uint64_t> yields{0};
    std::atomic<uint64_t> sleeps{0};
    std::atomic<uint64_t> wakes{0};
    std::atomic<uint64_t> samples{0};
    std::atomic<uint64_t> fastpath_skips{0};
    std::atomic<uint64_t> ring_writes{0};

    void reset() {
        ctx_switches.store(0, std::memory_order_relaxed);
        preemptions.store(0, std::memory_order_relaxed);
        yields.store(0, std::memory_order_relaxed);
        sleeps.store(0, std::memory_order_relaxed);
        wakes.store(0, std::memory_order_relaxed);
        samples.store(0, std::memory_order_relaxed);
        fastpath_skips.store(0, std::memory_order_relaxed);
        ring_writes.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] auto snapshot() const -> PerfCpuStats {
        return {
            .ctx_switches = ctx_switches.load(std::memory_order_relaxed),
            .preemptions = preemptions.load(std::memory_order_relaxed),
            .yields = yields.load(std::memory_order_relaxed),
            .sleeps = sleeps.load(std::memory_order_relaxed),
            .wakes = wakes.load(std::memory_order_relaxed),
            .samples = samples.load(std::memory_order_relaxed),
            .fastpath_skips = fastpath_skips.load(std::memory_order_relaxed),
            .ring_writes = ring_writes.load(std::memory_order_relaxed),
        };
    }
};

// ===========================================================================
// Per-CPU ring buffer  (2 048 events * 48 bytes = 96 KiB per CPU)
// ===========================================================================

constexpr size_t PERF_RING_SHIFT = 11;
constexpr size_t PERF_RING_ENTRIES = 1U << PERF_RING_SHIFT;  // 2048
constexpr size_t PERF_RING_MASK = PERF_RING_ENTRIES - 1;
constexpr size_t PERF_MAX_CPUS = 16;

struct PerfCpuRing {
    PerfCpuStatsAtomic stats;
    uint64_t head;                        // Monotonically-increasing write index
    uint64_t drain;                       // Consumer read index (advances on drain_events)
    ker::mod::sys::Spinlock lock;         // IRQ-safe spinlock protecting head/drain/events
    PerfEvent events[PERF_RING_ENTRIES];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

static_assert(offsetof(PerfCpuRing, stats) == 0, "PerfCpuRing stats must stay first");
static_assert(offsetof(PerfCpuRing, events) > offsetof(PerfCpuRing, lock), "PerfCpuRing event storage offset changed unexpectedly");
static_assert(sizeof(PerfCpuRing) == offsetof(PerfCpuRing, events) + (sizeof(PerfEvent) * PERF_RING_ENTRIES),
              "PerfCpuRing layout must remain a fixed trailing event ring");

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
void record_wake(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t sleep_us, uint64_t callsite,
                 const char* wait_channel);

// Record a task voluntarily entering the wait list.
void record_sleep(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t run_us, uint64_t callsite,
                  const char* wait_channel);

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
void set_event_mask(uint16_t mask);
uint16_t get_event_mask();

bool is_wki_recording_enabled();
bool is_wki_scope_recording_enabled(WkiPerfScope scope, uint8_t op = 0);
bool is_local_xfs_recording_enabled();
bool is_local_irq_recording_enabled();
void register_local_vmem_zero_page(const void* page);
bool is_local_vmem_zero_page(const void* page);

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
void record_local_xfs_summary(WkiPerfLocalXfsOp op, int32_t status, uint32_t latency_us, bool has_latency, uint64_t bytes);
void record_local_irq_summary(WkiPerfLocalIrqOp op, uint16_t vector, uint16_t kind, int32_t status, uint32_t latency_us, bool has_latency);
size_t get_wki_summary_snapshots(WkiPerfSummarySnapshot* dst, size_t max);

// Parse a comma-separated event type string into a mask.
// Recognized names: sample, switch, wake, sleep, container, wki, wki_launch,
// local_pipe, local_proc, local_vmem/vmem, local_loader/loader,
// local_xfs/xfs, local_irq/irq, local, all.
// Returns 0 on parse error.
uint16_t parse_event_mask(const char* str, size_t len);

}  // namespace ker::mod::perf
