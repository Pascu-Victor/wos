#include "perf_events.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <platform/ktime/ktime.hpp>
#include <platform/smt/smt.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::mod::perf {

// Recording is OFF by default - enabled explicitly via perf record / /proc/kperfctl.
// Stats counters (ctx_switches etc.) always increment regardless of this flag so
// /proc/kcpustat is always live and has zero overhead impact on the hot path.
static std::atomic<bool> g_enabled{false};

// Event type mask: bitmask of PerfEventType values to record.
// Default: all types enabled (0x1F). Only checked when g_enabled is true.
static std::atomic<uint8_t> g_event_mask{PERF_MASK_ALL};

// Static ring buffers — dynamically allocated at init() time based on actual CPU count.
static PerfCpuRing* g_rings = nullptr;
static size_t g_num_cpus = 0;

// Sub-sampler: emit one SAMPLE per 10 timer ticks (~100 Hz at 1 kHz tick rate)
static uint64_t* g_tick_count = nullptr;

// Per-subsystem aggregate statistics (always-on, lock-free via atomics).
// Total: ~13 subsystems * 48 bytes = ~624 bytes BSS.
static PerfSubsystemStats g_subsys_stats[PERF_SUBSYSTEM_COUNT];
static std::atomic<uint32_t> g_wki_trace_correlation{1};

struct WkiPerfSummaryBucket {
    bool used = false;
    uint8_t scope = 0;
    uint8_t op = 0;
    uint16_t peer = 0;
    uint16_t channel = 0;
    uint64_t calls = 0;
    uint64_t errors = 0;
    uint64_t retries = 0;
    uint64_t bytes = 0;
    uint64_t latency_samples = 0;
    uint64_t total_latency_us = 0;
    uint32_t max_latency_us = 0;
    std::array<uint32_t, WKI_PERF_HIST_BUCKETS> latency_hist = {};
};

static WkiPerfSummaryBucket g_wki_summary[WKI_PERF_SUMMARY_BUCKETS];
static ker::mod::sys::Spinlock g_wki_summary_lock;

namespace {

void perf_push_event(PerfCpuRing& ring, const PerfEvent& evt) {
    uint64_t slot = ring.head & PERF_RING_MASK;
    ring.events[slot] = evt;
    ring.head++;
    if ((ring.head - ring.drain) > PERF_RING_ENTRIES) {
        ring.drain = ring.head - PERF_RING_ENTRIES;
    }
}

auto wki_scope_hash(uint8_t scope, uint8_t op, uint16_t peer, uint16_t channel) -> size_t {
    uint32_t mixed = static_cast<uint32_t>(scope);
    mixed = (mixed * 131U) ^ static_cast<uint32_t>(op);
    mixed = (mixed * 65537U) ^ static_cast<uint32_t>(peer);
    mixed = (mixed * 65537U) ^ static_cast<uint32_t>(channel);
    return static_cast<size_t>(mixed % WKI_PERF_SUMMARY_BUCKETS);
}

auto wki_get_or_create_summary_bucket(uint8_t scope, uint8_t op, uint16_t peer, uint16_t channel) -> WkiPerfSummaryBucket* {
    size_t slot = wki_scope_hash(scope, op, peer, channel);
    for (size_t probe = 0; probe < WKI_PERF_SUMMARY_BUCKETS; ++probe) {
        auto& bucket = g_wki_summary[(slot + probe) % WKI_PERF_SUMMARY_BUCKETS];
        if (!bucket.used) {
            bucket.used = true;
            bucket.scope = scope;
            bucket.op = op;
            bucket.peer = peer;
            bucket.channel = channel;
            bucket.calls = 0;
            bucket.errors = 0;
            bucket.retries = 0;
            bucket.bytes = 0;
            bucket.latency_samples = 0;
            bucket.total_latency_us = 0;
            bucket.max_latency_us = 0;
            bucket.latency_hist.fill(0);
            return &bucket;
        }
        if (bucket.scope == scope && bucket.op == op && bucket.peer == peer && bucket.channel == channel) {
            return &bucket;
        }
    }
    return nullptr;
}

auto wki_hist_bucket(uint32_t latency_us) -> size_t {
    if (latency_us <= 1) {
        return 0;
    }

    size_t bucket = 0;
    uint32_t value = latency_us;
    while (value > 1 && bucket + 1 < WKI_PERF_HIST_BUCKETS) {
        value >>= 1U;
        bucket++;
    }
    return bucket;
}

auto wki_hist_bucket_value(size_t bucket) -> uint32_t {
    if (bucket == 0) {
        return 1;
    }
    if (bucket >= 31) {
        return 1U << 31U;
    }
    return 1U << static_cast<uint32_t>(bucket);
}

auto wki_hist_percentile(const std::array<uint32_t, WKI_PERF_HIST_BUCKETS>& hist, uint64_t total, uint32_t numer, uint32_t denom)
    -> uint32_t {
    if (total == 0 || denom == 0) {
        return 0;
    }

    uint64_t threshold = ((total * static_cast<uint64_t>(numer)) + (denom - 1U)) / static_cast<uint64_t>(denom);
    if (threshold == 0) {
        threshold = 1;
    }

    uint64_t cumulative = 0;
    for (size_t i = 0; i < hist.size(); ++i) {
        cumulative += hist[i];
        if (cumulative >= threshold) {
            return wki_hist_bucket_value(i);
        }
    }

    return wki_hist_bucket_value(hist.size() - 1);
}

auto wki_phase_flags(WkiPerfPhase phase) -> uint8_t {
    switch (phase) {
        case WkiPerfPhase::BEGIN:
            return PERF_FLAG_WKI_BEGIN;
        case WkiPerfPhase::END:
            return PERF_FLAG_WKI_END;
        case WkiPerfPhase::POINT:
        default:
            return PERF_FLAG_WKI_POINT;
    }
}

auto wki_is_launch_measurement_event(WkiPerfScope scope, uint8_t op) -> bool {
    if (scope != WkiPerfScope::REMOTE_COMPUTE) {
        return false;
    }

    switch (static_cast<WkiPerfComputeOp>(op)) {
        case WkiPerfComputeOp::SUBMIT_INLINE:
        case WkiPerfComputeOp::SUBMIT_VFS_REF:
        case WkiPerfComputeOp::COMPLETE_WAIT:
        case WkiPerfComputeOp::ACCEPT:
        case WkiPerfComputeOp::REJECT:
        case WkiPerfComputeOp::COMPLETE:
        case WkiPerfComputeOp::PROXY_READY:
        case WkiPerfComputeOp::DEFER_WAIT:
        case WkiPerfComputeOp::LOAD_ELF:
        case WkiPerfComputeOp::HANDLE_SUBMIT:
        case WkiPerfComputeOp::TASK_RUNTIME:
        case WkiPerfComputeOp::PROXY_READY_WAIT:
        case WkiPerfComputeOp::COMPLETE_HOLD:
            return true;
        default:
            return false;
    }
}

auto wki_should_record(uint8_t mask, WkiPerfScope scope, uint8_t op) -> bool {
    if ((mask & PERF_MASK_WKI) != 0U) {
        return true;
    }

    return ((mask & PERF_MASK_WKI_LAUNCH) != 0U) && wki_is_launch_measurement_event(scope, op);
}

}  // namespace

const char* subsystem_name(PerfSubsystem s) {
    switch (s) {
        case PerfSubsystem::FD_TABLE:
            return "fd_table";
        case PerfSubsystem::FUTEX:
            return "futex";
        case PerfSubsystem::DEVICE_REG:
            return "device_reg";
        case PerfSubsystem::BLOCK_DEV:
            return "block_dev";
        case PerfSubsystem::PTY_POOL:
            return "pty_pool";
        case PerfSubsystem::MOUNT_TABLE:
            return "mount_table";
        case PerfSubsystem::PIPE_WAITQ:
            return "pipe_waitq";
        case PerfSubsystem::ACCEPT_Q:
            return "accept_q";
        case PerfSubsystem::NAPI_REG:
            return "napi_reg";
        case PerfSubsystem::VFS_RULES:
            return "vfs_rules";
        case PerfSubsystem::EXIT_WAITERS:
            return "exit_waiters";
        case PerfSubsystem::PTY_WAITERS:
            return "pty_waiters";
        default:
            return "unknown";
    }
}

const char* wki_scope_name(WkiPerfScope scope) {
    switch (scope) {
        case WkiPerfScope::TRANSPORT:
            return "transport";
        case WkiPerfScope::REMOTE_VFS:
            return "remote_vfs";
        case WkiPerfScope::REMOTE_COMPUTE:
            return "remote_compute";
        case WkiPerfScope::EVENT_BUS:
            return "event_bus";
        default:
            return "unknown";
    }
}

const char* wki_phase_name(WkiPerfPhase phase) {
    switch (phase) {
        case WkiPerfPhase::BEGIN:
            return "begin";
        case WkiPerfPhase::END:
            return "end";
        case WkiPerfPhase::POINT:
            return "point";
        default:
            return "unknown";
    }
}

const char* wki_op_name(WkiPerfScope scope, uint8_t op) {
    switch (scope) {
        case WkiPerfScope::TRANSPORT:
            switch (static_cast<WkiPerfTransportOp>(op)) {
                case WkiPerfTransportOp::SEND:
                    return "send";
                case WkiPerfTransportOp::ACK_RTT:
                    return "ack_rtt";
                case WkiPerfTransportOp::RETRANSMIT:
                    return "retransmit";
                case WkiPerfTransportOp::FAST_RETRANSMIT:
                    return "fast_retransmit";
                case WkiPerfTransportOp::NO_CREDITS:
                    return "no_credits";
                case WkiPerfTransportOp::WAIT:
                    return "wait";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_VFS:
            switch (static_cast<WkiPerfVfsOp>(op)) {
                case WkiPerfVfsOp::ATTACH_WAIT:
                    return "attach_wait";
                case WkiPerfVfsOp::PROXY_WAIT:
                    return "proxy_wait";
                case WkiPerfVfsOp::OPEN:
                    return "open";
                case WkiPerfVfsOp::STAT:
                    return "stat";
                case WkiPerfVfsOp::READ:
                    return "read";
                case WkiPerfVfsOp::READDIR:
                    return "readdir";
                case WkiPerfVfsOp::WRITE:
                    return "write";
                case WkiPerfVfsOp::SEEK:
                    return "seek";
                case WkiPerfVfsOp::TRUNCATE:
                    return "truncate";
                case WkiPerfVfsOp::READLINK:
                    return "readlink";
                case WkiPerfVfsOp::CLOSE:
                    return "close";
                case WkiPerfVfsOp::MKDIR:
                    return "mkdir";
                case WkiPerfVfsOp::UNLINK:
                    return "unlink";
                case WkiPerfVfsOp::RMDIR:
                    return "rmdir";
                case WkiPerfVfsOp::RENAME:
                    return "rename";
                case WkiPerfVfsOp::RETRY:
                    return "retry";
                default:
                    return "unknown";
            }
        case WkiPerfScope::REMOTE_COMPUTE:
            switch (static_cast<WkiPerfComputeOp>(op)) {
                case WkiPerfComputeOp::SUBMIT_INLINE:
                    return "submit_inline";
                case WkiPerfComputeOp::SUBMIT_VFS_REF:
                    return "submit_vfs_ref";
                case WkiPerfComputeOp::COMPLETE_WAIT:
                    return "complete_wait";
                case WkiPerfComputeOp::ACCEPT:
                    return "accept";
                case WkiPerfComputeOp::REJECT:
                    return "reject";
                case WkiPerfComputeOp::COMPLETE:
                    return "complete";
                case WkiPerfComputeOp::PROXY_READY:
                    return "proxy_ready";
                case WkiPerfComputeOp::DEFER_WAIT:
                    return "defer_wait";
                case WkiPerfComputeOp::LOAD_ELF:
                    return "load_elf";
                case WkiPerfComputeOp::HANDLE_SUBMIT:
                    return "handle_submit";
                case WkiPerfComputeOp::TASK_RUNTIME:
                    return "task_runtime";
                case WkiPerfComputeOp::PROXY_READY_WAIT:
                    return "proxy_ready_wait";
                case WkiPerfComputeOp::COMPLETE_HOLD:
                    return "complete_hold";
                default:
                    return "unknown";
            }
        case WkiPerfScope::EVENT_BUS:
            switch (static_cast<WkiPerfEventOp>(op)) {
                case WkiPerfEventOp::SUBSCRIBE:
                    return "subscribe";
                case WkiPerfEventOp::UNSUBSCRIBE:
                    return "unsubscribe";
                case WkiPerfEventOp::PUBLISH:
                    return "publish";
                case WkiPerfEventOp::ACK:
                    return "ack";
                case WkiPerfEventOp::RETRY:
                    return "retry";
                case WkiPerfEventOp::REPLAY:
                    return "replay";
                default:
                    return "unknown";
            }
        default:
            return "unknown";
    }
}

void init() {
    g_num_cpus = ker::mod::smt::get_core_count();
    if (g_num_cpus == 0) g_num_cpus = 1;
    g_rings = new PerfCpuRing[g_num_cpus]{};
    g_tick_count = new uint64_t[g_num_cpus]{};
    for (size_t i = 0; i < g_num_cpus; ++i) {
        g_rings[i].head = 0;
        g_rings[i].drain = 0;
        g_tick_count[i] = 0;
        memset(&g_rings[i].stats, 0, sizeof(PerfCpuStats));
    }
    g_enabled.store(false, std::memory_order_release);  // Off by default; enabled via perf record
    g_event_mask.store(PERF_MASK_ALL, std::memory_order_release);
    g_wki_trace_correlation.store(1, std::memory_order_release);
    for (size_t i = 0; i < PERF_SUBSYSTEM_COUNT; ++i) {
        g_subsys_stats[i].inserts.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].removes.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].resizes.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].oom_failures.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].peak_count.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].current_count.store(0, std::memory_order_relaxed);
    }
    for (auto& bucket : g_wki_summary) {
        bucket.used = false;
        bucket.scope = 0;
        bucket.op = 0;
        bucket.peer = 0;
        bucket.channel = 0;
        bucket.calls = 0;
        bucket.errors = 0;
        bucket.retries = 0;
        bucket.bytes = 0;
        bucket.latency_samples = 0;
        bucket.total_latency_us = 0;
        bucket.max_latency_us = 0;
        bucket.latency_hist.fill(0);
    }
}

bool is_enabled() { return g_enabled.load(std::memory_order_acquire); }
void enable() { g_enabled.store(true, std::memory_order_release); }
void disable() { g_enabled.store(false, std::memory_order_release); }

size_t get_num_perf_cpus() { return g_num_cpus; }

void reset_rings() {
    // IRQ-safe reset of all ring head/drain pointers.
    // Stats counters are NOT cleared - they accumulate across sessions for /proc/kcpustat.
    for (size_t i = 0; i < g_num_cpus; ++i) {
        auto& ring = g_rings[i];
        auto saved = ring.lock.lock_irqsave();
        ring.head = 0;
        ring.drain = 0;
        ring.lock.unlock_irqrestore(saved);
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    for (auto& bucket : g_wki_summary) {
        bucket.used = false;
        bucket.scope = 0;
        bucket.op = 0;
        bucket.peer = 0;
        bucket.channel = 0;
        bucket.calls = 0;
        bucket.errors = 0;
        bucket.retries = 0;
        bucket.bytes = 0;
        bucket.latency_samples = 0;
        bucket.total_latency_us = 0;
        bucket.max_latency_us = 0;
        bucket.latency_hist.fill(0);
    }
    g_wki_trace_correlation.store(1, std::memory_order_release);
    g_wki_summary_lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_sample
// ---------------------------------------------------------------------------
void record_sample(uint32_t cpu, uint64_t pid, uint64_t rip, bool user_mode, int64_t lag_v) {
    if (cpu >= g_num_cpus) return;

    // Sub-sample to ~100 Hz (tick count advances even when recording is off
    // so the phase is stable when recording is enabled mid-run).
    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool do_record = g_enabled.load(std::memory_order_acquire) && (mask & PERF_MASK_SAMPLE) && ((++g_tick_count[cpu] % 10) == 0);

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    ring.stats.samples++;  // always counted
    if (do_record) {
        PerfEvent evt{};
        evt.ts_ns = ker::mod::time::get_monotonic_ns();
        evt.pid = pid;
        evt.data = rip;
        evt.lag_v = lag_v;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SAMPLE);
        evt.flags = user_mode ? PERF_FLAG_USER_MODE : 0u;
        perf_push_event(ring, evt);
    }
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_switch
// ---------------------------------------------------------------------------
void record_switch(uint32_t cpu, uint64_t prev_pid, uint64_t next_pid, uint8_t flags, int64_t lag_v, uint32_t run_us, uint64_t callsite) {
    if (cpu >= g_num_cpus) return;

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool do_record = g_enabled.load(std::memory_order_acquire) && (mask & PERF_MASK_SWITCH);

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    auto& s = ring.stats;
    s.ctx_switches++;  // always counted
    if (flags & PERF_FLAG_PREEMPT) s.preemptions++;
    if (flags & PERF_FLAG_YIELD) s.yields++;
    if (flags & PERF_FLAG_BLOCK) s.sleeps++;
    if (do_record) {
        PerfEvent evt{};
        evt.ts_ns = ker::mod::time::get_monotonic_ns();
        evt.pid = prev_pid;
        evt.data = next_pid;
        evt.callsite = callsite;
        evt.lag_v = lag_v;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SWITCH);
        evt.flags = flags;
        evt.aux = run_us;
        perf_push_event(ring, evt);
    }
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_wake
// ---------------------------------------------------------------------------
void record_wake(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t sleep_us, uint64_t callsite) {
    if (cpu >= g_num_cpus) return;

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool do_record = g_enabled.load(std::memory_order_acquire) && (mask & PERF_MASK_WAKE);

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    ring.stats.wakes++;  // always counted
    if (do_record) {
        PerfEvent evt{};
        evt.ts_ns = ker::mod::time::get_monotonic_ns();
        evt.pid = pid;
        evt.data = wake_at_us;
        evt.callsite = callsite;
        evt.lag_v = 0;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::WAKE);
        evt.flags = flags;
        evt.aux = sleep_us;
        perf_push_event(ring, evt);
    }
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// record_sleep
// ---------------------------------------------------------------------------
void record_sleep(uint32_t cpu, uint64_t pid, uint64_t wake_at_us, uint8_t flags, uint32_t run_us, uint64_t callsite) {
    if (cpu >= g_num_cpus) return;

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool do_record = g_enabled.load(std::memory_order_acquire) && (mask & PERF_MASK_SLEEP);

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    ring.stats.sleeps++;  // always counted
    if (do_record) {
        PerfEvent evt{};
        evt.ts_ns = ker::mod::time::get_monotonic_ns();
        evt.pid = pid;
        evt.data = wake_at_us;
        evt.callsite = callsite;
        evt.lag_v = 0;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SLEEP);
        evt.flags = flags;
        evt.aux = run_us;
        perf_push_event(ring, evt);
    }
    ring.lock.unlock_irqrestore(saved);
}

// ---------------------------------------------------------------------------
// drain_events - called from procfs read (process context, may sleep)
// ---------------------------------------------------------------------------
size_t drain_events(PerfEvent* dst, size_t max_events, uint32_t cpu_filter) {
    size_t total = 0;
    for (uint32_t c = 0; c < g_num_cpus && total < max_events; ++c) {
        if (cpu_filter < g_num_cpus && c != cpu_filter) continue;

        auto& ring = g_rings[c];
        auto saved = ring.lock.lock_irqsave();
        while (ring.drain < ring.head && total < max_events) {
            dst[total++] = ring.events[ring.drain & PERF_RING_MASK];
            ring.drain++;
        }
        ring.lock.unlock_irqrestore(saved);
    }
    return total;
}

// ---------------------------------------------------------------------------
// get_cpu_stats - non-destructive snapshot
// ---------------------------------------------------------------------------
PerfCpuStats get_cpu_stats(uint32_t cpu) {
    if (cpu >= g_num_cpus) return {};
    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    auto s = ring.stats;
    ring.lock.unlock_irqrestore(saved);
    return s;
}

// ---------------------------------------------------------------------------
// record_container_stat
// ---------------------------------------------------------------------------
void record_container_stat(uint32_t cpu, uint64_t pid, PerfSubsystem subsystem, uint32_t instance_id, uint8_t flags, int64_t element_count,
                           uint32_t capacity, uint64_t callsite) {
    // Always update subsystem atomic stats
    update_subsystem_stat(subsystem, flags, static_cast<uint64_t>(element_count >= 0 ? element_count : 0));

    if (cpu >= g_num_cpus) return;

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    bool do_record = g_enabled.load(std::memory_order_acquire) && (mask & PERF_MASK_CONTAINER);

    if (!do_record) return;

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();

    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    // Pack subsystem (upper 32b) + instance_id (lower 32b) into data
    evt.data = (static_cast<uint64_t>(static_cast<uint8_t>(subsystem)) << 32) | static_cast<uint64_t>(instance_id);
    evt.callsite = callsite;
    evt.lag_v = element_count;  // reuse: current element count
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::CONTAINER_STAT);
    evt.flags = flags;
    evt.aux = capacity;

    perf_push_event(ring, evt);

    ring.lock.unlock_irqrestore(saved);
}

bool is_wki_recording_enabled() {
    if (!g_enabled.load(std::memory_order_acquire)) {
        return false;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    return (mask & (PERF_MASK_WKI | PERF_MASK_WKI_LAUNCH)) != 0U;
}

uint64_t wki_pack_event_data(WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel) {
    return static_cast<uint64_t>(static_cast<uint8_t>(scope)) | (static_cast<uint64_t>(op) << 8U) |
           (static_cast<uint64_t>(static_cast<uint8_t>(phase)) << 16U) | (static_cast<uint64_t>(peer) << 24U) |
           (static_cast<uint64_t>(channel) << 40U);
}

void wki_unpack_event_data(uint64_t data, WkiPerfScope& scope, uint8_t& op, WkiPerfPhase& phase, uint16_t& peer, uint16_t& channel) {
    scope = static_cast<WkiPerfScope>(data & 0xFFU);
    op = static_cast<uint8_t>((data >> 8U) & 0xFFU);
    phase = static_cast<WkiPerfPhase>((data >> 16U) & 0xFFU);
    peer = static_cast<uint16_t>((data >> 24U) & 0xFFFFU);
    channel = static_cast<uint16_t>((data >> 40U) & 0xFFFFU);
}

uint64_t wki_pack_trace_state(uint32_t correlation, int32_t status) {
    return (static_cast<uint64_t>(correlation) << 32U) | static_cast<uint32_t>(status);
}

uint32_t wki_unpack_trace_correlation(int64_t packed) { return static_cast<uint32_t>(static_cast<uint64_t>(packed) >> 32U); }

int32_t wki_unpack_trace_status(int64_t packed) { return static_cast<int32_t>(static_cast<uint32_t>(packed)); }

uint32_t next_wki_trace_correlation() { return g_wki_trace_correlation.fetch_add(1, std::memory_order_relaxed); }

void record_wki_event(uint32_t cpu, uint64_t pid, WkiPerfScope scope, uint8_t op, WkiPerfPhase phase, uint16_t peer, uint16_t channel,
                      uint32_t correlation, int32_t status, uint32_t aux, uint64_t callsite) {
    if (cpu >= g_num_cpus || !g_enabled.load(std::memory_order_acquire)) {
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if (!wki_should_record(mask, scope, op)) {
        return;
    }

    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();

    PerfEvent evt{};
    evt.ts_ns = ker::mod::time::get_monotonic_ns();
    evt.pid = pid;
    evt.data = wki_pack_event_data(scope, op, phase, peer, channel);
    evt.callsite = callsite;
    evt.lag_v = static_cast<int64_t>(wki_pack_trace_state(correlation, status));
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::WKI);
    evt.flags = wki_phase_flags(phase);
    evt.aux = aux;
    perf_push_event(ring, evt);

    ring.lock.unlock_irqrestore(saved);
}

void record_wki_summary(WkiPerfScope scope, uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us,
                        bool has_latency, uint32_t retries, uint64_t bytes) {
    if (!g_enabled.load(std::memory_order_acquire)) {
        return;
    }

    auto mask = g_event_mask.load(std::memory_order_relaxed);
    if (!wki_should_record(mask, scope, op)) {
        return;
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    WkiPerfSummaryBucket* bucket = wki_get_or_create_summary_bucket(static_cast<uint8_t>(scope), op, peer, channel);
    if (bucket != nullptr) {
        bucket->calls++;
        if (status < 0) {
            bucket->errors++;
        }
        bucket->retries += retries;
        bucket->bytes += bytes;
        if (has_latency) {
            bucket->latency_samples++;
            bucket->total_latency_us += latency_us;
            bucket->max_latency_us = std::max(bucket->max_latency_us, latency_us);
            bucket->latency_hist[wki_hist_bucket(latency_us)]++;
        }
    }
    g_wki_summary_lock.unlock_irqrestore(saved);
}

size_t get_wki_summary_snapshots(WkiPerfSummarySnapshot* dst, size_t max) {
    if (dst == nullptr || max == 0) {
        return 0;
    }

    auto saved = g_wki_summary_lock.lock_irqsave();
    size_t total = 0;
    for (const auto& bucket : g_wki_summary) {
        if (!bucket.used || total >= max) {
            continue;
        }

        dst[total++] = WkiPerfSummarySnapshot{
            .scope = bucket.scope,
            .op = bucket.op,
            .peer = bucket.peer,
            .channel = bucket.channel,
            .reserved = 0,
            .calls = bucket.calls,
            .errors = bucket.errors,
            .retries = bucket.retries,
            .bytes = bucket.bytes,
            .total_latency_us = bucket.total_latency_us,
            .max_latency_us = bucket.max_latency_us,
            .p95_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 95, 100),
            .p99_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 99, 100),
            .p999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 999, 1000),
            .p9999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 9999, 10000),
            .p99999_us = wki_hist_percentile(bucket.latency_hist, bucket.latency_samples, 99999, 100000),
        };
    }
    g_wki_summary_lock.unlock_irqrestore(saved);
    return total;
}

// ---------------------------------------------------------------------------
// update_subsystem_stat - always-on atomic counters
// ---------------------------------------------------------------------------
void update_subsystem_stat(PerfSubsystem subsystem, uint8_t flags, uint64_t current_count) {
    auto idx = static_cast<size_t>(subsystem);
    if (idx == 0 || idx >= PERF_SUBSYSTEM_COUNT) return;

    auto& s = g_subsys_stats[idx];

    if (flags & PERF_FLAG_CT_INSERT) s.inserts.fetch_add(1, std::memory_order_relaxed);
    if (flags & PERF_FLAG_CT_REMOVE) s.removes.fetch_add(1, std::memory_order_relaxed);
    if (flags & PERF_FLAG_CT_RESIZE) s.resizes.fetch_add(1, std::memory_order_relaxed);
    if (flags & PERF_FLAG_CT_OOM) s.oom_failures.fetch_add(1, std::memory_order_relaxed);

    s.current_count.store(current_count, std::memory_order_relaxed);

    // Update peak (lock-free CAS loop)
    uint64_t old_peak = s.peak_count.load(std::memory_order_relaxed);
    while (current_count > old_peak) {
        if (s.peak_count.compare_exchange_weak(old_peak, current_count, std::memory_order_relaxed)) break;
    }
}

// ---------------------------------------------------------------------------
// get_subsystem_stats - non-destructive snapshot
// ---------------------------------------------------------------------------
PerfSubsystemSnapshot get_subsystem_stats(PerfSubsystem subsystem) {
    auto idx = static_cast<size_t>(subsystem);
    if (idx == 0 || idx >= PERF_SUBSYSTEM_COUNT) return {};

    auto& s = g_subsys_stats[idx];
    return PerfSubsystemSnapshot{
        .inserts = s.inserts.load(std::memory_order_relaxed),
        .removes = s.removes.load(std::memory_order_relaxed),
        .resizes = s.resizes.load(std::memory_order_relaxed),
        .oom_failures = s.oom_failures.load(std::memory_order_relaxed),
        .peak_count = s.peak_count.load(std::memory_order_relaxed),
        .current_count = s.current_count.load(std::memory_order_relaxed),
    };
}

// ---------------------------------------------------------------------------
// Event mask control
// ---------------------------------------------------------------------------
void set_event_mask(uint8_t mask) { g_event_mask.store(mask, std::memory_order_release); }

uint8_t get_event_mask() { return g_event_mask.load(std::memory_order_acquire); }

// ---------------------------------------------------------------------------
// parse_event_mask - parse "switch,wake,container,wki,wki_launch" into bitmask
// ---------------------------------------------------------------------------
uint8_t parse_event_mask(const char* str, size_t len) {
    uint8_t mask = 0;
    size_t i = 0;

    while (i < len) {
        // Skip whitespace and commas
        while (i < len && (str[i] == ',' || str[i] == ' ' || str[i] == '\n')) i++;
        if (i >= len) break;

        // Find end of token
        size_t start = i;
        while (i < len && str[i] != ',' && str[i] != ' ' && str[i] != '\n') i++;
        size_t tok_len = i - start;

        if (tok_len == 6 && memcmp(str + start, "sample", 6) == 0)
            mask |= PERF_MASK_SAMPLE;
        else if (tok_len == 6 && memcmp(str + start, "switch", 6) == 0)
            mask |= PERF_MASK_SWITCH;
        else if (tok_len == 4 && memcmp(str + start, "wake", 4) == 0)
            mask |= PERF_MASK_WAKE;
        else if (tok_len == 5 && memcmp(str + start, "sleep", 5) == 0)
            mask |= PERF_MASK_SLEEP;
        else if (tok_len == 9 && memcmp(str + start, "container", 9) == 0)
            mask |= PERF_MASK_CONTAINER;
        else if (tok_len == 3 && memcmp(str + start, "wki", 3) == 0)
            mask |= PERF_MASK_WKI;
        else if (tok_len == 10 && memcmp(str + start, "wki_launch", 10) == 0)
            mask |= PERF_MASK_WKI_LAUNCH;
        else if (tok_len == 3 && memcmp(str + start, "all", 3) == 0)
            mask = PERF_MASK_ALL;
        else
            return 0;  // unknown token
    }
    return mask;
}

}  // namespace ker::mod::perf
