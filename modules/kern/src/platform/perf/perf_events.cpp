#include "perf_events.hpp"

#include <atomic>
#include <cstring>
#include <platform/ktime/ktime.hpp>
#include <platform/smt/smt.hpp>

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
    for (size_t i = 0; i < PERF_SUBSYSTEM_COUNT; ++i) {
        g_subsys_stats[i].inserts.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].removes.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].resizes.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].oom_failures.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].peak_count.store(0, std::memory_order_relaxed);
        g_subsys_stats[i].current_count.store(0, std::memory_order_relaxed);
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
        evt.ts_ns = ker::mod::time::getMonotonicNs();
        evt.pid = pid;
        evt.data = rip;
        evt.lag_v = lag_v;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SAMPLE);
        evt.flags = user_mode ? PERF_FLAG_USER_MODE : 0u;
        uint64_t slot = ring.head & PERF_RING_MASK;
        ring.events[slot] = evt;
        ring.head++;
        if ((ring.head - ring.drain) > PERF_RING_ENTRIES) ring.drain = ring.head - PERF_RING_ENTRIES;
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
        evt.ts_ns = ker::mod::time::getMonotonicNs();
        evt.pid = prev_pid;
        evt.data = next_pid;
        evt.callsite = callsite;
        evt.lag_v = lag_v;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SWITCH);
        evt.flags = flags;
        evt.aux = run_us;
        uint64_t slot = ring.head & PERF_RING_MASK;
        ring.events[slot] = evt;
        ring.head++;
        if ((ring.head - ring.drain) > PERF_RING_ENTRIES) ring.drain = ring.head - PERF_RING_ENTRIES;
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
        evt.ts_ns = ker::mod::time::getMonotonicNs();
        evt.pid = pid;
        evt.data = wake_at_us;
        evt.callsite = callsite;
        evt.lag_v = 0;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::WAKE);
        evt.flags = flags;
        evt.aux = sleep_us;
        uint64_t slot = ring.head & PERF_RING_MASK;
        ring.events[slot] = evt;
        ring.head++;
        if ((ring.head - ring.drain) > PERF_RING_ENTRIES) ring.drain = ring.head - PERF_RING_ENTRIES;
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
        evt.ts_ns = ker::mod::time::getMonotonicNs();
        evt.pid = pid;
        evt.data = wake_at_us;
        evt.callsite = callsite;
        evt.lag_v = 0;
        evt.cpu = static_cast<uint16_t>(cpu);
        evt.type = static_cast<uint8_t>(PerfEventType::SLEEP);
        evt.flags = flags;
        evt.aux = run_us;
        uint64_t slot = ring.head & PERF_RING_MASK;
        ring.events[slot] = evt;
        ring.head++;
        if ((ring.head - ring.drain) > PERF_RING_ENTRIES) ring.drain = ring.head - PERF_RING_ENTRIES;
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
    evt.ts_ns = ker::mod::time::getMonotonicNs();
    evt.pid = pid;
    // Pack subsystem (upper 32b) + instance_id (lower 32b) into data
    evt.data = (static_cast<uint64_t>(static_cast<uint8_t>(subsystem)) << 32) | static_cast<uint64_t>(instance_id);
    evt.callsite = callsite;
    evt.lag_v = element_count;  // reuse: current element count
    evt.cpu = static_cast<uint16_t>(cpu);
    evt.type = static_cast<uint8_t>(PerfEventType::CONTAINER_STAT);
    evt.flags = flags;
    evt.aux = capacity;

    uint64_t slot = ring.head & PERF_RING_MASK;
    ring.events[slot] = evt;
    ring.head++;
    if ((ring.head - ring.drain) > PERF_RING_ENTRIES) ring.drain = ring.head - PERF_RING_ENTRIES;

    ring.lock.unlock_irqrestore(saved);
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
// parse_event_mask - parse "switch,wake,container" into bitmask
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
        else if (tok_len == 3 && memcmp(str + start, "all", 3) == 0)
            mask = PERF_MASK_ALL;
        else
            return 0;  // unknown token
    }
    return mask;
}

}  // namespace ker::mod::perf
