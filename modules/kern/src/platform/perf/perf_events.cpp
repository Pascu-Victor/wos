#include "perf_events.hpp"

#include <atomic>
#include <cstring>
#include <platform/ktime/ktime.hpp>

namespace ker::mod::perf {

// Recording is OFF by default — enabled explicitly via perf record / /proc/kperfctl.
// Stats counters (ctx_switches etc.) always increment regardless of this flag so
// /proc/kcpustat is always live and has zero overhead impact on the hot path.
static std::atomic<bool> g_enabled{false};

// Static ring buffers — no kernel allocator needed.
// Total size: 16 × (sizeof(PerfCpuRing)) ≈ 16 × 97 KiB ≈ 1.5 MiB in BSS.
static PerfCpuRing g_rings[PERF_MAX_CPUS];

// Sub-sampler: emit one SAMPLE per 10 timer ticks (~100 Hz at 1 kHz tick rate)
static uint64_t g_tick_count[PERF_MAX_CPUS];

void init() {
    for (size_t i = 0; i < PERF_MAX_CPUS; ++i) {
        g_rings[i].head = 0;
        g_rings[i].drain = 0;
        g_tick_count[i] = 0;
        memset(&g_rings[i].stats, 0, sizeof(PerfCpuStats));
    }
    g_enabled.store(false, std::memory_order_release);  // Off by default; enabled via perf record
}

bool is_enabled() { return g_enabled.load(std::memory_order_acquire); }
void enable() { g_enabled.store(true, std::memory_order_release); }
void disable() { g_enabled.store(false, std::memory_order_release); }

void reset_rings() {
    // IRQ-safe reset of all ring head/drain pointers.
    // Stats counters are NOT cleared — they accumulate across sessions for /proc/kcpustat.
    for (size_t i = 0; i < PERF_MAX_CPUS; ++i) {
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
    if (cpu >= PERF_MAX_CPUS) return;

    // Sub-sample to ~100 Hz (tick count advances even when recording is off
    // so the phase is stable when recording is enabled mid-run).
    bool do_record = g_enabled.load(std::memory_order_acquire) && ((++g_tick_count[cpu] % 10) == 0);

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
    if (cpu >= PERF_MAX_CPUS) return;

    bool do_record = g_enabled.load(std::memory_order_acquire);

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
    if (cpu >= PERF_MAX_CPUS) return;

    bool do_record = g_enabled.load(std::memory_order_acquire);

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
    if (cpu >= PERF_MAX_CPUS) return;

    bool do_record = g_enabled.load(std::memory_order_acquire);

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
// drain_events — called from procfs read (process context, may sleep)
// ---------------------------------------------------------------------------
size_t drain_events(PerfEvent* dst, size_t max_events, uint32_t cpu_filter) {
    size_t total = 0;
    for (uint32_t c = 0; c < PERF_MAX_CPUS && total < max_events; ++c) {
        if (cpu_filter < PERF_MAX_CPUS && c != cpu_filter) continue;

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
// get_cpu_stats — non-destructive snapshot
// ---------------------------------------------------------------------------
PerfCpuStats get_cpu_stats(uint32_t cpu) {
    if (cpu >= PERF_MAX_CPUS) return {};
    auto& ring = g_rings[cpu];
    auto saved = ring.lock.lock_irqsave();
    auto s = ring.stats;
    ring.lock.unlock_irqrestore(saved);
    return s;
}

}  // namespace ker::mod::perf
