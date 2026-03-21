#pragma once

// Lightweight per-span cycle-count instrumentation for the network hot path.
// Enable with -DNET_TRACE.  Accumulates min/avg/max every DUMP_INTERVAL
// packets; actual serial output is deferred to net_trace_flush() which is
// called from the TCP timer thread — never from the RX/TX hot path.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::trace {

#ifdef NET_TRACE

constexpr uint32_t DUMP_INTERVAL = 1000;

struct Span {
    const char* name;
    uint64_t total_cycles = 0;
    uint64_t min_cycles = ~0ULL;
    uint64_t max_cycles = 0;
    uint32_t count = 0;

    void record(uint64_t cycles) {
        total_cycles += cycles;
        min_cycles = std::min(cycles, min_cycles);
        max_cycles = std::max(cycles, max_cycles);
        count++;
    }
};

// Global spans — index with SpanId
enum SpanId : uint32_t {
    SPAN_NAPI_POLL = 0,
    SPAN_TX_RECLAIM,
    SPAN_RX_BUDGET,
    SPAN_NETDEV_RX,
    SPAN_ETH_RX,
    SPAN_TCP_FIND_CB,
    SPAN_TCP_PROCESS,
    SPAN_START_XMIT,
    SPAN_COUNT
};

inline Span spans[SPAN_COUNT] = {
    {.name = "napi_poll"}, {.name = "tx_reclaim"},  {.name = "rx_budget"},   {.name = "netdev_rx"},
    {.name = "eth_rx"},    {.name = "tcp_find_cb"}, {.name = "tcp_process"}, {.name = "start_xmit"},
};

// Hot-path counter: just increments and sets a flag — no serial writes.
inline std::atomic<uint32_t> pkt_counter{0};
inline std::atomic<bool> dump_pending{false};

inline void maybe_dump() {
    uint32_t prev = pkt_counter.fetch_add(1, std::memory_order_relaxed);
    if (prev + 1 >= DUMP_INTERVAL) {
        pkt_counter.store(0, std::memory_order_relaxed);
        dump_pending.store(true, std::memory_order_release);
    }
}

// Called from a non-hot context (e.g. TCP timer thread) to do the actual print.
// Safe to call frequently — no-op when nothing is pending.
inline void flush() {
    if (!dump_pending.load(std::memory_order_acquire)) {
        return;
    }
    dump_pending.store(false, std::memory_order_relaxed);

    // Snapshot and reset each span.
    for (auto& s : spans) {
        if (s.count == 0) {
            continue;
        }
        uint64_t avg = s.total_cycles / s.count;
        // keep format minimal kernel sprintf is not a full impl
        ker::mod::dbg::log("TRACE %s n=%u  min=%lu  avg=%lu  max=%lu", s.name, s.count, static_cast<unsigned long>(s.min_cycles),
                           static_cast<unsigned long>(avg), static_cast<unsigned long>(s.max_cycles));
        s.total_cycles = 0;
        s.min_cycles = ~0ULL;
        s.max_cycles = 0;
        s.count = 0;
    }
    ker::mod::dbg::log("TRACE ---");
}

struct ScopedSpan {
    Span& span;
    uint64_t start;
    ScopedSpan(SpanId id) : span(spans[id]), start(rdtsc()) {}
    ~ScopedSpan() { span.record(rdtsc() - start); }
};

#define NET_TRACE_SPAN(id) ker::net::trace::ScopedSpan _trace_##id(ker::net::trace::id)
#define NET_TRACE_TICK() ker::net::trace::maybe_dump()

#else

#define NET_TRACE_SPAN(id) ((void)0)
#define NET_TRACE_TICK() ((void)0)

#endif

}  // namespace ker::net::trace
