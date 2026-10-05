#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::net {

struct NetDevice;

// NAPI states - managed atomically for lock-free IRQ/worker coordination
enum class NapiState : uint8_t {
    IDLE = 0,       // No pending work, device interrupts enabled
    SCHEDULED = 1,  // Work pending, worker thread will poll
    POLLING = 2,    // Worker actively polling device
    DISABLED = 3    // Device disabled (going down)
};

struct NapiStruct;

enum class NapiScheduleSource : uint8_t { UNSPECIFIED, IRQ, REARM, STARTUP, COUNT };
enum class NapiPollSource : uint8_t { WORKER, INLINE, RESCUE, COUNT };
constexpr size_t NAPI_SCHEDULE_SOURCES = static_cast<size_t>(NapiScheduleSource::COUNT);
constexpr size_t NAPI_POLL_SOURCES = static_cast<size_t>(NapiPollSource::COUNT);

// Independent cumulative counters, not a transactional state snapshot. Source
// travels as a callback argument: completion can admit a new poll owner before
// the previous callback returns, so a shared "current source" would be racy.
template <typename Counter>
struct NapiDiagnosticCounters {
    std::array<Counter, NAPI_SCHEDULE_SOURCES> schedules{};
    std::array<Counter, NAPI_SCHEDULE_SOURCES> accepted{};
    std::array<Counter, NAPI_POLL_SOURCES> polls{};
    std::array<Counter, NAPI_POLL_SOURCES> rx_work{};
    std::array<Counter, NAPI_POLL_SOURCES> rx_zero{};
    std::array<Counter, NAPI_POLL_SOURCES> tx_reclaimed{};
    Counter coalesced{};
    Counter polling_rejects{};
    Counter disabled_rejects{};
    Counter park_calls{};
    Counter park_empty_returns{};
    Counter contention_sleeps{};
    Counter inline_refused{};
    Counter budget_handoffs{};
};

struct NapiDiagSnapshot : NapiDiagnosticCounters<uint64_t> {
    bool enabled = false;
};

// Poll function signature: returns number of packets processed.
// If return < budget, polling is complete and driver should re-enable interrupts.
using NapiPollFn = int (*)(NapiStruct* napi, int budget, NapiPollSource source);

// Per-device NAPI context - embedded in driver device struct
struct NapiStruct {
    NetDevice* dev;                // Parent network device
    NapiPollFn poll;               // Driver poll function
    std::atomic<NapiState> state;  // Current NAPI state (lock-free)
    int weight;                    // Max packets per poll (default: 64)

    // Per-device worker thread. A kernel-lifetime task reference acquired before
    // scheduling protects snapshots/wakeups after cooperative worker exit.
    ker::mod::sched::task::Task* worker;  // Dedicated kernel thread
    std::atomic<bool> has_work;           // Signal to wake worker (set by IRQ)

    // Statistics
    std::atomic<uint64_t> poll_count;      // Number of poll calls
    std::atomic<uint64_t> complete_count;  // Number of napi_complete calls
    bool diagnostics_enabled = false;      // Immutable after napi_init, before publication.
    NapiDiagnosticCounters<std::atomic<uint64_t>> diagnostics{};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free, "NAPI counters must remain IRQ-safe");

// Caller owns the NAPI lifetime. Optional counters stay zero without net.napi_diag.
auto napi_diag_snapshot(const NapiStruct* napi) -> NapiDiagSnapshot;
void napi_diag_tx_reclaimed(NapiStruct* napi, NapiPollSource source, uint64_t count);

constexpr int NAPI_DEFAULT_WEIGHT = 64;

// Initialize NAPI structure (called during driver init, before napi_enable)
void napi_init(NapiStruct* napi, NetDevice* dev, NapiPollFn poll, int weight);

// Enable NAPI - creates and starts the per-device worker thread.
// If cpu_affinity is not UINT64_MAX the worker is pinned to that CPU
// (same CPU that services the corresponding MSI-X vector).
void napi_enable(NapiStruct* napi, uint64_t cpu_affinity = UINT64_MAX);

// Disable polling/registry admission. This does not reclaim the NAPI object or
// its retained worker: existing snapshots and worker epilogues may still refer
// to them. Drivers must separately close/drain callbacks before freeing queues.
void napi_disable(NapiStruct* napi);

// Schedule NAPI poll - IRQ, completion rearm, or initial queue activation.
// This is IRQ-safe: only uses atomic operations and rescheduleTaskForCpu
// Returns true if successfully scheduled, false if already scheduled/polling
bool napi_schedule(NapiStruct* napi, NapiScheduleSource source = NapiScheduleSource::UNSPECIFIED);

// Signal poll completion - called by driver poll function when processed < budget
// Transitions state from POLLING back to IDLE
// Driver should re-enable device interrupts after calling this
void napi_complete(NapiStruct* napi);

// Drain all registered NAPI devices inline.  Call this after kern_yield() in
// blocking kernel paths (tcp_send, tcp_recv) so incoming ACKs/data are
// processed immediately instead of waiting for the next timer tick.
// Returns total number of packets processed across all devices.
int napi_poll_all_pending(NapiPollSource source = NapiPollSource::INLINE);

// Migrate the NAPI worker for this struct to `cpu` and pin it there.
// Returns false if the active worker is currently running on another CPU.
bool napi_set_worker_cpu(NapiStruct* napi, uint64_t cpu);

auto net_watchdog_enabled() -> bool;

}  // namespace ker::net
