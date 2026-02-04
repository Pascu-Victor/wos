#pragma once

#include <atomic>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::net {

struct NetDevice;

// NAPI states - managed atomically for lock-free IRQ/worker coordination
enum class NapiState : uint32_t {
    IDLE = 0,       // No pending work, device interrupts enabled
    SCHEDULED = 1,  // Work pending, worker thread will poll
    POLLING = 2,    // Worker actively polling device
    DISABLED = 3    // Device disabled (going down)
};

struct NapiStruct;

// Poll function signature: returns number of packets processed.
// If return < budget, polling is complete and driver should re-enable interrupts.
using NapiPollFn = int (*)(NapiStruct* napi, int budget);

// Per-device NAPI context - embedded in driver device struct
struct NapiStruct {
    NetDevice* dev;                       // Parent network device
    NapiPollFn poll;                      // Driver poll function
    std::atomic<NapiState> state;         // Current NAPI state (lock-free)
    int weight;                           // Max packets per poll (default: 64)

    // Per-device worker thread
    ker::mod::sched::task::Task* worker;  // Dedicated kernel thread
    std::atomic<bool> has_work;           // Signal to wake worker (set by IRQ)

    // Statistics
    uint64_t poll_count;                  // Number of poll calls
    uint64_t complete_count;              // Number of napi_complete calls
};

constexpr int NAPI_DEFAULT_WEIGHT = 64;

// Initialize NAPI structure (called during driver init, before napi_enable)
void napi_init(NapiStruct* napi, NetDevice* dev, NapiPollFn poll, int weight);

// Enable NAPI - creates and starts the per-device worker thread
void napi_enable(NapiStruct* napi);

// Disable NAPI - stops the worker thread (call before device shutdown)
void napi_disable(NapiStruct* napi);

// Schedule NAPI poll - called from IRQ handler
// This is IRQ-safe: only uses atomic operations and rescheduleTaskForCpu
// Returns true if successfully scheduled, false if already scheduled/polling
bool napi_schedule(NapiStruct* napi);

// Signal poll completion - called by driver poll function when processed < budget
// Transitions state from POLLING back to IDLE
// Driver should re-enable device interrupts after calling this
void napi_complete(NapiStruct* napi);

}  // namespace ker::net
