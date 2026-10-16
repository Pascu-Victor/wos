#pragma once

#include <signal.h>
#include <sys/types.h>

#include <cstdint>

namespace uma_reference {

// This is one phase's direct-child ownership component, not the whole-arm
// controller or a timing/initial-init/runtime qualification interface.
enum class OwnerFailure : uint8_t {
    NONE,
    INVALID_REQUEST,
    SETUP,
    ACKNOWLEDGMENT,
    WAIT_AUTHORITY,
    CANCELLED,
    DEADLINE,
    TIMEOUT_MARKER,
    OUTPUT,
    RAW_STATUS,
    SIGNAL,
    CLOCK,
    REAP
};
struct PhaseRequest {
    const char* phase{};                          // warmup or measure
    const char* nonce{};                          // fixed 32 lower-case hex digits
    const volatile sig_atomic_t* cancellation{};  // live whole-arm flag; never reset by this component
    uint64_t work_stop_ns{};                      // actual init deadline minus 30s
    uint64_t prior_output_bytes{};                // previous strict-decoded phase only
    uint64_t prior_transport_bytes{};
#ifdef WOS_UMA_OWNER_HOST
    const char* host_root{};
    char* const* host_argv{};
    uint64_t host_grace_ns{};  // Host test time scaling only
#endif
};
struct PhaseOutcome {
    OwnerFailure failure{OwnerFailure::NONE};
    pid_t child{-1};
    int observed_raw{-1}, consumed_raw{-1}, system_error{};
    uint32_t nonconsuming_completions{}, consuming_completions{}, term_calls{}, kill_calls{};
    uint64_t output_highwater{}, transport_highwater{};
    bool admitted{}, consumed{}, raw_phase_zero{};
    // Always false: the component neither inventories escaped groups/REMOTE
    // helpers nor substitutes for the independent whole-VM cleanup authority.
    bool descendant_closure_proven{};
};

// Preconditions: ordinary single-threaded controller, sole child waiter, no
// SIGCHLD handler/reaper and trusted fixed phase directories already admitted.
// No second phase may start after any failure. Raw phase zero is insufficient
// without strict full117 decoding and all whole-arm/init/runtime gates.
[[nodiscard]] auto own_suite_phase(const PhaseRequest& request) -> PhaseOutcome;

}  // namespace uma_reference
