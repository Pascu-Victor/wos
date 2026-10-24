#pragma once

#include <signal.h>
#include <sys/init_control.h>
#include <sys/types.h>

#include <array>
#include <cstddef>
#include <cstdint>

namespace uma_reference::arm {
namespace init = ker::abi::init_control;
inline constexpr uint64_t SECOND = 1'000'000'000, MARGIN = 30 * SECOND;
inline constexpr char ROOT[] = "/var/lib/uma-reference";
inline constexpr char SERVICE[] = "uma-reference";
inline constexpr char OBSERVER[] = "uma-reference-exit-witness";
inline constexpr char FINALIZER[] = "uma-reference-finalizer";
inline constexpr std::array<const char*, 9> SERVICES{"journald", "dropbear-keygen", "netd",   "httpd",  "dropbear",
                                                     "testd",    SERVICE,           OBSERVER, FINALIZER};

// Ordinary one-arm lexical owner. The handler only sets caller-owned storage;
// install never clears it. This object outlives both suite phases and audits.
class Cancellation {
    std::array<struct sigaction, 3> old_{};
    size_t installed_{};

   public:
    volatile sig_atomic_t value{};
    Cancellation() = default;
    Cancellation(const Cancellation&) = delete;
    auto operator=(const Cancellation&) -> Cancellation& = delete;
    ~Cancellation();
    [[nodiscard]] auto install() -> bool;
};

struct Profile {
    uint64_t uid[3]{}, gid[3]{}, groups{}, pid{}, parent{}, tid{}, cpus{}, affinity{};
    int64_t authority{}, nnp{}, privileges{}, priority{}, target_result{};
    uint32_t target_flags{};
    bool empty_target{};
};
[[nodiscard]] auto profile_valid(const Profile&, pid_t expected_parent, bool witness = false) -> bool;
[[nodiscard]] auto process_admitted(pid_t expected_parent, bool witness = false) -> bool;
[[nodiscard]] auto request_poweroff() -> bool;
[[nodiscard]] auto environment_valid() -> bool;
[[nodiscard]] auto snapshot_read(init::StatusSnapshot&) -> int64_t;
[[nodiscard]] auto snapshot_valid(const init::StatusSnapshot&, init::ServiceStatus& selected) -> bool;
[[nodiscard]] auto running(const init::StatusSnapshot&, pid_t controller, uint64_t& deadline) -> bool;
[[nodiscard]] auto consumed(const init::StatusSnapshot&, pid_t observer) -> bool;
// Pending exec publication permits only another bounded poll, never final credit.
[[nodiscard]] auto awaiting_exec(const init::StatusSnapshot&, pid_t observer) -> bool;
// These require original controller AND witness consumption, never their initialized wait0.
[[nodiscard]] auto witness_consumed(const init::StatusSnapshot&, const init::StatusSnapshot& earlier, pid_t finalizer,
                                    uint64_t original_deadline) -> bool;
[[nodiscard]] auto witness_awaiting_exec(const init::StatusSnapshot&, const init::StatusSnapshot& earlier, pid_t finalizer,
                                         uint64_t original_deadline) -> bool;
[[nodiscard]] auto now(uint64_t&) -> bool;
[[nodiscard]] auto save(const char* name, const void*, size_t) -> bool;
[[nodiscard]] auto read(const char* name, void*, size_t capacity, size_t& bytes) -> bool;
[[nodiscard]] auto clock_capture(const char* name, uint64_t& resolution) -> bool;
[[nodiscard]] auto clock_pair(uint64_t resolution) -> bool;
[[nodiscard]] auto root_path() -> const char*;

struct PhaseAudit {
    uint64_t first{}, last{}, resolution{}, transport{}, output{};
    uint32_t cases{}, metrics{};
};
// Reuses the actual standalone stamp's parsers and owner's output scan.
// Output is unchanged on any refusal.
[[nodiscard]] auto audit_phase(const char* phase, const char* nonce, uint64_t deadline, pid_t suite, PhaseAudit&) -> bool;
[[nodiscard]] auto phase_output_bytes(const char* phase, uint64_t&) -> bool;

struct Completion {
    std::array<char, 33> nonce{};
    uint64_t controller{}, warm{}, measure{}, deadline{}, work_stop{};
    PhaseAudit phases[2]{};
};
inline constexpr size_t COMPLETION_LIMIT = 1536;
[[nodiscard]] auto format_completion(char*, size_t, const Completion&) -> int;
[[nodiscard]] auto parse_completion(const char*, size_t, const char* nonce, Completion&) -> bool;
[[nodiscard]] auto audit_completion(const Completion&) -> bool;
[[nodiscard]] auto valid_nonce(const char*) -> bool;
}  // namespace uma_reference::arm
