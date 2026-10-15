#pragma once

#ifdef WOS_SELFTEST
#include <cstddef>
#include <cstdint>
#include <syscalls_impl/process/native_construction_token.hpp>

namespace ker::mod::sched::task {
struct Task;
}
namespace ker::syscall::process {
enum class NativeConstructionFault : uint8_t;
}

namespace ker::test::lifecycle60 {
using Task = ker::mod::sched::task::Task;
using Fault = ker::syscall::process::NativeConstructionFault;
using Token = ker::syscall::process::NativeConstructionToken;
inline constexpr size_t WARMUPS = 8;
inline constexpr size_t FAULTS = 24;
inline constexpr size_t SUCCESSES = 1000;
inline constexpr size_t ATTEMPTS = WARMUPS + FAULTS + SUCCESSES;
inline constexpr char SERVICE_NAME[] = "lifecycle-raw-pilot";

// ACCEPT is actual validated evidence, not a caller claim. WAIT retries only
// before release, with one campaign deadline; REFUSE permanently poisons it.
enum class GateResult : uint8_t { WAIT, ACCEPT, REFUSE };
enum class Boundary : uint8_t { BEFORE_DRIVER, BEFORE_CREATE, AFTER_FAILURE, AFTER_WAIT, AFTER_DRIVER };
struct AttemptView {
    uint64_t index{};
    uint64_t cookie{};
    Fault fault{};
    int64_t expected_error{};
    int64_t create_result{};
    uint64_t subject_pid{};
    uint64_t visible_pid{};
    int32_t wait_status{};
    bool warmup{};
    bool consumed{};
    bool create_returned{};
    bool wait_consumed{};
    bool task_debt_empty{};
};
struct CampaignSummary {
    uint64_t warmups{};
    uint64_t faults{};
    uint64_t successes{};
    uint64_t consumed_faults{};
    uint64_t actual_wait9{};
    uint64_t last_cookie{};
    bool original_init_reaped_driver{};
    bool all_attempt_task_debts_empty{};
};

// Permission to observe executor-owned non-atomic FD/recovery fields. A Task
// lifetime reference is NOT this permission. Only the actual controller task
// can acquire it after the driver itself ACKs its execution-owned kernel park.
// The driver withdraws ACK and drains every lease before resume/exit/backend.
class CensusLease final {
   public:
    CensusLease() noexcept = default;
    ~CensusLease();
    CensusLease(const CensusLease&) = delete;
    auto operator=(const CensusLease&) -> CensusLease& = delete;
    CensusLease(CensusLease&& other) noexcept;
    auto operator=(CensusLease&& other) noexcept -> CensusLease&;
    [[nodiscard]] explicit operator bool() const noexcept { return subject_ != nullptr; }
    [[nodiscard]] auto subject() const noexcept -> Task* { return subject_; }
    [[nodiscard]] auto cookie() const noexcept -> uint64_t { return cookie_; }
    void reset() noexcept;

   private:
    friend auto acquire_census_lease() noexcept -> CensusLease;
    Task* subject_{};
    uint64_t cookie_{};
    uint64_t generation_{};
};
[[nodiscard]] auto acquire_census_lease() noexcept -> CensusLease;

// Permission for the separate no-ACK backend phase. Fatal/timeout marks
// cancellation but cannot let driver resume/cleanup until this owner resets.
// This does not authorize I/O during CensusLease/ACK and never owns a Task ref.
class EvidenceLease final {
   public:
    EvidenceLease() noexcept = default;
    ~EvidenceLease();
    EvidenceLease(const EvidenceLease&) = delete;
    auto operator=(const EvidenceLease&) -> EvidenceLease& = delete;
    EvidenceLease(EvidenceLease&&) noexcept;
    auto operator=(EvidenceLease&&) noexcept -> EvidenceLease&;
    [[nodiscard]] explicit operator bool() const noexcept { return subject_ != nullptr; }
    [[nodiscard]] auto subject() const noexcept -> Task* { return subject_; }
    [[nodiscard]] auto cookie() const noexcept -> uint64_t { return generation_; }
    void reset() noexcept;

   private:
    friend auto acquire_evidence_lease() noexcept -> EvidenceLease;
    Task* subject_{};
    uint64_t generation_{};
};
[[nodiscard]] auto acquire_evidence_lease() noexcept -> EvidenceLease;

// Image/report and census gates run in the controller's separate backend and
// parked-reader phases. Their definitions validate actual process evidence
// before the controller can accept a boundary or complete the campaign.
// Called by actual daemon while driver parks, WITHOUT ACK or census readers.
// Verify exact admitted executable+credentials/service and no FD/root sharing
// writer/worker. May use VFS; release all temporary owners before returning.
// BEFORE_DRIVER uses exact original init before ANY service pipe or fork reservation.
auto validate_prefork_evidence(const EvidenceLease&) noexcept -> GateResult;
auto validate_host_baseline(const CensusLease&) noexcept -> GateResult;
auto admit_driver_image_and_sharing(const EvidenceLease&, uint64_t image_epoch) noexcept -> GateResult;
auto validate_attempt_evidence(const EvidenceLease&, const AttemptView&, Boundary) noexcept -> GateResult;
// Called ONLY with a live permission; no I/O/usercopy/backend/exit. Compose
// full A then full B using every corrected owner version/inflight plus storage
// sequence, actual scalar debts and executor RETURNED. First BEFORE_CREATE
// after warmups establishes baseline; generations are not baseline equality.
auto validate_quiescent_census(const CensusLease&, const AttemptView&, Boundary, bool establish_baseline) noexcept -> GateResult;
// AFTER_DRIVER: actual consuming WAIT of driver EXIT0 was observed AND
// original init published the same completed/quiescent service generation after
// closing all supervisor handles. Driver ref drops before final census ACK.
// Compare genuine BEFORE_DRIVER Host baseline with same init diagnostic ref.
// Must prove final driver/witness/storage/callback drains, not fake zeros.
auto validate_final_evidence(const EvidenceLease&, const CampaignSummary&) noexcept -> GateResult;
auto validate_final_census(const CensusLease&, const CampaignSummary&) noexcept -> GateResult;
void publish_campaign_result(const CampaignSummary&, bool passed) noexcept;

// Actual integration hooks; no user-selected stage, new syscall or fault ABI.
void enable_boot_capture(bool enabled) noexcept;
void original_init_posted(Task&) noexcept;
void before_selected_service_fork(Task& parent) noexcept;
void service_resource_read_completed(Task& actor, uint64_t slot, uint64_t result) noexcept;
void fork_will_publish(Task& parent, Task& child) noexcept;
void fork_publication_failed(Task& parent, Task& child) noexcept;
void successful_fork(Task& parent, Task& child) noexcept;
void image_committed(Task& subject, bool remote) noexcept;
void create_returned(Token, int64_t result) noexcept;
void consumed_wait(Task& waiter, uint64_t subject, uint64_t visible, int32_t status, bool parent_exit, bool nowait) noexcept;
void driver_exit_begin(Task&) noexcept;
void allocated_attempt_task(Token, Task*) noexcept;
struct TaskDeletionClaim {
    uint64_t attempt{};
    uint64_t cookie{};
    uint64_t deletion_cookie{};
    bool ntp_boot{};
};
// Independent finite-boot debt. These never retain/dereference a saved Task.
// Registration is retained even if it precedes explicit boot capture enabling.
void ntp_boot_initializing() noexcept;
void ntp_boot_will_post(Task&) noexcept;
void ntp_boot_posted(bool posted) noexcept;
void ntp_boot_creation_failed() noexcept;
void ntp_boot_periodic() noexcept;
void ntp_boot_exiting() noexcept;
[[nodiscard]] auto ntp_boot_settlement() noexcept -> GateResult;
auto before_task_deallocation(void*) noexcept -> TaskDeletionClaim;
void after_task_deallocation(TaskDeletionClaim) noexcept;
auto start_scheduled_controller() -> bool;
}  // namespace ker::test::lifecycle60
#endif
