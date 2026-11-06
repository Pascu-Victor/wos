#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "mmio_cache.hpp"

namespace ker::mod::mm::cpu_maintenance {
namespace detail {
class Runtime;
}

inline constexpr size_t CPU_CAPACITY = 64;
inline constexpr size_t POOL_CAPACITY = 16;
enum class Kind : uint8_t { translations, translations_then_cache };
enum class Phase : uint8_t { empty, prepared, translations, caches, closing, complete };
enum class Status : uint8_t {
    ready,
    pending,
    busy,
    complete,
    timeout_retained,
    failed_retained,
    unconfigured,
    invalid_configuration,
    invalid_context,
    invalid_ticket,
    invalid_kind,
    empty_range,
    output_not_empty,
    not_ready,
    capacity,
    generation_exhausted,
    clock_unavailable
};

class Ticket {
    uint64_t id_{};
    size_t slot_{POOL_CAPACITY};
    friend class detail::Runtime;
    Ticket(size_t slot, uint64_t id);

   public:
    Ticket() = default;
    ~Ticket();
    Ticket(const Ticket&) = delete;
    auto operator=(const Ticket&) -> Ticket& = delete;
    Ticket(Ticket&& other) noexcept;
    auto operator=(Ticket&& other) noexcept -> Ticket&;
    [[nodiscard]] auto empty() const -> bool { return id_ == 0; }
    [[nodiscard]] auto id() const -> uint64_t { return id_; }
};

struct CpuReport {
    uint32_t target_apic{UINT32_MAX};
    uint32_t translation_apic{UINT32_MAX};
    uint32_t cache_apic{UINT32_MAX};
    uint64_t translation_claimed{};
    uint64_t translation_completed{};
    uint64_t cache_claimed{};
    uint64_t cache_completed{};
    mmio_cache::Status translation_status{mmio_cache::Status::pending};
    mmio_cache::Status cache_status{mmio_cache::Status::pending};
    uint64_t translation_cr3{};
    uint64_t cache_cr3{};
};

// Copied controller observations only. No PTE, physical-resource, alias,
// quiescence, cache-type-transition or storage-release authority is issued.
struct Report {
    Status status{Status::invalid_ticket};
    Phase phase{Phase::empty};
    Kind kind{Kind::translations};
    uint64_t id{};
    uint64_t physical{};
    uint64_t bytes{};
    uint64_t cohort_incarnation{};
    uint64_t targets{};
    uint64_t translation_claimed{};
    uint64_t translation_completed{};
    uint64_t cache_claimed{};
    uint64_t cache_completed{};
    uint64_t failed{};
    uint64_t readers{};
    bool admission_closed{};
    bool timed_out_ever{};
    bool abandoned{};
    uint64_t translation_withheld{};
    uint64_t cache_withheld{};
    mmio_cache::Status service_error{mmio_cache::Status::ready};
    std::array<CpuReport, CPU_CAPACITY> cpus{};
    // Copied inherited selector only; no permission to publish a cache alias.
    uint8_t pat_index{mmio_cache::QualifiedUcRange::PAT_INDEX};
};

struct ServiceObservation {
    Status status{Status::unconfigured};
    uint64_t expected_mask{};
    uint64_t ready_mask{};
    uint8_t vector{};
    mmio_cache::Status service_error{mmio_cache::Status::ready};
};
struct PoolStats {
    size_t capacity{POOL_CAPACITY};
    size_t prepared{};
    size_t published{};
    size_t complete{};
    size_t retained{};
};

// One Ticket has one externally serialized controller. Prepare/publish/wait,
// reports and explicit release require an ordinary scheduled DAEMON with its
// actual KernelShutdownHold, IF enabled/preempt0, outside MM/PCI/driver locks.
// Failure leaves prepare inputs unchanged. No hardware work occurs in prepare.
// Successful prepare returns ready; successful publication returns pending.
// Each overload retains its genuine policy type through every CPU action.
[[nodiscard]] auto prepare(mmio_cache::QualifiedUcRange&& range, Kind kind, Ticket& empty_output) -> Status;
[[nodiscard]] auto prepare(mmio_cache::QualifiedWcRange&& range, Kind kind, Ticket& empty_output) -> Status;
[[nodiscard]] auto publish(Ticket& ticket) -> Status;
[[nodiscard]] auto observe(const Ticket& ticket) -> Report;
// Only an explicit wait may advance translations into the cache round.
// Every noncomplete published result retains request/range/targets. Timeout
// history survives a later same-ticket retry. No bound on a stalled CPU or
// an already-entered local instruction is implied by the software deadline.
[[nodiscard]] auto wait_until(Ticket& ticket, uint64_t absolute_monotonic_ns) -> Report;
[[nodiscard]] auto release_complete(Ticket& ticket) -> Status;
[[nodiscard]] auto service_observation() -> ServiceObservation;
[[nodiscard]] auto pool_stats() -> PoolStats;
[[nodiscard]] auto status_name(Status status) -> const char*;

namespace detail {
// Actual boot/scheduler/service hooks only; no supplied target or ready masks.
[[nodiscard]] auto initialize_boot(uint8_t actual_tlb_service_vector) -> Status;
[[nodiscard]] auto note_current_cpu_ready() -> Status;
void service_current_cpu() noexcept;
}  // namespace detail

#ifdef WOS_SELFTEST
// Exact-ticket deferral can only withhold real work before its claim. Arming
// requires PREPARED state; clearing never creates completion or advances phase.
[[nodiscard]] auto diagnostic_defer(Ticket& ticket, Phase phase, size_t logical_cpu) -> Status;
[[nodiscard]] auto diagnostic_resume(Ticket& ticket, Phase phase) -> Status;
#endif

}  // namespace ker::mod::mm::cpu_maintenance
