#pragma once

#include <cstddef>
#include <cstdint>

#include "mmio_cache_observation.hpp"

namespace ker::mod::mm::mmio_cache {
namespace detail {
class Runtime;
}

struct CohortObservation {
    Status status{Status::unconfigured};
    uint64_t expected_mask{};
    uint64_t terminal_mask{};
    uint64_t ready_mask{};
    uint64_t invalidated_mask{};
    uint64_t incarnation{};
    bool sealed{};
};

struct CpuObservation {
    uint32_t logical{};
    uint32_t apic{};
    Status status{Status::pending};
    detail::RawCpu raw{};
    detail::CpuPolicy policy{};
};

class QualifiedUcRange {
    uint64_t physical_{};
    uint64_t bytes_{};
    uint64_t incarnation_{};
    friend class detail::Runtime;
    QualifiedUcRange(uint64_t physical, uint64_t bytes, uint64_t incarnation);

   public:
    QualifiedUcRange() = default;
    QualifiedUcRange(const QualifiedUcRange&) = delete;
    auto operator=(const QualifiedUcRange&) -> QualifiedUcRange& = delete;
    QualifiedUcRange(QualifiedUcRange&& other) noexcept;
    auto operator=(QualifiedUcRange&& other) noexcept -> QualifiedUcRange&;
    [[nodiscard]] auto empty() const -> bool { return incarnation_ == 0; }
    [[nodiscard]] auto physical() const -> uint64_t { return physical_; }
    [[nodiscard]] auto bytes() const -> uint64_t { return bytes_; }
    [[nodiscard]] auto incarnation() const -> uint64_t { return incarnation_; }
};

// Configure before AP launch; actual post-IDT boot hooks supply the expected
// immutable logical identity, which is checked against real GS and local APIC.
[[nodiscard]] auto configure_boot_cohort(size_t cpus) -> Status;
[[nodiscard]] auto capture_boot_cpu(size_t expected_logical) -> Status;
[[nodiscard]] auto cohort_observation() -> CohortObservation;
[[nodiscard]] auto cpu_observation(size_t logical, CpuObservation& output) -> bool;

// A token proves the observed ordinary UC policy and protected-range exclusions
// only. It grants no physical/BAR ownership, alias absence, flush completion,
// device quiescence, MMIO publication, WC or reset authority.
// Qualification performs bounded policy preparation in boot/task context,
// outside MM/PCI/IRQ locks; concurrent preparation returns busy without waiting.
[[nodiscard]] auto qualify_uc_range(uint64_t physical, uint64_t page_rounded_bytes, QualifiedUcRange& empty_output) -> Status;
[[nodiscard]] auto revalidate_current_cpu(const QualifiedUcRange& range) -> Status;
[[nodiscard]] auto status_name(Status status) -> const char*;

}  // namespace ker::mod::mm::mmio_cache
