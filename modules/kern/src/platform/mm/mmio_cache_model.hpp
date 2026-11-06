#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "mtrr_policy.hpp"

namespace ker::mod::mm::mmio_cache {

enum class Status : uint8_t {
    ready,
    pending,
    busy,
    unconfigured,
    invalid_configuration,
    duplicate_capture,
    wrong_cpu,
    local_idt_unavailable,
    missing_features,
    unsupported_vendor,
    unsupported_layout,
    unsupported_capability,
    unsupported_mode,
    read_fault,
    invalid_reserved,
    invalid_geometry,
    invalid_type,
    missing_observation,
    undefined_overlap,
    inconsistent_policy,
    state_drift,
    capacity,
    unknown_extension,
    needs_address_geometry,
    fixed_routing_unqualified,
    ambiguous_routing,
    routing_override_unqualified,
    routed_to_memory,
    ambiguous_tom2_default,
    protected_ranges_unobservable,
    protected_range,
    not_strict_uc,
    output_not_empty,
};

namespace detail {

enum class Observation : uint8_t { dormant, value, fault };
struct Register {
    uint64_t value{};
    Observation observation{Observation::dormant};
    [[nodiscard]] auto observed() const -> bool { return observation == Observation::value; }
};

struct AmdInput {
    Register syscfg{};
    Register top_mem{};
    Register top_mem2{};
    std::array<Register, 2> iorr_base{};
    std::array<Register, 2> iorr_mask{};
    uint32_t encryption_eax{};
    uint32_t encryption_ebx{};
    Register sev_status{};
};

struct IoRange {
    uint64_t begin{};
    uint64_t end{};
    bool read_memory{};
    bool write_memory{};
};

struct AmdPolicy {
    bool fixed_routing{};
    bool fixed_routing_visible{};
    bool top_mem_enabled{};
    bool top_mem2_enabled{};
    bool force_wb_default{};
    uint64_t top_mem{};
    uint64_t top_mem2{};
    std::array<IoRange, 2> iorr{};
    size_t iorr_count{};
};

struct IntelProtectedInput {
    uint64_t capability{};
    Register smrr_base{};
    Register smrr_mask{};
    Register seamrr_base{};
    Register seamrr_mask{};
};

struct AmdProtectedInput {
    Register smm_base{};
    Register smm_mask{};
    uint32_t signature{};
};

enum class ProtectedKind : uint8_t { smrr, seamrr, amd_aseg, amd_tseg };
struct ExcludedRange {
    uint64_t begin{};
    uint64_t end{};
    // Diagnostic only: qualification rejects the range before composing types.
    cache_arch::MemoryType forced_type{cache_arch::MemoryType::unknown};
    ProtectedKind kind{ProtectedKind::smrr};
};

struct ProtectedPolicy {
    std::array<ExcludedRange, 4> ranges{};
    size_t count{};
};

struct CpuPolicy {
    cache_arch::MtrrPolicy mtrr{};
    uint64_t pat{};
    uint64_t cache_cr0{};
    uint64_t cache_cr4{};
    AmdPolicy amd{};
    ProtectedPolicy protected_ranges{};
};

// These are pure model values, never cache/mapping capabilities. Only the
// runtime's actual once-published CPU observations can mint a qualified range.
[[nodiscard]] auto from_mtrr_status(cache_arch::Status status) -> Status;
[[nodiscard]] auto decode_amd(const AmdInput& input, const cache_arch::MtrrPolicy& mtrr, AmdPolicy& output) -> Status;
[[nodiscard]] auto amd_default_type(const AmdPolicy& amd, const cache_arch::MtrrPolicy& mtrr, const cache_arch::Point& point,
                                    uint64_t physical) -> cache_arch::MemoryType;
[[nodiscard]] auto amd_io_at(const AmdPolicy& amd, uint64_t physical) -> Status;
[[nodiscard]] auto append_amd_boundaries(const AmdPolicy& amd, uint64_t begin, uint64_t end, cache_arch::Boundaries& output) -> Status;
[[nodiscard]] auto equivalent_amd_routing(const AmdPolicy& left, const AmdPolicy& right) -> bool;
[[nodiscard]] auto decode_intel_protected(const IntelProtectedInput& input, uint8_t physical_bits, ProtectedPolicy& output) -> Status;
[[nodiscard]] auto decode_amd_protected(const AmdProtectedInput& input, uint8_t physical_bits, ProtectedPolicy& output) -> Status;
[[nodiscard]] auto validate_pat(uint64_t pat) -> Status;
[[nodiscard]] auto validate_controls(uint64_t cr0, uint64_t cr4) -> Status;
[[nodiscard]] auto append_cpu_boundaries(const CpuPolicy& policy, uint64_t begin, uint64_t end, cache_arch::Boundaries& output) -> Status;
[[nodiscard]] auto equivalent_cpu_policy(const CpuPolicy& left, const CpuPolicy& right) -> Status;
[[nodiscard]] auto qualify_cpu_uc(const CpuPolicy& policy, uint64_t begin, uint64_t bytes) -> Status;

}  // namespace detail
}  // namespace ker::mod::mm::mmio_cache
