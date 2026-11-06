#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::mod::mm::cache_arch {

enum class Vendor : uint8_t { unknown, intel, amd };
enum class MemoryType : uint8_t { uc = 0, wc = 1, wt = 4, wp = 5, wb = 6, uc_minus = 7, cd = 8, unknown = 255 };
enum class Status : uint8_t {
    ok,
    unsupported_vendor,
    unsupported_capability,
    unsupported_layout,
    invalid_geometry,
    invalid_reserved,
    invalid_type,
    missing_observation,
    undefined_overlap,
    inconsistent_policy,
    capacity,
};

inline constexpr size_t VARIABLE_LIMIT = 10;
inline constexpr size_t FIXED_REGISTER_COUNT = 11;
inline constexpr size_t FIXED_TYPE_COUNT = 88;
inline constexpr size_t BOUNDARY_LIMIT = 256;
inline constexpr uint64_t PAGE_BYTES = 4096;

struct RawVariable {
    uint64_t base{};
    uint64_t mask{};
};

// Plain inputs and decoded values are a pure architectural model. They cannot
// adopt hardware state or mint a kernel mapping/cache-completion capability.
struct RawMtrrs {
    Vendor vendor{Vendor::unknown};
    uint8_t physical_bits{};
    uint64_t capability{};
    uint64_t default_type{};
    std::array<RawVariable, VARIABLE_LIMIT> variable{};
    std::array<uint64_t, FIXED_REGISTER_COUNT> fixed{};
    uint16_t observed_masks{};
    uint16_t observed_bases{};
    bool observed_fixed{};
};

struct VariableRange {
    uint64_t begin{};
    uint64_t end{};
    MemoryType type{MemoryType::uc};
};

struct MtrrPolicy {
    Vendor vendor{Vendor::unknown};
    uint8_t physical_bits{};
    uint64_t address_ceiling{};
    bool enabled{};
    bool fixed_enabled{};
    bool wc_supported{};
    MemoryType default_type{MemoryType::uc};
    std::array<VariableRange, VARIABLE_LIMIT> variable{};
    size_t variable_count{};
    std::array<MemoryType, FIXED_TYPE_COUNT> fixed{};
};

enum class Source : uint8_t { disabled, fixed, variable, default_value };
struct Point {
    Status status{Status::invalid_geometry};
    MemoryType type{MemoryType::uc};
    Source source{Source::disabled};
};

struct Boundaries {
    std::array<uint64_t, BOUNDARY_LIMIT> values{};
    size_t count{};
    [[nodiscard]] auto insert(uint64_t value) -> Status;
};

// Outputs remain unchanged on failure. Unsupported enabled counts are rejected
// before interpreting any unproven row, including fixed/PAT/DEF_TYPE collisions.
[[nodiscard]] auto variable_layout_count(Vendor vendor) -> size_t;
[[nodiscard]] auto decode_mtrrs(const RawMtrrs& raw, MtrrPolicy& output) -> Status;
[[nodiscard]] auto resolve_mtrr(const MtrrPolicy& policy, uint64_t physical) -> Point;
[[nodiscard]] auto append_mtrr_boundaries(const MtrrPolicy& policy, uint64_t begin, uint64_t end, Boundaries& output) -> Status;
[[nodiscard]] auto equivalent_mtrr_types(const MtrrPolicy& left, const MtrrPolicy& right) -> Status;
[[nodiscard]] auto effective_uc_pat_type(Vendor vendor, MemoryType mtrr) -> MemoryType;
[[nodiscard]] auto effective_wc_pat_type(Vendor vendor, MemoryType mtrr) -> MemoryType;

}  // namespace ker::mod::mm::cache_arch
