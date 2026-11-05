#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace ker::mod::mm::cache {

struct CpuPat {
    bool supported = false;
    uint64_t raw = 0;
};

struct PatQualification {
    uint64_t expected_mask = 0;
    uint64_t reported_mask = 0;
    uint64_t mismatch_mask = 0;
    bool configured = false;
    bool complete = false;
    bool wb_encoding = false;
    bool uc_encoding = false;
    bool wc_encoding = false;
};

namespace detail {
inline constexpr size_t PAT_CPU_LIMIT = 64;
inline constexpr uint32_t PAT_FEATURE = uint32_t{1} << 16;
inline constexpr uint32_t MSR_FEATURE = uint32_t{1} << 5;

[[nodiscard]] constexpr auto pat_entry(uint64_t raw, unsigned index) -> uint8_t { return index < 8 ? (raw >> (index * 8U)) & 255U : 255U; }

// Actual capture uses this gate; a processor without PAT never executes RDMSR.
template <typename CpuIo>
[[nodiscard]] auto observe_pat(CpuIo& io) -> std::optional<CpuPat> {
    uint32_t const FEATURES = io.features();
    if ((FEATURES & PAT_FEATURE) == 0) return CpuPat{};
    if ((FEATURES & MSR_FEATURE) == 0) return std::nullopt;
    return CpuPat{true, io.read_pat()};
}

// Fixed boot storage. Each cell has one producer and immutable scalar contents;
// readers acquire its publication. No reset or privileged test setter exists.
class PatReports {
    enum class State : uint8_t { empty, capturing, ready, failed };
    struct Cell {
        std::atomic<State> state{State::empty};
        CpuPat value{};
    };
    static_assert(std::atomic<State>::is_always_lock_free && std::atomic<uint64_t>::is_always_lock_free &&
                  std::atomic<bool>::is_always_lock_free);
    std::array<Cell, PAT_CPU_LIMIT> cells_{};
    std::atomic<uint64_t> expected_{0};
    std::atomic<uint64_t> reported_{0};
    std::atomic<uint64_t> mismatch_{0};
    std::atomic<bool> poisoned_{false};

   public:
    [[nodiscard]] auto configure(size_t count) -> bool {
        if (count == 0 || count > PAT_CPU_LIMIT) {
            poisoned_.store(true, std::memory_order_release);
            return false;
        }
        uint64_t const MASK = count == PAT_CPU_LIMIT ? UINT64_MAX : (uint64_t{1} << count) - 1;
        uint64_t expected = 0;
        if (expected_.compare_exchange_strong(expected, MASK, std::memory_order_acq_rel, std::memory_order_acquire) || expected == MASK)
            return true;
        poisoned_.store(true, std::memory_order_release);
        return false;
    }

    template <typename Reader>
    [[nodiscard]] auto capture(size_t index, Reader read) -> bool {
        uint64_t const EXPECTED = expected_.load(std::memory_order_acquire);
        if (index >= PAT_CPU_LIMIT || (EXPECTED & (uint64_t{1} << index)) == 0) {
            poisoned_.store(true, std::memory_order_release);
            return false;
        }
        uint64_t const BIT = uint64_t{1} << index;
        auto& cell = cells_[index];
        State empty = State::empty;
        if (!cell.state.compare_exchange_strong(empty, State::capturing, std::memory_order_acq_rel, std::memory_order_acquire)) {
            mismatch_.fetch_or(BIT, std::memory_order_acq_rel);
            poisoned_.store(true, std::memory_order_release);
            return false;
        }
        auto const VALUE = read();
        if (!VALUE) {
            mismatch_.fetch_or(BIT, std::memory_order_acq_rel);
            poisoned_.store(true, std::memory_order_release);
            cell.state.store(State::failed, std::memory_order_release);
            return false;
        }
        cell.value = *VALUE;
        if (VALUE->supported && (pat_entry(VALUE->raw, 0) != 6 || pat_entry(VALUE->raw, 3) != 0 || pat_entry(VALUE->raw, 5) != 1))
            mismatch_.fetch_or(BIT, std::memory_order_acq_rel);
        cell.state.store(State::ready, std::memory_order_release);
        reported_.fetch_or(BIT, std::memory_order_release);
        return true;
    }

    [[nodiscard]] auto cpu_pat(size_t index, CpuPat& out) const -> bool {
        if (index >= PAT_CPU_LIMIT || (expected_.load(std::memory_order_acquire) & (uint64_t{1} << index)) == 0 ||
            cells_[index].state.load(std::memory_order_acquire) != State::ready)
            return false;
        out = cells_[index].value;
        return true;
    }

    [[nodiscard]] auto qualification() const -> PatQualification {
        PatQualification result{};
        result.expected_mask = expected_.load(std::memory_order_acquire);
        result.configured = result.expected_mask != 0;
        result.reported_mask = reported_.load(std::memory_order_acquire);
        result.mismatch_mask = mismatch_.load(std::memory_order_acquire);
        result.complete = result.configured && result.reported_mask == result.expected_mask && !poisoned_.load(std::memory_order_acquire);
        if (!result.complete) return result;
        result.wb_encoding = result.uc_encoding = result.wc_encoding = true;
        for (size_t i = 0; i < PAT_CPU_LIMIT; ++i) {
            if ((result.expected_mask & (uint64_t{1} << i)) == 0) continue;
            CpuPat value{};
            if (!cpu_pat(i, value)) {
                result.complete = result.wb_encoding = result.uc_encoding = result.wc_encoding = false;
                return result;
            }
            result.wb_encoding &= !value.supported || pat_entry(value.raw, 0) == 6;
            result.uc_encoding &= !value.supported || pat_entry(value.raw, 3) == 0;
            result.wc_encoding &= value.supported && pat_entry(value.raw, 5) == 1;
        }
        result.wc_encoding &= result.wb_encoding && result.uc_encoding;
        return result;
    }
};
}  // namespace detail

// Read-only inherited encoding observations. These do not qualify MTRRs,
// physical resources, cache aliases or permission to publish WC mappings.
[[nodiscard]] auto configure_boot_cpu_count(size_t count) -> bool;
[[nodiscard]] auto capture_boot_cpu(size_t index) -> bool;
[[nodiscard]] auto qualification() -> PatQualification;
[[nodiscard]] auto cpu_pat(size_t index, CpuPat& out) -> bool;

}  // namespace ker::mod::mm::cache
