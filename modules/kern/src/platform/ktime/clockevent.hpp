#pragma once

#include <cstdint>
#include <platform/ktime/arithmetic.hpp>

namespace ker::mod::time {

struct ClockeventCalibration {
    uint64_t sample_ticks{};
    uint64_t sample_us{};
    static constexpr uint64_t MAX_COUNT = UINT32_MAX;

    [[nodiscard]] constexpr auto valid() const -> bool { return sample_ticks != 0 && sample_us != 0; }

    // A zero request or unavailable calibration is a near recheck, never an
    // accidental timer disarm. Rounding upward prevents conversion-only early
    // expiry; requests beyond hardware range require subsequent rechecks.
    [[nodiscard]] auto ticks_for_delay_us(uint64_t delay_us) const -> uint32_t {
        if (!valid() || delay_us == 0) return 1;
        uint64_t const TICKS = arithmetic::mul_div(delay_us, sample_ticks, sample_us, true);
        if (TICKS == 0) return 1;
        return static_cast<uint32_t>(TICKS > MAX_COUNT ? MAX_COUNT : TICKS);
    }

    [[nodiscard]] auto resolution_ns() const -> uint64_t { return valid() ? arithmetic::mul_div(sample_us, 1000, sample_ticks, true) : 0; }
    [[nodiscard]] auto minimum_delay_us() const -> uint64_t { return valid() ? arithmetic::mul_div(sample_us, 1, sample_ticks, true) : 0; }
    [[nodiscard]] auto maximum_delay_us() const -> uint64_t {
        return valid() ? arithmetic::mul_div(MAX_COUNT, sample_us, sample_ticks) : 0;
    }
};

struct ClockeventStatus {
    ClockeventCalibration calibration{};
    uint64_t quantum_us{};
    bool initialized{};
};

}  // namespace ker::mod::time
