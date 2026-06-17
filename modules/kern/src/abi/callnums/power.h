#pragma once

#include <cstdint>

namespace ker::abi::power {

// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class ops : uint64_t {
    REBOOT = 0,
    GET_STATE = 1,
    PREPARE = 2,
};

constexpr uint64_t RB_AUTOBOOT = 0x01234567;
constexpr uint64_t RB_HALT_SYSTEM = 0xcdef0123;
constexpr uint64_t RB_ENABLE_CAD = 0x89abcdef;
constexpr uint64_t RB_DISABLE_CAD = 0;
constexpr uint64_t RB_POWER_OFF = 0x4321fedc;

}  // namespace ker::abi::power
