#pragma once

#include <cstdint>

namespace ker::mod::acpi::power {

struct SleepType {
    uint8_t pm1a{};
    uint8_t pm1b{};
};

auto find_s5_sleep_type(const uint8_t* aml, uint32_t length, SleepType& out) -> bool;
auto reboot_via_fadt() -> bool;
auto poweroff_via_s5() -> bool;

}  // namespace ker::mod::acpi::power
