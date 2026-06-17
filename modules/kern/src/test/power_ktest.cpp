#include <cstdint>
#include <platform/acpi/power.hpp>
#include <test/ktest.hpp>

KTEST(PowerAcpi, ParsesS5Package) {
    constexpr uint8_t AML[] = {
        0x08, '_', 'S', '5', '_', 0x12, 0x06, 0x02, 0x0a, 0x05, 0x0a, 0x07,
    };

    ker::mod::acpi::power::SleepType s5{};
    KEXPECT_TRUE(ker::mod::acpi::power::find_s5_sleep_type(AML, sizeof(AML), s5));
    KEXPECT_EQ(s5.pm1a, 5U);
    KEXPECT_EQ(s5.pm1b, 7U);
}

KTEST(PowerAcpi, RejectsMalformedS5Package) {
    constexpr uint8_t TRUNCATED_AML[] = {
        0x08, '_', 'S', '5', '_', 0x12, 0x06, 0x02, 0x0a, 0x05,
    };
    constexpr uint8_t INVALID_VALUE_AML[] = {
        0x08, '_', 'S', '5', '_', 0x12, 0x06, 0x02, 0x0a, 0x08, 0x0a, 0x01,
    };

    ker::mod::acpi::power::SleepType s5{};
    KEXPECT_FALSE(ker::mod::acpi::power::find_s5_sleep_type(TRUNCATED_AML, sizeof(TRUNCATED_AML), s5));
    KEXPECT_FALSE(ker::mod::acpi::power::find_s5_sleep_type(INVALID_VALUE_AML, sizeof(INVALID_VALUE_AML), s5));
}
