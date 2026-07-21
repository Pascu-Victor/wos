#include "rtc.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mod/io/port/port.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/tsc/tsc.hpp>

namespace ker::mod::rtc {

namespace {

using log = ker::mod::dbg::logger<"rtc">;

// CMOS port pair: write index to 0x70, read data from 0x71.
// Setting bit 7 of the index byte disables NMI delivery while accessing CMOS.
constexpr uint16_t CMOS_INDEX = 0x70;
constexpr uint16_t CMOS_DATA = 0x71;

// CMOS register indices
constexpr uint8_t RTC_SEC = 0x00;
constexpr uint8_t RTC_MIN = 0x02;
constexpr uint8_t RTC_HOUR = 0x04;
constexpr uint8_t RTC_DAY = 0x07;
constexpr uint8_t RTC_MONTH = 0x08;
constexpr uint8_t RTC_YEAR = 0x09;
constexpr uint8_t RTC_STA = 0x0A;      // Status Register A
constexpr uint8_t RTC_STB = 0x0B;      // Status Register B
constexpr uint8_t RTC_CENTURY = 0x32;  // May not be present on all hardware
constexpr uint64_t NS_PER_SEC = 1000000000ULL;
constexpr uint64_t SEC_PER_MIN = 60;
constexpr uint64_t SEC_PER_HOUR = 60 * SEC_PER_MIN;
constexpr uint64_t SEC_PER_DAY = 24 * SEC_PER_HOUR;

// Epoch seconds read from CMOS at boot, plus any NTP correction.
uint64_t epoch_sec_at_boot = 0;
// TSC nanoseconds snapshot taken at the moment we read the RTC.
uint64_t tsc_ns_at_boot = 0;
// Signed NTP correction relative to the CMOS/TSC wall clock.
std::atomic<int64_t> ntp_delta_ns{0};

// ---------------------------------------------------------------------------
// CMOS helpers
// ---------------------------------------------------------------------------

uint8_t cmos_read(uint8_t reg) {
    outb(CMOS_INDEX, static_cast<uint8_t>(0x80U | reg));  // NMI disable + register
    io_wait();
    return inb(CMOS_DATA);
}

// Wait until the RTC "update in progress" flag (Status A bit 7) is clear.
void wait_rtc_ready() {
    for (int i = 0; i < 100000; ++i) {
        if ((cmos_read(RTC_STA) & 0x80U) == 0) {
            return;
        }
        asm volatile("pause");
    }
    // Timed out – proceed anyway; worst case we read a slightly corrupt value.
}

// ---------------------------------------------------------------------------
// Calendar conversion: calendar date + time -> Unix epoch seconds
// ---------------------------------------------------------------------------

constexpr bool is_leap_year(int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); }

constexpr std::array<uint8_t, 12> DAYS_IN_MONTH{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

constexpr uint64_t ymd_hms_to_epoch(int year, int month, int day, int hour, int min, int sec) {
    uint64_t days = 0;

    // Accumulate whole years from 1970.
    for (int y = 1970; y < year; ++y) {
        days += is_leap_year(y) ? 366U : 365U;
    }

    // Accumulate whole months in the current year.
    for (int m = 1; m < month; ++m) {
        days += DAYS_IN_MONTH.at(static_cast<std::size_t>(m - 1));
        if (m == 2 && is_leap_year(year)) {
            days += 1;
        }
    }

    days += static_cast<uint64_t>(day - 1);

    return (days * SEC_PER_DAY) + (static_cast<uint64_t>(hour) * SEC_PER_HOUR) + (static_cast<uint64_t>(min) * SEC_PER_MIN) +
           static_cast<uint64_t>(sec);
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void init() {
    // Snapshot the TSC monotonic time first so that epoch_sec_at_boot and
    // tsc_ns_at_boot refer to the same instant.
    tsc_ns_at_boot = tsc::get_ns();

    wait_rtc_ready();

    uint8_t const SEC_RAW = cmos_read(RTC_SEC);
    uint8_t const MIN_RAW = cmos_read(RTC_MIN);
    uint8_t const HOUR_RAW = cmos_read(RTC_HOUR);
    uint8_t const DAY_RAW = cmos_read(RTC_DAY);
    uint8_t const MONTH_RAW = cmos_read(RTC_MONTH);
    uint8_t const YEAR_RAW = cmos_read(RTC_YEAR);
    uint8_t const CENTURY_RAW = cmos_read(RTC_CENTURY);
    uint8_t const STB = cmos_read(RTC_STB);

    // Decode BCD if Status Register B bit 2 is 0.
    bool const BINARY_MODE = (STB & 0x04U) != 0;
    auto bcd = [&](uint8_t v) -> int { return BINARY_MODE ? v : static_cast<int>((v & 0x0FU) + ((v >> 4) * 10)); };

    int const SEC = bcd(SEC_RAW);
    int const MIN = bcd(MIN_RAW);
    int hour = bcd(HOUR_RAW & 0x7FU);  // strip 12/24 PM bit before BCD decode
    int const DAY = bcd(DAY_RAW);
    int const MONTH = bcd(MONTH_RAW);
    int year = bcd(YEAR_RAW);
    int const CENT = bcd(CENTURY_RAW);

    // Handle 12-hour mode: bit 7 of hour register indicates PM.
    bool const MODE_24H = (STB & 0x02U) != 0;
    if (!MODE_24H) {
        bool const PM = (HOUR_RAW & 0x80U) != 0;
        if (hour == 12) {
            hour = PM ? 12 : 0;
        } else if (PM) {
            hour += 12;
        }
    }

    // Determine century.
    if (CENT >= 20 && CENT <= 21) {
        year += CENT * 100;
    } else {
        // Century register absent or unreliable; assume 21st century.
        year += (year < 70) ? 2000 : 1900;
    }

    epoch_sec_at_boot = ymd_hms_to_epoch(year, MONTH, DAY, hour, MIN, SEC);

    log::info("wall clock %d-%02d-%02d %02d:%02d:%02d UTC (epoch %lu)", year, MONTH, DAY, hour, MIN, SEC,
              static_cast<unsigned long>(epoch_sec_at_boot));
}

auto get_epoch_sec() -> uint64_t { return get_epoch_ns() / NS_PER_SEC; }

auto get_epoch_ns() -> uint64_t {
    uint64_t const MONO_NS = tsc::get_ns() - tsc_ns_at_boot;
    uint64_t const BOOT_EPOCH_NS = epoch_sec_at_boot * NS_PER_SEC;
    int64_t const NTP_NS = ntp_delta_ns.load(std::memory_order_relaxed);
    return BOOT_EPOCH_NS + MONO_NS + static_cast<uint64_t>(NTP_NS);
}

void adjust_offset_ns(int64_t delta_ns) { ntp_delta_ns.fetch_add(delta_ns, std::memory_order_relaxed); }

}  // namespace ker::mod::rtc
