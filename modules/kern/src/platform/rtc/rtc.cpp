#include "rtc.hpp"

#include <mod/io/port/port.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/tsc/tsc.hpp>

// CMOS port pair: write index to 0x70, read data from 0x71.
// Setting bit 7 of the index byte disables NMI delivery while accessing CMOS.
static constexpr uint16_t CMOS_INDEX = 0x70;
static constexpr uint16_t CMOS_DATA  = 0x71;

// CMOS register indices
static constexpr uint8_t RTC_SEC     = 0x00;
static constexpr uint8_t RTC_MIN     = 0x02;
static constexpr uint8_t RTC_HOUR    = 0x04;
static constexpr uint8_t RTC_DAY     = 0x07;
static constexpr uint8_t RTC_MONTH   = 0x08;
static constexpr uint8_t RTC_YEAR    = 0x09;
static constexpr uint8_t RTC_STA     = 0x0A;  // Status Register A
static constexpr uint8_t RTC_STB     = 0x0B;  // Status Register B
static constexpr uint8_t RTC_CENTURY = 0x32;  // May not be present on all hardware

namespace ker::mod::rtc {

// Epoch seconds read from CMOS at boot, plus any NTP correction.
static uint64_t epoch_sec_at_boot  = 0;
// TSC nanoseconds snapshot taken at the moment we read the RTC.
static uint64_t tsc_ns_at_boot     = 0;
// Signed NTP correction applied by setOffset().
static int64_t  ntp_delta_sec      = 0;

// ---------------------------------------------------------------------------
// CMOS helpers
// ---------------------------------------------------------------------------

static uint8_t cmos_read(uint8_t reg) {
    outb(CMOS_INDEX, static_cast<uint8_t>(0x80U | reg));  // NMI disable + register
    io_wait();
    return inb(CMOS_DATA);
}

// Wait until the RTC "update in progress" flag (Status A bit 7) is clear.
static void wait_rtc_ready() {
    for (int i = 0; i < 100000; ++i) {
        if ((cmos_read(RTC_STA) & 0x80U) == 0) {
            return;
        }
        asm volatile("pause");
    }
    // Timed out – proceed anyway; worst case we read a slightly corrupt value.
}

// ---------------------------------------------------------------------------
// Calendar conversion: calendar date + time → Unix epoch seconds
// ---------------------------------------------------------------------------

static bool is_leap_year(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

static const uint8_t DAYS_IN_MONTH[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static uint64_t ymd_hms_to_epoch(int year, int month, int day, int hour, int min, int sec) {
    uint64_t days = 0;

    // Accumulate whole years from 1970.
    for (int y = 1970; y < year; ++y) {
        days += is_leap_year(y) ? 366U : 365U;
    }

    // Accumulate whole months in the current year.
    for (int m = 1; m < month; ++m) {
        days += DAYS_IN_MONTH[m - 1];
        if (m == 2 && is_leap_year(year)) {
            days += 1;
        }
    }

    days += static_cast<uint64_t>(day - 1);

    return days * 86400ULL
         + static_cast<uint64_t>(hour) * 3600ULL
         + static_cast<uint64_t>(min)  * 60ULL
         + static_cast<uint64_t>(sec);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void init() {
    // Snapshot the TSC monotonic time first so that epoch_sec_at_boot and
    // tsc_ns_at_boot refer to the same instant.
    tsc_ns_at_boot = tsc::getNs();

    wait_rtc_ready();

    uint8_t sec_raw     = cmos_read(RTC_SEC);
    uint8_t min_raw     = cmos_read(RTC_MIN);
    uint8_t hour_raw    = cmos_read(RTC_HOUR);
    uint8_t day_raw     = cmos_read(RTC_DAY);
    uint8_t month_raw   = cmos_read(RTC_MONTH);
    uint8_t year_raw    = cmos_read(RTC_YEAR);
    uint8_t century_raw = cmos_read(RTC_CENTURY);
    uint8_t stb         = cmos_read(RTC_STB);

    // Decode BCD if Status Register B bit 2 is 0.
    bool binary_mode = (stb & 0x04U) != 0;
    auto bcd = [&](uint8_t v) -> int {
        return binary_mode ? v : static_cast<int>((v & 0x0FU) + (v >> 4) * 10);
    };

    int sec   = bcd(sec_raw);
    int min   = bcd(min_raw);
    int hour  = bcd(hour_raw & 0x7FU);  // strip 12/24 PM bit before BCD decode
    int day   = bcd(day_raw);
    int month = bcd(month_raw);
    int year  = bcd(year_raw);
    int cent  = bcd(century_raw);

    // Handle 12-hour mode: bit 7 of hour register indicates PM.
    bool mode_24h = (stb & 0x02U) != 0;
    if (!mode_24h) {
        bool pm = (hour_raw & 0x80U) != 0;
        if (hour == 12) {
            hour = pm ? 12 : 0;
        } else if (pm) {
            hour += 12;
        }
    }

    // Determine century.
    if (cent >= 20 && cent <= 21) {
        year += cent * 100;
    } else {
        // Century register absent or unreliable; assume 21st century.
        year += (year < 70) ? 2000 : 1900;
    }

    epoch_sec_at_boot = ymd_hms_to_epoch(year, month, day, hour, min, sec);

    dbg::log("rtc: wall clock %d-%02d-%02d %02d:%02d:%02d UTC (epoch %lu)",
             year, month, day, hour, min, sec, (unsigned long)epoch_sec_at_boot);
}

uint64_t getEpochSec() {
    uint64_t mono_sec = (tsc::getNs() - tsc_ns_at_boot) / 1000000000ULL;
    return epoch_sec_at_boot + mono_sec + static_cast<uint64_t>(ntp_delta_sec);
}

uint64_t getEpochNs() {
    uint64_t mono_ns       = tsc::getNs() - tsc_ns_at_boot;
    uint64_t boot_epoch_ns = epoch_sec_at_boot * 1000000000ULL;
    int64_t  ntp_ns        = ntp_delta_sec * static_cast<int64_t>(1000000000LL);
    return boot_epoch_ns + mono_ns + static_cast<uint64_t>(ntp_ns);
}

void setOffset(int64_t delta_sec) {
    ntp_delta_sec = delta_sec;
}

}  // namespace ker::mod::rtc
