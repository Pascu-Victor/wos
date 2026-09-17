#include "rtc.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <mod/io/port/port.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/arithmetic.hpp>
#include <platform/sys/spinlock.hpp>
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
WallClock clock_state{};
sys::Spinlock clock_lock;

// ---------------------------------------------------------------------------
// CMOS helpers
// ---------------------------------------------------------------------------

uint8_t cmos_read(uint8_t reg) {
    outb(CMOS_INDEX, static_cast<uint8_t>(0x80U | reg));  // NMI disable + register
    io_wait();
    return inb(CMOS_DATA);
}

// Wait until the RTC "update in progress" flag (Status A bit 7) is clear.
auto wait_rtc_ready() -> bool {
    for (int i = 0; i < 100000; ++i) {
        if ((cmos_read(RTC_STA) & 0x80U) == 0) {
            return true;
        }
        asm volatile("pause");
    }
    return false;
}

struct CmosSnapshot {
    uint8_t second{}, minute{}, hour{}, day{}, month{}, year{}, century{}, status{};
    auto operator==(const CmosSnapshot&) const -> bool = default;
};

auto read_snapshot(CmosSnapshot& out) -> bool {
    if (!wait_rtc_ready()) return false;
    out = {cmos_read(RTC_SEC),   cmos_read(RTC_MIN),  cmos_read(RTC_HOUR),    cmos_read(RTC_DAY),
           cmos_read(RTC_MONTH), cmos_read(RTC_YEAR), cmos_read(RTC_CENTURY), cmos_read(RTC_STB)};
    return (cmos_read(RTC_STA) & 0x80U) == 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void init() {
    clock_state.anchor_mono_ns = tsc::get_ns();
    CmosSnapshot first{}, sample{};
    bool stable = false;
    for (unsigned attempt = 0; attempt < 4; ++attempt) {
        if (read_snapshot(first) && read_snapshot(sample) && first == sample) {
            stable = true;
            break;
        }
    }
    // Restore NMI delivery after the boot-only CMOS access sequence.
    outb(CMOS_INDEX, RTC_STA);
    if (!stable) {
        log::warn("RTC unstable or unavailable; wall clock starts at Unix epoch");
        return;
    }
    clock_state.anchor_mono_ns = tsc::get_ns();
    auto const [SEC_RAW, MIN_RAW, HOUR_RAW, DAY_RAW, MONTH_RAW, YEAR_RAW, CENTURY_RAW, STB] = sample;

    // Decode BCD if Status Register B bit 2 is 0.
    bool const BINARY_MODE = (STB & 0x04U) != 0;
    auto bcd = [&](uint8_t v) -> int {
        if (BINARY_MODE) return v;
        if ((v & 0x0FU) > 9 || (v >> 4) > 9) return -1;
        return static_cast<int>((v & 0x0FU) + ((v >> 4) * 10));
    };

    int const SEC = bcd(SEC_RAW);
    int const MIN = bcd(MIN_RAW);
    int hour = bcd(HOUR_RAW & 0x7FU);  // strip 12/24 PM bit before BCD decode
    int const DAY = bcd(DAY_RAW);
    int const MONTH = bcd(MONTH_RAW);
    int year = bcd(YEAR_RAW);
    int const CENT = bcd(CENTURY_RAW);

    // Handle 12-hour mode: bit 7 of hour register indicates PM.
    bool const MODE_24H = (STB & 0x02U) != 0;
    if (!MODE_24H && (hour < 1 || hour > 12)) {
        log::warn("invalid RTC 12-hour value; wall clock starts at Unix epoch");
        return;
    }
    if (!MODE_24H) {
        bool const PM = (HOUR_RAW & 0x80U) != 0;
        if (hour == 12) {
            hour = PM ? 12 : 0;
        } else if (PM) {
            hour += 12;
        }
    }

    if (year < 0 || year > 99) {
        log::warn("invalid RTC year; wall clock starts at Unix epoch");
        return;
    }
    // Determine century.
    if (CENT >= 20 && CENT <= 21) {
        year += CENT * 100;
    } else {
        // Century register absent or unreliable; assume 21st century.
        year += (year < 70) ? 2000 : 1900;
    }

    uint64_t epoch_sec_at_boot = 0;
    if (!calendar_epoch(year, MONTH, DAY, hour, MIN, SEC, epoch_sec_at_boot)) {
        log::warn("invalid RTC calendar; wall clock starts at Unix epoch");
        return;
    }
    clock_state.anchor_epoch_ns = time::arithmetic::mul_div(epoch_sec_at_boot, NS_PER_SEC, 1);
    clock_state.rtc_valid = true;

    log::info("wall clock %d-%02d-%02d %02d:%02d:%02d UTC (epoch %lu)", year, MONTH, DAY, hour, MIN, SEC,
              static_cast<unsigned long>(epoch_sec_at_boot));
}

auto get_epoch_sec() -> uint64_t { return get_epoch_ns() / NS_PER_SEC; }

auto get_epoch_ns() -> uint64_t {
    auto const FLAGS = clock_lock.lock_irqsave();
    uint64_t const RESULT = clock_state.epoch(tsc::get_ns());
    clock_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

auto adjust_offset_ns(int64_t delta_ns) -> bool {
    auto const FLAGS = clock_lock.lock_irqsave();
    bool const ACCEPTED = clock_state.adjust(tsc::get_ns(), delta_ns);
    clock_lock.unlock_irqrestore(FLAGS);
    return ACCEPTED;
}

auto status() -> WallClock {
    auto const FLAGS = clock_lock.lock_irqsave();
    auto const RESULT = clock_state;
    clock_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

}  // namespace ker::mod::rtc
