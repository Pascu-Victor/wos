#include "tsc.hpp"

#include <cstdint>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::mod::tsc {

static uint64_t tsc_hz = 0;
static uint64_t tsc_base = 0;

static bool check_invariant() {
    uint32_t eax_out = 0;
    uint32_t edx_out = 0;
    cpuid(0x80000007U, &eax_out, &edx_out);
    return (edx_out & (1U << 8)) != 0;
}

void init() {
    if (check_invariant()) {
        dbg::log("tsc: invariant TSC detected");
    } else {
        dbg::log("tsc: invariant TSC not reported by CPUID (proceeding)");
    }

    // Calibrate TSC frequency against HPET over a 10 ms window.
    // hpet::sleepUs uses the HPET counter directly so it is accurate
    // regardless of CPU frequency.
    uint64_t const HPET_START = hpet::get_us();
    uint64_t const TSC_START = rdtsc();
    hpet::sleep_us(10000);  // 10 ms
    uint64_t const TSC_DELTA = rdtsc() - TSC_START;
    uint64_t const ELAPSED_US = hpet::get_us() - HPET_START;

    if (ELAPSED_US == 0) {
        dbg::log("tsc: HPET calibration window returned 0 us; defaulting to 1 GHz");
        tsc_hz = 1000000000ULL;
    } else {
        tsc_hz = (TSC_DELTA * 1000000ULL) / ELAPSED_US;
    }

    tsc_base = rdtsc();
    dbg::log("tsc: calibrated at %lu MHz", static_cast<unsigned long>(tsc_hz / 1000000ULL));
}

uint64_t get_hz() { return tsc_hz; }

uint64_t ticks_to_ns(uint64_t delta) {
    if (tsc_hz == 0) {
        return 0;
    }

    uint64_t const SECS = delta / tsc_hz;
    uint64_t const REM = delta % tsc_hz;
    return (SECS * 1000000000ULL) + ((REM * 1000000000ULL) / tsc_hz);
}

uint64_t get_ns() { return ticks_to_ns(rdtsc() - tsc_base); }

}  // namespace ker::mod::tsc
