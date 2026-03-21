#include "tsc.hpp"

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
    uint64_t hpet_start = hpet::getUs();
    uint64_t tsc_start = rdtsc();
    hpet::sleepUs(10000);  // 10 ms
    uint64_t tsc_delta = rdtsc() - tsc_start;
    uint64_t elapsed_us = hpet::getUs() - hpet_start;

    if (elapsed_us == 0) {
        dbg::log("tsc: HPET calibration window returned 0 us; defaulting to 1 GHz");
        tsc_hz = 1000000000ULL;
    } else {
        tsc_hz = (tsc_delta * 1000000ULL) / elapsed_us;
    }

    tsc_base = rdtsc();
    dbg::log("tsc: calibrated at %lu MHz", (unsigned long)(tsc_hz / 1000000ULL));
}

uint64_t getHz() { return tsc_hz; }

uint64_t ticksToNs(uint64_t delta) {
    if (tsc_hz == 0) {
        return 0;
    }

    uint64_t secs = delta / tsc_hz;
    uint64_t rem = delta % tsc_hz;
    return (secs * 1000000000ULL) + ((rem * 1000000000ULL) / tsc_hz);
}

uint64_t getNs() { return ticksToNs(rdtsc() - tsc_base); }

}  // namespace ker::mod::tsc
