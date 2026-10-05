#include "tsc.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/asm/msr.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/arithmetic.hpp>
#include <platform/ktime/clocksource_policy.hpp>
#include <platform/ktime/kvmclock.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::tsc {

namespace {

using log = ker::mod::dbg::logger<"tsc">;
namespace policy = time::clocksource;
using State = policy::State;

uint64_t tsc_hz = 0;
uint64_t tsc_base = 0;
uint64_t hpet_base_ns = 0;
uint64_t calibration_spread = 0;
bool invariant_tsc = false;
// Time initializes before global constructors. Keep all read-side atomics in
// constant initialization, including diagnostic counters.
constinit std::atomic<State> source_state{State::unavailable};
constinit std::atomic<uint64_t> monotonic_floor{0};
constinit std::atomic<uint64_t> watchdog_deviation{0};

uint64_t ordered_tsc() {
    uint32_t lo = 0;
    uint32_t hi = 0;
    asm volatile("lfence; rdtsc; lfence" : "=a"(lo), "=d"(hi) : : "memory");
    return (static_cast<uint64_t>(hi) << 32) | lo;
}

bool check_invariant() {
    uint32_t max_leaf = 0;
    uint32_t edx = 0;
    cpuid(0x80000000U, &max_leaf, &edx);
    if (max_leaf < 0x80000007U) {
        return false;
    }
    cpuid(0x80000007U, &max_leaf, &edx);
    return (edx & (1U << 8)) != 0;
}

}  // namespace

void init() {
    if (source_state.load(std::memory_order_acquire) != State::unavailable) {
        return;
    }

    uint32_t eax = 0;
    uint32_t edx = 0;
    cpuid(1, &eax, &edx);
    bool const HAS_TSC = (edx & CPUID_FEAT_EDX_TSC) != 0;
    invariant_tsc = HAS_TSC && check_invariant();

    uint64_t minimum = UINT64_MAX;
    uint64_t maximum = 0;
    if (invariant_tsc) {
        for (unsigned sample = 0; sample < 3; ++sample) {
            uint64_t const HPET_START = hpet::get_ns();
            uint64_t const TSC_START = ordered_tsc();
            hpet::sleep_us(10000);
            uint64_t const TSC_END = ordered_tsc();
            uint64_t const HPET_END = hpet::get_ns();
            uint64_t const HZ = TSC_END > TSC_START && HPET_END > HPET_START
                                    ? time::arithmetic::mul_div(TSC_END - TSC_START, 1000000000, HPET_END - HPET_START)
                                    : 0;
            minimum = std::min(HZ, minimum);
            maximum = std::max(HZ, maximum);
        }
        calibration_spread = policy::spread_ppm(minimum, maximum);
    }

    State state = policy::qualify(HAS_TSC, invariant_tsc, minimum, maximum, smt::get_early_cpu_count());
    if (state == State::tsc || state == State::smp_unqualified) {
        tsc_hz = minimum + ((maximum - minimum) / 2);
    }
    if (state == State::tsc) {
        uint64_t const BEFORE = hpet::get_ns();
        tsc_base = ordered_tsc();
        uint64_t const AFTER = hpet::get_ns();
        hpet_base_ns = BEFORE + ((AFTER - BEFORE) / 2);
        if (AFTER - BEFORE > policy::MAX_DEVIATION_NS) {
            state = State::calibration_failed;
        }
    }
    source_state.store(state, std::memory_order_release);
    time::kvmclock::prepare(smt::get_early_cpu_count());
    log::info("clocksource=%s TSC=%lu MHz calibration_spread=%lu ppm", state_name(state), static_cast<unsigned long>(tsc_hz / 1000000),
              static_cast<unsigned long>(calibration_spread));
}

uint64_t get_hz() { return tsc_hz; }

uint64_t get_resolution_ns() {
    if (time::kvmclock::mode() == time::kvmclock::Mode::active) return 1;
    if (time::kvmclock::mode() == time::kvmclock::Mode::failed) return hpet::get_resolution_ns();
    State const STATE = source_state.load(std::memory_order_acquire);
    if (STATE == State::unavailable) {
        return 1;
    }
    return STATE == State::tsc ? time::arithmetic::mul_div(1, 1000000000, tsc_hz, true) : hpet::get_resolution_ns();
}

uint64_t ticks_to_ns(uint64_t delta) {
    if (tsc_hz == 0) {
        return 0;
    }
    return time::arithmetic::mul_div(delta, 1000000000ULL, tsc_hz);
}

uint64_t get_ns() {
    State const STATE = source_state.load(std::memory_order_acquire);
    if (STATE == State::unavailable) {
        return 0;
    }
    if (time::kvmclock::mode() == time::kvmclock::Mode::active) {
        uint64_t candidate = 0;
        if (time::kvmclock::read(candidate)) return policy::publish_max(monotonic_floor, candidate);
    }
    if (time::kvmclock::mode() == time::kvmclock::Mode::failed) {
        return policy::publish_max(monotonic_floor, time::kvmclock::fallback_hpet_time(hpet::get_ns()));
    }
    if (STATE != State::tsc) {
        return policy::publish_max(monotonic_floor, hpet::get_ns());
    }

    // The HPET bracket tolerates preemption and VM exits between samples without
    // mistaking that sampling delay for TSC drift. No logger on this IRQ path.
    uint64_t const BEFORE = hpet::get_ns();
    uint64_t const COUNTER = ordered_tsc();
    uint64_t const AFTER = hpet::get_ns();
    uint64_t const CANDIDATE = COUNTER < tsc_base ? 0 : time::arithmetic::saturating_add(hpet_base_ns, ticks_to_ns(COUNTER - tsc_base));
    uint64_t const DEVIATION = policy::deviation(CANDIDATE, BEFORE, AFTER);
    (void)policy::publish_max(watchdog_deviation, DEVIATION);
    if (COUNTER < tsc_base || DEVIATION > policy::MAX_DEVIATION_NS) {
        source_state.store(COUNTER < tsc_base ? State::tsc_backwards : State::tsc_drift, std::memory_order_release);
        return policy::publish_max(monotonic_floor, AFTER);
    }
    return policy::publish_max(monotonic_floor, CANDIDATE);
}

ClocksourceStatus get_status() {
    auto const KVM_MODE = time::kvmclock::mode();
    State const STATE = KVM_MODE == time::kvmclock::Mode::active   ? State::kvm_clock
                        : KVM_MODE == time::kvmclock::Mode::failed ? State::kvm_clock_failed
                                                                   : source_state.load(std::memory_order_acquire);
    if (STATE == State::unavailable) {
        return {.state = STATE,
                .invariant_tsc = false,
                .frequency_hz = 0,
                .resolution_ns = 0,
                .conversion_multiplier = 0,
                .conversion_divisor = 0,
                .calibration_spread_ppm = 0,
                .watchdog_deviation_ns = 0};
    }
    bool const TSC = STATE == State::tsc;
    bool const KVM = STATE == State::kvm_clock;
    return {.state = STATE,
            .invariant_tsc = invariant_tsc,
            .frequency_hz = KVM   ? 1000000000
                            : TSC ? tsc_hz
                                  : time::arithmetic::mul_div(1, 1000000000000000ULL, hpet::get_period_fs()),
            .resolution_ns = KVM   ? 1
                             : TSC ? time::arithmetic::mul_div(1, 1000000000, tsc_hz, true)
                                   : hpet::get_resolution_ns(),
            .conversion_multiplier = KVM   ? 1
                                     : TSC ? 1000000000
                                           : hpet::get_period_fs(),
            .conversion_divisor = KVM   ? 1
                                  : TSC ? tsc_hz
                                        : 1000000,
            .calibration_spread_ppm = calibration_spread,
            .watchdog_deviation_ns = std::max(watchdog_deviation.load(std::memory_order_relaxed), time::kvmclock::deviation()),
            .kvm_registered_cpus = time::kvmclock::registered(),
            .kvm_expected_cpus = time::kvmclock::expected(),
            .kvm_watchdog_checks = time::kvmclock::checks()};
}

const char* state_name(State state) {
    switch (state) {
        case State::unavailable:
            return "unavailable";
        case State::tsc:
            return "tsc";
        case State::missing_tsc:
            return "hpet:missing-tsc";
        case State::noninvariant_tsc:
            return "hpet:noninvariant-tsc";
        case State::calibration_failed:
            return "hpet:calibration-failed";
        case State::smp_unqualified:
            return "hpet:smp-unqualified";
        case State::tsc_backwards:
            return "hpet:tsc-backwards";
        case State::tsc_drift:
            return "hpet:tsc-drift";
        case State::kvm_clock:
            return "kvm-clock";
        case State::kvm_clock_failed:
            return "hpet:kvm-clock-failed";
    }
    return "unavailable";
}

}  // namespace ker::mod::tsc
