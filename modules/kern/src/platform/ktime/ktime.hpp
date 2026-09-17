#pragma once

#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/arithmetic.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/task.hpp>
#include <platform/tsc/tsc.hpp>

namespace ker::mod::time {

void init();

// HPET-backed helpers (use only where HPET precision is required, e.g. early boot calibration)
inline uint64_t get_ticks() { return hpet::get_ticks(); }
inline uint64_t get_hpet_us() { return hpet::get_us(); }
inline void sleep_ticks(uint64_t ticks) { hpet::sleep_ticks(ticks); }
inline void sleep_us(uint64_t us) { hpet::sleep_us(us); }
inline void sleep(uint64_t ms) { hpet::sleep_us(arithmetic::mul_div(ms, 1000, 1)); }

// Qualified monotonic source (TSC or explicit HPET fallback).
inline uint64_t get_monotonic_ns() { return tsc::get_ns(); }
inline uint64_t get_us() { return tsc::get_ns() / 1000; }
inline uint64_t get_ms() { return tsc::get_ns() / 1000000; }

// Disciplined wall-clock epoch nanoseconds.
inline uint64_t get_epoch_ns() { return rtc::get_epoch_ns(); }

}  // namespace ker::mod::time
