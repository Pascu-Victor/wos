#pragma once

#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/task.hpp>
#include <platform/tsc/tsc.hpp>
#include <util/list.hpp>
#include <util/mem.hpp>

namespace ker::mod::time {

void init(void);

// HPET-backed helpers
inline uint64_t getTicks(void) { return hpet::getTicks(); }
inline uint64_t getUs(void) { return hpet::getUs(); }
inline uint64_t getMs(void) { return getUs() / 1000; }
inline void sleepTicks(uint64_t ticks) { hpet::sleepTicks(ticks); }
inline void sleepUs(uint64_t us) { hpet::sleepUs(us); }
inline void sleep(uint64_t ms) { hpet::sleepUs(ms * 1000); }

// TSC-backed monotonic nanoseconds
inline uint64_t getMonotonicNs(void) { return tsc::getNs(); }

// RTC + TSC epoch nanoseconds
inline uint64_t getEpochNs(void) { return rtc::getEpochNs(); }

void pushTask(uint64_t ticks, void (*task)(gates::interruptFrame*), void* arg);
}  // namespace ker::mod::time
