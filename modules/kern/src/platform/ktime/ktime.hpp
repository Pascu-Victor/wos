#pragma once

#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/acpi/hpet/hpet.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/task.hpp>
#include <std/list.hpp>
#include <std/mem.hpp>

namespace ker::mod::time {

void init(void);

inline uint64_t getTicks(void) { return hpet::getTicks(); }

inline uint64_t getUs(void) { return hpet::getUs(); }

inline uint64_t getMs(void) { return getUs() / 1000; }

inline void sleepTicks(uint64_t ticks) { hpet::sleepTicks(ticks); }

inline void sleepUs(uint64_t us) { hpet::sleepUs(us); }

inline void sleep(uint64_t ms) { hpet::sleepUs(ms * 1000); }

void pushTask(uint64_t ticks, void (*task)(gates::interruptFrame *), void *arg);
}  // namespace ker::mod::time
