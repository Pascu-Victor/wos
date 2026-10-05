#pragma once

#include <platform/ktime/kvmclock_policy.hpp>

namespace ker::mod::time::kvmclock {
void prepare(uint64_t cpus);
void register_current_cpu(uint64_t cpu, bool bootstrap = false);
[[nodiscard]] auto read(uint64_t& result) -> bool;
void watchdog_tick();
[[nodiscard]] auto mode() -> Mode;
[[nodiscard]] auto registered() -> uint64_t;
[[nodiscard]] auto expected() -> uint64_t;
[[nodiscard]] auto deviation() -> uint64_t;
[[nodiscard]] auto checks() -> uint64_t;
[[nodiscard]] auto fallback_hpet_time(uint64_t reference) -> uint64_t;
}  // namespace ker::mod::time::kvmclock
