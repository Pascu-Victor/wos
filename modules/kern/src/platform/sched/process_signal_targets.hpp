#pragma once

#include <cstdint>

namespace ker::mod::sched {

// Called only for a fresh registry insertion. Existing registered tasks retain
// their wake/repark path. Providers remain available until their own stop phase.
[[nodiscard]] constexpr auto fresh_admission_allowed(bool process, bool daemon, bool shutdown, bool workers_stopping) -> bool {
    return !(process && shutdown) && !(daemon && workers_stopping);
}

// Ordinary exit cleanup remains ACTIVE, but WKI proxy cleanup can block while
// EXITING. Terminal shutdown conservatively counts every PROCESS until DEAD;
// it does not rely on the mutable proxy visibility or plain has_exited flags.
struct ProcessSignalTarget {
    uint64_t pid = 0;
    uint64_t process_pid = 0;
    bool active = false;
    bool exiting = false;
    bool process = false;
    bool visible = false;
};

[[nodiscard]] constexpr auto visible_process_signal_target(const ProcessSignalTarget& target, uint64_t excluded_pid,
                                                           uint64_t excluded_owner_pid, bool terminal_shutdown = false) -> bool {
    return (target.active || (terminal_shutdown && target.exiting)) && target.process && (terminal_shutdown || target.visible) &&
           target.pid != excluded_pid && target.process_pid != excluded_owner_pid && (terminal_shutdown || target.process_pid != 1);
}

}  // namespace ker::mod::sched
