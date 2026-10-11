#pragma once

#include <cstdint>

#ifdef WOS_SELFTEST
namespace ker::mod::sched::task {
struct Task;
}
#endif

namespace ker::mod::power {

// NOLINTNEXTLINE(performance-enum-size)
enum class ShutdownAction : uint64_t {
    NONE = 0,
    REBOOT = 1,
    POWEROFF = 2,
    HALT = 3,
};

// NOLINTNEXTLINE(performance-enum-size)
enum class ShutdownPhase : uint64_t {
    IDLE = 0,
    FINALIZING = 1,
    PROCESS_TEARDOWN = 2,
    VFS_SYNC = 3,
    ROOT_UNMOUNT = 4,
    CPU_QUIESCE = 5,
    PLATFORM_ACTION = 6,
    PREPARING = 7,
};

[[nodiscard]] auto shutdown_in_progress() -> bool;
[[nodiscard]] auto shutdown_phase() -> ShutdownPhase;
[[nodiscard]] auto prepare_shutdown() -> int;
// Normal task entry without preemption-disabled ownership or a pending handoff.
// IF may be clear; an accepted non-returning finalizer enables completion IRQs.
[[nodiscard]] auto begin_reboot_command(uint64_t cmd) -> int;
#ifdef WOS_SELFTEST
// Borrow an unpublished diagnostic subject; share ordinary authorization and
// effect implementations without replacing the executing scheduler context.
[[nodiscard]] auto selftest_prepare_shutdown_for_subject(sched::task::Task* subject) -> int;
[[nodiscard]] auto selftest_begin_reboot_for_subject(sched::task::Task* subject, uint64_t cmd) -> int;
[[noreturn]] void selftest_poweroff();
// Task-context live diagnostics use the ordinary finalizer, including VFS and
// driver retirement. No userspace entrypoint or authority bypass is exposed.
[[noreturn]] void selftest_shutdown();
#endif

}  // namespace ker::mod::power
