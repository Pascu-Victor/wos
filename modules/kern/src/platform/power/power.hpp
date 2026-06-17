#pragma once

#include <cstdint>

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
[[nodiscard]] auto begin_reboot_command(uint64_t cmd) -> int;

}  // namespace ker::mod::power
