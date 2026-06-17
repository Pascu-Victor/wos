#pragma once

#include <cstdint>

enum class ShutdownAction : uint8_t {
    NONE,
    REBOOT,
    POWEROFF,
    HALT,
};

void shutdown_init();
auto shutdown_poll() -> ShutdownAction;
auto shutdown_reboot_cmd(ShutdownAction action) -> int;
void shutdown_perform(ShutdownAction action);
