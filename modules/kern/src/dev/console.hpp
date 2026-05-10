#pragma once

#include "device.hpp"

namespace ker::dev::console {

// Initialize console devices (serial and VGA)
auto console_init() -> void;

// Get console device instances
auto get_serial_console() -> Device*;
auto get_vga_console() -> Device*;
auto get_console() -> Device*;  // Main console (currently points to serial)

}  // namespace ker::dev::console
