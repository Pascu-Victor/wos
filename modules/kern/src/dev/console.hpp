#pragma once

#include "device.hpp"

namespace ker::dev::console {

// Initialize console devices (serial and VGA)
void console_init();

// Get console device instances
Device* get_serial_console();
Device* get_vga_console();
Device* get_console();  // Main console (currently points to serial)

}  // namespace ker::dev::console
