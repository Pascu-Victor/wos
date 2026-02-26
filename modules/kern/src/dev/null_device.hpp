#pragma once

#include "device.hpp"

namespace ker::dev::null_device {

// Initialize /dev/null and /dev/zero devices
void null_device_init();

Device* get_null_device();
Device* get_zero_device();

}  // namespace ker::dev::null_device
