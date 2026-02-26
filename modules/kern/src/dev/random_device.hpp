#pragma once

#include "device.hpp"

namespace ker::dev::random_device {

// Initialize /dev/urandom device (uses RDRAND instruction)
void random_device_init();

Device* get_urandom_device();

}  // namespace ker::dev::random_device
