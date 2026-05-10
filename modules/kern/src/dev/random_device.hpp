#pragma once

#include "device.hpp"

namespace ker::dev::random_device {

// Initialize /dev/urandom device (uses RDRAND instruction)
auto random_device_init() -> void;

auto get_urandom_device() -> Device*;

}  // namespace ker::dev::random_device
