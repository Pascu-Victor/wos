#pragma once

#include "device.hpp"

namespace ker::dev::random_device {

// Initialize the kernel DRBG and register /dev/urandom. The device remains
// unavailable when the hardware-backed seed cannot be obtained.
auto random_device_init() -> void;

auto get_urandom_device() -> Device*;

}  // namespace ker::dev::random_device
