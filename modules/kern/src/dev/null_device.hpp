#pragma once

#include "device.hpp"

namespace ker::dev::null_device {

// Initialize /dev/null and /dev/zero devices
auto null_device_init() -> void;

auto get_null_device() -> Device*;
auto get_zero_device() -> Device*;

}  // namespace ker::dev::null_device
