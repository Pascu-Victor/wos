#pragma once

#include <net/netdevice.hpp>

namespace ker::util::netdevconf {

// Scan /etc/netdevs for the first NIC assigned to the given driver name.
// Returns nullptr if the config is missing or no matching entry exists.
// Callers should fall back to a hardcoded default when nullptr is returned.
auto find_device(const char* driver) -> net::NetDevice*;

}  // namespace ker::util::netdevconf
