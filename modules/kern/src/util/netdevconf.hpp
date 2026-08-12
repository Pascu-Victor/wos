#pragma once

#include <net/netdevice.hpp>

namespace ker::util::netdevconf {

// Scan /etc/netdevs for the first NIC assigned to the given driver name.
// Returns an empty reference if the config is missing or no matching entry
// exists. Callers keep the reference alive while consuming the assignment.
auto find_device(const char* driver) -> net::NetDeviceRef;

}  // namespace ker::util::netdevconf
