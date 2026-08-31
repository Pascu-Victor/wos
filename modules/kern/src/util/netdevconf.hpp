#pragma once

#include <cstddef>
#include <net/netdevice.hpp>
#include <span>

namespace ker::util::netdevconf {

// Scan /etc/netdevs for NICs assigned to the given driver name. Missing
// devices are skipped. Returns the number of retained references written to
// `devices`, in configuration order.
auto find_devices(const char* driver, std::span<net::NetDeviceRef> devices) -> size_t;

}  // namespace ker::util::netdevconf
