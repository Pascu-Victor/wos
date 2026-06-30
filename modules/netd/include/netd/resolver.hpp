#pragma once

#include "netd/dhcp.hpp"

namespace netd {

void write_resolv_conf(const DhcpLease& lease);

}  // namespace netd
