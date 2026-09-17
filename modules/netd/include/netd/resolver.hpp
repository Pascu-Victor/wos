#pragma once

#include "netd/dhcp.hpp"

namespace netd {

void write_resolv_conf(const DhcpLease& lease);
// Directory descriptor is retained across validation, creation, and publication.
auto write_resolv_conf_at(int directory, const DhcpLease& lease) -> int;

}  // namespace netd
