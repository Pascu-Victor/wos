#pragma once

#include <array>
#include <cstdint>

#include "netd/dhcp.hpp"

namespace netd {

auto get_mac(int sock, const char* ifname, std::array<uint8_t, 6>& mac) -> bool;
auto apply_lease(const char* ifname, const DhcpLease& lease) -> bool;

}  // namespace netd
