#pragma once

namespace netd {

auto run_dhcp_client(const char* ifname) -> int;
auto run_network_daemon() -> int;

}  // namespace netd
