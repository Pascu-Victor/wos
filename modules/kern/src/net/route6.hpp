#pragma once

#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>

namespace ker::net {

constexpr size_t MAX_IPV6_ROUTES = 64;

constexpr uint32_t IPV6_ROUTE_F_GATEWAY = 0x00000001U;
constexpr uint32_t IPV6_ROUTE_F_AUTOCONF = 0x00000002U;
constexpr uint32_t IPV6_ROUTE_F_CONNECTED = 0x80000000U;

struct IPv6RouteSpec {
    proto::IPv6Address prefix{};
    proto::IPv6Address gateway{};
    uint8_t prefix_len{};
    uint32_t metric{};
    uint32_t flags{};
    uint64_t expires_at_ms{UINT64_MAX};
    NetDeviceIdentity dev_identity{};
};

struct IPv6RouteSnapshot {
    proto::IPv6Address prefix{};
    proto::IPv6Address gateway{};
    uint8_t prefix_len{};
    uint32_t metric{};
    uint32_t flags{};
    uint64_t expires_at_ms{UINT64_MAX};
    uint32_t ifindex{};
    NetDeviceIdentity dev_identity{};
};

auto route6_add(const IPv6RouteSpec& spec) -> int;
auto route6_del(const IPv6RouteSpec& match) -> int;
auto route6_del_for_dev(NetDeviceIdentity identity) -> size_t;
auto route6_add_connected(NetDeviceIdentity identity, const proto::IPv6Address& address, uint8_t prefix_len) -> int;
auto route6_del_connected(NetDeviceIdentity identity, const proto::IPv6Address& address, uint8_t prefix_len) -> int;
// Returns the total number of live routes and copies at most capacity rows.
auto route6_snapshot(IPv6RouteSnapshot* out, size_t capacity) -> size_t;
void route6_expire(uint64_t now_ms);
void route6_init();

// Internal lookup used by ipv6_route_resolve. The selected device is retained
// before this function returns, so a stale generation is never observable.
auto route6_lookup(const proto::IPv6Address& dst, uint32_t bound_ifindex, IPv6RouteSnapshot& out, NetDeviceRef& device) -> int;

}  // namespace ker::net
