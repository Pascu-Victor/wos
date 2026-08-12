#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net {

constexpr size_t MAX_ADDRS_PER_IF = 8;
constexpr size_t MAX_NET_INTERFACES = MAX_NET_DEVICES;

struct IPv4Addr {
    proto::IPv4Address addr;
    proto::IPv4Address netmask;
};

struct IPv6Addr {
    proto::IPv6Address addr;
    uint8_t prefix_len{};
    enum class State : uint8_t { TENTATIVE, PREFERRED, DEPRECATED, DADFAILED };
    State state = State::TENTATIVE;
    uint32_t flags{};
    // Absolute monotonic deadlines. UINT64_MAX means no expiry; zero expires
    // at the next bounded address timer tick.
    uint64_t preferred_until_ms{UINT64_MAX};
    uint64_t valid_until_ms{UINT64_MAX};
    uint64_t dad_deadline_ms{};
    uint8_t dad_probes_sent{};
};

// These values are also exposed by the append-only netctl IPv6 ABI.
constexpr uint32_t IPV6_ADDR_F_DADFAILED = 0x00000008U;
constexpr uint32_t IPV6_ADDR_F_DEPRECATED = 0x00000020U;
constexpr uint32_t IPV6_ADDR_F_TENTATIVE = 0x00000040U;
constexpr uint32_t IPV6_ADDR_F_PERMANENT = 0x00000080U;
constexpr uint32_t IPV6_ADDR_F_NOPREFIXROUTE = 0x00000200U;
constexpr uint32_t IPV6_ADDR_F_AUTOCONF = 0x00010000U;
constexpr uint32_t IPV6_ADDR_F_NODAD = 0x00020000U;

struct IPv6DadProbe {
    NetDeviceIdentity dev_identity{};
    NetDeviceRef device{};
    proto::IPv6Address target{};
};

struct NetInterface {
    NetDevice* dev = nullptr;
    NetDeviceIdentity dev_identity{};
    std::array<IPv4Addr, MAX_ADDRS_PER_IF> ipv4_addrs = {};
    size_t ipv4_addr_count = 0;
    std::array<IPv6Addr, MAX_ADDRS_PER_IF> ipv6_addrs = {};
    size_t ipv6_addr_count = 0;
    uint64_t ipv6_addr_generation = 0;
    mod::sys::Spinlock ipv6_addr_lock;
};

// Get or create interface config for a device. Returned interfaces remain at a
// permanent address after registry deletion and are never reused.
auto netif_get(NetDevice* dev) -> NetInterface*;
// Find existing interface config without allocating or publishing a new row.
// Packet/RX and read-only inspection paths must use this form.
auto netif_find_by_dev(NetDevice* dev) -> NetInterface*;
auto netif_del_for_dev(NetDevice* dev) -> bool;

// Add addresses
auto netif_add_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int;
auto netif_set_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask, bool replace) -> int;
auto netif_del_ipv4(NetDevice* dev, proto::IPv4Address addr, proto::IPv4Address mask) -> int;
auto netif_add_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix) -> int;
auto netif_set_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix, uint32_t flags, uint64_t preferred_until_ms,
                    uint64_t valid_until_ms, bool replace) -> int;
auto netif_del_ipv6(NetDevice* dev, const proto::IPv6Address& addr, uint8_t prefix) -> int;

// Copy-only inspection APIs. Callers never retain a reference into the
// mutable per-interface address array after its IRQ-safe lock is released.
auto netif_ipv6_snapshot(NetDevice* dev, IPv6Addr* out, size_t capacity) -> size_t;
auto netif_ipv6_find(NetDevice* dev, const proto::IPv6Address& addr, IPv6Addr& out) -> bool;
auto netif_ipv6_find_owner(const proto::IPv6Address& addr, IPv6Addr& out, NetDeviceRef& device) -> bool;
auto netif_ipv6_select_source(NetDevice* dev, const proto::IPv6Address& dst, const proto::IPv6Address* requested, proto::IPv6Address& out)
    -> int;
auto netif_ipv6_dad_failed(NetDeviceIdentity identity, const proto::IPv6Address& addr) -> bool;
auto netif_ipv6_timer_tick(uint64_t now_ms, IPv6DadProbe* probes, size_t capacity) -> size_t;

// Lookup: find interface that owns a given IPv4 address
auto netif_find_by_ipv4(proto::IPv4Address addr) -> NetInterface*;

// Lookup: find interface that owns a given IPv6 address
auto netif_find_by_ipv6(const proto::IPv6Address& addr) -> NetInterface*;

}  // namespace ker::net
