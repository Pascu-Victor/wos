#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace netd {

constexpr size_t IPV6_ADDRESS_BYTES = 16;
constexpr size_t MAX_RA_PREFIXES = 4;
constexpr size_t MAX_RA_OPTIONS = 32;

enum class RaParseError : uint8_t {
    NONE,
    TRUNCATED,
    BAD_VERSION,
    BAD_LENGTH,
    BAD_NEXT_HEADER,
    BAD_HOP_LIMIT,
    BAD_SOURCE,
    BAD_TYPE_CODE,
    TOO_MANY_OPTIONS,
    MALFORMED_OPTION,
    BAD_PREFIX,
};

struct RaPrefix {
    std::array<uint8_t, IPV6_ADDRESS_BYTES> prefix{};
    uint8_t prefix_len = 0;
    bool on_link = false;
    bool autonomous = false;
    uint32_t valid_lifetime_s = 0;
    uint32_t preferred_lifetime_s = 0;
};

struct RouterAdvertisement {
    std::array<uint8_t, IPV6_ADDRESS_BYTES> source{};
    std::array<uint8_t, IPV6_ADDRESS_BYTES> destination{};
    uint16_t router_lifetime_s = 0;
    uint32_t reachable_time_ms = 0;
    uint32_t retrans_timer_ms = 0;
    uint32_t mtu = 0;
    std::array<RaPrefix, MAX_RA_PREFIXES> prefixes{};
    size_t prefix_count = 0;
};

auto parse_router_advertisement(std::span<const uint8_t> packet, RouterAdvertisement& out) -> RaParseError;
auto make_slaac_address(const RaPrefix& prefix, std::span<const uint8_t, 6> mac, std::array<uint8_t, IPV6_ADDRESS_BYTES>& out) -> bool;
auto ipv6_is_link_local(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address) -> bool;
auto ipv6_link_local_usable(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address, uint8_t scope, uint32_t flags) -> bool;

struct RaManagedState {
    bool address_installed = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> address{};
    uint8_t prefix_len = 0;
    bool onlink_route_installed = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> onlink_prefix{};
    uint8_t onlink_prefix_len = 0;
    bool route_installed = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> router{};
};

struct RaUpdatePlan {
    bool delete_address = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> old_address{};
    uint8_t old_prefix_len = 0;
    bool set_address = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> address{};
    uint8_t prefix_len = 0;
    uint32_t preferred_lifetime_s = 0;
    uint32_t valid_lifetime_s = 0;
    bool no_prefix_route = false;
    bool delete_onlink_route = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> old_onlink_prefix{};
    uint8_t old_onlink_prefix_len = 0;
    bool set_onlink_route = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> onlink_prefix{};
    uint8_t onlink_prefix_len = 0;
    uint32_t onlink_lifetime_s = 0;
    bool set_mtu = false;
    uint32_t mtu = 0;
    bool delete_route = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> old_router{};
    bool set_route = false;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> router{};
    uint16_t router_lifetime_s = 0;
};

auto plan_ra_update(const RaManagedState& state, const RouterAdvertisement& advert, std::span<const uint8_t, 6> mac,
                    uint32_t interface_mtu_ceiling = 0, uint32_t current_interface_mtu = 0) -> RaUpdatePlan;

}  // namespace netd
