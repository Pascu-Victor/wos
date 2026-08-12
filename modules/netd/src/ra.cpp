#include "netd/ra.hpp"

#include <algorithm>
#include <cstring>

namespace netd {
namespace {

constexpr size_t IPV6_HEADER_SIZE = 40;
constexpr size_t IPV6_PAYLOAD_LENGTH_OFFSET = 4;
constexpr size_t IPV6_NEXT_HEADER_OFFSET = 6;
constexpr size_t IPV6_HOP_LIMIT_OFFSET = 7;
constexpr size_t IPV6_SOURCE_OFFSET = 8;
constexpr size_t IPV6_DESTINATION_OFFSET = 24;
constexpr uint8_t IPPROTO_ICMPV6_VALUE = 58;
constexpr uint8_t ICMPV6_ROUTER_ADVERT = 134;
constexpr size_t RA_FIXED_SIZE = 16;
constexpr size_t RA_OPTIONS_OFFSET = IPV6_HEADER_SIZE + RA_FIXED_SIZE;
constexpr uint8_t ND_OPT_PREFIX_INFORMATION = 3;
constexpr uint8_t ND_OPT_MTU = 5;
constexpr uint8_t ND_OPT_PREFIX_INFORMATION_LENGTH = 4;
constexpr uint8_t ND_OPT_MTU_LENGTH = 1;
constexpr uint8_t PREFIX_FLAG_ON_LINK = 0x80;
constexpr uint8_t PREFIX_FLAG_AUTONOMOUS = 0x40;
constexpr uint32_t IPV6_MINIMUM_MTU = 1280;
constexpr uint32_t IPV6_ADDR_F_DADFAILED = 0x00000008U;
constexpr uint32_t IPV6_ADDR_F_TENTATIVE = 0x00000040U;

auto load_be16(const uint8_t* data) -> uint16_t { return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8U) | data[1]); }

auto load_be32(const uint8_t* data) -> uint32_t {
    return (static_cast<uint32_t>(data[0]) << 24U) | (static_cast<uint32_t>(data[1]) << 16U) | (static_cast<uint32_t>(data[2]) << 8U) |
           data[3];
}

auto ipv6_is_multicast(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address) -> bool { return address[0] == 0xFF; }

auto ipv6_is_unspecified(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address) -> bool {
    return std::ranges::all_of(address, [](uint8_t byte) { return byte == 0; });
}

auto prefix_is_usable_for_slaac(const RaPrefix& prefix) -> bool {
    return prefix.autonomous && prefix.prefix_len == 64 && !ipv6_is_multicast(prefix.prefix) && !ipv6_is_link_local(prefix.prefix) &&
           !ipv6_is_unspecified(prefix.prefix);
}

auto prefix_is_usable_on_link(const RaPrefix& prefix) -> bool {
    return prefix.on_link && !ipv6_is_multicast(prefix.prefix) && !ipv6_is_link_local(prefix.prefix) && !ipv6_is_unspecified(prefix.prefix);
}

auto masked_prefix(const RaPrefix& prefix) -> std::array<uint8_t, IPV6_ADDRESS_BYTES> {
    auto result = prefix.prefix;
    size_t const FULL_BYTES = prefix.prefix_len / 8U;
    uint8_t const REMAINING_BITS = prefix.prefix_len % 8U;
    size_t clear_from = FULL_BYTES;
    if (REMAINING_BITS != 0) {
        result[FULL_BYTES] &= static_cast<uint8_t>(0xFFU << (8U - REMAINING_BITS));
        clear_from++;
    }
    for (size_t i = clear_from; i < result.size(); ++i) {
        result[i] = 0;
    }
    return result;
}

}  // namespace

auto ipv6_is_link_local(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address) -> bool {
    return address[0] == 0xFE && (address[1] & 0xC0U) == 0x80U;
}

auto ipv6_link_local_usable(std::span<const uint8_t, IPV6_ADDRESS_BYTES> address, uint8_t scope, uint32_t flags) -> bool {
    return ipv6_is_link_local(address) && scope == 253 && (flags & (IPV6_ADDR_F_TENTATIVE | IPV6_ADDR_F_DADFAILED)) == 0;
}

auto parse_router_advertisement(std::span<const uint8_t> packet, RouterAdvertisement& out) -> RaParseError {
    out = {};
    if (packet.size() < RA_OPTIONS_OFFSET) {
        return RaParseError::TRUNCATED;
    }
    if ((packet[0] >> 4U) != 6) {
        return RaParseError::BAD_VERSION;
    }
    size_t const PAYLOAD_LENGTH = load_be16(packet.data() + IPV6_PAYLOAD_LENGTH_OFFSET);
    if (PAYLOAD_LENGTH < RA_FIXED_SIZE || IPV6_HEADER_SIZE + PAYLOAD_LENGTH != packet.size()) {
        return RaParseError::BAD_LENGTH;
    }
    if (packet[IPV6_NEXT_HEADER_OFFSET] != IPPROTO_ICMPV6_VALUE) {
        return RaParseError::BAD_NEXT_HEADER;
    }
    if (packet[IPV6_HOP_LIMIT_OFFSET] != 255) {
        return RaParseError::BAD_HOP_LIMIT;
    }

    std::copy_n(packet.data() + IPV6_SOURCE_OFFSET, out.source.size(), out.source.data());
    std::copy_n(packet.data() + IPV6_DESTINATION_OFFSET, out.destination.size(), out.destination.data());
    if (!ipv6_is_link_local(out.source)) {
        return RaParseError::BAD_SOURCE;
    }
    if (packet[IPV6_HEADER_SIZE] != ICMPV6_ROUTER_ADVERT || packet[IPV6_HEADER_SIZE + 1] != 0) {
        return RaParseError::BAD_TYPE_CODE;
    }

    out.router_lifetime_s = load_be16(packet.data() + IPV6_HEADER_SIZE + 6);
    out.reachable_time_ms = load_be32(packet.data() + IPV6_HEADER_SIZE + 8);
    out.retrans_timer_ms = load_be32(packet.data() + IPV6_HEADER_SIZE + 12);

    size_t offset = RA_OPTIONS_OFFSET;
    size_t option_count = 0;
    while (offset < packet.size()) {
        if (++option_count > MAX_RA_OPTIONS || offset + 2 > packet.size()) {
            return option_count > MAX_RA_OPTIONS ? RaParseError::TOO_MANY_OPTIONS : RaParseError::MALFORMED_OPTION;
        }
        uint8_t const TYPE = packet[offset];
        uint8_t const LENGTH_UNITS = packet[offset + 1];
        if (LENGTH_UNITS == 0) {
            return RaParseError::MALFORMED_OPTION;
        }
        size_t const OPTION_LENGTH = static_cast<size_t>(LENGTH_UNITS) * 8U;
        if (OPTION_LENGTH > packet.size() - offset) {
            return RaParseError::MALFORMED_OPTION;
        }

        if (TYPE == ND_OPT_PREFIX_INFORMATION) {
            if (LENGTH_UNITS != ND_OPT_PREFIX_INFORMATION_LENGTH) {
                return RaParseError::MALFORMED_OPTION;
            }
            RaPrefix prefix{};
            prefix.prefix_len = packet[offset + 2];
            prefix.on_link = (packet[offset + 3] & PREFIX_FLAG_ON_LINK) != 0;
            prefix.autonomous = (packet[offset + 3] & PREFIX_FLAG_AUTONOMOUS) != 0;
            prefix.valid_lifetime_s = load_be32(packet.data() + offset + 4);
            prefix.preferred_lifetime_s = load_be32(packet.data() + offset + 8);
            std::copy_n(packet.data() + offset + 16, prefix.prefix.size(), prefix.prefix.data());
            if (prefix.prefix_len > 128 || prefix.preferred_lifetime_s > prefix.valid_lifetime_s) {
                return RaParseError::BAD_PREFIX;
            }
            if (out.prefix_count < out.prefixes.size()) {
                out.prefixes[out.prefix_count++] = prefix;
            }
        } else if (TYPE == ND_OPT_MTU) {
            if (LENGTH_UNITS != ND_OPT_MTU_LENGTH) {
                return RaParseError::MALFORMED_OPTION;
            }
            uint32_t const MTU = load_be32(packet.data() + offset + 4);
            if (MTU != 0 && MTU < IPV6_MINIMUM_MTU) {
                return RaParseError::MALFORMED_OPTION;
            }
            out.mtu = MTU;
        }
        offset += OPTION_LENGTH;
    }
    return offset == packet.size() ? RaParseError::NONE : RaParseError::MALFORMED_OPTION;
}

auto make_slaac_address(const RaPrefix& prefix, std::span<const uint8_t, 6> mac, std::array<uint8_t, IPV6_ADDRESS_BYTES>& out) -> bool {
    if (!prefix_is_usable_for_slaac(prefix)) {
        return false;
    }
    out = prefix.prefix;
    out[8] = static_cast<uint8_t>(mac[0] ^ 0x02U);
    out[9] = mac[1];
    out[10] = mac[2];
    out[11] = 0xFF;
    out[12] = 0xFE;
    out[13] = mac[3];
    out[14] = mac[4];
    out[15] = mac[5];
    return true;
}

auto plan_ra_update(const RaManagedState& state, const RouterAdvertisement& advert, std::span<const uint8_t, 6> mac,
                    uint32_t interface_mtu_ceiling, uint32_t current_interface_mtu) -> RaUpdatePlan {
    RaUpdatePlan plan{};
    uint32_t const CURRENT_MTU = current_interface_mtu != 0 ? current_interface_mtu : interface_mtu_ceiling;
    if (advert.mtu >= IPV6_MINIMUM_MTU && (interface_mtu_ceiling == 0 || advert.mtu <= interface_mtu_ceiling) &&
        advert.mtu != CURRENT_MTU) {
        plan.set_mtu = true;
        plan.mtu = advert.mtu;
    }
    const RaPrefix* selected = nullptr;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> selected_address{};
    for (size_t i = 0; i < advert.prefix_count; ++i) {
        auto const& candidate = advert.prefixes[i];
        if (make_slaac_address(candidate, mac, selected_address)) {
            selected = &candidate;
            break;
        }
    }

    if (selected != nullptr) {
        bool const SAME_ADDRESS = state.address_installed && state.address == selected_address && state.prefix_len == selected->prefix_len;
        if (selected->valid_lifetime_s == 0) {
            if (SAME_ADDRESS) {
                plan.delete_address = true;
                plan.old_address = state.address;
                plan.old_prefix_len = state.prefix_len;
            }
        } else {
            if (state.address_installed && !SAME_ADDRESS) {
                plan.delete_address = true;
                plan.old_address = state.address;
                plan.old_prefix_len = state.prefix_len;
            }
            plan.set_address = true;
            plan.address = selected_address;
            plan.prefix_len = selected->prefix_len;
            plan.preferred_lifetime_s = selected->preferred_lifetime_s;
            plan.valid_lifetime_s = selected->valid_lifetime_s;
            plan.no_prefix_route = !selected->on_link;
        }
    }

    const RaPrefix* onlink = nullptr;
    std::array<uint8_t, IPV6_ADDRESS_BYTES> onlink_prefix{};
    for (size_t i = 0; i < advert.prefix_count; ++i) {
        auto const& candidate = advert.prefixes[i];
        if (prefix_is_usable_on_link(candidate)) {
            onlink = &candidate;
            onlink_prefix = masked_prefix(candidate);
            break;
        }
    }
    bool const ADDRESS_SUPPLIES_ONLINK = selected != nullptr && selected->on_link && selected->valid_lifetime_s != 0 && onlink != nullptr &&
                                         selected->prefix_len == onlink->prefix_len && masked_prefix(*selected) == onlink_prefix;
    if (ADDRESS_SUPPLIES_ONLINK) {
        if (state.onlink_route_installed) {
            plan.delete_onlink_route = true;
            plan.old_onlink_prefix = state.onlink_prefix;
            plan.old_onlink_prefix_len = state.onlink_prefix_len;
        }
    } else if (onlink != nullptr) {
        bool const SAME_ONLINK =
            state.onlink_route_installed && state.onlink_prefix == onlink_prefix && state.onlink_prefix_len == onlink->prefix_len;
        if (onlink->valid_lifetime_s == 0) {
            if (SAME_ONLINK) {
                plan.delete_onlink_route = true;
                plan.old_onlink_prefix = state.onlink_prefix;
                plan.old_onlink_prefix_len = state.onlink_prefix_len;
            }
        } else {
            if (state.onlink_route_installed && !SAME_ONLINK) {
                plan.delete_onlink_route = true;
                plan.old_onlink_prefix = state.onlink_prefix;
                plan.old_onlink_prefix_len = state.onlink_prefix_len;
            }
            plan.set_onlink_route = true;
            plan.onlink_prefix = onlink_prefix;
            plan.onlink_prefix_len = onlink->prefix_len;
            plan.onlink_lifetime_s = onlink->valid_lifetime_s;
        }
    } else if (state.onlink_route_installed) {
        for (size_t i = 0; i < advert.prefix_count; ++i) {
            auto const& candidate = advert.prefixes[i];
            if (candidate.prefix_len == state.onlink_prefix_len && masked_prefix(candidate) == state.onlink_prefix) {
                plan.delete_onlink_route = true;
                plan.old_onlink_prefix = state.onlink_prefix;
                plan.old_onlink_prefix_len = state.onlink_prefix_len;
                break;
            }
        }
    }

    bool const SAME_ROUTER = state.route_installed && state.router == advert.source;
    if (advert.router_lifetime_s == 0) {
        if (SAME_ROUTER) {
            plan.delete_route = true;
            plan.old_router = state.router;
        }
    } else {
        if (state.route_installed && !SAME_ROUTER) {
            plan.delete_route = true;
            plan.old_router = state.router;
        }
        plan.set_route = true;
        plan.router = advert.source;
        plan.router_lifetime_s = advert.router_lifetime_s;
    }
    return plan;
}

}  // namespace netd
