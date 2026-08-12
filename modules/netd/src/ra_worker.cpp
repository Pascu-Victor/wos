#include "netd/ra_worker.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <wos/netctl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

#include "netd/config.hpp"
#include "netd/interface.hpp"
#include "netd/log.hpp"
#include "netd/ra.hpp"
#include "netd/time.hpp"

namespace netd {
namespace {

constexpr uint32_t RA_RETRY_DELAY_S = 5;
constexpr uint32_t RA_POLL_INTERVAL_US = 50'000;
constexpr uint32_t ROUTER_SOLICIT_INTERVAL_S = 4;
constexpr uint32_t IDENTITY_RECHECK_INTERVAL_S = 1;
constexpr size_t MAX_ROUTER_SOLICITATIONS = 3;
constexpr size_t RA_PACKET_CAPACITY = 2048;
constexpr uint8_t ICMPV6_ROUTER_SOLICIT = 133;
constexpr uint8_t ND_OPT_SOURCE_LINK_LAYER_ADDRESS = 1;
constexpr uint32_t RA_DEFAULT_ROUTE_METRIC = 1024;

struct InterfaceIdentity {
    uint32_t ifindex = 0;
    uint32_t mtu = 0;
    uint32_t mtu_ceiling = 0;
    std::array<uint8_t, 6> mac{};
    std::array<uint8_t, IPV6_ADDRESS_BYTES> link_local{};
};

auto find_interface_identity(const char* ifname, InterfaceIdentity& identity) -> bool {
    std::array<wos_net_if_info, MAX_CONFIGURED_INTERFACES> interfaces{};
    size_t interface_count = interfaces.size();
    if (wos_net_if_list(interfaces.data(), &interface_count) != 0) {
        return false;
    }
    size_t const EMITTED = std::min(interface_count, interfaces.size());
    for (size_t i = 0; i < EMITTED; ++i) {
        auto const& interface = interfaces[i];
        if (std::strncmp(interface.name, ifname, sizeof(interface.name)) != 0) {
            continue;
        }
        identity.ifindex = interface.ifindex;
        identity.mtu = interface.mtu;
        identity.mtu_ceiling = interface.mtu;
        std::copy_n(interface.addr, identity.mac.size(), identity.mac.data());
        break;
    }
    if (identity.ifindex == 0) {
        return false;
    }

    std::array<wos_net_addr_info, 64> addresses{};
    size_t address_count = addresses.size();
    if (wos_net_addr_list(addresses.data(), &address_count) != 0) {
        return false;
    }
    size_t const ADDRESS_EMITTED = std::min(address_count, addresses.size());
    for (size_t i = 0; i < ADDRESS_EMITTED; ++i) {
        auto const& address = addresses[i];
        if (address.ifindex != identity.ifindex || address.family != AF_INET6) {
            continue;
        }
        std::array<uint8_t, IPV6_ADDRESS_BYTES> candidate{};
        std::copy_n(address.local, candidate.size(), candidate.data());
        if (ipv6_link_local_usable(candidate, address.scope, address.flags)) {
            identity.link_local = candidate;
            return true;
        }
    }
    return false;
}

auto bind_ra_socket(int socket_fd, const char* ifname, const InterfaceIdentity& identity) -> bool {
    if (setsockopt(socket_fd, SOL_SOCKET, SO_BINDTODEVICE, ifname, std::strlen(ifname) + 1) != 0) {
        return false;
    }
    int const HOP_LIMIT = 255;
    if (setsockopt(socket_fd, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &HOP_LIMIT, sizeof(HOP_LIMIT)) != 0 ||
        setsockopt(socket_fd, IPPROTO_IPV6, IPV6_UNICAST_HOPS, &HOP_LIMIT, sizeof(HOP_LIMIT)) != 0) {
        return false;
    }

    sockaddr_in6 local{};
    local.sin6_family = AF_INET6;
    local.sin6_scope_id = identity.ifindex;
    std::memcpy(&local.sin6_addr, identity.link_local.data(), identity.link_local.size());
    return bind(socket_fd, reinterpret_cast<const sockaddr*>(&local), sizeof(local)) == 0;
}

auto send_router_solicitation(int socket_fd, const InterfaceIdentity& identity) -> bool {
    std::array<uint8_t, 16> solicitation{};
    solicitation[0] = ICMPV6_ROUTER_SOLICIT;
    solicitation[8] = ND_OPT_SOURCE_LINK_LAYER_ADDRESS;
    solicitation[9] = 1;
    std::copy(identity.mac.begin(), identity.mac.end(), solicitation.begin() + 10);

    sockaddr_in6 destination{};
    destination.sin6_family = AF_INET6;
    destination.sin6_scope_id = identity.ifindex;
    constexpr std::array<uint8_t, IPV6_ADDRESS_BYTES> ALL_ROUTERS = {0xFF, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2};
    std::memcpy(&destination.sin6_addr, ALL_ROUTERS.data(), ALL_ROUTERS.size());
    ssize_t const SENT = sendto(socket_fd, solicitation.data(), solicitation.size(), 0, reinterpret_cast<const sockaddr*>(&destination),
                                sizeof(destination));
    return std::cmp_equal(SENT, solicitation.size());
}

auto delete_autoconf_address(uint32_t ifindex, const std::array<uint8_t, IPV6_ADDRESS_BYTES>& address, uint8_t prefix_len) -> bool {
    wos_net_addr_req request{};
    request.ifindex = ifindex;
    request.family = AF_INET6;
    request.prefix_len = prefix_len;
    request.flags = WOS_NET_ADDR_F_AUTOCONF;
    std::copy(address.begin(), address.end(), request.address);
    std::copy(address.begin(), address.end(), request.local);
    if (wos_net_addr_del(&request) == 0 || errno == EADDRNOTAVAIL || errno == ENOENT) {
        return true;
    }
    return false;
}

auto set_autoconf_address(uint32_t ifindex, const RaUpdatePlan& plan) -> bool {
    wos_net_addr_req_v2 request{};
    request.size = sizeof(request);
    request.version = WOS_NETCTL_VERSION_1;
    request.address.ifindex = ifindex;
    request.address.family = AF_INET6;
    request.address.prefix_len = plan.prefix_len;
    request.address.flags = WOS_NET_ADDR_F_AUTOCONF;
    if (plan.no_prefix_route) {
        request.address.flags |= WOS_NET_ADDR_F_NOPREFIXROUTE;
    }
    request.address.replace = 1;
    std::copy(plan.address.begin(), plan.address.end(), request.address.address);
    std::copy(plan.address.begin(), plan.address.end(), request.address.local);
    request.preferred_lifetime_s = plan.preferred_lifetime_s;
    request.valid_lifetime_s = plan.valid_lifetime_s;
    return wos_net_addr_set_v2(&request) == 0;
}

auto update_default_route(uint32_t ifindex, const std::array<uint8_t, IPV6_ADDRESS_BYTES>& router, uint16_t lifetime_s, bool remove)
    -> bool {
    wos_net_route_record request{};
    request.size = sizeof(request);
    request.version = WOS_NETCTL_VERSION_1;
    request.family = AF_INET6;
    request.ifindex = ifindex;
    request.metric = RA_DEFAULT_ROUTE_METRIC;
    request.flags = WOS_NET_ROUTE_F_GATEWAY | WOS_NET_ROUTE_F_AUTOCONF;
    request.prefix_len = 0;
    request.scope = 253;
    std::copy(router.begin(), router.end(), request.gateway);
    request.lifetime_s = lifetime_s;
    if (remove) {
        return wos_net_route_del(&request) == 0 || errno == ENOENT;
    }
    return wos_net_route_set(&request) == 0;
}

auto update_onlink_route(uint32_t ifindex, const std::array<uint8_t, IPV6_ADDRESS_BYTES>& prefix, uint8_t prefix_len, uint32_t lifetime_s,
                         bool remove) -> bool {
    wos_net_route_record request{};
    request.size = sizeof(request);
    request.version = WOS_NETCTL_VERSION_1;
    request.family = AF_INET6;
    request.ifindex = ifindex;
    request.flags = WOS_NET_ROUTE_F_AUTOCONF;
    request.prefix_len = prefix_len;
    std::copy(prefix.begin(), prefix.end(), request.destination);
    request.lifetime_s = lifetime_s;
    if (remove) {
        return wos_net_route_del(&request) == 0 || errno == ENOENT;
    }
    return wos_net_route_set(&request) == 0;
}

auto update_interface_mtu(uint32_t ifindex, uint32_t mtu) -> bool {
    wos_net_link_set_req request{};
    request.ifindex = ifindex;
    request.fields = WOS_NET_LINK_SET_MTU;
    request.mtu = mtu;
    return wos_net_link_set(&request) == 0;
}

void withdraw_ra_state(const char* ifname, const InterfaceIdentity& identity, RaManagedState& state, bool device_still_matches) {
    if (device_still_matches && state.route_installed) {
        static_cast<void>(update_default_route(identity.ifindex, state.router, 0, true));
    }
    if (device_still_matches && state.onlink_route_installed) {
        static_cast<void>(update_onlink_route(identity.ifindex, state.onlink_prefix, state.onlink_prefix_len, 0, true));
    }
    if (device_still_matches && state.address_installed) {
        static_cast<void>(delete_autoconf_address(identity.ifindex, state.address, state.prefix_len));
    }
    if (device_still_matches && identity.mtu_ceiling != 0 && identity.mtu != identity.mtu_ceiling) {
        static_cast<void>(update_interface_mtu(identity.ifindex, identity.mtu_ceiling));
    }
    state = {};
    logger::info("netd: retired managed IPv6 state for stale %s identity (device-match=%u)", ifname,
                 static_cast<unsigned>(device_still_matches));
}

auto same_interface_identity(const InterfaceIdentity& left, const InterfaceIdentity& right) -> bool {
    return left.ifindex == right.ifindex && left.mtu == right.mtu && left.mac == right.mac && left.link_local == right.link_local;
}

auto apply_ra_plan(const char* ifname, uint32_t ifindex, const RaUpdatePlan& plan, RaManagedState& state) -> bool {
    bool mtu_applied = false;
    if (plan.set_mtu && update_interface_mtu(ifindex, plan.mtu)) {
        mtu_applied = true;
        logger::info("netd: %s RA MTU set to %u", ifname, plan.mtu);
    } else if (plan.set_mtu) {
        logger::warn("netd: failed to apply %s RA MTU %u: errno=%d", ifname, plan.mtu, errno);
    }
    if (plan.delete_route && update_default_route(ifindex, plan.old_router, 0, true)) {
        state.route_installed = false;
        state.router = {};
    }
    if (plan.delete_onlink_route && update_onlink_route(ifindex, plan.old_onlink_prefix, plan.old_onlink_prefix_len, 0, true)) {
        state.onlink_route_installed = false;
        state.onlink_prefix = {};
        state.onlink_prefix_len = 0;
    }
    if (plan.delete_address && delete_autoconf_address(ifindex, plan.old_address, plan.old_prefix_len)) {
        state.address_installed = false;
        state.address = {};
        state.prefix_len = 0;
    }
    if (plan.set_address) {
        if (set_autoconf_address(ifindex, plan)) {
            state.address_installed = true;
            state.address = plan.address;
            state.prefix_len = plan.prefix_len;
            std::array<char, INET6_ADDRSTRLEN> text{};
            inet_ntop(AF_INET6, plan.address.data(), text.data(), text.size());
            logger::info("netd: %s SLAAC address %s/%u preferred=%us valid=%us", ifname, text.data(), plan.prefix_len,
                         plan.preferred_lifetime_s, plan.valid_lifetime_s);
        } else {
            logger::warn("netd: failed to apply %s SLAAC address: errno=%d", ifname, errno);
        }
    }
    if (plan.set_onlink_route) {
        if (update_onlink_route(ifindex, plan.onlink_prefix, plan.onlink_prefix_len, plan.onlink_lifetime_s, false)) {
            state.onlink_route_installed = true;
            state.onlink_prefix = plan.onlink_prefix;
            state.onlink_prefix_len = plan.onlink_prefix_len;
            std::array<char, INET6_ADDRSTRLEN> text{};
            inet_ntop(AF_INET6, plan.onlink_prefix.data(), text.data(), text.size());
            logger::info("netd: %s RA on-link route %s/%u lifetime=%us", ifname, text.data(), plan.onlink_prefix_len,
                         plan.onlink_lifetime_s);
        } else {
            logger::warn("netd: failed to apply %s RA on-link route: errno=%d", ifname, errno);
        }
    }
    if (plan.set_route) {
        if (update_default_route(ifindex, plan.router, plan.router_lifetime_s, false)) {
            state.route_installed = true;
            state.router = plan.router;
            std::array<char, INET6_ADDRSTRLEN> text{};
            inet_ntop(AF_INET6, plan.router.data(), text.data(), text.size());
            logger::info("netd: %s RA default route via %s%%%u lifetime=%us", ifname, text.data(), ifindex, plan.router_lifetime_s);
        } else {
            logger::warn("netd: failed to apply %s RA default route: errno=%d", ifname, errno);
        }
    }
    return mtu_applied;
}

void receive_router_advertisements(int socket_fd, const char* ifname, InterfaceIdentity identity) {
    std::array<uint8_t, RA_PACKET_CAPACITY> packet{};
    RaManagedState state{};
    size_t solicitations = 0;
    uint64_t next_solicitation_us = 0;
    uint64_t next_identity_check_us = monotonic_now_us() + static_cast<uint64_t>(IDENTITY_RECHECK_INTERVAL_S) * USEC_PER_SEC;
    uint64_t rejected_packets = 0;

    for (;;) {
        uint64_t const NOW_US = monotonic_now_us();
        if (NOW_US >= next_identity_check_us) {
            InterfaceIdentity current{};
            bool const USABLE = find_interface_identity(ifname, current);
            if (!USABLE || !same_interface_identity(identity, current)) {
                bool const SAME_DEVICE = current.ifindex == identity.ifindex && current.mac == identity.mac;
                withdraw_ra_state(ifname, identity, state, SAME_DEVICE);
                return;
            }
            next_identity_check_us = NOW_US + static_cast<uint64_t>(IDENTITY_RECHECK_INTERVAL_S) * USEC_PER_SEC;
        }
        if (solicitations < MAX_ROUTER_SOLICITATIONS && NOW_US >= next_solicitation_us) {
            if (!send_router_solicitation(socket_fd, identity)) {
                logger::warn("netd: %s router solicitation failed: errno=%d", ifname, errno);
            }
            ++solicitations;
            next_solicitation_us = NOW_US + static_cast<uint64_t>(ROUTER_SOLICIT_INTERVAL_S) * USEC_PER_SEC;
        }

        sockaddr_in6 source{};
        socklen_t source_length = sizeof(source);
        ssize_t const RECEIVED =
            recvfrom(socket_fd, packet.data(), packet.size(), MSG_DONTWAIT, reinterpret_cast<sockaddr*>(&source), &source_length);
        if (RECEIVED < 0) {
            if (errno != EAGAIN && errno != EINTR) {
                logger::warn("netd: %s RA receive failed: errno=%d", ifname, errno);
            }
            sleep_until_us(monotonic_now_us() + RA_POLL_INTERVAL_US);
            continue;
        }
        if (source_length != sizeof(source) || source.sin6_family != AF_INET6 || source.sin6_scope_id != identity.ifindex) {
            ++rejected_packets;
            continue;
        }

        RouterAdvertisement advert{};
        RaParseError const ERROR =
            parse_router_advertisement(std::span<const uint8_t>{packet.data(), static_cast<size_t>(RECEIVED)}, advert);
        if (ERROR != RaParseError::NONE || std::memcmp(&source.sin6_addr, advert.source.data(), advert.source.size()) != 0) {
            ++rejected_packets;
            if (rejected_packets == 1 || rejected_packets % 64 == 0) {
                logger::warn("netd: %s rejected malformed RA (reason=%u total=%llu)", ifname, static_cast<unsigned>(ERROR),
                             static_cast<unsigned long long>(rejected_packets));
            }
            continue;
        }

        RaUpdatePlan const PLAN = plan_ra_update(state, advert, identity.mac, identity.mtu_ceiling, identity.mtu);
        bool const MTU_APPLIED = apply_ra_plan(ifname, identity.ifindex, PLAN, state);
        if (MTU_APPLIED) {
            identity.mtu = PLAN.mtu;
        }
    }
}

}  // namespace

void run_ra_worker(const char* ifname) {
    for (;;) {
        InterfaceIdentity identity{};
        if (!find_interface_identity(ifname, identity)) {
            logger::warn("netd: IPv6 worker waiting for %s identity/link-local address", ifname);
            sleep_for_seconds(RA_RETRY_DELAY_S);
            continue;
        }

        int const SOCKET_FD = socket(AF_INET6, SOCK_RAW, IPPROTO_ICMPV6);
        if (SOCKET_FD < 0 || !bind_ra_socket(SOCKET_FD, ifname, identity)) {
            logger::warn("netd: failed to start %s RA socket: errno=%d", ifname, errno);
            if (SOCKET_FD >= 0) {
                close(SOCKET_FD);
            }
            sleep_for_seconds(RA_RETRY_DELAY_S);
            continue;
        }

        logger::info("netd: IPv6 RS/RA worker active on %s (ifindex=%u)", ifname, identity.ifindex);
        receive_router_advertisements(SOCKET_FD, ifname, identity);
        close(SOCKET_FD);
    }
}

}  // namespace netd
