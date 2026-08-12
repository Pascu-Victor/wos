#pragma once
// Host shim for net/socket.hpp — minimal stubs for Socket and SocketProtoOps.

#include <sys/types.h>  // ssize_t on Linux host

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>

namespace ker::net {

constexpr uint16_t SOCKADDR_V4_FAMILY = 2;
constexpr uint16_t SOCKADDR_V6_FAMILY = 10;

enum class SocketState : uint8_t {
    CLOSED,
    UNBOUND,
    BOUND,
    LISTENING,
    CONNECTING,
    CONNECTED,
    CLOSE_WAIT,
};

constexpr auto socket_state_allows_explicit_bind(SocketState state) -> bool { return state == SocketState::UNBOUND; }

struct SocketEndpoint {
    uint16_t family = 0;
    uint16_t port = 0;
    std::array<uint8_t, proto::IPv6Address::SIZE_BYTES> address{};
    uint32_t scope_id = 0;

    static constexpr auto ipv4(proto::IPv4Address ip = proto::IPv4Address::any(), uint16_t endpoint_port = 0) -> SocketEndpoint {
        SocketEndpoint endpoint{.family = SOCKADDR_V4_FAMILY, .port = endpoint_port};
        endpoint.address.at(12) = ip.octet(0);
        endpoint.address.at(13) = ip.octet(1);
        endpoint.address.at(14) = ip.octet(2);
        endpoint.address.at(15) = ip.octet(3);
        return endpoint;
    }

    static constexpr auto ipv6(const proto::IPv6Address& ip = proto::IPv6Address::unspecified(), uint16_t endpoint_port = 0,
                               uint32_t endpoint_scope_id = 0) -> SocketEndpoint {
        bool const LINK_LOCAL = ip.bytes.at(0) == 0xFE && (ip.bytes.at(1) & 0xC0U) == 0x80U;
        bool const SCOPED_MULTICAST = ip.bytes.at(0) == 0xFF && (ip.bytes.at(1) & 0x0FU) <= 0x02U;
        return {.family = SOCKADDR_V6_FAMILY,
                .port = endpoint_port,
                .address = ip.bytes,
                .scope_id = LINK_LOCAL || SCOPED_MULTICAST ? endpoint_scope_id : 0};
    }

    static constexpr auto mapped_ipv4(proto::IPv4Address ip, uint16_t endpoint_port = 0) -> SocketEndpoint {
        SocketEndpoint endpoint = ipv6({}, endpoint_port);
        endpoint.address.at(10) = 0xFF;
        endpoint.address.at(11) = 0xFF;
        endpoint.address.at(12) = ip.octet(0);
        endpoint.address.at(13) = ip.octet(1);
        endpoint.address.at(14) = ip.octet(2);
        endpoint.address.at(15) = ip.octet(3);
        return endpoint;
    }

    constexpr auto is_ipv4() const -> bool { return family == SOCKADDR_V4_FAMILY; }
    constexpr auto is_ipv6() const -> bool { return family == SOCKADDR_V6_FAMILY; }
    constexpr auto is_scoped_ipv6() const -> bool {
        if (!is_ipv6()) {
            return false;
        }
        bool const LINK_LOCAL = address.at(0) == 0xFE && (address.at(1) & 0xC0U) == 0x80U;
        bool const SCOPED_MULTICAST = address.at(0) == 0xFF && (address.at(1) & 0x0FU) <= 0x02U;
        return LINK_LOCAL || SCOPED_MULTICAST;
    }
    constexpr auto is_v4_mapped() const -> bool {
        if (!is_ipv6() || address.at(10) != 0xFF || address.at(11) != 0xFF) {
            return false;
        }
        for (size_t i = 0; i < 10; ++i) {
            if (address.at(i) != 0) {
                return false;
            }
        }
        return true;
    }
    constexpr auto is_unspecified() const -> bool {
        if (is_ipv4()) {
            return ipv4_address().is_any();
        }
        for (uint8_t const BYTE : address) {
            if (BYTE != 0) {
                return false;
            }
        }
        return true;
    }
    constexpr auto ipv4_address() const -> proto::IPv4Address {
        return proto::IPv4Address{(static_cast<uint32_t>(address.at(12)) << 24U) | (static_cast<uint32_t>(address.at(13)) << 16U) |
                                  (static_cast<uint32_t>(address.at(14)) << 8U) | address.at(15)};
    }
    constexpr auto ipv6_address() const -> proto::IPv6Address { return proto::IPv6Address::from_bytes(address); }
};

inline auto socket_endpoint_address_equal(const SocketEndpoint& lhs, const SocketEndpoint& rhs) -> bool {
    if (lhs.family == rhs.family) {
        bool const SCOPE_MATCHES = (!lhs.is_scoped_ipv6() && !rhs.is_scoped_ipv6()) || lhs.scope_id == rhs.scope_id;
        return lhs.address == rhs.address && SCOPE_MATCHES;
    }
    if (lhs.is_ipv4() && rhs.is_v4_mapped()) {
        return lhs.ipv4_address() == rhs.ipv4_address();
    }
    if (lhs.is_v4_mapped() && rhs.is_ipv4()) {
        return lhs.ipv4_address() == rhs.ipv4_address();
    }
    return false;
}

inline auto socket_endpoint_scope_agrees_with_ifindex(const SocketEndpoint& endpoint, uint32_t ifindex) -> bool {
    return !endpoint.is_scoped_ipv6() || endpoint.scope_id == 0 || ifindex == 0 || endpoint.scope_id == ifindex;
}

inline auto socket_endpoint_requires_interface_address(const SocketEndpoint& endpoint) -> bool {
    return endpoint.is_ipv6() && !endpoint.is_v4_mapped() && !endpoint.is_unspecified();
}

inline auto socket_effective_endpoint_scope(const SocketEndpoint& endpoint, uint32_t bound_ifindex, uint32_t local_scope_id,
                                            uint32_t& effective_scope_id) -> int {
    effective_scope_id = 0;
    if (!endpoint.is_scoped_ipv6()) {
        return 0;
    }
    uint32_t scope_id = endpoint.scope_id;
    auto incorporate = [&scope_id](uint32_t candidate) -> bool {
        if (candidate == 0) {
            return true;
        }
        if (scope_id != 0 && scope_id != candidate) {
            return false;
        }
        scope_id = candidate;
        return true;
    };
    if (!incorporate(bound_ifindex) || !incorporate(local_scope_id)) {
        return -EINVAL;
    }
    if (scope_id == 0) {
        return -EADDRNOTAVAIL;
    }
    effective_scope_id = scope_id;
    return 0;
}

struct RingBuffer {
    size_t capacity = 0;
    size_t used = 0;

    auto available() const -> size_t { return used; }
    auto free_space() const -> size_t { return capacity > used ? capacity - used : 0; }
};

struct SocketProtoOps {
    void* placeholder = nullptr;
};

struct Socket {
    int domain = 0;
    int type = 0;
    int protocol = 0;
    RingBuffer rcvbuf{};
    void* private_data = nullptr;
    uint32_t bound_ifindex = 0;
};

}  // namespace ker::net
