#include "icmpv6.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ndp.hpp>
#include <net/proto/raw.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
constexpr uint64_t ERROR_TOKEN_INTERVAL_MS = 100;
constexpr uint8_t ERROR_TOKEN_BURST = 8;
constexpr size_t MAX_ROUTER_OPTION_BYTES = 512;
constexpr size_t MAX_ROUTER_OPTIONS = 32;

struct ErrorRateBucket {
    NetDeviceIdentity identity{};
    uint64_t last_refill_ms{};
    uint8_t tokens{ERROR_TOKEN_BURST};
};

std::array<ErrorRateBucket, MAX_NET_DEVICES> error_rate{};
mod::sys::Spinlock error_rate_lock;

auto error_rate_admit(NetDeviceIdentity identity, uint64_t now_ms) -> bool {
    bool admit = false;
    uint64_t const FLAGS = error_rate_lock.lock_irqsave();
    ErrorRateBucket* bucket = nullptr;
    ErrorRateBucket* free_bucket = nullptr;
    ErrorRateBucket* oldest_bucket = nullptr;
    for (auto& candidate : error_rate) {
        if (candidate.identity == identity) {
            bucket = &candidate;
            break;
        }
        if (!candidate.identity.valid() && free_bucket == nullptr) {
            free_bucket = &candidate;
        } else if (candidate.identity.valid() && (oldest_bucket == nullptr || candidate.last_refill_ms < oldest_bucket->last_refill_ms)) {
            oldest_bucket = &candidate;
        }
    }
    if (bucket == nullptr) {
        bucket = free_bucket != nullptr ? free_bucket : oldest_bucket;
        if (bucket != nullptr) {
            *bucket = {.identity = identity, .last_refill_ms = now_ms, .tokens = ERROR_TOKEN_BURST};
        }
    }
    if (bucket != nullptr) {
        uint64_t const ELAPSED = now_ms - bucket->last_refill_ms;
        uint64_t const REFILLS = ELAPSED / ERROR_TOKEN_INTERVAL_MS;
        if (REFILLS != 0) {
            bucket->tokens = static_cast<uint8_t>(std::min<uint64_t>(ERROR_TOKEN_BURST, bucket->tokens + REFILLS));
            bucket->last_refill_ms += REFILLS * ERROR_TOKEN_INTERVAL_MS;
        }
        if (bucket->tokens != 0) {
            --bucket->tokens;
            admit = true;
        }
    }
    error_rate_lock.unlock_irqrestore(FLAGS);
    return admit;
}

void handle_echo_request(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) {
    NetDeviceRef device = netdev_retain_registered(dev);
    if (!device) {
        pkt_free(pkt);
        return;
    }
    IPv6Address reply_source = info.dst;
    if (reply_source.is_multicast() && netif_ipv6_select_source(device.get(), info.src, nullptr, reply_source) < 0) {
        pkt_free(pkt);
        return;
    }
    if (pkt->len < sizeof(ICMPv6Header) + sizeof(ICMPv6Echo)) {
        pkt_free(pkt);
        return;
    }
    PacketBuffer* reply = pkt_alloc_tx();
    if (reply == nullptr || reply->tailroom() < pkt->len) {
        pkt_free(reply);
        pkt_free(pkt);
        return;
    }
    std::memcpy(reply->put(pkt->len), pkt->data, pkt->len);
    auto* header = reinterpret_cast<ICMPv6Header*>(reply->data);
    header->type = ICMPV6_ECHO_REPLY;
    header->code = 0;
    header->checksum = 0;
    header->checksum =
        checksum_pseudo_ipv6(reply_source, info.src, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(reply->len), reply->data, reply->len);
    pkt_free(pkt);
    (void)ipv6_tx_on_dev(reply, device.get(), reply_source, info.src, IPV6_PROTO_ICMPV6, 64);
}

auto is_valid_error_code(uint8_t type, uint8_t code) -> bool {
    switch (type) {
        case ICMPV6_DEST_UNREACH:
            return code <= 7;
        case ICMPV6_PACKET_TOO_BIG:
            return code == 0;
        case ICMPV6_TIME_EXCEEDED:
            return code <= 1;
        case ICMPV6_PARAM_PROBLEM:
            return code <= 2;
        default:
            return false;
    }
}

auto validate_router_options(const uint8_t* data, size_t length, bool router_advertisement, bool source_unspecified)
    -> ICMPv6ValidationError {
    if (length > MAX_ROUTER_OPTION_BYTES) {
        return ICMPv6ValidationError::BAD_OPTIONS;
    }
    size_t offset = 0;
    size_t count = 0;
    bool source_link_seen = false;
    bool mtu_seen = false;
    while (offset < length) {
        if (++count > MAX_ROUTER_OPTIONS || length - offset < sizeof(NdpOptionHeader)) {
            return ICMPv6ValidationError::BAD_OPTIONS;
        }
        auto const* option = reinterpret_cast<const NdpOptionHeader*>(data + offset);
        if (option->length == 0) {
            return ICMPv6ValidationError::BAD_OPTIONS;
        }
        size_t const OPTION_LENGTH = static_cast<size_t>(option->length) * 8U;
        if (OPTION_LENGTH > length - offset) {
            return ICMPv6ValidationError::BAD_OPTIONS;
        }
        if (option->type == NDP_OPT_SRC_LINK_ADDR) {
            if (OPTION_LENGTH != 8 || source_link_seen || source_unspecified) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
            MacAddress link_address{};
            std::memcpy(link_address.data(), data + offset + sizeof(NdpOptionHeader), link_address.size());
            if (link_address == MacAddress::zero() || link_address.is_multicast()) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
            source_link_seen = true;
        } else if (option->type == NDP_OPT_PREFIX_INFORMATION) {
            constexpr uint8_t PREFIX_FLAG_AUTONOMOUS = 0x40;
            uint8_t const PREFIX_LENGTH = data[offset + 2];
            uint8_t const PREFIX_FLAGS = data[offset + 3];
            if (!router_advertisement || OPTION_LENGTH != 32 || PREFIX_LENGTH > 128 ||
                ((PREFIX_FLAGS & PREFIX_FLAG_AUTONOMOUS) != 0 && PREFIX_LENGTH != 64)) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
            uint32_t const VALID_LIFETIME = (static_cast<uint32_t>(data[offset + 4]) << 24U) |
                                            (static_cast<uint32_t>(data[offset + 5]) << 16U) |
                                            (static_cast<uint32_t>(data[offset + 6]) << 8U) | data[offset + 7];
            uint32_t const PREFERRED_LIFETIME = (static_cast<uint32_t>(data[offset + 8]) << 24U) |
                                                (static_cast<uint32_t>(data[offset + 9]) << 16U) |
                                                (static_cast<uint32_t>(data[offset + 10]) << 8U) | data[offset + 11];
            if (PREFERRED_LIFETIME > VALID_LIFETIME) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
        } else if (option->type == NDP_OPT_MTU) {
            if (!router_advertisement || OPTION_LENGTH != 8 || mtu_seen) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
            uint32_t const MTU = (static_cast<uint32_t>(data[offset + 4]) << 24U) | (static_cast<uint32_t>(data[offset + 5]) << 16U) |
                                 (static_cast<uint32_t>(data[offset + 6]) << 8U) | data[offset + 7];
            if (MTU < 1280) {
                return ICMPv6ValidationError::BAD_OPTIONS;
            }
            mtu_seen = true;
        }
        offset += OPTION_LENGTH;
    }
    return ICMPv6ValidationError::NONE;
}

void handle_transport_error(NetDevice* ingress, PacketBuffer* pkt, const ICMPv6Header& header) {
    constexpr size_t FIXED_ERROR_LENGTH = sizeof(ICMPv6Header) + sizeof(ICMPv6ErrorBody);
    if (pkt->len < FIXED_ERROR_LENGTH + IPV6_HLEN) {
        pkt_free(pkt);
        return;
    }
    auto const* body = reinterpret_cast<const ICMPv6ErrorBody*>(pkt->data + sizeof(ICMPv6Header));
    const uint8_t* const QUOTED = pkt->data + FIXED_ERROR_LENGTH;
    size_t const QUOTED_LENGTH = pkt->len - FIXED_ERROR_LENGTH;
    IPv6RxInfo quoted_info{};
    if (ipv6_parse_quoted(QUOTED, QUOTED_LENGTH, quoted_info) != IPv6ParseError::NONE || quoted_info.upper_layer_length < 4) {
        pkt_free(pkt);
        return;
    }
    IPv6Addr local_address{};
    NetDeviceRef local_device{};
    uint32_t const SCOPE_ID = ingress != nullptr ? ingress->ifindex : 0;
    if (!netif_ipv6_find_owner(quoted_info.src, local_address, local_device) ||
        (quoted_info.src.is_link_local_unicast() && local_device->ifindex != SCOPE_ID)) {
        pkt_free(pkt);
        return;
    }
    uint16_t const LOCAL_PORT = static_cast<uint16_t>((static_cast<uint16_t>(QUOTED[quoted_info.upper_layer_offset]) << 8U) |
                                                      QUOTED[quoted_info.upper_layer_offset + 1]);
    uint16_t const REMOTE_PORT = static_cast<uint16_t>((static_cast<uint16_t>(QUOTED[quoted_info.upper_layer_offset + 2]) << 8U) |
                                                       QUOTED[quoted_info.upper_layer_offset + 3]);
    uint32_t const MTU = header.type == ICMPV6_PACKET_TOO_BIG ? std::max<uint32_t>(1280, ntohl(body->parameter)) : 0;
    if (quoted_info.next_header == IPV6_PROTO_UDP) {
        udp_error_v6(quoted_info.src, LOCAL_PORT, quoted_info.dst, REMOTE_PORT, header.type, header.code, MTU, SCOPE_ID);
    } else if (quoted_info.next_header == IPV6_PROTO_TCP) {
        tcp_error_v6(quoted_info.src, LOCAL_PORT, quoted_info.dst, REMOTE_PORT, header.type, header.code, MTU, SCOPE_ID);
    }
    pkt_free(pkt);
}

}  // namespace

auto icmpv6_validate(const PacketBuffer* pkt, const IPv6RxInfo& info) -> ICMPv6ValidationError {
    if (pkt == nullptr || pkt->len < sizeof(ICMPv6Header)) {
        return ICMPv6ValidationError::TRUNCATED;
    }
    auto const* header = reinterpret_cast<const ICMPv6Header*>(pkt->data);
    if (checksum_pseudo_ipv6(info.src, info.dst, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(pkt->len), pkt->data, pkt->len) != 0) {
        return ICMPv6ValidationError::BAD_CHECKSUM;
    }
    switch (header->type) {
        case ICMPV6_ECHO_REQUEST:
        case ICMPV6_ECHO_REPLY:
            if (header->code != 0) {
                return ICMPv6ValidationError::BAD_CODE;
            }
            if (info.src.is_unspecified() || info.src.is_multicast()) {
                return ICMPv6ValidationError::BAD_SOURCE;
            }
            return pkt->len >= sizeof(ICMPv6Header) + sizeof(ICMPv6Echo) ? ICMPv6ValidationError::NONE : ICMPv6ValidationError::TRUNCATED;
        case ICMPV6_NEIGHBOR_SOLICIT:
        case ICMPV6_NEIGHBOR_ADVERT:
        case ICMPV6_ROUTER_SOLICIT:
        case ICMPV6_ROUTER_ADVERT:
            if (header->code != 0) {
                return ICMPv6ValidationError::BAD_CODE;
            }
            if (info.hop_limit != 255) {
                return ICMPv6ValidationError::BAD_HOP_LIMIT;
            }
            if (header->type == ICMPV6_ROUTER_ADVERT && !info.src.is_link_local_unicast()) {
                return ICMPv6ValidationError::BAD_SOURCE;
            }
            if (header->type == ICMPV6_ROUTER_SOLICIT && !info.src.is_unspecified() && !info.src.is_link_local_unicast()) {
                return ICMPv6ValidationError::BAD_SOURCE;
            }
            if (header->type == ICMPV6_ROUTER_SOLICIT) {
                constexpr size_t FIXED_LENGTH = sizeof(ICMPv6Header) + sizeof(ICMPv6RouterSolicitation);
                if (pkt->len < FIXED_LENGTH) {
                    return ICMPv6ValidationError::TRUNCATED;
                }
                if (info.dst.is_multicast() && info.dst != IPV6_ALL_ROUTERS_MULTICAST) {
                    return ICMPv6ValidationError::BAD_DESTINATION;
                }
                return validate_router_options(pkt->data + FIXED_LENGTH, pkt->len - FIXED_LENGTH, false, info.src.is_unspecified());
            }
            if (header->type == ICMPV6_ROUTER_ADVERT) {
                constexpr size_t FIXED_LENGTH = sizeof(ICMPv6Header) + sizeof(ICMPv6RouterAdvertisement);
                if (pkt->len < FIXED_LENGTH) {
                    return ICMPv6ValidationError::TRUNCATED;
                }
                if (info.dst.is_multicast() && info.dst != IPV6_ALL_NODES_MULTICAST) {
                    return ICMPv6ValidationError::BAD_DESTINATION;
                }
                return validate_router_options(pkt->data + FIXED_LENGTH, pkt->len - FIXED_LENGTH, true, false);
            }
            if (header->type == ICMPV6_NEIGHBOR_SOLICIT && pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit)) {
                return ICMPv6ValidationError::TRUNCATED;
            }
            if (header->type == ICMPV6_NEIGHBOR_ADVERT && pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert)) {
                return ICMPv6ValidationError::TRUNCATED;
            }
            return ICMPv6ValidationError::NONE;
        case ICMPV6_DEST_UNREACH:
        case ICMPV6_PACKET_TOO_BIG:
        case ICMPV6_TIME_EXCEEDED:
        case ICMPV6_PARAM_PROBLEM:
            if (info.src.is_unspecified() || info.src.is_multicast()) {
                return ICMPv6ValidationError::BAD_SOURCE;
            }
            if (!is_valid_error_code(header->type, header->code)) {
                return ICMPv6ValidationError::BAD_CODE;
            }
            return pkt->len >= sizeof(ICMPv6Header) + sizeof(ICMPv6ErrorBody) + IPV6_HLEN ? ICMPv6ValidationError::NONE
                                                                                          : ICMPv6ValidationError::TRUNCATED;
        default:
            return ICMPv6ValidationError::BAD_TYPE;
    }
}

void icmpv6_rx(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) {
    if (icmpv6_validate(pkt, info) != ICMPv6ValidationError::NONE) {
        pkt_free(pkt);
        return;
    }
    auto const* header = reinterpret_cast<const ICMPv6Header*>(pkt->data);
    switch (header->type) {
        case ICMPV6_ECHO_REQUEST:
            handle_echo_request(dev, pkt, info);
            return;
        case ICMPV6_NEIGHBOR_SOLICIT:
            ndp_handle_ns(dev, pkt, info);
            return;
        case ICMPV6_NEIGHBOR_ADVERT:
            ndp_handle_na(dev, pkt, info);
            return;
        case ICMPV6_DEST_UNREACH:
        case ICMPV6_PACKET_TOO_BIG:
        case ICMPV6_TIME_EXCEEDED:
        case ICMPV6_PARAM_PROBLEM:
            handle_transport_error(dev, pkt, *header);
            return;
        case ICMPV6_ECHO_REPLY:
        case ICMPV6_ROUTER_SOLICIT:
        case ICMPV6_ROUTER_ADVERT:
            raw_deliver_v6(pkt, info, dev != nullptr ? dev->ifindex : 0);
            return;
        default:
            pkt_free(pkt);
            return;
    }
}

void icmpv6_send_port_unreachable(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) {
    if (pkt == nullptr) {
        return;
    }
    NetDeviceIdentity const IDENTITY = netdev_registered_identity(dev);
    NetDeviceRef device = netdev_try_retain(IDENTITY);
    if (!device || info.src.is_unspecified() || info.src.is_multicast() || info.dst.is_multicast() ||
        !error_rate_admit(IDENTITY, mod::time::get_ms())) {
        pkt_free(pkt);
        return;
    }

    // Quote a reconstructed fixed IPv6 header plus as much invoking payload as
    // fits within IPv6's minimum MTU. Extension bytes were validated already;
    // omitting them makes the quote deterministic and safely bounded.
    constexpr size_t ERROR_FIXED = sizeof(ICMPv6Header) + sizeof(ICMPv6ErrorBody);
    constexpr size_t MAX_QUOTE = 1280 - IPV6_HLEN - ERROR_FIXED;
    size_t const QUOTED_PAYLOAD = std::min({pkt->len, info.upper_layer_length, MAX_QUOTE - IPV6_HLEN});
    size_t const ERROR_LENGTH = ERROR_FIXED + IPV6_HLEN + QUOTED_PAYLOAD;
    PacketBuffer* reply = pkt_alloc_tx();
    if (reply == nullptr || reply->tailroom() < ERROR_LENGTH) {
        pkt_free(reply);
        pkt_free(pkt);
        return;
    }
    uint8_t* const DATA = reply->put(ERROR_LENGTH);
    std::memset(DATA, 0, ERROR_LENGTH);
    auto* error = reinterpret_cast<ICMPv6Header*>(DATA);
    error->type = ICMPV6_DEST_UNREACH;
    error->code = ICMPV6_DEST_UNREACH_PORT;
    auto* quoted = reinterpret_cast<IPv6Header*>(DATA + ERROR_FIXED);
    quoted->version_tc_flow = htonl(uint32_t{6} << 28U);
    quoted->payload_length = htons(static_cast<uint16_t>(QUOTED_PAYLOAD));
    quoted->next_header = info.next_header;
    quoted->hop_limit = info.hop_limit;
    quoted->src = info.src;
    quoted->dst = info.dst;
    std::memcpy(DATA + ERROR_FIXED + IPV6_HLEN, pkt->data, QUOTED_PAYLOAD);
    error->checksum = checksum_pseudo_ipv6(info.dst, info.src, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(ERROR_LENGTH), DATA, ERROR_LENGTH);
    pkt_free(pkt);
    (void)ipv6_tx_on_dev(reply, device.get(), info.dst, info.src, IPV6_PROTO_ICMPV6, 64);
}

}  // namespace ker::net::proto
