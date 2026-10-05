#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <utility>

#include "tcp.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"tcp">;

namespace {
constexpr size_t TCP_SYN_OPTIONS_LEN = 12;
constexpr size_t TCP_ACK_SACK_OPTIONS_LEN = 12;
constexpr uint8_t TCP_DEFAULT_HOP_LIMIT = 64;

struct TcpOutputRef {
    TcpCB* cb;
    explicit TcpOutputRef(TcpCB* value) : cb(value) { tcp_cb_acquire(cb); }
    ~TcpOutputRef() { tcp_cb_release(cb); }
};

auto tcp_output_device_by_ifindex(uint32_t ifindex) -> NetDeviceRef {
    if (ifindex == 0) {
        return {};
    }
    size_t const COUNT = netdev_count();
    for (size_t i = 0; i < COUNT; ++i) {
        NetDeviceRef dev = netdev_at_ref(i);
        if (dev && dev->ifindex == ifindex) {
            return dev;
        }
    }
    return {};
}

auto write_sack_option_locked(const TcpCB* cb, uint8_t* options, size_t capacity) -> size_t {
    if (cb == nullptr || options == nullptr || capacity < TCP_ACK_SACK_OPTIONS_LEN || !cb->sack_permitted) {
        return 0;
    }
    uint32_t left_edge = 0;
    uint32_t right_edge = 0;
    if (cb->ooo_head != nullptr) {
        left_edge = cb->ooo_head->seq;
        right_edge = cb->ooo_head->seq + static_cast<uint32_t>(cb->ooo_head->len);
    } else if (cb->ooo_fin_pending) {
        left_edge = cb->ooo_fin_seq;
        right_edge = cb->ooo_fin_seq + 1;
    } else {
        return 0;
    }
    uint32_t const LEFT_EDGE = htonl(left_edge);
    uint32_t const RIGHT_EDGE = htonl(right_edge);
    options[0] = TCP_OPTION_NOP;
    options[1] = TCP_OPTION_NOP;
    options[2] = TCP_OPTION_SACK;
    options[3] = TCP_OPTION_SACK_1_BLOCK_LEN;
    std::memcpy(options + 4, &LEFT_EDGE, sizeof(LEFT_EDGE));
    std::memcpy(options + 8, &RIGHT_EDGE, sizeof(RIGHT_EDGE));
    return TCP_ACK_SACK_OPTIONS_LEN;
}

auto tcp_fill_control_locked(TcpCB* cb, PacketBuffer* pkt, uint32_t seq, uint8_t flags, bool include_sack) -> bool {
    if (cb == nullptr || pkt == nullptr) {
        return false;
    }
    pkt->data = pkt->storage.data() + PKT_HEADROOM;
    pkt->len = 0;
    std::array<uint8_t, TCP_ACK_SACK_OPTIONS_LEN> options{};
    size_t const OPTS_LEN = include_sack ? write_sack_option_locked(cb, options.data(), options.size()) : 0;
    size_t const HDR_LEN = sizeof(TcpHeader) + OPTS_LEN;
    auto* payload = pkt->put(HDR_LEN);
    if (OPTS_LEN != 0) {
        std::memcpy(payload + sizeof(TcpHeader), options.data(), OPTS_LEN);
    }
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    *hdr = TcpHeader{};
    hdr->src_port = htons(cb->local.port);
    hdr->dst_port = htons(cb->remote.port);
    hdr->seq = htonl(seq);
    hdr->ack = htonl(cb->rcv_nxt);
    hdr->data_offset = static_cast<uint8_t>((HDR_LEN / 4U) << 4U);
    hdr->flags = flags;
    hdr->window = cb->ws_enabled ? htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale))
                                 : htons(static_cast<uint16_t>(std::min<uint32_t>(cb->rcv_wnd, UINT16_MAX)));
    return true;
}

auto tcp_build_control_locked(TcpCB* cb, uint32_t seq, uint8_t flags, bool include_sack, SocketEndpoint* out_local,
                              SocketEndpoint* out_remote) -> PacketBuffer* {
    // Control/liveness can consume the existing reserve, but may not grow the
    // packet pool or allocate physical memory with a protocol lock held.
    auto* pkt = pkt_alloc();
    if (!tcp_fill_control_locked(cb, pkt, seq, flags, include_sack)) {
        pkt_free(pkt);
        return nullptr;
    }
    if (out_local != nullptr) {
        *out_local = cb->local;
    }
    if (out_remote != nullptr) {
        *out_remote = cb->remote;
    }
    return pkt;
}
}  // namespace

auto tcp_transmit_prebuilt(PacketBuffer* pkt, const SocketEndpoint& local, const SocketEndpoint& remote, uint32_t bound_ifindex) -> int {
    if (pkt == nullptr || pkt->len < sizeof(TcpHeader)) {
        pkt_free(pkt);
        return -EINVAL;
    }
    auto* hdr = reinterpret_cast<TcpHeader*>(pkt->data);
    hdr->checksum = 0;
    if (remote.is_ipv4() || remote.is_v4_mapped()) {
        IPv4Address const SRC = local.ipv4_address();
        IPv4Address const DST = remote.ipv4_address();
        hdr->checksum = pseudo_header_checksum(SRC, DST, IPV6_PROTO_TCP, pkt->data, pkt->len);
        if (bound_ifindex != 0) {
            NetDeviceRef device = tcp_output_device_by_ifindex(bound_ifindex);
            auto* raw_device = device.get();
            if (!device || !pkt_adopt_netdev_ref(pkt, std::move(device))) {
                pkt_free(pkt);
                return -ENODEV;
            }
            return ipv4_tx_on_dev(pkt, raw_device, SRC, DST, IPV6_PROTO_TCP, IPV4_DEFAULT_TTL);
        }
        return ipv4_tx(pkt, SRC, DST, IPV6_PROTO_TCP, IPV4_DEFAULT_TTL);
    }

    IPv6Address requested{};
    const IPv6Address* requested_ptr = nullptr;
    if (!local.is_unspecified()) {
        requested = local.ipv6_address();
        requested_ptr = &requested;
    }
    uint32_t const IFINDEX = bound_ifindex != 0 ? bound_ifindex : remote.scope_id;
    IPv6OutputRoute route{};
    int const ROUTE_RESULT = ipv6_route_resolve(remote.ipv6_address(), requested_ptr, IFINDEX, route);
    if (ROUTE_RESULT < 0) {
        pkt_free(pkt);
        return ROUTE_RESULT;
    }
    hdr->checksum =
        checksum_pseudo_ipv6(route.source, remote.ipv6_address(), IPV6_PROTO_TCP, static_cast<uint32_t>(pkt->len), pkt->data, pkt->len);
    return ipv6_tx_routed(pkt, std::move(route), remote.ipv6_address(), IPV6_PROTO_TCP, TCP_DEFAULT_HOP_LIMIT);
}

bool tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len, bool persist_probe) {
    size_t const OPTIONS_LEN = (flags & TCP_SYN) != 0 ? TCP_SYN_OPTIONS_LEN : 0;
    if (cb == nullptr || len > PKT_BUF_SIZE - PKT_HEADROOM - sizeof(TcpHeader) - OPTIONS_LEN) {
        return false;
    }
    TcpOutputRef retained_cb{cb};
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return false;
    }
    size_t const SEQ_LEN = len + (((flags & TCP_SYN) != 0) ? 1U : 0U) + (((flags & TCP_FIN) != 0) ? 1U : 0U);
    PacketBuffer* rtx_pkt = nullptr;
    RetransmitEntry* rtx_entry = nullptr;
    if (SEQ_LEN > 0) {
        rtx_pkt = pkt_alloc_tx();
        if (rtx_pkt != nullptr) {
            rtx_entry = new (std::nothrow) RetransmitEntry{};
        }
        if (rtx_pkt == nullptr || rtx_entry == nullptr) {
            pkt_free(rtx_pkt);
            pkt_free(pkt);
            delete rtx_entry;
            return false;
        }
    }

    SocketEndpoint local{};
    SocketEndpoint remote{};
    uint32_t bound_ifindex = 0;
    uint64_t tx_id = 0;
    cb->lock.lock();
    TcpState const PREVIOUS_STATE = cb->state;
    uint64_t const GENERATION = cb->output_generation;
    bool const STREAM_DATA = len != 0 && (flags & (TCP_SYN | TCP_FIN)) == 0;
    bool const STREAM_STATE = cb->state == TcpState::ESTABLISHED || cb->state == TcpState::CLOSE_WAIT;
    bool const PERSIST_ADMISSION = persist_probe && STREAM_STATE && len == 1 && cb->snd_wnd == 0 && cb->snd_nxt == cb->snd_una;
    if ((STREAM_DATA && (!STREAM_STATE || (!PERSIST_ADMISSION && len > tcp_send_available_bytes(cb)))) ||
        (persist_probe && !PERSIST_ADMISSION) || ((flags & TCP_FIN) != 0 && !STREAM_STATE) ||
        (cb->sender_enabled && SEQ_LEN != 0 && cb->retransmit_entries >= TCP_RETRANSMIT_MAX_ENTRIES)) {
        cb->lock.unlock();
        pkt_free(rtx_pkt);
        pkt_free(pkt);
        delete rtx_entry;
        return false;
    }
    local = cb->local;
    remote = cb->remote;
    if (cb->socket != nullptr) {
        bound_ifindex = cb->socket->bound_ifindex;
    }
    uint32_t const SEQ = cb->snd_nxt - (((flags & TCP_SYN) != 0) ? 1U : 0U);

    std::array<uint8_t, TCP_SYN_OPTIONS_LEN> options{};
    size_t opts_len = 0;
    if ((flags & TCP_SYN) != 0) {
        options.at(0) = TCP_OPTION_MSS;
        options.at(1) = TCP_OPTION_MSS_LEN;
        uint16_t const MSS = htons(cb->rcv_mss);
        std::memcpy(options.data() + 2, &MSS, sizeof(MSS));
        options.at(4) = TCP_OPTION_SACK_PERMITTED;
        options.at(5) = TCP_OPTION_SACK_PERMITTED_LEN;
        options.at(6) = TCP_OPTION_NOP;
        options.at(7) = TCP_OPTION_WSCALE;
        options.at(8) = TCP_OPTION_WSCALE_LEN;
        options.at(9) = cb->rcv_wscale;
        options.at(10) = TCP_OPTION_EOL;
        options.at(11) = TCP_OPTION_EOL;
        opts_len = TCP_SYN_OPTIONS_LEN;
    }
    size_t const HDR_LEN = sizeof(TcpHeader) + opts_len;
    auto* payload = pkt->put(HDR_LEN + len);
    if (len > 0 && data != nullptr) {
        std::memcpy(payload + HDR_LEN, data, len);
    }
    if (opts_len != 0) {
        std::memcpy(payload + sizeof(TcpHeader), options.data(), opts_len);
    }
    auto* hdr = reinterpret_cast<TcpHeader*>(payload);
    *hdr = TcpHeader{};
    hdr->src_port = htons(local.port);
    hdr->dst_port = htons(remote.port);
    hdr->seq = htonl(SEQ);
    hdr->ack = (flags & TCP_ACK) != 0 ? htonl(cb->rcv_nxt) : 0;
    hdr->data_offset = static_cast<uint8_t>((HDR_LEN / 4U) << 4U);
    hdr->flags = flags;
    hdr->window = ((flags & TCP_SYN) != 0 || !cb->ws_enabled) ? htons(static_cast<uint16_t>(std::min<uint32_t>(cb->rcv_wnd, UINT16_MAX)))
                                                              : htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale));

    if (rtx_pkt != nullptr) {
        std::memcpy(rtx_pkt->storage.data(), pkt->storage.data(), PKT_BUF_SIZE);
        rtx_pkt->data = rtx_pkt->storage.data() + (pkt->data - pkt->storage.data());
        rtx_pkt->len = pkt->len;
    }
    if ((flags & TCP_ACK) != 0 && len > 0) {
        cb->segs_pending_ack = 0;
        cb->delayed_ack_deadline = 0;
    }
    cb->snd_nxt += static_cast<uint32_t>(len);
    if ((flags & TCP_FIN) != 0) {
        cb->snd_nxt++;
        cb->state = PREVIOUS_STATE == TcpState::ESTABLISHED ? TcpState::FIN_WAIT_1 : TcpState::LAST_ACK;
    }
    if (rtx_entry != nullptr) {
        rtx_entry->pkt = rtx_pkt;
        rtx_entry->seq = SEQ;
        rtx_entry->len = SEQ_LEN;
        rtx_entry->send_time_ms = tcp_now_ms();
        tx_id = ++cb->next_tx_id;
        rtx_entry->tx_id = tx_id;
        ++cb->retransmit_entries;
        if (PERSIST_ADMISSION) {
            cb->persist_needed = true;
            cb->persist_interval = TCP_PERSIST_INITIAL_MS;
            cb->persist_deadline = tcp_deadline_after_ms(tcp_now_ms(), cb->persist_interval);
        }
        if (cb->retransmit_head == nullptr) {
            cb->retransmit_head = rtx_entry;
            cb->retransmit_tail = rtx_entry;
            cb->retransmit_deadline = tcp_deadline_after_ms(tcp_now_ms(), cb->rto_ms);
            tcp_timer_arm(cb);
        } else {
            cb->retransmit_tail->next = rtx_entry;
            cb->retransmit_tail = rtx_entry;
        }
    }
    cb->lock.unlock();

    int const TRANSMIT_RESULT = tcp_transmit_prebuilt(pkt, local, remote, bound_ifindex);
    if (TRANSMIT_RESULT < 0) {
        bool accepted = false;
        if (rtx_entry != nullptr) {
            cb->lock.lock();
            RetransmitEntry** link = &cb->retransmit_head;
            RetransmitEntry* previous = nullptr;
            while (*link != nullptr && (*link)->tx_id != tx_id) {
                previous = *link;
                link = &(*link)->next;
            }
            if (*link != nullptr) {
                rtx_entry = *link;
                // Another writer or a staged retry may already depend on this
                // sequence range. Keep it queued rather than leave a hole.
                accepted = cb->output_generation != GENERATION || cb->snd_nxt != SEQ + SEQ_LEN || rtx_entry->retries != 0 ||
                           tcp_seq_after(cb->snd_una, SEQ);
            } else {
                // An inline/parallel ACK can retire the range before output
                // returns. The bytes have already been accepted by the peer.
                accepted = true;
            }
            if (*link != nullptr && !accepted) {
                rtx_entry = *link;
                *link = rtx_entry->next;
                if (cb->retransmit_tail == rtx_entry) {
                    cb->retransmit_tail = previous;
                }
                uint32_t const ADVANCED = static_cast<uint32_t>(len) + (((flags & TCP_FIN) != 0) ? 1U : 0U);
                if (ADVANCED != 0 && cb->snd_nxt == SEQ + SEQ_LEN) {
                    cb->snd_nxt -= ADVANCED;
                    if ((flags & TCP_FIN) != 0) {
                        cb->state = PREVIOUS_STATE;
                    }
                }
                if (cb->retransmit_head == nullptr) {
                    cb->retransmit_deadline = 0;
                }
                --cb->retransmit_entries;
                pkt_free(rtx_entry->pkt);
                delete rtx_entry;
            }
            if (cb->socket != nullptr && !accepted) {
                cb->socket->pending_error.store(-TRANSMIT_RESULT, std::memory_order_release);
            }
            cb->lock.unlock();
        }
        return accepted;
    }
    return true;
}

void tcp_send_rst(const SocketEndpoint& source, const SocketEndpoint& destination, uint32_t seq, uint32_t ack, uint8_t extra_flags,
                  uint32_t bound_ifindex) {
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return;
    }
    auto* hdr = reinterpret_cast<TcpHeader*>(pkt->put(sizeof(TcpHeader)));
    *hdr = TcpHeader{};
    hdr->src_port = htons(source.port);
    hdr->dst_port = htons(destination.port);
    hdr->seq = htonl(seq);
    hdr->ack = htonl(ack);
    hdr->data_offset = (sizeof(TcpHeader) / 4U) << 4U;
    hdr->flags = TCP_RST | extra_flags;
    static_cast<void>(tcp_transmit_prebuilt(pkt, source, destination, bound_ifindex));
}

auto tcp_build_ack(TcpCB* cb, SocketEndpoint* out_local, SocketEndpoint* out_remote) -> PacketBuffer* {
    return tcp_build_control_locked(cb, cb->snd_nxt, TCP_ACK, true, out_local, out_remote);
}

bool tcp_send_ack(TcpCB* cb) {
    TcpOutputRef retained_cb{cb};
    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return false;
    }
    SocketEndpoint local{};
    SocketEndpoint remote{};
    uint32_t bound_ifindex = 0;
    cb->lock.lock();
    static_cast<void>(tcp_fill_control_locked(cb, pkt, cb->snd_nxt, TCP_ACK, false));
    local = cb->local;
    remote = cb->remote;
    if (cb->socket != nullptr) {
        bound_ifindex = cb->socket->bound_ifindex;
    }
    cb->lock.unlock();
    return pkt != nullptr && tcp_transmit_prebuilt(pkt, local, remote, bound_ifindex) >= 0;
}

auto tcp_build_keepalive_probe(TcpCB* cb, SocketEndpoint* out_local, SocketEndpoint* out_remote) -> PacketBuffer* {
    return tcp_build_control_locked(cb, cb->snd_una - 1, TCP_ACK, false, out_local, out_remote);
}

auto tcp_build_ack_locked(TcpCB* cb, PacketBuffer* spare) -> bool { return tcp_fill_control_locked(cb, spare, cb->snd_nxt, TCP_ACK, true); }

auto tcp_build_keepalive_locked(TcpCB* cb, PacketBuffer* spare) -> bool {
    return tcp_fill_control_locked(cb, spare, cb->snd_una - 1, TCP_ACK, false);
}

auto tcp_build_retransmit_locked(TcpCB* cb, RetransmitEntry* entry, uint32_t start_seq, uint32_t end_seq, PacketBuffer* spare) -> bool {
    if (cb == nullptr || entry == nullptr || entry->pkt == nullptr || spare == nullptr || entry->pkt->len < sizeof(TcpHeader)) {
        return false;
    }
    auto const* original = reinterpret_cast<const TcpHeader*>(entry->pkt->data);
    size_t const HEADER_LEN = static_cast<size_t>(original->data_offset >> 4U) * 4U;
    if (HEADER_LEN < sizeof(TcpHeader) || HEADER_LEN > entry->pkt->len) {
        return false;
    }
    uint32_t const START = start_seq - entry->seq;
    uint32_t const END = end_seq - entry->seq;
    if (START >= END || END > entry->len) {
        return false;
    }
    uint32_t const SYN_LEN = (original->flags & TCP_SYN) != 0 ? 1U : 0U;
    uint32_t const DATA_END = SYN_LEN + static_cast<uint32_t>(entry->pkt->len - HEADER_LEN);
    uint32_t const DATA_START = std::max(START, SYN_LEN);
    uint32_t const DATA_STOP = std::min(END, DATA_END);
    size_t const DATA_LEN = DATA_STOP > DATA_START ? DATA_STOP - DATA_START : 0;
    bool const INCLUDE_SYN = SYN_LEN != 0 && START == 0;
    bool const INCLUDE_FIN = (original->flags & TCP_FIN) != 0 && END > DATA_END && START <= DATA_END;
    size_t const OUTPUT_HEADER = INCLUDE_SYN ? HEADER_LEN : sizeof(TcpHeader);
    spare->data = spare->storage.data() + PKT_HEADROOM;
    spare->len = 0;
    uint8_t* bytes = spare->put(OUTPUT_HEADER + DATA_LEN);
    std::memcpy(bytes, entry->pkt->data, OUTPUT_HEADER);
    if (DATA_LEN != 0) {
        std::memcpy(bytes + OUTPUT_HEADER, entry->pkt->data + HEADER_LEN + DATA_START - SYN_LEN, DATA_LEN);
    }
    auto* header = reinterpret_cast<TcpHeader*>(bytes);
    header->seq = htonl(start_seq);
    header->ack = (header->flags & TCP_ACK) != 0 ? htonl(cb->rcv_nxt) : 0;
    header->data_offset = static_cast<uint8_t>((OUTPUT_HEADER / 4U) << 4U);
    header->flags &= static_cast<uint8_t>(~(TCP_SYN | TCP_FIN));
    header->flags |= static_cast<uint8_t>((INCLUDE_SYN ? TCP_SYN : 0) | (INCLUDE_FIN ? TCP_FIN : 0));
    header->window = (!cb->ws_enabled || INCLUDE_SYN) ? htons(static_cast<uint16_t>(std::min<uint32_t>(cb->rcv_wnd, UINT16_MAX)))
                                                      : htons(static_cast<uint16_t>(cb->rcv_wnd >> cb->rcv_wscale));
    header->checksum = 0;
    if (entry->retries < UINT8_MAX) {
        ++entry->retries;
    }
    entry->send_time_ms = tcp_now_ms();
    return true;
}

auto tcp_build_probe_locked(TcpCB* cb, PacketBuffer* spare) -> bool {
    for (auto* entry = cb->retransmit_head; entry != nullptr; entry = entry->next) {
        uint32_t const END = entry->seq + static_cast<uint32_t>(entry->len);
        if (tcp_seq_after(END, cb->snd_una)) {
            return tcp_build_retransmit_locked(cb, entry, cb->snd_una, cb->snd_una + 1, spare);
        }
    }
    return tcp_build_keepalive_locked(cb, spare);
}

}  // namespace ker::net::proto
