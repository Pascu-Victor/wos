#include <bits/ssize_t.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <utility>

#include "net/socket.hpp"
#include "tcp.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"tcp">;

namespace {
constexpr auto TCP_IPV4_TTL = static_cast<uint8_t>(IPV4_DEFAULT_TTL);
constexpr uint8_t TCP_OPTION_EOL = 0;
constexpr uint8_t TCP_OPTION_NOP = 1;
constexpr uint8_t TCP_OPTION_MSS = 2;
constexpr uint8_t TCP_OPTION_WSCALE = 3;
constexpr uint8_t TCP_OPTION_MSS_LEN = 4;
constexpr uint8_t TCP_OPTION_WSCALE_LEN = 3;
constexpr size_t TCP_OPTION_LEN_OFFSET = 1;
constexpr size_t TCP_OPTION_DATA_OFFSET = 2;
constexpr size_t TCP_OOO_MAX_BYTES = static_cast<size_t>(1024U) * 1024U;
constexpr size_t TCP_OOO_MAX_SEGMENTS = 512;

auto tcp_header_len(const TcpHeader* hdr) -> size_t { return static_cast<size_t>(hdr->data_offset >> 4U) * sizeof(uint32_t); }

auto read_be16_unaligned(const uint8_t* bytes) -> uint16_t {
    return static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8U) | bytes[1]);
}

void parse_syn_options(TcpCB* cb, const TcpHeader* hdr, bool refresh_receive_wscale) {
    size_t const HDR_LEN = tcp_header_len(hdr);
    if (HDR_LEN <= sizeof(TcpHeader)) {
        return;
    }

    const auto* opts = reinterpret_cast<const uint8_t*>(hdr) + sizeof(TcpHeader);
    size_t const OPTS_LEN = HDR_LEN - sizeof(TcpHeader);
    for (size_t i = 0; i < OPTS_LEN;) {
        if (opts[i] == TCP_OPTION_EOL) {
            break;
        }
        if (opts[i] == TCP_OPTION_NOP) {
            i++;
            continue;
        }
        if (i + TCP_OPTION_LEN_OFFSET >= OPTS_LEN) {
            break;
        }
        uint8_t const OPT_LEN = opts[i + TCP_OPTION_LEN_OFFSET];
        if (OPT_LEN < TCP_OPTION_DATA_OFFSET || i + OPT_LEN > OPTS_LEN) {
            break;
        }
        if (opts[i] == TCP_OPTION_MSS && OPT_LEN == TCP_OPTION_MSS_LEN) {
            cb->snd_mss = read_be16_unaligned(opts + i + TCP_OPTION_DATA_OFFSET);
        } else if (opts[i] == TCP_OPTION_WSCALE && OPT_LEN == TCP_OPTION_WSCALE_LEN) {
            cb->snd_wscale = opts[i + TCP_OPTION_DATA_OFFSET];
            cb->ws_enabled = true;
            if (refresh_receive_wscale && cb->socket != nullptr) {
                cb->rcv_wscale = tcp_wscale_for_buf(cb->socket->rcvbuf.capacity);
            }
        }
        i += OPT_LEN;
    }
}

// Wake a task blocked on this socket.
void wake_socket(Socket* sock) {
    if (sock == nullptr) {
        return;
    }
    uint64_t const PID = sock->owner_pid;
#ifdef TCP_DEBUG
    log::debug("wake_socket: sock=%p owner_pid=%lu", static_cast<void*>(sock), PID);
#endif
    if (PID != 0) {
        auto* task = ker::mod::sched::find_task_by_pid_safe(PID);
#ifdef TCP_DEBUG
        log::debug("wake_socket: pid=%lu task=%p", PID, static_cast<void*>(task));
#endif
        if (task != nullptr) {
#ifdef TCP_DEBUG
            log::debug("wake_socket: event-waking pid=%lu", PID);
#endif
            ker::mod::sched::wake_task_from_event(task);
            task->release();
        }
    }
}

// Free all retransmit entries.
void drain_retransmit_queue(TcpCB* cb) {
    [[maybe_unused]]
    size_t n = 0;
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        delete entry;
        n++;
    }
    cb->retransmit_tail = nullptr;
#ifdef TCP_DEBUG
    if (n > 0) {
        log::debug("drain_rtx: freed %zu entries port=%u pool_free=%zu", n, cb->local_port, ker::net::pkt_pool_free_count());
    }
#endif
}

auto queue_in_order_payload(TcpCB* cb, Socket* sock, const uint8_t* payload, size_t payload_len) -> bool {
    if (cb == nullptr || sock == nullptr || payload == nullptr || payload_len == 0) {
        return false;
    }

    size_t const FREE = sock->rcvbuf.free_space();
    if (FREE < payload_len) {
        tcp_refresh_receive_window(cb);
#ifdef TCP_DEBUG
        log::trace("rx full: port=%u free=%zu payload=%zu rcv_nxt=%u", cb->local_port, FREE, payload_len, cb->rcv_nxt);
#endif
        return false;
    }

    ssize_t const WRITTEN = sock->rcvbuf.write(payload, payload_len);
    if (std::cmp_not_equal(WRITTEN, payload_len)) {
        log::warn("rx partial queue write port=%u written=%zd payload=%zu free=%zu", cb->local_port, WRITTEN, payload_len, FREE);
        if (WRITTEN <= 0) {
            tcp_refresh_receive_window(cb);
            return false;
        }
        cb->rcv_nxt += static_cast<uint32_t>(WRITTEN);
        tcp_refresh_receive_window(cb);
        return true;
    }

    cb->rcv_nxt += static_cast<uint32_t>(payload_len);
    tcp_refresh_receive_window(cb);
    return true;
}

auto tcp_seq_end(uint32_t seq, size_t len) -> uint32_t { return seq + static_cast<uint32_t>(len); }

auto count_out_of_order_segments(const TcpCB* cb) -> size_t {
    size_t count = 0;
    for (auto* seg = cb != nullptr ? cb->ooo_head : nullptr; seg != nullptr; seg = seg->next) {
        ++count;
    }
    return count;
}

void drop_out_of_order_segment(TcpCB* cb, TcpOutOfOrderSegment* seg) {
    if (cb == nullptr || seg == nullptr) {
        return;
    }
    size_t const OOO_BYTES = cb->ooo_bytes.load(std::memory_order_acquire);
    cb->ooo_bytes.store(OOO_BYTES > seg->len ? OOO_BYTES - seg->len : 0, std::memory_order_release);
    cb->ooo_allocated_bytes = cb->ooo_allocated_bytes > seg->allocated_len ? cb->ooo_allocated_bytes - seg->allocated_len : 0;
    delete[] seg->data;
    delete seg;
    tcp_refresh_receive_window(cb);
}

void discard_stale_out_of_order_segments(TcpCB* cb) {
    while (cb != nullptr && cb->ooo_head != nullptr) {
        uint32_t const END = tcp_seq_end(cb->ooo_head->seq, cb->ooo_head->len);
        if (!tcp_seq_after(END, cb->rcv_nxt)) {
            auto* stale = cb->ooo_head;
            cb->ooo_head = stale->next;
            drop_out_of_order_segment(cb, stale);
            continue;
        }

        if (tcp_seq_before(cb->ooo_head->seq, cb->rcv_nxt)) {
            auto const TRIM = static_cast<size_t>(cb->rcv_nxt - cb->ooo_head->seq);
            if (TRIM >= cb->ooo_head->len) {
                auto* stale = cb->ooo_head;
                cb->ooo_head = stale->next;
                drop_out_of_order_segment(cb, stale);
                continue;
            }
            std::memmove(cb->ooo_head->data, cb->ooo_head->data + TRIM, cb->ooo_head->len - TRIM);
            cb->ooo_head->seq = cb->rcv_nxt;
            cb->ooo_head->len -= TRIM;
            size_t const OOO_BYTES = cb->ooo_bytes.load(std::memory_order_acquire);
            cb->ooo_bytes.store(OOO_BYTES > TRIM ? OOO_BYTES - TRIM : 0, std::memory_order_release);
            tcp_refresh_receive_window(cb);
        }
        break;
    }
}

auto queue_out_of_order_payload(TcpCB* cb, const uint8_t* payload, size_t payload_len, uint32_t seq) -> bool {
    if (cb == nullptr || payload == nullptr || payload_len == 0 || cb->socket == nullptr) {
        return false;
    }

    size_t const OOO_BYTES = cb->ooo_bytes.load(std::memory_order_acquire);
    if (payload_len > TCP_OOO_MAX_BYTES || cb->ooo_allocated_bytes + payload_len > TCP_OOO_MAX_BYTES ||
        count_out_of_order_segments(cb) >= TCP_OOO_MAX_SEGMENTS) {
        tcp_refresh_receive_window(cb);
        return false;
    }

    size_t const BUFFERED = cb->socket->rcvbuf.available() + OOO_BYTES;
    if (BUFFERED > cb->socket->rcvbuf.capacity || payload_len > cb->socket->rcvbuf.capacity - BUFFERED) {
        tcp_refresh_receive_window(cb);
        return false;
    }

    uint32_t const END = tcp_seq_end(seq, payload_len);
    uint32_t const WINDOW_END = cb->rcv_nxt + tcp_receive_window_space(cb, cb->socket);
    if (!tcp_seq_after(seq, cb->rcv_nxt) || tcp_seq_after(END, WINDOW_END)) {
        tcp_refresh_receive_window(cb);
        return false;
    }

    TcpOutOfOrderSegment** link = &cb->ooo_head;
    while (*link != nullptr && tcp_seq_before((*link)->seq, seq)) {
        uint32_t const EXISTING_END = tcp_seq_end((*link)->seq, (*link)->len);
        if (tcp_seq_after(EXISTING_END, seq)) {
            tcp_refresh_receive_window(cb);
            return false;
        }
        link = &(*link)->next;
    }

    if (*link != nullptr && tcp_seq_after(END, (*link)->seq)) {
        tcp_refresh_receive_window(cb);
        return false;
    }

    auto* data = new (std::nothrow) uint8_t[payload_len];
    if (data == nullptr) {
        return false;
    }
    std::memcpy(data, payload, payload_len);

    auto* seg = new (std::nothrow) TcpOutOfOrderSegment{
        .seq = seq,
        .len = payload_len,
        .allocated_len = payload_len,
        .data = data,
        .next = *link,
    };
    if (seg == nullptr) {
        delete[] data;
        return false;
    }

    *link = seg;
    cb->ooo_allocated_bytes += payload_len;
    cb->ooo_bytes.fetch_add(payload_len, std::memory_order_release);
    tcp_refresh_receive_window(cb);
    return true;
}

auto drain_out_of_order_payload(TcpCB* cb, Socket* sock) -> bool {
    if (cb == nullptr || sock == nullptr) {
        return false;
    }

    bool progressed = false;
    discard_stale_out_of_order_segments(cb);
    while (cb->ooo_head != nullptr && cb->ooo_head->seq == cb->rcv_nxt) {
        auto* seg = cb->ooo_head;
        if (sock->rcvbuf.free_space() < seg->len) {
            tcp_refresh_receive_window(cb);
            break;
        }
        cb->ooo_head = seg->next;
        if (queue_in_order_payload(cb, sock, seg->data, seg->len)) {
            progressed = true;
        }
        drop_out_of_order_segment(cb, seg);
        discard_stale_out_of_order_segments(cb);
    }
    return progressed;
}

// Handle SYN on a listening socket.
void handle_listen_syn(TcpCB* listener, const TcpHeader* hdr, IPv4Address src_ip, IPv4Address dst_ip) {
    Socket const* listen_sock = listener->socket;
    if (listen_sock == nullptr) {
        return;
    }

    if (std::cmp_greater_equal(listen_sock->aq_count, listen_sock->backlog)) {
        log::warn("accept queue full port=%u aq=%zu backlog=%d", listener->local_port, listen_sock->aq_count, listen_sock->backlog);
        return;
    }

    // socket_create() already allocates a TcpCB for SOCK_STREAM.
    Socket* child = socket_create(listen_sock->domain, listen_sock->type, listen_sock->protocol);
    if (child == nullptr) {
        return;
    }

    auto* child_cb = static_cast<TcpCB*>(child->proto_data);
    if (child_cb == nullptr) {
        socket_destroy(child);
        return;
    }

    child_cb->socket = child;

    child_cb->local_ip = dst_ip;
    child_cb->local_port = listener->local_port;
    child_cb->remote_ip = src_ip;
    child_cb->remote_port = ntohs(hdr->src_port);

    child->local_v4.addr = dst_ip;
    child->local_v4.port = listener->local_port;
    child->remote_v4.addr = src_ip;
    child->remote_v4.port = ntohs(hdr->src_port);

    child->nonblock = listen_sock->nonblock;

    child_cb->irs = ntohl(hdr->seq);
    child_cb->rcv_nxt = child_cb->irs + 1;
    child_cb->rcv_wnd = child->rcvbuf.capacity;
    child_cb->rcv_wscale = tcp_wscale_for_buf(child->rcvbuf.capacity);

    child_cb->iss = ntohl(hdr->seq) ^ 0xDEADBEEF;
    child_cb->iss += tcp_now_ms();
    child_cb->snd_una = child_cb->iss;
    child_cb->snd_nxt = child_cb->iss + 1;
    // SYN window is not scaled (RFC 1323).
    child_cb->snd_wnd = ntohs(hdr->window);

    parse_syn_options(child_cb, hdr, false);

    child_cb->state = TcpState::SYN_RECEIVED;
    child->state = SocketState::CONNECTING;

    tcp_insert_cb(child_cb);

    tcp_send_segment(child_cb, TCP_SYN | TCP_ACK, nullptr, 0);
}
}  // namespace

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload, size_t payload_len, IPv4Address src_ip,
                         IPv4Address dst_ip) {
    NET_TRACE_SPAN(SPAN_TCP_PROCESS);
    uint8_t flags = hdr->flags;
    uint32_t const SEG_SEQ = ntohl(hdr->seq);
    uint32_t seg_ack = ntohl(hdr->ack);
    uint16_t seg_wnd = ntohs(hdr->window);

    // Build ACK/wake decisions under lock, execute after unlock.
    PacketBuffer* deferred_ack = nullptr;
    uint32_t defer_local = 0;
    uint32_t defer_remote = 0;
    bool defer_ack_pending = false;
    bool deferred_wake = false;
    uint64_t cb_lock_flags = 0;

    // Rebuilding supersedes an earlier deferred ACK.
    auto build_deferred_ack = [&]() {
        if (deferred_ack != nullptr) {
            pkt_free(deferred_ack);
        }
        deferred_ack = tcp_build_ack(cb, &defer_local, &defer_remote);
        if (deferred_ack == nullptr) {
            defer_ack_pending = true;
        }
    };

    auto process_stream_ack = [&]() -> void {
        if ((flags & TCP_ACK) == 0) {
            return;
        }

        bool const VALID_ACK = !tcp_seq_after(seg_ack, cb->snd_nxt);
        bool const ACK_ADVANCES = tcp_seq_after(seg_ack, cb->snd_una);
        if (VALID_ACK && (ACK_ADVANCES || seg_wnd != cb->snd_wnd)) {
            cb->snd_wnd = static_cast<uint32_t>(seg_wnd) << cb->snd_wscale;
            if (!ACK_ADVANCES) {
                deferred_wake = true;
            }
        }
        if (ACK_ADVANCES && VALID_ACK) {
            cb->snd_una = seg_ack;

            bool rtt_sampled = false;
            uint64_t const NOW = tcp_now_ms();
            while (cb->retransmit_head != nullptr) {
                auto* entry = cb->retransmit_head;
                uint32_t const ENTRY_END = entry->seq + static_cast<uint32_t>(entry->len);
                if (!tcp_seq_after(ENTRY_END, seg_ack)) {
                    if (!rtt_sampled && entry->retries == 0) {
                        uint64_t rtt = NOW - entry->send_time_ms;
                        if (rtt == 0) {
                            rtt = 1;
                        }
                        if (cb->srtt_ms == 0) {
                            cb->srtt_ms = rtt;
                            cb->rttvar_ms = rtt / 2;
                        } else {
                            int64_t const DELTA = static_cast<int64_t>(rtt) - static_cast<int64_t>(cb->srtt_ms);
                            cb->srtt_ms = cb->srtt_ms + (DELTA / 8);
                            int64_t const ABS_DELTA = DELTA < 0 ? -DELTA : DELTA;
                            cb->rttvar_ms = cb->rttvar_ms + ((ABS_DELTA - static_cast<int64_t>(cb->rttvar_ms)) / 4);
                        }
                        cb->rto_ms = cb->srtt_ms + (4 * cb->rttvar_ms);
                        cb->rto_ms = std::max<uint64_t>(cb->rto_ms, 50ULL);
                        cb->rto_ms = std::min<uint64_t>(cb->rto_ms, 60000);
                        rtt_sampled = true;
                    }

                    cb->retransmit_head = entry->next;
                    if (cb->retransmit_head == nullptr) {
                        cb->retransmit_tail = nullptr;
                    }
                    if (entry->pkt != nullptr) {
                        pkt_free(entry->pkt);
                    }
                    delete entry;
                } else {
                    break;
                }
            }

            if (cb->retransmit_head != nullptr) {
                cb->retransmit_deadline = NOW + cb->rto_ms;
            }

            cb->ack_pending = false;
            deferred_wake = true;
        }
    };

    cb_lock_flags = cb->lock.lock_irqsave();

    switch (cb->state) {
        case TcpState::SYN_SENT: {
            if ((flags & TCP_ACK) != 0 && (flags & TCP_SYN) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->irs = SEG_SEQ;
                    cb->rcv_nxt = SEG_SEQ + 1;
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;
                    cb->lock.unlock_irqrestore(cb_lock_flags);

                    parse_syn_options(cb, hdr, true);

                    cb->state = TcpState::ESTABLISHED;
                    tcp_send_ack(cb);

                    wake_socket(cb->socket);

                    // Lock already dropped above.
                    tcp_cb_release(cb);
                    return;
                }
            } else if ((flags & TCP_RST) != 0) {
                cb->state = TcpState::CLOSED;
                deferred_wake = true;
            }
            break;
        }

        case TcpState::SYN_RECEIVED: {
            if ((flags & TCP_RST) != 0) {
                while (cb->retransmit_head != nullptr) {
                    auto* entry = cb->retransmit_head;
                    cb->retransmit_head = entry->next;
                    if (entry->pkt != nullptr) {
                        pkt_free(entry->pkt);
                    }
                    delete entry;
                }
                cb->retransmit_tail = nullptr;
                cb->state = TcpState::CLOSED;
                break;
            }
            if ((flags & TCP_ACK) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->state = TcpState::ESTABLISHED;
                    cb->snd_una = seg_ack;

                    // Drop SYN-ACK retransmit entries after handshake.
                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t const ENTRY_END = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(ENTRY_END, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (cb->retransmit_head == nullptr) {
                                cb->retransmit_tail = nullptr;
                            }
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            delete entry;
                        } else {
                            break;
                        }
                    }
                    cb->snd_wnd = static_cast<uint32_t>(seg_wnd) << cb->snd_wscale;

                    // Release cb->lock before tcp_find_listener (takes tcb_list_lock).
                    Socket* child_sock = cb->socket;
                    uint32_t const SAVED_LOCAL_IP = cb->local_ip;
                    uint16_t const SAVED_LOCAL_PORT = cb->local_port;
                    cb->lock.unlock_irqrestore(cb_lock_flags);

                    if (child_sock != nullptr) {
                        child_sock->state = SocketState::CONNECTED;
                        TcpCB* listener = tcp_find_listener(SAVED_LOCAL_IP, SAVED_LOCAL_PORT);
#ifdef TCP_DEBUG
                        log::debug("SYN_RCVD->ESTAB: port=%u listener=%p owner_pid=%lu", SAVED_LOCAL_PORT, static_cast<void*>(listener),
                                   (listener != nullptr && listener->socket != nullptr) ? listener->socket->owner_pid : 0UL);
#endif
                        if (listener != nullptr && listener->socket != nullptr) {
                            Socket* lsock = listener->socket;
                            uint64_t const LSOCK_FLAGS = lsock->lock.lock_irqsave();
                            if (std::cmp_less(lsock->aq_count, lsock->backlog)) {
                                child_sock->accept_next = nullptr;
                                if (lsock->aq_tail != nullptr) {
                                    lsock->aq_tail->accept_next = child_sock;
                                } else {
                                    lsock->aq_head = child_sock;
                                }
                                lsock->aq_tail = child_sock;
                                lsock->aq_count++;
                            }
#ifdef TCP_DEBUG
                            log::debug("SYN_RCVD->ESTAB: enqueued child, aq_count=%zu", lsock->aq_count);
#endif
                            lsock->lock.unlock_irqrestore(LSOCK_FLAGS);
                            wake_socket(lsock);
                        } else {
#ifdef TCP_DEBUG
                            log::debug("SYN_RCVD->ESTAB: no listener found for port=%u", SAVED_LOCAL_PORT);
#endif
                        }
                        if (listener != nullptr) {
                            tcp_cb_release(listener);
                        }
                    }

                    if (payload_len > 0 && child_sock != nullptr) {
                        cb_lock_flags = cb->lock.lock_irqsave();
                        if (SEG_SEQ == cb->rcv_nxt) {
                            if (queue_in_order_payload(cb, child_sock, payload, payload_len)) {
                                static_cast<void>(drain_out_of_order_payload(cb, child_sock));
                                deferred_wake = true;
                            }
                            build_deferred_ack();
                        }
                        cb->lock.unlock_irqrestore(cb_lock_flags);
                        if (deferred_ack != nullptr) {
                            if (ipv4_tx(deferred_ack, defer_local, defer_remote, IPPROTO_TCP, TCP_IPV4_TTL) < 0) {
                                cb_lock_flags = cb->lock.lock_irqsave();
                                cb->ack_pending = true;
                                tcp_timer_arm(cb);
                                cb->lock.unlock_irqrestore(cb_lock_flags);
                            }
                            deferred_ack = nullptr;
                        } else if (defer_ack_pending) {
                            cb_lock_flags = cb->lock.lock_irqsave();
                            cb->ack_pending = true;
                            tcp_timer_arm(cb);
                            cb->lock.unlock_irqrestore(cb_lock_flags);
                            defer_ack_pending = false;
                        }
                    }

                    tcp_cb_release(cb);
                    return;
                }
            }
            break;
        }

        case TcpState::ESTABLISHED: {
            if ((flags & TCP_RST) != 0) {
                while (cb->retransmit_head != nullptr) {
                    auto* entry = cb->retransmit_head;
                    cb->retransmit_head = entry->next;
                    if (entry->pkt != nullptr) {
                        pkt_free(entry->pkt);
                    }
                    delete entry;
                }
                cb->retransmit_tail = nullptr;
                cb->state = TcpState::CLOSED;
                cb->keepalive_deadline = 0;
                deferred_wake = true;
                break;
            }

            // Any valid segment resets the keepalive idle timer.
            if (cb->keepalive_enabled) {
                cb->keepalive_count = 0;
                cb->keepalive_deadline = tcp_now_ms() + cb->keepalive_idle_ms;
                tcp_timer_arm(cb);
            }

            process_stream_ack();

            if (payload_len > 0 && SEG_SEQ == cb->rcv_nxt) {
                bool accepted_payload = false;
                bool drained_ooo = false;
                if (cb->socket != nullptr) {
                    if (queue_in_order_payload(cb, cb->socket, payload, payload_len)) {
                        accepted_payload = true;
                        drained_ooo = drain_out_of_order_payload(cb, cb->socket);
                        deferred_wake = true;
                    } else {
                        log::warn("rcvbuf full port=%u avail=%zu cap=%zu pktlen=%zu", cb->local_port, cb->socket->rcvbuf.available(),
                                  cb->socket->rcvbuf.capacity, payload_len);
                    }
                }
                // Interactive streams such as SSH commonly send tiny PSH
                // records during key exchange. ACK those immediately; relying
                // on the delayed-ACK timer here can stall the peer if the timer
                // worker is not scheduled quickly enough under early boot/load.
                const bool ACK_IMMEDIATELY = (flags & TCP_PSH) != 0 || payload_len < cb->rcv_mss;
                bool should_ack_now = !accepted_payload || drained_ooo || ACK_IMMEDIATELY;
                if (!should_ack_now) {
                    should_ack_now = ++cb->segs_pending_ack >= 2;
                }
                if (should_ack_now) {
                    cb->segs_pending_ack = 0;
                    cb->delayed_ack_deadline = 0;
                    build_deferred_ack();
                    if (deferred_ack == nullptr) {
                        cb->ack_pending = true;
                        tcp_timer_arm(cb);
                    }
                } else if (cb->delayed_ack_deadline == 0) {
                    cb->delayed_ack_deadline = tcp_now_ms() + 40;
                    tcp_timer_arm(cb);
                }
            } else if (payload_len > 0) {
                // Duplicate or out-of-order data still needs an ACK carrying
                // the current rcv_nxt. Without this, a lost delayed ACK leaves
                // the peer retransmitting a segment we already consumed.
                if (cb->socket != nullptr) {
                    tcp_refresh_receive_window(cb);
                    if (tcp_seq_after(SEG_SEQ, cb->rcv_nxt)) {
                        static_cast<void>(queue_out_of_order_payload(cb, payload, payload_len, SEG_SEQ));
                    }
                }
                cb->segs_pending_ack = 0;
                cb->delayed_ack_deadline = 0;
                build_deferred_ack();
            } else if (payload_len == 0 && (flags & TCP_ACK) != 0 && tcp_seq_before(SEG_SEQ, cb->rcv_nxt)) {
                // Keepalive probe
                build_deferred_ack();
            }

            // FIN: ACK immediately and cancel delayed ACK.
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = SEG_SEQ + static_cast<uint32_t>(payload_len) + 1;
                cb->segs_pending_ack = 0;
                cb->delayed_ack_deadline = 0;
                cb->keepalive_deadline = 0;
                build_deferred_ack();
                cb->state = TcpState::CLOSE_WAIT;
                deferred_wake = true;
            }
            break;
        }

        case TcpState::CLOSE_WAIT: {
            if ((flags & TCP_RST) != 0) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSED;
                cb->keepalive_deadline = 0;
                deferred_wake = true;
                break;
            }

            process_stream_ack();

            if ((flags & TCP_FIN) != 0) {
                build_deferred_ack();
            }
            break;
        }

        case TcpState::FIN_WAIT_1: {
            if ((flags & TCP_RST) != 0) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSED;
                deferred_wake = true;
                break;
            }

            if ((flags & TCP_ACK) != 0) {
                // Accept partial or full ACK to advance snd_una.
                if (tcp_seq_after(seg_ack, cb->snd_una) && !tcp_seq_after(seg_ack, cb->snd_nxt)) {
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = static_cast<uint32_t>(seg_wnd) << cb->snd_wscale;

                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t const ENTRY_END = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(ENTRY_END, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (cb->retransmit_head == nullptr) {
                                cb->retransmit_tail = nullptr;
                            }
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            delete entry;
                        } else {
                            break;
                        }
                    }

                    if (cb->retransmit_head != nullptr) {
                        cb->retransmit_deadline = tcp_now_ms() + cb->rto_ms;
                    }

                    // Full ACK includes our FIN.
                    if (seg_ack == cb->snd_nxt) {
                        if ((flags & TCP_FIN) != 0) {
                            cb->rcv_nxt = SEG_SEQ + static_cast<uint32_t>(payload_len) + 1;
                            build_deferred_ack();
                            drain_retransmit_queue(cb);
                            cb->state = TcpState::TIME_WAIT;
                            cb->time_wait_deadline = tcp_now_ms() + 10000;
                            tcp_timer_arm(cb);
                            deferred_wake = true;
                        } else {
                            cb->state = TcpState::FIN_WAIT_2;
                            deferred_wake = true;
                        }
                    }
                }
            }
            if ((flags & TCP_FIN) != 0 && cb->state == TcpState::FIN_WAIT_1) {
                cb->rcv_nxt = SEG_SEQ + static_cast<uint32_t>(payload_len) + 1;
                build_deferred_ack();
                cb->state = TcpState::CLOSING;
                deferred_wake = true;
            }
            if (payload_len > 0 && cb->socket != nullptr) {
                if (SEG_SEQ == cb->rcv_nxt) {
                    if (queue_in_order_payload(cb, cb->socket, payload, payload_len)) {
                        deferred_wake = true;
                    }
                    build_deferred_ack();
                }
            }
            break;
        }

        case TcpState::FIN_WAIT_2: {
            if ((flags & TCP_RST) != 0) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSED;
                deferred_wake = true;
                break;
            }

            // Allow SYN to recycle FIN_WAIT_2 like TIME_WAIT.
            if ((flags & TCP_SYN) != 0 && (flags & TCP_ACK) == 0) {
                cb->state = TcpState::CLOSED;
                cb->lock.unlock_irqrestore(cb_lock_flags);

                tcp_free_cb(cb);
                tcp_cb_release(cb);

                uint16_t const L_PORT = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, L_PORT);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
                    tcp_cb_release(listener);
                }
                return;
            }

            if (payload_len > 0 && cb->socket != nullptr) {
                if (SEG_SEQ == cb->rcv_nxt) {
                    if (queue_in_order_payload(cb, cb->socket, payload, payload_len)) {
                        deferred_wake = true;
                    }
                    build_deferred_ack();
                }
            }
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = SEG_SEQ + static_cast<uint32_t>(payload_len) + 1;
                build_deferred_ack();
                drain_retransmit_queue(cb);
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 10000;
                tcp_timer_arm(cb);
                deferred_wake = true;
            }
            break;
        }

        case TcpState::CLOSING: {
            if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 10000;
                tcp_timer_arm(cb);
                deferred_wake = true;
            }
            break;
        }

        case TcpState::LAST_ACK: {
            if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt) {
                cb->state = TcpState::CLOSED;
                if (cb->socket != nullptr) {
                    cb->socket->proto_data = nullptr;
                }
                cb->lock.unlock_irqrestore(cb_lock_flags);
                tcp_free_cb(cb);
                tcp_cb_release(cb);
                return;
            }
            break;
        }

        case TcpState::TIME_WAIT: {
            // Allow SYN to recycle TIME_WAIT.
            if ((flags & TCP_SYN) != 0 && (flags & TCP_ACK) == 0) {
                cb->state = TcpState::CLOSED;
                cb->lock.unlock_irqrestore(cb_lock_flags);

                tcp_free_cb(cb);
                tcp_cb_release(cb);

                uint16_t const L_PORT = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, L_PORT);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
                    tcp_cb_release(listener);
                }
                return;
            }

            if ((flags & TCP_FIN) != 0) {
                build_deferred_ack();
            }
            break;
        }

        default:
            break;
    }

    cb->lock.unlock_irqrestore(cb_lock_flags);

    // Send deferred ACK outside cb->lock.
    if (deferred_ack != nullptr) {
        if (ipv4_tx(deferred_ack, defer_local, defer_remote, IPPROTO_TCP, TCP_IPV4_TTL) < 0) {
            cb_lock_flags = cb->lock.lock_irqsave();
            cb->ack_pending = true;
            tcp_timer_arm(cb);
            cb->lock.unlock_irqrestore(cb_lock_flags);
        }
    } else if (defer_ack_pending) {
        cb_lock_flags = cb->lock.lock_irqsave();
        cb->ack_pending = true;
        tcp_timer_arm(cb);
        cb->lock.unlock_irqrestore(cb_lock_flags);
    }

    // Wake outside cb->lock to avoid scheduler lock-order issues.
    if (deferred_wake && cb->socket != nullptr) {
        wake_socket(cb->socket);
    }

    tcp_cb_release(cb);
}

void tcp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip) {
    (void)dev;

    if (pkt->len < sizeof(TcpHeader)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const TcpHeader*>(pkt->data);
    size_t const HDR_LEN = tcp_header_len(hdr);
    if (HDR_LEN < sizeof(TcpHeader) || HDR_LEN > pkt->len) {
        pkt_free(pkt);
        return;
    }

    uint16_t const STORED_CSUM = hdr->checksum;
    if (STORED_CSUM != 0) {
        uint16_t const COMPUTED = pseudo_header_checksum(src_ip, dst_ip, IPPROTO_TCP, pkt->data, pkt->len);
        if (COMPUTED != 0 && COMPUTED != 0xFFFF) {
            pkt_free(pkt);
            return;
        }
    }

    uint16_t const DST_PORT = ntohs(hdr->dst_port);
    uint16_t const SRC_PORT = ntohs(hdr->src_port);

    const uint8_t* payload = pkt->data + HDR_LEN;
    size_t const PAYLOAD_LEN = pkt->len - HDR_LEN;

    TcpCB* cb = tcp_find_cb(dst_ip, DST_PORT, src_ip, SRC_PORT);
    if (cb != nullptr) {
        tcp_process_segment(cb, hdr, payload, PAYLOAD_LEN, src_ip, dst_ip);
        pkt_free(pkt);
        return;
    }

    if ((hdr->flags & TCP_SYN) != 0 && (hdr->flags & TCP_ACK) == 0) {
        TcpCB* listener = tcp_find_listener(dst_ip, DST_PORT);
        if (listener != nullptr) {
            handle_listen_syn(listener, hdr, src_ip, dst_ip);
            tcp_cb_release(listener);
            pkt_free(pkt);
            return;
        }
    }

    // No match: send RST.
    if ((hdr->flags & TCP_RST) == 0) {
        if ((hdr->flags & TCP_ACK) != 0) {
            // RST replies intentionally reverse the incoming tuple.
            // NOLINTNEXTLINE(readability-suspicious-call-argument)
            tcp_send_rst(dst_ip, src_ip, DST_PORT, SRC_PORT, ntohl(hdr->ack), 0, 0);
        } else {
            uint32_t ack_seq = ntohl(hdr->seq) + static_cast<uint32_t>(PAYLOAD_LEN);
            if ((hdr->flags & TCP_SYN) != 0) {
                ack_seq++;
            }
            if ((hdr->flags & TCP_FIN) != 0) {
                ack_seq++;
            }
            // RST replies intentionally reverse the incoming tuple.
            // NOLINTNEXTLINE(readability-suspicious-call-argument)
            tcp_send_rst(dst_ip, src_ip, DST_PORT, SRC_PORT, 0, ack_seq, TCP_ACK);
        }
    }

    pkt_free(pkt);
}

}  // namespace ker::net::proto
