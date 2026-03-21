#include <algorithm>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

namespace {
// Wake a task blocked on this socket.
void wake_socket(Socket* sock) {
    if (sock == nullptr) {
        return;
    }
    uint64_t pid = sock->owner_pid;
    if (pid != 0) {
        auto* task = ker::mod::sched::find_task_by_pid(pid);
        if (task != nullptr) {
            task->deferredTaskSwitch = false;
            ker::mod::sched::reschedule_task_for_cpu(task->cpu, task);
        }
    }
}

// Free all retransmit entries.
void drain_retransmit_queue(TcpCB* cb) {
    size_t n = 0;
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
        n++;
    }
    cb->retransmit_tail = nullptr;
    if (n > 0) {
        ker::mod::dbg::log("[net] drain_rtx: freed %zu entries port=%u pool_free=%zu", n, cb->local_port, ker::net::pkt_pool_free_count());
    }
}

// Handle SYN on a listening socket.
void handle_listen_syn(TcpCB* listener, const TcpHeader* hdr, uint32_t src_ip, uint32_t dst_ip) {
    Socket* listen_sock = listener->socket;
    if (listen_sock == nullptr) {
        return;
    }

    if (listen_sock->aq_count >= static_cast<size_t>(listen_sock->backlog)) {
        ker::mod::dbg::log("[net] ACCEPT QUEUE FULL port=%u aq=%zu backlog=%d", listener->local_port, listen_sock->aq_count,
                           listen_sock->backlog);
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

    // Parse MSS and WSCALE options.
    uint8_t hdr_len = (hdr->data_offset >> 4) * 4;
    if (hdr_len > sizeof(TcpHeader)) {
        const auto* opts = reinterpret_cast<const uint8_t*>(hdr) + sizeof(TcpHeader);
        size_t opts_len = hdr_len - sizeof(TcpHeader);
        for (size_t i = 0; i < opts_len;) {
            if (opts[i] == 0) break;
            if (opts[i] == 1) {
                i++;
                continue;
            }
            if (i + 1 >= opts_len) break;
            uint8_t opt_len = opts[i + 1];
            if (opt_len < 2 || i + opt_len > opts_len) break;
            if (opts[i] == 2 && opt_len == 4) {
                child_cb->snd_mss = ntohs(*reinterpret_cast<const uint16_t*>(opts + i + 2));
            } else if (opts[i] == 3 && opt_len == 3) {
                child_cb->snd_wscale = opts[i + 2];
                child_cb->ws_enabled = true;
            }
            i += opt_len;
        }
    }

    child_cb->state = TcpState::SYN_RECEIVED;
    child->state = SocketState::CONNECTING;

    tcp_insert_cb(child_cb);

    tcp_send_segment(child_cb, TCP_SYN | TCP_ACK, nullptr, 0);
}
}  // namespace

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload, size_t payload_len, uint32_t src_ip, uint32_t dst_ip) {
    NET_TRACE_SPAN(SPAN_TCP_PROCESS);
    uint8_t flags = hdr->flags;
    uint32_t seg_seq = ntohl(hdr->seq);
    uint32_t seg_ack = ntohl(hdr->ack);
    uint16_t seg_wnd = ntohs(hdr->window);

    // Build ACK/wake decisions under lock, execute after unlock.
    PacketBuffer* deferred_ack = nullptr;
    uint32_t defer_local = 0, defer_remote = 0;
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

    cb_lock_flags = cb->lock.lock_irqsave();

    switch (cb->state) {
        case TcpState::SYN_SENT: {
            if ((flags & TCP_ACK) != 0 && (flags & TCP_SYN) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->irs = seg_seq;
                    cb->rcv_nxt = seg_seq + 1;
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;
                    cb->lock.unlock_irqrestore(cb_lock_flags);

                    // Parse MSS and WSCALE options.
                    uint8_t hdr_len = (hdr->data_offset >> 4) * 4;
                    if (hdr_len > sizeof(TcpHeader)) {
                        const auto* opts = reinterpret_cast<const uint8_t*>(hdr) + sizeof(TcpHeader);
                        size_t opts_len = hdr_len - sizeof(TcpHeader);
                        for (size_t i = 0; i < opts_len;) {
                            if (opts[i] == 0) break;
                            if (opts[i] == 1) {
                                i++;
                                continue;
                            }
                            if (i + 1 >= opts_len) break;
                            uint8_t opt_len = opts[i + 1];
                            if (opt_len < 2 || i + opt_len > opts_len) break;
                            if (opts[i] == 2 && opt_len == 4) {
                                cb->snd_mss = ntohs(*reinterpret_cast<const uint16_t*>(opts + i + 2));
                            } else if (opts[i] == 3 && opt_len == 3) {
                                cb->snd_wscale = opts[i + 2];
                                cb->ws_enabled = true;
                                if (cb->socket != nullptr) {
                                    cb->rcv_wscale = tcp_wscale_for_buf(cb->socket->rcvbuf.capacity);
                                }
                            }
                            i += opt_len;
                        }
                    }

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
                    ker::mod::mm::dyn::kmalloc::free(entry);
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
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (cb->retransmit_head == nullptr) {
                                cb->retransmit_tail = nullptr;
                            }
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            ker::mod::mm::dyn::kmalloc::free(entry);
                        } else {
                            break;
                        }
                    }
                    cb->snd_wnd = static_cast<uint32_t>(seg_wnd) << cb->snd_wscale;

                    // Release cb->lock before tcp_find_listener (takes tcb_list_lock).
                    Socket* child_sock = cb->socket;
                    uint32_t saved_local_ip = cb->local_ip;
                    uint16_t saved_local_port = cb->local_port;
                    cb->lock.unlock_irqrestore(cb_lock_flags);

                    if (child_sock != nullptr) {
                        child_sock->state = SocketState::CONNECTED;
                        TcpCB* listener = tcp_find_listener(saved_local_ip, saved_local_port);
                        if (listener != nullptr && listener->socket != nullptr) {
                            Socket* lsock = listener->socket;
                            uint64_t lsock_flags = lsock->lock.lock_irqsave();
                            if (lsock->aq_count < SOCKET_ACCEPT_QUEUE) {
                                lsock->accept_queue[lsock->aq_tail] = child_sock;
                                lsock->aq_tail = (lsock->aq_tail + 1) % SOCKET_ACCEPT_QUEUE;
                                lsock->aq_count++;
                            }
                            lsock->lock.unlock_irqrestore(lsock_flags);
                            wake_socket(lsock);
                        }
                    }

                    if (payload_len > 0 && child_sock != nullptr) {
                        cb_lock_flags = cb->lock.lock_irqsave();
                        if (seg_seq == cb->rcv_nxt) {
                            ssize_t written = child_sock->rcvbuf.write(payload, payload_len);
                            if (written > 0) {
                                cb->rcv_nxt += static_cast<uint32_t>(written);
                                cb->rcv_wnd = child_sock->rcvbuf.free_space();
                            }
                            build_deferred_ack();
                        }
                        cb->lock.unlock_irqrestore(cb_lock_flags);
                        if (deferred_ack != nullptr) {
                            if (ipv4_tx(deferred_ack, defer_local, defer_remote, 6, 64) < 0) {
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
                    ker::mod::mm::dyn::kmalloc::free(entry);
                }
                cb->retransmit_tail = nullptr;
                cb->state = TcpState::CLOSED;
                deferred_wake = true;
                break;
            }

            if ((flags & TCP_ACK) != 0) {
                // Accept ACKs in [snd_una, snd_nxt], including window-only updates.
                bool valid_ack = !tcp_seq_after(seg_ack, cb->snd_nxt);
                bool ack_advances = tcp_seq_after(seg_ack, cb->snd_una);
                if (valid_ack && (ack_advances || seg_wnd != cb->snd_wnd)) {
                    cb->snd_wnd = static_cast<uint32_t>(seg_wnd) << cb->snd_wscale;
                    if (!ack_advances) {
                        deferred_wake = true;
                    }
                }
                if (ack_advances && valid_ack) {
                    cb->snd_una = seg_ack;

                    // Remove ACKed retransmits and sample RTT once.
                    bool rtt_sampled = false;
                    uint64_t now = tcp_now_ms();
                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            // Karn's algorithm.
                            if (!rtt_sampled && entry->retries == 0) {
                                uint64_t rtt = now - entry->send_time_ms;
                                if (rtt == 0) {
                                    rtt = 1;
                                }
                                if (cb->srtt_ms == 0) {
                                    cb->srtt_ms = rtt;
                                    cb->rttvar_ms = rtt / 2;
                                } else {
                                    int64_t delta = static_cast<int64_t>(rtt) - static_cast<int64_t>(cb->srtt_ms);
                                    cb->srtt_ms = cb->srtt_ms + (delta / 8);
                                    int64_t abs_delta = delta < 0 ? -delta : delta;
                                    cb->rttvar_ms = cb->rttvar_ms + ((abs_delta - static_cast<int64_t>(cb->rttvar_ms)) / 4);
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
                            ker::mod::mm::dyn::kmalloc::free(entry);
                        } else {
                            break;
                        }
                    }

                    if (cb->retransmit_head != nullptr) {
                        cb->retransmit_deadline = now + cb->rto_ms;
                    }

                    // Peer ACK confirms our prior ACK.
                    cb->ack_pending = false;

                    deferred_wake = true;
                }
            }

            if (payload_len > 0 && seg_seq == cb->rcv_nxt) {
                if (cb->socket != nullptr) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        deferred_wake = true;
                    } else {
                        ker::mod::dbg::log("[net] RCVBUF FULL port=%u avail=%zu cap=%zu pktlen=%zu", cb->local_port,
                                           cb->socket->rcvbuf.available(), cb->socket->rcvbuf.capacity, payload_len);
                    }
                    // Keep advertised window in sync with receive buffer.
                    cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                }
                // Delayed ACK: every 2 segments or 40 ms.
                cb->segs_pending_ack++;
                if (cb->segs_pending_ack >= 2) {
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
                // Drop out-of-order data; no dup-ACK path here.
                if (cb->socket != nullptr) {
                    cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                }
            }

            // FIN: ACK immediately and cancel delayed ACK.
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                cb->segs_pending_ack = 0;
                cb->delayed_ack_deadline = 0;
                build_deferred_ack();
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSE_WAIT;
                deferred_wake = true;
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
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (cb->retransmit_head == nullptr) {
                                cb->retransmit_tail = nullptr;
                            }
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            ker::mod::mm::dyn::kmalloc::free(entry);
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
                            cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
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
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                build_deferred_ack();
                cb->state = TcpState::CLOSING;
                deferred_wake = true;
            }
            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        cb->rcv_wnd = cb->socket->rcvbuf.free_space();
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

                uint16_t l_port = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, l_port);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
                }
                return;
            }

            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                    }
                    build_deferred_ack();
                }
            }
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
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

                uint16_t l_port = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, l_port);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
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
        if (ipv4_tx(deferred_ack, defer_local, defer_remote, 6, 64) < 0) {
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

void tcp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip) {
    (void)dev;

    if (pkt->len < sizeof(TcpHeader)) {
        pkt_free(pkt);
        return;
    }

    auto* hdr = reinterpret_cast<const TcpHeader*>(pkt->data);
    uint8_t hdr_len = (hdr->data_offset >> 4) * 4;
    if (hdr_len < sizeof(TcpHeader) || hdr_len > pkt->len) {
        pkt_free(pkt);
        return;
    }

    uint16_t stored_csum = hdr->checksum;
    if (stored_csum != 0) {
        uint16_t computed = pseudo_header_checksum(src_ip, dst_ip, 6, pkt->data, pkt->len);
        if (computed != 0 && computed != 0xFFFF) {
            pkt_free(pkt);
            return;
        }
    }

    uint16_t dst_port = ntohs(hdr->dst_port);
    uint16_t src_port = ntohs(hdr->src_port);

    const uint8_t* payload = pkt->data + hdr_len;
    size_t payload_len = pkt->len - hdr_len;

    TcpCB* cb = tcp_find_cb(dst_ip, dst_port, src_ip, src_port);
    if (cb != nullptr) {
        tcp_process_segment(cb, hdr, payload, payload_len, src_ip, dst_ip);
        pkt_free(pkt);
        return;
    }

    if ((hdr->flags & TCP_SYN) != 0 && (hdr->flags & TCP_ACK) == 0) {
        TcpCB* listener = tcp_find_listener(dst_ip, dst_port);
        if (listener != nullptr) {
            handle_listen_syn(listener, hdr, src_ip, dst_ip);
            pkt_free(pkt);
            return;
        }
    }

    // No match: send RST.
    if ((hdr->flags & TCP_RST) == 0) {
        if ((hdr->flags & TCP_ACK) != 0) {
            tcp_send_rst(dst_ip, src_ip, dst_port, src_port, ntohl(hdr->ack), 0, 0);
        } else {
            uint32_t ack_seq = ntohl(hdr->seq) + static_cast<uint32_t>(payload_len);
            if ((hdr->flags & TCP_SYN) != 0) ack_seq++;
            if ((hdr->flags & TCP_FIN) != 0) ack_seq++;
            tcp_send_rst(dst_ip, src_ip, dst_port, src_port, 0, ack_seq, TCP_ACK);
        }
    }

    pkt_free(pkt);
}

}  // namespace ker::net::proto
