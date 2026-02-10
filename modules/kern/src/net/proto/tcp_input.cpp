#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

namespace {
// Wake any task blocked on this socket (recv, accept, connect).
// Clears the deferredTaskSwitch flag so the task returns to userspace
// immediately (via sysret) rather than blocking in waitQueue, since
// data is now available. rescheduleTaskForCpu handles queue cleanup.
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

// Drain the retransmit queue, freeing all held packet buffers.
// Call when a connection reaches a terminal state (TIME_WAIT, CLOSE_WAIT)
// where no further retransmissions will occur.
void drain_retransmit_queue(TcpCB* cb) {
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        if (entry->pkt != nullptr) {
            pkt_free(entry->pkt);
        }
        ker::mod::mm::dyn::kmalloc::free(entry);
    }
}

// Handle SYN on a listening socket: create child TCB + socket, send SYN-ACK
void handle_listen_syn(TcpCB* listener, const TcpHeader* hdr, uint32_t src_ip, uint32_t dst_ip) {
    Socket* listen_sock = listener->socket;
    if (listen_sock == nullptr) {
        return;
    }

    // Check accept queue capacity
    if (listen_sock->aq_count >= static_cast<size_t>(listen_sock->backlog)) {
        return;  // Queue full, drop SYN
    }

    // Create child socket (socket_create already allocates a TcpCB
    // for SOCK_STREAM sockets — reuse it instead of allocating a second)
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

    // Inherit blocking mode from the listening socket
    child->nonblock = listen_sock->nonblock;

    // Set up sequence numbers
    child_cb->irs = ntohl(hdr->seq);
    child_cb->rcv_nxt = child_cb->irs + 1;
    child_cb->rcv_wnd = SOCKET_BUF_SIZE;

    child_cb->iss = ntohl(hdr->seq) ^ 0xDEADBEEF;  // Simple ISS
    child_cb->iss += tcp_now_ms();
    child_cb->snd_una = child_cb->iss;
    child_cb->snd_nxt = child_cb->iss + 1;
    child_cb->snd_wnd = ntohs(hdr->window);

    // Parse MSS option from SYN
    uint8_t hdr_len = (hdr->data_offset >> 4) * 4;
    if (hdr_len > sizeof(TcpHeader)) {
        const auto* opts = reinterpret_cast<const uint8_t*>(hdr) + sizeof(TcpHeader);
        size_t opts_len = hdr_len - sizeof(TcpHeader);
        for (size_t i = 0; i < opts_len;) {
            if (opts[i] == 0) break;  // End of options
            if (opts[i] == 1) {
                i++;
                continue;
            }  // NOP
            if (i + 1 >= opts_len) break;
            uint8_t opt_len = opts[i + 1];
            if (opt_len < 2 || i + opt_len > opts_len) break;
            if (opts[i] == 2 && opt_len == 4) {  // MSS option
                child_cb->snd_mss = ntohs(*reinterpret_cast<const uint16_t*>(opts + i + 2));
            }
            i += opt_len;
        }
    }

    child_cb->state = TcpState::SYN_RECEIVED;
    child->state = SocketState::CONNECTING;

    // Send SYN-ACK
    tcp_send_segment(child_cb, TCP_SYN | TCP_ACK, nullptr, 0);
}
}  // namespace

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload, size_t payload_len, uint32_t src_ip, uint32_t dst_ip) {
    uint8_t flags = hdr->flags;
    uint32_t seg_seq = ntohl(hdr->seq);
    uint32_t seg_ack = ntohl(hdr->ack);
    uint16_t seg_wnd = ntohs(hdr->window);

    cb->lock.lock();

    switch (cb->state) {
        case TcpState::SYN_SENT: {
            // Expecting SYN-ACK
            if ((flags & TCP_ACK) != 0 && (flags & TCP_SYN) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->irs = seg_seq;
                    cb->rcv_nxt = seg_seq + 1;
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;
                    cb->lock.unlock();

                    // Parse MSS from SYN-ACK
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
                            }
                            i += opt_len;
                        }
                    }

                    cb->state = TcpState::ESTABLISHED;
                    tcp_send_ack(cb);

                    // Wake connect() caller
                    wake_socket(cb->socket);
                }
            } else if ((flags & TCP_RST) != 0) {
                cb->state = TcpState::CLOSED;
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::SYN_RECEIVED: {
            if ((flags & TCP_RST) != 0) {
                // Drain retransmit queue (SYN-ACK clone)
                while (cb->retransmit_head != nullptr) {
                    auto* entry = cb->retransmit_head;
                    cb->retransmit_head = entry->next;
                    if (entry->pkt != nullptr) {
                        pkt_free(entry->pkt);
                    }
                    ker::mod::mm::dyn::kmalloc::free(entry);
                }
                cb->state = TcpState::CLOSED;
                break;
            }
            if ((flags & TCP_ACK) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->state = TcpState::ESTABLISHED;
                    cb->snd_una = seg_ack;

                    // Clear the SYN-ACK from the retransmit queue now that
                    // the handshake is complete.  Without this the timer
                    // keeps resending the SYN-ACK after ESTABLISHED.
                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            ker::mod::mm::dyn::kmalloc::free(entry);
                        } else {
                            break;
                        }
                    }
                    cb->snd_wnd = seg_wnd;

                    // Enqueue into parent's accept queue.
                    // Save fields and release cb->lock BEFORE calling
                    // tcp_find_listener, which acquires tcb_list_lock.
                    // Lock ordering: cb->lock must never be held when
                    // tcb_list_lock is acquired (timer holds list lock
                    // first).
                    Socket* child_sock = cb->socket;
                    uint32_t saved_local_ip = cb->local_ip;
                    uint16_t saved_local_port = cb->local_port;
                    cb->lock.unlock();

                    if (child_sock != nullptr) {
                        child_sock->state = SocketState::CONNECTED;
                        TcpCB* listener = tcp_find_listener(saved_local_ip, saved_local_port);
                        if (listener != nullptr && listener->socket != nullptr) {
                            Socket* lsock = listener->socket;
                            lsock->lock.lock();
                            if (lsock->aq_count < SOCKET_ACCEPT_QUEUE) {
                                lsock->accept_queue[lsock->aq_tail] = child_sock;
                                lsock->aq_tail = (lsock->aq_tail + 1) % SOCKET_ACCEPT_QUEUE;
                                lsock->aq_count++;
                            }
                            lsock->lock.unlock();
                            wake_socket(lsock);
                        }
                    }

                    // Handle data piggy-backed on the ACK
                    if (payload_len > 0 && child_sock != nullptr) {
                        cb->lock.lock();
                        if (seg_seq == cb->rcv_nxt) {
                            ssize_t written = child_sock->rcvbuf.write(payload, payload_len);
                            if (written > 0) {
                                cb->rcv_nxt += static_cast<uint32_t>(written);
                                cb->rcv_wnd = child_sock->rcvbuf.free_space();
                            }
                            tcp_send_ack(cb);
                        }
                        cb->lock.unlock();
                    }

                    tcp_cb_release(cb);
                    return;
                }
            }
            break;
        }

        case TcpState::ESTABLISHED: {
            if ((flags & TCP_RST) != 0) {
                // Drain retransmit queue to avoid leaking packet buffers
                while (cb->retransmit_head != nullptr) {
                    auto* entry = cb->retransmit_head;
                    cb->retransmit_head = entry->next;
                    if (entry->pkt != nullptr) {
                        pkt_free(entry->pkt);
                    }
                    ker::mod::mm::dyn::kmalloc::free(entry);
                }
                cb->state = TcpState::CLOSED;
                wake_socket(cb->socket);
                break;
            }

            // Process ACK
            if ((flags & TCP_ACK) != 0) {
                if (tcp_seq_after(seg_ack, cb->snd_una) && !tcp_seq_after(seg_ack, cb->snd_nxt)) {
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;

                    // Remove ACKed segments from retransmit queue and
                    // use the first non-retransmitted sample for RTT.
                    bool rtt_sampled = false;
                    uint64_t now = tcp_now_ms();
                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            // Karn's algorithm: only sample RTT from
                            // segments that were NOT retransmitted.
                            if (!rtt_sampled && entry->retries == 0) {
                                uint64_t rtt = now - entry->send_time_ms;
                                if (rtt == 0) rtt = 1;
                                if (cb->srtt_ms == 0) {
                                    // First measurement
                                    cb->srtt_ms = rtt;
                                    cb->rttvar_ms = rtt / 2;
                                } else {
                                    // Jacobson/Karels
                                    int64_t delta = static_cast<int64_t>(rtt) - static_cast<int64_t>(cb->srtt_ms);
                                    cb->srtt_ms = cb->srtt_ms + delta / 8;
                                    int64_t abs_delta = delta < 0 ? -delta : delta;
                                    cb->rttvar_ms = cb->rttvar_ms + (abs_delta - static_cast<int64_t>(cb->rttvar_ms)) / 4;
                                }
                                cb->rto_ms = cb->srtt_ms + 4 * cb->rttvar_ms;
                                if (cb->rto_ms < 200) cb->rto_ms = 200;      // floor
                                if (cb->rto_ms > 60000) cb->rto_ms = 60000;  // ceiling
                                rtt_sampled = true;
                            }

                            cb->retransmit_head = entry->next;
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            ker::mod::mm::dyn::kmalloc::free(entry);
                        } else {
                            break;
                        }
                    }

                    // Reset retransmit deadline based on updated RTO
                    if (cb->retransmit_head != nullptr) {
                        cb->retransmit_deadline = now + cb->rto_ms;
                    }

                    // Wake tasks waiting to send (window opened)
                    wake_socket(cb->socket);
                }
            }

            // Process incoming data
            if (payload_len > 0 && seg_seq == cb->rcv_nxt) {
                // In-order data — deliver to receive buffer
                if (cb->socket != nullptr) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        // Update receive window to reflect actual free space
                        cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                        wake_socket(cb->socket);
                    }
                }
                tcp_send_ack(cb);
            } else if (payload_len > 0) {
                // Out-of-order: send duplicate ACK
                tcp_send_ack(cb);
            }

            // Process FIN
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                tcp_send_ack(cb);
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSE_WAIT;
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::FIN_WAIT_1: {
            // Handle RST
            if ((flags & TCP_RST) != 0) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSED;
                wake_socket(cb->socket);
                break;
            }

            if ((flags & TCP_ACK) != 0) {
                // Accept any valid ACK (partial or full) to advance snd_una
                // and free retransmit entries.  A partial ACK means the peer
                // ACKed our data but not yet our FIN.
                if (tcp_seq_after(seg_ack, cb->snd_una) && !tcp_seq_after(seg_ack, cb->snd_nxt)) {
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;

                    // Remove ACKed segments from retransmit queue
                    while (cb->retransmit_head != nullptr) {
                        auto* entry = cb->retransmit_head;
                        uint32_t entry_end = entry->seq + static_cast<uint32_t>(entry->len);
                        if (!tcp_seq_after(entry_end, seg_ack)) {
                            cb->retransmit_head = entry->next;
                            if (entry->pkt != nullptr) {
                                pkt_free(entry->pkt);
                            }
                            ker::mod::mm::dyn::kmalloc::free(entry);
                        } else {
                            break;
                        }
                    }

                    // Reset retransmit deadline if entries remain
                    if (cb->retransmit_head != nullptr) {
                        cb->retransmit_deadline = tcp_now_ms() + cb->rto_ms;
                    }

                    // Full ACK (includes our FIN) → transition
                    if (seg_ack == cb->snd_nxt) {
                        if ((flags & TCP_FIN) != 0) {
                            // Simultaneous close: FIN+ACK
                            cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                            tcp_send_ack(cb);
                            drain_retransmit_queue(cb);
                            cb->state = TcpState::TIME_WAIT;
                            cb->time_wait_deadline = tcp_now_ms() + 10000;
                            wake_socket(cb->socket);
                        } else {
                            cb->state = TcpState::FIN_WAIT_2;
                            wake_socket(cb->socket);
                        }
                    }
                }
            }
            if ((flags & TCP_FIN) != 0 && cb->state == TcpState::FIN_WAIT_1) {
                // FIN without ACK of our FIN -> CLOSING
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                tcp_send_ack(cb);
                cb->state = TcpState::CLOSING;
                wake_socket(cb->socket);
            }
            // Also deliver any data
            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                    }
                    tcp_send_ack(cb);
                }
            }
            break;
        }

        case TcpState::FIN_WAIT_2: {
            // Handle RST
            if ((flags & TCP_RST) != 0) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::CLOSED;
                wake_socket(cb->socket);
                break;
            }

            // Allow a new SYN to recycle a FIN_WAIT_2 connection (same
            // as TIME_WAIT recycling).  This prevents ephemeral-port
            // collisions from blocking new connections.
            if ((flags & TCP_SYN) != 0 && (flags & TCP_ACK) == 0) {
                cb->state = TcpState::CLOSED;
                cb->lock.unlock();

                tcp_free_cb(cb);
                tcp_cb_release(cb);

                uint16_t l_port = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, l_port);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
                }
                return;
            }

            // Deliver data
            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                        cb->rcv_wnd = cb->socket->rcvbuf.free_space();
                    }
                    tcp_send_ack(cb);
                }
            }
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                tcp_send_ack(cb);
                drain_retransmit_queue(cb);
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 10000;  // 10s
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::CLOSING: {
            if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt) {
                drain_retransmit_queue(cb);
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 10000;  // 10s
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::LAST_ACK: {
            if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt) {
                cb->state = TcpState::CLOSED;
                // Socket will be cleaned up
                if (cb->socket != nullptr) {
                    cb->socket->proto_data = nullptr;
                }
                cb->lock.unlock();
                tcp_free_cb(cb);
                tcp_cb_release(cb);
                return;
            }
            break;
        }

        case TcpState::TIME_WAIT: {
            // RFC 1122 §4.2.2.13 / RFC 6191: allow a new SYN to recycle
            // a TIME_WAIT connection.  This prevents port exhaustion
            // when a client reconnects quickly.
            if ((flags & TCP_SYN) != 0 && (flags & TCP_ACK) == 0) {
                // Kill the old TIME_WAIT TCB so the SYN falls through
                // to the listener on the next lookup.
                cb->state = TcpState::CLOSED;
                cb->lock.unlock();

                tcp_free_cb(cb);
                tcp_cb_release(cb);

                // Re-process this packet: look for a listener
                uint16_t l_port = ntohs(hdr->dst_port);
                TcpCB* listener = tcp_find_listener(dst_ip, l_port);
                if (listener != nullptr) {
                    handle_listen_syn(listener, hdr, src_ip, dst_ip);
                }
                return;
            }

            // Re-ACK any segment in TIME_WAIT (e.g. retransmitted FIN)
            if ((flags & TCP_FIN) != 0) {
                tcp_send_ack(cb);
            }
            break;
        }

        default:
            break;
    }

    cb->lock.unlock();
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

    // Verify TCP checksum
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

    // Look up existing connection
    TcpCB* cb = tcp_find_cb(dst_ip, dst_port, src_ip, src_port);
    if (cb != nullptr) {
        tcp_process_segment(cb, hdr, payload, payload_len, src_ip, dst_ip);
        pkt_free(pkt);
        return;
    }

    // Check for listener (SYN on listening socket)
    if ((hdr->flags & TCP_SYN) != 0 && (hdr->flags & TCP_ACK) == 0) {
        TcpCB* listener = tcp_find_listener(dst_ip, dst_port);
        if (listener != nullptr) {
            handle_listen_syn(listener, hdr, src_ip, dst_ip);
            pkt_free(pkt);
            return;
        }
    }

    // No matching connection or listener — send RST
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
