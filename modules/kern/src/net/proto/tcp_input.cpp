#include "tcp.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

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
        auto* task = ker::mod::sched::findTaskByPid(pid);
        if (task != nullptr) {
            task->deferredTaskSwitch = false;
            ker::mod::sched::rescheduleTaskForCpu(task->cpu, task);
        }
    }
}

// Handle SYN on a listening socket: create child TCB + socket, send SYN-ACK
void handle_listen_syn(TcpCB* listener, const TcpHeader* hdr,
                       uint32_t src_ip, uint32_t dst_ip) {
    Socket* listen_sock = listener->socket;
    if (listen_sock == nullptr) {
        return;
    }

    // Check accept queue capacity
    if (listen_sock->aq_count >= static_cast<size_t>(listen_sock->backlog)) {
        return;  // Queue full, drop SYN
    }

    // Create child socket
    Socket* child = socket_create(listen_sock->domain, listen_sock->type, listen_sock->protocol);
    if (child == nullptr) {
        return;
    }

    // Create child TCB
    TcpCB* child_cb = tcp_alloc_cb();
    if (child_cb == nullptr) {
        socket_destroy(child);
        return;
    }

    child->proto_data = child_cb;
    child->proto_ops = get_tcp_proto_ops();
    child_cb->socket = child;

    child_cb->local_ip = dst_ip;
    child_cb->local_port = listener->local_port;
    child_cb->remote_ip = src_ip;
    child_cb->remote_port = ntohs(hdr->src_port);

    child->local_v4.addr = dst_ip;
    child->local_v4.port = listener->local_port;
    child->remote_v4.addr = src_ip;
    child->remote_v4.port = ntohs(hdr->src_port);

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
            if (opts[i] == 0) break;        // End of options
            if (opts[i] == 1) { i++; continue; }  // NOP
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

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload,
                         size_t payload_len, uint32_t src_ip, uint32_t dst_ip) {
    uint8_t flags = hdr->flags;
    uint32_t seg_seq = ntohl(hdr->seq);
    uint32_t seg_ack = ntohl(hdr->ack);
    uint16_t seg_wnd = ntohs(hdr->window);

    (void)src_ip;
    (void)dst_ip;

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

                    // Parse MSS from SYN-ACK
                    uint8_t hdr_len = (hdr->data_offset >> 4) * 4;
                    if (hdr_len > sizeof(TcpHeader)) {
                        const auto* opts = reinterpret_cast<const uint8_t*>(hdr) + sizeof(TcpHeader);
                        size_t opts_len = hdr_len - sizeof(TcpHeader);
                        for (size_t i = 0; i < opts_len;) {
                            if (opts[i] == 0) break;
                            if (opts[i] == 1) { i++; continue; }
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
                cb->state = TcpState::CLOSED;
                break;
            }
            if ((flags & TCP_ACK) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->state = TcpState::ESTABLISHED;
                    cb->snd_una = seg_ack;
                    cb->snd_wnd = seg_wnd;

                    // Enqueue into parent's accept queue
                    Socket* child_sock = cb->socket;
                    if (child_sock != nullptr) {
                        child_sock->state = SocketState::CONNECTED;
                        // Find the listener that spawned this
                        TcpCB* listener = tcp_find_listener(cb->local_ip, cb->local_port);
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
                }
            }
            // Fall through to handle data if present
            if (payload_len > 0 && cb->state == TcpState::ESTABLISHED) {
                goto handle_data;
            }
            break;
        }

        case TcpState::ESTABLISHED: {
        handle_data:
            if ((flags & TCP_RST) != 0) {
                cb->state = TcpState::CLOSED;
                wake_socket(cb->socket);
                break;
            }

            // Process ACK
            if ((flags & TCP_ACK) != 0) {
                if (tcp_seq_after(seg_ack, cb->snd_una) &&
                    !tcp_seq_after(seg_ack, cb->snd_nxt)) {
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
                }
            }

            // Process incoming data
            if (payload_len > 0 && seg_seq == cb->rcv_nxt) {
                // In-order data — deliver to receive buffer
                if (cb->socket != nullptr) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
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
                cb->state = TcpState::CLOSE_WAIT;
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::FIN_WAIT_1: {
            if ((flags & TCP_ACK) != 0) {
                if (seg_ack == cb->snd_nxt) {
                    cb->snd_una = seg_ack;
                    if ((flags & TCP_FIN) != 0) {
                        // Simultaneous close: FIN+ACK
                        cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                        tcp_send_ack(cb);
                        cb->state = TcpState::TIME_WAIT;
                        cb->time_wait_deadline = tcp_now_ms() + 2 * 60000;  // 2*MSL
                    } else {
                        cb->state = TcpState::FIN_WAIT_2;
                    }
                }
            }
            if ((flags & TCP_FIN) != 0 && cb->state == TcpState::FIN_WAIT_1) {
                // FIN without ACK of our FIN -> CLOSING
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                tcp_send_ack(cb);
                cb->state = TcpState::CLOSING;
            }
            // Also deliver any data
            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                    }
                    tcp_send_ack(cb);
                }
            }
            break;
        }

        case TcpState::FIN_WAIT_2: {
            // Deliver data
            if (payload_len > 0 && cb->socket != nullptr) {
                if (seg_seq == cb->rcv_nxt) {
                    ssize_t written = cb->socket->rcvbuf.write(payload, payload_len);
                    if (written > 0) {
                        cb->rcv_nxt += static_cast<uint32_t>(written);
                    }
                    tcp_send_ack(cb);
                }
            }
            if ((flags & TCP_FIN) != 0) {
                cb->rcv_nxt = seg_seq + static_cast<uint32_t>(payload_len) + 1;
                tcp_send_ack(cb);
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 2 * 60000;
                wake_socket(cb->socket);
            }
            break;
        }

        case TcpState::CLOSING: {
            if ((flags & TCP_ACK) != 0 && seg_ack == cb->snd_nxt) {
                cb->state = TcpState::TIME_WAIT;
                cb->time_wait_deadline = tcp_now_ms() + 2 * 60000;
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
                return;  // cb is freed
            }
            break;
        }

        case TcpState::TIME_WAIT: {
            // Re-ACK any segment in TIME_WAIT
            if ((flags & TCP_FIN) != 0) {
                tcp_send_ack(cb);
            }
            break;
        }

        default:
            break;
    }

    cb->lock.unlock();
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
            tcp_send_rst(dst_ip, src_ip, dst_port, src_port,
                         ntohl(hdr->ack), 0, 0);
        } else {
            uint32_t ack_seq = ntohl(hdr->seq) + static_cast<uint32_t>(payload_len);
            if ((hdr->flags & TCP_SYN) != 0) ack_seq++;
            if ((hdr->flags & TCP_FIN) != 0) ack_seq++;
            tcp_send_rst(dst_ip, src_ip, dst_port, src_port,
                         0, ack_seq, TCP_ACK);
        }
    }

    pkt_free(pkt);
}

}  // namespace ker::net::proto
