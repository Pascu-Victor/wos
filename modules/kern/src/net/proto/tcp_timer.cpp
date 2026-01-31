#include "tcp.hpp"

#include <cstring>
#include <net/proto/ipv4.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net::proto {

// Defined in tcp.cpp (namespace scope, not anonymous)
extern TcpCB* tcb_list_head;
extern ker::mod::sys::Spinlock tcb_list_lock;
extern volatile uint64_t tcp_ms_counter;

namespace {
constexpr uint8_t MAX_RETRIES = 8;

void retransmit_segment(TcpCB* cb, RetransmitEntry* entry) {
    if (entry->pkt == nullptr) {
        return;
    }

    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    std::memcpy(pkt->storage, entry->pkt->storage, PKT_BUF_SIZE);
    pkt->data = pkt->storage + (entry->pkt->data - entry->pkt->storage);
    pkt->len = entry->pkt->len;

    ipv4_tx(pkt, cb->local_ip, cb->remote_ip, 6, 64);

    entry->retries++;
    entry->send_time_ms = tcp_now_ms();
}
}  // namespace

void tcp_timer_tick(uint64_t now_ms) {
    tcp_ms_counter = now_ms;

    tcb_list_lock.lock();
    TcpCB* cb = tcb_list_head;
    TcpCB* prev = nullptr;

    while (cb != nullptr) {
        TcpCB* next = cb->next;

        // TIME_WAIT cleanup
        if (cb->state == TcpState::TIME_WAIT) {
            if (now_ms >= cb->time_wait_deadline) {
                if (prev != nullptr) {
                    prev->next = next;
                } else {
                    tcb_list_head = next;
                }

                if (cb->socket != nullptr) {
                    cb->socket->proto_data = nullptr;
                }

                tcb_list_lock.unlock();
                tcp_free_cb(cb);
                tcb_list_lock.lock();

                cb = next;
                continue;
            }
        }

        // Retransmit timeout
        if (cb->retransmit_head != nullptr && cb->state != TcpState::CLOSED &&
            cb->state != TcpState::TIME_WAIT && cb->state != TcpState::LISTEN) {
            if (now_ms >= cb->retransmit_deadline) {
                auto* entry = cb->retransmit_head;

                if (entry->retries >= MAX_RETRIES) {
                    cb->state = TcpState::CLOSED;
                    while (cb->retransmit_head != nullptr) {
                        auto* e = cb->retransmit_head;
                        cb->retransmit_head = e->next;
                        if (e->pkt != nullptr) {
                            pkt_free(e->pkt);
                        }
                        ker::mod::mm::dyn::kmalloc::free(e);
                    }
                } else {
                    retransmit_segment(cb, entry);

                    cb->rto_ms = cb->rto_ms * 2;
                    if (cb->rto_ms > 60000) {
                        cb->rto_ms = 60000;
                    }
                    cb->retransmit_deadline = now_ms + cb->rto_ms;

                    cb->ssthresh = cb->cwnd / 2;
                    if (cb->ssthresh < 2 * cb->snd_mss) {
                        cb->ssthresh = 2 * cb->snd_mss;
                    }
                    cb->cwnd = cb->snd_mss;
                }
            }
        }

        prev = cb;
        cb = next;
    }

    tcb_list_lock.unlock();
}

}  // namespace ker::net::proto
