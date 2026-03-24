#include <algorithm>
#include <cstring>
#include <net/net_trace.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

// Defined in tcp.cpp (namespace scope, not anonymous)
extern TcpHashBucket tcb_hash[];
extern volatile uint64_t tcp_ms_counter;

// Timer list: only TCBs with pending timer work are scanned.
namespace {

TcpCB* timer_head = nullptr;
ker::mod::sys::Spinlock timer_lock;

// Earliest deadline across timer-list entries.
std::atomic<uint64_t> timer_earliest{UINT64_MAX};

constexpr uint8_t MAX_RETRIES = 8;
constexpr size_t MAX_DEFERRED_RETRANSMITS = 16;
constexpr uint64_t TCP_TIMER_IDLE_SLEEP_US = 10000;
constexpr uint64_t TCP_TIMER_MS_TO_US = 1000;

struct DeferredRetransmit {
    PacketBuffer* pkt;
    uint32_t local_ip;
    uint32_t remote_ip;
    uint32_t seq_end;
    const TcpCB* cb;
    TcpCB* ack_cb;
};

void retransmit_segment(TcpCB* cb, RetransmitEntry* entry, DeferredRetransmit* deferred, size_t& deferred_count) {
    entry->retries++;
    entry->send_time_ms = tcp_now_ms();

    if (entry->pkt == nullptr) {
        return;
    }
    if (deferred_count >= MAX_DEFERRED_RETRANSMITS) {
        return;
    }

    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return;
    }

    std::memcpy(pkt->storage.data(), entry->pkt->storage.data(), PKT_BUF_SIZE);
    pkt->data = pkt->storage.data() + (entry->pkt->data - entry->pkt->storage.data());
    pkt->len = entry->pkt->len;

    deferred[deferred_count].pkt = pkt;
    deferred[deferred_count].local_ip = cb->local_ip;
    deferred[deferred_count].remote_ip = cb->remote_ip;
    deferred[deferred_count].seq_end = entry->seq + static_cast<uint32_t>(entry->len);
    deferred[deferred_count].cb = cb;
    deferred_count++;
}

void wake_socket_timer(Socket* sock) {
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

}  // namespace

void tcp_timer_arm(TcpCB* cb) {
    // cb->lock must be held by the caller.
    uint64_t deadline = UINT64_MAX;

    if (cb->retransmit_head != nullptr && cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN) {
        deadline = std::min(deadline, cb->retransmit_deadline);
    }
    if (cb->state == TcpState::TIME_WAIT) {
        deadline = std::min(deadline, cb->time_wait_deadline);
    }
    if (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr) {
        deadline = std::min(deadline, cb->time_wait_deadline != 0 ? cb->time_wait_deadline : tcp_now_ms() + 60000);
    }
    if (cb->ack_pending) {
        deadline = std::min(deadline, tcp_now_ms() + 10);
    }
    if (cb->delayed_ack_deadline != 0) {
        deadline = std::min(deadline, cb->delayed_ack_deadline);
    }

    if (deadline == UINT64_MAX) {
        return;
    }

    timer_lock.lock();
    if (!cb->on_timer_list) {
        tcp_cb_acquire(cb);
        cb->timer_next = timer_head;
        timer_head = cb;
        cb->on_timer_list = true;
    }
    // Conservatively early is fine.
    uint64_t cur = timer_earliest.load(std::memory_order_relaxed);
    if (deadline < cur) {
        timer_earliest.store(deadline, std::memory_order_relaxed);
    }
    timer_lock.unlock();
}

void tcp_timer_disarm(TcpCB* cb) {
    timer_lock.lock();
    if (!cb->on_timer_list) {
        timer_lock.unlock();
        return;
    }

    TcpCB** pp = &timer_head;
    while (*pp != nullptr) {
        if (*pp == cb) {
            *pp = cb->timer_next;
            cb->timer_next = nullptr;
            cb->on_timer_list = false;
            timer_lock.unlock();
            tcp_cb_release(cb);
            return;
        }
        pp = &(*pp)->timer_next;
    }
    cb->on_timer_list = false;
    timer_lock.unlock();
}

void tcp_timer_tick(uint64_t now_ms) {
    tcp_ms_counter = now_ms;
#ifdef TCP_DEBUG
    static uint64_t last_status_ms = 0;
    if (now_ms - last_status_ms >= 5000) {
        last_status_ms = now_ms;
        size_t timer_list_len = 0;
        timer_lock.lock();
        for (TcpCB* c = timer_head; c != nullptr; c = c->timer_next) {
            timer_list_len++;
        }
        timer_lock.unlock();
        ker::mod::dbg::log("[net] pool_free=%zu/%zu timer_list=%zu", ker::net::pkt_pool_free_count(), ker::net::pkt_pool_size(),
                           timer_list_len);
    }
#endif

    DeferredRetransmit deferred[MAX_DEFERRED_RETRANSMITS] = {};
    size_t deferred_count = 0;
    Socket* sockets_to_wake[MAX_DEFERRED_RETRANSMITS];
    size_t wake_count = 0;

    // Snapshot work under timer_lock; process under cb->lock.
    constexpr size_t MAX_BATCH = 32;
    TcpCB* batch[MAX_BATCH];
    size_t batch_count = 0;

    TcpCB* to_free = nullptr;
    uint64_t next_earliest = UINT64_MAX;

    timer_lock.lock();
    TcpCB** pp = &timer_head;
    while (*pp != nullptr && batch_count < MAX_BATCH) {
        TcpCB* cb = *pp;

        uint64_t deadline = UINT64_MAX;
        bool should_remove = false;

        // Cleanup checks; stale reads are safe and handled next tick.
        if (cb->state == TcpState::TIME_WAIT) {
            deadline = cb->time_wait_deadline;
            if (now_ms >= deadline) {
                should_remove = true;
            }
        } else if (cb->state == TcpState::CLOSED && cb->socket == nullptr) {
            should_remove = true;
        } else if (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr) {
            if (cb->time_wait_deadline == 0) {
                cb->time_wait_deadline = now_ms + 60000;
            }
            deadline = cb->time_wait_deadline;
            if (now_ms >= deadline) {
                should_remove = true;
            }
        }

        if (should_remove) {
            *pp = cb->timer_next;
            cb->on_timer_list = false;
            // Reuse timer_next to chain to_free; keep hash_next intact.
            if (cb->socket != nullptr) {
                cb->socket->proto_data = nullptr;
            }
            cb->timer_next = to_free;
            to_free = cb;
            continue;
        }

        bool needs_work = false;
        if (cb->retransmit_head != nullptr && cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN &&
            now_ms >= cb->retransmit_deadline) {
            needs_work = true;
        }
        if (cb->ack_pending && cb->state == TcpState::ESTABLISHED) {
            needs_work = true;
        }
        if (cb->delayed_ack_deadline != 0 && now_ms >= cb->delayed_ack_deadline && cb->state == TcpState::ESTABLISHED) {
            needs_work = true;
        }

        if (needs_work) {
            tcp_cb_acquire(cb);
            batch[batch_count++] = cb;
        }

        if (cb->retransmit_head != nullptr) {
            next_earliest = std::min(next_earliest, cb->retransmit_deadline);
        }
        if (cb->ack_pending) {
            next_earliest = std::min(next_earliest, now_ms + 10);
        }
        if (cb->delayed_ack_deadline != 0) {
            next_earliest = std::min(next_earliest, cb->delayed_ack_deadline);
        }
        if (cb->state == TcpState::TIME_WAIT) {
            next_earliest = std::min(next_earliest, cb->time_wait_deadline);
        }
        if (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr && cb->time_wait_deadline != 0) {
            next_earliest = std::min(next_earliest, cb->time_wait_deadline);
        }

        pp = &(*pp)->timer_next;
    }
    timer_earliest.store(next_earliest, std::memory_order_relaxed);
    timer_lock.unlock();

    // Free dead TCBs after removing from hash table.
    while (to_free != nullptr) {
        auto* next_free = to_free->timer_next;
        to_free->timer_next = nullptr;
        // hash_next still has the original bucket chain.
        uint32_t idx = tcp_hash_4tuple(to_free->local_ip, to_free->local_port, to_free->remote_ip, to_free->remote_port) % TCB_HASH_SIZE;
        auto& bucket = tcb_hash[idx];
        bucket.lock.lock();
        TcpCB** hp = &bucket.head;
        while (*hp != nullptr) {
            if (*hp == to_free) {
                *hp = to_free->hash_next;
                to_free->hash_next = nullptr;
                break;
            }
            hp = &(*hp)->hash_next;
        }
        bucket.lock.unlock();
        tcp_cb_release(to_free);
        to_free = next_free;
    }

    constexpr size_t MAX_TIMER_BATCH_ENTRIES = 4;
    for (size_t i = 0; i < batch_count; i++) {
        TcpCB* rcb = batch[i];
        uint64_t rcb_lock_flags = rcb->lock.lock_irqsave();

        if (rcb->delayed_ack_deadline != 0 && now_ms >= rcb->delayed_ack_deadline && rcb->state == TcpState::ESTABLISHED &&
            deferred_count < MAX_DEFERRED_RETRANSMITS) {
            rcb->delayed_ack_deadline = 0;
            rcb->segs_pending_ack = 0;
            uint32_t ack_local = 0;
            uint32_t ack_remote = 0;
            auto* ack_pkt = tcp_build_ack(rcb, &ack_local, &ack_remote);
            if (ack_pkt != nullptr) {
                tcp_cb_acquire(rcb);
                deferred[deferred_count++] = {
                    .pkt = ack_pkt,
                    .local_ip = ack_local,
                    .remote_ip = ack_remote,
                    .seq_end = 0,
                    .cb = nullptr,
                    .ack_cb = rcb,
                };
            } else {
                rcb->ack_pending = true;
            }
        }

        if (rcb->ack_pending && rcb->state == TcpState::ESTABLISHED && deferred_count < MAX_DEFERRED_RETRANSMITS) {
            uint32_t ack_local = 0;
            uint32_t ack_remote = 0;
            auto* ack_pkt = tcp_build_ack(rcb, &ack_local, &ack_remote);
            if (ack_pkt != nullptr) {
                tcp_cb_acquire(rcb);
                deferred[deferred_count++] = {
                    .pkt = ack_pkt,
                    .local_ip = ack_local,
                    .remote_ip = ack_remote,
                    .seq_end = 0,
                    .cb = nullptr,
                    .ack_cb = rcb,
                };
            }
        }

        if (rcb->retransmit_head != nullptr && rcb->state != TcpState::CLOSED && rcb->state != TcpState::TIME_WAIT &&
            rcb->state != TcpState::LISTEN) {
            if (now_ms >= rcb->retransmit_deadline) {
                auto* entry = rcb->retransmit_head;

                if (entry->retries >= MAX_RETRIES) {
                    rcb->state = TcpState::CLOSED;
                    while (rcb->retransmit_head != nullptr) {
                        auto* e = rcb->retransmit_head;
                        rcb->retransmit_head = e->next;
                        if (e->pkt != nullptr) {
                            pkt_free(e->pkt);
                        }
                        ker::mod::mm::dyn::kmalloc::free(e);
                    }
                    rcb->retransmit_tail = nullptr;
                    if (rcb->socket != nullptr && wake_count < MAX_DEFERRED_RETRANSMITS) {
                        sockets_to_wake[wake_count++] = rcb->socket;
                    }
                } else {
                    retransmit_segment(rcb, entry, deferred, deferred_count);

                    if (entry->retries == 1) {
                        auto* nxt = entry->next;
                        while (nxt != nullptr && nxt->retries == 0 && deferred_count < MAX_TIMER_BATCH_ENTRIES) {
                            retransmit_segment(rcb, nxt, deferred, deferred_count);
                            nxt = nxt->next;
                        }
                    }

                    rcb->rto_ms = rcb->rto_ms * 2;
                    rcb->rto_ms = std::min<uint64_t>(rcb->rto_ms, 60000);
                    rcb->retransmit_deadline = now_ms + rcb->rto_ms;
                }
            }
        }

        bool still_active = rcb->retransmit_head != nullptr || rcb->ack_pending || rcb->delayed_ack_deadline != 0 ||
                            rcb->state == TcpState::TIME_WAIT || (rcb->state == TcpState::FIN_WAIT_2 && rcb->socket == nullptr);
        if (!still_active) {
            tcp_timer_disarm(rcb);
        }

        rcb->lock.unlock_irqrestore(rcb_lock_flags);
        tcp_cb_release(rcb);
    }

    // Send deferred packets outside all locks.
    for (size_t i = 0; i < deferred_count; i++) {
        int tx_res = 0;
        if (deferred[i].seq_end != 0 && deferred[i].cb != nullptr) {
            uint32_t una = deferred[i].cb->snd_una;
            if (!tcp_seq_after(deferred[i].seq_end, una)) {
                pkt_free(deferred[i].pkt);
                if (deferred[i].ack_cb != nullptr) {
                    tcp_cb_release(deferred[i].ack_cb);
                }
                continue;
            }
        }
        tx_res = ipv4_tx(deferred[i].pkt, deferred[i].local_ip, deferred[i].remote_ip, 6, 64);

        if (deferred[i].ack_cb != nullptr) {
            TcpCB* ack_cb = deferred[i].ack_cb;
            uint64_t ack_cb_flags = ack_cb->lock.lock_irqsave();
            if (tx_res >= 0) {
                ack_cb->ack_pending = false;
            } else {
                ack_cb->ack_pending = true;
                tcp_timer_arm(ack_cb);
            }
            ack_cb->lock.unlock_irqrestore(ack_cb_flags);
            tcp_cb_release(ack_cb);
        }
    }

    for (size_t i = 0; i < wake_count; i++) {
        wake_socket_timer(sockets_to_wake[i]);
    }
}

[[noreturn]] void tcp_timer_thread() {
    for (;;) {
        uint64_t now_ms = ker::mod::time::getUs() / TCP_TIMER_MS_TO_US;
        uint64_t earliest = timer_earliest.load(std::memory_order_relaxed);
        uint64_t sleep_us = TCP_TIMER_IDLE_SLEEP_US;

        if (now_ms >= earliest) {
            tcp_timer_tick(now_ms);
        } else if (earliest != UINT64_MAX) {
            uint64_t delta_ms = earliest - now_ms;
            sleep_us = std::min(delta_ms * TCP_TIMER_MS_TO_US, sleep_us);
        }
#ifdef NET_TRACE
        ker::net::trace::flush();
#endif
        ker::mod::sched::kern_sleep_us(sleep_us);
    }
}

void tcp_timer_thread_start() {
    auto* task = ker::mod::sched::task::Task::createKernelThread("tcp_timer", tcp_timer_thread);
    if (task == nullptr) {
        ker::mod::dbg::log("FATAL: Failed to create TCP timer kernel thread");
        return;
    }
    ker::mod::sched::post_task_balanced(task);
    ker::mod::dbg::log("TCP timer kernel thread created (PID %x)", task->pid);
}

}  // namespace ker::net::proto
