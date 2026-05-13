#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/packet.hpp>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "net/socket.hpp"
#include "platform/sys/spinlock.hpp"
#include "tcp.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"tcp">;

// Defined in tcp.cpp (namespace scope, not anonymous)
extern std::array<TcpHashBucket, TCB_HASH_SIZE> tcb_hash;
extern volatile uint64_t tcp_ms_counter;

// Timer list: only TCBs with pending timer work are scanned.
namespace {

TcpCB* timer_head = nullptr;
ker::mod::sys::Spinlock timer_lock;
ker::mod::sched::task::Task* timer_task = nullptr;

// Earliest deadline across timer-list entries.
std::atomic<uint64_t> timer_earliest{UINT64_MAX};
std::atomic<uint64_t> timer_lock_contentions{0};

constexpr uint8_t MAX_RETRIES = 8;
constexpr size_t MAX_DEFERRED_RETRANSMITS = 16;
constexpr uint64_t TCP_TIMER_IDLE_SLEEP_US = 100000;
constexpr uint64_t TCP_TIMER_OVERDUE_BACKOFF_US = 1000;
constexpr uint64_t TCP_TIMER_MS_TO_US = 1000;

struct DeferredRetransmit {
    PacketBuffer* pkt;
    uint32_t local_ip;
    uint32_t remote_ip;
    uint32_t seq_end;
    TcpCB* cb;
    TcpCB* ack_cb;
};

void timer_lock_acquire() {
    // Lock order: callers may hold cb->lock when arming/disarming. The timer
    // worker snapshots the list under timer_lock, drops it, then takes cb->lock
    // for protocol work. Spinlock itself disables task preemption, so this lock
    // stays IRQ-enabled while still being safe for preemptible DAEMON workers.
    if (!timer_lock.try_lock()) {
        timer_lock_contentions.fetch_add(1, std::memory_order_relaxed);
        timer_lock.lock();
    }
}

void timer_lock_release() { timer_lock.unlock(); }

auto retransmit_segment(TcpCB* cb, RetransmitEntry* entry, std::array<DeferredRetransmit, MAX_DEFERRED_RETRANSMITS>& deferred,
                        size_t deferred_count) -> size_t {
    entry->retries++;
    entry->send_time_ms = tcp_now_ms();

    if (entry->pkt == nullptr) {
        return deferred_count;
    }
    if (deferred_count >= MAX_DEFERRED_RETRANSMITS) {
        return deferred_count;
    }

    auto* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return deferred_count;
    }

    std::memcpy(pkt->storage.data(), entry->pkt->storage.data(), PKT_BUF_SIZE);
    pkt->data = pkt->storage.data() + (entry->pkt->data - entry->pkt->storage.data());
    pkt->len = entry->pkt->len;

    auto& deferred_entry = deferred.at(deferred_count);
    deferred_entry.pkt = pkt;
    deferred_entry.local_ip = cb->local_ip;
    deferred_entry.remote_ip = cb->remote_ip;
    deferred_entry.seq_end = entry->seq + static_cast<uint32_t>(entry->len);
    tcp_cb_acquire(cb);
    deferred_entry.cb = cb;
    return deferred_count + 1;
}

void wake_socket_timer(Socket* sock) {
    if (sock == nullptr) {
        return;
    }
    uint64_t const PID = sock->owner_pid;
    static_cast<void>(ker::mod::sched::wake_task_by_pid_from_event(PID));
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
    if (cb->keepalive_deadline != 0) {
        deadline = std::min(deadline, cb->keepalive_deadline);
    }

    if (deadline == UINT64_MAX) {
        return;
    }

    bool wake_timer = false;
    timer_lock_acquire();
    if (!cb->on_timer_list) {
        tcp_cb_acquire(cb);
        cb->timer_next = timer_head;
        timer_head = cb;
        cb->on_timer_list = true;
    }
    // Conservatively early is fine.
    uint64_t const CUR = timer_earliest.load(std::memory_order_relaxed);
    if (deadline < CUR) {
        timer_earliest.store(deadline, std::memory_order_relaxed);
        wake_timer = true;
    }
    timer_lock_release();

    if (wake_timer && timer_task != nullptr) {
        ker::mod::sched::kern_wake(timer_task);
    }
}

void tcp_timer_disarm(TcpCB* cb) {
    timer_lock_acquire();
    if (!cb->on_timer_list) {
        timer_lock_release();
        return;
    }

    TcpCB** pp = &timer_head;
    while (*pp != nullptr) {
        if (*pp == cb) {
            *pp = cb->timer_next;
            cb->timer_next = nullptr;
            cb->on_timer_list = false;
            timer_lock_release();
            tcp_cb_release(cb);
            return;
        }
        pp = &(*pp)->timer_next;
    }
    cb->on_timer_list = false;
    timer_lock_release();
}

void tcp_timer_tick(uint64_t now_ms) {
    tcp_ms_counter = now_ms;
#ifdef TCP_DEBUG
    static uint64_t last_status_ms = 0;
    if (now_ms - last_status_ms >= 5000) {
        last_status_ms = now_ms;
        size_t timer_list_len = 0;
        timer_lock_acquire();
        for (TcpCB* c = timer_head; c != nullptr; c = c->timer_next) {
            timer_list_len++;
        }
        timer_lock_release();
        log::debug("pool_free=%zu/%zu timer_list=%zu timer_lock_contentions=%lu", ker::net::pkt_pool_free_count(),
                   ker::net::pkt_pool_size(), timer_list_len, timer_lock_contentions.load(std::memory_order_relaxed));
    }
#endif

    std::array<DeferredRetransmit, MAX_DEFERRED_RETRANSMITS> deferred{};
    size_t deferred_count = 0;
    std::array<Socket*, MAX_DEFERRED_RETRANSMITS> sockets_to_wake{};
    size_t wake_count = 0;

    // Snapshot work under timer_lock; process under cb->lock.
    constexpr size_t MAX_BATCH = 32;
    std::array<TcpCB*, MAX_BATCH> batch{};
    size_t batch_count = 0;

    TcpCB* to_free = nullptr;
    uint64_t next_earliest = UINT64_MAX;

    timer_lock_acquire();
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
        if (cb->keepalive_deadline != 0 && now_ms >= cb->keepalive_deadline && cb->state == TcpState::ESTABLISHED) {
            needs_work = true;
        }

        if (needs_work) {
            tcp_cb_acquire(cb);
            batch.at(batch_count++) = cb;
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
        if (cb->keepalive_deadline != 0) {
            next_earliest = std::min(next_earliest, cb->keepalive_deadline);
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
    timer_lock_release();

    // Free dead TCBs after removing from hash table.
    while (to_free != nullptr) {
        auto* next_free = to_free->timer_next;
        to_free->timer_next = nullptr;
        // hash_next still has the original bucket chain.
        uint32_t const IDX =
            tcp_hash_4tuple(to_free->local_ip, to_free->local_port, to_free->remote_ip, to_free->remote_port) % TCB_HASH_SIZE;
        auto& bucket = tcb_hash.at(IDX);
        bool removed_from_hash = false;
        bucket.lock.lock();
        TcpCB** hp = &bucket.head;
        while (*hp != nullptr) {
            if (*hp == to_free) {
                *hp = to_free->hash_next;
                to_free->hash_next = nullptr;
                removed_from_hash = true;
                break;
            }
            hp = &(*hp)->hash_next;
        }
        bucket.lock.unlock();
        if (removed_from_hash) {
            tcp_cb_release(to_free);
        }
        // Releasing the timer-list membership ref is separate from hash removal.
        tcp_cb_release(to_free);
        to_free = next_free;
    }

    constexpr size_t MAX_TIMER_BATCH_ENTRIES = 4;
    for (size_t i = 0; i < batch_count; i++) {
        TcpCB* rcb = batch.at(i);
        uint64_t const RCB_LOCK_FLAGS = rcb->lock.lock_irqsave();

        if (rcb->delayed_ack_deadline != 0 && now_ms >= rcb->delayed_ack_deadline && rcb->state == TcpState::ESTABLISHED &&
            deferred_count < MAX_DEFERRED_RETRANSMITS) {
            rcb->delayed_ack_deadline = 0;
            rcb->segs_pending_ack = 0;
            uint32_t ack_local = 0;
            uint32_t ack_remote = 0;
            auto* ack_pkt = tcp_build_ack(rcb, &ack_local, &ack_remote);
            if (ack_pkt != nullptr) {
                tcp_cb_acquire(rcb);
                deferred.at(deferred_count++) = {
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
                deferred.at(deferred_count++) = {
                    .pkt = ack_pkt,
                    .local_ip = ack_local,
                    .remote_ip = ack_remote,
                    .seq_end = 0,
                    .cb = nullptr,
                    .ack_cb = rcb,
                };
            }
        }

        // Keepalive probe.
        if (rcb->keepalive_deadline != 0 && now_ms >= rcb->keepalive_deadline && rcb->state == TcpState::ESTABLISHED &&
            deferred_count < MAX_DEFERRED_RETRANSMITS) {
            if (rcb->keepalive_count >= rcb->keepalive_probes_max) {
                // Peer is unreachable; kill the connection.
                rcb->keepalive_deadline = 0;
                rcb->state = TcpState::CLOSED;
                while (rcb->retransmit_head != nullptr) {
                    auto* e = rcb->retransmit_head;
                    rcb->retransmit_head = e->next;
                    if (e->pkt != nullptr) {
                        pkt_free(e->pkt);
                    }
                    delete e;
                }
                rcb->retransmit_tail = nullptr;
                if (rcb->socket != nullptr && wake_count < MAX_DEFERRED_RETRANSMITS) {
                    sockets_to_wake.at(wake_count++) = rcb->socket;
                }
            } else {
                uint32_t ka_local = 0;
                uint32_t ka_remote = 0;
                auto* ka_pkt = tcp_build_keepalive_probe(rcb, &ka_local, &ka_remote);
                if (ka_pkt != nullptr) {
                    deferred.at(deferred_count++) = {
                        .pkt = ka_pkt,
                        .local_ip = ka_local,
                        .remote_ip = ka_remote,
                        .seq_end = 0,
                        .cb = nullptr,
                        .ack_cb = nullptr,
                    };
                }
                rcb->keepalive_count++;
                rcb->keepalive_deadline = now_ms + rcb->keepalive_intvl_ms;
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
                        delete e;
                    }
                    rcb->retransmit_tail = nullptr;
                    if (rcb->socket != nullptr && wake_count < MAX_DEFERRED_RETRANSMITS) {
                        sockets_to_wake.at(wake_count++) = rcb->socket;
                    }
                } else {
                    deferred_count = retransmit_segment(rcb, entry, deferred, deferred_count);

                    if (entry->retries == 1) {
                        auto* nxt = entry->next;
                        while (nxt != nullptr && nxt->retries == 0 && deferred_count < MAX_TIMER_BATCH_ENTRIES) {
                            deferred_count = retransmit_segment(rcb, nxt, deferred, deferred_count);
                            nxt = nxt->next;
                        }
                    }

                    rcb->rto_ms = rcb->rto_ms * 2;
                    rcb->rto_ms = std::min<uint64_t>(rcb->rto_ms, 60000);
                    rcb->retransmit_deadline = now_ms + rcb->rto_ms;
                }
            }
        }

        bool const STILL_ACTIVE = rcb->retransmit_head != nullptr || rcb->ack_pending || rcb->delayed_ack_deadline != 0 ||
                                  rcb->keepalive_deadline != 0 || rcb->state == TcpState::TIME_WAIT ||
                                  (rcb->state == TcpState::FIN_WAIT_2 && rcb->socket == nullptr);
        if (!STILL_ACTIVE) {
            tcp_timer_disarm(rcb);
        }

        rcb->lock.unlock_irqrestore(RCB_LOCK_FLAGS);
        tcp_cb_release(rcb);
    }

    // Send deferred packets outside all locks.
    for (size_t i = 0; i < deferred_count; i++) {
        int tx_res = 0;
        auto& deferred_entry = deferred.at(i);
        if (deferred_entry.seq_end != 0 && deferred_entry.cb != nullptr) {
            uint32_t const UNA = deferred_entry.cb->snd_una;
            if (!tcp_seq_after(deferred_entry.seq_end, UNA)) {
                pkt_free(deferred_entry.pkt);
                tcp_cb_release(deferred_entry.cb);
                if (deferred_entry.ack_cb != nullptr) {
                    tcp_cb_release(deferred_entry.ack_cb);
                }
                continue;
            }
        }
        tx_res = ipv4_tx(deferred_entry.pkt, deferred_entry.local_ip, deferred_entry.remote_ip, 6, 64);

        if (deferred_entry.cb != nullptr) {
            tcp_cb_release(deferred_entry.cb);
        }

        if (deferred_entry.ack_cb != nullptr) {
            TcpCB* ack_cb = deferred_entry.ack_cb;
            uint64_t const ACK_CB_FLAGS = ack_cb->lock.lock_irqsave();
            if (tx_res >= 0) {
                ack_cb->ack_pending = false;
            } else {
                ack_cb->ack_pending = true;
                tcp_timer_arm(ack_cb);
            }
            ack_cb->lock.unlock_irqrestore(ACK_CB_FLAGS);
            tcp_cb_release(ack_cb);
        }
    }

    for (size_t i = 0; i < wake_count; i++) {
        wake_socket_timer(sockets_to_wake.at(i));
    }
}

[[noreturn]] void tcp_timer_thread() {
    for (;;) {
        uint64_t now_ms = ker::mod::time::get_us() / TCP_TIMER_MS_TO_US;
        uint64_t earliest = timer_earliest.load(std::memory_order_relaxed);

        if (now_ms >= earliest) {
            tcp_timer_tick(now_ms);
        }

        uint64_t sleep_us = TCP_TIMER_IDLE_SLEEP_US;
        now_ms = ker::mod::time::get_us() / TCP_TIMER_MS_TO_US;
        earliest = timer_earliest.load(std::memory_order_relaxed);
        if (earliest != UINT64_MAX) {
            if (earliest <= now_ms) {
                sleep_us = TCP_TIMER_OVERDUE_BACKOFF_US;
            } else {
                sleep_us = std::max<uint64_t>((earliest - now_ms) * TCP_TIMER_MS_TO_US, 1);
            }
        }
#ifdef NET_TRACE
        ker::net::trace::flush();
#endif
        ker::mod::sched::kern_sleep_us(sleep_us);
    }
}

void tcp_timer_thread_start() {
    auto* task = ker::mod::sched::task::Task::create_kernel_thread("tcp_timer", tcp_timer_thread);
    if (task == nullptr) {
        log::critical("failed to create TCP timer kernel thread");
        return;
    }
    timer_task = task;
    ker::mod::sched::post_task_balanced(task);
    log::info("TCP timer kernel thread created (PID %x)", task->pid);
}

}  // namespace ker::net::proto
