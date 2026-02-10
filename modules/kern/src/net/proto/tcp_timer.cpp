#include <algorithm>
#include <cstring>
#include <net/proto/ipv4.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "tcp.hpp"

namespace ker::net::proto {

// Defined in tcp.cpp (namespace scope, not anonymous)
extern TcpCB* tcb_list_head;
extern ker::mod::sys::Spinlock tcb_list_lock;
extern volatile uint64_t tcp_ms_counter;

// Forward declare wake helper (defined in tcp_input.cpp's anonymous namespace,
// but we need our own copy since it's static there).
namespace {
constexpr uint8_t MAX_RETRIES = 8;
constexpr size_t MAX_DEFERRED_RETRANSMITS = 16;
constexpr size_t MAX_RETRANSMIT_BATCH = 32;

// Deferred retransmit work — collected under lock, executed after unlock.
struct DeferredRetransmit {
    PacketBuffer* pkt;
    uint32_t local_ip;
    uint32_t remote_ip;
};

void retransmit_segment(TcpCB* cb, RetransmitEntry* entry, DeferredRetransmit* deferred, size_t& deferred_count) {
    // Always count the attempt so we eventually hit MAX_RETRIES
    // even when the packet pool is exhausted.
    entry->retries++;
    entry->send_time_ms = tcp_now_ms();

    if (entry->pkt == nullptr) {
        return;
    }

    if (deferred_count >= MAX_DEFERRED_RETRANSMITS) {
        return;  // Too many pending; skip this tick
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

void tcp_timer_tick(uint64_t now_ms) {
    tcp_ms_counter = now_ms;

    DeferredRetransmit deferred[MAX_DEFERRED_RETRANSMITS];
    size_t deferred_count = 0;
    Socket* sockets_to_wake[MAX_DEFERRED_RETRANSMITS];
    size_t wake_count = 0;

    // TCBs that need retransmit processing — collected under
    // tcb_list_lock, processed later under cb->lock only.
    TcpCB* retransmit_batch[MAX_RETRANSMIT_BATCH];
    size_t retransmit_count = 0;

    tcb_list_lock.lock();
    TcpCB* cb = tcb_list_head;
    TcpCB* prev = nullptr;
    TcpCB* to_free = nullptr;

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

                cb->next = to_free;
                to_free = cb;
                cb = next;
                continue;
            }
        }

        // Orphaned CLOSED TCB cleanup (socket already freed by close())
        if (cb->state == TcpState::CLOSED && cb->socket == nullptr) {
            if (prev != nullptr) {
                prev->next = next;
            } else {
                tcb_list_head = next;
            }

            cb->next = to_free;
            to_free = cb;
            cb = next;
            continue;
        }

        // FIN_WAIT_2 orphan timeout — if the socket is already closed
        // and the peer hasn't sent FIN within 60 seconds, give up.
        if (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr) {
            if (cb->time_wait_deadline == 0) {
                // First time we notice it's orphaned — start the timer
                cb->time_wait_deadline = now_ms + 60000;
            } else if (now_ms >= cb->time_wait_deadline) {
                if (prev != nullptr) {
                    prev->next = next;
                } else {
                    tcb_list_head = next;
                }

                cb->next = to_free;
                to_free = cb;
                cb = next;
                continue;
            }
        }

        // Collect TCBs that may need retransmit — do NOT acquire
        // cb->lock here to avoid ABBA with tcp_process_segment
        // which holds cb->lock then acquires tcb_list_lock.
        if (cb->retransmit_head != nullptr && cb->state != TcpState::CLOSED && cb->state != TcpState::TIME_WAIT &&
            cb->state != TcpState::LISTEN) {
            if (now_ms >= cb->retransmit_deadline && retransmit_count < MAX_RETRANSMIT_BATCH) {
                tcp_cb_acquire(cb);
                retransmit_batch[retransmit_count++] = cb;
            }
        }

        prev = cb;
        cb = next;
    }

    tcb_list_lock.unlock();

    // Process retransmits under cb->lock only (no tcb_list_lock held).
    for (size_t i = 0; i < retransmit_count; i++) {
        TcpCB* rcb = retransmit_batch[i];
        rcb->lock.lock();

        // Re-check under lock — state may have changed
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
                    if (rcb->socket != nullptr && wake_count < MAX_DEFERRED_RETRANSMITS) {
                        sockets_to_wake[wake_count++] = rcb->socket;
                    }
                } else {
                    retransmit_segment(rcb, entry, deferred, deferred_count);

                    rcb->rto_ms = rcb->rto_ms * 2;
                    rcb->rto_ms = std::min<uint64_t>(rcb->rto_ms, 60000);
                    rcb->retransmit_deadline = now_ms + rcb->rto_ms;

                    rcb->ssthresh = rcb->cwnd / 2;
                    rcb->ssthresh = std::max<uint32_t>(rcb->ssthresh, 2 * rcb->snd_mss);
                    rcb->cwnd = rcb->snd_mss;
                }
            }
        }

        rcb->lock.unlock();
        tcp_cb_release(rcb);
    }

    // Send deferred retransmits OUTSIDE the lock so the NAPI RX worker
    // can acquire tcb_list_lock for tcp_find_cb concurrently.
    for (size_t i = 0; i < deferred_count; i++) {
        ipv4_tx(deferred[i].pkt, deferred[i].local_ip, deferred[i].remote_ip, 6, 64);
    }

    // Wake sockets that had retransmit failures (state → CLOSED)
    for (size_t i = 0; i < wake_count; i++) {
        wake_socket_timer(sockets_to_wake[i]);
    }

    while (to_free != nullptr) {
        auto* next_free = to_free->next;
        to_free->next = nullptr;
        tcp_cb_release(to_free);
        to_free = next_free;
    }
}

[[noreturn]] void tcp_timer_thread() {
    uint64_t last_tick_ms = 0;
    for (;;) {
        uint64_t now_ms = ker::mod::time::getUs() / 1000;
        if (now_ms - last_tick_ms >= 100) {
            tcp_timer_tick(now_ms);
            last_tick_ms = now_ms;
        }
        // Sleep until next interrupt (~10ms timer tick).
        // The scheduler will preempt this thread if other tasks need CPU time.
        ker::mod::sched::kern_yield();
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
