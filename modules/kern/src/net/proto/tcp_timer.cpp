#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <net/packet.hpp>
#include <net/proto/tcp_sender.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#ifdef NET_TRACE
#include <net/net_trace.hpp>
#endif

#include "net/socket.hpp"
#include "platform/sys/spinlock.hpp"
#include "tcp.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"tcp">;

// Defined in tcp.cpp (namespace scope, not anonymous)
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
constexpr uint64_t TCP_TIMER_ALLOCATION_RETRY_MS = 10;

struct DeferredRetransmit {
    PacketBuffer* pkt{};
    SocketEndpoint local{};
    SocketEndpoint remote{};
    uint32_t bound_ifindex{};
    uint32_t seq_end{};
    bool check_sequence{};
    bool ack_completion{};
    bool persist_probe{};
    uint64_t generation{};
    TcpCB* cb{};
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

void stage_packet_locked(TcpCB* cb, PacketBuffer* pkt, DeferredRetransmit& deferred_entry, bool check_sequence = false,
                         uint32_t seq_end = 0, bool ack_completion = false, bool persist_probe = false) {
    tcp_cb_acquire(cb);
    deferred_entry = {
        .pkt = pkt,
        .local = cb->local,
        .remote = cb->remote,
        .bound_ifindex = cb->socket != nullptr ? cb->socket->bound_ifindex : 0,
        .seq_end = seq_end,
        .check_sequence = check_sequence,
        .ack_completion = ack_completion,
        .persist_probe = persist_probe,
        .generation = cb->output_generation,
        .cb = cb,
    };
}

auto find_retransmit_locked(TcpCB* cb, uint32_t sequence) -> RetransmitEntry* {
    for (auto* entry = cb->retransmit_head; entry != nullptr; entry = entry->next) {
        if (tcp_seq_between(sequence, entry->seq, entry->seq + static_cast<uint32_t>(entry->len))) {
            return entry;
        }
    }
    return nullptr;
}

void clear_retransmits_locked(TcpCB* cb) {
    while (cb->retransmit_head != nullptr) {
        auto* entry = cb->retransmit_head;
        cb->retransmit_head = entry->next;
        pkt_free(entry->pkt);
        delete entry;
    }
    cb->retransmit_tail = nullptr;
    cb->retransmit_entries = 0;
    cb->retransmit_deadline = 0;
    cb->fast_retransmit_pending = false;
    cb->fast_retransmit_deadline = 0;
    cb->persist_needed = false;
    cb->persist_deadline = 0;
    cb->keepalive_deadline = 0;
}

auto ooo_ack_probe_interval_ms(uint8_t probes_sent) -> uint64_t {
    uint64_t interval = TCP_OOO_ACK_PROBE_INITIAL_MS;
    for (uint8_t i = 0; i < probes_sent && interval < TCP_OOO_ACK_PROBE_MAX_MS; ++i) {
        interval = std::min<uint64_t>(interval * 2, TCP_OOO_ACK_PROBE_MAX_MS);
    }
    return interval;
}

}  // namespace

void tcp_timer_arm(TcpCB* cb) {
    // cb->lock must be held. Publish one deadline; the list lock never reads
    // protocol fields or nests with cb->lock in the opposite direction.
    uint64_t const NOW = tcp_now_ms();
    uint64_t deadline = UINT64_MAX;
    bool const LIVE = cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN;
    bool const PERSIST = cb->sender_enabled && (cb->state == TcpState::ESTABLISHED || cb->state == TcpState::CLOSE_WAIT) &&
                         cb->snd_wnd == 0 && (cb->persist_needed || cb->retransmit_head != nullptr);
    if (PERSIST) {
        cb->persist_needed = true;
        if (cb->persist_interval == 0) {
            cb->persist_interval = TCP_PERSIST_INITIAL_MS;
        }
        if (cb->persist_deadline == 0) {
            cb->persist_deadline = tcp_deadline_after_ms(NOW, cb->persist_interval);
        }
        deadline = std::min(deadline, cb->persist_deadline);
    } else {
        cb->persist_needed = false;
        cb->persist_deadline = 0;
        cb->persist_interval = TCP_PERSIST_INITIAL_MS;
        if (LIVE && cb->state != TcpState::TIME_WAIT && cb->retransmit_head != nullptr) {
            deadline = std::min(deadline, cb->retransmit_deadline);
        }
        if (LIVE && cb->fast_retransmit_pending) {
            deadline = std::min(deadline, cb->fast_retransmit_deadline != 0 ? cb->fast_retransmit_deadline : NOW);
        }
    }
    if (cb->state == TcpState::TIME_WAIT) {
        deadline = std::min(deadline, cb->time_wait_deadline);
    }
    if (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr) {
        if (cb->time_wait_deadline == 0) {
            cb->time_wait_deadline = tcp_deadline_after_ms(NOW, 60000);
        }
        deadline = std::min(deadline, cb->time_wait_deadline);
    }
    if (cb->state == TcpState::CLOSED && cb->socket == nullptr) {
        deadline = NOW;
    }
    if (LIVE) {
        if (cb->ack_pending) {
            deadline = std::min(deadline, tcp_deadline_after_ms(NOW, TCP_TIMER_ALLOCATION_RETRY_MS));
        }
        if (cb->delayed_ack_deadline != 0) {
            deadline = std::min(deadline, cb->delayed_ack_deadline);
        }
        if (cb->ooo_ack_deadline != 0) {
            deadline = std::min(deadline, cb->ooo_ack_deadline);
        }
        if (cb->keepalive_deadline != 0 && cb->state == TcpState::ESTABLISHED) {
            deadline = std::min(deadline, cb->keepalive_deadline);
        }
    }

    if (deadline == UINT64_MAX) {
        tcp_timer_disarm(cb);
        return;
    }

    bool wake_timer = false;
    timer_lock_acquire();
    cb->timer_deadline.store(deadline, std::memory_order_release);
    if (!cb->on_timer_list) {
        tcp_cb_acquire(cb);
        cb->timer_next = timer_head;
        timer_head = cb;
        cb->on_timer_list = true;
    }
    if (deadline < timer_earliest.load(std::memory_order_relaxed)) {
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
    cb->timer_deadline.store(UINT64_MAX, std::memory_order_release);
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
    constexpr size_t MAX_BATCH = 32;
    std::array<TcpCB*, MAX_BATCH> batch{};
    size_t batch_count = 0;
    uint64_t next_earliest = UINT64_MAX;
#ifdef TCP_DEBUG
    size_t timer_list_len = 0;
#endif

    // Membership owns a reference. Snapshot due work before taking any TCB
    // lock; no protocol fields are accessed while timer_lock is held.
    timer_lock_acquire();
    for (TcpCB* cb = timer_head; cb != nullptr; cb = cb->timer_next) {
#ifdef TCP_DEBUG
        ++timer_list_len;
#endif
        uint64_t const DEADLINE = cb->timer_deadline.load(std::memory_order_acquire);
        next_earliest = std::min(next_earliest, DEADLINE);
        if (DEADLINE <= now_ms && batch_count < MAX_BATCH) {
            tcp_cb_acquire(cb);
            batch.at(batch_count++) = cb;
        }
    }
    timer_earliest.store(next_earliest, std::memory_order_relaxed);
    timer_lock_release();
#ifdef TCP_DEBUG
    static uint64_t last_status_ms = 0;
    if (now_ms - last_status_ms >= 5000) {
        last_status_ms = now_ms;
        log::debug("pool_free=%zu/%zu timer_list=%zu timer_lock_contentions=%lu", ker::net::pkt_pool_free_count(),
                   ker::net::pkt_pool_size(), timer_list_len, timer_lock_contentions.load(std::memory_order_relaxed));
    }
#endif

    std::array<PacketBuffer*, MAX_DEFERRED_RETRANSMITS> spare{};
    size_t spare_count = 0;
    if (batch_count != 0) {
        for (auto& pkt : spare) {
            pkt = pkt_alloc_tx();
            if (pkt == nullptr) {
                break;
            }
            ++spare_count;
        }
    }
    std::array<DeferredRetransmit, MAX_DEFERRED_RETRANSMITS> deferred{};
    size_t deferred_count = 0;
    std::array<Socket*, MAX_BATCH> sockets_to_wake{};
    size_t wake_count = 0;

    for (size_t i = 0; i < batch_count; ++i) {
        TcpCB* cb = batch.at(i);
        uint64_t const FLAGS = cb->lock.lock_irqsave();
        bool const EXPIRED = (cb->state == TcpState::TIME_WAIT && now_ms >= cb->time_wait_deadline) ||
                             (cb->state == TcpState::FIN_WAIT_2 && cb->socket == nullptr && cb->time_wait_deadline != 0 &&
                              now_ms >= cb->time_wait_deadline) ||
                             (cb->state == TcpState::CLOSED && cb->socket == nullptr);
        if (EXPIRED) {
            cb->state = TcpState::CLOSED;
            ++cb->output_generation;
            clear_retransmits_locked(cb);
            tcp_timer_disarm(cb);
            cb->lock.unlock_irqrestore(FLAGS);
            tcp_free_cb(cb);
            tcp_cb_release(cb);
            continue;
        }

        auto stage = [&](bool check_sequence = false, uint32_t seq_end = 0, bool ack_completion = false, bool persist_probe = false) {
            stage_packet_locked(cb, spare.at(deferred_count), deferred.at(deferred_count), check_sequence, seq_end, ack_completion,
                                persist_probe);
            spare.at(deferred_count++) = nullptr;
        };
        auto wake_after_unlock = [&] {
            if (cb->socket != nullptr && wake_count < sockets_to_wake.size() && socket_try_acquire(cb->socket)) {
                sockets_to_wake.at(wake_count++) = cb->socket;
            }
        };
        bool const STREAM = cb->state == TcpState::ESTABLISHED || cb->state == TcpState::CLOSE_WAIT;
        bool const LIVE = cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN && cb->state != TcpState::TIME_WAIT;
        bool const PERSIST = cb->sender_enabled && STREAM && cb->snd_wnd == 0 && cb->persist_needed;
        if (PERSIST && cb->persist_deadline != 0 && now_ms >= cb->persist_deadline) {
            if (deferred_count < spare_count && tcp_build_probe_locked(cb, spare.at(deferred_count))) {
                stage(false, 0, false, true);
                cb->persist_interval =
                    std::min<uint64_t>(std::max<uint64_t>(cb->persist_interval, TCP_PERSIST_INITIAL_MS), TCP_PERSIST_MAX_MS / 2) * 2;
                cb->persist_deadline = tcp_deadline_after_ms(now_ms, cb->persist_interval);
            } else {
                cb->persist_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
            }
        } else if (LIVE && !PERSIST && cb->retransmit_head != nullptr && now_ms >= cb->retransmit_deadline) {
            auto* entry = find_retransmit_locked(cb, cb->snd_una);
            if (entry == nullptr || entry->rto_retries >= MAX_RETRIES) {
                cb->state = TcpState::CLOSED;
                ++cb->output_generation;
                clear_retransmits_locked(cb);
                if (cb->socket != nullptr) {
                    cb->socket->pending_error.store(ETIMEDOUT, std::memory_order_release);
                }
                wake_after_unlock();
            } else if (entry != nullptr && deferred_count < spare_count) {
                uint32_t const START = cb->snd_una;
                uint32_t const REMAINING = entry->seq + static_cast<uint32_t>(entry->len) - START;
                uint32_t const END = START + std::min<uint32_t>(REMAINING, std::max<uint16_t>(cb->snd_mss, 1));
                if (tcp_build_retransmit_locked(cb, entry, START, END, spare.at(deferred_count))) {
                    stage(true, END);
                    ++entry->rto_retries;
                    if (cb->sender_enabled) {
                        tcp_sender_on_timeout(cb->sender, cb->snd_una, cb->snd_nxt, !cb->rto_backed_off);
                    }
                    cb->rto_backed_off = true;
                    cb->fast_retransmit_pending = false;
                    cb->fast_retransmit_deadline = 0;
                    cb->rto_ms = std::min<uint64_t>(std::max<uint64_t>(cb->rto_ms, TCP_RTO_MIN_MS), TCP_RTO_MAX_MS / 2) * 2;
                    cb->retransmit_deadline = tcp_deadline_after_ms(now_ms, cb->rto_ms);
                } else {
                    cb->retransmit_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
                }
            } else {
                cb->retransmit_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
            }
        } else if (LIVE && !PERSIST && cb->fast_retransmit_pending &&
                   (cb->fast_retransmit_deadline == 0 || now_ms >= cb->fast_retransmit_deadline)) {
            TcpSackRange hole{};
            bool const HAS_HOLE = tcp_sack_first_hole(cb->sender.sack, cb->snd_una, cb->snd_nxt, hole);
            uint32_t const START = hole.left;
            auto* entry = HAS_HOLE ? find_retransmit_locked(cb, START) : nullptr;
            if (entry == nullptr) {
                cb->fast_retransmit_pending = false;
                cb->fast_retransmit_deadline = 0;
            } else if (deferred_count < spare_count) {
                uint32_t const REMAINING = entry->seq + static_cast<uint32_t>(entry->len) - START;
                uint32_t const END =
                    START + std::min({REMAINING, hole.right - START, static_cast<uint32_t>(std::max<uint16_t>(cb->snd_mss, 1))});
                if (tcp_build_retransmit_locked(cb, entry, START, END, spare.at(deferred_count))) {
                    stage(true, END);
                    cb->fast_retransmit_seq = START;
                    cb->fast_retransmit_pending = false;
                    cb->fast_retransmit_deadline = 0;
                } else {
                    cb->fast_retransmit_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
                }
            } else {
                cb->fast_retransmit_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
            }
        }

        bool const ACK_STATE = cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN && cb->state != TcpState::SYN_SENT;
        if (ACK_STATE && (cb->ack_pending || (cb->delayed_ack_deadline != 0 && now_ms >= cb->delayed_ack_deadline))) {
            if (deferred_count < spare_count && tcp_build_ack_locked(cb, spare.at(deferred_count))) {
                stage(false, 0, true);
                // Clear at admission. A later RX request must survive successful
                // completion of this older ACK; only failed sends re-arm it.
                cb->ack_pending = false;
                cb->delayed_ack_deadline = 0;
                cb->segs_pending_ack = 0;
            } else {
                cb->ack_pending = true;
                cb->delayed_ack_deadline = 0;
            }
        }
        if (STREAM && cb->ooo_ack_deadline != 0 && now_ms >= cb->ooo_ack_deadline) {
            if (cb->ooo_head == nullptr && !cb->ooo_fin_pending) {
                cb->ooo_ack_deadline = 0;
                cb->ooo_ack_probes = 0;
            } else if (deferred_count < spare_count && tcp_build_ack_locked(cb, spare.at(deferred_count))) {
                stage(false, 0, true);
                ++cb->ooo_ack_probes;
                cb->ooo_ack_deadline = cb->ooo_ack_probes < TCP_OOO_ACK_PROBE_MAX_COUNT
                                           ? tcp_deadline_after_ms(now_ms, ooo_ack_probe_interval_ms(cb->ooo_ack_probes))
                                           : 0;
            } else {
                cb->ooo_ack_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
            }
        }
        if (cb->state == TcpState::ESTABLISHED && cb->keepalive_deadline != 0 && now_ms >= cb->keepalive_deadline) {
            if (cb->keepalive_count >= cb->keepalive_probes_max) {
                cb->state = TcpState::CLOSED;
                ++cb->output_generation;
                clear_retransmits_locked(cb);
                if (cb->socket != nullptr) {
                    cb->socket->pending_error.store(ETIMEDOUT, std::memory_order_release);
                }
                wake_after_unlock();
            } else if (deferred_count < spare_count && tcp_build_keepalive_locked(cb, spare.at(deferred_count))) {
                stage();
                ++cb->keepalive_count;
                cb->keepalive_deadline = tcp_deadline_after_ms(now_ms, cb->keepalive_intvl_ms);
            } else {
                cb->keepalive_deadline = tcp_deadline_after_ms(now_ms, TCP_TIMER_ALLOCATION_RETRY_MS);
            }
        }

        tcp_timer_arm(cb);
        cb->lock.unlock_irqrestore(FLAGS);
        tcp_cb_release(cb);
    }

    // Retained TCBs and copied packets survive ACK/close. Revalidate under the
    // TCB lock, then route/transmit only after releasing every protocol lock.
    for (size_t i = 0; i < deferred_count; ++i) {
        auto& entry = deferred.at(i);
        TcpCB* cb = entry.cb;
        uint64_t flags = cb->lock.lock_irqsave();
        bool const LIVE = cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN;
        bool const NEEDED = LIVE && cb->output_generation == entry.generation &&
                            (!entry.check_sequence || tcp_seq_after(entry.seq_end, cb->snd_una)) &&
                            (!entry.persist_probe || (cb->persist_needed && cb->snd_wnd == 0));
        cb->lock.unlock_irqrestore(flags);
        int const RESULT = NEEDED ? tcp_transmit_prebuilt(entry.pkt, entry.local, entry.remote, entry.bound_ifindex) : 0;
        if (!NEEDED) {
            pkt_free(entry.pkt);
        }
        if (NEEDED && (entry.check_sequence || entry.persist_probe) && RESULT >= 0) {
            flags = cb->lock.lock_irqsave();
            if (cb->output_generation == entry.generation) {
                if (entry.persist_probe) {
                    if (cb->persist_probes != UINT64_MAX) {
                        ++cb->persist_probes;
                    }
                } else if (cb->retransmit_packets != UINT64_MAX) {
                    ++cb->retransmit_packets;
                }
            }
            cb->lock.unlock_irqrestore(flags);
        }
        if (NEEDED && entry.ack_completion && RESULT < 0) {
            flags = cb->lock.lock_irqsave();
            if (cb->state != TcpState::CLOSED && cb->state != TcpState::LISTEN && cb->output_generation == entry.generation) {
                cb->ack_pending = true;
                tcp_timer_arm(cb);
            }
            cb->lock.unlock_irqrestore(flags);
        }
        tcp_cb_release(cb);
    }
    for (auto* pkt : spare) {
        pkt_free(pkt);
    }
    for (size_t i = 0; i < wake_count; ++i) {
        socket_wake_waiters(sockets_to_wake.at(i));
        socket_release(sockets_to_wake.at(i));
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
                // Periodic bounded sleep also covers an arm/wake racing with
                // this worker entering sleep. Clamp before multiplication.
                sleep_us = std::max<uint64_t>(
                    std::min<uint64_t>(earliest - now_ms, TCP_TIMER_IDLE_SLEEP_US / TCP_TIMER_MS_TO_US) * TCP_TIMER_MS_TO_US, 1);
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
