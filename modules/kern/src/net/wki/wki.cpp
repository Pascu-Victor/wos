#include "wki.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <net/backlog.hpp>
#include <net/netpoll.hpp>
#include <net/wki/channel.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/event.hpp>
#include <net/wki/irq_fwd.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_compute.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/routing.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <ranges>
#include <span>
#include <util/hostname.hpp>
#include <utility>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "net/packet.hpp"
#include "platform/sys/spinlock.hpp"
namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Global state
// -----------------------------------------------------------------------------

WkiState g_wki;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

namespace {

// Re-entrancy guard for deferred work kicked by wki_timer_tick().
// wki_remote_vfs_mount() may spin-wait via wki_spin_yield(), which calls
// wki_timer_tick() recursively.  The guard prevents deferred mount/attach
// processing from re-entering itself while the worker is blocked inside
// wki_wait_for_op().
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_timer_deferred_running{false};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<mod::sched::task::Task*> s_timer_deferred_task{nullptr};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_deferred_work_pending{false};
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
mod::sched::task::Task* s_deferred_work_task = nullptr;

constexpr uint32_t WKI_DEFERRED_WORK_SLICE_NS = 2'000'000;
constexpr int WKI_DEFERRED_WORK_NICE = -5;

// RAII guard: ensures s_timer_deferred_running is reset even if a deferred
// work function panics or asserts.  Without this, a crash inside the
// deferred section would leave the flag permanently true, silently blocking
// all future deferred work (mounts, net attaches, zone creates, etc.).
struct DeferredGuard {
    bool entered = false;
    bool try_enter() {
        bool expected = false;
        entered = s_timer_deferred_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
        if (entered) {
            s_timer_deferred_task.store(mod::sched::get_current_task(), std::memory_order_release);
        }
        return entered;
    }
    ~DeferredGuard() {
        if (entered) {
            s_timer_deferred_task.store(nullptr, std::memory_order_release);
            s_timer_deferred_running.store(false, std::memory_order_release);
        }
    }
};

auto is_timer_deferred_waiter(const mod::sched::task::Task* task) -> bool {
    return task != nullptr && s_timer_deferred_task.load(std::memory_order_acquire) == task;
}

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }
    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t') {
            ++cursor;
        }
        const char* start = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t') {
            ++cursor;
        }
        if (std::cmp_equal(cursor - start, TOKEN_LEN) && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto wait_diag_enabled() -> bool {
    static std::atomic<int> cached{-1};
    int value = cached.load(std::memory_order_acquire);
    if (value >= 0) {
        return value != 0;
    }
    int const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.wait_diag") ? 1 : 0;
    cached.store(ENABLED, std::memory_order_release);
    return ENABLED != 0;
}

constexpr uint64_t WKI_WAIT_DIAG_FIRST_US = 500'000;
constexpr uint64_t WKI_WAIT_DIAG_INTERVAL_US = 1'000'000;

void log_wait_diag(const char* event, const WkiWaitEntry* entry, uint64_t started_us, uint64_t now_us, uint64_t timeout_us) {
    if (entry == nullptr) {
        return;
    }
    auto* task = entry->task.load(std::memory_order_acquire);
    const char* const TASK_NAME = (task != nullptr && task->name != nullptr) ? task->name : "?";
    const uint64_t PID = task != nullptr ? task->pid : 0;
    const uint32_t TASK_TYPE = task != nullptr ? static_cast<uint32_t>(task->type) : 0;
    log::warn(
        "wait_%s entry=%p elapsed_us=%lu timeout_us=%lu deadline=%lu state=%u result=%d task=0x%lx(%s) type=%u diag=%s op=0x%x "
        "peer=0x%04x chan=%u res=%u cookie=%u arg0=0x%lx arg1=0x%lx site=0x%lx",
        event != nullptr ? event : "pending", entry, static_cast<unsigned long>(now_us - started_us),
        static_cast<unsigned long>(timeout_us), static_cast<unsigned long>(entry->deadline_us),
        static_cast<unsigned>(entry->state.load(std::memory_order_acquire)), entry->result, static_cast<unsigned long>(PID), TASK_NAME,
        TASK_TYPE, entry->diag_name != nullptr ? entry->diag_name : "-", static_cast<unsigned>(entry->diag_op_id),
        static_cast<unsigned>(entry->diag_peer), static_cast<unsigned>(entry->diag_channel), entry->diag_resource_id,
        static_cast<unsigned>(entry->diag_cookie), static_cast<unsigned long>(entry->diag_arg0),
        static_cast<unsigned long>(entry->diag_arg1), static_cast<unsigned long>(entry->diag_callsite));
}

void process_deferred_blocking_work() {
    DeferredGuard deferred;
    if (!deferred.try_enter()) {
        return;
    }

    // Process deferred RDMA zone creations (must run outside NAPI poll context)
    wki_dev_server_process_pending_zones();

    // Process deferred VFS auto-mounts (must run outside NAPI poll context)
    wki_remotable_process_pending_mounts();

    // V2: Process deferred NET auto-attaches (must run outside NAPI poll context)
    wki_remotable_process_pending_net_attaches();

    // Poll RDMA block ring SQ entries on server bindings
    wki_dev_server_poll_rings();

    // Refresh proxy NIC stats without blocking on a response.
    wki_remote_net_poll_stats();
}

void wki_deferred_work_notify() {
    s_deferred_work_pending.store(true, std::memory_order_release);
    if (s_deferred_work_task != nullptr) {
        mod::sched::kern_wake(s_deferred_work_task);
    }
}

[[noreturn]] void wki_deferred_work_thread() {
    for (;;) {
        if (!s_deferred_work_pending.exchange(false, std::memory_order_acq_rel)) {
            mod::sched::kern_block();
            continue;
        }

        process_deferred_blocking_work();
    }
}

auto find_transport_for_peer(uint16_t dst_node) -> WkiTransport*;
auto resolve_next_hop(uint16_t dst_node) -> uint16_t;

auto perf_current_pid() -> uint64_t { return 0; }

auto perf_current_cpu() -> uint32_t { return static_cast<uint32_t>(mod::cpu::get_current_cpu_id_safe()); }

void mark_peer_rx_progress(uint16_t src_node) {
    if (src_node == WKI_NODE_INVALID || src_node == WKI_NODE_BROADCAST || src_node == g_wki.my_node_id) {
        return;
    }

    WkiPeer* peer = wki_peer_find(src_node);
    if (peer != nullptr && peer->state == PeerState::CONNECTED) {
        peer->last_rx_activity = wki_now_us();
        peer->missed_beats = 0;
        peer->fence_defer_until_us = 0;
    }
}

void mark_peer_tx_progress(uint16_t dst_node) {
    if (dst_node == WKI_NODE_INVALID || dst_node == WKI_NODE_BROADCAST || dst_node == g_wki.my_node_id) {
        return;
    }

    WkiPeer* peer = wki_peer_find(dst_node);
    if (peer != nullptr && peer->state == PeerState::CONNECTED) {
        peer->last_tx_activity = wki_now_us();
    }
}

auto reliable_rx_peer_state_accepts(PeerState state) -> bool { return state == PeerState::CONNECTED; }

auto reliable_rx_peer_accepts(uint16_t src_node) -> bool {
    if (src_node == WKI_NODE_INVALID || src_node == WKI_NODE_BROADCAST || src_node == g_wki.my_node_id) {
        return false;
    }

    WkiPeer const* peer = wki_peer_find(src_node);
    return peer != nullptr && reliable_rx_peer_state_accepts(peer->state);
}

auto message_uses_compute_lifecycle(MsgType type) -> bool {
    switch (type) {
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_ACCEPT:
        case MsgType::TASK_REJECT:
            return true;
        default:
            return false;
    }
}

auto message_uses_deferred_compute_rx(MsgType type) -> bool { return type == MsgType::TASK_COMPLETE || type == MsgType::TASK_CANCEL; }

class PeerLifecycleTryLease {
   public:
    PeerLifecycleTryLease() = default;
    PeerLifecycleTryLease(const PeerLifecycleTryLease&) = delete;
    auto operator=(const PeerLifecycleTryLease&) -> PeerLifecycleTryLease& = delete;

    ~PeerLifecycleTryLease() { wki_peer_lifecycle_release(claimed_peer); }

    auto try_acquire(uint16_t node_id) -> bool {
        if (claimed_peer != nullptr) {
            return false;
        }
        WkiPeer* peer = wki_peer_find(node_id);
        if (!wki_peer_lifecycle_try_acquire(peer)) {
            return false;
        }
        claimed_peer = peer;
        return true;
    }

    [[nodiscard]] auto owns(uint16_t node_id) const -> bool { return claimed_peer != nullptr && claimed_peer->node_id == node_id; }

   private:
    WkiPeer* claimed_peer = nullptr;
};

void perf_record_transport_point(mod::perf::WkiPerfTransportOp op, uint16_t peer, uint16_t channel, int32_t status, uint32_t aux,
                                 uint32_t correlation, uint64_t callsite) {
    if (!mod::perf::is_wki_recording_enabled()) {
        return;
    }

    mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), mod::perf::WkiPerfScope::TRANSPORT, static_cast<uint8_t>(op),
                                mod::perf::WkiPerfPhase::POINT, peer, channel, correlation, status, aux, callsite);
    mod::perf::record_wki_summary(mod::perf::WkiPerfScope::TRANSPORT, static_cast<uint8_t>(op), peer, channel, status, 0, false, 0, 0);
}

void perf_record_transport_begin(uint16_t peer, uint16_t channel, uint32_t correlation, uint32_t bytes, uint64_t callsite) {
    if (!mod::perf::is_wki_recording_enabled()) {
        return;
    }

    mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), mod::perf::WkiPerfScope::TRANSPORT,
                                static_cast<uint8_t>(mod::perf::WkiPerfTransportOp::SEND), mod::perf::WkiPerfPhase::BEGIN, peer, channel,
                                correlation, 0, bytes, callsite);
}

void perf_record_transport_end(uint16_t peer, uint16_t channel, uint32_t correlation, int32_t status, uint32_t bytes, uint64_t callsite) {
    if (!mod::perf::is_wki_recording_enabled()) {
        return;
    }

    mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), mod::perf::WkiPerfScope::TRANSPORT,
                                static_cast<uint8_t>(mod::perf::WkiPerfTransportOp::SEND), mod::perf::WkiPerfPhase::END, peer, channel,
                                correlation, status, bytes, callsite);
    mod::perf::record_wki_summary(mod::perf::WkiPerfScope::TRANSPORT, static_cast<uint8_t>(mod::perf::WkiPerfTransportOp::SEND), peer,
                                  channel, status, 0, false, 0, bytes);
}

void perf_record_transport_rtt(uint16_t peer, uint16_t channel, uint32_t correlation, uint32_t rtt_us, uint32_t retries) {
    if (!mod::perf::is_wki_recording_enabled()) {
        return;
    }

    mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), mod::perf::WkiPerfScope::TRANSPORT,
                                static_cast<uint8_t>(mod::perf::WkiPerfTransportOp::ACK_RTT), mod::perf::WkiPerfPhase::END, peer, channel,
                                correlation, 0, rtt_us, 0);
    mod::perf::record_wki_summary(mod::perf::WkiPerfScope::TRANSPORT, static_cast<uint8_t>(mod::perf::WkiPerfTransportOp::ACK_RTT), peer,
                                  channel, 0, rtt_us, true, retries, 0);
}

constexpr uint32_t WKI_PERF_STALL_THRESHOLD_US = 5000;
constexpr uint32_t WKI_PERF_STALL_REPORT_INTERVAL_US = 50000;

constexpr uint32_t WKI_PERF_STALL_FLAG_OUTSTANDING = 1U << 0;
constexpr uint32_t WKI_PERF_STALL_FLAG_NO_CREDITS = 1U << 1;
constexpr uint32_t WKI_PERF_STALL_FLAG_ACK_PENDING = 1U << 2;
constexpr uint32_t WKI_PERF_STALL_FLAG_HAS_RETRANSMIT = 1U << 3;
constexpr uint32_t WKI_PERF_STALL_FLAG_RETRANSMIT_DUE = 1U << 4;
constexpr uint32_t WKI_PERF_STALL_FLAG_FAST_RETRANSMIT = 1U << 5;
constexpr uint32_t WKI_PERF_STALL_FLAG_ACK_DELAY_DUE = 1U << 6;

constexpr uint32_t WKI_IPC_DATA_ACK_DELAY_US = 500;

auto ack_delay_for_channel(uint16_t channel_id) -> uint32_t {
    if (channel_id == WKI_CHAN_IPC_DATA) {
        return WKI_IPC_DATA_ACK_DELAY_US;
    }
    return WKI_ACK_DELAY_US;
}

auto pack_transport_stall_status(WkiChannel const* ch, uint64_t now_us) -> uint32_t {
    uint32_t flags = 0;
    if (ch->tx_seq != ch->tx_ack) {
        flags |= WKI_PERF_STALL_FLAG_OUTSTANDING;
    }
    if (ch->tx_credits == 0) {
        flags |= WKI_PERF_STALL_FLAG_NO_CREDITS;
    }
    if (ch->ack_pending) {
        flags |= WKI_PERF_STALL_FLAG_ACK_PENDING;
        if (now_us >= wki_future_deadline_us(ch->ack_pending_since_us, ack_delay_for_channel(ch->channel_id))) {
            flags |= WKI_PERF_STALL_FLAG_ACK_DELAY_DUE;
        }
    }
    if (ch->retransmit_head != nullptr) {
        flags |= WKI_PERF_STALL_FLAG_HAS_RETRANSMIT;
        if (now_us >= ch->retransmit_deadline) {
            flags |= WKI_PERF_STALL_FLAG_RETRANSMIT_DUE;
        }
    }
    if (ch->dup_ack_count >= WKI_FAST_RETRANSMIT_THRESH) {
        flags |= WKI_PERF_STALL_FLAG_FAST_RETRANSMIT;
    }

    uint32_t const TX_CREDITS = std::min<uint32_t>(ch->tx_credits, UINT8_MAX);
    uint32_t const RETRANSMIT_COUNT = std::min<uint32_t>(ch->retransmit_count, UINT8_MAX);
    uint32_t const INFLIGHT = std::min<uint32_t>(ch->tx_seq - ch->tx_ack, 0x7FU);
    return flags | (TX_CREDITS << 8U) | (RETRANSMIT_COUNT << 16U) | (INFLIGHT << 24U);
}

void perf_record_transport_stall_locked(WkiChannel* ch, uint64_t now_us) {
    if (!mod::perf::is_wki_recording_enabled()) {
        ch->perf_last_stall_report_us = 0;
        ch->perf_last_stall_status = 0;
        return;
    }

    if (ch->retransmit_head == nullptr || now_us < ch->retransmit_head->send_time_us) {
        ch->perf_last_stall_report_us = 0;
        ch->perf_last_stall_status = 0;
        return;
    }

    uint64_t const STALL_AGE_US = now_us - ch->retransmit_head->send_time_us;
    if (STALL_AGE_US < WKI_PERF_STALL_THRESHOLD_US) {
        ch->perf_last_stall_report_us = 0;
        ch->perf_last_stall_status = 0;
        return;
    }

    uint32_t const STATUS = pack_transport_stall_status(ch, now_us);
    bool const SHOULD_REPORT = (ch->perf_last_stall_report_us == 0) ||
                               (now_us - ch->perf_last_stall_report_us >= WKI_PERF_STALL_REPORT_INTERVAL_US) ||
                               (STATUS != ch->perf_last_stall_status);
    if (!SHOULD_REPORT) {
        return;
    }

    perf_record_transport_point(mod::perf::WkiPerfTransportOp::STALL, ch->peer_node_id, ch->channel_id, static_cast<int32_t>(STATUS),
                                static_cast<uint32_t>(std::min<uint64_t>(STALL_AGE_US, UINT32_MAX)), ch->retransmit_head->seq,
                                WOS_PERF_CALLSITE());
    ch->perf_last_stall_report_us = now_us;
    ch->perf_last_stall_status = STATUS;
}

}  // namespace

// -----------------------------------------------------------------------------
// Time source
// -----------------------------------------------------------------------------

auto wki_now_us() -> uint64_t { return mod::time::get_us(); }

auto wki_current_wait_must_drive_progress() -> bool { return is_timer_deferred_waiter(mod::sched::get_current_task()); }

void wki_spin_yield() {
    // Drive inline NAPI poll so the NIC's RX queue is drained even when
    // the caller is busy-waiting on this CPU.
    net::NetDevice* dev = wki_eth_get_netdev();
    if (dev != nullptr) {
        net::napi_poll_all_pending();
        net::backlog_drain_all_pending_inline();
    }

    // Process retransmit timers and delayed ACKs
    wki_timer_tick(wki_now_us());
}

// -----------------------------------------------------------------------------
// CRC32 (standard polynomial 0xEDB88320)
// -----------------------------------------------------------------------------

namespace {
// NOLINTBEGIN(readability-magic-numbers)
constexpr std::array<uint32_t, 256> CRC32_TABLE = [] {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320U : crc >> 1;
        }
        table.at(i) = crc;
    }
    return table;
}();
// NOLINTEND(readability-magic-numbers)
}  // namespace

auto wki_crc32(const void* data, size_t len) -> uint32_t {
    const auto* p = static_cast<const uint8_t*>(data);
    std::span<const uint8_t> const BYTES{p, len};
    uint32_t crc = 0xFFFFFFFF;
    for (uint8_t const BYTE : BYTES) {
        crc = CRC32_TABLE.at((crc ^ BYTE) & 0xFF) ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

auto wki_crc32_continue(uint32_t prev_crc, const void* data, size_t len) -> uint32_t {
    const auto* p = static_cast<const uint8_t*>(data);
    std::span<const uint8_t> const BYTES{p, len};
    uint32_t crc = prev_crc ^ 0xFFFFFFFF;  // un-finalize previous
    for (uint8_t const BYTE : BYTES) {
        crc = CRC32_TABLE.at((crc ^ BYTE) & 0xFF) ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

void wki_deferred_work_thread_start() {
    if (s_deferred_work_task != nullptr) {
        return;
    }

    auto* task = mod::sched::task::Task::create_kernel_thread("wki_deferred", wki_deferred_work_thread);
    if (task == nullptr) {
        log::error("Failed to create WKI deferred worker");
        return;
    }
    task->slice_ns = WKI_DEFERRED_WORK_SLICE_NS;
    mod::sched::set_task_nice(task, WKI_DEFERRED_WORK_NICE);
    s_deferred_work_task = task;
    mod::sched::post_task_balanced(task);
    log::info("Deferred worker started (PID %d)", task->pid);
}

namespace {

auto wki_frame_checksum(const WkiHeader& hdr, const uint8_t* payload) -> uint32_t {
    WkiHeader hdr_copy = hdr;
    hdr_copy.checksum = 0;
    uint32_t crc = wki_crc32(&hdr_copy, WKI_HEADER_SIZE);
    if (hdr.payload_len > 0) {
        crc = wki_crc32_continue(crc, payload, hdr.payload_len);
    }
    return crc;
}

}  // namespace

// -----------------------------------------------------------------------------
// V2: Async Wait Queue
// Lock-free polling via kern_yield() - matches existing WKI blocking patterns.
// The pending list is protected by a spinlock for link/unlink only. Result
// writes are serialized by WkiWaitEntry::state so stale responses cannot write
// into a stack waiter after the owner has timed out and unwound.
// -----------------------------------------------------------------------------

namespace {

mod::sys::Spinlock s_wait_lock;       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
WkiWaitEntry* s_wait_head = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void wait_list_link(WkiWaitEntry* entry) {
    s_wait_lock.lock();
    entry->prev = nullptr;
    entry->next = s_wait_head;
    if (s_wait_head != nullptr) {
        s_wait_head->prev = entry;
    }
    s_wait_head = entry;
    s_wait_lock.unlock();
}

void wait_list_unlink(WkiWaitEntry* entry) {
    s_wait_lock.lock();
    if (entry->prev != nullptr) {
        entry->prev->next = entry->next;
    } else if (s_wait_head == entry) {
        s_wait_head = entry->next;
    }
    if (entry->next != nullptr) {
        entry->next->prev = entry->prev;
    }
    entry->next = nullptr;
    entry->prev = nullptr;
    s_wait_lock.unlock();
}

[[nodiscard]] auto wait_state(WkiWaitEntry const* entry) -> uint8_t { return entry->state.load(std::memory_order_acquire); }

[[nodiscard]] auto wait_done(WkiWaitEntry const* entry) -> bool { return wait_state(entry) == WkiWaitEntry::DONE; }

[[nodiscard]] auto claim_wait_entry(WkiWaitEntry* entry) -> bool {
    uint8_t expected = WkiWaitEntry::PENDING;
    return entry->state.compare_exchange_strong(expected, WkiWaitEntry::CLAIMED, std::memory_order_acq_rel, std::memory_order_acquire);
}

void publish_claimed_wait_entry(WkiWaitEntry* entry, int result) {
    entry->result = result;
    entry->state.store(WkiWaitEntry::DONE, std::memory_order_release);
}

}  // namespace

auto wki_wait_for_op(WkiWaitEntry* entry, uint64_t timeout_us) -> int {
    // Record the calling task so wake_op can send an IPI. Do not reset
    // state/result here: many callers publish the stack wait entry before
    // sending, and a fast response may complete it before we enter this helper.
    uint64_t const STARTED_US = wki_now_us();
    entry->task.store(mod::sched::get_current_task(), std::memory_order_release);
    entry->deadline_us = (timeout_us > 0) ? wki_future_deadline_us(STARTED_US, timeout_us) : 0;

    bool linked = false;
    if (!wait_done(entry)) {
        // Link into pending list (for timeout scan)
        wait_list_link(entry);
        linked = true;
        if (entry->deadline_us != 0) {
            wki_timer_notify();
        }
    }

    // Sleep until RX/timer completion.
    //
    // DAEMON waiters can use a true block. PROCESS waiters must stay on the
    // existing voluntary-yield path for now: proxy exec handoff intentionally
    // suppresses one class of concurrent wake in deferred_task_switch(), and
    // using kern_block() here can therefore lose a wake and strand the task
    // until an unrelated signal arrives.
    uint64_t next_diag_us = wait_diag_enabled() ? wki_future_deadline_us(STARTED_US, WKI_WAIT_DIAG_FIRST_US) : 0;
    while (!wait_done(entry)) {
        uint64_t const NOW_US = wki_now_us();
        if (entry->deadline_us != 0 && NOW_US >= entry->deadline_us) {
            // Timed out locally. Claim completion before returning so a late
            // response cannot still complete the same stack waiter after the
            // caller has started timeout cleanup/retry handling.
            if (claim_wait_entry(entry)) {
                publish_claimed_wait_entry(entry, WKI_ERR_TIMEOUT);
            }
            if (linked) {
                wait_list_unlink(entry);
            }
            if (next_diag_us != 0) {
                log_wait_diag("timeout", entry, STARTED_US, NOW_US, timeout_us);
            }
            while (!wait_done(entry)) {
                mod::sched::kern_yield();
            }
            return entry->result;
        }
        if (next_diag_us != 0 && NOW_US >= next_diag_us) {
            log_wait_diag("pending", entry, STARTED_US, NOW_US, timeout_us);
            next_diag_us = wki_future_deadline_us(NOW_US, WKI_WAIT_DIAG_INTERVAL_US);
        }
        auto* waiter_task = entry->task.load(std::memory_order_acquire);
        if (waiter_task != nullptr) {
            waiter_task->set_wait_channel("wki_wait");
            if (waiter_task->type == mod::sched::task::TaskType::DAEMON && !is_timer_deferred_waiter(waiter_task)) {
                mod::sched::kern_block();
            } else {
                if (is_timer_deferred_waiter(waiter_task)) {
                    wki_spin_yield();
                }
                mod::sched::kern_yield();
            }
        } else {
            mod::sched::kern_yield();
        }
        if (wait_done(entry)) {
            break;
        }
    }

    // Completed - unlink and return result
    if (linked) {
        wait_list_unlink(entry);
    }
    if (next_diag_us != 0 && wki_now_us() >= next_diag_us) {
        log_wait_diag("done", entry, STARTED_US, wki_now_us(), timeout_us);
    }
    return entry->result;
}

void wki_wake_op(WkiWaitEntry* entry, int result) {
    if (!wki_claim_op(entry)) {
        return;  // already completed - stale wake
    }
    wki_finish_claimed_op(entry, result);
}

auto wki_claim_op(WkiWaitEntry* entry) -> bool {
    if (entry == nullptr) {
        return false;
    }
    return claim_wait_entry(entry);
}

void wki_finish_claimed_op(WkiWaitEntry* entry, int result) {
    if (entry == nullptr) {
        return;
    }
    auto* waiter_task = entry->task.load(std::memory_order_acquire);
    bool const RETAINED_WAITER_TASK = waiter_task != nullptr && waiter_task->try_acquire();
    // DONE is the completion claimant's final access to the stack-backed wait
    // entry. Subsystems with an external retirement owner separately gate
    // stack reclamation with retirement_pending.
    publish_claimed_wait_entry(entry, result);
    // Wake the waiter whether it has already moved to WAITING or is still at
    // the current-task voluntary block point.
    if (RETAINED_WAITER_TASK) {
        mod::sched::kern_wake(waiter_task);
        waiter_task->release();
    }
}

void wki_wait_timeout_scan(uint64_t now_us) {
    // Called from timer tick context - scan for expired waiters.
    // We unlink completed/timed-out entries directly (we already hold the lock)
    // so that stale entries cannot linger if the owning task is killed or its
    // stack is reclaimed before the polling loop runs wait_list_unlink().
    s_wait_lock.lock();
    WkiWaitEntry* cur = s_wait_head;
    while (cur != nullptr) {
        // guard against corrupted lists
        if (reinterpret_cast<uintptr_t>(cur) % alignof(WkiWaitEntry) != 0) {
            ker::mod::dbg::log("[WKI] wait_timeout_scan: misaligned entry %p, truncating list", cur);
            s_wait_head = nullptr;
            break;
        }
        WkiWaitEntry* next = cur->next;

        bool should_unlink = false;

        // Check for timeout
        if (cur->deadline_us != 0 && now_us >= cur->deadline_us) {
            if (claim_wait_entry(cur)) {
                wki_finish_claimed_op(cur, WKI_ERR_TIMEOUT);
                should_unlink = true;
            }
        }

        // Also reap entries that were already completed (e.g. by wki_wake_op
        // or peer cleanup) but whose owning task died before it could unlink.
        if (!should_unlink && wait_done(cur)) {
            should_unlink = true;
        }

        if (should_unlink) {
            if (cur->prev != nullptr) {
                cur->prev->next = cur->next;
            } else if (s_wait_head == cur) {
                s_wait_head = cur->next;
            }
            if (cur->next != nullptr) {
                cur->next->prev = cur->prev;
            }
            cur->next = nullptr;
            cur->prev = nullptr;
        }

        cur = next;
    }
    s_wait_lock.unlock();
}

void wki_wait_cleanup_for_task(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    while (true) {
        WkiWaitEntry* claimed_waiter = nullptr;
        s_wait_lock.lock();
        WkiWaitEntry* cur = s_wait_head;
        while (cur != nullptr) {
            WkiWaitEntry* next = cur->next;
            if (cur->task.load(std::memory_order_acquire) == task) {
                // Unlink this entry - the task is dying and won't clean up itself.
                if (cur->prev != nullptr) {
                    cur->prev->next = cur->next;
                } else if (s_wait_head == cur) {
                    s_wait_head = cur->next;
                }
                if (cur->next != nullptr) {
                    cur->next->prev = cur->prev;
                }
                cur->next = nullptr;
                cur->prev = nullptr;
                cur->task.store(nullptr, std::memory_order_release);

                uint8_t expected = WkiWaitEntry::PENDING;
                if (cur->state.compare_exchange_strong(expected, WkiWaitEntry::CLAIMED, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
                    publish_claimed_wait_entry(cur, WKI_ERR_PEER_FENCED);
                } else if (expected == WkiWaitEntry::CLAIMED) {
                    // The claimant may still be publishing through this task's
                    // stack. Quiesce it before exit can reclaim that stack.
                    claimed_waiter = cur;
                    break;
                }
            }
            cur = next;
        }
        s_wait_lock.unlock();

        if (claimed_waiter == nullptr) {
            break;
        }
        // kern_yield() re-enters the kernel-thread shutdown exit hook. Keep
        // this lock-free wait preemptible and let the claim owner publish DONE.
        while (!wait_done(claimed_waiter)) {
            asm volatile("pause" ::: "memory");
        }
    }

    wki_remote_vfs_cleanup_for_task(task->pid);
}

#ifdef WOS_SELFTEST
void wki_selftest_wait_list_link(WkiWaitEntry* entry) { wait_list_link(entry); }

auto wki_selftest_wait_list_contains(WkiWaitEntry const* entry) -> bool {
    bool found = false;
    s_wait_lock.lock();
    for (WkiWaitEntry const* cur = s_wait_head; cur != nullptr; cur = cur->next) {
        if (cur == entry) {
            found = true;
            break;
        }
    }
    s_wait_lock.unlock();
    return found;
}
#endif

namespace {

[[nodiscard]] auto local_hostname_data() -> char* {
    return g_wki.local_hostname.data();  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
}

// -----------------------------------------------------------------------------
// V2: Hostname resolution
// Priority: 1) /etc/hostname  2) fallback "node-{id:04x}"
// Note: kernel cmdline parsing for wki.hostname= deferred until cmdline API exists
// -----------------------------------------------------------------------------

void resolve_local_hostname() {
    // Use the kernel-wide hostname cache (populated from /etc/hostname at boot)
    const char* cached = ker::util::hostname::get();
    size_t len = strlen(cached);
    if (len >= WKI_HOSTNAME_MAX) {
        len = WKI_HOSTNAME_MAX - 1;
    }
    memcpy(local_hostname_data(), cached, len + 1);
}

auto wki_nonzero_epoch_from_seed(uint64_t seed) -> uint32_t {
    seed ^= seed >> 30U;
    seed *= 0xbf58476d1ce4e5b9ULL;
    seed ^= seed >> 27U;
    seed *= 0x94d049bb133111ebULL;
    seed ^= seed >> 31U;
    auto epoch = static_cast<uint32_t>(seed ^ (seed >> 32U));
    if (epoch == 0) {
        epoch = 1;
    }
    return epoch;
}

}  // namespace

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_init() {
    if (g_wki.initialized) {
        return;
    }

    // Generate a random-ish node ID from lower bits of timestamp
    // Collision handled during HELLO handshake
    uint64_t const SEED = ker::mod::time::get_ticks();
    auto id = static_cast<uint16_t>(SEED ^ (SEED >> 16) ^ (SEED >> 32));
    if (id == WKI_NODE_INVALID || id == WKI_NODE_BROADCAST) {
        id = 0x0001;
    }
    g_wki.my_node_id = id;
    g_wki.local_boot_epoch = wki_nonzero_epoch_from_seed(SEED ^ (static_cast<uint64_t>(id) << 32U));

    // V2: Resolve local hostname
    resolve_local_hostname();

    // Init peer table
    g_wki.peer_count = 0;
    for (auto& peer : g_wki.peers) {
        peer.node_id = WKI_NODE_INVALID;
        peer.state = PeerState::UNKNOWN;
    }

    g_wki.transports = nullptr;
    g_wki.transport_count = 0;
    g_wki.my_lsa_seq = 0;
    g_wki.capabilities = 0;
    g_wki.initialized = true;

    // Init routing subsystem (LSDB, routing table)
    wki_routing_init();

    // Init zone subsystem (shared memory zones)
    wki_zone_init();

    // Init RoCE RDMA transport (L2, MAC-based GIDs)
    wki_roce_transport_init();

    // Init remotable subsystem (device remoting)
    wki_remotable_init();

    // Init device server subsystem (owner-side remote device operations)
    wki_dev_server_init();

    // Init device proxy subsystem (consumer-side remote device operations)
    wki_dev_proxy_init();

    // Init event bus subsystem (pub/sub events)
    wki_event_init();

    // Init IRQ forwarding subsystem
    wki_irq_fwd_init();

    // Init remote VFS subsystem
    wki_remote_vfs_init();

    // Init remote NIC subsystem
    wki_remote_net_init();

    // Init remote compute subsystem
    wki_remote_compute_init();

    // Init IPC proxy subsystem (cross-node pipe, socket, epoll forwarding)
    wki_ipc_subsystem_init();

    // V2: Create /wki/ base directory for remote VFS mounts
    ker::vfs::vfs_mkdir("/wki", 0755);

    // Create /wki/<local_hostname> -> / so that local paths are accessible
    // via the same /wki/<hostname>/... scheme used for every remote peer.
    // Any system can then refer to /wki/<hostname>/some/path and have it
    // resolve correctly regardless of whether the node is local or remote.
    // FIXME:does not seem to link VFS properly inside of subdirs that are on different mounts need to update symlink handling to re-resolve
    // after crossing mount point
    {
        std::array<char, WKI_HOSTNAME_MAX + 6> self_link{};  // "/wki/" + hostname + NUL
        snprintf(self_link.data(), self_link.size(), "/wki/%s", local_hostname_data());
        ker::vfs::vfs_symlink("/", self_link.data());
    }

    ker::vfs::vfs_wki_load_default_rules();

    // V2: Create /dev/nodes/ hierarchy and register self
    ker::vfs::devfs::devfs_nodes_init();
    ker::vfs::devfs::devfs_nodes_add_peer(local_hostname_data(), g_wki.my_node_id);

    ker::mod::dbg::log("[WKI] Initialized, node_id=0x%04x hostname='%s'", g_wki.my_node_id, local_hostname_data());
}

void wki_shutdown() {
    if (!g_wki.initialized) {
        return;
    }

    ker::mod::dbg::log("[WKI] Shutting down...");

    std::array<uint16_t, WKI_MAX_PEERS> connected_peers{};
    size_t connected_count = 0;
    for (auto& peer : g_wki.peers) {
        if (peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED) {
            connected_peers.at(connected_count++) = peer.node_id;
            if (connected_count >= connected_peers.size()) {
                break;
            }
        }
    }

    PeerGoodbyePayload goodbye = {};
    goodbye.leaving_node = g_wki.my_node_id;
    goodbye.reason = WKI_GOODBYE_REASON_SHUTDOWN;

    for (size_t i = 0; i < connected_count; ++i) {
        (void)wki_send_raw(connected_peers.at(i), MsgType::PEER_GOODBYE, &goodbye, sizeof(goodbye), WKI_FLAG_PRIORITY);
    }

    // Gracefully detach connected peers locally. This reuses the normal cleanup
    // cascade without claiming that healthy peers failed.
    for (size_t i = 0; i < connected_count; ++i) {
        WkiPeer* peer = wki_peer_find(connected_peers.at(i));
        if (peer != nullptr && peer->state == PeerState::CONNECTED) {
            wki_peer_graceful_leave(peer);
        }
    }

    // Unregister all transports (stop receiving new frames)
    g_wki.transport_lock.lock();
    WkiTransport* t = g_wki.transports;
    while (t != nullptr) {
        WkiTransport* next = t->next;
        t->set_rx_handler(t, nullptr);
        t = next;
    }
    g_wki.transports = nullptr;
    g_wki.transport_count = 0;
    g_wki.transport_lock.unlock();

    g_wki.initialized = false;
    ker::mod::dbg::log("[WKI] Shutdown complete");
}

// -----------------------------------------------------------------------------
// Transport registry
// -----------------------------------------------------------------------------

void wki_transport_register(WkiTransport* transport) {
    g_wki.transport_lock.lock();

    transport->next = g_wki.transports;
    g_wki.transports = transport;
    g_wki.transport_count++;

    g_wki.transport_lock.unlock();

    // Set ourselves as the RX handler
    transport->set_rx_handler(transport, [](WkiTransport* t, const void* data, uint16_t len) { wki_rx(t, data, len); });

    ker::mod::dbg::log("[WKI] Transport registered: %s (mtu=%u, rdma=%d)", transport->name, transport->mtu, transport->rdma_capable);
}

void wki_transport_unregister(WkiTransport* transport) {
    g_wki.transport_lock.lock();

    WkiTransport** pp = &g_wki.transports;
    while (*pp != nullptr) {
        if (*pp == transport) {
            *pp = transport->next;
            g_wki.transport_count--;
            break;
        }
        pp = &(*pp)->next;
    }

    g_wki.transport_lock.unlock();

    ker::mod::dbg::log("[WKI] Transport unregistered: %s", transport->name);
}

#ifdef WOS_SELFTEST
auto wki_selftest_reliable_rx_peer_state_accepts(PeerState state) -> bool { return reliable_rx_peer_state_accepts(state); }
#endif

// -----------------------------------------------------------------------------
// Peer management - compact hash with multiplicative hashing + linear probing
// -----------------------------------------------------------------------------

namespace {

inline auto peer_hash_slot(uint16_t node_id) -> uint8_t { return static_cast<uint8_t>((static_cast<uint32_t>(node_id) * 0x9E37U) >> 8); }

void peer_hash_insert(uint16_t node_id, int16_t peer_idx) {
    uint8_t const SLOT = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        auto& entry = g_wki.peer_hash.at((SLOT + probe) & (WKI_PEER_HASH_SIZE - 1));
        if (entry.node_id == WKI_NODE_INVALID) {
            entry.node_id = node_id;
            entry.peer_idx = peer_idx;
            return;
        }
    }
}

[[maybe_unused]] void peer_hash_remove(uint16_t node_id) {
    uint8_t const SLOT = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        size_t const IDX = (SLOT + probe) & (WKI_PEER_HASH_SIZE - 1);
        auto& entry = g_wki.peer_hash.at(IDX);
        if (entry.node_id == node_id) {
            entry.node_id = WKI_NODE_INVALID;
            entry.peer_idx = -1;
            // Re-insert any entries that were displaced past this slot
            size_t next = (IDX + 1) & (WKI_PEER_HASH_SIZE - 1);
            while (g_wki.peer_hash.at(next).node_id != WKI_NODE_INVALID) {
                auto displaced = g_wki.peer_hash.at(next);
                g_wki.peer_hash.at(next).node_id = WKI_NODE_INVALID;
                g_wki.peer_hash.at(next).peer_idx = -1;
                peer_hash_insert(displaced.node_id, displaced.peer_idx);
                next = (next + 1) & (WKI_PEER_HASH_SIZE - 1);
            }
            return;
        }
        if (entry.node_id == WKI_NODE_INVALID) {
            return;  // not found
        }
    }
}

}  // namespace

auto wki_peer_find(uint16_t node_id) -> WkiPeer* {
    uint8_t const SLOT = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        auto& entry = g_wki.peer_hash.at((SLOT + probe) & (WKI_PEER_HASH_SIZE - 1));
        if (entry.node_id == node_id) {
            return entry.peer_idx >= 0 ? &g_wki.peers.at(static_cast<size_t>(entry.peer_idx)) : nullptr;
        }
        if (entry.node_id == WKI_NODE_INVALID) {
            return nullptr;
        }
    }
    return nullptr;
}

auto wki_peer_list_by_zone(uint8_t zone_id) -> auto {
    return std::ranges::views::all(g_wki.peers) | std::ranges::views::filter([zone_id](const WkiPeer& peer) {
               return peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED && peer.rdma_zone_id == zone_id;
           });
}

auto wki_peer_alloc(uint16_t node_id) -> WkiPeer* {
    // Check if already exists
    WkiPeer* existing = wki_peer_find(node_id);
    if (existing != nullptr) {
        return existing;
    }

    // Find an empty slot
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        auto& peer = g_wki.peers.at(i);
        if (peer.node_id == WKI_NODE_INVALID) {
            new (&peer) WkiPeer();  // placement new to reset
            peer.node_id = node_id;
            g_wki.peer_count++;
            peer_hash_insert(node_id, static_cast<int16_t>(i));
            return &peer;
        }
    }
    return nullptr;  // peer table full
}

auto wki_peer_count() -> uint16_t { return g_wki.peer_count; }

// -----------------------------------------------------------------------------
// Channel management - per-peer O(1) lookup via peer->channels[channel_id]
// -----------------------------------------------------------------------------

// Channels are allocated from a contiguous flat pool (cache-friendly) but
// indexed via per-peer pointer arrays for O(1) lookup without global locks.

constexpr size_t CHANNEL_POOL_SIZE = 1024;
namespace {
std::array<WkiChannel, CHANNEL_POOL_SIZE> s_channel_pool;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_channel_pool_init = false;                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_channel_pool_lock;               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
// Highest channel-pool index ever published, plus one. It is monotonic so a
// timer snapshot remains safe across channel close and slot reuse.
std::atomic<size_t> s_channel_pool_high_water{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void channel_pool_init() {
    if (s_channel_pool_init) {
        return;
    }
    for (auto& channel : s_channel_pool) {
        channel.active = false;
    }
    s_channel_pool_init = true;
}

auto channel_pool_timer_slots() -> std::span<WkiChannel> {
    size_t const LIMIT = std::min(s_channel_pool_high_water.load(std::memory_order_acquire), s_channel_pool.size());
    return {s_channel_pool.data(), LIMIT};
}

// Free a retransmit entry - checks whether it's the inline pre-allocated entry
// (part of the channel struct) or a heap-allocated fallback, and acts accordingly.
void rt_entry_free(WkiChannel* ch, WkiRetransmitEntry* rt) {
    if (rt == &ch->tx_rt_entry) {
        // Inline entry: just mark as available, don't free memory
        ch->tx_rt_entry_in_use = false;
    } else {
        // Heap-allocated fallback
        delete[] rt->data;
        delete rt;
    }
}

// Initialize a freshly allocated channel with default values
void channel_init(WkiChannel* ch, uint16_t peer_node, uint16_t chan_id, PriorityClass prio, uint16_t credits) {
    ch->channel_id = chan_id;
    ch->peer_node_id = peer_node;
    ch->priority = prio;
    ch->generation++;
    if (ch->generation == 0) {
        ch->generation = 1;
    }
    ch->active = true;
    ch->tx_seq = 0;
    ch->tx_ack = 0;
    ch->rx_seq = 0;
    ch->rx_baseline_initialized = false;
    ch->rx_dispatch_seq = 0;
    ch->rx_dispatch_waiters.fill(nullptr);
    ch->rx_ack_pending = WKI_ACK_NONE;
    ch->ack_pending = false;
    ch->ack_pending_since_us = 0;
    ch->tx_credits = credits;
    ch->rx_credits = credits;
    ch->retransmit_head = nullptr;
    ch->retransmit_tail = nullptr;
    ch->retransmit_count = 0;
    ch->reorder_head = nullptr;
    ch->reorder_count = 0;
    ch->dup_ack_count = 0;
    ch->rto_us = WKI_INITIAL_RTO_US;
    ch->srtt_us = 0;
    ch->rttvar_us = 0;
    ch->bytes_sent = 0;
    ch->bytes_received = 0;
    ch->retransmits = 0;
    ch->perf_last_stall_report_us = 0;
    ch->perf_last_stall_status = 0;
    ch->tx_rt_entry_in_use = false;
}

using DispatchWaiterList = std::array<ker::mod::sched::task::Task*, WKI_RX_DISPATCH_WAITER_SLOTS>;

auto record_reliable_dispatch_waiter_locked(WkiChannel* ch, ker::mod::sched::task::Task* task) -> bool {
    if (ch == nullptr || task == nullptr) {
        return false;
    }

    for (auto* waiter : ch->rx_dispatch_waiters) {
        if (waiter == task) {
            return true;
        }
    }

    for (auto& waiter : ch->rx_dispatch_waiters) {
        if (waiter == nullptr) {
            waiter = task;
            return true;
        }
    }
    return false;
}

void drain_reliable_dispatch_waiters_locked(WkiChannel* ch, DispatchWaiterList& waiters) {
    if (ch == nullptr) {
        return;
    }

    waiters = ch->rx_dispatch_waiters;
    ch->rx_dispatch_waiters.fill(nullptr);
}

void wake_reliable_dispatch_waiters(const DispatchWaiterList& waiters) {
    for (auto* waiter : waiters) {
        if (waiter != nullptr) {
            ker::mod::sched::kern_wake(waiter);
        }
    }
}

// Allocate a pool slot and register in peer->channels[].
// Caller must hold s_channel_pool_lock.
auto channel_pool_alloc(WkiPeer* peer, uint16_t peer_node, uint16_t chan_id, PriorityClass prio, uint16_t credits) -> WkiChannel* {
    for (size_t channel_index = 0; channel_index < s_channel_pool.size(); ++channel_index) {
        auto& pool_entry = s_channel_pool.at(channel_index);
        if (!pool_entry.active) {
            WkiChannel* ch = &pool_entry;
            ch->lock.lock();
            channel_init(ch, peer_node, chan_id, prio, credits);
            size_t const ALLOCATED_LIMIT = channel_index + 1;
            if (ALLOCATED_LIMIT > s_channel_pool_high_water.load(std::memory_order_relaxed)) {
                s_channel_pool_high_water.store(ALLOCATED_LIMIT, std::memory_order_release);
            }
            peer->channels.at(chan_id) = ch;
            auto& scan_limit =
                chan_id < WKI_CHAN_DYNAMIC_RESERVED_BASE ? peer->ordinary_channel_scan_limit : peer->reserved_channel_scan_limit;
            auto const PUBLISHED_LIMIT = static_cast<uint16_t>(chan_id + 1U);
            scan_limit.store(std::max(scan_limit.load(std::memory_order_relaxed), PUBLISHED_LIMIT), std::memory_order_release);
            ch->lock.unlock();
            return ch;
        }
    }
    return nullptr;
}

auto default_credits_for_channel(uint16_t channel_id) -> uint16_t {
    switch (channel_id) {
        case WKI_CHAN_CONTROL:
            return WKI_CREDITS_CONTROL;
        case WKI_CHAN_ZONE_MGMT:
            return WKI_CREDITS_ZONE_MGMT;
        case WKI_CHAN_EVENT_BUS:
            return WKI_CREDITS_EVENT_BUS;
        case WKI_CHAN_RESOURCE:
            return WKI_CREDITS_RESOURCE;
        case WKI_CHAN_IPC_DATA:
            return WKI_CREDITS_IPC_DATA;
        default:
            return WKI_CREDITS_DYNAMIC;
    }
}

auto reorder_limit_for_channel(uint16_t channel_id) -> uint16_t {
    switch (channel_id) {
        case WKI_CHAN_CONTROL:
            return WKI_REORDER_CONTROL;
        case WKI_CHAN_ZONE_MGMT:
            return WKI_REORDER_ZONE_MGMT;
        case WKI_CHAN_EVENT_BUS:
            return WKI_REORDER_EVENT_BUS;
        case WKI_CHAN_RESOURCE:
            return WKI_REORDER_RESOURCE;
        case WKI_CHAN_IPC_DATA:
            return WKI_REORDER_IPC_DATA;
        default:
            return WKI_REORDER_DYNAMIC;
    }
}

auto default_priority_for_channel(uint16_t channel_id) -> PriorityClass {
    // IPC_DATA carries jumbo pipe/socket payloads. Keep it on the throughput
    // ACK policy so bulk receive streams coalesce ACKs instead of emitting an
    // immediate pure ACK for every data frame.
    return (channel_id <= WKI_CHAN_RESOURCE) ? PriorityClass::LATENCY : PriorityClass::THROUGHPUT;
}

auto channel_allows_initial_resync(uint16_t channel_id) -> bool { return channel_id != WKI_CHAN_IPC_DATA; }

auto channel_requires_existing_rx_reservation(uint16_t channel_id) -> bool {
    return channel_id >= WKI_CHAN_DYNAMIC_BASE && channel_id < WKI_CHAN_DYNAMIC_RESERVED_BASE;
}

auto wki_channel_get_for_reliable_rx(uint16_t peer_node, uint16_t channel_id) -> WkiChannel* {
    if (channel_requires_existing_rx_reservation(channel_id)) {
        return wki_channel_lookup(peer_node, channel_id);
    }
    return wki_channel_get(peer_node, channel_id);
}

void refresh_rx_credits(WkiChannel* ch) {
    uint16_t const MAX_CREDITS = default_credits_for_channel(ch->channel_id);
    uint32_t const USED = std::min<uint32_t>(ch->reorder_count, MAX_CREDITS);
    ch->rx_credits = static_cast<uint16_t>(MAX_CREDITS - USED);
}

auto drop_stale_reorder_entries_locked(WkiChannel* ch) -> bool {
    bool dropped = false;
    while (ch->reorder_head != nullptr && seq_before(ch->reorder_head->seq, ch->rx_seq)) {
        WkiReorderEntry* stale = ch->reorder_head;
        ch->reorder_head = stale->next;
        if (ch->reorder_count != 0) {
            ch->reorder_count--;
        }
        delete[] stale->data;
        delete stale;
        dropped = true;
    }

    if (dropped) {
        refresh_rx_credits(ch);
    }
    return dropped;
}

void refresh_tx_credits_from_advertisement_locked(WkiChannel* ch, uint8_t advertised_credits) {
    uint16_t const MAX_CREDITS = default_credits_for_channel(ch->channel_id);
    uint32_t const ADVERTISED = std::min<uint32_t>(advertised_credits, MAX_CREDITS);
    uint32_t const IN_FLIGHT = seq_after(ch->tx_seq, ch->tx_ack) ? ch->tx_seq - ch->tx_ack : 0;
    ch->tx_credits = static_cast<uint16_t>(ADVERTISED > IN_FLIGHT ? ADVERTISED - IN_FLIGHT : 0);
}

struct AckSnapshot {
    WkiHeader hdr = {};
    WkiChannel* channel = nullptr;
    uint16_t peer = WKI_NODE_INVALID;
    uint16_t channel_id = 0;
    uint32_t generation = 0;
    uint32_t ack_num = 0;
};

struct RetransmitSnapshot {
    uint8_t* data = nullptr;
    uint16_t len = 0;
    uint16_t peer = WKI_NODE_INVALID;
    uint32_t seq = 0;
    uint32_t generation = 0;
};

auto retransmit_snapshot_present(const RetransmitSnapshot& snapshot) -> bool {
    return snapshot.data != nullptr && snapshot.len != 0 && snapshot.peer != WKI_NODE_INVALID;
}

void free_retransmit_snapshot(RetransmitSnapshot& snapshot) {
    delete[] snapshot.data;
    snapshot = {};
}

auto retransmit_head_matches_locked(WkiChannel* ch, uint64_t now_us, bool require_deadline, bool has_expected_seq, uint32_t expected_seq,
                                    uint32_t expected_generation, uint16_t expected_len) -> bool {
    if (ch == nullptr || !ch->active || ch->generation != expected_generation || ch->retransmit_head == nullptr) {
        return false;
    }
    WkiRetransmitEntry const* rt = ch->retransmit_head;
    if (rt->len != expected_len || (has_expected_seq && rt->seq != expected_seq)) {
        return false;
    }
    return !require_deadline || now_us >= ch->retransmit_deadline;
}

void postpone_retransmit_after_snapshot_alloc_failure(WkiChannel* ch, uint64_t now_us, bool require_deadline, bool has_expected_seq,
                                                      uint32_t expected_seq, uint32_t expected_generation, uint16_t expected_len) {
    if (ch == nullptr || !require_deadline) {
        return;
    }
    ch->lock.lock();
    if (retransmit_head_matches_locked(ch, now_us, require_deadline, has_expected_seq, expected_seq, expected_generation, expected_len)) {
        ch->retransmit_deadline = wki_future_deadline_us(now_us, ch->rto_us);
    }
    ch->lock.unlock();
}

auto capture_retransmit_head_snapshot(WkiChannel* ch, uint64_t now_us, bool require_deadline, bool has_expected_seq, uint32_t expected_seq,
                                      RetransmitSnapshot& snapshot) -> bool {
    snapshot = {};
    if (ch == nullptr) {
        return false;
    }

    uint16_t len = 0;
    uint16_t peer = WKI_NODE_INVALID;
    uint32_t seq = 0;
    uint32_t generation = 0;
    ch->lock.lock();
    if (ch->active && ch->retransmit_head != nullptr && (!require_deadline || now_us >= ch->retransmit_deadline)) {
        WkiRetransmitEntry const* rt = ch->retransmit_head;
        if (!has_expected_seq || rt->seq == expected_seq) {
            len = rt->len;
            peer = ch->peer_node_id;
            seq = rt->seq;
            generation = ch->generation;
        }
    }
    ch->lock.unlock();

    if (len == 0 || len > WKI_MAX_FRAME_SIZE || peer == WKI_NODE_INVALID) {
        return false;
    }

    auto* data = new (std::nothrow) uint8_t[len];
    if (data == nullptr) {
        postpone_retransmit_after_snapshot_alloc_failure(ch, now_us, require_deadline, has_expected_seq, expected_seq, generation, len);
        return false;
    }

    ch->lock.lock();
    if (!retransmit_head_matches_locked(ch, now_us, require_deadline, has_expected_seq, expected_seq, generation, len)) {
        ch->lock.unlock();
        delete[] data;
        return false;
    }

    std::memcpy(data, ch->retransmit_head->data, len);
    snapshot.data = data;
    snapshot.len = len;
    snapshot.peer = peer;
    snapshot.seq = seq;
    snapshot.generation = generation;
    ch->lock.unlock();
    return true;
}

auto capture_ack_snapshot_locked(WkiChannel* ch) -> AckSnapshot {
    AckSnapshot ack = {};
    ack.hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
    ack.hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);
    ack.hdr.src_node = g_wki.my_node_id;
    ack.hdr.dst_node = ch->peer_node_id;
    ack.hdr.channel_id = ch->channel_id;
    ack.hdr.seq_num = 0;
    ack.hdr.ack_num = ch->rx_ack_pending;
    ack.hdr.payload_len = 0;
    ack.hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
    ack.hdr.hop_ttl = WKI_DEFAULT_TTL;
    ack.hdr.checksum = 0;
    ack.hdr.reserved = 0;
    ack.channel = ch;
    ack.peer = ch->peer_node_id;
    ack.channel_id = ch->channel_id;
    ack.generation = ch->generation;
    ack.ack_num = ch->rx_ack_pending;
    return ack;
}

auto ack_snapshot_present(const AckSnapshot& ack) -> bool { return ack.peer != WKI_NODE_INVALID && ack.channel != nullptr; }

auto capture_pending_peer_ack_for_tx_locked(WkiPeer* peer, WkiChannel* tx_ch) -> AckSnapshot {
    if (peer == nullptr || tx_ch == nullptr) {
        return {};
    }

    auto capture_in_range = [peer, tx_ch](uint16_t first, uint16_t limit) -> AckSnapshot {
        for (uint16_t channel_id = first; channel_id < limit; ++channel_id) {
            WkiChannel* candidate = peer->channels.at(channel_id);
            if (candidate == nullptr || candidate == tx_ch) {
                continue;
            }
            if (!candidate->lock.try_lock()) {
                continue;
            }

            AckSnapshot ack = {};
            if (candidate->active && candidate->peer_node_id == peer->node_id && candidate->ack_pending) {
                ack = capture_ack_snapshot_locked(candidate);
            }

            candidate->lock.unlock();
            if (ack_snapshot_present(ack)) {
                return ack;
            }
        }
        return {};
    };

    uint16_t const ORDINARY_LIMIT = peer->ordinary_channel_scan_limit.load(std::memory_order_acquire);
    if (AckSnapshot ack = capture_in_range(0, ORDINARY_LIMIT); ack_snapshot_present(ack)) {
        return ack;
    }

    uint16_t const RESERVED_LIMIT = peer->reserved_channel_scan_limit.load(std::memory_order_acquire);
    return capture_in_range(WKI_CHAN_DYNAMIC_RESERVED_BASE, RESERVED_LIMIT);
}

auto transmit_ack_snapshot(const AckSnapshot& ack) -> int {
    if (ack.peer == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    WkiTransport* transport = find_transport_for_peer(ack.peer);
    if (transport == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t const NEXT_HOP = resolve_next_hop(ack.peer);
    if (NEXT_HOP == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    int const RET = transport->tx(transport, NEXT_HOP, &ack.hdr, WKI_HEADER_SIZE);
    if (RET >= 0) {
        mark_peer_tx_progress(ack.peer);
    }
    return RET;
}

void complete_ack_transmit_locked(WkiChannel* ch, uint32_t ack_num, int tx_ret, bool& notify_timer) {
    if (ch == nullptr || !ch->active || !ch->ack_pending || ch->rx_ack_pending != ack_num) {
        return;
    }

    uint64_t const NOW_US = wki_now_us();
    if (tx_ret >= 0) {
        if (ch->reorder_count == 0) {
            ch->ack_pending = false;
        } else {
            // A reorder gap means the peer may be waiting for our latest
            // cumulative ACK to advance its retransmit head. Pure ACKs are not
            // themselves reliable, so keep repeating the ACK until the buffered
            // future packets drain.
            ch->ack_pending = true;
            ch->ack_pending_since_us = NOW_US;
            notify_timer = true;
        }
    } else {
        ch->ack_pending_since_us = NOW_US;
        notify_timer = true;
    }
}

void complete_ack_transmit_for_generation_locked(WkiChannel* ch, uint32_t generation, uint32_t ack_num, int tx_ret, bool& notify_timer) {
    if (ch == nullptr || ch->generation != generation) {
        return;
    }
    complete_ack_transmit_locked(ch, ack_num, tx_ret, notify_timer);
}

void apply_ack_transmit_result(WkiChannel* ch, const AckSnapshot& ack, int tx_ret, bool& notify_timer) {
    if (ch == nullptr) {
        return;
    }
    ch->lock.lock();
    if (ch->peer_node_id == ack.peer && ch->channel_id == ack.channel_id && ch->generation == ack.generation) {
        complete_ack_transmit_locked(ch, ack.ack_num, tx_ret, notify_timer);
    }
    ch->lock.unlock();
}

}  // namespace

auto wki_has_fast_timer_work() -> bool { return wki_next_fast_timer_deadline_us(wki_now_us()) != UINT64_MAX; }

auto wki_next_fast_timer_deadline_us(uint64_t now_us) -> uint64_t {
    uint64_t next_deadline = UINT64_MAX;

    s_channel_pool_lock.lock();
    for (const auto& channel : channel_pool_timer_slots()) {
        WkiChannel const* ch = &channel;
        if (!ch->active) {
            continue;
        }
        if (ch->ack_pending) {
            uint64_t const FIRE_AT = wki_future_deadline_us(ch->ack_pending_since_us, ack_delay_for_channel(ch->channel_id));
            next_deadline = std::min(next_deadline, wki_next_or_immediate_deadline_us(FIRE_AT, now_us));
        }
        if (ch->retransmit_head != nullptr && ch->retransmit_deadline != 0) {
            next_deadline = std::min(next_deadline, ch->retransmit_deadline);
        }
    }
    s_channel_pool_lock.unlock();

    s_wait_lock.lock();
    for (WkiWaitEntry const* cur = s_wait_head; cur != nullptr; cur = cur->next) {
        if (cur->deadline_us != 0 && wait_state(cur) == WkiWaitEntry::PENDING) {
            next_deadline = std::min(next_deadline, cur->deadline_us);
        }
    }
    s_wait_lock.unlock();

    return next_deadline;
}

auto wki_channel_lookup(uint16_t peer_node, uint16_t channel_id) -> WkiChannel* {
    channel_pool_init();

    if (channel_id >= WKI_MAX_CHANNELS) {
        return nullptr;
    }

    WkiPeer* peer = wki_peer_find(peer_node);
    return wki_channel_lookup_in_peer(peer, peer_node, channel_id);
}

auto wki_channel_get(uint16_t peer_node, uint16_t channel_id) -> WkiChannel* {
    channel_pool_init();

    if (channel_id >= WKI_MAX_CHANNELS) {
        return nullptr;
    }

    // Fast path: O(1) lookup via peer->channels[]
    WkiPeer* peer = wki_peer_find(peer_node);
    if (WkiChannel* ch = wki_channel_lookup_in_peer(peer, peer_node, channel_id); ch != nullptr) {
        return ch;
    }

    // Slow path: allocate a new channel from the global pool
    s_channel_pool_lock.lock();

    // Re-check under lock (another CPU may have allocated while we were unlocked)
    if (WkiChannel* ch = wki_channel_lookup_in_peer(peer, peer_node, channel_id); ch != nullptr) {
        s_channel_pool_lock.unlock();
        return ch;
    }

    // Need peer for per-peer index; if peer doesn't exist yet, fall back
    if (peer == nullptr) {
        s_channel_pool_lock.unlock();
        return nullptr;
    }

    uint16_t const CREDITS = default_credits_for_channel(channel_id);
    PriorityClass const PRIO = default_priority_for_channel(channel_id);

    WkiChannel* ch = channel_pool_alloc(peer, peer_node, channel_id, PRIO, CREDITS);
    s_channel_pool_lock.unlock();
    return ch;
}

auto wki_channel_alloc(uint16_t peer_node, PriorityClass priority) -> WkiChannel* {
    channel_pool_init();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        return nullptr;
    }

    s_channel_pool_lock.lock();

    // Find the first free dynamic channel ID for this peer via per-peer array.
    uint16_t next_id = WKI_MAX_CHANNELS;
    for (uint16_t i = WKI_CHAN_DYNAMIC_BASE; i < WKI_CHAN_DYNAMIC_RESERVED_BASE; i++) {
        WkiChannel const* existing = peer->channels.at(i);
        if (existing == nullptr || !existing->active) {
            if (existing != nullptr && !existing->active) {
                peer->channels.at(i) = nullptr;
            }
            next_id = i;
            break;
        }
    }

    if (next_id >= WKI_MAX_CHANNELS) {
        s_channel_pool_lock.unlock();
        return nullptr;
    }

    WkiChannel* ch = channel_pool_alloc(peer, peer_node, next_id, priority, WKI_CREDITS_DYNAMIC);
    s_channel_pool_lock.unlock();
    return ch;
}

auto wki_channel_reserve(uint16_t peer_node, uint16_t channel_id, PriorityClass priority) -> WkiChannel* {
    channel_pool_init();

    if (channel_id < WKI_CHAN_DYNAMIC_BASE || channel_id >= WKI_CHAN_DYNAMIC_RESERVED_BASE) {
        return nullptr;
    }

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        return nullptr;
    }

    s_channel_pool_lock.lock();

    WkiChannel const* existing = peer->channels.at(channel_id);
    if (existing != nullptr) {
        if (existing->active) {
            s_channel_pool_lock.unlock();
            return nullptr;
        }
        peer->channels.at(channel_id) = nullptr;
    }

    WkiChannel* ch = channel_pool_alloc(peer, peer_node, channel_id, priority, WKI_CREDITS_DYNAMIC);
    s_channel_pool_lock.unlock();
    return ch;
}

auto wki_channel_close_generation(WkiChannel* ch, uint16_t expected_peer, uint16_t expected_channel_id, uint32_t expected_generation)
    -> bool {
    if (ch == nullptr || expected_generation == 0) {
        return false;
    }

    s_channel_pool_lock.lock();
    ch->lock.lock();

    if (!ch->active || ch->peer_node_id != expected_peer || ch->channel_id != expected_channel_id ||
        ch->generation != expected_generation) {
        ch->lock.unlock();
        s_channel_pool_lock.unlock();
        return false;
    }

    DispatchWaiterList waiters{};
    uint16_t const PEER_NODE = ch->peer_node_id;
    uint16_t const CHANNEL_ID = ch->channel_id;
    ch->active = false;
    drain_reliable_dispatch_waiters_locked(ch, waiters);
    wki_channel_reset(ch);

    // Clear per-peer index entry
    WkiPeer* peer = wki_peer_find(PEER_NODE);
    if (peer != nullptr && CHANNEL_ID < WKI_MAX_CHANNELS && peer->channels.at(CHANNEL_ID) == ch) {
        peer->channels.at(CHANNEL_ID) = nullptr;
    }

    ch->lock.unlock();
    wake_reliable_dispatch_waiters(waiters);
    s_channel_pool_lock.unlock();
    return true;
}

void wki_channel_close(WkiChannel* ch) {
    if (ch == nullptr) {
        return;
    }

    s_channel_pool_lock.lock();
    ch->lock.lock();
    if (!ch->active) {
        ch->lock.unlock();
        s_channel_pool_lock.unlock();
        return;
    }

    DispatchWaiterList waiters{};
    uint16_t const PEER_NODE = ch->peer_node_id;
    uint16_t const CHANNEL_ID = ch->channel_id;
    ch->active = false;
    drain_reliable_dispatch_waiters_locked(ch, waiters);
    wki_channel_reset(ch);

    WkiPeer* peer = wki_peer_find(PEER_NODE);
    if (peer != nullptr && CHANNEL_ID < WKI_MAX_CHANNELS && peer->channels.at(CHANNEL_ID) == ch) {
        peer->channels.at(CHANNEL_ID) = nullptr;
    }

    ch->lock.unlock();
    wake_reliable_dispatch_waiters(waiters);
    s_channel_pool_lock.unlock();
}

void wki_channels_close_for_peer(uint16_t node_id) {
    channel_pool_init();

    WkiPeer* peer = wki_peer_find(node_id);

    s_channel_pool_lock.lock();
    for (auto& pool_entry : s_channel_pool) {
        if (pool_entry.active && pool_entry.peer_node_id == node_id) {
            WkiChannel* ch = &pool_entry;
            DispatchWaiterList waiters{};
            ch->lock.lock();
            ch->active = false;
            drain_reliable_dispatch_waiters_locked(ch, waiters);
            wki_channel_reset(ch);
            ch->lock.unlock();
            wake_reliable_dispatch_waiters(waiters);
        }
    }
    if (peer != nullptr) {
        // Clear all per-peer channel pointers while the pool lock still
        // prevents a concurrent allocator from publishing a new entry that
        // this teardown would immediately erase.
        peer->channels.fill(nullptr);
    }
    s_channel_pool_lock.unlock();
}

auto wki_channel_diag_snapshot(WkiChannelDiag* out, size_t max) -> size_t {
    channel_pool_init();
    if (out == nullptr || max == 0) {
        return 0;
    }

    size_t count = 0;
    s_channel_pool_lock.lock();
    for (auto& pool_entry : s_channel_pool) {
        if (!pool_entry.active || count >= max) {
            continue;
        }

        WkiChannel* ch = &pool_entry;
        auto& row = out[count];
        ch->lock.lock();
        if (!ch->active) {
            ch->lock.unlock();
            continue;
        }
        row.peer_node = ch->peer_node_id;
        row.channel_id = ch->channel_id;
        row.priority = static_cast<uint8_t>(ch->priority);
        row.active = ch->active;
        row.tx_seq = ch->tx_seq;
        row.tx_ack = ch->tx_ack;
        row.rx_seq = ch->rx_seq;
        row.rx_ack_pending = ch->rx_ack_pending;
        row.ack_pending = ch->ack_pending;
        row.tx_credits = ch->tx_credits;
        row.rx_credits = ch->rx_credits;
        row.retransmit_count = ch->retransmit_count;
        row.reorder_count = ch->reorder_count;
        row.retransmits = ch->retransmits;
        row.bytes_sent = ch->bytes_sent;
        row.bytes_received = ch->bytes_received;
        ch->lock.unlock();

        WkiPeer const* peer = wki_peer_find(row.peer_node);
        if (peer != nullptr) {
            row.peer_state = peer->state;
            row.peer_direct = peer->is_direct;
            row.next_hop = peer->next_hop;
            row.hop_count = peer->hop_count;
        }
        ++count;
    }
    s_channel_pool_lock.unlock();
    return count;
}

// -----------------------------------------------------------------------------
// Sending - raw (unreliable, for HELLO/HEARTBEAT)
// -----------------------------------------------------------------------------
namespace {

auto find_transport_for_peer(uint16_t dst_node) -> WkiTransport* {
    // For direct peers, use their stored transport
    WkiPeer const* peer = wki_peer_find(dst_node);
    if ((peer != nullptr) && (peer->transport != nullptr)) {
        return peer->transport;
    }

    // For routed peers, find the next hop's transport via peer table
    if ((peer != nullptr) && peer->next_hop != WKI_NODE_INVALID) {
        WkiPeer const* hop = wki_peer_find(peer->next_hop);
        if ((hop != nullptr) && (hop->transport != nullptr)) {
            return hop->transport;
        }
    }

    // Routing table lookup for multi-hop destinations
    RoutingEntry route = {};
    if (wki_routing_lookup(dst_node, &route) && route.valid && route.next_hop != WKI_NODE_INVALID) {
        WkiPeer const* hop = wki_peer_find(route.next_hop);
        if ((hop != nullptr) && (hop->transport != nullptr)) {
            return hop->transport;
        }
    }

    // For broadcast (HELLO), use the first transport
    if (dst_node == WKI_NODE_BROADCAST) {
        return g_wki.transports;
    }

    return nullptr;
}

auto resolve_next_hop(uint16_t dst_node) -> uint16_t {
    if (dst_node == WKI_NODE_BROADCAST) {
        return dst_node;
    }

    // Direct peer check
    WkiPeer const* peer = wki_peer_find(dst_node);
    if ((peer != nullptr) && peer->is_direct) {
        return dst_node;
    }

    // Routing table (populated by Dijkstra SPF)
    RoutingEntry route = {};
    if (wki_routing_lookup(dst_node, &route) && route.valid) {
        return route.next_hop;
    }

    // Peer table fallback (next_hop set during routing recompute)
    if ((peer != nullptr) && peer->next_hop != WKI_NODE_INVALID) {
        return peer->next_hop;
    }

    return WKI_NODE_INVALID;
}

}  // namespace

auto wki_send_raw(uint16_t dst_node, MsgType msg_type, const void* payload, uint16_t payload_len, uint8_t flags) -> int {
    if (!g_wki.initialized) {
        return WKI_ERR_INVALID;
    }

    WkiTransport* transport = find_transport_for_peer(dst_node);
    if (transport == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t const NEXT_HOP = resolve_next_hop(dst_node);
    if (NEXT_HOP == WKI_NODE_INVALID && dst_node != WKI_NODE_BROADCAST) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t const FRAME_LEN = WKI_HEADER_SIZE + payload_len;
    constexpr size_t INLINE_RAW_FRAME_SIZE = WKI_HEADER_SIZE + sizeof(HelloPayload);
    std::array<uint8_t, INLINE_RAW_FRAME_SIZE> inline_frame{};
    uint8_t* heap_frame = nullptr;
    uint8_t* frame = inline_frame.data();
    if (FRAME_LEN > inline_frame.size()) {
        heap_frame = new (std::nothrow) uint8_t[FRAME_LEN];
        frame = heap_frame;
    }
    if (frame == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    auto* hdr = reinterpret_cast<WkiHeader*>(frame);
    hdr->version_flags = wki_version_flags(WKI_VERSION, flags);
    hdr->msg_type = static_cast<uint8_t>(msg_type);
    hdr->src_node = g_wki.my_node_id;
    hdr->dst_node = dst_node;
    hdr->channel_id = WKI_CHAN_CONTROL;
    hdr->seq_num = 0;
    hdr->ack_num = 0;
    hdr->payload_len = payload_len;
    hdr->credits = 0;
    hdr->hop_ttl = WKI_DEFAULT_TTL;
    hdr->src_port = 0;
    hdr->dst_port = 0;
    hdr->checksum = 0;
    hdr->reserved = 0;

    if ((payload != nullptr) && payload_len > 0) {
        memcpy(frame + WKI_HEADER_SIZE, payload, payload_len);
    }

    // CRC32: skip for direct single-hop peers (Ethernet FCS provides integrity)
    WkiPeer const* peer = wki_peer_find(dst_node);
    if (peer != nullptr && peer->is_direct) {
        hdr->checksum = 0;
    } else {
        hdr->checksum = wki_frame_checksum(*hdr, frame + WKI_HEADER_SIZE);
    }

    int ret = 0;

    if (dst_node == WKI_NODE_BROADCAST) {
        // Broadcast on ALL registered transports so that peers reachable on
        // different L2 networks (e.g. data bridge vs WKI bridge) all receive
        // the HELLO / broadcast message.
        ret = WKI_ERR_NO_ROUTE;
        g_wki.transport_lock.lock();
        WkiTransport* t = g_wki.transports;
        while (t != nullptr) {
            int const R = t->tx(t, WKI_NODE_BROADCAST, frame, FRAME_LEN);
            if (R >= 0) {
                ret = WKI_OK;
            }
            t = t->next;
        }
        g_wki.transport_lock.unlock();
    } else {
        ret = transport->tx(transport, NEXT_HOP, frame, FRAME_LEN);
        if (ret < 0) {
            ret = WKI_ERR_TX_FAILED;
        } else {
            ret = WKI_OK;
        }
    }

    if (ret == WKI_OK && dst_node != WKI_NODE_BROADCAST) {
        mark_peer_tx_progress(dst_node);
    }

    delete[] heap_frame;

    return ret;
}

// -----------------------------------------------------------------------------
// Sending - reliable (via channel)
// -----------------------------------------------------------------------------

namespace {

auto wki_send_impl(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len,
                   WkiChannel* expected_channel, uint32_t expected_generation) -> int {
    if (!g_wki.initialized) {
        return WKI_ERR_INVALID;
    }

    WkiPeer* peer = wki_peer_find(dst_node);
    if (peer == nullptr) {
        return WKI_ERR_NOT_FOUND;
    }
    if (peer->state == PeerState::FENCED) {
        return WKI_ERR_PEER_FENCED;
    }

    WkiChannel* ch = expected_channel != nullptr ? wki_channel_lookup(dst_node, channel_id) : wki_channel_get(dst_node, channel_id);
    if (ch == nullptr) {
        return expected_channel != nullptr ? WKI_ERR_NOT_FOUND : WKI_ERR_NO_MEM;
    }
    if (expected_channel != nullptr && ch != expected_channel) {
        return WKI_ERR_NOT_FOUND;
    }

    WkiTransport* transport = find_transport_for_peer(dst_node);
    if (transport == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t const NEXT_HOP = resolve_next_hop(dst_node);
    if (NEXT_HOP == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    // Zero-copy TX: allocate PacketBuffer up front and build the WKI frame
    // directly in pkt->data, avoiding the extra memcpy in eth_wki_tx().
    // Use pkt_alloc_tx() to preserve an RX reserve and avoid TX exhausting
    // the global pool, which can starve ACK/heartbeat receive progress.
    uint16_t const FRAME_LEN = WKI_HEADER_SIZE + payload_len;
    net::PacketBuffer* pkt = net::pkt_alloc_tx();
    if (pkt == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    // Build frame directly in packet buffer data region
    uint8_t* frame = pkt->data;

    WkiRetransmitEntry* heap_rt_entry = nullptr;
    uint8_t* heap_rt_data = nullptr;
    bool heap_rt_prepare_attempted = false;
    uint32_t heap_rt_channel_generation = 0;
    bool use_inline_retransmit = false;
    uint64_t const TRACE_CALLSITE = WOS_PERF_CALLSITE();

    ch->lock.lock();
    for (;;) {
        bool const CHANNEL_REUSED = heap_rt_prepare_attempted && ch->generation != heap_rt_channel_generation;
        bool const EXPECTED_CHANNEL_RETIRED =
            expected_channel != nullptr && (ch != expected_channel || ch->generation != expected_generation);
        if (!ch->active || ch->peer_node_id != dst_node || ch->channel_id != channel_id || CHANNEL_REUSED || EXPECTED_CHANNEL_RETIRED) {
            ch->lock.unlock();
            delete[] heap_rt_data;
            delete heap_rt_entry;
            net::pkt_free(pkt);
            return WKI_ERR_NOT_FOUND;
        }

        // Check credits before allocating fallback storage. Another sender may
        // consume the final credit while this thread temporarily drops the lock,
        // so revalidate on every pass through the two-phase reservation.
        if (ch->tx_credits == 0) {
            uint32_t const TRACE_CORRELATION = ch->tx_seq;
            ch->lock.unlock();
            delete[] heap_rt_data;
            delete heap_rt_entry;
            net::pkt_free(pkt);
            perf_record_transport_point(mod::perf::WkiPerfTransportOp::NO_CREDITS, dst_node, channel_id, WKI_ERR_NO_CREDITS, 0,
                                        TRACE_CORRELATION, TRACE_CALLSITE);
            return WKI_ERR_NO_CREDITS;
        }

        bool const INLINE_RETRANSMIT_AVAILABLE = wki_channel_has_inline_retransmit_storage(ch, FRAME_LEN);
        if (heap_rt_entry != nullptr || INLINE_RETRANSMIT_AVAILABLE) {
            use_inline_retransmit = heap_rt_entry == nullptr && INLINE_RETRANSMIT_AVAILABLE;
            break;
        }
        if (heap_rt_prepare_attempted) {
            break;
        }

        // Heap allocation cannot run while holding a channel spinlock. Drop the
        // lock only for the uncommon large/multiple-in-flight frame, then retry
        // channel generation, identity, credit, and inline-slot checks after
        // allocation so a recycled pool slot cannot receive this frame.
        heap_rt_channel_generation = ch->generation;
        ch->lock.unlock();
        heap_rt_prepare_attempted = true;
        heap_rt_entry = new (std::nothrow) WkiRetransmitEntry{};
        if (heap_rt_entry != nullptr) {
            heap_rt_data = new (std::nothrow) uint8_t[FRAME_LEN];
            if (heap_rt_data == nullptr) {
                delete heap_rt_entry;
                heap_rt_entry = nullptr;
            }
        }
        ch->lock.lock();
    }

    uint32_t const TRACE_CORRELATION = ch->tx_seq;

    perf_record_transport_begin(dst_node, channel_id, TRACE_CORRELATION, payload_len, TRACE_CALLSITE);

    // Give pending ACK state a chance to ride on this reliable frame. Prefer
    // the carrier channel's cumulative ACK; otherwise borrow one pending ACK
    // from another channel to the same peer without blocking on its lock.
    AckSnapshot piggyback_ack = {};
    if (ch->ack_pending || ch->rx_seq != 0) {
        piggyback_ack = capture_ack_snapshot_locked(ch);
    } else {
        piggyback_ack = capture_pending_peer_ack_for_tx_locked(peer, ch);
    }

    uint8_t flags = 0;
    if (ch->priority == PriorityClass::LATENCY) {
        flags |= WKI_FLAG_PRIORITY;
    }
    if (ack_snapshot_present(piggyback_ack)) {
        flags |= WKI_FLAG_ACK_PRESENT;
        if (piggyback_ack.channel_id != channel_id) {
            flags |= WKI_FLAG_ACK_CHANNEL;
        }
    }

    auto* hdr = reinterpret_cast<WkiHeader*>(frame);
    hdr->version_flags = wki_version_flags(WKI_VERSION, flags);
    hdr->msg_type = static_cast<uint8_t>(msg_type);
    hdr->src_node = g_wki.my_node_id;
    hdr->dst_node = dst_node;
    hdr->channel_id = channel_id;
    hdr->seq_num = ch->tx_seq;
    hdr->ack_num = ack_snapshot_present(piggyback_ack) ? piggyback_ack.ack_num : 0;
    hdr->payload_len = payload_len;
    auto const LOCAL_RX_CREDITS = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
    hdr->credits = ack_snapshot_present(piggyback_ack) ? piggyback_ack.hdr.credits : LOCAL_RX_CREDITS;
    hdr->hop_ttl = WKI_DEFAULT_TTL;
    hdr->src_port = 0;
    hdr->dst_port = 0;
    hdr->checksum = 0;
    hdr->reserved = ((flags & WKI_FLAG_ACK_CHANNEL) != 0) ? wki_ack_reserved(piggyback_ack.channel_id) : 0;

    if ((payload != nullptr) && payload_len > 0) {
        memcpy(frame + WKI_HEADER_SIZE, payload, payload_len);
    }

    pkt->len = FRAME_LEN;

    // CRC32: skip for direct single-hop peers (Ethernet FCS provides integrity)
    if (peer->is_direct) {
        hdr->checksum = 0;  // RX path skips validation when checksum == 0
    } else {
        hdr->checksum = wki_frame_checksum(*hdr, frame + WKI_HEADER_SIZE);
    }

    // Queue for retransmit - use inline entry if available, else allocate from heap
    WkiRetransmitEntry* rt_entry = nullptr;
    uint8_t* rt_data = nullptr;

    if (heap_rt_entry != nullptr) {
        // The slow path already paid for fallback storage. Keep the inline slot
        // free for another small frame that may arrive before this one is ACKed.
        rt_entry = heap_rt_entry;
        rt_data = heap_rt_data;
        heap_rt_entry = nullptr;
        heap_rt_data = nullptr;
    } else if (use_inline_retransmit) {
        // Fast path: use pre-allocated inline buffers (fits small frames)
        rt_entry = &ch->tx_rt_entry;
        rt_data = ch->tx_rt_buf.data();
        ch->tx_rt_entry_in_use = true;
    } else {
        // Allocation failed while the lock was dropped, and the inline slot did
        // not become available before the channel was revalidated.
        rt_entry = nullptr;
        rt_data = nullptr;
    }

    bool notify_timer = false;
    if (rt_entry != nullptr) {
        memcpy(rt_data, frame, FRAME_LEN);
        rt_entry->data = rt_data;
        rt_entry->len = FRAME_LEN;
        rt_entry->seq = ch->tx_seq;
        rt_entry->send_time_us = 0;
        rt_entry->retries = 0;
        rt_entry->next = nullptr;
    } else {
        // Reliable sequence numbers are only safe to publish if the frame is
        // recoverable. Sending without a retransmit copy can permanently wedge
        // the receiver behind a missing head-of-line packet.
        ch->lock.unlock();
        delete[] heap_rt_data;
        delete heap_rt_entry;
        net::pkt_free(pkt);
        perf_record_transport_end(dst_node, channel_id, TRACE_CORRELATION, WKI_ERR_NO_MEM, payload_len, TRACE_CALLSITE);
        return WKI_ERR_NO_MEM;
    }

#ifdef DEBUG_WKI_TRANSPORT
    // Debug: trace DEV_ATTACH messages with their sequence numbers
    if (msg_type == MsgType::DEV_ATTACH_REQ || msg_type == MsgType::DEV_ATTACH_ACK) {
        ker::mod::dbg::log("[WKI-DBG] TX type=%u dst=0x%04x ch=%u seq=%u ack_present=%u ack_num=%u", static_cast<uint8_t>(msg_type),
                           dst_node, channel_id, ch->tx_seq, (flags & WKI_FLAG_ACK_PRESENT) ? 1 : 0, hdr->ack_num);
    }
#endif

    // Zero-copy transmit: pkt already has the WKI frame at pkt->data,
    // transport just prepends link header and sends.  Transport takes
    // ownership of pkt (frees on error).
    int const RET = transport->tx_pkt(transport, NEXT_HOP, pkt);
    bool const INITIAL_TX_FAILED = RET < 0;

    if (rt_entry != nullptr) {
        uint64_t const SEND_TIME_US = wki_now_us();
        rt_entry->send_time_us = SEND_TIME_US;
        if (INITIAL_TX_FAILED) {
            // The frame did not leave the NIC, but it is recoverable now that
            // the retransmit copy exists. Accept it into the reliable stream
            // and let the timer retry instead of dropping one logical write.
            rt_entry->retries = 1;
        }

        if (ch->retransmit_tail != nullptr) {
            ch->retransmit_tail->next = rt_entry;
        } else {
            ch->retransmit_head = rt_entry;
        }
        ch->retransmit_tail = rt_entry;
        ch->retransmit_count++;

        if (ch->retransmit_count == 1) {
            ch->retransmit_deadline = wki_future_deadline_us(SEND_TIME_US, ch->rto_us);
            notify_timer = true;
        }
    }

    // Publish the logical send once the frame is recoverable from the
    // retransmit queue. The first transport attempt may fail under transient TX
    // pressure, but the retry timer can still deliver this sequence.
    ch->tx_seq++;
    ch->tx_credits--;
    AckSnapshot post_unlock_ack = {};
    if (ack_snapshot_present(piggyback_ack)) {
        if (piggyback_ack.channel == ch) {
            complete_ack_transmit_locked(ch, piggyback_ack.ack_num, WKI_OK, notify_timer);
        } else {
            post_unlock_ack = piggyback_ack;
        }
    }
    ch->bytes_sent += payload_len;

    ch->lock.unlock();
    delete[] heap_rt_data;
    delete heap_rt_entry;

    if (ack_snapshot_present(post_unlock_ack)) {
        bool ack_notify_timer = false;
        apply_ack_transmit_result(post_unlock_ack.channel, post_unlock_ack, WKI_OK, ack_notify_timer);
        notify_timer = notify_timer || ack_notify_timer;
    }

    if (notify_timer) {
        wki_timer_notify();
    }

    if (!INITIAL_TX_FAILED) {
        // Update TX activity timestamp so the heartbeat suppression logic can
        // correctly skip sending a heartbeat when we've recently proved our
        // liveness to this peer via data traffic.
        peer->last_tx_activity = wki_now_us();
    }

    return WKI_OK;
}

}  // namespace

auto wki_send(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len) -> int {
    return wki_send_impl(dst_node, channel_id, msg_type, payload, payload_len, nullptr, 0);
}

auto wki_send_on_channel_generation(uint16_t dst_node, WkiChannel* expected_channel, uint32_t expected_generation, MsgType msg_type,
                                    const void* payload, uint16_t payload_len) -> int {
    if (expected_channel == nullptr || expected_generation == 0) {
        return WKI_ERR_INVALID;
    }
    return wki_send_impl(dst_node, WKI_CHAN_RESOURCE, msg_type, payload, payload_len, expected_channel, expected_generation);
}

// -----------------------------------------------------------------------------
// RX Dispatch
// -----------------------------------------------------------------------------

// Dispatch a reliable (sequenced) message to the appropriate handler.
// Used for both in-order delivery and reorder buffer drain.
namespace {

void wki_dispatch_reliable_msg(WkiChannel* rx_channel, uint32_t rx_channel_generation, MsgType type, const WkiHeader* hdr,
                               const uint8_t* payload, uint16_t payload_len) {
    switch (type) {
        case MsgType::LSA:
            detail::handle_lsa(hdr, payload, payload_len);
            break;
        case MsgType::FENCE_NOTIFY:
            detail::handle_fence_notify(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_CREATE_REQ:
            detail::handle_zone_create_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_CREATE_ACK:
            detail::handle_zone_create_ack(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_DESTROY:
            detail::handle_zone_destroy(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_PRE:
            detail::handle_zone_notify_pre(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_POST:
            detail::handle_zone_notify_post(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_PRE_ACK:
        case MsgType::ZONE_NOTIFY_POST_ACK:
            // ACKs for zone notifications - currently informational only
            break;
        case MsgType::ZONE_READ_REQ:
            detail::handle_zone_read_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_READ_RESP:
            detail::handle_zone_read_resp(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_WRITE_REQ:
            detail::handle_zone_write_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_WRITE_ACK:
            detail::handle_zone_write_ack(hdr, payload, payload_len);
            break;
        case MsgType::RESOURCE_ADVERT:
            detail::handle_resource_advert(hdr, payload, payload_len);
            break;
        case MsgType::RESOURCE_WITHDRAW:
            detail::handle_resource_withdraw(hdr, payload, payload_len);
            break;
        case MsgType::DEV_ATTACH_REQ:
            detail::handle_dev_attach_req(hdr, payload, payload_len);
            break;
        case MsgType::DEV_DETACH:
            detail::handle_dev_detach(hdr, payload, payload_len);
            break;
        case MsgType::DEV_OP_REQ:
            detail::handle_dev_op_req(hdr, payload, payload_len);
            break;
        case MsgType::DEV_ATTACH_ACK:
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] DISPATCH DEV_ATTACH_ACK src=0x%04x ch=%u payload_len=%u", hdr->src_node, hdr->channel_id,
                               payload_len);
#endif
            detail::handle_dev_attach_ack(hdr, payload, payload_len);
            detail::handle_vfs_attach_ack(hdr, payload, payload_len);
            detail::handle_net_attach_ack(hdr, payload, payload_len);
            break;
        case MsgType::DEV_OP_RESP:
            detail::handle_dev_op_resp(hdr, payload, payload_len);
            detail::handle_vfs_op_resp(hdr, payload, payload_len);
            detail::handle_net_op_resp(hdr, payload, payload_len);
            detail::handle_ipc_dev_op_resp(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_SUBSCRIBE:
            detail::handle_event_subscribe(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_UNSUBSCRIBE:
            detail::handle_event_unsubscribe(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_PUBLISH:
            detail::handle_event_publish(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_ACK:
            detail::handle_event_ack(hdr, payload, payload_len);
            break;
        case MsgType::DEV_IRQ_FWD:
            detail::handle_dev_irq_fwd(hdr, payload, payload_len);
            break;
        case MsgType::TASK_SUBMIT:
            detail::handle_task_submit(hdr, payload, payload_len, rx_channel, rx_channel_generation);
            break;
        case MsgType::TASK_ACCEPT:
            detail::handle_task_accept(hdr, payload, payload_len, rx_channel, rx_channel_generation);
            break;
        case MsgType::TASK_REJECT:
            detail::handle_task_reject(hdr, payload, payload_len, rx_channel, rx_channel_generation);
            break;
        case MsgType::TASK_COMPLETE:
            detail::handle_task_complete(hdr, payload, payload_len, rx_channel, rx_channel_generation);
            break;
        case MsgType::TASK_CANCEL:
            detail::handle_task_cancel(hdr, payload, payload_len, rx_channel, rx_channel_generation);
            break;
        case MsgType::LOAD_REPORT:
            detail::handle_load_report(hdr, payload, payload_len);
            break;
        default:
            break;
    }
}

auto reliable_dispatch_needs_serial_order(const WkiChannel* ch) -> bool { return ch != nullptr && ch->channel_id == WKI_CHAN_IPC_DATA; }

auto wait_for_reliable_dispatch_turn(WkiChannel* ch, uint32_t generation, uint32_t seq) -> bool {
    if (!reliable_dispatch_needs_serial_order(ch)) {
        return true;
    }

    auto* current_task = ker::mod::sched::get_current_task();
    for (;;) {
        bool should_block = false;
        ch->lock.lock();
        bool const ACTIVE = ch->active && ch->generation == generation;
        uint32_t const NEXT_DISPATCH = ch->rx_dispatch_seq;
        if (!ACTIVE || seq_before(seq, NEXT_DISPATCH)) {
            ch->lock.unlock();
            return false;
        }
        if (seq == NEXT_DISPATCH) {
            ch->lock.unlock();
            return true;
        }
        should_block = current_task != nullptr && current_task->type == ker::mod::sched::task::TaskType::DAEMON &&
                       record_reliable_dispatch_waiter_locked(ch, current_task);
        ch->lock.unlock();
        if (should_block) {
            ker::mod::sched::kern_block();
        } else {
            ker::mod::sched::kern_yield();
        }
    }
}

void finish_reliable_dispatch_turn(WkiChannel* ch, uint32_t generation, uint32_t seq) {
    if (!reliable_dispatch_needs_serial_order(ch)) {
        return;
    }

    ch->lock.lock();
    DispatchWaiterList waiters{};
    if (ch->active && ch->generation == generation && ch->rx_dispatch_seq == seq) {
        ch->rx_dispatch_seq++;
        drain_reliable_dispatch_waiters_locked(ch, waiters);
    }
    ch->lock.unlock();
    wake_reliable_dispatch_waiters(waiters);
}

void wki_dispatch_reliable_msg_ordered(WkiChannel* ch, uint32_t generation, MsgType type, const WkiHeader* hdr, const uint8_t* payload,
                                       uint16_t payload_len) {
    uint32_t const SEQ = hdr != nullptr ? hdr->seq_num : 0U;
    if (!wait_for_reliable_dispatch_turn(ch, generation, SEQ)) {
        return;
    }

    wki_dispatch_reliable_msg(ch, generation, type, hdr, payload, payload_len);
    finish_reliable_dispatch_turn(ch, generation, SEQ);
}

}  // namespace

void wki_rx(WkiTransport* transport, const void* data, uint16_t len) {
    if (len < WKI_HEADER_SIZE) {
        return;
    }

    const auto* hdr = static_cast<const WkiHeader*>(data);

    // Version check
    if (wki_version(hdr->version_flags) != WKI_VERSION) {
        return;
    }

    uint16_t const PAYLOAD_LEN = hdr->payload_len;

    // Sanity: payload_len must fit in the frame before checksum reads it.
    if (WKI_HEADER_SIZE + PAYLOAD_LEN > len) {
        return;
    }

    // Checksum verification (if non-zero)
    if (hdr->checksum != 0) {
        uint32_t const CRC = wki_frame_checksum(*hdr, static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE);
        if (CRC != hdr->checksum) {
            return;  // corrupted frame
        }
    }

    const auto* payload = static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE;
    auto msg = static_cast<MsgType>(hdr->msg_type);

    // Forwarding: if this packet is not for us, forward it
    if (hdr->dst_node != g_wki.my_node_id && hdr->dst_node != WKI_NODE_BROADCAST) {
        // Decrement TTL
        if (hdr->hop_ttl <= 1) {
            // TTL expired, drop
            return;
        }

        // Find next hop
        uint16_t const NEXT_HOP = resolve_next_hop(hdr->dst_node);
        if (NEXT_HOP == WKI_NODE_INVALID) {
            return;  // no route
        }

        WkiTransport* fwd_transport = find_transport_for_peer(NEXT_HOP);
        if (fwd_transport == nullptr) {
            return;
        }

        // Make a mutable copy to decrement TTL
        auto* fwd_frame = new (std::nothrow) uint8_t[len];
        if (fwd_frame == nullptr) {
            return;
        }
        memcpy(fwd_frame, data, len);

        auto* fwd_hdr = reinterpret_cast<WkiHeader*>(fwd_frame);
        fwd_hdr->hop_ttl--;
        if (fwd_hdr->checksum != 0) {
            fwd_hdr->checksum = wki_frame_checksum(*fwd_hdr, fwd_frame + WKI_HEADER_SIZE);
        }

        fwd_transport->tx(fwd_transport, NEXT_HOP, fwd_frame, len);
        delete[] fwd_frame;
        return;
    }

    // Inline TASK handlers publish scheduler-visible state. Serialize their
    // channel lookup and dispatch with HELLO/fence cleanup; RX cannot wait
    // here, so contention drops the reliable frame for retry. COMPLETE/CANCEL
    // only copy into stable bounded storage here and reacquire this lifecycle
    // gate in their task-context worker.
    PeerLifecycleTryLease compute_lifecycle;
    if (message_uses_compute_lifecycle(msg) && !compute_lifecycle.try_acquire(hdr->src_node)) {
        return;
    }

    bool const RELIABLE_RX_ACCEPTED = reliable_rx_peer_accepts(hdr->src_node);

    // Process piggybacked ACK
    if ((wki_flags(hdr->version_flags) & WKI_FLAG_ACK_PRESENT) != 0 && RELIABLE_RX_ACCEPTED) {
        uint16_t const ACK_CHANNEL_ID = wki_ack_channel_id(*hdr);
        WkiChannel* ch = wki_channel_lookup(hdr->src_node, ACK_CHANNEL_ID);
        if (ch != nullptr) {
            RetransmitSnapshot fast_retransmit = {};
            uint32_t fast_retransmit_expected_seq = 0;
            bool need_fast_retransmit = false;
            bool ack_progress = false;

            ch->lock.lock();

            if (ch->active && ch->peer_node_id == hdr->src_node && ch->channel_id == ACK_CHANNEL_ID) {
                // Advance tx_ack - release ACKed retransmit entries
                uint32_t const ACK_NEXT = hdr->ack_num + 1;
                bool const ACK_IN_SENT_WINDOW = wki_channel_ack_next_within_sent_window(ch, ACK_NEXT);
                if (ACK_IN_SENT_WINDOW && seq_after(ACK_NEXT, ch->tx_ack)) {
                    ack_progress = true;
                    ch->tx_ack = ACK_NEXT;
                    ch->last_dup_ack = hdr->ack_num;
                    ch->dup_ack_count = 0;

                    // Remove ACKed entries from retransmit queue
                    while ((ch->retransmit_head != nullptr) && !seq_after(ch->retransmit_head->seq + 1, ch->tx_ack)) {
                        WkiRetransmitEntry* rt = ch->retransmit_head;
                        ch->retransmit_head = rt->next;
                        if (ch->retransmit_head == nullptr) {
                            ch->retransmit_tail = nullptr;
                        }
                        ch->retransmit_count--;

                        // RTT sample (only from non-retransmitted packets)
                        if (rt->retries == 0 && ch->srtt_us > 0) {
                            uint64_t const NOW = wki_now_us();
                            auto sample = static_cast<uint32_t>(NOW - rt->send_time_us);
                            // Jacobson/Karels
                            int32_t const ERR = static_cast<int32_t>(sample) - static_cast<int32_t>(ch->srtt_us);
                            ch->srtt_us = ch->srtt_us + (ERR / 8);
                            ch->rttvar_us = ch->rttvar_us + (((ERR < 0 ? -ERR : ERR) - static_cast<int32_t>(ch->rttvar_us)) / 4);
                            ch->rto_us = ch->srtt_us + (4 * ch->rttvar_us);
                            ch->rto_us = std::max(ch->rto_us, WKI_MIN_RTO_US);
                            ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
                            perf_record_transport_rtt(ch->peer_node_id, ch->channel_id, rt->seq, sample, rt->retries);
                        } else if (rt->retries == 0) {
                            // First RTT sample
                            uint64_t const NOW = wki_now_us();
                            auto sample = static_cast<uint32_t>(NOW - rt->send_time_us);
                            ch->srtt_us = sample;
                            ch->rttvar_us = ch->srtt_us / 2;
                            ch->rto_us = ch->srtt_us + (4 * ch->rttvar_us);
                            ch->rto_us = std::max(ch->rto_us, WKI_MIN_RTO_US);
                            ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
                            perf_record_transport_rtt(ch->peer_node_id, ch->channel_id, rt->seq, sample, rt->retries);
                        }

                        rt_entry_free(ch, rt);
                    }
                } else if (ACK_IN_SENT_WINDOW && hdr->ack_num + 1 == ch->tx_ack && ch->retransmit_head != nullptr) {
                    // Duplicate ACK for the current head-of-line gap.
                    // If we see enough of these, immediately retransmit the oldest
                    // unacked frame instead of waiting for the RTO to expire.
                    if (ch->last_dup_ack == hdr->ack_num) {
                        ch->dup_ack_count++;
                    } else {
                        ch->last_dup_ack = hdr->ack_num;
                        ch->dup_ack_count = 1;
                    }

                    if (ch->dup_ack_count >= WKI_FAST_RETRANSMIT_THRESH) {
                        WkiRetransmitEntry const* rt = ch->retransmit_head;
                        fast_retransmit_expected_seq = rt->seq;
                        need_fast_retransmit = true;
                        ch->dup_ack_count = 0;
                    }
                } else if (ACK_IN_SENT_WINDOW) {
                    ch->last_dup_ack = hdr->ack_num;
                    ch->dup_ack_count = 0;
                }

                // The wire field advertises the peer's current receive window.
                // Keep our send allowance to the advertised free slots minus the
                // packets still in flight; otherwise each ACK can reset the window
                // to full and overflow the peer's reorder buffer after one loss.
                if (ACK_IN_SENT_WINDOW) {
                    refresh_tx_credits_from_advertisement_locked(ch, hdr->credits);
                }
            }

            ch->lock.unlock();

            if (ack_progress) {
                // Only count ACK traffic as liveness when it advanced the
                // peer's acknowledgement state. Duplicate ACK-only chatter can
                // otherwise keep a wedged peer falsely "alive".
                mark_peer_rx_progress(hdr->src_node);
            }

            if (need_fast_retransmit) {
                static_cast<void>(
                    capture_retransmit_head_snapshot(ch, wki_now_us(), false, true, fast_retransmit_expected_seq, fast_retransmit));
            }

            if (retransmit_snapshot_present(fast_retransmit)) {
                int tx_ret = -1;
                WkiTransport* transport = find_transport_for_peer(fast_retransmit.peer);
                if (transport != nullptr) {
                    uint16_t const NEXT_HOP = resolve_next_hop(fast_retransmit.peer);
                    if (NEXT_HOP != WKI_NODE_INVALID) {
                        tx_ret = transport->tx(transport, NEXT_HOP, fast_retransmit.data, fast_retransmit.len);
                    }
                }

                ch->lock.lock();
                if (ch->active && ch->generation == fast_retransmit.generation && ch->retransmit_head != nullptr &&
                    ch->retransmit_head->seq == fast_retransmit.seq) {
                    uint64_t const NOW = wki_now_us();
                    if (tx_ret >= 0) {
                        WkiRetransmitEntry* rt = ch->retransmit_head;
                        rt->retries++;
                        rt->send_time_us = NOW;
                        ch->retransmits++;
                        ch->retransmit_deadline = wki_future_deadline_us(NOW, ch->rto_us);
                        perf_record_transport_point(mod::perf::WkiPerfTransportOp::FAST_RETRANSMIT, ch->peer_node_id, ch->channel_id, 0,
                                                    rt->len, rt->seq, 0);
                    } else {
                        ch->retransmit_deadline = wki_future_deadline_us(NOW, ch->rto_us);
                    }
                }
                ch->lock.unlock();
                free_retransmit_snapshot(fast_retransmit);
            }
        }
    }

    // Dispatch by message type
    switch (msg) {
        // Unreliable control messages - handled directly
        case MsgType::HELLO:
            detail::handle_hello(transport, hdr, payload, PAYLOAD_LEN);
            return;
        case MsgType::HELLO_ACK:
            detail::handle_hello_ack(transport, hdr, payload, PAYLOAD_LEN);
            return;
        case MsgType::HEARTBEAT:
            detail::handle_heartbeat(hdr, payload, PAYLOAD_LEN);
            return;
        case MsgType::HEARTBEAT_ACK:
            detail::handle_heartbeat_ack(hdr, payload, PAYLOAD_LEN);
            return;
        case MsgType::PEER_GOODBYE:
            detail::handle_peer_goodbye(hdr, payload, PAYLOAD_LEN);
            return;

        // Reliable control messages - check seq ordering
        case MsgType::LSA:
        case MsgType::LSA_ACK:
        case MsgType::FENCE_NOTIFY:
        case MsgType::RECONCILE_REQ:
        case MsgType::RECONCILE_ACK:
        case MsgType::RESOURCE_ADVERT:
        case MsgType::RESOURCE_WITHDRAW:
        // Zone messages
        case MsgType::ZONE_CREATE_REQ:
        case MsgType::ZONE_CREATE_ACK:
        case MsgType::ZONE_DESTROY:
        case MsgType::ZONE_NOTIFY_PRE:
        case MsgType::ZONE_NOTIFY_POST:
        case MsgType::ZONE_NOTIFY_PRE_ACK:
        case MsgType::ZONE_NOTIFY_POST_ACK:
        case MsgType::ZONE_READ_REQ:
        case MsgType::ZONE_READ_RESP:
        case MsgType::ZONE_WRITE_REQ:
        case MsgType::ZONE_WRITE_ACK:
        // Event messages
        case MsgType::EVENT_SUBSCRIBE:
        case MsgType::EVENT_UNSUBSCRIBE:
        case MsgType::EVENT_PUBLISH:
        case MsgType::EVENT_ACK:
        // Device messages
        case MsgType::DEV_ATTACH_REQ:
        case MsgType::DEV_ATTACH_ACK:
        case MsgType::DEV_DETACH:
        case MsgType::DEV_OP_REQ:
        case MsgType::DEV_OP_RESP:
        case MsgType::DEV_IRQ_FWD:
        case MsgType::CHANNEL_OPEN:
        case MsgType::CHANNEL_OPEN_ACK:
        case MsgType::CHANNEL_CLOSE:
        // Compute messages
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_ACCEPT:
        case MsgType::TASK_REJECT:
        case MsgType::TASK_COMPLETE:
        case MsgType::TASK_CANCEL:
        case MsgType::LOAD_REPORT: {
            if (!RELIABLE_RX_ACCEPTED) {
                return;
            }

            // For reliable messages, handle seq ordering on the channel
            WkiChannel* ch = wki_channel_get_for_reliable_rx(hdr->src_node, hdr->channel_id);
            if (ch == nullptr) {
                return;
            }

            ch->lock.lock();
            if (!ch->active || ch->peer_node_id != hdr->src_node || ch->channel_id != hdr->channel_id) {
                ch->lock.unlock();
                return;
            }

#ifdef DEBUG_WKI_TRANSPORT
            // Debug: trace all ch3 messages and all DEV_ATTACH messages
            if (hdr->channel_id == WKI_CHAN_RESOURCE || hdr->msg_type == static_cast<uint8_t>(MsgType::DEV_ATTACH_ACK) ||
                hdr->msg_type == static_cast<uint8_t>(MsgType::DEV_ATTACH_REQ)) {
                ker::mod::dbg::log("[WKI-DBG] RX type=%u src=0x%04x ch=%u seq=%u rx_seq=%u (delta=%d)", hdr->msg_type, hdr->src_node,
                                   hdr->channel_id, hdr->seq_num, ch->rx_seq,
                                   static_cast<int>(hdr->seq_num) - static_cast<int>(ch->rx_seq));
            }
#endif

            // Reconnect resync: if this channel was just recreated locally (all-zero RX state)
            // but the peer continued its sequence space, adopt the peer's current seq baseline.
            // This avoids getting stuck buffering every frame as permanently out-of-order.
            if (channel_allows_initial_resync(hdr->channel_id) && !ch->rx_baseline_initialized && ch->rx_seq == 0 &&
                ch->bytes_received == 0 && ch->reorder_head == nullptr && hdr->seq_num != 0) {
                ker::mod::dbg::log("[WKI] Resyncing channel seq: src=0x%04x ch=%u local_rx_seq=0 remote_seq=%u", hdr->src_node,
                                   hdr->channel_id, hdr->seq_num);
                ch->rx_seq = hdr->seq_num;
                ch->rx_dispatch_seq = hdr->seq_num;
            }
            // Record observation even when bounded deferred admission below
            // returns RETRY. Otherwise a later seq=1 frame could trigger the
            // reconnect baseline heuristic and skip a refused seq=0 frame.
            ch->rx_baseline_initialized = true;

            if (hdr->seq_num == ch->rx_seq) {
                WkiComputeRxAdmission admission = WkiComputeRxAdmission::INLINE;
                if (ch->channel_id == WKI_CHAN_RESOURCE && message_uses_deferred_compute_rx(msg)) {
                    admission = wki_remote_compute_admit_rx(msg, hdr, payload, PAYLOAD_LEN, ch, ch->generation);
                    if (admission == WkiComputeRxAdmission::RETRY) {
                        ch->lock.unlock();
                        return;
                    }
                }
                // In-order: advance rx_seq, mark ACK pending
                ch->rx_seq++;
                ch->rx_ack_pending = hdr->seq_num;
                if (!ch->ack_pending) {
                    ch->ack_pending_since_us = wki_now_us();
                }
                ch->ack_pending = true;
                ch->bytes_received += PAYLOAD_LEN;
                uint32_t const RX_CHANNEL_GENERATION = ch->generation;

                bool notify_timer = false;
                ch->lock.unlock();

                mark_peer_rx_progress(hdr->src_node);

                if (admission == WkiComputeRxAdmission::DEFERRED) {
                    wki_remote_compute_notify_deferred_rx(hdr->src_node);
                } else if (admission == WkiComputeRxAdmission::INLINE) {
                    // Dispatch to handler via shared helper.
                    wki_dispatch_reliable_msg_ordered(ch, RX_CHANNEL_GENERATION, msg, hdr, payload, PAYLOAD_LEN);
                }

                // Deliver any buffered reorder entries that are now in-order
                ch->lock.lock();
                if (!ch->active || ch->generation != RX_CHANNEL_GENERATION || ch->peer_node_id != hdr->src_node ||
                    ch->channel_id != hdr->channel_id) {
                    ch->lock.unlock();
                    return;
                }
                if (drop_stale_reorder_entries_locked(ch)) {
                    ch->ack_pending = true;
                    ch->ack_pending_since_us = 0;
                }
                while ((ch->reorder_head != nullptr) && ch->reorder_head->seq == ch->rx_seq) {
                    auto const RO_MSG = static_cast<MsgType>(ch->reorder_head->msg_type);
                    // An inline TASK packet may have been buffered by an
                    // earlier RX call whose lifecycle lease ended before this
                    // gap-filling packet arrived. Acquire the dispatch gate
                    // before consuming it. Deferred COMPLETE/CANCEL admission
                    // only needs the channel and fixed queue locks here.
                    if (message_uses_compute_lifecycle(RO_MSG) && !compute_lifecycle.owns(hdr->src_node) &&
                        !compute_lifecycle.try_acquire(hdr->src_node)) {
                        break;
                    }

                    WkiComputeRxAdmission ro_admission = WkiComputeRxAdmission::INLINE;
                    if (ch->channel_id == WKI_CHAN_RESOURCE && message_uses_deferred_compute_rx(RO_MSG)) {
                        ro_admission = wki_remote_compute_admit_rx(RO_MSG, &ch->reorder_head->hdr, ch->reorder_head->data,
                                                                   ch->reorder_head->len, ch, ch->reorder_head->channel_generation);
                        if (ro_admission == WkiComputeRxAdmission::RETRY) {
                            break;
                        }
                    }

                    WkiReorderEntry* ro = ch->reorder_head;
                    ch->reorder_head = ro->next;
                    ch->reorder_count--;
                    refresh_rx_credits(ch);
                    ch->rx_seq++;
                    ch->rx_ack_pending = ro->seq;
                    if (!ch->ack_pending) {
                        ch->ack_pending_since_us = wki_now_us();
                    }
                    ch->ack_pending = true;
                    ch->bytes_received += ro->len;

                    WkiHeader const RO_HDR = ro->hdr;
                    uint8_t* ro_data = ro->data;
                    uint16_t const RO_LEN = ro->len;
                    uint32_t const RO_CHANNEL_GENERATION = ro->channel_generation;
                    ch->lock.unlock();

                    if (ro_admission == WkiComputeRxAdmission::DEFERRED) {
                        wki_remote_compute_notify_deferred_rx(RO_HDR.src_node);
                    } else if (ro_admission == WkiComputeRxAdmission::INLINE) {
                        // Re-dispatch reordered message through the same handler switch.
                        wki_dispatch_reliable_msg_ordered(ch, RO_CHANNEL_GENERATION, RO_MSG, &RO_HDR, ro_data, RO_LEN);
                    }

                    delete[] ro_data;
                    delete ro;
                    ch->lock.lock();
                    if (!ch->active || ch->generation != RX_CHANNEL_GENERATION || ch->peer_node_id != hdr->src_node ||
                        ch->channel_id != hdr->channel_id) {
                        ch->lock.unlock();
                        return;
                    }
                }
                // For latency traffic and IPC pipe data, send a pure ACK after
                // ordered local dispatch if no response or other reliable frame
                // consumed the pending ACK.
                bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);
                WkiHeader imm_ack_hdr = {};
                uint16_t imm_ack_peer = 0;
                uint32_t imm_ack_num = 0;
                uint32_t imm_ack_generation = 0;
                if (IMM_ACK) {
                    imm_ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
                    imm_ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);
                    imm_ack_hdr.src_node = g_wki.my_node_id;
                    imm_ack_hdr.dst_node = ch->peer_node_id;
                    imm_ack_hdr.channel_id = ch->channel_id;
                    imm_ack_hdr.seq_num = 0;
                    imm_ack_hdr.ack_num = ch->rx_ack_pending;
                    imm_ack_hdr.payload_len = 0;
                    imm_ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
                    imm_ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
                    imm_ack_hdr.checksum = 0;
                    imm_ack_hdr.reserved = 0;
                    imm_ack_peer = ch->peer_node_id;
                    imm_ack_num = ch->rx_ack_pending;
                    imm_ack_generation = ch->generation;
                } else {
                    notify_timer = ch->ack_pending;
                }

                ch->lock.unlock();

                // TX outside the lock to avoid deadlock with RX interrupt
                if (IMM_ACK) {
                    int tx_ret = -1;
                    WkiTransport* transport = find_transport_for_peer(imm_ack_peer);
                    if (transport != nullptr) {
                        uint16_t const NEXT_HOP = resolve_next_hop(imm_ack_peer);
                        if (NEXT_HOP != WKI_NODE_INVALID) {
                            tx_ret = transport->tx(transport, NEXT_HOP, &imm_ack_hdr, WKI_HEADER_SIZE);
                        }
                    }

                    ch->lock.lock();
                    complete_ack_transmit_for_generation_locked(ch, imm_ack_generation, imm_ack_num, tx_ret, notify_timer);
                    if (tx_ret >= 0) {
                        mark_peer_tx_progress(imm_ack_peer);
                    }
                    ch->lock.unlock();
                }

                if (notify_timer) {
                    wki_timer_notify();
                }

            } else if (seq_after(hdr->seq_num, ch->rx_seq)) {
                // Out-of-order: buffer in reorder queue, send dup ACK
                AckSnapshot dup_ack = {};
                if (drop_stale_reorder_entries_locked(ch)) {
                    ch->ack_pending = true;
                    ch->ack_pending_since_us = 0;
                }
                bool already_buffered = false;
                for (WkiReorderEntry const* cur = ch->reorder_head; cur != nullptr; cur = cur->next) {
                    if (cur->seq == hdr->seq_num) {
                        already_buffered = true;
                        break;
                    }
                }

                if (!already_buffered) {
                    uint16_t const MAX_REORDER = reorder_limit_for_channel(ch->channel_id);
                    uint32_t const REORDER_DISTANCE = hdr->seq_num - ch->rx_seq;
#ifdef DEBUG_WKI_TRANSPORT
                    log::debug("Out-of-order msg: src=0x%04x ch=%u seq=%u expected=%u type=%u gap=%u buffered=%u limit=%u", hdr->src_node,
                               hdr->channel_id, hdr->seq_num, ch->rx_seq, hdr->msg_type, REORDER_DISTANCE, ch->reorder_count, MAX_REORDER);
#endif

                    if (REORDER_DISTANCE <= MAX_REORDER && ch->reorder_count < MAX_REORDER) {
                        auto* ro = new (std::nothrow) WkiReorderEntry{};
                        if (ro != nullptr) {
                            ro->data = new (std::nothrow) uint8_t[PAYLOAD_LEN];
                            if (ro->data != nullptr) {
                                memcpy(ro->data, payload, PAYLOAD_LEN);
                                ro->hdr = *hdr;
                                ro->len = PAYLOAD_LEN;
                                ro->msg_type = hdr->msg_type;
                                ro->seq = hdr->seq_num;
                                ro->channel_generation = ch->generation;

                                // Insert sorted by seq.
                                WkiReorderEntry** pp = &ch->reorder_head;
                                while ((*pp != nullptr) && seq_before((*pp)->seq, ro->seq)) {
                                    pp = &(*pp)->next;
                                }
                                ro->next = *pp;
                                *pp = ro;
                                ch->reorder_count++;
                                refresh_rx_credits(ch);
                                // A newly buffered future sequence number is
                                // evidence of fresh peer progress. Old
                                // duplicates do not get to refresh liveness.
                                mark_peer_rx_progress(hdr->src_node);
                            } else {
                                delete ro;
                            }
                        }
                    }
                }

                ch->ack_pending = true;
                ch->ack_pending_since_us = 0;  // out-of-order: ACK immediately (no piggyback opportunity)
                dup_ack = capture_ack_snapshot_locked(ch);

                ch->lock.unlock();
                int const ACK_RET = transmit_ack_snapshot(dup_ack);
                if (ACK_RET >= 0) {
                    ch->lock.lock();
                    bool notify_timer = false;
                    complete_ack_transmit_for_generation_locked(ch, dup_ack.generation, dup_ack.ack_num, ACK_RET, notify_timer);
                    ch->lock.unlock();
                    if (notify_timer) {
                        wki_timer_notify();
                    }
                } else {
                    wki_timer_notify();
                }
            } else {
                // Old/duplicate: discard but re-arm ACK so the sender's retransmit
                // queue gets drained.  Without this the sender never receives the
                // ACK and keeps retransmitting the same stale packet.
                AckSnapshot dup_ack = {};
                if (drop_stale_reorder_entries_locked(ch)) {
                    ch->ack_pending = true;
                    ch->ack_pending_since_us = 0;
                }
                ch->ack_pending = true;
                ch->ack_pending_since_us = 0;  // duplicate: ACK immediately
                dup_ack = capture_ack_snapshot_locked(ch);
                ch->lock.unlock();
                int const ACK_RET = transmit_ack_snapshot(dup_ack);
                if (ACK_RET >= 0) {
                    ch->lock.lock();
                    bool notify_timer = false;
                    complete_ack_transmit_for_generation_locked(ch, dup_ack.generation, dup_ack.ack_num, ACK_RET, notify_timer);
                    ch->lock.unlock();
                    if (notify_timer) {
                        wki_timer_notify();
                    }
                } else {
                    wki_timer_notify();
                }
            }
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// Single-channel timer tick (used by wki_spin_yield_channel)
// -----------------------------------------------------------------------------

namespace {

void wki_timer_tick_single(WkiChannel* ch, uint64_t now_us) {
    if (ch == nullptr || !ch->active) {
        return;
    }

    RetransmitSnapshot retransmit = {};
    bool need_retransmit = false;

    WkiHeader ack_hdr = {};
    uint16_t ack_peer = 0;
    uint32_t ack_num = 0;
    uint32_t ack_generation = 0;
    bool do_ack = false;

    ch->lock.lock();
    if (!ch->active) {
        ch->lock.unlock();
        return;
    }
    if (drop_stale_reorder_entries_locked(ch)) {
        ch->ack_pending = true;
        ch->ack_pending_since_us = 0;
    }

    if ((ch->retransmit_head != nullptr) && now_us >= ch->retransmit_deadline) {
        need_retransmit = true;
    }

    if (ch->ack_pending && now_us >= wki_future_deadline_us(ch->ack_pending_since_us, ack_delay_for_channel(ch->channel_id))) {
        ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
        ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);
        ack_hdr.src_node = g_wki.my_node_id;
        ack_hdr.dst_node = ch->peer_node_id;
        ack_hdr.channel_id = ch->channel_id;
        ack_hdr.seq_num = 0;
        ack_hdr.ack_num = ch->rx_ack_pending;
        ack_hdr.payload_len = 0;
        ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
        ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
        ack_hdr.checksum = 0;
        ack_hdr.reserved = 0;
        ack_peer = ch->peer_node_id;
        ack_num = ch->rx_ack_pending;
        ack_generation = ch->generation;
        do_ack = true;
    }

    ch->lock.unlock();

    if (need_retransmit) {
        static_cast<void>(capture_retransmit_head_snapshot(ch, now_us, true, false, 0, retransmit));
    }

    if (retransmit_snapshot_present(retransmit)) {
        int tx_ret = -1;
        WkiTransport* transport = find_transport_for_peer(retransmit.peer);
        if (transport != nullptr) {
            uint16_t const NEXT_HOP = resolve_next_hop(retransmit.peer);
            if (NEXT_HOP != WKI_NODE_INVALID) {
                tx_ret = transport->tx(transport, NEXT_HOP, retransmit.data, retransmit.len);
            }
        }
        ch->lock.lock();
        if (ch->active && ch->generation == retransmit.generation && ch->retransmit_head != nullptr &&
            ch->retransmit_head->seq == retransmit.seq) {
            if (tx_ret >= 0) {
                WkiRetransmitEntry* rt = ch->retransmit_head;
                rt->retries++;
                rt->send_time_us = now_us;
                ch->retransmits++;
                perf_record_transport_point(mod::perf::WkiPerfTransportOp::RETRANSMIT, ch->peer_node_id, ch->channel_id, 0, rt->len,
                                            rt->seq, 0);

                ch->rto_us *= 2;
                ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
            }
            ch->retransmit_deadline = wki_future_deadline_us(now_us, ch->rto_us);
        }
        ch->lock.unlock();
        free_retransmit_snapshot(retransmit);
    }

    if (do_ack) {
        int tx_ret = -1;
        WkiTransport* transport = find_transport_for_peer(ack_peer);
        if (transport != nullptr) {
            uint16_t const NEXT_HOP = resolve_next_hop(ack_peer);
            if (NEXT_HOP != WKI_NODE_INVALID) {
                tx_ret = transport->tx(transport, NEXT_HOP, &ack_hdr, WKI_HEADER_SIZE);
            }
        }
        ch->lock.lock();
        bool notify_timer = false;
        complete_ack_transmit_for_generation_locked(ch, ack_generation, ack_num, tx_ret, notify_timer);
        if (tx_ret >= 0) {
            mark_peer_tx_progress(ack_peer);
        }
        ch->lock.unlock();
        if (notify_timer) {
            wki_timer_notify();
        }
    }
}

}  // namespace

void wki_spin_yield_channel(WkiChannel* ch) {
    net::NetDevice* dev = wki_eth_get_netdev();
    if (dev != nullptr) {
        net::napi_poll_all_pending();
        net::backlog_drain_all_pending_inline();
    }

    wki_timer_tick_single(ch, wki_now_us());
}

// -----------------------------------------------------------------------------
// Timer tick - heartbeats, retransmit, ACK delays
// -----------------------------------------------------------------------------

void wki_timer_tick(uint64_t now_us) {
    if (!g_wki.initialized) {
        return;
    }

    // Check retransmit deadlines on every pool slot that has ever been used.
    for (auto& pool_entry : channel_pool_timer_slots()) {
        WkiChannel* ch = &pool_entry;
        if (!ch->active) {
            continue;
        }

        // Gather pending work under lock, then release before TX to avoid deadlock
        // (RX interrupt could try to lock the same channel)
        RetransmitSnapshot retransmit = {};
        bool need_retransmit = false;

        WkiHeader ack_hdr = {};
        uint16_t ack_peer = 0;
        uint32_t ack_num = 0;
        uint32_t ack_generation = 0;
        bool do_ack = false;

        ch->lock.lock();
        if (!ch->active) {
            ch->lock.unlock();
            continue;
        }
        if (drop_stale_reorder_entries_locked(ch)) {
            ch->ack_pending = true;
            ch->ack_pending_since_us = 0;
        }

        // Retransmit timeout
        if ((ch->retransmit_head != nullptr) && now_us >= ch->retransmit_deadline) {
            need_retransmit = true;
        }

        // Delayed ACK - send standalone ACK if pending.
        // wki_next_fast_timer_deadline_us() schedules the timer after the
        // channel ACK delay, giving outgoing messages time to piggyback first.
        if (ch->ack_pending && now_us >= wki_future_deadline_us(ch->ack_pending_since_us, ack_delay_for_channel(ch->channel_id))) {
            // Build ACK header for TX outside lock
            ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
            ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);  // reuse as ACK carrier
            ack_hdr.src_node = g_wki.my_node_id;
            ack_hdr.dst_node = ch->peer_node_id;
            ack_hdr.channel_id = ch->channel_id;
            ack_hdr.seq_num = 0;
            ack_hdr.ack_num = ch->rx_ack_pending;
            ack_hdr.payload_len = 0;
            ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
            ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
            ack_hdr.checksum = 0;
            ack_hdr.reserved = 0;
            ack_peer = ch->peer_node_id;
            ack_num = ch->rx_ack_pending;
            ack_generation = ch->generation;
            do_ack = true;
        }

        perf_record_transport_stall_locked(ch, now_us);

        ch->lock.unlock();

        // Now TX outside the lock to avoid deadlock with RX interrupt
        if (need_retransmit) {
            static_cast<void>(capture_retransmit_head_snapshot(ch, now_us, true, false, 0, retransmit));
        }

        if (retransmit_snapshot_present(retransmit)) {
            int tx_ret = -1;
            WkiTransport* transport = find_transport_for_peer(retransmit.peer);
            if (transport != nullptr) {
                uint16_t const NEXT_HOP = resolve_next_hop(retransmit.peer);
                if (NEXT_HOP != WKI_NODE_INVALID) {
                    tx_ret = transport->tx(transport, NEXT_HOP, retransmit.data, retransmit.len);
                }
            }

            ch->lock.lock();
            if (ch->active && ch->generation == retransmit.generation && ch->retransmit_head != nullptr &&
                ch->retransmit_head->seq == retransmit.seq) {
                if (tx_ret >= 0) {
                    WkiRetransmitEntry* rt = ch->retransmit_head;
                    rt->retries++;
                    rt->send_time_us = now_us;
                    ch->retransmits++;
                    perf_record_transport_point(mod::perf::WkiPerfTransportOp::RETRANSMIT, ch->peer_node_id, ch->channel_id, 0, rt->len,
                                                rt->seq, 0);

                    ch->rto_us *= 2;
                    ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
                }
                ch->retransmit_deadline = wki_future_deadline_us(now_us, ch->rto_us);
            }
            ch->lock.unlock();
            free_retransmit_snapshot(retransmit);
        }

        if (do_ack) {
            int tx_ret = -1;
            WkiTransport* transport = find_transport_for_peer(ack_peer);
            if (transport != nullptr) {
                uint16_t const NEXT_HOP = resolve_next_hop(ack_peer);
                if (NEXT_HOP != WKI_NODE_INVALID) {
                    tx_ret = transport->tx(transport, NEXT_HOP, &ack_hdr, WKI_HEADER_SIZE);
                }
            }

            ch->lock.lock();
            bool notify_timer = false;
            complete_ack_transmit_for_generation_locked(ch, ack_generation, ack_num, tx_ret, notify_timer);
            if (tx_ret >= 0) {
                mark_peer_tx_progress(ack_peer);
            }
            ch->lock.unlock();
            if (notify_timer) {
                wki_timer_notify();
            }
        }
    }

    // Heartbeat checks are done in peer.cpp's wki_peer_timer_tick().
    // Potentially blocking resource work is handled by wki_deferred_work_thread().
    wki_deferred_work_notify();
}

}  // namespace ker::net::wki
