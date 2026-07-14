#include "remote_net.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/route.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <util/hcf.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

// -------------------------------------------------------------------------------
// Storage
// -------------------------------------------------------------------------------

namespace {

// unique_ptr indirection: ProxyNetState contains Spinlock with deleted move-assignment.
// Published rows remain in storage as raw-NetDevice lifetime tombstones, while
// the pointer-only live index is pruned on every terminal retirement so normal
// lookups do not grow with historical proxy churn.
std::deque<std::unique_ptr<ProxyNetState>> g_net_proxy_storage;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<ProxyNetState*> g_net_proxies;                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_net_initialized = false;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_net_proxy_lock;                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint8_t g_net_attach_next_cookie = 1;                            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_net_stats_poll_us = 0;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyNetState* g_pending_net_detach_head = nullptr;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyNetState* g_pending_net_detach_tail = nullptr;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr uint64_t NET_PROXY_CONTENTION_SLEEP_US = 1000;
constexpr uint64_t NET_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_OP_TIMEOUT_US;
constexpr uint16_t NET_OP_COOKIE_BYTES = sizeof(uint16_t);
constexpr size_t NET_DETACH_RETRY_BATCH = 32;
constexpr size_t NET_DETACH_RETRY_SCAN = NET_DETACH_RETRY_BATCH * 2;

// Caller must hold s_net_proxy_lock.
auto net_detach_pending_for_resource_locked(uint16_t owner_node, uint32_t resource_id) -> bool {
    for (auto* state = g_pending_net_detach_head; state != nullptr; state = state->detach_next) {
        if (state->detach_owner_node == owner_node && state->detach_resource_id == resource_id) {
            return true;
        }
    }
    return false;
}

class NetPeerLifecycleLease {
   public:
    NetPeerLifecycleLease() = default;
    explicit NetPeerLifecycleLease(WkiPeer* peer_ref) : peer(peer_ref) {
        if (!wki_peer_lifecycle_acquire(peer)) {
            peer = nullptr;
        }
    }
    NetPeerLifecycleLease(const NetPeerLifecycleLease&) = delete;
    auto operator=(const NetPeerLifecycleLease&) -> NetPeerLifecycleLease& = delete;
    ~NetPeerLifecycleLease() { wki_peer_lifecycle_release(peer); }

    [[nodiscard]] auto acquired() const -> bool { return peer != nullptr; }
    [[nodiscard]] auto try_acquire(WkiPeer* peer_ref) -> bool {
        if (peer != nullptr || !wki_peer_lifecycle_try_acquire(peer_ref)) {
            return false;
        }
        peer = peer_ref;
        return true;
    }
    void release() {
        wki_peer_lifecycle_release(peer);
        peer = nullptr;
    }

   private:
    WkiPeer* peer = nullptr;
};

auto net_channel_identity_matches(const WkiChannelIdentity& expected, const WkiChannelIdentity& actual) -> bool {
    return expected.channel != nullptr && expected.channel == actual.channel && expected.peer_node_id == actual.peer_node_id &&
           expected.channel_id == actual.channel_id && expected.generation != 0 && expected.generation == actual.generation;
}

auto net_channel_identity_matches_header(const WkiHeader* hdr, const WkiChannelIdentity& identity) -> bool {
    return hdr != nullptr && identity.channel != nullptr && identity.generation != 0 && hdr->src_node == identity.peer_node_id &&
           hdr->channel_id == identity.channel_id;
}

void close_net_channel_identity(const WkiChannelIdentity& identity) {
    static_cast<void>(wki_channel_close_generation(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation));
}

// s_net_proxy_lock must be held by caller
auto find_net_proxy_by_channel(const WkiChannelIdentity& channel_identity) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->active && net_channel_identity_matches(p->channel_identity, channel_identity)) {
            return p;
        }
    }
    return nullptr;
}

// s_net_proxy_lock must be held by caller.  The cookie is part of lookup, not
// merely post-lookup validation: two overlapping attempts for the same
// resource must not let the first row consume and discard the second ACK.
auto find_net_proxy_by_attach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->attach_pending && p->owner_node == owner_node && p->resource_id == resource_id && p->attach_cookie == attach_cookie) {
            return p;
        }
    }
    return nullptr;
}

// s_net_proxy_lock must be held by caller
auto find_net_proxy_by_dev(ker::net::NetDevice* dev) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->active && &p->netdev == dev) {
            return p;
        }
    }
    return nullptr;
}

// s_net_proxy_lock must be held by caller
auto find_net_proxy_by_resource(uint16_t owner_node, uint32_t resource_id) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (!p->retiring.load(std::memory_order_acquire) && p->owner_node == owner_node && p->resource_id == resource_id &&
            (p->active || p->attaching || p->attach_pending.load(std::memory_order_acquire))) {
            return p;
        }
    }
    return nullptr;
}

auto net_proxy_op_slot_busy(ProxyNetState const* state) -> bool { return state->op_pending.load(std::memory_order_acquire); }

auto net_stats_poll_owns_slot(ProxyNetState const* state) -> bool {
    return state->op_wait_entry == nullptr && state->op_expected_id == OP_NET_GET_STATS;
}

void clear_net_op_state_locked(ProxyNetState* state, int status) {
    state->op_status = static_cast<int16_t>(status);
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_deadline_us = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->op_pending.store(false, std::memory_order_release);
}

auto net_stats_poll_expired_locked(ProxyNetState const* state, uint64_t now_us) -> bool {
    return net_proxy_op_slot_busy(state) && net_stats_poll_owns_slot(state) &&
           (state->op_deadline_us == 0 || now_us >= state->op_deadline_us);
}

auto acquire_net_op_slot_locked(ProxyNetState* state, uint64_t start_us) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, NET_PROXY_SLOT_WAIT_TIMEOUT_US);
    while (true) {
        state->lock.lock();
        if (state->retiring.load(std::memory_order_acquire)) {
            state->lock.unlock();
            return WKI_ERR_PEER_FENCED;
        }
        if (net_proxy_op_slot_busy(state) && net_stats_poll_owns_slot(state)) {
            clear_net_op_state_locked(state, WKI_ERR_TIMEOUT);
        }
        if (!net_proxy_op_slot_busy(state)) {
            return WKI_OK;
        }
        state->lock.unlock();
        if (!net_proxy_op_slot_busy(state)) {
            continue;
        }
        if (wki_now_us() >= DEADLINE_US) {
            return WKI_ERR_TIMEOUT;
        }
        ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US);
    }
}

auto net_req_cookie_from_header(const WkiHeader* hdr) -> uint16_t {
    return hdr != nullptr ? static_cast<uint16_t>(hdr->seq_num & UINT16_MAX) : 0;
}

auto allocate_net_op_cookie_locked(ProxyNetState* state) -> uint16_t {
    uint16_t cookie = state->op_next_cookie;
    if (cookie == 0) {
        cookie = 1;
    }
    state->op_next_cookie = static_cast<uint16_t>(cookie + 1U);
    if (state->op_next_cookie == 0) {
        state->op_next_cookie = 1;
    }
    return cookie;
}

// s_net_proxy_lock must be held by caller. NET has no incarnation token on the
// wire, so the active local owner/resource tuple reserves its cookie directly.
auto allocate_net_attach_cookie_locked(uint16_t owner_node, uint32_t resource_id) -> uint8_t {
    for (uint16_t attempt = 0; attempt < UINT8_MAX; ++attempt) {
        uint8_t cookie = g_net_attach_next_cookie;
        if (cookie == 0) {
            cookie = 1;
        }
        g_net_attach_next_cookie = static_cast<uint8_t>(cookie + 1U);
        if (g_net_attach_next_cookie == 0) {
            g_net_attach_next_cookie = 1;
        }

        bool const RESERVED = std::ranges::any_of(g_net_proxies, [=](const ProxyNetState* proxy) {
            return proxy != nullptr && proxy->owner_node == owner_node && proxy->resource_id == resource_id &&
                   proxy->attach_cookie == cookie;
        });
        bool pending_reserved = false;
        for (auto* proxy = g_pending_net_detach_head; proxy != nullptr; proxy = proxy->detach_next) {
            if (proxy->detach_owner_node == owner_node && proxy->detach_resource_id == resource_id &&
                proxy->detach_attach_cookie == cookie) {
                pending_reserved = true;
                break;
            }
        }
        if (!RESERVED && !pending_reserved) {
            return cookie;
        }
    }
    return 0;
}

void write_net_op_cookie(uint8_t* dst, uint16_t cookie) {
    if (dst != nullptr) {
        memcpy(dst, &cookie, sizeof(cookie));
    }
}

auto read_net_op_cookie(const uint8_t* data, uint16_t data_len, uint16_t fallback) -> uint16_t {
    if (data == nullptr || data_len < NET_OP_COOKIE_BYTES) {
        return fallback;
    }
    uint16_t cookie = 0;
    memcpy(&cookie, data, sizeof(cookie));
    return cookie != 0 ? cookie : fallback;
}

void retain_net_proxy(ProxyNetState* state) {
    if (state != nullptr) {
        state->refs.fetch_add(1, std::memory_order_acq_rel);
    }
}

void release_net_proxy(ProxyNetState* state) {
    if (state != nullptr) {
        state->refs.fetch_sub(1, std::memory_order_acq_rel);
    }
}

void release_net_proxy_lifetime(void* ctx) { release_net_proxy(static_cast<ProxyNetState*>(ctx)); }

auto acquire_net_proxy_by_dev(ker::net::NetDevice* dev) -> ProxyNetState* {
    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_dev(dev);
    if (state != nullptr && !state->retiring.load(std::memory_order_acquire)) {
        retain_net_proxy(state);
    } else {
        state = nullptr;
    }
    s_net_proxy_lock.unlock();
    return state;
}

auto acquire_net_proxy_by_channel(const WkiChannelIdentity& channel_identity) -> ProxyNetState* {
    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_channel(channel_identity);
    if (state != nullptr && !state->retiring.load(std::memory_order_acquire)) {
        retain_net_proxy(state);
    } else {
        state = nullptr;
    }
    s_net_proxy_lock.unlock();
    return state;
}

auto acquire_net_proxy_by_attach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> ProxyNetState* {
    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_attach(owner_node, resource_id, attach_cookie);
    if (state != nullptr && !state->retiring.load(std::memory_order_acquire)) {
        retain_net_proxy(state);
    } else {
        state = nullptr;
    }
    s_net_proxy_lock.unlock();
    return state;
}

auto try_retire_net_proxy_locked(ProxyNetState* state) -> bool {
    if (state == nullptr || state->retiring.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    state->active = false;
    state->attaching = false;
    state->cleanup_started = true;
    state->netdev.state = 0;
    return true;
}

void wait_for_net_proxy_refs_to_drain(ProxyNetState* state) {
    while (state != nullptr && state->refs.load(std::memory_order_acquire) != 0) {
        ker::mod::sched::kern_yield();
    }
}

// Caller must hold s_net_proxy_lock.
void erase_unpublished_net_proxy_storage_locked(ProxyNetState* state) {
    if (state == nullptr || state->ever_published || state->detach_pending || !state->cleanup_complete) {
        return;
    }
    for (auto it = g_net_proxy_storage.begin(); it != g_net_proxy_storage.end(); ++it) {
        if (it->get() == state) {
            g_net_proxy_storage.erase(it);
            return;
        }
    }
}

void erase_net_proxy(ProxyNetState* state) {
    if (state == nullptr) {
        return;
    }

    s_net_proxy_lock.lock();
    std::erase(g_net_proxies, state);
    state->cleanup_complete = true;
    erase_unpublished_net_proxy_storage_locked(state);
    s_net_proxy_lock.unlock();
}

void unpublish_proxy_netdev(ProxyNetState* state) {
    if (state == nullptr) {
        return;
    }

    // Route and interface tables retain raw NetDevice pointers. Remove every
    // exact publication before taking the device out of the registry so a
    // replacement proxy cannot be shadowed by stale forwarding/local-address
    // state. Published proxy storage itself remains a lifetime tombstone.
    static_cast<void>(ker::net::route_del_for_dev(&state->netdev));
    ker::net::netdev_unregister(&state->netdev);
    static_cast<void>(ker::net::netif_del_for_dev(&state->netdev));
}

void retire_unregistered_net_proxy(ProxyNetState* state, const WkiChannelIdentity& channel_to_close) {
    if (state == nullptr) {
        close_net_channel_identity(channel_to_close);
        return;
    }

    s_net_proxy_lock.lock();
    bool const RETIRE_OWNER = try_retire_net_proxy_locked(state);
    s_net_proxy_lock.unlock();
    release_net_proxy(state);

    if (!RETIRE_OWNER) {
        // A connected-epoch marker may already own deferred state teardown.
        // The attach path still owns this exact local channel identity, and an
        // exact close is safe even if deferred cleanup captured it too.
        close_net_channel_identity(channel_to_close);
        return;
    }

    state->lock.lock();
    clear_net_op_state_locked(state, -1);
    state->attach_expected_cookie = 0;
    state->attach_pending.store(false, std::memory_order_release);
    state->lock.unlock();

    wait_for_net_proxy_refs_to_drain(state);
    close_net_channel_identity(channel_to_close);
    erase_net_proxy(state);
}

// Caller must hold ProxyNetState::lock.
auto claim_and_clear_waiter_locked(WkiWaitEntry*& waiter_slot) -> WkiWaitEntry* {
    WkiWaitEntry* waiter = waiter_slot;
    waiter_slot = nullptr;
    if (!wki_claim_op(waiter)) {
        return nullptr;
    }
    return waiter;
}

void finish_claimed_waiter(WkiWaitEntry* waiter, int result) {
    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, result);
    }
}

void finish_or_wait_for_cancelled_waiter(WkiWaitEntry& wait, bool claimed, int result) {
    if (claimed) {
        wki_finish_claimed_op(&wait, result);
        return;
    }

    while (wait.state.load(std::memory_order_acquire) != static_cast<uint8_t>(WkiWaitEntry::DONE)) {
        ker::mod::sched::kern_yield();
    }
}

auto prepare_net_op_wait(ProxyNetState* state, uint16_t op_id, WkiWaitEntry& wait, void* resp_buf, uint16_t resp_max, uint64_t timeout_us,
                         uint16_t* cookie_out) -> int {
    int const SLOT_RET = acquire_net_op_slot_locked(state, wki_now_us());
    if (SLOT_RET != WKI_OK) {
        return SLOT_RET;
    }

    uint16_t const COOKIE = allocate_net_op_cookie_locked(state);

    state->op_wait_entry = &wait;
    state->op_expected_id = op_id;
    state->op_expected_seq = COOKIE;
    state->op_deadline_us = timeout_us != 0 ? wki_future_deadline_us(wki_now_us(), timeout_us) : 0;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_max;
    state->op_resp_len = 0;
    state->op_pending.store(true, std::memory_order_release);
    state->lock.unlock();
    if (cookie_out != nullptr) {
        *cookie_out = COOKIE;
    }
    return WKI_OK;
}

void consume_net_op_result(ProxyNetState* state, WkiWaitEntry& wait, int* status_out, uint16_t* resp_len_out) {
    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    int const STATUS = static_cast<int>(state->op_status);
    uint16_t const RESP_LEN = state->op_resp_len;
    clear_net_op_state_locked(state, STATUS);
    state->lock.unlock();

    if (status_out != nullptr) {
        *status_out = STATUS;
    }
    if (resp_len_out != nullptr) {
        *resp_len_out = RESP_LEN;
    }
}

void cancel_op_waiter(ProxyNetState* state, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
        clear_net_op_state_locked(state, result);
    }
    claimed = wki_claim_op(&wait);
    state->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void cancel_attach_waiter(ProxyNetState* state, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    state->lock.lock();
    if (state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
        state->attach_status = static_cast<uint8_t>(DevAttachStatus::BUSY);
        state->attach_expected_cookie = 0;
        state->attach_pending.store(false, std::memory_order_release);
    }
    claimed = wki_claim_op(&wait);
    state->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void clear_attach_waiter_after_wait(ProxyNetState* state, WkiWaitEntry& wait) {
    state->lock.lock();
    if (state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
    }
    state->lock.unlock();
}

auto send_net_detach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie, WkiReliableTxToken* tx_token_out) -> int {
    std::array<uint8_t, wki_dev_detach_payload_size(false)> det_buf{};
    auto* det = reinterpret_cast<DevDetachPayload*>(det_buf.data());
    det->target_node = owner_node;
    det->resource_type = static_cast<uint16_t>(ResourceType::NET);
    det->resource_id = resource_id;
    det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie;
    return wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data(), static_cast<uint16_t>(det_buf.size()),
                            tx_token_out);
}

struct NetDetachAttempt {
    ProxyNetState* state = nullptr;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint8_t attach_cookie = 0;
    uint32_t peer_boot_epoch = 0;
    WkiReliableTxToken tx_token = {};
};

// Caller must hold s_net_proxy_lock.
void link_pending_net_detach_locked(ProxyNetState* state) {
    state->detach_prev = g_pending_net_detach_tail;
    state->detach_next = nullptr;
    if (g_pending_net_detach_tail != nullptr) {
        g_pending_net_detach_tail->detach_next = state;
    } else {
        g_pending_net_detach_head = state;
    }
    g_pending_net_detach_tail = state;
}

// Caller must hold s_net_proxy_lock.
void unlink_pending_net_detach_locked(ProxyNetState* state) {
    if (state->detach_prev != nullptr) {
        state->detach_prev->detach_next = state->detach_next;
    } else {
        g_pending_net_detach_head = state->detach_next;
    }
    if (state->detach_next != nullptr) {
        state->detach_next->detach_prev = state->detach_prev;
    } else {
        g_pending_net_detach_tail = state->detach_prev;
    }
    state->detach_prev = nullptr;
    state->detach_next = nullptr;
}

// Caller must hold s_net_proxy_lock.
auto pending_net_detach_matches_locked(const ProxyNetState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie)
    -> bool {
    return state->detach_pending && state->detach_owner_node == owner_node && state->detach_resource_id == resource_id &&
           state->detach_attach_cookie == attach_cookie;
}

// Caller must hold s_net_proxy_lock. A deferred-only reservation starts idle
// so task context can claim it after teardown releases registry locks.
auto stage_net_detach_locked(ProxyNetState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                             bool initial_attempt_in_progress) -> bool {
    if (state == nullptr || attach_cookie == 0) {
        return false;
    }
    if (state->detach_pending) {
        bool const SAME = pending_net_detach_matches_locked(state, owner_node, resource_id, attach_cookie);
        if (!SAME) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI NET proxy: overlapping detach idempotence tuples");
            hcf();
        }
        return false;
    }

    state->detach_pending = true;
    state->detach_retry_in_progress = initial_attempt_in_progress;
    state->detach_owner_node = owner_node;
    state->detach_resource_id = resource_id;
    state->detach_attach_cookie = attach_cookie;
    state->detach_peer_boot_epoch = state->binding_peer_boot_epoch;
    state->detach_tx_token = {};
    link_pending_net_detach_locked(state);
    return true;
}

auto stage_net_detach(ProxyNetState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> bool {
    s_net_proxy_lock.lock();
    bool const STAGED = stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false);
    s_net_proxy_lock.unlock();
    return STAGED;
}

void finish_net_detach_attempt(const NetDetachAttempt& attempt, WkiReliableTxStatus tx_status, int send_result,
                               const WkiReliableTxToken& replacement_token, bool peer_epoch_invalidated = false) {
    if (attempt.state == nullptr) {
        return;
    }

    s_net_proxy_lock.lock();
    ProxyNetState* const state = attempt.state;
    if (!pending_net_detach_matches_locked(state, attempt.owner_node, attempt.resource_id, attempt.attach_cookie) ||
        !state->detach_retry_in_progress) {
        s_net_proxy_lock.unlock();
        return;
    }

    if (tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated) {
        unlink_pending_net_detach_locked(state);
        state->detach_pending = false;
        state->detach_retry_in_progress = false;
        state->detach_owner_node = WKI_NODE_INVALID;
        state->detach_resource_id = 0;
        state->detach_attach_cookie = 0;
        state->detach_peer_boot_epoch = 0;
        state->detach_tx_token = {};
        erase_unpublished_net_proxy_storage_locked(state);
    } else if (tx_status == WkiReliableTxStatus::PENDING) {
        state->detach_retry_in_progress = false;
    } else {
        state->detach_tx_token = send_result == WKI_OK ? replacement_token : WkiReliableTxToken{};
        state->detach_retry_in_progress = false;
    }
    s_net_proxy_lock.unlock();
}

auto send_or_defer_net_detach(ProxyNetState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> int {
    bool const STAGED = stage_net_detach(state, owner_node, resource_id, attach_cookie);
    wki_deferred_work_notify();
    return STAGED ? WKI_OK : WKI_ERR_BUSY;
}

void apply_owner_net_state(ProxyNetState* state, const NetStateNotifyPayload& notify) {
    if (state == nullptr) {
        return;
    }

    state->owner_ipv4_addr = notify.ipv4_addr;
    state->owner_ipv4_mask = notify.ipv4_mask;
    state->owner_real_mac = notify.real_mac;
    state->owner_link_state = notify.link_state;
    state->owner_mtu = notify.mtu != 0 ? notify.mtu : state->owner_mtu;

    state->netdev.state = static_cast<uint8_t>(state->owner_link_state != 0 ? 1 : 0);
    if (notify.mtu != 0) {
        state->netdev.mtu = notify.mtu;
    }
}

// Caller must hold ProxyNetState::lock.
auto set_net_rx_credits_locked(ProxyNetState* state, uint16_t credits) -> uint16_t {
    if (state == nullptr) {
        return 0;
    }
    state->rx_credits_remaining = credits;
    return credits;
}

// Caller must hold ProxyNetState::lock.
auto consume_net_rx_credit_locked(ProxyNetState* state) -> uint16_t {
    if (state == nullptr) {
        return 0;
    }

    if (state->rx_credits_remaining > 0) {
        state->rx_credits_remaining--;
    }

    if (state->rx_credits_remaining > WKI_NET_RX_CREDITS / 4) {
        return 0;
    }

    uint16_t const REPLENISH = WKI_NET_RX_CREDITS - state->rx_credits_remaining;
    state->rx_credits_remaining = WKI_NET_RX_CREDITS;
    return REPLENISH;
}

// -----------------------------------------------------------------------------
// Consumer-side NetDeviceOps
// -----------------------------------------------------------------------------

auto proxy_net_open(ker::net::NetDevice* dev) -> int {
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) {
        return -1;
    }

    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_OPEN, wait, nullptr, 0, WKI_OP_TIMEOUT_US, &op_cookie);
    if (PREP_RET != WKI_OK) {
        release_net_proxy(state);
        return -1;
    }

    std::array<uint8_t, sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES> req_buf{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_OPEN;
    req->data_len = NET_OP_COOKIE_BYTES;
    write_net_op_cookie(req_buf.data() + sizeof(DevOpReqPayload), op_cookie);

    int const SEND_RET =
        wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(req_buf.size()));
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        release_net_proxy(state);
        return -1;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] proxy_net_open timeout: node=0x%04x", state->owner_node);
        } else {
            ker::mod::dbg::log("[WKI] proxy_net_open aborted: node=0x%04x err=%d", state->owner_node, WAIT_RC);
        }
        release_net_proxy(state);
        return -1;
    }

    int op_status = 0;
    consume_net_op_result(state, wait, &op_status, nullptr);
    if (op_status == 0) {
        dev->state = 1;

        // V2: Send initial RX credit grant to server
        state->lock.lock();
        uint16_t const INITIAL_CREDITS = set_net_rx_credits_locked(state, WKI_NET_RX_CREDITS);
        state->lock.unlock();
        std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint16_t)> credit_buf{};
        auto* credit_req = reinterpret_cast<DevOpReqPayload*>(credit_buf.data());
        credit_req->op_id = OP_NET_RX_CREDIT;
        credit_req->data_len = sizeof(uint16_t);
        memcpy(credit_buf.data() + sizeof(DevOpReqPayload), &INITIAL_CREDITS, sizeof(uint16_t));
        static_cast<void>(wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, credit_buf.data(),
                                                       static_cast<uint16_t>(sizeof(DevOpReqPayload) + sizeof(uint16_t))));

        ker::mod::dbg::log("[WKI] proxy_net_open success: %s", dev->name.data());
    }
    release_net_proxy(state);
    return op_status;
}

void proxy_net_close(ker::net::NetDevice* dev) {
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) {
        return;
    }

    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_CLOSE, wait, nullptr, 0, WKI_OP_TIMEOUT_US, &op_cookie);
    if (PREP_RET != WKI_OK) {
        dev->state = 0;
        release_net_proxy(state);
        return;
    }

    std::array<uint8_t, sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES> req_buf{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_CLOSE;
    req->data_len = NET_OP_COOKIE_BYTES;
    write_net_op_cookie(req_buf.data() + sizeof(DevOpReqPayload), op_cookie);

    int const SEND_RET =
        wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, req_buf.data(), static_cast<uint16_t>(req_buf.size()));
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        dev->state = 0;
        release_net_proxy(state);
        return;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] proxy_net_close timeout: node=0x%04x", state->owner_node);
        } else {
            ker::mod::dbg::log("[WKI] proxy_net_close aborted: node=0x%04x err=%d", state->owner_node, WAIT_RC);
        }
    } else {
        consume_net_op_result(state, wait, nullptr, nullptr);
    }

    dev->state = 0;
    ker::mod::dbg::log("[WKI] proxy_net_close: %s", dev->name.data());
    release_net_proxy(state);
}

auto proxy_net_xmit(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt) -> int {
    if (pkt == nullptr) {
        return -1;
    }
    auto drop_pkt = [&]() -> int {
        ker::net::pkt_free(pkt);
        return -1;
    };

    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) {
        return drop_pkt();
    }

    WkiRemoteNetXmitRequestSize const REQ_SIZE = wki_remote_net_xmit_request_size(pkt->len);
    if (!REQ_SIZE.ok) {
        release_net_proxy(state);
        return drop_pkt();
    }
    uint16_t const REQ_TOTAL = REQ_SIZE.total_len;

    // Fire-and-forget: send OP_NET_XMIT, don't wait for response
    // Request data: raw packet bytes (pkt->data, pkt->len)
    auto* req_buf = new (std::nothrow) uint8_t[REQ_TOTAL];
    if (req_buf == nullptr) {
        release_net_proxy(state);
        return drop_pkt();
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = OP_NET_XMIT;
    req->data_len = static_cast<uint16_t>(pkt->len);
    memcpy(req_buf + sizeof(DevOpReqPayload), pkt->data, pkt->len);

    int const SEND_RET = wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, req_buf, REQ_TOTAL);
    delete[] req_buf;

    if (SEND_RET == WKI_OK) {
        dev->tx_packets++;
        dev->tx_bytes += pkt->len;
    } else {
        dev->tx_dropped++;
    }
    release_net_proxy(state);

    // Free the packet buffer (we've copied the data into the WKI message)
    ker::net::pkt_free(pkt);
    return (SEND_RET == WKI_OK) ? 0 : -1;
}

void proxy_net_set_mac(ker::net::NetDevice* dev, const uint8_t* mac) {
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr || mac == nullptr) {
        release_net_proxy(state);
        return;
    }

    WkiWaitEntry wait = {};
    uint16_t op_cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_SET_MAC, wait, nullptr, 0, WKI_DEV_PROXY_TIMEOUT_US, &op_cookie);
    if (PREP_RET != WKI_OK) {
        release_net_proxy(state);
        return;
    }

    constexpr uint16_t MAC_LEN = 6;
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES + MAC_LEN);
    std::array<uint8_t, sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES + MAC_LEN> req_buf{};

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_SET_MAC;
    req->data_len = NET_OP_COOKIE_BYTES + MAC_LEN;
    write_net_op_cookie(req_buf.data() + sizeof(DevOpReqPayload), op_cookie);
    memcpy(req_buf.data() + sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES, mac, MAC_LEN);

    int const SEND_RET = wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, req_buf.data(), req_total);

    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        release_net_proxy(state);
        return;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] proxy_net_set_mac timeout: node=0x%04x", state->owner_node);
        } else {
            ker::mod::dbg::log("[WKI] proxy_net_set_mac aborted: node=0x%04x err=%d", state->owner_node, WAIT_RC);
        }
        release_net_proxy(state);
        return;
    }

    int op_status = 0;
    consume_net_op_result(state, wait, &op_status, nullptr);

    // Update local MAC on success
    if (op_status == 0) {
        memcpy(dev->mac.data(), mac, 6);
    }
    release_net_proxy(state);
}

// Static NetDeviceOps for proxy NIC
const ker::net::NetDeviceOps G_PROXY_NET_OPS = {
    .open = proxy_net_open,
    .close = proxy_net_close,
    .start_xmit = proxy_net_xmit,
    .set_mac = proxy_net_set_mac,
    .set_queue_cpu = nullptr,
};

}  // namespace

// -------------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------------

void wki_remote_net_init() {
    if (g_remote_net_initialized) {
        return;
    }
    g_remote_net_initialized = true;
    ker::mod::dbg::log("[WKI] Remote NIC subsystem initialized");
}

auto wki_remote_net_detach_pending_for_resource(uint16_t owner_node, uint32_t resource_id) -> bool {
    s_net_proxy_lock.lock();
    bool const PENDING = net_detach_pending_for_resource_locked(owner_node, resource_id);
    s_net_proxy_lock.unlock();
    return PENDING;
}

void wki_remote_net_process_pending_detaches() {
    std::array<NetDetachAttempt, NET_DETACH_RETRY_BATCH> attempts{};
    size_t attempt_count = 0;
    size_t scanned = 0;

    s_net_proxy_lock.lock();
    ProxyNetState* state = g_pending_net_detach_head;
    while (state != nullptr && scanned < NET_DETACH_RETRY_SCAN && attempt_count < attempts.size()) {
        ProxyNetState* const NEXT = state->detach_next;
        scanned++;
        if (!state->detach_retry_in_progress) {
            state->detach_retry_in_progress = true;
            attempts.at(attempt_count++) = {
                .state = state,
                .owner_node = state->detach_owner_node,
                .resource_id = state->detach_resource_id,
                .attach_cookie = state->detach_attach_cookie,
                .peer_boot_epoch = state->detach_peer_boot_epoch,
                .tx_token = state->detach_tx_token,
            };
            if (state != g_pending_net_detach_tail) {
                unlink_pending_net_detach_locked(state);
                link_pending_net_detach_locked(state);
            }
        }
        state = NEXT;
    }
    s_net_proxy_lock.unlock();

    for (size_t i = 0; i < attempt_count; ++i) {
        NetDetachAttempt const& ATTEMPT = attempts.at(i);
        bool const PEER_EPOCH_INVALIDATED = wki_peer_remote_boot_epoch_invalidated(ATTEMPT.owner_node, ATTEMPT.peer_boot_epoch);
        WkiReliableTxStatus const TX_STATUS = wki_reliable_tx_status(ATTEMPT.tx_token);
        WkiReliableTxToken replacement_token = {};
        int send_ret = WKI_ERR_BUSY;
        if (!PEER_EPOCH_INVALIDATED && (TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED)) {
            send_ret = send_net_detach(ATTEMPT.owner_node, ATTEMPT.resource_id, ATTEMPT.attach_cookie, &replacement_token);
        }
        finish_net_detach_attempt(ATTEMPT, TX_STATUS, send_ret, replacement_token, PEER_EPOCH_INVALIDATED);
    }
}

// -------------------------------------------------------------------------------
// Server Side - NET Operation Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_net_op(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, ker::net::NetDevice* net_dev, uint16_t op_id,
                   const uint8_t* data, uint16_t data_len) {
    if (net_dev == nullptr || !net_channel_identity_matches_header(hdr, channel_identity)) {
        return;
    }

    // Exact local generation matching fences work that was dispatched before
    // a slot was retired.  The legacy DEV_OP_REQ wire format has no binding
    // nonce, so a frame delayed on the wire until after the same numeric ID is
    // re-reserved remains protocol-level indistinguishable here.
    uint16_t const REQUEST_COOKIE = net_req_cookie_from_header(hdr);

    switch (op_id) {
        case OP_NET_XMIT: {
            // Fire-and-forget: no response sent
            if (data_len == 0 || net_dev->ops == nullptr || net_dev->ops->start_xmit == nullptr) {
                return;
            }

            // Allocate PacketBuffer, copy data, call start_xmit
            ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
            if (pkt == nullptr) {
                return;
            }

            // Copy packet data into the buffer
            size_t copy_len = data_len;
            copy_len = std::min(copy_len, ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM);
            memcpy(pkt->data, data, copy_len);
            pkt->len = copy_len;
            pkt->dev = net_dev;

            int const RET = net_dev->ops->start_xmit(net_dev, pkt);
            (void)RET;
            // No response for fire-and-forget XMIT
            break;
        }

        case OP_NET_SET_MAC: {
            // Request: legacy {mac:u8[6]} or {cookie:u16, mac:u8[6]}.
            uint16_t const RESPONSE_COOKIE =
                (data_len >= NET_OP_COOKIE_BYTES + 6U) ? read_net_op_cookie(data, data_len, REQUEST_COOKIE) : REQUEST_COOKIE;
            const uint8_t* mac_data = data;
            uint16_t mac_len = data_len;
            if (data_len >= NET_OP_COOKIE_BYTES + 6U) {
                mac_data = data + NET_OP_COOKIE_BYTES;
                mac_len = static_cast<uint16_t>(data_len - NET_OP_COOKIE_BYTES);
            }
            if (mac_len < 6) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_NET_SET_MAC;
                resp.status = -1;
                resp.data_len = 0;
                resp.reserved = RESPONSE_COOKIE;
                static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
                break;
            }

            if (net_dev->ops != nullptr && net_dev->ops->set_mac != nullptr) {
                net_dev->ops->set_mac(net_dev, mac_data);
            }
            wki_dev_server_notify_net_changed(net_dev);

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_SET_MAC;
            resp.status = 0;
            resp.data_len = 0;
            resp.reserved = RESPONSE_COOKIE;
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            break;
        }

        case OP_NET_RX_NOTIFY:
        case OP_NET_STATE_NOTIFY: {
            // D11: This op is sent by the server (owner) to the consumer.
            // Owner-pushed state updates should also only reach the consumer.
            break;
        }

        case OP_NET_GET_STATS: {
            // D13: Response: {rx_pkt:u64, tx_pkt:u64, rx_bytes:u64, tx_bytes:u64, rx_drop:u64, tx_drop:u64} = 48 bytes
            uint16_t const RESPONSE_COOKIE = read_net_op_cookie(data, data_len, REQUEST_COOKIE);
            constexpr uint16_t STATS_DATA_LEN = 48;
            std::array<uint8_t, sizeof(DevOpRespPayload) + STATS_DATA_LEN> resp_buf{};

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_NET_GET_STATS;
            resp->status = 0;
            resp->data_len = STATS_DATA_LEN;
            resp->reserved = RESPONSE_COOKIE;

            auto* stats_data = resp_buf.data() + sizeof(DevOpRespPayload);
            memcpy(stats_data, &net_dev->rx_packets, sizeof(uint64_t));
            memcpy(stats_data + 8, &net_dev->tx_packets, sizeof(uint64_t));
            memcpy(stats_data + 16, &net_dev->rx_bytes, sizeof(uint64_t));
            memcpy(stats_data + 24, &net_dev->tx_bytes, sizeof(uint64_t));
            memcpy(stats_data + 32, &net_dev->rx_dropped, sizeof(uint64_t));
            memcpy(stats_data + 40, &net_dev->tx_dropped, sizeof(uint64_t));

            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, resp_buf.data(),
                                                           static_cast<uint16_t>(sizeof(DevOpRespPayload) + STATS_DATA_LEN)));
            break;
        }

        case OP_NET_OPEN: {
            uint16_t const RESPONSE_COOKIE = read_net_op_cookie(data, data_len, REQUEST_COOKIE);
            // V2: Bring up the NIC and subscribe consumer to RX forwarding
            int status = 0;
            if (net_dev->ops != nullptr && net_dev->ops->open != nullptr) {
                status = net_dev->ops->open(net_dev);
            }

            if (status == 0) {
                wki_dev_server_mark_net_opened(channel_identity, net_dev, true);
            }
            wki_dev_server_notify_net_changed(net_dev);

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_OPEN;
            resp.status = static_cast<int16_t>(status);
            resp.data_len = 0;
            resp.reserved = RESPONSE_COOKIE;
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            break;
        }

        case OP_NET_CLOSE: {
            uint16_t const RESPONSE_COOKIE = read_net_op_cookie(data, data_len, REQUEST_COOKIE);
            // V2: Shut down NIC and unsubscribe consumer from RX forwarding
            if (net_dev->ops != nullptr && net_dev->ops->close != nullptr) {
                net_dev->ops->close(net_dev);
            }

            wki_dev_server_mark_net_opened(channel_identity, net_dev, false);
            wki_dev_server_notify_net_changed(net_dev);

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_CLOSE;
            resp.status = 0;
            resp.data_len = 0;
            resp.reserved = RESPONSE_COOKIE;
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            break;
        }

        case OP_NET_RX_CREDIT: {
            // V2: Consumer replenishes RX forwarding credits
            if (data_len >= sizeof(uint16_t)) {
                uint16_t credits = 0;
                memcpy(&credits, data, sizeof(uint16_t));
                wki_dev_server_add_net_rx_credits(channel_identity, net_dev, credits);
            }
            // No response - fire-and-forget credit replenishment
            break;
        }

        default: {
            DevOpRespPayload resp = {};
            resp.op_id = op_id;
            resp.status = -1;
            resp.data_len = 0;
            resp.reserved = REQUEST_COOKIE;
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &resp, sizeof(resp)));
            break;
        }
    }
}

}  // namespace detail

// -------------------------------------------------------------------------------
// Consumer Side - Attach / Response Handlers
// -------------------------------------------------------------------------------

auto wki_remote_net_attach(uint16_t owner_node, uint32_t resource_id, const char* local_name, uint64_t expected_resource_generation)
    -> ker::net::NetDevice* {
    if (local_name == nullptr) {
        return nullptr;
    }

    WkiPeer* const PEER = wki_peer_find(owner_node);
    NetPeerLifecycleLease ATTACH_PEER_LIFECYCLE(PEER);
    if (!ATTACH_PEER_LIFECYCLE.acquired() || PEER->node_id != owner_node || PEER->state != PeerState::CONNECTED ||
        PEER->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
        return nullptr;
    }

    uint64_t const RESOURCE_GENERATION = expected_resource_generation != 0
                                             ? expected_resource_generation
                                             : wki_resource_generation_snapshot(owner_node, ResourceType::NET, resource_id);
    ResourceIncarnationToken owner_incarnation = {};
    if (!wki_resource_observation_snapshot(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, &owner_incarnation)) {
        return nullptr;
    }

    uint8_t attach_cookie = 0;

    // Allocate proxy state (lock protects deque mutation)
    s_net_proxy_lock.lock();
    if (net_detach_pending_for_resource_locked(owner_node, resource_id)) {
        s_net_proxy_lock.unlock();
        return nullptr;
    }
    if (auto* existing = find_net_proxy_by_resource(owner_node, resource_id); existing != nullptr) {
        auto* dev = existing->active ? &existing->netdev : nullptr;
        s_net_proxy_lock.unlock();
        return dev;
    }
    if (ker::net::netdev_find_by_name(local_name) != nullptr) {
        s_net_proxy_lock.unlock();
        return nullptr;
    }
    attach_cookie = allocate_net_attach_cookie_locked(owner_node, resource_id);
    if (attach_cookie == 0) {
        s_net_proxy_lock.unlock();
        return nullptr;
    }
    g_net_proxy_storage.push_back(std::make_unique<ProxyNetState>());
    auto* state = g_net_proxy_storage.back().get();
    g_net_proxies.push_back(state);
    state->attaching = true;
    state->owner_node = owner_node;
    state->resource_id = resource_id;
    state->resource_generation = RESOURCE_GENERATION;
    state->attach_cookie = attach_cookie;
    // The peer lifecycle lease excludes HELLO epoch mutation here. Persist the
    // binding epoch so later terminal teardown never inverts peer_lock against
    // that lifecycle gate merely to stage a retry reservation.
    state->binding_peer_boot_epoch = PEER->remote_boot_epoch;
    retain_net_proxy(state);
    s_net_proxy_lock.unlock();

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::NET);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.attach_cookie = attach_cookie;

    WkiChannelIdentity reserved_channel_identity{};
    if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, owner_node)) {
        if (wki_channel_alloc(owner_node, PriorityClass::THROUGHPUT, &reserved_channel_identity) == nullptr) {
            retire_unregistered_net_proxy(state, reserved_channel_identity);
            return nullptr;
        }
        attach_req.requested_channel = reserved_channel_identity.channel_id;
        state->lock.lock();
        state->attach_channel = reserved_channel_identity.channel_id;
        state->channel_identity = reserved_channel_identity;
        state->lock.unlock();
    } else {
        attach_req.requested_channel = 0;
        state->lock.lock();
        state->attach_channel = 0;
        state->lock.unlock();
    }

    WkiWaitEntry wait = {};
    state->lock.lock();
    state->attach_wait_entry = &wait;
    state->attach_expected_cookie = attach_req.attach_cookie;
    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->lock.unlock();

    if (state->retiring.load(std::memory_order_acquire)) {
        cancel_attach_waiter(state, wait, WKI_ERR_PEER_FENCED);
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    int const SEND_RET = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    // Reliable RX needs this same peer gate to dispatch DEV_ATTACH_ACK. Keep
    // the gate through request publication, then release it before waiting.
    ATTACH_PEER_LIFECYCLE.release();
    if (SEND_RET != WKI_OK) {
        cancel_attach_waiter(state, wait, SEND_RET);
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    clear_attach_waiter_after_wait(state, wait);
    if (WAIT_RC != 0) {
        // The request is already in the reliable stream. Any local wait
        // cancellation can race an accepted owner binding, not just timeout.
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] Remote NIC attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
        } else {
            ker::mod::dbg::log("[WKI] Remote NIC attach aborted: node=0x%04x res_id=%u err=%d", owner_node, resource_id, WAIT_RC);
        }
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    state->lock.lock();
    uint8_t const ATTACH_STATUS = state->attach_status;
    uint16_t const ATTACH_CHANNEL = state->attach_channel;
    state->lock.unlock();
    if (ATTACH_STATUS != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Remote NIC attach rejected: status=%u", ATTACH_STATUS);
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    // Do not use the blocking lifecycle acquire here: peer cleanup owns this
    // gate while waiting for our retained proxy reference to drain. A retrying
    // try-acquire lets that cleanup mark the state retiring, at which point the
    // attach path releases its reference instead of deadlocking with it.
    NetPeerLifecycleLease PUBLICATION_PEER_LIFECYCLE;
    while (!PUBLICATION_PEER_LIFECYCLE.try_acquire(PEER)) {
        if (state->retiring.load(std::memory_order_acquire)) {
            static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
            retire_unregistered_net_proxy(state, reserved_channel_identity);
            return nullptr;
        }
        ker::mod::sched::kern_yield();
    }
    if (PEER->node_id != owner_node || PEER->state != PeerState::CONNECTED ||
        PEER->vfs_reset_rebind_pending.load(std::memory_order_acquire) || state->retiring.load(std::memory_order_acquire) ||
        !wki_resource_observation_is_live(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, owner_incarnation)) {
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    if (reserved_channel_identity.channel != nullptr) {
        if (ATTACH_CHANNEL != reserved_channel_identity.channel_id) {
            ker::mod::dbg::log("[WKI] Remote NIC attach channel mismatch: node=0x%04x res_id=%u requested=%u assigned=%u", owner_node,
                               resource_id, reserved_channel_identity.channel_id, ATTACH_CHANNEL);
            static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
            retire_unregistered_net_proxy(state, reserved_channel_identity);
            return nullptr;
        }
    } else {
        if (wki_channel_reserve(owner_node, ATTACH_CHANNEL, PriorityClass::THROUGHPUT, &reserved_channel_identity) == nullptr) {
            ker::mod::dbg::log("[WKI] Remote NIC attach local reserve failed: node=0x%04x res_id=%u ch=%u", owner_node, resource_id,
                               ATTACH_CHANNEL);
            static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
            retire_unregistered_net_proxy(state, reserved_channel_identity);
            return nullptr;
        }
        state->lock.lock();
        state->channel_identity = reserved_channel_identity;
        state->lock.unlock();
    }

    if (state->retiring.load(std::memory_order_acquire) ||
        !wki_channel_generation_is_live(reserved_channel_identity.channel, owner_node, reserved_channel_identity.channel_id,
                                        reserved_channel_identity.generation)) {
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    state->lock.lock();
    state->assigned_channel = reserved_channel_identity.channel_id;
    state->channel_identity = reserved_channel_identity;
    state->max_op_size = state->attach_max_op_size;
    state->lock.unlock();

    // V2: Populate proxy NetDevice with proper name and MAC
    // Name: "wki-<hostname>" truncated to NETDEV_NAME_LEN-1
    size_t name_len = strlen(local_name);
    if (name_len >= ker::net::NETDEV_NAME_LEN) {
        name_len = ker::net::NETDEV_NAME_LEN - 1;
    }
    memcpy(state->netdev.name.data(), local_name, name_len);
    state->netdev.name.at(name_len) = '\0';

    // V2: Locally-administered MAC: 02:57:4B:xx:yy:zz
    // 0x57,0x4B = "WK", xx:yy = local node_id (big-endian), zz = sequential index
    state->netdev.mac.bytes = {
        0x02,
        0x57,
        0x4B,
        static_cast<uint8_t>((g_wki.my_node_id >> 8) & 0xFF),
        static_cast<uint8_t>(g_wki.my_node_id & 0xFF),
        g_wki.proxy_nic_index++,
    };

    state->netdev.ops = &G_PROXY_NET_OPS;
    state->netdev.private_data = state;
    state->netdev.mtu = state->owner_mtu != 0 ? state->owner_mtu : 1500;
    state->netdev.state = static_cast<uint8_t>(state->owner_link_state != 0 ? 1 : 0);

    // V2: Initialize RX backpressure credits
    state->lock.lock();
    set_net_rx_credits_locked(state, WKI_NET_RX_CREDITS);
    state->lock.unlock();

    // The publication lifecycle lease serializes this final publication with HELLO
    // reconciliation. Resource replacement/withdrawal is generation-qualified
    // and can still race independently, so validate the exact observation and
    // channel again immediately before entering the netdev registry.
    if (PEER->node_id != owner_node || PEER->state != PeerState::CONNECTED ||
        PEER->vfs_reset_rebind_pending.load(std::memory_order_acquire) || state->retiring.load(std::memory_order_acquire) ||
        !wki_resource_observation_is_live(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, owner_incarnation) ||
        !wki_channel_generation_is_live(reserved_channel_identity.channel, owner_node, reserved_channel_identity.channel_id,
                                        reserved_channel_identity.generation)) {
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    // netdev_register() publishes a raw NetDevice pointer before returning.
    // Permanently pin the containing proxy first so an epoch-reset race cannot
    // unregister and free storage in that publication window. Retaining a
    // failed-registration tombstone is an intentional safety tradeoff.
    s_net_proxy_lock.lock();
    state->ever_published = true;
    s_net_proxy_lock.unlock();

    // Register in the netdev subsystem
    if (ker::net::netdev_register(&state->netdev) != 0) {
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    bool publish_proxy = false;
    s_net_proxy_lock.lock();
    if (!state->retiring.load(std::memory_order_acquire) && !state->cleanup_started && !state->epoch_reset_pending) {
        state->netdev_registered = true;
        state->active = true;
        state->attaching = false;
        publish_proxy = true;
    } else {
        // This attach path registered the device, so it also unregisters it.
        // Publishing false here prevents deferred cleanup from doing so twice.
        state->netdev_registered = false;
    }
    s_net_proxy_lock.unlock();
    if (!publish_proxy) {
        unpublish_proxy_netdev(state);
        static_cast<void>(send_or_defer_net_detach(state, owner_node, resource_id, attach_cookie));
        retire_unregistered_net_proxy(state, reserved_channel_identity);
        return nullptr;
    }

    bool const RESOURCE_STILL_LIVE =
        wki_resource_observation_is_live(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, owner_incarnation);
    s_net_proxy_lock.lock();
    bool const PROXY_STILL_PUBLISHED =
        state->active && state->netdev_registered && !state->cleanup_started && !state->retiring.load(std::memory_order_acquire);
    s_net_proxy_lock.unlock();
    if (!RESOURCE_STILL_LIVE || !PROXY_STILL_PUBLISHED) {
        release_net_proxy(state);
        wki_remote_net_detach_resource_generation(owner_node, resource_id, RESOURCE_GENERATION);
        return nullptr;
    }

    ker::mod::dbg::log("[WKI] Remote NIC attached: %s -> node=0x%04x res_id=%u ch=%u mac=%02x:%02x:%02x:%02x:%02x:%02x", local_name,
                       owner_node, resource_id, state->assigned_channel, state->netdev.mac.at(0), state->netdev.mac.at(1),
                       state->netdev.mac.at(2), state->netdev.mac.at(3), state->netdev.mac.at(4), state->netdev.mac.at(5));

    release_net_proxy(state);
    return &state->netdev;
}

auto wki_remote_net_has_proxy(uint16_t owner_node, uint32_t resource_id) -> bool {
    s_net_proxy_lock.lock();
    bool const FOUND = find_net_proxy_by_resource(owner_node, resource_id) != nullptr;
    s_net_proxy_lock.unlock();
    return FOUND;
}

void wki_remote_net_detach(ker::net::NetDevice* proxy_dev) {
    uint16_t owner_node{};
    uint32_t resource_id{};
    WkiChannelIdentity channel_identity{};
    uint8_t attach_cookie{};
    ProxyNetState* state = nullptr;
    WkiWaitEntry* op_waiter = nullptr;
    WkiWaitEntry* attach_waiter = nullptr;
    bool detach_staged = false;

    // Lock: find proxy, copy info, mark inactive, erase from deque
    s_net_proxy_lock.lock();
    state = find_net_proxy_by_dev(proxy_dev);
    if (state == nullptr) {
        s_net_proxy_lock.unlock();
        return;
    }
    if (!try_retire_net_proxy_locked(state)) {
        s_net_proxy_lock.unlock();
        return;
    }
    state->netdev_registered = false;

    owner_node = state->owner_node;
    resource_id = state->resource_id;
    attach_cookie = state->attach_cookie;
    // Reserve the legacy owner/resource/cookie tuple before the retiring row
    // becomes invisible to replacement attach lookup.
    detach_staged = stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false);
    state->lock.lock();
    channel_identity = state->channel_identity;
    if (state->op_pending.load(std::memory_order_acquire)) {
        op_waiter = claim_and_clear_waiter_locked(state->op_wait_entry);
        clear_net_op_state_locked(state, -1);
    }
    if (state->attach_pending.load(std::memory_order_acquire)) {
        state->attach_status = static_cast<uint8_t>(DevAttachStatus::BUSY);
        attach_waiter = claim_and_clear_waiter_locked(state->attach_wait_entry);
        state->attach_expected_cookie = 0;
        state->attach_pending.store(false, std::memory_order_release);
    }
    state->lock.unlock();
    s_net_proxy_lock.unlock();

    finish_claimed_waiter(op_waiter, -1);
    finish_claimed_waiter(attach_waiter, -1);
    wait_for_net_proxy_refs_to_drain(state);

    unpublish_proxy_netdev(state);

    if (detach_staged) {
        wki_deferred_work_notify();
    }

    // Close only the dynamic-channel allocation owned by this proxy.
    close_net_channel_identity(channel_identity);
    erase_net_proxy(state);
}

void wki_remote_net_detach_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation) {
    if (resource_generation == 0) {
        return;
    }

    ProxyNetState* state = nullptr;
    WkiChannelIdentity channel_identity = {};
    uint8_t attach_cookie = 0;
    bool unregister_netdev = false;
    bool detach_staged = false;
    WkiWaitEntry* op_waiter = nullptr;
    WkiWaitEntry* attach_waiter = nullptr;

    s_net_proxy_lock.lock();
    for (auto& proxy : g_net_proxies) {
        if (proxy == nullptr || proxy->owner_node != owner_node || proxy->resource_id != resource_id ||
            proxy->resource_generation != resource_generation || proxy->cleanup_started ||
            (!proxy->active && !proxy->attaching && !proxy->attach_pending.load(std::memory_order_acquire) &&
             !proxy->epoch_reset_pending)) {
            continue;
        }
        state = proxy;
        state->retiring.store(true, std::memory_order_release);
        state->active = false;
        state->attaching = false;
        state->epoch_reset_pending = false;
        state->cleanup_started = true;
        state->netdev.state = 0;
        unregister_netdev = state->netdev_registered;
        state->netdev_registered = false;
        attach_cookie = state->attach_cookie;
        // Make the exact retirement reservation part of the same registry
        // transition that hides this generation from replacement lookup.
        detach_staged = stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false);

        state->lock.lock();
        channel_identity = state->channel_identity;
        if (state->op_pending.load(std::memory_order_acquire)) {
            op_waiter = claim_and_clear_waiter_locked(state->op_wait_entry);
            clear_net_op_state_locked(state, WKI_ERR_PEER_FENCED);
        }
        if (state->attach_pending.load(std::memory_order_acquire)) {
            state->attach_status = static_cast<uint8_t>(DevAttachStatus::STALE_RESOURCE);
            attach_waiter = claim_and_clear_waiter_locked(state->attach_wait_entry);
            state->attach_expected_cookie = 0;
            state->attach_pending.store(false, std::memory_order_release);
        }
        state->lock.unlock();
        break;
    }
    s_net_proxy_lock.unlock();

    if (state == nullptr) {
        return;
    }

    finish_claimed_waiter(op_waiter, WKI_ERR_PEER_FENCED);
    finish_claimed_waiter(attach_waiter, WKI_ERR_PEER_FENCED);
    wait_for_net_proxy_refs_to_drain(state);
    if (unregister_netdev) {
        unpublish_proxy_netdev(state);
    }
    if (detach_staged) {
        wki_deferred_work_notify();
    }
    close_net_channel_identity(channel_identity);
    erase_net_proxy(state);
}

// -------------------------------------------------------------------------------
// Consumer Side - RX Handlers
// -------------------------------------------------------------------------------

namespace detail {

void handle_net_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    ProxyNetState* state = acquire_net_proxy_by_attach(hdr->src_node, ack->resource_id, ack->reserved);
    if (state == nullptr) {
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    state->lock.lock();
    if (!state->attach_pending.load(std::memory_order_acquire) ||
        !wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, *ack)) {
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }
    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    // V2: Extract NET extension fields if present
    if (payload_len >= sizeof(DevAttachAckNetPayload)) {
        const auto* net_ack = reinterpret_cast<const DevAttachAckNetPayload*>(payload);
        state->owner_ipv4_addr = net_ack->ipv4_addr;
        state->owner_ipv4_mask = net_ack->ipv4_mask;
        state->owner_real_mac = net_ack->real_mac;
        state->owner_link_state = net_ack->link_state;
        state->owner_mtu = net_ack->mtu != 0 ? net_ack->mtu : state->owner_mtu;
    }

    state->attach_expected_cookie = 0;
    state->attach_pending.store(false, std::memory_order_release);
    wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
    state->lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
    release_net_proxy(state);
}

void handle_net_op_resp(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const uint8_t* payload, uint16_t payload_len) {
    if (!net_channel_identity_matches_header(hdr, channel_identity) || payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t const RESP_DATA_LEN = resp->data_len;

    if (sizeof(DevOpRespPayload) + RESP_DATA_LEN > payload_len) {
        return;
    }

    ProxyNetState* state = acquire_net_proxy_by_channel(channel_identity);
    if (state == nullptr) {
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    state->lock.lock();
    if (!state->op_pending.load(std::memory_order_acquire)) {
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }
    if (!wki_dev_op_response_matches_expected(state->op_expected_id, state->op_expected_seq, *resp)) {
        ker::mod::dbg::log("[WKI] Ignoring stale NET response: node=0x%04x ch=%u resp_op=%u expected_op=%u resp_seq=%u expected_seq=%u",
                           hdr->src_node, hdr->channel_id, resp->op_id, state->op_expected_id, resp->reserved, state->op_expected_seq);
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }

    // D13: For GET_STATS, update proxy NIC counters directly (no caller waiting)
    constexpr uint16_t STATS_RESP_LEN = 48;
    if (resp->op_id == OP_NET_GET_STATS) {
        if (resp->status == 0 && RESP_DATA_LEN >= STATS_RESP_LEN) {
            memcpy(&state->netdev.rx_packets, resp_data, sizeof(uint64_t));
            memcpy(&state->netdev.tx_packets, resp_data + 8, sizeof(uint64_t));
            memcpy(&state->netdev.rx_bytes, resp_data + 16, sizeof(uint64_t));
            memcpy(&state->netdev.tx_bytes, resp_data + 24, sizeof(uint64_t));
            memcpy(&state->netdev.rx_dropped, resp_data + 32, sizeof(uint64_t));
            memcpy(&state->netdev.tx_dropped, resp_data + 40, sizeof(uint64_t));
        }
        clear_net_op_state_locked(state, resp->status);
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }

    wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry);
    if (wait_entry == nullptr) {
        clear_net_op_state_locked(state, resp->status);
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }

    state->op_status = resp->status;
    if (RESP_DATA_LEN > 0 && state->op_resp_buf != nullptr) {
        uint16_t const COPY_LEN = (RESP_DATA_LEN > state->op_resp_max) ? state->op_resp_max : RESP_DATA_LEN;
        memcpy(state->op_resp_buf, resp_data, COPY_LEN);
        state->op_resp_len = COPY_LEN;
    } else {
        state->op_resp_len = 0;
    }
    state->lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
    release_net_proxy(state);
}

// D11: Consumer receives forwarded packets from the owner NIC
void handle_net_rx_notify(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const uint8_t* data, uint16_t data_len) {
    if (!net_channel_identity_matches_header(hdr, channel_identity) || data_len < sizeof(NetNotifyHeader)) {
        return;
    }

    ProxyNetState* state = acquire_net_proxy_by_channel(channel_identity);
    if (state == nullptr) {
        release_net_proxy(state);
        return;
    }

    const auto* notify = reinterpret_cast<const NetNotifyHeader*>(data);
    state->lock.lock();
    bool const NOTIFY_MATCHES =
        wki_net_notify_header_matches_expected(state->attach_cookie, *notify) && wki_net_notify_payload_fits(data_len, *notify);
    state->lock.unlock();
    if (!NOTIFY_MATCHES) {
        release_net_proxy(state);
        return;
    }

    const uint8_t* frame_data = data + sizeof(NetNotifyHeader);
    uint16_t const FRAME_LEN = notify->data_len;

    ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
    if (pkt == nullptr) {
        state->netdev.rx_dropped++;
        release_net_proxy(state);
        return;
    }

    size_t const COPY_LEN = std::min<size_t>(FRAME_LEN, ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM);
    memcpy(pkt->data, frame_data, COPY_LEN);
    pkt->len = COPY_LEN;
    pkt->dev = &state->netdev;
    retain_net_proxy(state);
    pkt->lifetime_ctx = state;
    pkt->lifetime_release = release_net_proxy_lifetime;

    // Feed into the local network stack
    ker::net::netdev_rx(&state->netdev, pkt);

    // V2: Replenish RX credits periodically
    // Send a credit batch every 16 packets to amortize overhead
    state->lock.lock();
    uint16_t const REPLENISH = consume_net_rx_credit_locked(state);
    state->lock.unlock();
    if (REPLENISH != 0) {
        std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint16_t)> credit_buf{};
        auto* credit_req = reinterpret_cast<DevOpReqPayload*>(credit_buf.data());
        credit_req->op_id = OP_NET_RX_CREDIT;
        credit_req->data_len = sizeof(uint16_t);
        memcpy(credit_buf.data() + sizeof(DevOpReqPayload), &REPLENISH, sizeof(uint16_t));
        static_cast<void>(wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, credit_buf.data(),
                                                       static_cast<uint16_t>(sizeof(DevOpReqPayload) + sizeof(uint16_t))));
    }
    release_net_proxy(state);
}

void handle_net_state_notify(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const uint8_t* data, uint16_t data_len) {
    if (!net_channel_identity_matches_header(hdr, channel_identity) || data_len < sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload)) {
        return;
    }

    ProxyNetState* state = acquire_net_proxy_by_channel(channel_identity);
    if (state == nullptr) {
        release_net_proxy(state);
        return;
    }

    const auto* notify = reinterpret_cast<const NetNotifyHeader*>(data);
    const auto* state_payload = reinterpret_cast<const NetStateNotifyPayload*>(data + sizeof(NetNotifyHeader));
    state->lock.lock();
    if (!wki_net_notify_header_matches_expected(state->attach_cookie, *notify) || !wki_net_notify_payload_fits(data_len, *notify) ||
        notify->data_len != sizeof(NetStateNotifyPayload)) {
        state->lock.unlock();
        release_net_proxy(state);
        return;
    }
    apply_owner_net_state(state, *state_payload);
    state->lock.unlock();
    release_net_proxy(state);
}

}  // namespace detail

// -------------------------------------------------------------------------------
// D13: Periodic stats polling (non-blocking, called from timer tick)
// -------------------------------------------------------------------------------

void wki_remote_net_poll_stats() {
    if (!g_remote_net_initialized) {
        return;
    }

    uint64_t const NOW_US = wki_now_us();
    if (!wki_remote_net_stats_poll_due(NOW_US, g_last_net_stats_poll_us)) {
        return;
    }
    g_last_net_stats_poll_us = NOW_US;

    // Collect proxies to poll under lock
    struct PollTarget {
        ProxyNetState* state = nullptr;
        WkiChannelIdentity channel_identity{};
    };
    constexpr size_t MAX_POLL_TARGETS = 32;
    std::array<PollTarget, MAX_POLL_TARGETS> targets{};
    size_t target_count = 0;

    s_net_proxy_lock.lock();
    for (auto& p : g_net_proxies) {
        if (!p->active || p->retiring.load(std::memory_order_acquire)) {
            continue;
        }
        if (target_count < MAX_POLL_TARGETS) {
            retain_net_proxy(p);
            targets.at(target_count++) = {.state = p, .channel_identity = p->channel_identity};
        }
    }
    s_net_proxy_lock.unlock();

    // Send stats requests outside lock
    for (size_t i = 0; i < target_count; i++) {
        const auto& target = targets.at(i);
        auto* st = target.state;
        if (st == nullptr || st->retiring.load(std::memory_order_acquire)) {
            release_net_proxy(st);
            continue;
        }

        st->lock.lock();
        if (net_stats_poll_expired_locked(st, NOW_US)) {
            clear_net_op_state_locked(st, WKI_ERR_TIMEOUT);
        }
        if (net_proxy_op_slot_busy(st)) {
            st->lock.unlock();
            release_net_proxy(st);
            continue;
        }
        uint16_t const EXPECTED_COOKIE = allocate_net_op_cookie_locked(st);
        st->op_wait_entry = nullptr;
        st->op_expected_id = OP_NET_GET_STATS;
        st->op_expected_seq = EXPECTED_COOKIE;
        st->op_deadline_us = wki_future_deadline_us(NOW_US, WKI_DEV_PROXY_TIMEOUT_US);
        st->op_status = 0;
        st->op_resp_buf = nullptr;
        st->op_resp_max = 0;
        st->op_resp_len = 0;
        st->op_pending.store(true, std::memory_order_release);
        st->lock.unlock();

        std::array<uint8_t, sizeof(DevOpReqPayload) + NET_OP_COOKIE_BYTES> req_buf{};
        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
        req->op_id = OP_NET_GET_STATS;
        req->data_len = NET_OP_COOKIE_BYTES;
        write_net_op_cookie(req_buf.data() + sizeof(DevOpReqPayload), EXPECTED_COOKIE);

        int const SEND_RET = wki_send_on_channel_identity(target.channel_identity, MsgType::DEV_OP_REQ, req_buf.data(),
                                                          static_cast<uint16_t>(req_buf.size()));
        if (SEND_RET != WKI_OK) {
            st->lock.lock();
            if (net_stats_poll_owns_slot(st) && st->op_expected_seq == EXPECTED_COOKIE) {
                clear_net_op_state_locked(st, SEND_RET);
            }
            st->lock.unlock();
        }
        release_net_proxy(st);
    }
}

// -------------------------------------------------------------------------------
// Fencing Cleanup
// -------------------------------------------------------------------------------

void wki_remote_net_mark_epoch_reset(uint16_t node_id) {
    s_net_proxy_lock.lock();
    for (auto& proxy : g_net_proxies) {
        auto* state = proxy;
        if (state == nullptr || state->owner_node != node_id || state->cleanup_started ||
            (!state->active && !state->attaching && !state->attach_pending.load(std::memory_order_acquire))) {
            continue;
        }

        // HELLO RX may neither allocate nor wait for device/channel teardown.
        // Retiring fences new operations immediately; task-context cleanup
        // consumes epoch_reset_pending after the channel pool reset.
        state->active = false;
        state->epoch_reset_pending = true;
        state->retiring.store(true, std::memory_order_release);
        state->netdev.state = 0;
    }
    s_net_proxy_lock.unlock();
}

namespace {

enum class NetProxyCleanupScope : uint8_t {
    ALL_FOR_PEER,
    EPOCH_MARKED_ONLY,
};

void cleanup_net_proxies_for_peer(uint16_t node_id, NetProxyCleanupScope scope, bool owner_reboot_proven) {
    struct CleanupEntry {
        ProxyNetState* state = nullptr;
        WkiChannelIdentity channel_identity{};
        bool unregister_netdev = false;
        bool detach_staged = false;
        WkiWaitEntry* op_wait_entry = nullptr;
        WkiWaitEntry* attach_wait_entry = nullptr;
    };
    constexpr size_t MAX_CLEANUP = 32;

    while (true) {
        std::array<CleanupEntry, MAX_CLEANUP> cleanup_entries{};
        size_t cleanup_count = 0;

        s_net_proxy_lock.lock();
        for (auto& p : g_net_proxies) {
            bool const EPOCH_ONLY = scope == NetProxyCleanupScope::EPOCH_MARKED_ONLY;
            bool const LIVE_OR_MARKED =
                p->active || p->attaching || p->attach_pending.load(std::memory_order_acquire) || p->epoch_reset_pending;
            if (p->owner_node != node_id || p->cleanup_started || !LIVE_OR_MARKED || (EPOCH_ONLY && !p->epoch_reset_pending)) {
                continue;
            }
            if (cleanup_count >= cleanup_entries.size()) {
                break;
            }

            p->retiring.store(true, std::memory_order_release);
            p->active = false;
            p->attaching = false;
            p->epoch_reset_pending = false;
            p->cleanup_started = true;
            p->netdev.state = 0;

            WkiWaitEntry* op_waiter = nullptr;
            WkiWaitEntry* attach_waiter = nullptr;
            p->lock.lock();
            WkiChannelIdentity const CHANNEL_IDENTITY = p->channel_identity;
            if (p->op_pending.load(std::memory_order_acquire)) {
                op_waiter = claim_and_clear_waiter_locked(p->op_wait_entry);
                clear_net_op_state_locked(p, WKI_ERR_PEER_FENCED);
            }

            if (p->attach_pending.load(std::memory_order_acquire)) {
                p->attach_status = static_cast<uint8_t>(DevAttachStatus::BUSY);
                attach_waiter = claim_and_clear_waiter_locked(p->attach_wait_entry);
                p->attach_expected_cookie = 0;
                p->attach_pending.store(false, std::memory_order_release);
            }
            p->lock.unlock();

            auto& entry = cleanup_entries.at(cleanup_count++);
            entry.state = p;
            entry.channel_identity = CHANNEL_IDENTITY;
            // The proxy leaves the cookie-allocation registry at the end of
            // cleanup. Reserve the exact legacy tuple first unless a known
            // owner reboot proves that no server binding can survive.
            if (!owner_reboot_proven) {
                entry.detach_staged = stage_net_detach_locked(p, p->owner_node, p->resource_id, p->attach_cookie, false);
            }
            entry.unregister_netdev = p->netdev_registered;
            p->netdev_registered = false;
            entry.op_wait_entry = op_waiter;
            entry.attach_wait_entry = attach_waiter;
        }
        s_net_proxy_lock.unlock();

        if (cleanup_count == 0) {
            return;
        }

        for (size_t i = 0; i < cleanup_count; i++) {
            const auto& entry = cleanup_entries.at(i);
            finish_claimed_waiter(entry.op_wait_entry, WKI_ERR_PEER_FENCED);
            finish_claimed_waiter(entry.attach_wait_entry, WKI_ERR_PEER_FENCED);
            wait_for_net_proxy_refs_to_drain(entry.state);

            if (entry.unregister_netdev && entry.state != nullptr) {
                unpublish_proxy_netdev(entry.state);
            }
            close_net_channel_identity(entry.channel_identity);
            erase_net_proxy(entry.state);
            if (entry.detach_staged) {
                wki_deferred_work_notify();
            }
            ker::mod::dbg::log("[WKI] Remote NIC proxy cleanup: node=0x%04x", node_id);
        }
    }
}

}  // namespace

void wki_remote_net_cleanup_for_peer(uint16_t node_id, bool owner_reboot_proven) {
    cleanup_net_proxies_for_peer(node_id, NetProxyCleanupScope::ALL_FOR_PEER, owner_reboot_proven);
}

void wki_remote_net_cleanup_epoch_reset_for_peer(uint16_t node_id, bool owner_reboot_proven) {
    cleanup_net_proxies_for_peer(node_id, NetProxyCleanupScope::EPOCH_MARKED_ONLY, owner_reboot_proven);
}

auto wki_remote_net_selftest_cancel_preserves_successor_op() -> bool {
    ProxyNetState state = {};
    WkiWaitEntry old_wait = {};
    WkiWaitEntry next_wait = {};

    old_wait.state.store(WkiWaitEntry::DONE, std::memory_order_release);
    state.op_wait_entry = &next_wait;
    state.op_expected_id = OP_NET_SET_MAC;
    state.op_expected_seq = 0x1234;
    state.op_deadline_us = 0x5678;
    state.op_status = 19;
    state.op_pending.store(true, std::memory_order_release);

    cancel_op_waiter(&state, old_wait, WKI_ERR_TIMEOUT);

    bool const SUCCESSOR_PRESERVED = state.op_wait_entry == &next_wait && state.op_expected_id == OP_NET_SET_MAC &&
                                     state.op_expected_seq == 0x1234 && state.op_deadline_us == 0x5678 && state.op_status == 19 &&
                                     state.op_pending.load(std::memory_order_acquire);

    WkiWaitEntry owned_wait = {};
    state.op_wait_entry = &owned_wait;
    state.op_expected_id = OP_NET_OPEN;
    state.op_expected_seq = 0x9abc;
    state.op_deadline_us = 0xdef0;
    state.op_status = 0;
    state.op_pending.store(true, std::memory_order_release);

    cancel_op_waiter(&state, owned_wait, WKI_ERR_TIMEOUT);

    bool const OWNED_SLOT_CLEARED = state.op_wait_entry == nullptr && state.op_expected_id == 0 && state.op_expected_seq == 0 &&
                                    state.op_deadline_us == 0 && state.op_status == WKI_ERR_TIMEOUT &&
                                    !state.op_pending.load(std::memory_order_acquire) && owned_wait.result == WKI_ERR_TIMEOUT &&
                                    owned_wait.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;

    return SUCCESSOR_PRESERVED && OWNED_SLOT_CLEARED;
}

}  // namespace ker::net::wki
