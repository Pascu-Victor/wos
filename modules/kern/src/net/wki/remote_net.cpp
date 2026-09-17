#include "remote_net.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/route.hpp>
#include <net/wki/auth_policy.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/credential_record.hpp>
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
    state->op_completion = nullptr;
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

// The caller has already fenced new proxy operations. An in-flight control
// owns its field writes until finish_control; its completion must publish DOWN.
void publish_retiring_net_state(ProxyNetState* state) { ker::net::netdev_mark_control_retiring(&state->netdev); }

auto try_retire_net_proxy_locked(ProxyNetState* state) -> bool {
    if (state == nullptr || state->retiring.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    state->active = false;
    state->attaching = false;
    state->cleanup_started = true;
    publish_retiring_net_state(state);
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

auto prepare_net_op_wait(ProxyNetState* state, uint16_t op_id, NetOpCompletion& completion, void* resp_buf, uint16_t resp_max,
                         uint64_t timeout_us, uint16_t* cookie_out) -> int {
    int const SLOT_RET = acquire_net_op_slot_locked(state, wki_now_us());
    if (SLOT_RET != WKI_OK) {
        return SLOT_RET;
    }

    uint16_t const COOKIE = allocate_net_op_cookie_locked(state);

    auto& wait = completion.wait;
    completion.status = 0;
    completion.response_length = 0;
    state->op_completion = &completion;
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

// Caller holds ProxyNetState::lock; wake the returned waiter after unlocking.
auto publish_net_op_response_locked(ProxyNetState* state, const DevOpRespPayload& response, const uint8_t* data) -> WkiWaitEntry* {
    auto* completion = state->op_completion;
    // A duplicate reply after the first writer detached the waiter cannot
    // replace its result or release the still-owned operation slot.
    if (completion == nullptr || state->op_wait_entry != &completion->wait) return nullptr;
    auto* wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry);
    if (wait_entry == nullptr) {
        clear_net_op_state_locked(state, response.status);
        return nullptr;
    }
    completion->status = response.status;
    state->op_status = response.status;
    if (response.data_len > 0 && state->op_resp_buf != nullptr) {
        uint16_t const COPY_LEN = (response.data_len > state->op_resp_max) ? state->op_resp_max : response.data_len;
        memcpy(state->op_resp_buf, data, COPY_LEN);
        state->op_resp_len = COPY_LEN;
    } else {
        state->op_resp_len = 0;
    }
    completion->response_length = state->op_resp_len;
    return wait_entry;
}

// Caller has observed successful wait completion (acquire of wait.DONE).
void consume_net_op_result(ProxyNetState* state, NetOpCompletion& completion, int* status_out, uint16_t* resp_len_out) {
    int const STATUS = completion.status;
    uint16_t const RESP_LEN = completion.response_length;
    state->lock.lock();
    if (state->op_completion == &completion) {
        state->op_wait_entry = nullptr;
        clear_net_op_state_locked(state, STATUS);
    }
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
    if (state->op_wait_entry == &wait || (state->op_completion != nullptr && &state->op_completion->wait == &wait)) {
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

// Caller holds control_publication_lock, then ProxyNetState::lock.
void publish_owner_link_state_locked(ProxyNetState* state) {
    state->netdev.state = static_cast<uint8_t>(state->owner_link_state != 0 ? 1 : 0);
    if (state->owner_mtu_pending) state->netdev.mtu = state->owner_mtu;
    state->owner_state_pending = false;
    state->owner_mtu_pending = false;
}

void apply_owner_net_state(ProxyNetState* state, const NetStateNotifyPayload& notify, const NetIpv6StateSuffix& ipv6) {
    if (state == nullptr || state->retiring.load(std::memory_order_acquire)) return;
    state->owner_ipv4_addr = notify.ipv4_addr;
    state->owner_ipv4_mask = notify.ipv4_mask;
    state->owner_real_mac = notify.real_mac;
    state->owner_link_state = notify.link_state;
    state->owner_mtu = notify.mtu != 0 ? notify.mtu : state->owner_mtu;
    state->owner_ipv6 = ipv6;
    state->owner_state_pending = true;
    state->owner_mtu_pending |= notify.mtu != 0;
    if (!state->netdev.control_busy.load(std::memory_order_acquire)) publish_owner_link_state_locked(state);
}

// The caller owns a proxy reference or unpublished test storage.
auto snapshot_owner_l3(ProxyNetState* state) -> WkiRemoteNetOwnerL3Snapshot {
    if (state == nullptr) return {};
    WkiRemoteNetOwnerL3Snapshot snapshot{};
    uint64_t const FLAGS = state->lock.lock_irqsave();
    if (!state->retiring.load(std::memory_order_acquire)) {
        snapshot = {.valid = true, .ipv4_addr = state->owner_ipv4_addr, .ipv4_mask = state->owner_ipv4_mask, .ipv6 = state->owner_ipv6};
    }
    state->lock.unlock_irqrestore(FLAGS);
    return snapshot;
}

// NetDeviceControlGuard holds control_publication_lock. No notification can
// enter between this flush and release of control_busy, or across acquisition.
void proxy_net_finish_control(ker::net::NetDevice* dev) {
    auto* state = static_cast<ProxyNetState*>(dev->private_data);
    if (state == nullptr) return;
    state->lock.lock();
    if (state->retiring.load(std::memory_order_acquire)) {
        dev->state = 0;
        state->owner_state_pending = false;
        state->owner_mtu_pending = false;
    } else if (state->owner_state_pending) {
        publish_owner_link_state_locked(state);
    }
    state->lock.unlock();
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

// WKI transport codes overlap POSIX errno values. Only transport-domain
// preparation/send/wait results enter here; an owner's errno passes through.
auto net_open_transport_errno(int status) -> int {
    switch (status) {
        case WKI_OK:
            return 0;
        case WKI_ERR_NO_MEM:
            return -ENOMEM;
        case WKI_ERR_NO_ROUTE:
        case WKI_ERR_NOT_FOUND:
            return -EHOSTUNREACH;
        case WKI_ERR_PEER_FENCED:
            return -ENOTCONN;
        case WKI_ERR_NO_CREDITS:
        case WKI_ERR_BUSY:
            return -EAGAIN;
        case WKI_ERR_TIMEOUT:
            return -ETIMEDOUT;
        case WKI_ERR_INVALID:
            return -EINVAL;
        case WKI_ERR_TX_FAILED:
        default:
            return -EIO;
    }
}

// Retry only failures before reliable sequence publication. WKI_OK transfers
// retransmit ownership even when the initial transport attempt failed.
constexpr uint64_t NET_CONTROL_SEND_RETRY_US = 100'000;

auto net_control_send_retryable(int status) -> bool {
    return status == WKI_ERR_NO_ROUTE || status == WKI_ERR_NO_CREDITS || status == WKI_ERR_NO_MEM || status == WKI_ERR_NOT_FOUND ||
           status == WKI_ERR_BUSY;
}

template <typename Send, typename Live, typename Pause, typename Now>
auto retry_net_control_send(uint64_t deadline, Send&& send, Live&& live, Pause&& pause, Now&& now) -> int {
    int status = send();
    while (net_control_send_retryable(status) && now() < deadline && live()) {
        pause();
        // Retirement can wait for our retained reference. Do not wait for its
        // lifecycle writer, nor send on a replacement after dropping ownership.
        if (now() >= deadline || !live()) break;
        status = send();
    }
    return status;
}

// Task context only. The caller retains the proxy/binding and the complete
// request or completed response bytes until this bounded admission finishes.
auto send_net_control(const WkiChannelIdentity& identity, MsgType type, const void* bytes, uint16_t size, const std::atomic<bool>* retiring)
    -> int {
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), NET_CONTROL_SEND_RETRY_US);
    return retry_net_control_send(
        DEADLINE, [&] { return wki_send_on_channel_identity(identity, type, bytes, size); },
        [&] {
            return retiring != nullptr && !retiring->load(std::memory_order_acquire) &&
                   wki_channel_generation_is_live(identity.channel, identity.peer_node_id, identity.channel_id, identity.generation);
        },
        [] { ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US); }, [] { return wki_now_us(); });
}

// The caller retains this proxy for the entire synchronous operation.
auto open_retained_net_proxy(ProxyNetState* state, const ker::mod::security::Snapshot& caller) -> int {
    if (!state->open_identity_supported || state->max_op_size < WKI_NET_OPEN_IDENTITY_SIZE) return -ENOTSUP;
    auto* dev = &state->netdev;
    NetOpCompletion completion{};
    auto& wait = completion.wait;
    uint16_t op_cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_OPEN_AS, completion, nullptr, 0, WKI_OP_TIMEOUT_US, &op_cookie);
    if (PREP_RET != WKI_OK) {
        return net_open_transport_errno(PREP_RET);
    }

    static_assert(WKI_NET_OPEN_CALLER_SIZE == ker::mod::security::credential_record::SIZE);
    std::array<uint8_t, sizeof(DevOpReqPayload) + WKI_NET_OPEN_IDENTITY_SIZE> req_buf{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_OPEN_AS;
    req->data_len = WKI_NET_OPEN_IDENTITY_SIZE;
    NetOpenIdentityHeader const HEADER{.cookie = op_cookie, .version = WKI_NET_OPEN_IDENTITY_VERSION};
    auto const RECORD = ker::mod::security::credential_record::encode(caller);
    std::memcpy(req_buf.data() + sizeof(DevOpReqPayload), &HEADER, sizeof(HEADER));
    std::memcpy(req_buf.data() + sizeof(DevOpReqPayload) + sizeof(HEADER), RECORD.data(), RECORD.size());

    int const SEND_RET = send_net_control(state->channel_identity, MsgType::DEV_OP_REQ, req_buf.data(),
                                          static_cast<uint16_t>(req_buf.size()), &state->retiring);
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        return net_open_transport_errno(SEND_RET);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            ker::mod::dbg::log("[WKI] proxy_net_open timeout: node=0x%04x", state->owner_node);
        } else {
            ker::mod::dbg::log("[WKI] proxy_net_open aborted: node=0x%04x err=%d", state->owner_node, WAIT_RC);
        }
        return net_open_transport_errno(WAIT_RC);
    }

    int op_status = 0;
    consume_net_op_result(state, completion, &op_status, nullptr);
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
    return op_status;
}

auto proxy_net_open(ker::net::NetDevice*) -> int { return -ENOTSUP; }

auto proxy_net_open_as(ker::net::NetDevice* dev, const ker::mod::security::Snapshot& caller) -> int {
    int const AUTHORIZATION = ker::mod::security::authorize_network_administration(caller);
    if (AUTHORIZATION != 0) return AUTHORIZATION;
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) {
        return -ENODEV;
    }
    int const STATUS = open_retained_net_proxy(state, caller);
    release_net_proxy(state);
    return STATUS;
}

// Callerless remote control cannot acquire owner device authority.
void proxy_net_close(ker::net::NetDevice*) {}

auto close_retained_net_proxy(ProxyNetState* state, const ker::mod::security::Snapshot& caller) -> int {
    if (!state->close_identity_supported || state->max_op_size < WKI_NET_CLOSE_IDENTITY_SIZE) return -ENOTSUP;
    NetOpCompletion completion{};
    auto& wait = completion.wait;
    uint16_t op_cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_CLOSE_AS, completion, nullptr, 0, WKI_OP_TIMEOUT_US, &op_cookie);
    if (PREP_RET != WKI_OK) return net_open_transport_errno(PREP_RET);

    std::array<uint8_t, sizeof(DevOpReqPayload) + WKI_NET_CLOSE_IDENTITY_SIZE> req_buf{};
    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_CLOSE_AS;
    req->data_len = WKI_NET_CLOSE_IDENTITY_SIZE;
    NetCloseIdentityHeader const HEADER{.cookie = op_cookie, .version = WKI_NET_CLOSE_IDENTITY_VERSION};
    auto const RECORD = ker::mod::security::credential_record::encode(caller);
    std::memcpy(req_buf.data() + sizeof(DevOpReqPayload), &HEADER, sizeof(HEADER));
    std::memcpy(req_buf.data() + sizeof(DevOpReqPayload) + sizeof(HEADER), RECORD.data(), RECORD.size());
    int const SEND_RET = send_net_control(state->channel_identity, MsgType::DEV_OP_REQ, req_buf.data(),
                                          static_cast<uint16_t>(req_buf.size()), &state->retiring);
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        return net_open_transport_errno(SEND_RET);
    }
    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        return net_open_transport_errno(WAIT_RC);
    }
    int op_status = 0;
    consume_net_op_result(state, completion, &op_status, nullptr);
    if (op_status == 0) state->netdev.state = 0;
    return op_status;
}

auto proxy_net_close_as(ker::net::NetDevice* dev, const ker::mod::security::Snapshot& caller) -> int {
    int const AUTHORIZATION = ker::mod::security::authorize_network_administration(caller);
    if (AUTHORIZATION != 0) return AUTHORIZATION;
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) return -ENODEV;
    int const STATUS = close_retained_net_proxy(state, caller);
    release_net_proxy(state);
    return STATUS;
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

void proxy_net_set_mac(ker::net::NetDevice*, const uint8_t*) {}

auto apply_retained_net_link(ProxyNetState* state, const ker::net::NetDeviceLinkControl& control,
                             const ker::mod::security::Snapshot& caller) -> int {
    if (!state->link_identity_supported || state->max_op_size < WKI_NET_LINK_IDENTITY_SIZE) return -ENOTSUP;
    NetOpCompletion completion{};
    auto& wait = completion.wait;
    uint16_t cookie = 0;
    int const PREP_RET = prepare_net_op_wait(state, OP_NET_LINK_AS, completion, nullptr, 0, WKI_OP_TIMEOUT_US, &cookie);
    if (PREP_RET != WKI_OK) return net_open_transport_errno(PREP_RET);
    std::array<uint8_t, sizeof(DevOpReqPayload) + WKI_NET_LINK_IDENTITY_SIZE> bytes{};
    DevOpReqPayload const REQUEST{.op_id = OP_NET_LINK_AS, .data_len = WKI_NET_LINK_IDENTITY_SIZE};
    NetOpenIdentityHeader const HEADER{.cookie = cookie, .version = WKI_NET_OPEN_IDENTITY_VERSION};
    auto const RECORD = ker::mod::security::credential_record::encode(caller);
    NetLinkControlPayload payload{};
    payload.fields = (control.change_state ? WKI_NET_LINK_STATE : 0) | (control.change_mac ? WKI_NET_LINK_MAC : 0);
    payload.up = control.change_state && control.up ? 1 : 0;
    if (control.change_mac) std::copy(control.mac.begin(), control.mac.end(), payload.mac);
    std::memcpy(bytes.data(), &REQUEST, sizeof(REQUEST));
    std::memcpy(bytes.data() + sizeof(REQUEST), &HEADER, sizeof(HEADER));
    std::memcpy(bytes.data() + sizeof(REQUEST) + sizeof(HEADER), RECORD.data(), RECORD.size());
    std::memcpy(bytes.data() + sizeof(REQUEST) + WKI_NET_OPEN_IDENTITY_SIZE, &payload, sizeof(payload));
    int const SEND_RET = send_net_control(state->channel_identity, MsgType::DEV_OP_REQ, bytes.data(), bytes.size(), &state->retiring);
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        return net_open_transport_errno(SEND_RET);
    }
    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        return net_open_transport_errno(WAIT_RC);
    }
    int status = 0;
    consume_net_op_result(state, completion, &status, nullptr);
    if (status == 0) {
        if (control.change_state) state->netdev.state = control.up ? 1 : 0;
        if (control.change_mac) std::copy(control.mac.begin(), control.mac.end(), state->netdev.mac.bytes.begin());
        if (control.change_state && control.up) {
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
        }
    }
    return status;
}

auto proxy_net_apply_link_as(ker::net::NetDevice* dev, const ker::net::NetDeviceLinkControl& control,
                             const ker::mod::security::Snapshot& caller) -> int {
    int const AUTHORIZATION = ker::mod::security::authorize_network_administration(caller);
    if (AUTHORIZATION != 0) return AUTHORIZATION;
    if (!control.change_state && !control.change_mac) return -EINVAL;
    auto* state = acquire_net_proxy_by_dev(dev);
    if (state == nullptr) return -ENODEV;
    int const STATUS = apply_retained_net_link(state, control, caller);
    release_net_proxy(state);
    return STATUS;
}

// Static NetDeviceOps for proxy NIC
const ker::net::NetDeviceOps G_PROXY_NET_OPS = {
    .open = proxy_net_open,
    .close = proxy_net_close,
    .start_xmit = proxy_net_xmit,
    .set_mac = proxy_net_set_mac,
    .set_queue_cpu = nullptr,
    .open_as = proxy_net_open_as,
    .close_as = proxy_net_close_as,
    .apply_link_as = proxy_net_apply_link_as,
    .finish_control = proxy_net_finish_control,
};

}  // namespace

namespace {
auto net_control_caller_status(const uint8_t* data, uint16_t data_len, uint64_t identity_policy) -> int {
    using namespace ker::mod::security;
    if (data == nullptr || data_len != WKI_NET_OPEN_IDENTITY_SIZE) return -EINVAL;
    NetOpenIdentityHeader header{};
    std::memcpy(&header, data, sizeof(header));
    if (header.version != WKI_NET_OPEN_IDENTITY_VERSION) return -ENOTSUP;
    if (header.cookie == 0 || header.reserved != 0) return -EINVAL;
    Snapshot supplied, mapped;
    int status = credential_record::decode({data + sizeof(header), credential_record::SIZE}, supplied);
    if (status == 0) status = auth_policy::map_net_control_identity(identity_policy, supplied, mapped);
    if (status == 0) status = authorize_network_administration(mapped);
    return status;
}

auto apply_net_link_as(ker::net::NetDevice* dev, const uint8_t* data, uint16_t data_len, uint64_t policy,
                       ker::net::NetDeviceControlGuard& control_guard) -> int {
    if (data == nullptr || data_len != WKI_NET_LINK_IDENTITY_SIZE) return -EINVAL;
    NetLinkControlPayload control{};
    std::memcpy(&control, data + WKI_NET_OPEN_IDENTITY_SIZE, sizeof(control));
    bool const HAS_STATE = (control.fields & WKI_NET_LINK_STATE) != 0;
    bool const HAS_MAC = (control.fields & WKI_NET_LINK_MAC) != 0;
    if (control.fields == 0 || (control.fields & ~(WKI_NET_LINK_STATE | WKI_NET_LINK_MAC)) != 0 || control.reserved != 0 ||
        control.reserved_tail != 0 || control.up > 1 || (!HAS_STATE && control.up != 0))
        return -EINVAL;
    if (!HAS_MAC && std::any_of(std::begin(control.mac), std::end(control.mac), [](uint8_t byte) { return byte != 0; })) return -EINVAL;
    int status = net_control_caller_status(data, WKI_NET_OPEN_IDENTITY_SIZE, policy);
    if (status != 0) return status;
    if (dev == nullptr) return -ENOTCONN;
    int const CONTROL_ADMISSION = control_guard.try_acquire(dev);
    if (CONTROL_ADMISSION != 0) return CONTROL_ADMISSION;
    if (HAS_MAC) {
        status = ker::net::netdev_check_mac(dev, control.mac);
        if (status != 0) return status;
    }
    // Native MAC commit cannot refuse after admission. Perform the only
    // fallible driver phase first, before state or MAC publication.
    if (HAS_STATE) {
        if (control.up != 0) {
            if (dev->ops != nullptr && dev->ops->open != nullptr) status = dev->ops->open(dev);
            if (status != 0) return status < 0 ? status : -EIO;
        } else if (dev->ops != nullptr && dev->ops->close != nullptr)
            dev->ops->close(dev);
        dev->state = control.up != 0 ? 1 : 0;
    }
    if (HAS_MAC && !std::equal(dev->mac.bytes.begin(), dev->mac.bytes.end(), control.mac)) {
        if (dev->ops != nullptr && dev->ops->set_mac != nullptr)
            dev->ops->set_mac(dev, control.mac);
        else
            std::copy_n(control.mac, 6, dev->mac.data());
    }
    return 0;
}

auto close_net_device_as(ker::net::NetDevice* dev, const uint8_t* data, uint16_t data_len, uint64_t identity_policy,
                         ker::net::NetDeviceControlGuard& control_guard) -> int {
    int const STATUS = net_control_caller_status(data, data_len, identity_policy);
    if (STATUS != 0) return STATUS;
    if (dev == nullptr) return -ENOTCONN;
    int const CONTROL_ADMISSION = control_guard.try_acquire(dev);
    if (CONTROL_ADMISSION != 0) return CONTROL_ADMISSION;
    if (dev->ops != nullptr && dev->ops->close != nullptr) dev->ops->close(dev);
    dev->state = 0;
    return 0;
}

auto open_net_device_as(ker::net::NetDevice* dev, const uint8_t* data, uint16_t data_len, uint64_t identity_policy,
                        ker::net::NetDeviceControlGuard& control_guard) -> int {
    int status = net_control_caller_status(data, data_len, identity_policy);
    if (status != 0) return status;
    if (dev == nullptr) return -ENOTCONN;
    int const CONTROL_ADMISSION = control_guard.try_acquire(dev);
    if (CONTROL_ADMISSION != 0) return CONTROL_ADMISSION;
    if (dev->ops != nullptr && dev->ops->open != nullptr) status = dev->ops->open(dev);
    return status <= 0 ? status : -EIO;
}
}  // namespace

// -------------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------------

auto wki_remote_net_owner_l3_snapshot(net::NetDevice* proxy_dev) -> WkiRemoteNetOwnerL3Snapshot {
    auto* state = acquire_net_proxy_by_dev(proxy_dev);
    auto const SNAPSHOT = snapshot_owner_l3(state);
    release_net_proxy(state);
    return SNAPSHOT;
}

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

void handle_net_link_as(const WkiChannelIdentity& channel_identity, ker::net::NetDevice* net_dev, const uint8_t* data, uint16_t data_len,
                        uint64_t identity_policy, const std::atomic<bool>* binding_retiring) {
    ker::net::NetDeviceControlGuard control_guard;
    int const STATUS = apply_net_link_as(net_dev, data, data_len, identity_policy, control_guard);
    if (STATUS == 0) {
        NetLinkControlPayload control{};
        std::memcpy(&control, data + WKI_NET_OPEN_IDENTITY_SIZE, sizeof(control));
        if ((control.fields & WKI_NET_LINK_STATE) != 0) wki_dev_server_mark_net_opened(channel_identity, net_dev, control.up != 0);
        wki_dev_server_notify_net_changed(net_dev);
    }
    control_guard.finish();
    DevOpRespPayload response{};
    response.op_id = OP_NET_LINK_AS;
    response.status = static_cast<int16_t>(STATUS);
    response.reserved = read_net_op_cookie(data, data_len, 0);
    static_cast<void>(send_net_control(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response), binding_retiring));
}

void handle_net_close_as(const WkiChannelIdentity& channel_identity, ker::net::NetDevice* net_dev, const uint8_t* data, uint16_t data_len,
                         uint64_t identity_policy, const std::atomic<bool>* binding_retiring) {
    ker::net::NetDeviceControlGuard control_guard;
    int const STATUS = close_net_device_as(net_dev, data, data_len, identity_policy, control_guard);
    if (STATUS == 0) {
        wki_dev_server_mark_net_opened(channel_identity, net_dev, false);
        wki_dev_server_notify_net_changed(net_dev);
    }
    control_guard.finish();
    DevOpRespPayload response{};
    response.op_id = OP_NET_CLOSE_AS;
    response.status = static_cast<int16_t>(STATUS);
    response.reserved = read_net_op_cookie(data, data_len, 0);
    static_cast<void>(send_net_control(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response), binding_retiring));
}

void handle_net_open_as(const WkiChannelIdentity& channel_identity, ker::net::NetDevice* net_dev, const uint8_t* data, uint16_t data_len,
                        uint64_t identity_policy, const std::atomic<bool>* binding_retiring) {
    ker::net::NetDeviceControlGuard control_guard;
    int const STATUS = open_net_device_as(net_dev, data, data_len, identity_policy, control_guard);
    if (STATUS == 0) {
        wki_dev_server_mark_net_opened(channel_identity, net_dev, true);
        wki_dev_server_notify_net_changed(net_dev);
    }
    control_guard.finish();
    DevOpRespPayload response{};
    response.op_id = OP_NET_OPEN_AS;
    response.status = static_cast<int16_t>(STATUS);
    response.reserved = read_net_op_cookie(data, data_len, 0);
    static_cast<void>(send_net_control(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response), binding_retiring));
}

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
            DevOpRespPayload response{};
            response.op_id = OP_NET_SET_MAC;
            response.status = -ENOTSUP;
            response.reserved = data_len >= NET_OP_COOKIE_BYTES + 6U ? read_net_op_cookie(data, data_len, REQUEST_COOKIE) : REQUEST_COOKIE;
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response)));
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
            // Credentialless control cannot acquire native device authority.
            DevOpRespPayload response{};
            response.op_id = OP_NET_OPEN;
            response.status = -ENOTSUP;
            response.reserved = read_net_op_cookie(data, data_len, REQUEST_COOKIE);
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response)));
            break;
        }

        case OP_NET_CLOSE: {
            DevOpRespPayload response{};
            response.op_id = OP_NET_CLOSE;
            response.status = -ENOTSUP;
            response.reserved = read_net_op_cookie(data, data_len, REQUEST_COOKIE);
            static_cast<void>(wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP, &response, sizeof(response)));
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
    if (ker::net::netdev_find_by_name_ref(local_name)) {
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
    // Attach ACK/notifications may update the owner cache before registration.
    // Consume it under the same publication order as later control completion.
    uint64_t const PUBLICATION_FLAGS = state->netdev.control_publication_lock.lock_irqsave();
    state->lock.lock();
    state->netdev.mtu = state->owner_mtu != 0 ? state->owner_mtu : 1500;
    state->netdev.state = static_cast<uint8_t>(!state->retiring.load(std::memory_order_acquire) && state->owner_link_state != 0 ? 1 : 0);

    // V2: Initialize RX backpressure credits
    set_net_rx_credits_locked(state, WKI_NET_RX_CREDITS);
    state->lock.unlock();
    state->netdev.control_publication_lock.unlock_irqrestore(PUBLICATION_FLAGS);

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

auto wki_remote_net_diag_snapshot(WkiRemoteNetDiagRow* rows, size_t capacity, size_t* total) -> size_t {
    size_t row_count = 0;
    size_t total_rows = 0;
    s_net_proxy_lock.lock();
    for (const auto& storage : g_net_proxy_storage) {
        auto* state = storage.get();
        if (state == nullptr) {
            continue;
        }
        total_rows++;
        if (rows == nullptr || row_count >= capacity) {
            continue;
        }
        auto& row = rows[row_count++];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        row.owner_node = state->owner_node;
        row.assigned_channel = state->assigned_channel;
        row.channel_generation = state->channel_identity.generation;
        row.resource_id = state->resource_id;
        row.resource_generation = state->resource_generation;
        row.refs = state->refs.load(std::memory_order_acquire);
        row.active = state->active;
        row.attaching = state->attaching;
        row.netdev_registered = state->netdev_registered;
        row.ever_published = state->ever_published;
        row.epoch_reset_pending = state->epoch_reset_pending;
        row.cleanup_started = state->cleanup_started;
        row.cleanup_complete = state->cleanup_complete;
        row.retiring = state->retiring.load(std::memory_order_acquire);
        row.attach_pending = state->attach_pending.load(std::memory_order_acquire);
        row.attach_cookie = state->attach_cookie;
        row.binding_peer_boot_epoch = state->binding_peer_boot_epoch;
        row.detach_pending = state->detach_pending;
        row.detach_retry_in_progress = state->detach_retry_in_progress;
        row.detach_attach_cookie = state->detach_attach_cookie;
        row.detach_peer_boot_epoch = state->detach_peer_boot_epoch;
        row.rx_credits_remaining = state->rx_credits_remaining;
        row.rx_packets = state->netdev.rx_packets;
        row.rx_bytes = state->netdev.rx_bytes;
        row.rx_dropped = state->netdev.rx_dropped;
        row.tx_packets = state->netdev.tx_packets;
        row.tx_bytes = state->netdev.tx_bytes;
        row.tx_dropped = state->netdev.tx_dropped;
        // Never wait for an operation while holding the registry lock. A
        // contended detail lock makes this row explicitly incomplete instead
        // of publishing a racy waiter/cookie tuple as convergence evidence.
        row.detail_complete = state->lock.try_lock();
        if (row.detail_complete) {
            row.op_pending = state->op_pending.load(std::memory_order_acquire);
            row.op_expected_id = state->op_expected_id;
            row.op_expected_seq = state->op_expected_seq;
            row.op_waiter_owned = state->op_wait_entry != nullptr;
            row.attach_waiter_owned = state->attach_wait_entry != nullptr;
            row.attach_expected_cookie = state->attach_expected_cookie;
            state->lock.unlock();
        }
    }
    s_net_proxy_lock.unlock();
    if (total != nullptr) {
        *total = total_rows;
    }
    return row_count;
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
        publish_retiring_net_state(state);
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
    if (hdr == nullptr || payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);
    NetIpv6StateSuffix ipv6 = wki_net_ipv6_state_empty();
    if (ack->status == static_cast<uint8_t>(DevAttachStatus::OK)) {
        bool const WITH_IPV6 = wki_peer_capability_negotiated(hdr->src_node, WKI_CAP_NET_IPV6_STATE);
        if (!wki_net_ipv6_extended_length_valid(WITH_IPV6, payload_len, sizeof(DevAttachAckNetPayload))) {
            return;
        }
        if (WITH_IPV6) {
            std::memcpy(&ipv6, payload + sizeof(DevAttachAckNetPayload), sizeof(ipv6));
            if (!wki_net_ipv6_state_valid(ipv6)) {
                return;
            }
        }
    } else if (payload_len != sizeof(DevAttachAckPayload)) {
        return;
    }

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

    // V2: Successful NET ACKs always carry the legacy NET extension. IPv6 is
    // an exact capability-gated suffix and never mutates local address state.
    if (ack->status == static_cast<uint8_t>(DevAttachStatus::OK)) {
        const auto* net_ack = reinterpret_cast<const DevAttachAckNetPayload*>(payload);
        state->open_identity_supported = (net_ack->rdma_flags & DEV_ATTACH_NET_OPEN_IDENTITY) != 0;
        state->close_identity_supported = (net_ack->rdma_flags & DEV_ATTACH_NET_CLOSE_IDENTITY) != 0;
        state->link_identity_supported = (net_ack->rdma_flags & DEV_ATTACH_NET_LINK_IDENTITY) != 0;
        state->owner_ipv4_addr = net_ack->ipv4_addr;
        state->owner_ipv4_mask = net_ack->ipv4_mask;
        state->owner_real_mac = net_ack->real_mac;
        state->owner_link_state = net_ack->link_state;
        state->owner_mtu = net_ack->mtu != 0 ? net_ack->mtu : state->owner_mtu;
        state->owner_ipv6 = ipv6;
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

    wait_entry = publish_net_op_response_locked(state, *resp, resp_data);
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
    bool const WITH_IPV6 = wki_peer_capability_negotiated(hdr->src_node, WKI_CAP_NET_IPV6_STATE);
    uint16_t const EXPECTED_STATE_SIZE =
        static_cast<uint16_t>(sizeof(NetStateNotifyPayload) + (WITH_IPV6 ? sizeof(NetIpv6StateSuffix) : 0));
    NetIpv6StateSuffix ipv6 = wki_net_ipv6_state_empty();
    uint64_t const PUBLICATION_FLAGS = state->netdev.control_publication_lock.lock_irqsave();
    state->lock.lock();
    if (!wki_net_notify_header_matches_expected(state->attach_cookie, *notify) || !wki_net_notify_payload_fits(data_len, *notify) ||
        notify->data_len != EXPECTED_STATE_SIZE ||
        !wki_net_ipv6_extended_length_valid(WITH_IPV6, data_len, sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload))) {
        state->lock.unlock();
        state->netdev.control_publication_lock.unlock_irqrestore(PUBLICATION_FLAGS);
        release_net_proxy(state);
        return;
    }
    if (WITH_IPV6) {
        std::memcpy(&ipv6, data + sizeof(NetNotifyHeader) + sizeof(NetStateNotifyPayload), sizeof(ipv6));
        if (!wki_net_ipv6_state_valid(ipv6)) {
            state->lock.unlock();
            state->netdev.control_publication_lock.unlock_irqrestore(PUBLICATION_FLAGS);
            release_net_proxy(state);
            return;
        }
    }
    apply_owner_net_state(state, *state_payload, ipv6);
    state->lock.unlock();
    state->netdev.control_publication_lock.unlock_irqrestore(PUBLICATION_FLAGS);
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
        st->op_completion = nullptr;
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
        publish_retiring_net_state(state);
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
            publish_retiring_net_state(p);

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

auto wki_remote_net_selftest_completed_result_survives_retirement() -> bool {
    constexpr std::array STATUSES{0, -EPERM, -EACCES, -EIO, -EINVAL, -ENODEV, -ETIMEDOUT};
    constexpr std::array OPERATIONS{OP_NET_OPEN_AS, OP_NET_CLOSE_AS, OP_NET_LINK_AS};
    constexpr std::array<uint8_t, 3> DATA{0x19, 0x27, 0x35};
    for (auto operation : OPERATIONS) {
        for (int status : STATUSES) {
            for (bool wake_first : {false, true}) {
                ProxyNetState state{};
                NetOpCompletion completion{};
                auto& wait = completion.wait;
                std::array<uint8_t, DATA.size()> output{};
                uint16_t cookie = 0;
                if (prepare_net_op_wait(&state, operation, completion, output.data(), output.size(), WKI_OP_TIMEOUT_US, &cookie) != WKI_OK)
                    return false;
                DevOpRespPayload response{};
                response.op_id = operation;
                response.status = static_cast<int16_t>(status);
                response.data_len = DATA.size();
                response.reserved = cookie;
                state.lock.lock();
                auto* writer = publish_net_op_response_locked(&state, response, DATA.data());
                state.lock.unlock();
                if (wake_first) finish_claimed_waiter(writer, 0);

                // Exact operation-side ordering used by peer cleanup after a
                // response writer wins, while the caller still owns its proxy pin.
                state.lock.lock();
                auto* retirement_writer = claim_and_clear_waiter_locked(state.op_wait_entry);
                clear_net_op_state_locked(&state, WKI_ERR_PEER_FENCED);
                state.lock.unlock();
                finish_claimed_waiter(retirement_writer, WKI_ERR_PEER_FENCED);
                if (!wake_first) finish_claimed_waiter(writer, 0);

                int actual = 123;
                uint16_t length = 0;
                consume_net_op_result(&state, completion, &actual, &length);
                if (writer != &wait || retirement_writer != nullptr || wait.result != 0 || actual != status || length != DATA.size() ||
                    output != DATA || state.op_pending.load(std::memory_order_acquire))
                    return false;
            }
        }
    }
    return true;
}

namespace {
struct NetOpenOwnerTestContext {
    unsigned calls = 0;
    int error = 0;
    unsigned mac_calls = 0;
    unsigned checks = 0;
    int check_error = 0;
};
auto net_open_owner_test_driver(ker::net::NetDevice* dev) -> int {
    auto& context = *static_cast<NetOpenOwnerTestContext*>(dev->private_data);
    ++context.calls;
    if (context.error != 0) return context.error;
    dev->state = 1;
    return 0;
}
void net_close_owner_test_driver(ker::net::NetDevice* dev) {
    auto& context = *static_cast<NetOpenOwnerTestContext*>(dev->private_data);
    ++context.calls;
    dev->state = 0;
}
}  // namespace

namespace {
auto net_link_owner_test_check(ker::net::NetDevice* dev, const uint8_t*) -> int {
    auto& context = *static_cast<NetOpenOwnerTestContext*>(dev->private_data);
    ++context.checks;
    return context.check_error;
}
void net_link_owner_test_mac(ker::net::NetDevice* dev, const uint8_t* mac) {
    auto& context = *static_cast<NetOpenOwnerTestContext*>(dev->private_data);
    ++context.mac_calls;
    std::copy_n(mac, 6, dev->mac.data());
}
}  // namespace

auto wki_remote_net_selftest_owner_l3_snapshot() -> bool {
    ProxyNetState state{};
    state.netdev.ops = &G_PROXY_NET_OPS;
    state.netdev.private_data = &state;
    auto const EMPTY = snapshot_owner_l3(&state);
    if (!EMPTY.valid || EMPTY.ipv4_addr != 0 || EMPTY.ipv4_mask != 0 || EMPTY.ipv6.count != 0) return false;
    if (snapshot_owner_l3(nullptr).valid || wki_remote_net_owner_l3_snapshot(nullptr).valid ||
        wki_remote_net_owner_l3_snapshot(&state.netdev).valid)
        return false;

    auto notify = [&](uint32_t address, uint32_t mask, const NetIpv6StateSuffix& ipv6) {
        NetStateNotifyPayload payload{};
        payload.ipv4_addr = address;
        payload.ipv4_mask = mask;
        uint64_t const FLAGS = state.netdev.control_publication_lock.lock_irqsave();
        state.lock.lock();
        apply_owner_net_state(&state, payload, ipv6);
        state.lock.unlock();
        state.netdev.control_publication_lock.unlock_irqrestore(FLAGS);
    };
    auto ipv6 = wki_net_ipv6_state_empty();
    ipv6.count = 1;
    ipv6.entries.front().address = {0x20, 0x01, 0x0d, 0xb8};
    ipv6.entries.front().prefix_len = 64;
    ipv6.entries.front().scope = wki_net_ipv6_address_scope(ipv6.entries.front().address);
    if (!wki_net_ipv6_state_valid(ipv6)) return false;
    notify(0x0a000001, 0xffffff00, ipv6);
    auto const BEFORE = snapshot_owner_l3(&state);
    if (!BEFORE.valid || BEFORE.ipv4_addr != 0x0a000001 || BEFORE.ipv4_mask != 0xffffff00 ||
        std::memcmp(&BEFORE.ipv6, &ipv6, sizeof(ipv6)) != 0)
        return false;

    // Link projection may be deferred by a control. L3 policy must still copy
    // the latest complete received record without changing local addresses.
    ker::net::NetDeviceControlGuard control;
    if (control.try_acquire(&state.netdev) != 0) return false;
    auto replacement = ipv6;
    replacement.entries.front().address.back() = 2;
    replacement.entries.front().prefix_len = 80;
    notify(0xc0000202, 0xffff0000, replacement);
    auto const AFTER = snapshot_owner_l3(&state);
    if (!AFTER.valid || AFTER.ipv4_addr != 0xc0000202 || AFTER.ipv4_mask != 0xffff0000 ||
        std::memcmp(&AFTER.ipv6, &replacement, sizeof(replacement)) != 0 || BEFORE.ipv4_addr != 0x0a000001 ||
        BEFORE.ipv4_mask != 0xffffff00 || std::memcmp(&BEFORE.ipv6, &ipv6, sizeof(ipv6)) != 0 ||
        ker::net::netif_find_by_dev(&state.netdev) != nullptr)
        return false;
    state.retiring.store(true, std::memory_order_release);
    auto const RETIRED = snapshot_owner_l3(&state);
    return !RETIRED.valid && RETIRED.ipv4_addr == 0 && RETIRED.ipv4_mask == 0 && RETIRED.ipv6.count == 0;
}

auto wki_remote_net_selftest_control_state_publication() -> bool {
    ProxyNetState state{};
    state.netdev.ops = &G_PROXY_NET_OPS;
    state.netdev.private_data = &state;
    auto notify = [&](uint16_t link, uint32_t mtu) {
        NetStateNotifyPayload payload{};
        payload.link_state = link;
        payload.mtu = mtu;
        uint64_t const FLAGS = state.netdev.control_publication_lock.lock_irqsave();
        state.lock.lock();
        apply_owner_net_state(&state, payload, wki_net_ipv6_state_empty());
        state.lock.unlock();
        state.netdev.control_publication_lock.unlock_irqrestore(FLAGS);
    };
    // Idle notifications publish before the next writer is admitted.
    notify(1, 9000);
    if (state.netdev.state != 1 || state.netdev.mtu != 9000 || state.owner_state_pending) return false;
    {
        ker::net::NetDeviceControlGuard control;
        if (control.try_acquire(&state.netdev) != 0) return false;
        notify(0, 1400);
        notify(1, 1300);
        if (state.netdev.state != 1 || state.netdev.mtu != 9000 || !state.owner_state_pending || !state.owner_mtu_pending) return false;
        // Represent the admitted request's final local field commit.
        state.netdev.state = 0;
        state.netdev.mtu = 1200;
        {
            ker::net::NetDeviceControlGuard competing;
            if (competing.try_acquire(&state.netdev) != -EBUSY) return false;
        }
        if (state.netdev.state != 0 || state.netdev.mtu != 1200 || !state.owner_state_pending) return false;
    }
    if (state.netdev.state != 1 || state.netdev.mtu != 1300 || state.owner_state_pending || state.owner_mtu_pending ||
        state.netdev.control_busy.load(std::memory_order_acquire))
        return false;
    // A later notification cannot be stranded behind the completed guard.
    notify(0, 1250);
    if (state.netdev.state != 0 || state.netdev.mtu != 1250 || state.owner_state_pending) return false;
    notify(1, 0);
    if (state.netdev.state != 1 || state.netdev.mtu != 1250) return false;
    {
        ker::net::NetDeviceControlGuard control;
        if (control.try_acquire(&state.netdev) != 0) return false;
        notify(0, 0);
        state.netdev.mtu = 1100;
    }
    if (state.netdev.state != 0 || state.netdev.mtu != 1100) return false;
    {
        ker::net::NetDeviceControlGuard control;
        if (control.try_acquire(&state.netdev) != 0) return false;
        notify(1, 1400);
        notify(0, 0);
    }
    if (state.netdev.state != 0 || state.netdev.mtu != 1400) return false;
    for (bool pending : {false, true}) {
        // Reset this unregistered fixture for the next independent scenario.
        state.netdev.control_retiring = false;
        state.retiring.store(false, std::memory_order_release);
        state.netdev.state = 1;
        auto const OWNER_STATE = state.owner_link_state;
        {
            ker::net::NetDeviceControlGuard control;
            if (control.try_acquire(&state.netdev) != 0) return false;
            if (pending) notify(1, 1500);
            state.retiring.store(true, std::memory_order_release);
            publish_retiring_net_state(&state);
            // Retirement must not race the admitted writer's field stores.
            if (state.netdev.state != 1) return false;
            state.netdev.state = 1;
            notify(1, 1600);  // A handler retained before fencing arrives late.
            if (state.owner_link_state != (pending ? 1 : OWNER_STATE)) return false;
        }
        if (state.netdev.state != 0 || state.owner_state_pending || state.owner_mtu_pending ||
            state.netdev.control_busy.load(std::memory_order_acquire))
            return false;
        notify(1, 1700);
        if (state.netdev.state != 0) return false;
    }
    state.netdev.state = 1;
    publish_retiring_net_state(&state);
    return state.netdev.state == 0;
}

auto wki_remote_net_selftest_link_identity_owner() -> bool {
    using namespace ker::mod::security;
    constexpr uint64_t BASE = wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr uint64_t ROOT_POLICY = BASE | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    struct Case {
        uint32_t uid;
        uint64_t ceiling;
        uint64_t grants;
        int same;
        int root;
    };
    constexpr std::array CASES{
        Case{0, ALL_AUTHORITY, 0, -EACCES, 0},   Case{0, 0, 0, -EACCES, -EPERM},  Case{1000, ALL_AUTHORITY, 0, -EPERM, -EPERM},
        Case{102, ALL_AUTHORITY, 2, -EACCES, 0}, Case{102, 0, 2, -EPERM, -EPERM}, Case{101, ALL_AUTHORITY, 1, -EACCES, -EPERM}};
    ker::net::NetDevice dev{};
    NetOpenOwnerTestContext context{};
    ker::net::NetDeviceOps const OPS{.open = net_open_owner_test_driver,
                                     .close = net_close_owner_test_driver,
                                     .set_mac = net_link_owner_test_mac,
                                     .check_mac = net_link_owner_test_check};
    dev.ops = &OPS;
    dev.private_data = &context;
    std::array<uint8_t, WKI_NET_LINK_IDENTITY_SIZE> bytes{};
    NetOpenIdentityHeader const HEADER{.cookie = 37, .version = WKI_NET_OPEN_IDENTITY_VERSION};
    std::memcpy(bytes.data(), &HEADER, sizeof(HEADER));
    NetLinkControlPayload control{.fields = WKI_NET_LINK_STATE | WKI_NET_LINK_MAC, .up = 1, .mac = {2, 0, 0, 0, 0, 37}};
    auto invoke = [&](const uint8_t* data, uint16_t length, uint64_t policy) {
        ker::net::NetDeviceControlGuard control_guard;
        return apply_net_link_as(&dev, data, length, policy, control_guard);
    };
    auto prepare = [&](const Snapshot& caller) {
        auto const RECORD = credential_record::encode(caller);
        std::memcpy(bytes.data() + sizeof(HEADER), RECORD.data(), RECORD.size());
        std::memcpy(bytes.data() + WKI_NET_OPEN_IDENTITY_SIZE, &control, sizeof(control));
        dev.state = control.up ? 0 : 1;
        dev.mac.bytes = {2, 0, 0, 0, 0, 11};
        context = {};
    };
    for (auto const& item : CASES) {
        Snapshot caller;
        if (Snapshot::restore_fields({.user = {item.uid, item.uid, item.uid},
                                      .group = {item.uid, item.uid, item.uid},
                                      .authority_ceiling = item.ceiling,
                                      .service_privileges = item.grants,
                                      .no_new_privileges = true,
                                      .dumpable = false,
                                      .service_privileges_locked = true},
                                     caller) != 0)
            return false;
        for (uint64_t policy : {uint64_t{0}, wki_policy_bits(WkiPolicy::REMOTE_NET), BASE, ROOT_POLICY}) {
            for (uint32_t up : {0U, 1U}) {
                control.up = up;
                prepare(caller);
                int const EXPECTED = policy == ROOT_POLICY ? item.root : policy == BASE ? item.same : -EACCES;
                int const STATUS = invoke(bytes.data(), bytes.size(), policy);
                {
                    ker::net::NetDeviceControlGuard competing;
                    if (competing.try_acquire(&dev) != 0) return false;
                    unsigned const BEFORE = context.calls;
                    unsigned const CHECKS = context.checks;
                    unsigned const MAC_CALLS = context.mac_calls;
                    if (invoke(bytes.data(), bytes.size(), policy) != (EXPECTED == 0 ? -EBUSY : EXPECTED) || context.calls != BEFORE ||
                        context.checks != CHECKS || context.mac_calls != MAC_CALLS)
                        return false;
                }
                unsigned const CALLS = EXPECTED == 0 ? 1U : 0U;
                if (STATUS != EXPECTED || context.calls != CALLS || context.mac_calls != CALLS || context.checks != CALLS ||
                    dev.state != (EXPECTED == 0 ? up : 1U - up) || dev.mac[5] != (EXPECTED == 0 ? 37 : 11))
                    return false;
            }
        }
    }
    auto const ROOT = Snapshot::bootstrap_root();
    control.up = 1;
    for (int error : {-EPERM, -ENOTSUP, -EIO, 7}) {
        prepare(ROOT);
        context.check_error = error;
        if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != (error < 0 ? error : -EIO) || context.checks != 1 || context.calls != 0 ||
            context.mac_calls != 0 || dev.state != 0 || dev.mac[5] != 11)
            return false;
    }
    for (int error : {-EACCES, -EIO, -ENODEV, 7}) {
        prepare(ROOT);
        context.error = error;
        if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != (error < 0 ? error : -EIO) || context.checks != 1 || context.calls != 1 ||
            context.mac_calls != 0 || dev.state != 0 || dev.mac[5] != 11)
            return false;
    }
    prepare(ROOT);
    for (uint16_t size = 0; size < bytes.size(); ++size)
        if (invoke(bytes.data(), size, ROOT_POLICY) != -EINVAL || context.calls != 0 || context.checks != 0) return false;
    auto const VALID = control;
    for (unsigned kind = 0; kind < 6; ++kind) {
        control = VALID;
        if (kind == 0) control.fields = 0;
        if (kind == 1) control.fields = 4;
        if (kind == 2) control.reserved = 1;
        if (kind == 3) control.reserved_tail = 1;
        if (kind == 4) control.up = 2;
        if (kind == 5) control.fields = WKI_NET_LINK_MAC;
        prepare(ROOT);
        if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != -EINVAL || context.calls != 0 || context.checks != 0) return false;
    }
    return true;
}

auto net_control_selftest_identity_owner(bool closing) -> bool {
    using namespace ker::mod::security;
    constexpr uint64_t BASE = wki_policy_bits(WkiPolicy::REMOTE_NET) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    constexpr uint64_t ROOT_POLICY = BASE | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    constexpr std::array<uint32_t, 2> GROUPS{117, 119};
    struct Case {
        uint32_t uid;
        uint64_t ceiling;
        uint64_t grant;
        int same_domain;
        int root_domain;
    };
    constexpr std::array CASES{
        Case{0, ALL_AUTHORITY, 0, -EACCES, 0},   Case{0, 0, 0, -EACCES, -EPERM},  Case{1000, ALL_AUTHORITY, 0, -EPERM, -EPERM},
        Case{102, ALL_AUTHORITY, 2, -EACCES, 0}, Case{102, 0, 2, -EPERM, -EPERM}, Case{101, ALL_AUTHORITY, 1, -EACCES, -EPERM},
    };
    ker::net::NetDevice dev{};
    NetOpenOwnerTestContext context{};
    ker::net::NetDeviceOps const OPS{.open = net_open_owner_test_driver, .close = net_close_owner_test_driver};
    dev.ops = &OPS;
    dev.private_data = &context;
    std::array<uint8_t, WKI_NET_OPEN_IDENTITY_SIZE> bytes{};
    NetOpenIdentityHeader header{.cookie = 17, .version = WKI_NET_OPEN_IDENTITY_VERSION};
    std::memcpy(bytes.data(), &header, sizeof(header));
    auto invoke = [&](const uint8_t* data, uint16_t length, uint64_t policy) {
        ker::net::NetDeviceControlGuard control_guard;
        return closing ? close_net_device_as(&dev, data, length, policy, control_guard)
                       : open_net_device_as(&dev, data, length, policy, control_guard);
    };
    for (auto const& item : CASES) {
        Snapshot supplied;
        SnapshotFields const FIELDS{.user = {item.uid, item.uid, item.uid},
                                    .group = {item.uid, item.uid, item.uid},
                                    .groups = GROUPS,
                                    .authority_ceiling = item.ceiling,
                                    .service_privileges = item.grant,
                                    .no_new_privileges = true,
                                    .dumpable = false,
                                    .service_privileges_locked = true};
        if (Snapshot::restore_fields(FIELDS, supplied) != 0) return false;
        auto const RECORD = credential_record::encode(supplied);
        std::memcpy(bytes.data() + sizeof(header), RECORD.data(), RECORD.size());
        for (uint64_t policy : {uint64_t{0}, wki_policy_bits(WkiPolicy::REMOTE_NET), BASE, ROOT_POLICY}) {
            int const EXPECTED = policy == ROOT_POLICY ? item.root_domain : policy == BASE ? item.same_domain : -EACCES;
            dev.state = closing ? 1 : 0;
            context = {};
            {
                ker::net::NetDeviceControlGuard competing;
                if (competing.try_acquire(&dev) != 0) return false;
                if (invoke(bytes.data(), bytes.size(), policy) != (EXPECTED == 0 ? -EBUSY : EXPECTED) || context.calls != 0) return false;
            }
            int const RESULT = invoke(bytes.data(), bytes.size(), policy);
            if (RESULT != EXPECTED || context.calls != (EXPECTED == 0 ? 1U : 0U) ||
                dev.state != (closing ? (EXPECTED == 0 ? 0 : 1) : (EXPECTED == 0 ? 1 : 0)))
                return false;
        }
    }
    auto const ROOT_RECORD = credential_record::encode(Snapshot::bootstrap_root());
    std::memcpy(bytes.data() + sizeof(header), ROOT_RECORD.data(), ROOT_RECORD.size());
    if (!closing)
        for (int error : {-EPERM, -EACCES, -EIO, -ENODEV, -ETIMEDOUT, 7}) {
            dev.state = closing ? 1 : 0;
            context = {.error = error};
            if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != (error < 0 ? error : -EIO) || context.calls != 1 || dev.state != 0)
                return false;
        }
    context = {};
    for (uint16_t size = 0; size < bytes.size(); ++size) {
        if (invoke(bytes.data(), size, ROOT_POLICY) != -EINVAL || context.calls != 0) return false;
    }
    header.version = 2;
    std::memcpy(bytes.data(), &header, sizeof(header));
    if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != -ENOTSUP || context.calls != 0) return false;
    header.version = WKI_NET_OPEN_IDENTITY_VERSION;
    header.reserved = 1;
    std::memcpy(bytes.data(), &header, sizeof(header));
    if (invoke(bytes.data(), bytes.size(), ROOT_POLICY) != -EINVAL || context.calls != 0) return false;
    header.reserved = 0;
    header.cookie = 0;
    std::memcpy(bytes.data(), &header, sizeof(header));
    return invoke(bytes.data(), bytes.size(), ROOT_POLICY) == -EINVAL && context.calls == 0;
}

auto wki_remote_net_selftest_open_identity_owner() -> bool { return net_control_selftest_identity_owner(false); }
auto wki_remote_net_selftest_close_identity_owner() -> bool { return net_control_selftest_identity_owner(true); }

auto wki_remote_net_selftest_completion_ownership() -> bool {
    ProxyNetState state{};
    NetOpCompletion first{};
    constexpr std::array<uint8_t, 3> DATA{0x18, 0x29, 0x3a};
    std::array<uint8_t, 3> output{0x55, 0x55, 0x55};
    uint16_t first_cookie = 0;
    if (prepare_net_op_wait(&state, OP_NET_OPEN, first, output.data(), 2, WKI_OP_TIMEOUT_US, &first_cookie) != WKI_OK) return false;
    DevOpRespPayload response{};
    response.op_id = OP_NET_OPEN;
    response.status = -EACCES;
    response.data_len = DATA.size();
    response.reserved = first_cookie;
    state.lock.lock();
    auto* writer = publish_net_op_response_locked(&state, response, DATA.data());
    auto* duplicate = publish_net_op_response_locked(&state, response, DATA.data());
    bool const DUPLICATE_PRESERVED = duplicate == nullptr && state.op_completion == &first &&
                                     state.op_pending.load(std::memory_order_acquire) && first.status == -EACCES &&
                                     first.response_length == 2 && output[0] == DATA[0] && output[1] == DATA[1] && output[2] == 0x55;
    state.lock.unlock();
    finish_claimed_waiter(writer, 0);

    // A cancellation arriving after response claim still releases its slot.
    // Its completed owner result remains immutable even when another operation
    // takes the slot before the old caller consumes or cancels again.
    cancel_op_waiter(&state, first.wait, WKI_ERR_TIMEOUT);
    NetOpCompletion next{};
    uint16_t next_cookie = 0;
    if (prepare_net_op_wait(&state, OP_NET_SET_MAC, next, nullptr, 0, WKI_OP_TIMEOUT_US, &next_cookie) != WKI_OK) return false;
    int first_status = 0;
    uint16_t first_length = 0;
    consume_net_op_result(&state, first, &first_status, &first_length);
    cancel_op_waiter(&state, first.wait, WKI_ERR_TIMEOUT);
    bool const SUCCESSOR_PRESERVED = state.op_completion == &next && state.op_wait_entry == &next.wait &&
                                     state.op_expected_id == OP_NET_SET_MAC && state.op_expected_seq == next_cookie &&
                                     state.op_pending.load(std::memory_order_acquire) && first_status == -EACCES && first_length == 2;
    response.op_id = OP_NET_SET_MAC;
    response.status = -EIO;
    response.data_len = 0;
    response.reserved = next_cookie;
    state.lock.lock();
    auto* next_writer = publish_net_op_response_locked(&state, response, nullptr);
    state.lock.unlock();
    finish_claimed_waiter(next_writer, 0);
    int next_status = 0;
    consume_net_op_result(&state, next, &next_status, nullptr);
    bool const NEXT_FINISHED = next_writer == &next.wait && next_status == -EIO && state.op_completion == nullptr &&
                               !state.op_pending.load(std::memory_order_acquire);

    NetOpCompletion timed_out{};
    output.fill(0x55);
    uint16_t timeout_cookie = 0;
    if (prepare_net_op_wait(&state, OP_NET_OPEN, timed_out, output.data(), output.size(), WKI_OP_TIMEOUT_US, &timeout_cookie) != WKI_OK)
        return false;
    bool const TIMEOUT_CLAIMED = wki_claim_op(&timed_out.wait);
    if (TIMEOUT_CLAIMED) wki_finish_claimed_op(&timed_out.wait, WKI_ERR_TIMEOUT);
    response.op_id = OP_NET_OPEN;
    response.status = 0;
    response.data_len = DATA.size();
    response.reserved = timeout_cookie;
    state.lock.lock();
    auto* late_writer = publish_net_op_response_locked(&state, response, DATA.data());
    state.lock.unlock();
    cancel_op_waiter(&state, timed_out.wait, WKI_ERR_TIMEOUT);
    bool const TIMEOUT_PRESERVED = TIMEOUT_CLAIMED && late_writer == nullptr && timed_out.wait.result == WKI_ERR_TIMEOUT &&
                                   timed_out.response_length == 0 && output == std::array<uint8_t, 3>{0x55, 0x55, 0x55} &&
                                   state.op_completion == nullptr && state.op_wait_entry == nullptr &&
                                   !state.op_pending.load(std::memory_order_acquire);
    return writer == &first.wait && DUPLICATE_PRESERVED && SUCCESSOR_PRESERVED && NEXT_FINISHED && TIMEOUT_PRESERVED;
}

auto wki_remote_net_selftest_control_send_retry() -> bool {
    constexpr std::array PRESSURE{WKI_ERR_NO_ROUTE, WKI_ERR_NO_CREDITS, WKI_ERR_NO_MEM, WKI_ERR_NOT_FOUND, WKI_ERR_BUSY, WKI_OK};
    uint64_t now = 0;
    size_t sends = 0;
    size_t pauses = 0;
    int const ACCEPTED = retry_net_control_send(
        10, [&] { return sends < PRESSURE.size() ? PRESSURE.at(sends++) : WKI_ERR_INVALID; }, [] { return true; },
        [&] {
            ++now;
            ++pauses;
        },
        [&] { return now; });
    if (ACCEPTED != WKI_OK || sends != PRESSURE.size() || pauses != PRESSURE.size() - 1) return false;

    // A successful enqueue is terminal even if the caller has time remaining.
    // Permanent errors are also terminal; do not convert them into success.
    constexpr std::array TERMINAL{WKI_OK, WKI_ERR_INVALID, WKI_ERR_PEER_FENCED, WKI_ERR_TIMEOUT, WKI_ERR_TX_FAILED};
    for (int const STATUS : TERMINAL) {
        sends = pauses = 0;
        int const RESULT = retry_net_control_send(
            10,
            [&] {
                ++sends;
                return STATUS;
            },
            [] { return true; }, [&] { ++pauses; }, [] { return uint64_t{0}; });
        if (RESULT != STATUS || sends != 1 || pauses != 0) return false;
    }

    // The deadline never restarts. A pause reaching it must not send again.
    now = sends = pauses = 0;
    int const EXPIRED = retry_net_control_send(
        3,
        [&] {
            ++sends;
            return WKI_ERR_NO_ROUTE;
        },
        [] { return true; },
        [&] {
            ++now;
            ++pauses;
        },
        [&] { return now; });
    if (EXPIRED != WKI_ERR_NO_ROUTE || sends != 3 || pauses != 3) return false;

    // A teardown that needs our retained reference must end the retry both
    // before parking and when it begins while the worker is asleep.
    for (bool const RETIRE_DURING_PAUSE : {false, true}) {
        bool live = RETIRE_DURING_PAUSE;
        sends = pauses = 0;
        int const RETIRED = retry_net_control_send(
            10,
            [&] {
                ++sends;
                return WKI_ERR_NO_CREDITS;
            },
            [&] { return live; },
            [&] {
                live = false;
                ++pauses;
            },
            [] { return uint64_t{0}; });
        if (RETIRED != WKI_ERR_NO_CREDITS || sends != 1 || pauses != static_cast<size_t>(RETIRE_DURING_PAUSE)) return false;
    }
    return true;
}

auto wki_remote_net_selftest_open_error_mapping() -> bool {
    struct Case {
        int transport;
        int error;
    };
    constexpr std::array CASES{
        Case{WKI_OK, 0},
        Case{WKI_ERR_NO_MEM, -ENOMEM},
        Case{WKI_ERR_NO_ROUTE, -EHOSTUNREACH},
        Case{WKI_ERR_PEER_FENCED, -ENOTCONN},
        Case{WKI_ERR_NO_CREDITS, -EAGAIN},
        Case{WKI_ERR_TIMEOUT, -ETIMEDOUT},
        Case{WKI_ERR_INVALID, -EINVAL},
        Case{WKI_ERR_NOT_FOUND, -EHOSTUNREACH},
        Case{WKI_ERR_BUSY, -EAGAIN},
        Case{WKI_ERR_TX_FAILED, -EIO},
        Case{-100, -EIO},
        Case{100, -EIO},
    };
    for (auto const& item : CASES) {
        if (net_open_transport_errno(item.transport) != item.error) return false;
    }
    // Exercise an actual bounded wait timeout rather than sleeping through an
    // entire network operation timeout or requiring a live peer.
    WkiWaitEntry wait{};
    int const RESULT = wki_wait_for_op(&wait, 1);
    return RESULT == WKI_ERR_TIMEOUT && net_open_transport_errno(RESULT) == -ETIMEDOUT &&
           wait.state.load(std::memory_order_acquire) == WkiWaitEntry::DONE;
}

auto wki_remote_net_selftest_open_transport_failures() -> bool {
    ProxyNetState state{};
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    bool const UNSUPPORTED = open_retained_net_proxy(&state, ROOT) == -ENOTSUP;
    state.open_identity_supported = true;
    state.max_op_size = WKI_NET_OPEN_IDENTITY_SIZE;
    state.netdev.state = 0;
    state.netdev.link_flags = 0x100;
    state.lock.lock();
    static_cast<void>(set_net_rx_credits_locked(&state, 17));
    state.lock.unlock();
    bool const MISSING = proxy_net_open_as(&state.netdev, ROOT) == -ENODEV;

    state.retiring.store(true, std::memory_order_release);
    bool const FENCED = open_retained_net_proxy(&state, ROOT) == -ENOTCONN;
    state.retiring.store(false, std::memory_order_release);
    // A null exact-channel identity fails inside the real send entrypoint,
    // before lookup, allocation or transmission. Cancellation must drain the
    // stack waiter and release the operation slot.
    bool const INVALID_CHANNEL = open_retained_net_proxy(&state, ROOT) == -EINVAL;
    bool const UNCHANGED = state.netdev.state == 0 && state.netdev.link_flags == 0x100 && state.rx_credits_remaining == 17 &&
                           state.op_wait_entry == nullptr && !state.op_pending.load(std::memory_order_acquire) &&
                           state.op_expected_id == 0 && state.op_expected_seq == 0;
    return UNSUPPORTED && MISSING && FENCED && INVALID_CHANNEL && UNCHANGED;
}

auto wki_remote_net_selftest_close_transport_failures() -> bool {
    ProxyNetState state{};
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    bool const UNSUPPORTED = close_retained_net_proxy(&state, ROOT) == -ENOTSUP;
    state.close_identity_supported = true;
    state.max_op_size = WKI_NET_CLOSE_IDENTITY_SIZE;
    state.netdev.state = 1;
    state.netdev.link_flags = 0x100;
    state.lock.lock();
    static_cast<void>(set_net_rx_credits_locked(&state, 17));
    state.lock.unlock();
    bool const MISSING = proxy_net_close_as(&state.netdev, ROOT) == -ENODEV;

    state.retiring.store(true, std::memory_order_release);
    bool const FENCED = close_retained_net_proxy(&state, ROOT) == -ENOTCONN;
    state.retiring.store(false, std::memory_order_release);
    // A null exact-channel identity fails inside the real send entrypoint,
    // before lookup, allocation or transmission. Cancellation must drain the
    // stack waiter and release the operation slot.
    bool const INVALID_CHANNEL = close_retained_net_proxy(&state, ROOT) == -EINVAL;
    bool const UNCHANGED = state.netdev.state == 1 && state.netdev.link_flags == 0x100 && state.rx_credits_remaining == 17 &&
                           state.op_wait_entry == nullptr && !state.op_pending.load(std::memory_order_acquire) &&
                           state.op_expected_id == 0 && state.op_expected_seq == 0;
    return UNSUPPORTED && MISSING && FENCED && INVALID_CHANNEL && UNCHANGED;
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
