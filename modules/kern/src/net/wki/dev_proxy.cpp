#include "dev_proxy.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <memory>
#include <net/backlog.hpp>
#include <net/netpoll.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <util/hcf.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
std::deque<std::unique_ptr<ProxyBlockState>> g_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<ProxyBlockState*> g_routable_proxies;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyBlockState* g_pending_detach_head = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ProxyBlockState* g_pending_detach_tail = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_dev_proxy_initialized = false;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_proxy_lock;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint8_t g_block_attach_next_cookie = 1;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr size_t MAX_PROXY_SCRATCH = 64;
constexpr size_t BLOCK_DETACH_RETRY_BATCH = 32;
constexpr size_t BLOCK_DETACH_RETRY_SCAN = BLOCK_DETACH_RETRY_BATCH * 2;
constexpr uint64_t DEV_PROXY_CONTENTION_SLEEP_US = 1000;
constexpr uint64_t DEV_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_DEV_PROXY_TIMEOUT_US;
constexpr uint64_t DEV_PROXY_BULK_WAIT_TIMEOUT_US = wki_saturating_mul_us(WKI_DEV_PROXY_TIMEOUT_US, 4);

auto tag_completion(ProxyBlockState* state, uint32_t tag) -> ProxyBlockState::TagCompletion& { return state->tag_completions.at(tag); }

auto tag_in_use(ProxyBlockState const* state, uint32_t tag) -> bool {
    return tag < ProxyBlockState::TAG_POOL_SIZE && (state->tag_bitmap & (1ULL << tag)) != 0;
}

auto proxy_block_active(ProxyBlockState const* state) -> bool { return state != nullptr && state->active.load(std::memory_order_acquire); }

auto proxy_block_fenced(ProxyBlockState const* state) -> bool { return state != nullptr && state->fenced.load(std::memory_order_acquire); }

void set_proxy_block_active(ProxyBlockState* state, bool value) { state->active.store(value, std::memory_order_release); }

void set_proxy_block_fenced(ProxyBlockState* state, bool value) { state->fenced.store(value, std::memory_order_release); }

auto block_proxy_op_slot_busy(ProxyBlockState* state) -> bool { return state->op_pending.load(std::memory_order_acquire); }

auto channel_identity_equal(const WkiChannelIdentity& lhs, const WkiChannelIdentity& rhs) -> bool {
    return lhs.channel == rhs.channel && lhs.peer_node_id == rhs.peer_node_id && lhs.channel_id == rhs.channel_id &&
           lhs.generation == rhs.generation;
}

auto acquire_block_op_slot_locked(ProxyBlockState* state, uint64_t start_us) -> int {
    uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, DEV_PROXY_SLOT_WAIT_TIMEOUT_US);
    while (true) {
        state->lock.lock();
        if (!block_proxy_op_slot_busy(state)) {
            return WKI_OK;
        }
        state->lock.unlock();
        if (!block_proxy_op_slot_busy(state)) {
            continue;
        }
        if (wki_now_us() >= DEADLINE_US) {
            return WKI_ERR_TIMEOUT;
        }
        ker::mod::sched::kern_sleep_us(DEV_PROXY_CONTENTION_SLEEP_US);
    }
}

// Caller must hold s_proxy_lock.
auto find_proxy_by_bdev(ker::dev::BlockDevice* bdev) -> ProxyBlockState* {
    for (auto* state : g_routable_proxies) {
        if (proxy_block_active(state) && &state->bdev == bdev) {
            return state;
        }
    }
    return nullptr;
}

// Caller must hold s_proxy_lock.
auto find_proxy_by_channel(const WkiChannelIdentity& identity) -> ProxyBlockState* {
    for (auto* state : g_routable_proxies) {
        state->lock.lock();
        bool const MATCHES = (proxy_block_active(state) || block_proxy_op_slot_busy(state)) && state->owner_node == identity.peer_node_id &&
                             channel_identity_equal(state->assigned_channel_identity, identity);
        state->lock.unlock();
        if (MATCHES) {
            return state;
        }
    }
    return nullptr;
}

// Caller must hold s_proxy_lock.
auto find_proxy_by_attach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie) -> ProxyBlockState* {
    for (auto* state : g_routable_proxies) {
        if (state->owner_node != owner_node || state->resource_id != resource_id) {
            continue;
        }
        state->lock.lock();
        bool const MATCHES = state->attach_pending.load(std::memory_order_acquire) && state->attach_expected_cookie == attach_cookie;
        state->lock.unlock();
        if (MATCHES) {
            return state;
        }
    }
    return nullptr;
}

void clear_block_op_state_locked(ProxyBlockState* state, int status);

// Caller must hold s_proxy_lock.
void unindex_proxy_locked(ProxyBlockState* state) {
    for (auto it = g_routable_proxies.begin(); it != g_routable_proxies.end(); ++it) {
        if (*it == state) {
            g_routable_proxies.erase(it);
            return;
        }
    }
}

// Caller must hold s_proxy_lock.
auto erase_proxy_exact_locked(ProxyBlockState* state) -> bool {
    for (auto it = g_proxies.begin(); it != g_proxies.end(); ++it) {
        if (it->get() == state) {
            if (state->ever_published || state->detach_pending || state->cleanup_in_progress) {
                return false;
            }
            unindex_proxy_locked(state);
            g_proxies.erase(it);
            return true;
        }
    }
    return false;
}

auto erase_proxy_exact(ProxyBlockState* state) -> bool {
    s_proxy_lock.lock();
    bool const ERASED = erase_proxy_exact_locked(state);
    s_proxy_lock.unlock();
    return ERASED;
}

auto proxy_count() -> size_t {
    s_proxy_lock.lock();
    size_t const COUNT = g_routable_proxies.size();
    s_proxy_lock.unlock();
    return COUNT;
}

auto block_channel_identity_snapshot(ProxyBlockState* state) -> WkiChannelIdentity {
    if (state == nullptr) {
        return {};
    }
    state->lock.lock();
    WkiChannelIdentity const IDENTITY = state->assigned_channel_identity;
    state->lock.unlock();
    return IDENTITY;
}

enum class BlockAttachLeaseTryResult : uint8_t {
    ACQUIRED,
    BUSY,
    STALE,
};

class BlockAttachIdentityLease {
   public:
    BlockAttachIdentityLease() = default;
    BlockAttachIdentityLease(uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                             const ResourceIncarnationToken& expected_owner_incarnation, const WkiChannelIdentity* expected_channel) {
        if (expected_resource_generation == 0) {
            return;
        }

        peer_ = wki_peer_find(owner_node);
        if (!wki_peer_lifecycle_acquire(peer_)) {
            peer_ = nullptr;
            return;
        }

        if (!identity_is_live(peer_, owner_node, resource_id, expected_resource_generation, expected_owner_incarnation, expected_channel)) {
            wki_peer_lifecycle_release(peer_);
            peer_ = nullptr;
        }
    }

    BlockAttachIdentityLease(const BlockAttachIdentityLease&) = delete;
    auto operator=(const BlockAttachIdentityLease&) -> BlockAttachIdentityLease& = delete;
    ~BlockAttachIdentityLease() { wki_peer_lifecycle_release(peer_); }

    explicit operator bool() const { return peer_ != nullptr; }
    auto try_acquire(uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                     const ResourceIncarnationToken& expected_owner_incarnation, const WkiChannelIdentity* expected_channel)
        -> BlockAttachLeaseTryResult {
        if (peer_ != nullptr) {
            return BlockAttachLeaseTryResult::ACQUIRED;
        }
        if (expected_resource_generation == 0) {
            return BlockAttachLeaseTryResult::STALE;
        }

        WkiPeer* const PEER = wki_peer_find(owner_node);
        if (!wki_peer_lifecycle_try_acquire(PEER)) {
            return BlockAttachLeaseTryResult::BUSY;
        }
        if (!identity_is_live(PEER, owner_node, resource_id, expected_resource_generation, expected_owner_incarnation, expected_channel)) {
            wki_peer_lifecycle_release(PEER);
            return BlockAttachLeaseTryResult::STALE;
        }
        peer_ = PEER;
        return BlockAttachLeaseTryResult::ACQUIRED;
    }

   private:
    static auto identity_is_live(WkiPeer const* peer, uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                                 const ResourceIncarnationToken& expected_owner_incarnation, const WkiChannelIdentity* expected_channel)
        -> bool {
        bool const PEER_LIVE = peer != nullptr && peer->node_id == owner_node && peer->state == PeerState::CONNECTED &&
                               !peer->vfs_reset_rebind_pending.load(std::memory_order_acquire);
        bool const RESOURCE_LIVE = wki_resource_observation_is_live(owner_node, ResourceType::BLOCK, resource_id,
                                                                    expected_resource_generation, expected_owner_incarnation);
        bool const CHANNEL_LIVE =
            expected_channel == nullptr || (expected_channel->peer_node_id == owner_node && expected_channel->channel != nullptr &&
                                            wki_channel_generation_is_live(expected_channel->channel, owner_node,
                                                                           expected_channel->channel_id, expected_channel->generation));
        return PEER_LIVE && RESOURCE_LIVE && CHANNEL_LIVE;
    }

    WkiPeer* peer_ = nullptr;
};

auto block_attach_identity_is_live(uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                                   const ResourceIncarnationToken& expected_owner_incarnation, const WkiChannelIdentity* expected_channel)
    -> bool {
    BlockAttachIdentityLease const LEASE(owner_node, resource_id, expected_resource_generation, expected_owner_incarnation,
                                         expected_channel);
    return static_cast<bool>(LEASE);
}

auto send_block_detach(uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                       const ResourceIncarnationToken& resource_incarnation, WkiReliableTxToken* tx_token_out) -> int {
    constexpr size_t DETACH_MAX_SIZE = wki_dev_detach_payload_size(true);
    std::array<uint8_t, DETACH_MAX_SIZE> det_buf{};
    auto* det = reinterpret_cast<DevDetachPayload*>(det_buf.data());
    det->target_node = owner_node;
    det->resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    det->resource_id = resource_id;
    det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie;

    bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(owner_node, ResourceType::BLOCK);
    if (WITH_INCARNATION) {
        if (!wki_resource_incarnation_valid(resource_incarnation)) {
            return WKI_ERR_INVALID;
        }
        std::memcpy(det_buf.data() + WKI_DEV_DETACH_INCARNATION_OFFSET, &resource_incarnation, sizeof(resource_incarnation));
    }
    uint16_t const DETACH_SIZE = wki_dev_detach_payload_size(WITH_INCARNATION);
    return wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data(), DETACH_SIZE, tx_token_out);
}

struct BlockDetachAttempt {
    ProxyBlockState* state = nullptr;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint8_t attach_cookie = 0;
    ResourceIncarnationToken incarnation = {};
    uint32_t peer_boot_epoch = 0;
    WkiReliableTxToken tx_token = {};
};

auto block_detach_incarnation_equal(const ResourceIncarnationToken& lhs, const ResourceIncarnationToken& rhs) -> bool {
    bool const LHS_VALID = wki_resource_incarnation_valid(lhs);
    bool const RHS_VALID = wki_resource_incarnation_valid(rhs);
    return LHS_VALID == RHS_VALID && (!LHS_VALID || wki_resource_incarnation_equal(lhs, rhs));
}

// Caller must hold s_proxy_lock.
void link_pending_block_detach_locked(ProxyBlockState* state) {
    state->detach_prev = g_pending_detach_tail;
    state->detach_next = nullptr;
    if (g_pending_detach_tail != nullptr) {
        g_pending_detach_tail->detach_next = state;
    } else {
        g_pending_detach_head = state;
    }
    g_pending_detach_tail = state;
}

// Caller must hold s_proxy_lock.
void unlink_pending_block_detach_locked(ProxyBlockState* state) {
    if (state->detach_prev != nullptr) {
        state->detach_prev->detach_next = state->detach_next;
    } else {
        g_pending_detach_head = state->detach_next;
    }
    if (state->detach_next != nullptr) {
        state->detach_next->detach_prev = state->detach_prev;
    } else {
        g_pending_detach_tail = state->detach_prev;
    }
    state->detach_prev = nullptr;
    state->detach_next = nullptr;
}

// Caller must hold s_proxy_lock.
auto pending_block_detach_matches_locked(const ProxyBlockState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                                         const ResourceIncarnationToken& incarnation) -> bool {
    return state->detach_pending && state->detach_owner_node == owner_node && state->detach_resource_id == resource_id &&
           state->detach_attach_cookie == attach_cookie && block_detach_incarnation_equal(state->detach_incarnation, incarnation);
}

// Caller must hold s_proxy_lock. An indexed inactive row is either still
// attaching or awaiting failed-publication cleanup, so it can already own a
// server binding even before cleanup has staged its exact tuple. A fenced
// published row likewise represents an owner binding until reconnect
// retirement is ACKed.
auto block_attach_blocked_by_retiring_binding_locked(uint16_t owner_node, uint32_t resource_id) -> bool {
    for (auto* state = g_pending_detach_head; state != nullptr; state = state->detach_next) {
        if (state->detach_owner_node == owner_node && state->detach_resource_id == resource_id) {
            return true;
        }
    }

    return std::ranges::any_of(g_routable_proxies, [owner_node, resource_id](ProxyBlockState const* state) -> bool {
        if (state == nullptr || state->owner_node != owner_node || state->resource_id != resource_id) {
            return false;
        }
        bool const ACTIVE = proxy_block_active(state);
        return !ACTIVE || state->cleanup_in_progress || state->epoch_reset_pending || state->resume_pending || state->resume_in_progress ||
               state->resume_after_detach || (ACTIVE && proxy_block_fenced(state));
    });
}

// Caller must hold s_proxy_lock. A deferred-only reservation starts idle so
// the task-context retry worker can claim it after teardown releases locks.
auto stage_block_detach_locked(ProxyBlockState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                               const ResourceIncarnationToken& incarnation, bool initial_attempt_in_progress) -> bool {
    if (state == nullptr || attach_cookie == 0) {
        return false;
    }
    if (state->detach_pending) {
        bool const SAME = pending_block_detach_matches_locked(state, owner_node, resource_id, attach_cookie, incarnation);
        if (!SAME) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI block proxy: overlapping detach idempotence tuples");
            hcf();
        }
        return false;
    }

    state->detach_pending = true;
    state->detach_retry_in_progress = initial_attempt_in_progress;
    state->detach_owner_node = owner_node;
    state->detach_resource_id = resource_id;
    state->detach_attach_cookie = attach_cookie;
    state->detach_incarnation = incarnation;
    state->detach_peer_boot_epoch =
        wki_resource_incarnation_valid(incarnation) ? incarnation.owner_boot_epoch : state->binding_peer_boot_epoch;
    state->detach_tx_token = {};
    link_pending_block_detach_locked(state);
    return true;
}

auto stage_block_detach(ProxyBlockState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                        const ResourceIncarnationToken& incarnation) -> bool {
    s_proxy_lock.lock();
    bool const STAGED = stage_block_detach_locked(state, owner_node, resource_id, attach_cookie, incarnation, false);
    s_proxy_lock.unlock();
    return STAGED;
}

void finish_block_detach_attempt(const BlockDetachAttempt& attempt, WkiReliableTxStatus tx_status, int send_result,
                                 const WkiReliableTxToken& replacement_token, bool peer_epoch_invalidated = false) {
    if (attempt.state == nullptr) {
        return;
    }

    bool notify_resume = false;
    uint16_t resume_owner_node = WKI_NODE_INVALID;
    s_proxy_lock.lock();
    ProxyBlockState* const state = attempt.state;
    if (!pending_block_detach_matches_locked(state, attempt.owner_node, attempt.resource_id, attempt.attach_cookie, attempt.incarnation) ||
        !state->detach_retry_in_progress) {
        s_proxy_lock.unlock();
        return;
    }

    if (tx_status == WkiReliableTxStatus::ACKED || peer_epoch_invalidated) {
        unlink_pending_block_detach_locked(state);
        state->detach_pending = false;
        state->detach_retry_in_progress = false;
        state->detach_owner_node = WKI_NODE_INVALID;
        state->detach_resource_id = 0;
        state->detach_attach_cookie = 0;
        state->detach_incarnation = {};
        state->detach_peer_boot_epoch = 0;
        state->detach_tx_token = {};
        if (state->resume_after_detach) {
            state->resume_after_detach = false;
            bool const CAN_RESUME =
                proxy_block_active(state) && proxy_block_fenced(state) && !state->epoch_reset_pending && !state->cleanup_in_progress;
            state->resume_detach_confirmed = CAN_RESUME;
            if (CAN_RESUME && !state->resume_in_progress) {
                state->resume_pending = true;
                notify_resume = true;
                resume_owner_node = state->owner_node;
            }
        }
        static_cast<void>(erase_proxy_exact_locked(state));
    } else if (tx_status == WkiReliableTxStatus::PENDING) {
        state->detach_retry_in_progress = false;
    } else {
        state->detach_tx_token = send_result == WKI_OK ? replacement_token : WkiReliableTxToken{};
        state->detach_retry_in_progress = false;
    }
    s_proxy_lock.unlock();

    if (notify_resume) {
        WkiPeer* const PEER = wki_peer_find(resume_owner_node);
        if (PEER != nullptr && PEER->node_id == resume_owner_node) {
            PEER->block_resume_pending.store(true, std::memory_order_release);
            wki_timer_notify();
        }
    }
}

auto send_or_defer_block_detach(ProxyBlockState* state, uint16_t owner_node, uint32_t resource_id, uint8_t attach_cookie,
                                const ResourceIncarnationToken& incarnation) -> int {
    bool const STAGED = stage_block_detach(state, owner_node, resource_id, attach_cookie, incarnation);
    wki_deferred_work_notify();
    return STAGED ? WKI_OK : WKI_ERR_BUSY;
}

void cleanup_failed_block_attach(ProxyBlockState* state, const WkiChannelIdentity& channel_to_close, bool send_detach) {
    if (state == nullptr) {
        return;
    }

    uint16_t owner_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint32_t rdma_zone_id = 0;
    uint8_t attach_cookie = 0;
    ResourceIncarnationToken detach_incarnation = {};
    uint8_t* ra_buf = nullptr;
    uint8_t* bulk_buf = nullptr;

    s_proxy_lock.lock();
    state->cleanup_in_progress = true;
    state->lock.lock();
    owner_node = state->owner_node;
    resource_id = state->resource_id;
    rdma_zone_id = state->rdma_zone_id;
    attach_cookie = state->binding_attach_cookie;
    detach_incarnation =
        wki_resource_incarnation_valid(state->binding_incarnation) ? state->binding_incarnation : state->attach_expected_incarnation;
    ra_buf = state->ra_buffer;
    bulk_buf = state->bulk_staging_buf;
    if (send_detach) {
        // Reserve the exact possible owner binding before this failed state is
        // made inactive. A replacement attach checks this list under the same
        // registry lock, so it cannot cross the teardown/staging boundary.
        static_cast<void>(stage_block_detach_locked(state, owner_node, resource_id, attach_cookie, detach_incarnation, false));
    }
    state->attach_wait_entry = nullptr;
    state->attach_expected_cookie = 0;
    state->binding_attach_cookie = 0;
    state->attach_expect_incarnation = false;
    state->attach_expected_incarnation = {};
    state->binding_incarnation = {};
    state->assigned_channel = 0;
    state->assigned_channel_identity = {};
    state->attach_read_only = false;
    state->attach_pending.store(false, std::memory_order_release);
    state->op_wait_entry = nullptr;
    clear_block_op_state_locked(state, WKI_ERR_PEER_FENCED);
    state->ra_buffer = nullptr;
    state->bulk_staging_buf = nullptr;
    state->bulk_capable = false;
    state->bulk_staging_rkey = 0;
    state->bulk_staging_size = 0;
    state->bulk_max_transfer = 0;
    state->rdma_attached = false;
    state->rdma_zone_ptr = nullptr;
    state->rdma_zone_id = 0;
    state->rdma_roce = false;
    state->rdma_transport = nullptr;
    state->rdma_remote_rkey = 0;
    ker::dev::block_device_set_read_only(&state->bdev, false);
    set_proxy_block_active(state, false);
    state->lock.unlock();
    s_proxy_lock.unlock();

    static_cast<void>(wki_channel_close_generation(channel_to_close.channel, channel_to_close.peer_node_id, channel_to_close.channel_id,
                                                   channel_to_close.generation));
    if (rdma_zone_id != 0) {
        wki_zone_destroy(rdma_zone_id);
    }
    delete[] ra_buf;
    delete[] bulk_buf;

    s_proxy_lock.lock();
    state->cleanup_in_progress = false;
    bool const DETACH_PENDING = state->detach_pending;
    s_proxy_lock.unlock();
    erase_proxy_exact(state);
    if (DETACH_PENDING) {
        wki_deferred_work_notify();
    }
}

// Caller must hold s_proxy_lock. The server's idempotence key includes this
// owner/resource/incarnation/cookie tuple, so skip any cookie still represented
// by a local proxy for the same resource incarnation.
auto allocate_block_attach_cookie_locked(uint16_t owner_node, uint32_t resource_id, const ResourceIncarnationToken& owner_incarnation)
    -> uint8_t {
    for (uint16_t attempt = 0; attempt < UINT8_MAX; ++attempt) {
        uint8_t cookie = g_block_attach_next_cookie;
        if (cookie == 0) {
            cookie = 1;
        }
        g_block_attach_next_cookie = static_cast<uint8_t>(cookie + 1U);
        if (g_block_attach_next_cookie == 0) {
            g_block_attach_next_cookie = 1;
        }

        bool reserved = false;
        for (auto* state : g_routable_proxies) {
            if (state == nullptr || state->owner_node != owner_node || state->resource_id != resource_id) {
                continue;
            }
            state->lock.lock();
            ResourceIncarnationToken const& EXISTING_INCARNATION = wki_resource_incarnation_valid(state->binding_incarnation)
                                                                       ? state->binding_incarnation
                                                                       : state->attach_expected_incarnation;
            reserved = state->binding_attach_cookie == cookie &&
                       (!wki_resource_incarnation_valid(owner_incarnation) || !wki_resource_incarnation_valid(EXISTING_INCARNATION) ||
                        wki_resource_incarnation_equal(EXISTING_INCARNATION, owner_incarnation));
            state->lock.unlock();
            if (reserved) {
                break;
            }
        }
        if (!reserved) {
            for (auto* state = g_pending_detach_head; state != nullptr; state = state->detach_next) {
                if (state->detach_owner_node != owner_node || state->detach_resource_id != resource_id ||
                    state->detach_attach_cookie != cookie) {
                    continue;
                }
                ResourceIncarnationToken const& PENDING_INCARNATION = state->detach_incarnation;
                reserved = !wki_resource_incarnation_valid(owner_incarnation) || !wki_resource_incarnation_valid(PENDING_INCARNATION) ||
                           wki_resource_incarnation_equal(PENDING_INCARNATION, owner_incarnation);
                if (reserved) {
                    break;
                }
            }
        }
        if (!reserved) {
            return cookie;
        }
    }
    return 0;
}

// Caller must hold the owning ProxyBlockState lock.
auto block_attach_ack_matches_pending_locked(ProxyBlockState const* state, const DevAttachAckPayload& ack, const uint8_t* payload,
                                             uint16_t payload_len) -> bool {
    if (state == nullptr || payload == nullptr || !state->attach_pending.load(std::memory_order_acquire) ||
        state->attach_expected_cookie == 0 || !wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)) {
        return false;
    }

    size_t const EXPECTED_SIZE = sizeof(DevAttachAckPayload) + (state->attach_expect_incarnation ? sizeof(ResourceIncarnationToken) : 0);
    if (payload_len != EXPECTED_SIZE) {
        return false;
    }
    if (!state->attach_expect_incarnation || ack.status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        return true;
    }

    ResourceIncarnationToken ack_incarnation = {};
    std::memcpy(&ack_incarnation, payload + sizeof(DevAttachAckPayload), sizeof(ack_incarnation));
    return wki_resource_incarnation_equal(ack_incarnation, state->attach_expected_incarnation);
}

void clear_block_op_state_locked(ProxyBlockState* state, int status) {
    state->op_status = static_cast<int16_t>(status);
    state->op_expected_id = 0;
    state->op_expected_seq = 0;
    state->op_resp_buf = nullptr;
    state->op_resp_max = 0;
    state->op_resp_len = 0;
    state->op_pending.store(false, std::memory_order_release);
}

auto peek_channel_tx_seq16(const WkiChannelIdentity& identity, uint16_t* seq_out) -> bool {
    if (seq_out == nullptr || identity.channel == nullptr || identity.generation == 0) {
        return false;
    }

    WkiChannel* const ch = identity.channel;
    ch->lock.lock();
    if (!ch->active || ch->peer_node_id != identity.peer_node_id || ch->channel_id != identity.channel_id ||
        ch->generation != identity.generation) {
        ch->lock.unlock();
        return false;
    }
    *seq_out = static_cast<uint16_t>(ch->tx_seq & UINT16_MAX);
    ch->lock.unlock();
    return true;
}

auto prepare_block_op_wait(ProxyBlockState* state, uint16_t op_id, WkiWaitEntry& wait, void* resp_buf, uint16_t resp_max,
                           WkiChannelIdentity* channel_identity_out) -> int {
    if (channel_identity_out == nullptr) {
        return WKI_ERR_INVALID;
    }
    int const SLOT_RET = acquire_block_op_slot_locked(state, wki_now_us());
    if (SLOT_RET != WKI_OK) {
        return SLOT_RET;
    }

    uint16_t expected_seq = 0;
    WkiChannelIdentity const CHANNEL_IDENTITY = state->assigned_channel_identity;
    if (!peek_channel_tx_seq16(CHANNEL_IDENTITY, &expected_seq)) {
        state->lock.unlock();
        return WKI_ERR_NOT_FOUND;
    }

    state->op_wait_entry = &wait;
    state->op_expected_id = op_id;
    state->op_expected_seq = expected_seq;
    state->op_status = 0;
    state->op_resp_buf = resp_buf;
    state->op_resp_max = resp_max;
    state->op_resp_len = 0;
    state->op_pending.store(true, std::memory_order_release);
    *channel_identity_out = CHANNEL_IDENTITY;
    state->lock.unlock();
    return WKI_OK;
}

void consume_block_op_result(ProxyBlockState* state, WkiWaitEntry& wait, int* status_out, uint16_t* resp_len_out) {
    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    int const STATUS = static_cast<int>(state->op_status);
    uint16_t const RESP_LEN = state->op_resp_len;
    clear_block_op_state_locked(state, STATUS);
    state->lock.unlock();

    if (status_out != nullptr) {
        *status_out = STATUS;
    }
    if (resp_len_out != nullptr) {
        *resp_len_out = RESP_LEN;
    }
}

// Caller must hold the owning ProxyBlockState lock.
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

void cancel_op_waiter(ProxyBlockState* state, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    state->lock.lock();
    if (state->op_wait_entry == &wait) {
        state->op_wait_entry = nullptr;
    }
    clear_block_op_state_locked(state, result);
    claimed = wki_claim_op(&wait);
    state->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void cancel_attach_waiter(ProxyBlockState* state, WkiWaitEntry& wait, int result) {
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

void clear_attach_waiter_after_wait(ProxyBlockState* state, WkiWaitEntry& wait) {
    state->lock.lock();
    if (state->attach_wait_entry == &wait) {
        state->attach_wait_entry = nullptr;
        state->attach_expected_cookie = 0;
    }
    state->lock.unlock();
}

// -----------------------------------------------------------------------------
// RDMA ring helpers
// -----------------------------------------------------------------------------

// Allocate a data slot from the bitmap. Returns slot index or -1 if none free.
auto rdma_alloc_slot(ProxyBlockState* state) -> int {
    uint32_t const MAX_SLOTS = BLK_RING_DEFAULT_DATA_SLOTS < 64 ? BLK_RING_DEFAULT_DATA_SLOTS : 64;
    for (uint32_t i = 0; i < MAX_SLOTS; i++) {
        if ((state->data_slot_bitmap & (1ULL << i)) == 0) {
            state->data_slot_bitmap |= (1ULL << i);
            return static_cast<int>(i);
        }
    }
    return -1;
}

void rdma_free_slot(ProxyBlockState* state, uint32_t slot) { state->data_slot_bitmap &= ~(1ULL << slot); }

// Allocate a tag from the bitmap pool. Returns tag index (0..63) or -1 if none free.
auto rdma_alloc_tag(ProxyBlockState* state) -> int {
    for (uint32_t i = 0; i < ProxyBlockState::TAG_POOL_SIZE; i++) {
        if ((state->tag_bitmap & (1ULL << i)) == 0) {
            state->tag_bitmap |= (1ULL << i);
            auto& completion = tag_completion(state, i);
            completion.pending = true;
            completion.completed = false;
            return static_cast<int>(i);
        }
    }
    return -1;
}

void rdma_free_tag(ProxyBlockState* state, uint32_t tag) {
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        state->tag_bitmap &= ~(1ULL << tag);
        auto& completion = tag_completion(state, tag);
        completion.pending = false;
        completion.completed = false;
    }
}

void roce_write_header_u32(ProxyBlockState* state, uint64_t offset, uint32_t value) {
    state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, offset, &value, sizeof(value));
}

void roce_read_header_u32(ProxyBlockState* state, uint64_t offset, uint32_t* value) {
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, offset, value, sizeof(*value));
}

// RoCE helper: push SQ entries and consumer-owned indices to the server.
void roce_push_sq(ProxyBlockState* state) {
    if (!state->rdma_roce || state->rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    uint32_t const SQ_OFF = blk_ring_sq_offset();
    uint32_t const SQ_SIZE = blk_ring_sq_size(hdr->sq_depth);
    state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, SQ_OFF,
                                      static_cast<uint8_t*>(state->rdma_zone_ptr) + SQ_OFF, SQ_SIZE);

    // sq_head and cq_tail are consumer-owned.  Do not overwrite the server's
    // sq_tail/cq_head with stale copies from our local header.
    roce_write_header_u32(state, __builtin_offsetof(BlkRingHeader, cq_tail), hdr->cq_tail);
    roce_write_header_u32(state, __builtin_offsetof(BlkRingHeader, sq_head), hdr->sq_head);
}

// RoCE helper: push a data slot to the server (for WRITE ops - data must be visible before SQE).
void roce_push_data_slot(ProxyBlockState* state, uint32_t slot, uint32_t bytes) {
    if (!state->rdma_roce || state->rdma_transport == nullptr || bytes == 0) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    uint32_t const SLOT_OFFSET = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (slot * hdr->data_slot_size);
    state->rdma_transport->rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, SLOT_OFFSET,
                                      blk_data_slot(state->rdma_zone_ptr, hdr, slot), bytes);
}

// RoCE helper: pull CQ region and server-owned indices from server to see completions.
void roce_pull_cq(ProxyBlockState* state) {
    if (!state->rdma_roce || state->rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);

    // Pull CQ entries
    uint32_t const CQ_OFF = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t const CQ_TOTAL = blk_ring_cq_size(hdr->cq_depth);
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, CQ_OFF,
                                     static_cast<uint8_t*>(state->rdma_zone_ptr) + CQ_OFF, CQ_TOTAL);

    // sq_tail and cq_head are server-owned.  Leave our sq_head/cq_tail intact.
    uint32_t sq_tail = hdr->sq_tail;
    uint32_t cq_head = hdr->cq_head;
    roce_read_header_u32(state, __builtin_offsetof(BlkRingHeader, sq_tail), &sq_tail);
    roce_read_header_u32(state, __builtin_offsetof(BlkRingHeader, cq_head), &cq_head);
    hdr->sq_tail = sq_tail;
    hdr->cq_head = cq_head;
}

// RoCE helper: pull a data slot from server (for READ ops - data filled by server).
void roce_pull_data_slot(ProxyBlockState* state, uint32_t slot, uint32_t bytes) {
    if (!state->rdma_roce || state->rdma_transport == nullptr || bytes == 0) {
        return;
    }
    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    uint32_t const SLOT_OFFSET = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (slot * hdr->data_slot_size);
    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, SLOT_OFFSET,
                                     blk_data_slot(state->rdma_zone_ptr, hdr, slot), bytes);
}

// -----------------------------------------------------------------------------
// Read-ahead cache helpers
// -----------------------------------------------------------------------------

void ra_invalidate(ProxyBlockState* state) {
    state->ra_valid = false;
    state->ra_block_count = 0;
}

auto ra_cache_hit(ProxyBlockState* state, uint64_t lba, uint32_t count) -> bool {
    if (!state->ra_valid || state->ra_buffer == nullptr) {
        return false;
    }
    return lba >= state->ra_base_lba && (lba + count) <= (state->ra_base_lba + state->ra_block_count);
}

// Server-push CQ drain: reads all available CQEs into per-tag completion array.
// In multi-outstanding mode, multiple CQEs may arrive for different tags.
auto rdma_wait_cqe_push(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    // Check if already completed in a previous drain
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        auto& completion = tag_completion(state, tag);
        if (completion.completed) {
            *out_cqe = completion.cqe;
            completion.completed = false;
            return true;
        }
    }

    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    asm volatile("" ::: "memory");  // read barrier

    // Drain all available CQEs into per-tag tracking (multi-outstanding mode)
    while (!blk_cq_empty(hdr)) {
        uint32_t const IDX = hdr->cq_tail % hdr->cq_depth;
        uint32_t const CTAG = cq[IDX].tag;
        if (tag_in_use(state, CTAG)) {
            auto& completion = tag_completion(state, CTAG);
            completion.cqe = cq[IDX];
            completion.completed = true;
            completion.pending = false;
        }
        asm volatile("" ::: "memory");
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
    }

    // Check if our target tag completed
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        auto& completion = tag_completion(state, tag);
        if (completion.completed) {
            *out_cqe = completion.cqe;
            completion.completed = false;
            return true;
        }
    }

    return false;
}

// Drain all available CQ entries into the per-tag completion tracking array.
void rdma_drain_cq(ProxyBlockState* state) {
    // For RoCE zones: pull CQ + server-owned indices before checking for completions
    roce_pull_cq(state);

    auto* hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);

    while (!blk_cq_empty(hdr)) {
        asm volatile("" ::: "memory");  // read barrier
        uint32_t const IDX = hdr->cq_tail % hdr->cq_depth;
        const auto& cqe = cq[IDX];

        // Store completion in per-tag tracking array (O(1) lookup by tag)
        uint32_t const TAG = cqe.tag;
        if (tag_in_use(state, TAG)) {
            auto& completion = tag_completion(state, TAG);
            completion.cqe = cqe;
            completion.completed = true;
            completion.pending = false;
        }
        // Tags not in the bitmap are stale/orphaned - safe to drop

        asm volatile("" ::: "memory");  // write barrier before advancing tail
        hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
    }
}

auto wait_for_rdma_sq_space(ProxyBlockState* state, BlkRingHeader* ring_hdr, const WkiChannelIdentity& channel_identity,
                            uint64_t deadline_us) -> bool {
    while (blk_sq_full(ring_hdr)) {
        if (!proxy_block_active(state) || proxy_block_fenced(state)) {
            return false;
        }
        if (wki_now_us() >= deadline_us) {
            return false;
        }
        asm volatile("pause" ::: "memory");
        rdma_drain_cq(state);
        wki_spin_yield_channel_identity(channel_identity);
    }
    return true;
}

// Drain CQ and look for a specific tag. Returns true if found and fills out_cqe.
auto rdma_drain_cq_for_tag(ProxyBlockState* state, uint32_t tag, BlkCqEntry* out_cqe) -> bool {
    // Check per-tag completion array (O(1) lookup)
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        auto& completion = tag_completion(state, tag);
        if (completion.completed) {
            *out_cqe = completion.cqe;
            completion.completed = false;
            return true;
        }
    }

    // Drain fresh CQ entries
    rdma_drain_cq(state);

    // Check again
    if (tag < ProxyBlockState::TAG_POOL_SIZE) {
        auto& completion = tag_completion(state, tag);
        if (completion.completed) {
            *out_cqe = completion.cqe;
            completion.completed = false;
            return true;
        }
    }

    return false;
}

// Signal server that new SQ entries are available (tiered signaling).
void rdma_signal_server(ProxyBlockState* state) {
    auto* peer = wki_peer_find(state->owner_node);
    if (peer == nullptr) {
        return;
    }

    // Tier 1: ivshmem doorbell (near-zero latency)
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        // ivshmem transport has native doorbell - zone post handler will fire
        ZoneNotifyPayload notify = {};
        notify.zone_id = state->rdma_zone_id;
        notify.op_type = 1;  // WRITE (new SQ entries available)
        wki_send(state->owner_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
        return;
    }

    // Tier 2: RoCE doorbell (raw Ethernet frame)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->doorbell != nullptr) {
        peer->rdma_transport->doorbell(peer->rdma_transport, state->owner_node, state->rdma_zone_id);
    }

    // Reliable notify as fallback/backup for RoCE doorbell loss. The server
    // poll path is idempotent, so duplicate raw + reliable signals are okay.
    ZoneNotifyPayload notify = {};
    notify.zone_id = state->rdma_zone_id;
    notify.op_type = 1;  // WRITE (new SQ entries available)
    wki_send(state->owner_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
}

// -----------------------------------------------------------------------------
// Remote block device function pointers
// -----------------------------------------------------------------------------

// Block until the proxy is no longer fenced, or until the fence timeout expires.
// Returns true if the proxy came back, false if the fence timed out (proxy torn down).
auto wait_for_fence_lift(ProxyBlockState* state) -> bool {
    while (proxy_block_fenced(state)) {
        // Check if the fence timeout has expired (hard teardown will clear active)
        if (!proxy_block_active(state)) {
            return false;
        }
        asm volatile("pause" ::: "memory");
        // Yield the current task so other kernel work (including the WKI timer
        // thread that processes reconnection) can make progress.
        ker::mod::sched::kern_yield();
    }
    return proxy_block_active(state);
}

class BlockIoLease {
   public:
    BlockIoLease() = default;
    BlockIoLease(const BlockIoLease&) = delete;
    auto operator=(const BlockIoLease&) -> BlockIoLease& = delete;
    ~BlockIoLease() {
        if (state_ != nullptr) {
            state_->io_lock.unlock();
        }
    }

    auto acquire(ProxyBlockState* state) -> bool {
        if (!proxy_block_active(state) || proxy_block_fenced(state)) {
            return false;
        }
        state->io_lock.lock();
        if (!proxy_block_active(state) || proxy_block_fenced(state)) {
            state->io_lock.unlock();
            return false;
        }
        state_ = state;
        return true;
    }

   private:
    ProxyBlockState* state_ = nullptr;
};

class BlockResumeClaim {
   public:
    explicit BlockResumeClaim(ProxyBlockState* state) : state_(state) {}
    BlockResumeClaim(const BlockResumeClaim&) = delete;
    auto operator=(const BlockResumeClaim&) -> BlockResumeClaim& = delete;
    ~BlockResumeClaim() {
        s_proxy_lock.lock();
        if (state_->resume_detach_confirmed && proxy_block_active(state_) && proxy_block_fenced(state_) && !state_->epoch_reset_pending &&
            !state_->cleanup_in_progress) {
            // An exact old-binding detach was ACKed while this claim was
            // active. Requeue locally only after releasing the claim so the
            // outer bounded resume pass can consume it without spinning on
            // resume_in_progress.
            state_->resume_pending = true;
        }
        state_->resume_in_progress = false;
        s_proxy_lock.unlock();
    }

   private:
    ProxyBlockState* state_;
};

void wait_for_block_io_quiescence(ProxyBlockState* state) {
    if (state != nullptr) {
        state->io_lock.lock();
        state->io_lock.unlock();
    }
}

// Message-based block read (fallback when RDMA not available)
auto remote_block_read_msg(ProxyBlockState* state, ker::dev::BlockDevice* dev, uint64_t block, uint32_t count, void* buffer) -> int {
    auto* dest = static_cast<uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    // Calculate max blocks per chunk based on response payload capacity
    auto max_resp_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));
    uint32_t const BLOCKS_PER_CHUNK = max_resp_data / static_cast<uint32_t>(dev->block_size);
    if (BLOCKS_PER_CHUNK == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > BLOCKS_PER_CHUNK) ? BLOCKS_PER_CHUNK : remaining;
        auto chunk_bytes = static_cast<uint16_t>(chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32}
        constexpr uint16_t REQ_DATA_LEN = 12;
        std::array<uint8_t, sizeof(DevOpReqPayload) + REQ_DATA_LEN> req_buf = {};

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
        req->op_id = OP_BLOCK_READ;
        req->data_len = REQ_DATA_LEN;

        auto* req_data = req_buf.data() + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));

        // Set up blocking state
        WkiWaitEntry wait = {};
        WkiChannelIdentity channel_identity = {};
        if (prepare_block_op_wait(state, OP_BLOCK_READ, wait, dest, chunk_bytes, &channel_identity) != WKI_OK) {
            return -1;
        }

        // Send request
        int const SEND_RET = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_REQ, req_buf.data(),
                                                          static_cast<uint16_t>(sizeof(DevOpReqPayload) + REQ_DATA_LEN));
        if (SEND_RET != WKI_OK) {
            cancel_op_waiter(state, wait, SEND_RET);
            return -1;
        }

        // Async wait for response
        int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        if (WAIT_RC != 0) {
            cancel_op_waiter(state, wait, WAIT_RC);
            return -1;
        }

        int op_status = 0;
        consume_block_op_result(state, wait, &op_status, nullptr);
        if (op_status != 0) {
            return op_status;
        }

        dest += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

// RDMA ring-based block read - with read-ahead cache and server-push optimisation.
//
// Fetches random misses at the caller's requested size, then expands to a full
// data-slot prefetch once access becomes sequential. For RoCE zones the server
// already pushes data, CQ, and server-owned indices via rdma_write, so the
// consumer avoids all rdma_read round-trips (no roce_pull_cq / roce_pull_data_slot).
auto remote_block_read_rdma(ProxyBlockState* state, uint64_t block, uint32_t count, void* buffer) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    uint32_t const BLK_SZ = ring_hdr->block_size;
    uint32_t const BLOCKS_PER_SLOT = ring_hdr->data_slot_size / BLK_SZ;
    if (BLOCKS_PER_SLOT == 0) {
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (block >= ring_hdr->total_blocks) {
        return -1;
    }
    uint64_t const AVAILABLE_BLOCKS = ring_hdr->total_blocks - block;
    if (static_cast<uint64_t>(count) > AVAILABLE_BLOCKS) {
        return -1;
    }
    if (count > BLOCKS_PER_SLOT) {
        auto* dest = static_cast<uint8_t*>(buffer);
        uint64_t lba = block;
        uint32_t remaining = count;
        while (remaining > 0) {
            uint32_t const CHUNK = (remaining > BLOCKS_PER_SLOT) ? BLOCKS_PER_SLOT : remaining;
            int const RET = remote_block_read_rdma(state, lba, CHUNK, dest);
            if (RET != 0) {
                return RET;
            }
            dest += static_cast<size_t>(CHUNK) * BLK_SZ;
            lba += CHUNK;
            remaining -= CHUNK;
        }
        return 0;
    }

    // -- 1. Serve from read-ahead cache if the request is fully covered ------
    if (ra_cache_hit(state, block, count)) {
        uint64_t const OFF = (block - state->ra_base_lba) * BLK_SZ;
        memcpy(buffer, state->ra_buffer + OFF, static_cast<size_t>(count) * BLK_SZ);
        return 0;
    }

    // -- 2. Cache miss -------------------------------------------------------
    // Random XFS metadata reads are commonly 4 KiB (8 x 512-byte sectors).
    // Fetching a full 64 KiB slot for every miss overloads RoCE completion
    // latency during directory walks.  Keep the caller's span for random
    // misses, and expand only once the access pattern proves sequential.
    bool const SEQUENTIAL_MISS = state->ra_buffer != nullptr && state->ra_valid && block == (state->ra_base_lba + state->ra_block_count);
    uint32_t fetch_count = SEQUENTIAL_MISS ? BLOCKS_PER_SLOT : count;
    if (fetch_count > AVAILABLE_BLOCKS) {
        fetch_count = static_cast<uint32_t>(AVAILABLE_BLOCKS);
    }
    if (fetch_count < count) {
        return -1;
    }
    auto fetch_bytes = fetch_count * BLK_SZ;

    // Allocate a data slot
    int slot = rdma_alloc_slot(state);
    if (slot < 0) {
        rdma_drain_cq(state);
        slot = rdma_alloc_slot(state);
        if (slot < 0) {
            return -1;
        }
    }

    // Wait for SQ space
    WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
    if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        return -1;
    }

    // Post SQE - request the full prefetch range
    int const TAG_ID = rdma_alloc_tag(state);
    if (TAG_ID < 0) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        return -1;
    }
    auto const TAG = static_cast<uint32_t>(TAG_ID);
    uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
    auto& sqe = sq[SQ_IDX];
    sqe.tag = TAG;
    sqe.opcode = static_cast<uint8_t>(BlkOpcode::READ);
    sqe.lba = block;
    sqe.block_count = fetch_count;
    sqe.data_slot = static_cast<uint32_t>(slot);

    asm volatile("" ::: "memory");  // write barrier before advancing head
    ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

    // Push SQ to server (RoCE)
    roce_push_sq(state);

    // Signal server
    rdma_signal_server(state);

    // -- 3. Wait for completion ----------------------------------------------
    BlkCqEntry cqe = {};
    bool got_cqe = false;

    if (state->rdma_roce) {
        // Server-push mode: the server pushes data slot + CQ + server-owned indices into our
        // local zone via rdma_write.  wki_spin_yield_channel drives NIC RX so
        // those frames land in local memory.  No rdma_read needed.
        while (!got_cqe) {
            got_cqe = rdma_wait_cqe_push(state, TAG, &cqe);
            if (got_cqe) {
                break;
            }
            if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, TAG);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
        }
    } else {
        // ivshmem path: shared memory is coherent, use existing drain logic
        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, TAG, &cqe);
            if (got_cqe) {
                break;
            }
            if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, TAG);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
        }
    }

    if (cqe.status != 0) {
        rdma_free_slot(state, static_cast<uint32_t>(slot));
        rdma_free_tag(state, TAG);
        ra_invalidate(state);
        return cqe.status;
    }

    // -- 4. Populate read-ahead cache from the data slot ---------------------
    // For RoCE the server already pushed data into our local zone - no pull needed.
    if (!state->rdma_roce) {
        roce_pull_data_slot(state, static_cast<uint32_t>(slot), fetch_bytes);
    }

    auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));

    if (state->ra_buffer != nullptr) {
        memcpy(state->ra_buffer, slot_data, fetch_bytes);
        state->ra_base_lba = block;
        state->ra_block_count = fetch_count;
        state->ra_valid = true;
    }

    rdma_free_slot(state, static_cast<uint32_t>(slot));
    rdma_free_tag(state, TAG);

    // -- 5. Copy the originally-requested data to the caller's buffer --------
    memcpy(buffer, state->ra_buffer != nullptr ? state->ra_buffer : slot_data, static_cast<size_t>(count) * BLK_SZ);

    return 0;
}

// Dispatcher: uses RDMA ring if available, falls back to message-based
auto remote_block_read(ker::dev::BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }

    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!proxy_block_active(state)) {
        return -1;
    }

    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }

    auto cnt = static_cast<uint32_t>(count);
    if (state->rdma_attached) {
        return remote_block_read_rdma(state, block, cnt, buffer);
    }
    return remote_block_read_msg(state, dev, block, cnt, buffer);
}

// Message-based block write (fallback when RDMA not available)
auto remote_block_write_msg(ProxyBlockState* state, ker::dev::BlockDevice* dev, uint64_t block, uint32_t count, const void* buffer) -> int {
    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    // Calculate max blocks per chunk based on request payload capacity
    // Request: DevOpReqPayload(4) + lba(8) + count(4) + data
    constexpr uint32_t WRITE_HDR_OVERHEAD = sizeof(DevOpReqPayload) + 12;
    auto max_req_data = static_cast<uint32_t>(WKI_ETH_MAX_PAYLOAD - WRITE_HDR_OVERHEAD);
    uint32_t const BLOCKS_PER_CHUNK = max_req_data / static_cast<uint32_t>(dev->block_size);
    if (BLOCKS_PER_CHUNK == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t chunk = (remaining > BLOCKS_PER_CHUNK) ? BLOCKS_PER_CHUNK : remaining;
        auto chunk_bytes = (chunk * static_cast<uint32_t>(dev->block_size));

        // Build request: DevOpReqPayload + {lba:u64, count:u32} + data
        auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + 12 + chunk_bytes);
        auto* req_buf = new (std::nothrow) uint8_t[req_total];
        if (req_buf == nullptr) {
            return -1;
        }

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_BLOCK_WRITE;
        req->data_len = static_cast<uint16_t>(12 + chunk_bytes);

        auto* req_data = req_buf + sizeof(DevOpReqPayload);
        memcpy(req_data, &lba, sizeof(uint64_t));
        memcpy(req_data + sizeof(uint64_t), &chunk, sizeof(uint32_t));
        memcpy(req_data + 12, src, chunk_bytes);

        // Set up blocking state
        WkiWaitEntry wait = {};
        WkiChannelIdentity channel_identity = {};
        if (prepare_block_op_wait(state, OP_BLOCK_WRITE, wait, nullptr, 0, &channel_identity) != WKI_OK) {
            delete[] req_buf;
            return -1;
        }

        // Send request
        int const SEND_RET = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_REQ, req_buf, req_total);
        delete[] req_buf;

        if (SEND_RET != WKI_OK) {
            cancel_op_waiter(state, wait, SEND_RET);
            return -1;
        }

        // Async wait for response
        int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        if (WAIT_RC != 0) {
            cancel_op_waiter(state, wait, WAIT_RC);
            return -1;
        }

        int op_status = 0;
        consume_block_op_result(state, wait, &op_status, nullptr);
        if (op_status != 0) {
            return op_status;
        }

        src += chunk_bytes;
        lba += chunk;
        remaining -= chunk;
    }

    return 0;
}

// RDMA ring-based block write - consumer copies data into slot, server reads from it
auto remote_block_write_rdma(ProxyBlockState* state, uint64_t block, uint32_t count, const void* buffer) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    // Invalidate read-ahead cache - written data may overlap cached range
    ra_invalidate(state);

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t lba = block;
    uint32_t remaining = count;

    uint32_t const BLOCKS_PER_SLOT = ring_hdr->data_slot_size / ring_hdr->block_size;
    if (BLOCKS_PER_SLOT == 0) {
        return -1;
    }

    while (remaining > 0) {
        uint32_t const CHUNK = (remaining > BLOCKS_PER_SLOT) ? BLOCKS_PER_SLOT : remaining;
        auto chunk_bytes = CHUNK * ring_hdr->block_size;

        // Allocate a data slot
        int slot = rdma_alloc_slot(state);
        if (slot < 0) {
            rdma_drain_cq(state);
            slot = rdma_alloc_slot(state);
            if (slot < 0) {
                return -1;
            }
        }

        // Copy data INTO the RDMA zone data slot before posting SQE
        auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));
        memcpy(slot_data, src, chunk_bytes);

        // For RoCE: push data slot to server before posting SQE (server needs data visible)
        roce_push_data_slot(state, static_cast<uint32_t>(slot), chunk_bytes);

        // Wait for SQ space
        WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
        uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
        if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
            rdma_free_slot(state, static_cast<uint32_t>(slot));
            return -1;
        }

        // Post SQE
        int const TAG_ID = rdma_alloc_tag(state);
        if (TAG_ID < 0) {
            rdma_free_slot(state, static_cast<uint32_t>(slot));
            return -1;
        }
        auto const TAG = static_cast<uint32_t>(TAG_ID);
        uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[SQ_IDX];
        sqe.tag = TAG;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::WRITE);
        sqe.lba = lba;
        sqe.block_count = CHUNK;
        sqe.data_slot = static_cast<uint32_t>(slot);

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        // For RoCE: push SQ to server so it can see the new entry
        roce_push_sq(state);

        // Signal server
        rdma_signal_server(state);

        // Spin-wait for CQE with matching tag
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, TAG, &cqe);
            if (got_cqe) {
                break;
            }
            if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
                rdma_free_tag(state, TAG);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
        }

        rdma_free_slot(state, static_cast<uint32_t>(slot));
        rdma_free_tag(state, TAG);

        if (cqe.status != 0) {
            return cqe.status;
        }

        src += chunk_bytes;
        lba += CHUNK;
        remaining -= CHUNK;
    }

    return 0;
}

// Dispatcher: uses RDMA ring if available, falls back to message-based
auto remote_block_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }

    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!proxy_block_active(state)) {
        return -1;
    }
    if (ker::dev::block_device_is_read_only(dev)) {
        return -EROFS;
    }

    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }

    auto cnt = static_cast<uint32_t>(count);
    if (state->rdma_attached) {
        return remote_block_write_rdma(state, block, cnt, buffer);
    }
    return remote_block_write_msg(state, dev, block, cnt, buffer);
}

// Message-based block flush (fallback when RDMA not available)
auto remote_block_flush_msg(ProxyBlockState* state) -> int {
    DevOpReqPayload req = {};
    req.op_id = OP_BLOCK_FLUSH;
    req.data_len = 0;

    WkiWaitEntry wait = {};
    WkiChannelIdentity channel_identity = {};
    if (prepare_block_op_wait(state, OP_BLOCK_FLUSH, wait, nullptr, 0, &channel_identity) != WKI_OK) {
        return -1;
    }

    int const SEND_RET = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (SEND_RET != WKI_OK) {
        cancel_op_waiter(state, wait, SEND_RET);
        return -1;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    if (WAIT_RC != 0) {
        cancel_op_waiter(state, wait, WAIT_RC);
        return -1;
    }

    int op_status = 0;
    consume_block_op_result(state, wait, &op_status, nullptr);
    return op_status;
}

// RDMA ring-based flush
auto remote_block_flush_rdma(ProxyBlockState* state) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr) {
        return -1;
    }
    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);

    // Wait for SQ space
    WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
    if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
        return -1;
    }

    // Post SQE - flush uses no data slot
    int const TAG_ID = rdma_alloc_tag(state);
    if (TAG_ID < 0) {
        return -1;
    }
    auto const TAG = static_cast<uint32_t>(TAG_ID);
    uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
    auto& sqe = sq[SQ_IDX];
    sqe.tag = TAG;
    sqe.opcode = static_cast<uint8_t>(BlkOpcode::FLUSH);
    sqe.lba = 0;
    sqe.block_count = 0;
    sqe.data_slot = 0;

    asm volatile("" ::: "memory");
    ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

    // For RoCE: push SQ to server so it can see the flush entry
    roce_push_sq(state);

    rdma_signal_server(state);

    // Spin-wait for CQE
    BlkCqEntry cqe = {};
    bool got_cqe = false;

    while (!got_cqe) {
        got_cqe = rdma_drain_cq_for_tag(state, TAG, &cqe);
        if (got_cqe) {
            break;
        }
        if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
            rdma_free_tag(state, TAG);
            return -1;
        }
        asm volatile("pause" ::: "memory");
        wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
    }

    rdma_free_tag(state, TAG);
    return cqe.status;
}

// Dispatcher
auto remote_block_flush(ker::dev::BlockDevice* dev) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }

    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }

    // Re-check active after potentially blocking in fence wait
    if (!proxy_block_active(state)) {
        return -1;
    }

    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_flush_rdma(state);
    }
    return remote_block_flush_msg(state);
}

// -----------------------------------------------------------------------------
// Batch I/O - async SQ pipeline (submits multiple SQEs, single doorbell)
// -----------------------------------------------------------------------------

// Per-batch tracking entry (tag + data slot allocated for one SQE)
struct BatchEntry {
    uint32_t tag;
    uint32_t slot;
};

// Post up to `count` SQEs into the SQ ring without signaling the server.
// Allocates tags and data slots for each entry.  Returns number actually posted.
auto rdma_batch_submit(ProxyBlockState* state, BlkOpcode opcode, const BlockRange* ranges, uint32_t count,
                       std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH>& entries_out) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);
    WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);

    uint32_t posted = 0;
    for (uint32_t i = 0; i < count; i++) {
        // Allocate tag
        int tag = rdma_alloc_tag(state);
        if (tag < 0) {
            rdma_drain_cq(state);
            tag = rdma_alloc_tag(state);
            if (tag < 0) {
                break;
            }
        }

        // Allocate data slot (not needed for FLUSH)
        int slot = -1;
        if (opcode != BlkOpcode::FLUSH) {
            slot = rdma_alloc_slot(state);
            if (slot < 0) {
                rdma_drain_cq(state);
                slot = rdma_alloc_slot(state);
                if (slot < 0) {
                    rdma_free_tag(state, static_cast<uint32_t>(tag));
                    break;
                }
            }
        }

        // For writes: copy data to slot before posting
        if (opcode == BlkOpcode::WRITE && slot >= 0 && ranges[i].buffer != nullptr) {
            uint32_t const BYTES = ranges[i].block_count * ring_hdr->block_size;
            auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, static_cast<uint32_t>(slot));
            memcpy(slot_data, ranges[i].buffer, BYTES);
            roce_push_data_slot(state, static_cast<uint32_t>(slot), BYTES);
        }

        // Wait for SQ space
        if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
            if (slot >= 0) {
                rdma_free_slot(state, static_cast<uint32_t>(slot));
            }
            rdma_free_tag(state, static_cast<uint32_t>(tag));
            break;
        }

        // Post SQE
        uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[SQ_IDX];
        sqe.tag = static_cast<uint32_t>(tag);
        sqe.opcode = static_cast<uint8_t>(opcode);
        sqe.lba = ranges[i].lba;
        sqe.block_count = ranges[i].block_count;
        sqe.data_slot = (slot >= 0) ? static_cast<uint32_t>(slot) : 0;

        asm volatile("" ::: "memory");  // write barrier before advancing head
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        auto& entry = entries_out.at(posted);
        entry.tag = static_cast<uint32_t>(tag);
        entry.slot = (slot >= 0) ? static_cast<uint32_t>(slot) : 0;
        posted++;
    }

    return static_cast<int>(posted);
}

// Send a single doorbell/notification after all SQEs are posted.
void rdma_batch_signal(ProxyBlockState* state) {
    roce_push_sq(state);
    rdma_signal_server(state);
}

// Poll CQ for completions matching the given batch entries.
// In blocking mode, spins until all `count` entries complete or timeout.
// Returns the number of completed entries.
auto rdma_batch_collect(ProxyBlockState* state, const std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH>& entries, uint32_t count,
                        bool blocking) -> int {
    WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);

    for (;;) {
        // Count completed entries
        uint32_t completed = 0;
        for (uint32_t i = 0; i < count; i++) {
            uint32_t const TAG = entries.at(i).tag;
            if (TAG < ProxyBlockState::TAG_POOL_SIZE && tag_completion(state, TAG).completed) {
                completed++;
            }
        }
        if (completed == count) {
            return static_cast<int>(completed);
        }

        // Drain CQ entries into per-tag array
        if (state->rdma_roce) {
            // Server-push mode: CQEs arrive via RDMA writes into local zone memory;
            // drive NIC RX to receive those frames, then scan CQ ring locally.
            auto* hdr = blk_ring_header(state->rdma_zone_ptr);
            auto* cq = blk_cq_entries(state->rdma_zone_ptr, hdr);
            asm volatile("" ::: "memory");
            while (!blk_cq_empty(hdr)) {
                uint32_t const IDX = hdr->cq_tail % hdr->cq_depth;
                uint32_t const CTAG = cq[IDX].tag;
                if (tag_in_use(state, CTAG)) {
                    auto& completion = tag_completion(state, CTAG);
                    completion.cqe = cq[IDX];
                    completion.completed = true;
                    completion.pending = false;
                }
                asm volatile("" ::: "memory");
                hdr->cq_tail = (hdr->cq_tail + 1) % hdr->cq_depth;
            }
        } else {
            rdma_drain_cq(state);
        }

        if (!blocking) {
            // Non-blocking: count what we got and return
            uint32_t got = 0;
            for (uint32_t i = 0; i < count; i++) {
                uint32_t const TAG = entries.at(i).tag;
                if (TAG < ProxyBlockState::TAG_POOL_SIZE && tag_completion(state, TAG).completed) {
                    got++;
                }
            }
            return static_cast<int>(got);
        }

        if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
            return -1;  // timeout
        }

        asm volatile("pause" ::: "memory");
        wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
    }
}

// Batch RDMA read: submits multiple read SQEs, single doorbell, collects all CQEs.
auto remote_block_read_batch_rdma(ProxyBlockState* state, const BlockRange* ranges, uint32_t count) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);

    // Invalidate RA cache - batch reads bypass the single-block read-ahead
    ra_invalidate(state);

    // Clamp batch to max depth
    uint32_t const BATCH_COUNT = (count > WKI_DEV_PROXY_MAX_BATCH) ? WKI_DEV_PROXY_MAX_BATCH : count;

    std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH> entries = {};

    int const POSTED = rdma_batch_submit(state, BlkOpcode::READ, ranges, BATCH_COUNT, entries);
    if (POSTED <= 0) {
        return -1;
    }

    rdma_batch_signal(state);

    int const COLLECTED = rdma_batch_collect(state, entries, static_cast<uint32_t>(POSTED), true);
    if (COLLECTED != POSTED) {
        // Timeout - free resources for all posted entries
        for (int i = 0; i < POSTED; i++) {
            auto const& entry = entries.at(static_cast<size_t>(i));
            rdma_free_slot(state, entry.slot);
            rdma_free_tag(state, entry.tag);
        }
        return -1;
    }

    // Collect results: copy data from slots to caller buffers
    int result = 0;
    for (int i = 0; i < POSTED; i++) {
        auto const& entry = entries.at(static_cast<size_t>(i));
        uint32_t const TAG = entry.tag;
        auto& tc = tag_completion(state, TAG);

        if (tc.cqe.status != 0) {
            result = tc.cqe.status;
        } else {
            // For non-RoCE ivshmem: data is already in local shared memory.
            // roce_pull_data_slot is a no-op for both paths (server-push for
            // RoCE, coherent shared mem for ivshmem).
            auto* slot_data = blk_data_slot(state->rdma_zone_ptr, ring_hdr, entry.slot);
            uint32_t const COPY_BYTES = ranges[i].block_count * ring_hdr->block_size;
            memcpy(ranges[i].buffer, slot_data, COPY_BYTES);
        }

        tc.completed = false;
        rdma_free_slot(state, entry.slot);
        rdma_free_tag(state, entry.tag);
    }

    // Handle remaining ranges (if count > MAX_BATCH) via recursive call
    if (count > BATCH_COUNT && result == 0) {
        return remote_block_read_batch_rdma(state, ranges + BATCH_COUNT, count - BATCH_COUNT);
    }

    return result;
}

// Batch RDMA write: submits multiple write SQEs, single doorbell, collects all CQEs.
auto remote_block_write_batch_rdma(ProxyBlockState* state, const BlockRange* ranges, uint32_t count) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr || count == 0) {
        return -1;
    }

    // Invalidate read-ahead cache - written data may overlap cached range
    ra_invalidate(state);

    // Clamp batch to max depth
    uint32_t const BATCH_COUNT = (count > WKI_DEV_PROXY_MAX_BATCH) ? WKI_DEV_PROXY_MAX_BATCH : count;

    std::array<BatchEntry, WKI_DEV_PROXY_MAX_BATCH> entries = {};

    int const POSTED = rdma_batch_submit(state, BlkOpcode::WRITE, ranges, BATCH_COUNT, entries);
    if (POSTED <= 0) {
        return -1;
    }

    rdma_batch_signal(state);

    int const COLLECTED = rdma_batch_collect(state, entries, static_cast<uint32_t>(POSTED), true);
    if (COLLECTED != POSTED) {
        // Timeout - free resources
        for (int i = 0; i < POSTED; i++) {
            auto const& entry = entries.at(static_cast<size_t>(i));
            rdma_free_slot(state, entry.slot);
            rdma_free_tag(state, entry.tag);
        }
        return -1;
    }

    // Collect results
    int result = 0;
    for (int i = 0; i < POSTED; i++) {
        auto const& entry = entries.at(static_cast<size_t>(i));
        uint32_t const TAG = entry.tag;
        auto& tc = tag_completion(state, TAG);

        if (tc.cqe.status != 0) {
            result = tc.cqe.status;
        }

        tc.completed = false;
        rdma_free_slot(state, entry.slot);
        rdma_free_tag(state, entry.tag);
    }

    // Handle remaining ranges (if count > MAX_BATCH)
    if (count > BATCH_COUNT && result == 0) {
        return remote_block_write_batch_rdma(state, ranges + BATCH_COUNT, count - BATCH_COUNT);
    }

    return result;
}

// Dispatcher for batch read: RDMA batch if available, fallback to sequential single reads
auto remote_block_read_batch(ker::dev::BlockDevice* dev, const BlockRange* ranges, uint32_t count) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }

    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!proxy_block_active(state)) {
        return -1;
    }

    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_read_batch_rdma(state, ranges, count);
    }

    // Fallback: sequential single reads via message path
    for (uint32_t i = 0; i < count; i++) {
        int const RET = remote_block_read_msg(state, dev, ranges[i].lba, ranges[i].block_count, ranges[i].buffer);
        if (RET != 0) {
            return RET;
        }
    }
    return 0;
}

// Dispatcher for batch write: RDMA batch if available, fallback to sequential single writes
auto remote_block_write_batch(ker::dev::BlockDevice* dev, const BlockRange* ranges, uint32_t count) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }

    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (!proxy_block_active(state)) {
        return -1;
    }
    if (ker::dev::block_device_is_read_only(dev)) {
        return -EROFS;
    }

    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }

    if (state->rdma_attached) {
        return remote_block_write_batch_rdma(state, ranges, count);
    }

    // Fallback: sequential single writes via message path
    for (uint32_t i = 0; i < count; i++) {
        int const RET = remote_block_write_msg(state, dev, ranges[i].lba, ranges[i].block_count, ranges[i].buffer);
        if (RET != 0) {
            return RET;
        }
    }
    return 0;
}

}  // namespace

// -----------------------------------------------------------------------------
// Streaming Bulk Transfer - large contiguous I/O via direct RDMA to/from
// a pre-registered consumer staging buffer.  Bypasses the normal per-slot
// SQ/CQ pipeline for transfers exceeding BLK_RING_BULK_THRESHOLD bytes.
// -----------------------------------------------------------------------------

namespace {

auto setup_block_bulk_staging(ProxyBlockState* state) -> bool {
    if (state == nullptr) {
        return false;
    }

    state->bdev.capabilities &= ~ker::dev::BDEV_CAP_BULK_RDMA;
    if (!state->rdma_attached || !state->bulk_capable || state->rdma_transport == nullptr ||
        state->rdma_transport->rdma_register_region == nullptr) {
        return false;
    }
    if (state->bulk_staging_buf != nullptr && state->bulk_staging_rkey != 0 && state->bulk_staging_size != 0) {
        state->bdev.capabilities |= ker::dev::BDEV_CAP_BULK_RDMA;
        return true;
    }

    // Default bulk staging buffer: 2 MB (clamped to the advertised maximum).
    constexpr uint32_t DEFAULT_BULK_STAGING = 2 * 1024 * 1024;
    uint32_t const STAGING_SIZE =
        (state->bulk_max_transfer > 0 && state->bulk_max_transfer < DEFAULT_BULK_STAGING) ? state->bulk_max_transfer : DEFAULT_BULK_STAGING;
    auto* staging = new (std::nothrow) uint8_t[STAGING_SIZE];
    if (staging == nullptr) {
        state->bulk_capable = false;
        return false;
    }

    uint32_t rkey = 0;
    int const REGISTER_RET =
        state->rdma_transport->rdma_register_region(state->rdma_transport, reinterpret_cast<uint64_t>(staging), STAGING_SIZE, &rkey);
    if (REGISTER_RET != 0 || rkey == 0) {
        delete[] staging;
        state->bulk_capable = false;
        ker::mod::dbg::log("[WKI] Dev proxy bulk staging register failed: err=%d", REGISTER_RET);
        return false;
    }

    state->bulk_staging_buf = staging;
    state->bulk_staging_size = STAGING_SIZE;
    state->bulk_staging_rkey = rkey;
    state->bdev.capabilities |= ker::dev::BDEV_CAP_BULK_RDMA;
    ker::mod::dbg::log("[WKI] Dev proxy bulk staging registered: size=%uKB rkey=0x%08x", STAGING_SIZE / 1024, rkey);
    return true;
}

// Bulk RDMA read: post a BULK_READ SQE, server RDMA-writes entire range into
// consumer's staging buffer, single CQE covers the whole transfer.
auto remote_block_bulk_read_rdma(ProxyBlockState* state, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr || !state->bulk_capable) {
        return -1;
    }
    if (state->bulk_staging_buf == nullptr || state->bulk_staging_rkey == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);
    uint32_t const BLK_SZ = ring_hdr->block_size;

    // Clamp to staging buffer size; if larger, split into chunks
    auto* dest = static_cast<uint8_t*>(buffer);
    uint64_t remaining_lba = lba;
    uint32_t remaining_blocks = block_count;

    while (remaining_blocks > 0) {
        uint32_t chunk_blocks = remaining_blocks;
        uint64_t chunk_bytes = static_cast<uint64_t>(chunk_blocks) * BLK_SZ;
        if (chunk_bytes > state->bulk_staging_size) {
            chunk_blocks = state->bulk_staging_size / BLK_SZ;
            chunk_bytes = static_cast<uint64_t>(chunk_blocks) * BLK_SZ;
        }
        if (chunk_blocks == 0) {
            return -1;
        }

        // Invalidate RA cache (bulk bypasses it)
        ra_invalidate(state);

        // Wait for SQ space
        WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
        uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), DEV_PROXY_BULK_WAIT_TIMEOUT_US);
        if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
            return -1;
        }

        // Allocate tag
        int tag_id = rdma_alloc_tag(state);
        if (tag_id < 0) {
            rdma_drain_cq(state);
            tag_id = rdma_alloc_tag(state);
            if (tag_id < 0) {
                return -1;
            }
        }
        auto tag = static_cast<uint32_t>(tag_id);

        // Post Bulk SQE (same binary layout as BlkSqEntry, data_slot = roce_rkey)
        uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[SQ_IDX];
        sqe.tag = tag;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::BULK_READ);
        sqe.lba = remaining_lba;
        sqe.block_count = chunk_blocks;
        sqe.data_slot = state->bulk_staging_rkey;  // consumer's rkey for staging buffer

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        // Push SQ to server and signal
        roce_push_sq(state);
        rdma_signal_server(state);

        // Wait for completion - server RDMA-writes data into our staging buffer
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        if (state->rdma_roce) {
            while (!got_cqe) {
                got_cqe = rdma_wait_cqe_push(state, tag, &cqe);
                if (got_cqe) {
                    break;
                }
                if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                    rdma_free_tag(state, tag);
                    return -1;
                }
                asm volatile("pause" ::: "memory");
                wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
            }
        } else {
            while (!got_cqe) {
                got_cqe = rdma_drain_cq_for_tag(state, tag, &cqe);
                if (got_cqe) {
                    break;
                }
                if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                    rdma_free_tag(state, tag);
                    return -1;
                }
                asm volatile("pause" ::: "memory");
                wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
            }
        }

        rdma_free_tag(state, tag);

        if (cqe.status != 0) {
            return cqe.status;
        }

        // Copy from staging buffer to caller's buffer
        memcpy(dest, state->bulk_staging_buf, static_cast<size_t>(chunk_bytes));

        dest += chunk_bytes;
        remaining_lba += chunk_blocks;
        remaining_blocks -= chunk_blocks;
    }

    return 0;
}

// Bulk RDMA write: copy data into staging buffer, post a BULK_WRITE SQE,
// server RDMA-reads from staging buffer and writes to disk.
auto remote_block_bulk_write_rdma(ProxyBlockState* state, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    if (!proxy_block_active(state) || state->rdma_zone_ptr == nullptr || !state->bulk_capable) {
        return -1;
    }
    if (state->bulk_staging_buf == nullptr || state->bulk_staging_rkey == 0) {
        return -1;
    }

    auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
    auto* sq = blk_sq_entries(state->rdma_zone_ptr);
    uint32_t const BLK_SZ = ring_hdr->block_size;

    // Invalidate RA cache - written data may overlap cached range
    ra_invalidate(state);

    const auto* src = static_cast<const uint8_t*>(buffer);
    uint64_t remaining_lba = lba;
    uint32_t remaining_blocks = block_count;

    while (remaining_blocks > 0) {
        uint32_t chunk_blocks = remaining_blocks;
        uint64_t chunk_bytes = static_cast<uint64_t>(chunk_blocks) * BLK_SZ;
        if (chunk_bytes > state->bulk_staging_size) {
            chunk_blocks = state->bulk_staging_size / BLK_SZ;
            chunk_bytes = static_cast<uint64_t>(chunk_blocks) * BLK_SZ;
        }
        if (chunk_blocks == 0) {
            return -1;
        }

        // Copy caller data into staging buffer
        memcpy(state->bulk_staging_buf, src, static_cast<size_t>(chunk_bytes));

        // Wait for SQ space
        WkiChannelIdentity const CHANNEL_IDENTITY = block_channel_identity_snapshot(state);
        uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), DEV_PROXY_BULK_WAIT_TIMEOUT_US);
        if (!wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)) {
            return -1;
        }

        // Allocate tag
        int tag_id = rdma_alloc_tag(state);
        if (tag_id < 0) {
            rdma_drain_cq(state);
            tag_id = rdma_alloc_tag(state);
            if (tag_id < 0) {
                return -1;
            }
        }
        auto const TAG = static_cast<uint32_t>(tag_id);

        // Post Bulk SQE
        uint32_t const SQ_IDX = ring_hdr->sq_head % ring_hdr->sq_depth;
        auto& sqe = sq[SQ_IDX];
        sqe.tag = TAG;
        sqe.opcode = static_cast<uint8_t>(BlkOpcode::BULK_WRITE);
        sqe.lba = remaining_lba;
        sqe.block_count = chunk_blocks;
        sqe.data_slot = state->bulk_staging_rkey;  // consumer's rkey for staging buffer

        asm volatile("" ::: "memory");
        ring_hdr->sq_head = (ring_hdr->sq_head + 1) % ring_hdr->sq_depth;

        roce_push_sq(state);
        rdma_signal_server(state);

        // Wait for completion
        BlkCqEntry cqe = {};
        bool got_cqe = false;

        while (!got_cqe) {
            got_cqe = rdma_drain_cq_for_tag(state, TAG, &cqe);
            if (got_cqe) {
                break;
            }
            if (!proxy_block_active(state) || proxy_block_fenced(state) || wki_now_us() >= DEADLINE) {
                rdma_free_tag(state, TAG);
                return -1;
            }
            asm volatile("pause" ::: "memory");
            wki_spin_yield_channel_identity(CHANNEL_IDENTITY);
        }

        rdma_free_tag(state, TAG);

        if (cqe.status != 0) {
            return cqe.status;
        }

        src += chunk_bytes;
        remaining_lba += chunk_blocks;
        remaining_blocks -= chunk_blocks;
    }

    return 0;
}

// Dispatcher: bulk read (called from public API)
auto remote_block_bulk_read(ker::dev::BlockDevice* dev, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }
    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }
    if (!state->rdma_attached || !state->bulk_capable) {
        return -1;
    }
    return remote_block_bulk_read_rdma(state, lba, block_count, buffer);
}

// Dispatcher: bulk write (called from public API)
auto remote_block_bulk_write(ker::dev::BlockDevice* dev, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    s_proxy_lock.lock();
    auto* state = find_proxy_by_bdev(dev);
    s_proxy_lock.unlock();
    if (!proxy_block_active(state)) {
        return -1;
    }
    if (proxy_block_fenced(state)) {
        if (!wait_for_fence_lift(state)) {
            return -1;
        }
    }
    if (ker::dev::block_device_is_read_only(dev)) {
        return -EROFS;
    }
    BlockIoLease io_lease;
    if (!io_lease.acquire(state)) {
        return -1;
    }
    if (!state->rdma_attached || !state->bulk_capable) {
        return -1;
    }
    return remote_block_bulk_write_rdma(state, lba, block_count, buffer);
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_dev_proxy_init() {
    if (g_dev_proxy_initialized) {
        return;
    }
    g_dev_proxy_initialized = true;
    ker::mod::dbg::log("[WKI] Dev proxy subsystem initialized");
}

void wki_dev_proxy_process_pending_detaches() {
    std::array<BlockDetachAttempt, BLOCK_DETACH_RETRY_BATCH> attempts{};
    size_t attempt_count = 0;
    size_t scanned = 0;

    s_proxy_lock.lock();
    ProxyBlockState* state = g_pending_detach_head;
    while (state != nullptr && scanned < BLOCK_DETACH_RETRY_SCAN && attempt_count < attempts.size()) {
        ProxyBlockState* const NEXT = state->detach_next;
        scanned++;
        if (!state->detach_retry_in_progress && !state->cleanup_in_progress) {
            state->detach_retry_in_progress = true;
            attempts.at(attempt_count++) = {
                .state = state,
                .owner_node = state->detach_owner_node,
                .resource_id = state->detach_resource_id,
                .attach_cookie = state->detach_attach_cookie,
                .incarnation = state->detach_incarnation,
                .peer_boot_epoch = state->detach_peer_boot_epoch,
                .tx_token = state->detach_tx_token,
            };
            if (state != g_pending_detach_tail) {
                unlink_pending_block_detach_locked(state);
                link_pending_block_detach_locked(state);
            }
        }
        state = NEXT;
    }
    s_proxy_lock.unlock();

    for (size_t i = 0; i < attempt_count; ++i) {
        BlockDetachAttempt const& ATTEMPT = attempts.at(i);
        bool const PEER_EPOCH_INVALIDATED = wki_peer_remote_boot_epoch_invalidated(ATTEMPT.owner_node, ATTEMPT.peer_boot_epoch);
        WkiReliableTxStatus const TX_STATUS = wki_reliable_tx_status(ATTEMPT.tx_token);
        WkiReliableTxToken replacement_token = {};
        int send_ret = WKI_ERR_BUSY;
        if (!PEER_EPOCH_INVALIDATED && (TX_STATUS == WkiReliableTxStatus::INVALID || TX_STATUS == WkiReliableTxStatus::RETIRED)) {
            send_ret =
                send_block_detach(ATTEMPT.owner_node, ATTEMPT.resource_id, ATTEMPT.attach_cookie, ATTEMPT.incarnation, &replacement_token);
        }
        finish_block_detach_attempt(ATTEMPT, TX_STATUS, send_ret, replacement_token, PEER_EPOCH_INVALIDATED);
    }
}

// -----------------------------------------------------------------------------
// Attach / Detach
// -----------------------------------------------------------------------------

auto wki_dev_proxy_attach_block(uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                                const ResourceIncarnationToken& expected_owner_incarnation, const char* local_name)
    -> ker::dev::BlockDevice* {
    uint32_t const BINDING_PEER_BOOT_EPOCH = wki_resource_incarnation_valid(expected_owner_incarnation)
                                                 ? expected_owner_incarnation.owner_boot_epoch
                                                 : wki_peer_remote_boot_epoch_snapshot(owner_node);
    // Reserve the server-idempotence cookie and publish the fully identified
    // proxy row atomically in the registry domain.
    ProxyBlockState* state = nullptr;
    uint8_t attach_cookie = 0;
    s_proxy_lock.lock();
    if (block_attach_blocked_by_retiring_binding_locked(owner_node, resource_id)) {
        s_proxy_lock.unlock();
        return nullptr;
    }
    attach_cookie = allocate_block_attach_cookie_locked(owner_node, resource_id, expected_owner_incarnation);
    if (attach_cookie == 0) {
        s_proxy_lock.unlock();
        return nullptr;
    }
    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    state = g_proxies.back().get();
    state->owner_node = owner_node;
    state->resource_id = resource_id;
    state->resource_generation = expected_resource_generation;
    state->attach_pending.store(false, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;
    state->attach_expected_cookie = 0;
    state->binding_attach_cookie = attach_cookie;
    state->attach_read_only = false;
    state->attach_expect_incarnation = wki_resource_incarnation_negotiated(owner_node, ResourceType::BLOCK);
    state->attach_expected_incarnation = expected_owner_incarnation;
    state->binding_peer_boot_epoch = BINDING_PEER_BOOT_EPOCH;
    g_routable_proxies.push_back(state);
    s_proxy_lock.unlock();
    // Proxy pointer is stable (std::deque + unique_ptr); safe to use after unlock.

    // Send DEV_ATTACH_REQ with retry logic
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_ACCESS_READ | DEV_ATTACH_ACCESS_WRITE;

    WkiChannelIdentity reserved_channel_identity{};
    if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, owner_node)) {
        if (wki_channel_alloc(owner_node, PriorityClass::THROUGHPUT, &reserved_channel_identity) == nullptr) {
            cleanup_failed_block_attach(state, reserved_channel_identity, false);
            return nullptr;
        }
        attach_req.requested_channel = reserved_channel_identity.channel_id;
    } else {
        attach_req.requested_channel = 0;
    }

    attach_req.attach_cookie = attach_cookie;
    std::array<uint8_t, sizeof(DevAttachReqPayload) + sizeof(ResourceIncarnationToken)> attach_buf{};
    std::memcpy(attach_buf.data(), &attach_req, sizeof(attach_req));
    uint16_t attach_len = sizeof(DevAttachReqPayload);
    if (state->attach_expect_incarnation) {
        if (!wki_resource_incarnation_valid(expected_owner_incarnation)) {
            cleanup_failed_block_attach(state, reserved_channel_identity, false);
            return nullptr;
        }
        std::memcpy(attach_buf.data() + sizeof(DevAttachReqPayload), &expected_owner_incarnation, sizeof(expected_owner_incarnation));
        attach_len = static_cast<uint16_t>(attach_len + sizeof(ResourceIncarnationToken));
    }
    bool attach_request_sent = false;

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] attach_block: starting attach to node=0x%04x res_id=%u cpu=%u proxies=%u", owner_node, resource_id,
                       static_cast<unsigned>(ker::mod::cpu::current_cpu()), static_cast<unsigned>(g_routable_proxies.size()));
#endif

    constexpr int MAX_ATTACH_RETRIES = 3;
    for (int retry = 0; retry < MAX_ATTACH_RETRIES; retry++) {
        const WkiChannelIdentity* const EXPECTED_CHANNEL =
            reserved_channel_identity.channel != nullptr ? &reserved_channel_identity : nullptr;
        if (!block_attach_identity_is_live(owner_node, resource_id, expected_resource_generation, expected_owner_incarnation,
                                           EXPECTED_CHANNEL)) {
            cleanup_failed_block_attach(state, reserved_channel_identity, attach_request_sent);
            return nullptr;
        }

        WkiWaitEntry wait = {};
        state->lock.lock();
        state->attach_wait_entry = &wait;
        state->attach_expected_cookie = attach_cookie;
        state->attach_pending.store(true, std::memory_order_release);
        if (retry > 0) {
            ker::mod::dbg::log("[WKI] Dev proxy attach retry %d: node=0x%04x res_id=%u pending=%d", retry, owner_node, resource_id,
                               state->attach_pending.load(std::memory_order_relaxed) ? 1 : 0);
        }
        state->lock.unlock();

        int const SEND_RET = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, attach_buf.data(), attach_len);
        if (SEND_RET != WKI_OK) {
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] attach_block: send failed err=%d retry=%d", send_ret, retry);
#endif
            cancel_attach_waiter(state, wait, SEND_RET);
            continue;  // Retry on send failure
        }
        attach_request_sent = true;

        // Async wait for attach ACK
        int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        clear_attach_waiter_after_wait(state, wait);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            continue;  // Timeout, try again
        }

        if (!state->attach_pending) {
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] attach_block: ACK received on retry=%d", retry);
#endif
            break;  // Got ACK, exit retry loop
        }
    }

    if (state->attach_pending) {
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] attach_block: TIMEOUT - setting attach_pending=false, erasing failed proxy");
#endif
        state->lock.lock();
        state->attach_expected_cookie = 0;
        state->attach_pending.store(false, std::memory_order_release);
        state->lock.unlock();
        cleanup_failed_block_attach(state, reserved_channel_identity, attach_request_sent);
        size_t const PROXIES_LEFT = proxy_count();
        ker::mod::dbg::log("[WKI] Dev proxy attach timeout after %d retries: node=0x%04x res_id=%u proxies_left=%u", MAX_ATTACH_RETRIES,
                           owner_node, resource_id, static_cast<unsigned>(PROXIES_LEFT));
        return nullptr;
    }

    // Check attach result
    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Dev proxy attach rejected: node=0x%04x res_id=%u status=%u", owner_node, resource_id,
                           state->attach_status);
        // A locally cancelled wait can publish BUSY before a delayed accepted
        // ACK. If any request entered the reliable stream, retire that exact
        // cookie even when the currently visible status is non-OK.
        cleanup_failed_block_attach(state, reserved_channel_identity, attach_request_sent);
        return nullptr;
    }

    const WkiChannelIdentity* const ACK_EXPECTED_CHANNEL =
        reserved_channel_identity.channel != nullptr ? &reserved_channel_identity : nullptr;
    if (!block_attach_identity_is_live(owner_node, resource_id, expected_resource_generation, expected_owner_incarnation,
                                       ACK_EXPECTED_CHANNEL)) {
        cleanup_failed_block_attach(state, reserved_channel_identity, true);
        return nullptr;
    }

    if (reserved_channel_identity.channel != nullptr) {
        if (state->attach_channel != reserved_channel_identity.channel_id) {
            ker::mod::dbg::log("[WKI] Dev proxy attach channel mismatch: node=0x%04x res_id=%u requested=%u assigned=%u", owner_node,
                               resource_id, reserved_channel_identity.channel_id, state->attach_channel);
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }
    } else {
        if (wki_channel_reserve(owner_node, state->attach_channel, PriorityClass::THROUGHPUT, &reserved_channel_identity) == nullptr) {
            ker::mod::dbg::log("[WKI] Dev proxy attach local reserve failed: node=0x%04x res_id=%u ch=%u", owner_node, resource_id,
                               state->attach_channel);
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }
    }

    if (!block_attach_identity_is_live(owner_node, resource_id, expected_resource_generation, expected_owner_incarnation,
                                       &reserved_channel_identity)) {
        cleanup_failed_block_attach(state, reserved_channel_identity, true);
        return nullptr;
    }

    state->lock.lock();
    state->assigned_channel = reserved_channel_identity.channel_id;
    state->assigned_channel_identity = reserved_channel_identity;
    state->max_op_size = state->attach_max_op_size;
    state->lock.unlock();

    uint64_t block_size = 0;
    uint64_t total_blocks = 0;

    // Try RDMA ring path: wait for zone to become active and read device info from ring header
    if (state->rdma_zone_id != 0) {
        mod::dbg::log("[WKI] Dev proxy attach ACK received, waiting for RDMA zone: node=0x%04x res_id=%u zone_id=0x%08x", owner_node,
                      resource_id, state->rdma_zone_id);
        uint64_t const ZONE_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
        WkiZone const* zone = nullptr;

        // Wait for the RDMA zone to appear (server creates it, zone negotiation completes)
        while (wki_now_us() < ZONE_DEADLINE) {
            zone = wki_zone_find(state->rdma_zone_id);
            if (zone != nullptr && zone->state == ZoneState::ACTIVE) {
                break;
            }
            zone = nullptr;
            asm volatile("pause" ::: "memory");
            WkiChannel* res_ch = wki_channel_get(owner_node, WKI_CHAN_RESOURCE);
            wki_spin_yield_channel(res_ch);
        }

        if (zone != nullptr) {
            state->rdma_zone_ptr = zone->local_vaddr;

            // Populate RoCE state from zone (must be done before server_ready check
            // so that roce_pull_cq etc. work correctly once we start using the ring)
            state->rdma_roce = zone->is_roce;
            state->rdma_transport = zone->rdma_transport;

            // For RoCE zones: the initiator (server) sends its rkey via a
            // ZONE_NOTIFY_POST (op_type=0xFE) after confirming the zone.  We must
            // wait for zone->remote_rkey to become non-zero before we can do any
            // RDMA operations targeting the server's zone memory.
            if (state->rdma_roce) {
                uint64_t const RKEY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
                while (zone->remote_rkey == 0 && wki_now_us() < RKEY_DEADLINE) {
                    asm volatile("pause" ::: "memory");
                    // Drive NIC + WKI RX so the rkey-exchange notification can be processed
                    net::NetDevice* net_dev = wki_eth_get_netdev();
                    if (net_dev != nullptr) {
                        net::napi_poll_all_pending();
                        net::backlog_drain_all_pending_inline();
                    }
                }
            }
            state->rdma_remote_rkey = zone->remote_rkey;

            // For RoCE zones: pull the ring header from server to get server_ready.
            // Even if rdma_remote_rkey is still 0 (rkey-exchange notification was
            // late), the server may have pushed the header via rdma_write to our
            // local zone memory.  We must poll the NIC so those RDMA_WRITE frames
            // are received and copied into our zone backing.
            if (state->rdma_roce && state->rdma_transport != nullptr && state->rdma_remote_rkey != 0) {
                state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, state->rdma_zone_ptr,
                                                 BLK_RING_HEADER_SIZE);
            }

            // Wait for server_ready flag in ring header
            auto* ring_hdr = blk_ring_header(state->rdma_zone_ptr);
            uint64_t const READY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US);
            while (ring_hdr->server_ready == 0 && wki_now_us() < READY_DEADLINE) {
                asm volatile("pause" ::: "memory");

                // Always poll NIC: the server pushes the ring header via
                // RDMA_WRITE (EtherType 0x88B8) to our local zone memory.
                // Without polling, those frames stay in the RX queue and
                // ring_hdr->server_ready is never updated locally.
                net::NetDevice* net_dev = wki_eth_get_netdev();
                if (net_dev != nullptr) {
                    net::napi_poll_all_pending();
                    net::backlog_drain_all_pending_inline();
                }

                // Also re-check rkey - the 0xFE notification may arrive
                // during the NIC poll above.
                if (state->rdma_remote_rkey == 0 && zone->remote_rkey != 0) {
                    state->rdma_remote_rkey = zone->remote_rkey;
                }

                // For RoCE with valid rkey: also actively pull the header
                if (state->rdma_roce && state->rdma_transport != nullptr && state->rdma_remote_rkey != 0) {
                    state->rdma_transport->rdma_read(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0,
                                                     state->rdma_zone_ptr, BLK_RING_HEADER_SIZE);
                }
            }

            if (ring_hdr->server_ready != 0) {
                // Read device info directly from the ring header - no OP_BLOCK_INFO needed
                block_size = ring_hdr->block_size;
                total_blocks = ring_hdr->total_blocks;
                state->rdma_attached = true;
                state->data_slot_bitmap = 0;
                state->tag_bitmap = 0;
                for (auto& tc : state->tag_completions) {
                    tc = {};
                }

                // Allocate read-ahead cache buffer (one data-slot's worth)
                state->ra_buffer = new (std::nothrow) uint8_t[ring_hdr->data_slot_size];
                if (state->ra_buffer != nullptr) {
                    state->ra_capacity = ring_hdr->data_slot_size / ring_hdr->block_size;
                } else {
                    state->ra_capacity = 0;
                }
                state->ra_valid = false;
                state->ra_block_count = 0;

                ker::mod::dbg::log("[WKI] Dev proxy RDMA ring attached: zone=0x%08x bs=%u tb=%llu slots=%u roce=%d ra=%s",
                                   state->rdma_zone_id, ring_hdr->block_size, ring_hdr->total_blocks, ring_hdr->data_slot_count,
                                   state->rdma_roce ? 1 : 0, state->ra_buffer != nullptr ? "yes" : "no");
            } else {
                ker::mod::dbg::log("[WKI] Dev proxy RDMA ring server_ready timeout - falling back to msg path");
                state->rdma_zone_id = 0;
                state->rdma_zone_ptr = nullptr;
                state->rdma_roce = false;
                state->rdma_transport = nullptr;
                state->rdma_remote_rkey = 0;
            }
        } else {
            ker::mod::dbg::log("[WKI] Dev proxy RDMA zone not found (0x%08x) - falling back to msg path", state->rdma_zone_id);
            state->rdma_zone_id = 0;
            state->rdma_roce = false;
            state->rdma_transport = nullptr;
            state->rdma_remote_rkey = 0;
        }
    }

    // Fallback: query block device info via OP_BLOCK_INFO message if no RDMA
    if (!state->rdma_attached) {
        DevOpReqPayload info_req = {};
        info_req.op_id = OP_BLOCK_INFO;
        info_req.data_len = 0;

        std::array<uint8_t, 16> info_buf = {};
        WkiWaitEntry wait = {};
        WkiChannelIdentity channel_identity = {};
        if (prepare_block_op_wait(state, OP_BLOCK_INFO, wait, static_cast<void*>(info_buf.data()), static_cast<uint16_t>(info_buf.size()),
                                  &channel_identity) != WKI_OK) {
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }

        int const SEND_RET = wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_REQ, &info_req, sizeof(info_req));
        if (SEND_RET != WKI_OK) {
            cancel_op_waiter(state, wait, SEND_RET);
            mod::dbg::log("[WKI] Dev proxy block info request send failed: node=0x%04x res_id=%u err=%d", owner_node, resource_id,
                          SEND_RET);
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }

        int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
        if (WAIT_RC != 0) {
            cancel_op_waiter(state, wait, WAIT_RC);
            mod::dbg::log("[WKI] Dev proxy block info request timeout: node=0x%04x res_id=%u", owner_node, resource_id);
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }

        int op_status = 0;
        uint16_t op_resp_len = 0;
        consume_block_op_result(state, wait, &op_status, &op_resp_len);

        if (op_status != 0 || op_resp_len < 16) {
            cleanup_failed_block_attach(state, reserved_channel_identity, true);
            return nullptr;
        }

        memcpy(&block_size, static_cast<const void*>(info_buf.data()), sizeof(uint64_t));
        memcpy(&total_blocks, static_cast<const void*>(info_buf.data() + sizeof(uint64_t)), sizeof(uint64_t));
    }

    // Populate the proxy BlockDevice
    state->bdev.major = 0;
    state->bdev.minor = 0;
    if (local_name != nullptr) {
        size_t name_len = strlen(local_name);
        if (name_len >= ker::dev::BLOCK_NAME_SIZE) {
            name_len = ker::dev::BLOCK_NAME_SIZE - 1;
        }
        memcpy(state->bdev.name.data(), local_name, name_len);
        state->bdev.name.at(name_len) = '\0';
    }
    state->bdev.capabilities = 0;
    ker::dev::block_device_set_read_only(&state->bdev, state->attach_read_only);
    state->bdev.block_size = static_cast<size_t>(block_size);
    state->bdev.total_blocks = total_blocks;
    state->bdev.read_blocks = remote_block_read;
    state->bdev.write_blocks = remote_block_write;
    state->bdev.flush = remote_block_flush;
    state->bdev.private_data = state;

    // Set up streaming bulk transfer if the server advertised support.
    static_cast<void>(setup_block_bulk_staging(state));

    // Linearize the final local publication with peer/resource lifecycle
    // changes. Construction above may block, so only this validation and the
    // registry insertion run while the lifecycle lease is held.
    bool registered = false;
    {
        BlockAttachIdentityLease const PUBLICATION_LEASE(owner_node, resource_id, expected_resource_generation, expected_owner_incarnation,
                                                         &reserved_channel_identity);
        if (PUBLICATION_LEASE) {
            // Peer lifecycle serialization also prevents two concurrent
            // resolvers for this peer from racing the name check/registration.
            if (ker::dev::block_device_find_by_name(state->bdev.name.data()) != nullptr) {
                ker::mod::dbg::log("[WKI] Dev proxy name collision: %s already registered", state->bdev.name.data());
            } else {
                // Registry readers can immediately call through the bdev, so
                // publish proxy activity before making the bdev visible.
                set_proxy_block_active(state, true);
                if (ker::dev::block_device_register(&state->bdev) == 0) {
                    state->ever_published = true;
                    registered = true;
                } else {
                    set_proxy_block_active(state, false);
                }
            }
        }
    }
    if (!registered) {
        cleanup_failed_block_attach(state, reserved_channel_identity, true);
        return nullptr;
    }

    ker::mod::dbg::log("[WKI] Dev proxy attached: %s node=0x%04x res_id=%u ch=%u bs=%u tb=%llu access=%s", state->bdev.name.data(),
                       owner_node, resource_id, state->assigned_channel, static_cast<unsigned>(block_size), total_blocks,
                       ker::dev::block_device_is_read_only(&state->bdev) ? "ro" : "rw");

    return &state->bdev;
}

void wki_dev_proxy_detach_block(ker::dev::BlockDevice* proxy_bdev) {
    // Lock to find proxy and copy info needed for cleanup outside the lock.
    ProxyBlockState* state = nullptr;
    while (true) {
        s_proxy_lock.lock();
        state = find_proxy_by_bdev(proxy_bdev);
        if (state == nullptr || !proxy_block_active(state) || state->cleanup_in_progress) {
            s_proxy_lock.unlock();
            return;
        }
        if (state->resume_in_progress) {
            s_proxy_lock.unlock();
            ker::mod::sched::kern_yield();
            continue;
        }

        state->lock.lock();
        // Consume any deferred epoch-reset work while claiming terminal
        // cleanup.  cleanup_in_progress remains set until all external
        // teardown completes.
        state->epoch_reset_pending = false;
        state->cleanup_in_progress = true;
        set_proxy_block_fenced(state, true);
        state->lock.unlock();
        s_proxy_lock.unlock();
        break;
    }

    wait_for_block_io_quiescence(state);

    s_proxy_lock.lock();
    state->lock.lock();

    // Copy info for cleanup outside the lock
    uint16_t const OWNER_NODE = state->owner_node;
    uint32_t const RESOURCE_ID = state->resource_id;
    WkiChannelIdentity const ASSIGNED_CHANNEL_IDENTITY = state->assigned_channel_identity;
    uint16_t const ASSIGNED_CHANNEL = ASSIGNED_CHANNEL_IDENTITY.channel_id;
    uint8_t const BINDING_ATTACH_COOKIE = state->binding_attach_cookie;
    ResourceIncarnationToken const BINDING_INCARNATION = state->binding_incarnation;
    bool const HAD_RDMA = state->rdma_attached;
    uint32_t const RDMA_ZONE_ID = state->rdma_zone_id;
    uint8_t const* ra_buf = state->ra_buffer;
    uint8_t const* bulk_buf = state->bulk_staging_buf;

    // Publish the exact retirement reservation before removing this state from
    // the routable index. Replacement attach admission uses the same lock.
    static_cast<void>(stage_block_detach_locked(state, OWNER_NODE, RESOURCE_ID, BINDING_ATTACH_COOKIE, BINDING_INCARNATION, false));

    // Neutralise proxy under lock so no other thread picks it up
    state->ra_buffer = nullptr;
    state->bulk_staging_buf = nullptr;
    state->bulk_capable = false;
    state->bulk_staging_rkey = 0;
    state->bulk_staging_size = 0;
    state->bulk_max_transfer = 0;
    state->attach_read_only = false;
    state->binding_attach_cookie = 0;
    state->binding_incarnation = {};
    state->assigned_channel_identity = {};
    ker::dev::block_device_set_read_only(&state->bdev, false);
    ra_invalidate(state);
    set_proxy_block_active(state, false);
    unindex_proxy_locked(state);
    if (HAD_RDMA) {
        state->rdma_attached = false;
        state->rdma_zone_ptr = nullptr;
        state->rdma_zone_id = 0;
        state->rdma_roce = false;
        state->rdma_transport = nullptr;
        state->rdma_remote_rkey = 0;
    }
    state->lock.unlock();
    s_proxy_lock.unlock();

    // --- Cleanup outside lock (no blocking operations while holding spinlock) ---

    // Free read-ahead cache

    delete[] ra_buf;

    // Free bulk staging buffer

    delete[] bulk_buf;

    // Destroy RDMA zone before sending detach
    if (HAD_RDMA && RDMA_ZONE_ID != 0) {
        wki_zone_destroy(RDMA_ZONE_ID);
    }

    // Unregister from block device subsystem
    ker::dev::block_device_unregister(proxy_bdev);

    // Close the dynamic channel
    static_cast<void>(wki_channel_close_generation(ASSIGNED_CHANNEL_IDENTITY.channel, ASSIGNED_CHANNEL_IDENTITY.peer_node_id,
                                                   ASSIGNED_CHANNEL_IDENTITY.channel_id, ASSIGNED_CHANNEL_IDENTITY.generation));

    ker::mod::dbg::log("[WKI] Dev proxy detached: node=0x%04x res=%u ch=%u", OWNER_NODE, RESOURCE_ID, ASSIGNED_CHANNEL);

    s_proxy_lock.lock();
    state->cleanup_in_progress = false;
    bool const DETACH_PENDING = state->detach_pending;
    s_proxy_lock.unlock();
    if (DETACH_PENDING) {
        wki_deferred_work_notify();
    }

    // Published ProxyBlockState storage is permanent: bdev/private_data may
    // still be held by a raw block-device user after registry removal.
}

// -----------------------------------------------------------------------------
// Fencing - suspend / resume / hard teardown
// -----------------------------------------------------------------------------

void wki_dev_proxy_mark_epoch_reset(uint16_t node_id) {
    uint64_t const NOW = wki_now_us();
    ProxyBlockState* wake_head = nullptr;

    // One bounded pass over live/pending states.  Published tombstones remain
    // in g_proxies for raw-pointer safety but never enter this ingress index.
    s_proxy_lock.lock();
    for (auto* state : g_routable_proxies) {
        if (!proxy_block_active(state) || state->owner_node != node_id || state->epoch_reset_pending || state->cleanup_in_progress) {
            continue;
        }
        state->lock.lock();
        state->epoch_reset_pending = true;
        state->op_status = static_cast<int16_t>(WKI_ERR_PEER_FENCED);
        state->op_pending.store(false, std::memory_order_release);
        state->attach_status = static_cast<uint8_t>(DevAttachStatus::BUSY);
        state->attach_expected_cookie = 0;
        state->attach_pending.store(false, std::memory_order_release);
        state->epoch_op_waiter_to_wake = claim_and_clear_waiter_locked(state->op_wait_entry);
        state->epoch_attach_waiter_to_wake = claim_and_clear_waiter_locked(state->attach_wait_entry);
        set_proxy_block_fenced(state, true);
        state->fence_time_us = NOW;
        if (state->epoch_op_waiter_to_wake != nullptr || state->epoch_attach_waiter_to_wake != nullptr) {
            state->epoch_wake_next = wake_head;
            wake_head = state;
        }
        state->lock.unlock();
    }
    s_proxy_lock.unlock();

    while (wake_head != nullptr) {
        ProxyBlockState* const state = wake_head;
        wake_head = state->epoch_wake_next;
        WkiWaitEntry* const op_waiter = state->epoch_op_waiter_to_wake;
        WkiWaitEntry* const attach_waiter = state->epoch_attach_waiter_to_wake;
        state->epoch_wake_next = nullptr;
        state->epoch_op_waiter_to_wake = nullptr;
        state->epoch_attach_waiter_to_wake = nullptr;
        finish_claimed_waiter(op_waiter, WKI_ERR_PEER_FENCED);
        finish_claimed_waiter(attach_waiter, WKI_ERR_PEER_FENCED);
    }
}

void wki_dev_proxy_cleanup_epoch_reset_for_peer(uint16_t node_id, bool retire_proxy, bool owner_reboot_proven) {
    while (true) {
        ProxyBlockState* state = nullptr;
        WkiChannelIdentity channel_identity = {};
        uint32_t rdma_zone_id = 0;
        uint8_t* ra_buf = nullptr;
        uint8_t* bulk_buf = nullptr;
        WkiWaitEntry* op_waiter = nullptr;
        WkiWaitEntry* attach_waiter = nullptr;
        bool unregister_bdev = false;
        bool detach_staged = false;
        bool wait_for_resume = false;

        s_proxy_lock.lock();
        for (auto* proxy : g_routable_proxies) {
            if (proxy->owner_node != node_id || !proxy->epoch_reset_pending || proxy->cleanup_in_progress) {
                continue;
            }
            if (proxy->resume_in_progress) {
                wait_for_resume = true;
                continue;
            }
            state = proxy;
            break;
        }
        if (state == nullptr) {
            s_proxy_lock.unlock();
            if (wait_for_resume) {
                ker::mod::sched::kern_yield();
                continue;
            }
            break;
        }

        state->epoch_reset_pending = false;
        state->cleanup_in_progress = true;
        s_proxy_lock.unlock();

        wait_for_block_io_quiescence(state);

        s_proxy_lock.lock();
        state->lock.lock();
        channel_identity = state->assigned_channel_identity;
        rdma_zone_id = state->rdma_zone_id;
        ra_buf = state->ra_buffer;
        bulk_buf = state->bulk_staging_buf;
        state->ra_buffer = nullptr;
        state->bulk_staging_buf = nullptr;
        state->bulk_capable = false;
        state->bulk_staging_rkey = 0;
        state->bulk_staging_size = 0;
        state->bulk_max_transfer = 0;
        state->bdev.capabilities &= ~ker::dev::BDEV_CAP_BULK_RDMA;
        ra_invalidate(state);
        op_waiter = claim_and_clear_waiter_locked(state->op_wait_entry);
        attach_waiter = claim_and_clear_waiter_locked(state->attach_wait_entry);
        clear_block_op_state_locked(state, WKI_ERR_PEER_FENCED);
        state->attach_pending.store(false, std::memory_order_release);
        state->attach_expected_cookie = 0;
        state->assigned_channel_identity = {};
        state->rdma_attached = false;
        state->rdma_zone_ptr = nullptr;
        state->rdma_zone_id = 0;
        state->data_slot_bitmap = 0;
        state->tag_bitmap = 0;
        for (auto& completion : state->tag_completions) {
            completion = {};
        }
        state->rdma_roce = false;
        state->rdma_transport = nullptr;
        state->rdma_remote_rkey = 0;
        if (retire_proxy) {
            // Disconnect and fence timeout require terminal local teardown but
            // do not prove the server forgot this binding. Reserve the exact
            // tuple before clearing/unindexing it; only a concrete reboot can
            // safely skip the detach.
            if (!owner_reboot_proven) {
                detach_staged = stage_block_detach_locked(state, state->owner_node, state->resource_id, state->binding_attach_cookie,
                                                          state->binding_incarnation, false);
            }
            unregister_bdev = state->ever_published && proxy_block_active(state);
            set_proxy_block_active(state, false);
            unindex_proxy_locked(state);
            set_proxy_block_fenced(state, false);
            state->fence_time_us = 0;
            state->binding_attach_cookie = 0;
            state->binding_incarnation = {};
            state->resume_pending = false;
        } else {
            state->resume_pending = true;
        }
        state->lock.unlock();
        s_proxy_lock.unlock();

        finish_claimed_waiter(op_waiter, WKI_ERR_PEER_FENCED);
        finish_claimed_waiter(attach_waiter, WKI_ERR_PEER_FENCED);
        static_cast<void>(wki_channel_close_generation(channel_identity.channel, channel_identity.peer_node_id, channel_identity.channel_id,
                                                       channel_identity.generation));
        if (rdma_zone_id != 0) {
            wki_zone_destroy(rdma_zone_id);
        }
        delete[] ra_buf;
        delete[] bulk_buf;
        if (unregister_bdev) {
            ker::dev::block_device_unregister(&state->bdev);
        }

        s_proxy_lock.lock();
        state->cleanup_in_progress = false;
        s_proxy_lock.unlock();
        if (detach_staged) {
            wki_deferred_work_notify();
        }
    }
}

void wki_dev_proxy_suspend_for_peer(uint16_t node_id) {
    wki_dev_proxy_mark_epoch_reset(node_id);
    wki_dev_proxy_cleanup_epoch_reset_for_peer(node_id, false, false);
}

auto wki_dev_proxy_reactivate_resource_observation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation,
                                                   const ResourceIncarnationToken& owner_incarnation) -> bool {
    bool queued = false;
    s_proxy_lock.lock();
    for (auto* state : g_routable_proxies) {
        if (!proxy_block_active(state) || !proxy_block_fenced(state) || state->owner_node != owner_node ||
            state->resource_id != resource_id || state->resource_generation != resource_generation) {
            continue;
        }

        state->lock.lock();
        bool const SAME_INCARNATION = block_detach_incarnation_equal(state->binding_incarnation, owner_incarnation);
        state->lock.unlock();
        if (!SAME_INCARNATION) {
            continue;
        }
        state->resume_pending = true;
        queued = true;
    }
    s_proxy_lock.unlock();
    return queued;
}

void wki_dev_proxy_resume_for_peer(uint16_t node_id) {
    while (true) {
        // Collect matching proxies under lock (deque pointer stability guarantees
        // the raw pointers remain valid after unlock as long as no erase occurs;
        // erases only happen in detach/cleanup paths which mark active=false first).
        std::array<ProxyBlockState*, MAX_PROXY_SCRATCH> to_resume = {};
        size_t resume_count = 0;

        s_proxy_lock.lock();
        for (auto* p : g_routable_proxies) {
            if (proxy_block_active(p) && proxy_block_fenced(p) && p->resume_pending && p->owner_node == node_id &&
                !p->cleanup_in_progress && !p->resume_in_progress && !p->detach_pending && resume_count < to_resume.size()) {
                p->resume_pending = false;
                p->resume_in_progress = true;
                to_resume.at(resume_count++) = p;
            }
        }
        s_proxy_lock.unlock();
        if (resume_count == 0) {
            break;
        }

        for (size_t ri = 0; ri < resume_count; ri++) {
            auto* p = to_resume.at(ri);
            BlockResumeClaim const RESUME_CLAIM(p);

            // Re-check the preclaimed batch item under the claim-domain lock.
            // A new epoch reset should cancel all remaining batch entries
            // promptly instead of making them run full attach retries.
            s_proxy_lock.lock();
            bool const CAN_RESUME =
                proxy_block_active(p) && proxy_block_fenced(p) && !p->epoch_reset_pending && !p->cleanup_in_progress && !p->detach_pending;
            s_proxy_lock.unlock();
            if (!CAN_RESUME) {
                continue;
            }

            // The owner may never have observed the partition, so the old
            // binding can still be live even though its channel was closed
            // locally. Keep its exact detach in the reliable resource stream
            // until it is ACKed before allocating or publishing a replacement
            // cookie. Retirement/retry remains in the paced detach worker;
            // ACK completion requeues this proxy for resume.
            uint8_t previous_attach_cookie = 0;
            ResourceIncarnationToken previous_binding_incarnation = {};
            s_proxy_lock.lock();
            bool const CAN_RETIRE_OLD_BINDING =
                proxy_block_active(p) && proxy_block_fenced(p) && !p->epoch_reset_pending && !p->cleanup_in_progress && !p->detach_pending;
            if (CAN_RETIRE_OLD_BINDING) {
                p->lock.lock();
                if (p->resume_detach_confirmed) {
                    p->resume_detach_confirmed = false;
                    p->binding_attach_cookie = 0;
                }
                previous_attach_cookie = p->binding_attach_cookie;
                previous_binding_incarnation = p->binding_incarnation;
                if (previous_attach_cookie != 0) {
                    p->resume_after_detach = true;
                    p->resume_detach_confirmed = false;
                }
                p->lock.unlock();
            }
            s_proxy_lock.unlock();
            if (!CAN_RETIRE_OLD_BINDING) {
                continue;
            }
            if (previous_attach_cookie != 0) {
                static_cast<void>(
                    send_or_defer_block_detach(p, node_id, p->resource_id, previous_attach_cookie, previous_binding_incarnation));
                continue;
            }

            // Re-attach: send DEV_ATTACH_REQ to get a fresh dynamic channel
            DevAttachReqPayload attach_req = {};
            attach_req.target_node = node_id;
            attach_req.resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
            attach_req.resource_id = p->resource_id;
            attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY) | DEV_ATTACH_ACCESS_READ | DEV_ATTACH_ACCESS_WRITE;
            WkiChannelIdentity rebound_channel_identity = {};
            if (wki_requester_controls_dynamic_channel(g_wki.my_node_id, node_id)) {
                if (wki_channel_alloc(node_id, PriorityClass::THROUGHPUT, &rebound_channel_identity) == nullptr) {
                    continue;
                }
                attach_req.requested_channel = rebound_channel_identity.channel_id;
            } else {
                attach_req.requested_channel = 0;
            }

            ResourceIncarnationToken resume_incarnation = {};
            p->lock.lock();
            resume_incarnation = p->binding_incarnation;
            p->lock.unlock();
            uint32_t const RESUME_PEER_BOOT_EPOCH = wki_resource_incarnation_valid(resume_incarnation)
                                                        ? resume_incarnation.owner_boot_epoch
                                                        : wki_peer_remote_boot_epoch_snapshot(node_id);

            // Keep selection and publication in the same registry critical
            // section. In particular, exclude the old binding cookie before
            // replacing it with the fresh reservation.
            uint8_t attach_cookie = 0;
            s_proxy_lock.lock();
            bool const STILL_RESUMABLE =
                proxy_block_active(p) && proxy_block_fenced(p) && !p->epoch_reset_pending && !p->cleanup_in_progress && !p->detach_pending;
            if (STILL_RESUMABLE) {
                attach_cookie = allocate_block_attach_cookie_locked(node_id, p->resource_id, resume_incarnation);
            }
            if (attach_cookie != 0) {
                p->lock.lock();
                p->attach_pending.store(false, std::memory_order_release);
                p->attach_status = 0;
                p->attach_channel = 0;
                p->attach_read_only = false;
                p->attach_expected_cookie = 0;
                p->attach_expect_incarnation = wki_resource_incarnation_negotiated(node_id, ResourceType::BLOCK);
                p->attach_expected_incarnation = resume_incarnation;
                p->binding_attach_cookie = attach_cookie;
                p->binding_peer_boot_epoch = RESUME_PEER_BOOT_EPOCH;
                p->lock.unlock();
            }
            s_proxy_lock.unlock();
            if (attach_cookie == 0) {
                static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                               rebound_channel_identity.channel_id, rebound_channel_identity.generation));
                continue;
            }

            attach_req.attach_cookie = attach_cookie;
            std::array<uint8_t, sizeof(DevAttachReqPayload) + sizeof(ResourceIncarnationToken)> attach_buf{};
            std::memcpy(attach_buf.data(), &attach_req, sizeof(attach_req));
            uint16_t attach_len = sizeof(DevAttachReqPayload);
            if (p->attach_expect_incarnation) {
                if (!wki_resource_incarnation_valid(p->attach_expected_incarnation)) {
                    p->attach_pending.store(false, std::memory_order_release);
                    static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                                   rebound_channel_identity.channel_id,
                                                                   rebound_channel_identity.generation));
                    continue;
                }
                std::memcpy(attach_buf.data() + sizeof(DevAttachReqPayload), &p->attach_expected_incarnation,
                            sizeof(p->attach_expected_incarnation));
                attach_len = static_cast<uint16_t>(attach_len + sizeof(ResourceIncarnationToken));
            }

            constexpr int MAX_RESUME_RETRIES = 5;
            bool attached = false;
            bool attach_request_sent = false;
            for (int retry = 0; retry < MAX_RESUME_RETRIES; retry++) {
                const WkiChannelIdentity* const EXPECTED_CHANNEL =
                    rebound_channel_identity.channel != nullptr ? &rebound_channel_identity : nullptr;
                if (!block_attach_identity_is_live(node_id, p->resource_id, p->resource_generation, p->binding_incarnation,
                                                   EXPECTED_CHANNEL)) {
                    break;
                }
                WkiWaitEntry wait = {};
                p->lock.lock();
                p->attach_wait_entry = &wait;
                p->attach_expected_cookie = attach_cookie;
                p->attach_pending.store(true, std::memory_order_release);
                p->lock.unlock();

                int const SEND_RET = wki_send(node_id, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, attach_buf.data(), attach_len);
                if (SEND_RET != WKI_OK) {
                    cancel_attach_waiter(p, wait, SEND_RET);
                    continue;
                }
                attach_request_sent = true;

                int const WAIT_RC = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
                clear_attach_waiter_after_wait(p, wait);
                if (WAIT_RC == WKI_ERR_TIMEOUT) {
                    continue;
                }

                if (!p->attach_pending && p->attach_status == static_cast<uint8_t>(DevAttachStatus::OK)) {
                    attached = true;
                    break;
                }
            }

            if (!attached) {
                ker::mod::dbg::log("[WKI] Dev proxy resume FAILED (re-attach): %s node=0x%04x - will hard-detach", p->bdev.name.data(),
                                   node_id);
                p->lock.lock();
                p->attach_expected_cookie = 0;
                p->attach_pending.store(false, std::memory_order_release);
                p->lock.unlock();
                if (attach_request_sent) {
                    static_cast<void>(
                        send_or_defer_block_detach(p, node_id, p->resource_id, attach_cookie, p->attach_expected_incarnation));
                }
                static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                               rebound_channel_identity.channel_id, rebound_channel_identity.generation));
                // Leave fenced=true; the fence_timeout_tick will clean it up
                continue;
            }

            if (rebound_channel_identity.channel != nullptr) {
                if (p->attach_channel != rebound_channel_identity.channel_id) {
                    static_cast<void>(
                        send_or_defer_block_detach(p, node_id, p->resource_id, p->binding_attach_cookie, p->binding_incarnation));
                    static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                                   rebound_channel_identity.channel_id,
                                                                   rebound_channel_identity.generation));
                    continue;
                }
            } else if (wki_channel_reserve(node_id, p->attach_channel, PriorityClass::THROUGHPUT, &rebound_channel_identity) == nullptr) {
                static_cast<void>(send_or_defer_block_detach(p, node_id, p->resource_id, p->binding_attach_cookie, p->binding_incarnation));
                continue;
            }
            if (!block_attach_identity_is_live(node_id, p->resource_id, p->resource_generation, p->binding_incarnation,
                                               &rebound_channel_identity)) {
                static_cast<void>(send_or_defer_block_detach(p, node_id, p->resource_id, p->binding_attach_cookie, p->binding_incarnation));
                static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                               rebound_channel_identity.channel_id, rebound_channel_identity.generation));
                continue;
            }

            // Publish the exact fresh channel allocation before lifting the fence.
            p->lock.lock();
            p->assigned_channel = rebound_channel_identity.channel_id;
            p->assigned_channel_identity = rebound_channel_identity;
            p->max_op_size = p->attach_max_op_size;
            ker::dev::block_device_set_read_only(&p->bdev, p->attach_read_only);
            p->lock.unlock();

            // Re-attach RDMA zone if the new attach ACK provided one (RDMA ops outside lock)
            if (p->rdma_zone_id != 0 && !p->rdma_attached) {
                WkiZone const* zone = wki_zone_find(p->rdma_zone_id);
                if (zone != nullptr && zone->state == ZoneState::ACTIVE) {
                    p->rdma_zone_ptr = zone->local_vaddr;
                    p->rdma_roce = zone->is_roce;
                    p->rdma_transport = zone->rdma_transport;
                    p->rdma_remote_rkey = zone->remote_rkey;

                    // For RoCE: pull ring header from server to check server_ready
                    if (p->rdma_roce && p->rdma_transport != nullptr) {
                        p->rdma_transport->rdma_read(p->rdma_transport, p->owner_node, p->rdma_remote_rkey, 0, p->rdma_zone_ptr,
                                                     BLK_RING_HEADER_SIZE);
                    }

                    auto* ring_hdr = blk_ring_header(p->rdma_zone_ptr);
                    if (ring_hdr->server_ready != 0) {
                        p->rdma_attached = true;
                        p->data_slot_bitmap = 0;
                        p->tag_bitmap = 0;
                        for (auto& tc : p->tag_completions) {
                            tc = {};
                        }

                        // Re-allocate read-ahead cache if needed
                        if (p->ra_buffer == nullptr) {
                            p->ra_buffer = new (std::nothrow) uint8_t[ring_hdr->data_slot_size];
                            if (p->ra_buffer != nullptr) {
                                p->ra_capacity = ring_hdr->data_slot_size / ring_hdr->block_size;
                            }
                        }
                        ra_invalidate(p);

                        ker::mod::dbg::log("[WKI] Dev proxy RDMA ring re-attached on resume: zone=0x%08x roce=%d", p->rdma_zone_id,
                                           p->rdma_roce ? 1 : 0);
                    }
                }
            }

            // Epoch cleanup releases the old registered staging buffer.  A
            // successful re-attach must register a fresh consumer buffer and
            // restore the public bulk capability before the fence is lifted.
            static_cast<void>(setup_block_bulk_staging(p));

            // Resource replacement is independent of HELLO epoch markers. The
            // exact resource/channel identity therefore has to remain leased
            // across the final cancellation check and fence publication. Use a
            // retrying try-acquire so epoch cleanup (which waits for this resume
            // claim) can mark cancellation instead of deadlocking on the gate.
            bool cancelled = false;
            BlockAttachIdentityLease RESUME_PUBLICATION_LEASE;
            while (true) {
                BlockAttachLeaseTryResult const LEASE_RESULT = RESUME_PUBLICATION_LEASE.try_acquire(
                    node_id, p->resource_id, p->resource_generation, p->binding_incarnation, &rebound_channel_identity);
                if (LEASE_RESULT == BlockAttachLeaseTryResult::ACQUIRED) {
                    break;
                }
                s_proxy_lock.lock();
                cancelled = LEASE_RESULT == BlockAttachLeaseTryResult::STALE || p->epoch_reset_pending || p->cleanup_in_progress ||
                            !proxy_block_active(p);
                s_proxy_lock.unlock();
                if (cancelled) {
                    break;
                }
                ker::mod::sched::kern_yield();
            }

            if (!cancelled) {
                s_proxy_lock.lock();
                cancelled = p->epoch_reset_pending || p->cleanup_in_progress || !proxy_block_active(p);
                if (!cancelled) {
                    set_proxy_block_fenced(p, false);
                    p->fence_time_us = 0;
                }
                s_proxy_lock.unlock();
            }
            if (cancelled) {
                static_cast<void>(send_or_defer_block_detach(p, node_id, p->resource_id, attach_cookie, p->binding_incarnation));
                static_cast<void>(wki_channel_close_generation(rebound_channel_identity.channel, rebound_channel_identity.peer_node_id,
                                                               rebound_channel_identity.channel_id, rebound_channel_identity.generation));
                continue;
            }

            ker::mod::dbg::log("[WKI] Dev proxy resumed: %s node=0x%04x ch=%u access=%s - blocked I/O will now proceed",
                               p->bdev.name.data(), node_id, p->assigned_channel,
                               ker::dev::block_device_is_read_only(&p->bdev) ? "ro" : "rw");
        }
    }
}

void wki_dev_proxy_detach_all_for_peer(uint16_t node_id) {
    wki_dev_proxy_mark_epoch_reset(node_id);
    wki_dev_proxy_cleanup_epoch_reset_for_peer(node_id, true, false);
}

void wki_dev_proxy_fence_timeout_tick(uint64_t now_us) {
    while (true) {
        ProxyBlockState* timed_out = nullptr;
        uint16_t owner_node = WKI_NODE_INVALID;
        uint64_t elapsed = 0;

        s_proxy_lock.lock();
        for (auto* proxy : g_routable_proxies) {
            if (!proxy_block_active(proxy) || !proxy_block_fenced(proxy) || proxy->epoch_reset_pending || proxy->cleanup_in_progress ||
                proxy->resume_in_progress || proxy->fence_time_us == 0 || now_us < proxy->fence_time_us) {
                continue;
            }
            elapsed = now_us - proxy->fence_time_us;
            if (elapsed < WKI_DEV_PROXY_FENCE_WAIT_US) {
                continue;
            }
            timed_out = proxy;
            owner_node = proxy->owner_node;
            proxy->epoch_reset_pending = true;
            break;
        }
        s_proxy_lock.unlock();

        if (timed_out == nullptr) {
            break;
        }
        ker::mod::dbg::log("[WKI] Dev proxy fence timeout (%llu s): %s node=0x%04x - tearing down", elapsed / 1000000ULL,
                           timed_out->bdev.name.data(), owner_node);
        wki_dev_proxy_cleanup_epoch_reset_for_peer(owner_node, true, false);
    }
}

auto wki_dev_proxy_batch_read(ker::dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int {
    if (bdev == nullptr || ranges == nullptr || count == 0) {
        return -1;
    }
    return remote_block_read_batch(bdev, ranges, count);
}

auto wki_dev_proxy_batch_write(ker::dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int {
    if (bdev == nullptr || ranges == nullptr || count == 0) {
        return -1;
    }
    return remote_block_write_batch(bdev, ranges, count);
}

// -----------------------------------------------------------------------------
// Streaming Bulk Transfer - public API
// -----------------------------------------------------------------------------

auto wki_dev_proxy_bulk_read(ker::dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr || block_count == 0) {
        return -1;
    }
    return remote_block_bulk_read(bdev, lba, block_count, buffer);
}

auto wki_dev_proxy_bulk_write(ker::dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr || block_count == 0) {
        return -1;
    }
    return remote_block_bulk_write(bdev, lba, block_count, buffer);
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: src=0x%04x ch=%u payload_len=%u", hdr->src_node, hdr->channel_id, payload_len);
#endif

    if (payload_len < sizeof(DevAttachAckPayload)) {
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: payload too small (%u < %u)", payload_len,
                           static_cast<unsigned>(sizeof(DevAttachAckPayload)));
#endif
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: status=%u assigned_ch=%u max_op=%u", ack->status, ack->assigned_channel,
                       ack->max_op_size);

    // Find the pending attach for this owner node
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: searching %u proxies for node 0x%04x (cpu=%u)",
                       static_cast<unsigned>(g_routable_proxies.size()), hdr->src_node,
                       static_cast<unsigned>(ker::mod::cpu::current_cpu()));
    for (size_t i = 0; i < g_routable_proxies.size(); i++) {
        auto* p = g_routable_proxies[i];
        ker::mod::dbg::log("[WKI-DBG]   proxy[%u]: owner=0x%04x attach_pending=%d active=%d", static_cast<unsigned>(i), p->owner_node,
                           p->attach_pending ? 1 : 0, proxy_block_active(p) ? 1 : 0);
    }
#endif
    WkiWaitEntry* wait_entry = nullptr;
    s_proxy_lock.lock();
    ProxyBlockState* state = find_proxy_by_attach(hdr->src_node, ack->resource_id, ack->reserved);
    if (state == nullptr) {
        s_proxy_lock.unlock();
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: NO proxy found for node 0x%04x res_id=%u", hdr->src_node, ack->resource_id);
#endif
        return;
    }

#ifdef DEBUG_WKI_TRANSPORT
    ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: proxy found, clearing attach_pending");
#endif

    state->lock.lock();
    if (!block_attach_ack_matches_pending_locked(state, *ack, payload, payload_len)) {
        state->lock.unlock();
        s_proxy_lock.unlock();
        return;
    }
    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;
    state->attach_read_only = (ack->rdma_flags & DEV_ATTACH_READ_ONLY) != 0;
    if (ack->status == static_cast<uint8_t>(DevAttachStatus::OK)) {
        state->binding_attach_cookie = ack->reserved;
        state->binding_incarnation = state->attach_expected_incarnation;
    }

    // Check for RDMA block ring zone in the extended payload
    if (payload_len >= sizeof(DevAttachAckPayload) && (ack->rdma_flags & DEV_ATTACH_RDMA_BLK_RING) != 0 && ack->blk_zone_id != 0) {
        state->rdma_zone_id = ack->blk_zone_id;
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: RDMA blk ring zone_id=0x%08x", ack->blk_zone_id);
#endif
    }

    // Check for bulk RDMA transfer support
    if (payload_len >= sizeof(DevAttachAckPayload) && (ack->rdma_flags & DEV_ATTACH_RDMA_BULK) != 0) {
        state->bulk_capable = true;
        // Default max transfer size if not explicitly negotiated (2 MB)
        state->bulk_max_transfer = 2 * 1024 * 1024;
#ifdef DEBUG_WKI_TRANSPORT
        ker::mod::dbg::log("[WKI-DBG] handle_dev_attach_ack: bulk transfer supported, max=%u", state->bulk_max_transfer);
#endif
    }

    // Release the waiter
    state->attach_expected_cookie = 0;
    state->attach_pending.store(false, std::memory_order_release);
    wait_entry = claim_and_clear_waiter_locked(state->attach_wait_entry);
    state->lock.unlock();
    s_proxy_lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
}

}  // namespace detail

auto wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion() -> bool {
    ProxyBlockState state = {};
    DevAttachAckPayload ack = {};
    state.attach_pending.store(true, std::memory_order_release);
    state.attach_expected_cookie = 0x42;

    ack.reserved = 0x41;
    if (block_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    ack.reserved = 0x42;
    if (!block_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    state.attach_expected_cookie = 0;
    if (block_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack))) {
        return false;
    }

    state.attach_expected_cookie = 0x42;
    state.attach_pending.store(false, std::memory_order_release);
    return !block_attach_ack_matches_pending_locked(&state, ack, reinterpret_cast<const uint8_t*>(&ack), sizeof(ack));
}

auto wki_dev_proxy_selftest_failed_attach_erases_exact_proxy() -> bool {
    s_proxy_lock.lock();
    size_t const START_SIZE = g_proxies.size();

    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    auto* first = g_proxies.back().get();
    g_proxies.push_back(std::make_unique<ProxyBlockState>());
    auto* second = g_proxies.back().get();

    bool const ERASED_FIRST = erase_proxy_exact_locked(first);
    bool second_still_present = false;
    for (auto const& proxy : g_proxies) {
        if (proxy.get() == second) {
            second_still_present = true;
            break;
        }
    }
    bool const ONE_PROXY_REMOVED = g_proxies.size() == START_SIZE + 1;
    bool const ERASED_SECOND = erase_proxy_exact_locked(second);
    bool const SIZE_RESTORED = g_proxies.size() == START_SIZE;

    s_proxy_lock.unlock();
    return ERASED_FIRST && second_still_present && ONE_PROXY_REMOVED && ERASED_SECOND && SIZE_RESTORED;
}

auto wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence() -> bool {
    ProxyBlockState state = {};
    BlkRingHeader ring = {};
    ring.sq_depth = 2;

    ring.sq_head = 1;
    ring.sq_tail = 0;
    state.active.store(true, std::memory_order_release);
    state.fenced.store(true, std::memory_order_release);
    WkiChannelIdentity const NO_CHANNEL = {};
    bool const FENCED_FULL_RING_STOPS =
        !wait_for_rdma_sq_space(&state, &ring, NO_CHANNEL, wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US));

    state.fenced.store(false, std::memory_order_release);
    state.active.store(false, std::memory_order_release);
    bool const INACTIVE_FULL_RING_STOPS =
        !wait_for_rdma_sq_space(&state, &ring, NO_CHANNEL, wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US));

    ring.sq_head = 0;
    ring.sq_tail = 0;
    state.active.store(true, std::memory_order_release);
    bool const AVAILABLE_RING_PROCEEDS =
        wait_for_rdma_sq_space(&state, &ring, NO_CHANNEL, wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US));

    return FENCED_FULL_RING_STOPS && INACTIVE_FULL_RING_STOPS && AVAILABLE_RING_PROCEEDS;
}

namespace detail {

void handle_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& rx_channel_identity) {
    if (payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t const RESP_DATA_LEN = resp->data_len;

    // Verify data fits
    if (sizeof(DevOpRespPayload) + RESP_DATA_LEN > payload_len) {
        return;
    }

    WkiWaitEntry* wait_entry = nullptr;
    s_proxy_lock.lock();
    ProxyBlockState* state = find_proxy_by_channel(rx_channel_identity);
    if (state == nullptr) {
        s_proxy_lock.unlock();
        return;
    }

    state->lock.lock();
    if (!channel_identity_equal(state->assigned_channel_identity, rx_channel_identity) ||
        !state->op_pending.load(std::memory_order_acquire)) {
        state->lock.unlock();
        s_proxy_lock.unlock();
        return;
    }

    if (!wki_dev_op_response_matches_expected(state->op_expected_id, state->op_expected_seq, *resp)) {
        ker::mod::dbg::log("[WKI] Ignoring stale block response: node=0x%04x ch=%u resp_op=%u expected_op=%u resp_seq=%u expected_seq=%u",
                           hdr->src_node, hdr->channel_id, resp->op_id, state->op_expected_id, resp->reserved, state->op_expected_seq);
        state->lock.unlock();
        s_proxy_lock.unlock();
        return;
    }

    wait_entry = claim_and_clear_waiter_locked(state->op_wait_entry);
    if (wait_entry != nullptr) {
        state->op_status = resp->status;

        // Copy response data if there's a destination buffer
        if (RESP_DATA_LEN > 0 && state->op_resp_buf != nullptr) {
            uint16_t const COPY_LEN = (RESP_DATA_LEN > state->op_resp_max) ? state->op_resp_max : RESP_DATA_LEN;
            memcpy(state->op_resp_buf, resp_data, COPY_LEN);
            state->op_resp_len = COPY_LEN;
        } else {
            state->op_resp_len = 0;
        }
    }
    state->lock.unlock();
    s_proxy_lock.unlock();

    finish_claimed_waiter(wait_entry, 0);
}

}  // namespace detail

}  // namespace ker::net::wki
