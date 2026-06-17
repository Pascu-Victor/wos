#include "zone.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/wki/event.hpp>
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <vector>

#include "platform/mm/addr.hpp"
#include "platform/mm/phys.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Zone table - static storage for all zones on this node
// -----------------------------------------------------------------------------

namespace {

std::array<WkiZone, WKI_MAX_ZONES> s_zone_table;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_zone_table_lock;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_zone_initialized = false;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_zone_slot(uint32_t zone_id) -> WkiZone* {
    for (auto& zone : s_zone_table) {
        if (zone.state != ZoneState::NONE && zone.zone_id == zone_id) {
            return &zone;
        }
    }
    return nullptr;
}

auto alloc_zone_slot() -> WkiZone* {
    for (auto& zone : s_zone_table) {
        if (zone.state == ZoneState::NONE && !zone.retiring.load(std::memory_order_acquire) &&
            zone.refs.load(std::memory_order_acquire) == 0) {
            return &zone;
        }
    }
    return nullptr;
}

void free_zone_backing(WkiZone* zone) {
    if (zone->local_vaddr != nullptr) {
        if (zone->is_rdma && !zone->is_roce) {
            // ivshmem RDMA-backed: free from ivshmem RDMA pool
            auto offset = static_cast<int64_t>(zone->local_phys_addr);
            wki_ivshmem_rdma_free(offset, zone->size);
        } else {
            // RoCE-backed or non-RDMA: regular kernel pages
            ker::mod::mm::phys::page_free(zone->local_vaddr);
        }
    }
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->rdma_transport = nullptr;
}

auto allocate_zone_backing(uint32_t size) -> void* {
    // Allocate physically contiguous pages from buddy allocator
    return ker::mod::mm::phys::page_alloc(size);
}

auto zone_range_valid(uint32_t offset, uint32_t len, uint32_t size) -> bool { return offset <= size && len <= size - offset; }

auto allocate_zone_op_cookie_locked(WkiZone* zone) -> uint32_t {
    uint32_t cookie = zone->next_op_cookie;
    if (cookie == 0) {
        cookie = 1;
    }
    zone->next_op_cookie = cookie + 1U;
    if (zone->next_op_cookie == 0) {
        zone->next_op_cookie = 1;
    }
    return cookie;
}

auto zone_read_response_matches_pending(const WkiZone& zone, const ZoneReadRespPayload& resp) -> bool {
    return resp.op_cookie == zone.read_expected_cookie && resp.offset == zone.read_expected_offset && resp.length == zone.read_expected_len;
}

auto zone_write_ack_matches_pending(const WkiZone& zone, const ZoneWriteAckPayload& ack) -> bool {
    return ack.op_cookie == zone.write_expected_cookie && ack.offset == zone.write_expected_offset && ack.length == zone.write_expected_len;
}

void reset_zone_metadata(WkiZone* zone) {
    zone->zone_id = 0;
    zone->peer_node_id = WKI_NODE_INVALID;
    zone->state = ZoneState::NONE;
    zone->size = 0;
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->access_policy = 0;
    zone->notify_mode = ZoneNotifyMode::NONE;
    zone->type_hint = ZoneTypeHint::BUFFER;
    zone->is_rdma = false;
    zone->is_roce = false;
    zone->remote_rkey = 0;
    zone->remote_phys_addr = 0;
    zone->rdma_transport = nullptr;
    zone->is_initiator = false;
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->next_op_cookie = 1;
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_result_len = 0;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->read_status = 0;
    zone->read_wait_entry = nullptr;
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->write_status = 0;
    zone->write_wait_entry = nullptr;
}

void finalize_retired_zone_if_idle(WkiZone* zone) {
    if (zone == nullptr || !zone->retiring.load(std::memory_order_acquire) || zone->refs.load(std::memory_order_acquire) != 0) {
        return;
    }

    s_zone_table_lock.lock();
    if (zone->retiring.load(std::memory_order_acquire) && zone->refs.load(std::memory_order_acquire) == 0) {
        free_zone_backing(zone);
        reset_zone_metadata(zone);
        zone->retiring.store(false, std::memory_order_release);
    }
    s_zone_table_lock.unlock();
}

void retain_zone(WkiZone* zone) {
    if (zone != nullptr) {
        zone->refs.fetch_add(1, std::memory_order_acq_rel);
    }
}

void release_zone(WkiZone* zone) {
    if (zone == nullptr) {
        return;
    }
    uint32_t const PREV = zone->refs.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 1) {
        finalize_retired_zone_if_idle(zone);
    }
}

auto acquire_zone_slot(uint32_t zone_id) -> WkiZone* {
    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(zone_id);
    if (zone != nullptr && !zone->retiring.load(std::memory_order_acquire)) {
        retain_zone(zone);
    } else {
        zone = nullptr;
    }
    s_zone_table_lock.unlock();
    return zone;
}

void mark_zone_retiring_locked(WkiZone* zone) {
    if (zone == nullptr || zone->retiring.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    zone->state = ZoneState::NONE;
}

void retire_zone_after_timeout(WkiZone* zone) {
    s_zone_table_lock.lock();
    mark_zone_retiring_locked(zone);
    s_zone_table_lock.unlock();
}

// Caller must hold WkiZone::lock.
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

void cancel_read_waiter(WkiZone* zone, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    zone->lock.lock();
    if (zone->read_wait_entry == &wait) {
        zone->read_wait_entry = nullptr;
        zone->read_status = result;
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
    }
    claimed = wki_claim_op(&wait);
    zone->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void cancel_write_waiter(WkiZone* zone, WkiWaitEntry& wait, int result) {
    bool claimed = false;
    zone->lock.lock();
    if (zone->write_wait_entry == &wait) {
        zone->write_wait_entry = nullptr;
        zone->write_status = result;
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
    }
    claimed = wki_claim_op(&wait);
    zone->lock.unlock();
    finish_or_wait_for_cancelled_waiter(wait, claimed, result);
}

void clear_read_waiter_after_wait(WkiZone* zone, WkiWaitEntry& wait, int wait_result) {
    zone->lock.lock();
    if (zone->read_wait_entry == &wait) {
        zone->read_wait_entry = nullptr;
    }
    if (wait_result == WKI_ERR_TIMEOUT) {
        zone->read_status = WKI_ERR_ZONE_TIMEOUT;
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
    }
    zone->lock.unlock();
}

void clear_write_waiter_after_wait(WkiZone* zone, WkiWaitEntry& wait, int wait_result) {
    zone->lock.lock();
    if (zone->write_wait_entry == &wait) {
        zone->write_wait_entry = nullptr;
    }
    if (wait_result == WKI_ERR_TIMEOUT) {
        zone->write_status = WKI_ERR_ZONE_TIMEOUT;
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
    }
    zone->lock.unlock();
}

// Check if a peer has any RDMA-capable transport (ivshmem or RoCE)
auto peer_has_rdma(uint16_t node_id) -> bool {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return false;
    }
    // Check dedicated RDMA transport (RoCE or ivshmem set during HELLO)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        return true;
    }
    // Fallback: check primary message transport (ivshmem has rdma_capable=true)
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        return true;
    }
    return false;
}

// Get the RDMA transport for a peer (prefers rdma_transport, falls back to transport)
auto peer_rdma_transport(uint16_t node_id) -> WkiTransport* {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return nullptr;
    }
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        return peer->rdma_transport;
    }
    if (peer->transport != nullptr && peer->transport->rdma_capable) {
        return peer->transport;
    }
    return nullptr;
}

// Check if the peer's RDMA transport is RoCE (not ivshmem shared memory)
[[maybe_unused]]
auto peer_rdma_is_roce(uint16_t node_id) -> bool {
    WkiPeer const* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return false;
    }
    // If rdma_transport is set and it's NOT the same as the primary transport,
    // it's the RoCE overlay. Also check if the primary transport is not rdma_capable.
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        // If the primary transport is also rdma_capable, it's ivshmem (preferred)
        return peer->transport == nullptr || !peer->transport->rdma_capable;
    }
    return false;
}

// Allocate RDMA-backed zone memory from ivshmem shared region.
// Returns the local virtual pointer and sets out_offset to the RDMA offset (used as rkey).
// Returns nullptr if RDMA allocation fails.
auto allocate_rdma_zone_backing(uint32_t size, int64_t& out_offset) -> void* {
    int64_t const OFFSET = wki_ivshmem_rdma_alloc(size);
    if (OFFSET < 0) {
        return nullptr;
    }
    out_offset = OFFSET;
    return wki_ivshmem_rdma_ptr(OFFSET);
}

// Allocate RoCE-backed zone memory: kernel pages registered with the RoCE transport.
// Each side has separate local memory; RDMA write/read must be used to synchronize.
// Returns the local virtual pointer and sets out_rkey to the registered RDMA key.
auto allocate_roce_zone_backing(WkiTransport* transport, uint32_t size, uint32_t& out_rkey) -> void* {
    if (transport == nullptr || transport->rdma_register_region == nullptr) {
        return nullptr;
    }
    void* backing = ker::mod::mm::phys::page_alloc(size);
    if (backing == nullptr) {
        return nullptr;
    }
    uint32_t rkey = 0;
    int const RET = transport->rdma_register_region(transport, reinterpret_cast<uint64_t>(backing), size, &rkey);
    if (RET != 0) {
        ker::mod::mm::phys::page_free(backing);
        return nullptr;
    }
    out_rkey = rkey;
    return backing;
}

}  // namespace

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_zone_init() {
    if (s_zone_initialized) {
        return;
    }

    for (auto& zone : s_zone_table) {
        zone.state = ZoneState::NONE;
        zone.zone_id = 0;
        zone.refs.store(0, std::memory_order_release);
        zone.retiring.store(false, std::memory_order_release);
    }

    s_zone_initialized = true;
    log::info("Zone subsystem initialized");
}

// -----------------------------------------------------------------------------
// Public API - Zone creation
// -----------------------------------------------------------------------------

auto wki_zone_create(uint16_t peer, uint32_t zone_id, uint32_t size, uint8_t access_policy, ZoneNotifyMode notify, ZoneTypeHint hint)
    -> int {
    if (!s_zone_initialized) {
        return WKI_ERR_INVALID;
    }

    // Size must be page-aligned
    if (size == 0 || (size & 0xFFF) != 0) {
        return WKI_ERR_INVALID;
    }

    // Check peer is connected
    WkiPeer const* p = wki_peer_find(peer);
    if (p == nullptr || p->state != PeerState::CONNECTED) {
        return WKI_ERR_PEER_FENCED;
    }

    s_zone_table_lock.lock();

    // Check zone_id doesn't already exist
    if (find_zone_slot(zone_id) != nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_EXISTS;
    }

    // Allocate a slot
    WkiZone* zone = alloc_zone_slot();
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_NO_MEM;
    }

    // Set up zone in NEGOTIATING state
    zone->zone_id = zone_id;
    zone->peer_node_id = peer;
    zone->state = ZoneState::NEGOTIATING;
    zone->size = size;
    zone->access_policy = access_policy;
    zone->notify_mode = notify;
    zone->type_hint = hint;
    zone->is_initiator = true;
    zone->is_rdma = false;
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->remote_rkey = 0;
    zone->remote_phys_addr = 0;
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->next_op_cookie = 1;
    zone->read_pending = false;
    zone->read_dest_buf = nullptr;
    zone->read_result_len = 0;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->read_wait_entry = nullptr;
    zone->read_status = 0;
    zone->write_pending = false;
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->write_wait_entry = nullptr;
    zone->write_status = 0;
    zone->retiring.store(false, std::memory_order_release);
    retain_zone(zone);

    s_zone_table_lock.unlock();

    // Send ZONE_CREATE_REQ
    ZoneCreateReqPayload req = {};
    req.zone_id = zone_id;
    req.size = size;
    req.access_policy = access_policy;
    req.notify_mode = static_cast<uint8_t>(notify);
    req.zone_type_hint = static_cast<uint8_t>(hint);

    // Set up wait entry before send (temporarily use read_wait_entry;
    // zone is NEGOTIATING so no reads are possible)
    WkiWaitEntry create_wait = {};
    zone->lock.lock();
    zone->read_wait_entry = &create_wait;
    zone->read_status = 0;
    zone->lock.unlock();

    int const RET = wki_send(peer, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_REQ, &req, sizeof(req));
    if (RET != WKI_OK) {
        cancel_read_waiter(zone, create_wait, RET);
        s_zone_table_lock.lock();
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();
        release_zone(zone);
        return RET;
    }

    // Wait for ACK via async wait queue
    int const WAIT_RC = wki_wait_for_op(&create_wait, WKI_OP_TIMEOUT_US);
    clear_read_waiter_after_wait(zone, create_wait, WAIT_RC);
    if (WAIT_RC == WKI_ERR_TIMEOUT) {
        // Timeout - clean up
        s_zone_table_lock.lock();
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();
        release_zone(zone);
        return WKI_ERR_ZONE_TIMEOUT;
    }

    if (zone->state == ZoneState::ACTIVE) {
        release_zone(zone);
        return WKI_OK;
    }

    // Zone was rejected - slot already cleaned up by ACK handler
    release_zone(zone);
    return WKI_ERR_ZONE_REJECTED;
}

// -----------------------------------------------------------------------------
// Public API - Zone destruction
// -----------------------------------------------------------------------------

auto wki_zone_destroy(uint32_t zone_id) -> int {
    WkiWaitEntry* read_waiter = nullptr;
    WkiWaitEntry* write_waiter = nullptr;
    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(zone_id);
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_NOT_FOUND;
    }

    uint16_t const PEER = zone->peer_node_id;

    mark_zone_retiring_locked(zone);
    zone->lock.lock();
    zone->read_status = -1;
    read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->write_status = -1;
    write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();

    s_zone_table_lock.unlock();

    finish_claimed_waiter(read_waiter, -1);
    finish_claimed_waiter(write_waiter, -1);
    finalize_retired_zone_if_idle(zone);

    // Notify peer
    ZoneDestroyPayload destroy = {};
    destroy.zone_id = zone_id;
    wki_send(PEER, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));

    log::info("Zone 0x%08x destroyed", zone_id);
    wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_DESTROYED, &zone_id, sizeof(zone_id));
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API - Zone lookup
// -----------------------------------------------------------------------------

auto wki_zone_find(uint32_t zone_id) -> WkiZone* {
    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(zone_id);
    s_zone_table_lock.unlock();
    return zone;
}

// -----------------------------------------------------------------------------
// Public API - Zone read (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_read(uint32_t zone_id, uint32_t offset, void* buf, uint32_t len) -> int {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return WKI_ERR_ZONE_INACTIVE;
    }

    // Check access policy - we need REMOTE_READ on the peer's zone
    // (The remote side validates this too, but checking early avoids network round-trip)
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        release_zone(zone);
        return WKI_ERR_ZONE_ACCESS;
    }

    if (!zone_range_valid(offset, len, zone->size)) {
        release_zone(zone);
        return WKI_ERR_INVALID;
    }

    // For RDMA zones, the caller should use wki_zone_get_ptr() directly
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        memcpy(buf, static_cast<uint8_t*>(zone->local_vaddr) + offset, len);
        release_zone(zone);
        return WKI_OK;
    }

    // Message-based: chunk if necessary
    auto* dest = static_cast<uint8_t*>(buf);
    uint32_t remaining = len;
    uint32_t cur_offset = offset;

    while (remaining > 0) {
        uint32_t chunk = remaining;
        chunk = std::min<size_t>(chunk, WKI_ZONE_MAX_MSG_DATA);

        // Set up pending read state
        WkiWaitEntry read_wait = {};
        zone->lock.lock();
        if (zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE) {
            zone->lock.unlock();
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (zone->read_pending.load(std::memory_order_acquire)) {
            zone->lock.unlock();
            release_zone(zone);
            return WKI_ERR_BUSY;
        }
        uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone);
        zone->read_wait_entry = &read_wait;
        zone->read_dest_buf = dest;
        zone->read_result_len = 0;
        zone->read_expected_offset = cur_offset;
        zone->read_expected_len = chunk;
        zone->read_expected_cookie = OP_COOKIE;
        zone->read_status = 0;
        zone->read_pending.store(true, std::memory_order_release);
        zone->lock.unlock();

        // Send ZONE_READ_REQ
        ZoneReadReqPayload req = {};
        req.zone_id = zone_id;
        req.offset = cur_offset;
        req.length = chunk;
        req.op_cookie = OP_COOKIE;

        int const RET = wki_send(zone->peer_node_id, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_READ_REQ, &req, sizeof(req));
        if (RET != WKI_OK) {
            cancel_read_waiter(zone, read_wait, RET);
            release_zone(zone);
            return RET;
        }

        // Wait for response via async wait queue
        int const WAIT_RC = wki_wait_for_op(&read_wait, WKI_OP_TIMEOUT_US);
        clear_read_waiter_after_wait(zone, read_wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            retire_zone_after_timeout(zone);
            release_zone(zone);
            return WKI_ERR_ZONE_TIMEOUT;
        }

        if (zone->read_status != 0) {
            int const STATUS = zone->read_status;
            release_zone(zone);
            return STATUS;
        }

        dest += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    release_zone(zone);
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API - Zone write (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_write(uint32_t zone_id, uint32_t offset, const void* buf, uint32_t len) -> int {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return WKI_ERR_ZONE_INACTIVE;
    }

    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        release_zone(zone);
        return WKI_ERR_ZONE_ACCESS;
    }

    if (!zone_range_valid(offset, len, zone->size)) {
        release_zone(zone);
        return WKI_ERR_INVALID;
    }

    // For RDMA zones, the caller should use wki_zone_get_ptr() directly
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        memcpy(static_cast<uint8_t*>(zone->local_vaddr) + offset, buf, len);
        release_zone(zone);
        return WKI_OK;
    }

    // Message-based: chunk if necessary
    const auto* src = static_cast<const uint8_t*>(buf);
    uint32_t remaining = len;
    uint32_t cur_offset = offset;

    while (remaining > 0) {
        uint32_t chunk = remaining;
        chunk = std::min<size_t>(chunk, WKI_ZONE_MAX_MSG_DATA);

        // Build variable-length ZONE_WRITE_REQ: header + data
        auto msg_len = static_cast<uint16_t>(sizeof(ZoneWriteReqPayload) + chunk);
        auto* msg_buf = new (std::nothrow) uint8_t[msg_len];
        if (msg_buf == nullptr) {
            release_zone(zone);
            return WKI_ERR_ZONE_NO_MEM;
        }

        auto* req = reinterpret_cast<ZoneWriteReqPayload*>(msg_buf);
        req->zone_id = zone_id;
        req->offset = cur_offset;
        req->length = chunk;
        memcpy(msg_buf + sizeof(ZoneWriteReqPayload), src, chunk);

        // Set up pending write state
        WkiWaitEntry write_wait = {};
        zone->lock.lock();
        if (zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE) {
            zone->lock.unlock();
            delete[] msg_buf;
            release_zone(zone);
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (zone->write_pending.load(std::memory_order_acquire)) {
            zone->lock.unlock();
            delete[] msg_buf;
            release_zone(zone);
            return WKI_ERR_BUSY;
        }
        uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone);
        zone->write_wait_entry = &write_wait;
        zone->write_expected_offset = cur_offset;
        zone->write_expected_len = chunk;
        zone->write_expected_cookie = OP_COOKIE;
        zone->write_status = 0;
        zone->write_pending.store(true, std::memory_order_release);
        zone->lock.unlock();
        req->op_cookie = OP_COOKIE;

        int const RET = wki_send(zone->peer_node_id, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_REQ, msg_buf, msg_len);
        delete[] msg_buf;

        if (RET != WKI_OK) {
            cancel_write_waiter(zone, write_wait, RET);
            release_zone(zone);
            return RET;
        }

        // Wait for ACK via async wait queue
        int const WAIT_RC = wki_wait_for_op(&write_wait, WKI_OP_TIMEOUT_US);
        clear_write_waiter_after_wait(zone, write_wait, WAIT_RC);
        if (WAIT_RC == WKI_ERR_TIMEOUT) {
            retire_zone_after_timeout(zone);
            release_zone(zone);
            return WKI_ERR_ZONE_TIMEOUT;
        }

        if (zone->write_status != 0) {
            int const STATUS = zone->write_status;
            release_zone(zone);
            return STATUS;
        }

        src += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    release_zone(zone);
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API - RDMA direct access
// -----------------------------------------------------------------------------

// Returns a raw pointer to the zone's local backing memory for RDMA direct access.
// The caller is responsible for respecting the zone's access_policy bits
// (ZONE_ACCESS_LOCAL_READ/WRITE, ZONE_ACCESS_REMOTE_READ/WRITE from wire.hpp).
// No enforcement is done here - policy checks are performed at the message-based
// read/write paths (wki_zone_read / wki_zone_write).
auto wki_zone_get_ptr(uint32_t zone_id) -> void* {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return nullptr;
    }
    void* ptr = zone->local_vaddr;
    release_zone(zone);
    return ptr;
}

// -----------------------------------------------------------------------------
// Public API - Notification handlers
// -----------------------------------------------------------------------------

void wki_zone_set_handlers(uint32_t zone_id, ZoneNotifyHandler pre, ZoneNotifyHandler post) {
    WkiZone* zone = acquire_zone_slot(zone_id);
    if (zone == nullptr) {
        return;
    }
    zone->lock.lock();
    if (zone->state == ZoneState::ACTIVE && !zone->retiring.load(std::memory_order_acquire)) {
        zone->pre_handler = pre;
        zone->post_handler = post;
    }
    zone->lock.unlock();
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// Public API - Fencing cleanup
// -----------------------------------------------------------------------------

void wki_zones_destroy_for_peer(uint16_t node_id) {
    while (true) {
        WkiZone* zone = nullptr;
        uint32_t zone_id = 0;
        WkiWaitEntry* read_waiter = nullptr;
        WkiWaitEntry* write_waiter = nullptr;

        s_zone_table_lock.lock();
        for (auto& candidate : s_zone_table) {
            if (candidate.state == ZoneState::NONE || candidate.peer_node_id != node_id) {
                continue;
            }

            zone = &candidate;
            zone_id = candidate.zone_id;
            mark_zone_retiring_locked(zone);

            zone->lock.lock();
            zone->read_status = -1;
            read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
            zone->read_pending.store(false, std::memory_order_release);
            zone->read_dest_buf = nullptr;
            zone->read_expected_offset = 0;
            zone->read_expected_len = 0;
            zone->read_expected_cookie = 0;
            zone->write_status = -1;
            write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
            zone->write_pending.store(false, std::memory_order_release);
            zone->write_expected_offset = 0;
            zone->write_expected_len = 0;
            zone->write_expected_cookie = 0;
            zone->lock.unlock();
            break;
        }

        s_zone_table_lock.unlock();

        if (zone == nullptr) {
            break;
        }

        log::info("Destroying zone 0x%08x during peer cleanup (peer 0x%04x)", zone_id, node_id);
        finish_claimed_waiter(read_waiter, -1);
        finish_claimed_waiter(write_waiter, -1);
        finalize_retired_zone_if_idle(zone);
    }
}

// -----------------------------------------------------------------------------
// Public API - Zone listing
// -----------------------------------------------------------------------------

auto wki_zones_list() -> auto {
    s_zone_table_lock.lock();
    auto zones = std::vector<WkiZone*>{};
    for (auto& zone : s_zone_table) {
        if (zone.state != ZoneState::NONE) {
            zones.push_back(&zone);
        }
    }
    s_zone_table_lock.unlock();
    return zones;
}

#ifdef WOS_SELFTEST
auto wki_zone_selftest_timeout_retirement_fences_stale_completion() -> bool {
    WkiZone read_zone = {};
    WkiWaitEntry read_wait = {};

    read_zone.zone_id = 0xABCD;
    read_zone.peer_node_id = 0x1234;
    read_zone.state = ZoneState::ACTIVE;
    read_zone.read_pending.store(true, std::memory_order_release);
    read_zone.read_wait_entry = &read_wait;

    clear_read_waiter_after_wait(&read_zone, read_wait, WKI_ERR_TIMEOUT);
    retire_zone_after_timeout(&read_zone);

    bool const READ_FENCED = read_zone.retiring.load(std::memory_order_acquire) && read_zone.state == ZoneState::NONE &&
                             !read_zone.read_pending.load(std::memory_order_acquire) && read_zone.read_wait_entry == nullptr &&
                             read_zone.read_status == WKI_ERR_ZONE_TIMEOUT;

    WkiZone write_zone = {};
    WkiWaitEntry write_wait = {};

    write_zone.zone_id = 0xBCDE;
    write_zone.peer_node_id = 0x1234;
    write_zone.state = ZoneState::ACTIVE;
    write_zone.write_pending.store(true, std::memory_order_release);
    write_zone.write_wait_entry = &write_wait;

    clear_write_waiter_after_wait(&write_zone, write_wait, WKI_ERR_TIMEOUT);
    retire_zone_after_timeout(&write_zone);

    bool const WRITE_FENCED = write_zone.retiring.load(std::memory_order_acquire) && write_zone.state == ZoneState::NONE &&
                              !write_zone.write_pending.load(std::memory_order_acquire) && write_zone.write_wait_entry == nullptr &&
                              write_zone.write_status == WKI_ERR_ZONE_TIMEOUT;

    return READ_FENCED && WRITE_FENCED;
}

auto wki_zone_selftest_range_and_read_response_validation() -> bool {
    if (!zone_range_valid(0, 0, 4096) || !zone_range_valid(4096, 0, 4096) || !zone_range_valid(1024, 2048, 4096)) {
        return false;
    }
    if (zone_range_valid(4097, 0, 4096) || zone_range_valid(4000, 128, 4096) || zone_range_valid(0xFFFFF000U, 0x2000U, 0xFFFFFFFFU)) {
        return false;
    }

    WkiZone zone = {};
    zone.read_expected_offset = 128;
    zone.read_expected_len = 64;
    zone.read_expected_cookie = 77;

    ZoneReadRespPayload resp = {};
    resp.offset = 128;
    resp.length = 64;
    resp.op_cookie = 77;
    if (!zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.op_cookie = 78;
    if (zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.op_cookie = 77;
    resp.length = 65;
    if (zone_read_response_matches_pending(zone, resp)) {
        return false;
    }

    resp.length = 64;
    resp.offset = 129;
    return !zone_read_response_matches_pending(zone, resp);
}

auto wki_zone_selftest_waiter_slots_and_cookies() -> bool {
    WkiZone cookie_zone = {};
    cookie_zone.next_op_cookie = 0;
    if (allocate_zone_op_cookie_locked(&cookie_zone) != 1 || cookie_zone.next_op_cookie != 2) {
        return false;
    }

    cookie_zone.next_op_cookie = 0xFFFFFFFFU;
    if (allocate_zone_op_cookie_locked(&cookie_zone) != 0xFFFFFFFFU || cookie_zone.next_op_cookie != 1) {
        return false;
    }

    WkiZone read_zone = {};
    read_zone.read_expected_offset = 32;
    read_zone.read_expected_len = 16;
    read_zone.read_expected_cookie = 1234;

    ZoneReadRespPayload read_resp = {};
    read_resp.offset = 32;
    read_resp.length = 16;
    read_resp.op_cookie = 1235;
    if (zone_read_response_matches_pending(read_zone, read_resp)) {
        return false;
    }

    read_resp.op_cookie = 1234;
    if (!zone_read_response_matches_pending(read_zone, read_resp)) {
        return false;
    }

    WkiZone write_zone = {};
    write_zone.write_expected_offset = 64;
    write_zone.write_expected_len = 24;
    write_zone.write_expected_cookie = 4321;

    ZoneWriteAckPayload write_ack = {};
    write_ack.offset = 64;
    write_ack.length = 24;
    write_ack.op_cookie = 4322;
    if (zone_write_ack_matches_pending(write_zone, write_ack)) {
        return false;
    }

    write_ack.op_cookie = 4321;
    write_ack.offset = 65;
    if (zone_write_ack_matches_pending(write_zone, write_ack)) {
        return false;
    }

    write_ack.offset = 64;
    return zone_write_ack_matches_pending(write_zone, write_ack);
}
#endif

// -----------------------------------------------------------------------------
// RX Handlers - Zone negotiation
// -----------------------------------------------------------------------------

namespace detail {

void handle_zone_create_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneCreateReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneCreateReqPayload*>(payload);
    uint16_t const SRC_NODE = hdr->src_node;

    // Validate size is page-aligned and non-zero
    if (req->size == 0 || (req->size & 0xFFF) != 0) {
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        wki_send(SRC_NODE, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
        return;
    }

    s_zone_table_lock.lock();

    // Check for zone_id collision
    if (find_zone_slot(req->zone_id) != nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        wki_send(SRC_NODE, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
        return;
    }

    // Allocate a slot
    WkiZone* zone = alloc_zone_slot();
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_NO_MEM);
        wki_send(SRC_NODE, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
        return;
    }

    // Try RDMA allocation: ivshmem first, then RoCE, then message-based fallback
    bool use_rdma = false;
    bool use_roce = false;
    void* backing = nullptr;
    uint64_t phys_addr = 0;
    int64_t rdma_offset = -1;
    uint32_t rkey = 0;
    WkiTransport* zone_rdma_transport = nullptr;

    if (peer_has_rdma(SRC_NODE)) {
        // Try ivshmem shared memory first (zero-copy, lowest latency)
        backing = allocate_rdma_zone_backing(req->size, rdma_offset);
        if (backing != nullptr) {
            use_rdma = true;
            phys_addr = static_cast<uint64_t>(rdma_offset);
            rkey = static_cast<uint32_t>(rdma_offset);
            memset(backing, 0, req->size);
        }

        // Try RoCE RDMA if ivshmem unavailable
        if (backing == nullptr) {
            WkiTransport* roce = peer_rdma_transport(SRC_NODE);
            if (roce != nullptr) {
                uint32_t roce_rkey = 0;
                backing = allocate_roce_zone_backing(roce, req->size, roce_rkey);
                if (backing != nullptr) {
                    use_rdma = true;
                    use_roce = true;
                    rkey = roce_rkey;
                    phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(backing)));
                    zone_rdma_transport = roce;
                    memset(backing, 0, req->size);
                }
            }
        }
    }

    // Fall back to message-based allocation
    if (backing == nullptr) {
        backing = allocate_zone_backing(req->size);
        if (backing == nullptr) {
            s_zone_table_lock.unlock();
            ZoneCreateAckPayload ack = {};
            ack.zone_id = req->zone_id;
            ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_NO_MEM);
            wki_send(SRC_NODE, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
            return;
        }
        memset(backing, 0, req->size);
        phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(backing)));
    }

    // Populate zone entry
    zone->zone_id = req->zone_id;
    zone->peer_node_id = SRC_NODE;
    zone->state = ZoneState::ACTIVE;
    zone->local_vaddr = backing;
    zone->local_phys_addr = phys_addr;
    zone->size = req->size;
    zone->access_policy = req->access_policy;
    zone->notify_mode = static_cast<ZoneNotifyMode>(req->notify_mode);
    zone->type_hint = static_cast<ZoneTypeHint>(req->zone_type_hint);
    zone->is_rdma = use_rdma;
    zone->is_roce = use_roce;
    zone->is_initiator = false;
    zone->local_rkey = rkey;
    zone->remote_rkey = 0;
    zone->remote_phys_addr = 0;
    zone->rdma_transport = zone_rdma_transport;
    zone->next_op_cookie = 1;
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->read_wait_entry = nullptr;
    zone->read_status = 0;
    zone->read_result_len = 0;
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->read_pending = false;
    zone->write_wait_entry = nullptr;
    zone->write_status = 0;
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->write_pending = false;
    zone->retiring.store(false, std::memory_order_release);

    s_zone_table_lock.unlock();

    // Send accept ACK
    ZoneCreateAckPayload ack = {};
    ack.zone_id = req->zone_id;
    ack.status = static_cast<uint8_t>(ZoneCreateStatus::ACCEPTED);
    ack.phys_addr = phys_addr;
    ack.rkey = rkey;

    wki_send(SRC_NODE, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));

    log::info("Zone 0x%08x created (responder, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", req->zone_id, SRC_NODE, req->size,
              use_rdma ? 1 : 0, use_roce ? 1 : 0);

    wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &req->zone_id, sizeof(req->zone_id));
}

void handle_zone_create_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneCreateAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const ZoneCreateAckPayload*>(payload);
    auto const STATUS = static_cast<ZoneCreateStatus>(ack->status);
    WkiWaitEntry* create_waiter = nullptr;

    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(ack->zone_id);
    if (zone == nullptr || zone->state != ZoneState::NEGOTIATING) {
        s_zone_table_lock.unlock();
        return;
    }

    // Verify the ACK came from the expected peer
    if (zone->peer_node_id != hdr->src_node) {
        s_zone_table_lock.unlock();
        return;
    }

    uint32_t const ZONE_SIZE = zone->size;

    zone->lock.lock();
    zone->read_status = 0;
    create_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->lock.unlock();

    if (create_waiter == nullptr) {
        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();

        if (STATUS == ZoneCreateStatus::ACCEPTED) {
            ZoneDestroyPayload destroy = {};
            destroy.zone_id = ack->zone_id;
            wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));
        }
        finalize_retired_zone_if_idle(zone);
        return;
    }

    if (STATUS == ZoneCreateStatus::ACCEPTED) {
        bool use_rdma = false;
        bool use_roce = false;
        void* backing = nullptr;
        int64_t rdma_offset = -1;
        uint32_t local_rkey = 0;
        WkiTransport* zone_rdma_transport = nullptr;

        // If responder provided an rkey, try RDMA allocation on our side too
        if (ack->rkey != 0 && peer_has_rdma(hdr->src_node)) {
            // Try ivshmem first
            backing = allocate_rdma_zone_backing(ZONE_SIZE, rdma_offset);
            if (backing != nullptr) {
                use_rdma = true;
                local_rkey = static_cast<uint32_t>(rdma_offset);
            }

            // Try RoCE if ivshmem unavailable
            if (backing == nullptr) {
                WkiTransport* roce = peer_rdma_transport(hdr->src_node);
                if (roce != nullptr) {
                    uint32_t roce_rkey = 0;
                    backing = allocate_roce_zone_backing(roce, ZONE_SIZE, roce_rkey);
                    if (backing != nullptr) {
                        use_rdma = true;
                        use_roce = true;
                        local_rkey = roce_rkey;
                        zone_rdma_transport = roce;
                    }
                }
            }
        }

        // Fall back to message-based allocation
        if (backing == nullptr) {
            backing = allocate_zone_backing(ZONE_SIZE);
            if (backing == nullptr) {
                mark_zone_retiring_locked(zone);
                s_zone_table_lock.unlock();

                finish_claimed_waiter(create_waiter, 0);
                ZoneDestroyPayload destroy = {};
                destroy.zone_id = ack->zone_id;
                wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));
                finalize_retired_zone_if_idle(zone);
                return;
            }
        }

        memset(backing, 0, ZONE_SIZE);

        zone->local_vaddr = backing;
        zone->is_rdma = use_rdma;
        zone->is_roce = use_roce;
        zone->local_rkey = local_rkey;
        zone->rdma_transport = zone_rdma_transport;
        if (use_rdma && !use_roce) {
            zone->local_phys_addr = static_cast<uint64_t>(rdma_offset);
        } else {
            zone->local_phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(backing)));
        }
        zone->remote_phys_addr = ack->phys_addr;
        zone->remote_rkey = ack->rkey;
        zone->state = ZoneState::ACTIVE;

        s_zone_table_lock.unlock();

        // For RoCE zones: tell the responder our local_rkey so it can RDMA
        // write/read our zone memory.  The ACK only carries the responder's
        // rkey (responder -> initiator); we send ours back via a ZONE_NOTIFY_POST
        // with op_type=0xFE (rkey-exchange).  The rkey is encoded in the offset field.
        if (use_roce && local_rkey != 0) {
            ZoneNotifyPayload rkey_notify = {};
            rkey_notify.zone_id = ack->zone_id;
            rkey_notify.offset = local_rkey;  // encode our rkey
            rkey_notify.length = 0;
            rkey_notify.op_type = 0xFE;  // rkey-exchange sentinel
            wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &rkey_notify, sizeof(rkey_notify));
        }

        log::info("Zone 0x%08x active (initiator, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", ack->zone_id, hdr->src_node, ZONE_SIZE,
                  use_rdma ? 1 : 0, use_roce ? 1 : 0);

        wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &ack->zone_id, sizeof(ack->zone_id));
        finish_claimed_waiter(create_waiter, 0);
    } else {
        // Rejected
        log::warn("Zone 0x%08x rejected by peer 0x%04x (status=%u)", ack->zone_id, hdr->src_node, ack->status);

        mark_zone_retiring_locked(zone);
        s_zone_table_lock.unlock();

        finish_claimed_waiter(create_waiter, 0);
        finalize_retired_zone_if_idle(zone);
    }
}

void handle_zone_destroy(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneDestroyPayload)) {
        return;
    }

    const auto* destroy = reinterpret_cast<const ZoneDestroyPayload*>(payload);
    WkiWaitEntry* read_waiter = nullptr;
    WkiWaitEntry* write_waiter = nullptr;

    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(destroy->zone_id);
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return;
    }

    // Only the peer that shares this zone can destroy it
    if (zone->peer_node_id != hdr->src_node) {
        s_zone_table_lock.unlock();
        return;
    }

    log::info("Zone 0x%08x destroyed by peer 0x%04x", destroy->zone_id, hdr->src_node);

    mark_zone_retiring_locked(zone);
    zone->lock.lock();
    zone->read_status = -1;
    read_waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->write_status = -1;
    write_waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();

    s_zone_table_lock.unlock();

    finish_claimed_waiter(read_waiter, -1);
    finish_claimed_waiter(write_waiter, -1);
    finalize_retired_zone_if_idle(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone notifications
// -----------------------------------------------------------------------------

void handle_zone_notify_pre(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        release_zone(zone);
        return;
    }

    ZoneNotifyHandler handler = nullptr;
    zone->lock.lock();
    if (!zone->retiring.load(std::memory_order_acquire) && zone->state == ZoneState::ACTIVE) {
        handler = zone->pre_handler;
    }
    zone->lock.unlock();

    // Invoke pre-notification handler if registered
    if (handler != nullptr) {
        handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_PRE_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

void handle_zone_notify_post(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        release_zone(zone);
        return;
    }

    // op_type=0xFE: rkey-exchange - the initiator is telling us its RDMA rkey
    // so we can write/read its zone memory.  Store it in remote_rkey.
    if (notify->op_type == 0xFE) {
        zone->lock.lock();
        zone->remote_rkey = notify->offset;  // rkey encoded in offset field
        zone->lock.unlock();
        // No ACK needed for rkey-exchange - the initiator doesn't wait.
        release_zone(zone);
        return;
    }

    ZoneNotifyHandler handler = nullptr;
    zone->lock.lock();
    if (!zone->retiring.load(std::memory_order_acquire) && zone->state == ZoneState::ACTIVE) {
        handler = zone->post_handler;
    }
    zone->lock.unlock();

    // Invoke post-notification handler if registered
    if (handler != nullptr) {
        handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone read (message-based)
// -----------------------------------------------------------------------------

void handle_zone_read_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneReadReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneReadReqPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(req->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        release_zone(zone);
        return;
    }

    // Check access policy - peer wants to read our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        release_zone(zone);
        return;
    }

    // Bounds check
    if (req->length > WKI_ZONE_MAX_MSG_DATA || !zone_range_valid(req->offset, req->length, zone->size)) {
        release_zone(zone);
        return;
    }

    // Build response: ZoneReadRespPayload + data
    uint32_t const DATA_LEN = req->length;
    auto resp_len = static_cast<uint16_t>(sizeof(ZoneReadRespPayload) + DATA_LEN);
    auto* resp_buf = new (std::nothrow) uint8_t[resp_len];
    if (resp_buf == nullptr) {
        release_zone(zone);
        return;
    }

    auto* resp = reinterpret_cast<ZoneReadRespPayload*>(resp_buf);
    resp->zone_id = req->zone_id;
    resp->offset = req->offset;
    resp->length = DATA_LEN;
    resp->op_cookie = req->op_cookie;

    // Copy data from local zone backing
    memcpy(resp_buf + sizeof(ZoneReadRespPayload), static_cast<uint8_t*>(zone->local_vaddr) + req->offset, DATA_LEN);

    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_READ_RESP, resp_buf, resp_len);
    delete[] resp_buf;
    release_zone(zone);
}

void handle_zone_read_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneReadRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const ZoneReadRespPayload*>(payload);
    WkiWaitEntry* waiter = nullptr;

    WkiZone* zone = acquire_zone_slot(resp->zone_id);
    if (zone == nullptr) {
        return;
    }

    zone->lock.lock();
    if (zone->peer_node_id != hdr->src_node || zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE ||
        !zone->read_pending.load(std::memory_order_acquire)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    if (resp->length > payload_len - sizeof(ZoneReadRespPayload) || !zone_read_response_matches_pending(*zone, *resp)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    waiter = claim_and_clear_waiter_locked(zone->read_wait_entry);
    if (waiter == nullptr) {
        zone->read_pending.store(false, std::memory_order_release);
        zone->read_dest_buf = nullptr;
        zone->read_expected_offset = 0;
        zone->read_expected_len = 0;
        zone->read_expected_cookie = 0;
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    // Copy data to the waiting buffer
    if (zone->read_dest_buf != nullptr && resp->length > 0) {
        memcpy(zone->read_dest_buf, payload + sizeof(ZoneReadRespPayload), resp->length);
    }

    zone->read_result_len = resp->length;
    zone->read_status = 0;
    zone->read_pending.store(false, std::memory_order_release);
    zone->read_dest_buf = nullptr;
    zone->read_expected_offset = 0;
    zone->read_expected_len = 0;
    zone->read_expected_cookie = 0;
    zone->lock.unlock();
    finish_claimed_waiter(waiter, 0);
    release_zone(zone);
}

// -----------------------------------------------------------------------------
// RX Handlers - Zone write (message-based)
// -----------------------------------------------------------------------------

void handle_zone_write_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneWriteReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneWriteReqPayload*>(payload);

    WkiZone* zone = acquire_zone_slot(req->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        release_zone(zone);
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        release_zone(zone);
        return;
    }

    // Check access policy - peer wants to write to our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        ZoneWriteAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.offset = req->offset;
        ack.length = req->length;
        ack.status = WKI_ERR_ZONE_ACCESS;
        ack.op_cookie = req->op_cookie;
        wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
        release_zone(zone);
        return;
    }

    // Validate data
    if (req->length > payload_len - sizeof(ZoneWriteReqPayload)) {
        release_zone(zone);
        return;
    }
    if (req->length > WKI_ZONE_MAX_MSG_DATA || !zone_range_valid(req->offset, req->length, zone->size)) {
        release_zone(zone);
        return;
    }

    // Copy data into local zone backing
    memcpy(static_cast<uint8_t*>(zone->local_vaddr) + req->offset, payload + sizeof(ZoneWriteReqPayload), req->length);

    // Send ACK
    ZoneWriteAckPayload ack = {};
    ack.zone_id = req->zone_id;
    ack.offset = req->offset;
    ack.length = req->length;
    ack.status = 0;  // success
    ack.op_cookie = req->op_cookie;
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
    release_zone(zone);
}

void handle_zone_write_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneWriteAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const ZoneWriteAckPayload*>(payload);
    WkiWaitEntry* waiter = nullptr;

    WkiZone* zone = acquire_zone_slot(ack->zone_id);
    if (zone == nullptr) {
        return;
    }

    zone->lock.lock();
    if (zone->peer_node_id != hdr->src_node || zone->retiring.load(std::memory_order_acquire) || zone->state != ZoneState::ACTIVE ||
        !zone->write_pending.load(std::memory_order_acquire)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    if (!zone_write_ack_matches_pending(*zone, *ack)) {
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    waiter = claim_and_clear_waiter_locked(zone->write_wait_entry);
    if (waiter == nullptr) {
        zone->write_pending.store(false, std::memory_order_release);
        zone->write_expected_offset = 0;
        zone->write_expected_len = 0;
        zone->write_expected_cookie = 0;
        zone->lock.unlock();
        release_zone(zone);
        return;
    }

    zone->write_status = ack->status;
    zone->write_pending.store(false, std::memory_order_release);
    zone->write_expected_offset = 0;
    zone->write_expected_len = 0;
    zone->write_expected_cookie = 0;
    zone->lock.unlock();
    finish_claimed_waiter(waiter, 0);
    release_zone(zone);
}

}  // namespace detail

}  // namespace ker::net::wki
