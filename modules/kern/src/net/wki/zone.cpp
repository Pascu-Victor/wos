#include "zone.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <net/netpoll.hpp>
#include <net/wki/event.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <vector>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Zone table — static storage for all zones on this node
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
        if (zone.state == ZoneState::NONE) {
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
            ker::mod::mm::phys::pageFree(zone->local_vaddr);
        }
    }
    zone->local_vaddr = nullptr;
    zone->local_phys_addr = 0;
    zone->local_rkey = 0;
    zone->rdma_transport = nullptr;
}

auto allocate_zone_backing(uint32_t size) -> void* {
    // Allocate physically contiguous pages from buddy allocator
    return ker::mod::mm::phys::pageAlloc(size);
}

// Check if a peer has any RDMA-capable transport (ivshmem or RoCE)
auto peer_has_rdma(uint16_t node_id) -> bool {
    WkiPeer* peer = wki_peer_find(node_id);
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
    WkiPeer* peer = wki_peer_find(node_id);
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
auto peer_rdma_is_roce(uint16_t node_id) -> bool {
    WkiPeer* peer = wki_peer_find(node_id);
    if (peer == nullptr) {
        return false;
    }
    // If rdma_transport is set and it's NOT the same as the primary transport,
    // it's the RoCE overlay. Also check if the primary transport is not rdma_capable.
    if (peer->rdma_transport != nullptr && peer->rdma_transport->rdma_capable) {
        // If the primary transport is also rdma_capable, it's ivshmem (preferred)
        if (peer->transport != nullptr && peer->transport->rdma_capable) {
            return false;  // ivshmem
        }
        return true;  // RoCE
    }
    return false;
}

// Allocate RDMA-backed zone memory from ivshmem shared region.
// Returns the local virtual pointer and sets out_offset to the RDMA offset (used as rkey).
// Returns nullptr if RDMA allocation fails.
auto allocate_rdma_zone_backing(uint32_t size, int64_t& out_offset) -> void* {
    int64_t offset = wki_ivshmem_rdma_alloc(size);
    if (offset < 0) {
        return nullptr;
    }
    out_offset = offset;
    return wki_ivshmem_rdma_ptr(offset);
}

// Allocate RoCE-backed zone memory: kernel pages registered with the RoCE transport.
// Each side has separate local memory; RDMA write/read must be used to synchronize.
// Returns the local virtual pointer and sets out_rkey to the registered RDMA key.
auto allocate_roce_zone_backing(WkiTransport* transport, uint32_t size, uint32_t& out_rkey) -> void* {
    if (transport == nullptr || transport->rdma_register_region == nullptr) {
        return nullptr;
    }
    void* backing = ker::mod::mm::phys::pageAlloc(size);
    if (backing == nullptr) {
        return nullptr;
    }
    uint32_t rkey = 0;
    int ret = transport->rdma_register_region(transport, reinterpret_cast<uint64_t>(backing), size, &rkey);
    if (ret != 0) {
        ker::mod::mm::phys::pageFree(backing);
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
    }

    s_zone_initialized = true;
    ker::mod::dbg::log("[WKI] Zone subsystem initialized");
}

// -----------------------------------------------------------------------------
// Public API — Zone creation
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
    WkiPeer* p = wki_peer_find(peer);
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
    zone->read_pending = false;
    zone->write_pending = false;

    s_zone_table_lock.unlock();

    // Send ZONE_CREATE_REQ
    ZoneCreateReqPayload req = {};
    req.zone_id = zone_id;
    req.size = size;
    req.access_policy = access_policy;
    req.notify_mode = static_cast<uint8_t>(notify);
    req.zone_type_hint = static_cast<uint8_t>(hint);

    int ret = wki_send(peer, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_REQ, &req, sizeof(req));
    if (ret != WKI_OK) {
        // Clean up on send failure
        s_zone_table_lock.lock();
        zone->state = ZoneState::NONE;
        zone->zone_id = 0;
        s_zone_table_lock.unlock();
        return ret;
    }

    // Spin-wait for ACK (zone transitions to ACTIVE or back to NONE)
    // Poll the NIC and yield during the spin-wait so incoming packets (including
    // the ZONE_CREATE_ACK) can be processed.  napi_poll_inline() will harmlessly
    // return 0 if we are already inside the NAPI poll handler (re-entrancy safe).
    uint64_t deadline = wki_now_us() + WKI_ZONE_TIMEOUT_US;
    while (zone->state == ZoneState::NEGOTIATING) {
        asm volatile("pause" ::: "memory");
        if (zone->state != ZoneState::NEGOTIATING) {
            break;
        }
        if (wki_now_us() >= deadline) {
            // Timeout — clean up
            s_zone_table_lock.lock();
            zone->state = ZoneState::NONE;
            zone->zone_id = 0;
            s_zone_table_lock.unlock();
            return WKI_ERR_ZONE_TIMEOUT;
        }
        // Drive NIC polling + yield so the zone create ACK can be received.
        // When called from the timer thread (the deferred zone creation path),
        // this allows the ACK to be processed.  When called from inside the
        // NAPI poll handler, napi_poll_inline returns 0 (harmless).
        net::NetDevice* net_dev = wki_eth_get_netdev();
        if (net_dev != nullptr) {
            net::napi_poll_inline(net_dev);
        }
        ker::mod::sched::kern_yield();
    }

    if (zone->state == ZoneState::ACTIVE) {
        return WKI_OK;
    }

    // Zone was rejected — slot already cleaned up by ACK handler
    return WKI_ERR_ZONE_REJECTED;
}

// -----------------------------------------------------------------------------
// Public API — Zone destruction
// -----------------------------------------------------------------------------

auto wki_zone_destroy(uint32_t zone_id) -> int {
    s_zone_table_lock.lock();

    WkiZone* zone = find_zone_slot(zone_id);
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        return WKI_ERR_ZONE_NOT_FOUND;
    }

    uint16_t peer = zone->peer_node_id;

    // Free backing memory
    free_zone_backing(zone);

    // Clear the slot
    zone->state = ZoneState::NONE;
    zone->zone_id = 0;

    s_zone_table_lock.unlock();

    // Notify peer
    ZoneDestroyPayload destroy = {};
    destroy.zone_id = zone_id;
    wki_send(peer, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));

    ker::mod::dbg::log("[WKI] Zone 0x%08x destroyed", zone_id);
    wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_DESTROYED, &zone_id, sizeof(zone_id));
    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API — Zone lookup
// -----------------------------------------------------------------------------

auto wki_zone_find(uint32_t zone_id) -> WkiZone* {
    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(zone_id);
    s_zone_table_lock.unlock();
    return zone;
}

// -----------------------------------------------------------------------------
// Public API — Zone read (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_read(uint32_t zone_id, uint32_t offset, void* buf, uint32_t len) -> int {
    WkiZone* zone = wki_zone_find(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        return WKI_ERR_ZONE_INACTIVE;
    }

    // Check access policy — we need REMOTE_READ on the peer's zone
    // (The remote side validates this too, but checking early avoids network round-trip)
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        return WKI_ERR_ZONE_ACCESS;
    }

    // For RDMA zones, the caller should use wki_zone_get_ptr() directly
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (offset + len > zone->size) {
            return WKI_ERR_INVALID;
        }
        memcpy(buf, static_cast<uint8_t*>(zone->local_vaddr) + offset, len);
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
        zone->lock.lock();
        zone->read_pending = true;
        zone->read_dest_buf = dest;
        zone->read_result_len = 0;
        zone->read_status = 0;
        zone->lock.unlock();

        // Send ZONE_READ_REQ
        ZoneReadReqPayload req = {};
        req.zone_id = zone_id;
        req.offset = cur_offset;
        req.length = chunk;

        int ret = wki_send(zone->peer_node_id, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_READ_REQ, &req, sizeof(req));
        if (ret != WKI_OK) {
            zone->read_pending = false;
            return ret;
        }

        // Spin-wait for response with memory fence
        uint64_t deadline = wki_now_us() + WKI_ZONE_TIMEOUT_US;
        while (zone->read_pending) {
            asm volatile("pause" ::: "memory");
            if (!zone->read_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                zone->read_pending = false;
                return WKI_ERR_ZONE_TIMEOUT;
            }
        }

        if (zone->read_status != 0) {
            return zone->read_status;
        }

        dest += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API — Zone write (message-based, blocking)
// -----------------------------------------------------------------------------

auto wki_zone_write(uint32_t zone_id, uint32_t offset, const void* buf, uint32_t len) -> int {
    WkiZone* zone = wki_zone_find(zone_id);
    if (zone == nullptr) {
        return WKI_ERR_ZONE_NOT_FOUND;
    }
    if (zone->state != ZoneState::ACTIVE) {
        return WKI_ERR_ZONE_INACTIVE;
    }

    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        return WKI_ERR_ZONE_ACCESS;
    }

    // For RDMA zones, the caller should use wki_zone_get_ptr() directly
    if (zone->is_rdma) {
        if (zone->local_vaddr == nullptr) {
            return WKI_ERR_ZONE_INACTIVE;
        }
        if (offset + len > zone->size) {
            return WKI_ERR_INVALID;
        }
        memcpy(static_cast<uint8_t*>(zone->local_vaddr) + offset, buf, len);
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
        auto* msg_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
        if (msg_buf == nullptr) {
            return WKI_ERR_ZONE_NO_MEM;
        }

        auto* req = reinterpret_cast<ZoneWriteReqPayload*>(msg_buf);
        req->zone_id = zone_id;
        req->offset = cur_offset;
        req->length = chunk;
        memcpy(msg_buf + sizeof(ZoneWriteReqPayload), src, chunk);

        // Set up pending write state
        zone->lock.lock();
        zone->write_pending = true;
        zone->write_status = 0;
        zone->lock.unlock();

        int ret = wki_send(zone->peer_node_id, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_REQ, msg_buf, msg_len);
        ker::mod::mm::dyn::kmalloc::free(msg_buf);

        if (ret != WKI_OK) {
            zone->write_pending = false;
            return ret;
        }

        // Spin-wait for ACK with memory fence
        uint64_t deadline = wki_now_us() + WKI_ZONE_TIMEOUT_US;
        while (zone->write_pending) {
            asm volatile("pause" ::: "memory");
            if (!zone->write_pending) {
                break;
            }
            if (wki_now_us() >= deadline) {
                zone->write_pending = false;
                return WKI_ERR_ZONE_TIMEOUT;
            }
        }

        if (zone->write_status != 0) {
            return zone->write_status;
        }

        src += chunk;
        cur_offset += chunk;
        remaining -= chunk;
    }

    return WKI_OK;
}

// -----------------------------------------------------------------------------
// Public API — RDMA direct access
// -----------------------------------------------------------------------------

// Returns a raw pointer to the zone's local backing memory for RDMA direct access.
// The caller is responsible for respecting the zone's access_policy bits
// (ZONE_ACCESS_LOCAL_READ/WRITE, ZONE_ACCESS_REMOTE_READ/WRITE from wire.hpp).
// No enforcement is done here — policy checks are performed at the message-based
// read/write paths (wki_zone_read / wki_zone_write).
auto wki_zone_get_ptr(uint32_t zone_id) -> void* {
    WkiZone* zone = wki_zone_find(zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        return nullptr;
    }
    return zone->local_vaddr;
}

// -----------------------------------------------------------------------------
// Public API — Notification handlers
// -----------------------------------------------------------------------------

void wki_zone_set_handlers(uint32_t zone_id, ZoneNotifyHandler pre, ZoneNotifyHandler post) {
    WkiZone* zone = wki_zone_find(zone_id);
    if (zone == nullptr) {
        return;
    }
    zone->lock.lock();
    zone->pre_handler = pre;
    zone->post_handler = post;
    zone->lock.unlock();
}

// -----------------------------------------------------------------------------
// Public API — Fencing cleanup
// -----------------------------------------------------------------------------

void wki_zones_destroy_for_peer(uint16_t node_id) {
    s_zone_table_lock.lock();

    for (auto& zone : s_zone_table) {
        if (zone.state == ZoneState::NONE) {
            continue;
        }
        if (zone.peer_node_id != node_id) {
            continue;
        }

        ker::mod::dbg::log("[WKI] Destroying zone 0x%08x (peer 0x%04x fenced)", zone.zone_id, node_id);

        free_zone_backing(&zone);
        zone.state = ZoneState::NONE;
        zone.zone_id = 0;
    }

    s_zone_table_lock.unlock();
}

// -----------------------------------------------------------------------------
// Public API — Zone listing
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

// -----------------------------------------------------------------------------
// RX Handlers — Zone negotiation
// -----------------------------------------------------------------------------

namespace detail {

void handle_zone_create_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneCreateReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneCreateReqPayload*>(payload);
    uint16_t src_node = hdr->src_node;

    // Validate size is page-aligned and non-zero
    if (req->size == 0 || (req->size & 0xFFF) != 0) {
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        wki_send(src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
        return;
    }

    s_zone_table_lock.lock();

    // Check for zone_id collision
    if (find_zone_slot(req->zone_id) != nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_POLICY);
        wki_send(src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
        return;
    }

    // Allocate a slot
    WkiZone* zone = alloc_zone_slot();
    if (zone == nullptr) {
        s_zone_table_lock.unlock();
        ZoneCreateAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = static_cast<uint8_t>(ZoneCreateStatus::REJECTED_NO_MEM);
        wki_send(src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
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

    if (peer_has_rdma(src_node)) {
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
            WkiTransport* roce = peer_rdma_transport(src_node);
            if (roce != nullptr) {
                uint32_t roce_rkey = 0;
                backing = allocate_roce_zone_backing(roce, req->size, roce_rkey);
                if (backing != nullptr) {
                    use_rdma = true;
                    use_roce = true;
                    rkey = roce_rkey;
                    phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(reinterpret_cast<uint64_t>(backing)));
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
            wki_send(src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));
            return;
        }
        memset(backing, 0, req->size);
        phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(reinterpret_cast<uint64_t>(backing)));
    }

    // Populate zone entry
    zone->zone_id = req->zone_id;
    zone->peer_node_id = src_node;
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
    zone->pre_handler = nullptr;
    zone->post_handler = nullptr;
    zone->read_pending = false;
    zone->write_pending = false;

    s_zone_table_lock.unlock();

    // Send accept ACK
    ZoneCreateAckPayload ack = {};
    ack.zone_id = req->zone_id;
    ack.status = static_cast<uint8_t>(ZoneCreateStatus::ACCEPTED);
    ack.phys_addr = phys_addr;
    ack.rkey = rkey;

    wki_send(src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_CREATE_ACK, &ack, sizeof(ack));

    ker::mod::dbg::log("[WKI] Zone 0x%08x created (responder, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", req->zone_id, src_node, req->size,
                       use_rdma ? 1 : 0, use_roce ? 1 : 0);

    wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &req->zone_id, sizeof(req->zone_id));
}

void handle_zone_create_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneCreateAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const ZoneCreateAckPayload*>(payload);

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

    auto status = static_cast<ZoneCreateStatus>(ack->status);

    if (status == ZoneCreateStatus::ACCEPTED) {
        bool use_rdma = false;
        bool use_roce = false;
        void* backing = nullptr;
        int64_t rdma_offset = -1;
        uint32_t local_rkey = 0;
        WkiTransport* zone_rdma_transport = nullptr;

        // If responder provided an rkey, try RDMA allocation on our side too
        if (ack->rkey != 0 && peer_has_rdma(hdr->src_node)) {
            // Try ivshmem first
            backing = allocate_rdma_zone_backing(zone->size, rdma_offset);
            if (backing != nullptr) {
                use_rdma = true;
                local_rkey = static_cast<uint32_t>(rdma_offset);
            }

            // Try RoCE if ivshmem unavailable
            if (backing == nullptr) {
                WkiTransport* roce = peer_rdma_transport(hdr->src_node);
                if (roce != nullptr) {
                    uint32_t roce_rkey = 0;
                    backing = allocate_roce_zone_backing(roce, zone->size, roce_rkey);
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
            backing = allocate_zone_backing(zone->size);
            if (backing == nullptr) {
                zone->state = ZoneState::NONE;
                zone->zone_id = 0;
                s_zone_table_lock.unlock();

                ZoneDestroyPayload destroy = {};
                destroy.zone_id = ack->zone_id;
                wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_DESTROY, &destroy, sizeof(destroy));
                return;
            }
        }

        memset(backing, 0, zone->size);

        zone->local_vaddr = backing;
        zone->is_rdma = use_rdma;
        zone->is_roce = use_roce;
        zone->local_rkey = local_rkey;
        zone->rdma_transport = zone_rdma_transport;
        if (use_rdma && !use_roce) {
            zone->local_phys_addr = static_cast<uint64_t>(rdma_offset);
        } else {
            zone->local_phys_addr = reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(reinterpret_cast<uint64_t>(backing)));
        }
        zone->remote_phys_addr = ack->phys_addr;
        zone->remote_rkey = ack->rkey;
        zone->state = ZoneState::ACTIVE;

        s_zone_table_lock.unlock();

        // For RoCE zones: tell the responder our local_rkey so it can RDMA
        // write/read our zone memory.  The ACK only carries the responder's
        // rkey (responder → initiator); we send ours back via a ZONE_NOTIFY_POST
        // with op_type=0xFE (rkey-exchange).  The rkey is encoded in the offset field.
        if (use_roce && local_rkey != 0) {
            ZoneNotifyPayload rkey_notify = {};
            rkey_notify.zone_id = ack->zone_id;
            rkey_notify.offset = local_rkey;  // encode our rkey
            rkey_notify.length = 0;
            rkey_notify.op_type = 0xFE;  // rkey-exchange sentinel
            wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &rkey_notify, sizeof(rkey_notify));
        }

        ker::mod::dbg::log("[WKI] Zone 0x%08x active (initiator, peer 0x%04x, %u bytes, rdma=%d, roce=%d)", ack->zone_id, hdr->src_node,
                           zone->size, use_rdma ? 1 : 0, use_roce ? 1 : 0);

        wki_event_publish(EVENT_CLASS_ZONE, EVENT_ZONE_CREATED, &ack->zone_id, sizeof(ack->zone_id));
    } else {
        // Rejected
        ker::mod::dbg::log("[WKI] Zone 0x%08x rejected by peer 0x%04x (status=%u)", ack->zone_id, hdr->src_node, ack->status);

        zone->state = ZoneState::NONE;
        zone->zone_id = 0;
        s_zone_table_lock.unlock();
    }
}

void handle_zone_destroy(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneDestroyPayload)) {
        return;
    }

    const auto* destroy = reinterpret_cast<const ZoneDestroyPayload*>(payload);

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

    ker::mod::dbg::log("[WKI] Zone 0x%08x destroyed by peer 0x%04x", destroy->zone_id, hdr->src_node);

    free_zone_backing(zone);
    zone->state = ZoneState::NONE;
    zone->zone_id = 0;

    s_zone_table_lock.unlock();
}

// -----------------------------------------------------------------------------
// RX Handlers — Zone notifications
// -----------------------------------------------------------------------------

void handle_zone_notify_pre(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = wki_zone_find(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        return;
    }

    // Invoke pre-notification handler if registered
    if (zone->pre_handler != nullptr) {
        zone->pre_handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_PRE_ACK, &ack, sizeof(ack));
}

void handle_zone_notify_post(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneNotifyPayload)) {
        return;
    }

    const auto* notify = reinterpret_cast<const ZoneNotifyPayload*>(payload);

    WkiZone* zone = wki_zone_find(notify->zone_id);
    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        return;
    }

    // op_type=0xFE: rkey-exchange — the initiator is telling us its RDMA rkey
    // so we can write/read its zone memory.  Store it in remote_rkey.
    if (notify->op_type == 0xFE) {
        zone->remote_rkey = notify->offset;  // rkey encoded in offset field
        // No ACK needed for rkey-exchange — the initiator doesn't wait.
        return;
    }

    // Invoke post-notification handler if registered
    if (zone->post_handler != nullptr) {
        zone->post_handler(notify->zone_id, notify->offset, notify->length, notify->op_type);
    }

    // Send ACK back to notifier
    ZoneNotifyAckPayload ack = {};
    ack.zone_id = notify->zone_id;
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST_ACK, &ack, sizeof(ack));
}

// -----------------------------------------------------------------------------
// RX Handlers — Zone read (message-based)
// -----------------------------------------------------------------------------

void handle_zone_read_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneReadReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneReadReqPayload*>(payload);

    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(req->zone_id);
    s_zone_table_lock.unlock();

    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        return;
    }

    // Check access policy — peer wants to read our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_READ) == 0) {
        return;
    }

    // Bounds check
    if (req->offset + req->length > zone->size) {
        return;
    }

    // Build response: ZoneReadRespPayload + data
    uint32_t data_len = req->length;
    auto resp_len = static_cast<uint16_t>(sizeof(ZoneReadRespPayload) + data_len);
    auto* resp_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(resp_len));
    if (resp_buf == nullptr) {
        return;
    }

    auto* resp = reinterpret_cast<ZoneReadRespPayload*>(resp_buf);
    resp->zone_id = req->zone_id;
    resp->offset = req->offset;
    resp->length = data_len;

    // Copy data from local zone backing
    memcpy(resp_buf + sizeof(ZoneReadRespPayload), static_cast<uint8_t*>(zone->local_vaddr) + req->offset, data_len);

    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_READ_RESP, resp_buf, resp_len);
    ker::mod::mm::dyn::kmalloc::free(resp_buf);
}

void handle_zone_read_resp(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneReadRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const ZoneReadRespPayload*>(payload);

    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(resp->zone_id);
    s_zone_table_lock.unlock();

    if (zone == nullptr || !zone->read_pending) {
        return;
    }

    // Validate data fits
    if (sizeof(ZoneReadRespPayload) + resp->length > payload_len) {
        zone->read_status = WKI_ERR_INVALID;
        zone->read_pending = false;
        return;
    }

    // Copy data to the waiting buffer
    if (zone->read_dest_buf != nullptr && resp->length > 0) {
        memcpy(zone->read_dest_buf, payload + sizeof(ZoneReadRespPayload), resp->length);
    }

    zone->read_result_len = resp->length;
    zone->read_status = 0;
    zone->read_pending = false;  // Unblock the waiting caller
}

// -----------------------------------------------------------------------------
// RX Handlers — Zone write (message-based)
// -----------------------------------------------------------------------------

void handle_zone_write_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneWriteReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const ZoneWriteReqPayload*>(payload);

    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(req->zone_id);
    s_zone_table_lock.unlock();

    if (zone == nullptr || zone->state != ZoneState::ACTIVE) {
        return;
    }
    if (zone->peer_node_id != hdr->src_node) {
        return;
    }

    // Check access policy — peer wants to write to our local data
    if ((zone->access_policy & ZONE_ACCESS_REMOTE_WRITE) == 0) {
        ZoneWriteAckPayload ack = {};
        ack.zone_id = req->zone_id;
        ack.status = WKI_ERR_ZONE_ACCESS;
        wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
        return;
    }

    // Validate data
    if (sizeof(ZoneWriteReqPayload) + req->length > payload_len) {
        return;
    }
    if (req->offset + req->length > zone->size) {
        return;
    }

    // Copy data into local zone backing
    memcpy(static_cast<uint8_t*>(zone->local_vaddr) + req->offset, payload + sizeof(ZoneWriteReqPayload), req->length);

    // Send ACK
    ZoneWriteAckPayload ack = {};
    ack.zone_id = req->zone_id;
    ack.status = 0;  // success
    wki_send(hdr->src_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_WRITE_ACK, &ack, sizeof(ack));
}

void handle_zone_write_ack(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ZoneWriteAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const ZoneWriteAckPayload*>(payload);

    s_zone_table_lock.lock();
    WkiZone* zone = find_zone_slot(ack->zone_id);
    s_zone_table_lock.unlock();

    if (zone == nullptr || !zone->write_pending) {
        return;
    }

    zone->write_status = ack->status;
    zone->write_pending = false;  // Unblock the waiting caller
}

}  // namespace detail

}  // namespace ker::net::wki
