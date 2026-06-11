#include "transport_roce.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

namespace {

// -----------------------------------------------------------------------------
// RoCE wire format - raw L2 Ethernet (EtherType 0x88B8), no IP/UDP
// -----------------------------------------------------------------------------

enum class RoceOpcode : uint8_t {
    RDMA_WRITE = 0x01,
    RDMA_READ_REQ = 0x02,
    RDMA_READ_RESP = 0x03,
    DOORBELL = 0x04,
};

struct RoceHeader {
    uint8_t opcode;
    uint8_t version;
    uint16_t src_node;
    uint32_t rkey;
    uint64_t offset;
    uint32_t length;
    uint32_t doorbell_val;
} __attribute__((packed));

static_assert(sizeof(RoceHeader) == 24, "RoceHeader must be 24 bytes");

constexpr uint8_t ROCE_VERSION = 1;
constexpr uint32_t ROCE_MAX_PAYLOAD = 9000 - proto::ETH_HLEN - sizeof(RoceHeader);  // ~8962 bytes
constexpr uint32_t ROCE_WRITE_TAG_VALID = 0x80000000U;
constexpr uint32_t ROCE_WRITE_TAG_COOKIE_MASK = 0x0000FFFFU;
constexpr uint64_t ROCE_RDMA_READ_TIMEOUT_US = 100'000;

auto roce_write_tag(uint16_t cookie) -> uint32_t { return ROCE_WRITE_TAG_VALID | cookie; }

auto roce_write_tag_valid(uint32_t tag) -> bool { return (tag & ROCE_WRITE_TAG_VALID) != 0; }

auto roce_write_tag_cookie(uint32_t tag) -> uint16_t { return static_cast<uint16_t>(tag & ROCE_WRITE_TAG_COOKIE_MASK); }

// -----------------------------------------------------------------------------
// Memory region registry - maps rkey -> (vaddr, size)
// -----------------------------------------------------------------------------

constexpr size_t ROCE_REGION_BUCKETS = 256;
static_assert((ROCE_REGION_BUCKETS & (ROCE_REGION_BUCKETS - 1)) == 0, "RoCE region bucket count must be a power of two");

struct RoceRegion {
    uint32_t rkey = 0;
    void* vaddr = nullptr;
    uint32_t size = 0;
    bool temporary = false;
    uint32_t received_bytes = 0;
    bool receive_gap = false;
    bool write_complete = false;
    bool tagged_receive = false;
    uint16_t received_cookie = 0;
    RoceRegion* next = nullptr;
};

struct RoceRegionSnapshot {
    void* vaddr = nullptr;
    uint32_t size = 0;
};

std::array<RoceRegion*, ROCE_REGION_BUCKETS> s_region_buckets{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_region_lock;                            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t s_next_rkey = 1;                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// -----------------------------------------------------------------------------
// Transport state
// -----------------------------------------------------------------------------

WkiTransport s_roce_transport;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_roce_initialized = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// -----------------------------------------------------------------------------
// Region management
// -----------------------------------------------------------------------------

auto region_bucket_index(uint32_t rkey) -> size_t {
    constexpr uint64_t HASH_MULT = 0x9E3779B97F4A7C15ULL;
    return (static_cast<uint64_t>(rkey) * HASH_MULT) & (ROCE_REGION_BUCKETS - 1);
}

auto region_bucket(uint32_t rkey) -> RoceRegion*& { return s_region_buckets.at(region_bucket_index(rkey)); }

auto region_find_locked(uint32_t rkey) -> RoceRegion* {
    RoceRegion* cur = region_bucket(rkey);
    while (cur != nullptr) {
        if (cur->rkey == rkey) {
            return cur;
        }
        cur = cur->next;
    }
    return nullptr;
}

auto region_unlink_locked(uint32_t rkey) -> RoceRegion* {
    RoceRegion** link = &region_bucket(rkey);
    while (*link != nullptr) {
        if ((*link)->rkey == rkey) {
            RoceRegion* removed = *link;
            *link = removed->next;
            removed->next = nullptr;
            return removed;
        }
        link = &((*link)->next);
    }
    return nullptr;
}

auto region_temporary_complete_locked(const RoceRegion* region) -> bool {
    return region != nullptr && region->temporary && region->write_complete && !region->receive_gap &&
           region->received_bytes >= region->size;
}

auto next_region_rkey_locked() -> uint32_t {
    // Keep RoCE region rkeys below 0x00010000 so they stay disjoint from
    // zone doorbell values (node_id<<16 | counter, minimum 0x00010001).
    // Always allocate a fresh key when recycling a slot so late RDMA_WRITE /
    // DOORBELL packets from an older read cannot target a newly registered
    // response buffer.
    for (size_t attempts = 0; attempts < 0xFFFF; ++attempts) {
        uint32_t candidate = s_next_rkey;
        s_next_rkey++;
        if (s_next_rkey == 0 || s_next_rkey >= 0x00010000U) {
            s_next_rkey = 1;
        }

        if (region_find_locked(candidate) == nullptr) {
            return candidate;
        }
    }
    return 0;
}

auto region_register(uint64_t vaddr, uint32_t size, bool temporary, uint32_t* rkey) -> int {
    if (rkey == nullptr) {
        return WKI_ERR_INVALID;
    }

    auto* region = new (std::nothrow) RoceRegion{};
    if (region == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    uint32_t const NEW_RKEY = next_region_rkey_locked();
    if (NEW_RKEY == 0) {
        s_region_lock.unlock_irqrestore(FLAGS);
        delete region;
        return WKI_ERR_NO_MEM;
    }

    region->rkey = NEW_RKEY;
    // In wOS callers pass a kernel virtual address here for non-ivshmem RoCE.
    region->vaddr = reinterpret_cast<void*>(vaddr);
    region->size = size;
    region->temporary = temporary;
    region->received_bytes = 0;
    region->receive_gap = false;
    region->write_complete = false;
    region->tagged_receive = false;
    region->received_cookie = 0;

    RoceRegion*& bucket = region_bucket(NEW_RKEY);
    region->next = bucket;
    bucket = region;
    *rkey = NEW_RKEY;
    s_region_lock.unlock_irqrestore(FLAGS);
    return 0;
}

auto region_unregister(uint32_t rkey) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* removed = region_unlink_locked(rkey);
    s_region_lock.unlock_irqrestore(FLAGS);

    delete removed;
    return removed != nullptr;
}

auto region_note_temporary_doorbell(uint32_t rkey) -> bool {
    RoceRegion* removed = nullptr;
    bool handled = false;
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region != nullptr && region->temporary) {
        handled = true;
        region->write_complete = true;
        if (region_temporary_complete_locked(region)) {
            removed = region_unlink_locked(rkey);
        }
    }
    s_region_lock.unlock_irqrestore(FLAGS);

    delete removed;
    return handled;
}

auto region_is_registered(uint32_t rkey) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    bool const FOUND = region_find_locked(rkey) != nullptr;
    s_region_lock.unlock_irqrestore(FLAGS);
    return FOUND;
}

auto region_reset_received(uint32_t rkey) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region == nullptr) {
        s_region_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    region->received_bytes = 0;
    region->receive_gap = false;
    region->write_complete = false;
    region->tagged_receive = false;
    region->received_cookie = 0;
    s_region_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto region_prepare_tagged_write(uint32_t rkey, uint16_t cookie) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region == nullptr) {
        s_region_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    region->received_bytes = 0;
    region->receive_gap = false;
    region->tagged_receive = true;
    region->received_cookie = cookie;
    region->write_complete = false;
    s_region_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto region_received_at_least(uint32_t rkey, uint32_t len) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    bool const RECEIVED = region != nullptr && len <= region->size && !region->receive_gap && region->received_bytes >= len;
    s_region_lock.unlock_irqrestore(FLAGS);
    return RECEIVED;
}

auto region_tagged_write_received_at_least(uint32_t rkey, uint16_t cookie, uint32_t len) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    bool const RECEIVED = region != nullptr && len <= region->size && region->tagged_receive && region->received_cookie == cookie &&
                          !region->receive_gap && region->received_bytes >= len;
    s_region_lock.unlock_irqrestore(FLAGS);
    return RECEIVED;
}

auto region_mark_write_complete(uint32_t rkey) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region == nullptr || region->temporary) {
        s_region_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    region->write_complete = true;
    s_region_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto region_snapshot(uint32_t rkey, RoceRegionSnapshot& out) -> bool {
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region == nullptr) {
        s_region_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    out.vaddr = region->vaddr;
    out.size = region->size;
    s_region_lock.unlock_irqrestore(FLAGS);
    return true;
}

auto wait_for_temporary_region_completion(uint32_t rkey) -> int {
    uint64_t const DEADLINE = wki_now_us() + ROCE_RDMA_READ_TIMEOUT_US;
    uint32_t spins = 0;

    while (region_is_registered(rkey)) {
        if (wki_now_us() >= DEADLINE) {
            return region_unregister(rkey) ? WKI_ERR_TIMEOUT : WKI_OK;
        }

        // RDMA read completion is itself delivered by RX packets. Keep the
        // network queues moving even when the caller is a WKI daemon.
        wki_spin_yield();

        if ((++spins & 0x3FU) == 0) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }

    return WKI_OK;
}

auto region_write(uint32_t rkey, uint64_t offset, const uint8_t* payload, uint32_t length, uint32_t payload_len, uint32_t write_tag)
    -> bool {
    if (payload == nullptr || length > payload_len) {
        return false;
    }

    RoceRegion* completed = nullptr;
    uint64_t const FLAGS = s_region_lock.lock_irqsave();
    RoceRegion* region = region_find_locked(rkey);
    if (region == nullptr || offset > region->size || length > (static_cast<uint64_t>(region->size) - offset)) {
        s_region_lock.unlock_irqrestore(FLAGS);
        return false;
    }

    if (roce_write_tag_valid(write_tag)) {
        uint16_t const COOKIE = roce_write_tag_cookie(write_tag);
        if (!region->tagged_receive || region->received_cookie != COOKIE) {
            s_region_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }

    memcpy(static_cast<uint8_t*>(region->vaddr) + offset, payload, length);
    region->received_bytes = std::min(region->size, region->received_bytes + length);
    if (region_temporary_complete_locked(region)) {
        completed = region_unlink_locked(rkey);
    }
    s_region_lock.unlock_irqrestore(FLAGS);

    delete completed;
    return true;
}

// -----------------------------------------------------------------------------
// Raw Ethernet TX helper - sends a RoCE frame (EtherType 0x88B8)
// -----------------------------------------------------------------------------

auto roce_eth_tx_resolved(NetDevice* netdev, const proto::MacAddress& dst_mac, const RoceHeader& hdr, const void* payload,
                          uint32_t payload_len) -> int {
    PacketBuffer* pkt = pkt_alloc_tx();
    if (pkt == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    uint32_t const TOTAL = sizeof(RoceHeader) + payload_len;
    if (TOTAL > PKT_BUF_SIZE - PKT_HEADROOM - proto::ETH_HLEN) {
        pkt_free(pkt);
        return WKI_ERR_INVALID;
    }

    memcpy(pkt->data, &hdr, sizeof(RoceHeader));
    if (payload_len > 0 && payload != nullptr) {
        memcpy(pkt->data + sizeof(RoceHeader), payload, payload_len);
    }
    pkt->len = static_cast<uint16_t>(TOTAL);
    pkt->dev = netdev;

    int const RET = proto::eth_tx(netdev, pkt, dst_mac, WKI_ETHERTYPE_ROCE);
    return (RET == 0) ? WKI_OK : WKI_ERR_TX_FAILED;
}

auto roce_eth_tx(uint16_t neighbor_id, const RoceHeader& hdr, const void* payload, uint32_t payload_len) -> int {
    auto* netdev = wki_eth_get_netdev();
    if (netdev == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    WkiPeer const* peer = wki_peer_find(neighbor_id);
    if (peer == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    return roce_eth_tx_resolved(netdev, peer->mac, hdr, payload, payload_len);
}

// -----------------------------------------------------------------------------
// RDMA operations - WkiTransport function pointers
// -----------------------------------------------------------------------------

int roce_rdma_register_region(WkiTransport* /*self*/, uint64_t phys_addr, uint32_t size, uint32_t* rkey) {
    return region_register(phys_addr, size, false, rkey);
}

auto roce_rdma_write_impl(uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len,
                          uint32_t write_tag) -> int {
    uint64_t const START_US = wki_now_us();
    const auto* src = static_cast<const uint8_t*>(local_buf);
    uint64_t offset = remote_offset;
    uint32_t remaining = len;
    int status = WKI_OK;
    auto* netdev = wki_eth_get_netdev();
    WkiPeer const* peer = wki_peer_find(neighbor_id);
    if (netdev == nullptr || peer == nullptr) {
        status = WKI_ERR_NO_ROUTE;
        ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::TRANSPORT,
                                           static_cast<uint8_t>(ker::mod::perf::WkiPerfTransportOp::RDMA_WRITE), neighbor_id, 0, status,
                                           static_cast<uint32_t>(wki_now_us() - START_US), true, 0, 0);
        return status;
    }

    proto::MacAddress const DST_MAC = peer->mac;

    // Fragment into multiple frames if needed
    while (remaining > 0) {
        uint32_t const CHUNK = (remaining > ROCE_MAX_PAYLOAD) ? ROCE_MAX_PAYLOAD : remaining;

        RoceHeader hdr = {};
        hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_WRITE);
        hdr.version = ROCE_VERSION;
        hdr.src_node = g_wki.my_node_id;
        hdr.rkey = rkey;
        hdr.offset = offset;
        hdr.length = CHUNK;
        hdr.doorbell_val = write_tag;

        int const RET = roce_eth_tx_resolved(netdev, DST_MAC, hdr, src, CHUNK);
        if (RET != 0) {
            status = RET;
            break;
        }

        src += CHUNK;
        offset += CHUNK;
        remaining -= CHUNK;
    }

    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::TRANSPORT,
                                       static_cast<uint8_t>(ker::mod::perf::WkiPerfTransportOp::RDMA_WRITE), neighbor_id, 0, status,
                                       static_cast<uint32_t>(wki_now_us() - START_US), true, 0, len - remaining);
    return status;
}

int roce_rdma_write(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf,
                    uint32_t len) {
    return roce_rdma_write_impl(neighbor_id, rkey, remote_offset, local_buf, len, 0);
}

int roce_rdma_read(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, void* local_buf, uint32_t len) {
    // Send RDMA_READ_REQ - responder sends back data as RDMA_WRITE frames
    // into a temporary registered region, then a DOORBELL to signal completion.
    // Use a synchronous inline poll so the caller keeps RX/backlog progress
    // moving while waiting for the doorbell.

    // Register our local buffer as a temporary region for the response
    uint32_t local_rkey = 0;
    int const REG_RET = region_register(reinterpret_cast<uint64_t>(local_buf), len, true, &local_rkey);
    if (REG_RET != 0) {
        return REG_RET;
    }

    RoceHeader hdr = {};
    hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_READ_REQ);
    hdr.version = ROCE_VERSION;
    hdr.src_node = g_wki.my_node_id;
    hdr.rkey = rkey;
    hdr.offset = remote_offset;
    hdr.length = len;
    hdr.doorbell_val = local_rkey;  // tell responder where to write the result

    int const RET = roce_eth_tx(neighbor_id, hdr, nullptr, 0);
    if (RET != 0) {
        region_unregister(local_rkey);
        return RET;
    }

    return wait_for_temporary_region_completion(local_rkey);
}

int roce_doorbell(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t value) {
    RoceHeader hdr = {};
    hdr.opcode = static_cast<uint8_t>(RoceOpcode::DOORBELL);
    hdr.version = ROCE_VERSION;
    hdr.src_node = g_wki.my_node_id;
    hdr.rkey = 0;
    hdr.offset = 0;
    hdr.length = 0;
    hdr.doorbell_val = value;

    return roce_eth_tx(neighbor_id, hdr, nullptr, 0);
}

}  // namespace

auto wki_roce_region_reset_received(uint32_t rkey) -> bool { return region_reset_received(rkey); }

auto wki_roce_region_prepare_tagged_write(uint32_t rkey, uint16_t cookie) -> bool { return region_prepare_tagged_write(rkey, cookie); }

auto wki_roce_region_wait_received(uint32_t rkey, uint32_t len, uint64_t timeout_us) -> bool {
    if (len == 0) {
        return region_is_registered(rkey);
    }

    uint64_t const DEADLINE = wki_now_us() + timeout_us;
    uint32_t spins = 0;
    while (wki_now_us() < DEADLINE) {
        if (region_received_at_least(rkey, len)) {
            return true;
        }

        // This wait can run on a VFS worker while the peer is streaming a large
        // RDMA_WRITE burst. Drive RX inline like roce_rdma_read() so queued
        // RoCE frames do not depend on unrelated scheduler progress.
        napi_poll_all_pending();
        backlog_drain_all_pending_inline();

        if ((++spins & 0x3FU) == 0) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }

    return region_received_at_least(rkey, len);
}

auto wki_roce_region_wait_tagged_write(uint32_t rkey, uint16_t cookie, uint32_t len, uint64_t timeout_us) -> bool {
    if (len == 0) {
        return region_is_registered(rkey);
    }

    uint64_t const DEADLINE = wki_now_us() + timeout_us;
    uint32_t spins = 0;
    while (wki_now_us() < DEADLINE) {
        if (region_tagged_write_received_at_least(rkey, cookie, len)) {
            return true;
        }

        napi_poll_all_pending();
        backlog_drain_all_pending_inline();

        if ((++spins & 0x3FU) == 0) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }

    return region_tagged_write_received_at_least(rkey, cookie, len);
}

auto wki_roce_rdma_write_tagged(uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len,
                                uint16_t cookie) -> int {
    return roce_rdma_write_impl(neighbor_id, rkey, remote_offset, local_buf, len, roce_write_tag(cookie));
}

// -----------------------------------------------------------------------------
// RX entry point - called from ethernet.cpp for EtherType 0x88B8
// -----------------------------------------------------------------------------

void roce_rx(ker::net::NetDevice* /*dev*/, ker::net::PacketBuffer* pkt) {
    if (pkt->len < sizeof(RoceHeader)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const RoceHeader*>(pkt->data);

    if (hdr->version != ROCE_VERSION) {
        pkt_free(pkt);
        return;
    }

    const uint8_t* payload = pkt->data + sizeof(RoceHeader);
    uint32_t const PAYLOAD_LEN = pkt->len - sizeof(RoceHeader);

    switch (static_cast<RoceOpcode>(hdr->opcode)) {
        case RoceOpcode::RDMA_WRITE: {
            region_write(hdr->rkey, hdr->offset, payload, hdr->length, PAYLOAD_LEN, hdr->doorbell_val);
            break;
        }

        case RoceOpcode::RDMA_READ_REQ: {
            // Peer wants to read from our registered region - send data back as RDMA_WRITE
            RoceRegionSnapshot region = {};
            if (!region_snapshot(hdr->rkey, region)) {
                break;
            }

            if (hdr->offset > region.size || hdr->length > (static_cast<uint64_t>(region.size) - hdr->offset)) {
                break;
            }

            // Send the data back to the requestor using RDMA_WRITE to their local_rkey
            uint32_t const RESP_RKEY = hdr->doorbell_val;  // requestor's local region key
            const auto* src_data = static_cast<const uint8_t*>(region.vaddr) + hdr->offset;
            uint32_t remaining = hdr->length;
            uint64_t write_offset = 0;
            bool send_ok = true;

            while (remaining > 0) {
                uint32_t const CHUNK = (remaining > ROCE_MAX_PAYLOAD) ? ROCE_MAX_PAYLOAD : remaining;

                RoceHeader resp_hdr = {};
                resp_hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_WRITE);
                resp_hdr.version = ROCE_VERSION;
                resp_hdr.src_node = g_wki.my_node_id;
                resp_hdr.rkey = RESP_RKEY;
                resp_hdr.offset = write_offset;
                resp_hdr.length = CHUNK;
                resp_hdr.doorbell_val = 0;

                if (roce_eth_tx(hdr->src_node, resp_hdr, src_data, CHUNK) != 0) {
                    send_ok = false;
                    break;
                }

                src_data += CHUNK;
                write_offset += CHUNK;
                remaining -= CHUNK;
            }

            if (!send_ok) {
                break;
            }

            // Send a DOORBELL to signal read completion - deregister the temp region
            RoceHeader db_hdr = {};
            db_hdr.opcode = static_cast<uint8_t>(RoceOpcode::DOORBELL);
            db_hdr.version = ROCE_VERSION;
            db_hdr.src_node = g_wki.my_node_id;
            db_hdr.rkey = 0;
            db_hdr.offset = 0;
            db_hdr.length = 0;
            db_hdr.doorbell_val = RESP_RKEY;

            roce_eth_tx(hdr->src_node, db_hdr, nullptr, 0);
            break;
        }

        case RoceOpcode::RDMA_READ_RESP:
            // Handled same as RDMA_WRITE - data arrives into our local region
            // (shouldn't normally be sent separately; requestor uses RDMA_WRITE)
            break;

        case RoceOpcode::DOORBELL: {
            // Doorbell received - determine if it's a read-completion (deregister
            // a temp region) or a zone notification (invoke post_handler).
            //
            // The namespaces are disjoint: RoCE region rkeys are small sequential
            // values below 0x00010000, while zone_ids are node_id<<16 | counter
            // (always >= 0x00010001).
            uint32_t const VAL = hdr->doorbell_val;

            if (region_note_temporary_doorbell(VAL)) {
                // Read-completion doorbell. If data fragments are still in
                // flight, the final RDMA_WRITE will deregister the region.
                break;
            }

            if (region_mark_write_complete(VAL)) {
                break;
            }

            if ((VAL & WKI_DOORBELL_IPC_MASK) == WKI_DOORBELL_IPC_BASE) {
                if (wki_ipc_doorbell_rx(hdr->src_node, VAL)) {
                    break;
                }
            }

            // Zone doorbell - dispatch to zone post handler
            WkiZone const* zone = wki_zone_find(VAL);
            if (zone != nullptr && zone->post_handler != nullptr) {
                zone->post_handler(VAL, 0, 0, 0);
            }
            break;
        }

        default:
            break;
    }

    pkt_free(pkt);
}

// -----------------------------------------------------------------------------
// Transport initialization
// -----------------------------------------------------------------------------

void wki_roce_transport_init() {
    if (s_roce_initialized) {
        return;
    }

    s_region_buckets.fill(nullptr);
    s_next_rkey = 1;

    // RoCE is an RDMA-only overlay transport - no WKI message TX (tx/tx_pkt are null)
    s_roce_transport.name = "wki-roce";
    s_roce_transport.mtu = static_cast<uint16_t>(ROCE_MAX_PAYLOAD);
    s_roce_transport.rdma_capable = true;
    s_roce_transport.private_data = nullptr;
    s_roce_transport.tx = nullptr;      // not a message transport
    s_roce_transport.tx_pkt = nullptr;  // not a message transport
    s_roce_transport.set_rx_handler = nullptr;
    s_roce_transport.rdma_register_region = roce_rdma_register_region;
    s_roce_transport.rdma_read = roce_rdma_read;
    s_roce_transport.rdma_write = roce_rdma_write;
    s_roce_transport.doorbell = roce_doorbell;
    s_roce_transport.next = nullptr;

    // Do NOT register with wki_transport_register - RoCE is not a message transport.
    // Peers discover it via wki_roce_transport_get() during HELLO completion.

    s_roce_initialized = true;
    ker::mod::dbg::log("[WKI] RoCE RDMA transport initialized (L2, EtherType 0x%04x)", WKI_ETHERTYPE_ROCE);
}

auto wki_roce_transport_get() -> WkiTransport* { return s_roce_initialized ? &s_roce_transport : nullptr; }

}  // namespace ker::net::wki
