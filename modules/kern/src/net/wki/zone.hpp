#pragma once

#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

constexpr size_t WKI_MAX_ZONES = 256;
constexpr size_t WKI_ZONE_MAX_MSG_DATA = 1024;   // max data bytes per zone read/write message
constexpr uint64_t WKI_ZONE_TIMEOUT_US = 50000;  // 50 ms timeout for blocking zone ops

// Zone error codes
constexpr int WKI_ERR_ZONE_NOT_FOUND = -20;
constexpr int WKI_ERR_ZONE_EXISTS = -21;
constexpr int WKI_ERR_ZONE_NO_MEM = -22;
constexpr int WKI_ERR_ZONE_REJECTED = -23;
constexpr int WKI_ERR_ZONE_ACCESS = -24;
constexpr int WKI_ERR_ZONE_TIMEOUT = -25;
constexpr int WKI_ERR_ZONE_INACTIVE = -26;

// -----------------------------------------------------------------------------
// Zone State
// -----------------------------------------------------------------------------

enum class ZoneState : uint8_t {
    NONE = 0,
    NEGOTIATING = 1,
    ACTIVE = 2,
};

// -----------------------------------------------------------------------------
// Zone Notify Handler — callback for PRE/POST notifications
// -----------------------------------------------------------------------------

using ZoneNotifyHandler = void (*)(uint32_t zone_id, uint32_t offset, uint32_t length, uint8_t op_type);

// -----------------------------------------------------------------------------
// WkiZone — per-zone state
// -----------------------------------------------------------------------------

struct WkiZone {
    uint32_t zone_id = 0;
    uint16_t peer_node_id = WKI_NODE_INVALID;
    ZoneState state = ZoneState::NONE;

    // Local memory backing
    void* local_vaddr = nullptr;   // HHDM ptr (pageAlloc) or BAR2 ptr (ivshmem)
    uint64_t local_phys_addr = 0;  // physical address of local allocation
    uint32_t size = 0;             // page-aligned zone size in bytes

    // Access policy and notifications
    uint8_t access_policy = 0;  // ZONE_ACCESS_* bits from wire.hpp
    ZoneNotifyMode notify_mode = ZoneNotifyMode::NONE;
    ZoneTypeHint type_hint = ZoneTypeHint::BUFFER;

    // RDMA state
    bool is_rdma = false;
    bool is_roce = false;           // true if RDMA via RoCE (needs explicit rdma_write/read sync)
    uint32_t local_rkey = 0;        // our RDMA key (if RDMA-backed)
    uint32_t remote_rkey = 0;       // peer's RDMA key (if RDMA-backed)
    uint64_t remote_phys_addr = 0;  // peer's physical address (from ACK)
    WkiTransport* rdma_transport = nullptr;  // transport for RDMA ops (RoCE; nullptr for ivshmem)

    // Ownership
    bool is_initiator = false;  // true if we sent ZONE_CREATE_REQ

    // Notification callbacks
    ZoneNotifyHandler pre_handler = nullptr;
    ZoneNotifyHandler post_handler = nullptr;

    // Synchronous read/write state (for message-based zones)
    volatile bool read_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    void* read_dest_buf = nullptr;
    uint32_t read_result_len = 0;
    int read_status = 0;

    volatile bool write_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    int write_status = 0;

    ker::mod::sys::Spinlock lock;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the zone subsystem. Called from wki_init().
void wki_zone_init();

// Create a shared memory zone with a peer.
// Sends ZONE_CREATE_REQ and blocks until ACK (or timeout).
// Returns WKI_OK on success, negative error code on failure.
auto wki_zone_create(uint16_t peer, uint32_t zone_id, uint32_t size, uint8_t access_policy, ZoneNotifyMode notify,
                     ZoneTypeHint hint = ZoneTypeHint::BUFFER) -> int;

// Destroy a shared memory zone. Sends ZONE_DESTROY to peer.
auto wki_zone_destroy(uint32_t zone_id) -> int;

// Find a zone by ID. Returns nullptr if not found.
auto wki_zone_find(uint32_t zone_id) -> WkiZone*;

// Read from a zone (message-based). Blocks until response.
// For RDMA zones, use wki_zone_get_ptr() and memcpy directly.
auto wki_zone_read(uint32_t zone_id, uint32_t offset, void* buf, uint32_t len) -> int;

// Write to a zone (message-based). Blocks until ACK.
// For RDMA zones, use wki_zone_get_ptr() and memcpy directly.
auto wki_zone_write(uint32_t zone_id, uint32_t offset, const void* buf, uint32_t len) -> int;

// Get the local virtual address pointer for a zone (for RDMA direct access).
// Returns nullptr if zone not found or not active.
auto wki_zone_get_ptr(uint32_t zone_id) -> void*;

// Set PRE/POST notification handlers on a zone.
void wki_zone_set_handlers(uint32_t zone_id, ZoneNotifyHandler pre, ZoneNotifyHandler post);

// Destroy all zones associated with a peer (called during fencing).
void wki_zones_destroy_for_peer(uint16_t node_id);

auto wki_zones_list() -> auto;

// -----------------------------------------------------------------------------
// Internal — RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_zone_create_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_create_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_destroy(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_notify_pre(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_notify_post(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_read_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_read_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_write_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_zone_write_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
