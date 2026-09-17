#pragma once

#include <atomic>
#include <cstddef>
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
// Zone Notify Handler - callback for PRE/POST notifications
// -----------------------------------------------------------------------------

using ZoneNotifyHandler = void (*)(uint32_t zone_id, uint32_t offset, uint32_t length, uint8_t op_type);

// -----------------------------------------------------------------------------
// WkiZone - per-zone state
// -----------------------------------------------------------------------------

struct WkiZone {
    uint32_t zone_id = 0;
    uint16_t peer_node_id = WKI_NODE_INVALID;
    WkiChannelIdentity channel_identity{};  // Immutable until the retained zone is retired.
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
    bool is_roce = false;                    // true if RDMA via RoCE (needs explicit rdma_write/read sync)
    uint32_t local_rkey = 0;                 // our RDMA key (if RDMA-backed)
    uint32_t remote_rkey = 0;                // peer's RDMA key (if RDMA-backed)
    uint64_t remote_phys_addr = 0;           // peer's physical address (from ACK)
    WkiTransport* rdma_transport = nullptr;  // transport for RDMA ops (RoCE; nullptr for ivshmem)

    // Ownership
    bool is_initiator = false;  // true if we sent ZONE_CREATE_REQ

    // Notification callbacks
    ZoneNotifyHandler pre_handler = nullptr;
    ZoneNotifyHandler post_handler = nullptr;

    // Synchronous read/write state (for message-based zones)
    uint32_t next_op_cookie = 1;

    std::atomic<bool> read_pending{false};
    void* read_dest_buf = nullptr;
    uint32_t read_result_len = 0;
    uint32_t read_expected_offset = 0;
    uint32_t read_expected_len = 0;
    uint32_t read_expected_cookie = 0;
    int read_status = 0;
    WkiWaitEntry* read_wait_entry = nullptr;  // V2 I-4: async wait for ZONE_READ_RESP

    std::atomic<bool> write_pending{false};
    uint32_t write_expected_offset = 0;
    uint32_t write_expected_len = 0;
    uint32_t write_expected_cookie = 0;
    int write_status = 0;
    WkiWaitEntry* write_wait_entry = nullptr;  // V2 I-4: async wait for ZONE_WRITE_ACK

    std::atomic<uint32_t> refs{0};
    std::atomic<bool> retiring{false};
    mod::sys::Spinlock lock;
};

struct WkiZoneMemory {
    void* data = nullptr;
    uint32_t size = 0;
    bool is_roce = false;
    uint32_t remote_rkey = 0;
    WkiTransport* transport = nullptr;
};

// Owns exact local backing, including after retirement. Call live() before
// admitting new work; a pin alone does not extend the zone's authorization.
// The owner must drain users before reset/move/destruction. Memory and transport
// metadata are captured under the zone lock; this is not a mutable slot borrow.
class WkiZoneRef {
   public:
    WkiZoneRef() = default;
    WkiZoneRef(const WkiZoneRef&) = delete;
    auto operator=(const WkiZoneRef&) -> WkiZoneRef& = delete;
    WkiZoneRef(WkiZoneRef&& other) noexcept;
    auto operator=(WkiZoneRef&& other) noexcept -> WkiZoneRef&;
    ~WkiZoneRef();
    explicit operator bool() const { return zone_ != nullptr; }
    auto memory() const -> WkiZoneMemory { return memory_; }
    auto live() const -> bool;
    // Refresh late transport metadata from this pinned slot; preserve output on refusal.
    auto snapshot_memory(WkiZoneMemory& output) const -> bool;
    void reset();
    auto destroy() const -> int;
    // Zero-range READ(0)/WRITE(1) wake on this zone's original management channel.
    auto notify_post(uint8_t op_type) const -> int;
    auto set_handlers(ZoneNotifyHandler pre, ZoneNotifyHandler post) const -> bool;
    static auto acquire(uint32_t zone_id, uint16_t peer, uint8_t local_access) -> WkiZoneRef;

   private:
    friend auto wki_zone_create(uint16_t, uint32_t, uint32_t, uint8_t, ZoneNotifyMode, ZoneTypeHint, WkiZoneRef*) -> int;
    static auto retain(WkiZone* zone, uint16_t peer, uint8_t local_access) -> WkiZoneRef;
    WkiZone* zone_ = nullptr;
    WkiZoneMemory memory_{};
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the zone subsystem. Called from wki_init().
void wki_zone_init();

// Start the task-context worker that performs allocation-bearing zone-create
// request/ACK completion after bounded RX admission.
void wki_zone_worker_start();

// Create a shared memory zone with a peer.
// Sends ZONE_CREATE_REQ and blocks until ACK (or timeout).
// Returns WKI_OK on success, negative error code on failure.
auto wki_zone_create(uint16_t peer, uint32_t zone_id, uint32_t size, uint8_t access_policy, ZoneNotifyMode notify,
                     ZoneTypeHint hint = ZoneTypeHint::BUFFER, WkiZoneRef* local_ref = nullptr) -> int;

// Destroy a shared memory zone. Sends ZONE_DESTROY to peer.
auto wki_zone_destroy(uint32_t zone_id) -> int;

// Find a zone by ID. Returns nullptr if not found.
auto wki_zone_find(uint32_t zone_id) -> WkiZone*;

// Read from a zone (message-based). Blocks until response.
// Direct local users must retain a WkiZoneRef with the required local access.
auto wki_zone_read(uint32_t zone_id, uint32_t offset, void* buf, uint32_t len) -> int;

// Write to a zone (message-based). Blocks until ACK.
// Direct local users must retain a WkiZoneRef with the required local access.
auto wki_zone_write(uint32_t zone_id, uint32_t offset, const void* buf, uint32_t len) -> int;

// Set PRE/POST notification handlers on a zone.
void wki_zone_set_handlers(uint32_t zone_id, ZoneNotifyHandler pre, ZoneNotifyHandler post);

// Destroy all zones associated with a peer (called during fencing).
void wki_zones_destroy_for_peer(uint16_t node_id);

auto wki_zones_list() -> auto;

#ifdef WOS_SELFTEST
auto wki_zone_selftest_post_notification_authority() -> bool;
auto wki_zone_selftest_with_local_backing(uint32_t size, bool (*test)(WkiZoneRef&)) -> bool;
auto wki_zone_selftest_local_backing_retention() -> bool;
auto wki_zone_selftest_deferred_channel_retirement() -> bool;
auto wki_zone_selftest_timeout_retirement_fences_stale_completion() -> bool;
auto wki_zone_selftest_range_and_read_response_validation() -> bool;
auto wki_zone_selftest_waiter_slots_and_cookies() -> bool;
#endif

// -----------------------------------------------------------------------------
// Internal - RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_zone_create_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_create_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_destroy(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_notify_pre(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_notify_post(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_read_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_read_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_write_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);
void handle_zone_write_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& identity);

}  // namespace detail

}  // namespace ker::net::wki
