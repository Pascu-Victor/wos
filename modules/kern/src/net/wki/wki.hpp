#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Forward declarations
// ─────────────────────────────────────────────────────────────────────────────

struct WkiPeer;
struct WkiChannel;
struct WkiTransport;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration constants
// ─────────────────────────────────────────────────────────────────────────────

constexpr size_t WKI_MAX_PEERS = 256;
constexpr size_t WKI_MAX_TRANSPORTS = 8;
constexpr size_t WKI_MAX_CHANNELS = 256;  // per-peer

// Heartbeat defaults (in milliseconds to fit in uint16_t for wire format)
constexpr uint16_t WKI_DEFAULT_HEARTBEAT_INTERVAL_MS = 300;  // 1 second
constexpr uint16_t WKI_MIN_HEARTBEAT_INTERVAL_MS = 100;      // 100 ms
constexpr uint16_t WKI_MAX_HEARTBEAT_INTERVAL_MS = 1000;     // 5 seconds
constexpr uint8_t WKI_DEFAULT_MISS_THRESHOLD = 5;            // 5 misses = 5 second timeout

// Grace period for newly connected peers
constexpr uint32_t WKI_PEER_GRACE_PERIOD_MS = 5000;  // 5 seconds after connection

// Jitter range for heartbeat timing (percentage of interval)
constexpr uint8_t WKI_HEARTBEAT_JITTER_PERCENT = 25;  // +/- 25% jitter

// Reliability defaults
constexpr uint32_t WKI_INITIAL_RTO_US = 100000;    // 100 ms (conservative for VM environment)
constexpr uint32_t WKI_MIN_RTO_US = 50000;         // 50 ms
constexpr uint32_t WKI_MAX_RTO_US = 500000;        // 500 ms
constexpr uint32_t WKI_ACK_DELAY_US = 1000;        // 1 ms
constexpr uint8_t WKI_FAST_RETRANSMIT_THRESH = 3;  // 3 dup ACKs

// Credit defaults
constexpr uint16_t WKI_CREDITS_CONTROL = 64;
constexpr uint16_t WKI_CREDITS_ZONE_MGMT = 32;
constexpr uint16_t WKI_CREDITS_EVENT_BUS = 128;
constexpr uint16_t WKI_CREDITS_RESOURCE = 32;
constexpr uint16_t WKI_CREDITS_DYNAMIC = 256;

// LSA
constexpr uint32_t WKI_LSA_REFRESH_MS = 5000;  // 5 seconds

// ─────────────────────────────────────────────────────────────────────────────
// Error codes
// ─────────────────────────────────────────────────────────────────────────────

constexpr int WKI_OK = 0;
constexpr int WKI_ERR_NO_MEM = -1;
constexpr int WKI_ERR_NO_ROUTE = -2;
constexpr int WKI_ERR_PEER_FENCED = -3;
constexpr int WKI_ERR_NO_CREDITS = -4;
constexpr int WKI_ERR_TIMEOUT = -5;
constexpr int WKI_ERR_INVALID = -6;
constexpr int WKI_ERR_NOT_FOUND = -7;
constexpr int WKI_ERR_BUSY = -8;
constexpr int WKI_ERR_TX_FAILED = -9;

// ─────────────────────────────────────────────────────────────────────────────
// Locality tag — attached to all remote resources
// ─────────────────────────────────────────────────────────────────────────────

enum class Locality : uint8_t {
    LOCAL = 0,       // same node
    RDMA_LOCAL = 1,  // same RDMA zone, direct peer
    REMOTE = 2,      // multi-hop, routed
};

// ─────────────────────────────────────────────────────────────────────────────
// Transport Abstraction (Layer 1)
// ─────────────────────────────────────────────────────────────────────────────

// RX callback type: called by transport when a frame arrives
using WkiRxHandler = void (*)(WkiTransport* transport, const void* data, uint16_t len);

struct WkiTransport {
    const char* name;  // "eth0", "ivshmem0", etc.
    uint16_t mtu;      // max payload excluding WKI header
    bool rdma_capable;
    void* private_data;

    // Transmit a frame to a direct neighbor
    int (*tx)(WkiTransport* self, uint16_t neighbor_id, const void* data, uint16_t len);

    // Set the receive handler
    void (*set_rx_handler)(WkiTransport* self, WkiRxHandler handler);

    // RDMA operations (NULL if !rdma_capable)
    int (*rdma_register_region)(WkiTransport* self, uint64_t phys_addr, uint32_t size, uint32_t* rkey);
    int (*rdma_read)(WkiTransport* self, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, void* local_buf, uint32_t len);
    int (*rdma_write)(WkiTransport* self, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len);
    int (*doorbell)(WkiTransport* self, uint16_t neighbor_id, uint32_t value);

    // Linkage for transport registry
    WkiTransport* next;
};

// ─────────────────────────────────────────────────────────────────────────────
// Peer State Machine
// ─────────────────────────────────────────────────────────────────────────────

enum class PeerState : uint8_t {
    UNKNOWN = 0,
    HELLO_SENT = 1,
    CONNECTED = 2,
    FENCED = 3,
    RECONNECTING = 4,
};

struct WkiPeer {
    uint16_t node_id = WKI_NODE_INVALID;
    std::array<uint8_t, 6> mac = {};
    WkiTransport* transport = nullptr;
    PeerState state = PeerState::UNKNOWN;
    uint64_t last_heartbeat = 0;                                         // timestamp of last recv'd heartbeat (microseconds)
    uint64_t connected_time = 0;                                         // timestamp when peer transitioned to CONNECTED
    uint32_t rtt_us = 0;                                                 // smoothed RTT (microseconds)
    uint32_t rtt_var_us = 0;                                             // RTT variance
    uint16_t heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;  // in milliseconds
    uint8_t miss_threshold = WKI_DEFAULT_MISS_THRESHOLD;
    uint8_t missed_beats = 0;       // consecutive missed heartbeats
    bool is_direct = false;         // direct neighbor?
    uint16_t rdma_zone_id = 0;      // RDMA zone (0 = none)
    uint32_t rdma_zone_bitmap = 0;  // peer's zone membership

    // Routing (for non-direct peers)
    uint16_t next_hop = WKI_NODE_INVALID;
    uint8_t hop_count = 0;
    uint16_t link_cost = 0;

    // Flow control (aggregate)
    uint16_t credits_available = 0;
    uint16_t credits_granted = 0;

    // Capabilities from HELLO
    uint16_t capabilities = 0;
    uint16_t max_channels = 0;

    // HELLO retry
    uint64_t hello_sent_time = 0;
    uint8_t hello_retries = 0;

    ker::mod::sys::Spinlock lock;

    WkiPeer() = default;
    ~WkiPeer() = default;
    WkiPeer(const WkiPeer&) = delete;
    WkiPeer& operator=(const WkiPeer&) = delete;
    WkiPeer(WkiPeer&&) = delete;
    WkiPeer& operator=(WkiPeer&&) = delete;
};

// ─────────────────────────────────────────────────────────────────────────────
// Retransmit queue entry
// ─────────────────────────────────────────────────────────────────────────────

struct WkiRetransmitEntry {
    uint8_t* data;  // copy of full frame (header + payload)
    uint16_t len;
    uint32_t seq;
    uint64_t send_time_us;
    uint8_t retries;
    WkiRetransmitEntry* next;
};

// ─────────────────────────────────────────────────────────────────────────────
// Reorder buffer entry
// ─────────────────────────────────────────────────────────────────────────────

struct WkiReorderEntry {
    uint8_t* data;  // payload only
    uint16_t len;
    uint8_t msg_type;
    uint32_t seq;
    WkiReorderEntry* next;
};

// ─────────────────────────────────────────────────────────────────────────────
// Channel — per-peer, per-channel reliability and flow control
// ─────────────────────────────────────────────────────────────────────────────

struct WkiChannel {
    uint16_t channel_id = 0;
    uint16_t peer_node_id = WKI_NODE_INVALID;
    PriorityClass priority = PriorityClass::LATENCY;
    bool active = false;

    // Reliability (seq/ack)
    uint32_t tx_seq = 0;          // next seq to send
    uint32_t tx_ack = 0;          // last ACK received from peer
    uint32_t rx_seq = 0;          // next expected seq from peer
    uint32_t rx_ack_pending = 0;  // highest seq received, not yet ACKed
    bool ack_pending = false;

    // Flow control (credits)
    uint16_t tx_credits = 0;  // credits available for sending
    uint16_t rx_credits = 0;  // credits we've granted to peer

    // Retransmit
    uint32_t rto_us = WKI_INITIAL_RTO_US;
    uint32_t srtt_us = 0;
    uint32_t rttvar_us = 0;
    uint64_t retransmit_deadline = 0;
    WkiRetransmitEntry* retransmit_head = nullptr;
    WkiRetransmitEntry* retransmit_tail = nullptr;
    uint32_t retransmit_count = 0;

    // Reorder buffer
    WkiReorderEntry* reorder_head = nullptr;
    uint32_t reorder_count = 0;

    // Duplicate ACK tracking (for fast retransmit)
    uint32_t last_dup_ack = 0;
    uint8_t dup_ack_count = 0;

    // Statistics
    uint64_t bytes_sent = 0;
    uint64_t bytes_received = 0;
    uint32_t retransmits = 0;

    ker::mod::sys::Spinlock lock;
};

// ─────────────────────────────────────────────────────────────────────────────
// Global WKI State
// ─────────────────────────────────────────────────────────────────────────────

struct WkiState {
    uint16_t my_node_id = WKI_NODE_INVALID;
    std::array<uint8_t, 6> my_mac = {};
    bool initialized = false;

    // Peer table
    std::array<WkiPeer, WKI_MAX_PEERS> peers;
    uint16_t peer_count = 0;
    ker::mod::sys::Spinlock peer_lock;

    // Transport registry (linked list)
    WkiTransport* transports = nullptr;
    uint16_t transport_count = 0;
    ker::mod::sys::Spinlock transport_lock;

    // Own capabilities
    uint16_t capabilities = 0;
    uint16_t max_channels = WKI_MAX_CHANNELS;
    uint32_t rdma_zone_bitmap = 0;

    // LSA state
    uint32_t my_lsa_seq = 0;
};

// Global WKI state singleton
extern WkiState g_wki;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Core
// ─────────────────────────────────────────────────────────────────────────────

// Initialize WKI subsystem. Called from main.cpp during boot.
void wki_init();

// Shutdown WKI subsystem.
void wki_shutdown();

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Transport
// ─────────────────────────────────────────────────────────────────────────────

void wki_transport_register(WkiTransport* transport);
void wki_transport_unregister(WkiTransport* transport);

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Peer Management
// ─────────────────────────────────────────────────────────────────────────────

// Find peer by node ID (returns nullptr if not found)
auto wki_peer_find(uint16_t node_id) -> WkiPeer*;

// Allocate a new peer slot
auto wki_peer_alloc(uint16_t node_id) -> WkiPeer*;

// Get the peer count
auto wki_peer_count() -> uint16_t;

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Sending
// ─────────────────────────────────────────────────────────────────────────────

// Send a message on a specific channel (handles reliability, credits, routing)
auto wki_send(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len) -> int;

// Send a raw frame (bypasses reliability — used for HELLO, HEARTBEAT)
auto wki_send_raw(uint16_t dst_node, MsgType msg_type, const void* payload, uint16_t payload_len, uint8_t flags = 0) -> int;

// ─────────────────────────────────────────────────────────────────────────────
// Public API — RX Dispatch (called by transport layer)
// ─────────────────────────────────────────────────────────────────────────────

// Main RX entry point — called when a WKI frame arrives from any transport
void wki_rx(WkiTransport* transport, const void* data, uint16_t len);

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Channel Management
// ─────────────────────────────────────────────────────────────────────────────

// Find or create a well-known channel to a peer
auto wki_channel_get(uint16_t peer_node, uint16_t channel_id) -> WkiChannel*;

// Allocate a dynamic channel to a peer
auto wki_channel_alloc(uint16_t peer_node, PriorityClass priority) -> WkiChannel*;

// Close and free a channel
void wki_channel_close(WkiChannel* ch);

// Close all channels to a specific peer (used during fencing)
void wki_channels_close_for_peer(uint16_t node_id);

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Timer Tick (called from timer interrupt / periodic thread)
// ─────────────────────────────────────────────────────────────────────────────

// Process heartbeats, retransmit timers, ACK delays
void wki_timer_tick(uint64_t now_us);

// ─────────────────────────────────────────────────────────────────────────────
// Public API — Time source (must be provided by platform)
// ─────────────────────────────────────────────────────────────────────────────

auto wki_now_us() -> uint64_t;

// CRC32 checksum (used for WKI header + payload integrity)
auto wki_crc32(const void* data, size_t len) -> uint32_t;
auto wki_crc32_continue(uint32_t crc, const void* data, size_t len) -> uint32_t;

// ─────────────────────────────────────────────────────────────────────────────
// Internal — RX message handlers (implemented in peer.cpp, channel.cpp, etc.)
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

void handle_hello(WkiTransport* transport, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_hello_ack(WkiTransport* transport, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_heartbeat(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_heartbeat_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_lsa(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_fence_notify(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
