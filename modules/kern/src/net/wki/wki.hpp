#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/wki/wire.hpp>
#include <platform/sys/spinlock.hpp>

#include "platform/sched/task.hpp"

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

struct WkiPeer;
struct WkiChannel;
struct WkiTransport;

}  // namespace ker::net::wki

// Forward declare PacketBuffer in its own namespace for tx_pkt
namespace ker::net {
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Configuration constants
// -----------------------------------------------------------------------------

constexpr size_t WKI_MAX_PEERS = 256;
constexpr size_t WKI_MAX_TRANSPORTS = 8;
constexpr size_t WKI_MAX_CHANNELS = 256;  // per-peer
constexpr size_t WKI_RX_DISPATCH_WAITER_SLOTS = 8;
static_assert(WKI_CHAN_DYNAMIC_BASE < WKI_CHAN_DYNAMIC_RESERVED_BASE, "dynamic channel allocation range must not be empty");
static_assert(WKI_CHAN_DYNAMIC_RESERVED_BASE < WKI_MAX_CHANNELS, "reserved WKI channels must fit in the per-peer channel table");
static_assert(std::atomic<uint16_t>::is_always_lock_free, "WKI channel scan bounds must stay lock-free");

// Heartbeat defaults (in milliseconds to fit in uint16_t for wire format)
constexpr uint16_t WKI_DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;  // 1 second
constexpr uint16_t WKI_MIN_HEARTBEAT_INTERVAL_MS = 300;       // 300 ms
constexpr uint16_t WKI_MAX_HEARTBEAT_INTERVAL_MS = 5000;      // 5 seconds
constexpr uint8_t WKI_DEFAULT_MISS_THRESHOLD = 10;            // 10 misses = 10 second timeout
constexpr uint32_t WKI_PEER_FENCE_CONFIRM_GRACE_MS = 5000;    // per-probe grace before destructive fence cleanup
constexpr uint8_t WKI_PEER_FENCE_PROBE_ROUNDS = 2;            // candidate + one re-probe before fencing

// Grace period for newly connected peers
constexpr uint32_t WKI_PEER_GRACE_PERIOD_MS = static_cast<uint32_t>(WKI_DEFAULT_MISS_THRESHOLD) * WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;

// Jitter range for heartbeat timing (percentage of interval)
constexpr uint8_t WKI_HEARTBEAT_JITTER_PERCENT = 25;  // +/- 25% jitter

// Reliability defaults (tuned for single-hop switched Ethernet, ~5-20us RTT)
constexpr uint32_t WKI_INITIAL_RTO_US = 500;       // 500 us (Jacobson/Karels adapts to actual RTT)
constexpr uint32_t WKI_MIN_RTO_US = 100;           // 100 us (floor: 5-10x single-hop RTT)
constexpr uint32_t WKI_MAX_RTO_US = 50000;         // 50 ms (cap for pathological cases)
constexpr uint32_t WKI_ACK_DELAY_US = 2000;        // 2 ms: window for piggybacking on BULK channels (LATENCY channels ack inline)
constexpr uint8_t WKI_FAST_RETRANSMIT_THRESH = 3;  // 3 dup ACKs
// Cumulative ACK sentinel: ack_num + 1 wraps to the initial tx_ack (zero), so
// it advertises that no receive sequence has been consumed yet.
constexpr uint32_t WKI_ACK_NONE = UINT32_MAX;

// Credit defaults
constexpr uint16_t WKI_CREDITS_CONTROL = 64;
constexpr uint16_t WKI_CREDITS_ZONE_MGMT = 32;
constexpr uint16_t WKI_CREDITS_EVENT_BUS = 128;
constexpr uint16_t WKI_CREDITS_RESOURCE = 32;
constexpr uint16_t WKI_CREDITS_DYNAMIC = 256;
// IPC pipe/socket data is bulkier than latency control traffic. Use the
// largest window representable by the 8-bit wire credit advertisement.
constexpr uint16_t WKI_CREDITS_IPC_DATA = 255;

// Reorder-buffer limits are intentionally separate from advertised credits.
// Credits bound what the peer should send concurrently; reorder limits bound
// how much future traffic we can retain while one missing sequence is being
// retransmitted. Resource channels fan in pipe/device traffic and can arrive
// much farther ahead than their nominal credit window under load.
constexpr uint16_t WKI_REORDER_CONTROL = WKI_CREDITS_CONTROL;
constexpr uint16_t WKI_REORDER_ZONE_MGMT = WKI_CREDITS_ZONE_MGMT;
constexpr uint16_t WKI_REORDER_EVENT_BUS = WKI_CREDITS_EVENT_BUS;
constexpr uint16_t WKI_REORDER_RESOURCE = 1024;
constexpr uint16_t WKI_REORDER_DYNAMIC = WKI_CREDITS_DYNAMIC;
constexpr uint16_t WKI_REORDER_IPC_DATA = WKI_CREDITS_IPC_DATA;

// -----------------------------------------------------------------------------
// Error codes
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Locality tag - attached to all remote resources
// -----------------------------------------------------------------------------

enum class Locality : uint8_t {
    LOCAL = 0,       // same node
    RDMA_LOCAL = 1,  // same RDMA zone, direct peer
    REMOTE = 2,      // multi-hop, routed
};

// V2: NIC attachment policy [V2 A5.1]
enum class WkiNicPolicy : uint8_t {
    ATTACH_ALL = 0,     // Auto-attach every discovered remote NIC
    ATTACH_SPARSE = 1,  // Auto-attach only if no local NIC covers that subnet (default)
    MANUAL = 2,         // Never auto-attach; require explicit kernel API call
};

// V2: RX backpressure credits for forwarded NET packets [V2 A5.6]
constexpr uint16_t WKI_NET_RX_CREDITS = 64;

// -----------------------------------------------------------------------------
// Transport Abstraction (Layer 1)
// -----------------------------------------------------------------------------

// RX callback type: called by transport when a frame arrives
using WkiRxHandler = void (*)(WkiTransport* transport, const void* data, uint16_t len);

struct WkiTransport {
    const char* name;  // "eth0", "ivshmem0", etc.
    uint16_t mtu;      // max payload excluding WKI header
    bool rdma_capable;
    void* private_data;

    // Transmit a frame to a direct neighbor (copies data into a new PacketBuffer)
    int (*tx)(WkiTransport* self, uint16_t neighbor_id, const void* data, uint16_t len);

    // Zero-copy transmit: caller provides a pre-built PacketBuffer with WKI
    // frame already at pkt->data.  Transport prepends link header and sends.
    // The transport takes ownership of pkt (frees on error).
    int (*tx_pkt)(WkiTransport* self, uint16_t neighbor_id, ker::net::PacketBuffer* pkt);

    // Set the receive handler
    void (*set_rx_handler)(WkiTransport* self, WkiRxHandler handler);

    // RDMA operations (NULL if !rdma_capable)
    int (*rdma_register_region)(WkiTransport* self, uint64_t phys_addr, uint32_t size, uint32_t* rkey);
    // Optionally drop a locally registered region before its backing storage
    // is freed. Only transports with a quiescent remote-revocation guarantee
    // provide this; size is required by bitmap-backed transports and ignored
    // by RoCE.
    int (*rdma_unregister_region)(WkiTransport* self, uint32_t rkey, uint32_t size);
    int (*rdma_read)(WkiTransport* self, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, void* local_buf, uint32_t len);
    int (*rdma_write)(WkiTransport* self, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len);
    int (*doorbell)(WkiTransport* self, uint16_t neighbor_id, uint32_t value);

    // Linkage for transport registry
    WkiTransport* next;
};

// -----------------------------------------------------------------------------
// Peer State Machine
// -----------------------------------------------------------------------------

enum class PeerState : uint8_t {
    UNKNOWN = 0,
    HELLO_SENT = 1,
    CONNECTED = 2,
    FENCED = 3,
    RECONNECTING = 4,
};

struct WkiPeer {
    uint16_t node_id = WKI_NODE_INVALID;
    proto::MacAddress mac;
    WkiTransport* transport = nullptr;
    WkiTransport* rdma_transport = nullptr;  // RDMA-capable overlay (ivshmem or RoCE)
    PeerState state = PeerState::UNKNOWN;
    uint64_t last_heartbeat = 0;        // timestamp of last recv'd heartbeat (microseconds)
    uint64_t last_rx_activity = 0;      // timestamp of last received packet (any type) from this peer
    uint64_t last_tx_activity = 0;      // timestamp of last packet sent to this peer (any type); used to suppress redundant heartbeats
    uint64_t fence_defer_until_us = 0;  // do not destructively fence before this local confirmation deadline
    uint64_t connected_time = 0;        // timestamp when peer transitioned to CONNECTED
    uint32_t rtt_us = 0;                // smoothed RTT (microseconds)
    uint32_t rtt_var_us = 0;            // RTT variance
    uint16_t heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;  // in milliseconds
    uint8_t miss_threshold = WKI_DEFAULT_MISS_THRESHOLD;
    uint8_t missed_beats = 0;       // consecutive missed heartbeats
    bool is_direct = false;         // direct neighbor?
    uint16_t rdma_zone_id = 0;      // RDMA zone (0 = none)
    uint32_t rdma_zone_bitmap = 0;  // peer's zone membership

    // V2: Hostname identity [V2 A1.5]
    std::array<char, WKI_HOSTNAME_MAX> hostname;  // UTF-8, NUL-terminated if shorter than WKI_HOSTNAME_MAX

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

    // Channel reset epochs advertised through HELLO reserved bytes.  When a
    // peer observes our local epoch change, it drops stale channel seq state
    // for this node so a post-fence seq=0 stream is not mistaken for an old
    // duplicate.
    uint32_t local_channel_epoch = 0;
    uint32_t remote_channel_epoch = 0;
    uint32_t remote_boot_epoch = 0;
    // Serializes peer/channel teardown with the final scheduler publication
    // of receiver-side remote compute work.
    std::atomic<bool> disconnect_cleanup_in_progress{false};
    // HELLO RX retires exact channel/session state synchronously, then asks the
    // task-context timer worker to terminalize work bound to that retired
    // generation.
    std::atomic<bool> compute_reset_cleanup_pending{false};
    std::atomic<bool> vfs_reset_rebind_pending{false};
    std::atomic<bool> block_resume_pending{false};
    // A boot-epoch change invalidates remote discovery.  A channel-only epoch
    // may reuse the still-live resource observation and rebind it locally.
    std::atomic<bool> vfs_reset_invalidate_discovery{false};
    // Only a transition between two known nonzero boot epochs proves that old
    // remote bindings cannot survive. Kept separate from discovery invalidation
    // so an unknown-to-known observation still tears down fail-closed.
    std::atomic<bool> vfs_reset_owner_reboot_proven{false};

    // HELLO retry
    uint64_t hello_sent_time = 0;
    uint8_t hello_retries = 0;

    // Per-peer channel index - O(1) lookup by channel_id
    // Pointers into the global channel pool; nullptr = not allocated.
    std::array<WkiChannel*, WKI_MAX_CHANNELS> channels = {};
    // Monotonic exclusive bounds for nonblocking cross-channel ACK scans.
    // Keeping the reserved range separate prevents IPC_DATA at channel 240
    // from forcing every send to walk the unused ordinary-channel gap.
    std::atomic<uint16_t> ordinary_channel_scan_limit{0};
    std::atomic<uint16_t> reserved_channel_scan_limit{WKI_CHAN_DYNAMIC_RESERVED_BASE};

    ker::mod::sys::Spinlock lock;

    WkiPeer() = default;
    ~WkiPeer() = default;
    WkiPeer(const WkiPeer&) = delete;
    WkiPeer& operator=(const WkiPeer&) = delete;
    WkiPeer(WkiPeer&&) = delete;
    WkiPeer& operator=(WkiPeer&&) = delete;
};

// -----------------------------------------------------------------------------
// Retransmit queue entry
// -----------------------------------------------------------------------------

struct WkiRetransmitEntry {
    uint8_t* data;  // copy of full frame (header + payload)
    uint16_t len;
    uint32_t seq;
    uint64_t send_time_us;
    uint8_t retries;
    WkiRetransmitEntry* next;
};

// -----------------------------------------------------------------------------
// Reorder buffer entry
// -----------------------------------------------------------------------------

struct WkiReorderEntry {
    WkiHeader hdr = {};
    uint8_t* data = nullptr;  // payload only
    uint16_t len = 0;
    uint8_t msg_type = 0;
    uint32_t seq = 0;
    uint32_t channel_generation = 0;
    WkiReorderEntry* next = nullptr;
};

// -----------------------------------------------------------------------------
// Channel - per-peer, per-channel reliability and flow control
// -----------------------------------------------------------------------------

// Max frame size: WKI header + max Ethernet payload
constexpr size_t WKI_MAX_FRAME_SIZE = WKI_HEADER_SIZE + WKI_ETH_MAX_PAYLOAD;

struct WkiChannel {
    uint16_t channel_id = 0;
    uint16_t peer_node_id = WKI_NODE_INVALID;
    PriorityClass priority = PriorityClass::LATENCY;
    bool active = false;
    uint32_t generation = 0;  // Incremented on each pool-slot allocation.

    // Reliability (seq/ack)
    uint32_t tx_seq = 0;           // next seq to send
    uint32_t tx_ack = 0;           // last ACK received from peer
    uint32_t rx_seq = 0;           // next expected seq from peer
    uint32_t rx_dispatch_seq = 0;  // next received seq allowed to enter handlers
    std::array<ker::mod::sched::task::Task*, WKI_RX_DISPATCH_WAITER_SLOTS> rx_dispatch_waiters = {};
    uint32_t rx_ack_pending = WKI_ACK_NONE;  // highest seq received, or WKI_ACK_NONE before the first consumed frame
    bool ack_pending = false;
    // Prevent reconnect baseline resync from skipping a first frame that was
    // observed but deliberately left unconsumed for bounded-queue retry.
    bool rx_baseline_initialized = false;
    uint64_t ack_pending_since_us = 0;  // time when ack_pending was last set (for delay enforcement)

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
    uint64_t perf_last_stall_report_us = 0;
    uint32_t perf_last_stall_status = 0;

    // Pre-allocated inline retransmit buffers - eliminates kmalloc for
    // typical small messages (control, ACKs, small payloads).  Frames
    // larger than WKI_RT_INLINE_SIZE fall back to kmalloc.
    // With zero-copy TX, outgoing frames are built directly in PacketBuffer
    // so no separate tx_frame_buf is needed.
    static constexpr size_t WKI_RT_INLINE_SIZE = 1500;
    std::array<uint8_t, WKI_RT_INLINE_SIZE> tx_rt_buf = {};
    WkiRetransmitEntry tx_rt_entry = {};
    bool tx_rt_entry_in_use = false;  // true while inline entry is in the retransmit queue

    ker::mod::sys::Spinlock lock;
};

// Immutable identity of one allocation of a reusable channel-pool slot.
// Capture this while the pool and channel locks still protect publication;
// consumers must not recover identity later from the reusable raw pointer.
struct WkiChannelIdentity {
    WkiChannel* channel = nullptr;
    uint16_t peer_node_id = WKI_NODE_INVALID;
    uint16_t channel_id = 0;
    uint32_t generation = 0;
};

// Immutable receipt for one recoverably published reliable frame.  The
// channel identity prevents a recycled pool slot or a reset sequence space
// from being mistaken for acknowledgement of the original frame.
struct WkiReliableTxToken {
    WkiChannelIdentity channel_identity = {};
    uint32_t sequence = 0;
};

enum class WkiReliableTxStatus : uint8_t {
    INVALID = 0,
    PENDING = 1,
    ACKED = 2,
    RETIRED = 3,
};

constexpr size_t WKI_CHANNEL_DIAG_MAX = 256;

struct WkiChannelDiag {
    uint16_t peer_node = WKI_NODE_INVALID;
    PeerState peer_state = PeerState::UNKNOWN;
    bool peer_direct = false;
    uint16_t next_hop = WKI_NODE_INVALID;
    uint8_t hop_count = 0;
    uint16_t channel_id = 0;
    uint8_t priority = 0;
    bool active = false;
    uint32_t tx_seq = 0;
    uint32_t tx_ack = 0;
    uint32_t rx_seq = 0;
    uint32_t rx_ack_pending = 0;
    bool ack_pending = false;
    uint16_t tx_credits = 0;
    uint16_t rx_credits = 0;
    uint32_t retransmit_count = 0;
    uint32_t reorder_count = 0;
    uint32_t retransmits = 0;
    uint64_t bytes_sent = 0;
    uint64_t bytes_received = 0;
};

// -----------------------------------------------------------------------------
// Global WKI State
// -----------------------------------------------------------------------------

// Peer hash table entry - compact open-addressing index for O(1) peer lookup.
// The entire table fits in ~1KB (~16 cache lines).
struct PeerHashEntry {
    uint16_t node_id = WKI_NODE_INVALID;
    int16_t peer_idx = -1;  // index into WkiState::peers[]
};

constexpr size_t WKI_PEER_HASH_SIZE = 256;

struct WkiState {
    uint16_t my_node_id = WKI_NODE_INVALID;
    proto::MacAddress my_mac;
    bool initialized = false;

    // V2: Local hostname identity [V2 A1.6]
    std::array<char, WKI_HOSTNAME_MAX> local_hostname = {};

    // Peer table
    std::array<WkiPeer, WKI_MAX_PEERS> peers;
    uint16_t peer_count = 0;
    ker::mod::sys::Spinlock peer_lock;

    // Peer hash index - multiplicative hash with linear probing
    std::array<PeerHashEntry, WKI_PEER_HASH_SIZE> peer_hash = {};

    // Transport registry (linked list)
    WkiTransport* transports = nullptr;
    uint16_t transport_count = 0;
    ker::mod::sys::Spinlock transport_lock;

    // Own capabilities
    uint16_t capabilities = 0;
    uint16_t max_channels = WKI_MAX_CHANNELS;
    uint32_t rdma_zone_bitmap = 0;
    uint32_t local_boot_epoch = 1;

    // LSA state
    uint32_t my_lsa_seq = 0;

    // V2: NIC attachment policy [V2 A5.1]
    WkiNicPolicy nic_policy = WkiNicPolicy::ATTACH_SPARSE;

    // V2: Sequential index for proxy NIC MAC generation [V2 A5.4]
    uint8_t proxy_nic_index = 0;
};

// Global WKI state singleton
extern WkiState g_wki;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr auto wki_channel_allocator_node(uint16_t node_a, uint16_t node_b) -> uint16_t { return (node_a < node_b) ? node_a : node_b; }

constexpr auto wki_requester_controls_dynamic_channel(uint16_t requester_node, uint16_t owner_node) -> bool {
    return requester_node == wki_channel_allocator_node(requester_node, owner_node);
}

// -----------------------------------------------------------------------------
// Public API - Core
// -----------------------------------------------------------------------------

// Initialize WKI subsystem. Called from main.cpp during boot.
void wki_init();

// Shutdown WKI subsystem.
void wki_shutdown();

// -----------------------------------------------------------------------------
// Public API - Transport
// -----------------------------------------------------------------------------

void wki_transport_register(WkiTransport* transport);
void wki_transport_unregister(WkiTransport* transport);

// -----------------------------------------------------------------------------
// Public API - Peer Management
// -----------------------------------------------------------------------------

// Find peer by node ID (returns nullptr if not found)
auto wki_peer_find(uint16_t node_id) -> WkiPeer*;

// List peers in a specific RDMA zone
auto wki_peer_list_by_zone(uint8_t zone_id) -> auto;

// Allocate a new peer slot
auto wki_peer_alloc(uint16_t node_id) -> WkiPeer*;

// Get the peer count
auto wki_peer_count() -> uint16_t;

// True only when both this node and the connected peer advertise every bit in
// the requested capability mask.
auto wki_peer_capability_negotiated(uint16_t peer_node, uint16_t capabilities) -> bool;

// Serialize peer/channel teardown with deferred work that is about to become
// externally visible. The blocking form is task-context only.
auto wki_peer_lifecycle_try_acquire(WkiPeer* peer) -> bool;
auto wki_peer_lifecycle_acquire(WkiPeer* peer) -> bool;
void wki_peer_lifecycle_release(WkiPeer* peer);

// -----------------------------------------------------------------------------
// Public API - Sending
// -----------------------------------------------------------------------------

// Send a message on a specific channel (handles reliability, credits, routing)
auto wki_send(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len) -> int;

// Task-context reliable send that serializes token capture with peer/channel
// reset. WKI_OK means the frame is recoverably enqueued; callers must poll the
// returned token to distinguish acknowledgement from channel retirement.
auto wki_send_tracked(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len,
                      WkiReliableTxToken* token_out) -> int;

// Inspect a tracked reliable send without allocating or waiting. A structurally
// valid token becomes RETIRED, never ACKED, when its exact channel allocation is
// reset or reused before cumulative acknowledgement reaches its sequence.
auto wki_reliable_tx_status(const WkiReliableTxToken& token) -> WkiReliableTxStatus;

// Send only if a stable pool slot still names the exact resource-channel
// generation captured by deferred compute work.
auto wki_send_on_channel_generation(uint16_t dst_node, WkiChannel* expected_channel, uint32_t expected_generation, MsgType msg_type,
                                    const void* payload, uint16_t payload_len) -> int;

// Send only if the exact dynamic-channel allocation captured by the caller is
// still live. Unlike wki_send(), this never allocates or falls through to a
// replacement channel that happens to reuse the same numeric ID.
auto wki_send_on_channel_identity(const WkiChannelIdentity& identity, MsgType msg_type, const void* payload, uint16_t payload_len) -> int;

// Send two logical payload segments on an exact channel allocation. Both
// segments are copied into the reliable frame before this function returns;
// no caller-owned pointer is retained.
auto wki_send_on_channel_identity_split(const WkiChannelIdentity& identity, MsgType msg_type, const void* payload, uint16_t payload_len,
                                        const void* payload_tail, uint16_t payload_tail_len) -> int;

// Send a raw frame (bypasses reliability - used for HELLO, HEARTBEAT)
auto wki_send_raw(uint16_t dst_node, MsgType msg_type, const void* payload, uint16_t payload_len, uint8_t flags = 0) -> int;

// -----------------------------------------------------------------------------
// Public API - RX Dispatch (called by transport layer)
// -----------------------------------------------------------------------------

// Main RX entry point - called when a WKI frame arrives from any transport
void wki_rx(WkiTransport* transport, const void* data, uint16_t len);

// -----------------------------------------------------------------------------
// Public API - Channel Management
// -----------------------------------------------------------------------------

// Find or create a well-known channel to a peer
auto wki_channel_get(uint16_t peer_node, uint16_t channel_id) -> WkiChannel*;

// Find an existing channel to a peer without allocating a pool slot.
auto wki_channel_lookup(uint16_t peer_node, uint16_t channel_id) -> WkiChannel*;

// Allocate a dynamic channel to a peer. When requested, identity_out is
// populated atomically with publication of the reusable pool slot.
auto wki_channel_alloc(uint16_t peer_node, PriorityClass priority, WkiChannelIdentity* identity_out = nullptr) -> WkiChannel*;

// Reserve a specific dynamic channel ID to a peer. Returns nullptr if the
// requested channel is already in use.
auto wki_channel_reserve(uint16_t peer_node, uint16_t channel_id, PriorityClass priority, WkiChannelIdentity* identity_out = nullptr)
    -> WkiChannel*;

// Close and free a channel
void wki_channel_close(WkiChannel* ch);
auto wki_channel_close_generation(WkiChannel* ch, uint16_t expected_peer, uint16_t expected_channel_id, uint32_t expected_generation)
    -> bool;
auto wki_channel_generation_is_live(WkiChannel* ch, uint16_t expected_peer, uint16_t expected_channel_id, uint32_t expected_generation)
    -> bool;

// Close all channels to a specific peer (used during fencing)
void wki_channels_close_for_peer(uint16_t node_id);

// Snapshot active channels for diagnostics. Values are approximate but bounded.
auto wki_channel_diag_snapshot(WkiChannelDiag* out, size_t max) -> size_t;

// -----------------------------------------------------------------------------
// Public API - Timer Tick (called from timer interrupt / periodic thread)
// -----------------------------------------------------------------------------

// Process heartbeats, retransmit timers, ACK delays
void wki_timer_tick(uint64_t now_us);

// Worker for blocking WKI deferred tasks such as auto-mounts and remote device
// attaches. Timer ticks only wake this worker so peer liveness remains timely.
void wki_deferred_work_thread_start();

// Wake the task-context WKI worker. Safe for RX-side producers; the worker
// coalesces notifications and the periodic timer supplies retry pacing.
void wki_deferred_work_notify();

// Snapshot and validate the remote boot epoch under the peer lock. A missing
// peer or an as-yet unknown replacement epoch is not proof of invalidation.
auto wki_peer_remote_boot_epoch_snapshot(uint16_t node_id) -> uint32_t;
auto wki_peer_remote_boot_epoch_invalidated(uint16_t node_id, uint32_t expected_epoch) -> bool;

// Returns true when channel retransmits/ACKs or async wait deadlines require
// the WKI timer thread to stay on its fast cadence.
auto wki_has_fast_timer_work() -> bool;

// Earliest fast-path deadline from channel retransmits/ACKs and timed waits.
// Returns UINT64_MAX when no fast timer work is armed.
auto wki_next_fast_timer_deadline_us(uint64_t now_us) -> uint64_t;

// -----------------------------------------------------------------------------
// Public API - Time source (must be provided by platform)
// -----------------------------------------------------------------------------

auto wki_now_us() -> uint64_t;

// Yield from a spin-wait: drive inline NAPI polling and timer ticks so that
// incoming packets (e.g. ACKs) are processed even when the caller is busy-
// waiting on the current CPU.  Call this instead of bare `pause` loops.
void wki_spin_yield();

// Targeted yield: like wki_spin_yield() but only checks retransmit/ACK
// deadlines for the single channel the caller is waiting on, avoiding the
// O(CHANNEL_POOL_SIZE) scan of wki_timer_tick().
void wki_spin_yield_channel(WkiChannel* ch);
void wki_spin_yield_channel_identity(const WkiChannelIdentity& identity);

// CRC32 checksum (used for WKI header + payload integrity)
auto wki_crc32(const void* data, size_t len) -> uint32_t;
auto wki_crc32_continue(uint32_t crc, const void* data, size_t len) -> uint32_t;

// -----------------------------------------------------------------------------
// V2: Hostname Lookup API [V2 A1.7]
// -----------------------------------------------------------------------------

// Resolve hostname to node_id. Returns WKI_NODE_INVALID on failure.
auto wki_peer_find_by_hostname(const char* hostname) -> uint16_t;

// Get hostname for a node_id. Returns nullptr if unknown.
auto wki_peer_get_hostname(uint16_t node_id) -> const char*;

// -----------------------------------------------------------------------------
// V2: Async Wait Queue API [V2 A8]
// -----------------------------------------------------------------------------

// Forward declaration for Task -  avoiding circular header inclusion
struct WkiWaitEntry {
    enum State : uint8_t {
        PENDING = 0,
        CLAIMED = 1,
        DONE = 2,
    };

    std::atomic<ker::mod::sched::task::Task*> task{nullptr};  // The task that is waiting
    std::atomic<uint8_t> state{PENDING};                      // Result writer ownership/state
    std::atomic<bool> retirement_pending{false};              // External owner still references this stack entry
    int result = 0;                                           // Operation result code
    uint64_t deadline_us = 0;                                 // Timeout deadline (0 = no timeout)
    WkiWaitEntry* next = nullptr;                             // Intrusive linked list
    WkiWaitEntry* prev = nullptr;

    // Optional diagnostic metadata.  The wait core does not interpret these
    // fields; callers fill them before wki_wait_for_op() so boot-gated wait
    // diagnostics can name the stranded operation without changing wire ABI.
    const char* diag_name = nullptr;
    uint64_t diag_callsite = 0;
    uint64_t diag_arg0 = 0;
    uint64_t diag_arg1 = 0;
    uint32_t diag_resource_id = 0;
    uint16_t diag_op_id = 0;
    uint16_t diag_peer = WKI_NODE_INVALID;
    uint16_t diag_channel = 0;
    uint16_t diag_cookie = 0;
};

// Default operation timeout: 15 seconds (allows for CPU-loaded VFS servers)
constexpr uint64_t WKI_OP_TIMEOUT_US = 15'000'000;

// Put the calling task to sleep until woken or timeout.
// Returns: 0 on success, WKI_ERR_TIMEOUT on timeout, WKI_ERR_PEER_FENCED on fencing.
// Caller allocates WkiWaitEntry on kernel stack (no heap allocation needed).
auto wki_wait_for_op(WkiWaitEntry* entry, uint64_t timeout_us) -> int;

// True while the current WKI deferred worker must keep polling transport work
// instead of entering a long scheduler park.
auto wki_current_wait_must_drive_progress() -> bool;

// Wake a waiting task. Called from RX handler context.
void wki_wake_op(WkiWaitEntry* entry, int result);

// Split wake for code that must claim a stack wait entry while holding an
// owning object lock, publish side-band response data, then wake outside.
auto wki_claim_op(WkiWaitEntry* entry) -> bool;
void wki_finish_claimed_op(WkiWaitEntry* entry, int result);
// Allow a preempted completion claimant to reach its final DONE publication.
// Eligible ACTIVE process/daemon task contexts yield; non-schedulable contexts
// use a CPU pause. Blocking RX resource teardown is deferred before reaching
// this helper.
void wki_wait_quiescence_point();
void wki_quiesce_claimed_op(WkiWaitEntry* entry);

// Scan pending waits for timeouts. Called from timer thread.
void wki_wait_timeout_scan(uint64_t now_us);

// Unlink all pending wait entries belonging to task. Called on task exit.
void wki_wait_cleanup_for_task(ker::mod::sched::task::Task* task);

#ifdef WOS_SELFTEST
void wki_selftest_wait_list_link(WkiWaitEntry* entry);
auto wki_selftest_wait_list_contains(WkiWaitEntry const* entry) -> bool;
auto wki_selftest_reliable_rx_peer_state_accepts(PeerState state) -> bool;
auto wki_selftest_split_payload_validation_and_copy() -> bool;
#endif

// -----------------------------------------------------------------------------
// Internal - RX message handlers (implemented in peer.cpp, channel.cpp, etc.)
// -----------------------------------------------------------------------------

namespace detail {

void handle_hello(WkiTransport* transport, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_hello_ack(WkiTransport* transport, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_heartbeat(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_heartbeat_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_lsa(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_fence_notify(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_peer_goodbye(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
