#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Protocol Constants
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_ETHERTYPE = 0x88B7;
constexpr uint16_t WKI_ETHERTYPE_ROCE = 0x88B8;  // RoCE RDMA data frames (L2, MAC-based GIDs)
constexpr uint8_t WKI_VERSION = 3;
constexpr uint32_t WKI_HELLO_MAGIC = 0x574B4900;  // "WKI\0"
constexpr uint16_t WKI_NODE_INVALID = 0x0000;
constexpr uint16_t WKI_NODE_BROADCAST = 0xFFFF;
constexpr uint8_t WKI_DEFAULT_TTL = 16;
constexpr size_t WKI_HEADER_SIZE = 32;

constexpr size_t WKI_AUTH_SESSION_ID_SIZE = 16;
constexpr size_t WKI_AUTH_TAG_SIZE = 16;

struct WkiAuthTrailer {
    std::array<uint8_t, WKI_AUTH_SESSION_ID_SIZE> session_id{};
    uint64_t counter = 0;
    std::array<uint8_t, WKI_AUTH_TAG_SIZE> tag{};
} __attribute__((packed));

static_assert(sizeof(WkiAuthTrailer) == 40, "WKI authentication trailer must be 40 bytes");

constexpr size_t WKI_AUTH_TRAILER_SIZE = sizeof(WkiAuthTrailer);

// Keep every authenticated L2 frame within the existing 9000-byte Ethernet
// envelope: Ethernet header + WKI header + payload + authentication trailer.
constexpr size_t WKI_ETH_MAX_PAYLOAD = 8954 - WKI_AUTH_TRAILER_SIZE;

// -----------------------------------------------------------------------------
// Header Flags (lower 4 bits of version_flags byte)
// -----------------------------------------------------------------------------

constexpr uint8_t WKI_FLAG_ACK_PRESENT = 0x08;  // ack_num/credits fields are valid
constexpr uint8_t WKI_FLAG_PRIORITY = 0x04;     // latency-optimized path
constexpr uint8_t WKI_FLAG_FRAGMENT = 0x02;     // fragment of larger message
constexpr uint8_t WKI_FLAG_ACK_CHANNEL = 0x01;  // reserved[15:0] carries ACK channel_id
constexpr uint8_t WKI_FLAG_RESERVED = WKI_FLAG_ACK_CHANNEL;

// Helper: build version_flags byte
constexpr uint8_t wki_version_flags(uint8_t version, uint8_t flags) { return static_cast<uint8_t>((version << 4) | (flags & 0x0F)); }

// Helper: extract version from version_flags byte
constexpr uint8_t wki_version(uint8_t vf) { return vf >> 4; }

// Helper: extract flags from version_flags byte
constexpr uint8_t wki_flags(uint8_t vf) { return vf & 0x0F; }

// -----------------------------------------------------------------------------
// Message Types
// -----------------------------------------------------------------------------

enum class MsgType : uint8_t {
    // Control plane (channel 0)
    HELLO = 0x01,
    HELLO_ACK = 0x02,
    HEARTBEAT = 0x03,
    HEARTBEAT_ACK = 0x04,
    LSA = 0x05,
    LSA_ACK = 0x06,
    FENCE_NOTIFY = 0x07,
    RECONCILE_REQ = 0x08,
    RECONCILE_ACK = 0x09,
    RESOURCE_ADVERT = 0x0A,
    RESOURCE_WITHDRAW = 0x0B,
    PEER_GOODBYE = 0x0C,
    HELLO_CONFIRM = 0x0D,

    // Zone management (channel 1)
    ZONE_CREATE_REQ = 0x20,
    ZONE_CREATE_ACK = 0x21,
    ZONE_DESTROY = 0x22,
    ZONE_NOTIFY_PRE = 0x23,
    ZONE_NOTIFY_POST = 0x24,
    ZONE_READ_REQ = 0x25,
    ZONE_READ_RESP = 0x26,
    ZONE_WRITE_REQ = 0x27,
    ZONE_WRITE_ACK = 0x28,
    ZONE_NOTIFY_PRE_ACK = 0x29,
    ZONE_NOTIFY_POST_ACK = 0x2A,

    // Event bus (channel 2)
    EVENT_SUBSCRIBE = 0x30,
    EVENT_UNSUBSCRIBE = 0x31,
    EVENT_PUBLISH = 0x32,
    EVENT_ACK = 0x33,

    // Resource operations (channel 3 + dynamic)
    DEV_ATTACH_REQ = 0x40,
    DEV_ATTACH_ACK = 0x41,
    DEV_DETACH = 0x42,
    DEV_OP_REQ = 0x43,
    DEV_OP_RESP = 0x44,
    DEV_IRQ_FWD = 0x45,
    CHANNEL_OPEN = 0x46,
    CHANNEL_OPEN_ACK = 0x47,
    CHANNEL_CLOSE = 0x48,

    // Compute (uses RESOURCE channel)
    TASK_SUBMIT = 0x50,
    TASK_ACCEPT = 0x51,
    TASK_REJECT = 0x52,
    TASK_COMPLETE = 0x53,
    TASK_CANCEL = 0x54,
    LOAD_REPORT = 0x55,
    TASK_SUBMIT_FRAGMENT = 0x56,
    TASK_SERVICE_REPORT = 0x57,
    TASK_SIGNAL_REQ = 0x58,
    TASK_SIGNAL_RESP = 0x59,
    GROUP_SIGNAL_REQ = 0x5A,
    GROUP_SIGNAL_RESP = 0x5B,
    TASK_EXEC_READY = 0x5C,
    TASK_EXEC_COMMIT = 0x5D,
    GROUP_ROUTE_REQ = 0x5E,
    GROUP_ROUTE_RESP = 0x5F,
    GROUP_RECEIVER_REQ = 0x60,
    GROUP_RECEIVER_RESP = 0x61,
    TASK_TERMINAL_SIGNAL = 0x62,
    TASK_PARENT_DEATH = 0x63,
    TASK_CHILD_SIGNAL = 0x64,
    TASK_CHILD_QUERY_REQ = 0x65,
    TASK_CHILD_QUERY_RESP = 0x66,
    TASK_CHILD_CLAIM_REQ = 0x67,
    TASK_CHILD_CLAIM_RESP = 0x68,

    // Secure direct data plane: authenticated extension on channel4.
    SECURE_RDMA_OFFER = 0x70,
    SECURE_RDMA_CONFIRM = 0x71,
    SECURE_RDMA_GRANT = 0x72,
    SECURE_RDMA_REVOKE = 0x73,
    SECURE_RDMA_ARM = 0x74,
    SECURE_RDMA_ARM_ACK = 0x75,
    SECURE_RDMA_CANCEL = 0x76,
    SECURE_RDMA_RESULT = 0x77,
};

// -----------------------------------------------------------------------------
// Well-known Channel IDs
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_CHAN_CONTROL = 0;
constexpr uint16_t WKI_CHAN_ZONE_MGMT = 1;
constexpr uint16_t WKI_CHAN_EVENT_BUS = 2;
constexpr uint16_t WKI_CHAN_RESOURCE = 3;
constexpr uint16_t WKI_CHAN_SECURE_RDMA = 4;
constexpr uint16_t WKI_CHAN_DYNAMIC_BASE = 16;
constexpr uint16_t WKI_CHAN_DYNAMIC_RESERVED_BASE = 240;
constexpr uint16_t WKI_CHAN_IPC_DATA = WKI_CHAN_DYNAMIC_RESERVED_BASE;

// -----------------------------------------------------------------------------
// Sequence Number Arithmetic (RFC 1982)
// -----------------------------------------------------------------------------

inline bool seq_before(uint32_t a, uint32_t b) { return static_cast<int32_t>(a - b) < 0; }

inline bool seq_after(uint32_t a, uint32_t b) { return seq_before(b, a); }

inline bool seq_between(uint32_t seq, uint32_t low, uint32_t high) { return !seq_before(seq, low) && seq_before(seq, high); }

// -----------------------------------------------------------------------------
// WKI Header - 32 bytes, fixed size, RDMA-aligned
// -----------------------------------------------------------------------------

struct WkiHeader {
    uint8_t version_flags;  // [7:4] version, [3:0] flags
    uint8_t msg_type;       // MsgType enum
    uint16_t src_node;
    uint16_t dst_node;
    uint16_t channel_id;
    uint32_t seq_num;
    uint32_t ack_num;  // valid if ACK_PRESENT flag set
    uint16_t payload_len;
    uint8_t credits;  // flow control credits granted
    uint8_t hop_ttl;
    uint16_t src_port;  // resource addressing
    uint16_t dst_port;  // resource addressing
    uint32_t checksum;  // CRC32 of header+payload (0 = disabled)
    uint32_t reserved;  // ACK_CHANNEL uses low 16 bits for ack channel_id
} __attribute__((packed));

static_assert(sizeof(WkiHeader) == 32, "WkiHeader must be 32 bytes");

// ACK_PRESENT normally acknowledges the frame's channel_id. When ACK_CHANNEL
// is also set, reserved[15:0] carries the acknowledged channel so any outgoing
// reliable frame can carry an ACK for another channel to the same peer.
constexpr uint32_t WKI_ACK_CHANNEL_MASK = 0x0000FFFFU;

inline auto wki_ack_channel_id(const WkiHeader& hdr) -> uint16_t {
    uint8_t const FLAGS = wki_flags(hdr.version_flags);
    if ((FLAGS & WKI_FLAG_ACK_PRESENT) == 0 || (FLAGS & WKI_FLAG_ACK_CHANNEL) == 0) {
        return hdr.channel_id;
    }
    return static_cast<uint16_t>(hdr.reserved & WKI_ACK_CHANNEL_MASK);
}

inline auto wki_ack_reserved(uint16_t channel_id) -> uint32_t { return static_cast<uint32_t>(channel_id); }

// -----------------------------------------------------------------------------
// HELLO / HELLO_ACK Payload - 32 bytes
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_CAP_RDMA_SUPPORT = 0x0001;
constexpr uint16_t WKI_CAP_ZONE_SUPPORT = 0x0002;
constexpr uint16_t WKI_CAP_RESOURCE_INCARNATION = 0x0004;
// Both peers understand explicit auxiliary VFS-lane identity and may reserve
// RDMA staging for more than the mount anchor without creating extra logical
// invalidation anchors.
constexpr uint16_t WKI_CAP_VFS_MULTI_RDMA_LANES = 0x0008;
// Both peers understand the bounded metadata-batch request/response framing.
// The HELLO payload layout is unchanged; this is an additive capability bit.
constexpr uint16_t WKI_CAP_VFS_METADATA_BATCH = 0x0010;
// Both peers understand the fixed, append-only IPv6 state suffix carried by
// NET resource adverts, attach ACKs, and state notifications.
constexpr uint16_t WKI_CAP_NET_IPV6_STATE = 0x0020;
// Both peers understand the bounded, replay-safe remote xattr transfer
// protocol carried by OP_VFS_XATTR.
constexpr uint16_t WKI_CAP_VFS_XATTR = 0x0040;
// Receiver-challenged, origin/session-bound routing leases. Opt-in until mixed
// version verification completes; the existing LSA prefix remains unchanged.
constexpr uint16_t WKI_CAP_ROUTING_LEASE = 0x0080;
// Resourcev2 admission/completion and version1 subtree service reports. Both
// endpoints require this capability for remote compute execution.
constexpr uint16_t WKI_CAP_RESOURCE_LIMITS = 0x0100;
// Explicit regular-file reads carrying canonical full credentials.
constexpr uint16_t WKI_CAP_VFS_READ_IDENTITY = 0x0200;
// Full canonical credentials appended after the existing compute resource record.
constexpr uint16_t WKI_CAP_COMPUTE_CREDENTIALS = 0x0400;
// Ordinary file/directory OPEN with canonical complete caller credentials.
constexpr uint16_t WKI_CAP_VFS_OPEN_IDENTITY = 0x0800;
// Metadata from a retained channel-scoped file description, independent of path.
constexpr uint16_t WKI_CAP_VFS_FD_METADATA = 0x1000;
// Scalar and batch pathname metadata carrying complete caller credentials.
constexpr uint16_t WKI_CAP_VFS_STAT_IDENTITY = 0x2000;
// Extensible path operations carrying canonical complete caller credentials.
constexpr uint16_t WKI_CAP_VFS_PATH_IDENTITY = 0x4000;
// Mapped credential path operations preserve dot components for confined owner traversal.
constexpr uint16_t WKI_CAP_VFS_COMPONENT_PATHS = 0x8000;
constexpr uint16_t WKI_AUTH_SUITE_HMAC_SHA256_HKDF_SHA256 = 1;
constexpr size_t WKI_AUTH_NONCE_SIZE = 32;

// Hostname constants
constexpr size_t WKI_HOSTNAME_MAX = 64;  // Matches Linux HOST_NAME_MAX (including NUL)

struct HelloPayload {
    uint32_t magic{};  // WKI_HELLO_MAGIC
    uint16_t protocol_version{};
    uint16_t node_id{};                // sender's claimed node ID
    proto::MacAddress mac_addr;        // sender's MAC (for Ethernet transport)
    uint16_t capabilities{};           // bitmask: WKI_CAP_*
    uint16_t heartbeat_interval_ms{};  // proposed heartbeat interval (milliseconds)
    uint16_t max_channels{};
    uint32_t rdma_zone_bitmap{};  // RDMA zone membership (32 zones max)
    std::array<uint8_t, 8> reserved{};
    // --- V2 extension (offset 32) ---
    std::array<char, WKI_HOSTNAME_MAX> hostname{};  // sender's hostname, UTF-8, NUL-terminated if shorter than WKI_HOSTNAME_MAX
    // --- V3 authenticated-session extension (offset 96) ---
    uint16_t auth_suite{};
    uint16_t auth_key_id{};
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> auth_nonce{};
    uint32_t auth_reserved{};
    // --- V3 challenge-confirmation extension (offset 136) ---
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> auth_echo_nonce{};
    uint32_t auth_echo_channel_epoch{};
} __attribute__((packed));

static_assert(sizeof(HelloPayload) == 172, "HelloPayload must be 172 bytes");
static_assert(offsetof(HelloPayload, hostname) == 32, "HelloPayload hostname offset must stay wire-compatible");
static_assert(offsetof(HelloPayload, auth_suite) == 96, "HelloPayload v3 extension must be append-only");
static_assert(offsetof(HelloPayload, auth_echo_nonce) == 136, "HelloPayload confirmation extension must be append-only");

constexpr auto wki_hello_boot_epoch(const HelloPayload& hello) -> uint32_t {
    return static_cast<uint32_t>(hello.reserved.at(4)) | (static_cast<uint32_t>(hello.reserved.at(5)) << 8U) |
           (static_cast<uint32_t>(hello.reserved.at(6)) << 16U) | (static_cast<uint32_t>(hello.reserved.at(7)) << 24U);
}

constexpr auto wki_hello_channel_epoch(const HelloPayload& hello) -> uint32_t {
    return static_cast<uint32_t>(hello.reserved.at(0)) | (static_cast<uint32_t>(hello.reserved.at(1)) << 8U) |
           (static_cast<uint32_t>(hello.reserved.at(2)) << 16U) | (static_cast<uint32_t>(hello.reserved.at(3)) << 24U);
}

// -----------------------------------------------------------------------------
// HEARTBEAT Payload - 16 bytes
// -----------------------------------------------------------------------------

struct HeartbeatPayload {
    uint64_t send_timestamp;   // nanoseconds, for RTT calculation
    uint16_t sender_load;      // CPU load 0-1000
    uint16_t sender_mem_free;  // free memory in units of 256 pages
    uint32_t reserved;
} __attribute__((packed));

static_assert(sizeof(HeartbeatPayload) == 16, "HeartbeatPayload must be 16 bytes");

// HEARTBEAT_ACK echoes the same format (send_timestamp echoed for RTT calc)

// -----------------------------------------------------------------------------
// LSA (Link-State Advertisement) Payload - variable length
// -----------------------------------------------------------------------------

struct LsaNeighborEntry {
    uint16_t node_id;
    uint16_t link_cost;
    uint16_t transport_mtu;
} __attribute__((packed));

struct LsaPayload {
    uint16_t origin_node;  // node that generated this LSA
    uint32_t lsa_seq;      // monotonically increasing
    uint16_t num_neighbors;
    uint32_t rdma_zone_bitmap;
    // Followed by num_neighbors * LsaNeighborEntry
} __attribute__((packed));

// Access the variable-length neighbor array following the fixed header
inline auto lsa_neighbors(LsaPayload* lsa) -> LsaNeighborEntry* {
    return reinterpret_cast<LsaNeighborEntry*>(reinterpret_cast<uint8_t*>(lsa) + sizeof(LsaPayload));
}

inline auto lsa_neighbors(const LsaPayload* lsa) -> const LsaNeighborEntry* {
    return reinterpret_cast<const LsaNeighborEntry*>(reinterpret_cast<const uint8_t*>(lsa) + sizeof(LsaPayload));
}

inline size_t lsa_total_size(const LsaPayload* lsa) { return sizeof(LsaPayload) + (lsa->num_neighbors * sizeof(LsaNeighborEntry)); }

constexpr uint32_t WKI_LSA_LEASE_MAGIC = 0x574C5341;  // "WLSA"
constexpr uint8_t WKI_LSA_LEASE_VERSION = 1;
constexpr uint8_t WKI_LSA_LEASE_WITHDRAWN = 0x01;
constexpr uint32_t WKI_LSA_LEASE_MAX_AGE_MS = 30'000;

// Appended after the unchanged LsaNeighborEntry array. Token zero is a
// discovery hint; only an origin-authenticated reply to our nonzero challenge
// may establish a service lease. Relaying this suffix never renews that lease.
struct LsaLeaseSuffix {
    uint32_t magic = WKI_LSA_LEASE_MAGIC;
    uint8_t version = WKI_LSA_LEASE_VERSION;
    uint8_t flags = 0;
    uint16_t reserved = 0;
    uint32_t boot_epoch = 0;
    std::array<uint8_t, WKI_AUTH_NONCE_SIZE> origin_nonce{};
    uint64_t token = 0;
    uint32_t age_ms = 0;
} __attribute__((packed));
static_assert(sizeof(LsaLeaseSuffix) == 56);
static_assert(sizeof(LsaPayload) == 12);

// MsgType::LSA_ACK is a lease challenge only when both endpoints negotiate
// WKI_CAP_ROUTING_LEASE. Responses use LSA plus LsaLeaseSuffix.
struct LsaLeaseRequest {
    uint32_t magic = WKI_LSA_LEASE_MAGIC;
    uint8_t version = WKI_LSA_LEASE_VERSION;
    uint8_t flags = 0;
    uint16_t reserved = 0;
    uint64_t token = 0;
} __attribute__((packed));
static_assert(sizeof(LsaLeaseRequest) == 16);

inline auto wki_lsa_lease_suffix_valid(LsaLeaseSuffix const& suffix, uint16_t neighbors) -> bool {
    bool nonce_present = false;
    for (auto byte : suffix.origin_nonce) {
        nonce_present = nonce_present || byte != 0;
    }
    return suffix.magic == WKI_LSA_LEASE_MAGIC && suffix.version == WKI_LSA_LEASE_VERSION && suffix.reserved == 0 &&
           (suffix.flags & ~WKI_LSA_LEASE_WITHDRAWN) == 0 && suffix.boot_epoch != 0 && nonce_present &&
           suffix.age_ms <= WKI_LSA_LEASE_MAX_AGE_MS && ((suffix.flags & WKI_LSA_LEASE_WITHDRAWN) == 0 || neighbors == 0);
}

inline auto wki_lsa_lease_request_valid(LsaLeaseRequest const& request) -> bool {
    return request.magic == WKI_LSA_LEASE_MAGIC && request.version == WKI_LSA_LEASE_VERSION && request.flags == 0 &&
           request.reserved == 0 && request.token != 0;
}

// -----------------------------------------------------------------------------
// FENCE_NOTIFY Payload - 8 bytes
// -----------------------------------------------------------------------------

struct FenceNotifyPayload {
    uint16_t fenced_node;   // node that was fenced
    uint16_t fencing_node;  // node that performed the fencing
    uint32_t reason;        // 0 = heartbeat timeout, 1 = manual
} __attribute__((packed));

// ---------------------------------------------------------------------------
// PEER_GOODBYE Payload - 8 bytes
// ---------------------------------------------------------------------------

constexpr uint16_t WKI_GOODBYE_REASON_SHUTDOWN = 1;

struct PeerGoodbyePayload {
    uint16_t leaving_node;  // node that is intentionally disconnecting
    uint16_t reason;        // WKI_GOODBYE_REASON_*
    uint32_t reserved;
} __attribute__((packed));

static_assert(sizeof(PeerGoodbyePayload) == 8, "PeerGoodbyePayload must be 8 bytes");

// -----------------------------------------------------------------------------
// RECONCILE_REQ / RECONCILE_ACK Payload - 8 bytes
// -----------------------------------------------------------------------------

struct ReconcilePayload {
    uint16_t node_id;
    uint16_t num_resources;  // count of resource adverts to follow
    uint32_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// RESOURCE_ADVERT / RESOURCE_WITHDRAW Payload - variable length
// -----------------------------------------------------------------------------

enum class ResourceType : uint16_t {  // NOLINT(performance-enum-size)
    BLOCK = 1,
    CHAR = 2,
    NET = 3,
    VFS = 4,
    COMPUTE = 5,
    CUSTOM = 6,
    IPC_PIPE = 7,
    IPC_EVENTFD = 8,
    IPC_PTY = 9,
    IPC_FUTEX = 10,
    IPC_EPOLL = 11,
    IPC_SOCKET = 12,
};

// Optional suffix for resource discovery and attach messages. The suffix is
// present only for BLOCK/VFS traffic when both peers negotiated
// WKI_CAP_RESOURCE_INCARNATION; all base payload layouts remain unchanged.
struct ResourceIncarnationToken {
    uint32_t owner_boot_epoch{};
    uint32_t resource_incarnation{};
} __attribute__((packed));

static_assert(sizeof(ResourceIncarnationToken) == 8, "ResourceIncarnationToken must be 8 bytes");

constexpr auto wki_resource_type_uses_incarnation(ResourceType type) -> bool {
    return type == ResourceType::BLOCK || type == ResourceType::VFS;
}

constexpr auto wki_resource_incarnation_valid(const ResourceIncarnationToken& token) -> bool {
    return token.owner_boot_epoch != 0 && token.resource_incarnation != 0;
}

constexpr auto wki_resource_incarnation_equal(const ResourceIncarnationToken& lhs, const ResourceIncarnationToken& rhs) -> bool {
    return lhs.owner_boot_epoch == rhs.owner_boot_epoch && lhs.resource_incarnation == rhs.resource_incarnation;
}

constexpr uint8_t RESOURCE_FLAG_SHAREABLE = 0x01;
constexpr uint8_t RESOURCE_FLAG_PASSTHROUGH_CAPABLE = 0x02;
constexpr uint8_t RESOURCE_FLAG_READABLE = 0x04;
constexpr uint8_t RESOURCE_FLAG_WRITABLE = 0x08;
constexpr uint8_t RESOURCE_FLAG_OCCUPIED = 0x10;
// VFS only: OP_VFS_EXEC_VIEW version 1 PIN and idempotent CANCEL support.
// This is admission evidence; clients must still validate the complete reply.
constexpr uint8_t RESOURCE_FLAG_VFS_EXEC_VIEW = 0x20;
// VFS only: caller-identity CHMOD via OP_VFS_PATH_AS; no legacy fallback.
constexpr uint8_t RESOURCE_FLAG_VFS_CHMOD_IDENTITY = 0x40;
constexpr uint8_t RESOURCE_FLAG_VFS_UTIMENS_IDENTITY = 0x80;

struct ResourceAdvertPayload {
    uint16_t node_id;        // owner node
    uint16_t resource_type;  // ResourceType enum
    uint32_t resource_id;    // unique on owning node
    uint8_t flags;           // RESOURCE_FLAG_*
    uint8_t name_len;
    // Followed by name_len bytes of name (e.g., "sda", "eth0")
} __attribute__((packed));

static_assert(sizeof(ResourceAdvertPayload) == 10, "ResourceAdvertPayload base must stay 10 bytes");
static_assert(offsetof(ResourceAdvertPayload, name_len) == 9, "ResourceAdvertPayload base offsets must stay wire-compatible");

struct ResourceAdvertNetPayload {
    uint16_t node_id{};        // owner node
    uint16_t resource_type{};  // ResourceType::NET
    uint32_t resource_id{};    // unique on owning node
    uint8_t flags{};           // RESOURCE_FLAG_*
    uint8_t name_len{};
    uint16_t reserved{};
    uint32_t ipv4_addr{};        // owner NIC IPv4 address (host byte order)
    uint32_t ipv4_mask{};        // owner NIC IPv4 mask (host byte order)
    proto::MacAddress real_mac;  // owner NIC real MAC
    uint16_t link_state{};       // 0=DOWN, 1=UP
    uint32_t mtu{};              // owner NIC MTU
    // Followed by name_len bytes of name (e.g., "eth0")
} __attribute__((packed));

static_assert(sizeof(ResourceAdvertNetPayload) == 32, "ResourceAdvertNetPayload must be 32 bytes");

constexpr uint16_t WKI_NET_IPV6_STATE_VERSION_1 = 1;
constexpr size_t WKI_NET_IPV6_STATE_MAX_ADDRS = 8;

struct NetIpv6StateEntry {
    std::array<uint8_t, 16> address{};
    uint8_t prefix_len{};
    uint8_t scope{};
    uint16_t flags{};
} __attribute__((packed));

static_assert(sizeof(NetIpv6StateEntry) == 20, "NetIpv6StateEntry must be 20 bytes");

struct NetIpv6StateSuffix {
    uint16_t version{};
    uint16_t length{};
    uint8_t count{};
    std::array<uint8_t, 3> reserved{};
    std::array<NetIpv6StateEntry, WKI_NET_IPV6_STATE_MAX_ADDRS> entries{};
} __attribute__((packed));

static_assert(sizeof(NetIpv6StateSuffix) == 168, "NetIpv6StateSuffix must be 168 bytes");
static_assert(offsetof(NetIpv6StateSuffix, entries) == 8, "NetIpv6StateSuffix entries offset must stay wire-compatible");

constexpr uint16_t WKI_NET_IPV6_ADDR_F_DADFAILED = 0x0008;
constexpr uint16_t WKI_NET_IPV6_ADDR_F_DEPRECATED = 0x0020;
constexpr uint16_t WKI_NET_IPV6_ADDR_F_TENTATIVE = 0x0040;
constexpr uint16_t WKI_NET_IPV6_ADDR_F_PERMANENT = 0x0080;
constexpr uint16_t WKI_NET_IPV6_ADDR_F_NOPREFIXROUTE = 0x0200;
constexpr uint16_t WKI_NET_IPV6_ADDR_STATE_MASK =
    WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_DEPRECATED | WKI_NET_IPV6_ADDR_F_TENTATIVE;
constexpr uint16_t WKI_NET_IPV6_ADDR_KNOWN_FLAGS =
    WKI_NET_IPV6_ADDR_STATE_MASK | WKI_NET_IPV6_ADDR_F_PERMANENT | WKI_NET_IPV6_ADDR_F_NOPREFIXROUTE;

constexpr auto wki_net_ipv6_address_scope(const std::array<uint8_t, 16>& address) -> uint8_t {
    bool loopback = address.back() == 1;
    for (size_t i = 0; i + 1 < address.size(); ++i) {
        loopback = loopback && address.at(i) == 0;
    }
    if (loopback) {
        return 254;
    }
    if (address.at(0) == 0xFE && (address.at(1) & 0xC0U) == 0x80U) {
        return 253;
    }
    return 0;
}

constexpr auto wki_net_ipv6_state_valid(const NetIpv6StateSuffix& state) -> bool {
    if (state.version != WKI_NET_IPV6_STATE_VERSION_1 || state.length != sizeof(NetIpv6StateSuffix) ||
        state.count > WKI_NET_IPV6_STATE_MAX_ADDRS || state.reserved != std::array<uint8_t, 3>{}) {
        return false;
    }
    for (size_t i = 0; i < state.count; ++i) {
        auto const& entry = state.entries.at(i);
        uint16_t const STATE_FLAGS = entry.flags & WKI_NET_IPV6_ADDR_STATE_MASK;
        bool nonzero = false;
        for (uint8_t byte : entry.address) {
            nonzero = nonzero || byte != 0;
        }
        if ((entry.flags & ~WKI_NET_IPV6_ADDR_KNOWN_FLAGS) != 0 ||
            (STATE_FLAGS != 0 && (STATE_FLAGS & static_cast<uint16_t>(STATE_FLAGS - 1)) != 0) || !nonzero ||
            entry.address.front() == 0xFF || entry.prefix_len > 128 || entry.scope != wki_net_ipv6_address_scope(entry.address)) {
            return false;
        }
    }
    for (size_t i = state.count; i < state.entries.size(); ++i) {
        if (state.entries.at(i).address != std::array<uint8_t, 16>{} || state.entries.at(i).prefix_len != 0 ||
            state.entries.at(i).scope != 0 || state.entries.at(i).flags != 0) {
            return false;
        }
    }
    return true;
}

constexpr auto wki_net_ipv6_state_has_usable_address(const NetIpv6StateSuffix& state) -> bool {
    if (!wki_net_ipv6_state_valid(state)) {
        return false;
    }
    for (size_t i = 0; i < state.count; ++i) {
        auto const& entry = state.entries.at(i);
        if ((entry.flags & (WKI_NET_IPV6_ADDR_F_DADFAILED | WKI_NET_IPV6_ADDR_F_TENTATIVE)) == 0) {
            return true;
        }
    }
    return false;
}

constexpr auto wki_net_ipv6_extended_length_valid(bool negotiated, size_t actual_length, size_t legacy_length) -> bool {
    return actual_length == legacy_length + (negotiated ? sizeof(NetIpv6StateSuffix) : 0);
}

constexpr auto wki_net_ipv6_state_empty() -> NetIpv6StateSuffix {
    NetIpv6StateSuffix state{};
    state.version = WKI_NET_IPV6_STATE_VERSION_1;
    state.length = sizeof(NetIpv6StateSuffix);
    return state;
}

inline auto resource_advert_name(ResourceAdvertPayload* p) -> char* {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(p) + sizeof(ResourceAdvertPayload));
}

inline auto resource_advert_name(const ResourceAdvertPayload* p) -> const char* {
    return reinterpret_cast<const char*>(reinterpret_cast<const uint8_t*>(p) + sizeof(ResourceAdvertPayload));
}

inline auto resource_advert_name(ResourceAdvertNetPayload* p) -> char* {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(p) + sizeof(ResourceAdvertNetPayload));
}

inline auto resource_advert_name(const ResourceAdvertNetPayload* p) -> const char* {
    return reinterpret_cast<const char*>(reinterpret_cast<const uint8_t*>(p) + sizeof(ResourceAdvertNetPayload));
}

// -----------------------------------------------------------------------------
// ZONE_CREATE_REQ Payload - 16 bytes
// -----------------------------------------------------------------------------

// Access policy bits
constexpr uint8_t ZONE_ACCESS_LOCAL_READ = 0x01;
constexpr uint8_t ZONE_ACCESS_LOCAL_WRITE = 0x02;
constexpr uint8_t ZONE_ACCESS_REMOTE_READ = 0x04;
constexpr uint8_t ZONE_ACCESS_REMOTE_WRITE = 0x08;

// Notification modes
enum class ZoneNotifyMode : uint8_t {
    NONE = 0,
    PRE_ONLY = 1,
    POST_ONLY = 2,
    PRE_AND_POST = 3,
};

// Zone type hints
enum class ZoneTypeHint : uint8_t {
    BUFFER = 0,
    MSG_QUEUE = 1,
    LOCK = 2,
    CUSTOM = 3,
};

struct ZoneCreateReqPayload {
    uint32_t zone_id;
    uint32_t size;           // bytes, must be page-aligned
    uint8_t access_policy;   // ZONE_ACCESS_* bits
    uint8_t notify_mode;     // ZoneNotifyMode
    uint8_t zone_type_hint;  // ZoneTypeHint
    uint8_t reserved1;
    uint32_t reserved2;
} __attribute__((packed));

static_assert(sizeof(ZoneCreateReqPayload) == 16, "ZoneCreateReqPayload must be 16 bytes");

// -----------------------------------------------------------------------------
// ZONE_CREATE_ACK Payload - 24 bytes
// -----------------------------------------------------------------------------

enum class ZoneCreateStatus : uint8_t {
    ACCEPTED = 0,
    REJECTED_NO_MEM = 1,
    REJECTED_POLICY = 2,
};

struct ZoneCreateAckPayload {
    uint32_t zone_id;
    uint8_t status;  // ZoneCreateStatus
    std::array<uint8_t, 3> reserved1;
    uint64_t phys_addr;  // physical address for RDMA mapping
    uint32_t rkey;       // RDMA remote key
    uint32_t reserved2;
} __attribute__((packed));

static_assert(sizeof(ZoneCreateAckPayload) == 24, "ZoneCreateAckPayload must be 24 bytes");

// -----------------------------------------------------------------------------
// ZONE_DESTROY Payload - 8 bytes
// -----------------------------------------------------------------------------

struct ZoneDestroyPayload {
    uint32_t zone_id;
    uint32_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// ZONE_NOTIFY_PRE / ZONE_NOTIFY_POST Payload - 16 bytes
// -----------------------------------------------------------------------------

struct ZoneNotifyPayload {
    uint32_t zone_id;
    uint32_t offset;  // region offset being accessed
    uint32_t length;  // length of the access
    uint8_t op_type;  // 0 = READ, 1 = WRITE
    std::array<uint8_t, 3> reserved;
} __attribute__((packed));

static_assert(sizeof(ZoneNotifyPayload) == 16, "ZoneNotifyPayload must be 16 bytes");

// ZONE_NOTIFY_PRE_ACK / ZONE_NOTIFY_POST_ACK Payload - 4 bytes
struct ZoneNotifyAckPayload {
    uint32_t zone_id;
} __attribute__((packed));

static_assert(sizeof(ZoneNotifyAckPayload) == 4, "ZoneNotifyAckPayload must be 4 bytes");

// -----------------------------------------------------------------------------
// ZONE_READ_REQ Payload - 16 bytes
// -----------------------------------------------------------------------------

struct ZoneReadReqPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    uint32_t op_cookie;
} __attribute__((packed));

static_assert(sizeof(ZoneReadReqPayload) == 16, "ZoneReadReqPayload must be 16 bytes");

// -----------------------------------------------------------------------------
// ZONE_READ_RESP / ZONE_WRITE_REQ - variable length
// Data follows immediately after the fixed portion
// -----------------------------------------------------------------------------

struct ZoneReadRespPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    uint32_t op_cookie;
    // Followed by `length` bytes of data
} __attribute__((packed));

static_assert(sizeof(ZoneReadRespPayload) == 16, "ZoneReadRespPayload must be 16 bytes");

struct ZoneWriteReqPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    uint32_t op_cookie;
    // Followed by `length` bytes of data
} __attribute__((packed));

static_assert(sizeof(ZoneWriteReqPayload) == 16, "ZoneWriteReqPayload must be 16 bytes");

struct ZoneWriteAckPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    int32_t status;  // 0 = success, negative = error
    uint32_t op_cookie;
} __attribute__((packed));

static_assert(sizeof(ZoneWriteAckPayload) == 20, "ZoneWriteAckPayload must be 20 bytes");

// -----------------------------------------------------------------------------
// EVENT_SUBSCRIBE / EVENT_UNSUBSCRIBE Payload - 8 bytes
// -----------------------------------------------------------------------------

constexpr uint8_t EVENT_DELIVERY_RELIABLE = 0;
constexpr uint8_t EVENT_DELIVERY_BEST_EFFORT = 1;

struct EventSubscribePayload {
    uint16_t event_class;   // 0xFFFF = all classes
    uint16_t event_id;      // 0xFFFF = all in class
    uint8_t delivery_mode;  // EVENT_DELIVERY_*
    std::array<uint8_t, 3> reserved;
} __attribute__((packed));

static_assert(sizeof(EventSubscribePayload) == 8, "EventSubscribePayload must be 8 bytes");

// -----------------------------------------------------------------------------
// EVENT_PUBLISH Payload - variable length
// -----------------------------------------------------------------------------

// Well-known event classes
constexpr uint16_t EVENT_CLASS_SYSTEM = 0x0001;
constexpr uint16_t EVENT_CLASS_MEMORY = 0x0002;
constexpr uint16_t EVENT_CLASS_SCHEDULER = 0x0003;
constexpr uint16_t EVENT_CLASS_DEVICE = 0x0004;
constexpr uint16_t EVENT_CLASS_STORAGE = 0x0005;
constexpr uint16_t EVENT_CLASS_ZONE = 0x0006;
constexpr uint16_t EVENT_CLASS_CUSTOM = 0x8000;

struct EventPublishPayload {
    uint16_t event_class;
    uint16_t event_id;
    uint16_t origin_node;
    uint16_t data_len;
    // Followed by data_len bytes of event-specific data
} __attribute__((packed));

static_assert(sizeof(EventPublishPayload) == 8, "EventPublishPayload must be 8 bytes");

// EVENT_ACK - just echoes the event identity
struct EventAckPayload {
    uint16_t event_class;
    uint16_t event_id;
    uint16_t origin_node;
    uint16_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// DEV_ATTACH_REQ Payload - 12 bytes
// -----------------------------------------------------------------------------

enum class AttachMode : uint8_t {
    PROXY = 0,
    PASSTHROUGH = 1,
};

constexpr uint8_t DEV_ATTACH_MODE_KIND_MASK = 0x0F;
constexpr uint8_t DEV_ATTACH_ACCESS_READ = 0x10;
constexpr uint8_t DEV_ATTACH_ACCESS_WRITE = 0x20;
constexpr uint8_t DEV_ATTACH_ACCESS_MASK = DEV_ATTACH_ACCESS_READ | DEV_ATTACH_ACCESS_WRITE;
// The consumer can keep a dedicated RPC attachment without asking the owner to
// reserve per-binding RDMA staging regions. This preserves the fixed attach
// payload layout and leaves the request usable by older peers, which simply
// ignore unknown high bits.
constexpr uint8_t DEV_ATTACH_DISABLE_RDMA = 0x40;
// Capability-gated auxiliary VFS-lane identity. Older peers ignore this high
// bit and continue to infer the anchor from DEV_ATTACH_DISABLE_RDMA.
constexpr uint8_t DEV_ATTACH_VFS_AUX_LANE = 0x80;

constexpr auto wki_vfs_proxy_attach_mode(bool lane_anchor, bool request_rdma) -> uint8_t {
    uint8_t mode = static_cast<uint8_t>(AttachMode::PROXY);
    if (!lane_anchor) {
        mode |= DEV_ATTACH_VFS_AUX_LANE;
    }
    if (!request_rdma) {
        mode |= DEV_ATTACH_DISABLE_RDMA;
    }
    return mode;
}

constexpr auto wki_vfs_attach_lane_is_anchor(uint8_t mode, bool explicit_aux_lane_negotiated) -> bool {
    if (explicit_aux_lane_negotiated) {
        return (mode & DEV_ATTACH_VFS_AUX_LANE) == 0;
    }
    return (mode & DEV_ATTACH_DISABLE_RDMA) == 0;
}

constexpr auto dev_attach_mode_kind(uint8_t mode) -> AttachMode { return static_cast<AttachMode>(mode & DEV_ATTACH_MODE_KIND_MASK); }

constexpr auto dev_attach_requests_read(uint8_t mode) -> bool {
    uint8_t const ACCESS = mode & DEV_ATTACH_ACCESS_MASK;
    return ACCESS == 0 || (ACCESS & DEV_ATTACH_ACCESS_READ) != 0;
}

constexpr auto dev_attach_requests_write(uint8_t mode) -> bool {
    uint8_t const ACCESS = mode & DEV_ATTACH_ACCESS_MASK;
    return ACCESS == 0 || (ACCESS & DEV_ATTACH_ACCESS_WRITE) != 0;
}

struct DevAttachReqPayload {
    uint16_t target_node;
    uint16_t resource_type;  // ResourceType enum
    uint32_t resource_id;
    uint8_t attach_mode;  // AttachMode
    uint8_t attach_cookie;
    uint16_t requested_channel;  // 0 = auto-assign
} __attribute__((packed));

static_assert(sizeof(DevAttachReqPayload) == 12, "DevAttachReqPayload must be 12 bytes");

// -----------------------------------------------------------------------------
// DEV_ATTACH_ACK Payload - 16 bytes
// -----------------------------------------------------------------------------

enum class DevAttachStatus : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    NOT_REMOTABLE = 2,
    BUSY = 3,
    NO_PASSTHROUGH = 4,
    STALE_RESOURCE = 5,
    ACCESS_DENIED = 6,
};

struct DevAttachAckPayload {
    uint8_t status;  // DevAttachStatus
    uint8_t reserved;
    uint16_t assigned_channel;
    uint32_t resource_id;             // echoed from DEV_ATTACH_REQ for consumer-side matching
    uint16_t max_op_size;             // max payload size for DEV_OP_REQ
    uint16_t rdma_flags;              // bit 0: RDMA block ring zone available
    uint32_t blk_zone_id;             // RDMA zone ID for block ring; for VFS: server write-recv rkey
    uint32_t rdma_read_staging_rkey;  // DEV_ATTACH_RDMA_VFS_READ: server-side read staging rkey (RoCE pull mode)
    uint32_t rdma_bulk_staging_rkey;  // DEV_ATTACH_RDMA_BULK_PULL: server-side bulk staging rkey (RoCE pull mode)
} __attribute__((packed));

static_assert(sizeof(DevAttachAckPayload) == 24, "DevAttachAckPayload must be 24 bytes");

constexpr auto wki_dev_attach_ack_matches_expected(uint8_t expected_cookie, const DevAttachAckPayload& ack) -> bool {
    return ack.reserved == expected_cookie;
}

// V2: Extended attach ACK for NET resources - includes owner NIC info [V2 A5.3]
struct DevAttachAckNetPayload {
    // V1 base fields (identical layout to DevAttachAckPayload)
    uint8_t status{};
    uint8_t attach_cookie{};
    uint16_t assigned_channel{};
    uint32_t resource_id{};
    uint16_t max_op_size{};
    uint16_t rdma_flags{};
    uint32_t blk_zone_id{};  // unused for NET, kept for layout compatibility
    // V2 NET extension fields
    uint32_t ipv4_addr{};        // Owner NIC's IPv4 address (network byte order)
    uint32_t ipv4_mask{};        // Owner NIC's IPv4 subnet mask
    proto::MacAddress real_mac;  // Owner NIC's real MAC address
    uint16_t link_state{};       // 0=DOWN, 1=UP
    uint32_t mtu{};              // Owner NIC MTU
} __attribute__((packed));

static_assert(sizeof(DevAttachAckNetPayload) == 36, "DevAttachAckNetPayload must be 36 bytes");

struct NetStateNotifyPayload {
    uint32_t ipv4_addr{};        // Owner NIC's IPv4 address (network byte order)
    uint32_t ipv4_mask{};        // Owner NIC's IPv4 subnet mask
    proto::MacAddress real_mac;  // Owner NIC's real MAC address
    uint16_t link_state{};       // 0=DOWN, 1=UP
    uint32_t mtu{};              // Owner NIC MTU
} __attribute__((packed));

static_assert(sizeof(NetStateNotifyPayload) == 20, "NetStateNotifyPayload must be 20 bytes");

constexpr uint32_t WKI_NET_NOTIFY_MAGIC = 0x314E4E57;  // "WNN1" little-endian

struct NetNotifyHeader {
    uint32_t magic{};
    uint8_t attach_cookie{};
    uint8_t reserved{};
    uint16_t data_len{};
} __attribute__((packed));

static_assert(sizeof(NetNotifyHeader) == 8, "NetNotifyHeader must be 8 bytes");

constexpr auto wki_net_notify_header_matches_expected(uint8_t expected_cookie, const NetNotifyHeader& notify) -> bool {
    return notify.magic == WKI_NET_NOTIFY_MAGIC && notify.attach_cookie == expected_cookie;
}

constexpr auto wki_net_notify_payload_fits(uint16_t payload_len, const NetNotifyHeader& notify) -> bool {
    return payload_len >= sizeof(NetNotifyHeader) && notify.data_len <= payload_len - sizeof(NetNotifyHeader);
}

// RDMA flags for DevAttachAckPayload
constexpr uint16_t DEV_ATTACH_RDMA_BLK_RING = 0x0001;   // block ring RDMA zone available
constexpr uint16_t DEV_ATTACH_RDMA_VFS = 0x0002;        // VFS RDMA available; blk_zone_id carries server write-recv rkey
constexpr uint16_t DEV_ATTACH_RDMA_BULK = 0x0004;       // bulk RDMA transfer supported (large sequential I/O)
constexpr uint16_t DEV_ATTACH_RDMA_VFS_READ = 0x0008;   // VFS read staging buf available (pull mode); rdma_read_staging_rkey valid
constexpr uint16_t DEV_ATTACH_RDMA_BULK_PULL = 0x0010;  // VFS bulk staging buf available (pull mode); rdma_bulk_staging_rkey valid
constexpr uint16_t DEV_ATTACH_READ_ONLY = 0x0020;       // attached device accepts reads but rejects writes
// NET-specific attach feature. Old NET owners advertise zero in this field.
constexpr uint16_t DEV_ATTACH_NET_OPEN_IDENTITY = 0x0040;
constexpr uint16_t DEV_ATTACH_NET_CLOSE_IDENTITY = 0x0080;
constexpr uint16_t DEV_ATTACH_NET_LINK_IDENTITY = 0x0100;

// -----------------------------------------------------------------------------
// DEV_DETACH Payload - 8 bytes
// -----------------------------------------------------------------------------

struct DevDetachPayload {
    uint16_t target_node;
    uint16_t resource_type;
    uint32_t resource_id;
} __attribute__((packed));

static_assert(sizeof(DevDetachPayload) == 8, "DevDetachPayload must be 8 bytes");

constexpr uint16_t WKI_DEV_DETACH_COOKIE_BYTES = sizeof(uint8_t);
constexpr uint16_t WKI_DEV_DETACH_INCARNATION_BYTES = sizeof(ResourceIncarnationToken);
constexpr size_t WKI_DEV_DETACH_COOKIE_OFFSET = sizeof(DevDetachPayload);
constexpr size_t WKI_DEV_DETACH_INCARNATION_OFFSET = WKI_DEV_DETACH_COOKIE_OFFSET + WKI_DEV_DETACH_COOKIE_BYTES;

constexpr auto wki_dev_detach_payload_size(bool with_incarnation) -> uint16_t {
    return static_cast<uint16_t>(sizeof(DevDetachPayload) + WKI_DEV_DETACH_COOKIE_BYTES +
                                 (with_incarnation ? WKI_DEV_DETACH_INCARNATION_BYTES : 0));
}

constexpr auto wki_dev_detach_payload_size_matches(uint16_t payload_len, bool with_incarnation) -> bool {
    return payload_len == wki_dev_detach_payload_size(with_incarnation);
}

constexpr auto wki_dev_detach_cookie_from_payload(const uint8_t* payload, uint16_t payload_len) -> uint8_t {
    return (payload != nullptr && payload_len >= wki_dev_detach_payload_size(false)) ? payload[WKI_DEV_DETACH_COOKIE_OFFSET] : 0;
}

constexpr auto wki_dev_detach_matches_binding(uint16_t binding_consumer_node, ResourceType binding_resource_type,
                                              uint32_t binding_resource_id, uint16_t request_src_node, const DevDetachPayload& detach)
    -> bool {
    return binding_consumer_node == request_src_node && binding_resource_type == static_cast<ResourceType>(detach.resource_type) &&
           binding_resource_id == detach.resource_id;
}

constexpr auto wki_dev_detach_cookie_matches_binding(uint8_t binding_attach_cookie, uint8_t request_cookie) -> bool {
    return binding_attach_cookie == request_cookie;
}

constexpr auto wki_dev_detach_incarnation_matches_binding(const ResourceIncarnationToken& binding_incarnation,
                                                          const ResourceIncarnationToken& request_incarnation, bool incarnation_required)
    -> bool {
    return !incarnation_required || (wki_resource_incarnation_valid(request_incarnation) &&
                                     wki_resource_incarnation_equal(binding_incarnation, request_incarnation));
}

// -----------------------------------------------------------------------------
// DEV_OP_REQ / DEV_OP_RESP Payload - variable length
// -----------------------------------------------------------------------------

struct DevOpReqPayload {
    uint16_t op_id;
    uint16_t data_len;
    // Followed by data_len bytes of marshaled request data
} __attribute__((packed));

struct DevOpRespPayload {
    uint16_t op_id;
    int16_t status;  // 0 = success, negative = error
    uint16_t data_len;
    uint16_t reserved;
    // Followed by data_len bytes of marshaled response data
} __attribute__((packed));

constexpr auto wki_dev_op_response_matches_expected(uint16_t expected_op_id, uint16_t expected_seq, const DevOpRespPayload& resp) -> bool {
    return resp.op_id == expected_op_id && resp.reserved == expected_seq;
}

// Well-known operation IDs for default marshaling
constexpr uint16_t OP_BLOCK_READ = 0x0100;
constexpr uint16_t OP_BLOCK_WRITE = 0x0101;
constexpr uint16_t OP_BLOCK_FLUSH = 0x0102;
constexpr uint16_t OP_BLOCK_SECURE_PREPARE = 0x0106;  // Independent secure BLOCK private-region preparation.
constexpr uint16_t OP_BLOCK_BULK_READ = 0x0104;       // Streaming bulk transfer: server RDMA-writes entire range to consumer buffer
constexpr uint16_t OP_BLOCK_BULK_WRITE = 0x0105;      // Streaming bulk transfer: server RDMA-reads entire range from consumer buffer

constexpr uint16_t OP_CHAR_OPEN = 0x0200;
constexpr uint16_t OP_CHAR_CLOSE = 0x0201;
constexpr uint16_t OP_CHAR_READ = 0x0202;
constexpr uint16_t OP_CHAR_WRITE = 0x0203;

constexpr uint16_t OP_NET_XMIT = 0x0300;
constexpr uint16_t OP_NET_SET_MAC = 0x0301;
constexpr uint16_t OP_NET_RX_NOTIFY = 0x0302;
constexpr uint16_t OP_NET_GET_STATS = 0x0303;
constexpr uint16_t OP_NET_OPEN = 0x0304;          // V2: bring up remote NIC [V2 A5.5]
constexpr uint16_t OP_NET_CLOSE = 0x0305;         // V2: shut down remote NIC [V2 A5.5]
constexpr uint16_t OP_NET_RX_CREDIT = 0x0306;     // V2: replenish RX credits [V2 A5.6]
constexpr uint16_t OP_NET_STATE_NOTIFY = 0x0307;  // V3: owner-pushed NIC state update
constexpr uint16_t OP_NET_OPEN_AS = 0x0308;
constexpr uint16_t OP_NET_CLOSE_AS = 0x0309;
constexpr uint16_t OP_NET_LINK_AS = 0x030A;
constexpr uint16_t WKI_NET_OPEN_IDENTITY_VERSION = 1;
constexpr size_t WKI_NET_OPEN_CALLER_SIZE = 80;
struct NetOpenIdentityHeader {
    uint16_t cookie{};
    uint16_t version{};
    uint32_t reserved{};
} __attribute__((packed));
static_assert(sizeof(NetOpenIdentityHeader) == 8);
constexpr size_t WKI_NET_OPEN_IDENTITY_SIZE = sizeof(NetOpenIdentityHeader) + WKI_NET_OPEN_CALLER_SIZE;
// CLOSE uses the same canonical caller framing, negotiated independently.
using NetCloseIdentityHeader = NetOpenIdentityHeader;
constexpr uint16_t WKI_NET_CLOSE_IDENTITY_VERSION = WKI_NET_OPEN_IDENTITY_VERSION;
constexpr size_t WKI_NET_CLOSE_IDENTITY_SIZE = WKI_NET_OPEN_IDENTITY_SIZE;
constexpr uint16_t WKI_NET_LINK_STATE = 1;
constexpr uint16_t WKI_NET_LINK_MAC = 2;
struct NetLinkControlPayload {
    uint16_t fields{};
    uint16_t reserved{};
    uint32_t up{};
    uint8_t mac[6]{};
    uint16_t reserved_tail{};
} __attribute__((packed));
static_assert(sizeof(NetLinkControlPayload) == 16);
constexpr size_t WKI_NET_LINK_IDENTITY_SIZE = WKI_NET_OPEN_IDENTITY_SIZE + sizeof(NetLinkControlPayload);

constexpr uint16_t OP_VFS_OPEN = 0x0400;
constexpr uint16_t OP_VFS_READ = 0x0401;
constexpr uint16_t OP_VFS_WRITE = 0x0402;
constexpr uint16_t OP_VFS_CLOSE = 0x0403;
// The optional fifth byte extends the legacy fd-only body without changing its
// prefix. Receivers ignore unknown flag bits; an absent marker requires a reply.
constexpr size_t WKI_VFS_CLOSE_LEGACY_DATA_LEN = sizeof(int32_t);
constexpr size_t WKI_VFS_CLOSE_FLAGS_OFFSET = WKI_VFS_CLOSE_LEGACY_DATA_LEN;
constexpr size_t WKI_VFS_CLOSE_EXTENDED_DATA_LEN = WKI_VFS_CLOSE_FLAGS_OFFSET + sizeof(uint8_t);
constexpr uint8_t WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE = 0x01;
static_assert(WKI_VFS_CLOSE_LEGACY_DATA_LEN == 4 && WKI_VFS_CLOSE_EXTENDED_DATA_LEN == 5);

constexpr auto wki_vfs_close_no_success_response_requested(const uint8_t* data, uint16_t data_len) -> bool {
    return data != nullptr && data_len >= WKI_VFS_CLOSE_EXTENDED_DATA_LEN &&
           (data[WKI_VFS_CLOSE_FLAGS_OFFSET] & WKI_VFS_CLOSE_FLAG_NO_SUCCESS_RESPONSE) != 0;
}

constexpr uint16_t OP_VFS_READDIR = 0x0404;
constexpr uint16_t OP_VFS_STAT = 0x0405;
constexpr uint16_t OP_VFS_MKDIR = 0x0406;
constexpr uint16_t OP_VFS_READLINK = 0x0407;  // D8: symlink target resolution
constexpr uint16_t OP_VFS_SYMLINK = 0x0408;   // D8: symlink creation
constexpr uint16_t OP_VFS_UNLINK = 0x0409;    // V2: file deletion [V2 A9.1]
constexpr uint16_t OP_VFS_RMDIR = 0x040A;     // V2: directory removal [V2 A9.1]
constexpr uint16_t OP_VFS_RENAME = 0x040B;    // V2: rename/move [V2 A9.1]
constexpr uint16_t OP_VFS_FSYNC = 0x040C;     // V2: flush to disk [V2 A9.1]
constexpr uint16_t OP_VFS_TRUNCATE = 0x040D;  // V2: truncate file [V2 A9.1]
constexpr uint16_t OP_VFS_SEEK_END = 0x040E;  // V2: seek from end [V2 A9.1]
constexpr uint16_t OP_VFS_CHMOD = 0x040F;     // Request: {mode:u32, flags:u8, path_len:u16, path[path_len]}
constexpr uint8_t WKI_VFS_CHMOD_FLAG_FOLLOW_FINAL_SYMLINK = 0x01;
constexpr uint16_t OP_VFS_READ_RDMA =
    0x0410;  // RDMA read:  req={fd:i32,len:u32,off:i64,rkey:u32}(20B) resp={bytes:u32}(4B), data pushed via rdma_write
constexpr uint16_t OP_VFS_WRITE_RDMA =
    0x0411;  // RDMA write: consumer rdma_write to server buf first, req={fd:i32,off:i64,len:u32}(16B) resp={bytes:u32}(4B)
constexpr uint16_t OP_VFS_READDIR_BATCH =
    0x0412;  // Batch readdir: req={fd:i32,start:u32,max:u32}(12B) resp={count:u32,entries:DirEntry[count]}
constexpr uint16_t OP_VFS_READ_BULK =
    0x0413;  // Bulk RDMA read: req={fd:i32,len:u32,off:i64,bulk_rkey:u32}(20B) resp={bytes:u32}(4B), up to 2 MB via rdma_write
constexpr uint16_t OP_VFS_INVALIDATE =
    0x0414;  // Owner->consumer notify: req={old_len:u16,new_len:u16,paths...}; no DEV_OP_RESP is expected
constexpr uint16_t OP_VFS_UTIMENS = 0x0415;
constexpr uint8_t WKI_VFS_UTIMENS_FLAG_FOLLOW_FINAL_SYMLINK = 0x01;
constexpr uint8_t WKI_VFS_UTIMENS_FLAG_TIMES_PRESENT = 0x02;

struct VfsUtimensReqPrefix {
    int64_t atime_sec;
    int64_t atime_nsec;
    int64_t mtime_sec;
    int64_t mtime_nsec;
    uint16_t path_len;
    uint8_t flags;
    uint8_t reserved;
} __attribute__((packed));

static_assert(sizeof(VfsUtimensReqPrefix) == 36);

constexpr uint16_t OP_VFS_METADATA_BATCH = 0x0416;
constexpr uint16_t VFS_METADATA_BATCH_VERSION = 1;
constexpr uint16_t VFS_METADATA_BATCH_STAT_VERSION = 2;
constexpr uint16_t VFS_METADATA_BATCH_MUTATION_VERSION = 3;
constexpr uint8_t VFS_METADATA_BATCH_MAX_ITEMS = 64;
// A stat response carries {status:i32, Stat:144} per item. Sixty such records,
// the batch header, and DevOpRespPayload fit in one WKI Ethernet payload.
constexpr uint8_t VFS_METADATA_BATCH_MAX_STAT_ITEMS = 60;
constexpr uint16_t VFS_METADATA_BATCH_MAX_PATH_LEN = 511;

enum class VfsMetadataBatchOperation : uint8_t {
    INVALID = 0,
    CREATE_CLOSE = 1,
    STAT_FOLLOW = 2,
    UNLINK = 3,
    RENAME = 4,
};

struct VfsMetadataBatchHeader {
    uint16_t version;
    VfsMetadataBatchOperation operation;
    uint8_t count;
    uint32_t mode;
} __attribute__((packed));

static_assert(sizeof(VfsMetadataBatchHeader) == 8);

constexpr uint16_t OP_VFS_XATTR = 0x0417;
// Credential record v1 followed by read-only OPEN fields; no prefetch suffix.
constexpr uint16_t OP_VFS_OPEN_READ_AS = 0x0418;
// Credential record v1 followed by ordinary OPEN fields and optional prefetch.
constexpr uint16_t OP_VFS_OPEN_AS = 0x0419;
// Request: remote_fd:i32 (4 bytes). Successful response: unchanged Stat (144 bytes).
constexpr uint16_t OP_VFS_FSTAT = 0x041a;
// {credential_record[80], flags:u8, reserved:u8=0, path_len:u16, path[N]}
constexpr uint16_t OP_VFS_STAT_AS = 0x041b;
constexpr uint8_t WKI_VFS_STAT_FOLLOW = 1;
constexpr uint8_t WKI_VFS_STAT_REQUIRE_DIRECTORY = 2;
// {credential_record[80], VfsPathIdentityHeader, operation_data[N]}.
// Success replies contain {VfsPathIdentityHeader, operation_result[N]}; errors
// have no body. Each new selector needs explicit policy and owner handling.
constexpr uint16_t OP_VFS_PATH_AS = 0x041c;
enum class VfsPathOperation : uint16_t {
    READLINK = 1,
    READLINK_AT = 2,
    STAT_AT = 3,
    OPEN_AT = 4,
    REALPATH = 5,
    CHMOD = 6,
    UTIMENS = 7,
    MKDIR = 8,
    SYMLINK = 9,
    REMOVE = 10,
    DIRECTORY_BINDING = 11,
    RENAME = 12,
    REALPATH_AT = 13,
    FCHMOD = 14,
    FCHOWN = 15,
    FUTIMENS = 16,
    CHOWN = 17
};
constexpr uint16_t WKI_VFS_PATH_REQUIRE_DIRECTORY = 1;
constexpr uint16_t WKI_VFS_PATH_FOLLOW_FINAL = 2;
// Descriptor selectors carry {directory_fd:i32, relative_path[N]} as operation data.
// OPEN_AT inserts {flags:u32, mode:u32} between the directory FD and path.
// REALPATH_AT uses the descriptor prefix above; REALPATH carries only a raw
// export-relative path. Both return canonical export-relative
// text without a NUL (0..511 bytes; empty denotes the export root).
struct VfsPathIdentityHeader {
    VfsPathOperation operation;
    uint16_t flags;
    uint16_t data_len;
    uint16_t reserved;
} __attribute__((packed));
static_assert(sizeof(VfsPathIdentityHeader) == 8);
// Descriptor metadata has fixed operation data, no path, and no flags.
// The positive FD selects a File admitted on this exact export binding.
struct VfsFileChmodRequest {
    int32_t fd;
    uint32_t mode;
} __attribute__((packed));
struct VfsFileChownRequest {
    int32_t fd;
    uint32_t owner;
    uint32_t group;
} __attribute__((packed));
struct VfsFileUtimensRequest {
    int32_t fd;
    uint32_t reserved;
    int64_t atime_sec;
    int64_t atime_nsec;
    int64_t mtime_sec;
    int64_t mtime_nsec;
} __attribute__((packed));
static_assert(sizeof(VfsFileChmodRequest) == 8);
static_assert(sizeof(VfsFileChownRequest) == 12);
static_assert(sizeof(VfsFileUtimensRequest) == 40);
constexpr auto wki_vfs_is_file_metadata(VfsPathOperation operation) -> bool {
    return operation == VfsPathOperation::FCHMOD || operation == VfsPathOperation::FCHOWN || operation == VfsPathOperation::FUTIMENS;
}
constexpr auto wki_vfs_file_metadata_header_valid(const VfsPathIdentityHeader& header, VfsPathOperation operation, size_t expected,
                                                  size_t length) -> bool {
    return header.operation == operation && header.flags == 0 && header.reserved == 0 && header.data_len == expected && length == expected;
}
constexpr auto wki_vfs_fchmod_request_valid(const VfsPathIdentityHeader& header, const VfsFileChmodRequest& request, size_t length)
    -> bool {
    return wki_vfs_file_metadata_header_valid(header, VfsPathOperation::FCHMOD, sizeof(request), length) && request.fd > 0;
}
constexpr auto wki_vfs_fchown_request_valid(const VfsPathIdentityHeader& header, const VfsFileChownRequest& request, size_t length)
    -> bool {
    return wki_vfs_file_metadata_header_valid(header, VfsPathOperation::FCHOWN, sizeof(request), length) && request.fd > 0;
}
constexpr auto wki_vfs_futimens_request_valid(const VfsPathIdentityHeader& header, const VfsFileUtimensRequest& request, size_t length)
    -> bool {
    return wki_vfs_file_metadata_header_valid(header, VfsPathOperation::FUTIMENS, sizeof(request), length) && request.fd > 0 &&
           request.reserved == 0;
}
// CHMOD operation data: this prefix followed by 0..511 raw path bytes.
// Only export-root-relative requests allow an empty path.
// directory_fd == -1 addresses the export root; positive values retain a
// directory description on this exact channel. The success result is empty.
struct VfsPathChmodPrefix {
    uint32_t mode;
    int32_t directory_fd;
} __attribute__((packed));
static_assert(sizeof(VfsPathChmodPrefix) == 8);
constexpr auto wki_vfs_chmod_request_valid(const VfsPathIdentityHeader& header, const VfsPathChmodPrefix& prefix, size_t operation_length)
    -> bool {
    return header.operation == VfsPathOperation::CHMOD && header.reserved == 0 &&
           (header.flags & ~(WKI_VFS_PATH_REQUIRE_DIRECTORY | WKI_VFS_PATH_FOLLOW_FINAL)) == 0 && header.data_len == operation_length &&
           operation_length >= sizeof(prefix) && operation_length <= sizeof(prefix) + 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && operation_length > sizeof(prefix)));
}
// CHOWN preserves uint32 UID/GID no-change sentinels and CHMOD path rules.
struct VfsPathChownPrefix {
    uint32_t owner;
    uint32_t group;
    int32_t directory_fd;
} __attribute__((packed));
static_assert(sizeof(VfsPathChownPrefix) == 12);
constexpr auto wki_vfs_chown_request_valid(const VfsPathIdentityHeader& header, const VfsPathChownPrefix& prefix, size_t operation_length)
    -> bool {
    return header.operation == VfsPathOperation::CHOWN && header.reserved == 0 &&
           (header.flags & ~(WKI_VFS_PATH_REQUIRE_DIRECTORY | WKI_VFS_PATH_FOLLOW_FINAL)) == 0 && header.data_len == operation_length &&
           operation_length >= sizeof(prefix) && operation_length <= sizeof(prefix) + 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && operation_length > sizeof(prefix)));
}
// UTIMENS data keeps NOW/OMIT intent; the owner resolves NOW on its clock.
// Null userspace times are encoded as NOW/NOW. Directory/path rules match CHMOD.
struct VfsPathUtimensPrefix {
    int32_t directory_fd;
    uint32_t reserved;
    int64_t atime_sec;
    int64_t atime_nsec;
    int64_t mtime_sec;
    int64_t mtime_nsec;
} __attribute__((packed));
static_assert(sizeof(VfsPathUtimensPrefix) == 40);
constexpr auto wki_vfs_utimens_request_valid(const VfsPathIdentityHeader& header, const VfsPathUtimensPrefix& prefix,
                                             size_t operation_length) -> bool {
    return header.operation == VfsPathOperation::UTIMENS && header.reserved == 0 && prefix.reserved == 0 &&
           (header.flags & ~(WKI_VFS_PATH_REQUIRE_DIRECTORY | WKI_VFS_PATH_FOLLOW_FINAL)) == 0 && header.data_len == operation_length &&
           operation_length >= sizeof(prefix) && operation_length <= sizeof(prefix) + 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && operation_length > sizeof(prefix)));
}
// Read-only extensible feature query on an exact VFS binding. Older owners
// can reject it without receiving a namespace mutation.
constexpr uint16_t OP_VFS_FEATURES = 0x041e;
constexpr uint16_t WKI_VFS_FEATURES_VERSION = 1;
constexpr uint64_t WKI_VFS_FEATURE_MKDIR_IDENTITY = 1;
constexpr uint64_t WKI_VFS_FEATURE_SYMLINK_IDENTITY = 2;
constexpr uint64_t WKI_VFS_FEATURE_REMOVE_IDENTITY = 4;
constexpr uint64_t WKI_VFS_FEATURE_RENAME_IDENTITY = 8;
constexpr uint64_t WKI_VFS_FEATURE_BATCH_MUTATION_IDENTITY = 16;
constexpr uint64_t WKI_VFS_FEATURE_XATTR_IDENTITY = 32;
constexpr uint64_t WKI_VFS_FEATURE_DIRECTORY_REFERENCE = 64;
constexpr uint64_t WKI_VFS_FEATURE_REALPATH_AT = 128;
constexpr uint64_t WKI_VFS_FEATURE_FILE_METADATA_IDENTITY = 256;
constexpr uint64_t WKI_VFS_FEATURE_CHOWN_IDENTITY = 512;
constexpr uint64_t WKI_VFS_FEATURE_DIRECTORY_GRANT = 1024;
constexpr uint64_t WKI_VFS_FEATURE_OPEN_LIFETIME = 2048;
struct VfsFeaturesRequest {
    uint16_t version;
    uint16_t reserved;
} __attribute__((packed));
struct VfsFeaturesResponse {
    uint16_t version;
    uint16_t length;
    uint32_t reserved;
    uint64_t features;
} __attribute__((packed));
static_assert(sizeof(VfsFeaturesRequest) == 4);
static_assert(sizeof(VfsFeaturesResponse) == 16);
constexpr auto wki_vfs_features_request_valid(const VfsFeaturesRequest& request, size_t size) -> bool {
    return size == sizeof(request) && request.version == WKI_VFS_FEATURES_VERSION && request.reserved == 0;
}
constexpr auto wki_vfs_features_response_valid(const VfsFeaturesResponse& response, size_t size) -> bool {
    return size == sizeof(response) && response.version == WKI_VFS_FEATURES_VERSION && response.length == sizeof(response) &&
           response.reserved == 0;
}
struct VfsPathMkdirPrefix {
    uint32_t mode;
    int32_t directory_fd;
} __attribute__((packed));
static_assert(sizeof(VfsPathMkdirPrefix) == 8);
constexpr auto wki_vfs_mkdir_request_valid(const VfsPathIdentityHeader& header, const VfsPathMkdirPrefix& prefix, size_t length) -> bool {
    return header.operation == VfsPathOperation::MKDIR && header.reserved == 0 && (header.flags & ~WKI_VFS_PATH_REQUIRE_DIRECTORY) == 0 &&
           header.data_len == length && length >= sizeof(prefix) && length <= sizeof(prefix) + 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && length > sizeof(prefix)));
}
struct VfsPathSymlinkPrefix {
    int32_t directory_fd;
    uint16_t target_length;
    uint16_t reserved;
} __attribute__((packed));
static_assert(sizeof(VfsPathSymlinkPrefix) == 8);
constexpr auto wki_vfs_symlink_request_valid(const VfsPathIdentityHeader& header, const VfsPathSymlinkPrefix& prefix, size_t length)
    -> bool {
    return header.operation == VfsPathOperation::SYMLINK && header.reserved == 0 && prefix.reserved == 0 &&
           (header.flags & ~WKI_VFS_PATH_REQUIRE_DIRECTORY) == 0 && header.data_len == length && length >= sizeof(prefix) &&
           prefix.target_length <= 511 && prefix.target_length <= length - sizeof(prefix) &&
           length - sizeof(prefix) - prefix.target_length <= 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && length > sizeof(prefix) + prefix.target_length));
}
// REMOVE never follows the final symlink. The flag selects rmdir semantics.
constexpr uint16_t WKI_VFS_REMOVE_DIRECTORY = 1;
struct VfsPathRemovePrefix {
    int32_t directory_fd;
    uint16_t flags;
    uint16_t reserved;
} __attribute__((packed));
static_assert(sizeof(VfsPathRemovePrefix) == 8);
constexpr auto wki_vfs_remove_request_valid(const VfsPathIdentityHeader& header, const VfsPathRemovePrefix& prefix, size_t length) -> bool {
    return header.operation == VfsPathOperation::REMOVE && header.reserved == 0 && prefix.reserved == 0 &&
           (header.flags & ~WKI_VFS_PATH_REQUIRE_DIRECTORY) == 0 && (prefix.flags & ~WKI_VFS_REMOVE_DIRECTORY) == 0 &&
           header.data_len == length && length >= sizeof(prefix) && length <= sizeof(prefix) + 511 &&
           (prefix.directory_fd == -1 || (prefix.directory_fd > 0 && length > sizeof(prefix)));
}
// Owner-local binding generation, returned on the directory's exact channel.
// No pointer or consumer-local generation is transmitted.
struct VfsRenameDirectoryRef {
    int32_t fd;
    uint16_t channel_id;
    uint16_t reserved;
    uint32_t generation;
} __attribute__((packed));
static_assert(sizeof(VfsRenameDirectoryRef) == 12);
constexpr auto wki_vfs_rename_directory_valid(const VfsRenameDirectoryRef& reference) -> bool {
    return reference.reserved == 0 &&
           (reference.fd == -1 ? reference.channel_id == 0 && reference.generation == 0
                               : reference.fd > 0 && reference.channel_id >= WKI_CHAN_DYNAMIC_BASE && reference.generation != 0);
}
constexpr uint16_t WKI_VFS_RENAME_SOURCE_DIRECTORY = 1;
constexpr uint16_t WKI_VFS_RENAME_DESTINATION_DIRECTORY = 2;
struct VfsPathRenamePrefix {
    VfsRenameDirectoryRef source;
    VfsRenameDirectoryRef destination;
    uint16_t source_length;
    uint16_t flags;
} __attribute__((packed));
static_assert(sizeof(VfsPathRenamePrefix) == 28);
constexpr auto wki_vfs_rename_request_valid(const VfsPathIdentityHeader& header, const VfsPathRenamePrefix& prefix, size_t length) -> bool {
    if (header.operation != VfsPathOperation::RENAME || header.flags != 0 || header.reserved != 0 || header.data_len != length ||
        length < sizeof(prefix) || prefix.source_length > 511 || prefix.source_length > length - sizeof(prefix) ||
        (prefix.flags & ~(WKI_VFS_RENAME_SOURCE_DIRECTORY | WKI_VFS_RENAME_DESTINATION_DIRECTORY)) != 0 ||
        !wki_vfs_rename_directory_valid(prefix.source) || !wki_vfs_rename_directory_valid(prefix.destination))
        return false;
    size_t const DESTINATION_LENGTH = length - sizeof(prefix) - prefix.source_length;
    return DESTINATION_LENGTH <= 511 && (prefix.source.fd == -1 || prefix.source_length != 0) &&
           (prefix.destination.fd == -1 || DESTINATION_LENGTH != 0);
}
struct VfsPathOpenResult {
    int32_t fd = -1;
    uint8_t is_directory = 0;
    std::array<uint8_t, 3> reserved{};
} __attribute__((packed));
static_assert(sizeof(VfsPathOpenResult) == 8);
constexpr auto wki_vfs_path_open_result_valid(const VfsPathOpenResult& result) -> bool {
    return result.fd > 0 && result.is_directory <= 1 && result.reserved == std::array<uint8_t, 3>{};
}
constexpr auto wki_vfs_realpath_result_valid(const uint8_t* bytes, size_t length) -> bool {
    if (length > 511 || (bytes == nullptr && length != 0)) return false;
    size_t start = 0;
    while (start < length) {
        size_t end = start;
        while (end < length && bytes[end] != '/') {
            if (bytes[end] == 0) return false;
            ++end;
        }
        size_t const COMPONENT = end - start;
        if (COMPONENT == 0 || (COMPONENT == 1 && bytes[start] == '.') || (COMPONENT == 2 && bytes[start] == '.' && bytes[start + 1] == '.'))
            return false;
        if (end == length) return true;
        start = end + 1;
        if (start == length) return false;
    }
    return true;
}
// Search-only directory references. PIN and PATH append one canonical caller
// record; PIN then appends raw relative path bytes. CANCEL carries only a token.
constexpr uint16_t OP_VFS_DIRECTORY_REFERENCE = 0x041f;
constexpr uint16_t WKI_VFS_DIRECTORY_REFERENCE_VERSION = 1;
constexpr size_t WKI_VFS_DIRECTORY_CALLER_SIZE = 80;
enum class VfsDirectoryOperation : uint16_t { PIN = 1, PATH = 2, CANCEL = 3 };
struct VfsDirectoryRequest {
    uint16_t version = WKI_VFS_DIRECTORY_REFERENCE_VERSION;
    VfsDirectoryOperation operation = VfsDirectoryOperation::PIN;
    int32_t directory_fd = -1;
    uint64_t token = 0;
    uint16_t path_length = 0;
    uint16_t reserved = 0;
    uint32_t reserved2 = 0;
} __attribute__((packed));
struct VfsDirectoryResponse {
    uint16_t version = WKI_VFS_DIRECTORY_REFERENCE_VERSION;
    uint16_t reserved = 0;
    int32_t fd = -1;
    uint64_t token = 0;
    uint16_t path_length = 0;
    uint16_t reserved2 = 0;
    uint32_t reserved3 = 0;
} __attribute__((packed));
static_assert(sizeof(VfsDirectoryRequest) == 24);
static_assert(sizeof(VfsDirectoryResponse) == 24);
constexpr auto wki_vfs_directory_request_valid(const VfsDirectoryRequest& request, size_t length) -> bool {
    if (request.version != WKI_VFS_DIRECTORY_REFERENCE_VERSION || request.token == 0 || request.reserved != 0 || request.reserved2 != 0)
        return false;
    if (request.operation == VfsDirectoryOperation::CANCEL)
        return request.directory_fd == 0 && request.path_length == 0 && length == sizeof(request);
    if (length != sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE + request.path_length) return false;
    if (request.operation == VfsDirectoryOperation::PATH) return request.directory_fd > 0 && request.path_length == 0;
    return request.operation == VfsDirectoryOperation::PIN && request.path_length <= 511 &&
           (request.directory_fd == -1 || (request.directory_fd > 0 && request.path_length != 0));
}
constexpr auto wki_vfs_directory_response_valid(const VfsDirectoryResponse& response, uint64_t token, const uint8_t* path, size_t length)
    -> bool {
    return response.version == WKI_VFS_DIRECTORY_REFERENCE_VERSION && response.reserved == 0 && response.reserved2 == 0 &&
           response.reserved3 == 0 && response.fd > 0 && token != 0 && response.token == token && response.path_length >= 1 &&
           response.path_length <= 511 && length == sizeof(response) + response.path_length && path != nullptr && path[0] == '/' &&
           wki_vfs_realpath_result_valid(path + 1, response.path_length - 1);
}
constexpr auto wki_vfs_directory_reply_size_valid(int status, size_t length, size_t capacity) -> bool {
    if (status > 0 || length > capacity) return false;
    if (status < 0 || capacity == 0) return length == 0;
    return length > sizeof(VfsDirectoryResponse) && length <= sizeof(VfsDirectoryResponse) + 511;
}
// Ordinary open lifetime, separate from immutable executable/directory pins.
// Tokens increase in send order on one exact binding. CANCEL is idempotent;
// consuming its token also prevents a later OPEN from resurrecting that owner.
constexpr uint16_t OP_VFS_OPEN_LIFETIME = 0x0421;
// Independently negotiated secure VFS fallback: canonical native intent and
// ordinary authenticated bytes preserve the exact tokenized open authority.
constexpr uint16_t OP_VFS_SECURE_IO = 0x0422;
constexpr uint16_t WKI_VFS_OPEN_LIFETIME_VERSION = 1;
struct VfsOpenLifetimeRequest {
    uint16_t version = WKI_VFS_OPEN_LIFETIME_VERSION;
    uint16_t inner_op = 0;  // zero: CANCEL; FEATURES: binding query; otherwise OPEN.
    uint16_t data_len = 0;
    uint16_t reserved = 0;
    uint64_t token = 0;
    uint64_t binding_id = 0;
    uint32_t owner_boot_epoch = 0;
    uint32_t reserved2 = 0;
} __attribute__((packed));
static_assert(sizeof(VfsOpenLifetimeRequest) == 32);
constexpr auto wki_vfs_open_lifetime_request_valid(const VfsOpenLifetimeRequest& request, size_t length) -> bool {
    if (request.version != WKI_VFS_OPEN_LIFETIME_VERSION || request.reserved != 0 || request.reserved2 != 0 || request.token == 0 ||
        length != sizeof(request) + request.data_len)
        return false;
    if (request.inner_op == OP_VFS_FEATURES) return request.data_len == 0 && request.binding_id == 0 && request.owner_boot_epoch == 0;
    if (request.binding_id == 0 || request.owner_boot_epoch == 0) return false;
    if (request.inner_op == 0) return request.data_len == 0;
    return request.data_len != 0 &&
           (request.inner_op == OP_VFS_OPEN_AS || request.inner_op == OP_VFS_OPEN_READ_AS || request.inner_op == OP_VFS_PATH_AS);
}
constexpr auto wki_vfs_open_binding_response_valid(const VfsOpenLifetimeRequest& response, uint64_t challenge, size_t length) -> bool {
    return length == sizeof(response) && response.version == WKI_VFS_OPEN_LIFETIME_VERSION && response.inner_op == OP_VFS_FEATURES &&
           response.token == challenge && challenge != 0 && response.data_len == 0 && response.reserved == 0 && response.reserved2 == 0 &&
           response.binding_id != 0 && response.owner_boot_epoch != 0;
}

// Transfer authority is issued by the owner and bound to one receiver and
// submission. The ordinary directory PIN token remains only a cleanup key.
constexpr uint16_t OP_VFS_DIRECTORY_GRANT = 0x0420;
constexpr uint16_t WKI_VFS_DIRECTORY_GRANT_VERSION = 1;
constexpr uint16_t WKI_VFS_DIRECTORY_GRANT_READONLY = 1;
constexpr uint16_t WKI_VFS_DIRECTORY_GRANT_NOSUID = 2;
constexpr uint16_t WKI_VFS_DIRECTORY_GRANT_POLICY = WKI_VFS_DIRECTORY_GRANT_READONLY | WKI_VFS_DIRECTORY_GRANT_NOSUID;
enum class VfsDirectoryGrantOperation : uint16_t { ISSUE = 1, ADOPT = 2 };
struct VfsDirectoryGrant {
    uint16_t version{};
    uint16_t policy{};
    uint16_t owner_node{};
    uint16_t source_node{};
    uint16_t target_node{};
    uint16_t reserved{};
    uint32_t resource_id{};
    ResourceIncarnationToken owner_incarnation{};
    uint32_t source_boot_epoch{};
    uint32_t target_boot_epoch{};
    uint64_t owner_grant_id{};
    uint64_t submission_id{};
} __attribute__((packed));
struct VfsDirectoryGrantRequest {
    uint16_t version = WKI_VFS_DIRECTORY_GRANT_VERSION;
    VfsDirectoryGrantOperation operation = VfsDirectoryGrantOperation::ISSUE;
    uint16_t policy{};
    uint16_t target_node{};
    int32_t directory_fd = -1;
    uint32_t target_boot_epoch{};
    uint64_t source_pin_token{};
    uint64_t pin_token{};
    uint64_t submission_id{};
    VfsDirectoryGrant grant{};
    // One canonical caller credential record follows.
} __attribute__((packed));
struct VfsDirectoryGrantResponse {
    VfsDirectoryResponse directory{};
    VfsDirectoryGrant grant{};
    // Current display path follows; it is never adoption authority.
} __attribute__((packed));
static_assert(sizeof(VfsDirectoryGrant) == 48);
static_assert(sizeof(VfsDirectoryGrantRequest) == 88);
static_assert(sizeof(VfsDirectoryGrantResponse) == 72);
constexpr auto wki_vfs_directory_grant_equal(const VfsDirectoryGrant& left, const VfsDirectoryGrant& right) -> bool {
    return left.version == right.version && left.policy == right.policy && left.owner_node == right.owner_node &&
           left.source_node == right.source_node && left.target_node == right.target_node && left.reserved == right.reserved &&
           left.resource_id == right.resource_id && wki_resource_incarnation_equal(left.owner_incarnation, right.owner_incarnation) &&
           left.source_boot_epoch == right.source_boot_epoch && left.target_boot_epoch == right.target_boot_epoch &&
           left.owner_grant_id == right.owner_grant_id && left.submission_id == right.submission_id;
}
constexpr auto wki_vfs_directory_grant_valid(const VfsDirectoryGrant& grant) -> bool {
    return grant.version == WKI_VFS_DIRECTORY_GRANT_VERSION && (grant.policy & ~WKI_VFS_DIRECTORY_GRANT_POLICY) == 0 &&
           grant.owner_node != 0 && grant.owner_node != UINT16_MAX && grant.source_node != 0 && grant.source_node != UINT16_MAX &&
           grant.target_node != 0 && grant.target_node != UINT16_MAX && grant.reserved == 0 && grant.resource_id != 0 &&
           wki_resource_incarnation_valid(grant.owner_incarnation) && grant.source_boot_epoch != 0 && grant.target_boot_epoch != 0 &&
           grant.owner_grant_id != 0 && grant.submission_id != 0;
}
constexpr auto wki_vfs_directory_grant_request_valid(const VfsDirectoryGrantRequest& request, size_t length) -> bool {
    if (request.version != WKI_VFS_DIRECTORY_GRANT_VERSION || request.pin_token == 0 ||
        length != sizeof(request) + WKI_VFS_DIRECTORY_CALLER_SIZE)
        return false;
    if (request.operation == VfsDirectoryGrantOperation::ISSUE)
        return (request.policy & ~WKI_VFS_DIRECTORY_GRANT_POLICY) == 0 && request.target_node != 0 && request.target_node != UINT16_MAX &&
               request.directory_fd > 0 && request.target_boot_epoch != 0 && request.source_pin_token != 0 &&
               request.source_pin_token != request.pin_token && request.submission_id != 0 &&
               wki_vfs_directory_grant_equal(request.grant, {});
    return request.operation == VfsDirectoryGrantOperation::ADOPT && request.policy == 0 && request.target_node == 0 &&
           request.directory_fd == -1 && request.target_boot_epoch == 0 && request.source_pin_token == 0 && request.submission_id == 0 &&
           wki_vfs_directory_grant_valid(request.grant);
}
constexpr auto wki_vfs_directory_grant_response_valid(const VfsDirectoryGrantResponse& response, uint64_t token, const uint8_t* path,
                                                      size_t length) -> bool {
    return wki_vfs_directory_grant_valid(response.grant) && length >= sizeof(response) &&
           wki_vfs_directory_response_valid(response.directory, token, path, length - sizeof(response.grant));
}
constexpr auto wki_vfs_directory_grant_reply_size_valid(int status, size_t length, size_t capacity) -> bool {
    if (status > 0 || length > capacity) return false;
    if (status < 0 || capacity == 0) return length == 0;
    return length > sizeof(VfsDirectoryGrantResponse) && length <= sizeof(VfsDirectoryGrantResponse) + 511;
}

constexpr size_t WKI_VFS_FSTAT_RESPONSE_SIZE = 144;
// Versioned retained-description operation. Existing HELLO capability bits and
// OPEN messages keep their meanings; only a valid PIN reply grants a view.
constexpr uint16_t OP_VFS_EXEC_VIEW = 0x041d;
constexpr uint16_t WKI_VFS_EXEC_VIEW_VERSION = 1;
enum class VfsExecViewOperation : uint16_t { PIN = 1, CANCEL = 2 };
struct VfsExecViewRequest {
    uint16_t version = WKI_VFS_EXEC_VIEW_VERSION;
    VfsExecViewOperation operation = VfsExecViewOperation::PIN;
    int32_t source_fd = 0;
    uint64_t token = 0;
} __attribute__((packed));
struct VfsExecViewResponse {
    uint16_t version = WKI_VFS_EXEC_VIEW_VERSION;
    uint16_t reserved = 0;
    int32_t fd = -1;
    uint64_t token = 0;
    std::array<uint8_t, WKI_VFS_FSTAT_RESPONSE_SIZE> metadata{};
} __attribute__((packed));
static_assert(sizeof(VfsExecViewRequest) == 16);
static_assert(sizeof(VfsExecViewResponse) == 160);
constexpr auto wki_vfs_exec_view_request_valid(const VfsExecViewRequest& request) -> bool {
    return request.version == WKI_VFS_EXEC_VIEW_VERSION && request.token != 0 &&
           ((request.operation == VfsExecViewOperation::PIN && request.source_fd > 0) ||
            (request.operation == VfsExecViewOperation::CANCEL && request.source_fd == 0));
}
constexpr auto wki_vfs_exec_view_response_valid(const VfsExecViewResponse& response, uint64_t token) -> bool {
    return response.version == WKI_VFS_EXEC_VIEW_VERSION && response.reserved == 0 && response.fd > 0 && token != 0 &&
           response.token == token;
}
constexpr auto wki_vfs_exec_view_reply_size_valid(int16_t status, size_t length, size_t expected) -> bool {
    return (expected == 0 || expected == sizeof(VfsExecViewResponse)) && status <= 0 && length == (status == 0 ? expected : 0);
}
constexpr auto wki_vfs_path_response_valid(int16_t status, size_t data_len, const VfsPathIdentityHeader& header) -> bool {
    if (status < 0) return data_len == 0;
    bool const RESULT_SIZE = (wki_vfs_is_file_metadata(header.operation) || header.operation == VfsPathOperation::CHOWN ||
                              header.operation == VfsPathOperation::CHMOD || header.operation == VfsPathOperation::UTIMENS ||
                              header.operation == VfsPathOperation::MKDIR || header.operation == VfsPathOperation::SYMLINK ||
                              header.operation == VfsPathOperation::REMOVE || header.operation == VfsPathOperation::RENAME)
                                 ? header.data_len == 0
                             : header.operation == VfsPathOperation::DIRECTORY_BINDING ? header.data_len == sizeof(VfsRenameDirectoryRef)
                             : header.operation == VfsPathOperation::OPEN_AT           ? header.data_len == sizeof(VfsPathOpenResult)
                             : (header.operation == VfsPathOperation::REALPATH || header.operation == VfsPathOperation::REALPATH_AT)
                                 ? header.data_len <= 511
                             : header.operation == VfsPathOperation::STAT_AT
                                 ? header.data_len == WKI_VFS_FSTAT_RESPONSE_SIZE
                                 : (header.operation == VfsPathOperation::READLINK || header.operation == VfsPathOperation::READLINK_AT) &&
                                       header.data_len <= 512;
    return status == 0 && data_len >= sizeof(header) && RESULT_SIZE && header.flags == 0 && header.reserved == 0 &&
           header.data_len == data_len - sizeof(header);
}
constexpr auto wki_vfs_fstat_response_valid(int16_t status, size_t data_len) -> bool {
    return status <= 0 && data_len == (status == 0 ? WKI_VFS_FSTAT_RESPONSE_SIZE : 0);
}
constexpr uint8_t WKI_VFS_XATTR_VERSION = 2;
constexpr size_t WKI_VFS_XATTR_CALLER_SIZE = 80;
constexpr uint16_t WKI_VFS_XATTR_NAME_MAX = 255;
constexpr uint32_t WKI_VFS_XATTR_DATA_MAX = 65536;

enum class VfsXattrPhase : uint8_t {
    INVALID = 0,
    BEGIN = 1,
    DATA = 2,
    COMMIT = 3,
    ABORT = 4,
};

enum class VfsXattrOperation : uint8_t {
    INVALID = 0,
    GET = 1,
    LIST = 2,
    SET = 3,
    REMOVE = 4,
};

enum class VfsXattrTarget : uint8_t {
    INVALID = 0,
    PATH = 1,
    FD = 2,
};

// Every phase carries the same explicit identity. Unlike the DEV_OP sequence
// cookie this identity survives response loss and is suitable for exact
// mutation replay detection.
struct VfsXattrPhaseHeader {
    uint64_t session_id;
    uint64_t operation_id;
    uint8_t version;
    VfsXattrPhase phase;
    VfsXattrOperation operation;
    VfsXattrTarget target;
} __attribute__((packed));

static_assert(sizeof(VfsXattrPhaseHeader) == 20);

// Version 2 BEGIN is followed by the canonical caller record, path bytes and
// name bytes. PATH remote_fd=-1 selects the export root; nonnegative FDs anchor
// nonempty relative paths to an exact-channel directory. FD targets carry no path.
struct VfsXattrBeginPayload {
    VfsXattrPhaseHeader header;
    int32_t remote_fd;
    uint32_t data_len;
    uint32_t flags;
    uint16_t path_len;
    uint16_t name_len;
    uint8_t follow_final_symlink;
    uint8_t require_directory;
    std::array<uint8_t, 2> reserved;
} __attribute__((packed));

static_assert(sizeof(VfsXattrBeginPayload) == 40);

// DATA requests and responses use contiguous offsets; chunk_len bytes follow.
struct VfsXattrDataPayload {
    VfsXattrPhaseHeader header;
    uint32_t offset;
    uint16_t chunk_len;
    uint16_t reserved;
} __attribute__((packed));

static_assert(sizeof(VfsXattrDataPayload) == 28);

struct VfsXattrResultPayload {
    VfsXattrPhaseHeader header;
    uint32_t data_len;
    uint32_t transferred;
} __attribute__((packed));

static_assert(sizeof(VfsXattrResultPayload) == 28);

constexpr auto wki_vfs_xattr_header_valid(const VfsXattrPhaseHeader& header) -> bool {
    return header.version == WKI_VFS_XATTR_VERSION && header.session_id != 0 && header.operation_id != 0 &&
           header.phase >= VfsXattrPhase::BEGIN && header.phase <= VfsXattrPhase::ABORT && header.operation >= VfsXattrOperation::GET &&
           header.operation <= VfsXattrOperation::REMOVE && header.target >= VfsXattrTarget::PATH && header.target <= VfsXattrTarget::FD;
}

constexpr auto wki_vfs_xattr_begin_valid(const VfsXattrBeginPayload& begin) -> bool {
    bool const PATH_TARGET = begin.header.target == VfsXattrTarget::PATH;
    bool const NEEDS_NAME = begin.header.operation != VfsXattrOperation::LIST;
    bool const CARRIES_DATA = begin.header.operation == VfsXattrOperation::SET || begin.header.operation == VfsXattrOperation::GET ||
                              begin.header.operation == VfsXattrOperation::LIST;
    return wki_vfs_xattr_header_valid(begin.header) && begin.header.phase == VfsXattrPhase::BEGIN &&
           begin.data_len <= WKI_VFS_XATTR_DATA_MAX && begin.name_len <= WKI_VFS_XATTR_NAME_MAX &&
           (NEEDS_NAME ? begin.name_len != 0 : begin.name_len == 0) && (PATH_TARGET || begin.path_len == 0) &&
           (PATH_TARGET ? begin.remote_fd >= -1 && (begin.remote_fd == -1 || begin.path_len != 0) : begin.remote_fd >= 0) &&
           (CARRIES_DATA || begin.data_len == 0) && begin.follow_final_symlink <= 1 && begin.require_directory <= 1 &&
           (PATH_TARGET || begin.require_directory == 0) && begin.reserved[0] == 0 && begin.reserved[1] == 0;
}

constexpr size_t WKI_VFS_XATTR_MAX_DATA_CHUNK = WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload) - sizeof(VfsXattrResultPayload);
static_assert(WKI_VFS_XATTR_MAX_DATA_CHUNK > 0 && WKI_VFS_XATTR_MAX_DATA_CHUNK <= UINT16_MAX);

// IPC Pipe (0x0700–0x070F) — data moves via wire messages
constexpr uint16_t OP_PIPE_CLOSE_READ = 0x0700;
constexpr uint16_t OP_PIPE_CLOSE_WRITE = 0x0701;
constexpr uint16_t OP_PIPE_DATA = 0x0702;  // req={resource_id:u32, data[]}
constexpr uint16_t OP_PIPE_POLL_STATE = 0x0703;
constexpr uint16_t OP_PIPE_DATA_FLOW = 0x0704;         // req={resource_id:u32, data[]}; application-credit controlled
constexpr uint16_t OP_PIPE_WRITE_FLOW = 0x0705;        // req={resource_id:u32, credit_bytes:u32, status:i32}
constexpr uint16_t OP_PIPE_CLOSE_WRITE_FLOW = 0x0706;  // terminal close for a flow-controlled exported writer
// Ordered after a retired export's final IPC_DATA message. Receivers discard
// any pre-attachment DATA/EOF state that can no longer acquire a proxy owner.
constexpr uint16_t OP_PIPE_DISCARD_PENDING = 0x0707;  // req={resource_id:u32}

struct IpcPipeWriteFlowPayload {
    uint32_t resource_id;
    uint32_t credit_bytes;
    int32_t status;
} __attribute__((packed));

static_assert(sizeof(IpcPipeWriteFlowPayload) == 12, "IpcPipeWriteFlowPayload must be 12 bytes");

// IPC Eventfd (0x0710–0x071F)
constexpr uint16_t OP_EVENTFD_CLOSE = 0x0710;

// IPC PTY (0x0720–0x072F) — control only; data via RDMA
constexpr uint16_t OP_PTY_IOCTL = 0x0720;
constexpr uint16_t OP_PTY_CLOSE = 0x0721;

// IPC Futex (0x0730–0x073F)
constexpr uint16_t OP_FUTEX_WAKE = 0x0730;

// IPC Epoll (0x0740–0x074F)
constexpr uint16_t OP_EPOLL_CTL = 0x0740;
constexpr uint16_t OP_EPOLL_READY_NOTIFY = 0x0741;

// IPC Socket (0x0750–0x075F)
constexpr uint16_t OP_SOCK_ACCEPT = 0x0750;
constexpr uint16_t OP_SOCK_CLOSE = 0x0751;
constexpr uint16_t OP_SOCK_SHUTDOWN = 0x0752;
constexpr uint16_t OP_SOCK_GETPEERNAME = 0x0753;
constexpr uint16_t OP_SOCK_GETSOCKOPT = 0x0754;
constexpr uint16_t OP_SOCK_SETSOCKOPT = 0x0755;

// -----------------------------------------------------------------------------
// DEV_IRQ_FWD Payload - 8 bytes
// -----------------------------------------------------------------------------

struct DevIrqFwdPayload {
    uint16_t device_id;
    uint16_t irq_vector;
    uint32_t irq_status;  // device-specific status register value
} __attribute__((packed));

static_assert(sizeof(DevIrqFwdPayload) == 8, "DevIrqFwdPayload must be 8 bytes");

// -----------------------------------------------------------------------------
// CHANNEL_OPEN / CHANNEL_OPEN_ACK / CHANNEL_CLOSE Payload
// -----------------------------------------------------------------------------

enum class PriorityClass : uint8_t {
    LATENCY = 0,
    THROUGHPUT = 1,
};

struct ChannelOpenPayload {
    uint16_t requested_channel_id;  // 0 = auto-assign
    uint8_t priority;               // PriorityClass
    uint8_t reserved;
    uint16_t initial_credits;  // credits to grant
    uint16_t reserved2;
} __attribute__((packed));

static_assert(sizeof(ChannelOpenPayload) == 8, "ChannelOpenPayload must be 8 bytes");

struct ChannelOpenAckPayload {
    uint16_t assigned_channel_id;
    uint8_t status;  // 0 = OK, 1 = REJECTED
    uint8_t reserved;
    uint16_t initial_credits;  // credits granted back
    uint16_t reserved2;
} __attribute__((packed));

struct ChannelClosePayload {
    uint16_t channel_id;
    uint16_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// TASK_SUBMIT Payload - variable length
// -----------------------------------------------------------------------------

enum class TaskDeliveryMode : uint8_t {
    INLINE = 0,
    VFS_REF = 1,
    RESOURCE_REF = 2,
};

// IPC fd entry for cross-node task submission — appended to TASK_SUBMIT payload
// Application delivery receipts for writable pipes, PTYs and native stream sockets.
constexpr uint16_t WKI_IPC_FD_FLAG_PIPE_FLOW_CONTROL = 0x8000;

struct WkiIpcFdEntry {
    uint16_t local_fd;
    uint16_t res_type;  // ResourceType enum
    uint32_t resource_id;
    uint16_t home_node;
    uint16_t reserved1;    // submitter-side access mode / open_flags low bits
    uint32_t rdma_rkey;    // home node's RDMA rkey for shared region
    uint64_t rdma_offset;  // offset within RDMA space
} __attribute__((packed));

static_assert(sizeof(WkiIpcFdEntry) == 24, "WkiIpcFdEntry must be 24 bytes");

constexpr uint16_t WKI_TASK_SUBMIT_EXEC_HANDOFF = 1;
constexpr uint16_t WKI_TASK_SUBMIT_SPAWN_TRANSITION = 8;
// v4 also preserves original read authority for already prepared spawn. Its
// transition never performs the old-process READY/COMMIT exec handoff.
constexpr uint32_t WKI_EXEC_HANDOFF_RESOURCE_ID = 0x45584304;
constexpr char WKI_EXEC_HANDOFF_RESOURCE_NAME[] = "process-exec-handoff-v4";

// Handoff v2 appends this tail after IPC entries. The ordinary identity remains
// the original caller's image-read authority; this metadata determines the
// replacement identity. v4 spawn uses the same tail under its distinct flag.
constexpr uint16_t WKI_EXEC_CREDENTIAL_VERSION = 1;
constexpr uint16_t WKI_EXEC_CREDENTIAL_NOSUID = 1;
constexpr uint16_t WKI_EXEC_CREDENTIAL_TRACED = 2;
struct TaskExecCredentialTransition {
    uint16_t version = WKI_EXEC_CREDENTIAL_VERSION;
    uint16_t flags{};
    uint32_t uid{};
    uint32_t gid{};
    uint32_t mode{};
} __attribute__((packed));
static_assert(sizeof(TaskExecCredentialTransition) == 16);

// Optional tail after the complete routed compute identity. Absent ROOT_GRANT
// selects the global namespace root unless ROOT_PATH explicitly retains legacy
// root-path semantics; absent CWD_GRANT selects that root as cwd.
// Grant display names never replace these object identities. Installation must
// be negotiated separately before the feature is advertised.
constexpr uint16_t WKI_TASK_SUBMIT_FILESYSTEM = 4;
constexpr uint16_t WKI_TASK_FILESYSTEM_VERSION = 1;
constexpr uint16_t WKI_TASK_FILESYSTEM_ROOT_GRANT = 1;
constexpr uint16_t WKI_TASK_FILESYSTEM_CWD_GRANT = 2;
constexpr uint16_t WKI_TASK_FILESYSTEM_ROOT_PATH = 4;
constexpr uint32_t WKI_FILESYSTEM_CONTEXT_RESOURCE_ID = 0x46534301;
constexpr char WKI_FILESYSTEM_CONTEXT_RESOURCE_NAME[] = "process-filesystem-context-v1";
// Implemented subset: global root plus one retained cwd returned to its native
// owner. This does not advertise owned-root or third-owner context transfer.
constexpr uint32_t WKI_RETURN_OWNER_CWD_RESOURCE_ID = 0x46534302;
constexpr char WKI_RETURN_OWNER_CWD_RESOURCE_NAME[] = "process-return-owner-cwd-v1";
constexpr uint32_t WKI_ROOTED_RETURN_OWNER_CWD_RESOURCE_ID = 0x46534303;
constexpr char WKI_ROOTED_RETURN_OWNER_CWD_RESOURCE_NAME[] = "process-rooted-return-owner-cwd-v1";

// ROOT_PATH is a canonical, bounded legacy namespace path, not a root grant.
// Wire padding must be zero; captured local buffers may have bytes after NUL.
template <size_t N>
constexpr auto wki_task_legacy_root_path_valid(const std::array<char, N>& root, bool zero_padding = true) -> bool {
    if (N < 3 || root[0] != '/' || root[1] == '\0') return false;
    size_t start = 1;
    for (size_t i = 1; i < N; ++i) {
        if (root[i] != '/' && root[i] != '\0') continue;
        size_t const LENGTH = i - start;
        if (LENGTH == 0 || (root[start] == '.' && (LENGTH == 1 || (LENGTH == 2 && root[start + 1] == '.')))) return false;
        if (root[i] == '\0') {
            if (zero_padding)
                for (size_t tail = i + 1; tail < N; ++tail)
                    if (root[tail] != '\0') return false;
            return true;
        }
        start = i + 1;
    }
    return false;
}
struct TaskFilesystemContext {
    uint16_t version = WKI_TASK_FILESYSTEM_VERSION;
    uint16_t flags = 0;
    uint32_t reserved = 0;
    uint64_t submission_id = 0;
    VfsDirectoryGrant root{};
    VfsDirectoryGrant cwd{};
} __attribute__((packed));
static_assert(sizeof(TaskFilesystemContext) == 112);
constexpr auto wki_task_filesystem_valid(const TaskFilesystemContext& context) -> bool {
    if (context.version != WKI_TASK_FILESYSTEM_VERSION || context.reserved != 0 || context.submission_id == 0 ||
        (context.flags & ~(WKI_TASK_FILESYSTEM_ROOT_GRANT | WKI_TASK_FILESYSTEM_CWD_GRANT | WKI_TASK_FILESYSTEM_ROOT_PATH)) != 0)
        return false;
    if ((context.flags & WKI_TASK_FILESYSTEM_ROOT_PATH) != 0 &&
        context.flags != (WKI_TASK_FILESYSTEM_ROOT_PATH | WKI_TASK_FILESYSTEM_CWD_GRANT))
        return false;
    auto valid = [&](const VfsDirectoryGrant& grant, uint16_t bit) {
        return (context.flags & bit) != 0 ? wki_vfs_directory_grant_valid(grant) && grant.submission_id == context.submission_id
                                          : wki_vfs_directory_grant_equal(grant, VfsDirectoryGrant{});
    };
    if (!valid(context.root, WKI_TASK_FILESYSTEM_ROOT_GRANT) || !valid(context.cwd, WKI_TASK_FILESYSTEM_CWD_GRANT)) return false;
    return context.flags != (WKI_TASK_FILESYSTEM_ROOT_GRANT | WKI_TASK_FILESYSTEM_CWD_GRANT) ||
           (context.root.source_node == context.cwd.source_node && context.root.source_boot_epoch == context.cwd.source_boot_epoch &&
            context.root.target_node == context.cwd.target_node && context.root.target_boot_epoch == context.cwd.target_boot_epoch);
}
constexpr auto wki_task_filesystem_matches_admission(const TaskFilesystemContext& context, uint16_t source, uint32_t source_boot,
                                                     uint16_t target, uint32_t target_boot) -> bool {
    if (!wki_task_filesystem_valid(context) || source == 0 || source == UINT16_MAX || source_boot == 0 || target == 0 ||
        target == UINT16_MAX || target_boot == 0)
        return false;
    auto matches = [&](const VfsDirectoryGrant& grant, uint16_t bit) {
        return (context.flags & bit) == 0 || (grant.source_node == source && grant.source_boot_epoch == source_boot &&
                                              grant.target_node == target && grant.target_boot_epoch == target_boot);
    };
    return matches(context.root, WKI_TASK_FILESYSTEM_ROOT_GRANT) && matches(context.cwd, WKI_TASK_FILESYSTEM_CWD_GRANT);
}

struct TaskSubmitPayload {
    uint32_t task_id;
    uint8_t delivery_mode;  // TaskDeliveryMode
    uint8_t prefer_inline;  // V2: 0=prefer VFS_REF, 1=prefer INLINE
    uint16_t args_len;
    // --- V2 extended environment (offset 8) ---
    uint16_t argc;          // Number of NUL-separated argument strings
    uint16_t envc;          // Number of NUL-separated env strings (KEY=VALUE)
    uint16_t cwd_len;       // Length of CWD string (0 = use default "/")
    uint16_t identity_len;  // Length of task identity block after argv/envp/cwd
    uint16_t ipc_fd_count;  // Number of WkiIpcFdEntry following context/policy data
    uint16_t reserved;
    // Variable portion follows at offset 20, depends on delivery_mode:
    //   INLINE:       uint32_t binary_len, binary[binary_len]
    //   VFS_REF:      uint16_t path_len, path[path_len]
    //   RESOURCE_REF: uint16_t ref_node_id, uint32_t ref_resource_id,
    //                 uint16_t path_len, path[path_len]
    // Then: argc NUL-terminated arg strings, envc NUL-terminated env strings,
    //       cwd string (cwd_len bytes including NUL), identity block, VFS policy,
    //       IPC fd entries.
} __attribute__((packed));

static_assert(sizeof(TaskSubmitPayload) == 20, "TaskSubmitPayload V3 must be 20 bytes");

struct TaskSubmitFragmentPayload {
    uint32_t task_id;
    uint16_t total_len;
    uint16_t offset;
    // Fragment bytes follow.
} __attribute__((packed));

static_assert(sizeof(TaskSubmitFragmentPayload) == 8, "TaskSubmitFragmentPayload must be 8 bytes");

// -----------------------------------------------------------------------------
// TASK_ACCEPT / TASK_REJECT Payload - 16 bytes
// -----------------------------------------------------------------------------

enum class TaskRejectReason : uint8_t {
    ACCEPTED = 0,
    OVERLOADED = 1,
    NO_MEM = 2,
    BINARY_NOT_FOUND = 3,
    FETCH_FAILED = 4,
    UNAUTHORIZED = 5,
    RESOURCE_NPROC = 6,
    RESOURCE_UNSUPPORTED = 7,
    RESOURCE_INVALID = 8,
    RESOURCE_CPU = 9,
};

struct TaskResponsePayload {
    uint32_t task_id;
    uint8_t status;  // TaskRejectReason (0 = accepted)
    // TASK_REJECT resource suffix length, little endian in bytes 0..1;
    // byte 2 remains zero. ACCEPT byte 0 advertises WKI_TASK_SIGNAL_VERSION;
    // bytes 1..2 remain zero. Legacy ACCEPT has all bytes zero.
    std::array<uint8_t, 3> reserved;
    uint64_t remote_pid;  // PID on executing node (if accepted)
} __attribute__((packed));

static_assert(sizeof(TaskResponsePayload) == 16, "TaskResponsePayload must be 16 bytes");

// -----------------------------------------------------------------------------
// TASK_COMPLETE Payload - variable length
// -----------------------------------------------------------------------------

struct TaskCompletePayload {
    uint32_t task_id;
    int32_t exit_status;
    uint16_t output_len;
    uint16_t reserved;  // Resource suffix length (0 or sizeof(TaskResourceSnapshot)), capability-gated.
    // Followed by output_len bytes of captured output, then resource suffix.
} __attribute__((packed));
static_assert(sizeof(TaskCompletePayload) == 12);

// -----------------------------------------------------------------------------
// TASK_CANCEL Payload - 8 bytes
// -----------------------------------------------------------------------------

struct TaskCancelPayload {
    uint32_t task_id;
    int32_t signum;
} __attribute__((packed));

static_assert(sizeof(TaskCancelPayload) == 8, "TaskCancelPayload must be 8 bytes");

// Versioned user signal operation, separate from trusted lifecycle cancel.
constexpr uint8_t WKI_TASK_SIGNAL_VERSION = 1;
struct TaskSessionOrigin {
    uint16_t node;
    uint16_t reserved;
    uint32_t boot;
} __attribute__((packed));
static_assert(sizeof(TaskSessionOrigin) == 8);

struct TaskJobOrigins {
    TaskSessionOrigin session;
    TaskSessionOrigin group;
} __attribute__((packed));
static_assert(sizeof(TaskJobOrigins) == 16);

// Terminal object authority, separate from user kill and lifecycle cancellation.
constexpr uint16_t WKI_TERMINAL_SIGNAL_VERSION = 1;
constexpr uint16_t WKI_TERMINAL_SIGNAL_HOPS = 64;
constexpr uint32_t WKI_TERMINAL_SIGNAL_RESOURCE_ID = 0x53494704;
constexpr char WKI_TERMINAL_SIGNAL_RESOURCE_NAME[] = "process-terminal-signals-v1";
struct TaskTerminalSignal {
    uint32_t task_id = 0;
    int32_t signum = 0;
    uint16_t version = WKI_TERMINAL_SIGNAL_VERSION;
    uint16_t hops = WKI_TERMINAL_SIGNAL_HOPS;
    uint32_t reserved = 0;
    uint64_t session_id = 0;
    uint64_t pgid = 0;
    TaskJobOrigins origins{};
} __attribute__((packed));
static_assert(sizeof(TaskTerminalSignal) == 48);

// Exec preserves signal values, never addresses into the replaced image.
constexpr uint16_t WKI_PARENT_DEATH_VERSION = 1;
constexpr uint16_t WKI_PARENT_DEATH_HOPS = 64;
constexpr uint32_t WKI_PARENT_DEATH_RESOURCE_ID = 0x53494705;
constexpr char WKI_PARENT_DEATH_RESOURCE_NAME[] = "process-parent-death-v1";
// The accepted execution chain grants this event. The receiver chooses its
// current registration; no sender-selected signal or ambient UID is carried.
struct TaskParentDeath {
    uint32_t task_id = 0;
    uint16_t version = WKI_PARENT_DEATH_VERSION;
    uint16_t hops = WKI_PARENT_DEATH_HOPS;
} __attribute__((packed));
static_assert(sizeof(TaskParentDeath) == 8);

// A child-event notification grants only SIGCHLD on the exact accepted image.
constexpr uint16_t WKI_CHILD_SIGNAL_VERSION = 1;
constexpr uint16_t WKI_CHILD_SIGNAL_HOPS = 64;
constexpr uint32_t WKI_CHILD_SIGNAL_RESOURCE_ID = 0x53494706;
constexpr char WKI_CHILD_SIGNAL_RESOURCE_NAME[] = "process-child-signal-v1";
struct TaskChildSignal {
    uint32_t task_id = 0;
    uint16_t version = WKI_CHILD_SIGNAL_VERSION;
    uint16_t hops = WKI_CHILD_SIGNAL_HOPS;
} __attribute__((packed));
static_assert(sizeof(TaskChildSignal) == 8);

// Discovery/validation only; this capability does not advertise child reaping.
constexpr uint16_t WKI_CHILD_QUERY_VERSION = 1;
constexpr uint32_t WKI_CHILD_QUERY_RESOURCE_ID = 0x53494707;
constexpr char WKI_CHILD_QUERY_RESOURCE_NAME[] = "process-child-query-v1";
constexpr uint8_t WKI_CHILD_QUERY_ALIAS = 1, WKI_CHILD_QUERY_OWN_GROUP = 2;
struct TaskChildQueryRequest {
    uint64_t sequence = 0;
    uint64_t receiver_pid = 0;
    uint64_t selector = 0;
    uint32_t task_id = 0;
    int32_t options = 0;
    uint16_t version = WKI_CHILD_QUERY_VERSION;
    uint16_t hops = 0;
    uint8_t lane = 0;
    uint8_t operation = 0;
    uint8_t generation = 0;
    uint8_t flags = 0;
    uint32_t reserved = 0;
    TaskSessionOrigin selector_origin{};
    TaskSessionOrigin caller_origin{};
    TaskSessionOrigin own_group{};
    TaskSessionOrigin selected_group{};
} __attribute__((packed));
static_assert(sizeof(TaskChildQueryRequest) == 76);
struct TaskChildQueryResponse {
    TaskChildQueryRequest request{};  // exact immutable request and action echo
    int32_t error = 0;
    uint8_t ready = 0;
    uint8_t potential = 0;
    uint8_t pid_flags = 0;  // found=1, ambiguous=2
    uint8_t group_flags = 0;
    uint16_t ready_depth = UINT16_MAX;
    uint16_t reserved = 0;
    TaskSessionOrigin pid_origin{};
    TaskSessionOrigin other_group{};
    uint64_t queue_version = 0;
    uint64_t group_version = 0;
} __attribute__((packed));
static_assert(sizeof(TaskChildQueryResponse) == 120);

constexpr uint32_t WKI_CHILD_CPU_RESOURCE_ID = 0x53494709;
constexpr char WKI_CHILD_CPU_RESOURCE_NAME[] = "process-child-cpu-v1";

constexpr uint16_t WKI_CHILD_CLAIM_VERSION = 1;
constexpr uint32_t WKI_CHILD_CLAIM_RESOURCE_ID = 0x53494708;
constexpr char WKI_CHILD_CLAIM_RESOURCE_NAME[] = "process-child-claim-v1";
// Refers only to an already validated query; selectors/options/versions are
// taken from retained authority. Query CLOSE is the final receipt ACK.
struct TaskChildClaimRequest {
    uint64_t sequence = 0;
    uint64_t receiver_pid = 0;
    uint32_t task_id = 0;
    uint16_t version = WKI_CHILD_CLAIM_VERSION;
    uint16_t depth = 0;
    uint8_t lane = 0;
    uint8_t command = 0;
    uint8_t generation = 0;
    uint8_t reserved = 0;
    uint32_t reserved2 = 0;
} __attribute__((packed));
static_assert(sizeof(TaskChildClaimRequest) == 32);
struct TaskChildClaimResponse {
    TaskChildClaimRequest request{};
    uint8_t phase = 0;
    uint8_t probe = 0;
    uint8_t committed = 0;
    uint8_t reserved = 0;
    int32_t error = 0;
    uint64_t pid = 0;
    uint64_t process_group = 0;
    uint64_t user_time_us = 0;
    uint64_t system_time_us = 0;
    TaskSessionOrigin group_origin{};
    int32_t status = 0;
    uint8_t kind = 0;
    uint8_t audience = 0;
    uint16_t reserved2 = 0;
} __attribute__((packed));
static_assert(sizeof(TaskChildClaimResponse) == 88);

constexpr uint16_t WKI_EXEC_SIGNALS_VERSION = 2;
constexpr uint64_t WKI_EXEC_UNBLOCKABLE_SIGNALS = (uint64_t{1} << 8) | (uint64_t{1} << 18);
struct TaskExecSignals {
    uint16_t version = WKI_EXEC_SIGNALS_VERSION;
    uint16_t reserved = 0;
    uint32_t reserved2 = 0;
    uint64_t blocked = 0;
    uint64_t ignored = 0;
    uint64_t pending = 0;
    uint32_t parent_death_signal = 0;
    uint32_t lifecycle_reserved = 0;
} __attribute__((packed));
static_assert(sizeof(TaskExecSignals) == 40);
[[nodiscard]] constexpr auto valid_exec_signals(const TaskExecSignals& state) -> bool {
    return state.version == WKI_EXEC_SIGNALS_VERSION && state.reserved == 0 && state.reserved2 == 0 && state.parent_death_signal <= 64 &&
           state.lifecycle_reserved == 0 && ((state.blocked | state.ignored) & WKI_EXEC_UNBLOCKABLE_SIGNALS) == 0;
}

constexpr uint16_t WKI_EXEC_HANDOFF_VERSION = 1;
struct TaskExecReady {
    uint32_t task_id;
    uint16_t version = WKI_EXEC_HANDOFF_VERSION;
    uint16_t reserved = 0;
    uint64_t remote_pid;
} __attribute__((packed));
static_assert(sizeof(TaskExecReady) == 16);
struct TaskExecCommit {
    TaskExecReady identity;
    TaskExecSignals signals;
} __attribute__((packed));
static_assert(sizeof(TaskExecCommit) == 56);

struct TaskSignalRequest {
    uint64_t request_id;
    uint32_t task_id;
    int32_t signum;
    uint64_t session_id;
    TaskSessionOrigin origin;
    std::array<uint8_t, 80> credentials;  // canonical credential_record v1
} __attribute__((packed));
static_assert(sizeof(TaskSignalRequest) == 112);

struct TaskSignalResponse {
    uint64_t request_id;
    uint32_t task_id;
    int32_t result;
} __attribute__((packed));
static_assert(sizeof(TaskSignalResponse) == 16);

// A named COMPUTE advertisement negotiates this service independently of
// positive-PID signals and the fully allocated HELLO capability word.
constexpr uint16_t WKI_GROUP_SIGNAL_VERSION = 1;
constexpr uint32_t WKI_GROUP_SIGNAL_RESOURCE_ID = 0x53494701;
constexpr char WKI_GROUP_SIGNAL_RESOURCE_NAME[] = "process-group-signals-v1";
enum class GroupSignalOperation : uint8_t { QUERY = 0, DELIVER = 1 };
struct GroupSignalRequest {
    uint64_t request_id;
    uint16_t version;
    uint8_t operation;
    uint8_t reserved;
    int32_t signum;
    uint64_t pgid;
    uint64_t caller_pgid;
    uint64_t session_id;
    TaskSessionOrigin session_origin;
    TaskSessionOrigin group_origin;  // caller's own for QUERY; selected group for DELIVER
    std::array<uint8_t, 80> credentials;
} __attribute__((packed));
static_assert(sizeof(GroupSignalRequest) == 136);
struct GroupSignalResponse {
    uint64_t request_id;
    int32_t result;
    uint16_t version;
    uint8_t flags;  // GroupCandidates flags for QUERY; zero for DELIVER/error
    uint8_t reserved;
    TaskSessionOrigin other;
} __attribute__((packed));
static_assert(sizeof(GroupSignalResponse) == 24);

// Logical process enumeration and direct receiver admission. Redirects never
// apply a signal; the original caller remaps its own snapshot at each receiver.
constexpr uint16_t WKI_GROUP_ROUTE_VERSION = 1;
constexpr uint32_t WKI_GROUP_ROUTE_RESOURCE_ID = 0x53494702;
constexpr char WKI_GROUP_ROUTE_RESOURCE_NAME[] = "process-group-routing-v1";
constexpr uint16_t WKI_TASK_SUBMIT_GROUP_HOME = 2;
struct TaskSignalHome {
    TaskSessionOrigin origin;
    uint64_t serial;
} __attribute__((packed));
static_assert(sizeof(TaskSignalHome) == 16);
struct GroupAnchor {
    uint64_t serial;
    uint64_t pid;
} __attribute__((packed));
static_assert(sizeof(GroupAnchor) == 16);
constexpr size_t WKI_GROUP_ANCHOR_PAGE = 32;
enum class GroupRouteOperation : uint8_t { ENUMERATE, QUERY, DELIVER };
enum class GroupRouteResult : uint8_t { FINAL, REDIRECT, RETRY };
struct GroupRouteRequest {
    uint64_t request_id = 0;
    uint16_t version = WKI_GROUP_ROUTE_VERSION;
    uint8_t operation = 0;
    uint8_t reserved = 0;
    int32_t signum = 0;
    uint64_t pgid = 0;
    uint64_t caller_pgid = 0;
    uint64_t session_id = 0;
    TaskSessionOrigin session_origin{};
    TaskSessionOrigin group_origin{};
    std::array<uint8_t, 80> credentials{};
    TaskSignalHome home{};
    uint64_t target_pid = 0;
    uint64_t cursor = 0;
    uint64_t ceiling = 0;
} __attribute__((packed));
static_assert(sizeof(GroupRouteRequest) == 176);
struct GroupRouteResponse {
    uint64_t request_id = 0;
    int32_t result = 0;
    uint16_t version = WKI_GROUP_ROUTE_VERSION;
    uint8_t kind = 0;
    uint8_t count = 0;
    uint32_t boot = 0;
    uint32_t reserved = 0;
    uint64_t ceiling = 0;
    uint64_t target_pgid = 0;
    TaskSessionOrigin group_origin{};
    TaskSessionOrigin destination{};
    uint64_t target_pid = 0;
    std::array<GroupAnchor, WKI_GROUP_ANCHOR_PAGE> anchors{};
} __attribute__((packed));
static_assert(sizeof(GroupRouteResponse) == 576);

// Current receiver discovery is separately negotiated. Requests use the
// ENUMERATE form of GroupRouteRequest; legacy route records remain unchanged.
constexpr uint16_t WKI_GROUP_RECEIVER_VERSION = 1;
constexpr uint32_t WKI_GROUP_RECEIVER_RESOURCE_ID = 0x53494703;
constexpr char WKI_GROUP_RECEIVER_RESOURCE_NAME[] = "process-group-receivers-v1";
constexpr size_t WKI_GROUP_RECEIVER_PAGE = 32;
struct GroupReceiver {
    uint64_t serial;  // nonreused receiver-local pagination identity
    uint64_t pid;
    TaskSignalHome home;
} __attribute__((packed));
static_assert(sizeof(GroupReceiver) == 32);
struct GroupReceiverResponse {
    uint64_t request_id = 0;
    int32_t result = 0;
    uint16_t version = WKI_GROUP_RECEIVER_VERSION;
    uint16_t count = 0;
    uint32_t boot = 0;
    uint32_t reserved = 0;
    uint64_t ceiling = 0;
    std::array<GroupReceiver, WKI_GROUP_RECEIVER_PAGE> receivers{};
} __attribute__((packed));
static_assert(sizeof(GroupReceiverResponse) == 1056);

// -----------------------------------------------------------------------------
// LOAD_REPORT Payload - variable length
// -----------------------------------------------------------------------------

struct LoadReportPayload {
    uint16_t num_cpus;
    uint16_t runnable_tasks;
    uint16_t avg_load_pct;    // 0-1000 non-idle utilization over the report interval
    uint16_t free_mem_pages;  // in units of 256 pages
    // Followed by num_cpus * uint16_t per-CPU utilization values (0-1000)
} __attribute__((packed));

static_assert(sizeof(LoadReportPayload) == 8, "LoadReportPayload must be 8 bytes");

inline auto load_report_per_cpu(LoadReportPayload* p) -> uint16_t* {
    return reinterpret_cast<uint16_t*>(reinterpret_cast<uint8_t*>(p) + sizeof(LoadReportPayload));
}

inline auto load_report_per_cpu(const LoadReportPayload* p) -> const uint16_t* {
    return reinterpret_cast<const uint16_t*>(reinterpret_cast<const uint8_t*>(p) + sizeof(LoadReportPayload));
}

}  // namespace ker::net::wki
