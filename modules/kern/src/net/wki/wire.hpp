#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Protocol Constants
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_ETHERTYPE = 0x88B7;
constexpr uint8_t WKI_VERSION = 1;
constexpr uint32_t WKI_HELLO_MAGIC = 0x574B4900;  // "WKI\0"
constexpr uint16_t WKI_NODE_INVALID = 0x0000;
constexpr uint16_t WKI_NODE_BROADCAST = 0xFFFF;
constexpr uint8_t WKI_DEFAULT_TTL = 16;
constexpr size_t WKI_HEADER_SIZE = 32;

// Maximum WKI payload with jumbo frames: 9000 - 14 (eth hdr) - 32 (wki hdr)
constexpr size_t WKI_ETH_MAX_PAYLOAD = 8954;

// -----------------------------------------------------------------------------
// Header Flags (lower 4 bits of version_flags byte)
// -----------------------------------------------------------------------------

constexpr uint8_t WKI_FLAG_ACK_PRESENT = 0x08;  // ack_num field is valid
constexpr uint8_t WKI_FLAG_PRIORITY = 0x04;     // latency-optimized path
constexpr uint8_t WKI_FLAG_FRAGMENT = 0x02;     // fragment of larger message
constexpr uint8_t WKI_FLAG_RESERVED = 0x01;

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
};

// -----------------------------------------------------------------------------
// Well-known Channel IDs
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_CHAN_CONTROL = 0;
constexpr uint16_t WKI_CHAN_ZONE_MGMT = 1;
constexpr uint16_t WKI_CHAN_EVENT_BUS = 2;
constexpr uint16_t WKI_CHAN_RESOURCE = 3;
constexpr uint16_t WKI_CHAN_DYNAMIC_BASE = 16;

// -----------------------------------------------------------------------------
// Sequence Number Arithmetic (RFC 1982)
// -----------------------------------------------------------------------------

inline bool seq_before(uint32_t a, uint32_t b) { return static_cast<int32_t>(a - b) < 0; }

inline bool seq_after(uint32_t a, uint32_t b) { return seq_before(b, a); }

inline bool seq_between(uint32_t seq, uint32_t low, uint32_t high) { return !seq_before(seq, low) && seq_before(seq, high); }

// -----------------------------------------------------------------------------
// WKI Header — 32 bytes, fixed size, RDMA-aligned
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
    uint32_t reserved;
} __attribute__((packed));

static_assert(sizeof(WkiHeader) == 32, "WkiHeader must be 32 bytes");

// -----------------------------------------------------------------------------
// HELLO / HELLO_ACK Payload — 32 bytes
// -----------------------------------------------------------------------------

constexpr uint16_t WKI_CAP_RDMA_SUPPORT = 0x0001;
constexpr uint16_t WKI_CAP_ZONE_SUPPORT = 0x0002;

struct HelloPayload {
    uint32_t magic;  // WKI_HELLO_MAGIC
    uint16_t protocol_version;
    uint16_t node_id;                 // sender's claimed node ID
    std::array<uint8_t, 6> mac_addr;  // sender's MAC (for Ethernet transport)
    uint16_t capabilities;            // bitmask: WKI_CAP_*
    uint16_t heartbeat_interval_ms;   // proposed heartbeat interval (milliseconds)
    uint16_t max_channels;
    uint32_t rdma_zone_bitmap;  // RDMA zone membership (32 zones max)
    std::array<uint8_t, 8> reserved;
} __attribute__((packed));

static_assert(sizeof(HelloPayload) == 32, "HelloPayload must be 32 bytes");

// -----------------------------------------------------------------------------
// HEARTBEAT Payload — 16 bytes
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
// LSA (Link-State Advertisement) Payload — variable length
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

// -----------------------------------------------------------------------------
// FENCE_NOTIFY Payload — 8 bytes
// -----------------------------------------------------------------------------

struct FenceNotifyPayload {
    uint16_t fenced_node;   // node that was fenced
    uint16_t fencing_node;  // node that performed the fencing
    uint32_t reason;        // 0 = heartbeat timeout, 1 = manual
} __attribute__((packed));

// -----------------------------------------------------------------------------
// RECONCILE_REQ / RECONCILE_ACK Payload — 8 bytes
// -----------------------------------------------------------------------------

struct ReconcilePayload {
    uint16_t node_id;
    uint16_t num_resources;  // count of resource adverts to follow
    uint32_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// RESOURCE_ADVERT / RESOURCE_WITHDRAW Payload — variable length
// -----------------------------------------------------------------------------

enum class ResourceType : uint16_t {  // NOLINT(performance-enum-size)
    BLOCK = 1,
    CHAR = 2,
    NET = 3,
    VFS = 4,
    COMPUTE = 5,
    CUSTOM = 6,
};

constexpr uint8_t RESOURCE_FLAG_SHAREABLE = 0x01;
constexpr uint8_t RESOURCE_FLAG_PASSTHROUGH_CAPABLE = 0x02;

struct ResourceAdvertPayload {
    uint16_t node_id;        // owner node
    uint16_t resource_type;  // ResourceType enum
    uint32_t resource_id;    // unique on owning node
    uint8_t flags;           // RESOURCE_FLAG_*
    uint8_t name_len;
    // Followed by name_len bytes of name (e.g., "sda", "eth0")
} __attribute__((packed));

inline auto resource_advert_name(ResourceAdvertPayload* p) -> char* {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(p) + sizeof(ResourceAdvertPayload));
}

inline auto resource_advert_name(const ResourceAdvertPayload* p) -> const char* {
    return reinterpret_cast<const char*>(reinterpret_cast<const uint8_t*>(p) + sizeof(ResourceAdvertPayload));
}

// -----------------------------------------------------------------------------
// ZONE_CREATE_REQ Payload — 16 bytes
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
// ZONE_CREATE_ACK Payload — 24 bytes
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
// ZONE_DESTROY Payload — 8 bytes
// -----------------------------------------------------------------------------

struct ZoneDestroyPayload {
    uint32_t zone_id;
    uint32_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// ZONE_NOTIFY_PRE / ZONE_NOTIFY_POST Payload — 16 bytes
// -----------------------------------------------------------------------------

struct ZoneNotifyPayload {
    uint32_t zone_id;
    uint32_t offset;  // region offset being accessed
    uint32_t length;  // length of the access
    uint8_t op_type;  // 0 = READ, 1 = WRITE
    std::array<uint8_t, 3> reserved;
} __attribute__((packed));

static_assert(sizeof(ZoneNotifyPayload) == 16, "ZoneNotifyPayload must be 16 bytes");

// ZONE_NOTIFY_PRE_ACK / ZONE_NOTIFY_POST_ACK Payload — 4 bytes
struct ZoneNotifyAckPayload {
    uint32_t zone_id;
} __attribute__((packed));

static_assert(sizeof(ZoneNotifyAckPayload) == 4, "ZoneNotifyAckPayload must be 4 bytes");

// -----------------------------------------------------------------------------
// ZONE_READ_REQ Payload — 12 bytes
// -----------------------------------------------------------------------------

struct ZoneReadReqPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// ZONE_READ_RESP / ZONE_WRITE_REQ — variable length
// Data follows immediately after the fixed portion
// -----------------------------------------------------------------------------

struct ZoneReadRespPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    // Followed by `length` bytes of data
} __attribute__((packed));

struct ZoneWriteReqPayload {
    uint32_t zone_id;
    uint32_t offset;
    uint32_t length;
    // Followed by `length` bytes of data
} __attribute__((packed));

// ZONE_WRITE_ACK uses ZoneDestroyPayload-like format (just zone_id + status)
struct ZoneWriteAckPayload {
    uint32_t zone_id;
    int32_t status;  // 0 = success, negative = error
} __attribute__((packed));

// -----------------------------------------------------------------------------
// EVENT_SUBSCRIBE / EVENT_UNSUBSCRIBE Payload — 8 bytes
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
// EVENT_PUBLISH Payload — variable length
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

// EVENT_ACK — just echoes the event identity
struct EventAckPayload {
    uint16_t event_class;
    uint16_t event_id;
    uint16_t origin_node;
    uint16_t reserved;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// DEV_ATTACH_REQ Payload — 12 bytes
// -----------------------------------------------------------------------------

enum class AttachMode : uint8_t {
    PROXY = 0,
    PASSTHROUGH = 1,
};

struct DevAttachReqPayload {
    uint16_t target_node;
    uint16_t resource_type;  // ResourceType enum
    uint32_t resource_id;
    uint8_t attach_mode;  // AttachMode
    uint8_t reserved;
    uint16_t requested_channel;  // 0 = auto-assign
} __attribute__((packed));

static_assert(sizeof(DevAttachReqPayload) == 12, "DevAttachReqPayload must be 12 bytes");

// -----------------------------------------------------------------------------
// DEV_ATTACH_ACK Payload — 8 bytes
// -----------------------------------------------------------------------------

enum class DevAttachStatus : uint8_t {
    OK = 0,
    NOT_FOUND = 1,
    NOT_REMOTABLE = 2,
    BUSY = 3,
    NO_PASSTHROUGH = 4,
};

struct DevAttachAckPayload {
    uint8_t status;  // DevAttachStatus
    uint8_t reserved;
    uint16_t assigned_channel;
    uint16_t max_op_size;  // max payload size for DEV_OP_REQ
    uint16_t reserved2;
} __attribute__((packed));

static_assert(sizeof(DevAttachAckPayload) == 8, "DevAttachAckPayload must be 8 bytes");

// -----------------------------------------------------------------------------
// DEV_DETACH Payload — 8 bytes
// -----------------------------------------------------------------------------

struct DevDetachPayload {
    uint16_t target_node;
    uint16_t resource_type;
    uint32_t resource_id;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// DEV_OP_REQ / DEV_OP_RESP Payload — variable length
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

// Well-known operation IDs for default marshaling
constexpr uint16_t OP_BLOCK_READ = 0x0100;
constexpr uint16_t OP_BLOCK_WRITE = 0x0101;
constexpr uint16_t OP_BLOCK_FLUSH = 0x0102;

constexpr uint16_t OP_CHAR_OPEN = 0x0200;
constexpr uint16_t OP_CHAR_CLOSE = 0x0201;
constexpr uint16_t OP_CHAR_READ = 0x0202;
constexpr uint16_t OP_CHAR_WRITE = 0x0203;

constexpr uint16_t OP_NET_XMIT = 0x0300;
constexpr uint16_t OP_NET_SET_MAC = 0x0301;
constexpr uint16_t OP_NET_RX_NOTIFY = 0x0302;
constexpr uint16_t OP_NET_GET_STATS = 0x0303;

constexpr uint16_t OP_VFS_OPEN = 0x0400;
constexpr uint16_t OP_VFS_READ = 0x0401;
constexpr uint16_t OP_VFS_WRITE = 0x0402;
constexpr uint16_t OP_VFS_CLOSE = 0x0403;
constexpr uint16_t OP_VFS_READDIR = 0x0404;
constexpr uint16_t OP_VFS_STAT = 0x0405;
constexpr uint16_t OP_VFS_MKDIR = 0x0406;
constexpr uint16_t OP_VFS_READLINK = 0x0407;  // D8: symlink target resolution
constexpr uint16_t OP_VFS_SYMLINK = 0x0408;   // D8: symlink creation

// -----------------------------------------------------------------------------
// DEV_IRQ_FWD Payload — 8 bytes
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
// TASK_SUBMIT Payload — variable length
// -----------------------------------------------------------------------------

enum class TaskDeliveryMode : uint8_t {
    INLINE = 0,
    VFS_REF = 1,
    RESOURCE_REF = 2,
};

struct TaskSubmitPayload {
    uint32_t task_id;
    uint8_t delivery_mode;  // TaskDeliveryMode
    uint8_t reserved;
    uint16_t args_len;
    // Variable portion depends on delivery_mode:
    //   INLINE:       uint32_t binary_len, binary[binary_len], args[args_len]
    //   VFS_REF:      uint16_t path_len, path[path_len], args[args_len]
    //   RESOURCE_REF: uint16_t ref_node_id, uint32_t ref_resource_id,
    //                 uint16_t path_len, path[path_len], args[args_len]
} __attribute__((packed));

// -----------------------------------------------------------------------------
// TASK_ACCEPT / TASK_REJECT Payload — 16 bytes
// -----------------------------------------------------------------------------

enum class TaskRejectReason : uint8_t {
    ACCEPTED = 0,
    OVERLOADED = 1,
    NO_MEM = 2,
    BINARY_NOT_FOUND = 3,
    FETCH_FAILED = 4,
};

struct TaskResponsePayload {
    uint32_t task_id;
    uint8_t status;  // TaskRejectReason (0 = accepted)
    std::array<uint8_t, 3> reserved;
    uint64_t remote_pid;  // PID on executing node (if accepted)
} __attribute__((packed));

static_assert(sizeof(TaskResponsePayload) == 16, "TaskResponsePayload must be 16 bytes");

// -----------------------------------------------------------------------------
// TASK_COMPLETE Payload — variable length
// -----------------------------------------------------------------------------

struct TaskCompletePayload {
    uint32_t task_id;
    int32_t exit_status;
    uint16_t output_len;
    uint16_t reserved;
    // Followed by output_len bytes of captured output
} __attribute__((packed));

// -----------------------------------------------------------------------------
// TASK_CANCEL Payload — 4 bytes
// -----------------------------------------------------------------------------

struct TaskCancelPayload {
    uint32_t task_id;
} __attribute__((packed));

// -----------------------------------------------------------------------------
// LOAD_REPORT Payload — variable length
// -----------------------------------------------------------------------------

struct LoadReportPayload {
    uint16_t num_cpus;
    uint16_t runnable_tasks;
    uint16_t avg_load_pct;    // 0-1000
    uint16_t free_mem_pages;  // in units of 256 pages
    // Followed by num_cpus * uint16_t per-CPU load values
} __attribute__((packed));

static_assert(sizeof(LoadReportPayload) == 8, "LoadReportPayload must be 8 bytes");

inline auto load_report_per_cpu(LoadReportPayload* p) -> uint16_t* {
    return reinterpret_cast<uint16_t*>(reinterpret_cast<uint8_t*>(p) + sizeof(LoadReportPayload));
}

inline auto load_report_per_cpu(const LoadReportPayload* p) -> const uint16_t* {
    return reinterpret_cast<const uint16_t*>(reinterpret_cast<const uint8_t*>(p) + sizeof(LoadReportPayload));
}

}  // namespace ker::net::wki
