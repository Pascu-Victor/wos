#pragma once

#include <atomic>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

constexpr uint32_t WKI_DOORBELL_IPC_BASE = 0x00070000;  // doorbell value base for IPC primitives
constexpr uint32_t WKI_DOORBELL_IPC_MASK = 0xFFFF0000;  // mask to identify IPC doorbells
constexpr uint32_t WKI_IPC_RESOURCE_MASK = 0x0000FFFF;  // lower 16 bits = resource_id

constexpr size_t WKI_IPC_MAX_EXPORTS = 64;
constexpr size_t WKI_IPC_MAX_PROXIES = 64;

// RDMA shared region sizes
constexpr uint32_t WKI_IPC_PIPE_REGION_SIZE = 65536;  // 64 KB pipe ring buffer
constexpr uint32_t WKI_IPC_EVENTFD_REGION_SIZE = 64;  // one cache line
constexpr uint32_t WKI_IPC_FUTEX_REGION_SIZE = 64;    // one waiter block
constexpr uint32_t WKI_IPC_EPOLL_REGION_SIZE = 512;   // readiness bitmap

// Pipe spin cycles before sleeping (hybrid spin-then-sleep)
constexpr uint32_t WKI_PIPE_SPIN_CYCLES = 1000;

// -----------------------------------------------------------------------------
// RDMA Shared Region Layouts
// -----------------------------------------------------------------------------

struct WkiPipeSharedRegion {
    std::atomic<uint32_t> head;     // producer write pos
    std::atomic<uint32_t> tail;     // consumer read pos
    std::atomic<uint32_t> credits;  // sender's credit view
    uint32_t capacity;
    uint32_t flags;  // WRITE_CLOSED | READ_CLOSED
    uint32_t _pad[3];
    // Followed by `capacity` bytes of ring data (power of 2, page-aligned)
};

static_assert(sizeof(WkiPipeSharedRegion) == 32, "WkiPipeSharedRegion header must be 32 bytes");

constexpr uint32_t WKI_PIPE_FLAG_WRITE_CLOSED = 0x01;
constexpr uint32_t WKI_PIPE_FLAG_READ_CLOSED = 0x02;

struct WkiEventfdSharedRegion {
    std::atomic<uint64_t> counter;
    uint64_t _pad[7];
};

static_assert(sizeof(WkiEventfdSharedRegion) == 64, "WkiEventfdSharedRegion must be 64 bytes");

struct WkiFutexWaiterBlock {
    uint64_t phys_addr;
    uint32_t expected_val;
    uint32_t waiter_pid;
    std::atomic<uint32_t> woken;
    uint32_t seqlock;
    uint64_t _pad[2];
};

static_assert(sizeof(WkiFutexWaiterBlock) == 40, "WkiFutexWaiterBlock must be 40 bytes");

struct WkiEpollSharedRegion {
    uint64_t readiness[8];  // 512-bit bitmap
    uint64_t last_seen[8];  // shadow bitmap for edge-triggered
    uint64_t generation;    // incremented by waker
    uint64_t _pad[7];
};

static_assert(sizeof(WkiEpollSharedRegion) == 192, "WkiEpollSharedRegion must be 192 bytes");

struct WkiSockControlRegion {
    std::atomic<size_t> consumer_ptr;
    uint32_t flags;  // SHUTDOWN_RD | SHUTDOWN_WR | CLOSED
    uint32_t _pad[1];
};

constexpr uint32_t WKI_SOCK_FLAG_SHUTDOWN_RD = 0x01;
constexpr uint32_t WKI_SOCK_FLAG_SHUTDOWN_WR = 0x02;
constexpr uint32_t WKI_SOCK_FLAG_CLOSED = 0x04;

// Union of all shared region types for generic handling
union WkiIpcSharedRegion {
    WkiPipeSharedRegion pipe;
    WkiEventfdSharedRegion eventfd;
    WkiFutexWaiterBlock futex;
    WkiEpollSharedRegion epoll;
    WkiSockControlRegion sock;
};

// -----------------------------------------------------------------------------
// ProxyIpcState (consumer/remote side) - per-fd proxy state
// Uses a local ring buffer filled by incoming wire messages from the home node.
// -----------------------------------------------------------------------------

constexpr uint32_t WKI_PROXY_PIPE_RING_SIZE = 65536;  // 64 KB local ring buffer

struct ProxyIpcState {
    std::atomic<bool> active{false};
    ResourceType res_type = ResourceType::CUSTOM;
    uint16_t home_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;

    // Local ring buffer for pipe data (filled by wire message handler)
    uint8_t* ring_buf = nullptr;  // allocated ring data
    uint32_t ring_capacity = 0;
    std::atomic<uint32_t> ring_head{0};     // writer (message handler) position
    std::atomic<uint32_t> ring_tail{0};     // reader (proxy_pipe_read) position
    std::atomic<uint32_t> write_closed{0};  // set when home sends EOF/close

    // Blocked reader task (for wakeup from message handler)
    std::atomic<ker::mod::sched::task::Task*> blocked_reader{nullptr};

    ker::mod::sys::Spinlock lock;

    // Reference count: one per live fops user (read, close, message handler).
    // Initialized to 1 (held by the File that owns this proxy).
    // The closer that drives it to 0 frees ring_buf and this struct.
    std::atomic<int> refcount{1};

    ProxyIpcState() = default;
    ProxyIpcState(const ProxyIpcState&) = delete;
    auto operator=(const ProxyIpcState&) -> ProxyIpcState& = delete;
};

// -----------------------------------------------------------------------------
// WkiIpcExport (server/home side) - per-exported IPC primitive
// -----------------------------------------------------------------------------

struct WkiIpcExport {
    bool active = false;
    uint32_t resource_id = 0;
    ResourceType res_type = ResourceType::CUSTOM;
    ker::vfs::File* file = nullptr;  // refcount bumped; original file kept alive
    uint16_t assigned_channel = 0;
    uint16_t consumer_node = WKI_NODE_INVALID;

    // Server-side pump state
    std::atomic<bool> pump_running{false};
    ker::mod::sched::task::Task* pump_task = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the IPC proxy subsystem. Called from wki_init().
void wki_ipc_subsystem_init();

// Export a task's IPC fds before remote submission.
// Iterates fd_table, identifies IPC primitives, creates exports.
// Returns the number of exported fds, fills map_out.
auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, WkiIpcFdEntry* map_out, uint16_t* count_out) -> bool;

// Attach IPC proxy fds on the remote task after TASK_SUBMIT.
// Replaces File::fops with proxy fops for each IPC fd.
void wki_ipc_attach_task_fds(ker::mod::sched::task::Task* task, const WkiIpcFdEntry* map, uint16_t count);

// Doorbell RX handler — called from ISR when IPC doorbell arrives
void wki_ipc_doorbell_rx(uint16_t src_node, uint32_t doorbell_value);

// Handle incoming DEV_OP_REQ for IPC (pipe data, close, etc.)
void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

// DEV_OP response handler for IPC control ops
void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

// Cleanup all IPC exports/proxies for a fenced peer
void wki_ipc_cleanup_for_peer(uint16_t node_id);

// -----------------------------------------------------------------------------
// Internal - RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_ipc_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_ipc_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_ipc_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
