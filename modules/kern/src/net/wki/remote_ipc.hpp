#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

#include "platform/sched/task.hpp"

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
    std::array<uint32_t, 3> pad;
    // Followed by `capacity` bytes of ring data (power of 2, page-aligned)
};

static_assert(sizeof(WkiPipeSharedRegion) == 32, "WkiPipeSharedRegion header must be 32 bytes");
static_assert(offsetof(WkiPipeSharedRegion, pad) == 20, "WkiPipeSharedRegion padding offset must remain stable");

constexpr uint32_t WKI_PIPE_FLAG_WRITE_CLOSED = 0x01;
constexpr uint32_t WKI_PIPE_FLAG_READ_CLOSED = 0x02;

struct WkiEventfdSharedRegion {
    std::atomic<uint64_t> counter;
    std::array<uint64_t, 7> pad;
};

static_assert(sizeof(WkiEventfdSharedRegion) == 64, "WkiEventfdSharedRegion must be 64 bytes");
static_assert(offsetof(WkiEventfdSharedRegion, pad) == 8, "WkiEventfdSharedRegion padding offset must remain stable");

struct WkiFutexWaiterBlock {
    uint64_t phys_addr;
    uint32_t expected_val;
    uint32_t waiter_pid;
    std::atomic<uint32_t> woken;
    uint32_t seqlock;
    std::array<uint64_t, 2> pad;
};

static_assert(sizeof(WkiFutexWaiterBlock) == 40, "WkiFutexWaiterBlock must be 40 bytes");
static_assert(offsetof(WkiFutexWaiterBlock, pad) == 24, "WkiFutexWaiterBlock padding offset must remain stable");

struct WkiEpollSharedRegion {
    std::array<uint64_t, 8> readiness;  // 512-bit bitmap
    std::array<uint64_t, 8> last_seen;  // shadow bitmap for edge-triggered
    uint64_t generation;                // incremented by waker
    std::array<uint64_t, 7> pad;
};

static_assert(sizeof(WkiEpollSharedRegion) == 192, "WkiEpollSharedRegion must be 192 bytes");
static_assert(offsetof(WkiEpollSharedRegion, last_seen) == 64, "WkiEpollSharedRegion bitmap offsets must remain stable");
static_assert(offsetof(WkiEpollSharedRegion, generation) == 128, "WkiEpollSharedRegion generation offset must remain stable");
static_assert(offsetof(WkiEpollSharedRegion, pad) == 136, "WkiEpollSharedRegion padding offset must remain stable");

struct WkiSockControlRegion {
    std::atomic<size_t> consumer_ptr;
    uint32_t flags;  // SHUTDOWN_RD | SHUTDOWN_WR | CLOSED
    std::array<uint32_t, 1> pad;
};

static_assert(sizeof(WkiSockControlRegion) == 16, "WkiSockControlRegion must be 16 bytes");
static_assert(offsetof(WkiSockControlRegion, pad) == 12, "WkiSockControlRegion padding offset must remain stable");

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

static_assert(sizeof(WkiIpcSharedRegion) == sizeof(WkiEpollSharedRegion), "WkiIpcSharedRegion size must match largest region");

// -----------------------------------------------------------------------------
// ProxyIpcState (consumer/remote side) - per-fd proxy state
// Uses a local ring buffer filled by incoming wire messages from the home node.
// -----------------------------------------------------------------------------

constexpr uint32_t WKI_PROXY_PIPE_RING_SIZE = 262144;  // 256 KB local ring buffer

struct ProxyIpcState {
    std::atomic<bool> active{false};
    ResourceType res_type = ResourceType::CUSTOM;
    uint16_t home_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;

    // Local ring buffer for pipe data (filled by wire message handler)
    uint8_t* ring_buf = nullptr;  // allocated ring data
    uint32_t ring_capacity = 0;
    std::atomic<uint32_t> ring_head{0};      // writer (message handler) position
    std::atomic<uint32_t> ring_tail{0};      // reader (proxy_pipe_read) position
    std::atomic<uint32_t> write_closed{0};   // set when home sends EOF/close
    std::atomic<uint64_t> bytes_written{0};  // bytes accepted by proxy_pipe_write()

    // Blocked reader task (for wakeup from message handler)
    std::atomic<ker::mod::sched::task::Task*> blocked_reader{nullptr};

    // poll()/epoll_wait() waiters registered on this proxy fd.
    ker::util::SmallVec<uint64_t, 2> poll_waiters;

    ker::mod::sys::Spinlock lock;

    // Synchronous control-op wait state (socket control ops: SHUTDOWN, GETPEERNAME, etc.)
    // Protected by lock; only one in-flight control op per proxy at a time.
    WkiWaitEntry* pending_wait = nullptr;
    uint16_t pending_wait_op = 0;
    int pending_wait_status = 0;
    static constexpr size_t SOCK_CTRL_RESP_MAX = 128;
    uint8_t pending_wait_resp[SOCK_CTRL_RESP_MAX] = {};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                                                         // Kept as a raw C buffer for existing memcpy/fops boundaries.
    uint16_t pending_wait_resp_len = 0;

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
    uint64_t pipe_bytes_received = 0;

    // Server-side pump state
    std::atomic<bool> pump_running{false};
    ker::mod::sched::task::Task* pump_task = nullptr;
    bool pump_queued = false;
    WkiIpcExport* pump_next = nullptr;
};

struct WkiIpcPerfSnapshot {
    uint64_t exports = 0;
    uint64_t proxies = 0;
    uint64_t pump_tasks = 0;
    uint64_t proxy_ring_bytes = 0;
    uint64_t proxy_ring_used_bytes = 0;
    uint64_t blocked_readers = 0;
    uint64_t poll_waiters = 0;
    uint64_t pending_deliveries = 0;
    uint64_t pending_chunks = 0;
    uint64_t pending_bytes = 0;
    uint64_t export_backlogs = 0;
    uint64_t export_backlog_chunks = 0;
    uint64_t export_backlog_bytes = 0;
    uint64_t export_flush_queue = 0;
    uint64_t dev_op_queue = 0;
    uint64_t dev_op_payload_bytes = 0;
    uint64_t approx_alloc_bytes = 0;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the IPC proxy subsystem. Called from wki_init().
void wki_ipc_subsystem_init();

// Read a best-effort IPC perf/memory snapshot for /proc and perf tooling.
void wki_ipc_get_perf_snapshot(WkiIpcPerfSnapshot& out);

// Export a task's IPC fds before remote submission.
// Iterates fd_table, identifies IPC primitives, creates exports.
// Returns the number of exported fds, fills map_out.
auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, WkiIpcFdEntry* map_out, uint16_t* count_out) -> bool;

// Tear down exports created by wki_ipc_export_task_fds() for one submitted task.
// This is idempotent and is used when submit/cancel/complete cleanup must not
// depend on the remote proxy close packet making it back to the home node.
void wki_ipc_cleanup_exported_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node);

// If one of task's local pipe fds already has its sibling exported to a remote
// node, return that node so pipeline stages can stay co-located.
auto wki_ipc_find_pipe_affinity_node(const ker::mod::sched::task::Task* task, uint16_t* node_out) -> bool;

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

// Shared proxy teardown helper for non-pipe proxy fops.
void wki_ipc_detach_proxy_file(ker::vfs::File* f, ProxyIpcState* proxy);

// Shared proxy poll waiter helpers for pipe/socket/pty proxy fops.
auto wki_ipc_proxy_register_poll_waiter(ProxyIpcState* proxy, uint64_t pid) -> bool;
void wki_ipc_proxy_wake_poll_waiters(ProxyIpcState* proxy);

// Socket proxy fops (implemented in remote_ipc_socket.cpp).
extern ker::vfs::FileOperations g_proxy_socket_fops;

// Socket proxy helpers for net syscalls operating on inherited remote sockets.
auto wki_ipc_is_socket_proxy_file(const ker::vfs::File* file) -> bool;
auto wki_ipc_socket_shutdown(ker::vfs::File* file, int how) -> int;
auto wki_ipc_socket_getpeername(ker::vfs::File* file, void* addr_out, size_t* addr_len) -> int;
auto wki_ipc_socket_getsockopt(ker::vfs::File* file, int level, int optname, void* optval, size_t* optlen) -> int;
auto wki_ipc_socket_setsockopt(ker::vfs::File* file, int level, int optname, const void* optval, size_t optlen) -> int;

// Socket-specific DEV_OP_RESP handling hook.
void wki_ipc_socket_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

// Forward a local epoll_ctl() on an IPC_EPOLL proxy fd to the home node.
// Returns -EOPNOTSUPP when epfile is not an IPC_EPOLL proxy.
auto wki_ipc_epoll_ctl_forward(ker::vfs::File* epfile, int op, int fd, uint32_t events, uint64_t data) -> int;

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
