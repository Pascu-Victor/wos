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
constexpr uint16_t WKI_IPC_OP_COOKIE_BYTES = sizeof(uint16_t);

constexpr size_t WKI_IPC_MAX_EXPORTS = 64;
constexpr size_t WKI_IPC_MAX_PROXIES = 64;

// RDMA shared region sizes
constexpr uint32_t WKI_IPC_PIPE_REGION_SIZE = 65536;  // 64 KB pipe ring buffer
constexpr size_t WKI_IPC_DIAG_MAX = 128;
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

constexpr uint32_t WKI_PROXY_PIPE_RING_SIZE = 1024 * 1024;  // 1 MB local ring buffer

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

    // Optional proxy-write RDMA pipe fast path.  The home/export side owns the
    // receive ring; proxy writers serialize reservations with writer_active so
    // byte order remains identical to the message path.
    bool pipe_rdma_enabled = false;
    uint32_t pipe_rdma_rkey = 0;
    uint32_t pipe_rdma_capacity = 0;
    uint32_t pipe_rdma_head = 0;
    uint32_t pipe_rdma_tail_cache = 0;
    WkiTransport* pipe_rdma_transport = nullptr;
    std::atomic<bool> pipe_rdma_writer_active{false};

    // Synchronous control-op wait state (socket control ops: SHUTDOWN, GETPEERNAME, etc.)
    // Protected by lock; only one in-flight control op per proxy at a time.
    WkiWaitEntry* pending_wait = nullptr;
    uint16_t pending_wait_op = 0;
    uint16_t pending_wait_cookie = 0;
    uint16_t next_wait_cookie = 1;
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

inline auto wki_ipc_allocate_wait_cookie_locked(ProxyIpcState* proxy) -> uint16_t {
    if (proxy == nullptr) {
        return 0;
    }
    uint16_t cookie = proxy->next_wait_cookie;
    if (cookie == 0) {
        cookie = 1;
    }
    proxy->next_wait_cookie = static_cast<uint16_t>(cookie + 1U);
    if (proxy->next_wait_cookie == 0) {
        proxy->next_wait_cookie = 1;
    }
    return cookie;
}

constexpr auto wki_ipc_response_matches_pending(uint16_t response_op, uint16_t response_cookie, const ProxyIpcState& proxy) -> bool {
    return proxy.pending_wait != nullptr && proxy.pending_wait_op == response_op && proxy.pending_wait_cookie == response_cookie;
}

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

    // Optional proxy-write RDMA receive ring.  This is intentionally separate
    // from the local pipe backlog so bulk bytes can arrive without consuming
    // WKI_CHAN_IPC_DATA payload credits.
    bool pipe_rdma_enabled = false;
    uint8_t* pipe_rdma_region = nullptr;
    uint32_t pipe_rdma_region_size = 0;
    uint32_t pipe_rdma_capacity = 0;
    uint32_t pipe_rdma_rkey = 0;
    WkiTransport* pipe_rdma_transport = nullptr;

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
    uint64_t export_close_pending = 0;
    uint64_t export_close_waiting_for_bytes = 0;
    uint64_t export_flush_queue = 0;
    uint64_t proxy_close_queue = 0;
    uint64_t proxy_close_attempts = 0;
    uint64_t dev_op_queue = 0;
    uint64_t dev_op_payload_bytes = 0;
    uint64_t proxy_write_payload_bytes = 0;
    uint64_t proxy_write_no_credit_waits = 0;
    uint64_t proxy_write_block_us = 0;
    uint64_t proxy_pipe_rdma_full_waits = 0;
    uint64_t proxy_ring_full_waits = 0;
    uint64_t proxy_ring_full_bytes = 0;
    uint64_t pipe_payload_bytes = 0;
    uint64_t approx_alloc_bytes = 0;
};

enum class WkiIpcDiagKind : uint8_t {
    UNKNOWN = 0,
    EXPORT = 1,
    PROXY = 2,
    EXPORT_BACKLOG = 3,
    PROXY_CLOSE = 4,
};

struct WkiIpcDiagCounts {
    size_t exports = 0;
    size_t proxies = 0;
    size_t export_backlogs = 0;
    size_t proxy_close_queue = 0;
    size_t truncated = 0;
};

struct WkiIpcDiagRow {
    WkiIpcDiagKind kind = WkiIpcDiagKind::EXPORT;
    uint32_t resource_id = 0;
    uint16_t peer_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint16_t res_type = 0;
    uint16_t op_id = 0;
    uint16_t msg_size = 0;
    uint32_t attempts = 0;
    int open_flags = 0;
    bool active = false;
    bool has_file = false;
    bool pump_running = false;
    bool pump_queued = false;
    bool has_pump_task = false;
    bool write_closed = false;
    bool close_pending = false;
    bool close_has_expected_bytes = false;
    uint64_t pipe_bytes_received = 0;
    uint64_t proxy_bytes_written = 0;
    uint64_t ring_used = 0;
    uint64_t ring_capacity = 0;
    uint64_t blocked_reader_pid = 0;
    uint64_t poll_waiters = 0;
    uint64_t backlog_bytes = 0;
    uint64_t backlog_chunks = 0;
    uint64_t close_expected_bytes = 0;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the IPC proxy subsystem. Called from wki_init().
void wki_ipc_subsystem_init();

// Read a best-effort IPC perf/memory snapshot for /proc and perf tooling.
void wki_ipc_get_perf_snapshot(WkiIpcPerfSnapshot& out);

// Read bounded IPC diagnostic rows for /proc/wki/netdiag.
auto wki_ipc_diag_snapshot(WkiIpcDiagRow* rows, size_t capacity, WkiIpcDiagCounts* counts) -> size_t;

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

// Doorbell RX handler — called from ISR when IPC doorbell arrives.
// Returns true when the doorbell matched a live IPC RDMA pipe export.
auto wki_ipc_doorbell_rx(uint16_t src_node, uint32_t doorbell_value) -> bool;

// Handle incoming DEV_OP_REQ for IPC (pipe data, close, etc.)
void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

// DEV_OP response handler for IPC control ops
void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

#ifdef WOS_SELFTEST
auto wki_ipc_selftest_poll_state_response_refs() -> int;
auto wki_ipc_selftest_export_compaction_frees() -> int;
auto wki_ipc_selftest_cleanup_for_peer_drains_over_capacity() -> int;
auto wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops() -> int;
auto wki_ipc_selftest_poll_wake_drains_over_capacity() -> int;
auto wki_ipc_selftest_inactive_proxy_poll_is_terminal() -> int;
auto wki_ipc_selftest_pty_proxy_poll_is_bidirectional() -> int;
auto wki_ipc_selftest_pty_close_without_export_queues_pending() -> int;
auto wki_ipc_selftest_pending_close_promotes_on_poll() -> int;
auto wki_ipc_selftest_epoll_close_releases_lookup_ref() -> int;
auto wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags() -> int;
auto wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode() -> int;
auto wki_ipc_selftest_attach_insert_failure_preserves_existing_fd() -> int;
auto wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion() -> int;
auto wki_ipc_selftest_dev_op_response_uses_home_node_identity() -> int;
auto wki_ipc_selftest_dev_op_response_keeps_slot_busy_until_consumed() -> int;
#endif

// Cleanup all IPC exports/proxies for a fenced peer
void wki_ipc_cleanup_for_peer(uint16_t node_id);

// Shared proxy teardown helper for non-pipe proxy fops.
void wki_ipc_detach_proxy_file(ker::vfs::File* f, ProxyIpcState* proxy);

// Cancel a locally published proxy control waiter before the caller unwinds.
// Safe when a send failed before wki_wait_for_op() started sleeping; if a
// concurrent close/fence path already claimed the waiter, this waits until that
// claim is fully published so the stack entry cannot be used after return.
void wki_ipc_cancel_pending_wait(ProxyIpcState* proxy, WkiWaitEntry* wait, int result);

// Consume side-band status/response data for a proxy control waiter that was
// completed by a DEV_OP_RESP. The op/cookie slot remains busy until this call
// clears it, preventing another control op from overwriting the response before
// the original waiter reads it.
auto wki_ipc_consume_pending_wait_response(ProxyIpcState* proxy, WkiWaitEntry* wait, uint16_t expected_op, uint16_t expected_cookie,
                                           void* resp_buf, uint16_t resp_max, uint16_t* resp_len_out) -> int;

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
