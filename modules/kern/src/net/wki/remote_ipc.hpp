#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/secure_rdma.hpp>
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
constexpr size_t WKI_IPC_MAX_UNANSWERED_CONTROLS = 64;

constexpr size_t WKI_IPC_MAX_EXPORTS = 64;
constexpr size_t WKI_IPC_MAX_PROXIES = 64;
constexpr uint32_t WKI_IPC_MESSAGE_PIPE_WINDOW = 1024 * 1024;

// Legacy region size constants; secure IPC registers private data-only storage.
constexpr uint32_t WKI_IPC_PIPE_REGION_SIZE = 65536;  // 64 KB pipe ring buffer
constexpr size_t WKI_IPC_DIAG_MAX = 128;
constexpr uint32_t WKI_IPC_EVENTFD_REGION_SIZE = 64;  // one cache line
constexpr uint32_t WKI_IPC_FUTEX_REGION_SIZE = 64;    // one waiter block
constexpr uint32_t WKI_IPC_EPOLL_REGION_SIZE = 512;   // readiness bitmap

// Pipe spin cycles before sleeping (hybrid spin-then-sleep)
constexpr uint32_t WKI_PIPE_SPIN_CYCLES = 1000;

// -----------------------------------------------------------------------------
// Legacy layout declarations, never enrolled as secure IPC memory authority.
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
    // Registry membership only, guarded by s_ipc_lock.
    ProxyIpcState* registry_previous = nullptr;
    ProxyIpcState* registry_next = nullptr;
    std::atomic<bool> active{false};
    ResourceType res_type = ResourceType::CUSTOM;
    uint16_t home_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannelIdentity control_channel{};  // immutable admitted submit-channel allocation
    WkiChannelIdentity data_channel{};     // immutable DATA allocation captured under that submit identity
    uint32_t resource_id = 0;

    // Local ring buffer for pipe data (filled by wire message handler)
    bool can_receive_data = false;  // immutable after publication in g_ipc_proxies
    uint8_t* ring_buf = nullptr;    // allocated ring data
    uint32_t ring_capacity = 0;
    std::atomic<uint32_t> ring_head{0};        // writer (message handler) position
    std::atomic<uint32_t> ring_tail{0};        // reader (proxy_pipe_read) position
    std::atomic<uint32_t> write_closed{0};     // set when home sends EOF/close
    std::atomic<uint64_t> bytes_written{0};    // bytes accepted by proxy_pipe_write()
    std::atomic<uint64_t> bytes_delivered{0};  // application-credit receipts, excluding local refunds
    uint16_t completion_refs = 0;              // guarded by s_ipc_lock; preserves receipt lookup after File close

    // Legacy unflagged socket reader (for wakeup from message handler)
    std::atomic<ker::mod::sched::task::Task*> blocked_reader{nullptr};

    // Shared stream readers. PIDs avoid retaining raw Task pointers; lock
    // guards slots. Overflow readers retry outside locks instead of allocating.
    static constexpr size_t MAX_READ_WAITERS = 32;
    std::array<uint64_t, MAX_READ_WAITERS> read_waiters{};

    // poll()/epoll_wait() waiters registered on this proxy fd.
    ker::util::SmallVec<uint64_t, 2> poll_waiters;
    // Message-based write proxies use application credits so transport RX
    // completion cannot outrun the exported local pipe by an unbounded amount.
    bool message_write_flow_control = false;
    std::atomic<uint32_t> message_write_credits{0};
    std::atomic<int32_t> message_write_error{0};
    ker::util::SmallVec<uint64_t, 2> message_write_waiters;

    ker::mod::sys::Spinlock lock;

    // Secure IPC candidate: rkey is a current full-grant lookup hint only.
    // Whole-call writer loans serialize QUERY/APPEND and zero-effect fallback.
    bool pipe_rdma_enabled = false;
    uint32_t pipe_rdma_rkey = 0;
    uint32_t pipe_rdma_capacity = 0;
    uint64_t pipe_rdma_head = 0;
    uint64_t pipe_rdma_tail_cache = 0;
    std::atomic<bool> pipe_secure_uncertain{false};
    std::atomic<bool> pipe_rdma_writer_active{false};

    // Synchronous control-op wait state (socket control ops: SHUTDOWN, GETPEERNAME, etc.)
    // Protected by lock; only one in-flight control op per proxy at a time.
    WkiWaitEntry* pending_wait = nullptr;
    uint16_t pending_wait_op = 0;
    uint16_t pending_wait_cookie = 0;
    uint16_t next_wait_cookie = 1;
    struct ControlIdentity {
        uint16_t op = 0;
        uint16_t cookie = 0;
    };
    // Includes the current request and timed-out requests whose reply may
    // still arrive. Protected by lock; only a matching reply or a definitive
    // failure before reliable-send acceptance releases an identity.
    std::array<ControlIdentity, WKI_IPC_MAX_UNANSWERED_CONTROLS> unanswered_controls{};
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

// One root remote execution owns a bounded set of receipt references, never
// File references. Explicit close still closes its endpoint immediately. The
// first completion scan freezes a finite stream prefix, including writes from
// inherited writers already accepted then, without waiting for their EOF.
class WkiIpcOutputCompletion {
   public:
    WkiIpcOutputCompletion() = default;
    ~WkiIpcOutputCompletion();
    WkiIpcOutputCompletion(const WkiIpcOutputCompletion&) = delete;
    auto operator=(const WkiIpcOutputCompletion&) -> WkiIpcOutputCompletion& = delete;
    void retain_locked(ProxyIpcState* proxy);
    auto ready() -> bool;

   private:
    std::array<ProxyIpcState*, 16> proxies{};
    std::array<uint64_t, 16> targets{};
    size_t count = 0;
    bool frozen = false;
};

inline auto wki_ipc_allocate_wait_cookie_locked(ProxyIpcState* proxy, uint16_t op) -> uint16_t {
    if (proxy == nullptr || op == 0) {
        return 0;
    }
    ProxyIpcState::ControlIdentity* available = nullptr;
    for (auto& identity : proxy->unanswered_controls) {
        if (identity.cookie == 0) {
            available = &identity;
            break;
        }
    }
    if (available == nullptr) return 0;
    uint16_t cookie = proxy->next_wait_cookie;
    // At most63 identities remain reserved, so64 consecutive nonzero
    // candidates suffice even when the sequence crosses UINT16_MAX.
    for (size_t attempt = 0; attempt < WKI_IPC_MAX_UNANSWERED_CONTROLS; ++attempt) {
        if (cookie == 0) cookie = 1;
        bool used = false;
        for (auto const& identity : proxy->unanswered_controls) {
            used |= identity.cookie == cookie;
        }
        auto const NEXT = static_cast<uint16_t>(cookie + 1U);
        if (!used) {
            *available = {.op = op, .cookie = cookie};
            proxy->next_wait_cookie = NEXT == 0 ? 1 : NEXT;
            return cookie;
        }
        cookie = NEXT;
    }
    return 0;
}

inline void wki_ipc_retire_wait_cookie_locked(ProxyIpcState* proxy, uint16_t op, uint16_t cookie) {
    if (proxy == nullptr || cookie == 0) return;
    for (auto& identity : proxy->unanswered_controls) {
        if (identity.op == op && identity.cookie == cookie) {
            identity = {};
            return;
        }
    }
}

constexpr auto wki_ipc_response_matches_pending(uint16_t response_op, uint16_t response_cookie, const ProxyIpcState& proxy) -> bool {
    return proxy.pending_wait != nullptr && proxy.pending_wait_op == response_op && proxy.pending_wait_cookie == response_cookie;
}

// -----------------------------------------------------------------------------
// WkiIpcExport (server/home side) - per-exported IPC primitive
// -----------------------------------------------------------------------------

struct WkiIpcChannels {
    WkiChannelIdentity control{};
    WkiChannelIdentity data{};
};

struct WkiIpcExport {
    // Registry membership only; guarded by s_ipc_lock, distinct from pump_next.
    WkiIpcExport* registry_previous = nullptr;
    WkiIpcExport* registry_next = nullptr;
    bool active = false;
    WkiIpcChannels channels{};  // immutable original grant allocations
    uint32_t resource_id = 0;
    ResourceType res_type = ResourceType::CUSTOM;
    ker::vfs::File* file = nullptr;  // refcount bumped; original file kept alive
    // Same retained PTY endpoint (distinct master/slave) or native stream socket. The opaque identity is
    // compared only while active; queued DATA keeps the never-reused group ID.
    const void* endpoint_identity = nullptr;
    uint32_t endpoint_delivery_group = 0;
    uint16_t assigned_channel = 0;
    uint16_t consumer_node = WKI_NODE_INVALID;
    uint64_t pipe_bytes_received = 0;
    uint64_t pipe_bytes_delivered = 0;
    bool message_write_flow_control = false;
    // The backlog worker or a secure native claim pins the exact exporter/File
    // during VFS writes outside s_ipc_lock. Guarded by that lock.
    bool pipe_write_in_flight = false;

    // Private secure IPC data backing. Head/tail/credit stay owner-local;
    // authenticated complete APPEND consumes bytes through native VFS only.
    bool pipe_rdma_enabled = false;
    uint8_t* pipe_rdma_region = nullptr;
    uint32_t pipe_rdma_region_size = 0;
    uint32_t pipe_rdma_capacity = 0;
    uint32_t pipe_rdma_rkey = 0;
    rdma_protocol::RegionDescriptor pipe_secure_descriptor{};
    uint64_t pipe_secure_lifetime = 0;
    uint64_t pipe_secure_epoch = 0;
    bool pipe_secure_activated = false;
    bool pipe_secure_reserved = false;
    bool pipe_secure_effect_claimed = false;
    WkiSecureRdmaRequest pipe_secure_request{};
    ker::vfs::File* pipe_secure_file = nullptr;

    // Server-side pump state; pending acceptance is protected by s_ipc_lock.
    bool pump_pending_accept = false;
    std::atomic<bool> pump_running{false};
    ker::mod::sched::task::Task* pump_task = nullptr;
    bool pump_queued = false;
    WkiIpcExport* pump_next = nullptr;
};

// Local-only ownership token for the submitter-side fd-table reference.  The
// wire entry identifies the remote proxy; this token pins the exact File until
// an accepted remote exec can transfer the shadow task's matching fd ownership
// to the export.  It is never serialized.
struct WkiIpcTaskFdHandoff {
    uint16_t local_fd = UINT16_MAX;
    ker::vfs::File* file = nullptr;
};

// Executor-linked ownership from export preparation through SubmittedTask
// publication. The arrays/count outlive this guard; export updates count under
// the IPC lock. Disarming transfers ownership without invalidating wire data.
class WkiIpcSubmissionGuard {
   public:
    WkiIpcSubmissionGuard(ker::mod::sched::task::Task* source, const WkiIpcFdEntry* map, WkiIpcTaskFdHandoff* handoff, uint16_t& count,
                          uint16_t consumer, const WkiIpcChannels& channels = {});
    ~WkiIpcSubmissionGuard();
    WkiIpcSubmissionGuard(const WkiIpcSubmissionGuard&) = delete;
    auto operator=(const WkiIpcSubmissionGuard&) -> WkiIpcSubmissionGuard& = delete;
    void arm();
    void cancel();
    void disarm();
    auto channels() const -> const WkiIpcChannels& { return original_channels; }

   private:
    ker::mod::sched::task::Task* source;
    ker::mod::sched::task::Task* executor = nullptr;
    const WkiIpcFdEntry* map;
    WkiIpcTaskFdHandoff* handoff;
    uint16_t& count;
    uint16_t consumer;
    WkiIpcChannels original_channels;
    uint16_t handoff_cursor = 0;
    uint16_t export_cursor = 0;
    bool owns = false;
    WkiIpcSubmissionGuard* previous = nullptr;
    WkiIpcSubmissionGuard* next = nullptr;
};
void wki_ipc_cancel_submission_guards(ker::mod::sched::task::Task& executor);

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
    PENDING_DELIVERY = 5,
    DEV_OP_WORK = 6,
    PEER_CLEANUP = 7,
};

struct WkiIpcDiagCounts {
    size_t exports = 0;
    size_t active_exports = 0;
    size_t proxies = 0;
    size_t active_proxies = 0;
    size_t export_backlogs = 0;
    size_t proxy_close_queue = 0;
    size_t pending_deliveries = 0;
    size_t dev_op_work = 0;
    size_t peer_cleanup_slots = 0;
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
    uint32_t sequence = 0;
    int open_flags = 0;
    int32_t refcount = 0;
    bool active = false;
    bool has_file = false;
    bool pump_running = false;
    bool pump_queued = false;
    bool has_pump_task = false;
    bool write_closed = false;
    bool close_pending = false;
    bool close_has_expected_bytes = false;
    bool has_pending_wait = false;
    bool cleanup_active = false;
    bool fenced = false;
    bool pipe_rdma_enabled = false;
    bool pipe_rdma_writer_active = false;
    uint64_t pipe_bytes_received = 0;
    uint64_t proxy_bytes_written = 0;
    uint64_t ring_used = 0;
    uint64_t ring_capacity = 0;
    uint64_t blocked_reader_pid = 0;
    uint64_t poll_waiters = 0;
    uint64_t backlog_bytes = 0;
    uint64_t backlog_chunks = 0;
    uint64_t close_expected_bytes = 0;
    uint64_t cleanup_epoch = 0;
    uint16_t pending_wait_op = 0;
    uint16_t pending_wait_cookie = 0;
    uint32_t message_write_credits = 0;
    int32_t message_write_error = 0;
    uint32_t message_write_waiters = 0;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the IPC proxy subsystem. Called from wki_init().
void wki_ipc_subsystem_init();
// Task-only, outside peer/channel/IPC gates. False retains native owners until
// flush/pump workers and exact unregister/claim pins have fully retired.
auto wki_ipc_secure_shutdown_drain() -> bool;

enum class WkiIpcDevOpRxAdmission : uint8_t {
    NOT_APPLICABLE,
    INLINE,
    DEFERRED,
    DISCARD,
    RETRY,
};

// Validate and reserve bounded IPC DEV_OP work before reliable RX publishes
// its ACK. RETRY leaves the frame unconsumed; DEFERRED owns a fixed-pool copy.
auto wki_ipc_admit_dev_op_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                             uint32_t rx_channel_generation) -> WkiIpcDevOpRxAdmission;
void wki_ipc_notify_deferred_dev_op(const uint8_t* payload, uint16_t payload_len);

// Read a best-effort IPC perf/memory snapshot for /proc and perf tooling.
void wki_ipc_get_perf_snapshot(WkiIpcPerfSnapshot& out);

// Read bounded IPC diagnostic rows for /proc/wki/netdiag.
auto wki_ipc_diag_snapshot(WkiIpcDiagRow* rows, size_t capacity, WkiIpcDiagCounts* counts) -> size_t;

// Export a task's IPC fds before remote submission.
// Retains a non-CLOEXEC snapshot, identifies IPC primitives, creates exports.
// Returns zero on success or negative errno with count_out zero on admission
// failure. A successful empty set also has count zero. Optional
// stdio_mask_out receives presence bits0..2 from that same snapshot, including
// non-IPC descriptors represented by receiver fallback files.
auto wki_ipc_capture_export_channels(uint16_t node) -> WkiIpcChannels;
auto wki_ipc_exports_match_submit(const WkiIpcFdEntry* map, uint16_t count, uint16_t node, const WkiIpcChannels& channels,
                                  const WkiChannelIdentity& submit) -> bool;
auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, const WkiIpcChannels& channels,
                             WkiIpcFdEntry* map_out, uint16_t* count_out, WkiIpcTaskFdHandoff* handoff_out = nullptr,
                             uint8_t* stdio_mask_out = nullptr) -> int;
// Call only after TASK_ACCEPT and after consuming temporary handoff pins.
// Starts selected read pumps by peer/resource identity; duplicate calls are inert.
void wki_ipc_activate_task_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node);
// ACTIVE task-exit phase, outside compute/IPC/FD locks; releases snapshot pins.
void wki_ipc_cancel_fd_snapshots(ker::mod::sched::task::Task& executor);

// Commit or abandon the local side of an IPC fd handoff.  Commit removes only
// fd slots that still contain the pinned File and drops their fd-table owner;
// release drops only the temporary pins, preserving every local fd on failed
// submission/fallback paths.
auto wki_ipc_commit_task_fd_handoff(ker::mod::sched::task::Task* task, WkiIpcTaskFdHandoff* handoff, uint16_t count) -> uint16_t;
void wki_ipc_release_task_fd_handoff(WkiIpcTaskFdHandoff* handoff, uint16_t count);

// Tear down exports created by wki_ipc_export_task_fds() for one submitted task.
// This is idempotent and is used when submit/cancel/complete cleanup must not
// depend on the remote proxy close packet making it back to the home node.
void wki_ipc_cleanup_exported_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node, const WkiIpcChannels& channels = {});

// If one of task's local pipe fds already has its sibling exported to a remote
// node, return that node so pipeline stages can stay co-located.
auto wki_ipc_find_pipe_affinity_node(const ker::mod::sched::task::Task* task, uint16_t* node_out) -> bool;

// Validate the complete descriptor map against the authenticated submitter.
// Bounded and effect-free: EACCES for provenance, EINVAL for malformed entries.
auto wki_ipc_validate_task_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t source_node) -> int;

// Attach IPC proxy fds on the remote task after TASK_SUBMIT.
// Replaces File::fops with proxy fops for each IPC fd.
// Returns false without changing any descriptor or publishing any proxy.
// Task-context DATA capture bracketed by the admitted RESOURCE generation.
auto wki_ipc_capture_data_channel(const WkiChannelIdentity& control) -> WkiChannelIdentity;
// Snapshot the original DATA allocation without holding any IPC lock.
auto wki_ipc_capture_data_sequence(const WkiChannelIdentity& identity, bool transmit, uint32_t& sequence) -> bool;
auto wki_ipc_attach_task_fds(ker::mod::sched::task::Task* task, const WkiIpcFdEntry* map, uint16_t count, uint16_t source_node,
                             const WkiChannelIdentity& control_channel, const WkiChannelIdentity& data_channel,
                             WkiIpcOutputCompletion* completion = nullptr) -> bool;

// Doorbell RX handler — called from ISR when IPC doorbell arrives.
// Returns true when the doorbell matched a live IPC RDMA pipe export.
auto wki_ipc_doorbell_rx(uint16_t src_node, uint32_t doorbell_value) -> bool;

// Handle incoming DEV_OP_REQ for IPC (pipe data, close, etc.)
void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

// DEV_OP response handler for IPC control ops
void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);

#ifdef WOS_SELFTEST
auto wki_ipc_selftest_poll_state_response_refs() -> int;
auto wki_ipc_selftest_export_consumer_authority() -> int;
auto wki_ipc_selftest_resource_id_exhaustion() -> int;
auto wki_ipc_selftest_import_provenance_preserves_state() -> int;
auto wki_ipc_selftest_unsupported_control_completion() -> int;
auto wki_ipc_selftest_export_compaction_frees() -> int;
auto wki_ipc_selftest_task_fd_handoff_transfers_exact_owner() -> int;
auto wki_ipc_selftest_rdma_write_survives_detachment() -> int;
auto wki_ipc_selftest_accepted_handoff_cancellation() -> int;
auto wki_ipc_selftest_pipe_growth_waits_for_write() -> int;
auto wki_ipc_selftest_submission_guard_cancellation() -> int;
auto wki_ipc_selftest_submission_guard_reentrant_put() -> int;
auto wki_ipc_selftest_private_rdma_preparation_unwinds() -> int;
auto wki_ipc_selftest_private_export_allocation_unwinds() -> int;
auto wki_ipc_selftest_intrusive_export_registry() -> int;
auto wki_ipc_selftest_pumps_wait_for_acceptance() -> int;
auto wki_ipc_selftest_export_admission_is_atomic() -> int;
auto wki_ipc_selftest_fd_snapshot_lifetime_and_cloexec() -> int;
auto wki_ipc_selftest_failed_export_write_releases_all_file_refs() -> int;
auto wki_ipc_selftest_message_pipe_flow_updates_proxy() -> int;
auto wki_ipc_selftest_output_completion_survives_close() -> int;
auto wki_ipc_selftest_pty_delivery_order() -> int;
auto wki_ipc_selftest_socket_delivery_fence() -> int;
auto wki_ipc_selftest_pty_flow_import() -> int;
auto wki_ipc_selftest_socket_flow_import() -> int;
auto wki_ipc_selftest_cleanup_for_peer_drains_over_capacity() -> int;
auto wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops() -> int;
auto wki_ipc_selftest_large_dev_op_work_coallocates_payload() -> int;
auto wki_ipc_selftest_large_dev_op_work_backs_pipe_chunk() -> int;
auto wki_ipc_selftest_dev_op_work_pool_is_bounded() -> int;
auto wki_ipc_selftest_stream_read_contract() -> int;
auto wki_ipc_selftest_stream_reader_waiters_are_bounded() -> int;
auto wki_ipc_selftest_poll_wake_drains_over_capacity() -> int;
auto wki_ipc_selftest_inactive_proxy_poll_is_terminal() -> int;
auto wki_ipc_selftest_pty_proxy_poll_is_bidirectional() -> int;
auto wki_ipc_selftest_pty_close_without_export_queues_pending() -> int;
auto wki_ipc_selftest_pending_close_promotes_on_poll() -> int;
auto wki_ipc_selftest_discard_retires_unattached_delivery() -> int;
auto wki_ipc_selftest_stopped_pump_publishes_discard_fence() -> int;
auto wki_ipc_selftest_epoll_close_releases_lookup_ref() -> int;
auto wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags() -> int;
auto wki_ipc_selftest_export_pipe_write_burst_is_bounded() -> int;
auto wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode() -> int;
auto wki_ipc_selftest_pipe_affinity_uses_read_endpoints_only() -> int;
auto wki_ipc_selftest_write_only_pipe_omits_receive_ring() -> int;
auto wki_ipc_selftest_attach_insert_failure_preserves_existing_fd() -> int;
auto wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion() -> int;
auto wki_ipc_selftest_dev_op_response_uses_home_node_identity() -> int;
auto wki_ipc_selftest_control_response_admission_preserves_waiter() -> int;
auto wki_ipc_selftest_cookie_retirement_preserves_current_waiter() -> int;
auto wki_ipc_selftest_healthy_controls_cross_cookie_wrap() -> int;
auto wki_ipc_selftest_control_channel_retirement() -> int;
auto wki_ipc_selftest_export_channel_retirement() -> int;
auto wki_ipc_selftest_data_channel_retirement() -> int;
auto wki_ipc_selftest_epoch_mark_and_drain() -> int;
auto wki_ipc_selftest_dev_op_response_keeps_slot_busy_until_consumed() -> int;
#endif

// Cleanup all IPC exports/proxies for a fenced peer
void wki_ipc_cleanup_for_peer(uint16_t node_id);
// Nonblocking HELLO half; deferred cleanup drains retained objects afterward.
void wki_ipc_mark_epoch_reset(uint16_t node_id);

// Shared proxy teardown helper for non-pipe proxy fops.
void wki_ipc_detach_proxy_file(ker::vfs::File* f, ProxyIpcState* proxy);

// Reliably publish a terminal proxy operation before detaching its File.
// The message is copied into the bounded asynchronous close queue when the
// reliable transport cannot take ownership immediately.
void wki_ipc_send_proxy_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id);

// Cancel a locally published proxy control waiter before the caller unwinds.
// Safe when a send failed before wki_wait_for_op() started sleeping; if a
// concurrent close/fence path already claimed the waiter, this waits until that
// claim is fully published so the stack entry cannot be used after return.
// request_sent=false is valid only for wki_send failure before reliable
// acceptance. A timeout is uncertain and retains its response identity.
void wki_ipc_cancel_pending_wait(ProxyIpcState* proxy, WkiWaitEntry* wait, int result, bool request_sent = true);

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
// Shared accounted message writer for flow-advertised stream proxies.
auto wki_ipc_read_stream_proxy(ker::vfs::File* file, void* buffer, size_t count) -> ssize_t;
auto wki_ipc_poll_stream_proxy(ker::vfs::File* file, int events) -> int;
auto wki_ipc_write_stream_proxy(ker::vfs::File* file, const void* buffer, size_t count) -> ssize_t;
extern ker::vfs::FileOperations g_proxy_socket_fops;

// Socket proxy helpers for net syscalls operating on inherited remote sockets.
auto wki_ipc_is_socket_proxy_file(const ker::vfs::File* file) -> bool;
auto wki_ipc_socket_shutdown(ker::vfs::File* file, int how) -> int;
auto wki_ipc_socket_getpeername(ker::vfs::File* file, void* addr_out, size_t* addr_len) -> int;
auto wki_ipc_socket_getsockopt(ker::vfs::File* file, int level, int optname, void* optval, size_t* optlen) -> int;
auto wki_ipc_socket_setsockopt(ker::vfs::File* file, int level, int optname, const void* optval, size_t optlen) -> int;

// Socket-specific DEV_OP_RESP handling hook.
void wki_ipc_socket_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len);
auto wki_ipc_send_control(ProxyIpcState* proxy, const void* payload, uint16_t len) -> int;

// Forward a local epoll_ctl() on an IPC_EPOLL proxy fd to the home node.
// Returns -EOPNOTSUPP when epfile is not an IPC_EPOLL proxy.
auto wki_ipc_epoll_ctl_forward(ker::vfs::File* epfile, int op, int fd, uint32_t events, uint64_t data) -> int;

// -----------------------------------------------------------------------------
// Internal - RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_ipc_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_ipc_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannelIdentity response_channel = {});
void handle_ipc_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
