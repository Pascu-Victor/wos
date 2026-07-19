#include "remote_compute.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>
#include <extern/elf.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iterator>
#include <net/wki/peer.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/usercopy.hpp>
#include <string_view>
#include <syscalls_impl/process/exec.hpp>
#include <util/errno_name.hpp>
#include <util/hcf.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/mm/paging.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

// ===============================================================================
// Storage
// ===============================================================================

namespace {

constexpr uint32_t WKI_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_LATENCY_DAEMON_NICE = -5;
constexpr auto WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
constexpr uint16_t WKI_LOAD_REPORT_MAX_CPUS = 64;
constexpr size_t WKI_COMPUTE_SUBMIT_WORKER_MAX = 4;
constexpr size_t WKI_COMPUTE_SUBMIT_QUEUE_MAX = 64;
constexpr size_t WKI_COMPUTE_SUBMIT_CANCEL_MAX = WKI_COMPUTE_SUBMIT_QUEUE_MAX * 2;
constexpr size_t WKI_COMPUTE_RX_WORKER_MAX = 4;
// One full resource-channel receive window per worker. A shared pool lets a
// busy compile peer borrow unused capacity from idle shards while remaining
// bounded across four-node bursts.
constexpr size_t WKI_COMPUTE_RX_QUEUE_MAX = WKI_CREDITS_RESOURCE * WKI_COMPUTE_RX_WORKER_MAX;
constexpr size_t WKI_COMPUTE_RX_PAYLOAD_MAX = sizeof(TaskCompletePayload) + WKI_TASK_MAX_OUTPUT;
static_assert(WKI_COMPUTE_RX_QUEUE_MAX <= UINT16_MAX);
static_assert(WKI_COMPUTE_RX_PAYLOAD_MAX <= WKI_ETH_MAX_PAYLOAD);
constexpr size_t WKI_SUBMITTED_TASK_INDEX_BUCKETS = 1024;
static_assert((WKI_SUBMITTED_TASK_INDEX_BUCKETS & (WKI_SUBMITTED_TASK_INDEX_BUCKETS - 1)) == 0);
constexpr uint64_t WKI_TASK_SUBMIT_VFS_TIMEOUT_US = 60'000'000;  // Remote binary fetch + launch.
constexpr size_t WKI_FILE_BACKED_ELF_MIN_SIZE = static_cast<size_t>(8) * 1024 * 1024;
constexpr uint64_t WKI_TASK_SUBMIT_INLINE_RECEIVER_TIMEOUT_US = WKI_OP_TIMEOUT_US - 1'000'000;
constexpr uint64_t WKI_TASK_SUBMIT_VFS_RECEIVER_TIMEOUT_US = WKI_TASK_SUBMIT_VFS_TIMEOUT_US - 5'000'000;
static_assert(WKI_TASK_SUBMIT_INLINE_RECEIVER_TIMEOUT_US > 0);
static_assert(WKI_ETH_MAX_PAYLOAD <= ker::mod::mm::KERNEL_STACK_SIZE / 16);

class PeerLifecycleLease {
   public:
    PeerLifecycleLease() = default;
    PeerLifecycleLease(const PeerLifecycleLease&) = delete;
    auto operator=(const PeerLifecycleLease&) -> PeerLifecycleLease& = delete;

    ~PeerLifecycleLease() { wki_peer_lifecycle_release(claimed_peer); }

    auto acquire(uint16_t node_id) -> bool {
        if (claimed_peer != nullptr) {
            return false;
        }
        WkiPeer* peer = wki_peer_find(node_id);
        if (!wki_peer_lifecycle_acquire(peer)) {
            return false;
        }
        claimed_peer = peer;
        return true;
    }

   private:
    WkiPeer* claimed_peer = nullptr;
};

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, WKI_LATENCY_DAEMON_NICE);
}

struct SubmittedTaskSlot {
    SubmittedTask task;
    SubmittedTaskSlot* id_next = nullptr;
    SubmittedTaskSlot* free_next = nullptr;
    bool occupied = false;
};

std::deque<SubmittedTaskSlot> g_submitted_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<SubmittedTaskSlot*, WKI_SUBMITTED_TASK_INDEX_BUCKETS>
    s_submitted_task_index{};                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
SubmittedTaskSlot* s_submitted_task_free = nullptr;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_submitted_task_count = 0;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteNodeLoad> g_remote_loads;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RunningRemoteTask> g_running_remote_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_task_accept_cursor = 0;               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
struct PendingTaskCompletion {
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    int32_t exit_status = -1;
    TaskOutputCapture* output = nullptr;
    uint64_t submit_session_epoch = 0;
    WkiChannel* submit_rx_channel = nullptr;
    uint32_t submit_rx_channel_generation = 0;
};
std::deque<PendingTaskCompletion> g_pending_task_completions;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_task_id = 1;                                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_compute_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_load_report_us = 0;                            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }
    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t') {
            ++cursor;
        }
        const char* start = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t') {
            ++cursor;
        }
        if (std::cmp_equal(cursor - start, TOKEN_LEN) && std::strncmp(start, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto spawn_diag_enabled() -> bool {
    static std::atomic<int> cached{-1};
    int value = cached.load(std::memory_order_acquire);
    if (value >= 0) {
        return value != 0;
    }
    int const ENABLED = cmdline_has_token(ker::init::get_kernel_cmdline(), "wki.spawn_diag") ? 1 : 0;
    cached.store(ENABLED, std::memory_order_release);
    return ENABLED != 0;
}

auto spawn_diag_result_name(WkiRemoteSpawnResult result) -> const char* {
    switch (result) {
        case WkiRemoteSpawnResult::LOCAL:
            return "local";
        case WkiRemoteSpawnResult::REMOTE:
            return "remote";
        case WkiRemoteSpawnResult::FAILED:
            return "failed";
    }
    return "unknown";
}

auto spawn_diag_fd_kind(ker::mod::sched::task::Task* task, uint64_t fd) -> const char* {
    if (task == nullptr) {
        return "no-task";
    }
    const char* kind = "closed";
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* file = static_cast<ker::vfs::File*>(task->fd_table.lookup(fd));
    if (file != nullptr) {
        if (ker::vfs::vfs_is_pipe_file(file)) {
            kind = "local-pipe";
        } else if (ker::vfs::vfs_is_epoll_file(file)) {
            kind = "epoll";
        } else {
            kind = "file";
        }
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);
    return kind;
}

void log_spawn_diag(ker::mod::sched::task::Task* task, WkiRemoteSpawnResult result, const char* reason, uint16_t node = WKI_NODE_INVALID) {
    if (!spawn_diag_enabled() || task == nullptr) {
        return;
    }
    const char* const TARGET = task->wki_target_hostname.front() != '\0' ? task->wki_target_hostname.data() : "<auto>";
    const char* const SUBMITTER = task->wki_submitter_hostname.front() != '\0' ? task->wki_submitter_hostname.data() : "<local>";
    const char* const NAME = task->name != nullptr ? task->name : "?";
    ker::mod::dbg::log(
        "[WKI-SPAWN] result=%s reason=%s pid=0x%lx name=%s exe='%s' target=%s flags=0x%x submitter=%s remote_pid=0x%lx skip=%u "
        "has_path=%u has_inline=%u fd0=%s fd1=%s node=0x%04x",
        spawn_diag_result_name(result), reason != nullptr ? reason : "?", task->pid, NAME, task->exe_path.data(), TARGET,
        task->wki_target_flags, SUBMITTER, task->wki_remote_pid, task->wki_skip_legacy_placement ? 1U : 0U,
        task->exe_path.front() != '\0' ? 1U : 0U, task->elf_buffer != nullptr && task->elf_buffer_size != 0 ? 1U : 0U,
        spawn_diag_fd_kind(task, 0), spawn_diag_fd_kind(task, 1), node);
}

uint16_t g_preferred_remote_cursor = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<ker::mod::sched::CpuAccountingSnapshot, WKI_LOAD_REPORT_MAX_CPUS>
    g_last_local_cpu_accounting{};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_last_local_cpu_accounting_valid{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint16_t g_cached_local_load_pct{};        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_cached_local_load_update_us{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_compute_lock;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct ComputeSubmitSessionToken {
    uint16_t node_id = WKI_NODE_INVALID;
    WkiChannel* rx_channel = nullptr;
    uint32_t rx_channel_generation = 0;
    uint64_t epoch = 0;
};

enum class RemoteComputeCleanupScope : uint8_t {
    ALL,
    RETIRED,
};

struct ComputeSubmitPeerSession {
    bool used = false;
    bool open = false;
    bool retiring = false;
    uint16_t node_id = WKI_NODE_INVALID;
    ComputeSubmitSessionToken current = {};
    WkiChannel* retired_channel = nullptr;
    uint32_t retired_channel_generation = 0;
};

std::array<ComputeSubmitPeerSession, WKI_MAX_PEERS>
    g_compute_submit_peer_sessions{};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_next_compute_submit_session_epoch = 1;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_next_shared_elf_cache_load_token = 1;   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct SubmittedIpcCleanup {
    std::array<WkiIpcFdEntry, 16> map = {};
    uint16_t count = 0;
    uint16_t target_node = WKI_NODE_INVALID;
};

void remember_submitted_ipc_fds(SubmittedTask& submitted, const WkiIpcFdEntry* ipc_fd_map, uint16_t ipc_fd_count) {
    submitted.ipc_fd_count = 0;
    if (ipc_fd_map == nullptr || ipc_fd_count == 0) {
        return;
    }

    uint16_t const COPY_COUNT = std::min<uint16_t>(ipc_fd_count, static_cast<uint16_t>(submitted.ipc_fd_map.size()));
    for (uint16_t i = 0; i < COPY_COUNT; ++i) {
        submitted.ipc_fd_map.at(i) = ipc_fd_map[i];
    }
    submitted.ipc_fd_count = COPY_COUNT;
}

auto submitted_ipc_cleanup_snapshot_locked(const SubmittedTask* submitted) -> SubmittedIpcCleanup {
    SubmittedIpcCleanup cleanup = {};
    if (submitted == nullptr || submitted->ipc_fd_count == 0) {
        return cleanup;
    }

    cleanup.target_node = submitted->target_node;
    cleanup.count = std::min<uint16_t>(submitted->ipc_fd_count, static_cast<uint16_t>(cleanup.map.size()));
    for (uint16_t i = 0; i < cleanup.count; ++i) {
        cleanup.map.at(i) = submitted->ipc_fd_map.at(i);
    }
    return cleanup;
}

void cleanup_submitted_ipc_exports(const SubmittedIpcCleanup& cleanup) {
    if (cleanup.count == 0 || cleanup.target_node == WKI_NODE_INVALID) {
        return;
    }
    wki_ipc_cleanup_exported_fds(cleanup.map.data(), cleanup.count, cleanup.target_node);
}

// Pending TASK_SUBMIT queue. VFS/RESOURCE executable fetches and INLINE
// PT_INTERP construction can all block in VFS, so receiver-side construction
// stays out of NAPI poll context.
struct PendingTaskSubmit {
    uint16_t src_node;
    uint8_t* payload;
    uint16_t payload_len;
    uint64_t queued_at_us;
    uint64_t deadline_us;
    ComputeSubmitSessionToken session;
};

struct CancelledTaskSubmit {
    bool active = false;
    uint32_t task_id = 0;
    int32_t signum = 0;
    uint64_t expires_at_us = 0;
    ComputeSubmitSessionToken session = {};
};
std::array<CancelledTaskSubmit, WKI_COMPUTE_SUBMIT_CANCEL_MAX>
    g_cancelled_task_submits{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<PendingTaskSubmit, WKI_COMPUTE_SUBMIT_QUEUE_MAX>
    g_pending_task_submits{};                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_task_submit_head = 0;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_task_submit_tail = 0;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_task_submit_count = 0;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_pending_submit_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<ker::mod::sched::task::Task*, WKI_COMPUTE_SUBMIT_WORKER_MAX>
    s_compute_submit_tasks{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<size_t> s_compute_submit_task_count{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct PendingComputeRx {
    uint8_t type = 0;
    WkiHeader hdr = {};
    WkiChannel* rx_channel = nullptr;
    uint32_t rx_channel_generation = 0;
    uint16_t payload_len = 0;
    std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> payload = {};
};

struct ComputeRxShard {
    std::array<uint16_t, WKI_COMPUTE_RX_QUEUE_MAX> slot_indices = {};
    size_t head = 0;
    size_t tail = 0;
    size_t count = 0;
};

std::array<PendingComputeRx, WKI_COMPUTE_RX_QUEUE_MAX> s_compute_rx_slots{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<bool, WKI_COMPUTE_RX_QUEUE_MAX> s_compute_rx_slot_used{};          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<ComputeRxShard, WKI_COMPUTE_RX_WORKER_MAX> s_compute_rx_shards{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_compute_rx_slot_cursor = 0;                                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_compute_rx_lock;                                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<ker::mod::sched::task::Task*, WKI_COMPUTE_RX_WORKER_MAX>
    s_compute_rx_tasks{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<size_t> s_compute_rx_task_count{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct SharedElfCacheEntry {
    bool valid = false;
    bool loading = false;
    int32_t load_status = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    ComputeSubmitSessionToken session = {};
    uint64_t load_token = 0;
    std::array<char, 512> path = {};
    ker::vfs::Stat freshness = {};
    uint8_t* buffer = nullptr;
    uint32_t size = 0;
    uint32_t refcount = 0;
    uint64_t last_used_us = 0;
};
std::deque<SharedElfCacheEntry> g_shared_elf_cache;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

void perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp op, uint16_t peer, uint32_t correlation, int32_t status, uint32_t aux,
                               uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_COMPUTE,
                                     static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::POINT, peer, WKI_CHAN_RESOURCE, correlation,
                                     status, aux, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_COMPUTE, static_cast<uint8_t>(op), peer, WKI_CHAN_RESOURCE,
                                       status, 0, false, 0, 0);
}

void perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp op, uint16_t peer, uint32_t correlation, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_COMPUTE,
                                     static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::BEGIN, peer, WKI_CHAN_RESOURCE, correlation, 0,
                                     aux, callsite);
}

void perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp op, uint16_t peer, uint32_t correlation, int32_t status, uint32_t latency_us,
                             uint64_t bytes, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_recording_enabled()) {
        return;
    }

    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_COMPUTE,
                                     static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::END, peer, WKI_CHAN_RESOURCE, correlation,
                                     status, latency_us, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_COMPUTE, static_cast<uint8_t>(op), peer, WKI_CHAN_RESOURCE,
                                       status, latency_us, true, 0, bytes);
}

auto perf_compute_reject_status(TaskRejectReason reason) -> int32_t {
    auto raw = static_cast<uint8_t>(reason);
    if (raw == static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        return -1;
    }
    return -static_cast<int32_t>(raw);
}

struct ScopedComputeMeasure {
    ker::mod::perf::WkiPerfComputeOp op;
    uint16_t peer;
    uint32_t correlation;
    uint64_t callsite;
    uint64_t start_us;
    bool active;

    ScopedComputeMeasure(ker::mod::perf::WkiPerfComputeOp perf_op, uint16_t perf_peer, uint32_t perf_correlation, uint32_t aux,
                         uint64_t perf_callsite)
        : op(perf_op),
          peer(perf_peer),
          correlation(perf_correlation),
          callsite(perf_callsite),
          start_us(wki_now_us()),
          active(ker::mod::perf::is_wki_recording_enabled()) {
        if (active) {
            perf_record_compute_begin(op, peer, correlation, aux, callsite);
        }
    }

    void finish(int32_t status, uint64_t bytes = 0) {
        if (!active) {
            return;
        }

        perf_record_compute_end(op, peer, correlation, status, static_cast<uint32_t>(wki_now_us() - start_us), bytes, callsite);
        active = false;
    }
};

// Keep executable preads bounded: remote VFS may split one logical pread into
// several RDMA sub-requests, and cold fan-out does this concurrently per node.
constexpr size_t WKI_VFS_LOAD_CHUNK = size_t{2} * 1024 * 1024;
constexpr uint32_t WKI_VFS_LOAD_IDLE_RETRIES = 8;
constexpr uint32_t WKI_VFS_LOAD_MAX_ATTEMPTS = 24;
constexpr uint64_t WKI_VFS_LOAD_RETRY_WINDOW_US = 15000000;
constexpr uint64_t WKI_VFS_LOAD_RETRY_BACKOFF_US = 750000;
static_assert(WKI_TASK_SUBMIT_VFS_RECEIVER_TIMEOUT_US > WKI_VFS_LOAD_RETRY_WINDOW_US);
// Remote executable buffers sit on the receiver and prevent repeat VFS_REF
// submits from retransferring multi-MiB binaries over the same WKI links that
// carry stdout/stderr pipes.  Keep enough room for the normal perf-suite mix
// (renderbench + testprog + testd) and retain finished workers long enough that
// early-finishing render nodes stay warm for the next run.
constexpr size_t WKI_EXEC_CACHE_MAX_ENTRIES = 16;
constexpr uint64_t WKI_EXEC_CACHE_MAX_BYTES = 96ULL * 1024ULL * 1024ULL;
constexpr uint64_t WKI_EXEC_CACHE_RETENTION_US = 300000000;
constexpr uint64_t WKI_EXEC_CACHE_INFLIGHT_WAIT_US = 1000;
constexpr uint64_t WKI_VFS_LOAD_BACKOFF_POLL_US = 10000;
constexpr int32_t WKI_SIGKILL_NUM = 9;
constexpr int32_t WKI_SIGTERM_NUM = 15;
constexpr uint64_t WKI_SIGCHLD_NUM = 17;
constexpr int32_t WKI_WAIT_STATUS_SIGNAL_MASK = 0x7f;
constexpr int32_t WKI_WAIT_STATUS_STOPPED = 0x7f;

struct SubmitterChannelToken {
    WkiChannel* channel = nullptr;
    uint32_t generation = 0;
};

auto capture_submitter_channel(uint16_t target_node) -> SubmitterChannelToken {
    WkiChannel* channel = wki_channel_get(target_node, WKI_CHAN_RESOURCE);
    if (channel == nullptr) {
        return {};
    }

    channel->lock.lock();
    SubmitterChannelToken token = {};
    if (channel->active && channel->peer_node_id == target_node && channel->channel_id == WKI_CHAN_RESOURCE && channel->generation != 0) {
        token.channel = channel;
        token.generation = channel->generation;
    }
    channel->lock.unlock();
    return token;
}

auto send_task_cancel_request(uint16_t target_node, uint32_t task_id, int32_t signum, WkiChannel* channel, uint32_t channel_generation)
    -> int {
    TaskCancelPayload cancel = {};
    cancel.task_id = task_id;
    cancel.signum = signum;
    if (channel != nullptr && channel_generation != 0) {
        return wki_send_on_channel_generation(target_node, channel, channel_generation, MsgType::TASK_CANCEL, &cancel, sizeof(cancel));
    }
    return WKI_ERR_NOT_FOUND;
}
constexpr int32_t WKI_WAIT_STATUS_EXIT_SHIFT = 8;
constexpr int32_t WKI_WAIT_STATUS_FAILURE_CODE = 255;
constexpr std::string_view WKI_PATH_PREFIX = "/wki/";

auto wki_local_hostname() -> const char* {
    return g_wki.local_hostname.data();  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
}

void sleep_until_us(uint64_t deadline_us, uint64_t max_sleep_us) {
    uint64_t const NOW_US = wki_now_us();
    if (NOW_US >= deadline_us) {
        ker::mod::sched::kern_yield();
        return;
    }

    ker::mod::sched::kern_sleep_us(std::min(deadline_us - NOW_US, max_sleep_us));
}

auto task_uses_local_vfs_route_for_path(const ker::mod::sched::task::Task* task, const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }

    auto route = static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::HOST);
    if (ker::vfs::vfs_wki_effective_route_for_path(task, path, &route) < 0) {
        return false;
    }
    return route == static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
}

auto submitted_task_bucket(uint32_t task_id) -> size_t {
    return static_cast<size_t>(task_id * 0x9E3779B1U) & (WKI_SUBMITTED_TASK_INDEX_BUCKETS - 1);
}

// s_compute_lock must be held by caller.
auto find_submitted_task_slot_locked(uint32_t task_id) -> SubmittedTaskSlot* {
    if (task_id == 0) {
        return nullptr;
    }
    for (SubmittedTaskSlot* slot = s_submitted_task_index.at(submitted_task_bucket(task_id)); slot != nullptr; slot = slot->id_next) {
        if (slot->occupied && slot->task.task_id == task_id) {
            return slot;
        }
    }
    return nullptr;
}

// s_compute_lock must be held by caller.
auto find_submitted_task_any(uint32_t task_id) -> SubmittedTask* {
    SubmittedTaskSlot* slot = find_submitted_task_slot_locked(task_id);
    return slot != nullptr ? &slot->task : nullptr;
}

// s_compute_lock must be held by caller.
auto find_submitted_task(uint32_t task_id) -> SubmittedTask* {
    SubmittedTask* task = find_submitted_task_any(task_id);
    return task != nullptr && task->active ? task : nullptr;
}

// s_compute_lock must be held by caller.
auto allocate_submitted_task_id_locked() -> uint32_t {
    if (s_submitted_task_count >= UINT32_MAX) {
        return 0;
    }
    for (;;) {
        uint32_t const CANDIDATE = g_next_task_id;
        g_next_task_id = CANDIDATE == UINT32_MAX ? 1U : CANDIDATE + 1U;
        if (CANDIDATE != 0 && find_submitted_task_slot_locked(CANDIDATE) == nullptr) {
            return CANDIDATE;
        }
    }
}

// s_compute_lock must be held by caller.
auto publish_submitted_task_locked(SubmittedTask&& task) -> SubmittedTask* {
    if (task.task_id == 0 || find_submitted_task_slot_locked(task.task_id) != nullptr) {
        return nullptr;
    }

    SubmittedTaskSlot* slot = s_submitted_task_free;
    if (slot != nullptr) {
        s_submitted_task_free = slot->free_next;
        slot->free_next = nullptr;
        slot->task = std::move(task);
    } else {
        g_submitted_tasks.emplace_back();
        slot = &g_submitted_tasks.back();
        slot->task = std::move(task);
    }

    size_t const BUCKET = submitted_task_bucket(slot->task.task_id);
    slot->id_next = s_submitted_task_index.at(BUCKET);
    slot->occupied = true;
    s_submitted_task_index.at(BUCKET) = slot;
    s_submitted_task_count++;
    return &slot->task;
}

auto submitted_task_can_reclaim_locked(const SubmittedTask& task) -> bool {
    return task.reclaim_requested && !task.active && !task.response_pending.load(std::memory_order_relaxed) &&
           task.response_wait_entry == nullptr && task.response_consumer_wait_entry == nullptr &&
           !task.complete_pending.load(std::memory_order_relaxed) && task.complete_wait_entry == nullptr &&
           task.complete_consumer_wait_entry == nullptr && !task.result_handle_owned && task.result_owner_task == nullptr &&
           task.local_task == nullptr && task.pending_proxy_output == nullptr;
}

// s_compute_lock must be held by caller. Eligibility guarantees the reset
// cannot release owned output or invalidate a published stack waiter.
void reclaim_submitted_task_if_safe_locked(SubmittedTask* task) {
    if (task == nullptr || !submitted_task_can_reclaim_locked(*task)) {
        return;
    }
    SubmittedTaskSlot* slot = find_submitted_task_slot_locked(task->task_id);
    if (slot == nullptr || &slot->task != task) {
        return;
    }

    size_t const BUCKET = submitted_task_bucket(task->task_id);
    SubmittedTaskSlot** link = &s_submitted_task_index.at(BUCKET);
    while (*link != nullptr && *link != slot) {
        link = &(*link)->id_next;
    }
    if (*link != slot) {
        return;
    }
    *link = slot->id_next;

    slot->occupied = false;
    slot->id_next = nullptr;
    slot->task = SubmittedTask{};
    slot->free_next = s_submitted_task_free;
    s_submitted_task_free = slot;
    if (s_submitted_task_count > 0) {
        s_submitted_task_count--;
    }
}

// s_compute_lock must be held by caller.
void request_submitted_task_reclaim_locked(SubmittedTask* task) {
    if (task == nullptr) {
        return;
    }
    task->reclaim_requested = true;
    reclaim_submitted_task_if_safe_locked(task);
}

void request_submitted_task_reclaim(uint32_t task_id) {
    if (task_id == 0) {
        return;
    }
    s_compute_lock.lock();
    request_submitted_task_reclaim_locked(find_submitted_task_any(task_id));
    s_compute_lock.unlock();
}

// s_compute_lock must be held by caller. A direct result consumer owns neither
// proxy publication nor captured proxy output; detach both before recycling
// the row and release the allocation after dropping the spinlock.
auto consume_submitted_task_result_locked(SubmittedTask* task) -> uint8_t* {
    if (task == nullptr) {
        return nullptr;
    }
    uint8_t* discarded_output = task->pending_proxy_output;
    task->pending_proxy_output = nullptr;
    task->pending_proxy_output_len = 0;
    task->reset_local_task_ref();
    task->result_handle_owned = false;
    task->result_owner_task = nullptr;
    request_submitted_task_reclaim_locked(task);
    return discarded_output;
}

// An accepted remote task cannot become a proxy until its local Task is
// registered and parked. If that publication fails, retire the direct result
// and exported IPC state before the caller falls back or destroys the child.
void abandon_submitted_task_after_proxy_publish_failure(uint32_t task_id) {
    SubmittedIpcCleanup ipc_cleanup = {};
    uint8_t* discarded_output = nullptr;
    uint16_t target_node = WKI_NODE_INVALID;
    WkiChannel* submit_channel = nullptr;
    uint32_t submit_channel_generation = 0;
    bool cancel_remote = false;

    s_compute_lock.lock();
    SubmittedTask* submitted = find_submitted_task_any(task_id);
    if (submitted != nullptr && submitted->result_handle_owned) {
        target_node = submitted->target_node;
        submit_channel = submitted->submit_channel;
        submit_channel_generation = submitted->submit_channel_generation;
        cancel_remote = submitted->active;
        ipc_cleanup = submitted_ipc_cleanup_snapshot_locked(submitted);
        submitted->ipc_fd_count = 0;
        submitted->response_pending.store(false, std::memory_order_release);
        submitted->complete_pending.store(false, std::memory_order_release);
        submitted->active = false;
        submitted->exit_status = -1;
        discarded_output = consume_submitted_task_result_locked(submitted);
    }
    s_compute_lock.unlock();

    if (cancel_remote) {
        static_cast<void>(send_task_cancel_request(target_node, task_id, WKI_SIGKILL_NUM, submit_channel, submit_channel_generation));
    }
    cleanup_submitted_ipc_exports(ipc_cleanup);
    delete[] discarded_output;
}

// s_compute_lock must be held by caller.
auto claim_and_clear_waiter_locked(WkiWaitEntry*& waiter_slot) -> WkiWaitEntry* {
    WkiWaitEntry* waiter = waiter_slot;
    waiter_slot = nullptr;
    if (!wki_claim_op(waiter)) {
        return nullptr;
    }
    return waiter;
}

void finish_or_wait_for_cancelled_waiter(WkiWaitEntry& wait, bool claimed, int result) {
    if (claimed) {
        wki_finish_claimed_op(&wait, result);
        return;
    }

    wki_quiesce_claimed_op(&wait);
}

// s_compute_lock must be held by caller.
auto publish_task_complete_waiter_locked(SubmittedTask* task, WkiWaitEntry& wait) -> bool {
    if (task == nullptr || task->complete_pending.load(std::memory_order_acquire)) {
        return false;
    }

    task->complete_wait_entry = &wait;
    task->complete_consumer_wait_entry = &wait;
    task->complete_pending.store(true, std::memory_order_release);
    return true;
}

// s_compute_lock must be held by caller.
auto clear_task_complete_waiter_after_wait_locked(SubmittedTask* task, WkiWaitEntry& wait, int wait_result, int32_t& completed_exit_status)
    -> bool {
    if (task == nullptr) {
        return false;
    }

    bool const WAIT_SLOT_OWNED = task->complete_wait_entry == &wait;
    bool const WAIT_CONSUMER_OWNED = task->complete_consumer_wait_entry == &wait;
    if (WAIT_SLOT_OWNED) {
        task->complete_wait_entry = nullptr;
    }
    if (WAIT_CONSUMER_OWNED) {
        task->complete_consumer_wait_entry = nullptr;
    }
    completed_exit_status = task->exit_status;
    if (wait_result == WKI_ERR_TIMEOUT && WAIT_SLOT_OWNED) {
        task->complete_pending.store(false, std::memory_order_release);
    }
    return WAIT_SLOT_OWNED || WAIT_CONSUMER_OWNED;
}

// s_compute_lock must be held by caller
auto find_remote_load(uint16_t node_id) -> RemoteNodeLoad* {
    for (auto& rl : g_remote_loads) {
        if (rl.valid && rl.node_id == node_id) {
            return &rl;
        }
    }
    return nullptr;
}

// s_compute_lock must be held by caller
auto find_running_task(uint32_t task_id, const ComputeSubmitSessionToken& session) -> RunningRemoteTask* {
    for (auto& rt : g_running_remote_tasks) {
        if (rt.active && rt.published && !rt.discard_completion && rt.task_id == task_id && rt.submitter_node == session.node_id &&
            rt.submit_session_epoch == session.epoch && rt.submit_rx_channel == session.rx_channel &&
            rt.submit_rx_channel_generation == session.rx_channel_generation) {
            return &rt;
        }
    }
    return nullptr;
}

// s_compute_lock must be held by caller
void compact_running_remote_tasks_locked() {
    std::erase_if(g_running_remote_tasks, [](const RunningRemoteTask& task) { return !task.active; });
}

// s_compute_lock must be held by caller
void compact_pending_task_completions_locked() {
    std::erase_if(g_pending_task_completions,
                  [](const PendingTaskCompletion& completion) { return completion.submitter_node == WKI_NODE_INVALID; });
}

auto next_nonzero_token(uint64_t& counter) -> uint64_t {
    uint64_t token = counter++;
    if (token == 0) {
        token = counter++;
    }
    if (counter == 0) {
        counter = 1;
    }
    return token;
}

auto compute_submit_session_token_matches(const ComputeSubmitSessionToken& lhs, const ComputeSubmitSessionToken& rhs) -> bool {
    return lhs.epoch != 0 && lhs.node_id == rhs.node_id && lhs.epoch == rhs.epoch && lhs.rx_channel == rhs.rx_channel &&
           lhs.rx_channel_generation == rhs.rx_channel_generation;
}

auto compute_submit_channel_token_matches(uint16_t node_id, WkiChannel* channel, uint32_t generation) -> bool {
    if (channel == nullptr || generation == 0) {
        return false;
    }

    channel->lock.lock();
    bool const MATCHES = channel->active && channel->peer_node_id == node_id && channel->channel_id == WKI_CHAN_RESOURCE &&
                         channel->generation == generation;
    channel->lock.unlock();
    return MATCHES;
}

auto find_compute_submit_peer_session_locked(uint16_t node_id) -> ComputeSubmitPeerSession* {
    for (auto& session : g_compute_submit_peer_sessions) {
        if (session.used && session.node_id == node_id) {
            return &session;
        }
    }
    return nullptr;
}

auto get_or_create_compute_submit_peer_session_locked(uint16_t node_id) -> ComputeSubmitPeerSession* {
    if (auto* session = find_compute_submit_peer_session_locked(node_id); session != nullptr) {
        return session;
    }
    for (auto& session : g_compute_submit_peer_sessions) {
        if (!session.used) {
            session.used = true;
            session.node_id = node_id;
            return &session;
        }
    }
    return nullptr;
}

auto running_task_session_matches(const RunningRemoteTask& task, const ComputeSubmitSessionToken& session) -> bool {
    return task.submitter_node == session.node_id && task.submit_session_epoch == session.epoch &&
           task.submit_rx_channel == session.rx_channel && task.submit_rx_channel_generation == session.rx_channel_generation;
}

void retire_running_compute_submit_session_locked(const ComputeSubmitSessionToken& session) {
    if (session.epoch == 0) {
        return;
    }
    for (auto& task : g_running_remote_tasks) {
        if (task.active && running_task_session_matches(task, session)) {
            task.discard_completion = true;
        }
    }
}

auto capture_compute_submit_session_locked(uint16_t node_id, WkiChannel* channel, uint32_t generation) -> ComputeSubmitSessionToken {
    if (!compute_submit_channel_token_matches(node_id, channel, generation)) {
        return {};
    }

    auto* peer_session = get_or_create_compute_submit_peer_session_locked(node_id);
    if (peer_session == nullptr) {
        return {};
    }
    if (peer_session->retiring) {
        return {};
    }
    if (peer_session->open && peer_session->current.rx_channel == channel && peer_session->current.rx_channel_generation == generation) {
        return peer_session->current;
    }
    if (!peer_session->open && peer_session->retired_channel == channel && peer_session->retired_channel_generation == generation) {
        return {};
    }
    if (peer_session->open) {
        retire_running_compute_submit_session_locked(peer_session->current);
        peer_session->retired_channel = peer_session->current.rx_channel;
        peer_session->retired_channel_generation = peer_session->current.rx_channel_generation;
    }

    peer_session->current = {
        .node_id = node_id,
        .rx_channel = channel,
        .rx_channel_generation = generation,
        .epoch = next_nonzero_token(s_next_compute_submit_session_epoch),
    };
    peer_session->open = true;
    return peer_session->current;
}

auto compute_submit_session_is_current_locked(const ComputeSubmitSessionToken& session) -> bool {
    auto* peer_session = find_compute_submit_peer_session_locked(session.node_id);
    return peer_session != nullptr && peer_session->open && !peer_session->retiring &&
           compute_submit_session_token_matches(peer_session->current, session) &&
           compute_submit_channel_token_matches(session.node_id, session.rx_channel, session.rx_channel_generation);
}

void gc_cancelled_task_submits_locked(uint64_t now_us) {
    for (auto& cancelled : g_cancelled_task_submits) {
        if (cancelled.active && now_us >= cancelled.expires_at_us) {
            cancelled = {};
        }
    }
}

auto find_cancelled_task_submit_locked(const ComputeSubmitSessionToken& session, uint32_t task_id) -> CancelledTaskSubmit* {
    for (auto& cancelled : g_cancelled_task_submits) {
        if (cancelled.active && cancelled.task_id == task_id && compute_submit_session_token_matches(cancelled.session, session)) {
            return &cancelled;
        }
    }
    return nullptr;
}

void record_cancelled_task_submit_locked(const ComputeSubmitSessionToken& session, uint32_t task_id, int32_t signum) {
    uint64_t const NOW_US = wki_now_us();
    gc_cancelled_task_submits_locked(NOW_US);
    if (auto* existing = find_cancelled_task_submit_locked(session, task_id); existing != nullptr) {
        existing->signum = signum;
        existing->expires_at_us = wki_future_deadline_us(NOW_US, WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        return;
    }

    CancelledTaskSubmit* slot = nullptr;
    for (auto& cancelled : g_cancelled_task_submits) {
        if (!cancelled.active) {
            slot = &cancelled;
            break;
        }
        if (slot == nullptr || cancelled.expires_at_us < slot->expires_at_us) {
            slot = &cancelled;
        }
    }
    if (slot != nullptr) {
        *slot = {
            .active = true,
            .task_id = task_id,
            .signum = signum,
            .expires_at_us = wki_future_deadline_us(NOW_US, WKI_TASK_SUBMIT_VFS_TIMEOUT_US),
            .session = session,
        };
    }
}

auto take_cancelled_task_submit_locked(const ComputeSubmitSessionToken& session, uint32_t task_id) -> int32_t {
    gc_cancelled_task_submits_locked(wki_now_us());
    auto* cancelled = find_cancelled_task_submit_locked(session, task_id);
    if (cancelled == nullptr) {
        return 0;
    }
    int32_t const SIGNUM = cancelled->signum;
    *cancelled = {};
    return SIGNUM;
}

auto compute_submit_request_is_current_locked(const ComputeSubmitSessionToken& session, uint32_t task_id) -> bool {
    gc_cancelled_task_submits_locked(wki_now_us());
    return compute_submit_session_is_current_locked(session) && find_cancelled_task_submit_locked(session, task_id) == nullptr;
}

void retire_compute_submit_session_locked(uint16_t node_id, WkiChannel* channel, uint32_t generation) {
    auto* peer_session = get_or_create_compute_submit_peer_session_locked(node_id);
    if (peer_session == nullptr) {
        return;
    }

    if (peer_session->open) {
        retire_running_compute_submit_session_locked(peer_session->current);
        peer_session->retired_channel = peer_session->current.rx_channel;
        peer_session->retired_channel_generation = peer_session->current.rx_channel_generation;
    } else if (channel != nullptr && generation != 0) {
        peer_session->retired_channel = channel;
        peer_session->retired_channel_generation = generation;
    }
    peer_session->open = false;
    peer_session->current = {};
    for (auto& cancelled : g_cancelled_task_submits) {
        if (cancelled.active && cancelled.session.node_id == node_id) {
            cancelled = {};
        }
    }
}

auto shared_elf_path_matches(const SharedElfCacheEntry& entry, const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }

    size_t const ENTRY_LEN = std::strlen(entry.path.data());
    size_t const PATH_LEN = std::strlen(path);
    return ENTRY_LEN == PATH_LEN && std::strncmp(entry.path.data(), path, PATH_LEN) == 0;
}

auto shared_elf_freshness_matches(const ker::vfs::Stat& lhs, const ker::vfs::Stat& rhs) -> bool {
    if (lhs.st_size <= 0 || lhs.st_size != rhs.st_size) {
        return false;
    }
    bool const LHS_HAS_TIMES = lhs.st_mtim.tv_sec != 0 || lhs.st_mtim.tv_nsec != 0 || lhs.st_ctim.tv_sec != 0 || lhs.st_ctim.tv_nsec != 0;
    bool const RHS_HAS_TIMES = rhs.st_mtim.tv_sec != 0 || rhs.st_mtim.tv_nsec != 0 || rhs.st_ctim.tv_sec != 0 || rhs.st_ctim.tv_nsec != 0;
    if (!LHS_HAS_TIMES || !RHS_HAS_TIMES) {
        return true;
    }
    return lhs.st_mtim.tv_sec == rhs.st_mtim.tv_sec && lhs.st_mtim.tv_nsec == rhs.st_mtim.tv_nsec &&
           lhs.st_ctim.tv_sec == rhs.st_ctim.tv_sec && lhs.st_ctim.tv_nsec == rhs.st_ctim.tv_nsec;
}

auto find_shared_elf_cache_locked(const ComputeSubmitSessionToken& session, const char* path, const ker::vfs::Stat& freshness)
    -> SharedElfCacheEntry* {
    for (auto& entry : g_shared_elf_cache) {
        if (!entry.valid || entry.loading || entry.buffer == nullptr || !compute_submit_session_token_matches(entry.session, session)) {
            continue;
        }
        if (!shared_elf_path_matches(entry, path)) {
            continue;
        }
        if (!shared_elf_freshness_matches(entry.freshness, freshness)) {
            continue;
        }
        return &entry;
    }
    return nullptr;
}

auto find_shared_elf_cache_entry_locked(const ComputeSubmitSessionToken& session, const char* path, const ker::vfs::Stat& freshness)
    -> SharedElfCacheEntry* {
    for (auto& entry : g_shared_elf_cache) {
        if (!entry.valid || !compute_submit_session_token_matches(entry.session, session)) {
            continue;
        }
        if (!shared_elf_path_matches(entry, path)) {
            continue;
        }
        if (!shared_elf_freshness_matches(entry.freshness, freshness)) {
            continue;
        }
        return &entry;
    }
    return nullptr;
}

auto find_shared_elf_cache_by_buffer_locked(const uint8_t* buffer) -> SharedElfCacheEntry* {
    for (auto& entry : g_shared_elf_cache) {
        if (entry.buffer == buffer) {
            return &entry;
        }
    }
    return nullptr;
}

auto shared_elf_cache_bytes_locked() -> uint64_t {
    uint64_t total = 0;
    for (const auto& entry : g_shared_elf_cache) {
        if (entry.buffer != nullptr) {
            total += entry.size;
        }
    }
    return total;
}

void gc_shared_elf_cache_locked(uint64_t now_us) {
    for (auto it = g_shared_elf_cache.begin(); it != g_shared_elf_cache.end();) {
        bool const EXPIRED = !it->loading && it->refcount == 0 &&
                             (!it->valid || (it->last_used_us != 0 && now_us >= it->last_used_us &&
                                             (now_us - it->last_used_us) >= WKI_EXEC_CACHE_RETENTION_US));
        if (!EXPIRED) {
            ++it;
            continue;
        }

        delete[] it->buffer;
        it = g_shared_elf_cache.erase(it);
    }

    auto evict_oldest_unref = [&]() -> bool {
        auto victim = g_shared_elf_cache.end();
        for (auto it = g_shared_elf_cache.begin(); it != g_shared_elf_cache.end(); ++it) {
            if (it->refcount != 0 || it->loading) {
                continue;
            }
            if (victim == g_shared_elf_cache.end() || it->last_used_us < victim->last_used_us) {
                victim = it;
            }
        }
        if (victim == g_shared_elf_cache.end()) {
            return false;
        }

        delete[] victim->buffer;
        g_shared_elf_cache.erase(victim);
        return true;
    };

    while (g_shared_elf_cache.size() > WKI_EXEC_CACHE_MAX_ENTRIES) {
        if (!evict_oldest_unref()) {
            break;
        }
    }

    while (shared_elf_cache_bytes_locked() > WKI_EXEC_CACHE_MAX_BYTES) {
        if (!evict_oldest_unref()) {
            break;
        }
    }
}

auto release_cached_elf_buffer(uint8_t* buffer) -> bool {
    if (buffer == nullptr) {
        return false;
    }

    s_compute_lock.lock();
    auto* entry = find_shared_elf_cache_by_buffer_locked(buffer);
    if (entry == nullptr) {
        s_compute_lock.unlock();
        return false;
    }

    if (entry->refcount > 0) {
        entry->refcount--;
    }
    entry->last_used_us = wki_now_us();
    gc_shared_elf_cache_locked(entry->last_used_us);
    s_compute_lock.unlock();
    return true;
}

void release_loaded_elf_buffer(uint8_t* buffer, bool shared_buffer) {
    if (buffer == nullptr) {
        return;
    }

    if (shared_buffer && release_cached_elf_buffer(buffer)) {
        return;
    }

    delete[] buffer;
}

// -----------------------------------------------------------------------------
// D19: Output capture FileOperations
// Write appends to the TaskOutputCapture buffer. All other ops are stubs.
// File::private_data points to the TaskOutputCapture.
// -----------------------------------------------------------------------------

auto capture_write(ker::vfs::File* file, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (file == nullptr || file->private_data == nullptr || buf == nullptr || count == 0) {
        return 0;
    }
    auto* cap = static_cast<TaskOutputCapture*>(file->private_data);
    uint16_t const SPACE = WKI_TASK_MAX_OUTPUT - cap->len;
    if (SPACE == 0) {
        return static_cast<ssize_t>(count);  // silently drop overflow
    }
    auto to_copy = static_cast<uint16_t>(std::min(static_cast<size_t>(SPACE), count));
    memcpy(std::next(cap->data.begin(), static_cast<ptrdiff_t>(cap->len)), buf, to_copy);
    cap->len += to_copy;
    return static_cast<ssize_t>(count);  // report all bytes "written"
}

auto capture_close(ker::vfs::File* /*file*/) -> int { return 0; }
auto capture_isatty(ker::vfs::File* /*file*/) -> bool { return false; }
auto stdin_null_read(ker::vfs::File* /*file*/, void* /*buf*/, size_t /*count*/, size_t /*offset*/) -> ssize_t { return 0; }

ker::vfs::FileOperations g_capture_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,
    .vfs_close = capture_close,
    .vfs_read = nullptr,
    .vfs_write = capture_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = capture_isatty,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

ker::vfs::FileOperations g_stdin_null_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,
    .vfs_close = capture_close,
    .vfs_read = stdin_null_read,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = capture_isatty,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

void close_proxy_fd_table(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    for (unsigned fd = 0; fd < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++fd) {
        auto* file = static_cast<ker::vfs::File*>(nullptr);
        // Take the fd-table ownership reference. Retaining here would leave
        // proxy-owned pipe ends open and suppress EOF on the submitter side.
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        file = static_cast<ker::vfs::File*>(task->fd_table.lookup(fd));
        if (file == nullptr) {
            task->fd_table_lock.unlock_irqrestore(IRQF);
            continue;
        }
        task->clear_fd_cloexec(fd);
        task->fd_table.remove(fd);
        task->fd_table_lock.unlock_irqrestore(IRQF);
        ker::vfs::vfs_put_file(file);
    }
}

void destroy_unpublished_remote_process(ker::mod::sched::task::Task* owner, ker::mod::sched::task::Task* task, TaskOutputCapture* output,
                                        const char* reason) {
    if (task != nullptr) {
        ker::mod::dbg::log("[WKI] Retiring unpublished remote process pid=0x%lx reason=%s", task->pid, reason);
        if (!ker::mod::sched::task::destroy_owned_unpublished_process(owner, task)) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI remote compute: lost unpublished-process teardown ownership");
            hcf();
        }
    }
    delete output;
}

auto encode_remote_wait_status(int32_t exit_status) -> int32_t {
    if (exit_status >= 0) {
        return exit_status;
    }

    switch (exit_status) {
        case -WKI_SIGKILL_NUM:
            return WKI_SIGKILL_NUM;
        case -WKI_SIGTERM_NUM:
            return WKI_SIGTERM_NUM;
        default:
            return WKI_WAIT_STATUS_FAILURE_CODE << WKI_WAIT_STATUS_EXIT_SHIFT;
    }
}

auto normalize_local_exit_status_for_wire(int32_t exit_status) -> int32_t {
    int32_t const TERMSIG = exit_status & WKI_WAIT_STATUS_SIGNAL_MASK;
    if (TERMSIG != 0 && TERMSIG != WKI_WAIT_STATUS_STOPPED) {
        switch (TERMSIG) {
            case WKI_SIGKILL_NUM:
                return -WKI_SIGKILL_NUM;
            case WKI_SIGTERM_NUM:
                return -WKI_SIGTERM_NUM;
            default:
                return exit_status;
        }
    }

    switch (exit_status) {
        case 128 + WKI_SIGKILL_NUM:
            return -WKI_SIGKILL_NUM;
        case 128 + WKI_SIGTERM_NUM:
            return -WKI_SIGTERM_NUM;
        default:
            return exit_status;
    }
}

auto proxy_waiter_context_can_be_completed(ker::mod::sched::task::Task* waiter) -> bool {
    return waiter != nullptr && !waiter->deferred_task_switch && !waiter->waitpid_publish_pending.load(std::memory_order_acquire);
}

auto write_proxy_wait_status(ker::mod::sched::task::Task* waiter, int32_t wait_status) -> bool {
    if (waiter == nullptr || waiter->wait_status_user_addr == 0 || waiter->pagemap == nullptr) {
        if (waiter != nullptr) {
            waiter->wait_status_user_addr = 0;
            waiter->wait_status_phys_addr = 0;
        }
        return true;
    }

    bool const OK = ker::mod::sys::usercopy::copy_value_to_task_mapped(*waiter, waiter->wait_status_user_addr, wait_status);
    waiter->wait_status_user_addr = 0;
    waiter->wait_status_phys_addr = 0;
    return OK;
}

auto proxy_matches_waiter(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* proxy) -> bool {
    if (waiter == nullptr || proxy == nullptr || proxy->is_thread || proxy->parent_pid != waiter->pid) {
        return false;
    }
    return waiter->waiting_for_pid == WAIT_ANY_CHILD || waiter->waiting_for_pid == proxy->pid;
}

auto try_complete_proxy_wait(ker::mod::sched::task::Task* waiter, ker::mod::sched::task::Task* proxy, int32_t wait_status) -> bool {
    if (!proxy_matches_waiter(waiter, proxy) || !proxy_waiter_context_can_be_completed(waiter)) {
        return false;
    }
    if (!ker::mod::sched::task::task_try_claim_waitpid_completion(*waiter)) {
        return false;
    }
    if (!proxy_matches_waiter(waiter, proxy) || !proxy_waiter_context_can_be_completed(waiter)) {
        ker::mod::sched::task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }
    if (!ker::mod::sched::task::task_try_mark_waited_on(*proxy)) {
        ker::mod::sched::task::task_release_waitpid_completion_claim(*waiter);
        return false;
    }

    ker::mod::sched::task::task_accumulate_waited_child_times(*waiter, *proxy);
    waiter->context.regs.rax = proxy->pid;
    if (!write_proxy_wait_status(waiter, wait_status)) {
        waiter->context.regs.rax = static_cast<uint64_t>(-EFAULT);
    }
    waiter->waitpid_publish_pending.store(false, std::memory_order_release);
    ker::mod::sched::task::task_clear_waitpid_block_state(*waiter);
    return true;
}

void cleanup_proxy_resources(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->is_thread) {
        return;
    }

    close_proxy_fd_table(task);
    if (task->elf_buffer != nullptr) {
        if (task->is_elf_buffer_shared) {
            release_cached_elf_buffer(task->elf_buffer);
        } else {
            delete[] task->elf_buffer;
        }
        task->elf_buffer = nullptr;
        task->elf_buffer_size = 0;
    }
    if (task->exec_image_file != nullptr) {
        ker::vfs::vfs_put_file(task->exec_image_file);
        task->exec_image_file = nullptr;
        task->exec_image_size = 0;
    }
}

void write_proxy_output(ker::mod::sched::task::Task* proxy, const uint8_t* output_data, uint16_t output_len, uint32_t task_id) {
#ifndef DEBUG_WKI_COMPUTE
    (void)task_id;
#endif
    if (proxy == nullptr || output_data == nullptr || output_len == 0) {
        return;
    }
#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] Task %u remote output (%u bytes):", task_id, output_len);
#endif
#ifdef DEBUG_WKI_COMPUTE
    ssize_t write_ret = -1;
    bool had_stdout = false;
#endif
    if (proxy->fd_table.lookup(1) != nullptr) {
#ifdef DEBUG_WKI_COMPUTE
        had_stdout = true;
#endif
        auto* file = static_cast<ker::vfs::File*>(proxy->fd_table.lookup(1));
        if (file->fops != nullptr && file->fops->vfs_write != nullptr) {
#ifdef DEBUG_WKI_COMPUTE
            write_ret =
#endif
                file->fops->vfs_write(file, output_data, output_len, 0);
        }
    }
#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] Task %u replay output: len=%u had_stdout=%d ret=%ld", task_id, output_len, had_stdout ? 1 : 0, write_ret);
#endif
}

void wake_proxy_waiters(ker::mod::sched::task::Task* proxy, int32_t exit_status) {
    if (proxy == nullptr) {
        return;
    }

    int32_t const WAIT_STATUS = encode_remote_wait_status(exit_status);
    uint64_t const WAITER_LOCK_FLAGS = proxy->exit_waiters_lock.lock_irqsave();
    size_t const WAITER_COUNT = proxy->awaitee_on_exit.size();
    std::array<uint64_t, 16> waiting_pids{};
    size_t const WAITING_PIDS_CAP = waiting_pids.size();
    for (size_t i = 0; i < WAITER_COUNT && i < WAITING_PIDS_CAP; ++i) {
        waiting_pids.at(i) = proxy->awaitee_on_exit.at(i);
    }
    proxy->awaitee_on_exit.clear();
    proxy->exit_waiters_lock.unlock_irqrestore(WAITER_LOCK_FLAGS);

    for (size_t i = 0; i < WAITER_COUNT && i < WAITING_PIDS_CAP; ++i) {
        uint64_t const WAITING_PID = waiting_pids.at(i);
        auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(WAITING_PID);
        if (waiting_task != nullptr) {
            (void)try_complete_proxy_wait(waiting_task, proxy, WAIT_STATUS);
            uint64_t const CPU = ker::mod::sched::get_least_loaded_cpu();
            ker::mod::sched::reschedule_task_for_cpu(CPU, waiting_task);
            waiting_task->release();
        }
    }
}

void finalize_proxy_task(ker::mod::sched::task::Task* proxy, int32_t exit_status, const uint8_t* output_data, uint16_t output_len,
                         uint32_t task_id) {
    if (proxy == nullptr) {
        return;
    }

    if (!proxy->transition_state(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
        auto state = proxy->state.load(std::memory_order_acquire);
        if (state == ker::mod::sched::task::TaskState::DEAD || state == ker::mod::sched::task::TaskState::EXITING ||
            proxy->sched_queue == ker::mod::sched::task::Task::sched_queue::DEAD_GC) {
            return;
        }
        return;
    }

    int32_t const WAIT_STATUS = encode_remote_wait_status(exit_status);

    write_proxy_output(proxy, output_data, output_len, task_id);

    proxy->exit_status = WAIT_STATUS;
    proxy->has_exited = true;
    proxy->exit_notify_ready.store(true, std::memory_order_release);
    proxy->wki_proxy_task_id = 0;

    // Send SIGCHLD to parent and wake it if blocked in waitpid(-1, ...).
    // The normal exit path (wos_proc_exit) does this, but proxy tasks never
    // go through wos_proc_exit — they are finalized here when TASK_COMPLETE
    // arrives.  Without SIGCHLD the parent (e.g. busybox shell calling
    // waitpid(-1,...)) would block forever because nobody added it to
    // proxy->awaitee_on_exit.
    if (!proxy->is_thread && proxy->parent_pid != 0) {
        auto* parent = ker::mod::sched::find_task_by_pid_safe(proxy->parent_pid);
        if (parent != nullptr) {
            parent->signal_add_pending_mask(1ULL << (WKI_SIGCHLD_NUM - 1));

            if (parent->waiting_for_pid == WAIT_ANY_CHILD && try_complete_proxy_wait(parent, proxy, WAIT_STATUS)) {
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            } else if (parent->deferred_task_switch || parent->is_voluntary_blocked()) {
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            } else if ((parent->signal_deliverable_bits() & (1ULL << (WKI_SIGCHLD_NUM - 1))) != 0 &&
                       parent->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING &&
                       parent->wait_channel_is(ker::mod::sched::task::WaitChannelKind::SIGSUSPEND)) {
                ker::mod::sched::wake_task_for_signal(parent);
            }
            parent->release();
        }
    }

    wake_proxy_waiters(proxy, exit_status);
    cleanup_proxy_resources(proxy);

    proxy->death_epoch.store(ker::mod::sched::EpochManager::current_epoch(), std::memory_order_release);
    proxy->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
    ker::mod::sched::insert_into_dead_list(proxy);
}

struct SubmitContextInfo {
    uint16_t argc = 0;
    uint16_t envc = 0;
    uint16_t cwd_len = 0;
    uint16_t args_len = 0;
    uint16_t identity_len = 0;
    uint16_t policy_len = 0;
    uint16_t data_len = 0;
};

struct WkiTaskIdentityContext {
    static constexpr uint16_t V1_SIZE = 112;

    uint32_t uid;
    uint32_t gid;
    uint32_t euid;
    uint32_t egid;
    uint32_t suid;
    uint32_t sgid;
    uint32_t umask;
    uint64_t session_id;
    uint64_t pgid;
    int32_t controlling_tty;
    std::array<char, WKI_HOSTNAME_MAX> submitter_hostname;
    std::array<char, ker::mod::sched::task::Task::CWD_MAX> root;
} __attribute__((packed));

static_assert(sizeof(WkiTaskIdentityContext) == 368, "WkiTaskIdentityContext V2 must stay wire-stable");

auto valid_task_identity_len(uint16_t identity_len) -> bool {
    return identity_len == 0 || identity_len == WkiTaskIdentityContext::V1_SIZE || identity_len == sizeof(WkiTaskIdentityContext);
}

void fill_task_identity_context(const ker::mod::sched::task::Task* task, WkiTaskIdentityContext* identity) {
    if (task == nullptr || identity == nullptr) {
        return;
    }

    identity->uid = task->uid;
    identity->gid = task->gid;
    identity->euid = task->euid;
    identity->egid = task->egid;
    identity->suid = task->suid;
    identity->sgid = task->sgid;
    identity->umask = task->umask;
    identity->session_id = task->session_id;
    identity->pgid = task->pgid;
    identity->controlling_tty = static_cast<int32_t>(task->controlling_tty);
    if (task->wki_submitter_hostname.front() != '\0') {
        std::strncpy(identity->submitter_hostname.data(), task->wki_submitter_hostname.data(), identity->submitter_hostname.size() - 1);
        identity->submitter_hostname.back() = '\0';
    }
    std::strncpy(identity->root.data(), task->root.data(), identity->root.size() - 1);
    identity->root.back() = '\0';
}

void apply_submitted_task_identity(ker::mod::sched::task::Task* task, const WkiTaskIdentityContext& identity) {
    if (task == nullptr) {
        return;
    }

    task->uid = identity.uid;
    task->gid = identity.gid;
    task->euid = identity.euid;
    task->egid = identity.egid;
    task->suid = identity.suid;
    task->sgid = identity.sgid;
    task->umask = identity.umask & 0777U;
    task->session_id = identity.session_id;
    task->pgid = identity.pgid != 0 ? identity.pgid : task->pid;
    // PTY indexes are node-local. Remote tasks use exported PTY fds rather
    // than a receiver-local controlling tty, so detach instead of accidentally
    // binding /dev/tty to a same-numbered PTY on the receiver.
    task->controlling_tty = -1;
    if (identity.root.front() == '/') {
        std::strncpy(task->root.data(), identity.root.data(), task->root.size() - 1);
        task->root.back() = '\0';
        task->root_len = static_cast<uint16_t>(std::strlen(task->root.data()));
    }
    if (identity.submitter_hostname.front() != '\0') {
        std::strncpy(task->wki_submitter_hostname.data(), identity.submitter_hostname.data(), task->wki_submitter_hostname.size() - 1);
        task->wki_submitter_hostname.back() = '\0';
    }
}

auto deserialize_task_vfs_rules(ker::mod::sched::task::Task* task, const uint8_t* data, uint16_t data_len) -> bool;

struct ScopedSubmitVfsIdentity {
    ker::mod::sched::task::Task* task = nullptr;
    ker::mod::sched::task::Task::PathBuffer saved_root{};
    uint16_t saved_root_len = 1;
    ker::mod::sched::task::Task::HostnameBuffer saved_submitter{};
    ker::util::SmallVec<ker::mod::sched::task::WkiVfsRule, 4> saved_vfs_rules;
    bool active = false;
    bool vfs_rules_installed = false;
    bool submitted_policy_valid = false;

    ScopedSubmitVfsIdentity(ker::mod::sched::task::Task* current_task, const WkiTaskIdentityContext* identity,
                            const char* submitter_hostname, const uint8_t* policy_data, uint16_t policy_len)
        : task(current_task) {
        if (task == nullptr) {
            return;
        }

        saved_root = task->root;
        saved_root_len = task->root_len;
        saved_submitter = task->wki_submitter_hostname;
        saved_vfs_rules = std::move(task->wki_vfs_rules);
        vfs_rules_installed = true;

        if (identity != nullptr && identity->root.front() == '/') {
            task->root = identity->root;
            task->root_len = static_cast<uint16_t>(std::strlen(task->root.data()));
        }

        if (submitter_hostname != nullptr && submitter_hostname[0] != '\0') {
            std::strncpy(task->wki_submitter_hostname.data(), submitter_hostname, task->wki_submitter_hostname.size() - 1);
            task->wki_submitter_hostname.back() = '\0';
        } else if (identity != nullptr && identity->submitter_hostname.front() != '\0') {
            task->wki_submitter_hostname = identity->submitter_hostname;
        } else {
            task->wki_submitter_hostname.front() = '\0';
        }

        active = true;
        submitted_policy_valid = deserialize_task_vfs_rules(task, policy_data, policy_len);
    }

    [[nodiscard]] auto policy_valid() const -> bool { return submitted_policy_valid; }

    [[nodiscard]] auto transfer_vfs_rules_to(ker::mod::sched::task::Task* submitted_task) -> bool {
        if (!active || !submitted_policy_valid || !vfs_rules_installed || task == nullptr || submitted_task == nullptr) {
            return false;
        }

        submitted_task->wki_vfs_rules = std::move(task->wki_vfs_rules);
        task->wki_vfs_rules = std::move(saved_vfs_rules);
        vfs_rules_installed = false;
        return true;
    }

    ~ScopedSubmitVfsIdentity() {
        if (!active || task == nullptr) {
            return;
        }
        if (vfs_rules_installed) {
            task->wki_vfs_rules = std::move(saved_vfs_rules);
        }
        task->root = saved_root;
        task->root_len = saved_root_len;
        task->wki_submitter_hostname = saved_submitter;
    }
};

auto serialized_task_vfs_rules_size(const ker::mod::sched::task::Task* task) -> uint16_t {
    if (task == nullptr || task->wki_vfs_rules.empty()) {
        return 0;
    }

    uint32_t total = sizeof(uint16_t);
    for (const auto& rule : task->wki_vfs_rules) {
        if (rule.prefix_len == 0 || rule.prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || rule.prefix.front() != '/' ||
            *std::next(rule.prefix.begin(), static_cast<ptrdiff_t>(rule.prefix_len)) != '\0') {
            continue;
        }
        total += sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t) + rule.prefix_len;
    }

    if (total > 0xFFFFU) {
        return 0;
    }
    return static_cast<uint16_t>(total);
}

void serialize_task_vfs_rules(uint8_t* dest, const ker::mod::sched::task::Task* task) {
    if (dest == nullptr || task == nullptr) {
        return;
    }

    auto* count_ptr = reinterpret_cast<uint16_t*>(dest);
    *count_ptr = 0;
    dest += sizeof(uint16_t);

    for (const auto& rule : task->wki_vfs_rules) {
        if (rule.prefix_len == 0 || rule.prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || rule.prefix.front() != '/' ||
            *std::next(rule.prefix.begin(), static_cast<ptrdiff_t>(rule.prefix_len)) != '\0') {
            continue;
        }

        *dest++ = rule.route;
        *dest++ = 0;
        memcpy(dest, &rule.prefix_len, sizeof(uint16_t));
        dest += sizeof(uint16_t);
        memcpy(dest, rule.prefix.data(), rule.prefix_len);
        dest += rule.prefix_len;
        (*count_ptr)++;
    }
}

auto deserialize_task_vfs_rules(ker::mod::sched::task::Task* task, const uint8_t* data, uint16_t data_len) -> bool {
    if (task == nullptr) {
        return false;
    }

    task->wki_vfs_rules.clear();
    if (data == nullptr || data_len == 0) {
        return true;
    }

    if (data_len < sizeof(uint16_t)) {
        return false;
    }

    uint16_t encoded_count = 0;
    memcpy(&encoded_count, data, sizeof(uint16_t));
    data += sizeof(uint16_t);
    data_len = static_cast<uint16_t>(data_len - sizeof(uint16_t));

    if (encoded_count > 256) {
        return false;
    }

    for (uint16_t i = 0; i < encoded_count; ++i) {
        if (data_len < sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t)) {
            return false;
        }

        uint8_t const ROUTE = data[0];
        uint16_t prefix_len = 0;
        memcpy(&prefix_len, data + sizeof(uint8_t) + sizeof(uint8_t), sizeof(uint16_t));
        data += sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t);
        data_len = static_cast<uint16_t>(data_len - (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t)));

        if (prefix_len == 0 || prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || data_len < prefix_len ||
            (ROUTE != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
             ROUTE != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST))) {
            return false;
        }

        ker::mod::sched::task::WkiVfsRule new_rule{};
        memcpy(new_rule.prefix.data(), data, prefix_len);
        *std::next(new_rule.prefix.begin(), static_cast<ptrdiff_t>(prefix_len)) = '\0';
        new_rule.prefix_len = prefix_len;
        new_rule.route = ROUTE;
        new_rule.reserved = 0;
        if (!task->wki_vfs_rules.push_back(new_rule)) {
            return false;
        }

        data += prefix_len;
        data_len = static_cast<uint16_t>(data_len - prefix_len);
    }

    return data_len == 0;
}

auto accumulate_string_block(const char* const* strings, uint16_t* count_out, uint16_t* bytes_out) -> bool {
    uint16_t count = 0;
    uint16_t bytes = 0;
    if (strings != nullptr) {
        while (strings[count] != nullptr) {
            size_t const LEN = std::strlen(strings[count]) + 1;
            if (LEN > 0xFFFFU || bytes > static_cast<uint16_t>(0xFFFFU - LEN) || count == 0xFFFFU) {
                return false;
            }
            bytes = static_cast<uint16_t>(bytes + LEN);
            count = static_cast<uint16_t>(count + 1);
        }
    }
    *count_out = count;
    *bytes_out = bytes;
    return true;
}

auto checked_submit_args_len(uint16_t argv_bytes, uint16_t envp_bytes, uint16_t cwd_len, uint16_t* args_len_out) -> bool {
    if (args_len_out == nullptr) {
        return false;
    }
    uint32_t const TOTAL = static_cast<uint32_t>(argv_bytes) + static_cast<uint32_t>(envp_bytes) + static_cast<uint32_t>(cwd_len);
    if (TOTAL > UINT16_MAX) {
        return false;
    }
    *args_len_out = static_cast<uint16_t>(TOTAL);
    return true;
}

auto build_submit_context_info(const ker::mod::sched::task::Task* task, const char* const* argv, const char* const* envp, const char* cwd,
                               SubmitContextInfo* info) -> bool {
    uint16_t argv_bytes = 0;
    uint16_t envp_bytes = 0;
    if (!accumulate_string_block(argv, &info->argc, &argv_bytes) || !accumulate_string_block(envp, &info->envc, &envp_bytes)) {
        return false;
    }

    if (cwd != nullptr && cwd[0] != '\0') {
        size_t const CWD_LEN = std::strlen(cwd) + 1;
        if (CWD_LEN > 0xFFFFU) {
            return false;
        }
        info->cwd_len = static_cast<uint16_t>(CWD_LEN);
    }

    if (!checked_submit_args_len(argv_bytes, envp_bytes, info->cwd_len, &info->args_len)) {
        return false;
    }
    info->identity_len = task != nullptr ? static_cast<uint16_t>(sizeof(WkiTaskIdentityContext)) : 0;
    info->policy_len = serialized_task_vfs_rules_size(task);

    uint32_t const TOTAL =
        static_cast<uint32_t>(info->args_len) + static_cast<uint32_t>(info->identity_len) + static_cast<uint32_t>(info->policy_len);
    if (TOTAL > 0xFFFFU) {
        return false;
    }

    info->data_len = static_cast<uint16_t>(TOTAL);
    return true;
}

void copy_submit_context_data(uint8_t* dest, const char* const* argv, const char* const* envp, const char* cwd) {
    uint8_t* cursor = dest;

    if (argv != nullptr) {
        for (size_t i = 0; argv[i] != nullptr; ++i) {
            size_t const LEN = std::strlen(argv[i]) + 1;
            memcpy(cursor, argv[i], LEN);
            cursor += LEN;
        }
    }

    if (envp != nullptr) {
        for (size_t i = 0; envp[i] != nullptr; ++i) {
            size_t const LEN = std::strlen(envp[i]) + 1;
            memcpy(cursor, envp[i], LEN);
            cursor += LEN;
        }
    }

    if (cwd != nullptr && cwd[0] != '\0') {
        size_t const LEN = std::strlen(cwd) + 1;
        memcpy(cursor, cwd, LEN);
    }
}

void copy_submit_identity_data(uint8_t* dest, const ker::mod::sched::task::Task* task) {
    WkiTaskIdentityContext identity = {};
    fill_task_identity_context(task, &identity);
    memcpy(dest, &identity, sizeof(identity));
}

auto compute_local_load() -> uint16_t { return wki_local_node_load_pct(); }

auto compute_local_runnable_load_fallback() -> uint16_t {
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::get_core_count());
    if (cpu_count == 0) {
        return 0;
    }

    uint16_t total_runnable = 0;
    for (uint16_t c = 0; c < cpu_count; ++c) {
        auto const STATS = ker::mod::sched::get_run_queue_stats(c);
        total_runnable += static_cast<uint16_t>(STATS.active_task_count);
    }

    auto const PCT = static_cast<uint16_t>((static_cast<uint32_t>(total_runnable) * 1000U) / cpu_count);
    return std::min<uint16_t>(PCT, 1000);
}

auto snapshot_total_time_us(ker::mod::sched::CpuAccountingSnapshot const& snapshot) -> uint64_t {
    return snapshot.user_us + snapshot.nice_us + snapshot.system_us + snapshot.idle_us + snapshot.iowait_us + snapshot.irq_us +
           snapshot.softirq_us + snapshot.steal_us;
}

auto snapshot_busy_time_us(ker::mod::sched::CpuAccountingSnapshot const& snapshot) -> uint64_t {
    return snapshot_total_time_us(snapshot) - snapshot.idle_us;
}

auto snapshot_delta_pct_milli(ker::mod::sched::CpuAccountingSnapshot const& current, ker::mod::sched::CpuAccountingSnapshot const& previous)
    -> uint16_t {
    uint64_t const TOTAL_NOW = snapshot_total_time_us(current);
    uint64_t const TOTAL_PREV = snapshot_total_time_us(previous);
    if (TOTAL_NOW <= TOTAL_PREV) {
        return 0;
    }

    uint64_t const BUSY_NOW = snapshot_busy_time_us(current);
    uint64_t const BUSY_PREV = snapshot_busy_time_us(previous);
    uint64_t const DELTA_TOTAL = TOTAL_NOW - TOTAL_PREV;
    uint64_t const DELTA_BUSY = BUSY_NOW > BUSY_PREV ? BUSY_NOW - BUSY_PREV : 0;
    return static_cast<uint16_t>(std::min<uint64_t>((DELTA_BUSY * 1000ULL) / DELTA_TOTAL, 1000ULL));
}

// -----------------------------------------------------------------------------
//  Check if this node has a VFS export covering the given path.
// If so, build the remote-accessible VFS_REF path:
//   local path /sbin/init  +  export "/"  => /wki/<our_hostname>/sbin/init
// Returns true if a VFS_REF path was built.
// -----------------------------------------------------------------------------

auto build_vfs_ref_path(uint16_t target_node, const char* local_path, char* out, size_t out_size, const ker::mod::sched::task::Task* task)
    -> bool {
    if (local_path == nullptr || local_path[0] == '\0') {
        return false;
    }

    // Check if target node is connected and knows our hostname
    const char* our_hostname = wki_local_hostname();
    if (our_hostname[0] == '\0') {
        return false;
    }

    // Build the VFS_REF path: /wki/<our_hostname>/<local_path without leading />.
    // If the path already starts with /wki/, it already references a specific
    // node's exported filesystem. When it points at the chosen target node,
    // send the receiver its direct local path instead of bouncing through its
    // own /wki/<self>/ alias during ELF load. Otherwise pass the explicit WKI
    // path through unchanged to preserve cross-node semantics.
    if (std::strncmp(local_path, WKI_PATH_PREFIX.data(), WKI_PATH_PREFIX.size()) == 0) {
        const char* target_hostname = wki_peer_get_hostname(target_node);
        const char* host_part = local_path + WKI_PATH_PREFIX.size();
        const char* host_end = host_part;
        while (*host_end != '\0' && *host_end != '/') {
            host_end++;
        }

        if (target_hostname != nullptr) {
            auto host_len = static_cast<size_t>(host_end - host_part);
            size_t const TARGET_LEN = std::strlen(target_hostname);
            if (host_len == TARGET_LEN && std::strncmp(host_part, target_hostname, host_len) == 0) {
                const char* suffix = (*host_end == '\0') ? "/" : host_end;
                size_t const SUFFIX_LEN = std::strlen(suffix);
                if (SUFFIX_LEN + 1 > out_size) {
                    return false;
                }
                std::memcpy(out, suffix, SUFFIX_LEN + 1);
                return true;
            }
        }

        size_t const LEN = std::strlen(local_path);
        if (LEN + 1 > out_size) {
            return false;
        }
        std::memcpy(out, local_path, LEN + 1);
        return true;
    }

    if (task_uses_local_vfs_route_for_path(task, local_path)) {
        size_t const LEN = std::strlen(local_path);
        if (LEN + 1 > out_size) {
            return false;
        }
        std::memcpy(out, local_path, LEN + 1);
        return true;
    }

    const char* stripped = local_path;
    while (*stripped == '/') {
        stripped++;
    }

    snprintf(out, out_size, "/wki/%s/%s", our_hostname, stripped);
    return true;
}

auto localize_receiver_logical_path(const char* path, char* out, size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    if (std::strncmp(path, WKI_PATH_PREFIX.data(), WKI_PATH_PREFIX.size()) != 0) {
        return false;
    }

    const char* local_hostname = wki_local_hostname();
    if (local_hostname[0] == '\0') {
        return false;
    }

    size_t const HOST_LEN = std::strlen(local_hostname);
    const char* host_part = path + WKI_PATH_PREFIX.size();
    if (std::strncmp(host_part, local_hostname, HOST_LEN) != 0 || (host_part[HOST_LEN] != '\0' && host_part[HOST_LEN] != '/')) {
        return false;
    }

    size_t const PATH_LEN = std::strlen(path);
    const char* const PATH_END = path + PATH_LEN;
    const char* suffix = host_part + HOST_LEN;
    while (suffix < PATH_END && *suffix == '/') {
        suffix++;
    }

    if (*suffix == '\0') {
        if (out_size < 2) {
            return false;
        }
        out[0] = '/';
        out[1] = '\0';
        return true;
    }

    size_t const SUFFIX_LEN = std::strlen(suffix);
    if (SUFFIX_LEN + 2 > out_size) {
        return false;
    }

    out[0] = '/';
    std::memcpy(out + 1, suffix, SUFFIX_LEN + 1);
    return true;
}

auto fallback_to_local_path_for_disconnected_wki_host(const char* path, char* out, size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    if (std::strncmp(path, WKI_PATH_PREFIX.data(), WKI_PATH_PREFIX.size()) != 0) {
        return false;
    }

    const char* host_part = path + WKI_PATH_PREFIX.size();
    const char* host_end = host_part;
    while (*host_end != '\0' && *host_end != '/') {
        host_end++;
    }
    if (host_end == host_part) {
        return false;
    }

    std::array<char, WKI_HOSTNAME_MAX> host = {};
    auto host_len = static_cast<size_t>(host_end - host_part);
    if (host_len >= host.size()) {
        host_len = host.size() - 1;
    }
    std::memcpy(host.data(), host_part, host_len);
    *std::next(host.begin(), static_cast<ptrdiff_t>(host_len)) = '\0';

    uint16_t const HOST_NODE = wki_peer_find_by_hostname(host.data());
    if (HOST_NODE != WKI_NODE_INVALID && HOST_NODE != g_wki.my_node_id) {
        return false;
    }

    const char* suffix = (*host_end == '\0') ? "/" : host_end;
    size_t const SUFFIX_LEN = std::strlen(suffix);
    if (SUFFIX_LEN + 1 > out_size) {
        return false;
    }

    std::memcpy(out, suffix, SUFFIX_LEN + 1);

    ker::vfs::Stat local_stat = {};
    if (ker::vfs::vfs_stat(out, &local_stat) != 0 || local_stat.st_size <= 0) {
        return false;
    }

    ker::mod::dbg::log("[WKI] VFS_REF: host '%s' unavailable, falling back to local path '%s'", host.data(), out);
    return true;
}

auto stat_is_regular_file(const ker::vfs::Stat& st) -> bool { return (st.st_mode & ker::vfs::S_IFMT) == ker::vfs::S_IFREG; }

auto stat_has_freshness(const ker::vfs::Stat& st) -> bool {
    return st.st_mtim.tv_sec != 0 || st.st_mtim.tv_nsec != 0 || st.st_ctim.tv_sec != 0 || st.st_ctim.tv_nsec != 0;
}

auto stat_freshness_matches(const ker::vfs::Stat& left, const ker::vfs::Stat& right) -> bool {
    return left.st_size == right.st_size && left.st_mtim.tv_sec == right.st_mtim.tv_sec && left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec && left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

auto equivalent_local_file_for_wki_ref(const char* path, const ker::vfs::Stat& remote_stat, char* out, size_t out_size,
                                       ker::vfs::Stat* local_stat_out) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0 || local_stat_out == nullptr || !stat_is_regular_file(remote_stat) ||
        !stat_has_freshness(remote_stat)) {
        return false;
    }

    if (std::strncmp(path, WKI_PATH_PREFIX.data(), WKI_PATH_PREFIX.size()) != 0) {
        return false;
    }

    const char* host_part = path + WKI_PATH_PREFIX.size();
    const char* host_end = host_part;
    while (*host_end != '\0' && *host_end != '/') {
        host_end++;
    }
    if (host_end == host_part || *host_end == '\0') {
        return false;
    }

    const char* local_hostname = wki_local_hostname();
    size_t const LOCAL_HOST_LEN = std::strlen(local_hostname);
    auto const REF_HOST_LEN = static_cast<size_t>(host_end - host_part);
    if (LOCAL_HOST_LEN == REF_HOST_LEN && std::strncmp(host_part, local_hostname, REF_HOST_LEN) == 0) {
        return false;
    }

    const char* suffix = host_end;
    size_t const SUFFIX_LEN = std::strlen(suffix);
    if (SUFFIX_LEN == 0 || SUFFIX_LEN + 1 > out_size) {
        return false;
    }
    std::memcpy(out, suffix, SUFFIX_LEN + 1);

    ker::vfs::Stat local_stat = {};
    if (ker::vfs::vfs_stat(out, &local_stat) != 0 || !stat_is_regular_file(local_stat) || !stat_has_freshness(local_stat) ||
        !stat_freshness_matches(remote_stat, local_stat)) {
        out[0] = '\0';
        return false;
    }

    *local_stat_out = local_stat;
    return true;
}

// -----------------------------------------------------------------------------
// D17: Scheduler auto-placement hook (V2).
// Called from postTaskBalanced() when WKI is active and task is a PROCESS.
// Distinguishes local fallback from terminal remote-placement failure.
//
// V2 changes:
//   - VFS_REF delivery selection per A6.2
//   - Proxy task stays alive in WAITING state per A7.2
// -----------------------------------------------------------------------------

auto try_remote_placement(ker::mod::sched::task::Task* task) -> ker::mod::sched::RemotePlacementResult {
    if (task == nullptr || task->wki_skip_legacy_placement) {
        return ker::mod::sched::RemotePlacementResult::LOCAL;
    }

    // The legacy scheduler hook runs after fork()/post_task_balanced(). Raw
    // fork children inherit exe_path from the parent but do not yet have a new
    // exec context; path-based VFS_REF placement here would incorrectly
    // relaunch the parent's binary on a remote node. Only auto-place tasks
    // that still carry an inline executable payload prepared by a real spawn.
    if (task->elf_buffer == nullptr || task->elf_buffer_size == 0) {
        return ker::mod::sched::RemotePlacementResult::LOCAL;
    }

    WkiRemoteSpawnSpec const SPEC = {};
    switch (wki_try_remote_spawn(task, SPEC)) {
        case WkiRemoteSpawnResult::REMOTE:
            return ker::mod::sched::RemotePlacementResult::REMOTE;
        case WkiRemoteSpawnResult::FAILED:
            return ker::mod::sched::RemotePlacementResult::FAILED;
        case WkiRemoteSpawnResult::LOCAL:
            return ker::mod::sched::RemotePlacementResult::LOCAL;
    }
    return ker::mod::sched::RemotePlacementResult::FAILED;
}

}  // namespace

namespace {
auto wki_preferred_remote_node() -> uint16_t;
}  // namespace

// ===============================================================================
// Init
// ===============================================================================

void wki_remote_compute_init() {
    if (g_remote_compute_initialized) {
        return;
    }
    g_remote_compute_initialized = true;

    // D17: Register the remote placement hook with the scheduler
    ker::mod::sched::wki_try_remote_placement_fn = try_remote_placement;

    ker::mod::dbg::log("[WKI] Remote compute subsystem initialized");
}

auto wki_try_remote_spawn(ker::mod::sched::task::Task* task, const WkiRemoteSpawnSpec& spec) -> WkiRemoteSpawnResult {
    if (!g_remote_compute_initialized || task == nullptr) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "not-initialized");
        return WkiRemoteSpawnResult::LOCAL;
    }

    const bool HAS_EXE_PATH = task->exe_path.front() != '\0';
    const bool HAS_INLINE_BINARY = task->elf_buffer_complete && task->elf_buffer != nullptr && task->elf_buffer_size != 0;
    if (!HAS_EXE_PATH && !HAS_INLINE_BINARY) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "no-exec-context");
        return WkiRemoteSpawnResult::LOCAL;
    }

    // WKI_TARGET_FLAG_LOCAL: task is pinned to the local node.
    if ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "local-flag");
        return WkiRemoteSpawnResult::LOCAL;
    }

    const bool EXPLICIT_TARGET = task->wki_target_hostname.front() != '\0';
    const bool STRICT_TARGET = EXPLICIT_TARGET && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool PREFER_REMOTE = !EXPLICIT_TARGET && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE) != 0);
    const bool STRICT_REMOTE = PREFER_REMOTE && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool AUTOMATIC_PLACEMENT = !EXPLICIT_TARGET && !PREFER_REMOTE;
    const bool REMOTE_RECEIVER =
        task->wki_submitter_hostname.front() != '\0' && std::strcmp(task->wki_submitter_hostname.data(), wki_local_hostname()) != 0;

    if (AUTOMATIC_PLACEMENT) {
        auto base_name = [](const char* name) -> const char* {
            if (name == nullptr || name[0] == '\0') {
                return "";
            }
            const char* base = name;
            for (const char* cursor = name; *cursor != '\0'; ++cursor) {
                if (*cursor == '/') {
                    base = cursor + 1;
                }
            }
            return base;
        };
        auto is_git_helper = [&base_name](const char* name) -> bool {
            const char* base = base_name(name);
            return std::strcmp(base, "git") == 0 || std::strncmp(base, "git-", 4) == 0;
        };
        auto is_toolchain_driver_helper = [&base_name](const char* name) -> bool {
            const char* base = base_name(name);
            if (base[0] == '\0') {
                return false;
            }
            constexpr std::array<const char*, 27> EXACT_LOCAL_TOOLS = {
                "sh",          "bash",    "dash",         "cmake", "ninja",    "make",     "gmake",     "clang-tidy-cache",
                "cc",          "c++",     "gcc",          "g++",   "clang",    "clang++",  "clang-cpp", "nasm",
                "as",          "ld",      "ld.lld",       "lld",   "lld-link", "ld64.lld", "wasm-ld",   "llvm-ar",
                "llvm-ranlib", "llvm-nm", "llvm-objcopy",
            };
            for (const char* tool : EXACT_LOCAL_TOOLS) {
                if (std::strcmp(base, tool) == 0) {
                    return true;
                }
            }
            if (std::strncmp(base, "llvm-", 5) == 0) {
                return true;
            }
            if (std::strncmp(base, "clang-", 6) == 0) {
                char const NEXT = base[6];
                return NEXT >= '0' && NEXT <= '9';
            }
            return false;
        };
        auto path_has_component = [](const char* path, const char* component) -> bool {
            if (path == nullptr || component == nullptr || component[0] == '\0') {
                return false;
            }
            size_t const COMPONENT_LEN = std::strlen(component);
            const char* cursor = path;
            while (*cursor != '\0') {
                while (*cursor == '/') {
                    ++cursor;
                }
                const char* start = cursor;
                while (*cursor != '\0' && *cursor != '/') {
                    ++cursor;
                }
                auto const LEN = static_cast<size_t>(cursor - start);
                if (LEN == COMPONENT_LEN && std::strncmp(start, component, COMPONENT_LEN) == 0) {
                    return true;
                }
            }
            return false;
        };
        auto path_has_component_suffix = [](const char* path, const char* suffix) -> bool {
            if (path == nullptr || suffix == nullptr || suffix[0] == '\0') {
                return false;
            }
            size_t const SUFFIX_LEN = std::strlen(suffix);
            const char* cursor = path;
            while (*cursor != '\0') {
                while (*cursor == '/') {
                    ++cursor;
                }
                const char* start = cursor;
                while (*cursor != '\0' && *cursor != '/') {
                    ++cursor;
                }
                auto const LEN = static_cast<size_t>(cursor - start);
                if (LEN >= SUFFIX_LEN && std::strncmp(cursor - SUFFIX_LEN, suffix, SUFFIX_LEN) == 0) {
                    return true;
                }
            }
            return false;
        };
        auto is_generated_build_tree_executable = [&base_name, &path_has_component, &path_has_component_suffix](const char* path) -> bool {
            if (path == nullptr || path[0] == '\0') {
                return false;
            }
            if (path_has_component(path, "build") || path_has_component(path, "CMakeFiles") || path_has_component(path, "meson-private")) {
                return true;
            }
            if (path_has_component_suffix(path, "-build")) {
                return true;
            }
            const char* base = base_name(path);
            return std::strncmp(base, "cmTC_", 5) == 0;
        };
        if (is_git_helper(task->name) || is_git_helper(task->exe_path.data()) ||
            std::strstr(task->exe_path.data(), "/git-core/") != nullptr) {
            return WkiRemoteSpawnResult::LOCAL;
        }
        if (is_toolchain_driver_helper(task->name) || is_toolchain_driver_helper(task->exe_path.data())) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "toolchain-helper");
            return WkiRemoteSpawnResult::LOCAL;
        }
        if (is_generated_build_tree_executable(task->exe_path.data())) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "generated-build-exe");
            return WkiRemoteSpawnResult::LOCAL;
        }

        bool stdin_is_pipe = false;
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        auto* stdin_file = static_cast<ker::vfs::File*>(task->fd_table.lookup(0));
        stdin_is_pipe = ker::vfs::vfs_is_pipe_file(stdin_file);
        task->fd_table_lock.unlock_irqrestore(IRQF);
        if (stdin_is_pipe) {
            return WkiRemoteSpawnResult::LOCAL;
        }
    }

    // A task that was already submitted from another node stays on its receiver
    // for automatic follow-on execs. Explicit targets and remote-preferred
    // policies are an opt-in fan-out path used by distributed launchers.
    if (REMOTE_RECEIVER && !EXPLICIT_TARGET && !PREFER_REMOTE) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "remote-receiver-auto");
        return WkiRemoteSpawnResult::LOCAL;
    }

    // Don't bounce tasks that are already remote-receiver instances.
    if (std::strncmp(task->name, "wki-remote", 10) == 0) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "wki-remote-task");
        return WkiRemoteSpawnResult::LOCAL;
    }

    // Pin locally when the task owns epoll instances — their interest lists
    // reference local File* objects with local poll callbacks that cannot be
    // proxied.  Tasks that merely write to sources watched by a remote epoll
    // migrate freely and signal via RDMA doorbell.
    {
        bool must_stay_local = false;
        task->fd_table.for_each([&](uint64_t, void* val) {
            if (val == nullptr) {
                return;
            }
            auto* f = static_cast<ker::vfs::File*>(val);
            if (ker::vfs::vfs_is_epoll_file(f)) {
                must_stay_local = true;
            }
        });
        if (must_stay_local) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "epoll-fd");
            return WkiRemoteSpawnResult::LOCAL;
        }
    }

    task->wki_skip_legacy_placement = true;

    uint16_t best_node = WKI_NODE_INVALID;
    if (EXPLICIT_TARGET) {
        const char* const TARGET_HOSTNAME = task->wki_target_hostname.data();
        uint16_t node_id = wki_peer_find_by_hostname(TARGET_HOSTNAME);
        if (node_id == WKI_NODE_INVALID) {
            constexpr std::string_view WOS_DOMAIN_SUFFIX = ".wos";
            size_t const TARGET_LEN = std::strlen(TARGET_HOSTNAME);
            if (TARGET_LEN > WOS_DOMAIN_SUFFIX.size() && std::strncmp(TARGET_HOSTNAME + TARGET_LEN - WOS_DOMAIN_SUFFIX.size(),
                                                                      WOS_DOMAIN_SUFFIX.data(), WOS_DOMAIN_SUFFIX.size()) == 0) {
                std::array<char, ker::mod::sched::task::Task::WKI_TARGET_HOSTNAME_MAX> short_target{};
                size_t const SHORT_TARGET_LEN = TARGET_LEN - WOS_DOMAIN_SUFFIX.size();
                std::memcpy(short_target.data(), TARGET_HOSTNAME, SHORT_TARGET_LEN);
                node_id = wki_peer_find_by_hostname(short_target.data());
            }
        }
        if (node_id == g_wki.my_node_id) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "explicit-local-target");
            return WkiRemoteSpawnResult::LOCAL;
        }
        if (node_id == WKI_NODE_INVALID) {
            WkiRemoteSpawnResult const RESULT = STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "target-not-found");
            return RESULT;
        }

        auto* peer = wki_peer_find(node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            WkiRemoteSpawnResult const RESULT = STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "target-not-connected", node_id);
            return RESULT;
        }

        best_node = node_id;
    } else if (PREFER_REMOTE) {
        if (!wki_ipc_find_pipe_affinity_node(task, &best_node)) {
            s_compute_lock.lock();
            best_node = wki_preferred_remote_node();
            s_compute_lock.unlock();
        }
        if (best_node == WKI_NODE_INVALID) {
            WkiRemoteSpawnResult const RESULT = STRICT_REMOTE ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "remote-preferred-no-node");
            return RESULT;
        }
    } else {
        uint16_t const LOCAL_LOAD = compute_local_load();
        s_compute_lock.lock();
        best_node = wki_least_loaded_node(LOCAL_LOAD);
        s_compute_lock.unlock();
        if (best_node == WKI_NODE_INVALID) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "automatic-no-node");
            return WkiRemoteSpawnResult::LOCAL;
        }
    }

    uint32_t tid = 0;
    bool const BINARY_FITS =
        HAS_INLINE_BINARY && (sizeof(TaskSubmitPayload) + sizeof(uint32_t) + task->elf_buffer_size) <= WKI_ETH_MAX_PAYLOAD;

    // Export IPC fds (pipes, sockets) so they can be proxied on the remote node.
    // Must happen before task submission so the remote side can attach proxy fops.
    std::array<WkiIpcFdEntry, 16> ipc_fd_map = {};
    uint16_t ipc_fd_count = 0;
    wki_ipc_export_task_fds(task, best_node, ipc_fd_map.data(), &ipc_fd_count);
    bool vfs_ref_submit_attempted = false;

    // The legacy scheduler hook must stay cheap: it can run from task
    // publication paths that should not block on remote VFS. Direct exec/spawn
    // calls already paid the exec syscall cost and carry argv/env/cwd context,
    // so automatic placement may use VFS_REF there for normal-sized tools that
    // cannot fit in an inline TASK_SUBMIT frame.
    bool const DIRECT_EXEC_SUBMIT = spec.argv != nullptr || spec.envp != nullptr || (spec.cwd != nullptr && spec.cwd[0] != '\0');
    bool const ALLOW_VFS_REF = !AUTOMATIC_PLACEMENT || DIRECT_EXEC_SUBMIT;
    if (ALLOW_VFS_REF && HAS_EXE_PATH && (!task->wki_prefer_inline || !HAS_INLINE_BINARY)) {
        // Resolve relative exe_path against the task's cwd so that the
        // VFS_REF path sent to the remote node is always absolute.
        // We cannot use make_absolute() here because it queries
        // get_current_task() which may differ from the task being submitted.
        using TaskT = ker::mod::sched::task::Task;
        constexpr size_t ABS_PATH_MAX = TaskT::CWD_MAX + 1 + TaskT::EXE_PATH_MAX;
        std::array<char, ABS_PATH_MAX> abs_exe_path = {};
        const char* local_path = task->exe_path.data();
        if (task->exe_path.front() != '/') {
            size_t const CWD_HINT = task->cwd_len;
            size_t const CWDLEN =
                (CWD_HINT > 0 && CWD_HINT < task->cwd.size() && task->cwd.at(CWD_HINT) == '\0') ? CWD_HINT : std::strlen(task->cwd.data());
            size_t const PATHLEN = std::strlen(task->exe_path.data());
            bool const NEED_SEP = (CWDLEN > 1);
            size_t const TOTAL = CWDLEN + (NEED_SEP ? 1 : 0) + PATHLEN + 1;
            if (TOTAL <= abs_exe_path.size()) {
                std::memcpy(abs_exe_path.data(), task->cwd.data(), CWDLEN);
                if (NEED_SEP) {
                    *std::next(abs_exe_path.begin(), static_cast<ptrdiff_t>(CWDLEN)) = '/';
                    std::memcpy(abs_exe_path.data() + CWDLEN + 1, task->exe_path.data(), PATHLEN + 1);
                } else {
                    std::memcpy(abs_exe_path.data() + CWDLEN, task->exe_path.data(), PATHLEN + 1);
                }
                local_path = abs_exe_path.data();
            }
        }

        constexpr size_t VFS_REF_PATH_MAX = 5 + WKI_HOSTNAME_MAX + ABS_PATH_MAX;
        std::array<char, VFS_REF_PATH_MAX> vfs_ref_path = {};
        if (build_vfs_ref_path(best_node, local_path, vfs_ref_path.data(), vfs_ref_path.size(), task)) {
            vfs_ref_submit_attempted = true;
            tid = wki_task_submit_vfs_ref(best_node, vfs_ref_path.data(), spec.argv, spec.envp, spec.cwd, task, ipc_fd_map.data(),
                                          ipc_fd_count);
#ifdef WKI_DEBUG
            if (tid != 0) {
                ker::mod::dbg::log("[WKI] Remote spawn using VFS_REF '%s' -> '%s'", task->exe_path.data(), vfs_ref_path.data());
            }
#endif
        }
    }

    if (tid == 0 && BINARY_FITS) {
        if (vfs_ref_submit_attempted) {
            ipc_fd_map = {};
            ipc_fd_count = 0;
            wki_ipc_export_task_fds(task, best_node, ipc_fd_map.data(), &ipc_fd_count);
        }
        tid = wki_task_submit_inline(best_node, task->elf_buffer, static_cast<uint32_t>(task->elf_buffer_size), spec.argv, spec.envp,
                                     spec.cwd, task, ipc_fd_map.data(), ipc_fd_count);
    }

    if (tid == 0) {
        wki_ipc_cleanup_exported_fds(ipc_fd_map.data(), ipc_fd_count, best_node);
        WkiRemoteSpawnResult const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "submit-failed", best_node);
        return RESULT;
    }

    bool const NEEDS_PROXY_PUBLICATION =
        task->sched_queue == ker::mod::sched::task::Task::sched_queue::NONE && task != ker::mod::sched::get_current_task();
    bool proxy_ready = false;
    if (NEEDS_PROXY_PUBLICATION) {
        // Publish the proxy identity before making the task discoverable.  A
        // signal may find the task as soon as post_task_waiting() inserts it in
        // the PID table; without these fields it could be mistaken for a local
        // process and woken to execute the ELF that is already running remotely.
        task->wki_proxy_task_id = tid;
        task->wki_proxy_task = true;
        task->set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
        proxy_ready = ker::mod::sched::post_task_waiting(task);
        if (!proxy_ready) {
            task->clear_wait_channel();
            task->wki_proxy_task = false;
            task->wki_proxy_task_id = 0;
            abandon_submitted_task_after_proxy_publish_failure(tid);
            log_spawn_diag(task, WkiRemoteSpawnResult::FAILED, "proxy-publication-failed", best_node);
            return WkiRemoteSpawnResult::FAILED;
        }
    }

    if (task->is_elf_buffer_shared) {
        release_cached_elf_buffer(task->elf_buffer);
    } else {
        delete[] task->elf_buffer;
    }
    task->elf_buffer = nullptr;
    task->elf_buffer_size = 0;
    task->elf_buffer_complete = true;
    if (task->exec_image_file != nullptr) {
        ker::vfs::vfs_put_file(task->exec_image_file);
        task->exec_image_file = nullptr;
        task->exec_image_size = 0;
    }
    if (!NEEDS_PROXY_PUBLICATION) {
        task->wki_proxy_task_id = tid;
        task->wki_proxy_task = true;
    }

    bool completed_immediately = false;
    int32_t completed_exit_status = 0;
    uint8_t* completed_output = nullptr;
    uint16_t completed_output_len = 0;
    ker::mod::sched::task::Task* completed_proxy_ref = nullptr;
    bool emit_proxy_ready_wait = false;
    uint32_t proxy_ready_wait_us = 0;
    bool emit_complete_hold = false;
    uint32_t complete_hold_us = 0;
    uint64_t const METRIC_NOW_US = wki_now_us();
    s_compute_lock.lock();
    SubmittedTask* st = find_submitted_task_any(tid);
    if (st != nullptr) {
        st->result_handle_owned = false;
        st->result_owner_task = nullptr;
        if (!st->set_local_task_ref(task)) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI remote compute: lost submitted proxy task lifetime");
            hcf();
        }
        st->local_pid = task != nullptr ? task->pid : 0;
        st->proxy_ready = proxy_ready;
        if (proxy_ready) {
            if (st->accepted_at_us != 0 && METRIC_NOW_US >= st->accepted_at_us) {
                emit_proxy_ready_wait = true;
                proxy_ready_wait_us = static_cast<uint32_t>(METRIC_NOW_US - st->accepted_at_us);
            }
            perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::PROXY_READY, st->target_node, tid, 0, 0, WOS_PERF_CALLSITE());
        }
        st->complete_pending = true;
        if (proxy_ready && !st->active) {
            completed_immediately = true;
            completed_exit_status = st->exit_status;
            st->complete_pending.store(false, std::memory_order_release);
            completed_output = st->pending_proxy_output;
            completed_output_len = st->pending_proxy_output_len;
            st->pending_proxy_output = nullptr;
            st->pending_proxy_output_len = 0;
            if (st->complete_received_at_us != 0 && METRIC_NOW_US >= st->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(METRIC_NOW_US - st->complete_received_at_us);
                st->complete_received_at_us = 0;
            }
            completed_proxy_ref = st->take_local_task_ref();
        }
    }
    s_compute_lock.unlock();

    if (proxy_ready && emit_proxy_ready_wait) {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, best_node, tid, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, best_node, tid, 0, proxy_ready_wait_us, 0, CALLSITE);
    }

    if (emit_complete_hold) {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, best_node, tid, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, best_node, tid, 0, complete_hold_us, 0, CALLSITE);
    }

    if (completed_immediately) {
        finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, tid);
        delete[] completed_output;
        request_submitted_task_reclaim(tid);
    }
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] Task '%s' (pid=0x%lx) placed as proxy on node 0x%04x (task_id=%u)", task->name, task->pid, best_node, tid);
#endif
    log_spawn_diag(task, WkiRemoteSpawnResult::REMOTE, "submitted", best_node);
    if (completed_proxy_ref != nullptr) {
        completed_proxy_ref->release();
    }
    return WkiRemoteSpawnResult::REMOTE;
}

// ===============================================================================
// Submitter Side - Task Submit (INLINE only in V1)
// ===============================================================================

auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len,
                            const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map,
                            uint16_t ipc_fd_count) -> uint32_t {
    auto cleanup_ipc_exports = [&]() { wki_ipc_cleanup_exported_fds(ipc_fd_map, ipc_fd_count, target_node); };
    if (binary == nullptr || binary_len == 0 || (ipc_fd_count != 0 && ipc_fd_map == nullptr)) {
        cleanup_ipc_exports();
        return 0;
    }

    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info)) {
        cleanup_ipc_exports();
        return 0;
    }

    // Check total size fits in WKI message:
    // TaskSubmitPayload + binary_len + binary + argv/envp/cwd + identity + policy + IPC fd entries.
    uint32_t const IPC_DATA_LEN = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    uint64_t const TOTAL =
        static_cast<uint64_t>(sizeof(TaskSubmitPayload)) + sizeof(uint32_t) + binary_len + context_info.data_len + IPC_DATA_LEN;
    if (TOTAL > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] Task binary too large for inline submit: %u bytes", binary_len);
        cleanup_ipc_exports();
        return 0;
    }

    SubmitterChannelToken const SUBMIT_CHANNEL = capture_submitter_channel(target_node);
    if (SUBMIT_CHANNEL.channel == nullptr) {
        cleanup_ipc_exports();
        return 0;
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = allocate_submitted_task_id_locked();

    // Create submitted task entry
    SubmittedTask st;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.submit_channel = SUBMIT_CHANNEL.channel;
    st.submit_channel_generation = SUBMIT_CHANNEL.generation;
    st.accept_status = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    if (!st.set_local_task_ref(local_task)) {
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    st.result_handle_owned = true;
    st.result_owner_task = ker::mod::sched::get_current_task();
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);

    if (publish_submitted_task_locked(std::move(st)) == nullptr) {
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, binary_len, CALLSITE);

    // Build TASK_SUBMIT message. Every byte in the transmitted prefix is
    // assigned below; do not pattern-initialize the unused bounded tail.
    auto msg_len = static_cast<uint16_t>(TOTAL);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> buf __attribute__((uninitialized));

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf.data());
    submit->task_id = TASK_ID;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::INLINE);
    submit->prefer_inline = 1;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->identity_len = context_info.identity_len;
    submit->ipc_fd_count = ipc_fd_count;
    submit->reserved = 0;

    // INLINE format: {binary_len:u32, binary[binary_len], argv/envp/cwd, identity, policy, ipc_fd_entries[]}
    uint8_t* cursor = buf.data() + sizeof(TaskSubmitPayload);
    memcpy(cursor, &binary_len, sizeof(uint32_t));
    cursor += sizeof(uint32_t);
    memcpy(cursor, binary, binary_len);
    cursor += binary_len;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.identity_len > 0) {
        copy_submit_identity_data(cursor, local_task);
        cursor += context_info.identity_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, IPC_DATA_LEN);
    }

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    s_compute_lock.lock();
    if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
        task_ptr->response_wait_entry = &wait;
        task_ptr->response_consumer_wait_entry = &wait;
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = wki_send_on_channel_generation(target_node, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation,
                                                        MsgType::TASK_SUBMIT, buf.data(), msg_len);

    if (SEND_RET != WKI_OK) {
        bool claimed_waiter = false;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_wait_entry == &wait) {
                task_ptr->response_wait_entry = nullptr;
            }
            // Keep the consumer slot and result owner published until every
            // claimant has made its final stack access.  Quiescence may yield;
            // fatal exit at that handoff must still be able to discover and
            // retire this otherwise-unlinked waiter.
            task_ptr->response_pending.store(false, std::memory_order_release);
            task_ptr->active = false;
        }
        claimed_waiter = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET);

        uint8_t* discarded_output = nullptr;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_consumer_wait_entry == &wait) {
                task_ptr->response_consumer_wait_entry = nullptr;
            }
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_ipc_exports();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, SEND_RET,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return 0;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    uint8_t accept_status = 0;
    s_compute_lock.lock();
    auto* task_ptr = find_submitted_task_any(TASK_ID);
    if (task_ptr != nullptr) {
        if (task_ptr->response_wait_entry == &wait) {
            task_ptr->response_wait_entry = nullptr;
        }
        if (task_ptr->response_consumer_wait_entry == &wait) {
            task_ptr->response_consumer_wait_entry = nullptr;
        }
        accept_status = task_ptr->accept_status;
    }
    if (WAIT_RC != 0) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        static_cast<void>(
            send_task_cancel_request(target_node, TASK_ID, WKI_SIGKILL_NUM, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation));
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] Task submit wait failed: task_id=%u target=0x%04x rc=%d (%s)", TASK_ID, target_node, WAIT_RC,
                           errno_name(WAIT_RC));
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return 0;
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->active = false;
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] Task rejected: task_id=%u status=%u", TASK_ID, accept_status);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID,
                                -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status),
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return 0;
    }
    s_compute_lock.unlock();

    ker::mod::dbg::log("[WKI] Task accepted: task_id=%u", TASK_ID);
    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, 0,
                            static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
    return TASK_ID;
}

// ===============================================================================
// Submitter Side - VFS_REF Submit (/ ===============================================================================

auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path,
                             const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map,
                             uint16_t ipc_fd_count) -> uint32_t {
    auto cleanup_ipc_exports = [&]() { wki_ipc_cleanup_exported_fds(ipc_fd_map, ipc_fd_count, target_node); };
    if (vfs_path == nullptr || vfs_path[0] == '\0' || (ipc_fd_count != 0 && ipc_fd_map == nullptr)) {
        cleanup_ipc_exports();
        return 0;
    }

    size_t const PATH_LEN = std::strlen(vfs_path);
    if (PATH_LEN == 0 || PATH_LEN >= 512 || PATH_LEN > UINT16_MAX) {
        cleanup_ipc_exports();
        return 0;
    }
    auto const PATH_LEN_WIRE = static_cast<uint16_t>(PATH_LEN);

    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info)) {
        cleanup_ipc_exports();
        return 0;
    }

    // TaskSubmitPayload + path_len + path + argv/envp/cwd + identity + policy + IPC fd entries.
    uint32_t const IPC_DATA_LEN = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint16_t) + PATH_LEN_WIRE + context_info.data_len + IPC_DATA_LEN);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] VFS_REF path too large: %u bytes", PATH_LEN_WIRE);
        cleanup_ipc_exports();
        return 0;
    }

    SubmitterChannelToken const SUBMIT_CHANNEL = capture_submitter_channel(target_node);
    if (SUBMIT_CHANNEL.channel == nullptr) {
        cleanup_ipc_exports();
        return 0;
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = allocate_submitted_task_id_locked();

    SubmittedTask st;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.submit_channel = SUBMIT_CHANNEL.channel;
    st.submit_channel_generation = SUBMIT_CHANNEL.generation;
    st.accept_status = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    if (!st.set_local_task_ref(local_task)) {
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    st.result_handle_owned = true;
    st.result_owner_task = ker::mod::sched::get_current_task();
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);

    if (publish_submitted_task_locked(std::move(st)) == nullptr) {
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, PATH_LEN_WIRE, CALLSITE);

    // Every byte in the transmitted prefix is assigned below; do not
    // pattern-initialize the unused bounded tail.
    auto msg_len = static_cast<uint16_t>(total);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> buf __attribute__((uninitialized));

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf.data());
    submit->task_id = TASK_ID;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::VFS_REF);
    submit->prefer_inline = 0;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->identity_len = context_info.identity_len;
    submit->ipc_fd_count = ipc_fd_count;
    submit->reserved = 0;

    // VFS_REF format: {path_len:u16, path[path_len], argv/envp/cwd, identity, policy, ipc_fd_entries[]}
    uint8_t* cursor = buf.data() + sizeof(TaskSubmitPayload);
    memcpy(cursor, &PATH_LEN_WIRE, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    memcpy(cursor, vfs_path, PATH_LEN_WIRE);
    cursor += PATH_LEN_WIRE;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.identity_len > 0) {
        copy_submit_identity_data(cursor, local_task);
        cursor += context_info.identity_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, IPC_DATA_LEN);
    }

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    s_compute_lock.lock();
    if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
        task_ptr->response_wait_entry = &wait;
        task_ptr->response_consumer_wait_entry = &wait;
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = wki_send_on_channel_generation(target_node, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation,
                                                        MsgType::TASK_SUBMIT, buf.data(), msg_len);

    if (SEND_RET != WKI_OK) {
        bool claimed_waiter = false;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_wait_entry == &wait) {
                task_ptr->response_wait_entry = nullptr;
            }
            // Keep the consumer slot and result owner published until every
            // claimant has made its final stack access.  Quiescence may yield;
            // fatal exit at that handoff must still be able to discover and
            // retire this otherwise-unlinked waiter.
            task_ptr->response_pending.store(false, std::memory_order_release);
            task_ptr->active = false;
        }
        claimed_waiter = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET);

        uint8_t* discarded_output = nullptr;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_consumer_wait_entry == &wait) {
                task_ptr->response_consumer_wait_entry = nullptr;
            }
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_ipc_exports();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, SEND_RET,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return 0;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    uint8_t accept_status = 0;
    s_compute_lock.lock();
    auto* task_ptr = find_submitted_task_any(TASK_ID);
    if (task_ptr != nullptr) {
        if (task_ptr->response_wait_entry == &wait) {
            task_ptr->response_wait_entry = nullptr;
        }
        if (task_ptr->response_consumer_wait_entry == &wait) {
            task_ptr->response_consumer_wait_entry = nullptr;
        }
        accept_status = task_ptr->accept_status;
    }
    if (WAIT_RC != 0) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        static_cast<void>(
            send_task_cancel_request(target_node, TASK_ID, WKI_SIGKILL_NUM, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation));
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] VFS_REF submit wait failed: task_id=%u rc=%d (%s) timeout_us=%llu", TASK_ID, WAIT_RC, errno_name(WAIT_RC),
                           WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return 0;
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->active = false;
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] VFS_REF task rejected: task_id=%u status=%u", TASK_ID, accept_status);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID,
                                -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status),
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return 0;
    }
    s_compute_lock.unlock();
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] VFS_REF task accepted: task_id=%u", TASK_ID);
#endif
    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, 0,
                            static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
    return TASK_ID;
}

// ===============================================================================
// Submitter Side - Wait for Completion
// ===============================================================================

auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int {
    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task_any(task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        return -1;
    }

    // The returned task ID is a one-shot result handle. It is not available
    // until the submit response caller has consumed acceptance, and a handler-
    // claimed completion remains reserved for its original stack consumer.
    if (!task->result_handle_owned || task->response_consumer_wait_entry != nullptr ||
        (!task->active && task->complete_consumer_wait_entry != nullptr)) {
        s_compute_lock.unlock();
        return -1;
    }
    auto* const CURRENT_TASK = ker::mod::sched::get_current_task();
    if (task->result_owner_task != nullptr && task->result_owner_task != CURRENT_TASK) {
        s_compute_lock.unlock();
        return -1;
    }
    task->result_owner_task = CURRENT_TASK;

    uint16_t const TARGET_NODE = task->target_node;
    if (!task->active) {
        int32_t const COMPLETED_EXIT_STATUS = task->exit_status;
        uint8_t* discarded_output = consume_submitted_task_result_locked(task);
        s_compute_lock.unlock();
        delete[] discarded_output;

        if (exit_status != nullptr) {
            *exit_status = COMPLETED_EXIT_STATUS;
        }
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, TARGET_NODE, task_id, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, TARGET_NODE, task_id, 0,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), 0, CALLSITE);
        return 0;
    }

    // V2 I-4: Set up async wait entry before marking pending
    WkiWaitEntry wait = {};
    if (!publish_task_complete_waiter_locked(task, wait)) {
        s_compute_lock.unlock();
        return -1;
    }
    s_compute_lock.unlock();

    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, TARGET_NODE, task_id, 0, CALLSITE);

    int const WAIT_RC = wki_wait_for_op(&wait, timeout_us);
    int32_t completed_exit_status = -1;
    bool task_still_present = false;
    s_compute_lock.lock();
    task = find_submitted_task_any(task_id);
    if (task != nullptr) {
        task_still_present = true;
        static_cast<void>(clear_task_complete_waiter_after_wait_locked(task, wait, WAIT_RC, completed_exit_status));
    }
    if (WAIT_RC == WKI_ERR_TIMEOUT) {
        s_compute_lock.unlock();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, TARGET_NODE, task_id, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), 0, CALLSITE);
        return -1;
    }
    if (task != nullptr) {
        task->active = false;
        uint8_t* discarded_output = consume_submitted_task_result_locked(task);
        s_compute_lock.unlock();
        delete[] discarded_output;
    } else {
        s_compute_lock.unlock();
    }

    if (exit_status != nullptr) {
        *exit_status = completed_exit_status;
    }

    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, TARGET_NODE, task_id, 0,
                            static_cast<uint32_t>(wki_now_us() - STARTED_US), 0, CALLSITE);

    return task_still_present ? 0 : -1;
}

void wki_proxy_task_blocked(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->wki_proxy_task_id == 0) {
        return;
    }

    bool completed_immediately = false;
    int32_t completed_exit_status = 0;
    uint8_t* completed_output = nullptr;
    uint16_t completed_output_len = 0;
    ker::mod::sched::task::Task* completed_proxy_ref = nullptr;
    uint16_t target_node = WKI_NODE_INVALID;
    bool emit_proxy_ready_wait = false;
    uint32_t proxy_ready_wait_us = 0;
    bool emit_complete_hold = false;
    uint32_t complete_hold_us = 0;
    uint64_t const METRIC_NOW_US = wki_now_us();

    s_compute_lock.lock();
    SubmittedTask* submitted = find_submitted_task_any(task->wki_proxy_task_id);
    if (submitted != nullptr && submitted->local_task == task) {
        target_node = submitted->target_node;
        submitted->proxy_ready = true;
        if (submitted->accepted_at_us != 0 && METRIC_NOW_US >= submitted->accepted_at_us) {
            emit_proxy_ready_wait = true;
            proxy_ready_wait_us = static_cast<uint32_t>(METRIC_NOW_US - submitted->accepted_at_us);
        }
        perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::PROXY_READY, submitted->target_node, task->wki_proxy_task_id, 0, 0,
                                  WOS_PERF_CALLSITE());
        if (!submitted->active) {
            completed_immediately = true;
            completed_exit_status = submitted->exit_status;
            submitted->complete_pending.store(false, std::memory_order_release);
            completed_output = submitted->pending_proxy_output;
            completed_output_len = submitted->pending_proxy_output_len;
            submitted->pending_proxy_output = nullptr;
            submitted->pending_proxy_output_len = 0;
            if (submitted->complete_received_at_us != 0 && METRIC_NOW_US >= submitted->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(METRIC_NOW_US - submitted->complete_received_at_us);
                submitted->complete_received_at_us = 0;
            }
            completed_proxy_ref = submitted->take_local_task_ref();
        }
    }
    s_compute_lock.unlock();

    if (emit_proxy_ready_wait) {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, target_node, task->wki_proxy_task_id, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, target_node, task->wki_proxy_task_id, 0,
                                proxy_ready_wait_us, 0, CALLSITE);
    }

    if (emit_complete_hold) {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, target_node, task->wki_proxy_task_id, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, target_node, task->wki_proxy_task_id, 0, complete_hold_us,
                                0, CALLSITE);
    }

    if (completed_immediately) {
        uint32_t const TASK_ID = task->wki_proxy_task_id;
        finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, TASK_ID);
        delete[] completed_output;
        request_submitted_task_reclaim(TASK_ID);
        completed_proxy_ref->release();
    }
}

// ===============================================================================
// Submitter Side - Cancel
// ===============================================================================

auto wki_task_cancel(uint32_t task_id, int signum) -> bool {
    SubmittedIpcCleanup cleanup = {};
    bool const TEARDOWN_EXPORTS = signum == WKI_SIGKILL_NUM || signum == WKI_SIGTERM_NUM;
    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        return false;
    }

    uint16_t const TARGET_NODE = task->target_node;
    WkiChannel* const SUBMIT_CHANNEL = task->submit_channel;
    uint32_t const SUBMIT_CHANNEL_GENERATION = task->submit_channel_generation;
    if (TEARDOWN_EXPORTS) {
        cleanup = submitted_ipc_cleanup_snapshot_locked(task);
    }
    s_compute_lock.unlock();

    int const SEND_RET = send_task_cancel_request(TARGET_NODE, task_id, signum, SUBMIT_CHANNEL, SUBMIT_CHANNEL_GENERATION);
    if (SEND_RET != WKI_OK) {
        return false;
    }

    // SIGINT is not proof that a remote interactive task will exit.  Keep the
    // submitter-side PTY/stdio exports alive so a remote shell can handle
    // Ctrl-C and return to its prompt instead of dropping the SSH session.
    if (TEARDOWN_EXPORTS) {
        cleanup_submitted_ipc_exports(cleanup);
    }
    return true;
}

void wki_remote_compute_cleanup_for_task(ker::mod::sched::task::Task* exiting_task) {
    if (exiting_task == nullptr) {
        return;
    }

    // A task owns at most one synchronous submit/wait at a time, but process
    // exit cleanup is rare and scanning to exhaustion also retires older
    // direct handles and any proxy row whose local representative is exiting.
    for (;;) {
        std::array<WkiWaitEntry*, 4> waiters = {};
        size_t waiter_count = 0;
        SubmittedIpcCleanup ipc_cleanup = {};
        uint8_t* discarded_output = nullptr;
        uint16_t target_node = WKI_NODE_INVALID;
        uint32_t task_id = 0;
        WkiChannel* submit_channel = nullptr;
        uint32_t submit_channel_generation = 0;
        bool cancel_remote = false;
        bool found = false;

        s_compute_lock.lock();
        for (auto& slot : g_submitted_tasks) {
            if (!slot.occupied) {
                continue;
            }
            SubmittedTask& submitted = slot.task;
            if (submitted.result_owner_task != exiting_task && submitted.local_task != exiting_task) {
                continue;
            }

            auto remember_waiter = [&](WkiWaitEntry* waiter) {
                if (waiter == nullptr) {
                    return;
                }
                for (size_t i = 0; i < waiter_count; ++i) {
                    if (waiters.at(i) == waiter) {
                        return;
                    }
                }
                if (waiter_count < waiters.size()) {
                    waiters.at(waiter_count++) = waiter;
                }
            };
            remember_waiter(submitted.response_wait_entry);
            remember_waiter(submitted.response_consumer_wait_entry);
            remember_waiter(submitted.complete_wait_entry);
            remember_waiter(submitted.complete_consumer_wait_entry);

            submitted.response_wait_entry = nullptr;
            submitted.response_consumer_wait_entry = nullptr;
            submitted.response_pending.store(false, std::memory_order_release);
            submitted.complete_wait_entry = nullptr;
            submitted.complete_consumer_wait_entry = nullptr;
            submitted.complete_pending.store(false, std::memory_order_release);

            target_node = submitted.target_node;
            task_id = submitted.task_id;
            submit_channel = submitted.submit_channel;
            submit_channel_generation = submitted.submit_channel_generation;
            cancel_remote = submitted.active;
            ipc_cleanup = submitted_ipc_cleanup_snapshot_locked(&submitted);
            submitted.ipc_fd_count = 0;
            submitted.exit_status = -1;
            submitted.active = false;
            discarded_output = consume_submitted_task_result_locked(&submitted);
            found = true;
            break;
        }
        s_compute_lock.unlock();

        if (!found) {
            break;
        }

        // Generic wait cleanup normally leaves each entry DONE. Cover a
        // response that completed before list linkage, or a claimant that was
        // already in flight, before the exiting task's stack can be reclaimed.
        for (size_t i = 0; i < waiter_count; ++i) {
            WkiWaitEntry* waiter = waiters.at(i);
            if (wki_claim_op(waiter)) {
                wki_finish_claimed_op(waiter, WKI_ERR_PEER_FENCED);
                continue;
            }
            wki_quiesce_claimed_op(waiter);
        }

        if (cancel_remote) {
            static_cast<void>(send_task_cancel_request(target_node, task_id, WKI_SIGKILL_NUM, submit_channel, submit_channel_generation));
        }
        cleanup_submitted_ipc_exports(ipc_cleanup);
        delete[] discarded_output;
    }
}

// ===============================================================================
//  Signal Forwarding for Proxy Tasks
// ===============================================================================

namespace {

auto task_process_owner_pid(const ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr) {
        return 0;
    }
    return ker::mod::sched::task::process_pid(*task);
}

auto queue_signal_for_process_tasks(uint64_t owner_pid, int signum) -> size_t {
    if (owner_pid == 0 || signum <= 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) {
        return 0;
    }

    size_t signaled = 0;
    uint64_t const MASK = 1ULL << (signum - 1);
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }
        if (!candidate->has_exited && task_process_owner_pid(candidate) == owner_pid) {
            candidate->signal_add_pending_mask(MASK);
            ker::mod::sched::wake_task_for_signal(candidate);
            ++signaled;
        }
        candidate->release();
    }
    return signaled;
}

auto running_task_matches_cleanup_locked(const RunningRemoteTask& running, uint16_t node_id, RemoteComputeCleanupScope scope) -> bool {
    if (!running.active || running.submitter_node != node_id) {
        return false;
    }
    if (scope == RemoteComputeCleanupScope::ALL || running.discard_completion) {
        return true;
    }

    ComputeSubmitSessionToken const SESSION = {
        .node_id = running.submitter_node,
        .rx_channel = running.submit_rx_channel,
        .rx_channel_generation = running.submit_rx_channel_generation,
        .epoch = running.submit_session_epoch,
    };
    return !compute_submit_session_is_current_locked(SESSION);
}

void terminate_running_remote_tasks_for_peer(uint16_t node_id, RemoteComputeCleanupScope scope) {
    for (;;) {
        constexpr size_t MAX_TERMINATIONS_PER_BATCH = 64;
        std::array<ker::mod::sched::task::Task*, MAX_TERMINATIONS_PER_BATCH> tasks_to_terminate = {};
        size_t termination_count = 0;
        bool drained_running = true;

        s_compute_lock.lock();
        for (auto& running : g_running_remote_tasks) {
            if (!running_task_matches_cleanup_locked(running, node_id, scope)) {
                continue;
            }
            running.discard_completion = true;
            if (running.termination_requested) {
                continue;
            }
            if (termination_count >= tasks_to_terminate.size()) {
                drained_running = false;
                continue;
            }
            running.termination_requested = true;
            if (running.task != nullptr && running.task->try_acquire()) {
                tasks_to_terminate.at(termination_count++) = running.task;
            }
        }
        s_compute_lock.unlock();

        for (size_t i = 0; i < termination_count; ++i) {
            auto* task = tasks_to_terminate.at(i);
            uint64_t const OWNER_PID = task_process_owner_pid(task);
            size_t const SIGNALED = queue_signal_for_process_tasks(OWNER_PID, WKI_SIGKILL_NUM);
            if (SIGNALED == 0 && !task->has_exited) {
                task->signal_add_pending_mask(1ULL << (WKI_SIGKILL_NUM - 1));
                ker::mod::sched::wake_task_for_signal(task);
            }
            task->release();
        }

        if (drained_running) {
            return;
        }
    }
}

}  // namespace

auto wki_proxy_task_forward_signal(ker::mod::sched::task::Task* task, int signum) -> bool {
    if (task == nullptr || task->wki_proxy_task_id == 0) {
        return false;
    }

    // A proxy must never consume a signal by resuming its stale local user
    // frame. The remote TASK_CANCEL handler accepts the complete kernel signal
    // range, so forward every valid signal and leave the proxy parked until
    // TASK_COMPLETE or peer fencing owns finalization.
    if (signum <= 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) {
        return false;
    }

    uint32_t const PROXY_TID = task->wki_proxy_task_id;
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] Forwarding signal %d to remote task_id=%u (proxy pid=0x%lx)", signum, PROXY_TID, task->pid);
#endif
    // The proxy task will be cleaned up when TASK_COMPLETE arrives
    // (or if the peer is fenced, the fencing cleanup will handle it)
    return wki_task_cancel(PROXY_TID, signum);
}

auto wki_proxy_task_find_by_remote_pid_safe(uint64_t remote_pid) -> ker::mod::sched::task::Task* {
    if (remote_pid == 0) {
        return nullptr;
    }

    ker::mod::sched::task::Task* match = nullptr;
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }

        bool const IS_PROXY_ALIAS = candidate->wki_proxy_task_id != 0 && candidate->wki_remote_pid == remote_pid;
        bool const IS_ACTIVE = candidate->state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE;
        bool const IS_LIVE_PROXY = IS_PROXY_ALIAS && IS_ACTIVE && !candidate->has_exited;
        if (!IS_LIVE_PROXY) {
            candidate->release();
            continue;
        }

        if (match != nullptr) {
            match->release();
            candidate->release();
            return nullptr;
        }
        match = candidate;
    }

    return match;
}

auto wki_proxy_task_remote_info(const ker::mod::sched::task::Task* task, uint16_t* target_node, char* hostname, size_t hostname_size)
    -> bool {
    if (task == nullptr || task->wki_proxy_task_id == 0) {
        return false;
    }

    uint16_t node = WKI_NODE_INVALID;
    s_compute_lock.lock();
    SubmittedTask const* submitted = find_submitted_task_any(task->wki_proxy_task_id);
    if (submitted != nullptr) {
        node = submitted->target_node;
    }
    s_compute_lock.unlock();

    if (node == WKI_NODE_INVALID) {
        return false;
    }

    if (target_node != nullptr) {
        *target_node = node;
    }
    if (hostname != nullptr && hostname_size > 0) {
        hostname[0] = '\0';
        const char* peer_hostname = wki_peer_get_hostname(node);
        if (peer_hostname != nullptr && peer_hostname[0] != '\0') {
            std::strncpy(hostname, peer_hostname, hostname_size - 1);
            hostname[hostname_size - 1] = '\0';
        }
    }
    return true;
}

// ===============================================================================
// Load Reporting
// ===============================================================================

void wki_load_report_send() {
    if (!g_remote_compute_initialized) {
        return;
    }

    uint64_t const NOW = wki_now_us();
    if (NOW - g_last_load_report_us < WKI_LOAD_REPORT_INTERVAL_US) {
        return;
    }
    g_last_load_report_us = NOW;

    // Build LoadReportPayload with real scheduler metrics
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::get_core_count());
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    uint16_t const REPORT_CPUS = std::min(cpu_count, WKI_LOAD_REPORT_MAX_CPUS);

    LoadReportPayload report = {};
    report.num_cpus = REPORT_CPUS;
    auto const LOADAVG = ker::mod::sched::get_load_average_snapshot();
    report.runnable_tasks = static_cast<uint16_t>(std::min<uint32_t>(LOADAVG.runnable_tasks, 0xFFFFU));
    report.free_mem_pages = static_cast<uint16_t>(std::min(ker::mod::mm::phys::get_free_mem_pages() / 256ULL, 0xFFFFULL));

    constexpr size_t LOAD_REPORT_BUFFER_SIZE = sizeof(LoadReportPayload) + (WKI_LOAD_REPORT_MAX_CPUS * sizeof(uint16_t));
    std::array<uint8_t, LOAD_REPORT_BUFFER_SIZE> buf = {};
    std::array<ker::mod::sched::CpuAccountingSnapshot, WKI_LOAD_REPORT_MAX_CPUS> current_accounting = {};
    uint64_t total_delta_busy_us = 0;
    uint64_t total_delta_time_us = 0;

    for (uint16_t c = 0; c < REPORT_CPUS; ++c) {
        current_accounting.at(c) = ker::mod::sched::get_cpu_accounting_snapshot(c);

        uint16_t cpu_load = 0;
        if (g_last_local_cpu_accounting_valid) {
            auto const& previous = g_last_local_cpu_accounting.at(c);
            cpu_load = snapshot_delta_pct_milli(current_accounting.at(c), previous);

            uint64_t const TOTAL_NOW = snapshot_total_time_us(current_accounting.at(c));
            uint64_t const TOTAL_PREV = snapshot_total_time_us(previous);
            if (TOTAL_NOW > TOTAL_PREV) {
                total_delta_time_us += TOTAL_NOW - TOTAL_PREV;

                uint64_t const BUSY_NOW = snapshot_busy_time_us(current_accounting.at(c));
                uint64_t const BUSY_PREV = snapshot_busy_time_us(previous);
                if (BUSY_NOW > BUSY_PREV) {
                    total_delta_busy_us += BUSY_NOW - BUSY_PREV;
                }
            }
        } else {
            auto const STATS = ker::mod::sched::get_run_queue_stats(c);
            cpu_load = static_cast<uint16_t>(STATS.active_task_count + STATS.wait_queue_count);
        }

        std::memcpy(buf.data() + sizeof(LoadReportPayload) + (static_cast<size_t>(c) * sizeof(uint16_t)), &cpu_load, sizeof(cpu_load));
    }

    if (g_last_local_cpu_accounting_valid && total_delta_time_us != 0) {
        report.avg_load_pct = static_cast<uint16_t>(std::min<uint64_t>((total_delta_busy_us * 1000ULL) / total_delta_time_us, 1000ULL));
    } else {
        report.avg_load_pct = compute_local_runnable_load_fallback();
    }

    s_compute_lock.lock();
    for (uint16_t c = 0; c < REPORT_CPUS; ++c) {
        g_last_local_cpu_accounting.at(c) = current_accounting.at(c);
    }
    g_last_local_cpu_accounting_valid = true;
    g_cached_local_load_pct = report.avg_load_pct;
    g_cached_local_load_update_us = NOW;
    s_compute_lock.unlock();

    auto total_len = static_cast<uint16_t>(sizeof(LoadReportPayload) + (REPORT_CPUS * sizeof(uint16_t)));
    memcpy(buf.data(), &report, sizeof(LoadReportPayload));

    // Send to all CONNECTED peers
    for (auto* peer = std::begin(g_wki.peers); peer != std::end(g_wki.peers); ++peer) {
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }

        wki_send(peer->node_id, WKI_CHAN_EVENT_BUS, MsgType::LOAD_REPORT, buf.data(), total_len);
    }
}

auto wki_local_node_load_pct() -> uint16_t {
    uint16_t cached = 0;
    uint64_t updated_at_us = 0;

    s_compute_lock.lock();
    cached = g_cached_local_load_pct;
    updated_at_us = g_cached_local_load_update_us;
    s_compute_lock.unlock();

    uint64_t const NOW = wki_now_us();
    if (updated_at_us != 0 && NOW >= updated_at_us && NOW - updated_at_us <= (WKI_LOAD_REPORT_INTERVAL_US * 2)) {
        return cached;
    }

    return compute_local_runnable_load_fallback();
}

auto wki_remote_node_load_snapshot(uint16_t node_id, RemoteNodeLoad* out) -> bool {
    if (out == nullptr) {
        return false;
    }

    s_compute_lock.lock();
    RemoteNodeLoad const* load = find_remote_load(node_id);
    bool const FOUND = load != nullptr && load->valid;
    if (FOUND) {
        *out = *load;
    }
    s_compute_lock.unlock();
    return FOUND;
}

auto wki_least_loaded_node(uint16_t local_load) -> uint16_t {
    uint16_t best_node = WKI_NODE_INVALID;
    uint16_t best_load = local_load;

    for (const auto& rl : g_remote_loads) {
        if (!rl.valid) {
            continue;
        }

        // Stale load reports (>1s old) are not considered
        uint64_t const AGE = wki_now_us() - rl.last_update_us;
        if (AGE > 1000000) {
            continue;
        }

        // Apply remote placement penalty
        uint16_t const ADJUSTED = rl.avg_load_pct + WKI_REMOTE_PLACEMENT_PENALTY;
        if (ADJUSTED < best_load) {
            best_load = ADJUSTED;
            best_node = rl.node_id;
        }
    }

    return best_node;
}

namespace {

// s_compute_lock must be held by caller.
auto wki_preferred_remote_node() -> uint16_t {
    std::array<uint16_t, WKI_MAX_PEERS> candidates = {};
    size_t candidate_count = 0;
    uint32_t best_load = UINT32_MAX;
    uint64_t const NOW = wki_now_us();

    for (const auto& rl : g_remote_loads) {
        if (!rl.valid) {
            continue;
        }
        if (NOW - rl.last_update_us > 1000000) {
            continue;
        }
        auto* peer = wki_peer_find(rl.node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            continue;
        }
        uint32_t const ADJUSTED = static_cast<uint32_t>(rl.avg_load_pct) + WKI_REMOTE_PLACEMENT_PENALTY;
        if (ADJUSTED < best_load) {
            best_load = ADJUSTED;
            candidate_count = 0;
        }
        if (ADJUSTED == best_load && candidate_count < candidates.size()) {
            candidates.at(candidate_count++) = rl.node_id;
        }
    }

    if (candidate_count != 0) {
        uint16_t const NODE = candidates.at(g_preferred_remote_cursor % candidate_count);
        ++g_preferred_remote_cursor;
        return NODE;
    }

    return WKI_NODE_INVALID;
}

}  // namespace

// ===============================================================================
// Fencing Cleanup
// ===============================================================================

void wki_remote_compute_retire_submit_session(uint16_t node_id) {
    s_compute_lock.lock();
    auto* peer_session = get_or_create_compute_submit_peer_session_locked(node_id);
    if (peer_session == nullptr) {
        s_compute_lock.unlock();
        return;
    }
    peer_session->retiring = true;
    ComputeSubmitSessionToken const RETIRED_SESSION = peer_session->current;
    s_compute_lock.unlock();

    WkiChannel* channel = RETIRED_SESSION.rx_channel;
    uint32_t generation = RETIRED_SESSION.rx_channel_generation;
    if (channel == nullptr || generation == 0) {
        channel = wki_channel_lookup(node_id, WKI_CHAN_RESOURCE);
        if (channel != nullptr) {
            channel->lock.lock();
            if (channel->active && channel->peer_node_id == node_id && channel->channel_id == WKI_CHAN_RESOURCE) {
                generation = channel->generation;
            }
            channel->lock.unlock();
        }
    }
    if (channel != nullptr && generation != 0) {
        static_cast<void>(wki_channel_close_generation(channel, node_id, WKI_CHAN_RESOURCE, generation));
    }

    s_compute_lock.lock();
    retire_compute_submit_session_locked(node_id, channel, generation);
    peer_session = find_compute_submit_peer_session_locked(node_id);
    if (peer_session != nullptr) {
        peer_session->retiring = false;
    }
    for (auto& entry : g_shared_elf_cache) {
        if (entry.submitter_node != node_id) {
            continue;
        }
        if (entry.loading) {
            entry.loading = false;
            entry.load_status = -1;
        }
        entry.valid = false;
    }
    s_compute_lock.unlock();
}

namespace {

auto submitted_task_matches_cleanup_locked(const SubmittedTask& submitted, uint16_t node_id, RemoteComputeCleanupScope scope) -> bool {
    if (!submitted.active || submitted.target_node != node_id) {
        return false;
    }
    return scope == RemoteComputeCleanupScope::ALL ||
           !compute_submit_channel_token_matches(node_id, submitted.submit_channel, submitted.submit_channel_generation);
}

void fail_submitted_tasks_for_peer(uint16_t node_id, RemoteComputeCleanupScope scope) {
    constexpr size_t MAX_PROXIES_PER_BATCH = 64;
    constexpr size_t MAX_WAITERS_PER_BATCH = MAX_PROXIES_PER_BATCH * 2;

    for (;;) {
        std::array<ker::mod::sched::task::Task*, MAX_PROXIES_PER_BATCH> proxy_tasks = {};
        std::array<uint32_t, MAX_PROXIES_PER_BATCH> proxy_task_ids = {};
        size_t proxy_count = 0;
        std::array<WkiWaitEntry*, MAX_WAITERS_PER_BATCH> waiters_to_finish = {};
        size_t waiter_count = 0;
        std::array<SubmittedIpcCleanup, MAX_PROXIES_PER_BATCH> ipc_cleanups = {};
        size_t ipc_cleanup_count = 0;
        bool drained_submitted = true;

        s_compute_lock.lock();

        // Fail submitted tasks targeting this peer in bounded batches. Each
        // stack waiter is claimed while s_compute_lock still owns the published
        // pointer so timeout and late-response paths cannot touch it after the
        // submitter unwinds.
        for (auto& slot : g_submitted_tasks) {
            if (!slot.occupied) {
                continue;
            }
            auto& t = slot.task;
            if (!submitted_task_matches_cleanup_locked(t, node_id, scope)) {
                continue;
            }

            size_t needed_waiters = 0;
            if (t.response_pending.load(std::memory_order_acquire) && t.response_wait_entry != nullptr) {
                needed_waiters++;
            }
            if (t.complete_pending.load(std::memory_order_acquire) && t.complete_wait_entry != nullptr) {
                needed_waiters++;
            }
            size_t const NEEDED_PROXIES = (t.local_task != nullptr && t.proxy_ready) ? 1U : 0U;
            size_t const NEEDED_IPC_CLEANUPS = t.ipc_fd_count != 0 ? 1U : 0U;
            if (waiter_count + needed_waiters > waiters_to_finish.size() || proxy_count + NEEDED_PROXIES > proxy_tasks.size() ||
                ipc_cleanup_count + NEEDED_IPC_CLEANUPS > ipc_cleanups.size()) {
                drained_submitted = false;
                break;
            }

            // Any active submit aimed at a fenced peer is terminal failure,
            // even if the local exec proxy has not reached its deferred
            // blocked state yet.  wki_proxy_task_blocked() may finalize that
            // proxy later and must not consume the default success status.
            t.exit_status = -1;

            if (t.response_pending.load(std::memory_order_acquire)) {
                WkiWaitEntry* waiter = claim_and_clear_waiter_locked(t.response_wait_entry);
                if (waiter != nullptr) {
                    t.accept_status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
                    *std::next(waiters_to_finish.begin(), static_cast<ptrdiff_t>(waiter_count++)) = waiter;
                }
                t.response_pending.store(false, std::memory_order_release);
            }
            if (t.complete_pending.load(std::memory_order_acquire)) {
                WkiWaitEntry* waiter = claim_and_clear_waiter_locked(t.complete_wait_entry);
                if (waiter != nullptr) {
                    *std::next(waiters_to_finish.begin(), static_cast<ptrdiff_t>(waiter_count++)) = waiter;
                }
                t.complete_pending.store(false, std::memory_order_release);
            }

            // Collect only proxies that are already safely parked.
            if (t.local_task != nullptr && t.proxy_ready) {
                *std::next(proxy_tasks.begin(), static_cast<ptrdiff_t>(proxy_count)) = t.take_local_task_ref();
                *std::next(proxy_task_ids.begin(), static_cast<ptrdiff_t>(proxy_count)) = t.task_id;
                proxy_count++;
            }
            if (t.ipc_fd_count != 0) {
                ipc_cleanups.at(ipc_cleanup_count++) = submitted_ipc_cleanup_snapshot_locked(&t);
                t.ipc_fd_count = 0;
            }

            t.active = false;
        }

        s_compute_lock.unlock();

        for (size_t i = 0; i < waiter_count; i++) {
            wki_finish_claimed_op(*std::next(waiters_to_finish.begin(), static_cast<ptrdiff_t>(i)), -1);
        }

        for (size_t i = 0; i < proxy_count; i++) {
            auto* proxy = *std::next(proxy_tasks.begin(), static_cast<ptrdiff_t>(i));
            uint32_t const TASK_ID = *std::next(proxy_task_ids.begin(), static_cast<ptrdiff_t>(i));
            finalize_proxy_task(proxy, -1, nullptr, 0, TASK_ID);
            request_submitted_task_reclaim(TASK_ID);

            ker::mod::dbg::log("[WKI] Proxy task cleanup: pid=0x%lx (peer 0x%04x)", proxy->pid, node_id);
            proxy->release();
        }

        for (size_t i = 0; i < ipc_cleanup_count; ++i) {
            cleanup_submitted_ipc_exports(ipc_cleanups.at(i));
        }

        if (drained_submitted) {
            break;
        }
    }
}

}  // namespace

void wki_remote_compute_cleanup_for_peer(uint16_t node_id) {
    wki_remote_compute_retire_submit_session(node_id);
    fail_submitted_tasks_for_peer(node_id, RemoteComputeCleanupScope::ALL);

    terminate_running_remote_tasks_for_peer(node_id, RemoteComputeCleanupScope::ALL);

    s_compute_lock.lock();

    // Invalidate load cache for this peer
    std::erase_if(g_remote_loads, [node_id](const RemoteNodeLoad& rl) { return rl.node_id == node_id; });

    // The process can still write through stdout/stderr File::private_data, so
    // keep its capture and lifetime reference until the termination requested
    // above reaches exit. The completion scanner releases both afterward.
    compact_running_remote_tasks_locked();

    for (auto& completion : g_pending_task_completions) {
        if (completion.submitter_node != node_id) {
            continue;
        }
        delete completion.output;
        completion.output = nullptr;
        completion.submitter_node = WKI_NODE_INVALID;
    }
    compact_pending_task_completions_locked();

    for (auto& entry : g_shared_elf_cache) {
        if (entry.submitter_node != node_id) {
            continue;
        }
        if (entry.loading) {
            entry.loading = false;
            entry.load_status = -1;
        }
        entry.valid = false;
    }
    gc_shared_elf_cache_locked(wki_now_us());

    s_compute_lock.unlock();
}

void wki_remote_compute_cleanup_retired_for_peer(uint16_t node_id) {
    // Peer epoch/channel resets keep the peer connected. Fail only work still
    // bound to a retired resource-channel generation so submissions admitted
    // after reconnection remain intact.
    fail_submitted_tasks_for_peer(node_id, RemoteComputeCleanupScope::RETIRED);
    terminate_running_remote_tasks_for_peer(node_id, RemoteComputeCleanupScope::RETIRED);

    s_compute_lock.lock();
    for (auto& completion : g_pending_task_completions) {
        if (completion.submitter_node != node_id) {
            continue;
        }
        ComputeSubmitSessionToken const SESSION = {
            .node_id = completion.submitter_node,
            .rx_channel = completion.submit_rx_channel,
            .rx_channel_generation = completion.submit_rx_channel_generation,
            .epoch = completion.submit_session_epoch,
        };
        if (compute_submit_session_is_current_locked(SESSION)) {
            continue;
        }
        delete completion.output;
        completion.output = nullptr;
        completion.submitter_node = WKI_NODE_INVALID;
    }
    compact_pending_task_completions_locked();
    gc_shared_elf_cache_locked(wki_now_us());
    s_compute_lock.unlock();
}

#ifdef WOS_SELFTEST
void remove_submitted_task_for_selftest_locked(uint32_t task_id) {
    SubmittedTask* task = find_submitted_task_any(task_id);
    if (task == nullptr || task->pending_proxy_output != nullptr) {
        return;
    }
    task->active = false;
    task->response_pending.store(false, std::memory_order_relaxed);
    task->response_wait_entry = nullptr;
    task->response_consumer_wait_entry = nullptr;
    task->complete_pending.store(false, std::memory_order_relaxed);
    task->complete_wait_entry = nullptr;
    task->complete_consumer_wait_entry = nullptr;
    task->reset_local_task_ref();
    task->result_handle_owned = false;
    task->result_owner_task = nullptr;
    request_submitted_task_reclaim_locked(task);
}

auto wki_remote_compute_selftest_cleanup_marks_unready_proxy_failure() -> bool {
    constexpr uint16_t TARGET_NODE = 0x7A21;
    constexpr uint32_t TASK_ID = 0xC0FFEEU;

    SubmittedTask task;
    task.active = true;
    task.task_id = TASK_ID;
    task.target_node = TARGET_NODE;
    task.exit_status = 0;
    task.proxy_ready = false;
    task.complete_pending.store(false, std::memory_order_relaxed);

    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID);
    bool const PUBLISHED = publish_submitted_task_locked(std::move(task)) != nullptr;
    s_compute_lock.unlock();

    if (!PUBLISHED) {
        return false;
    }

    wki_remote_compute_cleanup_for_peer(TARGET_NODE);

    bool ok = false;
    s_compute_lock.lock();
    if (SubmittedTask const* submitted = find_submitted_task_any(TASK_ID); submitted != nullptr) {
        ok = !submitted->active && submitted->exit_status == -1 && !submitted->complete_pending.load(std::memory_order_relaxed);
    }
    remove_submitted_task_for_selftest_locked(TASK_ID);
    s_compute_lock.unlock();

    return ok;
}

auto wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence() -> bool {
    ker::mod::sched::task::Task waiter{};
    ker::mod::sched::task::Task stale_waiter{};
    ker::mod::sched::task::Task proxy{};

    waiter.pid = 0x7A21;
    stale_waiter.pid = waiter.pid;
    proxy.pid = 0x7A22;
    proxy.parent_pid = waiter.pid;
    ker::mod::sched::task::task_clear_waited_on(proxy);

    waiter.context.regs.rax = 0xBAD0;
    waiter.waiting_for_pid = proxy.pid;
    waiter.deferred_task_switch = false;
    waiter.waitpid_publish_pending.store(true, std::memory_order_release);

    bool const BLOCKED_WHILE_PUBLISHING = !try_complete_proxy_wait(&waiter, &proxy, 0x1234) &&
                                          !ker::mod::sched::task::task_waited_on(proxy) && waiter.context.regs.rax == 0xBAD0 &&
                                          waiter.waiting_for_pid == proxy.pid;

    waiter.waitpid_publish_pending.store(false, std::memory_order_release);
    bool const COMPLETED_AFTER_PUBLISH = try_complete_proxy_wait(&waiter, &proxy, 0x1234) && ker::mod::sched::task::task_waited_on(proxy) &&
                                         waiter.context.regs.rax == proxy.pid && waiter.waiting_for_pid == 0;

    ker::mod::sched::task::task_clear_waited_on(proxy);
    stale_waiter.context.regs.rax = 0xCAFE;
    stale_waiter.waiting_for_pid = proxy.pid + 1;
    bool const REJECTED_STALE_SPECIFIC_WAIT = !try_complete_proxy_wait(&stale_waiter, &proxy, 0x1234) &&
                                              !ker::mod::sched::task::task_waited_on(proxy) && stale_waiter.context.regs.rax == 0xCAFE &&
                                              stale_waiter.waiting_for_pid == proxy.pid + 1;

    return BLOCKED_WHILE_PUBLISHING && COMPLETED_AFTER_PUBLISH && REJECTED_STALE_SPECIFIC_WAIT;
}

auto wki_remote_compute_selftest_task_wait_consumes_completed_row() -> bool {
    constexpr uint16_t TARGET_NODE = 0x7A23;
    constexpr uint32_t TASK_ID = 0xC0FFEFU;
    constexpr int32_t EXIT_STATUS = 0x4D2;
    WkiWaitEntry in_flight_consumer = {};
    ker::mod::sched::task::Task direct_task = {};

    SubmittedTask task;
    task.active = false;
    task.task_id = TASK_ID;
    task.target_node = TARGET_NODE;
    task.exit_status = EXIT_STATUS;
    task.result_handle_owned = true;
    task.result_owner_task = ker::mod::sched::get_current_task();
    task.complete_consumer_wait_entry = &in_flight_consumer;
    if (!task.set_local_task_ref(&direct_task)) {
        return false;
    }
    task.pending_proxy_output = new (std::nothrow) uint8_t[4];
    task.pending_proxy_output_len = task.pending_proxy_output != nullptr ? 4 : 0;
    if (task.pending_proxy_output == nullptr) {
        return false;
    }

    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID);
    bool const PUBLISHED = publish_submitted_task_locked(std::move(task)) != nullptr;
    s_compute_lock.unlock();

    if (!PUBLISHED) {
        return false;
    }

    int32_t blocked_status = -1;
    int const BLOCKED_WAIT_RC = wki_task_wait(TASK_ID, &blocked_status, 0);

    s_compute_lock.lock();
    SubmittedTask* stored = find_submitted_task_any(TASK_ID);
    bool const CONSUMER_PIN_PRESERVED =
        stored != nullptr && stored->result_handle_owned && stored->local_task == &direct_task && stored->pending_proxy_output != nullptr;
    if (stored != nullptr) {
        stored->complete_consumer_wait_entry = nullptr;
    }
    s_compute_lock.unlock();

    int32_t observed_status = -1;
    int const WAIT_RC = wki_task_wait(TASK_ID, &observed_status, 0);
    int32_t second_status = -1;
    int const SECOND_WAIT_RC = wki_task_wait(TASK_ID, &second_status, 0);

    s_compute_lock.lock();
    bool const ROW_RETIRED = find_submitted_task_any(TASK_ID) == nullptr;
    remove_submitted_task_for_selftest_locked(TASK_ID);
    s_compute_lock.unlock();

    return BLOCKED_WAIT_RC == -1 && blocked_status == -1 && CONSUMER_PIN_PRESERVED && WAIT_RC == 0 && observed_status == EXIT_STATUS &&
           SECOND_WAIT_RC == -1 && ROW_RETIRED;
}

auto wki_remote_compute_selftest_task_wait_timeout_preserves_successor() -> bool {
    SubmittedTask task;
    WkiWaitEntry pending_wait = {};
    WkiWaitEntry contender_wait = {};
    WkiWaitEntry stale_wait = {};
    WkiWaitEntry owned_wait = {};

    task.active = true;
    task.task_id = 0xC10000U;
    task.target_node = 0x7A24;
    task.exit_status = 33;
    task.complete_wait_entry = &pending_wait;
    task.complete_consumer_wait_entry = &pending_wait;
    task.complete_pending.store(true, std::memory_order_release);

    bool const BUSY_REJECTED = !publish_task_complete_waiter_locked(&task, contender_wait) && task.complete_wait_entry == &pending_wait &&
                               task.complete_pending.load(std::memory_order_acquire);

    int32_t observed_status = 0;
    bool const STALE_OWNED = clear_task_complete_waiter_after_wait_locked(&task, stale_wait, WKI_ERR_TIMEOUT, observed_status);
    bool const SUCCESSOR_PRESERVED = !STALE_OWNED && task.complete_wait_entry == &pending_wait &&
                                     task.complete_pending.load(std::memory_order_acquire) && observed_status == task.exit_status;

    task.complete_wait_entry = nullptr;
    task.complete_consumer_wait_entry = nullptr;
    task.complete_pending.store(false, std::memory_order_release);
    bool const PUBLISHED_AFTER_CLEAR = publish_task_complete_waiter_locked(&task, owned_wait) && task.complete_wait_entry == &owned_wait &&
                                       task.complete_consumer_wait_entry == &owned_wait &&
                                       task.complete_pending.load(std::memory_order_acquire);

    bool const OWNED_CLEARED = clear_task_complete_waiter_after_wait_locked(&task, owned_wait, WKI_ERR_TIMEOUT, observed_status) &&
                               task.complete_wait_entry == nullptr && task.complete_consumer_wait_entry == nullptr &&
                               !task.complete_pending.load(std::memory_order_acquire) && observed_status == task.exit_status;

    return BUSY_REJECTED && SUCCESSOR_PRESERVED && PUBLISHED_AFTER_CLEAR && OWNED_CLEARED;
}

auto wki_remote_compute_selftest_task_exit_retires_wait_owners() -> bool {
    enum class ExitCase : uint8_t {
        RESPONSE,
        COMPLETE,
        DONE_UNLINKED,
    };

    ker::mod::sched::task::Task exiting_task = {};
    exiting_task.pid = 0x7A26;

    auto run_case = [&](uint32_t task_id, ExitCase exit_case) {
        WkiWaitEntry wait = {};
        ker::mod::sched::task::Task submitted_subject = {};
        SubmittedTask submitted = {};
        submitted.active = true;
        submitted.task_id = task_id;
        submitted.target_node = WKI_NODE_INVALID;
        submitted.pending_proxy_output = new (std::nothrow) uint8_t[4];
        submitted.pending_proxy_output_len = submitted.pending_proxy_output != nullptr ? 4 : 0;
        if (submitted.pending_proxy_output == nullptr) {
            return false;
        }

        submitted.result_handle_owned = true;
        submitted.result_owner_task = &exiting_task;
        if (!submitted.set_local_task_ref(&submitted_subject)) {
            return false;
        }
        if (exit_case == ExitCase::RESPONSE || exit_case == ExitCase::DONE_UNLINKED) {
            submitted.response_consumer_wait_entry = &wait;
            if (exit_case == ExitCase::RESPONSE) {
                submitted.response_wait_entry = &wait;
                submitted.response_pending.store(true, std::memory_order_release);
            }
        } else if (exit_case == ExitCase::COMPLETE) {
            submitted.complete_wait_entry = &wait;
            submitted.complete_consumer_wait_entry = &wait;
            submitted.complete_pending.store(true, std::memory_order_release);
        }

        s_compute_lock.lock();
        remove_submitted_task_for_selftest_locked(task_id);
        bool const PUBLISHED = publish_submitted_task_locked(std::move(submitted)) != nullptr;
        s_compute_lock.unlock();
        if (!PUBLISHED) {
            return false;
        }

        bool const LINKED_CASE = exit_case == ExitCase::RESPONSE || exit_case == ExitCase::COMPLETE;
        if (LINKED_CASE) {
            wait.task.store(&exiting_task, std::memory_order_release);
            wki_selftest_wait_list_link(&wait);
        } else if (exit_case == ExitCase::DONE_UNLINKED) {
            wait.task.store(&exiting_task, std::memory_order_release);
            wait.result = 17;
            wait.state.store(WkiWaitEntry::DONE, std::memory_order_release);
        }

        wki_wait_cleanup_for_task(&exiting_task);

        bool const WAIT_RETIRED = !LINKED_CASE || (!wki_selftest_wait_list_contains(&wait) &&
                                                   wait.state.load(std::memory_order_acquire) == static_cast<uint8_t>(WkiWaitEntry::DONE) &&
                                                   wait.task.load(std::memory_order_acquire) == nullptr);
        s_compute_lock.lock();
        bool const ROW_RETIRED = find_submitted_task_any(task_id) == nullptr;
        remove_submitted_task_for_selftest_locked(task_id);
        s_compute_lock.unlock();
        bool const PUBLISHED_WAITER_RETIRED = !wki_claim_op(&wait);
        return WAIT_RETIRED && PUBLISHED_WAITER_RETIRED && ROW_RETIRED;
    };

    return run_case(0xC10020U, ExitCase::RESPONSE) && run_case(0xC10021U, ExitCase::COMPLETE) &&
           run_case(0xC10022U, ExitCase::DONE_UNLINKED);
}

auto wki_remote_compute_selftest_submitted_slots_reclaim_safely() -> bool {
    constexpr uint32_t TASK_ID_A = 0xC10010U;
    constexpr uint32_t TASK_ID_B = 0xC10011U;
    WkiWaitEntry response_consumer = {};
    WkiWaitEntry complete_consumer = {};
    ker::mod::sched::task::Task proxy = {};
    auto* pending_output = new (std::nothrow) uint8_t[4];
    if (pending_output == nullptr) {
        return false;
    }

    bool response_pin_preserved = false;
    bool complete_pin_preserved = false;
    bool proxy_ownership_preserved = false;
    bool proxy_lifetime_ref_held = false;
    bool proxy_lifetime_ref_released = false;
    bool row_reclaimed = false;
    bool slot_reused = false;
    bool ipc_metadata_did_not_pin = false;
    uint8_t* detached_output = nullptr;

    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID_A);
    remove_submitted_task_for_selftest_locked(TASK_ID_B);
    size_t const COUNT_BEFORE = s_submitted_task_count;

    SubmittedTask first = {};
    first.task_id = TASK_ID_A;
    first.active = false;
    first.response_consumer_wait_entry = &response_consumer;
    SubmittedTask* stored = publish_submitted_task_locked(std::move(first));
    size_t const HIGH_WATER_AFTER_FIRST = g_submitted_tasks.size();
    if (stored != nullptr) {
        request_submitted_task_reclaim_locked(stored);
        response_pin_preserved = find_submitted_task_any(TASK_ID_A) == stored;

        stored->response_consumer_wait_entry = nullptr;
        stored->complete_consumer_wait_entry = &complete_consumer;
        reclaim_submitted_task_if_safe_locked(stored);
        complete_pin_preserved = find_submitted_task_any(TASK_ID_A) == stored;

        if (!stored->set_local_task_ref(&proxy)) {
            s_compute_lock.unlock();
            delete[] pending_output;
            return false;
        }
        stored->pending_proxy_output = pending_output;
        stored->pending_proxy_output_len = 4;
        stored->complete_consumer_wait_entry = nullptr;
        reclaim_submitted_task_if_safe_locked(stored);
        proxy_ownership_preserved = find_submitted_task_any(TASK_ID_A) == stored;
        proxy_lifetime_ref_held = proxy.ref_count.load(std::memory_order_acquire) == 2;

        detached_output = stored->pending_proxy_output;
        stored->pending_proxy_output = nullptr;
        stored->pending_proxy_output_len = 0;
        stored->reset_local_task_ref();
        proxy_lifetime_ref_released = proxy.ref_count.load(std::memory_order_acquire) == 1;
        reclaim_submitted_task_if_safe_locked(stored);
        row_reclaimed = find_submitted_task_any(TASK_ID_A) == nullptr;
    }

    SubmittedTask second = {};
    second.task_id = TASK_ID_B;
    second.active = false;
    second.ipc_fd_count = 1;
    SubmittedTask* reused = publish_submitted_task_locked(std::move(second));
    slot_reused = reused != nullptr && g_submitted_tasks.size() == HIGH_WATER_AFTER_FIRST;
    request_submitted_task_reclaim_locked(reused);
    ipc_metadata_did_not_pin = find_submitted_task_any(TASK_ID_B) == nullptr;
    bool const COUNT_RESTORED = s_submitted_task_count == COUNT_BEFORE;
    remove_submitted_task_for_selftest_locked(TASK_ID_A);
    remove_submitted_task_for_selftest_locked(TASK_ID_B);
    s_compute_lock.unlock();

    delete[] detached_output;
    if (detached_output == nullptr) {
        delete[] pending_output;
    }
    return response_pin_preserved && complete_pin_preserved && proxy_ownership_preserved && proxy_lifetime_ref_held &&
           proxy_lifetime_ref_released && row_reclaimed && slot_reused && ipc_metadata_did_not_pin && COUNT_RESTORED;
}

auto wki_remote_compute_selftest_task_id_wrap_is_safe() -> bool {
    constexpr uint32_t OCCUPIED_MAX = UINT32_MAX;
    constexpr uint32_t OCCUPIED_ONE = 1;

    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(OCCUPIED_MAX);
    remove_submitted_task_for_selftest_locked(OCCUPIED_ONE);
    size_t const COUNT_BEFORE = s_submitted_task_count;

    SubmittedTask at_max = {};
    at_max.active = true;
    at_max.task_id = OCCUPIED_MAX;
    at_max.result_handle_owned = true;
    SubmittedTask at_one = {};
    at_one.active = true;
    at_one.task_id = OCCUPIED_ONE;
    at_one.result_handle_owned = true;
    bool const SEEDED =
        publish_submitted_task_locked(std::move(at_max)) != nullptr && publish_submitted_task_locked(std::move(at_one)) != nullptr;

    uint32_t expected = 2;
    while (find_submitted_task_slot_locked(expected) != nullptr) {
        expected++;
    }
    uint32_t const SAVED_NEXT = g_next_task_id;
    g_next_task_id = UINT32_MAX;
    uint32_t const ALLOCATED = allocate_submitted_task_id_locked();
    g_next_task_id = SAVED_NEXT;

    remove_submitted_task_for_selftest_locked(OCCUPIED_MAX);
    remove_submitted_task_for_selftest_locked(OCCUPIED_ONE);
    bool const COUNT_RESTORED = s_submitted_task_count == COUNT_BEFORE;
    s_compute_lock.unlock();

    return SEEDED && ALLOCATED != 0 && ALLOCATED == expected && COUNT_RESTORED;
}

auto wki_remote_compute_selftest_load_snapshot_survives_cleanup() -> bool {
    constexpr uint16_t NODE_ID = 0x7A25;
    constexpr uint16_t CPU_COUNT = 4;
    constexpr uint16_t LOAD_PCT = 321;
    constexpr uint64_t LAST_UPDATE_US = 0x12345678ULL;

    RemoteNodeLoad seeded = {};
    seeded.valid = true;
    seeded.node_id = NODE_ID;
    seeded.num_cpus = CPU_COUNT;
    seeded.avg_load_pct = LOAD_PCT;
    seeded.last_update_us = LAST_UPDATE_US;

    s_compute_lock.lock();
    std::erase_if(g_remote_loads, [](const RemoteNodeLoad& load) { return load.node_id == NODE_ID; });
    g_remote_loads.push_back(seeded);
    s_compute_lock.unlock();

    RemoteNodeLoad snapshot = {};
    bool const SNAPSHOT_FOUND = wki_remote_node_load_snapshot(NODE_ID, &snapshot);

    wki_remote_compute_cleanup_for_peer(NODE_ID);

    RemoteNodeLoad after_cleanup = {};
    bool const CLEANED_UP = !wki_remote_node_load_snapshot(NODE_ID, &after_cleanup);

    s_compute_lock.lock();
    std::erase_if(g_remote_loads, [](const RemoteNodeLoad& load) { return load.node_id == NODE_ID; });
    s_compute_lock.unlock();

    return SNAPSHOT_FOUND && snapshot.valid && snapshot.node_id == NODE_ID && snapshot.num_cpus == CPU_COUNT &&
           snapshot.avg_load_pct == LOAD_PCT && snapshot.last_update_us == LAST_UPDATE_US && CLEANED_UP;
}

auto wki_remote_compute_selftest_submit_policy_scope_restores_worker() -> bool {
    using ker::mod::sched::task::Task;
    using ker::mod::sched::task::WkiVfsRoute;
    using ker::mod::sched::task::WkiVfsRule;

    auto add_rule = [](Task* task, const char* prefix, WkiVfsRoute route) -> bool {
        WkiVfsRule rule{};
        size_t const PREFIX_LEN = std::strlen(prefix);
        if (PREFIX_LEN == 0 || PREFIX_LEN >= rule.prefix.size()) {
            return false;
        }
        memcpy(rule.prefix.data(), prefix, PREFIX_LEN);
        rule.prefix[PREFIX_LEN] = '\0';
        rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
        rule.route = static_cast<uint8_t>(route);
        return task->wki_vfs_rules.push_back(rule);
    };
    auto has_only_rule = [](const Task& task, const char* prefix, WkiVfsRoute route) -> bool {
        if (task.wki_vfs_rules.size() != 1) {
            return false;
        }
        const auto& rule = task.wki_vfs_rules[0];
        return rule.prefix_len == std::strlen(prefix) && std::strcmp(rule.prefix.data(), prefix) == 0 &&
               rule.route == static_cast<uint8_t>(route);
    };

    Task worker{};
    Task submitted{};
    std::strncpy(worker.root.data(), "/worker-root", worker.root.size() - 1);
    worker.root_len = static_cast<uint16_t>(std::strlen(worker.root.data()));
    std::strncpy(worker.wki_submitter_hostname.data(), "worker-host", worker.wki_submitter_hostname.size() - 1);
    if (!add_rule(&worker, "/worker", WkiVfsRoute::HOST)) {
        return false;
    }

    WkiTaskIdentityContext identity{};
    std::strncpy(identity.root.data(), "/submit-root", identity.root.size() - 1);
    std::strncpy(identity.submitter_hostname.data(), "submit-host", identity.submitter_hostname.size() - 1);

    constexpr char POLICY_PREFIX[] = "/lib";
    std::array<uint8_t, sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t) + sizeof(POLICY_PREFIX) - 1> policy{};
    uint16_t const RULE_COUNT = 1;
    uint16_t const PREFIX_LEN = sizeof(POLICY_PREFIX) - 1;
    memcpy(policy.data(), &RULE_COUNT, sizeof(RULE_COUNT));
    policy[sizeof(uint16_t)] = static_cast<uint8_t>(WkiVfsRoute::LOCAL);
    memcpy(policy.data() + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t), &PREFIX_LEN, sizeof(PREFIX_LEN));
    memcpy(policy.data() + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t), POLICY_PREFIX, PREFIX_LEN);

    bool scope_state_ok = false;
    {
        ScopedSubmitVfsIdentity scope(&worker, &identity, identity.submitter_hostname.data(), policy.data(), policy.size());
        bool const POLICY_INSTALLED = scope.policy_valid() && std::strcmp(worker.root.data(), identity.root.data()) == 0 &&
                                      std::strcmp(worker.wki_submitter_hostname.data(), identity.submitter_hostname.data()) == 0 &&
                                      has_only_rule(worker, POLICY_PREFIX, WkiVfsRoute::LOCAL);
        bool const TRANSFERRED = scope.transfer_vfs_rules_to(&submitted);
        bool const WORKER_RULES_RESTORED = has_only_rule(worker, "/worker", WkiVfsRoute::HOST);
        scope_state_ok =
            POLICY_INSTALLED && TRANSFERRED && WORKER_RULES_RESTORED && has_only_rule(submitted, POLICY_PREFIX, WkiVfsRoute::LOCAL);
    }

    bool const WORKER_IDENTITY_RESTORED =
        std::strcmp(worker.root.data(), "/worker-root") == 0 && std::strcmp(worker.wki_submitter_hostname.data(), "worker-host") == 0;

    std::array<uint8_t, sizeof(uint16_t)> truncated_policy{};
    memcpy(truncated_policy.data(), &RULE_COUNT, sizeof(RULE_COUNT));
    bool invalid_policy_rejected = false;
    {
        ScopedSubmitVfsIdentity scope(&worker, &identity, identity.submitter_hostname.data(), truncated_policy.data(),
                                      truncated_policy.size());
        invalid_policy_rejected = !scope.policy_valid();
    }

    bool const WORKER_RESTORED_AFTER_REJECT = std::strcmp(worker.root.data(), "/worker-root") == 0 &&
                                              std::strcmp(worker.wki_submitter_hostname.data(), "worker-host") == 0 &&
                                              has_only_rule(worker, "/worker", WkiVfsRoute::HOST);
    return scope_state_ok && WORKER_IDENTITY_RESTORED && invalid_policy_rejected && WORKER_RESTORED_AFTER_REJECT;
}
#endif

auto wki_remote_compute_diag_snapshot(WkiRemoteComputeDiagRow* rows, size_t capacity, WkiRemoteComputeDiagCounts* counts) -> size_t {
    WkiRemoteComputeDiagCounts local_counts{};
    size_t row_count = 0;
    uint64_t const NOW_US = wki_now_us();

    auto append_row = [&](const WkiRemoteComputeDiagRow& row) {
        if (row_count < capacity && rows != nullptr) {
            rows[row_count] = row;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            row_count++;
        } else {
            local_counts.truncated++;
        }
    };

    s_compute_lock.lock();
    local_counts.submitted_total = s_submitted_task_count;
    for (const auto& slot : g_submitted_tasks) {
        if (!slot.occupied) {
            continue;
        }
        const auto& task = slot.task;
        if (task.active) {
            local_counts.submitted_active++;
        }

        WkiRemoteComputeDiagRow row{};
        row.kind = WkiRemoteComputeDiagKind::SUBMITTED;
        row.task_id = task.task_id;
        row.peer_node = task.target_node;
        row.local_pid = task.local_pid;
        row.local_task_ptr = reinterpret_cast<uint64_t>(task.local_task);
        row.active = task.active;
        row.response_pending = task.response_pending.load(std::memory_order_relaxed);
        row.complete_pending = task.complete_pending.load(std::memory_order_relaxed);
        row.proxy_ready = task.proxy_ready;
        row.has_local_task = task.local_task != nullptr;
        row.exit_status = task.exit_status;
        row.accepted_age_us = task.accepted_at_us != 0 && NOW_US >= task.accepted_at_us ? NOW_US - task.accepted_at_us : 0;
        row.complete_age_us =
            task.complete_received_at_us != 0 && NOW_US >= task.complete_received_at_us ? NOW_US - task.complete_received_at_us : 0;
        row.ipc_fd_count = task.ipc_fd_count;
        append_row(row);
    }

    local_counts.running_total = g_running_remote_tasks.size();
    for (const auto& task : g_running_remote_tasks) {
        if (task.active) {
            local_counts.running_active++;
        }

        WkiRemoteComputeDiagRow row{};
        row.kind = WkiRemoteComputeDiagKind::RUNNING;
        row.task_id = task.task_id;
        row.peer_node = task.submitter_node;
        row.local_pid = task.local_pid;
        row.local_task_ptr = reinterpret_cast<uint64_t>(task.task);
        row.active = task.active;
        row.has_local_task = task.task != nullptr;
        row.output_len = task.output != nullptr ? task.output->len : 0;
        append_row(row);
    }

    local_counts.pending_completions = g_pending_task_completions.size();
    for (const auto& completion : g_pending_task_completions) {
        if (completion.submitter_node == WKI_NODE_INVALID) {
            continue;
        }

        WkiRemoteComputeDiagRow row{};
        row.kind = WkiRemoteComputeDiagKind::PENDING_COMPLETE;
        row.task_id = completion.task_id;
        row.peer_node = completion.submitter_node;
        row.local_pid = completion.local_pid;
        row.active = true;
        row.exit_status = completion.exit_status;
        row.output_len = completion.output != nullptr ? completion.output->len : 0;
        append_row(row);
    }
    s_compute_lock.unlock();

    if (counts != nullptr) {
        *counts = local_counts;
    }
    return row_count;
}

auto wki_remote_compute_release_elf_buffer(uint8_t* buffer) -> bool { return release_cached_elf_buffer(buffer); }

auto wki_shared_elf_cache_stats() -> WkiSharedElfCacheStats {
    WkiSharedElfCacheStats stats{
        .entries = 0,
        .bytes = 0,
        .max_entries = WKI_EXEC_CACHE_MAX_ENTRIES,
        .max_bytes = WKI_EXEC_CACHE_MAX_BYTES,
    };

    s_compute_lock.lock();
    for (const auto& entry : g_shared_elf_cache) {
        if (entry.buffer == nullptr) {
            continue;
        }
        stats.entries++;
        stats.bytes += entry.size;
    }
    s_compute_lock.unlock();
    return stats;
}

// ===============================================================================
// Receiver Side - Completion Monitoring
// ===============================================================================

namespace {

auto running_remote_task_matches_exit_ready(const RunningRemoteTask& running, const ker::mod::sched::task::Task* task) -> bool {
    return task != nullptr && running.active && running.task == task && running.local_pid == task->pid &&
           task->exit_notify_ready.load(std::memory_order_acquire) && task->has_exited;
}

auto running_remote_task_completion_eligible(const RunningRemoteTask& running) -> bool {
    return running.active && running.published && (!running.accept_pending || running.discard_completion);
}

struct PendingTaskAcceptAttempt {
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    ComputeSubmitSessionToken session = {};
};

auto collect_pending_task_accept_attempts_locked(std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX>& attempts) -> size_t {
    size_t const ROW_COUNT = g_running_remote_tasks.size();
    if (ROW_COUNT == 0) {
        s_pending_task_accept_cursor = 0;
        return 0;
    }

    size_t index = s_pending_task_accept_cursor % ROW_COUNT;
    size_t visited = 0;
    size_t attempt_count = 0;
    while (visited < ROW_COUNT && attempt_count < attempts.size()) {
        const auto& running = g_running_remote_tasks.at(index);
        if (running.active && running.published && running.accept_pending && !running.discard_completion) {
            attempts.at(attempt_count++) = {
                .task_id = running.task_id,
                .submitter_node = running.submitter_node,
                .local_pid = running.local_pid,
                .session =
                    {
                        .node_id = running.submitter_node,
                        .rx_channel = running.submit_rx_channel,
                        .rx_channel_generation = running.submit_rx_channel_generation,
                        .epoch = running.submit_session_epoch,
                    },
            };
        }
        index = (index + 1) % ROW_COUNT;
        ++visited;
    }
    s_pending_task_accept_cursor = index;
    return attempt_count;
}

void retry_pending_task_accepts() {
    std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX> attempts = {};

    s_compute_lock.lock();
    size_t const ATTEMPT_COUNT = collect_pending_task_accept_attempts_locked(attempts);
    s_compute_lock.unlock();

    for (size_t i = 0; i < ATTEMPT_COUNT; ++i) {
        const auto& attempt = attempts.at(i);
        TaskResponsePayload accept = {};
        accept.task_id = attempt.task_id;
        accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        accept.remote_pid = attempt.local_pid;
        int const SEND_RESULT =
            wki_send_on_channel_generation(attempt.submitter_node, attempt.session.rx_channel, attempt.session.rx_channel_generation,
                                           MsgType::TASK_ACCEPT, &accept, sizeof(accept));

        s_compute_lock.lock();
        auto* running = find_running_task(attempt.task_id, attempt.session);
        if (running != nullptr && running->local_pid == attempt.local_pid) {
            if (SEND_RESULT == WKI_OK) {
                running->accept_pending = false;
            } else if (!compute_submit_session_is_current_locked(attempt.session)) {
                running->accept_pending = false;
                running->discard_completion = true;
            }
        }
        s_compute_lock.unlock();
    }
}

}  // namespace

void wki_remote_compute_notify_task_exit_ready(ker::mod::sched::task::Task* task) {
    if (!g_remote_compute_initialized || task == nullptr || task->wki_remote_pid == 0 ||
        !task->exit_notify_ready.load(std::memory_order_acquire) || !task->has_exited) {
        return;
    }

    s_compute_lock.lock();
    bool const TRACKED = std::ranges::any_of(
        g_running_remote_tasks, [task](const RunningRemoteTask& running) { return running_remote_task_matches_exit_ready(running, task); });
    s_compute_lock.unlock();

    if (TRACKED) {
        wki_timer_notify();
    }
}

void wki_remote_compute_check_completions() {
    if (!g_remote_compute_initialized) {
        return;
    }

    retry_pending_task_accepts();

    s_compute_lock.lock();

    for (auto& rt : g_running_remote_tasks) {
        if (!running_remote_task_completion_eligible(rt)) {
            continue;
        }

        int32_t exit_status = -1;
        bool completed = false;

        auto* task = rt.task;
        if (task == nullptr) {
            task = ker::mod::sched::find_task_by_pid(rt.local_pid);
        }

        if (task == nullptr) {
            // Lost the task reference before we observed exit. Treat this as a
            // failed remote task, but this path should now be rare because
            // RunningRemoteTask holds a ref until completion is reported.
            completed = true;
        } else if (task->exit_notify_ready.load(std::memory_order_acquire) && task->has_exited) {
            // Wait for the same waitpid-visible point as local parents so
            // remote stdout/stderr proxy closes cannot be overtaken by
            // TASK_COMPLETE.
            exit_status = normalize_local_exit_status_for_wire(task->exit_status);
            completed = true;
        }

        if (!completed) {
            continue;
        }

        if (rt.discard_completion) {
            delete rt.output;
        } else {
            g_pending_task_completions.push_back(PendingTaskCompletion{.task_id = rt.task_id,
                                                                       .submitter_node = rt.submitter_node,
                                                                       .local_pid = rt.local_pid,
                                                                       .exit_status = exit_status,
                                                                       .output = rt.output,
                                                                       .submit_session_epoch = rt.submit_session_epoch,
                                                                       .submit_rx_channel = rt.submit_rx_channel,
                                                                       .submit_rx_channel_generation = rt.submit_rx_channel_generation});
        }
        if (rt.task != nullptr) {
            rt.task->release();
            rt.task = nullptr;
        }
        rt.output = nullptr;
        rt.active = false;
    }

    // Clean up inactive entries
    compact_running_remote_tasks_locked();

    s_compute_lock.unlock();

    // Process queued completions without lock. Keep entries queued until the
    // submitter actually accepts TASK_COMPLETE so transient transport stalls
    // cannot strand waitpid()/proxy waiters.
    s_compute_lock.lock();
    size_t completions_remaining = g_pending_task_completions.size();
    s_compute_lock.unlock();
    while (completions_remaining-- > 0) {
        PendingTaskCompletion info = {};
        {
            s_compute_lock.lock();
            if (g_pending_task_completions.empty()) {
                s_compute_lock.unlock();
                break;
            }
            info = g_pending_task_completions.front();
            g_pending_task_completions.pop_front();
            s_compute_lock.unlock();
        }

        ComputeSubmitSessionToken const SESSION = {
            .node_id = info.submitter_node,
            .rx_channel = info.submit_rx_channel,
            .rx_channel_generation = info.submit_rx_channel_generation,
            .epoch = info.submit_session_epoch,
        };
        s_compute_lock.lock();
        bool session_current = compute_submit_session_is_current_locked(SESSION);
        s_compute_lock.unlock();
        if (!session_current) {
            delete info.output;
            continue;
        }

        // D19: Build TASK_COMPLETE with captured output
        uint16_t const OUT_LEN =
            (info.output != nullptr) ? std::min<uint16_t>(info.output->len, WKI_TASK_MAX_OUTPUT) : static_cast<uint16_t>(0);
        auto const MSG_LEN = static_cast<uint16_t>(sizeof(TaskCompletePayload) + OUT_LEN);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): only the exact initialized prefix is transmitted.
        std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> buf __attribute__((uninitialized));
        auto* complete = reinterpret_cast<TaskCompletePayload*>(buf.data());
        complete->task_id = info.task_id;
        complete->exit_status = info.exit_status;
        complete->output_len = OUT_LEN;
        complete->reserved = 0;

        if (OUT_LEN > 0 && info.output != nullptr) {
            memcpy(buf.data() + sizeof(TaskCompletePayload), info.output->data.data(), OUT_LEN);
        }

        int const SEND_RESULT = wki_send_on_channel_generation(
            info.submitter_node, info.submit_rx_channel, info.submit_rx_channel_generation, MsgType::TASK_COMPLETE, buf.data(), MSG_LEN);

        if (SEND_RESULT != WKI_OK) {
            s_compute_lock.lock();
            session_current = compute_submit_session_is_current_locked(SESSION);
            if (session_current) {
                g_pending_task_completions.push_back(info);
            }
            s_compute_lock.unlock();
            if (!session_current) {
                delete info.output;
                continue;
            }
#if WKI_DEBUG
            ker::mod::dbg::log("[WKI] TASK_COMPLETE send deferred: task_id=%u pid=0x%lx submitter=0x%04x status=%d", info.task_id,
                               info.local_pid, info.submitter_node, SEND_RESULT);
#endif
            continue;
        }

#if WKI_DEBUG
        ker::mod::dbg::log("[WKI] Remote task completed: task_id=%u pid=0x%lx exit=%d output=%u bytes", info.task_id, info.local_pid,
                           info.exit_status, OUT_LEN);
#endif
        delete info.output;
    }
}

// ===============================================================================
// Internal Helpers - ELF execution + VFS loading
// ===============================================================================

namespace {

// ---------------------------------------------------------------------------
// Shared helper: execute an ELF buffer as a new process.
// Takes ownership of elf_buffer on success (Task owns it).
// On failure, elf_buffer is freed and reject_reason is set.
// Returns the new Task on success, nullptr on failure.
// ---------------------------------------------------------------------------

struct ExecResult {
    ker::mod::sched::task::Task* task = nullptr;
    TaskOutputCapture* output = nullptr;
    TaskRejectReason reject_reason = TaskRejectReason::FETCH_FAILED;
};

auto finish_remote_exec_task(ker::mod::sched::task::Task* new_task) -> ExecResult {
    ExecResult result;
    if (new_task == nullptr) {
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    // Set up stdin as /dev/null (EOF on read)
    {
        auto* stdin_file = new ker::vfs::File();  // NOLINT(cppcoreguidelines-owning-memory)
        stdin_file->fd = 0;
        stdin_file->private_data = nullptr;
        stdin_file->fops = &g_stdin_null_fops;
        stdin_file->pos = 0;
        stdin_file->is_directory = false;
        stdin_file->fs_type = ker::vfs::FSType::DEVFS;
        stdin_file->refcount = 1;
        stdin_file->open_flags = 0;
        stdin_file->fd_flags = 0;
        stdin_file->vfs_path = nullptr;
        stdin_file->dir_fs_count = 0;
        (void)new_task->fd_table.insert(0, stdin_file);
    }

    // D19: Set up stdout/stderr capture
    auto* output_cap = new TaskOutputCapture();
    for (unsigned fd_idx = 1; fd_idx <= 2; fd_idx++) {
        auto* capture_file = new ker::vfs::File();  // NOLINT(cppcoreguidelines-owning-memory)
        capture_file->fd = static_cast<int>(fd_idx);
        capture_file->private_data = output_cap;
        capture_file->fops = &g_capture_fops;
        capture_file->pos = 0;
        capture_file->is_directory = false;
        capture_file->fs_type = ker::vfs::FSType::DEVFS;
        capture_file->refcount = 1;
        capture_file->open_flags = 1;
        capture_file->fd_flags = 0;
        capture_file->vfs_path = nullptr;
        capture_file->dir_fs_count = 0;
        (void)new_task->fd_table.insert(fd_idx, capture_file);
    }

    // Caller prepares argv/envp/cwd before scheduler publication.
    result.task = new_task;
    result.output = output_cap;
    return result;
}

auto exec_elf_buffer(uint8_t* elf_buffer, uint32_t binary_len, bool shared_elf_buffer) -> ExecResult {
    ExecResult result;

    // Validate ELF magic
    if (binary_len < sizeof(Elf64_Ehdr)) {
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    const auto* elf_hdr = reinterpret_cast<const Elf64_Ehdr*>(elf_buffer);
    if (elf_hdr->e_ident[EI_MAG0] != ELFMAG0 || elf_hdr->e_ident[EI_MAG1] != ELFMAG1 || elf_hdr->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_hdr->e_ident[EI_MAG3] != ELFMAG3) {
        ker::mod::dbg::log("[WKI] exec_elf_buffer: bad ELF magic at %p len=%u bytes=[%02x %02x %02x %02x %02x %02x %02x %02x]", elf_buffer,
                           binary_len, elf_buffer[0], elf_buffer[1], elf_buffer[2], elf_buffer[3], elf_buffer[4], elf_buffer[5],
                           elf_buffer[6], elf_buffer[7]);
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    // Allocate kernel stack
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::kernel_stack_alloc("wki_exec_kstack"));
    if (stack_base == 0) {
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    uint64_t const KERNEL_RSP = stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    // Create the process task. The constructor maps the main ELF but records
    // PT_INTERP without entering VFS; the worker must first publish a recovery
    // owner for every resource now attached to the child.
    auto* new_task = new ker::mod::sched::task::Task(  // NOLINT(cppcoreguidelines-owning-memory)
        "wki-remote", reinterpret_cast<uint64_t>(elf_buffer), KERNEL_RSP, ker::mod::sched::task::TaskType::PROCESS);

    if (new_task == nullptr) {
        ker::mod::mm::phys::page_free(reinterpret_cast<void*>(stack_base));
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    new_task->elf_buffer = elf_buffer;
    new_task->elf_buffer_size = binary_len;
    new_task->is_elf_buffer_shared = shared_elf_buffer;

    auto* const OWNER = ker::mod::sched::get_current_task();
    if (OWNER == nullptr || !ker::mod::sched::task::claim_unpublished_process(OWNER, new_task)) [[unlikely]] {
        ker::mod::dbg::panic_handler("WKI remote compute: cannot publish receiver child recovery ownership");
        hcf();
    }

    if (new_task->thread == nullptr || new_task->pagemap == nullptr || new_task->entry == 0) {
        destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-construction");
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    if (!ker::mod::sched::task::complete_unpublished_process_construction(new_task)) {
        destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-interpreter");
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    return finish_remote_exec_task(new_task);
}

auto exec_elf_file(ker::vfs::File* owned_file, uint32_t binary_len, const ker::vfs::Stat& file_stat) -> ExecResult {
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::kernel_stack_alloc("wki_file_exec_kstack"));
    if (stack_base == 0) {
        ker::vfs::vfs_put_file(owned_file);
        ExecResult result;
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    auto* task = ker::syscall::process::create_file_backed_process_task("wki-remote", owned_file, binary_len, file_stat,
                                                                        stack_base + ker::mod::mm::KERNEL_STACK_SIZE);
    return finish_remote_exec_task(task);
}

// ---------------------------------------------------------------------------
// D14: Load ELF binary from a VFS path. Returns owned buffer + size.
// ---------------------------------------------------------------------------

struct VfsLoadResult {
    uint8_t* buffer = nullptr;
    ker::vfs::File* file = nullptr;
    uint32_t size = 0;
    ker::vfs::Stat file_stat = {};
    TaskRejectReason reject_reason = TaskRejectReason::FETCH_FAILED;
    bool shared = false;
};

auto load_elf_from_vfs_path(const char* path, uint16_t submitter_node, uint32_t correlation, const ComputeSubmitSessionToken& session,
                            uint64_t request_deadline_us) -> VfsLoadResult {
    VfsLoadResult result;
    ScopedComputeMeasure load_measure(ker::mod::perf::WkiPerfComputeOp::LOAD_ELF, submitter_node, correlation,
                                      path != nullptr ? static_cast<uint32_t>(std::strlen(path)) : 0U, WOS_PERF_CALLSITE());
    if (path == nullptr || path[0] == '\0') {
        result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }

    auto request_is_current = [&]() -> bool {
        if (request_deadline_us != 0 && wki_now_us() >= request_deadline_us) {
            return false;
        }
        s_compute_lock.lock();
        bool const CURRENT = compute_submit_request_is_current_locked(session, correlation);
        s_compute_lock.unlock();
        return CURRENT;
    };
    if (!request_is_current()) {
        result.reject_reason = TaskRejectReason::OVERLOADED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }

    std::array<char, 512> localized_path = {};
    std::array<char, 512> fallback_local_path = {};
    const char* resolved_path = path;
    if (localize_receiver_logical_path(path, localized_path.data(), localized_path.size())) {
        resolved_path = localized_path.data();
    }

    // Keep explicit /wki/<host>/... references on that host. Falling back to
    // the receiver's same local path mixes binary and dependency namespaces.
    bool using_disconnected_host_fallback = false;
    if (fallback_to_local_path_for_disconnected_wki_host(resolved_path, fallback_local_path.data(), fallback_local_path.size())) {
        resolved_path = fallback_local_path.data();
        using_disconnected_host_fallback = true;
    }

    ker::vfs::Stat statbuf = {};
    bool have_vfs_ref_stat = ker::vfs::vfs_stat(resolved_path, &statbuf) == 0 && statbuf.st_size > 0;
    if (!have_vfs_ref_stat) {
        if (!using_disconnected_host_fallback) {
            if (fallback_to_local_path_for_disconnected_wki_host(resolved_path, fallback_local_path.data(), fallback_local_path.size())) {
                resolved_path = fallback_local_path.data();
                using_disconnected_host_fallback = true;
                have_vfs_ref_stat = ker::vfs::vfs_stat(resolved_path, &statbuf) == 0 && statbuf.st_size > 0;
            }
        }

        if (!have_vfs_ref_stat) {
            ker::mod::dbg::log("[WKI] VFS_REF: failed to stat '%s'", resolved_path);
            result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }
    }
    if (std::cmp_greater(statbuf.st_size, UINT32_MAX)) {
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }

    std::array<char, 512> equivalent_local_path = {};
    ker::vfs::Stat equivalent_local_stat = {};
    if (!using_disconnected_host_fallback && equivalent_local_file_for_wki_ref(resolved_path, statbuf, equivalent_local_path.data(),
                                                                               equivalent_local_path.size(), &equivalent_local_stat)) {
        resolved_path = equivalent_local_path.data();
        statbuf = equivalent_local_stat;
    }

    // A complete large executable buffer recreates the same high-order buddy
    // allocation that local exec avoids. Keep the opened file instead; the
    // receiver will read bounded ELF metadata now and fault immutable PT_LOAD
    // pages from this retained handle on demand.
    if (std::cmp_greater_equal(statbuf.st_size, WKI_FILE_BACKED_ELF_MIN_SIZE)) {
        int open_flags = ker::vfs::O_NOTIFY_CACHE_CHANGE;
        if (using_disconnected_host_fallback) {
            open_flags |= ker::vfs::O_LOCAL;
        }
        int const FD = ker::vfs::vfs_open(resolved_path, open_flags, 0);
        if (FD < 0) {
            result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }

        auto* const CURRENT_TASK = ker::mod::sched::get_current_task();
        result.file = ker::vfs::vfs_get_file_retain(CURRENT_TASK, FD);
        ker::vfs::vfs_close(FD);
        if (result.file == nullptr || !request_is_current()) {
            if (result.file != nullptr) {
                ker::vfs::vfs_put_file(result.file);
                result.file = nullptr;
            }
            result.reject_reason = TaskRejectReason::OVERLOADED;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }

        if (!ker::syscall::process::supports_file_backed_process(result.file, static_cast<size_t>(statbuf.st_size))) {
            ker::vfs::vfs_put_file(result.file);
            result.file = nullptr;
        } else {
            ker::vfs::Stat post_open_stat = {};
            if (ker::vfs::vfs_stat(resolved_path, &post_open_stat) != 0 || !shared_elf_freshness_matches(statbuf, post_open_stat)) {
                ker::vfs::vfs_put_file(result.file);
                result.file = nullptr;
                result.reject_reason = TaskRejectReason::FETCH_FAILED;
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }

            result.size = static_cast<uint32_t>(statbuf.st_size);
            result.file_stat = statbuf;
            result.reject_reason = TaskRejectReason::ACCEPTED;
            result.shared = false;
            load_measure.finish(0, result.size);
            return result;
        }
    }

    // The I/O path may switch to a disconnected-host fallback during retry.
    // Keep the cache/single-flight identity fixed to the path whose freshness
    // key was captured so another worker cannot lose or replace our marker.
    std::array<char, 512> cache_path = {};
    std::strncpy(cache_path.data(), resolved_path, cache_path.size() - 1);
    ker::vfs::Stat cache_key = statbuf;
    bool is_loader = false;
    uint64_t loader_token = 0;
    uint64_t inflight_deadline_us = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    if (request_deadline_us != 0) {
        inflight_deadline_us = std::min(inflight_deadline_us, request_deadline_us);
    }

    auto fail_inflight_load = [&]() {
        if (!is_loader) {
            return;
        }

        s_compute_lock.lock();
        if (auto* inflight = find_shared_elf_cache_entry_locked(session, cache_path.data(), cache_key);
            inflight != nullptr && inflight->load_token == loader_token) {
            inflight->loading = false;
            inflight->load_status = -1;
            inflight->last_used_us = wki_now_us();
            inflight->valid = false;
            gc_shared_elf_cache_locked(inflight->last_used_us);
        }
        s_compute_lock.unlock();
    };

    while (true) {
        s_compute_lock.lock();
        if (!compute_submit_request_is_current_locked(session, correlation) ||
            (request_deadline_us != 0 && wki_now_us() >= request_deadline_us)) {
            s_compute_lock.unlock();
            result.reject_reason = TaskRejectReason::OVERLOADED;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }
        gc_shared_elf_cache_locked(wki_now_us());

        if (auto* cached = find_shared_elf_cache_locked(session, cache_path.data(), cache_key); cached != nullptr) {
            cached->refcount++;
            cached->last_used_us = wki_now_us();
            result.buffer = cached->buffer;
            result.size = cached->size;
            result.shared = true;
            s_compute_lock.unlock();
            load_measure.finish(0, result.size);
            return result;
        }

        auto* inflight = find_shared_elf_cache_entry_locked(session, cache_path.data(), cache_key);
        if (inflight != nullptr) {
            if (inflight->loading) {
                s_compute_lock.unlock();
                if (wki_now_us() >= inflight_deadline_us || !request_is_current()) {
                    result.reject_reason = TaskRejectReason::FETCH_FAILED;
                    load_measure.finish(perf_compute_reject_status(result.reject_reason));
                    return result;
                }
                ker::mod::sched::kern_sleep_us(WKI_EXEC_CACHE_INFLIGHT_WAIT_US);
                continue;
            }

            if (inflight->buffer != nullptr && inflight->load_status == 0) {
                inflight->refcount++;
                inflight->last_used_us = wki_now_us();
                result.buffer = inflight->buffer;
                result.size = inflight->size;
                result.shared = true;
                s_compute_lock.unlock();
                load_measure.finish(0, result.size);
                return result;
            }

            inflight->valid = false;
            gc_shared_elf_cache_locked(wki_now_us());
        }

        SharedElfCacheEntry pending = {};
        pending.valid = true;
        pending.loading = true;
        pending.load_status = 0;
        pending.submitter_node = submitter_node;
        pending.session = session;
        pending.load_token = next_nonzero_token(s_next_shared_elf_cache_load_token);
        std::strncpy(pending.path.data(), cache_path.data(), pending.path.size() - 1);
        pending.freshness = cache_key;
        pending.last_used_us = wki_now_us();
        g_shared_elf_cache.push_back(pending);
        is_loader = true;
        loader_token = pending.load_token;
        s_compute_lock.unlock();
        break;
    }

    auto file_size = static_cast<size_t>(statbuf.st_size);
    uint8_t* buf = nullptr;
    size_t total_read = 0;
    size_t final_file_size = file_size;
    bool load_ok = false;
    uint64_t const RETRY_WINDOW_START_US = wki_now_us();
    uint64_t retry_deadline_us = wki_future_deadline_us(RETRY_WINDOW_START_US, WKI_VFS_LOAD_RETRY_WINDOW_US);
    if (request_deadline_us != 0) {
        retry_deadline_us = std::min(retry_deadline_us, request_deadline_us);
    }

    for (uint32_t attempt = 0; attempt < WKI_VFS_LOAD_MAX_ATTEMPTS; attempt++) {
        uint64_t const NOW_US = wki_now_us();
        bool const RETRY_WINDOW_OPEN = NOW_US < retry_deadline_us;

        if (!RETRY_WINDOW_OPEN || !request_is_current()) {
            break;
        }

        if (attempt > 0) {
            ker::vfs::Stat retry_stat = {};
            if (ker::vfs::vfs_stat(resolved_path, &retry_stat) != 0 || retry_stat.st_size <= 0) {
                if (RETRY_WINDOW_OPEN && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                    uint64_t const WAIT_UNTIL_US =
                        std::min(wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US), retry_deadline_us);
                    while (wki_now_us() < WAIT_UNTIL_US) {
                        sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US);
                    }
                }
                continue;
            }
            if (std::cmp_greater(retry_stat.st_size, UINT32_MAX)) {
                fail_inflight_load();
                result.reject_reason = TaskRejectReason::FETCH_FAILED;
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            if (!shared_elf_freshness_matches(cache_key, retry_stat)) {
                fail_inflight_load();
                result.reject_reason = TaskRejectReason::FETCH_FAILED;
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            file_size = static_cast<size_t>(retry_stat.st_size);
        }

        int open_flags = ker::vfs::O_NOTIFY_CACHE_CHANGE;
        if (using_disconnected_host_fallback) {
            open_flags |= ker::vfs::O_LOCAL;
        }
        int const FD = ker::vfs::vfs_open(resolved_path, open_flags, 0);
        if (FD < 0) {
            if (RETRY_WINDOW_OPEN && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                uint64_t const WAIT_UNTIL_US =
                    std::min(wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US), retry_deadline_us);
                while (wki_now_us() < WAIT_UNTIL_US) {
                    sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US);
                }
                continue;
            }

            ker::mod::dbg::log("[WKI] VFS_REF: failed to open '%s'", resolved_path);
            fail_inflight_load();
            result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }

        buf = new uint8_t[file_size];  // NOLINT(cppcoreguidelines-owning-memory)
        total_read = 0;
        uint32_t idle_retries = 0;

        while (total_read < file_size) {
            if (!request_is_current()) {
                break;
            }
            size_t const CHUNK = std::min(WKI_VFS_LOAD_CHUNK, file_size - total_read);
            // Executable images need exact offset/data coupling; avoid sequential
            // remote read caches while assembling the ELF buffer.
            ssize_t const READ_RET = ker::vfs::vfs_pread(FD, buf + total_read, CHUNK, static_cast<off_t>(total_read));

            if (READ_RET < 0 || READ_RET == 0) {
                if (++idle_retries >= WKI_VFS_LOAD_IDLE_RETRIES) {
                    break;
                }
                continue;
            }

            total_read += static_cast<size_t>(READ_RET);
            idle_retries = 0;
        }

        ker::vfs::vfs_close(FD);

        if (total_read == file_size) {
            final_file_size = file_size;
            load_ok = true;
            break;
        }

        delete[] buf;
        buf = nullptr;
        if (RETRY_WINDOW_OPEN && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
            uint64_t const ELAPSED_MS = (wki_now_us() - RETRY_WINDOW_START_US) / 1000;
            ker::mod::dbg::log("[WKI] VFS_REF: short read retry for '%s' (%zu/%zu bytes) attempt %u/%u elapsed=%llu ms", resolved_path,
                               total_read, file_size, attempt + 1, WKI_VFS_LOAD_MAX_ATTEMPTS, static_cast<unsigned long long>(ELAPSED_MS));
            uint64_t const WAIT_UNTIL_US = std::min(wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US), retry_deadline_us);
            while (wki_now_us() < WAIT_UNTIL_US) {
                sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US);
            }
        }
    }

    if (!load_ok) {
        bool const REQUEST_CURRENT = request_is_current();
        fail_inflight_load();
        ker::mod::dbg::log("[WKI] VFS_REF: short read for '%s' (%zu/%zu bytes)", resolved_path, total_read, final_file_size);
        result.reject_reason = REQUEST_CURRENT ? TaskRejectReason::FETCH_FAILED : TaskRejectReason::OVERLOADED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason), total_read);
        return result;
    }

    ker::vfs::Stat post_read_stat = {};
    if (ker::vfs::vfs_stat(resolved_path, &post_read_stat) != 0 || !shared_elf_freshness_matches(cache_key, post_read_stat)) {
        fail_inflight_load();
        delete[] buf;
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason), total_read);
        return result;
    }

#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] VFS_REF: loaded '%s' %zu bytes hdr=[%02x %02x %02x %02x]", resolved_path, total_read, buf[0], buf[1], buf[2],
                       buf[3]);
#endif

    s_compute_lock.lock();
    if (!compute_submit_request_is_current_locked(session, correlation) ||
        (request_deadline_us != 0 && wki_now_us() >= request_deadline_us)) {
        if (auto* inflight = find_shared_elf_cache_entry_locked(session, cache_path.data(), cache_key);
            inflight != nullptr && inflight->load_token == loader_token) {
            inflight->loading = false;
            inflight->load_status = -1;
            inflight->valid = false;
        }
        gc_shared_elf_cache_locked(wki_now_us());
        s_compute_lock.unlock();
        delete[] buf;
        result.reject_reason = TaskRejectReason::OVERLOADED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason), total_read);
        return result;
    }
    gc_shared_elf_cache_locked(wki_now_us());
    if (auto* cached = find_shared_elf_cache_locked(session, cache_path.data(), cache_key); cached != nullptr) {
        cached->refcount++;
        cached->last_used_us = wki_now_us();
        result.buffer = cached->buffer;
        result.size = cached->size;
        result.shared = true;
        s_compute_lock.unlock();
        delete[] buf;
        load_measure.finish(0, result.size);
        return result;
    }

    auto* inflight = find_shared_elf_cache_entry_locked(session, cache_path.data(), cache_key);
    if (is_loader && inflight != nullptr && inflight->loading && inflight->load_token == loader_token) {
        inflight->loading = false;
        inflight->load_status = 0;
        inflight->buffer = buf;
        inflight->size = static_cast<uint32_t>(final_file_size);
        inflight->refcount = 1;
        inflight->last_used_us = wki_now_us();
        result.buffer = buf;
        result.size = static_cast<uint32_t>(final_file_size);
        result.shared = true;
        gc_shared_elf_cache_locked(inflight->last_used_us);
        s_compute_lock.unlock();
        load_measure.finish(0, result.size);
        return result;
    }

    // Cleanup or a same-key replacement can retire our exact marker while the
    // VFS read is outside s_compute_lock. Never publish those bytes into a new
    // marker or launch them uncached for the retired request.
    if (is_loader) {
        s_compute_lock.unlock();
        delete[] buf;
        result.reject_reason = TaskRejectReason::OVERLOADED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason), total_read);
        return result;
    }

    SharedElfCacheEntry cache_entry = {};
    cache_entry.valid = true;
    cache_entry.loading = false;
    cache_entry.load_status = 0;
    cache_entry.submitter_node = submitter_node;
    cache_entry.session = session;
    cache_entry.load_token = next_nonzero_token(s_next_shared_elf_cache_load_token);
    std::strncpy(cache_entry.path.data(), cache_path.data(), cache_entry.path.size() - 1);
    cache_entry.freshness = cache_key;
    cache_entry.buffer = buf;
    cache_entry.size = static_cast<uint32_t>(final_file_size);
    cache_entry.refcount = 1;
    cache_entry.last_used_us = wki_now_us();
    g_shared_elf_cache.push_back(cache_entry);
    gc_shared_elf_cache_locked(wki_now_us());
    s_compute_lock.unlock();

    result.buffer = buf;
    result.size = static_cast<uint32_t>(final_file_size);
    result.shared = true;
    load_measure.finish(0, result.size);
    return result;
}

}  // namespace

// ===============================================================================
// Receiver Side - RX Handlers
// ===============================================================================

// Internal implementation: runs the full TASK_SUBMIT handler including
// any blocking VFS reads.  Must only be called from a context where
// wki_spin_yield() / inline NAPI draining are safe (i.e. NOT from inside
// the NAPI poll dispatch path).
namespace {
void handle_task_submit_work(uint16_t src_node, const uint8_t* payload, uint16_t payload_len, uint64_t deadline_us,
                             const ComputeSubmitSessionToken& session) {
    if (payload_len < sizeof(TaskSubmitPayload)) {
        return;
    }

    // Build a minimal header for the internal helpers that need src_node.
    WkiHeader synth_hdr = {};
    synth_hdr.src_node = src_node;
    const WkiHeader* hdr = &synth_hdr;

    const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(payload);
    ScopedComputeMeasure handle_measure(ker::mod::perf::WkiPerfComputeOp::HANDLE_SUBMIT, src_node, submit->task_id, payload_len,
                                        WOS_PERF_CALLSITE());
    const uint8_t* var_data = payload + sizeof(TaskSubmitPayload);
    auto var_len = static_cast<uint16_t>(payload_len - sizeof(TaskSubmitPayload));

    uint8_t* elf_buffer = nullptr;
    ker::vfs::File* elf_file = nullptr;
    ker::vfs::Stat elf_file_stat = {};
    uint32_t binary_len = 0;
    TaskRejectReason reject_reason = TaskRejectReason::ACCEPTED;
    bool elf_buffer_shared = false;
    std::array<char, 256> exe_path_buf = {};
    const uint8_t* submit_context = nullptr;
    uint16_t submit_context_len = 0;
    WkiTaskIdentityContext submitted_identity = {};
    bool has_submitted_identity = false;
    std::strncpy(exe_path_buf.data(), "wki-remote", exe_path_buf.size() - 1);

    auto release_elf_source = [&]() {
        release_loaded_elf_buffer(elf_buffer, elf_buffer_shared);
        elf_buffer = nullptr;
        if (elf_file != nullptr) {
            ker::vfs::vfs_put_file(elf_file);
            elf_file = nullptr;
        }
    };

    auto send_submit_response = [&](MsgType type, const TaskResponsePayload& response) -> int {
        return wki_send_on_channel_generation(src_node, session.rx_channel, session.rx_channel_generation, type, &response,
                                              sizeof(response));
    };

    auto submit_deadline_expired = [&]() -> bool { return deadline_us != 0 && wki_now_us() >= deadline_us; };
    s_compute_lock.lock();
    bool const SESSION_CURRENT = compute_submit_session_is_current_locked(session);
    int32_t const EARLY_CANCEL_SIGNAL = take_cancelled_task_submit_locked(session, submit->task_id);
    s_compute_lock.unlock();
    if (!SESSION_CURRENT) {
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED));
        return;
    }
    if (EARLY_CANCEL_SIGNAL != 0) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED));
        return;
    }
    if (submit_deadline_expired()) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED));
        return;
    }

    auto mode = static_cast<TaskDeliveryMode>(submit->delivery_mode);
    switch (mode) {
        case TaskDeliveryMode::INLINE: {
            // Parse INLINE format: {binary_len:u32, binary[binary_len], args[args_len]}
            if (var_len < sizeof(uint32_t)) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }
            memcpy(&binary_len, var_data, sizeof(uint32_t));
            const uint8_t* binary_data = var_data + sizeof(uint32_t);

            if (binary_len == 0 || binary_len > static_cast<uint32_t>(var_len - sizeof(uint32_t))) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }

            elf_buffer = new uint8_t[binary_len];  // NOLINT(cppcoreguidelines-owning-memory)
            memcpy(elf_buffer, binary_data, binary_len);
            submit_context = binary_data + binary_len;
            submit_context_len = static_cast<uint16_t>(var_len - (sizeof(uint32_t) + binary_len));
            break;
        }

        case TaskDeliveryMode::VFS_REF: {
            // D14: Parse VFS_REF format: {path_len:u16, path[path_len], args[args_len]}
            if (var_len < sizeof(uint16_t)) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }
            uint16_t path_len = 0;
            memcpy(&path_len, var_data, sizeof(uint16_t));

            if (path_len == 0 || path_len >= 512 || sizeof(uint16_t) + path_len > var_len) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }

            // Build null-terminated path string
            std::array<char, 512> path_buf = {};
            auto copy_len = std::min<uint16_t>(path_len, static_cast<uint16_t>(path_buf.size() - 1));
            memcpy(path_buf.data(), var_data + sizeof(uint16_t), copy_len);
            *std::next(path_buf.begin(), static_cast<ptrdiff_t>(copy_len)) = '\0';

            auto vfs_result = load_elf_from_vfs_path(path_buf.data(), hdr->src_node, submit->task_id, session, deadline_us);
            if (vfs_result.buffer == nullptr && vfs_result.file == nullptr) {
                ker::mod::dbg::log("[WKI] Task submit VFS_REF load failed: task_id=%u path='%s' reason=%u", submit->task_id,
                                   path_buf.data(), static_cast<uint8_t>(vfs_result.reject_reason));
                reject_reason = vfs_result.reject_reason;
                break;
            }
            elf_buffer = vfs_result.buffer;
            elf_file = vfs_result.file;
            elf_file_stat = vfs_result.file_stat;
            binary_len = vfs_result.size;
            elf_buffer_shared = vfs_result.shared;
            std::strncpy(exe_path_buf.data(), path_buf.data(), exe_path_buf.size() - 1);
            submit_context = var_data + sizeof(uint16_t) + path_len;
            submit_context_len = static_cast<uint16_t>(var_len - (sizeof(uint16_t) + path_len));
            break;
        }

        case TaskDeliveryMode::RESOURCE_REF: {
            // D15: Parse RESOURCE_REF: {ref_node_id:u16, ref_resource_id:u32, path_len:u16, path[]}
            constexpr size_t MIN_REF_HDR = sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint16_t);
            if (var_len < MIN_REF_HDR) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }

            uint16_t ref_node = 0;
            uint32_t ref_resource = 0;
            uint16_t path_len = 0;
            size_t off = 0;
            memcpy(&ref_node, var_data + off, sizeof(uint16_t));
            off += sizeof(uint16_t);
            memcpy(&ref_resource, var_data + off, sizeof(uint32_t));
            off += sizeof(uint32_t);
            memcpy(&path_len, var_data + off, sizeof(uint16_t));
            off += sizeof(uint16_t);

            if (path_len == 0 || path_len >= 512 || off + path_len > var_len) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }

            std::array<char, 512> ref_path = {};
            auto copy_len = std::min<uint16_t>(path_len, static_cast<uint16_t>(ref_path.size() - 1));
            memcpy(ref_path.data(), var_data + off, copy_len);
            *std::next(ref_path.begin(), static_cast<ptrdiff_t>(copy_len)) = '\0';

            // Try to load from VFS path directly - if the remote resource is already
            // mounted (e.g., via wki_remote_vfs_mount), the path is accessible.
            // Construct full path: "/remote_<node>_<resource>/<ref_path>" or just try ref_path.
            // For simplicity: try the path as-is first (works if already mounted at that path).
            auto vfs_result = load_elf_from_vfs_path(ref_path.data(), hdr->src_node, submit->task_id, session, deadline_us);
            if (vfs_result.buffer == nullptr && vfs_result.file == nullptr) {
                ker::mod::dbg::log("[WKI] RESOURCE_REF: failed to load node=0x%04x res=%u path='%s'", ref_node, ref_resource,
                                   ref_path.data());
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }
            elf_buffer = vfs_result.buffer;
            elf_file = vfs_result.file;
            elf_file_stat = vfs_result.file_stat;
            binary_len = vfs_result.size;
            elf_buffer_shared = vfs_result.shared;
            std::strncpy(exe_path_buf.data(), ref_path.data(), exe_path_buf.size() - 1);
            submit_context = var_data + off + path_len;
            submit_context_len = static_cast<uint16_t>(var_len - (off + path_len));
            break;
        }

        default:
            reject_reason = TaskRejectReason::FETCH_FAILED;
            break;
    }

    if (reject_reason == TaskRejectReason::ACCEPTED && submit_deadline_expired()) {
        reject_reason = TaskRejectReason::OVERLOADED;
    }

    s_compute_lock.lock();
    int32_t const LOAD_CANCEL_SIGNAL = take_cancelled_task_submit_locked(session, submit->task_id);
    s_compute_lock.unlock();
    if (LOAD_CANCEL_SIGNAL != 0) {
        release_elf_source();
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    uint16_t context_without_ipc = submit_context_len;
    auto ipc_tail_size = static_cast<uint32_t>(submit->ipc_fd_count) * sizeof(WkiIpcFdEntry);
    if (ipc_tail_size > submit_context_len || ipc_tail_size > 0xFFFFU) {
        reject_reason = TaskRejectReason::FETCH_FAILED;
    } else {
        context_without_ipc = static_cast<uint16_t>(submit_context_len - ipc_tail_size);
    }

    bool const INVALID_SUBMIT_CONTEXT = submit->args_len > context_without_ipc ||
                                        submit->identity_len > static_cast<uint16_t>(context_without_ipc - submit->args_len) ||
                                        !valid_task_identity_len(submit->identity_len);
    const uint8_t* policy_cursor = nullptr;
    uint16_t policy_len = 0;
    if (INVALID_SUBMIT_CONTEXT) {
        reject_reason = TaskRejectReason::FETCH_FAILED;
    } else {
        policy_cursor = submit_context + submit->args_len + submit->identity_len;
        policy_len = static_cast<uint16_t>(context_without_ipc - submit->args_len - submit->identity_len);
        if (submit->identity_len == WkiTaskIdentityContext::V1_SIZE || submit->identity_len == sizeof(WkiTaskIdentityContext)) {
            memcpy(&submitted_identity, submit_context + submit->args_len, submit->identity_len);
            has_submitted_identity = true;
        }
    }

    // If binary loading failed, reject
    if ((elf_buffer == nullptr && elf_file == nullptr) || reject_reason != TaskRejectReason::ACCEPTED) {
        release_elf_source();
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(reject_reason);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit reject before exec: task_id=%u reason=%u", submit->task_id,
                           static_cast<uint8_t>(reject_reason));
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(reject_reason), binary_len);
        return;
    }

    // Execute the ELF buffer (creates task but does NOT schedule it yet)
    auto* current_task = ker::mod::sched::get_current_task();
    const char* effective_submitter_hostname = (has_submitted_identity && submitted_identity.submitter_hostname.front() != '\0')
                                                   ? submitted_identity.submitter_hostname.data()
                                                   : wki_peer_get_hostname(hdr->src_node);
    // Post-construction PT_INTERP loading resolves VFS through the current
    // worker. Install and validate the submitted routes first, then move that
    // exact policy into the unpublished task after construction succeeds.
    ScopedSubmitVfsIdentity submit_vfs_identity(current_task, has_submitted_identity ? &submitted_identity : nullptr,
                                                effective_submitter_hostname, policy_cursor, policy_len);
    if (!submit_vfs_identity.policy_valid()) {
        release_elf_source();
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::FETCH_FAILED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit policy parse failed before exec: task_id=%u", submit->task_id);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::FETCH_FAILED), binary_len);
        return;
    }
    ExecResult const EXEC = elf_file != nullptr ? exec_elf_file(elf_file, binary_len, elf_file_stat)
                                                : exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared);
    elf_file = nullptr;
    elf_buffer = nullptr;
    if (EXEC.task == nullptr) {
        auto exec_reason = EXEC.reject_reason;
        if (exec_reason == TaskRejectReason::ACCEPTED) {
            exec_reason = TaskRejectReason::FETCH_FAILED;
        }
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(exec_reason);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit exec failed: task_id=%u reason=%u", submit->task_id, static_cast<uint8_t>(exec_reason));
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(exec_reason), binary_len);
        return;
    }

    // -----------------------------------------------------------------------
    // et exe_path, cwd, and minimal argv/envp on the user stack
    // -----------------------------------------------------------------------
    auto* new_task = EXEC.task;

    const char* submitter_hostname = wki_peer_get_hostname(hdr->src_node);
    if (submitter_hostname != nullptr && submitter_hostname[0] != '\0') {
        std::strncpy(new_task->wki_submitter_hostname.data(), submitter_hostname, new_task->wki_submitter_hostname.size() - 1);
        new_task->wki_submitter_hostname.back() = '\0';
    } else {
        std::snprintf(new_task->wki_submitter_hostname.data(), new_task->wki_submitter_hostname.size(), "node-%04x", hdr->src_node);
    }
    new_task->wki_remote_pid = new_task->pid;

    std::array<char, ker::mod::sched::task::Task::EXE_PATH_MAX> logical_exe_path = {};
    const char* task_exe_path = exe_path_buf.data();
    if (localize_receiver_logical_path(exe_path_buf.data(), logical_exe_path.data(), logical_exe_path.size())) {
        task_exe_path = logical_exe_path.data();
    }

    // Set exe_path from the resolved path, localized to receiver semantics
    // when the submitter referenced this node via /wki/<local-host>/...
    std::strncpy(new_task->exe_path.data(), task_exe_path, ker::mod::sched::task::Task::EXE_PATH_MAX - 1);
    new_task->exe_path.back() = '\0';

    const char** argv_strings = nullptr;
    const char** envp_strings = nullptr;
    const char* cwd_string = "/";
    bool parse_ok = submit_vfs_identity.transfer_vfs_rules_to(new_task);

    if (submit->argc > 0) {
        argv_strings = new const char*[static_cast<size_t>(submit->argc) + 1];
        if (argv_strings == nullptr) {
            parse_ok = false;
        }
    }

    if (submit->envc > 0) {
        envp_strings = new const char*[static_cast<size_t>(submit->envc) + 1];
        if (envp_strings == nullptr) {
            parse_ok = false;
        }
    }

    const uint8_t* context_cursor = submit_context;
    uint16_t context_remaining = submit->args_len;
    auto next_context_string = [&](const char** out) -> bool {
        size_t len = 0;
        while (len < context_remaining && context_cursor[len] != '\0') {
            len++;
        }
        if (len >= context_remaining) {
            return false;
        }
        *out = reinterpret_cast<const char*>(context_cursor);
        context_cursor += len + 1;
        context_remaining = static_cast<uint16_t>(context_remaining - (len + 1));
        return true;
    };

    if (parse_ok && argv_strings != nullptr) {
        for (uint16_t i = 0; i < submit->argc; ++i) {
            if (!next_context_string(&argv_strings[i])) {
                parse_ok = false;
                break;
            }
        }
        argv_strings[submit->argc] = nullptr;
    }

    if (parse_ok && envp_strings != nullptr) {
        for (uint16_t i = 0; i < submit->envc; ++i) {
            if (!next_context_string(&envp_strings[i])) {
                parse_ok = false;
                break;
            }
        }
        envp_strings[submit->envc] = nullptr;
    }

    if (parse_ok && submit->cwd_len > 0) {
        if (submit->cwd_len > context_remaining || context_cursor[submit->cwd_len - 1] != '\0') {
            parse_ok = false;
        } else {
            cwd_string = reinterpret_cast<const char*>(context_cursor);
        }
    }

    if (!parse_ok) {
        delete[] argv_strings;
        delete[] envp_strings;
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-parse");

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::FETCH_FAILED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit parse failed: task_id=%u", submit->task_id);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::FETCH_FAILED), binary_len);
        return;
    }

    if (has_submitted_identity) {
        apply_submitted_task_identity(new_task, submitted_identity);
    }

    std::array<char, ker::mod::sched::task::Task::CWD_MAX> logical_cwd = {};
    const char* task_cwd = cwd_string;
    if (localize_receiver_logical_path(cwd_string, logical_cwd.data(), logical_cwd.size())) {
        task_cwd = logical_cwd.data();
    }

    // Receiver-created tasks keep the submitter hostname and inherited task VFS
    // rules.  Ordinary paths are then routed by the effective WKI VFS policy
    // rather than by a hidden receiver-local root override.

    size_t cwd_copy_len = std::strlen(task_cwd);
    if (cwd_copy_len >= ker::mod::sched::task::Task::CWD_MAX) {
        cwd_copy_len = ker::mod::sched::task::Task::CWD_MAX - 1;
    }
    memcpy(new_task->cwd.data(), task_cwd, cwd_copy_len);
    *std::next(new_task->cwd.begin(), static_cast<ptrdiff_t>(cwd_copy_len)) = '\0';
    new_task->cwd_len = static_cast<uint16_t>(cwd_copy_len);

    // The submitter's per-process root is part of the identity context.  Do not
    // inherit root from the receiver-side wki_compute kernel thread; that would
    // route relative user paths through the receiver's namespace.

    // Remote-receiver tasks execute locally by default. A later setwkitarget()
    // call can still opt an execve() into explicit or remote-preferred fan-out.
    new_task->wki_target_hostname.front() = '\0';
    new_task->wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL;
    new_task->wki_skip_legacy_placement = true;

    // Set task name to basename of exe_path so it shows in ps
    {
        const char* base = exe_path_buf.data();
        for (const char* p = exe_path_buf.data(); *p != '\0'; ++p) {
            if (*p == '/') {
                base = p + 1;
            }
        }
        if (base[0] != '\0') {
            size_t const BLEN = std::strlen(base);
            auto* name_buf = new char[BLEN + 1];
            std::memcpy(name_buf, base, BLEN + 1);
            delete[] new_task->name;
            new_task->name = name_buf;
        }
    }

    // Set up user stack: argc / argv / envp / auxv (System V x86-64 ABI)
    bool const STACK_SETUP_OK = [&]() {
        using namespace ker::mod::mm;

        uint64_t user_stack_top = new_task->thread->stack;
        uint64_t stack_offset = 0;
        constexpr uint64_t STACK_SETUP_LIMIT = 0x10000;  // 64 KiB safety limit

        auto stack_has_room = [&](size_t size) -> bool {
            if (size > STACK_SETUP_LIMIT) {
                return false;
            }
            return stack_offset <= STACK_SETUP_LIMIT - size;
        };

        auto copy_to_user_stack = [&](uint64_t vaddr, const void* data, size_t size) -> bool {
            if (size == 0) {
                return true;
            }
            uint64_t const END = vaddr + static_cast<uint64_t>(size);
            if (END < vaddr) {
                return false;
            }
            if (!ker::mod::sched::threading::ensure_stack_backing(new_task->thread, new_task->pagemap, vaddr, END)) {
                mod::dbg::log("remote_compute: failed to back stack range [0x%llx,0x%llx)", static_cast<unsigned long long>(vaddr),
                              static_cast<unsigned long long>(END));
                return false;
            }

            auto const* src = static_cast<const uint8_t*>(data);
            size_t copied = 0;
            while (copied < size) {
                uint64_t const CUR = vaddr + copied;
                uint64_t const PAGE_VIRT = CUR & ~(paging::PAGE_SIZE - 1);
                uint64_t const PAGE_OFF = CUR & (paging::PAGE_SIZE - 1);
                size_t const CHUNK = std::min(size - copied, static_cast<size_t>(paging::PAGE_SIZE - PAGE_OFF));

                uint64_t const PAGE_PHYS = virt::translate(new_task->pagemap, PAGE_VIRT);
                if (PAGE_PHYS == virt::PADDR_INVALID) {
                    mod::dbg::log("remote_compute: translate failed for stack vaddr 0x%x", PAGE_VIRT);
                    return false;
                }

                auto* dest = reinterpret_cast<uint8_t*>(addr::get_virt_pointer(PAGE_PHYS)) + PAGE_OFF;
                std::memcpy(dest, src + copied, CHUNK);
                copied += CHUNK;
            }
            return true;
        };

        auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
            if (!stack_has_room(size)) {
                return 0;
            }
            stack_offset += size;
            uint64_t const VADDR = user_stack_top - stack_offset;

            if (!copy_to_user_stack(VADDR, data, size)) {
                return 0;
            }
            return VADDR;
        };

        auto push_string = [&](const char* str) -> uint64_t {
            size_t const LEN = std::strlen(str) + 1;
            if (!stack_has_room(LEN)) {
                return 0;
            }
            stack_offset += LEN;
            uint64_t const VADDR = user_stack_top - stack_offset;

            if (!copy_to_user_stack(VADDR, str, LEN)) {
                return 0;
            }
            return VADDR;
        };

        auto* argv_addrs = new uint64_t[static_cast<size_t>(submit->argc) + 1];
        auto* envp_addrs = new uint64_t[static_cast<size_t>(submit->envc) + 1];
        if (argv_addrs == nullptr || envp_addrs == nullptr) {
            delete[] argv_addrs;
            delete[] envp_addrs;
            return false;
        }

        for (uint16_t i = 0; i < submit->argc; ++i) {
            argv_addrs[i] = push_string(argv_strings[i]);
            if (argv_addrs[i] == 0) {
                delete[] argv_addrs;
                delete[] envp_addrs;
                return false;
            }
        }
        argv_addrs[submit->argc] = 0;

        for (uint16_t i = 0; i < submit->envc; ++i) {
            envp_addrs[i] = push_string(envp_strings[i]);
            if (envp_addrs[i] == 0) {
                delete[] argv_addrs;
                delete[] envp_addrs;
                return false;
            }
        }
        envp_addrs[submit->envc] = 0;

        // Align to 16 bytes
        constexpr uint64_t ALIGN = 16;
        uint64_t const CUR = user_stack_top - stack_offset;
        uint64_t const ALIGNED = CUR & ~(ALIGN - 1);
        stack_offset += (CUR - ALIGNED);

        size_t const AUXV_QWORDS = 14 + (new_task->interp_base != 0 ? 2 : 0);
        size_t const STRUCTURED_QWORDS =
            AUXV_QWORDS + (static_cast<size_t>(submit->envc) + 1) + (static_cast<size_t>(submit->argc) + 1) + 1;
        if (STRUCTURED_QWORDS % 2 != 0) {
            uint64_t pad = 0;
            if (push_to_stack(&pad, sizeof(uint64_t)) == 0) {
                delete[] argv_addrs;
                delete[] envp_addrs;
                return false;
            }
        }

        // Mirror the normal execve() auxv contract so PT_INTERP binaries
        // (for example dropbearmulti) can bootstrap through ld.so.
        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_PHENT = 4;
        constexpr uint64_t AT_PHNUM = 5;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_BASE = 7;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_EHDR = 33;

        std::array<uint64_t, 16> auxv = {};
        size_t auxv_count = 0;
        auto append_auxv = [&](uint64_t key, uint64_t value) {
            *std::next(auxv.begin(), static_cast<ptrdiff_t>(auxv_count++)) = key;
            *std::next(auxv.begin(), static_cast<ptrdiff_t>(auxv_count++)) = value;
        };

        append_auxv(AT_PAGESZ, paging::PAGE_SIZE);
        append_auxv(AT_ENTRY, new_task->entry);
        append_auxv(AT_PHDR, new_task->program_header_addr);
        append_auxv(AT_PHENT, new_task->program_header_ent_size);
        append_auxv(AT_PHNUM, new_task->program_header_count);
        append_auxv(AT_EHDR, new_task->elf_header_addr);
        if (new_task->interp_base != 0) {
            append_auxv(AT_BASE, new_task->interp_base);
        }
        append_auxv(AT_NULL, 0);

        for (int j = static_cast<int>(auxv_count) - 1; j >= 0; --j) {
            uint64_t val = *std::next(auxv.begin(), j);
            if (push_to_stack(&val, sizeof(uint64_t)) == 0) {
                delete[] argv_addrs;
                delete[] envp_addrs;
                return false;
            }
        }

        uint64_t const ENVP_PTR = push_to_stack(envp_addrs, (static_cast<size_t>(submit->envc) + 1) * sizeof(uint64_t));
        uint64_t const ARGV_PTR = push_to_stack(argv_addrs, (static_cast<size_t>(submit->argc) + 1) * sizeof(uint64_t));
        delete[] envp_addrs;
        delete[] argv_addrs;
        if (ENVP_PTR == 0 || ARGV_PTR == 0) {
            return false;
        }

        // argc
        uint64_t argc_val = submit->argc;
        if (push_to_stack(&argc_val, sizeof(uint64_t)) == 0) {
            return false;
        }

        // Update user RSP and ABI registers
        new_task->context.frame.rsp = user_stack_top - stack_offset;
        new_task->context.regs.rdi = argc_val;
        new_task->context.regs.rsi = ARGV_PTR;
        new_task->context.regs.rdx = ENVP_PTR;
        return true;
    }();

    delete[] argv_strings;
    delete[] envp_strings;

    if (!STACK_SETUP_OK) {
        uint64_t const FAILED_PID = new_task->pid;
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-stack");

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit stack setup failed: task_id=%u pid=0x%lx", submit->task_id, FAILED_PID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
        return;
    }

    if (submit_deadline_expired()) {
        uint64_t const EXPIRED_PID = new_task->pid;
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-expired");

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit expired before post: task_id=%u pid=0x%lx", submit->task_id, EXPIRED_PID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    // Save PID before posting to scheduler.  Once posted, the task can
    // complete and be epoch-reclaimed before we touch exec.task again.
    uint64_t const LAUNCHED_PID = EXEC.task->pid;

    // Attach IPC proxy fds if the submitter exported any.
    if (submit->ipc_fd_count > 0 && submit_context != nullptr) {
        // IPC fd entries are appended after argv/envp/cwd in the submit context.
        // Find them by skipping past submit_context_len - (ipc_fd_count * sizeof(WkiIpcFdEntry)).
        uint16_t const IPC_DATA_SIZE = submit->ipc_fd_count * sizeof(WkiIpcFdEntry);
        if (submit_context_len >= IPC_DATA_SIZE) {
            const auto* ipc_entries = reinterpret_cast<const WkiIpcFdEntry*>(submit_context + submit_context_len - IPC_DATA_SIZE);
            wki_ipc_attach_task_fds(new_task, ipc_entries, submit->ipc_fd_count);
        }
    }
#ifdef DEBUG_WKI_COMPUTE
    int launched_cpu = -1;
    ker::mod::dbg::log("[WKI-DBG] task_id=%u pid=0x%lx pre-post: rsp=0x%lx rip=0x%lx entry=0x%lx stack_top=0x%lx cs=0x%x ss=0x%x",
                       submit->task_id, launched_pid, new_task->context.frame.rsp, new_task->context.frame.rip, new_task->entry,
                       new_task->thread ? new_task->thread->stack : 0, (uint32_t)new_task->context.frame.cs,
                       (uint32_t)new_task->context.frame.ss);
    ker::mod::dbg::log("[WKI-DBG] task_id=%u fsbase=0x%lx gsbase=0x%lx tlsBaseVirt=0x%lx safestackPtr=0x%lx", submit->task_id,
                       new_task->thread ? new_task->thread->fsbase : 0, new_task->thread ? new_task->thread->gsbase : 0,
                       new_task->thread ? new_task->thread->tlsBaseVirt : 0, new_task->thread ? new_task->thread->safestackPtrValue : 0);
#endif

    // Hold the completion-monitor reference before the task can run.  Very
    // short tasks may exit before post_task_balanced() returns.
    auto* completion_task_ref = static_cast<ker::mod::sched::task::Task*>(nullptr);
    auto* worker_task_ref = static_cast<ker::mod::sched::task::Task*>(nullptr);
    if (new_task->try_acquire()) {
        completion_task_ref = new_task;
    }
    // Scheduler publication transfers the creator's raw ownership. Keep a
    // distinct worker reference until every post-publication field access and
    // cancellation fallback has finished; the completion scanner may release
    // the monitor row's reference as soon as published becomes true.
    if (completion_task_ref != nullptr && new_task->try_acquire()) {
        worker_task_ref = new_task;
    }
    if (completion_task_ref == nullptr || worker_task_ref == nullptr) {
        if (completion_task_ref != nullptr) {
            completion_task_ref->release();
        }
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-ref");
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit ref failed: task_id=%u pid=0x%lx", submit->task_id, LAUNCHED_PID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    // Track for completion monitoring before the task can run. A very short
    // task, including one that fails immediately in the userspace loader, can
    // reach exit_notify_ready before post_task_balanced() returns.
    RunningRemoteTask rt;
    rt.active = true;
    rt.published = false;
    rt.accept_pending = false;
    rt.task_id = submit->task_id;
    rt.submitter_node = hdr->src_node;
    rt.local_pid = LAUNCHED_PID;
    rt.submit_session_epoch = session.epoch;
    rt.submit_rx_channel = session.rx_channel;
    rt.submit_rx_channel_generation = session.rx_channel_generation;
    rt.task = completion_task_ref;
    rt.output = EXEC.output;

    // Linearize scheduler publication with peer epoch/disconnect cleanup.
    // Lifecycle teardown owns the same lease before retiring this session, so
    // it either invalidates us before this final check or runs after the task
    // and its monitor row have been published consistently.
    PeerLifecycleLease publication_lease;
    if (!publication_lease.acquire(src_node)) {
        worker_task_ref->release();
        completion_task_ref->release();
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-peer");
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    s_compute_lock.lock();
    bool const FINAL_SESSION_CURRENT = compute_submit_session_is_current_locked(session);
    int32_t const FINAL_CANCEL_SIGNAL = take_cancelled_task_submit_locked(session, submit->task_id);
    bool const FINAL_DEADLINE_EXPIRED = submit_deadline_expired();
    if (!FINAL_SESSION_CURRENT || FINAL_CANCEL_SIGNAL != 0 || FINAL_DEADLINE_EXPIRED) {
        s_compute_lock.unlock();
        worker_task_ref->release();
        completion_task_ref->release();
        const char* failure_reason = "wki-submit-expired";
        if (!FINAL_SESSION_CURRENT) {
            failure_reason = "wki-submit-session";
        } else if (FINAL_CANCEL_SIGNAL != 0) {
            failure_reason = "wki-submit-cancel";
        }
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, failure_reason);
        if (FINAL_SESSION_CURRENT) {
            TaskResponsePayload reject = {};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            send_submit_response(MsgType::TASK_REJECT, reject);
        }
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }
    g_running_remote_tasks.push_back(rt);
    s_compute_lock.unlock();

    // The unpublished row owns the monitor reference/output but is invisible
    // to cancel and completion readers until scheduler publication returns.
    bool const POSTED = ker::mod::sched::post_task_balanced(new_task);
    if (POSTED && !ker::mod::sched::task::release_unpublished_process(current_task, new_task)) [[unlikely]] {
        ker::mod::dbg::panic_handler("WKI remote compute: lost receiver child ownership after scheduler publication");
        hcf();
    }
    bool send_accept = false;
    int32_t post_cancel_signal = 0;
    TaskOutputCapture* output_to_delete = EXEC.output;
    s_compute_lock.lock();
    RunningRemoteTask* published_row = nullptr;
    for (auto& running : g_running_remote_tasks) {
        if (running.active && !running.published && running.task == completion_task_ref && running.local_pid == LAUNCHED_PID) {
            published_row = &running;
            break;
        }
    }
    if (!POSTED) {
        if (published_row != nullptr) {
            published_row->task = nullptr;
            output_to_delete = published_row->output;
            published_row->output = nullptr;
            published_row->active = false;
        }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();

        worker_task_ref->release();
        completion_task_ref->release();
        destroy_unpublished_remote_process(current_task, new_task, output_to_delete, "wki-submit-post");
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit post failed: task_id=%u pid=0x%lx", submit->task_id, LAUNCHED_PID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }
    if (published_row != nullptr) {
        published_row->published = true;
        bool const CURRENT = compute_submit_session_is_current_locked(session);
        post_cancel_signal = take_cancelled_task_submit_locked(session, submit->task_id);
        bool const DEADLINE_EXPIRED = submit_deadline_expired();
        if ((!CURRENT || DEADLINE_EXPIRED) && post_cancel_signal == 0) {
            post_cancel_signal = WKI_SIGKILL_NUM;
        }
        if (!CURRENT || DEADLINE_EXPIRED || post_cancel_signal != 0) {
            published_row->discard_completion = true;
        }
        send_accept = CURRENT && !published_row->discard_completion;
        published_row->accept_pending = send_accept;
    }
    s_compute_lock.unlock();
#ifdef DEBUG_WKI_COMPUTE
    // post_task_balanced sets task->cpu before returning; capture it here
    // before the task can complete and be reclaimed.
    launched_cpu = static_cast<int>(new_task->cpu);
#endif

    // Send TASK_ACCEPT
    TaskResponsePayload accept = {};
    accept.task_id = submit->task_id;
    accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
    accept.remote_pid = LAUNCHED_PID;
    if (post_cancel_signal != 0) {
        uint64_t const OWNER_PID = task_process_owner_pid(new_task);
        size_t const SIGNALED = queue_signal_for_process_tasks(OWNER_PID, post_cancel_signal);
        if (SIGNALED == 0 && !new_task->has_exited && post_cancel_signal > 0 &&
            std::cmp_less_equal(post_cancel_signal, ker::mod::sched::task::Task::MAX_SIGNALS)) {
            new_task->signal_add_pending_mask(1ULL << (post_cancel_signal - 1));
            ker::mod::sched::wake_task_for_signal(new_task);
        }
    } else if (send_accept) {
        int const ACCEPT_RESULT = send_submit_response(MsgType::TASK_ACCEPT, accept);
        s_compute_lock.lock();
        if (auto* running = find_running_task(submit->task_id, session); running != nullptr) {
            if (ACCEPT_RESULT == WKI_OK) {
                running->accept_pending = false;
            } else if (!compute_submit_session_is_current_locked(session)) {
                running->accept_pending = false;
                running->discard_completion = true;
            }
        }
        s_compute_lock.unlock();
    }
    // A short-lived child can publish exit readiness before its monitor row or
    // TASK_ACCEPT state becomes visible. Recheck after both publication steps;
    // the worker reference keeps new_task alive through this notification.
    wki_remote_compute_notify_task_exit_ready(new_task);
    handle_measure.finish(0, binary_len);
#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] Remote task launched: task_id=%u pid=0x%lx on CPU %d mode=%u", submit->task_id, launched_pid, launched_cpu,
                       submit->delivery_mode);
#endif
    worker_task_ref->release();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Bounded kernel-thread pool for all task submits. Keeping remote VFS and
// PT_INTERP task construction off the RX path lets independent loads overlap
// without creating one worker per CPU.
// ---------------------------------------------------------------------------

auto compute_rx_worker_target(uint64_t cpu_count) -> size_t {
    uint64_t const AVAILABLE_CPUS = std::max<uint64_t>(cpu_count, 1);
    return static_cast<size_t>(std::min<uint64_t>(AVAILABLE_CPUS, WKI_COMPUTE_RX_WORKER_MAX));
}

auto compute_rx_worker_index(uint16_t src_node, size_t worker_count) -> size_t {
    if (worker_count == 0) {
        return 0;
    }
    uint32_t hash = src_node;
    hash ^= hash >> 16U;
    hash *= 0x7feb352dU;
    hash ^= hash >> 15U;
    hash *= 0x846ca68bU;
    hash ^= hash >> 16U;
    return static_cast<size_t>(hash % worker_count);
}

auto classify_compute_rx_payload(MsgType type, const uint8_t* payload, uint16_t payload_len, uint16_t* copy_len) -> WkiComputeRxAdmission {
    if (copy_len == nullptr) {
        return WkiComputeRxAdmission::DISCARD;
    }
    *copy_len = 0;

    if (type == MsgType::TASK_COMPLETE) {
        if (payload == nullptr || payload_len < sizeof(TaskCompletePayload)) {
            return WkiComputeRxAdmission::DISCARD;
        }
        TaskCompletePayload complete = {};
        std::memcpy(&complete, payload, sizeof(complete));
        size_t const EXPECTED_LEN = sizeof(TaskCompletePayload) + complete.output_len;
        if (complete.output_len > WKI_TASK_MAX_OUTPUT || EXPECTED_LEN > payload_len) {
            return WkiComputeRxAdmission::DISCARD;
        }
        *copy_len = static_cast<uint16_t>(EXPECTED_LEN);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_CANCEL) {
        if (payload == nullptr || payload_len < sizeof(uint32_t)) {
            return WkiComputeRxAdmission::DISCARD;
        }
        if (payload_len >= sizeof(TaskCancelPayload)) {
            TaskCancelPayload cancel = {};
            std::memcpy(&cancel, payload, sizeof(cancel));
            if (cancel.signum <= 0 || std::cmp_greater(cancel.signum, ker::mod::sched::task::Task::MAX_SIGNALS)) {
                return WkiComputeRxAdmission::DISCARD;
            }
            *copy_len = sizeof(TaskCancelPayload);
        } else {
            // Preserve the legacy task-id-only form, whose handler defaults to
            // SIGKILL. Ignore any partial signal bytes.
            *copy_len = sizeof(uint32_t);
        }
        return WkiComputeRxAdmission::DEFERRED;
    }

    return WkiComputeRxAdmission::INLINE;
}

auto dequeue_compute_rx(size_t worker_index, uint16_t* slot_index) -> bool {
    if (slot_index == nullptr || worker_index >= WKI_COMPUTE_RX_WORKER_MAX) {
        return false;
    }

    s_compute_rx_lock.lock();
    auto& shard = s_compute_rx_shards.at(worker_index);
    if (shard.count == 0) {
        s_compute_rx_lock.unlock();
        return false;
    }
    *slot_index = shard.slot_indices.at(shard.head);
    shard.head = (shard.head + 1) % WKI_COMPUTE_RX_QUEUE_MAX;
    shard.count--;
    s_compute_rx_lock.unlock();
    return true;
}

void release_compute_rx_slot(uint16_t slot_index) {
    if (slot_index >= WKI_COMPUTE_RX_QUEUE_MAX) {
        return;
    }
    s_compute_rx_lock.lock();
    s_compute_rx_slot_used.at(slot_index) = false;
    s_compute_rx_lock.unlock();
}

[[noreturn]] void compute_rx_worker_loop(size_t worker_index) {
    for (;;) {
        uint16_t slot_index = 0;
        if (!dequeue_compute_rx(worker_index, &slot_index)) {
            ker::mod::sched::kern_block();
            continue;
        }

        auto& work = s_compute_rx_slots.at(slot_index);
        {
            PeerLifecycleLease lifecycle;
            if (lifecycle.acquire(work.hdr.src_node)) {
                WkiPeer const* peer = wki_peer_find(work.hdr.src_node);
                if (peer != nullptr && peer->state == PeerState::CONNECTED &&
                    compute_submit_channel_token_matches(work.hdr.src_node, work.rx_channel, work.rx_channel_generation)) {
                    auto const WORK_TYPE = static_cast<MsgType>(work.type);
                    if (WORK_TYPE == MsgType::TASK_COMPLETE) {
                        detail::handle_task_complete(&work.hdr, work.payload.data(), work.payload_len, work.rx_channel,
                                                     work.rx_channel_generation);
                    } else if (WORK_TYPE == MsgType::TASK_CANCEL) {
                        detail::handle_task_cancel(&work.hdr, work.payload.data(), work.payload_len, work.rx_channel,
                                                   work.rx_channel_generation);
                    }
                }
            }
        }
        release_compute_rx_slot(slot_index);
    }
}

[[noreturn]] void compute_rx_worker_0() { compute_rx_worker_loop(0); }
[[noreturn]] void compute_rx_worker_1() { compute_rx_worker_loop(1); }
[[noreturn]] void compute_rx_worker_2() { compute_rx_worker_loop(2); }
[[noreturn]] void compute_rx_worker_3() { compute_rx_worker_loop(3); }

auto compute_submit_worker_target(uint64_t cpu_count) -> size_t {
    uint64_t const AVAILABLE_CPUS = std::max<uint64_t>(cpu_count, 1);
    return static_cast<size_t>(std::min<uint64_t>(AVAILABLE_CPUS, WKI_COMPUTE_SUBMIT_WORKER_MAX));
}

void destroy_unpublished_compute_submit_worker(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    delete reinterpret_cast<ker::mod::cpu::PerCpu*>(task->context.syscall_scratch_area);
    delete[] task->name;
    if (task->context.syscall_kernel_stack >= ker::mod::mm::KERNEL_STACK_SIZE) {
        ker::mod::mm::phys::page_free(reinterpret_cast<void*>(task->context.syscall_kernel_stack - ker::mod::mm::KERNEL_STACK_SIZE));
    }
    delete task;
}

void drain_pending_task_submits() {
    while (true) {
        s_pending_submit_lock.lock();
        if (s_pending_task_submit_count == 0) {
            s_pending_submit_lock.unlock();
            return;
        }
        PendingTaskSubmit pending = g_pending_task_submits.at(s_pending_task_submit_head);
        g_pending_task_submits.at(s_pending_task_submit_head) = {};
        s_pending_task_submit_head = (s_pending_task_submit_head + 1) % WKI_COMPUTE_SUBMIT_QUEUE_MAX;
        s_pending_task_submit_count--;
        s_pending_submit_lock.unlock();

        if (pending.payload_len >= sizeof(TaskSubmitPayload)) {
            const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(pending.payload);
            auto const WAIT_US = static_cast<uint32_t>(wki_now_us() - pending.queued_at_us);
            uint64_t const CALLSITE = WOS_PERF_CALLSITE();
            perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::DEFER_WAIT, pending.src_node, submit->task_id, pending.payload_len,
                                      CALLSITE);
            perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::DEFER_WAIT, pending.src_node, submit->task_id, 0, WAIT_US, 0,
                                    CALLSITE);
        }

        handle_task_submit_work(pending.src_node, pending.payload, pending.payload_len, pending.deadline_us, pending.session);
        delete[] pending.payload;
    }
}

[[noreturn]] void wki_compute_submit_thread() {
    for (;;) {
        drain_pending_task_submits();

        // Sleep until woken by wki_remote_compute_notify_pending_submit().
        // No polling — kern_wake() delivers an immediate wakeup when a new
        // TASK_SUBMIT is queued.
        s_pending_submit_lock.lock();
        bool const EMPTY = s_pending_task_submit_count == 0;
        s_pending_submit_lock.unlock();
        if (EMPTY) {
            ker::mod::sched::kern_block();
        }
    }
}

}  // namespace

auto wki_remote_compute_admit_rx(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation) -> WkiComputeRxAdmission {
    uint16_t copy_len = 0;
    WkiComputeRxAdmission const CLASSIFICATION = classify_compute_rx_payload(type, payload, payload_len, &copy_len);
    if (CLASSIFICATION != WkiComputeRxAdmission::DEFERRED) {
        return CLASSIFICATION;
    }
    if (hdr == nullptr || rx_channel == nullptr || rx_channel_generation == 0 || hdr->channel_id != WKI_CHAN_RESOURCE ||
        rx_channel->channel_id != WKI_CHAN_RESOURCE) {
        return WkiComputeRxAdmission::DISCARD;
    }

    size_t const WORKER_COUNT = s_compute_rx_task_count.load(std::memory_order_acquire);
    if (WORKER_COUNT == 0 || WORKER_COUNT > WKI_COMPUTE_RX_WORKER_MAX) {
        return WkiComputeRxAdmission::RETRY;
    }
    size_t const WORKER_INDEX = compute_rx_worker_index(hdr->src_node, WORKER_COUNT);

    size_t slot_index = WKI_COMPUTE_RX_QUEUE_MAX;
    s_compute_rx_lock.lock();
    for (size_t offset = 0; offset < WKI_COMPUTE_RX_QUEUE_MAX; ++offset) {
        size_t const CANDIDATE = (s_compute_rx_slot_cursor + offset) % WKI_COMPUTE_RX_QUEUE_MAX;
        if (!s_compute_rx_slot_used.at(CANDIDATE)) {
            slot_index = CANDIDATE;
            s_compute_rx_slot_used.at(CANDIDATE) = true;
            s_compute_rx_slot_cursor = (CANDIDATE + 1) % WKI_COMPUTE_RX_QUEUE_MAX;
            break;
        }
    }
    s_compute_rx_lock.unlock();
    if (slot_index == WKI_COMPUTE_RX_QUEUE_MAX) {
        return WkiComputeRxAdmission::RETRY;
    }

    // The slot is private until its index is published below. Keep the queue
    // lock out of the bounded 4 KiB copy while the reliable channel lock keeps
    // this frame stable.
    auto& slot = s_compute_rx_slots.at(slot_index);
    slot.type = static_cast<uint8_t>(type);
    slot.hdr = *hdr;
    slot.rx_channel = rx_channel;
    slot.rx_channel_generation = rx_channel_generation;
    slot.payload_len = copy_len;
    std::memcpy(slot.payload.data(), payload, copy_len);

    s_compute_rx_lock.lock();
    auto& shard = s_compute_rx_shards.at(WORKER_INDEX);
    if (shard.count >= WKI_COMPUTE_RX_QUEUE_MAX) {
        s_compute_rx_slot_used.at(slot_index) = false;
        s_compute_rx_lock.unlock();
        return WkiComputeRxAdmission::RETRY;
    }
    shard.slot_indices.at(shard.tail) = static_cast<uint16_t>(slot_index);
    shard.tail = (shard.tail + 1) % WKI_COMPUTE_RX_QUEUE_MAX;
    shard.count++;
    s_compute_rx_lock.unlock();
    return WkiComputeRxAdmission::DEFERRED;
}

void wki_remote_compute_notify_deferred_rx(uint16_t src_node) {
    size_t const WORKER_COUNT = s_compute_rx_task_count.load(std::memory_order_acquire);
    if (WORKER_COUNT == 0 || WORKER_COUNT > WKI_COMPUTE_RX_WORKER_MAX) {
        return;
    }
    size_t const WORKER_INDEX = compute_rx_worker_index(src_node, WORKER_COUNT);
    ker::mod::sched::wake_task_from_event(s_compute_rx_tasks.at(WORKER_INDEX));
}

void wki_remote_compute_start_rx_threads() {
    if (s_compute_rx_task_count.load(std::memory_order_acquire) != 0) {
        return;
    }

    using WorkerEntry = void (*)();
    constexpr std::array<WorkerEntry, WKI_COMPUTE_RX_WORKER_MAX> WORKER_ENTRIES = {
        compute_rx_worker_0,
        compute_rx_worker_1,
        compute_rx_worker_2,
        compute_rx_worker_3,
    };
    constexpr std::array<const char*, WKI_COMPUTE_RX_WORKER_MAX> WORKER_NAMES = {
        "wki_comp_rx0",
        "wki_comp_rx1",
        "wki_comp_rx2",
        "wki_comp_rx3",
    };
    size_t const TARGET_WORKERS = compute_rx_worker_target(ker::mod::smt::get_core_count());
    size_t started_workers = 0;
    for (size_t i = 0; i < TARGET_WORKERS; ++i) {
        auto* task = ker::mod::sched::task::Task::create_kernel_thread(WORKER_NAMES.at(i), WORKER_ENTRIES.at(i));
        if (task == nullptr) {
            ker::mod::dbg::log("[WKI] Failed to create compute RX worker %u", static_cast<unsigned>(i));
            break;
        }
        promote_latency_sensitive_daemon(task);
        if (!ker::mod::sched::post_task_balanced(task)) {
            ker::mod::dbg::log("[WKI] Failed to post compute RX worker %u", static_cast<unsigned>(i));
            destroy_unpublished_compute_submit_worker(task);
            break;
        }
        s_compute_rx_tasks.at(i) = task;
        started_workers++;
    }

    s_compute_rx_task_count.store(started_workers, std::memory_order_release);
    ker::mod::dbg::log("[WKI] Compute RX pool started (%u/%u workers)", static_cast<unsigned>(started_workers),
                       static_cast<unsigned>(TARGET_WORKERS));
}

void wki_remote_compute_start_submit_thread() {
    if (s_compute_submit_task_count.load(std::memory_order_acquire) != 0) {
        return;
    }

    constexpr std::array<const char*, WKI_COMPUTE_SUBMIT_WORKER_MAX> WORKER_NAMES = {
        "wki_compute0",
        "wki_compute1",
        "wki_compute2",
        "wki_compute3",
    };
    size_t const TARGET_WORKERS = compute_submit_worker_target(ker::mod::smt::get_core_count());
    size_t started_workers = 0;
    for (size_t i = 0; i < TARGET_WORKERS; ++i) {
        auto* task = ker::mod::sched::task::Task::create_kernel_thread(WORKER_NAMES.at(i), wki_compute_submit_thread);
        if (task == nullptr) {
            ker::mod::dbg::log("[WKI] Failed to create compute submit worker %u", static_cast<unsigned>(i));
            continue;
        }
        promote_latency_sensitive_daemon(task);
        if (!ker::mod::sched::post_task_balanced(task)) {
            ker::mod::dbg::log("[WKI] Failed to post compute submit worker %u", static_cast<unsigned>(i));
            destroy_unpublished_compute_submit_worker(task);
            continue;
        }
        s_compute_submit_tasks.at(started_workers++) = task;
    }

    s_compute_submit_task_count.store(started_workers, std::memory_order_release);
    wki_remote_compute_notify_pending_submit();
    ker::mod::dbg::log("[WKI] Compute submit pool started (%u/%u workers)", static_cast<unsigned>(started_workers),
                       static_cast<unsigned>(TARGET_WORKERS));
}

void wki_remote_compute_notify_pending_submit() {
    size_t const WORKER_COUNT = s_compute_submit_task_count.load(std::memory_order_acquire);
    for (size_t i = 0; i < WORKER_COUNT; ++i) {
        ker::mod::sched::kern_wake(s_compute_submit_tasks.at(i));
    }
}

void wki_remote_compute_process_pending_submits() { drain_pending_task_submits(); }

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_submit_context_lengths_are_checked() -> bool {
    uint16_t exact = 0;
    uint16_t untouched = 0xA55AU;
    bool const EXACT_MAX = checked_submit_args_len(UINT16_MAX - 2, 1, 1, &exact) && exact == UINT16_MAX;
    bool const COMBINED_OVERFLOW = !checked_submit_args_len(UINT16_MAX, 1, 0, &untouched) && untouched == 0xA55AU;
    bool const THREE_WAY_OVERFLOW = !checked_submit_args_len(0x8000U, 0x4000U, 0x4000U, &untouched) && untouched == 0xA55AU;
    bool const NULL_OUTPUT_REJECTED = !checked_submit_args_len(1, 2, 3, nullptr);
    return EXACT_MAX && COMBINED_OVERFLOW && THREE_WAY_OVERFLOW && NULL_OUTPUT_REJECTED;
}

auto wki_remote_compute_selftest_submit_worker_count_is_bounded() -> bool {
    return compute_submit_worker_target(0) == 1 && compute_submit_worker_target(1) == 1 && compute_submit_worker_target(2) == 2 &&
           compute_submit_worker_target(WKI_COMPUTE_SUBMIT_WORKER_MAX) == WKI_COMPUTE_SUBMIT_WORKER_MAX &&
           compute_submit_worker_target(WKI_COMPUTE_SUBMIT_WORKER_MAX + 1) == WKI_COMPUTE_SUBMIT_WORKER_MAX &&
           compute_submit_worker_target(256) == WKI_COMPUTE_SUBMIT_WORKER_MAX;
}

auto wki_remote_compute_selftest_rx_admission_is_bounded() -> bool {
    std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> complete_bytes = {};
    TaskCompletePayload complete = {};
    complete.output_len = WKI_TASK_MAX_OUTPUT;
    std::memcpy(complete_bytes.data(), &complete, sizeof(complete));

    uint16_t copy_len = 0;
    bool const COMPLETE_VALID =
        classify_compute_rx_payload(MsgType::TASK_COMPLETE, complete_bytes.data(), static_cast<uint16_t>(complete_bytes.size()),
                                    &copy_len) == WkiComputeRxAdmission::DEFERRED &&
        copy_len == complete_bytes.size();
    bool const COMPLETE_TRUNCATED =
        classify_compute_rx_payload(MsgType::TASK_COMPLETE, complete_bytes.data(), static_cast<uint16_t>(complete_bytes.size() - 1),
                                    &copy_len) == WkiComputeRxAdmission::DISCARD;
    complete.output_len = WKI_TASK_MAX_OUTPUT + 1;
    std::memcpy(complete_bytes.data(), &complete, sizeof(complete));
    bool const COMPLETE_OVERSIZED =
        classify_compute_rx_payload(MsgType::TASK_COMPLETE, complete_bytes.data(), static_cast<uint16_t>(complete_bytes.size()),
                                    &copy_len) == WkiComputeRxAdmission::DISCARD;

    TaskCancelPayload cancel = {.task_id = 1, .signum = WKI_SIGKILL_NUM};
    bool const CANCEL_VALID = classify_compute_rx_payload(MsgType::TASK_CANCEL, reinterpret_cast<const uint8_t*>(&cancel), sizeof(cancel),
                                                          &copy_len) == WkiComputeRxAdmission::DEFERRED &&
                              copy_len == sizeof(cancel);
    bool const CANCEL_LEGACY = classify_compute_rx_payload(MsgType::TASK_CANCEL, reinterpret_cast<const uint8_t*>(&cancel),
                                                           sizeof(cancel.task_id), &copy_len) == WkiComputeRxAdmission::DEFERRED &&
                               copy_len == sizeof(cancel.task_id);
    cancel.signum = 0;
    bool const CANCEL_INVALID = classify_compute_rx_payload(MsgType::TASK_CANCEL, reinterpret_cast<const uint8_t*>(&cancel), sizeof(cancel),
                                                            &copy_len) == WkiComputeRxAdmission::DISCARD;

    size_t const SHARD = compute_rx_worker_index(0x7A21, WKI_COMPUTE_RX_WORKER_MAX);
    return WKI_COMPUTE_RX_QUEUE_MAX == WKI_CREDITS_RESOURCE * WKI_COMPUTE_RX_WORKER_MAX && compute_rx_worker_target(0) == 1 &&
           compute_rx_worker_target(2) == 2 && compute_rx_worker_target(256) == WKI_COMPUTE_RX_WORKER_MAX &&
           SHARD < WKI_COMPUTE_RX_WORKER_MAX && SHARD == compute_rx_worker_index(0x7A21, WKI_COMPUTE_RX_WORKER_MAX) && COMPLETE_VALID &&
           COMPLETE_TRUNCATED && COMPLETE_OVERSIZED && CANCEL_VALID && CANCEL_LEGACY && CANCEL_INVALID;
}

auto wki_remote_compute_selftest_accept_retry_is_fair() -> bool {
    constexpr size_t ROW_COUNT = WKI_COMPUTE_SUBMIT_QUEUE_MAX + 2;
    std::deque<RunningRemoteTask> test_rows;
    for (size_t i = 0; i < ROW_COUNT; ++i) {
        RunningRemoteTask row{};
        row.active = true;
        row.published = true;
        row.accept_pending = true;
        row.task_id = static_cast<uint32_t>(i + 1);
        row.submitter_node = i < WKI_COMPUTE_SUBMIT_QUEUE_MAX ? 0x7A31 : 0x7A32;
        row.local_pid = i + 1;
        test_rows.push_back(row);
    }

    std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX> first = {};
    std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX> second = {};
    std::deque<RunningRemoteTask> saved_rows;

    s_compute_lock.lock();
    saved_rows.swap(g_running_remote_tasks);
    test_rows.swap(g_running_remote_tasks);
    size_t const SAVED_CURSOR = s_pending_task_accept_cursor;
    s_pending_task_accept_cursor = 0;
    size_t const FIRST_COUNT = collect_pending_task_accept_attempts_locked(first);
    size_t const SECOND_COUNT = collect_pending_task_accept_attempts_locked(second);
    s_pending_task_accept_cursor = SAVED_CURSOR;
    test_rows.swap(g_running_remote_tasks);
    saved_rows.swap(g_running_remote_tasks);
    s_compute_lock.unlock();

    bool later_peer_seen = false;
    for (size_t i = 0; i < SECOND_COUNT; ++i) {
        later_peer_seen = later_peer_seen || second.at(i).submitter_node == 0x7A32;
    }
    return FIRST_COUNT == WKI_COMPUTE_SUBMIT_QUEUE_MAX && SECOND_COUNT != 0 && later_peer_seen;
}

auto wki_remote_compute_selftest_exit_ready_completion_wake_is_exact() -> bool {
    ker::mod::sched::task::Task exact_task{};
    ker::mod::sched::task::Task other_task{};
    exact_task.pid = 0x7A41;
    exact_task.wki_remote_pid = exact_task.pid;
    exact_task.has_exited = true;
    exact_task.exit_notify_ready.store(true, std::memory_order_release);
    other_task.pid = exact_task.pid;
    other_task.has_exited = true;
    other_task.exit_notify_ready.store(true, std::memory_order_release);

    RunningRemoteTask row{};
    row.active = true;
    row.task = &exact_task;
    row.local_pid = exact_task.pid;

    bool const EXACT_MATCH = running_remote_task_matches_exit_ready(row, &exact_task);
    row.active = false;
    bool const INACTIVE_REJECTED = !running_remote_task_matches_exit_ready(row, &exact_task);
    row.active = true;
    bool const OTHER_TASK_REJECTED = !running_remote_task_matches_exit_ready(row, &other_task);
    row.local_pid++;
    bool const OTHER_PID_REJECTED = !running_remote_task_matches_exit_ready(row, &exact_task);
    row.local_pid = exact_task.pid;
    exact_task.exit_notify_ready.store(false, std::memory_order_release);
    bool const UNREADY_REJECTED = !running_remote_task_matches_exit_ready(row, &exact_task);
    exact_task.exit_notify_ready.store(true, std::memory_order_release);

    row.published = false;
    row.accept_pending = false;
    row.discard_completion = false;
    bool const UNPUBLISHED_INELIGIBLE = !running_remote_task_completion_eligible(row);
    row.published = true;
    row.accept_pending = true;
    bool const ACCEPT_PENDING_INELIGIBLE = !running_remote_task_completion_eligible(row);
    row.accept_pending = false;
    bool const ACCEPTED_ELIGIBLE = running_remote_task_completion_eligible(row);
    row.accept_pending = true;
    row.discard_completion = true;
    bool const DISCARD_ELIGIBLE = running_remote_task_completion_eligible(row);

    return EXACT_MATCH && INACTIVE_REJECTED && OTHER_TASK_REJECTED && OTHER_PID_REJECTED && UNREADY_REJECTED && UNPUBLISHED_INELIGIBLE &&
           ACCEPT_PENDING_INELIGIBLE && ACCEPTED_ELIGIBLE && DISCARD_ELIGIBLE;
}

auto wki_remote_compute_selftest_submit_cancel_is_session_scoped() -> bool {
    constexpr uint32_t TASK_ID = 0xC10101U;
    constexpr uint16_t NODE_ID = 0x7A26;
    WkiChannel channel{};
    ComputeSubmitSessionToken const SESSION_A = {
        .node_id = NODE_ID,
        .rx_channel = &channel,
        .rx_channel_generation = 7,
        .epoch = 11,
    };
    ComputeSubmitSessionToken const SESSION_B = {
        .node_id = NODE_ID,
        .rx_channel = &channel,
        .rx_channel_generation = 8,
        .epoch = 12,
    };

    s_compute_lock.lock();
    record_cancelled_task_submit_locked(SESSION_A, TASK_ID, WKI_SIGKILL_NUM);
    bool const EXACT_MATCH = find_cancelled_task_submit_locked(SESSION_A, TASK_ID) != nullptr;
    bool const OTHER_SESSION_CLEAR = find_cancelled_task_submit_locked(SESSION_B, TASK_ID) == nullptr;
    bool const CONSUMED = take_cancelled_task_submit_locked(SESSION_A, TASK_ID) == WKI_SIGKILL_NUM;
    bool const CLEARED = find_cancelled_task_submit_locked(SESSION_A, TASK_ID) == nullptr;
    s_compute_lock.unlock();
    return EXACT_MATCH && OTHER_SESSION_CLEAR && CONSUMED && CLEARED;
}
#endif

// ---------------------------------------------------------------------------
// detail:: RX handlers (called from NAPI dispatch in wki_rx)
// ---------------------------------------------------------------------------

namespace detail {

void handle_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskSubmitPayload)) {
        return;
    }

    const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(payload);
    auto mode = static_cast<TaskDeliveryMode>(submit->delivery_mode);
    s_compute_lock.lock();
    ComputeSubmitSessionToken const SESSION = capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation);
    s_compute_lock.unlock();
    if (SESSION.epoch == 0) {
        return;
    }

    auto reject_submit = [&](TaskRejectReason reason) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(reason);
        reject.remote_pid = 0;
        wki_send_on_channel_generation(hdr->src_node, SESSION.rx_channel, SESSION.rx_channel_generation, MsgType::TASK_REJECT, &reject,
                                       sizeof(reject));
    };

    // VFS/RESOURCE loads block directly, and INLINE construction can follow a
    // PT_INTERP path through VFS. Keep every process construction out of RX.
    if (s_compute_submit_task_count.load(std::memory_order_acquire) == 0) {
        reject_submit(TaskRejectReason::OVERLOADED);
        return;
    }

    auto* copy = new (std::nothrow) uint8_t[payload_len];
    if (copy == nullptr) {
        reject_submit(TaskRejectReason::NO_MEM);
        return;
    }
    std::memcpy(copy, payload, payload_len);

    PendingTaskSubmit pending{};
    pending.src_node = hdr->src_node;
    pending.payload = copy;
    pending.payload_len = payload_len;
    pending.queued_at_us = wki_now_us();
    uint64_t const RESPONSE_TIMEOUT_US =
        mode == TaskDeliveryMode::INLINE ? WKI_TASK_SUBMIT_INLINE_RECEIVER_TIMEOUT_US : WKI_TASK_SUBMIT_VFS_RECEIVER_TIMEOUT_US;
    pending.deadline_us = wki_future_deadline_us(pending.queued_at_us, RESPONSE_TIMEOUT_US);
    pending.session = SESSION;

    bool queued = false;
    s_pending_submit_lock.lock();
    if (s_pending_task_submit_count < WKI_COMPUTE_SUBMIT_QUEUE_MAX) {
        g_pending_task_submits.at(s_pending_task_submit_tail) = pending;
        s_pending_task_submit_tail = (s_pending_task_submit_tail + 1) % WKI_COMPUTE_SUBMIT_QUEUE_MAX;
        s_pending_task_submit_count++;
        queued = true;
    }
    s_pending_submit_lock.unlock();

    if (!queued) {
        delete[] copy;
        reject_submit(TaskRejectReason::OVERLOADED);
        return;
    }

    wki_remote_compute_notify_pending_submit();
}

void handle_task_accept(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || task->target_node != hdr->src_node || task->submit_channel != rx_channel ||
        task->submit_channel_generation != rx_channel_generation || !task->response_pending.load(std::memory_order_acquire)) {
        s_compute_lock.unlock();
        return;
    }

    WkiWaitEntry* waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
    if (waiter != nullptr) {
        task->accept_status = resp->status;
        task->accepted_at_us = wki_now_us();
        task->complete_received_at_us = 0;
        if (task->local_task != nullptr) {
            task->local_task->wki_remote_pid = resp->remote_pid;
        }
    }
    task->response_pending.store(false, std::memory_order_release);
    s_compute_lock.unlock();

    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, 0);
    }

    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::ACCEPT, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID, resp->task_id, 0,
                              0, WOS_PERF_CALLSITE());
}

void handle_task_reject(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || task->target_node != hdr->src_node || task->submit_channel != rx_channel ||
        task->submit_channel_generation != rx_channel_generation || !task->response_pending.load(std::memory_order_acquire)) {
        s_compute_lock.unlock();
        return;
    }

    WkiWaitEntry* waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
    if (waiter != nullptr) {
        task->accept_status = resp->status;
        if (task->local_task != nullptr) {
            task->local_task->wki_remote_pid = 0;
        }
    }
    task->response_pending.store(false, std::memory_order_release);
    s_compute_lock.unlock();

    if (waiter != nullptr) {
        wki_finish_claimed_op(waiter, 0);
    }

    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::REJECT, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID, resp->task_id,
                              -static_cast<int32_t>(resp->status == 0 ? 1 : resp->status), 0, WOS_PERF_CALLSITE());
}

void handle_task_complete(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                          uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskCompletePayload)) {
        return;
    }

    const auto* comp = reinterpret_cast<const TaskCompletePayload*>(payload);
    auto const AVAILABLE_OUTPUT = static_cast<uint16_t>(payload_len - sizeof(TaskCompletePayload));
    uint16_t const OUTPUT_LEN = std::min({comp->output_len, AVAILABLE_OUTPUT, WKI_TASK_MAX_OUTPUT});
    uint8_t* pending_output = nullptr;
    if (OUTPUT_LEN != 0) {
        pending_output = new (std::nothrow) uint8_t[OUTPUT_LEN];
        if (pending_output != nullptr) {
            std::memcpy(pending_output, payload + sizeof(TaskCompletePayload), OUTPUT_LEN);
        }
    }
    WkiWaitEntry* complete_waiter = nullptr;
    bool emit_task_runtime = false;
    uint32_t task_runtime_us = 0;
    uint64_t const COMPLETE_NOW_US = wki_now_us();
    ker::mod::sched::task::Task* proxy = nullptr;
    uint8_t* replaced_pending_output = nullptr;
    WkiWaitEntry* response_waiter = nullptr;

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(comp->task_id);
    if (task == nullptr || task->target_node != hdr->src_node || task->submit_channel != rx_channel ||
        task->submit_channel_generation != rx_channel_generation) {
        s_compute_lock.unlock();
        delete[] pending_output;
        ker::mod::dbg::log("[WKI] TASK_COMPLETE: no submitted task for task_id=%u", comp->task_id);
        return;
    }

    task->exit_status = comp->exit_status;
    if (task->accepted_at_us != 0 && COMPLETE_NOW_US >= task->accepted_at_us) {
        emit_task_runtime = true;
        task_runtime_us = static_cast<uint32_t>(COMPLETE_NOW_US - task->accepted_at_us);
    }

    proxy = task->proxy_ready ? task->take_local_task_ref() : nullptr;
    if (proxy != nullptr) {
        task->complete_received_at_us = 0;
    } else {
        task->complete_received_at_us = COMPLETE_NOW_US;
        if (task->local_task != nullptr && pending_output != nullptr) {
            replaced_pending_output = task->pending_proxy_output;
            task->pending_proxy_output = pending_output;
            task->pending_proxy_output_len = OUTPUT_LEN;
            pending_output = nullptr;
        }
    }

    if (task->response_pending.load(std::memory_order_acquire)) {
        response_waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
        if (response_waiter != nullptr) {
            task->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
            if (task->accepted_at_us == 0) {
                task->accepted_at_us = COMPLETE_NOW_US;
            }
        }
        task->response_pending.store(false, std::memory_order_release);
    }
    task->complete_pending.store(false, std::memory_order_release);
    complete_waiter = claim_and_clear_waiter_locked(task->complete_wait_entry);
    task->active = false;
    s_compute_lock.unlock();
    delete[] replaced_pending_output;

    // IPC data/close packets may already be queued on the deferred IPC worker
    // when TASK_COMPLETE arrives.  Normal proxy close, cancel, or peer fence
    // paths own export teardown so queued data cannot lose its backing file.
    if (emit_task_runtime) {
        uint64_t const CALLSITE = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::TASK_RUNTIME, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID,
                                  comp->task_id, 0, CALLSITE);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::TASK_RUNTIME, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID,
                                comp->task_id, comp->exit_status, task_runtime_us, 0, CALLSITE);
    }

    if (proxy != nullptr) {
        const uint8_t* output_data = payload + sizeof(TaskCompletePayload);
        if (OUTPUT_LEN > 0) {
            finalize_proxy_task(proxy, comp->exit_status, output_data, OUTPUT_LEN, comp->task_id);
        } else {
            finalize_proxy_task(proxy, comp->exit_status, nullptr, 0, comp->task_id);
        }
        request_submitted_task_reclaim(comp->task_id);
#ifdef DEBUG_WKI_COMPUTE
        ker::mod::dbg::log("[WKI] Proxy task pid=0x%lx completed: exit=%d (remote task_id=%u)", proxy->pid, comp->exit_status,
                           comp->task_id);
#endif
        proxy->release();
    } else {
        // proxy_ready=false: the proxy task hasn't blocked yet (it's still
        // in exec return / deferred-switch setup). Record exit_status and an
        // owned output copy so wki_proxy_task_blocked() can finalize without
        // racing an unlocked raw task/File pointer.
#ifdef DEBUG_WKI_COMPUTE
        ker::mod::dbg::log("[WKI] TASK_COMPLETE early (proxy not ready): task_id=%u exit=%d output=%u", comp->task_id, comp->exit_status,
                           OUTPUT_LEN);
#endif
    }
    delete[] pending_output;

    if (complete_waiter != nullptr) {
        wki_finish_claimed_op(complete_waiter, 0);
    }
    if (response_waiter != nullptr) {
        wki_finish_claimed_op(response_waiter, 0);
    }

    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::COMPLETE, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID, comp->task_id,
                              comp->exit_status, comp->output_len, WOS_PERF_CALLSITE());
}

void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(uint32_t)) {
        return;
    }

    const auto* cancel = reinterpret_cast<const TaskCancelPayload*>(payload);
    int const SIGNUM = payload_len >= sizeof(TaskCancelPayload) ? cancel->signum : WKI_SIGKILL_NUM;
    if (SIGNUM <= 0 || std::cmp_greater(SIGNUM, ker::mod::sched::task::Task::MAX_SIGNALS)) {
        return;
    }

    // D18: Find the running task and extract fields under lock
    s_compute_lock.lock();
    ComputeSubmitSessionToken const SESSION = capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation);
    RunningRemoteTask* rt = SESSION.epoch != 0 ? find_running_task(cancel->task_id, SESSION) : nullptr;
    if (rt == nullptr) {
        if (SESSION.epoch != 0 && SIGNUM > 0 && std::cmp_less_equal(SIGNUM, ker::mod::sched::task::Task::MAX_SIGNALS)) {
            record_cancelled_task_submit_locked(SESSION, cancel->task_id, SIGNUM);
        }
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] Task cancel queued before publication: task_id=%u from 0x%04x", cancel->task_id, hdr->src_node);
        return;
    }

    uint64_t const LOCAL_PID = rt->local_pid;
    if (rt->accept_pending) {
        rt->accept_pending = false;
        rt->discard_completion = true;
    }
    auto* task = rt->task;
    bool drop_task_ref = false;
    if (task != nullptr && task->try_acquire()) {
        drop_task_ref = true;
    } else {
        task = nullptr;
    }
    s_compute_lock.unlock();

    bool drop_lookup_ref = false;
    if (task == nullptr) {
        task = ker::mod::sched::find_task_by_pid_safe(LOCAL_PID);
        drop_lookup_ref = (task != nullptr);
    }
    uint64_t const OWNER_PID = task != nullptr ? task_process_owner_pid(task) : LOCAL_PID;
    size_t const SIGNALED = queue_signal_for_process_tasks(OWNER_PID, SIGNUM);
    if (SIGNALED != 0) {
        ker::mod::dbg::log("[WKI] Task cancel queued: task_id=%u pid=0x%lx sig=%d tasks=%lu", cancel->task_id, LOCAL_PID, SIGNUM,
                           static_cast<unsigned long>(SIGNALED));
    } else if (task != nullptr && !task->has_exited) {
        if (SIGNUM > 0 && std::cmp_less_equal(SIGNUM, ker::mod::sched::task::Task::MAX_SIGNALS)) {
            task->signal_add_pending_mask(1ULL << (SIGNUM - 1));
            ker::mod::sched::wake_task_for_signal(task);
            ker::mod::dbg::log("[WKI] Task cancel queued fallback: task_id=%u pid=0x%lx sig=%d", cancel->task_id, LOCAL_PID, SIGNUM);
        }
    }

    if (drop_lookup_ref) {
        task->release();
    }
    if (drop_task_ref) {
        task->release();
    }
}

void handle_load_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(LoadReportPayload)) {
        return;
    }

    const auto* report = reinterpret_cast<const LoadReportPayload*>(payload);

    // Find or create cache entry
    s_compute_lock.lock();
    RemoteNodeLoad* rl = find_remote_load(hdr->src_node);
    if (rl == nullptr) {
        RemoteNodeLoad new_rl;
        new_rl.valid = true;
        new_rl.node_id = hdr->src_node;
        g_remote_loads.push_back(new_rl);
        rl = &g_remote_loads.back();
    }

    rl->num_cpus = report->num_cpus;
    rl->runnable_tasks = report->runnable_tasks;
    rl->avg_load_pct = report->avg_load_pct;
    rl->free_mem_pages = report->free_mem_pages;
    rl->last_update_us = wki_now_us();
    s_compute_lock.unlock();
}

}  // namespace detail

}  // namespace ker::net::wki
