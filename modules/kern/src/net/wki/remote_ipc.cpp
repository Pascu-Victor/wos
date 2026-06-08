#include "remote_ipc.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <dev/pty.hpp>
#include <net/socket.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>
#include <syscalls_impl/futex/futex.hpp>
#include <utility>
#include <vfs/epoll.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "util/smallvec.hpp"

namespace ker::net::wki {

// =============================================================================
// File-scope globals (anonymous namespace)
// =============================================================================

namespace {

constexpr uint32_t WKI_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_LATENCY_DAEMON_NICE = -5;

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, WKI_LATENCY_DAEMON_NICE);
}

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
}

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return task->has_interrupting_signal_pending();
}

constexpr int WKI_SIGPIPE_NUM = 13;

void clear_current_daemon_sigpipe() {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->sig_pending &= ~(1ULL << (WKI_SIGPIPE_NUM - 1));
}

auto clamp_io_count(ssize_t result, size_t requested) -> ssize_t {
    if (result <= 0 || std::cmp_less_equal(result, requested)) {
        return result;
    }
    return static_cast<ssize_t>(requested);
}

void perf_record_ipc_event(uint8_t op, ker::mod::perf::WkiPerfPhase phase, uint16_t peer, uint16_t channel, uint32_t correlation,
                           int32_t status, uint32_t aux, uint64_t callsite) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op)) {
        return;
    }
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, phase, peer,
                                     channel, correlation, status, aux, callsite);
}

void perf_record_ipc_summary(uint8_t op, uint16_t peer, uint16_t channel, int32_t status, uint32_t latency_us, uint64_t bytes = 0,
                             uint32_t retries = 0) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op)) {
        return;
    }
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::REMOTE_IPC, op, peer, channel, status, latency_us, true, retries,
                                       bytes);
}

auto ipc_perf_enabled(ker::mod::perf::WkiPerfIpcOp op) -> bool {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::REMOTE_IPC, static_cast<uint8_t>(op));
}

class IpcPerfTrace {
   public:
    IpcPerfTrace(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint64_t callsite, uint32_t begin_aux = 0)
        : op_id(static_cast<uint8_t>(op)), peer_id(peer), channel_id(channel), trace_callsite(callsite) {
        if (!ipc_perf_enabled(op)) {
            return;
        }
        active = true;
        trace_correlation = ker::mod::perf::next_wki_trace_correlation();
        started_us = wki_now_us();
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::BEGIN, peer_id, channel_id, trace_correlation, 0, begin_aux,
                              trace_callsite);
    }

    IpcPerfTrace(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint64_t callsite, uint32_t begin_aux,
                 uint32_t correlation)
        : op_id(static_cast<uint8_t>(op)), peer_id(peer), channel_id(channel), trace_callsite(callsite), trace_correlation(correlation) {
        if (!ipc_perf_enabled(op)) {
            return;
        }
        active = true;
        started_us = wki_now_us();
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::BEGIN, peer_id, channel_id, trace_correlation, 0, begin_aux,
                              trace_callsite);
    }

    void finish(int32_t status, uint64_t bytes = 0, uint32_t retries = 0) const {
        if (!active) {
            return;
        }
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - started_us);
        perf_record_ipc_event(op_id, ker::mod::perf::WkiPerfPhase::END, peer_id, channel_id, trace_correlation, status, ELAPSED_US,
                              trace_callsite);
        perf_record_ipc_summary(op_id, peer_id, channel_id, status, ELAPSED_US, bytes, retries);
    }

   private:
    uint8_t op_id = 0;
    uint16_t peer_id = WKI_NODE_INVALID;
    uint16_t channel_id = WKI_CHAN_RESOURCE;
    uint64_t trace_callsite = 0;
    uint32_t trace_correlation = 0;
    uint64_t started_us = 0;
    bool active = false;
};

void perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp op, uint16_t peer, uint16_t channel, uint32_t correlation, int32_t status,
                           uint32_t aux, uint64_t callsite) {
    if (!ipc_perf_enabled(op)) {
        return;
    }
    perf_record_ipc_event(static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::POINT, peer, channel, correlation, status, aux, callsite);
}

void wake_blocked_ipc_reader(ker::mod::sched::task::Task* reader) {
    if (reader == nullptr) {
        return;
    }

    ker::mod::sched::wake_task_from_event(reader);
}

constexpr uint16_t WKI_IPC_FD_ACCESS_MASK = 0x0003;

bool g_ipc_initialized = false;
ker::mod::sys::Spinlock s_ipc_lock;

// Server-side (home node) exports
std::deque<WkiIpcExport*> g_ipc_exports;
uint32_t g_next_ipc_resource_id = 0x7000;

// Client-side (remote node) proxies — tracked for cleanup
std::deque<ProxyIpcState*> g_ipc_proxies;

struct PendingPipeChunk {
    uint8_t* data = nullptr;
    uint16_t len = 0;
    uint16_t offset = 0;
};

struct PendingPipeDelivery {
    uint16_t home_node = WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    std::deque<PendingPipeChunk> chunks;
    uint32_t buffered_bytes = 0;
    bool write_closed = false;
};

std::deque<PendingPipeDelivery*> g_pending_pipe_deliveries;

struct ExportPipeWriteBacklog {
    WkiIpcExport* exp = nullptr;
    uint32_t resource_id = 0;
    std::deque<PendingPipeChunk> chunks;
    uint32_t buffered_bytes = 0;
    bool close_pending = false;
    bool close_has_expected_bytes = false;
    uint64_t close_expected_bytes = 0;
    bool queued = false;
    bool persistent_for_rdma = false;
};

std::deque<ExportPipeWriteBacklog*> g_export_pipe_write_backlogs;
constexpr size_t WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT = 4;
std::array<std::deque<ExportPipeWriteBacklog*>, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> g_export_pipe_write_flush_queues;
std::array<ker::mod::sched::task::Task*, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> g_export_pipe_write_flush_tasks = {};
constexpr std::array<const char*, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_NAMES = {
    "wki_pipe_w0",
    "wki_pipe_w1",
    "wki_pipe_w2",
    "wki_pipe_w3",
};

struct IpcDevOpWork {
    WkiHeader hdr = {};
    uint8_t* payload = nullptr;
    uint16_t payload_len = 0;
};

constexpr size_t WKI_IPC_DEV_OP_WORKER_COUNT = 4;
std::array<std::deque<IpcDevOpWork*>, WKI_IPC_DEV_OP_WORKER_COUNT> g_ipc_dev_op_queues;
std::array<ker::mod::sched::task::Task*, WKI_IPC_DEV_OP_WORKER_COUNT> g_ipc_dev_op_worker_tasks = {};
constexpr size_t WKI_IPC_DEV_OP_MAX_PENDING = 256;

constexpr uint64_t WKI_IPC_PIPE_WRITE_RETRY_US = 1000;
constexpr uint32_t WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS = 128;
constexpr size_t WKI_IPC_MAX_POLL_WAKE_WAITERS = 32;
constexpr size_t WKI_IPC_PROXY_CLOSE_MSG_MAX = sizeof(DevOpReqPayload) + sizeof(uint32_t) + sizeof(uint64_t);
constexpr size_t WKI_IPC_PROXY_CLOSE_CLEANUP_BATCH = WKI_IPC_MAX_EXPORTS * 8;
constexpr int WKI_IPC_O_NONBLOCK = 04000;
constexpr size_t WKI_IPC_PIPE_DATA_HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
constexpr size_t WKI_IPC_PIPE_DATA_MAX_CHUNK = WKI_ETH_MAX_PAYLOAD - WKI_IPC_PIPE_DATA_HEADER_SIZE;
constexpr size_t WKI_IPC_EXPORT_PIPE_CAPACITY = 4UL * 1024UL * 1024UL;
static_assert(WKI_IPC_PIPE_DATA_HEADER_SIZE < WKI_ETH_MAX_PAYLOAD);
static_assert(WKI_IPC_PIPE_DATA_MAX_CHUNK <= UINT16_MAX);

#ifndef WKI_IPC_PIPE_RDMA_DOORBELL
#define WKI_IPC_PIPE_RDMA_DOORBELL 0  // NOLINT(cppcoreguidelines-macro-usage)
#endif

constexpr bool WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED = WKI_IPC_PIPE_RDMA_DOORBELL != 0;
constexpr uint32_t WKI_IPC_PIPE_RDMA_CAPACITY = WKI_IPC_PIPE_REGION_SIZE;
constexpr uint32_t WKI_IPC_PIPE_RDMA_TOTAL_SIZE = static_cast<uint32_t>(sizeof(WkiPipeSharedRegion) + WKI_IPC_PIPE_RDMA_CAPACITY);
constexpr uint32_t WKI_IPC_PIPE_RDMA_NOTIFY_RETRIES = 8;
constexpr uint32_t WKI_IPC_PIPE_RDMA_NOTIFY_BURST = 3;
constexpr uint64_t WKI_IPC_PIPE_RDMA_WATCH_US = 1000;
static_assert(WKI_IPC_PIPE_RDMA_CAPACITY != 0);

struct PendingProxyPipeClose {
    uint16_t home_node = WKI_NODE_INVALID;
    uint16_t msg_size = 0;
    uint16_t op_id = 0;
    uint32_t resource_id = 0;
    uint32_t attempts = 0;
    std::array<uint8_t, WKI_IPC_PROXY_CLOSE_MSG_MAX> msg = {};
};

std::deque<PendingProxyPipeClose*> g_pending_proxy_pipe_closes;
ker::mod::sched::task::Task* g_proxy_pipe_close_tx_task = nullptr;

auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool;
void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
auto stop_pipe_pump_locked(WkiIpcExport* exp) -> ker::mod::sched::task::Task*;
void wake_pipe_pump(ker::mod::sched::task::Task* task);
auto send_proxy_pipe_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> void;
auto enqueue_proxy_pipe_close_tx(uint16_t home_node, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> bool;
auto export_pipe_rdma_header(WkiIpcExport* exp) -> WkiPipeSharedRegion*;
auto export_pipe_rdma_front_locked(WkiIpcExport* exp, uint32_t* tail_out, uint16_t* len_out, uint8_t** data_out) -> bool;
auto queue_export_pipe_rdma_flush(uint16_t src_node, uint32_t resource_id) -> bool;

auto should_proxy_tty_like_file(ker::vfs::File* file) -> bool {
    if (file == nullptr || file->fs_type != ker::vfs::FSType::DEVFS) {
        return false;
    }

    bool const IS_DEVFS_WRAPPER = file->fops == ker::vfs::devfs::get_devfs_fops();
    if (IS_DEVFS_WRAPPER && ker::dev::pty::pty_is_file(file)) {
        return true;
    }

    return file->fops != nullptr && file->fops->vfs_isatty != nullptr && file->fops->vfs_isatty(file);
}

auto ipc_pipe_send_retry_sleep_us(int ret, uint32_t attempt) -> uint64_t {
    uint64_t const BASE_US = (ret == WKI_ERR_NO_CREDITS) ? 1000 : 2000;
    uint32_t const SHIFT = attempt < 5 ? attempt : 5;
    uint64_t const SLEEP_US = BASE_US << SHIFT;
    uint64_t const CAP_US = (ret == WKI_ERR_NO_CREDITS) ? 20000 : 50000;
    return std::min(SLEEP_US, CAP_US);
}

void pause_for_ipc_send_retry(int ret, uint32_t attempt) {
    if (ret == WKI_ERR_NO_CREDITS) {
        if (attempt == 0) {
            ker::mod::sched::kern_yield();
            return;
        }
    }
    ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(ret, attempt));
}

auto current_task_can_block_for_ipc_close() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return true;
    }

    return task->state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE && !task->has_exited;
}

void free_pending_proxy_pipe_close(PendingProxyPipeClose* pending) { delete pending; }

auto proxy_pipe_close_tx_terminal(int ret) -> bool {
    return ret == WKI_ERR_PEER_FENCED || ret == WKI_ERR_NO_ROUTE || ret == WKI_ERR_INVALID || ret == WKI_ERR_NOT_FOUND;
}

auto enqueue_proxy_pipe_close_tx(uint16_t home_node, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> bool {
    if (home_node == WKI_NODE_INVALID || msg == nullptr || msg_size == 0 || msg_size > WKI_IPC_PROXY_CLOSE_MSG_MAX) {
        return false;
    }

    auto* pending = new (std::nothrow) PendingProxyPipeClose();
    if (pending == nullptr) {
        return false;
    }

    pending->home_node = home_node;
    pending->msg_size = msg_size;
    pending->op_id = op_id;
    pending->resource_id = resource_id;
    std::memcpy(pending->msg.data(), msg, msg_size);

    ker::mod::sched::task::Task* worker = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    g_pending_proxy_pipe_closes.push_back(pending);
    worker = g_proxy_pipe_close_tx_task;
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (worker != nullptr) {
        ker::mod::sched::kern_wake(worker);
    }
    return true;
}

auto proxy_register_waiter_locked(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (auto waiter : waiters) {
        if (waiter == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

void proxy_collect_waiters_locked(ker::util::SmallVec<uint64_t, 2>& waiters, std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS>& pending,
                                  size_t* pending_count) {
    size_t copied = 0;
    while (copied < pending.size() && !waiters.empty()) {
        pending.at(copied++) = waiters.at(0);
        static_cast<void>(waiters.remove_at(0));
    }
    *pending_count = copied;
}

void proxy_reschedule_waiters(const std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS>& waiters, size_t waiter_count) {
    for (auto waiter_pid : std::span(waiters.data(), waiter_count)) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(waiter_pid);
        if (waiter == nullptr) {
            continue;
        }

        ker::mod::sched::wake_task_from_event(waiter, ker::mod::sched::EventWakeDeferredSwitch::CANCEL);
        waiter->release();
    }
}

// ---- Proxy fops for pipe (message-based, local ring buffer) ----

void proxy_release(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }
    if (proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] proxy->ring_buf;

        delete proxy;
    }
}

auto proxy_unregister_locked(ProxyIpcState* proxy) -> bool {
    for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end(); ++it) {
        if (*it == proxy) {
            g_ipc_proxies.erase(it);
            return true;
        }
    }
    return false;
}

auto find_proxy_by_resource_id_locked(uint32_t resource_id) -> ProxyIpcState* {
    for (auto* proxy : g_ipc_proxies) {
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->resource_id == resource_id) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            return proxy;
        }
    }
    return nullptr;
}

auto ipc_acquire_export_file_locked(uint32_t resource_id) -> ker::vfs::File* {
    for (auto* exp : g_ipc_exports) {
        if (exp == nullptr || !exp->active || exp->resource_id != resource_id || exp->file == nullptr) {
            continue;
        }

        exp->file->refcount.fetch_add(1, std::memory_order_acq_rel);
        return exp->file;
    }
    return nullptr;
}

void compact_inactive_exports_locked() {
    std::erase_if(g_ipc_exports, [](const WkiIpcExport* exp) { return exp == nullptr || !exp->active || exp->file == nullptr; });
}

void ipc_release_file_ref(ker::vfs::File* file) {
    if (file == nullptr) {
        return;
    }
    if (file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
            file->fops->vfs_close(file);
        }
        delete file;
    }
}

auto find_pending_pipe_delivery_locked(uint16_t home_node, uint32_t resource_id) -> PendingPipeDelivery* {
    for (auto* pending : g_pending_pipe_deliveries) {
        if (pending != nullptr && pending->home_node == home_node && pending->resource_id == resource_id) {
            return pending;
        }
    }
    return nullptr;
}

void erase_pending_pipe_delivery_locked(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }

    for (auto it = g_pending_pipe_deliveries.begin(); it != g_pending_pipe_deliveries.end(); ++it) {
        if (*it == pending) {
            g_pending_pipe_deliveries.erase(it);
            return;
        }
    }
}

auto take_pending_pipe_delivery_locked(uint16_t home_node, uint32_t resource_id) -> PendingPipeDelivery* {
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending != nullptr) {
        erase_pending_pipe_delivery_locked(pending);
    }
    return pending;
}

void free_pending_pipe_delivery(PendingPipeDelivery* pending) {
    if (pending == nullptr) {
        return;
    }
    for (auto& chunk : pending->chunks) {
        delete[] chunk.data;
    }
    delete pending;
}

auto find_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    for (auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog != nullptr && backlog->exp == exp) {
            return backlog;
        }
    }
    return nullptr;
}

auto export_pipe_write_backlog_shard(const ExportPipeWriteBacklog* backlog) -> size_t {
    if (backlog == nullptr) {
        return 0;
    }
    return static_cast<size_t>(backlog->resource_id % WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT);
}

void wake_export_pipe_write_flush_worker(size_t shard) {
    if (shard >= g_export_pipe_write_flush_tasks.size()) {
        return;
    }
    auto* task = g_export_pipe_write_flush_tasks.at(shard);
    if (task != nullptr) {
        ker::mod::sched::kern_wake(task);
    }
}

void wake_all_export_pipe_write_flush_workers() {
    for (size_t shard = 0; shard < g_export_pipe_write_flush_tasks.size(); ++shard) {
        wake_export_pipe_write_flush_worker(shard);
    }
}

auto export_pipe_write_flush_queue_depth_locked() -> size_t {
    size_t depth = 0;
    for (const auto& queue : g_export_pipe_write_flush_queues) {
        depth += queue.size();
    }
    return depth;
}

void erase_export_pipe_write_backlog_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto it = g_export_pipe_write_backlogs.begin(); it != g_export_pipe_write_backlogs.end(); ++it) {
        if (*it == backlog) {
            g_export_pipe_write_backlogs.erase(it);
            break;
        }
    }

    for (auto& queue : g_export_pipe_write_flush_queues) {
        for (auto it = queue.begin(); it != queue.end();) {
            if (*it == backlog) {
                it = queue.erase(it);
                continue;
            }
            ++it;
        }
    }

    backlog->queued = false;
}

void free_export_pipe_write_backlog(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr) {
        return;
    }

    for (auto& chunk : backlog->chunks) {
        delete[] chunk.data;
    }
    delete backlog;
}

auto ensure_export_pipe_write_backlog_locked(WkiIpcExport* exp) -> ExportPipeWriteBacklog* {
    auto* backlog = find_export_pipe_write_backlog_locked(exp);
    if (backlog != nullptr) {
        return backlog;
    }

    backlog = new ExportPipeWriteBacklog();
    if (backlog == nullptr) {
        return nullptr;
    }
    backlog->exp = exp;
    backlog->resource_id = exp->resource_id;
    g_export_pipe_write_backlogs.push_back(backlog);
    return backlog;
}

void queue_export_pipe_write_flush_locked(ExportPipeWriteBacklog* backlog) {
    if (backlog == nullptr || backlog->queued) {
        return;
    }

    backlog->queued = true;
    g_export_pipe_write_flush_queues.at(export_pipe_write_backlog_shard(backlog)).push_back(backlog);
}

void note_export_pipe_data_received_locked(WkiIpcExport* exp, uint16_t len) {
    if (exp == nullptr || len == 0) {
        return;
    }

    exp->pipe_bytes_received += len;
    auto* backlog = find_export_pipe_write_backlog_locked(exp);
    if (backlog != nullptr && backlog->close_pending) {
        queue_export_pipe_write_flush_locked(backlog);
    }
}

auto queue_export_pipe_write_data(WkiIpcExport* exp, const uint8_t* data, uint16_t len) -> bool {
    if (exp == nullptr || data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        delete[] copy;
        return false;
    }

    backlog->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    backlog->buffered_bytes += len;
    queue_export_pipe_write_flush_locked(backlog);
    size_t const FLUSH_SHARD = export_pipe_write_backlog_shard(backlog);
    s_ipc_lock.unlock_irqrestore(IRQF);

    wake_export_pipe_write_flush_worker(FLUSH_SHARD);
    return true;
}

auto export_pipe_write_nonblocking(ker::vfs::File* file, const uint8_t* data, uint16_t len) -> ssize_t {
    if (len == 0) {
        return 0;
    }
    if (file == nullptr || data == nullptr || file->fops == nullptr || file->fops->vfs_write == nullptr) {
        return -EBADF;
    }

    bool const IS_PIPE = ker::vfs::vfs_is_pipe_file(file);
    int const SAVED_FLAGS = file->open_flags;
    if (IS_PIPE) {
        file->open_flags = SAVED_FLAGS | WKI_IPC_O_NONBLOCK;
    }

    clear_current_daemon_sigpipe();
    ssize_t const RET = clamp_io_count(file->fops->vfs_write(file, data, len, static_cast<size_t>(file->pos)), len);
    clear_current_daemon_sigpipe();

    if (IS_PIPE) {
        file->open_flags = SAVED_FLAGS;
    }
    return RET;
}

auto mark_export_pipe_write_closed(WkiIpcExport* exp, uint64_t expected_bytes = 0, bool has_expected_bytes = false) -> bool {
    if (exp == nullptr) {
        return false;
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    if (!exp->active || exp->file == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return false;
    }

    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return false;
    }

    backlog->close_pending = true;
    if (has_expected_bytes) {
        backlog->close_has_expected_bytes = true;
        backlog->close_expected_bytes = expected_bytes;
    }
    queue_export_pipe_write_flush_locked(backlog);
    size_t const FLUSH_SHARD = export_pipe_write_backlog_shard(backlog);
    s_ipc_lock.unlock_irqrestore(IRQF);

    wake_export_pipe_write_flush_worker(FLUSH_SHARD);
    return true;
}

[[noreturn]] void export_pipe_write_flush_thread_loop(size_t shard) {
    for (;;) {
        ExportPipeWriteBacklog* backlog = nullptr;
        uint64_t irqf = s_ipc_lock.lock_irqsave();
        auto& queue = g_export_pipe_write_flush_queues.at(shard);
        if (!queue.empty()) {
            backlog = queue.front();
            queue.pop_front();
            if (backlog != nullptr) {
                backlog->queued = false;
            }
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        if (backlog == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        WkiIpcExport* exp = nullptr;
        ker::vfs::File* file = nullptr;
        uint32_t resource_id = 0;
        uint16_t remaining = 0;
        uint8_t const* data_ptr = nullptr;
        uint8_t* rdma_data_ptr = nullptr;
        uint32_t rdma_tail = 0;
        bool rdma_chunk = false;
        bool close_pending = false;
        bool close_ready = false;

        irqf = s_ipc_lock.lock_irqsave();
        exp = backlog->exp;
        if (exp == nullptr || !exp->active || exp->file == nullptr) {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        file = exp->file;
        resource_id = exp->resource_id;
        close_pending = backlog->close_pending;
        close_ready = close_pending && (!backlog->close_has_expected_bytes || exp->pipe_bytes_received >= backlog->close_expected_bytes);

        if (!backlog->chunks.empty()) {
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            auto& chunk = backlog->chunks.front();
            remaining = static_cast<uint16_t>(chunk.len - chunk.offset);
            data_ptr = chunk.data + chunk.offset;
        } else if (export_pipe_rdma_front_locked(exp, &rdma_tail, &remaining, &rdma_data_ptr)) {
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            data_ptr = rdma_data_ptr;
            rdma_chunk = true;
        } else if (close_pending) {
            if (!close_ready) {
                s_ipc_lock.unlock_irqrestore(irqf);
                continue;
            }
            auto* pump_task = stop_pipe_pump_locked(exp);
            exp->file = nullptr;
            exp->active = false;
            compact_inactive_exports_locked();
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            wake_pipe_pump(pump_task);
            ipc_release_file_ref(file);
            free_export_pipe_write_backlog(backlog);
            continue;
        } else {
            if (!backlog->persistent_for_rdma) {
                erase_export_pipe_write_backlog_locked(backlog);
            } else {
                queue_export_pipe_write_flush_locked(backlog);
            }
            s_ipc_lock.unlock_irqrestore(irqf);
            if (!backlog->persistent_for_rdma) {
                free_export_pipe_write_backlog(backlog);
            } else {
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_RDMA_WATCH_US);
            }
            continue;
        }
        s_ipc_lock.unlock_irqrestore(irqf);

        ssize_t const WRITE_RET = export_pipe_write_nonblocking(file, data_ptr, remaining);

        irqf = s_ipc_lock.lock_irqsave();
        exp = backlog->exp;
        if (exp == nullptr || !exp->active || exp->file != file) {
            erase_export_pipe_write_backlog_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            free_export_pipe_write_backlog(backlog);
            continue;
        }

        if (WRITE_RET > 0) {
            auto const ADVANCED = static_cast<uint16_t>(std::cmp_less(WRITE_RET, remaining) ? WRITE_RET : remaining);
            if (rdma_chunk) {
                auto* header = export_pipe_rdma_header(exp);
                if (header != nullptr) {
                    uint32_t const NEW_TAIL = rdma_tail + ADVANCED;
                    header->tail.store(NEW_TAIL, std::memory_order_release);
                    uint32_t const HEAD = header->head.load(std::memory_order_acquire);
                    uint32_t const USED = HEAD - NEW_TAIL;
                    uint32_t const CREDITS = USED < exp->pipe_rdma_capacity ? exp->pipe_rdma_capacity - USED : 0;
                    header->credits.store(CREDITS, std::memory_order_release);
                    exp->pipe_bytes_received += ADVANCED;
                }
            } else {
                auto& chunk = backlog->chunks.front();
                chunk.offset = static_cast<uint16_t>(chunk.offset + ADVANCED);
                backlog->buffered_bytes -= ADVANCED;
                if (chunk.offset == chunk.len) {
                    delete[] chunk.data;
                    backlog->chunks.pop_front();
                }
            }

            uint32_t next_tail = 0;
            uint16_t next_len = 0;
            uint8_t* next_data = nullptr;
            bool const HAS_RDMA = export_pipe_rdma_front_locked(exp, &next_tail, &next_len, &next_data);
            if (!backlog->chunks.empty() || HAS_RDMA || backlog->close_pending) {
                queue_export_pipe_write_flush_locked(backlog);
            } else if (!backlog->persistent_for_rdma) {
                erase_export_pipe_write_backlog_locked(backlog);
            }
            s_ipc_lock.unlock_irqrestore(irqf);
            ipc_release_file_ref(file);
            if (!backlog->persistent_for_rdma && backlog->chunks.empty() && !backlog->close_pending) {
                free_export_pipe_write_backlog(backlog);
            }
            continue;
        }

        if (WRITE_RET == -EAGAIN || WRITE_RET == -EINTR) {
            queue_export_pipe_write_flush_locked(backlog);
            s_ipc_lock.unlock_irqrestore(irqf);
            if (WRITE_RET == -EINTR) {
                ipc_release_file_ref(file);
                ker::mod::sched::kern_yield();
                continue;
            }
            bool ready_now = false;
            bool const POLL_REGISTERED = register_poll_write_waiter(file, &ready_now);
            ipc_release_file_ref(file);
            if (POLL_REGISTERED && !ready_now) {
                ker::mod::sched::kern_block();
            } else if (!POLL_REGISTERED) {
                ker::mod::sched::kern_sleep_us(WKI_IPC_PIPE_WRITE_RETRY_US);
            } else {
                ker::mod::sched::kern_yield();
            }
            continue;
        }

        ker::mod::dbg::log("[WKI] IPC export pipe write failed: resource_id=%u ret=%ld remaining=%u", resource_id, WRITE_RET, remaining);
        exp->file = nullptr;
        exp->active = false;
        compact_inactive_exports_locked();
        erase_export_pipe_write_backlog_locked(backlog);
        s_ipc_lock.unlock_irqrestore(irqf);
        ipc_release_file_ref(file);
        free_export_pipe_write_backlog(backlog);
    }
}

[[noreturn]] void export_pipe_write_flush_thread_0() { export_pipe_write_flush_thread_loop(0); }

[[noreturn]] void export_pipe_write_flush_thread_1() { export_pipe_write_flush_thread_loop(1); }

[[noreturn]] void export_pipe_write_flush_thread_2() { export_pipe_write_flush_thread_loop(2); }

[[noreturn]] void export_pipe_write_flush_thread_3() { export_pipe_write_flush_thread_loop(3); }

auto queue_pending_pipe_data(uint16_t home_node, uint32_t resource_id, const uint8_t* data, uint16_t len) -> bool {
    if (data == nullptr || len == 0) {
        return true;
    }

    auto* copy = new (std::nothrow) uint8_t[len];
    if (copy == nullptr) {
        return false;
    }
    std::memcpy(copy, data, len);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            delete[] copy;
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }

    pending->chunks.push_back(PendingPipeChunk{.data = copy, .len = len});
    pending->buffered_bytes += len;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto mark_pending_pipe_write_closed(uint16_t home_node, uint32_t resource_id) -> bool {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr) {
        pending = new (std::nothrow) PendingPipeDelivery();
        if (pending == nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return false;
        }
        pending->home_node = home_node;
        pending->resource_id = resource_id;
        g_pending_pipe_deliveries.push_back(pending);
    }

    pending->write_closed = true;
    s_ipc_lock.unlock_irqrestore(IRQF);
    return true;
}

auto drain_pending_pipe_data(uint16_t home_node, uint32_t resource_id, void* buf, size_t count) -> size_t {
    if (buf == nullptr || count == 0) {
        return 0;
    }

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending == nullptr || pending->chunks.empty()) {
        s_ipc_lock.unlock_irqrestore(IRQF);
        return 0;
    }

    auto& chunk = pending->chunks.front();
    auto const REMAINING = static_cast<uint16_t>(chunk.len - chunk.offset);
    size_t const TO_COPY = count < REMAINING ? count : REMAINING;
    std::memcpy(buf, chunk.data + chunk.offset, TO_COPY);
    chunk.offset = static_cast<uint16_t>(chunk.offset + TO_COPY);
    pending->buffered_bytes -= static_cast<uint32_t>(TO_COPY);

    if (chunk.offset == chunk.len) {
        delete[] chunk.data;
        pending->chunks.pop_front();
    }

    if (pending->chunks.empty() && !pending->write_closed) {
        erase_pending_pipe_delivery_locked(pending);
        s_ipc_lock.unlock_irqrestore(IRQF);
        free_pending_pipe_delivery(pending);
        return TO_COPY;
    }

    s_ipc_lock.unlock_irqrestore(IRQF);
    return TO_COPY;
}

auto consume_pending_pipe_closed(uint16_t home_node, uint32_t resource_id) -> bool {
    PendingPipeDelivery* closed_pending = nullptr;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* pending = find_pending_pipe_delivery_locked(home_node, resource_id);
    if (pending != nullptr && pending->write_closed && pending->chunks.empty()) {
        erase_pending_pipe_delivery_locked(pending);
        closed_pending = pending;
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (closed_pending == nullptr) {
        return false;
    }

    free_pending_pipe_delivery(closed_pending);
    return true;
}

void proxy_mark_pipe_closed(ProxyIpcState* proxy, uint32_t resource_id) {
    if (proxy == nullptr) {
        return;
    }

    (void)resource_id;

    auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
    uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
    proxy->write_closed.store(1U, std::memory_order_release);
    reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    proxy->lock.unlock_irqrestore(PROXY_IRQF);

#ifdef WKI_IPC_DEBUG
    ker::mod::dbg::log("[WKI] IPC proxy EOF received: resource_id=%u reader_pid=0x%lx", resource_id, reader != nullptr ? reader->pid : 0UL);
#endif

    wake_blocked_ipc_reader(reader);

    wki_ipc_proxy_wake_poll_waiters(proxy);
}

void wake_proxy_reader(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    wake_blocked_ipc_reader(reader);
}

auto proxy_pipe_read(ker::vfs::File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::PROXY_READ, proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE,
                       WOS_PERF_CALLSITE());
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        trace.finish(STATUS, bytes);
        return rc;
    };
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (proxy->ring_buf == nullptr) {
        return finish(-EIO);
    }

    // Spin briefly then block
    for (;;) {
        // Spin briefly then block
        for (int spin = 0; spin < 2000; spin++) {
            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
            uint32_t const AVAIL = HEAD - TAIL;

            if (AVAIL > 0) {
                uint32_t const TO_READ = count < AVAIL ? static_cast<uint32_t>(count) : AVAIL;
                uint32_t const CAP = proxy->ring_capacity;
                uint32_t const RING_TAIL = TAIL % CAP;
                auto* dst = static_cast<uint8_t*>(buf);

                uint32_t const FIRST = CAP - RING_TAIL;
                if (FIRST >= TO_READ) {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, TO_READ);
                } else {
                    std::memcpy(dst, proxy->ring_buf + RING_TAIL, FIRST);
                    std::memcpy(dst + FIRST, proxy->ring_buf, TO_READ - FIRST);
                }
                proxy->ring_tail.store(TAIL + TO_READ, std::memory_order_release);
                return finish(static_cast<ssize_t>(TO_READ), TO_READ);
            }

            size_t const QUEUED = drain_pending_pipe_data(proxy->home_node, proxy->resource_id, buf, count);
            if (QUEUED > 0) {
                return finish(static_cast<ssize_t>(QUEUED), QUEUED);
            }

            if (consume_pending_pipe_closed(proxy->home_node, proxy->resource_id)) {
                proxy_mark_pipe_closed(proxy, proxy->resource_id);
            }

            if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
                return finish(0);
            }

            asm volatile("pause" ::: "memory");
        }

        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return finish(-ESRCH);
        }

        // Serialize block registration with incoming DATA/CLOSE and pending
        // overflow queue updates so we don't sleep after a writer already
        // queued bytes for this proxy.
        uint64_t const PENDING_IRQF = s_ipc_lock.lock_irqsave();
        uint64_t const IRQF = proxy->lock.lock_irqsave();
        uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
        uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_relaxed);
        if (HEAD != TAIL) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            continue;
        }
        auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        if (pending != nullptr && !pending->chunks.empty()) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            continue;
        }
        bool const PENDING_CLOSED = pending != nullptr && pending->write_closed;
        if (PENDING_CLOSED) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            if (consume_pending_pipe_closed(proxy->home_node, proxy->resource_id)) {
                proxy_mark_pipe_closed(proxy, proxy->resource_id);
            }
            continue;
        }
        if (proxy->write_closed.load(std::memory_order_acquire) != 0U) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            return finish(0);
        }
        if (current_task_has_deliverable_signal()) {
            proxy->lock.unlock_irqrestore(IRQF);
            s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
            return finish(-EINTR);
        }

        task->wait_channel = "wki_proxy_pipe";
        proxy->blocked_reader.store(task, std::memory_order_release);
        proxy->lock.unlock_irqrestore(IRQF);
        s_ipc_lock.unlock_irqrestore(PENDING_IRQF);
        ker::mod::sched::preemptible_syscall_park("wki_proxy_pipe");
        if (current_task_has_deliverable_signal()) {
            auto* expected = task;
            proxy->blocked_reader.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel, std::memory_order_acquire);
            return finish(-EINTR);
        }
    }
}

auto ipc_pipe_rdma_transport_for_peer(uint16_t peer_node) -> WkiTransport* {
    if (!WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED) {
        return nullptr;
    }

    WkiPeer const* peer = wki_peer_find(peer_node);
    WkiTransport* transport = peer != nullptr ? peer->rdma_transport : nullptr;
    if (transport == nullptr || transport->name == nullptr) {
        return nullptr;
    }
    if (std::strcmp(transport->name, "wki-roce") != 0) {
        return nullptr;
    }
    if (!transport->rdma_capable || transport->rdma_register_region == nullptr || transport->rdma_read == nullptr ||
        transport->rdma_write == nullptr || transport->doorbell == nullptr) {
        return nullptr;
    }
    return transport;
}

auto acquire_proxy_pipe_rdma_writer(ProxyIpcState* proxy) -> bool {
    if (proxy == nullptr) {
        return false;
    }

    uint32_t spins = 0;
    while (proxy->active.load(std::memory_order_acquire)) {
        bool expected = false;
        if (proxy->pipe_rdma_writer_active.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
        if (current_task_has_deliverable_signal()) {
            return false;
        }
        if ((++spins & 0x3FU) == 0) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
    return false;
}

void release_proxy_pipe_rdma_writer(ProxyIpcState* proxy) {
    if (proxy != nullptr) {
        proxy->pipe_rdma_writer_active.store(false, std::memory_order_release);
    }
}

auto notify_proxy_pipe_rdma_data(ProxyIpcState* proxy) -> int {
    if (proxy == nullptr || proxy->pipe_rdma_transport == nullptr || proxy->pipe_rdma_transport->doorbell == nullptr) {
        return WKI_ERR_INVALID;
    }

    uint32_t const DOORBELL = WKI_DOORBELL_IPC_BASE | (proxy->resource_id & WKI_IPC_RESOURCE_MASK);
    int last_ret = WKI_ERR_TX_FAILED;
    bool any_ok = false;
    for (uint32_t attempt = 0; attempt < WKI_IPC_PIPE_RDMA_NOTIFY_BURST; ++attempt) {
        last_ret = proxy->pipe_rdma_transport->doorbell(proxy->pipe_rdma_transport, proxy->home_node, DOORBELL);
        if (last_ret != WKI_OK) {
            break;
        }
        any_ok = true;
    }
    return any_ok ? WKI_OK : last_ret;
}

auto proxy_pipe_write_rdma(ProxyIpcState* proxy, const uint8_t* src, size_t count, uint64_t callsite) -> ssize_t {
    if (proxy == nullptr || src == nullptr || count == 0 || !proxy->pipe_rdma_enabled || proxy->pipe_rdma_transport == nullptr ||
        proxy->pipe_rdma_rkey == 0 || proxy->pipe_rdma_capacity == 0) {
        return -EOPNOTSUPP;
    }

    if (!acquire_proxy_pipe_rdma_writer(proxy)) {
        return -EINTR;
    }

    auto release = [&]() { release_proxy_pipe_rdma_writer(proxy); };
    size_t sent = 0;
    uint32_t attempts = 0;

    while (sent < count && proxy->active.load(std::memory_order_acquire)) {
        uint32_t used = proxy->pipe_rdma_head - proxy->pipe_rdma_tail_cache;
        if (used >= proxy->pipe_rdma_capacity) {
            std::array<uint8_t, sizeof(WkiPipeSharedRegion)> header = {};
            int const READ_RET = proxy->pipe_rdma_transport->rdma_read(proxy->pipe_rdma_transport, proxy->home_node, proxy->pipe_rdma_rkey,
                                                                       0, header.data(), static_cast<uint32_t>(header.size()));
            if (READ_RET != WKI_OK) {
                release();
                return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EIO);
            }

            uint32_t remote_tail = 0;
            uint32_t remote_flags = 0;
            std::memcpy(&remote_tail, header.data() + offsetof(WkiPipeSharedRegion, tail), sizeof(remote_tail));
            std::memcpy(&remote_flags, header.data() + offsetof(WkiPipeSharedRegion, flags), sizeof(remote_flags));
            if ((remote_flags & WKI_PIPE_FLAG_READ_CLOSED) != 0U) {
                release();
                return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EPIPE);
            }
            proxy->pipe_rdma_tail_cache = remote_tail;
            used = proxy->pipe_rdma_head - proxy->pipe_rdma_tail_cache;
        }

        if (used >= proxy->pipe_rdma_capacity) {
            if (current_task_has_deliverable_signal()) {
                release();
                return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EINTR);
            }
            if (attempts < WKI_IPC_PIPE_RDMA_NOTIFY_RETRIES) {
                static_cast<void>(notify_proxy_pipe_rdma_data(proxy));
            }
            attempts++;
            ker::mod::sched::kern_sleep_us(ipc_pipe_send_retry_sleep_us(WKI_ERR_NO_CREDITS, attempts));
            continue;
        }

        attempts = 0;
        uint32_t const FREE = proxy->pipe_rdma_capacity - used;
        uint32_t const RING_HEAD = proxy->pipe_rdma_head % proxy->pipe_rdma_capacity;
        uint32_t const CONTIG = std::min(proxy->pipe_rdma_capacity - RING_HEAD, FREE);
        uint32_t const TO_SEND = static_cast<uint32_t>(std::min<size_t>(count - sent, CONTIG));
        uint64_t const DATA_OFFSET = sizeof(WkiPipeSharedRegion) + RING_HEAD;

        int const DATA_RET = proxy->pipe_rdma_transport->rdma_write(proxy->pipe_rdma_transport, proxy->home_node, proxy->pipe_rdma_rkey,
                                                                    DATA_OFFSET, src + sent, TO_SEND);
        if (DATA_RET != WKI_OK) {
            release();
            return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EIO);
        }

        uint32_t const NEW_HEAD = proxy->pipe_rdma_head + TO_SEND;
        int const HEAD_RET = proxy->pipe_rdma_transport->rdma_write(proxy->pipe_rdma_transport, proxy->home_node, proxy->pipe_rdma_rkey,
                                                                    offsetof(WkiPipeSharedRegion, head), &NEW_HEAD, sizeof(NEW_HEAD));
        if (HEAD_RET != WKI_OK) {
            release();
            return sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EIO);
        }

        proxy->pipe_rdma_head = NEW_HEAD;
        sent += TO_SEND;
        proxy->bytes_written.fetch_add(TO_SEND, std::memory_order_acq_rel);
        perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE, proxy->home_node, WKI_CHAN_IPC_DATA, 0, 0, TO_SEND, callsite);
        static_cast<void>(notify_proxy_pipe_rdma_data(proxy));
    }

    release();
    if (sent == 0 && !proxy->active.load(std::memory_order_acquire)) {
        return -EIO;
    }
    return static_cast<ssize_t>(sent);
}

auto proxy_pipe_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::PROXY_WRITE, proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_IPC_DATA,
                       WOS_PERF_CALLSITE());
    auto finish = [&](ssize_t rc, uint64_t bytes = 0) -> ssize_t {
        int32_t const STATUS = rc >= 0 ? 0 : static_cast<int32_t>(rc);
        trace.finish(STATUS, bytes);
        return rc;
    };
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (buf == nullptr && count != 0) {
        return finish(-EINVAL);
    }
    if (count == 0) {
        return finish(0);
    }

    if (proxy->pipe_rdma_enabled && proxy->pipe_rdma_transport != nullptr) {
        auto const RDMA_RET = proxy_pipe_write_rdma(proxy, static_cast<const uint8_t*>(buf), count, WOS_PERF_CALLSITE());
        if (RDMA_RET != -EOPNOTSUPP) {
            return finish(RDMA_RET, RDMA_RET > 0 ? static_cast<uint64_t>(RDMA_RET) : 0);
        }
    }

    // Send data via wire message to home node
    // Message format: [DevOpReqPayload(4B)] [resource_id:u32] [data...]
    constexpr size_t HEADER_SIZE = WKI_IPC_PIPE_DATA_HEADER_SIZE;
    constexpr size_t MAX_CHUNK = WKI_IPC_PIPE_DATA_MAX_CHUNK;
    size_t const MSG_SIZE = HEADER_SIZE + MAX_CHUNK;
    auto* msg = new (std::nothrow) uint8_t[MSG_SIZE];
    if (msg == nullptr) {
        return finish(-ENOMEM);
    }

    size_t sent = 0;
    const auto* src = static_cast<const uint8_t*>(buf);
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
    req->op_id = OP_PIPE_DATA;
    std::memcpy(msg + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    while (sent < count) {
        size_t const TO_SEND = std::min(count - sent, MAX_CHUNK);
        req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + TO_SEND);
        std::memcpy(msg + HEADER_SIZE, src + sent, TO_SEND);

        int ret = WKI_ERR_TX_FAILED;
        uint32_t attempts = 0;
        while (proxy->active.load(std::memory_order_acquire)) {
            ret = wki_send(proxy->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE + TO_SEND));
            if (ret == WKI_OK) {
                break;
            }
            if (current_task_has_deliverable_signal()) {
                delete[] msg;
                return finish(sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EINTR), sent);
            }
            pause_for_ipc_send_retry(ret, attempts++);
        }

        if (ret != WKI_OK) {
            delete[] msg;
            return finish(sent != 0 ? static_cast<ssize_t>(sent) : static_cast<ssize_t>(-EIO), sent);
        }

        sent += TO_SEND;
        proxy->bytes_written.fetch_add(TO_SEND, std::memory_order_acq_rel);
    }

    delete[] msg;
    return finish(static_cast<ssize_t>(sent), sent);
}

auto send_proxy_pipe_close(ProxyIpcState* proxy, const uint8_t* msg, uint16_t msg_size, uint32_t resource_id, uint16_t op_id) -> void {
    if (proxy == nullptr || msg == nullptr || msg_size == 0 || proxy->home_node == WKI_NODE_INVALID) {
        return;
    }

    bool const CAN_BLOCK = current_task_can_block_for_ipc_close();
    uint32_t const MAX_ATTEMPTS = CAN_BLOCK ? WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS : 1U;
    int ret = WKI_ERR_TX_FAILED;
    uint32_t attempts = 0;
    while (attempts < MAX_ATTEMPTS) {
        ret = wki_send(proxy->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg, msg_size);
        if (ret == WKI_OK) {
            return;
        }
        ++attempts;
        if (!CAN_BLOCK || attempts >= MAX_ATTEMPTS) {
            break;
        }
        pause_for_ipc_send_retry(ret, attempts);
    }

    if (enqueue_proxy_pipe_close_tx(proxy->home_node, msg, msg_size, resource_id, op_id)) {
        return;
    }

    ker::mod::dbg::log("[WKI] IPC proxy pipe close enqueue failed: resource_id=%u op=%u ret=%d attempts=%u can_block=%u", resource_id,
                       op_id, ret, attempts, CAN_BLOCK ? 1U : 0U);
}

auto proxy_pipe_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    // Send close to home node with resource_id
    uint16_t const OP = (f->fops != nullptr && f->fops->vfs_write == nullptr) ? OP_PIPE_CLOSE_READ : OP_PIPE_CLOSE_WRITE;

    constexpr size_t HEADER_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    constexpr size_t MAX_CLOSE_SIZE = HEADER_SIZE + sizeof(uint64_t);
    std::array<uint8_t, MAX_CLOSE_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    uint16_t msg_size = HEADER_SIZE;
    if (OP == OP_PIPE_CLOSE_WRITE) {
        uint64_t const EXPECTED_BYTES = proxy->bytes_written.load(std::memory_order_acquire);
        req->data_len = static_cast<uint16_t>(req->data_len + sizeof(EXPECTED_BYTES));
        std::memcpy(msg.data() + HEADER_SIZE, &EXPECTED_BYTES, sizeof(EXPECTED_BYTES));
        msg_size = static_cast<uint16_t>(msg_size + sizeof(EXPECTED_BYTES));
    }

    send_proxy_pipe_close(proxy, msg.data(), msg_size, proxy->resource_id, OP);

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

auto proxy_pipe_poll_check(ker::vfs::File* f, int events) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    int ready = 0;
    uint32_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
    uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
    uint32_t const AVAIL = HEAD - TAIL;
    bool const WR_CLOSED = proxy->write_closed.load(std::memory_order_acquire) != 0U;
    bool has_pending_data = false;

    uint64_t const PENDING_IRQF = s_ipc_lock.lock_irqsave();
    if (auto* pending = find_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id); pending != nullptr) {
        has_pending_data = !pending->chunks.empty();
    }
    s_ipc_lock.unlock_irqrestore(PENDING_IRQF);

    if (f->fops != nullptr && f->fops->vfs_write == nullptr) {
        // Read end
        if (((events & dev::pty::POLLIN) != 0) && (AVAIL > 0 || has_pending_data || WR_CLOSED)) {
            ready |= dev::pty::POLLIN;
        }
        if (WR_CLOSED && AVAIL == 0 && !has_pending_data) {
            ready |= dev::pty::POLLHUP;
        }
    } else {
        // Write end — always writable (sends via wire message)
        if ((events & dev::pty::POLLOUT) != 0) {
            ready |= dev::pty::POLLOUT;
        }
    }
    return ready;
}

auto proxy_pipe_poll_register_waiter(ker::vfs::File* f, uint64_t pid) -> bool {
    auto* proxy = (f != nullptr) ? static_cast<ProxyIpcState*>(f->private_data) : nullptr;
    return wki_ipc_proxy_register_poll_waiter(proxy, pid);
}

ker::vfs::FileOperations g_proxy_pipe_read_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pipe_close,
    .vfs_read = proxy_pipe_read,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = nullptr,
};

ker::vfs::FileOperations g_proxy_pipe_write_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pipe_close,
    .vfs_read = nullptr,
    .vfs_write = proxy_pipe_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = nullptr,
};

// ---- Server-side helpers ----

// ---- Proxy fops for epoll (IPC_EPOLL) ----
//
// The EpollInstance MUST be the first member of ProxyEpollFile so that
// static_cast<EpollInstance*>(file->private_data) works in kernel epoll_ctl /
// epoll_pwait (standard-layout guarantee: address of first member == address of
// the enclosing struct).

struct ProxyEpollFile {
    ker::vfs::EpollInstance inst{};  // MUST be first — addr(inst) == addr(ProxyEpollFile)
    uint32_t resource_id = 0;
    uint16_t home_node = WKI_NODE_INVALID;
};

static_assert(__builtin_offsetof(ProxyEpollFile, inst) == 0, "EpollInstance must be at offset 0 in ProxyEpollFile");

auto proxy_epoll_close(ker::vfs::File* f) -> int {
    auto* epf = reinterpret_cast<ProxyEpollFile*>(f->private_data);
    if (epf == nullptr) {
        return 0;
    }

    uint32_t resource_id = epf->resource_id;
    uint16_t const HOME_NODE = epf->home_node;
    f->private_data = nullptr;
    delete epf;

    ProxyIpcState* proxy = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_resource_id_locked(resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        if (proxy != nullptr) {
            proxy_release(proxy);
        }
        return 0;
    }

    // Send OP_PIPE_CLOSE_READ to home node — the server handler tears down the export
    // (sets exp->active = false, releases file ref) for any resource type.
    constexpr size_t CLOSE_MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::array<uint8_t, CLOSE_MSG_SIZE> close_msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(close_msg.data());
    req->op_id = OP_PIPE_CLOSE_READ;
    req->data_len = sizeof(uint32_t);
    std::memcpy(close_msg.data() + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
    if (HOME_NODE != WKI_NODE_INVALID) {
        wki_send(HOME_NODE, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, close_msg.data(), static_cast<uint16_t>(close_msg.size()));
    }

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

ker::vfs::FileOperations g_proxy_epoll_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_epoll_close,
    .vfs_read = nullptr,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

// ---- Proxy fops for PTY (IPC_PTY) ----
//
// Data plane: same ring-buffer / pump path as pipes.
// Control plane: ioctl forwarded to home node via OP_PTY_IOCTL.
// Close: OP_PTY_CLOSE fires and the export tears down.

constexpr size_t PTY_IOCTL_ARG_SIZE = 128;  // max bytes we send/receive for ioctl arg

auto proxy_pty_ioctl(ker::vfs::File* f, unsigned long cmd, unsigned long arg) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    auto finish = [&](int rc) -> int {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL), ker::mod::perf::WkiPerfPhase::END,
                              proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, rc, ELAPSED_US,
                              CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL),
                                proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, rc, ELAPSED_US);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::PTY_IOCTL), ker::mod::perf::WkiPerfPhase::BEGIN,
                          proxy != nullptr ? proxy->home_node : WKI_NODE_INVALID, WKI_CHAN_RESOURCE, CORRELATION, 0, 0, CALLSITE);
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return finish(-EBADF);
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        return finish(-EHOSTUNREACH);
    }

    // Wire layout (after resource_id): [cmd:u64][arg_val:u64][arg_in:128B]
    constexpr size_t EXTRA = sizeof(uint64_t) + sizeof(uint64_t) + PTY_IOCTL_ARG_SIZE;
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t) + EXTRA;
    std::array<uint8_t, MSG_SIZE> msg = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PTY_IOCTL;
    req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + EXTRA);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));

    auto cmd64 = static_cast<uint64_t>(cmd);
    auto arg64 = static_cast<uint64_t>(arg);
    size_t cursor = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::memcpy(msg.data() + cursor, &cmd64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);
    std::memcpy(msg.data() + cursor, &arg64, sizeof(uint64_t));
    cursor += sizeof(uint64_t);

    // If arg looks like a pointer, copy the pointed-to data into the request
    if (arg >= 8192) {
        std::memcpy(msg.data() + cursor, reinterpret_cast<const void*>(arg), PTY_IOCTL_ARG_SIZE);
    }

    WkiWaitEntry wait = {};
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait != nullptr) {
        proxy->lock.unlock_irqrestore(irqf);
        return finish(-EBUSY);
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_PTY_IOCTL;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    int const TX = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    if (TX != WKI_OK) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return finish(-EIO);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        return finish(-ETIMEDOUT);
    }

    irqf = proxy->lock.lock_irqsave();
    int const STATUS = proxy->pending_wait_status;
    // Copy response arg_out back to caller's pointer (output ioctls)
    if (arg >= 8192 && proxy->pending_wait_resp_len >= PTY_IOCTL_ARG_SIZE) {
        std::memcpy(reinterpret_cast<void*>(arg), static_cast<const void*>(proxy->pending_wait_resp), PTY_IOCTL_ARG_SIZE);
    }
    proxy->pending_wait = nullptr;
    proxy->lock.unlock_irqrestore(irqf);
    return finish(STATUS);
}

auto proxy_pty_isatty(ker::vfs::File* /*f*/) -> bool { return true; }

auto proxy_pty_close(ker::vfs::File* f) -> int {
    auto* proxy = static_cast<ProxyIpcState*>(f->private_data);
    if (proxy == nullptr) {
        return 0;
    }

    // Send OP_PTY_CLOSE to home node (no response expected)
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + sizeof(uint32_t);
    std::array<uint8_t, MSG_SIZE> msg = {};
    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_PTY_CLOSE;
    req->data_len = sizeof(uint32_t);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, sizeof(uint32_t));
    if (proxy->home_node != WKI_NODE_INVALID) {
        wki_send(proxy->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    }

    wki_ipc_detach_proxy_file(f, proxy);
    return 0;
}

ker::vfs::FileOperations g_proxy_pty_fops = {
    .vfs_open = nullptr,
    .vfs_close = proxy_pty_close,
    .vfs_read = proxy_pipe_read,    // reuse ring-buffer read
    .vfs_write = proxy_pipe_write,  // reuse OP_PIPE_DATA write
    .vfs_lseek = nullptr,
    .vfs_isatty = proxy_pty_isatty,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = proxy_pipe_poll_check,
    .vfs_poll_register_waiter = proxy_pipe_poll_register_waiter,
    .vfs_ioctl = proxy_pty_ioctl,
};

auto find_export_by_resource_id(uint32_t resource_id) -> WkiIpcExport* {
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->resource_id == resource_id) {
            return exp;
        }
    }
    return nullptr;
}

auto find_proxy_by_resource_id(uint32_t resource_id) -> ProxyIpcState* { return find_proxy_by_resource_id_locked(resource_id); }

auto allocate_ipc_resource_id() -> uint32_t { return g_next_ipc_resource_id++; }

auto export_pipe_rdma_header(WkiIpcExport* exp) -> WkiPipeSharedRegion* {
    if (exp == nullptr || !exp->pipe_rdma_enabled || exp->pipe_rdma_region == nullptr) {
        return nullptr;
    }
    return reinterpret_cast<WkiPipeSharedRegion*>(exp->pipe_rdma_region);
}

auto export_pipe_rdma_front_locked(WkiIpcExport* exp, uint32_t* tail_out, uint16_t* len_out, uint8_t** data_out) -> bool {
    if (tail_out == nullptr || len_out == nullptr || data_out == nullptr) {
        return false;
    }

    auto* header = export_pipe_rdma_header(exp);
    if (header == nullptr || exp->pipe_rdma_capacity == 0) {
        return false;
    }

    uint32_t const HEAD = header->head.load(std::memory_order_acquire);
    uint32_t const TAIL = header->tail.load(std::memory_order_relaxed);
    uint32_t used = HEAD - TAIL;
    if (used == 0) {
        return false;
    }
    used = std::min(used, exp->pipe_rdma_capacity);

    uint32_t const RING_TAIL = TAIL % exp->pipe_rdma_capacity;
    uint32_t const CONTIG = std::min(exp->pipe_rdma_capacity - RING_TAIL, used);
    *tail_out = TAIL;
    *len_out = static_cast<uint16_t>(std::min<uint32_t>(CONTIG, UINT16_MAX));
    *data_out = exp->pipe_rdma_region + sizeof(WkiPipeSharedRegion) + RING_TAIL;
    return *len_out != 0;
}

auto queue_export_pipe_rdma_flush(uint16_t src_node, uint32_t resource_id) -> bool {
    size_t shard = 0;
    bool wake = false;
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    auto* exp = find_export_by_resource_id(resource_id);
    if (exp != nullptr && exp->active && exp->consumer_node == src_node && exp->pipe_rdma_enabled) {
        auto* backlog = find_export_pipe_write_backlog_locked(exp);
        if (backlog != nullptr) {
            queue_export_pipe_write_flush_locked(backlog);
            shard = export_pipe_write_backlog_shard(backlog);
            wake = true;
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (wake) {
        wake_export_pipe_write_flush_worker(shard);
    }
    return wake;
}

void setup_export_pipe_rdma(WkiIpcExport* exp, uint16_t target_node, WkiIpcFdEntry& entry) {
    if (!WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED || exp == nullptr || exp->res_type != ResourceType::IPC_PIPE ||
        exp->resource_id > WKI_IPC_RESOURCE_MASK) {
        return;
    }

    auto* transport = ipc_pipe_rdma_transport_for_peer(target_node);
    if (transport == nullptr) {
        return;
    }

    uint64_t irqf = s_ipc_lock.lock_irqsave();
    auto* backlog = ensure_export_pipe_write_backlog_locked(exp);
    if (backlog == nullptr) {
        s_ipc_lock.unlock_irqrestore(irqf);
        return;
    }
    s_ipc_lock.unlock_irqrestore(irqf);

    auto* region = new (std::nothrow) uint8_t[WKI_IPC_PIPE_RDMA_TOTAL_SIZE];
    if (region == nullptr) {
        irqf = s_ipc_lock.lock_irqsave();
        erase_export_pipe_write_backlog_locked(backlog);
        s_ipc_lock.unlock_irqrestore(irqf);
        free_export_pipe_write_backlog(backlog);
        return;
    }
    std::fill_n(region, WKI_IPC_PIPE_RDMA_TOTAL_SIZE, uint8_t{0});

    auto* header = reinterpret_cast<WkiPipeSharedRegion*>(region);
    header->head.store(0, std::memory_order_relaxed);
    header->tail.store(0, std::memory_order_relaxed);
    header->credits.store(WKI_IPC_PIPE_RDMA_CAPACITY, std::memory_order_relaxed);
    header->capacity = WKI_IPC_PIPE_RDMA_CAPACITY;
    header->flags = 0;

    uint32_t rkey = 0;
    int const REG_RET = transport->rdma_register_region(transport, reinterpret_cast<uint64_t>(region), WKI_IPC_PIPE_RDMA_TOTAL_SIZE, &rkey);
    if (REG_RET != WKI_OK || rkey == 0) {
        irqf = s_ipc_lock.lock_irqsave();
        erase_export_pipe_write_backlog_locked(backlog);
        s_ipc_lock.unlock_irqrestore(irqf);
        free_export_pipe_write_backlog(backlog);
        delete[] region;
        return;
    }

    size_t shard = 0;
    irqf = s_ipc_lock.lock_irqsave();
    exp->pipe_rdma_enabled = true;
    exp->pipe_rdma_region = region;
    exp->pipe_rdma_region_size = WKI_IPC_PIPE_RDMA_TOTAL_SIZE;
    exp->pipe_rdma_capacity = WKI_IPC_PIPE_RDMA_CAPACITY;
    exp->pipe_rdma_rkey = rkey;
    exp->pipe_rdma_transport = transport;

    backlog->persistent_for_rdma = true;
    queue_export_pipe_write_flush_locked(backlog);
    shard = export_pipe_write_backlog_shard(backlog);
    s_ipc_lock.unlock_irqrestore(irqf);

    wake_export_pipe_write_flush_worker(shard);
    entry.rdma_rkey = rkey;
    entry.rdma_offset = WKI_IPC_PIPE_RDMA_CAPACITY;
}

auto proxy_ring_used_bytes(const ProxyIpcState* proxy) -> uint64_t {
    if (proxy == nullptr || proxy->ring_buf == nullptr || proxy->ring_capacity == 0) {
        return 0;
    }

    uint64_t const HEAD = proxy->ring_head.load(std::memory_order_acquire);
    uint64_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
    uint64_t const USED = HEAD >= TAIL ? HEAD - TAIL : 0;
    return std::min<uint64_t>(USED, proxy->ring_capacity);
}

// =============================================================================
// Server-side pipe pump: reads from real pipe, sends data via wire messages
// =============================================================================

struct PipePumpArg {
    std::atomic<WkiIpcExport*> exp{nullptr};
    ker::mod::sched::task::Task* worker = nullptr;
};

constexpr int MAX_PUMPS = 32;
std::array<PipePumpArg, MAX_PUMPS> g_pump_args;
WkiIpcExport* g_pipe_pump_queue_head = nullptr;
WkiIpcExport* g_pipe_pump_queue_tail = nullptr;

void remove_pipe_pump_queue_locked(WkiIpcExport* exp) {
    if (exp == nullptr || !exp->pump_queued) {
        return;
    }

    WkiIpcExport* prev = nullptr;
    auto* cur = g_pipe_pump_queue_head;
    while (cur != nullptr) {
        auto* next = cur->pump_next;
        if (cur == exp) {
            if (prev != nullptr) {
                prev->pump_next = next;
            } else {
                g_pipe_pump_queue_head = next;
            }
            if (g_pipe_pump_queue_tail == exp) {
                g_pipe_pump_queue_tail = prev;
            }
            exp->pump_queued = false;
            exp->pump_next = nullptr;
            return;
        }
        prev = cur;
        cur = next;
    }

    exp->pump_queued = false;
    exp->pump_next = nullptr;
}

void enqueue_pipe_pump_locked(WkiIpcExport* exp) {
    if (exp == nullptr || exp->pump_queued) {
        return;
    }

    exp->pump_queued = true;
    exp->pump_next = nullptr;
    if (g_pipe_pump_queue_tail != nullptr) {
        g_pipe_pump_queue_tail->pump_next = exp;
    } else {
        g_pipe_pump_queue_head = exp;
    }
    g_pipe_pump_queue_tail = exp;
}

auto pop_pipe_pump_queue_locked() -> WkiIpcExport* {
    while (g_pipe_pump_queue_head != nullptr) {
        auto* exp = g_pipe_pump_queue_head;
        g_pipe_pump_queue_head = exp->pump_next;
        if (g_pipe_pump_queue_tail == exp) {
            g_pipe_pump_queue_tail = nullptr;
        }
        exp->pump_queued = false;
        exp->pump_next = nullptr;

        if (exp->active && exp->file != nullptr) {
            return exp;
        }
        exp->pump_running.store(false, std::memory_order_release);
        exp->pump_task = nullptr;
    }

    return nullptr;
}

auto assign_next_pipe_pump_locked(PipePumpArg& pump_arg) -> WkiIpcExport* {
    if (pump_arg.worker == nullptr || pump_arg.exp.load(std::memory_order_acquire) != nullptr) {
        return nullptr;
    }

    auto* next = pop_pipe_pump_queue_locked();
    if (next == nullptr) {
        return nullptr;
    }

    next->pump_running.store(true, std::memory_order_release);
    next->pump_task = pump_arg.worker;
    pump_arg.exp.store(next, std::memory_order_release);
    return next;
}

void release_pipe_pump_slot_locked(PipePumpArg& pump_arg, WkiIpcExport* exp) {
    if (exp != nullptr && pump_arg.exp.load(std::memory_order_acquire) == exp) {
        exp->pump_running.store(false, std::memory_order_release);
        exp->pump_task = nullptr;
        pump_arg.exp.store(nullptr, std::memory_order_release);
    }

    static_cast<void>(assign_next_pipe_pump_locked(pump_arg));
}

auto stop_pipe_pump_locked(WkiIpcExport* exp) -> ker::mod::sched::task::Task* {
    if (exp == nullptr) {
        return nullptr;
    }
    remove_pipe_pump_queue_locked(exp);
    exp->pump_running.store(false, std::memory_order_release);
    return exp->pump_task;
}

void wake_pipe_pump(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        ker::mod::sched::kern_wake(task);
    }
}

auto register_poll_read_waiter(ker::vfs::File* file, bool* ready_now) -> bool {
    constexpr int POLL_READ_EVENTS = 0x0001 | 0x0010;  // POLLIN | POLLHUP
    if (ready_now != nullptr) {
        *ready_now = false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || file == nullptr || file->fops == nullptr || file->fops->vfs_poll_register_waiter == nullptr) {
        return false;
    }
    task->wait_channel = "wki_pipe_pump_poll";
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_READ_EVENTS) & POLL_READ_EVENTS) != 0;
    }
    return true;
}

auto pipe_pump_read_ready(ker::vfs::File* file) -> bool {
    constexpr int POLL_READ_EVENTS = 0x0001 | 0x0010;  // POLLIN | POLLHUP
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_poll_check == nullptr) {
        return true;
    }

    if ((file->fops->vfs_poll_check(file, POLL_READ_EVENTS) & POLL_READ_EVENTS) != 0) {
        return true;
    }

    bool ready_now = false;
    if (!register_poll_read_waiter(file, &ready_now)) {
        return true;
    }
    if (ready_now) {
        return true;
    }

    ker::mod::sched::kern_block();
    return false;
}

auto register_poll_write_waiter(ker::vfs::File* file, bool* ready_now) -> bool {
    constexpr int POLL_WRITE_EVENTS = dev::pty::POLLOUT | dev::pty::POLLERR;
    if (ready_now != nullptr) {
        *ready_now = false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || file == nullptr || file->fops == nullptr || file->fops->vfs_poll_register_waiter == nullptr) {
        return false;
    }
    task->wait_channel = "wki_pipe_write_poll";
    if (!file->fops->vfs_poll_register_waiter(file, task->pid)) {
        return false;
    }
    if (ready_now != nullptr && file->fops->vfs_poll_check != nullptr) {
        *ready_now = (file->fops->vfs_poll_check(file, POLL_WRITE_EVENTS) & POLL_WRITE_EVENTS) != 0;
    }
    return true;
}

template <int SLOT>
[[noreturn]] void pipe_pump_thread_fn() {
    auto* arg = &std::get<SLOT>(g_pump_args);

    for (;;) {
        auto* exp = arg->exp.load(std::memory_order_acquire);
        if (exp == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        ker::vfs::File* file = nullptr;
        uint16_t target = WKI_NODE_INVALID;
        uint32_t resource_id = 0;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (arg->exp.load(std::memory_order_acquire) != exp) {
                s_ipc_lock.unlock_irqrestore(IRQF);
                continue;
            }
            if (!exp->active || exp->file == nullptr) {
                release_pipe_pump_slot_locked(*arg, exp);
                s_ipc_lock.unlock_irqrestore(IRQF);
                continue;
            }
            file = exp->file;
            file->refcount.fetch_add(1, std::memory_order_acq_rel);
            target = exp->consumer_node;
            resource_id = exp->resource_id;
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        constexpr size_t HEADER_SIZE = WKI_IPC_PIPE_DATA_HEADER_SIZE;
        constexpr size_t BUF_SIZE = WKI_IPC_PIPE_DATA_MAX_CHUNK;
        auto* msg = new (std::nothrow) uint8_t[HEADER_SIZE + BUF_SIZE];
        if (msg == nullptr) {
            delete[] msg;
            ipc_release_file_ref(file);
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            release_pipe_pump_slot_locked(*arg, exp);
            s_ipc_lock.unlock_irqrestore(IRQF);
            ker::mod::dbg::log("[WKI] IPC pump: malloc failed for resource_id=%u", resource_id);
            continue;
        }
#ifdef DEBUG_WKI_IPC
        ker::mod::dbg::log("[WKI] IPC pipe pump started: resource_id=%u target=0x%04x", resource_id, target);
#endif
        while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
            if (!pipe_pump_read_ready(file)) {
                continue;
            }
            if (!exp->active || !exp->pump_running.load(std::memory_order_acquire)) {
                break;
            }

            IpcPerfTrace read_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_PUMP_READ, target, WKI_CHAN_RESOURCE, WOS_PERF_CALLSITE());
            ssize_t n = 0;
            if (file != nullptr && file->fops != nullptr && file->fops->vfs_read != nullptr) {
                n = clamp_io_count(file->fops->vfs_read(file, msg + HEADER_SIZE, BUF_SIZE, 0), BUF_SIZE);
            } else {
                break;
            }
            read_trace.finish(n >= 0 ? 0 : static_cast<int32_t>(n), n > 0 ? static_cast<uint64_t>(n) : 0);

            if (n > 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
                req->op_id = OP_PIPE_DATA;
                req->data_len = static_cast<uint16_t>(sizeof(uint32_t) + n);
                std::memcpy(msg + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));

                IpcPerfTrace send_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_PUMP_SEND, target, WKI_CHAN_IPC_DATA, WOS_PERF_CALLSITE(),
                                        static_cast<uint32_t>(n));
                int ret = WKI_ERR_TX_FAILED;
                uint32_t attempts = 0;
                while (exp->active && exp->pump_running.load(std::memory_order_acquire)) {
                    ret = wki_send(target, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE + n));
                    if (ret == WKI_OK) {
                        break;
                    }
                    pause_for_ipc_send_retry(ret, attempts++);
                }
                // NOLINTNEXTLINE(readability-suspicious-call-argument): ret is status; attempts is retry count.
                send_trace.finish(ret, static_cast<uint64_t>(n), attempts);
                if (ret != WKI_OK) {
                    ker::mod::dbg::logger<"wki">::warn("IPC pipe pump DATA send failed: resource_id=%u ret=%d len=%ld", resource_id, ret,
                                                       n);
                    break;
                }
            } else if (n == 0) {
                auto* req = reinterpret_cast<DevOpReqPayload*>(msg);
                req->op_id = OP_PIPE_CLOSE_WRITE;
                req->data_len = sizeof(uint32_t);
                std::memcpy(msg + sizeof(DevOpReqPayload), &resource_id, sizeof(uint32_t));
                int ret = WKI_ERR_TX_FAILED;
                uint32_t retries = 0;
                uint32_t attempts = 0;
                while (exp->active && (exp->pump_running.load(std::memory_order_acquire) || attempts == 0) &&
                       attempts < WKI_IPC_PIPE_EOF_MAX_SEND_ATTEMPTS) {
                    attempts++;
                    ret = wki_send(target, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, msg, static_cast<uint16_t>(HEADER_SIZE));
                    if (ret == WKI_OK) {
                        break;
                    }
                    retries++;
                    pause_for_ipc_send_retry(ret, attempts);
                }
                if (ret != WKI_OK) {
                    if (attempts != 0 && exp->active) {
                        ker::mod::dbg::logger<"wki">::warn("IPC pipe pump EOF send failed: resource_id=%u ret=%d attempts=%u retries=%u",
                                                           resource_id, ret, attempts, retries);
                    }
                }
#ifdef WKI_IPC_DEBUG
                else {
                    ker::mod::dbg::log("[WKI] IPC pipe pump EOF: resource_id=%u retries=%u", resource_id, retries);
                }
#endif
                break;
            } else if (n == -EAGAIN) {
                bool ready_now = false;
                if (register_poll_read_waiter(file, &ready_now)) {
                    if (!ready_now) {
                        ker::mod::sched::kern_block();
                    }
                } else {
                    ker::mod::sched::kern_sleep_us(1000);
                }
                continue;
            } else {
                ker::mod::dbg::logger<"wki">::warn("IPC pipe pump read error: resource_id=%u err=%ld", resource_id, n);
                break;
            }
        }

        delete[] msg;
        ipc_release_file_ref(file);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        release_pipe_pump_slot_locked(*arg, exp);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
}

using PumpFn = void (*)();
constexpr std::array<PumpFn, MAX_PUMPS> G_PUMP_FNS = {
    pipe_pump_thread_fn<0>,  pipe_pump_thread_fn<1>,  pipe_pump_thread_fn<2>,  pipe_pump_thread_fn<3>,  pipe_pump_thread_fn<4>,
    pipe_pump_thread_fn<5>,  pipe_pump_thread_fn<6>,  pipe_pump_thread_fn<7>,  pipe_pump_thread_fn<8>,  pipe_pump_thread_fn<9>,
    pipe_pump_thread_fn<10>, pipe_pump_thread_fn<11>, pipe_pump_thread_fn<12>, pipe_pump_thread_fn<13>, pipe_pump_thread_fn<14>,
    pipe_pump_thread_fn<15>, pipe_pump_thread_fn<16>, pipe_pump_thread_fn<17>, pipe_pump_thread_fn<18>, pipe_pump_thread_fn<19>,
    pipe_pump_thread_fn<20>, pipe_pump_thread_fn<21>, pipe_pump_thread_fn<22>, pipe_pump_thread_fn<23>, pipe_pump_thread_fn<24>,
    pipe_pump_thread_fn<25>, pipe_pump_thread_fn<26>, pipe_pump_thread_fn<27>, pipe_pump_thread_fn<28>, pipe_pump_thread_fn<29>,
    pipe_pump_thread_fn<30>, pipe_pump_thread_fn<31>,
};

auto create_pipe_pump_worker(PipePumpArg& pump_arg, PumpFn pump_fn) -> ker::mod::sched::task::Task* {
    if (pump_arg.worker != nullptr || pump_fn == nullptr) {
        return pump_arg.worker;
    }

    auto* task = ker::mod::sched::task::Task::create_kernel_thread("wki_pipe_pump", pump_fn);
    if (task == nullptr) {
        ker::mod::dbg::log("[WKI] IPC pump: failed to create kernel thread");
        return nullptr;
    }

    promote_latency_sensitive_daemon(task);
    pump_arg.worker = task;
    ker::mod::sched::post_task_balanced(task);
    return task;
}

void init_pipe_pump_workers() {
    const auto* pump_fn_it = G_PUMP_FNS.begin();
    for (auto& pump_arg : g_pump_args) {
        static_cast<void>(create_pipe_pump_worker(pump_arg, *pump_fn_it));
        ++pump_fn_it;
    }
}

void start_pipe_pump(WkiIpcExport* exp) {
    if (exp == nullptr) {
        return;
    }

    PipePumpArg* slot_arg = nullptr;
    PumpFn pump_fn = nullptr;
    ker::mod::sched::task::Task* task = nullptr;
    const auto* pump_fn_it = G_PUMP_FNS.begin();
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        if (!exp->active || exp->file == nullptr || exp->pump_queued || exp->pump_running.load(std::memory_order_acquire)) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }

        for (auto& pump_arg : g_pump_args) {
            if (pump_arg.exp.load(std::memory_order_acquire) == nullptr) {
                slot_arg = &pump_arg;
                pump_fn = *pump_fn_it;
                task = pump_arg.worker;
                exp->pump_running.store(true, std::memory_order_release);
                exp->pump_task = task;
                pump_arg.exp.store(exp, std::memory_order_release);
                break;
            }
            ++pump_fn_it;
        }

        if (slot_arg == nullptr || pump_fn == nullptr) {
            enqueue_pipe_pump_locked(exp);
#ifdef WKI_IPC_DEBUG
            ker::mod::dbg::log("[WKI] IPC pump queued: resource_id=%u", exp->resource_id);
#endif
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    if (task == nullptr) {
        task = create_pipe_pump_worker(*slot_arg, pump_fn);
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        if (task == nullptr) {
            if (slot_arg->exp.load(std::memory_order_acquire) == exp) {
                exp->pump_task = nullptr;
                slot_arg->exp.store(nullptr, std::memory_order_release);
                if (exp->active && exp->file != nullptr) {
                    exp->pump_running.store(false, std::memory_order_release);
                    enqueue_pipe_pump_locked(exp);
                } else {
                    exp->pump_running.store(false, std::memory_order_release);
                }
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
            return;
        }
        if (slot_arg->exp.load(std::memory_order_acquire) == exp && exp->active && exp->pump_running.load(std::memory_order_acquire)) {
            exp->pump_task = task;
        } else {
            release_pipe_pump_slot_locked(*slot_arg, exp);
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    ker::mod::sched::kern_wake(task);
}

void free_ipc_dev_op_work(IpcDevOpWork* work) {
    if (work == nullptr) {
        return;
    }

    delete[] work->payload;

    delete work;
}

auto ipc_dev_op_expects_response(uint16_t op_id) -> bool {
    return op_id == OP_PIPE_POLL_STATE || op_id == OP_EPOLL_CTL || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL ||
           (op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT);
}

auto ipc_dev_op_is_close(uint16_t op_id) -> bool {
    return op_id == OP_PIPE_CLOSE_READ || op_id == OP_PIPE_CLOSE_WRITE || op_id == OP_PTY_CLOSE || op_id == OP_SOCK_CLOSE;
}

auto ipc_dev_op_must_not_drop(uint16_t op_id) -> bool {
    // OP_PIPE_DATA has no response path; once WKI accepts it, dropping the
    // deferred work item silently corrupts the byte stream.
    return op_id == OP_PIPE_DATA || ipc_dev_op_is_close(op_id);
}

void send_ipc_dev_op_error_response(const WkiHeader* hdr, uint16_t op_id, uint32_t resource_id, int16_t status) {
    if (hdr == nullptr || !ipc_dev_op_expects_response(op_id)) {
        return;
    }

    DevOpRespPayload resp = {};
    resp.op_id = op_id;
    resp.status = status;
    resp.data_len = sizeof(uint32_t);

    std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> resp_buf = {};
    std::memcpy(resp_buf.data(), &resp, sizeof(resp));
    std::memcpy(resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));
    wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, resp_buf.data(), static_cast<uint16_t>(resp_buf.size()));
}

auto should_defer_ipc_dev_op(uint16_t op_id, uint32_t resource_id, uint16_t src_node) -> bool {
    if (op_id == OP_PIPE_DATA) {
        bool has_proxy = false;
        ProxyIpcState* proxy = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_resource_id_locked(resource_id);
        if (proxy != nullptr) {
            has_proxy = true;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            proxy_release(proxy);
        }

        // Proxy ring delivery is bounded and wakes local readers. Export-side
        // writes can enter arbitrary file/socket/PTY ops, so defer them.
        return !has_proxy;
    }

    if (op_id == OP_PIPE_POLL_STATE || op_id == OP_FUTEX_WAKE || op_id == OP_PTY_IOCTL || op_id == OP_PTY_CLOSE ||
        op_id == OP_PIPE_CLOSE_READ || op_id == OP_PIPE_CLOSE_WRITE || op_id == OP_SOCK_CLOSE) {
        (void)src_node;
        return true;
    }

    if (op_id == OP_EPOLL_CTL || (op_id >= OP_SOCK_ACCEPT && op_id <= OP_SOCK_SETSOCKOPT)) {
        (void)src_node;
        // Keep synchronous epoll/socket control ops on the immediate RX path.
        // The deferred IPC worker is a kernel daemon with an empty fd table, so
        // forwarded epoll_ctl() loses the export task's fd namespace there. The
        // socket control tests also rely on prompt request/response ordering for
        // short-lived exported sockets during exec/exit.
        return false;
    }

    return false;
}

auto ipc_dev_op_worker_index(uint32_t resource_id) -> size_t { return resource_id % WKI_IPC_DEV_OP_WORKER_COUNT; }

auto ipc_dev_op_queued_count_locked() -> size_t {
    size_t queued = 0;
    for (const auto& queue : g_ipc_dev_op_queues) {
        queued += queue.size();
    }
    return queued;
}

auto enqueue_ipc_dev_op_work(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, uint16_t op_id, uint32_t resource_id)
    -> bool {
    uint32_t const CORRELATION = hdr != nullptr ? static_cast<uint32_t>(hdr->seq_num & UINT16_MAX) : 0U;
    auto* work = new (std::nothrow) IpcDevOpWork();
    auto* payload_copy = new (std::nothrow) uint8_t[payload_len];
    if (work == nullptr || payload_copy == nullptr) {
        delete[] payload_copy;

        delete work;

        send_ipc_dev_op_error_response(hdr, op_id, resource_id, -ENOMEM);
        return false;
    }

    work->hdr = *hdr;
    work->payload = payload_copy;
    work->payload_len = payload_len;
    std::memcpy(work->payload, payload, payload_len);

    ker::mod::sched::task::Task* worker = nullptr;
    bool queued = false;
    size_t const WORKER_INDEX = ipc_dev_op_worker_index(resource_id);
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    size_t const QUEUED_TOTAL = ipc_dev_op_queued_count_locked();
    if (QUEUED_TOTAL < WKI_IPC_DEV_OP_MAX_PENDING || ipc_dev_op_must_not_drop(op_id)) {
        auto& queue = g_ipc_dev_op_queues.at(WORKER_INDEX);
        queue.push_back(work);
        worker = g_ipc_dev_op_worker_tasks.at(WORKER_INDEX);
        queued = true;
        perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::DEV_OP_QUEUE, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID,
                              hdr != nullptr ? hdr->channel_id : WKI_CHAN_RESOURCE, CORRELATION, 0, static_cast<uint32_t>(queue.size()),
                              WOS_PERF_CALLSITE());
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (!queued) {
        free_ipc_dev_op_work(work);
        send_ipc_dev_op_error_response(hdr, op_id, resource_id, -EAGAIN);
        return false;
    }

    if (worker != nullptr) {
        ker::mod::sched::kern_wake(worker);
    }
    return true;
}

[[noreturn]] void ipc_dev_op_worker_thread_fn(size_t worker_index) {
    for (;;) {
        IpcDevOpWork* work = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            auto& queue = g_ipc_dev_op_queues.at(worker_index);
            if (!queue.empty()) {
                work = queue.front();
                queue.pop_front();
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        if (work == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        auto const CORRELATION = static_cast<uint32_t>(work->hdr.seq_num & UINT16_MAX);
        IpcPerfTrace trace(ker::mod::perf::WkiPerfIpcOp::DEV_OP_HANDLE, work->hdr.src_node, work->hdr.channel_id, WOS_PERF_CALLSITE(), 0,
                           CORRELATION);
        handle_ipc_dev_op_req_inline(&work->hdr, work->payload, work->payload_len);
        trace.finish(0);
        free_ipc_dev_op_work(work);
    }
}

[[noreturn]] void ipc_dev_op_worker_thread_0() { ipc_dev_op_worker_thread_fn(0); }
[[noreturn]] void ipc_dev_op_worker_thread_1() { ipc_dev_op_worker_thread_fn(1); }
[[noreturn]] void ipc_dev_op_worker_thread_2() { ipc_dev_op_worker_thread_fn(2); }
[[noreturn]] void ipc_dev_op_worker_thread_3() { ipc_dev_op_worker_thread_fn(3); }

[[noreturn]] void proxy_pipe_close_tx_thread_fn() {
    for (;;) {
        PendingProxyPipeClose* pending = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            if (!g_pending_proxy_pipe_closes.empty()) {
                pending = g_pending_proxy_pipe_closes.front();
                g_pending_proxy_pipe_closes.pop_front();
            }
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        if (pending == nullptr) {
            ker::mod::sched::kern_block();
            continue;
        }

        int const RET = wki_send(pending->home_node, WKI_CHAN_IPC_DATA, MsgType::DEV_OP_REQ, pending->msg.data(), pending->msg_size);
        if (RET == WKI_OK) {
            free_pending_proxy_pipe_close(pending);
            continue;
        }

        pending->attempts++;
        if (proxy_pipe_close_tx_terminal(RET)) {
            ker::mod::dbg::log("[WKI] IPC proxy pipe close dropped: resource_id=%u op=%u ret=%d attempts=%u", pending->resource_id,
                               pending->op_id, RET, pending->attempts);
            free_pending_proxy_pipe_close(pending);
            continue;
        }

        uint64_t const SLEEP_US = ipc_pipe_send_retry_sleep_us(RET, pending->attempts);
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            g_pending_proxy_pipe_closes.push_back(pending);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        ker::mod::sched::kern_sleep_us(SLEEP_US);
    }
}

}  // namespace

// =============================================================================
// Public API: Initialization
// =============================================================================

void wki_ipc_subsystem_init() {
    if (g_ipc_initialized) {
        return;
    }
    g_ipc_initialized = true;
    init_pipe_pump_workers();
    using WorkerEntry = void (*)();
    constexpr std::array<WorkerEntry, WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_COUNT> FLUSH_WORKER_ENTRIES = {
        export_pipe_write_flush_thread_0,
        export_pipe_write_flush_thread_1,
        export_pipe_write_flush_thread_2,
        export_pipe_write_flush_thread_3,
    };
    for (size_t i = 0; i < FLUSH_WORKER_ENTRIES.size(); ++i) {
        g_export_pipe_write_flush_tasks.at(i) =
            ker::mod::sched::task::Task::create_kernel_thread(WKI_IPC_EXPORT_PIPE_FLUSH_WORKER_NAMES.at(i), FLUSH_WORKER_ENTRIES.at(i));
        if (g_export_pipe_write_flush_tasks.at(i) != nullptr) {
            promote_latency_sensitive_daemon(g_export_pipe_write_flush_tasks.at(i));
            ker::mod::sched::post_task_balanced(g_export_pipe_write_flush_tasks.at(i));
        } else {
            ker::mod::dbg::log("[WKI] IPC export pipe writer thread creation failed: shard=%u", static_cast<unsigned>(i));
        }
    }

    constexpr std::array<WorkerEntry, WKI_IPC_DEV_OP_WORKER_COUNT> DEV_OP_WORKER_ENTRIES = {
        ipc_dev_op_worker_thread_0,
        ipc_dev_op_worker_thread_1,
        ipc_dev_op_worker_thread_2,
        ipc_dev_op_worker_thread_3,
    };
    constexpr std::array<const char*, WKI_IPC_DEV_OP_WORKER_COUNT> DEV_OP_WORKER_NAMES = {
        "wki_ipc_dev0",
        "wki_ipc_dev1",
        "wki_ipc_dev2",
        "wki_ipc_dev3",
    };
    for (size_t i = 0; i < WKI_IPC_DEV_OP_WORKER_COUNT; ++i) {
        auto* worker = ker::mod::sched::task::Task::create_kernel_thread(DEV_OP_WORKER_NAMES.at(i), DEV_OP_WORKER_ENTRIES.at(i));
        g_ipc_dev_op_worker_tasks.at(i) = worker;
        if (worker != nullptr) {
            promote_latency_sensitive_daemon(worker);
            ker::mod::sched::post_task_balanced(worker);
        } else {
            ker::mod::dbg::log("[WKI] IPC dev-op worker thread creation failed: index=%u", static_cast<unsigned>(i));
        }
    }
    g_proxy_pipe_close_tx_task = ker::mod::sched::task::Task::create_kernel_thread("wki_ipc_close_tx", proxy_pipe_close_tx_thread_fn);
    if (g_proxy_pipe_close_tx_task != nullptr) {
        promote_latency_sensitive_daemon(g_proxy_pipe_close_tx_task);
        ker::mod::sched::post_task_balanced(g_proxy_pipe_close_tx_task);
    } else {
        ker::mod::dbg::log("[WKI] IPC close-tx worker thread creation failed");
    }
    ker::mod::dbg::log("[WKI] IPC proxy subsystem initialized");
}

void wki_ipc_cleanup_exported_fds(const WkiIpcFdEntry* map, uint16_t count, uint16_t consumer_node) {
    if (map == nullptr || count == 0 || consumer_node == WKI_NODE_INVALID) {
        return;
    }

    std::array<ker::mod::sched::task::Task*, WKI_IPC_MAX_EXPORTS> stopped_pumps = {};
    std::array<ker::vfs::File*, WKI_IPC_MAX_EXPORTS> released_files = {};
    size_t stopped_pump_count = 0;
    size_t released_file_count = 0;
    bool wake_export_pipe_flush = false;

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    for (uint16_t i = 0; i < count; ++i) {
        auto const& entry = map[i];
        auto* exp = find_export_by_resource_id(entry.resource_id);
        if (exp == nullptr || !exp->active || exp->consumer_node != consumer_node) {
            continue;
        }

        if (auto* pump_task = stop_pipe_pump_locked(exp); pump_task != nullptr && stopped_pump_count < stopped_pumps.size()) {
            stopped_pumps.at(stopped_pump_count++) = pump_task;
        }

        if (exp->file != nullptr && released_file_count < released_files.size()) {
            released_files.at(released_file_count++) = exp->file;
        }
        exp->file = nullptr;
        exp->active = false;

        for (auto* backlog : g_export_pipe_write_backlogs) {
            if (backlog != nullptr && backlog->exp == exp) {
                backlog->exp = nullptr;
                queue_export_pipe_write_flush_locked(backlog);
                wake_export_pipe_flush = true;
            }
        }
    }
    compact_inactive_exports_locked();
    s_ipc_lock.unlock_irqrestore(IRQF);

    for (auto* pump : std::span(stopped_pumps.data(), stopped_pump_count)) {
        wake_pipe_pump(pump);
    }

    if (wake_export_pipe_flush) {
        wake_all_export_pipe_write_flush_workers();
    }

    for (auto* file : std::span(released_files.data(), released_file_count)) {
        ipc_release_file_ref(file);
    }
}

void wki_ipc_get_perf_snapshot(WkiIpcPerfSnapshot& out) {
    WkiIpcPerfSnapshot snapshot{};

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();

    for (const auto* exp : g_ipc_exports) {
        if (exp == nullptr || !exp->active) {
            continue;
        }
        snapshot.exports++;
        if (exp->pump_running.load(std::memory_order_acquire)) {
            snapshot.pump_tasks++;
        }
    }

    for (auto* proxy : g_ipc_proxies) {
        if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
            continue;
        }
        snapshot.proxies++;
        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        if (proxy->ring_buf != nullptr) {
            snapshot.proxy_ring_bytes += proxy->ring_capacity;
            snapshot.proxy_ring_used_bytes += proxy_ring_used_bytes(proxy);
        }
        if (proxy->blocked_reader.load(std::memory_order_acquire) != nullptr) {
            snapshot.blocked_readers++;
        }
        snapshot.poll_waiters += proxy->poll_waiters.size();
        proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }

    for (const auto* pending : g_pending_pipe_deliveries) {
        if (pending == nullptr) {
            continue;
        }
        snapshot.pending_deliveries++;
        snapshot.pending_bytes += pending->buffered_bytes;
        snapshot.pending_chunks += pending->chunks.size();
    }

    for (const auto* backlog : g_export_pipe_write_backlogs) {
        if (backlog == nullptr) {
            continue;
        }
        snapshot.export_backlogs++;
        snapshot.export_backlog_bytes += backlog->buffered_bytes;
        snapshot.export_backlog_chunks += backlog->chunks.size();
    }

    snapshot.export_flush_queue = export_pipe_write_flush_queue_depth_locked();
    for (const auto& queue : g_ipc_dev_op_queues) {
        snapshot.dev_op_queue += queue.size();
        for (const auto* work : queue) {
            if (work != nullptr) {
                snapshot.dev_op_payload_bytes += work->payload_len;
            }
        }
    }

    snapshot.approx_alloc_bytes = (snapshot.exports * sizeof(WkiIpcExport)) + (snapshot.proxies * sizeof(ProxyIpcState)) +
                                  snapshot.proxy_ring_bytes + (snapshot.pending_deliveries * sizeof(PendingPipeDelivery)) +
                                  (snapshot.pending_chunks * sizeof(PendingPipeChunk)) + snapshot.pending_bytes +
                                  (snapshot.export_backlogs * sizeof(ExportPipeWriteBacklog)) +
                                  (snapshot.export_backlog_chunks * sizeof(PendingPipeChunk)) + snapshot.export_backlog_bytes +
                                  (snapshot.dev_op_queue * sizeof(IpcDevOpWork)) + snapshot.dev_op_payload_bytes;

    s_ipc_lock.unlock_irqrestore(IRQF);

    out = snapshot;
}

auto wki_ipc_proxy_register_poll_waiter(ProxyIpcState* proxy, uint64_t pid) -> bool {
    if (proxy == nullptr || !proxy->active.load(std::memory_order_acquire)) {
        return false;
    }

    uint64_t const IRQF = proxy->lock.lock_irqsave();
    bool const OK = proxy->active.load(std::memory_order_acquire) && proxy_register_waiter_locked(proxy->poll_waiters, pid);
    proxy->lock.unlock_irqrestore(IRQF);
    return OK;
}

void wki_ipc_proxy_wake_poll_waiters(ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        return;
    }

    std::array<uint64_t, WKI_IPC_MAX_POLL_WAKE_WAITERS> pending_waiters = {};
    size_t pending_waiter_count = 0;

    uint64_t const IRQF = proxy->lock.lock_irqsave();
    if (!proxy->poll_waiters.empty()) {
        proxy_collect_waiters_locked(proxy->poll_waiters, pending_waiters, &pending_waiter_count);
    }
    proxy->lock.unlock_irqrestore(IRQF);

    proxy_reschedule_waiters(pending_waiters, pending_waiter_count);
}

// =============================================================================
// Public API: Export task fds for remote submission
// =============================================================================

auto wki_ipc_export_task_fds(ker::mod::sched::task::Task* task, uint16_t target_node, WkiIpcFdEntry* map_out, uint16_t* count_out) -> bool {
    if (task == nullptr || map_out == nullptr || count_out == nullptr) {
        return false;
    }

    uint16_t count = 0;
    constexpr uint16_t MAX_IPC_FDS = 16;
    struct DeferredPtyPump {
        const void* identity = nullptr;
        uint64_t fd = UINT64_MAX;
        WkiIpcExport* exp = nullptr;
    };
    std::array<DeferredPtyPump, MAX_IPC_FDS> deferred_pty_pumps = {};
    size_t deferred_pty_pump_count = 0;

    task->fd_table.for_each([&](uint64_t fd_key, void* val) {
        if (val == nullptr || count >= MAX_IPC_FDS) {
            return;
        }
        auto* file = static_cast<ker::vfs::File*>(val);

        ResourceType res_type = ResourceType::CUSTOM;
        if (ker::vfs::vfs_is_pipe_file(file)) {
            res_type = ResourceType::IPC_PIPE;
        } else if (ker::vfs::vfs_is_socket_file(file)) {
            res_type = ResourceType::IPC_SOCKET;
        } else if (ker::vfs::vfs_is_epoll_file(file)) {
            res_type = ResourceType::IPC_EPOLL;
        } else if (should_proxy_tty_like_file(file)) {
            res_type = ResourceType::IPC_PTY;
        } else {
            return;
        }

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        uint32_t const RESOURCE_ID = allocate_ipc_resource_id();

        auto* exp = new WkiIpcExport();
        exp->active = true;
        exp->resource_id = RESOURCE_ID;
        exp->res_type = res_type;
        exp->file = file;
        file->refcount.fetch_add(1, std::memory_order_relaxed);
        exp->consumer_node = target_node;
        exp->assigned_channel = WKI_CHAN_RESOURCE;

        g_ipc_exports.push_back(exp);
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (res_type == ResourceType::IPC_PIPE && file->open_flags != 0) {
            static_cast<void>(ker::vfs::vfs_pipe_reserve_capacity(file, WKI_IPC_EXPORT_PIPE_CAPACITY));
        }

        bool const NEEDS_DATA_PUMP = (res_type == ResourceType::IPC_PIPE && file->open_flags == 0) || res_type == ResourceType::IPC_SOCKET;
        if (NEEDS_DATA_PUMP) {
            start_pipe_pump(exp);
        } else if (res_type == ResourceType::IPC_PTY) {
            void const* identity = nullptr;
            if (file->fops == ker::vfs::devfs::get_devfs_fops()) {
                identity = ker::dev::pty::pty_file_identity_key(file);
            }
            if (identity == nullptr) {
                identity = static_cast<const void*>(file);
            }

            bool found = false;
            for (size_t i = 0; i < deferred_pty_pump_count; ++i) {
                auto& candidate = deferred_pty_pumps.at(i);
                if (candidate.identity != identity) {
                    continue;
                }
                found = true;
                if (fd_key < candidate.fd) {
                    candidate.fd = fd_key;
                    candidate.exp = exp;
                }
                break;
            }

            if (!found && deferred_pty_pump_count < deferred_pty_pumps.size()) {
                auto& candidate = deferred_pty_pumps.at(deferred_pty_pump_count++);
                candidate.identity = identity;
                candidate.fd = fd_key;
                candidate.exp = exp;
            }
        }

        auto& entry = map_out[count];
        entry.local_fd = static_cast<uint16_t>(fd_key);
        entry.res_type = static_cast<uint16_t>(res_type);
        entry.resource_id = RESOURCE_ID;
        entry.home_node = g_wki.my_node_id;
        entry.reserved1 = static_cast<uint16_t>(file->open_flags) & WKI_IPC_FD_ACCESS_MASK;
        entry.rdma_rkey = 0;
        entry.rdma_offset = 0;
        if (res_type == ResourceType::IPC_PIPE && file->open_flags != 0) {
            setup_export_pipe_rdma(exp, target_node, entry);
        }
        count++;
    });

    for (size_t i = 0; i < deferred_pty_pump_count; ++i) {
        if (deferred_pty_pumps.at(i).exp != nullptr) {
            start_pipe_pump(deferred_pty_pumps.at(i).exp);
        }
    }

    *count_out = count;
    return count > 0;
}

auto wki_ipc_find_pipe_affinity_node(const ker::mod::sched::task::Task* task, uint16_t* node_out) -> bool {
    if (task == nullptr || node_out == nullptr) {
        return false;
    }

    uint16_t candidate_node = WKI_NODE_INVALID;
    bool found = false;
    bool conflict = false;

    task->fd_table.for_each([&](uint64_t, void* val) {
        if (val == nullptr || conflict) {
            return;
        }

        const auto* file = static_cast<const ker::vfs::File*>(val);
        if (!ker::vfs::vfs_is_pipe_file(file) || file->private_data == nullptr) {
            return;
        }

        uint16_t matched_node = WKI_NODE_INVALID;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        for (const auto* exp : g_ipc_exports) {
            if (exp == nullptr || !exp->active || exp->res_type != ResourceType::IPC_PIPE || exp->file == nullptr) {
                continue;
            }
            if (exp->file->private_data != file->private_data || exp->consumer_node == WKI_NODE_INVALID) {
                continue;
            }
            matched_node = exp->consumer_node;
            break;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (matched_node == WKI_NODE_INVALID) {
            return;
        }
        if (!found) {
            candidate_node = matched_node;
            found = true;
            return;
        }
        if (candidate_node != matched_node) {
            conflict = true;
        }
    });

    if (!found || conflict) {
        return false;
    }

    auto* peer = wki_peer_find(candidate_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return false;
    }

    *node_out = candidate_node;
    return true;
}

// =============================================================================
// Public API: Attach proxy fds on remote task
// =============================================================================

void wki_ipc_attach_task_fds(ker::mod::sched::task::Task* task, const WkiIpcFdEntry* map, uint16_t count) {
    if (task == nullptr || map == nullptr || count == 0) {
        return;
    }

    for (const auto& entry : std::span(map, count)) {
        auto* proxy = new ProxyIpcState();
        proxy->active = true;
        proxy->res_type = static_cast<ResourceType>(entry.res_type);
        proxy->home_node = entry.home_node;
        proxy->resource_id = entry.resource_id;
        proxy->assigned_channel = WKI_CHAN_RESOURCE;

        // Allocate local ring buffer for pipe read proxies, sockets, and PTY files (recv data)
        if (proxy->res_type == ResourceType::IPC_PIPE || proxy->res_type == ResourceType::IPC_SOCKET ||
            proxy->res_type == ResourceType::IPC_PTY) {
            constexpr uint32_t PIPE_RING_CAPACITY = WKI_PROXY_PIPE_RING_SIZE;
            auto* rb = new uint8_t[PIPE_RING_CAPACITY];
            if (rb != nullptr) {
                std::fill_n(rb, PIPE_RING_CAPACITY, uint8_t{0});
                proxy->ring_buf = rb;
                proxy->ring_capacity = PIPE_RING_CAPACITY;
            } else {
                ker::mod::dbg::log("[WKI] IPC proxy ring allocation failed: fd=%u res_id=%u cap=%u", entry.local_fd, entry.resource_id,
                                   PIPE_RING_CAPACITY);
            }
            proxy->ring_head.store(0, std::memory_order_relaxed);
            proxy->ring_tail.store(0, std::memory_order_relaxed);
            proxy->write_closed.store(0U, std::memory_order_relaxed);
            proxy->blocked_reader.store(nullptr, std::memory_order_relaxed);
        }

        // Replace the fd's file with proxy file
        auto* existing_file = static_cast<ker::vfs::File*>(task->fd_table.lookup(entry.local_fd));

        auto* proxy_file = new ker::vfs::File();
        proxy_file->fd = static_cast<int>(entry.local_fd);
        proxy_file->private_data = proxy;
        proxy_file->pos = 0;
        proxy_file->is_directory = false;
        proxy_file->fs_type = ker::vfs::FSType::TMPFS;
        proxy_file->refcount = 1;
        proxy_file->fd_flags = 0;
        proxy_file->vfs_path = nullptr;
        proxy_file->dir_fs_count = 0;

        if (proxy->res_type == ResourceType::IPC_PIPE) {
            int const OPEN_FLAGS = static_cast<int>(entry.reserved1 & WKI_IPC_FD_ACCESS_MASK);
            proxy_file->open_flags = OPEN_FLAGS;
            if (OPEN_FLAGS != 0 && entry.rdma_rkey != 0 && entry.rdma_offset != 0 && WKI_IPC_PIPE_RDMA_DOORBELL_ENABLED) {
                auto* transport = ipc_pipe_rdma_transport_for_peer(entry.home_node);
                if (transport != nullptr) {
                    proxy->pipe_rdma_enabled = true;
                    proxy->pipe_rdma_rkey = entry.rdma_rkey;
                    proxy->pipe_rdma_capacity = static_cast<uint32_t>(std::min<uint64_t>(entry.rdma_offset, UINT32_MAX));
                    proxy->pipe_rdma_head = 0;
                    proxy->pipe_rdma_tail_cache = 0;
                    proxy->pipe_rdma_transport = transport;
                    proxy->pipe_rdma_writer_active.store(false, std::memory_order_relaxed);
                }
            }
            if (OPEN_FLAGS == 0) {
                proxy_file->fops = &g_proxy_pipe_read_fops;
            } else {
                proxy_file->fops = &g_proxy_pipe_write_fops;
            }
        } else if (proxy->res_type == ResourceType::IPC_SOCKET) {
            proxy_file->fops = &g_proxy_socket_fops;
            proxy_file->open_flags = 0;
            proxy_file->fs_type = ker::vfs::FSType::TMPFS;  // not SOCKET to avoid bad cast via fd_to_socket
        } else if (proxy->res_type == ResourceType::IPC_EPOLL) {
            // Allocate a local EpollInstance so kernel epoll_ctl / epoll_pwait work
            // transparently on this fd.  EpollInstance MUST be at offset 0 in
            // ProxyEpollFile so the cast in epoll_ctl is valid.
            auto* epf = new ProxyEpollFile();
            epf->inst.count = 0;
            epf->inst.interests.fill({});
            epf->resource_id = proxy->resource_id;
            epf->home_node = proxy->home_node;
            proxy_file->private_data = epf;  // epoll_ctl casts this to EpollInstance*
            proxy_file->fops = &g_proxy_epoll_fops;
            proxy_file->open_flags = 0;
        } else if (proxy->res_type == ResourceType::IPC_PTY) {
            // PTY proxy: ring-buffer data plane + ioctl forwarding
            // private_data is already set to proxy above
            proxy_file->fops = &g_proxy_pty_fops;
            proxy_file->open_flags = 0;
        } else {
            proxy_file->fops = &g_proxy_pipe_read_fops;
            proxy_file->open_flags = 0;
        }

        if (existing_file != nullptr) {
            task->fd_table.remove(entry.local_fd);
            ker::vfs::vfs_put_file(existing_file);
        }
        static_cast<void>(task->fd_table.insert(entry.local_fd, proxy_file));

        bool pending_write_closed = false;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
        g_ipc_proxies.push_back(proxy);
        if (proxy->ring_buf != nullptr) {
            auto* pending = find_pending_pipe_delivery_locked(entry.home_node, entry.resource_id);
            pending_write_closed = pending != nullptr && pending->write_closed;
            if (pending != nullptr && pending->write_closed && pending->chunks.empty()) {
                erase_pending_pipe_delivery_locked(pending);
                free_pending_pipe_delivery(pending);
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (pending_write_closed) {
            proxy_mark_pipe_closed(proxy, entry.resource_id);
        }
#ifdef WKI_IPC_DEBUG
        ker::mod::dbg::log("[WKI] IPC proxy attached fd=%u type=%u res_id=%u home=0x%04x", entry.local_fd, entry.res_type,
                           entry.resource_id, entry.home_node);
#endif
    }
}

// =============================================================================
// Doorbell RX handler — called from ISR when IPC doorbell arrives
// =============================================================================

auto wki_ipc_doorbell_rx(uint16_t src_node, uint32_t doorbell_value) -> bool {
    if ((doorbell_value & WKI_DOORBELL_IPC_MASK) != WKI_DOORBELL_IPC_BASE) {
        return false;
    }

    uint32_t const RESOURCE_ID = doorbell_value & WKI_IPC_RESOURCE_MASK;
    return queue_export_pipe_rdma_flush(src_node, RESOURCE_ID);
}

auto wki_ipc_epoll_ctl_forward(ker::vfs::File* epfile, int op, int fd, uint32_t events, uint64_t data) -> int {
    if (epfile == nullptr) {
        return -EINVAL;
    }

    auto* epf = reinterpret_cast<ProxyEpollFile*>(epfile->private_data);
    if (epf == nullptr || epfile->fops != &g_proxy_epoll_fops) {
        return -EOPNOTSUPP;
    }

    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    uint32_t const CORRELATION = ker::mod::perf::next_wki_trace_correlation();
    uint64_t const STARTED_US = wki_now_us();
    uint16_t const HOME_NODE = epf->home_node;
    auto finish = [&](int rc, uint64_t bytes = 0) -> int {
        auto const ELAPSED_US = static_cast<uint32_t>(wki_now_us() - STARTED_US);
        perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), ker::mod::perf::WkiPerfPhase::END, HOME_NODE,
                              WKI_CHAN_RESOURCE, CORRELATION, rc, ELAPSED_US, CALLSITE);
        perf_record_ipc_summary(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), HOME_NODE, WKI_CHAN_RESOURCE, rc, ELAPSED_US,
                                bytes);
        return rc;
    };
    perf_record_ipc_event(static_cast<uint8_t>(ker::mod::perf::WkiPerfIpcOp::EPOLL_CTL), ker::mod::perf::WkiPerfPhase::BEGIN, HOME_NODE,
                          WKI_CHAN_RESOURCE, CORRELATION, 0, 0, CALLSITE);

    ProxyIpcState* proxy = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy = find_proxy_by_resource_id_locked(epf->resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }
    if (proxy == nullptr) {
        return finish(-EBADF);
    }

    auto release_proxy = [&]() {
        if (proxy != nullptr) {
            proxy_release(proxy);
            proxy = nullptr;
        }
    };

    if (!proxy->active.load(std::memory_order_acquire)) {
        release_proxy();
        return finish(-EBADF);
    }
    if (proxy->home_node == WKI_NODE_INVALID) {
        release_proxy();
        return finish(-EHOSTUNREACH);
    }

    struct __attribute__((packed)) EpollCtlWire {
        int32_t op;
        int32_t fd;
        uint32_t events;
        uint64_t data;
    } ctl = {
        .op = static_cast<int32_t>(op),
        .fd = static_cast<int32_t>(fd),
        .events = events,
        .data = data,
    };

    constexpr size_t RID_SIZE = sizeof(uint32_t);
    constexpr size_t EXTRA_SIZE = sizeof(EpollCtlWire);
    constexpr size_t MSG_SIZE = sizeof(DevOpReqPayload) + RID_SIZE + EXTRA_SIZE;
    std::array<uint8_t, MSG_SIZE> msg = {};

    auto* req = reinterpret_cast<DevOpReqPayload*>(msg.data());
    req->op_id = OP_EPOLL_CTL;
    req->data_len = static_cast<uint16_t>(RID_SIZE + EXTRA_SIZE);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload), &proxy->resource_id, RID_SIZE);
    std::memcpy(msg.data() + sizeof(DevOpReqPayload) + RID_SIZE, &ctl, EXTRA_SIZE);

    WkiWaitEntry wait = {};
    uint64_t irqf = proxy->lock.lock_irqsave();
    if (proxy->pending_wait != nullptr) {
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return finish(-EBUSY);
    }
    proxy->pending_wait = &wait;
    proxy->pending_wait_op = OP_EPOLL_CTL;
    proxy->pending_wait_status = -ETIMEDOUT;
    proxy->pending_wait_resp_len = 0;
    proxy->lock.unlock_irqrestore(irqf);

    int const TX = wki_send(proxy->home_node, WKI_CHAN_RESOURCE, MsgType::DEV_OP_REQ, msg.data(), static_cast<uint16_t>(msg.size()));
    if (TX != WKI_OK) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return finish(-EIO);
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    if (WAIT_RC != 0) {
        irqf = proxy->lock.lock_irqsave();
        proxy->pending_wait = nullptr;
        proxy->lock.unlock_irqrestore(irqf);
        release_proxy();
        return finish(-ETIMEDOUT);
    }

    irqf = proxy->lock.lock_irqsave();
    int const STATUS = proxy->pending_wait_status;
    proxy->pending_wait = nullptr;
    proxy->lock.unlock_irqrestore(irqf);
    release_proxy();
    return finish(STATUS, msg.size());
}

// =============================================================================
// DEV_OP response handler for IPC control ops
// =============================================================================

namespace {

auto claim_proxy_pending_wait_locked(ProxyIpcState* proxy, int status) -> WkiWaitEntry* {
    if (proxy == nullptr || proxy->pending_wait == nullptr) {
        return nullptr;
    }
    auto* wait = proxy->pending_wait;
    if (!wki_claim_op(wait)) {
        return nullptr;
    }
    proxy->pending_wait_status = status;
    proxy->pending_wait_resp_len = 0;
    return wait;
}

}  // namespace

void wki_ipc_handle_dev_op_resp(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    (void)src_node;
    (void)channel;

    if (len < sizeof(DevOpRespPayload)) {
        return;
    }

    DevOpRespPayload resp = {};
    std::memcpy(&resp, payload, sizeof(resp));
    uint16_t const OP_ID = resp.op_id;

    // Handle pipe close acknowledgement
    if (OP_ID == OP_PIPE_CLOSE_READ || OP_ID == OP_PIPE_CLOSE_WRITE) {
        // Close acknowledged — no further action needed
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Home node responded to a poll-state query with readiness flags.
        // Wake any blocked reader so it can re-check availability.
        if (len < static_cast<uint16_t>(sizeof(DevOpRespPayload) + (2 * sizeof(uint32_t)))) {
            return;
        }
        uint32_t poll_resource_id = 0;
        std::memcpy(&poll_resource_id, payload + sizeof(DevOpRespPayload), sizeof(uint32_t));
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id_locked(poll_resource_id);
        if (proxy != nullptr) {
            proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        if (proxy != nullptr) {
            auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            wake_blocked_ipc_reader(reader);
            proxy_release(proxy);
        }
        return;
    }

    if (OP_ID == OP_EPOLL_CTL || OP_ID == OP_FUTEX_WAKE || OP_ID == OP_PTY_IOCTL) {
        // Control-op completion path: payload carries [resource_id] first in data.
        wki_ipc_socket_handle_dev_op_resp(src_node, channel, payload, len);
        return;
    }

    if (OP_ID >= OP_SOCK_ACCEPT && OP_ID <= OP_SOCK_SETSOCKOPT) {
        wki_ipc_socket_handle_dev_op_resp(src_node, channel, payload, len);
    }
}

void wki_ipc_socket_handle_dev_op_resp(uint16_t /*src_node*/, uint16_t /*channel*/, const uint8_t* payload, uint16_t len) {
    if (len < sizeof(DevOpRespPayload)) {
        return;
    }

    DevOpRespPayload resp = {};
    std::memcpy(&resp, payload, sizeof(resp));

    uint16_t const DATA_LEN = resp.data_len;
    if (len < static_cast<uint16_t>(sizeof(DevOpRespPayload) + DATA_LEN)) {
        return;
    }
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);

    // First 4 bytes of response data = resource_id
    if (DATA_LEN < sizeof(uint32_t)) {
        return;
    }
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, resp_data, sizeof(uint32_t));
    auto const EXTRA_LEN = static_cast<uint16_t>(DATA_LEN - sizeof(uint32_t));
    const uint8_t* extra_data = resp_data + sizeof(uint32_t);

    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    ProxyIpcState* target_proxy = nullptr;
    for (auto* p : g_ipc_proxies) {
        if (p != nullptr && p->resource_id == resource_id) {
            target_proxy = p;
            target_proxy->refcount.fetch_add(1, std::memory_order_acq_rel);
            break;
        }
    }
    s_ipc_lock.unlock_irqrestore(IRQF);

    if (target_proxy == nullptr) {
        return;
    }

    WkiWaitEntry* wait = nullptr;
    {
        uint64_t const PROXY_IRQF = target_proxy->lock.lock_irqsave();
        wait = target_proxy->pending_wait;
        if (wait != nullptr && target_proxy->pending_wait_op == resp.op_id && wki_claim_op(wait)) {
            target_proxy->pending_wait_status = static_cast<int>(resp.status);
            if (EXTRA_LEN > 0) {
                uint16_t const COPY_LEN =
                    EXTRA_LEN < ProxyIpcState::SOCK_CTRL_RESP_MAX ? EXTRA_LEN : static_cast<uint16_t>(ProxyIpcState::SOCK_CTRL_RESP_MAX);
                std::memcpy(static_cast<void*>(target_proxy->pending_wait_resp), extra_data, COPY_LEN);
                target_proxy->pending_wait_resp_len = COPY_LEN;
            } else {
                target_proxy->pending_wait_resp_len = 0;
            }
        } else {
            wait = nullptr;  // op_id mismatch — stale response
        }
        target_proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }

    if (wait != nullptr) {
        wki_finish_claimed_op(wait, 0);
    }

    if (target_proxy->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        delete[] target_proxy->ring_buf;
        delete target_proxy;
    }
}

void wki_ipc_detach_proxy_file(ker::vfs::File* f, ProxyIpcState* proxy) {
    if (proxy == nullptr) {
        if (f != nullptr) {
            f->private_data = nullptr;
        }
        return;
    }

    bool dropped_registry_ref = false;
    PendingPipeDelivery* dropped_pending = nullptr;
    {
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        proxy->active.store(false, std::memory_order_release);
        dropped_registry_ref = proxy_unregister_locked(proxy);
        dropped_pending = take_pending_pipe_delivery_locked(proxy->home_node, proxy->resource_id);
        s_ipc_lock.unlock_irqrestore(IRQF);
    }

    auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
    wake_blocked_ipc_reader(reader);
    wki_ipc_proxy_wake_poll_waiters(proxy);
    WkiWaitEntry* pending_wait = nullptr;
    {
        uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
        pending_wait = claim_proxy_pending_wait_locked(proxy, -EIO);
        proxy->lock.unlock_irqrestore(PROXY_IRQF);
    }
    if (pending_wait != nullptr) {
        wki_finish_claimed_op(pending_wait, 0);
    }

    if (f != nullptr) {
        f->private_data = nullptr;
    }

    if (dropped_registry_ref) {
        proxy_release(proxy);
    }
    proxy_release(proxy);
    free_pending_pipe_delivery(dropped_pending);
}

// =============================================================================
// Peer cleanup
// =============================================================================

void wki_ipc_cleanup_for_peer(uint16_t node_id) {
    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
    std::array<ker::mod::sched::task::Task*, WKI_IPC_MAX_EXPORTS> stopped_pumps = {};
    size_t stopped_pump_count = 0;
    auto* stopped_pump_it = stopped_pumps.begin();

    // Clean up exports for this peer
    for (auto* exp : g_ipc_exports) {
        if (exp->active && exp->consumer_node == node_id) {
            auto* pump_task = stop_pipe_pump_locked(exp);
            if (pump_task != nullptr && stopped_pump_it != stopped_pumps.end()) {
                *stopped_pump_it = pump_task;
                ++stopped_pump_it;
                stopped_pump_count++;
            }
            if (exp->file != nullptr) {
                // Release the refcount bump taken at export time.
                // If this was the last reference, vfs_close handles teardown.
                if (exp->file->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    if (exp->file->fops != nullptr && exp->file->fops->vfs_close != nullptr) {
                        exp->file->fops->vfs_close(exp->file);
                    }
                    delete exp->file;
                }
                exp->file = nullptr;
            }
            exp->active = false;
        }
    }
    compact_inactive_exports_locked();

    size_t detached_proxy_count = 0;
    std::array<ProxyIpcState*, WKI_IPC_MAX_PROXIES> detached_proxies = {};
    std::array<PendingPipeDelivery*, WKI_IPC_MAX_EXPORTS> detached_pending = {};
    std::array<PendingProxyPipeClose*, WKI_IPC_PROXY_CLOSE_CLEANUP_BATCH> detached_proxy_closes = {};
    size_t detached_pending_count = 0;
    size_t detached_proxy_close_count = 0;
    auto* detached_proxy_it = detached_proxies.begin();
    auto* detached_pending_it = detached_pending.begin();
    auto* detached_proxy_close_it = detached_proxy_closes.begin();
    bool wake_export_pipe_flush = false;
    for (auto it = g_ipc_proxies.begin(); it != g_ipc_proxies.end();) {
        auto* proxy = *it;
        if (proxy != nullptr && proxy->active.load(std::memory_order_acquire) && proxy->home_node == node_id) {
            proxy->active.store(false, std::memory_order_release);
            if (detached_proxy_it != detached_proxies.end()) {
                *detached_proxy_it = proxy;
                ++detached_proxy_it;
                detached_proxy_count++;
            }
            it = g_ipc_proxies.erase(it);
            continue;
        }
        ++it;
    }

    for (auto it = g_pending_pipe_deliveries.begin(); it != g_pending_pipe_deliveries.end();) {
        auto* pending = *it;
        if (pending != nullptr && pending->home_node == node_id) {
            if (detached_pending_it != detached_pending.end()) {
                *detached_pending_it = pending;
                ++detached_pending_it;
                detached_pending_count++;
            }
            it = g_pending_pipe_deliveries.erase(it);
            continue;
        }
        ++it;
    }

    for (auto it = g_pending_proxy_pipe_closes.begin(); it != g_pending_proxy_pipe_closes.end();) {
        auto* pending = *it;
        if (pending != nullptr && pending->home_node == node_id && detached_proxy_close_it != detached_proxy_closes.end()) {
            *detached_proxy_close_it = pending;
            ++detached_proxy_close_it;
            detached_proxy_close_count++;
            it = g_pending_proxy_pipe_closes.erase(it);
            continue;
        }
        ++it;
    }

    for (auto* backlog : g_export_pipe_write_backlogs) {
        auto* exp = backlog != nullptr ? backlog->exp : nullptr;
        if (exp != nullptr && exp->consumer_node == node_id) {
            if (backlog != nullptr) {
                backlog->exp = nullptr;
                queue_export_pipe_write_flush_locked(backlog);
            }
            wake_export_pipe_flush = true;
        }
    }

    s_ipc_lock.unlock_irqrestore(IRQF);

    for (auto* stopped_pump : std::span(stopped_pumps.data(), stopped_pump_count)) {
        wake_pipe_pump(stopped_pump);
    }

    if (wake_export_pipe_flush) {
        wake_all_export_pipe_write_flush_workers();
    }

    for (auto* proxy : std::span(detached_proxies.data(), detached_proxy_count)) {
        WkiWaitEntry* pending_wait = nullptr;
        {
            uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
            pending_wait = claim_proxy_pending_wait_locked(proxy, -EIO);
            proxy->lock.unlock_irqrestore(PROXY_IRQF);
        }
        auto* reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
        wake_blocked_ipc_reader(reader);
        wki_ipc_proxy_wake_poll_waiters(proxy);
        if (pending_wait != nullptr) {
            wki_finish_claimed_op(pending_wait, 0);
        }
        proxy_release(proxy);
    }

    for (auto* pending : std::span(detached_pending.data(), detached_pending_count)) {
        free_pending_pipe_delivery(pending);
    }

    for (auto* pending : std::span(detached_proxy_closes.data(), detached_proxy_close_count)) {
        free_pending_proxy_pipe_close(pending);
    }
}

// =============================================================================
// Internal RX handlers (for future use with dedicated IPC channels)
// =============================================================================

namespace detail {

void handle_ipc_attach_req(const WkiHeader* /*hdr*/, const uint8_t* /*payload*/, uint16_t /*payload_len*/) {
    // Future: handle explicit IPC resource attach requests
}

void handle_ipc_attach_ack(const WkiHeader* /*hdr*/, const uint8_t* /*payload*/, uint16_t /*payload_len*/) {
    // Future: handle IPC resource attach acknowledgements
}

}  // namespace detail

namespace {

void handle_ipc_dev_op_req_inline(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    uint16_t const OP_ID = req.op_id;

    // All IPC ops carry resource_id as first 4 bytes of data
    if (req.data_len < sizeof(uint32_t)) {
        return;
    }
    if (sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return;
    }
    const auto* data = payload + sizeof(DevOpReqPayload);
    uint32_t resource_id = 0;
    std::memcpy(&resource_id, data, sizeof(uint32_t));
    const auto* op_data = data + sizeof(uint32_t);
    uint16_t const OP_DATA_LEN = req.data_len - sizeof(uint32_t);
    uint64_t const IPC_CALLSITE = WOS_PERF_CALLSITE();
    auto const IPC_CORRELATION = static_cast<uint32_t>(hdr->seq_num & UINT16_MAX);

    if (OP_ID == OP_PIPE_DATA) {
        IpcPerfTrace pipe_trace(ker::mod::perf::WkiPerfIpcOp::PIPE_DATA, hdr->src_node, hdr->channel_id, IPC_CALLSITE, OP_DATA_LEN,
                                IPC_CORRELATION);
        if (OP_DATA_LEN == 0 && queue_export_pipe_rdma_flush(hdr->src_node, resource_id)) {
            pipe_trace.finish(0);
            return;
        }

        // Consumer receives pipe data — write into the local proxy ring if
        // present, otherwise route it to the exported home-side pipe.
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* proxy = find_proxy_by_resource_id(resource_id);
        bool const PENDING_BUFFERED = find_pending_pipe_delivery_locked(hdr->src_node, resource_id) != nullptr;
        auto* export_exp = static_cast<WkiIpcExport*>(nullptr);
        ker::vfs::File* export_file = nullptr;
        bool export_queue_behind_backlog = false;
        if (proxy == nullptr) {
            auto* exp = find_export_by_resource_id(resource_id);
            if (exp != nullptr && exp->active && exp->file != nullptr) {
                export_exp = exp;
                export_file = exp->file;
                export_file->refcount.fetch_add(1, std::memory_order_acq_rel);
                note_export_pipe_data_received_locked(exp, OP_DATA_LEN);
                auto* backlog = find_export_pipe_write_backlog_locked(exp);
                export_queue_behind_backlog = backlog != nullptr && (!backlog->chunks.empty() || backlog->close_pending);
            }
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (proxy != nullptr) {
            if (OP_DATA_LEN == 0) {
                proxy_release(proxy);
                return;
            }

            if (PENDING_BUFFERED || proxy->ring_buf == nullptr) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            auto* reader = static_cast<ker::mod::sched::task::Task*>(nullptr);
            bool queue_overflow = false;
            uint64_t const PROXY_IRQF = proxy->lock.lock_irqsave();
            if (!proxy->active.load(std::memory_order_acquire) || proxy->ring_buf == nullptr) {
                proxy->lock.unlock_irqrestore(PROXY_IRQF);
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            uint32_t const HEAD = proxy->ring_head.load(std::memory_order_relaxed);
            uint32_t const TAIL = proxy->ring_tail.load(std::memory_order_acquire);
            uint32_t const CAP = proxy->ring_capacity;
            uint32_t const FREE_SPACE = CAP - (HEAD - TAIL);

            if (OP_DATA_LEN > FREE_SPACE) {
                queue_overflow = true;
            } else {
                uint32_t const RING_HEAD = HEAD % CAP;
                uint32_t const FIRST = CAP - RING_HEAD;
                if (FIRST >= OP_DATA_LEN) {
                    std::memcpy(proxy->ring_buf + RING_HEAD, op_data, OP_DATA_LEN);
                } else {
                    std::memcpy(proxy->ring_buf + RING_HEAD, op_data, FIRST);
                    std::memcpy(proxy->ring_buf, op_data + FIRST, OP_DATA_LEN - FIRST);
                }
                proxy->ring_head.store(HEAD + OP_DATA_LEN, std::memory_order_release);

                reader = proxy->blocked_reader.exchange(nullptr, std::memory_order_acq_rel);
            }
            proxy->lock.unlock_irqrestore(PROXY_IRQF);

            if (queue_overflow) {
                if (!queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                    ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
                } else {
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                          OP_DATA_LEN, IPC_CALLSITE);
                    wake_proxy_reader(proxy);
                    perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                          IPC_CALLSITE);
                    wki_ipc_proxy_wake_poll_waiters(proxy);
                }
                proxy_release(proxy);
                pipe_trace.finish(0, OP_DATA_LEN);
                return;
            }

            if (reader != nullptr) {
                perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::WAKE_READER, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0,
                                      OP_DATA_LEN, IPC_CALLSITE);
                wake_blocked_ipc_reader(reader);
            }
            perf_record_ipc_point(ker::mod::perf::WkiPerfIpcOp::POLL_WAKE, hdr->src_node, hdr->channel_id, IPC_CORRELATION, 0, 0,
                                  IPC_CALLSITE);
            wki_ipc_proxy_wake_poll_waiters(proxy);
            proxy_release(proxy);
            pipe_trace.finish(0, OP_DATA_LEN);
            return;
        }

        if (export_file == nullptr) {
            if (OP_DATA_LEN > 0 && !queue_pending_pipe_data(hdr->src_node, resource_id, op_data, OP_DATA_LEN)) {
                ker::mod::dbg::log("[WKI] IPC pending pipe DATA queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
            }
            return;
        }
        if (OP_DATA_LEN == 0) {
            ipc_release_file_ref(export_file);
            pipe_trace.finish(0);
            return;
        }
        if (export_queue_behind_backlog) {
            if (!queue_export_pipe_write_data(export_exp, op_data, OP_DATA_LEN)) {
                ker::mod::dbg::log("[WKI] IPC export pipe backlog ordered queue failed: resource_id=%u len=%u", resource_id, OP_DATA_LEN);
            }
            ipc_release_file_ref(export_file);
            pipe_trace.finish(0, OP_DATA_LEN);
            return;
        }
        if (export_file->fops != nullptr && export_file->fops->vfs_write != nullptr) {
            ssize_t const WRITE_RET = export_pipe_write_nonblocking(export_file, op_data, OP_DATA_LEN);
            if (std::cmp_not_equal(WRITE_RET, OP_DATA_LEN)) {
                uint16_t written = 0;
                if (WRITE_RET > 0) {
                    written = static_cast<uint16_t>(std::cmp_less(WRITE_RET, OP_DATA_LEN) ? WRITE_RET : OP_DATA_LEN);
                } else if (WRITE_RET != -EAGAIN && WRITE_RET != -EINTR) {
                    ker::mod::dbg::log("[WKI] IPC export pipe immediate write failed: resource_id=%u ret=%ld len=%u", resource_id,
                                       WRITE_RET, OP_DATA_LEN);
                }

                uint64_t const EXPORT_IRQF = s_ipc_lock.lock_irqsave();
                auto* exp = find_export_by_resource_id(resource_id);
                s_ipc_lock.unlock_irqrestore(EXPORT_IRQF);
                if (exp == nullptr || !queue_export_pipe_write_data(exp, op_data + written, static_cast<uint16_t>(OP_DATA_LEN - written))) {
                    ker::mod::dbg::log("[WKI] IPC export pipe backlog queue failed: resource_id=%u written=%u total=%u", resource_id,
                                       written, OP_DATA_LEN);
                }
            }
        }
        ipc_release_file_ref(export_file);
        pipe_trace.finish(0, OP_DATA_LEN);
        return;
    }

    if (OP_ID == OP_PIPE_POLL_STATE) {
        // Consumer queries current readiness of the exported pipe/socket.
        // Server checks poll_check on the underlying file and sends back a
        // response with readiness flags.
        ker::vfs::File* f = nullptr;
        {
            uint64_t const IRQF = s_ipc_lock.lock_irqsave();
            f = ipc_acquire_export_file_locked(resource_id);
            s_ipc_lock.unlock_irqrestore(IRQF);
        }

        DevOpRespPayload poll_resp = {};
        poll_resp.op_id = OP_PIPE_POLL_STATE;
        poll_resp.status = 0;
        poll_resp.data_len = sizeof(uint32_t) * 2;  // [resource_id, ready_flags]
        poll_resp.reserved = 0;

        std::array<uint8_t, sizeof(DevOpRespPayload) + (sizeof(uint32_t) * 2)> poll_resp_buf = {};
        std::memcpy(poll_resp_buf.data(), &poll_resp, sizeof(poll_resp));
        std::memcpy(poll_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        int ready_flags = 0;
        if (f != nullptr) {
            if (f->fops != nullptr && f->fops->vfs_poll_check != nullptr) {
                ready_flags = f->fops->vfs_poll_check(f, 0x0001 | 0x0004);  // POLLIN | POLLOUT
            }
            ipc_release_file_ref(f);
        } else {
            poll_resp.status = -ENOENT;
        }

        std::memcpy(poll_resp_buf.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t), &ready_flags, sizeof(uint32_t));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, poll_resp_buf.data(), static_cast<uint16_t>(poll_resp_buf.size()));
        return;
    }

    if (OP_ID == OP_EPOLL_CTL) {
        // Forward epoll_ctl to the home node epoll fd.
        // op_data layout: [op:i32][fd:i32][events:u32][data:u64]
        DevOpRespPayload ctl_resp = {};
        ctl_resp.op_id = OP_EPOLL_CTL;
        ctl_resp.status = -EINVAL;
        ctl_resp.data_len = sizeof(uint32_t);  // [resource_id]

        std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> ctl_resp_buf = {};
        std::memcpy(ctl_resp_buf.data(), &ctl_resp, sizeof(ctl_resp));
        std::memcpy(ctl_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        if (OP_DATA_LEN >= sizeof(int32_t) + sizeof(int32_t) + sizeof(uint32_t) + sizeof(uint64_t)) {
            int32_t ctl_op = 0;
            int32_t ctl_fd = -1;
            uint32_t ctl_events = 0;
            uint64_t ctl_data = 0;
            size_t cursor = 0;
            std::memcpy(&ctl_op, op_data + cursor, sizeof(int32_t));
            cursor += sizeof(int32_t);
            std::memcpy(&ctl_fd, op_data + cursor, sizeof(int32_t));
            cursor += sizeof(int32_t);
            std::memcpy(&ctl_events, op_data + cursor, sizeof(uint32_t));
            cursor += sizeof(uint32_t);
            std::memcpy(&ctl_data, op_data + cursor, sizeof(uint64_t));

            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }

            if (f != nullptr && ker::vfs::vfs_is_epoll_file(f)) {
                ker::vfs::EpollEvent event = {};
                event.events = ctl_events;
                event.data.u64 = ctl_data;
                ctl_resp.status = static_cast<int16_t>(ker::vfs::epoll_ctl(f->fd, ctl_op, ctl_fd, &event));
                ipc_release_file_ref(f);
            } else if (f != nullptr) {
                ctl_resp.status = -EINVAL;
                ipc_release_file_ref(f);
            } else {
                ctl_resp.status = -ENOENT;
            }
        }

        std::memcpy(ctl_resp_buf.data(), &ctl_resp, sizeof(ctl_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, ctl_resp_buf.data(), static_cast<uint16_t>(ctl_resp_buf.size()));
        return;
    }

    if (OP_ID == OP_FUTEX_WAKE) {
        // Wake waiters for a physical futex address owned by this node.
        // op_data layout: [phys_addr:u64]
        DevOpRespPayload wake_resp = {};
        wake_resp.op_id = OP_FUTEX_WAKE;
        wake_resp.status = -EINVAL;
        wake_resp.data_len = sizeof(uint32_t);  // [resource_id]

        std::array<uint8_t, sizeof(DevOpRespPayload) + sizeof(uint32_t)> wake_resp_buf = {};
        std::memcpy(wake_resp_buf.data(), &wake_resp, sizeof(wake_resp));
        std::memcpy(wake_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        if (OP_DATA_LEN >= sizeof(uint64_t)) {
            uint64_t phys_addr = 0;
            std::memcpy(&phys_addr, op_data, sizeof(uint64_t));
            wake_resp.status = static_cast<int16_t>(ker::syscall::futex::futex_wake_by_phys(phys_addr));
        }

        std::memcpy(wake_resp_buf.data(), &wake_resp, sizeof(wake_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, wake_resp_buf.data(), static_cast<uint16_t>(wake_resp_buf.size()));
        return;
    }

    if (OP_ID == OP_PTY_IOCTL) {
        // Forward ioctl to the home-node PTY file and return result.
        // op_data layout: [cmd:u64][arg_val:u64][arg_in:128B]
        constexpr size_t PTY_ARG_SIZE = 128;
        DevOpRespPayload pty_resp = {};
        pty_resp.op_id = OP_PTY_IOCTL;
        pty_resp.status = -EINVAL;
        pty_resp.data_len = static_cast<uint16_t>(sizeof(uint32_t) + PTY_ARG_SIZE);

        constexpr size_t RESP_BUF_SIZE = sizeof(DevOpRespPayload) + sizeof(uint32_t) + PTY_ARG_SIZE;
        std::array<uint8_t, RESP_BUF_SIZE> pty_resp_buf = {};
        std::memcpy(pty_resp_buf.data() + sizeof(DevOpRespPayload), &resource_id, sizeof(uint32_t));

        constexpr size_t MIN_REQ = sizeof(uint64_t) + sizeof(uint64_t) + PTY_ARG_SIZE;
        if (OP_DATA_LEN >= MIN_REQ) {
            uint64_t ioctl_cmd = 0;
            uint64_t ioctl_arg_val = 0;
            std::memcpy(&ioctl_cmd, op_data, sizeof(uint64_t));
            std::memcpy(&ioctl_arg_val, op_data + sizeof(uint64_t), sizeof(uint64_t));
            const uint8_t* arg_in = op_data + sizeof(uint64_t) + sizeof(uint64_t);

            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }

            if (f != nullptr) {
                unsigned long effective_arg = 0;
                alignas(8) std::array<uint8_t, PTY_ARG_SIZE> local_buf = {};
                if (ioctl_arg_val >= 4096) {
                    // Pointer-type ioctl: use local buffer
                    std::memcpy(local_buf.data(), arg_in, PTY_ARG_SIZE);
                    effective_arg = reinterpret_cast<unsigned long>(local_buf.data());
                } else {
                    // Value-type ioctl: pass arg_val directly
                    effective_arg = static_cast<unsigned long>(ioctl_arg_val);
                }

                int const RC = ker::vfs::devfs::devfs_ioctl(f, static_cast<unsigned long>(ioctl_cmd), effective_arg);
                pty_resp.status = static_cast<int16_t>(RC < 0 ? RC : 0);

                // Always copy back local buffer (for output ioctls)
                std::memcpy(pty_resp_buf.data() + sizeof(DevOpRespPayload) + sizeof(uint32_t), local_buf.data(), PTY_ARG_SIZE);
                ipc_release_file_ref(f);
            } else {
                pty_resp.status = -ENOENT;
            }
        }

        std::memcpy(pty_resp_buf.data(), &pty_resp, sizeof(pty_resp));
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, pty_resp_buf.data(), static_cast<uint16_t>(pty_resp_buf.size()));
        return;
    }

    if (OP_ID == OP_PTY_CLOSE) {
        // Consumer closed its proxy PTY — tear down the export
        ker::mod::sched::task::Task* pump_task = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active) {
            pump_task = stop_pipe_pump_locked(exp);
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            compact_inactive_exports_locked();
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake_pipe_pump(pump_task);
            if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                    f->fops->vfs_close(f);
                }
                delete f;
            }
        } else {
            s_ipc_lock.unlock_irqrestore(IRQF);
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_WRITE) {
        // Server-side: consumer closed write end — close the real pipe's read end
        // Client-side: home node reports write-end closed (EOF)
        bool has_expected_bytes = false;
        uint64_t expected_bytes = 0;
        if (OP_DATA_LEN >= sizeof(expected_bytes)) {
            std::memcpy(&expected_bytes, op_data, sizeof(expected_bytes));
            has_expected_bytes = true;
        }

        uint64_t const IRQF = s_ipc_lock.lock_irqsave();

        // Check consumer-side proxy first
        auto* proxy = find_proxy_by_resource_id(resource_id);
        if (proxy != nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);

            proxy_mark_pipe_closed(proxy, resource_id);
            proxy_release(proxy);
            return;
        }

        // Check server-side export
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active && exp->file != nullptr) {
            s_ipc_lock.unlock_irqrestore(IRQF);
            if (!mark_export_pipe_write_closed(exp, expected_bytes, has_expected_bytes)) {
                ker::mod::dbg::log("[WKI] IPC export pipe close queue failed: resource_id=%u", resource_id);
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);

        if (!mark_pending_pipe_write_closed(hdr->src_node, resource_id)) {
            ker::mod::dbg::log("[WKI] IPC pending pipe CLOSE queue failed: resource_id=%u", resource_id);
        }
        return;
    }

    if (OP_ID == OP_PIPE_CLOSE_READ) {
        // Consumer closed read end — stop the pump, release the export's file ref.
        ker::mod::sched::task::Task* pump_task = nullptr;
        uint64_t const IRQF = s_ipc_lock.lock_irqsave();
        auto* exp = find_export_by_resource_id(resource_id);
        if (exp != nullptr && exp->active) {
            pump_task = stop_pipe_pump_locked(exp);
            ker::vfs::File* f = exp->file;
            exp->file = nullptr;
            exp->active = false;
            compact_inactive_exports_locked();
            s_ipc_lock.unlock_irqrestore(IRQF);
            wake_pipe_pump(pump_task);
            if (f != nullptr && f->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                    f->fops->vfs_close(f);
                }
                delete f;
            }
            return;
        }
        s_ipc_lock.unlock_irqrestore(IRQF);
        return;
    }

    if (OP_ID >= OP_SOCK_ACCEPT && OP_ID <= OP_SOCK_SETSOCKOPT) {
        // Build a response with resource_id as first 4 bytes so the consumer
        // can find the right proxy in wki_ipc_socket_handle_dev_op_resp().
        constexpr size_t RESP_HEADER = sizeof(DevOpRespPayload);
        constexpr size_t RID_SIZE = sizeof(uint32_t);

        // Reserve up to 128 bytes for optional response payload (e.g. getpeername addr)
        constexpr size_t EXTRA_MAX = 128;
        std::array<uint8_t, RESP_HEADER + RID_SIZE + EXTRA_MAX> resp_buf = {};
        auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
        resp->op_id = OP_ID;
        resp->status = -ENOSYS;
        resp->data_len = RID_SIZE;  // at minimum include resource_id
        std::memcpy(resp_buf.data() + RESP_HEADER, &resource_id, RID_SIZE);

        if (OP_ID == OP_SOCK_CLOSE) {
            WkiIpcExport* exp = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                exp = find_export_by_resource_id(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            resp->status = mark_export_pipe_write_closed(exp) ? 0 : -ENOENT;
        } else if (OP_ID == OP_SOCK_SHUTDOWN) {
            // op_data: [how:int32]
            if (OP_DATA_LEN >= sizeof(int32_t)) {
                int32_t how = 0;
                std::memcpy(&how, op_data, sizeof(int32_t));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->shutdown != nullptr) {
                        resp->status = static_cast<int16_t>(sock->proto_ops->shutdown(sock, static_cast<int>(how)));
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                } else {
                    resp->status = -ENOENT;
                }
            } else {
                resp->status = -EINVAL;
            }
        } else if (OP_ID == OP_SOCK_GETPEERNAME) {
            // op_data: [] (no extra args needed)
            ker::vfs::File* f = nullptr;
            {
                uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                f = ipc_acquire_export_file_locked(resource_id);
                s_ipc_lock.unlock_irqrestore(IRQF);
            }
            if (f != nullptr) {
                auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                if (sock != nullptr) {
                    // Serialize peer addr as: [family:u16][addr:u32][port:u16] (AF_INET)
                    struct {
                        uint16_t family;
                        uint32_t addr;
                        uint16_t port;
                    } __attribute__((packed)) peer_addr = {};
                    static_assert(sizeof(peer_addr) == 8);
                    peer_addr.family = static_cast<uint16_t>(sock->domain);
                    peer_addr.addr = sock->remote_v4.addr;
                    peer_addr.port = sock->remote_v4.port;
                    std::memcpy(resp_buf.data() + RESP_HEADER + RID_SIZE, &peer_addr, sizeof(peer_addr));
                    resp->data_len = static_cast<uint16_t>(RID_SIZE + sizeof(peer_addr));
                    resp->status = 0;
                } else {
                    resp->status = -ENOTSOCK;
                }
                ipc_release_file_ref(f);
            } else {
                resp->status = -ENOENT;
            }
        } else if (OP_ID == OP_SOCK_GETSOCKOPT) {
            // op_data: [level:int32][optname:int32]
            if (OP_DATA_LEN >= 2 * sizeof(int32_t)) {
                int32_t level = 0;
                int32_t optname = 0;
                std::memcpy(&level, op_data, sizeof(int32_t));
                std::memcpy(&optname, op_data + sizeof(int32_t), sizeof(int32_t));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->getsockopt != nullptr) {
                        std::array<uint8_t, 64> opt_val = {};
                        size_t opt_len = opt_val.size();
                        int const RC =
                            sock->proto_ops->getsockopt(sock, static_cast<int>(level), static_cast<int>(optname), opt_val.data(), &opt_len);
                        resp->status = static_cast<int16_t>(RC);
                        if (RC == 0 && opt_len > 0 && opt_len <= EXTRA_MAX) {
                            std::memcpy(resp_buf.data() + RESP_HEADER + RID_SIZE, opt_val.data(), opt_len);
                            resp->data_len = static_cast<uint16_t>(RID_SIZE + opt_len);
                        }
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                } else {
                    resp->status = -ENOENT;
                }
            } else {
                resp->status = -EINVAL;
            }
        } else if (OP_ID == OP_SOCK_SETSOCKOPT) {
            // op_data: [level:int32][optname:int32][optval...]
            if (OP_DATA_LEN >= 2 * sizeof(int32_t)) {
                int32_t level = 0;
                int32_t optname = 0;
                std::memcpy(&level, op_data, sizeof(int32_t));
                std::memcpy(&optname, op_data + sizeof(int32_t), sizeof(int32_t));
                const uint8_t* optval = op_data + (2 * sizeof(int32_t));
                auto const OPTVAL_LEN = static_cast<uint16_t>(OP_DATA_LEN - (2 * sizeof(int32_t)));
                ker::vfs::File* f = nullptr;
                {
                    uint64_t const IRQF = s_ipc_lock.lock_irqsave();
                    f = ipc_acquire_export_file_locked(resource_id);
                    s_ipc_lock.unlock_irqrestore(IRQF);
                }
                if (f != nullptr) {
                    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
                    if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->setsockopt != nullptr) {
                        int const RC =
                            sock->proto_ops->setsockopt(sock, static_cast<int>(level), static_cast<int>(optname), optval, OPTVAL_LEN);
                        resp->status = static_cast<int16_t>(RC);
                    } else {
                        resp->status = -ENOSYS;
                    }
                    ipc_release_file_ref(f);
                } else {
                    resp->status = -ENOENT;
                }
            } else {
                resp->status = -EINVAL;
            }
        }

        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, resp_buf.data(),
                 static_cast<uint16_t>(RESP_HEADER + resp->data_len));
        return;
    }

    (void)hdr;
}

}  // namespace

namespace detail {

void handle_ipc_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));
    if (req.data_len < sizeof(uint32_t) || sizeof(DevOpReqPayload) + req.data_len > payload_len) {
        return;
    }

    uint32_t resource_id = 0;
    std::memcpy(&resource_id, payload + sizeof(DevOpReqPayload), sizeof(uint32_t));

    if (should_defer_ipc_dev_op(req.op_id, resource_id, hdr->src_node)) {
        enqueue_ipc_dev_op_work(hdr, payload, payload_len, req.op_id, resource_id);
        return;
    }

    handle_ipc_dev_op_req_inline(hdr, payload, payload_len);
}

void handle_ipc_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    wki_ipc_handle_dev_op_resp(hdr->src_node, hdr->channel_id, payload, payload_len);
}

}  // namespace detail

// =============================================================================
// Public: incoming IPC DEV_OP_REQ dispatch (called from wki.cpp)
// =============================================================================

void wki_ipc_handle_dev_op_req(uint16_t src_node, uint16_t channel, const uint8_t* payload, uint16_t len) {
    if (len < sizeof(DevOpReqPayload)) {
        return;
    }
    DevOpReqPayload req = {};
    std::memcpy(&req, payload, sizeof(req));

    // Route IPC ops (0x0700 - 0x07FF)
    if (req.op_id >= 0x0700 && req.op_id <= 0x07FF) {
        WkiHeader hdr = {};
        hdr.src_node = src_node;
        hdr.channel_id = channel;
        detail::handle_ipc_dev_op_req(&hdr, payload, len);
    }
}

}  // namespace ker::net::wki
