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
#include <net/wki/remote_ipc.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
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
#include <util/errno_name.hpp>
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

void promote_latency_sensitive_daemon(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->type != ker::mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_LATENCY_DAEMON_SLICE_NS;
    ker::mod::sched::set_task_nice(task, WKI_LATENCY_DAEMON_NICE);
}

std::deque<SubmittedTask> g_submitted_tasks;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteNodeLoad> g_remote_loads;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RunningRemoteTask> g_running_remote_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
struct PendingTaskCompletion {
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    int32_t exit_status = -1;
    TaskOutputCapture* output = nullptr;
};
std::deque<PendingTaskCompletion> g_pending_task_completions;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_task_id = 1;                                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_compute_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_load_report_us = 0;                            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint16_t g_preferred_remote_cursor = 0;                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_compute_lock;                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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

// Pending queue for VFS_REF/RESOURCE_REF task submits.
// These delivery modes call load_elf_from_vfs_path() which does blocking
// remote VFS reads.  Running those from NAPI poll context causes
// re-entrance deadlocks, so they are queued here and processed by a
// dedicated kernel thread (wki_compute_submit_thread).
struct PendingTaskSubmit {
    uint16_t src_node;
    uint8_t* payload;
    uint16_t payload_len;
    uint64_t queued_at_us;
};
std::deque<PendingTaskSubmit> g_pending_task_submits;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_pending_submit_lock;                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sched::task::Task* s_compute_submit_task = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct SharedElfCacheEntry {
    bool valid = false;
    bool loading = false;
    int32_t load_status = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
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

constexpr uint64_t WKI_TASK_SUBMIT_VFS_TIMEOUT_US = 60000000;  // 60 s for remote binary fetch + launch
// Keep executable preads bounded: remote VFS may split one logical pread into
// several RDMA sub-requests, and cold fan-out does this concurrently per node.
constexpr size_t WKI_VFS_LOAD_CHUNK = size_t{2} * 1024 * 1024;
constexpr uint32_t WKI_VFS_LOAD_IDLE_RETRIES = 8;
constexpr uint32_t WKI_VFS_LOAD_MAX_ATTEMPTS = 24;
constexpr uint64_t WKI_VFS_LOAD_RETRY_WINDOW_US = 15000000;
constexpr uint64_t WKI_VFS_LOAD_RETRY_BACKOFF_US = 750000;
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
constexpr int32_t WKI_SIGINT_NUM = 2;
constexpr int32_t WKI_SIGKILL_NUM = 9;
constexpr int32_t WKI_SIGTERM_NUM = 15;
constexpr uint64_t WKI_SIGCHLD_NUM = 17;
constexpr int32_t WKI_WAIT_STATUS_SIGNAL_MASK = 0x7f;
constexpr int32_t WKI_WAIT_STATUS_STOPPED = 0x7f;
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

// s_compute_lock must be held by caller
auto find_submitted_task(uint32_t task_id) -> SubmittedTask* {
    for (auto& t : g_submitted_tasks) {
        if (t.active && t.task_id == task_id) {
            return &t;
        }
    }
    return nullptr;
}

// s_compute_lock must be held by caller
auto find_submitted_task_any(uint32_t task_id) -> SubmittedTask* {
    for (auto& t : g_submitted_tasks) {
        if (t.task_id == task_id) {
            return &t;
        }
    }
    return nullptr;
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

    while (wait.state.load(std::memory_order_acquire) != static_cast<uint8_t>(WkiWaitEntry::DONE)) {
        ker::mod::sched::kern_yield();
    }
}

// s_compute_lock must be held by caller.
auto publish_task_complete_waiter_locked(SubmittedTask* task, WkiWaitEntry& wait) -> bool {
    if (task == nullptr || task->complete_pending.load(std::memory_order_acquire)) {
        return false;
    }

    task->complete_wait_entry = &wait;
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
    if (WAIT_SLOT_OWNED) {
        task->complete_wait_entry = nullptr;
    }
    completed_exit_status = task->exit_status;
    if (wait_result == WKI_ERR_TIMEOUT && WAIT_SLOT_OWNED) {
        task->complete_pending.store(false, std::memory_order_release);
    }
    return WAIT_SLOT_OWNED;
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
auto find_running_task(uint32_t task_id, uint16_t submitter) -> RunningRemoteTask* {
    for (auto& rt : g_running_remote_tasks) {
        if (rt.active && rt.task_id == task_id && rt.submitter_node == submitter) {
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

auto shared_elf_path_matches(const SharedElfCacheEntry& entry, const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }

    size_t const ENTRY_LEN = std::strlen(entry.path.data());
    size_t const PATH_LEN = std::strlen(path);
    return ENTRY_LEN == PATH_LEN && std::strncmp(entry.path.data(), path, PATH_LEN) == 0;
}

auto shared_elf_freshness_matches(const ker::vfs::Stat& lhs, const ker::vfs::Stat& rhs) -> bool {
    return lhs.st_size > 0 && lhs.st_size == rhs.st_size;
}

auto find_shared_elf_cache_locked(uint16_t submitter_node, const char* path, const ker::vfs::Stat& freshness) -> SharedElfCacheEntry* {
    for (auto& entry : g_shared_elf_cache) {
        if (!entry.valid || entry.loading || entry.submitter_node != submitter_node || entry.buffer == nullptr) {
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

auto find_shared_elf_cache_entry_locked(uint16_t submitter_node, const char* path, const ker::vfs::Stat& freshness)
    -> SharedElfCacheEntry* {
    for (auto& entry : g_shared_elf_cache) {
        if (!entry.valid || entry.submitter_node != submitter_node) {
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
        bool const EXPIRED = it->refcount == 0 && (!it->valid || (it->last_used_us != 0 && now_us >= it->last_used_us &&
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
            if (it->refcount != 0) {
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
auto capture_isatty(ker::vfs::File* /*file*/) -> bool { return true; }
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
    }
    if (identity.submitter_hostname.front() != '\0') {
        std::strncpy(task->wki_submitter_hostname.data(), identity.submitter_hostname.data(), task->wki_submitter_hostname.size() - 1);
        task->wki_submitter_hostname.back() = '\0';
    }
}

struct ScopedSubmitVfsIdentity {
    ker::mod::sched::task::Task* task = nullptr;
    ker::mod::sched::task::Task::PathBuffer saved_root{};
    ker::mod::sched::task::Task::HostnameBuffer saved_submitter{};
    bool active = false;

    ScopedSubmitVfsIdentity(ker::mod::sched::task::Task* current_task, const WkiTaskIdentityContext* identity,
                            const char* submitter_hostname)
        : task(current_task) {
        if (task == nullptr) {
            return;
        }

        saved_root = task->root;
        saved_submitter = task->wki_submitter_hostname;

        if (identity != nullptr && identity->root.front() == '/') {
            task->root = identity->root;
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
    }

    ~ScopedSubmitVfsIdentity() {
        if (!active || task == nullptr) {
            return;
        }
        task->root = saved_root;
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

    info->args_len = static_cast<uint16_t>(argv_bytes + envp_bytes + info->cwd_len);
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

auto compute_local_load() -> uint16_t {
    uint16_t local_load = 0;
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::get_core_count());
    if (cpu_count > 0) {
        uint16_t total_runnable = 0;
        for (uint16_t c = 0; c < cpu_count; c++) {
            auto stats = ker::mod::sched::get_run_queue_stats(c);
            total_runnable += static_cast<uint16_t>(stats.active_task_count);
        }
        auto pct = static_cast<uint16_t>((static_cast<uint32_t>(total_runnable) * 1000U) / cpu_count);
        local_load = std::min<uint16_t>(pct, 1000);
    }
    return local_load;
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
// Returns true if the task was submitted to a remote node (local task won't run).
//
// V2 changes:
//   - VFS_REF delivery selection per A6.2
//   - Proxy task stays alive in WAITING state per A7.2
// -----------------------------------------------------------------------------

auto try_remote_placement(ker::mod::sched::task::Task* task) -> bool {
    if (task == nullptr || task->wki_skip_legacy_placement) {
        return false;
    }

    // The legacy scheduler hook runs after fork()/post_task_balanced(). Raw
    // fork children inherit exe_path from the parent but do not yet have a new
    // exec context; path-based VFS_REF placement here would incorrectly
    // relaunch the parent's binary on a remote node. Only auto-place tasks
    // that still carry an inline executable payload prepared by a real spawn.
    if (task->elf_buffer == nullptr || task->elf_buffer_size == 0) {
        return false;
    }

    WkiRemoteSpawnSpec const SPEC = {};
    return wki_try_remote_spawn(task, SPEC) == WkiRemoteSpawnResult::REMOTE;
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
        return WkiRemoteSpawnResult::LOCAL;
    }

    const bool HAS_EXE_PATH = task->exe_path.front() != '\0';
    const bool HAS_INLINE_BINARY = task->elf_buffer != nullptr && task->elf_buffer_size != 0;
    if (!HAS_EXE_PATH && !HAS_INLINE_BINARY) {
        return WkiRemoteSpawnResult::LOCAL;
    }

    // WKI_TARGET_FLAG_LOCAL: task is pinned to the local node.
    if ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) {
        return WkiRemoteSpawnResult::LOCAL;
    }

    const bool EXPLICIT_TARGET = task->wki_target_hostname.front() != '\0';
    const bool STRICT_TARGET = EXPLICIT_TARGET && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool PREFER_REMOTE = !EXPLICIT_TARGET && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE) != 0);
    const bool STRICT_REMOTE = PREFER_REMOTE && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool AUTOMATIC_PLACEMENT = !EXPLICIT_TARGET && !PREFER_REMOTE;
    const bool REMOTE_RECEIVER =
        task->wki_submitter_hostname.front() != '\0' && std::strcmp(task->wki_submitter_hostname.data(), wki_local_hostname()) != 0;

    // A task that was already submitted from another node stays on its receiver
    // for automatic follow-on execs. Explicit targets and remote-preferred
    // policies are an opt-in fan-out path used by distributed launchers.
    if (REMOTE_RECEIVER && !EXPLICIT_TARGET && !PREFER_REMOTE) {
        return WkiRemoteSpawnResult::LOCAL;
    }

    // Don't bounce tasks that are already remote-receiver instances.
    if (std::strncmp(task->name, "wki-remote", 10) == 0) {
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
            return WkiRemoteSpawnResult::LOCAL;
        }
    }

    task->wki_skip_legacy_placement = true;

    uint16_t best_node = WKI_NODE_INVALID;
    if (EXPLICIT_TARGET) {
        if (std::strncmp(task->wki_target_hostname.data(), wki_local_hostname(), task->wki_target_hostname.size()) == 0) {
            return WkiRemoteSpawnResult::LOCAL;
        }

        uint16_t const NODE_ID = wki_peer_find_by_hostname(task->wki_target_hostname.data());
        if (NODE_ID == WKI_NODE_INVALID) {
            return STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        }

        auto* peer = wki_peer_find(NODE_ID);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            return STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        }

        best_node = NODE_ID;
    } else if (PREFER_REMOTE) {
        if (!wki_ipc_find_pipe_affinity_node(task, &best_node)) {
            s_compute_lock.lock();
            best_node = wki_preferred_remote_node();
            s_compute_lock.unlock();
        }
        if (best_node == WKI_NODE_INVALID) {
            return STRICT_REMOTE ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        }
    } else {
        uint16_t const LOCAL_LOAD = compute_local_load();
        s_compute_lock.lock();
        best_node = wki_least_loaded_node(LOCAL_LOAD);
        s_compute_lock.unlock();
        if (best_node == WKI_NODE_INVALID) {
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

    // Automatic placement runs on the normal exec path and must be cheap to
    // decline. VFS_REF can wait on remote filesystem progress for the full task
    // submit timeout, so reserve it for explicit remote policies.
    bool const ALLOW_VFS_REF = !AUTOMATIC_PLACEMENT;
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
            size_t const CWDLEN = std::strlen(task->cwd.data());
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
        return STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
    }

    if (task->is_elf_buffer_shared) {
        release_cached_elf_buffer(task->elf_buffer);
    } else {
        delete[] task->elf_buffer;
    }
    task->elf_buffer = nullptr;
    task->elf_buffer_size = 0;
    task->wki_proxy_task_id = tid;
    task->wki_proxy_task = true;

    bool proxy_ready = false;
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::NONE && task != ker::mod::sched::get_current_task()) {
        proxy_ready = ker::mod::sched::post_task_waiting(task);
    }

    bool completed_immediately = false;
    int32_t completed_exit_status = 0;
    bool emit_proxy_ready_wait = false;
    uint32_t proxy_ready_wait_us = 0;
    bool emit_complete_hold = false;
    uint32_t complete_hold_us = 0;
    uint64_t const METRIC_NOW_US = wki_now_us();
    s_compute_lock.lock();
    SubmittedTask* st = find_submitted_task_any(tid);
    if (st != nullptr) {
        st->local_task = task;
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
            if (st->complete_received_at_us != 0 && METRIC_NOW_US >= st->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(METRIC_NOW_US - st->complete_received_at_us);
                st->complete_received_at_us = 0;
            }
            st->local_task = nullptr;
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
        finalize_proxy_task(task, completed_exit_status, nullptr, 0, tid);
    }
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] Task '%s' (pid=0x%lx) placed as proxy on node 0x%04x (task_id=%u)", task->name, task->pid, best_node, tid);
#endif
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
    if (binary == nullptr || binary_len == 0) {
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
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint32_t) + binary_len + context_info.data_len + IPC_DATA_LEN);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] Task binary too large for inline submit: %u bytes", binary_len);
        cleanup_ipc_exports();
        return 0;
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = g_next_task_id++;

    // Create submitted task entry
    SubmittedTask st;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.accept_status = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    st.local_task = local_task;
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);

    g_submitted_tasks.push_back(std::move(st));
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, binary_len, CALLSITE);

    // Build TASK_SUBMIT message
    auto msg_len = static_cast<uint16_t>(total);
    auto* buf = new (std::nothrow) uint8_t[msg_len];
    if (buf == nullptr) {
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf);
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
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
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
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_SUBMIT, buf, msg_len);
    delete[] buf;

    if (SEND_RET != WKI_OK) {
        bool claimed_waiter = false;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_wait_entry == &wait) {
                task_ptr->response_wait_entry = nullptr;
            }
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
        }
        claimed_waiter = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET);
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
        task_ptr->response_wait_entry = nullptr;
        accept_status = task_ptr->accept_status;
    }
    if (WAIT_RC != 0) {
        if (task_ptr != nullptr) {
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] Task submit wait failed: task_id=%u target=0x%04x rc=%d (%s)", TASK_ID, target_node, WAIT_RC,
                           errno_name(WAIT_RC));
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return 0;
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        if (task_ptr != nullptr) {
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
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
    if (vfs_path == nullptr || vfs_path[0] == '\0') {
        cleanup_ipc_exports();
        return 0;
    }

    auto path_len = static_cast<uint16_t>(std::strlen(vfs_path));
    if (path_len == 0) {
        cleanup_ipc_exports();
        return 0;
    }

    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info)) {
        cleanup_ipc_exports();
        return 0;
    }

    // TaskSubmitPayload + path_len + path + argv/envp/cwd + identity + policy + IPC fd entries.
    uint32_t const IPC_DATA_LEN = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint16_t) + path_len + context_info.data_len + IPC_DATA_LEN);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] VFS_REF path too large: %u bytes", path_len);
        cleanup_ipc_exports();
        return 0;
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = g_next_task_id++;

    SubmittedTask st;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.accept_status = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    st.local_task = local_task;
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);

    g_submitted_tasks.push_back(std::move(st));
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, path_len, CALLSITE);

    auto msg_len = static_cast<uint16_t>(total);
    auto* buf = new (std::nothrow) uint8_t[msg_len];
    if (buf == nullptr) {
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        return 0;
    }

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf);
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
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &path_len, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    memcpy(cursor, vfs_path, path_len);
    cursor += path_len;
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
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_SUBMIT, buf, msg_len);
    delete[] buf;

    if (SEND_RET != WKI_OK) {
        bool claimed_waiter = false;
        s_compute_lock.lock();
        if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
            if (task_ptr->response_wait_entry == &wait) {
                task_ptr->response_wait_entry = nullptr;
            }
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
        }
        claimed_waiter = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, claimed_waiter, SEND_RET);
        cleanup_ipc_exports();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, SEND_RET,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), path_len, CALLSITE);
        return 0;
    }

    int const WAIT_RC = wki_wait_for_op(&wait, WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    uint8_t accept_status = 0;
    s_compute_lock.lock();
    auto* task_ptr = find_submitted_task_any(TASK_ID);
    if (task_ptr != nullptr) {
        task_ptr->response_wait_entry = nullptr;
        accept_status = task_ptr->accept_status;
    }
    if (WAIT_RC != 0) {
        if (task_ptr != nullptr) {
            task_ptr->response_pending.store(false, std::memory_order_relaxed);
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] VFS_REF submit wait failed: task_id=%u rc=%d (%s) timeout_us=%llu", TASK_ID, WAIT_RC, errno_name(WAIT_RC),
                           WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), path_len, CALLSITE);
        return 0;
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        if (task_ptr != nullptr) {
            task_ptr->active = false;
        }
        s_compute_lock.unlock();
        cleanup_ipc_exports();
        ker::mod::dbg::log("[WKI] VFS_REF task rejected: task_id=%u status=%u", TASK_ID, accept_status);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID,
                                -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status),
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), path_len, CALLSITE);
        return 0;
    }
    s_compute_lock.unlock();
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] VFS_REF task accepted: task_id=%u", TASK_ID);
#endif
    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, 0,
                            static_cast<uint32_t>(wki_now_us() - STARTED_US), path_len, CALLSITE);
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

    uint16_t const TARGET_NODE = task->target_node;
    if (!task->active) {
        int32_t const COMPLETED_EXIT_STATUS = task->exit_status;
        s_compute_lock.unlock();

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
    }
    s_compute_lock.unlock();

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
            if (submitted->complete_received_at_us != 0 && METRIC_NOW_US >= submitted->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(METRIC_NOW_US - submitted->complete_received_at_us);
                submitted->complete_received_at_us = 0;
            }
            submitted->local_task = nullptr;
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
        finalize_proxy_task(task, completed_exit_status, nullptr, 0, task->wki_proxy_task_id);
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
    if (TEARDOWN_EXPORTS) {
        cleanup = submitted_ipc_cleanup_snapshot_locked(task);
    }
    s_compute_lock.unlock();

    TaskCancelPayload cancel = {};
    cancel.task_id = task_id;
    cancel.signum = signum;

    int const SEND_RET = wki_send(TARGET_NODE, WKI_CHAN_RESOURCE, MsgType::TASK_CANCEL, &cancel, sizeof(cancel));
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

}  // namespace

auto wki_proxy_task_forward_signal(ker::mod::sched::task::Task* task, int signum) -> bool {
    if (task == nullptr || task->wki_proxy_task_id == 0) {
        return false;
    }

    // Remote compute currently exposes cancel semantics, not full POSIX job
    // control. Treat terminal-generated interrupts as cancellation requests so
    // Ctrl-C can stop remote foreground work promptly.
    if (signum != WKI_SIGINT_NUM && signum != WKI_SIGKILL_NUM && signum != WKI_SIGTERM_NUM) {
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
    // Cap per-CPU array size to prevent buffer overflow
    constexpr uint16_t MAX_REPORT_CPUS = 64;
    uint16_t const REPORT_CPUS = std::min(cpu_count, MAX_REPORT_CPUS);

    LoadReportPayload report = {};
    report.num_cpus = REPORT_CPUS;

    uint16_t total_runnable = 0;
    constexpr size_t LOAD_REPORT_BUFFER_SIZE = sizeof(LoadReportPayload) + (MAX_REPORT_CPUS * sizeof(uint16_t));
    std::array<uint8_t, LOAD_REPORT_BUFFER_SIZE> buf = {};

    for (uint16_t c = 0; c < REPORT_CPUS; c++) {
        auto stats = ker::mod::sched::get_run_queue_stats(c);
        auto cpu_load = static_cast<uint16_t>(stats.active_task_count + stats.wait_queue_count);
        std::memcpy(buf.data() + sizeof(LoadReportPayload) + (static_cast<size_t>(c) * sizeof(uint16_t)), &cpu_load, sizeof(cpu_load));
        total_runnable += static_cast<uint16_t>(stats.active_task_count);
    }

    report.runnable_tasks = total_runnable;
    // avg_load_pct: scale 0-1000. Approximation: (total_runnable / num_cpus) * 1000, capped at 1000.
    if (REPORT_CPUS > 0 && total_runnable > 0) {
        uint32_t const PCT = (static_cast<uint32_t>(total_runnable) * 1000U) / REPORT_CPUS;
        report.avg_load_pct = static_cast<uint16_t>(std::min(PCT, 1000U));
    } else {
        report.avg_load_pct = 0;
    }
    report.free_mem_pages = static_cast<uint16_t>(std::min(ker::mod::mm::phys::get_free_mem_pages() / 256ULL, 0xFFFFULL));

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

void wki_remote_compute_cleanup_for_peer(uint16_t node_id) {
    constexpr size_t MAX_PROXIES_PER_BATCH = 64;
    constexpr size_t MAX_WAITERS_PER_BATCH = MAX_PROXIES_PER_BATCH * 2;

    for (;;) {
        std::array<ker::mod::sched::task::Task*, MAX_PROXIES_PER_BATCH> proxy_tasks = {};
        size_t proxy_count = 0;
        std::array<WkiWaitEntry*, MAX_WAITERS_PER_BATCH> waiters_to_finish = {};
        size_t waiter_count = 0;
        bool drained_submitted = true;

        s_compute_lock.lock();

        // Fail submitted tasks targeting this peer in bounded batches. Each
        // stack waiter is claimed while s_compute_lock still owns the published
        // pointer so timeout and late-response paths cannot touch it after the
        // submitter unwinds.
        for (auto& t : g_submitted_tasks) {
            if (!t.active || t.target_node != node_id) {
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
            if (waiter_count + needed_waiters > waiters_to_finish.size() || proxy_count + NEEDED_PROXIES > proxy_tasks.size()) {
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
                *std::next(proxy_tasks.begin(), static_cast<ptrdiff_t>(proxy_count++)) = t.local_task;
                t.local_task = nullptr;
            }

            t.active = false;
        }

        s_compute_lock.unlock();

        for (size_t i = 0; i < waiter_count; i++) {
            wki_finish_claimed_op(*std::next(waiters_to_finish.begin(), static_cast<ptrdiff_t>(i)), -1);
        }

        for (size_t i = 0; i < proxy_count; i++) {
            auto* proxy = *std::next(proxy_tasks.begin(), static_cast<ptrdiff_t>(i));
            finalize_proxy_task(proxy, -1, nullptr, 0, 0);

            ker::mod::dbg::log("[WKI] Proxy task cleanup: pid=0x%lx (peer 0x%04x)", proxy->pid, node_id);
        }

        if (drained_submitted) {
            break;
        }
    }

    s_compute_lock.lock();

    // Invalidate load cache for this peer
    std::erase_if(g_remote_loads, [node_id](const RemoteNodeLoad& rl) { return rl.node_id == node_id; });

    // Cancel running remote tasks submitted by this peer (they'll exit on their own,
    // but we won't be able to send TASK_COMPLETE back)
    for (auto& rt : g_running_remote_tasks) {
        if (rt.active && rt.submitter_node == node_id) {
            delete rt.output;
            rt.output = nullptr;
            if (rt.task != nullptr) {
                rt.task->release();
                rt.task = nullptr;
            }
            rt.active = false;
        }
    }
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
        entry.valid = false;
    }
    gc_shared_elf_cache_locked(wki_now_us());

    s_compute_lock.unlock();
}

#ifdef WOS_SELFTEST
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
    std::erase_if(g_submitted_tasks, [](const SubmittedTask& submitted) { return submitted.task_id == TASK_ID; });
    g_submitted_tasks.push_back(std::move(task));
    s_compute_lock.unlock();

    wki_remote_compute_cleanup_for_peer(TARGET_NODE);

    bool ok = false;
    s_compute_lock.lock();
    for (const auto& submitted : g_submitted_tasks) {
        if (submitted.task_id == TASK_ID) {
            ok = !submitted.active && submitted.exit_status == -1 && !submitted.complete_pending.load(std::memory_order_relaxed);
            break;
        }
    }
    std::erase_if(g_submitted_tasks, [](const SubmittedTask& submitted) { return submitted.task_id == TASK_ID; });
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

    SubmittedTask task;
    task.active = false;
    task.task_id = TASK_ID;
    task.target_node = TARGET_NODE;
    task.exit_status = EXIT_STATUS;

    s_compute_lock.lock();
    std::erase_if(g_submitted_tasks, [](const SubmittedTask& submitted) { return submitted.task_id == TASK_ID; });
    g_submitted_tasks.push_back(std::move(task));
    s_compute_lock.unlock();

    int32_t observed_status = -1;
    int const WAIT_RC = wki_task_wait(TASK_ID, &observed_status, 0);

    s_compute_lock.lock();
    std::erase_if(g_submitted_tasks, [](const SubmittedTask& submitted) { return submitted.task_id == TASK_ID; });
    s_compute_lock.unlock();

    return WAIT_RC == 0 && observed_status == EXIT_STATUS;
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
    task.complete_pending.store(true, std::memory_order_release);

    bool const BUSY_REJECTED = !publish_task_complete_waiter_locked(&task, contender_wait) && task.complete_wait_entry == &pending_wait &&
                               task.complete_pending.load(std::memory_order_acquire);

    int32_t observed_status = 0;
    bool const STALE_OWNED = clear_task_complete_waiter_after_wait_locked(&task, stale_wait, WKI_ERR_TIMEOUT, observed_status);
    bool const SUCCESSOR_PRESERVED = !STALE_OWNED && task.complete_wait_entry == &pending_wait &&
                                     task.complete_pending.load(std::memory_order_acquire) && observed_status == task.exit_status;

    task.complete_wait_entry = nullptr;
    task.complete_pending.store(false, std::memory_order_release);
    bool const PUBLISHED_AFTER_CLEAR = publish_task_complete_waiter_locked(&task, owned_wait) && task.complete_wait_entry == &owned_wait &&
                                       task.complete_pending.load(std::memory_order_acquire);

    bool const OWNED_CLEARED = clear_task_complete_waiter_after_wait_locked(&task, owned_wait, WKI_ERR_TIMEOUT, observed_status) &&
                               task.complete_wait_entry == nullptr && !task.complete_pending.load(std::memory_order_acquire) &&
                               observed_status == task.exit_status;

    return BUSY_REJECTED && SUCCESSOR_PRESERVED && PUBLISHED_AFTER_CLEAR && OWNED_CLEARED;
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
    local_counts.submitted_total = g_submitted_tasks.size();
    for (const auto& task : g_submitted_tasks) {
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

void wki_remote_compute_check_completions() {
    if (!g_remote_compute_initialized) {
        return;
    }

    s_compute_lock.lock();

    for (auto& rt : g_running_remote_tasks) {
        if (!rt.active) {
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

        g_pending_task_completions.push_back(PendingTaskCompletion{.task_id = rt.task_id,
                                                                   .submitter_node = rt.submitter_node,
                                                                   .local_pid = rt.local_pid,
                                                                   .exit_status = exit_status,
                                                                   .output = rt.output});
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
    for (;;) {
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

        bool sent = false;
        // D19: Build TASK_COMPLETE with captured output
        uint16_t const OUT_LEN = (info.output != nullptr) ? info.output->len : static_cast<uint16_t>(0);
        auto msg_len = static_cast<uint16_t>(sizeof(TaskCompletePayload) + OUT_LEN);
        auto* buf = new (std::nothrow) uint8_t[msg_len];
        if (buf != nullptr) {
            auto* complete = reinterpret_cast<TaskCompletePayload*>(buf);
            complete->task_id = info.task_id;
            complete->exit_status = info.exit_status;
            complete->output_len = OUT_LEN;

            if (OUT_LEN > 0 && info.output != nullptr) {
                memcpy(buf + sizeof(TaskCompletePayload), info.output->data.data(), OUT_LEN);
            }

            sent = (wki_send(info.submitter_node, WKI_CHAN_RESOURCE, MsgType::TASK_COMPLETE, buf, msg_len) == WKI_OK);
            delete[] buf;
        }

        if (!sent) {
            s_compute_lock.lock();
            g_pending_task_completions.push_front(info);
            s_compute_lock.unlock();
#if WKI_DEBUG
            ker::mod::dbg::log("[WKI] TASK_COMPLETE send deferred: task_id=%u pid=0x%lx submitter=0x%04x", info.task_id, info.local_pid,
                               info.submitter_node);
#endif
            break;
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
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (stack_base == 0) {
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    uint64_t const KERNEL_RSP = stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    // Create the process task
    auto* new_task = new ker::mod::sched::task::Task(  // NOLINT(cppcoreguidelines-owning-memory)
        "wki-remote", reinterpret_cast<uint64_t>(elf_buffer), KERNEL_RSP, ker::mod::sched::task::TaskType::PROCESS);

    if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
        delete new_task;
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    new_task->elf_buffer = elf_buffer;
    new_task->elf_buffer_size = binary_len;
    new_task->is_elf_buffer_shared = shared_elf_buffer;

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

    // Note: Caller is responsible for calling post_task_balanced() after
    // setting up argv/envp/cwd on the user stack.
    result.task = new_task;
    result.output = output_cap;
    return result;
}

// ---------------------------------------------------------------------------
// D14: Load ELF binary from a VFS path. Returns owned buffer + size.
// ---------------------------------------------------------------------------

struct VfsLoadResult {
    uint8_t* buffer = nullptr;
    uint32_t size = 0;
    TaskRejectReason reject_reason = TaskRejectReason::FETCH_FAILED;
    bool shared = false;
};

auto load_elf_from_vfs_path(const char* path, uint16_t submitter_node, uint32_t correlation) -> VfsLoadResult {
    VfsLoadResult result;
    ScopedComputeMeasure load_measure(ker::mod::perf::WkiPerfComputeOp::LOAD_ELF, submitter_node, correlation,
                                      path != nullptr ? static_cast<uint32_t>(std::strlen(path)) : 0U, WOS_PERF_CALLSITE());
    if (path == nullptr || path[0] == '\0') {
        result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
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

    ker::vfs::Stat cache_key = statbuf;
    bool is_loader = false;
    uint64_t const INFLIGHT_DEADLINE_US = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);

    auto fail_inflight_load = [&]() {
        if (!is_loader) {
            return;
        }

        s_compute_lock.lock();
        if (auto* inflight = find_shared_elf_cache_entry_locked(submitter_node, resolved_path, cache_key); inflight != nullptr) {
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
        gc_shared_elf_cache_locked(wki_now_us());

        if (auto* cached = find_shared_elf_cache_locked(submitter_node, resolved_path, cache_key); cached != nullptr) {
            cached->refcount++;
            cached->last_used_us = wki_now_us();
            result.buffer = cached->buffer;
            result.size = cached->size;
            result.shared = true;
            s_compute_lock.unlock();
            load_measure.finish(0, result.size);
            return result;
        }

        auto* inflight = find_shared_elf_cache_entry_locked(submitter_node, resolved_path, cache_key);
        if (inflight != nullptr) {
            if (inflight->loading) {
                s_compute_lock.unlock();
                if (wki_now_us() >= INFLIGHT_DEADLINE_US) {
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
        std::strncpy(pending.path.data(), resolved_path, pending.path.size() - 1);
        pending.freshness = cache_key;
        pending.last_used_us = wki_now_us();
        g_shared_elf_cache.push_back(pending);
        is_loader = true;
        s_compute_lock.unlock();
        break;
    }

    auto file_size = static_cast<size_t>(statbuf.st_size);
    uint8_t* buf = nullptr;
    size_t total_read = 0;
    size_t final_file_size = file_size;
    bool load_ok = false;
    uint64_t const RETRY_WINDOW_START_US = wki_now_us();
    uint64_t const RETRY_DEADLINE_US = wki_future_deadline_us(RETRY_WINDOW_START_US, WKI_VFS_LOAD_RETRY_WINDOW_US);

    for (uint32_t attempt = 0; attempt < WKI_VFS_LOAD_MAX_ATTEMPTS; attempt++) {
        uint64_t const NOW_US = wki_now_us();
        bool const RETRY_WINDOW_OPEN = NOW_US < RETRY_DEADLINE_US;

        if (attempt > 0 && !RETRY_WINDOW_OPEN) {
            break;
        }

        if (attempt > 0) {
            if (!using_disconnected_host_fallback &&
                fallback_to_local_path_for_disconnected_wki_host(resolved_path, fallback_local_path.data(), fallback_local_path.size())) {
                resolved_path = fallback_local_path.data();
                using_disconnected_host_fallback = true;
            }

            ker::vfs::Stat retry_stat = {};
            if (ker::vfs::vfs_stat(resolved_path, &retry_stat) != 0 || retry_stat.st_size <= 0) {
                if (RETRY_WINDOW_OPEN && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                    uint64_t const WAIT_UNTIL_US = wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US);
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
            file_size = static_cast<size_t>(retry_stat.st_size);
        }

        int open_flags = ker::vfs::O_NOTIFY_CACHE_CHANGE;
        if (using_disconnected_host_fallback) {
            open_flags |= ker::vfs::O_LOCAL;
        }
        int const FD = ker::vfs::vfs_open(resolved_path, open_flags, 0);
        if (FD < 0) {
            if (RETRY_WINDOW_OPEN && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                uint64_t const WAIT_UNTIL_US = wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US);
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
            uint64_t const WAIT_UNTIL_US = wki_future_deadline_us(wki_now_us(), WKI_VFS_LOAD_RETRY_BACKOFF_US);
            while (wki_now_us() < WAIT_UNTIL_US) {
                sleep_until_us(WAIT_UNTIL_US, WKI_VFS_LOAD_BACKOFF_POLL_US);
            }
        }
    }

    if (!load_ok) {
        fail_inflight_load();
        ker::mod::dbg::log("[WKI] VFS_REF: short read for '%s' (%zu/%zu bytes)", resolved_path, total_read, final_file_size);
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason), total_read);
        return result;
    }

#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] VFS_REF: loaded '%s' %zu bytes hdr=[%02x %02x %02x %02x]", resolved_path, total_read, buf[0], buf[1], buf[2],
                       buf[3]);
#endif

    s_compute_lock.lock();
    gc_shared_elf_cache_locked(wki_now_us());
    if (auto* cached = find_shared_elf_cache_locked(submitter_node, resolved_path, cache_key); cached != nullptr) {
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

    auto* inflight = find_shared_elf_cache_entry_locked(submitter_node, resolved_path, cache_key);
    if (is_loader && inflight != nullptr && inflight->loading) {
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

    SharedElfCacheEntry cache_entry = {};
    cache_entry.valid = true;
    cache_entry.loading = false;
    cache_entry.load_status = 0;
    cache_entry.submitter_node = submitter_node;
    std::strncpy(cache_entry.path.data(), resolved_path, cache_entry.path.size() - 1);
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
void handle_task_submit_work(uint16_t src_node, const uint8_t* payload, uint16_t payload_len) {
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
    uint32_t binary_len = 0;
    TaskRejectReason reject_reason = TaskRejectReason::ACCEPTED;
    bool elf_buffer_shared = false;
    std::array<char, 256> exe_path_buf = {};
    const uint8_t* submit_context = nullptr;
    uint16_t submit_context_len = 0;
    WkiTaskIdentityContext submitted_identity = {};
    bool has_submitted_identity = false;
    std::strncpy(exe_path_buf.data(), "wki-remote", exe_path_buf.size() - 1);

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

            if (binary_len == 0 || binary_len + sizeof(uint32_t) > var_len) {
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

            if (path_len == 0 || sizeof(uint16_t) + path_len > var_len) {
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }

            // Build null-terminated path string
            std::array<char, 512> path_buf = {};
            auto copy_len = std::min<uint16_t>(path_len, static_cast<uint16_t>(path_buf.size() - 1));
            memcpy(path_buf.data(), var_data + sizeof(uint16_t), copy_len);
            *std::next(path_buf.begin(), static_cast<ptrdiff_t>(copy_len)) = '\0';

            auto vfs_result = load_elf_from_vfs_path(path_buf.data(), hdr->src_node, submit->task_id);
            if (vfs_result.buffer == nullptr) {
                ker::mod::dbg::log("[WKI] Task submit VFS_REF load failed: task_id=%u path='%s' reason=%u", submit->task_id,
                                   path_buf.data(), static_cast<uint8_t>(vfs_result.reject_reason));
                reject_reason = vfs_result.reject_reason;
                break;
            }
            elf_buffer = vfs_result.buffer;
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

            if (path_len == 0 || off + path_len > var_len) {
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
            auto vfs_result = load_elf_from_vfs_path(ref_path.data(), hdr->src_node, submit->task_id);
            if (vfs_result.buffer == nullptr) {
                ker::mod::dbg::log("[WKI] RESOURCE_REF: failed to load node=0x%04x res=%u path='%s'", ref_node, ref_resource,
                                   ref_path.data());
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }
            elf_buffer = vfs_result.buffer;
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
    if (INVALID_SUBMIT_CONTEXT) {
        reject_reason = TaskRejectReason::FETCH_FAILED;
    } else if (submit->identity_len == WkiTaskIdentityContext::V1_SIZE || submit->identity_len == sizeof(WkiTaskIdentityContext)) {
        memcpy(&submitted_identity, submit_context + submit->args_len, submit->identity_len);
        has_submitted_identity = true;
    }

    // If binary loading failed, reject
    if (elf_buffer == nullptr || reject_reason != TaskRejectReason::ACCEPTED) {
        release_loaded_elf_buffer(elf_buffer, elf_buffer_shared);
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(reject_reason);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit reject before exec: task_id=%u reason=%u", submit->task_id,
                           static_cast<uint8_t>(reject_reason));
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(reject_reason), binary_len);
        return;
    }

    // Execute the ELF buffer (creates task but does NOT schedule it yet)
    auto* current_task = ker::mod::sched::get_current_task();
    const char* effective_submitter_hostname = (has_submitted_identity && submitted_identity.submitter_hostname.front() != '\0')
                                                   ? submitted_identity.submitter_hostname.data()
                                                   : wki_peer_get_hostname(hdr->src_node);
    ScopedSubmitVfsIdentity const SUBMIT_VFS_IDENTITY(current_task, has_submitted_identity ? &submitted_identity : nullptr,
                                                      effective_submitter_hostname);
    ExecResult const EXEC = exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared);
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
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
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
    bool parse_ok = true;

    // IPC fd entries are appended after args+identity+policy in the submit context.
    // Subtract their size so policy_len covers only the actual VFS rules.
    const uint8_t* identity_cursor = submit_context + submit->args_len;
    const uint8_t* policy_cursor = identity_cursor + submit->identity_len;
    auto const POLICY_LEN = static_cast<uint16_t>(context_without_ipc - submit->args_len - submit->identity_len);

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

    if (parse_ok && !deserialize_task_vfs_rules(new_task, policy_cursor, POLICY_LEN)) {
        parse_ok = false;
    }

    if (!parse_ok) {
        delete[] argv_strings;
        delete[] envp_strings;
        delete new_task;
        delete EXEC.output;

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::FETCH_FAILED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit parse failed: task_id=%u", submit->task_id);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
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
        delete new_task;
        delete EXEC.output;

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit stack setup failed: task_id=%u pid=0x%lx", submit->task_id, FAILED_PID);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
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
    if (new_task->try_acquire()) {
        completion_task_ref = new_task;
    } else {
        delete new_task;
        delete EXEC.output;
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit ref failed: task_id=%u pid=0x%lx", submit->task_id, LAUNCHED_PID);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    // Post to scheduler
    if (!ker::mod::sched::post_task_balanced(new_task)) {
        completion_task_ref->release();
        delete new_task;
        delete EXEC.output;
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit post failed: task_id=%u pid=0x%lx", submit->task_id, LAUNCHED_PID);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }
#ifdef DEBUG_WKI_COMPUTE
    // post_task_balanced sets task->cpu before returning; capture it here
    // before the task can complete and be reclaimed.
    launched_cpu = static_cast<int>(new_task->cpu);
#endif

    // Track for completion monitoring (use saved PID, not exec.task->pid)
    RunningRemoteTask rt;
    rt.active = true;
    rt.task_id = submit->task_id;
    rt.submitter_node = hdr->src_node;
    rt.local_pid = LAUNCHED_PID;
    rt.task = completion_task_ref;
    rt.output = EXEC.output;
    s_compute_lock.lock();
    g_running_remote_tasks.push_back(rt);
    s_compute_lock.unlock();

    // Send TASK_ACCEPT
    TaskResponsePayload accept = {};
    accept.task_id = submit->task_id;
    accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
    accept.remote_pid = LAUNCHED_PID;
    wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_ACCEPT, &accept, sizeof(accept));
    handle_measure.finish(0, binary_len);
#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] Remote task launched: task_id=%u pid=0x%lx on CPU %d mode=%u", submit->task_id, launched_pid, launched_cpu,
                       submit->delivery_mode);
#endif
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Dedicated kernel thread for processing VFS_REF/RESOURCE_REF task submits.
// These operations perform blocking remote VFS reads (stat/open/read) which
// can each take up to 15s on timeout.  Running them from the timer tick's
// deferred-work section serialised them behind zone creations and mount
// retries, easily exceeding the 60s submit timeout on the submitter side.
// A dedicated thread lets VFS_REF processing proceed independently.
// ---------------------------------------------------------------------------

void drain_pending_task_submits() {
    while (true) {
        s_pending_submit_lock.lock();
        if (g_pending_task_submits.empty()) {
            s_pending_submit_lock.unlock();
            return;
        }
        auto pending = g_pending_task_submits.front();
        g_pending_task_submits.pop_front();
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

        handle_task_submit_work(pending.src_node, pending.payload, pending.payload_len);
        delete[] pending.payload;
    }
}

[[noreturn]] void wki_compute_submit_thread() {
    for (;;) {
        drain_pending_task_submits();

        // Sleep until woken by wki_remote_compute_notify_pending_submit().
        // No polling — kern_wake() delivers an immediate wakeup when a new
        // VFS_REF/RESOURCE_REF submit is queued.
        s_pending_submit_lock.lock();
        bool const EMPTY = g_pending_task_submits.empty();
        s_pending_submit_lock.unlock();
        if (EMPTY) {
            ker::mod::sched::kern_block();
        }
    }
}

}  // namespace

void wki_remote_compute_start_submit_thread() {
    auto* task = ker::mod::sched::task::Task::create_kernel_thread("wki_compute", wki_compute_submit_thread);
    if (task == nullptr) {
        ker::mod::dbg::log("[WKI] Failed to create compute submit kernel thread");
        return;
    }
    promote_latency_sensitive_daemon(task);
    s_compute_submit_task = task;
    ker::mod::sched::post_task_balanced(task);
    ker::mod::dbg::log("[WKI] Compute submit thread started (PID %d)", task->pid);
}

void wki_remote_compute_notify_pending_submit() {
    if (s_compute_submit_task != nullptr) {
        ker::mod::sched::kern_wake(s_compute_submit_task);
    }
}

void wki_remote_compute_process_pending_submits() { drain_pending_task_submits(); }

// ---------------------------------------------------------------------------
// detail:: RX handlers (called from NAPI dispatch in wki_rx)
// ---------------------------------------------------------------------------

namespace detail {

void handle_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskSubmitPayload)) {
        return;
    }

    const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(payload);
    auto mode = static_cast<TaskDeliveryMode>(submit->delivery_mode);

    // VFS_REF and RESOURCE_REF delivery modes call load_elf_from_vfs_path()
    // which does blocking remote VFS reads through vfs_proxy_send_and_wait().
    // Those waits call wki_spin_yield() -> inline NAPI draining, causing NAPI
    // re-entrance when dispatched from wki_rx().  Defer to the dedicated
    // compute submit thread where blocking VFS I/O is safe.
    if (mode == TaskDeliveryMode::VFS_REF || mode == TaskDeliveryMode::RESOURCE_REF) {
        auto* copy = new (std::nothrow) uint8_t[payload_len];
        if (copy == nullptr) {
            TaskResponsePayload reject = {};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
            reject.remote_pid = 0;
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
            return;
        }
        std::memcpy(copy, payload, payload_len);

        PendingTaskSubmit pending{};
        pending.src_node = hdr->src_node;
        pending.payload = copy;
        pending.payload_len = payload_len;
        pending.queued_at_us = wki_now_us();

        s_pending_submit_lock.lock();
        g_pending_task_submits.push_back(pending);
        s_pending_submit_lock.unlock();

        wki_remote_compute_notify_pending_submit();
        return;
    }

    // INLINE delivery has no blocking VFS I/O — safe to handle directly.
    handle_task_submit_work(hdr->src_node, payload, payload_len);
}

void handle_task_accept(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || !task->response_pending.load(std::memory_order_acquire)) {
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

void handle_task_reject(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || !task->response_pending.load(std::memory_order_acquire)) {
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

void handle_task_complete(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskCompletePayload)) {
        return;
    }

    const auto* comp = reinterpret_cast<const TaskCompletePayload*>(payload);
    WkiWaitEntry* complete_waiter = nullptr;
    bool emit_task_runtime = false;
    uint32_t task_runtime_us = 0;
    uint64_t const COMPLETE_NOW_US = wki_now_us();
    ker::mod::sched::task::Task* proxy = nullptr;
    ker::mod::sched::task::Task* local_task_for_output = nullptr;
    WkiWaitEntry* response_waiter = nullptr;

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(comp->task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] TASK_COMPLETE: no submitted task for task_id=%u", comp->task_id);
        return;
    }

    task->exit_status = comp->exit_status;
    if (task->accepted_at_us != 0 && COMPLETE_NOW_US >= task->accepted_at_us) {
        emit_task_runtime = true;
        task_runtime_us = static_cast<uint32_t>(COMPLETE_NOW_US - task->accepted_at_us);
    }

    proxy = task->proxy_ready ? task->local_task : nullptr;
    if (proxy != nullptr) {
        task->local_task = nullptr;
        task->complete_received_at_us = 0;
    } else {
        task->complete_received_at_us = COMPLETE_NOW_US;
        local_task_for_output = task->local_task;
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
        uint16_t out_len = comp->output_len;
        if (out_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
            auto avail = static_cast<uint16_t>(payload_len - sizeof(TaskCompletePayload));
            out_len = std::min(out_len, avail);
            finalize_proxy_task(proxy, comp->exit_status, output_data, out_len, comp->task_id);
        } else {
            finalize_proxy_task(proxy, comp->exit_status, nullptr, 0, comp->task_id);
        }
#ifdef DEBUG_WKI_COMPUTE
        ker::mod::dbg::log("[WKI] Proxy task pid=0x%lx completed: exit=%d (remote task_id=%u)", proxy->pid, comp->exit_status,
                           comp->task_id);
#endif
    } else {
        // proxy_ready=false: the proxy task hasn't blocked yet (it's still
        // in exec return / deferred-switch setup).  Record exit_status so
        // that the submit path or wki_proxy_task_blocked() can finalize when
        // it observes task->active became false. If local_task is set, also
        // forward any captured output.
#ifdef DEBUG_WKI_COMPUTE
        ker::mod::dbg::log("[WKI] TASK_COMPLETE early (proxy not ready): task_id=%u exit=%d local_task=%p", comp->task_id,
                           comp->exit_status, local_task_for_output);
#endif
        if (local_task_for_output != nullptr) {
            const uint8_t* output_data = payload + sizeof(TaskCompletePayload);
            uint16_t out_len = comp->output_len;
            if (out_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
                auto avail = static_cast<uint16_t>(payload_len - sizeof(TaskCompletePayload));
                out_len = std::min(out_len, avail);
                write_proxy_output(local_task_for_output, output_data, out_len, comp->task_id);
            }
        }
#ifdef DEBUG_WKI_COMPUTE
        else if (comp->output_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
            ker::mod::dbg::log("[WKI] Task %u output (%u bytes)", comp->task_id, comp->output_len);
        }
#endif
    }

    if (complete_waiter != nullptr) {
        wki_finish_claimed_op(complete_waiter, 0);
    }
    if (response_waiter != nullptr) {
        wki_finish_claimed_op(response_waiter, 0);
    }

    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::COMPLETE, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID, comp->task_id,
                              comp->exit_status, comp->output_len, WOS_PERF_CALLSITE());
}

void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(uint32_t)) {
        return;
    }

    const auto* cancel = reinterpret_cast<const TaskCancelPayload*>(payload);
    int const SIGNUM = payload_len >= sizeof(TaskCancelPayload) ? cancel->signum : WKI_SIGKILL_NUM;

    // D18: Find the running task and extract fields under lock
    s_compute_lock.lock();
    RunningRemoteTask const* rt = find_running_task(cancel->task_id, hdr->src_node);
    if (rt == nullptr) {
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] Task cancel: no matching running task task_id=%u from 0x%04x", cancel->task_id, hdr->src_node);
        return;
    }

    uint64_t const LOCAL_PID = rt->local_pid;
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
