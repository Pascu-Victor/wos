#include "remote_compute.hpp"

#include <extern/elf.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <net/wki/peer.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <util/errno_name.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sys/spinlock.hpp"
#include "util/hcf.hpp"

namespace ker::net::wki {

// ===============================================================================
// Storage
// ===============================================================================

namespace {

std::deque<SubmittedTask> g_submitted_tasks;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteNodeLoad> g_remote_loads;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RunningRemoteTask> g_running_remote_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_task_id = 1;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_compute_initialized = false;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_load_report_us = 0;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_compute_lock;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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
    ker::vfs::stat freshness = {};
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
constexpr size_t WKI_VFS_LOAD_CHUNK = 262144;                  // 256 KB: fewer round-trips for remote binary fetch
constexpr uint32_t WKI_VFS_LOAD_IDLE_RETRIES = 8;
constexpr uint32_t WKI_VFS_LOAD_MAX_ATTEMPTS = 24;
constexpr uint64_t WKI_VFS_LOAD_RETRY_WINDOW_US = 15000000;
constexpr uint64_t WKI_VFS_LOAD_RETRY_BACKOFF_US = 750000;
constexpr size_t WKI_EXEC_CACHE_MAX_ENTRIES = 8;
constexpr uint64_t WKI_EXEC_CACHE_MAX_BYTES = 32ULL * 1024ULL * 1024ULL;
constexpr uint64_t WKI_EXEC_CACHE_RETENTION_US = 30000000;
constexpr int32_t WKI_SIGKILL_NUM = 9;
constexpr int32_t WKI_SIGTERM_NUM = 15;
constexpr uint64_t WKI_SIGCHLD_NUM = 17;
constexpr int32_t WKI_WAIT_STATUS_EXIT_SHIFT = 8;
constexpr int32_t WKI_WAIT_STATUS_FAILURE_CODE = 255;

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

auto shared_elf_path_matches(const SharedElfCacheEntry& entry, const char* path) -> bool {
    return path != nullptr && std::strncmp(entry.path.data(), path, entry.path.size()) == 0;
}

auto shared_elf_freshness_matches(const ker::vfs::stat& lhs, const ker::vfs::stat& rhs) -> bool {
    return lhs.st_ino == rhs.st_ino && lhs.st_size == rhs.st_size && lhs.st_mtim.tv_sec == rhs.st_mtim.tv_sec &&
           lhs.st_mtim.tv_nsec == rhs.st_mtim.tv_nsec && lhs.st_ctim.tv_sec == rhs.st_ctim.tv_sec &&
           lhs.st_ctim.tv_nsec == rhs.st_ctim.tv_nsec;
}

auto find_shared_elf_cache_locked(uint16_t submitter_node, const char* path, const ker::vfs::stat& freshness) -> SharedElfCacheEntry* {
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

auto find_shared_elf_cache_entry_locked(uint16_t submitter_node, const char* path, const ker::vfs::stat& freshness)
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
        bool expired = it->refcount == 0 && (!it->valid || (it->last_used_us != 0 && now_us >= it->last_used_us &&
                                                            (now_us - it->last_used_us) >= WKI_EXEC_CACHE_RETENTION_US));
        if (!expired) {
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
    uint16_t space = WKI_TASK_MAX_OUTPUT - cap->len;
    if (space == 0) {
        return static_cast<ssize_t>(count);  // silently drop overflow
    }
    auto to_copy = static_cast<uint16_t>(std::min(static_cast<size_t>(space), count));
    memcpy(&cap->data[cap->len], buf, to_copy);
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
};

void close_proxy_fd_table(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    for (unsigned fd = 0; fd < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++fd) {
        auto* file = ker::vfs::vfs_get_file(task, static_cast<int>(fd));
        if (file == nullptr) {
            continue;
        }

        file->refcount--;
        ker::vfs::vfs_release_fd(task, static_cast<int>(fd));

        if (file->refcount <= 0) {
            if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
                file->fops->vfs_close(file);
            }
            if (file->vfs_path != nullptr) {
                ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(file->vfs_path));
            }
            ker::mod::mm::dyn::kmalloc::free(file);
        }
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
    switch (exit_status) {
        case 128 + WKI_SIGKILL_NUM:
            return -WKI_SIGKILL_NUM;
        case 128 + WKI_SIGTERM_NUM:
            return -WKI_SIGTERM_NUM;
        default:
            return exit_status;
    }
}

void cleanup_proxy_resources(ker::mod::sched::task::Task* task) {
    if (task == nullptr || task->isThread) {
        return;
    }

    close_proxy_fd_table(task);
    if (task->elfBuffer != nullptr) {
        if (!release_cached_elf_buffer(task->elfBuffer)) {
            delete[] task->elfBuffer;
        }
        task->elfBuffer = nullptr;
        task->elfBufferSize = 0;
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

    int32_t wait_status = encode_remote_wait_status(exit_status);

    for (size_t i = 0; i < proxy->awaitee_on_exit.size(); ++i) {
        uint64_t waiting_pid = proxy->awaitee_on_exit[i];
        auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(waiting_pid);
        if (waiting_task != nullptr) {
            if (!waiting_task->deferredTaskSwitch) {
                waiting_task->context.regs.rax = proxy->pid;
                if (waiting_task->waitStatusPhysAddr != 0) {
                    auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::get_virt_pointer(waiting_task->waitStatusPhysAddr));
                    *status_ptr = wait_status;
                }
            }
            uint64_t cpu = ker::mod::sched::get_least_loaded_cpu();
            ker::mod::sched::reschedule_task_for_cpu(cpu, waiting_task);
            waiting_task->release();
        }
    }
    proxy->awaitee_on_exit.clear();
}

void finalize_proxy_task(ker::mod::sched::task::Task* proxy, int32_t exit_status, const uint8_t* output_data, uint16_t output_len,
                         uint32_t task_id) {
    if (proxy == nullptr) {
        return;
    }

    if (!proxy->transitionState(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
        auto state = proxy->state.load(std::memory_order_acquire);
        if (state == ker::mod::sched::task::TaskState::DEAD || state == ker::mod::sched::task::TaskState::EXITING ||
            proxy->schedQueue == ker::mod::sched::task::Task::SchedQueue::DEAD_GC) {
            return;
        }
        return;
    }

    int32_t wait_status = encode_remote_wait_status(exit_status);

    write_proxy_output(proxy, output_data, output_len, task_id);

    proxy->exitStatus = wait_status;
    proxy->hasExited = true;
    proxy->wki_proxy_task_id = 0;

    // Send SIGCHLD to parent and wake it if blocked in waitpid(-1, ...).
    // The normal exit path (wos_proc_exit) does this, but proxy tasks never
    // go through wos_proc_exit — they are finalized here when TASK_COMPLETE
    // arrives.  Without SIGCHLD the parent (e.g. busybox shell calling
    // waitpid(-1,...)) would block forever because nobody added it to
    // proxy->awaitee_on_exit.
    if (!proxy->isThread && proxy->parentPid != 0) {
        auto* parent = ker::mod::sched::find_task_by_pid_safe(proxy->parentPid);
        if (parent != nullptr) {
            parent->sigPending |= (1ULL << (WKI_SIGCHLD_NUM - 1));

            static constexpr auto WAIT_ANY_CHILD = static_cast<uint64_t>(-1);
            if (parent->waitingForPid == WAIT_ANY_CHILD && !parent->deferredTaskSwitch) {
                parent->context.regs.rax = proxy->pid;
                if (parent->waitStatusPhysAddr != 0) {
                    auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::get_virt_pointer(parent->waitStatusPhysAddr));
                    *status_ptr = wait_status;
                }
                parent->waitingForPid = 0;
                proxy->waitedOn = true;
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            } else if (parent->deferredTaskSwitch || parent->voluntaryBlock) {
                uint64_t cpu = parent->cpu;
                if (cpu >= ker::mod::smt::get_core_count()) {
                    cpu = ker::mod::sched::get_least_loaded_cpu();
                }
                ker::mod::sched::reschedule_task_for_cpu(cpu, parent);
            }
            parent->release();
        }
    }

    wake_proxy_waiters(proxy, exit_status);
    cleanup_proxy_resources(proxy);

    proxy->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
    proxy->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
    ker::mod::sched::insert_into_dead_list(proxy);
}

struct SubmitContextInfo {
    uint16_t argc = 0;
    uint16_t envc = 0;
    uint16_t cwd_len = 0;
    uint16_t args_len = 0;
    uint16_t policy_len = 0;
    uint16_t data_len = 0;
};

auto serialized_task_vfs_rules_size(const ker::mod::sched::task::Task* task) -> uint16_t {
    if (task == nullptr || task->wki_vfs_rules.size() == 0) {
        return 0;
    }

    uint32_t total = sizeof(uint16_t);
    for (size_t i = 0; i < task->wki_vfs_rules.size(); ++i) {
        const auto& rule = task->wki_vfs_rules[i];
        if (rule.prefix_len == 0 || rule.prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || rule.prefix[0] != '/' ||
            rule.prefix[rule.prefix_len] != '\0') {
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

    for (size_t i = 0; i < task->wki_vfs_rules.size(); ++i) {
        const auto& rule = task->wki_vfs_rules[i];
        if (rule.prefix_len == 0 || rule.prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || rule.prefix[0] != '/' ||
            rule.prefix[rule.prefix_len] != '\0') {
            continue;
        }

        *dest++ = rule.route;
        *dest++ = 0;
        memcpy(dest, &rule.prefix_len, sizeof(uint16_t));
        dest += sizeof(uint16_t);
        memcpy(dest, rule.prefix, rule.prefix_len);
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

        uint8_t route = data[0];
        uint16_t prefix_len = 0;
        memcpy(&prefix_len, data + sizeof(uint8_t) + sizeof(uint8_t), sizeof(uint16_t));
        data += sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t);
        data_len = static_cast<uint16_t>(data_len - (sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint16_t)));

        if (prefix_len == 0 || prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX || data_len < prefix_len ||
            (route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
             route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST))) {
            return false;
        }

        ker::mod::sched::task::WkiVfsRule new_rule{};
        memcpy(new_rule.prefix, data, prefix_len);
        new_rule.prefix[prefix_len] = '\0';
        new_rule.prefix_len = prefix_len;
        new_rule.route = route;
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
            size_t len = std::strlen(strings[count]) + 1;
            if (len > 0xFFFFU || bytes > static_cast<uint16_t>(0xFFFFU - len) || count == 0xFFFFU) {
                return false;
            }
            bytes = static_cast<uint16_t>(bytes + len);
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
        size_t cwd_len = std::strlen(cwd) + 1;
        if (cwd_len > 0xFFFFU) {
            return false;
        }
        info->cwd_len = static_cast<uint16_t>(cwd_len);
    }

    info->args_len = static_cast<uint16_t>(argv_bytes + envp_bytes + info->cwd_len);
    info->policy_len = serialized_task_vfs_rules_size(task);

    uint32_t total = static_cast<uint32_t>(info->args_len) + static_cast<uint32_t>(info->policy_len);
    if (total > 0xFFFFU) {
        return false;
    }

    info->data_len = static_cast<uint16_t>(total);
    return true;
}

void copy_submit_context_data(uint8_t* dest, const char* const* argv, const char* const* envp, const char* cwd) {
    uint8_t* cursor = dest;

    if (argv != nullptr) {
        for (size_t i = 0; argv[i] != nullptr; ++i) {
            size_t len = std::strlen(argv[i]) + 1;
            memcpy(cursor, argv[i], len);
            cursor += len;
        }
    }

    if (envp != nullptr) {
        for (size_t i = 0; envp[i] != nullptr; ++i) {
            size_t len = std::strlen(envp[i]) + 1;
            memcpy(cursor, envp[i], len);
            cursor += len;
        }
    }

    if (cwd != nullptr && cwd[0] != '\0') {
        size_t len = std::strlen(cwd) + 1;
        memcpy(cursor, cwd, len);
    }
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

auto build_vfs_ref_path(uint16_t target_node, const char* local_path, char* out, size_t out_size) -> bool {
    if (local_path == nullptr || local_path[0] == '\0') {
        return false;
    }

    // Check if target node is connected and knows our hostname
    const char* our_hostname = g_wki.local_hostname;
    if (our_hostname[0] == '\0') {
        return false;
    }

    // Build the VFS_REF path: /wki/<our_hostname>/<local_path without leading />.
    // If the path already starts with /wki/, it already references a specific
    // node's exported filesystem. When it points at the chosen target node,
    // send the receiver its direct local path instead of bouncing through its
    // own /wki/<self>/ alias during ELF load. Otherwise pass the explicit WKI
    // path through unchanged to preserve cross-node semantics.
    if (std::strncmp(local_path, "/wki/", 5) == 0) {
        const char* target_hostname = wki_peer_get_hostname(target_node);
        const char* host_part = local_path + 5;
        const char* host_end = host_part;
        while (*host_end != '\0' && *host_end != '/') {
            host_end++;
        }

        if (target_hostname != nullptr) {
            auto host_len = static_cast<size_t>(host_end - host_part);
            size_t target_len = std::strlen(target_hostname);
            if (host_len == target_len && std::strncmp(host_part, target_hostname, host_len) == 0) {
                const char* suffix = (*host_end == '\0') ? "/" : host_end;
                size_t suffix_len = std::strlen(suffix);
                if (suffix_len + 1 > out_size) {
                    return false;
                }
                std::memcpy(out, suffix, suffix_len + 1);
                return true;
            }
        }

        size_t len = std::strlen(local_path);
        if (len + 1 > out_size) {
            return false;
        }
        std::memcpy(out, local_path, len + 1);
        return true;
    }

    const char* stripped = local_path;
    while (*stripped == '/') stripped++;

    snprintf(out, out_size, "/wki/%s/%s", our_hostname, stripped);
    return true;
}

auto localize_receiver_logical_path(const char* path, char* out, size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    constexpr char wki_prefix[] = "/wki/";
    constexpr size_t wki_prefix_len = sizeof(wki_prefix) - 1;
    if (std::strncmp(path, wki_prefix, wki_prefix_len) != 0) {
        return false;
    }

    const char* local_hostname = g_wki.local_hostname;
    if (local_hostname[0] == '\0') {
        return false;
    }

    size_t host_len = std::strlen(local_hostname);
    const char* host_part = path + wki_prefix_len;
    if (std::strncmp(host_part, local_hostname, host_len) != 0 || (host_part[host_len] != '\0' && host_part[host_len] != '/')) {
        return false;
    }

    const char* suffix = host_part + host_len;
    while (*suffix == '/') {
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

    size_t suffix_len = std::strlen(suffix);
    if (suffix_len + 2 > out_size) {
        return false;
    }

    out[0] = '/';
    std::memcpy(out + 1, suffix, suffix_len + 1);
    return true;
}

auto fallback_to_local_path_for_disconnected_wki_host(const char* path, char* out, size_t out_size) -> bool {
    if (path == nullptr || out == nullptr || out_size == 0) {
        return false;
    }

    constexpr char wki_prefix[] = "/wki/";
    constexpr size_t wki_prefix_len = sizeof(wki_prefix) - 1;
    if (std::strncmp(path, wki_prefix, wki_prefix_len) != 0) {
        return false;
    }

    const char* host_part = path + wki_prefix_len;
    const char* host_end = host_part;
    while (*host_end != '\0' && *host_end != '/') {
        host_end++;
    }
    if (host_end == host_part) {
        return false;
    }

    std::array<char, WKI_HOSTNAME_MAX> host = {};
    size_t host_len = static_cast<size_t>(host_end - host_part);
    if (host_len >= host.size()) {
        host_len = host.size() - 1;
    }
    std::memcpy(host.data(), host_part, host_len);
    host[host_len] = '\0';

    uint16_t host_node = wki_peer_find_by_hostname(host.data());
    if (host_node != WKI_NODE_INVALID && host_node != g_wki.my_node_id) {
        return false;
    }

    const char* suffix = (*host_end == '\0') ? "/" : host_end;
    size_t suffix_len = std::strlen(suffix);
    if (suffix_len + 1 > out_size) {
        return false;
    }

    std::memcpy(out, suffix, suffix_len + 1);

    ker::vfs::stat local_stat = {};
    if (ker::vfs::vfs_stat(out, &local_stat) != 0 || local_stat.st_size <= 0) {
        return false;
    }

    ker::mod::dbg::log("[WKI] VFS_REF: host '%s' unavailable, falling back to local path '%s'", host.data(), out);
    return true;
}

void force_receiver_local_root_rule(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }

    for (size_t i = 0; i < task->wki_vfs_rules.size(); ++i) {
        auto& rule = task->wki_vfs_rules[i];
        if (rule.prefix_len == 1 && rule.prefix[0] == '/' && rule.prefix[1] == '\0') {
            rule.route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
            rule.reserved = 0;
            return;
        }
    }

    ker::mod::sched::task::WkiVfsRule local_root_rule{};
    local_root_rule.prefix[0] = '/';
    local_root_rule.prefix[1] = '\0';
    local_root_rule.prefix_len = 1;
    local_root_rule.route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
    local_root_rule.reserved = 0;
    (void)task->wki_vfs_rules.push_back(local_root_rule);
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
    if (task->elfBuffer == nullptr || task->elfBufferSize == 0) {
        return false;
    }

    WkiRemoteSpawnSpec spec = {};
    return wki_try_remote_spawn(task, spec) == WkiRemoteSpawnResult::REMOTE;
}

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

    const bool has_exe_path = task->exe_path[0] != '\0';
    const bool has_inline_binary = task->elfBuffer != nullptr && task->elfBufferSize != 0;
    if (!has_exe_path && !has_inline_binary) {
        return WkiRemoteSpawnResult::LOCAL;
    }

    // WKI_TARGET_FLAG_LOCAL: task is pinned to the local node.
    if ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) {
        return WkiRemoteSpawnResult::LOCAL;
    }

    // A task that was already submitted from another node should stay on its
    // receiver node for subsequent exec/fork paths.  Re-placing those tasks
    // remotely again turns wrappers such as /bin/time into nested cross-node
    // launches and inflates launch latency for no benefit.
    if (task->wki_submitter_hostname[0] != '\0' && std::strcmp(task->wki_submitter_hostname, g_wki.local_hostname) != 0) {
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
            if (val == nullptr) return;
            auto* f = static_cast<ker::vfs::File*>(val);
            if (ker::vfs::vfs_is_epoll_file(f)) must_stay_local = true;
        });
        if (must_stay_local) return WkiRemoteSpawnResult::LOCAL;
    }

    task->wki_skip_legacy_placement = true;

    const bool explicit_target = task->wki_target_hostname[0] != '\0';
    const bool strict_target = explicit_target && ((task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);

    uint16_t best_node = WKI_NODE_INVALID;
    if (explicit_target) {
        if (std::strncmp(task->wki_target_hostname, g_wki.local_hostname, sizeof(task->wki_target_hostname)) == 0) {
            return WkiRemoteSpawnResult::LOCAL;
        }

        uint16_t node_id = wki_peer_find_by_hostname(task->wki_target_hostname);
        if (node_id == 0 || node_id == WKI_NODE_INVALID) {
            return strict_target ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        }

        auto* peer = wki_peer_find(node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            return strict_target ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        }

        best_node = node_id;
    } else {
        uint16_t local_load = compute_local_load();
        s_compute_lock.lock();
        best_node = wki_least_loaded_node(local_load);
        s_compute_lock.unlock();
        if (best_node == WKI_NODE_INVALID) {
            return WkiRemoteSpawnResult::LOCAL;
        }
    }

    uint32_t tid = 0;
    bool binary_fits = has_inline_binary && (sizeof(TaskSubmitPayload) + sizeof(uint32_t) + task->elfBufferSize) <= WKI_ETH_MAX_PAYLOAD;

    // Export IPC fds (pipes, sockets) so they can be proxied on the remote node.
    // Must happen before task submission so the remote side can attach proxy fops.
    std::array<WkiIpcFdEntry, 16> ipc_fd_map = {};
    uint16_t ipc_fd_count = 0;
    wki_ipc_export_task_fds(task, best_node, ipc_fd_map.data(), &ipc_fd_count);

    if (has_exe_path && (!task->wki_prefer_inline || !has_inline_binary)) {
        // Resolve relative exe_path against the task's cwd so that the
        // VFS_REF path sent to the remote node is always absolute.
        // We cannot use make_absolute() here because it queries
        // get_current_task() which may differ from the task being submitted.
        using TaskT = ker::mod::sched::task::Task;
        constexpr size_t ABS_PATH_MAX = TaskT::CWD_MAX + 1 + TaskT::EXE_PATH_MAX;
        std::array<char, ABS_PATH_MAX> abs_exe_path = {};
        const char* local_path = task->exe_path;
        if (task->exe_path[0] != '/') {
            size_t cwdlen = std::strlen(task->cwd);
            size_t pathlen = std::strlen(task->exe_path);
            bool need_sep = (cwdlen > 1);
            size_t total = cwdlen + (need_sep ? 1 : 0) + pathlen + 1;
            if (total <= abs_exe_path.size()) {
                std::memcpy(abs_exe_path.data(), task->cwd, cwdlen);
                if (need_sep) {
                    abs_exe_path[cwdlen] = '/';
                    std::memcpy(abs_exe_path.data() + cwdlen + 1, task->exe_path, pathlen + 1);
                } else {
                    std::memcpy(abs_exe_path.data() + cwdlen, task->exe_path, pathlen + 1);
                }
                local_path = abs_exe_path.data();
            }
        }

        constexpr size_t VFS_REF_PATH_MAX = 5 + WKI_HOSTNAME_MAX + ABS_PATH_MAX;
        std::array<char, VFS_REF_PATH_MAX> vfs_ref_path = {};
        if (build_vfs_ref_path(best_node, local_path, vfs_ref_path.data(), vfs_ref_path.size())) {
            tid = wki_task_submit_vfs_ref(best_node, vfs_ref_path.data(), spec.argv, spec.envp, spec.cwd, task, ipc_fd_map.data(),
                                          ipc_fd_count);
#ifdef WKI_DEBUG
            if (tid != 0) {
                ker::mod::dbg::log("[WKI] Remote spawn using VFS_REF '%s' -> '%s'", task->exe_path, vfs_ref_path.data());
            }
#endif
        }
    }

    if (tid == 0 && binary_fits) {
        tid = wki_task_submit_inline(best_node, task->elfBuffer, static_cast<uint32_t>(task->elfBufferSize), spec.argv, spec.envp, spec.cwd,
                                     task, ipc_fd_map.data(), ipc_fd_count);
    }

    if (tid == 0) {
        return strict_target ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
    }

    if (!release_cached_elf_buffer(task->elfBuffer)) {
        delete[] task->elfBuffer;
    }
    task->elfBuffer = nullptr;
    task->elfBufferSize = 0;
    task->wki_proxy_task_id = tid;

    bool proxy_ready = false;
    if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::NONE && task != ker::mod::sched::get_current_task()) {
        proxy_ready = ker::mod::sched::post_task_waiting(task);
    }

    bool completed_immediately = false;
    int32_t completed_exit_status = 0;
    bool emit_proxy_ready_wait = false;
    uint32_t proxy_ready_wait_us = 0;
    bool emit_complete_hold = false;
    uint32_t complete_hold_us = 0;
    uint64_t metric_now_us = wki_now_us();
    s_compute_lock.lock();
    SubmittedTask* st = find_submitted_task_any(tid);
    if (st != nullptr) {
        st->local_task = task;
        st->proxy_ready = proxy_ready;
        if (proxy_ready) {
            if (st->accepted_at_us != 0 && metric_now_us >= st->accepted_at_us) {
                emit_proxy_ready_wait = true;
                proxy_ready_wait_us = static_cast<uint32_t>(metric_now_us - st->accepted_at_us);
            }
            perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::PROXY_READY, st->target_node, tid, 0, 0, WOS_PERF_CALLSITE());
        }
        st->complete_pending = true;
        if (proxy_ready && !st->active) {
            completed_immediately = true;
            completed_exit_status = st->exit_status;
            if (st->complete_received_at_us != 0 && metric_now_us >= st->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(metric_now_us - st->complete_received_at_us);
                st->complete_received_at_us = 0;
            }
            st->local_task = nullptr;
        }
    }
    s_compute_lock.unlock();

    if (proxy_ready && emit_proxy_ready_wait) {
        uint64_t callsite = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, best_node, tid, 0, callsite);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, best_node, tid, 0, proxy_ready_wait_us, 0, callsite);
    }

    if (emit_complete_hold) {
        uint64_t callsite = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, best_node, tid, 0, callsite);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, best_node, tid, 0, complete_hold_us, 0, callsite);
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

auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len, const char* const argv[],
                            const char* const envp[], const char* cwd, ker::mod::sched::task::Task* local_task,
                            const WkiIpcFdEntry* ipc_fd_map, uint16_t ipc_fd_count) -> uint32_t {
    if (binary == nullptr || binary_len == 0) {
        return 0;
    }

    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info)) {
        return 0;
    }

    // Check total size fits in WKI message
    // TaskSubmitPayload(16) + binary_len(4) + binary + argv/envp/cwd blob + IPC fd entries
    uint32_t ipc_data_len = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint32_t) + binary_len + context_info.data_len + ipc_data_len);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] Task binary too large for inline submit: %u bytes", binary_len);
        return 0;
    }

    s_compute_lock.lock();
    uint32_t task_id = g_next_task_id++;

    // Create submitted task entry
    SubmittedTask st;
    st.active = true;
    st.task_id = task_id;
    st.target_node = target_node;
    st.response_pending = false;
    st.accept_status = 0;
    st.remote_pid = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    st.local_task = local_task;
    st.proxy_ready = false;

    g_submitted_tasks.push_back(std::move(st));
    SubmittedTask* task_ptr = &g_submitted_tasks.back();
    s_compute_lock.unlock();

    uint64_t started_us = wki_now_us();
    uint64_t callsite = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, task_id, binary_len, callsite);

    // Build TASK_SUBMIT message
    auto msg_len = static_cast<uint16_t>(total);
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
    if (buf == nullptr) {
        s_compute_lock.lock();
        task_ptr->active = false;
        s_compute_lock.unlock();
        return 0;
    }

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf);
    submit->task_id = task_id;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::INLINE);
    submit->prefer_inline = 1;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->ipc_fd_count = ipc_fd_count;

    // INLINE format: {binary_len:u32, binary[binary_len], argv/envp/cwd blob, ipc_fd_entries[]}
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &binary_len, sizeof(uint32_t));
    cursor += sizeof(uint32_t);
    memcpy(cursor, binary, binary_len);
    cursor += binary_len;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, ipc_data_len);
    }

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    task_ptr->response_wait_entry = &wait;
    task_ptr->response_pending.store(true, std::memory_order_release);

    int send_ret = wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_SUBMIT, buf, msg_len);
    ker::mod::mm::dyn::kmalloc::free(buf);

    if (send_ret != WKI_OK) {
        task_ptr->response_wait_entry = nullptr;
        s_compute_lock.lock();
        task_ptr->active = false;
        s_compute_lock.unlock();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, task_id, send_ret,
                                static_cast<uint32_t>(wki_now_us() - started_us), binary_len, callsite);
        return 0;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    task_ptr->response_wait_entry = nullptr;
    if (wait_rc != 0) {
        task_ptr->response_pending.store(false, std::memory_order_relaxed);
        task_ptr->active = false;
        ker::mod::dbg::log("[WKI] Task submit wait failed: task_id=%u target=0x%04x rc=%d (%s)", task_id, target_node, wait_rc,
                           errno_name(wait_rc));
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, task_id, wait_rc,
                                static_cast<uint32_t>(wki_now_us() - started_us), binary_len, callsite);
        return 0;
    }

    if (task_ptr->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        ker::mod::dbg::log("[WKI] Task rejected: task_id=%u status=%u", task_id, task_ptr->accept_status);
        task_ptr->active = false;
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, task_id,
                                -static_cast<int32_t>(task_ptr->accept_status == 0 ? 1 : task_ptr->accept_status),
                                static_cast<uint32_t>(wki_now_us() - started_us), binary_len, callsite);
        return 0;
    }

    ker::mod::dbg::log("[WKI] Task accepted: task_id=%u remote_pid=%lu", task_id, task_ptr->remote_pid);
    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, task_id, 0,
                            static_cast<uint32_t>(wki_now_us() - started_us), binary_len, callsite);
    return task_id;
}

// ===============================================================================
// Submitter Side - VFS_REF Submit (/ ===============================================================================

auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path, const char* const argv[], const char* const envp[],
                             const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map,
                             uint16_t ipc_fd_count) -> uint32_t {
    if (vfs_path == nullptr || vfs_path[0] == '\0') {
        return 0;
    }

    auto path_len = static_cast<uint16_t>(std::strlen(vfs_path));
    if (path_len == 0) {
        return 0;
    }

    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info)) {
        return 0;
    }

    // TaskSubmitPayload(16) + path_len_field(2) + path + argv/envp/cwd blob + IPC fd entries
    uint32_t ipc_data_len = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint16_t) + path_len + context_info.data_len + ipc_data_len);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] VFS_REF path too large: %u bytes", path_len);
        return 0;
    }

    s_compute_lock.lock();
    uint32_t task_id = g_next_task_id++;

    SubmittedTask st;
    st.active = true;
    st.task_id = task_id;
    st.target_node = target_node;
    st.response_pending = false;
    st.accept_status = 0;
    st.remote_pid = 0;
    st.complete_pending = false;
    st.exit_status = 0;
    st.local_task = local_task;
    st.proxy_ready = false;

    g_submitted_tasks.push_back(std::move(st));
    SubmittedTask* task_ptr = &g_submitted_tasks.back();
    s_compute_lock.unlock();

    uint64_t started_us = wki_now_us();
    uint64_t callsite = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, task_id, path_len, callsite);

    auto msg_len = static_cast<uint16_t>(total);
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
    if (buf == nullptr) {
        s_compute_lock.lock();
        task_ptr->active = false;
        s_compute_lock.unlock();
        return 0;
    }

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf);
    submit->task_id = task_id;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::VFS_REF);
    submit->prefer_inline = 0;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->ipc_fd_count = ipc_fd_count;

    // VFS_REF format: {path_len:u16, path[path_len], argv/envp/cwd blob, ipc_fd_entries[]}
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &path_len, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    memcpy(cursor, vfs_path, path_len);
    cursor += path_len;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, ipc_data_len);
    }

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    task_ptr->response_wait_entry = &wait;
    task_ptr->response_pending.store(true, std::memory_order_release);

    int send_ret = wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_SUBMIT, buf, msg_len);
    ker::mod::mm::dyn::kmalloc::free(buf);

    if (send_ret != WKI_OK) {
        task_ptr->response_wait_entry = nullptr;
        s_compute_lock.lock();
        task_ptr->active = false;
        s_compute_lock.unlock();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, task_id, send_ret,
                                static_cast<uint32_t>(wki_now_us() - started_us), path_len, callsite);
        return 0;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    task_ptr->response_wait_entry = nullptr;
    if (wait_rc != 0) {
        task_ptr->response_pending.store(false, std::memory_order_relaxed);
        task_ptr->active = false;
        ker::mod::dbg::log("[WKI] VFS_REF submit wait failed: task_id=%u rc=%d (%s) timeout_us=%llu", task_id, wait_rc, errno_name(wait_rc),
                           WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, task_id, wait_rc,
                                static_cast<uint32_t>(wki_now_us() - started_us), path_len, callsite);
        return 0;
    }

    if (task_ptr->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        ker::mod::dbg::log("[WKI] VFS_REF task rejected: task_id=%u status=%u", task_id, task_ptr->accept_status);
        task_ptr->active = false;
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, task_id,
                                -static_cast<int32_t>(task_ptr->accept_status == 0 ? 1 : task_ptr->accept_status),
                                static_cast<uint32_t>(wki_now_us() - started_us), path_len, callsite);
        return 0;
    }
#ifdef WKI_DEBUG
    ker::mod::dbg::log("[WKI] VFS_REF task accepted: task_id=%u remote_pid=%lu", task_id, task_ptr->remote_pid);
#endif
    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, task_id, 0,
                            static_cast<uint32_t>(wki_now_us() - started_us), path_len, callsite);
    return task_id;
}

// ===============================================================================
// Submitter Side - Wait for Completion
// ===============================================================================

auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int {
    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        return -1;
    }

    // V2 I-4: Set up async wait entry before marking pending
    WkiWaitEntry wait = {};
    task->complete_wait_entry = &wait;
    task->complete_pending.store(true, std::memory_order_release);
    uint16_t target_node = task->target_node;
    s_compute_lock.unlock();

    uint64_t started_us = wki_now_us();
    uint64_t callsite = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, target_node, task_id, 0, callsite);

    int wait_rc = wki_wait_for_op(&wait, timeout_us);
    task->complete_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        task->complete_pending.store(false, std::memory_order_relaxed);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, target_node, task_id, wait_rc,
                                static_cast<uint32_t>(wki_now_us() - started_us), 0, callsite);
        return -1;
    }

    if (exit_status != nullptr) {
        *exit_status = task->exit_status;
    }

    perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_WAIT, target_node, task_id, 0,
                            static_cast<uint32_t>(wki_now_us() - started_us), 0, callsite);

    // Clean up
    task->active = false;
    return 0;
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
    uint64_t metric_now_us = wki_now_us();

    s_compute_lock.lock();
    SubmittedTask* submitted = find_submitted_task_any(task->wki_proxy_task_id);
    if (submitted != nullptr && submitted->local_task == task) {
        target_node = submitted->target_node;
        submitted->proxy_ready = true;
        if (submitted->accepted_at_us != 0 && metric_now_us >= submitted->accepted_at_us) {
            emit_proxy_ready_wait = true;
            proxy_ready_wait_us = static_cast<uint32_t>(metric_now_us - submitted->accepted_at_us);
        }
        perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::PROXY_READY, submitted->target_node, task->wki_proxy_task_id, 0, 0,
                                  WOS_PERF_CALLSITE());
        if (!submitted->active) {
            completed_immediately = true;
            completed_exit_status = submitted->exit_status;
            if (submitted->complete_received_at_us != 0 && metric_now_us >= submitted->complete_received_at_us) {
                emit_complete_hold = true;
                complete_hold_us = static_cast<uint32_t>(metric_now_us - submitted->complete_received_at_us);
                submitted->complete_received_at_us = 0;
            }
            submitted->local_task = nullptr;
        }
    }
    s_compute_lock.unlock();

    if (emit_proxy_ready_wait) {
        uint64_t callsite = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, target_node, task->wki_proxy_task_id, 0, callsite);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::PROXY_READY_WAIT, target_node, task->wki_proxy_task_id, 0,
                                proxy_ready_wait_us, 0, callsite);
    }

    if (emit_complete_hold) {
        uint64_t callsite = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, target_node, task->wki_proxy_task_id, 0, callsite);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::COMPLETE_HOLD, target_node, task->wki_proxy_task_id, 0, complete_hold_us,
                                0, callsite);
    }

    if (completed_immediately) {
        finalize_proxy_task(task, completed_exit_status, nullptr, 0, task->wki_proxy_task_id);
    }
}

// ===============================================================================
// Submitter Side - Cancel
// ===============================================================================

void wki_task_cancel(uint32_t task_id) {
    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        return;
    }

    uint16_t target_node = task->target_node;
    s_compute_lock.unlock();

    TaskCancelPayload cancel = {};
    cancel.task_id = task_id;

    wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_CANCEL, &cancel, sizeof(cancel));

    s_compute_lock.lock();
    task->active = false;
    task->complete_pending = false;
    s_compute_lock.unlock();
}

// ===============================================================================
//  Signal Forwarding for Proxy Tasks
// ===============================================================================

auto wki_proxy_task_forward_signal(ker::mod::sched::task::Task* task, int signum) -> bool {
    if (task == nullptr || task->wki_proxy_task_id == 0) {
        return false;
    }

    // Only forward SIGKILL and SIGTERM - other signals have no meaning on remote.
    if (signum != WKI_SIGKILL_NUM && signum != WKI_SIGTERM_NUM) {
        return false;
    }

    uint32_t proxy_tid = task->wki_proxy_task_id;
    ker::mod::dbg::log("[WKI] Forwarding signal %d to remote task_id=%u (proxy pid=0x%lx)", signum, proxy_tid, task->pid);

    wki_task_cancel(proxy_tid);

    // The proxy task will be cleaned up when TASK_COMPLETE arrives
    // (or if the peer is fenced, the fencing cleanup will handle it)
    return true;
}

// ===============================================================================
// Load Reporting
// ===============================================================================

void wki_load_report_send() {
    if (!g_remote_compute_initialized) {
        return;
    }

    uint64_t now = wki_now_us();
    if (now - g_last_load_report_us < WKI_LOAD_REPORT_INTERVAL_US) {
        return;
    }
    g_last_load_report_us = now;

    // Build LoadReportPayload with real scheduler metrics
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::get_core_count());
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    // Cap per-CPU array size to prevent buffer overflow
    constexpr uint16_t MAX_REPORT_CPUS = 64;
    uint16_t report_cpus = std::min(cpu_count, MAX_REPORT_CPUS);

    LoadReportPayload report = {};
    report.num_cpus = report_cpus;

    uint16_t total_runnable = 0;
    uint8_t buf[sizeof(LoadReportPayload) + (MAX_REPORT_CPUS * sizeof(uint16_t))] =
        {};  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays,cppcoreguidelines-pro-type-reinterpret-cast)
    auto* per_cpu = reinterpret_cast<uint16_t*>(&buf[sizeof(LoadReportPayload)]);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

    for (uint16_t c = 0; c < report_cpus; c++) {
        auto stats = ker::mod::sched::get_run_queue_stats(c);
        auto cpu_load = static_cast<uint16_t>(stats.active_task_count + stats.wait_queue_count);
        per_cpu[c] = cpu_load;
        total_runnable += static_cast<uint16_t>(stats.active_task_count);
    }

    report.runnable_tasks = total_runnable;
    // avg_load_pct: scale 0-1000. Approximation: (total_runnable / num_cpus) * 1000, capped at 1000.
    if (report_cpus > 0 && total_runnable > 0) {
        uint32_t pct = (static_cast<uint32_t>(total_runnable) * 1000U) / report_cpus;
        report.avg_load_pct = static_cast<uint16_t>(std::min(pct, 1000U));
    } else {
        report.avg_load_pct = 0;
    }
    report.free_mem_pages = static_cast<uint16_t>(std::min(ker::mod::mm::phys::get_free_mem_bytes() / (256ULL * 4096ULL), 0xFFFFULL));

    auto total_len = static_cast<uint16_t>(sizeof(LoadReportPayload) + (report_cpus * sizeof(uint16_t)));
    memcpy(&buf[0], &report, sizeof(LoadReportPayload));  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)

    // Send to all CONNECTED peers
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }

        wki_send(peer->node_id, WKI_CHAN_EVENT_BUS, MsgType::LOAD_REPORT, &buf[0],
                 total_len);  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    }
}

auto wki_remote_node_load(uint16_t node_id) -> const RemoteNodeLoad* { return find_remote_load(node_id); }

auto wki_least_loaded_node(uint16_t local_load) -> uint16_t {
    uint16_t best_node = WKI_NODE_INVALID;
    uint16_t best_load = local_load;

    for (const auto& rl : g_remote_loads) {
        if (!rl.valid) {
            continue;
        }

        // Stale load reports (>1s old) are not considered
        uint64_t age = wki_now_us() - rl.last_update_us;
        if (age > 1000000) {
            continue;
        }

        // Apply remote placement penalty
        uint16_t adjusted = rl.avg_load_pct + WKI_REMOTE_PLACEMENT_PENALTY;
        if (adjusted < best_load) {
            best_load = adjusted;
            best_node = rl.node_id;
        }
    }

    return best_node;
}

// ===============================================================================
// Fencing Cleanup
// ===============================================================================

void wki_remote_compute_cleanup_for_peer(uint16_t node_id) {
    constexpr size_t MAX_PROXIES = 32;
    constexpr size_t MAX_WAITERS = 64;
    std::array<ker::mod::sched::task::Task*, MAX_PROXIES> proxy_tasks = {};
    size_t proxy_count = 0;
    std::array<WkiWaitEntry*, MAX_WAITERS> waiters_to_wake = {};
    size_t waiter_count = 0;

    s_compute_lock.lock();

    // Fail any submitted tasks targeting this peer
    for (auto& t : g_submitted_tasks) {
        if (!t.active || t.target_node != node_id) {
            continue;
        }

        if (t.response_pending.load(std::memory_order_acquire)) {
            t.accept_status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            t.response_pending.store(false, std::memory_order_release);
            if (t.response_wait_entry != nullptr && waiter_count < MAX_WAITERS) {
                waiters_to_wake[waiter_count++] = t.response_wait_entry;
                t.response_wait_entry = nullptr;
            }
        }
        if (t.complete_pending.load(std::memory_order_acquire)) {
            t.exit_status = -1;
            t.complete_pending.store(false, std::memory_order_release);
            if (t.complete_wait_entry != nullptr && waiter_count < MAX_WAITERS) {
                waiters_to_wake[waiter_count++] = t.complete_wait_entry;
                t.complete_wait_entry = nullptr;
            }
        }

        // Collect only proxies that are already safely parked.
        if (t.local_task != nullptr && t.proxy_ready && proxy_count < MAX_PROXIES) {
            proxy_tasks[proxy_count++] = t.local_task;
            t.local_task = nullptr;
        }

        t.active = false;
    }

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
    std::erase_if(g_running_remote_tasks, [](const RunningRemoteTask& rt) { return !rt.active; });

    for (auto& entry : g_shared_elf_cache) {
        if (entry.submitter_node != node_id) {
            continue;
        }
        entry.valid = false;
    }
    gc_shared_elf_cache_locked(wki_now_us());

    s_compute_lock.unlock();

    // V2 I-4: Wake any blocked waiters (outside lock)
    for (size_t i = 0; i < waiter_count; i++) {
        wki_wake_op(waiters_to_wake[i], -1);
    }

    // Wake proxy tasks with error exit status (outside lock - scheduling operations)
    for (size_t i = 0; i < proxy_count; i++) {
        auto* proxy = proxy_tasks[i];
        finalize_proxy_task(proxy, -1, nullptr, 0, 0);

        ker::mod::dbg::log("[WKI] Proxy task fenced: pid=0x%lx (peer 0x%04x)", proxy->pid, node_id);
    }
}

auto wki_remote_compute_release_elf_buffer(uint8_t* buffer) -> bool { return release_cached_elf_buffer(buffer); }

// ===============================================================================
// Receiver Side - Completion Monitoring
// ===============================================================================

void wki_remote_compute_check_completions() {
    if (!g_remote_compute_initialized) {
        return;
    }

    // Collect completed tasks under lock, then process without lock
    struct CompletionInfo {
        uint32_t task_id;
        uint16_t submitter_node;
        uint64_t local_pid;
        int32_t exit_status;
        TaskOutputCapture* output;
    };

    constexpr size_t MAX_COMPLETIONS = 32;
    std::array<CompletionInfo, MAX_COMPLETIONS> completions = {};
    size_t num_completions = 0;

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
        } else if (task->hasExited) {
            exit_status = normalize_local_exit_status_for_wire(task->exitStatus);
            completed = true;
        }

        if (!completed) {
            continue;
        }

        if (num_completions < MAX_COMPLETIONS) {
            completions[num_completions++] = {.task_id = rt.task_id,
                                              .submitter_node = rt.submitter_node,
                                              .local_pid = rt.local_pid,
                                              .exit_status = exit_status,
                                              .output = rt.output};
        } else {
            delete rt.output;
        }
        if (rt.task != nullptr) {
            rt.task->release();
            rt.task = nullptr;
        }
        rt.output = nullptr;
        rt.active = false;
    }

    // Clean up inactive entries
    std::erase_if(g_running_remote_tasks, [](const RunningRemoteTask& rt) { return !rt.active; });

    s_compute_lock.unlock();

    // Process completions without lock (wki_send, malloc, logging)
    for (size_t i = 0; i < num_completions; i++) {
        auto& info = completions[i];

        // D19: Build TASK_COMPLETE with captured output
        uint16_t out_len = (info.output != nullptr) ? info.output->len : static_cast<uint16_t>(0);
        auto msg_len = static_cast<uint16_t>(sizeof(TaskCompletePayload) + out_len);
        auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
        if (buf != nullptr) {
            auto* complete = reinterpret_cast<TaskCompletePayload*>(buf);
            complete->task_id = info.task_id;
            complete->exit_status = info.exit_status;
            complete->output_len = out_len;

            if (out_len > 0 && info.output != nullptr) {
                memcpy(buf + sizeof(TaskCompletePayload), info.output->data.data(), out_len);
            }

            wki_send(info.submitter_node, WKI_CHAN_RESOURCE, MsgType::TASK_COMPLETE, buf, msg_len);
            ker::mod::mm::dyn::kmalloc::free(buf);
        }
#if WKI_DEBUG
        ker::mod::dbg::log("[WKI] Remote task completed: task_id=%u pid=0x%lx exit=%d output=%u bytes", info.task_id, info.local_pid,
                           info.exit_status, out_len);
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
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::pageAlloc(KERNEL_STACK_SIZE));
    if (stack_base == 0) {
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    uint64_t kernel_rsp = stack_base + KERNEL_STACK_SIZE;

    // Create the process task
    auto* new_task = new ker::mod::sched::task::Task(  // NOLINT(cppcoreguidelines-owning-memory)
        "wki-remote", reinterpret_cast<uint64_t>(elf_buffer), kernel_rsp, ker::mod::sched::task::TaskType::PROCESS);

    if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
        delete new_task;
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    new_task->elfBuffer = elf_buffer;
    new_task->elfBufferSize = binary_len;

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

    std::array<char, 512> localized_path = {};
    std::array<char, 512> fallback_local_path = {};
    const char* resolved_path = path;
    if (localize_receiver_logical_path(path, localized_path.data(), localized_path.size())) {
        resolved_path = localized_path.data();
    }

    bool using_disconnected_host_fallback = false;
    if (fallback_to_local_path_for_disconnected_wki_host(resolved_path, fallback_local_path.data(), fallback_local_path.size())) {
        resolved_path = fallback_local_path.data();
        using_disconnected_host_fallback = true;
    }

    ker::vfs::stat statbuf = {};
    if (ker::vfs::vfs_stat(resolved_path, &statbuf) != 0 || statbuf.st_size <= 0) {
        ker::mod::dbg::log("[WKI] VFS_REF: failed to stat '%s'", resolved_path);
        result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }
    if (static_cast<uint64_t>(statbuf.st_size) > static_cast<uint64_t>(UINT32_MAX)) {
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }

    ker::vfs::stat cache_key = statbuf;
    bool is_loader = false;
    uint64_t inflight_deadline_us = wki_now_us() + WKI_TASK_SUBMIT_VFS_TIMEOUT_US;

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
                if (wki_now_us() >= inflight_deadline_us) {
                    result.reject_reason = TaskRejectReason::FETCH_FAILED;
                    load_measure.finish(perf_compute_reject_status(result.reject_reason));
                    return result;
                }
                ker::mod::sched::kern_yield();
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
        g_shared_elf_cache.push_back(std::move(pending));
        is_loader = true;
        s_compute_lock.unlock();
        break;
    }

    auto file_size = static_cast<size_t>(statbuf.st_size);
    uint8_t* buf = nullptr;
    size_t total_read = 0;
    size_t final_file_size = file_size;
    bool load_ok = false;
    uint64_t retry_window_start_us = wki_now_us();
    uint64_t retry_deadline_us = retry_window_start_us + WKI_VFS_LOAD_RETRY_WINDOW_US;

    for (uint32_t attempt = 0; attempt < WKI_VFS_LOAD_MAX_ATTEMPTS; attempt++) {
        uint64_t now_us = wki_now_us();
        bool retry_window_open = now_us < retry_deadline_us;

        if (attempt > 0 && !retry_window_open) {
            break;
        }

        if (attempt > 0) {
            if (!using_disconnected_host_fallback &&
                fallback_to_local_path_for_disconnected_wki_host(resolved_path, fallback_local_path.data(), fallback_local_path.size())) {
                resolved_path = fallback_local_path.data();
                using_disconnected_host_fallback = true;
            }

            ker::vfs::stat retry_stat = {};
            if (ker::vfs::vfs_stat(resolved_path, &retry_stat) != 0 || retry_stat.st_size <= 0) {
                if (retry_window_open && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                    uint64_t wait_until_us = wki_now_us() + WKI_VFS_LOAD_RETRY_BACKOFF_US;
                    while (wki_now_us() < wait_until_us) {
                        ker::mod::sched::kern_yield();
                    }
                }
                continue;
            }
            if (static_cast<uint64_t>(retry_stat.st_size) > static_cast<uint64_t>(UINT32_MAX)) {
                fail_inflight_load();
                result.reject_reason = TaskRejectReason::FETCH_FAILED;
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            file_size = static_cast<size_t>(retry_stat.st_size);
        }

        int fd = ker::vfs::vfs_open(resolved_path, 0, 0);
        if (fd < 0) {
            if (retry_window_open && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
                uint64_t wait_until_us = wki_now_us() + WKI_VFS_LOAD_RETRY_BACKOFF_US;
                while (wki_now_us() < wait_until_us) {
                    ker::mod::sched::kern_yield();
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
            size_t chunk = std::min(WKI_VFS_LOAD_CHUNK, file_size - total_read);
            size_t actual = 0;
            ssize_t read_ret = ker::vfs::vfs_read(fd, buf + total_read, chunk, &actual);

            if (read_ret < 0 || actual == 0) {
                if (++idle_retries >= WKI_VFS_LOAD_IDLE_RETRIES) {
                    break;
                }
                continue;
            }

            total_read += actual;
            idle_retries = 0;
        }

        ker::vfs::vfs_close(fd);

        if (total_read == file_size) {
            final_file_size = file_size;
            load_ok = true;
            break;
        }

        delete[] buf;
        buf = nullptr;
        if (retry_window_open && attempt + 1 < WKI_VFS_LOAD_MAX_ATTEMPTS) {
            uint64_t elapsed_ms = (wki_now_us() - retry_window_start_us) / 1000;
            ker::mod::dbg::log("[WKI] VFS_REF: short read retry for '%s' (%zu/%zu bytes) attempt %u/%u elapsed=%llu ms", resolved_path,
                               total_read, file_size, attempt + 1, WKI_VFS_LOAD_MAX_ATTEMPTS, static_cast<unsigned long long>(elapsed_ms));
            uint64_t wait_until_us = wki_now_us() + WKI_VFS_LOAD_RETRY_BACKOFF_US;
            while (wki_now_us() < wait_until_us) {
                ker::mod::sched::kern_yield();
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
    g_shared_elf_cache.push_back(std::move(cache_entry));
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
// wki_spin_yield() / napi_poll_inline() are safe (i.e. NOT from inside
// the NAPI poll dispatch path).
static void handle_task_submit_work(uint16_t src_node, const uint8_t* payload, uint16_t payload_len) {
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
            path_buf[static_cast<size_t>(copy_len)] = '\0';

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
            ref_path[static_cast<size_t>(copy_len)] = '\0';

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

    if (submit->args_len > submit_context_len) {
        reject_reason = TaskRejectReason::FETCH_FAILED;
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
    ExecResult exec = exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared);
    if (exec.task == nullptr) {
        auto exec_reason = exec.reject_reason;
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
    auto* new_task = exec.task;

    const char* submitter_hostname = wki_peer_get_hostname(hdr->src_node);
    if (submitter_hostname != nullptr && submitter_hostname[0] != '\0') {
        std::strncpy(new_task->wki_submitter_hostname, submitter_hostname, sizeof(new_task->wki_submitter_hostname) - 1);
        new_task->wki_submitter_hostname[sizeof(new_task->wki_submitter_hostname) - 1] = '\0';
    } else {
        std::snprintf(new_task->wki_submitter_hostname, sizeof(new_task->wki_submitter_hostname), "node-%04x", hdr->src_node);
    }

    std::array<char, ker::mod::sched::task::Task::EXE_PATH_MAX> logical_exe_path = {};
    const char* task_exe_path = exe_path_buf.data();
    if (localize_receiver_logical_path(exe_path_buf.data(), logical_exe_path.data(), logical_exe_path.size())) {
        task_exe_path = logical_exe_path.data();
    }

    // Set exe_path from the resolved path, localized to receiver semantics
    // when the submitter referenced this node via /wki/<local-host>/...
    std::strncpy(new_task->exe_path, task_exe_path, ker::mod::sched::task::Task::EXE_PATH_MAX - 1);
    new_task->exe_path[ker::mod::sched::task::Task::EXE_PATH_MAX - 1] = '\0';

    const char** argv_strings = nullptr;
    const char** envp_strings = nullptr;
    const char* cwd_string = "/";
    bool parse_ok = true;

    // IPC fd entries are appended after args+policy in the submit context.
    // Subtract their size so policy_len covers only the actual VFS rules.
    uint16_t ipc_tail_len = static_cast<uint16_t>(submit->ipc_fd_count * sizeof(WkiIpcFdEntry));
    uint16_t context_without_ipc =
        (submit_context_len >= ipc_tail_len) ? static_cast<uint16_t>(submit_context_len - ipc_tail_len) : submit_context_len;
    const uint8_t* policy_cursor = submit_context + submit->args_len;
    uint16_t policy_len = static_cast<uint16_t>(context_without_ipc - submit->args_len);

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

    if (parse_ok && !deserialize_task_vfs_rules(new_task, policy_cursor, policy_len)) {
        parse_ok = false;
    }

    if (!parse_ok) {
        delete[] argv_strings;
        delete[] envp_strings;
        delete new_task;
        delete exec.output;

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::FETCH_FAILED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit parse failed: task_id=%u", submit->task_id);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::FETCH_FAILED), binary_len);
        return;
    }

    std::array<char, ker::mod::sched::task::Task::CWD_MAX> logical_cwd = {};
    const char* task_cwd = cwd_string;
    if (localize_receiver_logical_path(cwd_string, logical_cwd.data(), logical_cwd.size())) {
        task_cwd = logical_cwd.data();
    }

    // Receiver-created tasks should resolve ordinary absolute and relative
    // paths like local tasks on the receiver. Explicit /wki/host/... still
    // reaches the submitter because /wki remains the more specific LOCAL rule.
    force_receiver_local_root_rule(new_task);

    size_t cwd_copy_len = std::strlen(task_cwd);
    if (cwd_copy_len >= ker::mod::sched::task::Task::CWD_MAX) {
        cwd_copy_len = ker::mod::sched::task::Task::CWD_MAX - 1;
    }
    memcpy(new_task->cwd, task_cwd, cwd_copy_len);
    new_task->cwd[cwd_copy_len] = '\0';

    // Inherit per-process root from the current (kernel) task so that
    // path resolution after pivot_root works identically to local tasks.
    {
        auto* cur = ker::mod::sched::get_current_task();
        if (cur != nullptr) {
            memcpy(new_task->root, cur->root, sizeof(new_task->root));
        }
    }

    // Remote-receiver tasks and their descendants should execute locally on
    // the receiver unless they opt into a different policy explicitly later.
    new_task->wki_target_hostname[0] = '\0';
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
            size_t blen = std::strlen(base);
            auto* name_buf = new char[blen + 1];
            std::memcpy(name_buf, base, blen + 1);
            delete[] new_task->name;
            new_task->name = name_buf;
        }
    }

    // Set up user stack: argc / argv / envp / auxv (System V x86-64 ABI)
    bool stack_setup_ok = [&]() {
        using namespace ker::mod::mm;

        uint64_t user_stack_top = new_task->thread->stack;
        uint64_t stack_offset = 0;

        auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
            if (stack_offset + size > 0x10000) {  // 64 KiB safety limit
                return 0;
            }
            stack_offset += size;
            uint64_t vaddr = user_stack_top - stack_offset;

            uint64_t page_virt = vaddr & ~(paging::PAGE_SIZE - 1);
            uint64_t page_off = vaddr & (paging::PAGE_SIZE - 1);

            uint64_t page_phys = virt::translate(new_task->pagemap, page_virt);
            if (page_phys == virt::PADDR_INVALID) {
                mod::dbg::log("remote_compute: translate failed for stack vaddr 0x%x", page_virt);
                hcf();
            }
            auto* dest = reinterpret_cast<uint8_t*>(addr::get_virt_pointer(page_phys)) + page_off;
            std::memcpy(dest, data, size);
            return vaddr;
        };

        auto push_string = [&](const char* str) -> uint64_t {
            size_t len = std::strlen(str) + 1;
            if (stack_offset + len > 0x10000) {
                return 0;
            }
            stack_offset += len;
            uint64_t vaddr = user_stack_top - stack_offset;

            uint64_t page_virt = vaddr & ~(paging::PAGE_SIZE - 1);
            uint64_t page_off = vaddr & (paging::PAGE_SIZE - 1);

            uint64_t page_phys = virt::translate(new_task->pagemap, page_virt);
            if (page_phys == virt::PADDR_INVALID) {
                mod::dbg::log("remote_compute push_string: translate failed for stack vaddr 0x%x", page_virt);
                hcf();
            }
            auto* dest = reinterpret_cast<uint8_t*>(addr::get_virt_pointer(page_phys)) + page_off;
            std::memcpy(dest, str, len);
            return vaddr;
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
        uint64_t cur = user_stack_top - stack_offset;
        uint64_t aligned = cur & ~(ALIGN - 1);
        stack_offset += (cur - aligned);

        size_t auxv_qwords = 14 + (new_task->interpBase != 0 ? 2 : 0);
        size_t structured_qwords = auxv_qwords + (static_cast<size_t>(submit->envc) + 1) + (static_cast<size_t>(submit->argc) + 1) + 1;
        if (structured_qwords % 2 != 0) {
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
            auxv[auxv_count++] = key;
            auxv[auxv_count++] = value;
        };

        append_auxv(AT_PAGESZ, paging::PAGE_SIZE);
        append_auxv(AT_ENTRY, new_task->entry);
        append_auxv(AT_PHDR, new_task->programHeaderAddr);
        append_auxv(AT_PHENT, new_task->programHeaderEntSize);
        append_auxv(AT_PHNUM, new_task->programHeaderCount);
        append_auxv(AT_EHDR, new_task->elfHeaderAddr);
        if (new_task->interpBase != 0) {
            append_auxv(AT_BASE, new_task->interpBase);
        }
        append_auxv(AT_NULL, 0);

        for (int j = static_cast<int>(auxv_count) - 1; j >= 0; --j) {
            uint64_t val = auxv[static_cast<size_t>(j)];
            if (push_to_stack(&val, sizeof(uint64_t)) == 0) {
                delete[] argv_addrs;
                delete[] envp_addrs;
                return false;
            }
        }

        uint64_t envp_ptr = push_to_stack(envp_addrs, (static_cast<size_t>(submit->envc) + 1) * sizeof(uint64_t));
        uint64_t argv_ptr = push_to_stack(argv_addrs, (static_cast<size_t>(submit->argc) + 1) * sizeof(uint64_t));
        delete[] envp_addrs;
        delete[] argv_addrs;
        if (envp_ptr == 0 || argv_ptr == 0) {
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
        new_task->context.regs.rsi = argv_ptr;
        new_task->context.regs.rdx = envp_ptr;
        return true;
    }();

    delete[] argv_strings;
    delete[] envp_strings;

    if (!stack_setup_ok) {
        uint64_t failed_pid = new_task->pid;
        delete new_task;
        delete exec.output;

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit stack setup failed: task_id=%u pid=0x%lx", submit->task_id, failed_pid);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
        return;
    }

    // Save PID before posting to scheduler.  Once posted, the task can
    // complete and be epoch-reclaimed before we touch exec.task again.
    uint64_t launched_pid = exec.task->pid;

    // Attach IPC proxy fds if the submitter exported any.
    if (submit->ipc_fd_count > 0 && submit_context != nullptr) {
        // IPC fd entries are appended after argv/envp/cwd in the submit context.
        // Find them by skipping past submit_context_len - (ipc_fd_count * sizeof(WkiIpcFdEntry)).
        uint16_t ipc_data_size = submit->ipc_fd_count * sizeof(WkiIpcFdEntry);
        if (submit_context_len >= ipc_data_size) {
            const auto* ipc_entries = reinterpret_cast<const WkiIpcFdEntry*>(submit_context + submit_context_len - ipc_data_size);
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

    // Post to scheduler
    if (!ker::mod::sched::post_task_balanced(new_task)) {
        delete new_task;
        delete exec.output;
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit post failed: task_id=%u pid=0x%lx", submit->task_id, launched_pid);
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
    rt.local_pid = launched_pid;
    if (new_task->tryAcquire()) {
        rt.task = new_task;
    }
    rt.output = exec.output;
    s_compute_lock.lock();
    g_running_remote_tasks.push_back(rt);
    s_compute_lock.unlock();

    // Send TASK_ACCEPT
    TaskResponsePayload accept = {};
    accept.task_id = submit->task_id;
    accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
    accept.remote_pid = launched_pid;
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

static void drain_pending_task_submits() {
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
            uint32_t wait_us = static_cast<uint32_t>(wki_now_us() - pending.queued_at_us);
            uint64_t callsite = WOS_PERF_CALLSITE();
            perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::DEFER_WAIT, pending.src_node, submit->task_id, pending.payload_len,
                                      callsite);
            perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::DEFER_WAIT, pending.src_node, submit->task_id, 0, wait_us, 0,
                                    callsite);
        }

        handle_task_submit_work(pending.src_node, pending.payload, pending.payload_len);
        ker::mod::mm::dyn::kmalloc::free(pending.payload);
    }
}

[[noreturn]] static void wki_compute_submit_thread() {
    for (;;) {
        drain_pending_task_submits();

        // Sleep until woken by wki_remote_compute_notify_pending_submit().
        // No polling — kern_wake() delivers an immediate wakeup when a new
        // VFS_REF/RESOURCE_REF submit is queued.
        s_pending_submit_lock.lock();
        bool empty = g_pending_task_submits.empty();
        s_pending_submit_lock.unlock();
        if (empty) {
            ker::mod::sched::kern_block();
        }
    }
}

void wki_remote_compute_start_submit_thread() {
    auto* task = ker::mod::sched::task::Task::createKernelThread("wki_compute", wki_compute_submit_thread);
    if (task == nullptr) {
        ker::mod::dbg::log("[WKI] Failed to create compute submit kernel thread");
        return;
    }
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
    // Those waits call wki_spin_yield() -> napi_poll_inline(), causing NAPI
    // re-entrance when dispatched from wki_rx().  Defer to the dedicated
    // compute submit thread where blocking VFS I/O is safe.
    if (mode == TaskDeliveryMode::VFS_REF || mode == TaskDeliveryMode::RESOURCE_REF) {
        auto* copy = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(payload_len));
        if (copy == nullptr) {
            TaskResponsePayload reject = {};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
            reject.remote_pid = 0;
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
            return;
        }
        std::memcpy(copy, payload, payload_len);

        PendingTaskSubmit pending;
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
    if (task == nullptr || !task->response_pending) {
        s_compute_lock.unlock();
        return;
    }

    task->accept_status = resp->status;
    task->remote_pid = resp->remote_pid;
    task->accepted_at_us = wki_now_us();
    task->complete_received_at_us = 0;

    task->response_pending.store(false, std::memory_order_release);
    WkiWaitEntry* waiter = task->response_wait_entry;
    s_compute_lock.unlock();

    if (waiter != nullptr) {
        wki_wake_op(waiter, 0);
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
    if (task == nullptr || !task->response_pending) {
        s_compute_lock.unlock();
        return;
    }

    task->accept_status = resp->status;
    task->remote_pid = 0;

    task->response_pending.store(false, std::memory_order_release);
    WkiWaitEntry* waiter = task->response_wait_entry;
    s_compute_lock.unlock();

    if (waiter != nullptr) {
        wki_wake_op(waiter, 0);
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
    uint64_t complete_now_us = wki_now_us();

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(comp->task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] TASK_COMPLETE: no submitted task for task_id=%u", comp->task_id);
        return;
    }

    task->exit_status = comp->exit_status;
    if (task->accepted_at_us != 0 && complete_now_us >= task->accepted_at_us) {
        emit_task_runtime = true;
        task_runtime_us = static_cast<uint32_t>(complete_now_us - task->accepted_at_us);
    }

    auto* proxy = task->proxy_ready ? task->local_task : nullptr;
    if (proxy != nullptr) {
        task->local_task = nullptr;
        task->complete_received_at_us = 0;
    } else {
        task->complete_received_at_us = complete_now_us;
    }

    task->complete_pending.store(false, std::memory_order_release);
    complete_waiter = task->complete_wait_entry;
    task->complete_wait_entry = nullptr;
    task->active = false;
    s_compute_lock.unlock();

    if (emit_task_runtime) {
        uint64_t callsite = WOS_PERF_CALLSITE();
        perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::TASK_RUNTIME, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID,
                                  comp->task_id, 0, callsite);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::TASK_RUNTIME, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID,
                                comp->task_id, comp->exit_status, task_runtime_us, 0, callsite);
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
                           comp->exit_status, task->local_task);
#endif
        if (task->local_task != nullptr) {
            const uint8_t* output_data = payload + sizeof(TaskCompletePayload);
            uint16_t out_len = comp->output_len;
            if (out_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
                auto avail = static_cast<uint16_t>(payload_len - sizeof(TaskCompletePayload));
                out_len = std::min(out_len, avail);
                write_proxy_output(task->local_task, output_data, out_len, comp->task_id);
            }
        }
#ifdef DEBUG_WKI_COMPUTE
        else if (comp->output_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
            ker::mod::dbg::log("[WKI] Task %u output (%u bytes)", comp->task_id, comp->output_len);
        }
#endif
    }

    if (complete_waiter != nullptr) {
        wki_wake_op(complete_waiter, 0);
    }

    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::COMPLETE, hdr != nullptr ? hdr->src_node : WKI_NODE_INVALID, comp->task_id,
                              comp->exit_status, comp->output_len, WOS_PERF_CALLSITE());
}

void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskCancelPayload)) {
        return;
    }

    const auto* cancel = reinterpret_cast<const TaskCancelPayload*>(payload);

    // D18: Find the running task and extract fields under lock
    s_compute_lock.lock();
    RunningRemoteTask* rt = find_running_task(cancel->task_id, hdr->src_node);
    if (rt == nullptr) {
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] Task cancel: no matching running task task_id=%u from 0x%04x", cancel->task_id, hdr->src_node);
        return;
    }

    uint64_t local_pid = rt->local_pid;
    s_compute_lock.unlock();

    auto* task = rt->task;
    bool drop_lookup_ref = false;
    if (task == nullptr) {
        task = ker::mod::sched::find_task_by_pid_safe(local_pid);
        drop_lookup_ref = (task != nullptr);
    }
    if (task != nullptr && !task->hasExited) {
        task->sigPending |= (1ULL << (WKI_SIGKILL_NUM - 1));
        ker::mod::sched::wake_task_for_signal(task);
        ker::mod::dbg::log("[WKI] Task cancel queued: task_id=%u pid=0x%lx", cancel->task_id, local_pid);
    }

    if (drop_lookup_ref) {
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
