#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

#include "platform/sched/task.hpp"

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

constexpr uint64_t WKI_LOAD_REPORT_INTERVAL_US = 1000000;  // 1 second
constexpr uint16_t WKI_REMOTE_PLACEMENT_PENALTY = 200;
constexpr uint64_t WKI_TASK_SUBMIT_TIMEOUT_US = 500000;         // 500ms
constexpr uint64_t WKI_TASK_WAIT_DEFAULT_TIMEOUT_US = 5000000;  // 5s

// -----------------------------------------------------------------------------
// Remote node load cache (from LOAD_REPORT messages)
// -----------------------------------------------------------------------------

struct RemoteNodeLoad {
    bool valid = false;
    uint16_t node_id = WKI_NODE_INVALID;
    uint16_t num_cpus = 0;
    uint16_t runnable_tasks = 0;
    uint16_t avg_load_pct = 0;  // 0-1000
    uint16_t free_mem_pages = 0;
    uint64_t last_update_us = 0;
};

// -----------------------------------------------------------------------------
// Submitted task tracking (submitter side)
// -----------------------------------------------------------------------------

struct SubmittedTask {
    bool active = false;
    uint32_t task_id = 0;
    uint16_t target_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;

    std::atomic<bool> response_pending{false};
    uint8_t accept_status = 0;                    // TaskRejectReason
    WkiWaitEntry* response_wait_entry = nullptr;  // V2 I-4: async wait for TASK_ACCEPT/REJECT

    std::atomic<bool> complete_pending{false};
    WkiWaitEntry* complete_wait_entry = nullptr;  // V2 I-4: async wait for TASK_COMPLETE
    int32_t exit_status = 0;
    uint64_t accepted_at_us = 0;
    uint64_t complete_received_at_us = 0;

    // V2 A7: Proxy task pointer - kept alive in WAITING state until remote completes
    ker::mod::sched::task::Task* local_task = nullptr;
    bool proxy_ready = false;

    std::array<WkiIpcFdEntry, 16> ipc_fd_map = {};
    uint16_t ipc_fd_count = 0;

    SubmittedTask() = default;
    SubmittedTask(const SubmittedTask&) = delete;
    auto operator=(const SubmittedTask&) -> SubmittedTask& = delete;
    SubmittedTask(SubmittedTask&& o) noexcept
        : active(o.active),
          task_id(o.task_id),
          target_node(o.target_node),
          local_pid(o.local_pid),
          response_pending(o.response_pending.load(std::memory_order_relaxed)),
          accept_status(o.accept_status),
          response_wait_entry(o.response_wait_entry),
          complete_pending(o.complete_pending.load(std::memory_order_relaxed)),
          complete_wait_entry(o.complete_wait_entry),
          exit_status(o.exit_status),
          accepted_at_us(o.accepted_at_us),
          complete_received_at_us(o.complete_received_at_us),
          local_task(o.local_task),
          proxy_ready(o.proxy_ready),
          ipc_fd_map(o.ipc_fd_map),
          ipc_fd_count(o.ipc_fd_count) {}
    auto operator=(SubmittedTask&& o) noexcept -> SubmittedTask& {
        if (this != &o) {
            active = o.active;
            task_id = o.task_id;
            target_node = o.target_node;
            local_pid = o.local_pid;
            response_pending.store(o.response_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            accept_status = o.accept_status;
            response_wait_entry = o.response_wait_entry;
            complete_pending.store(o.complete_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            complete_wait_entry = o.complete_wait_entry;
            exit_status = o.exit_status;
            accepted_at_us = o.accepted_at_us;
            complete_received_at_us = o.complete_received_at_us;
            local_task = o.local_task;
            proxy_ready = o.proxy_ready;
            ipc_fd_map = o.ipc_fd_map;
            ipc_fd_count = o.ipc_fd_count;
        }
        return *this;
    }
};

// -----------------------------------------------------------------------------
// Running remote task tracking (receiver side)
// -----------------------------------------------------------------------------

// D19: Output capture buffer for remote tasks (V2: increased from 1KB to 4KB)
constexpr uint16_t WKI_TASK_MAX_OUTPUT = 4096;

struct TaskOutputCapture {
    std::array<uint8_t, WKI_TASK_MAX_OUTPUT> data = {};
    uint16_t len = 0;
};

struct RunningRemoteTask {
    bool active = false;
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    ker::mod::sched::task::Task* task = nullptr;

    // D19: stdout/stderr capture
    TaskOutputCapture* output = nullptr;
};

constexpr size_t WKI_REMOTE_COMPUTE_DIAG_MAX = 96;

enum class WkiRemoteComputeDiagKind : uint8_t {
    UNKNOWN = 0,
    SUBMITTED = 1,
    RUNNING = 2,
    PENDING_COMPLETE = 3,
};

struct WkiRemoteComputeDiagCounts {
    size_t submitted_total = 0;
    size_t submitted_active = 0;
    size_t running_total = 0;
    size_t running_active = 0;
    size_t pending_completions = 0;
    size_t truncated = 0;
};

struct WkiRemoteComputeDiagRow {
    WkiRemoteComputeDiagKind kind = WkiRemoteComputeDiagKind::SUBMITTED;
    uint32_t task_id = 0;
    uint16_t peer_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    uint64_t local_task_ptr = 0;
    bool active = false;
    bool response_pending = false;
    bool complete_pending = false;
    bool proxy_ready = false;
    bool has_local_task = false;
    int32_t exit_status = 0;
    uint64_t accepted_age_us = 0;
    uint64_t complete_age_us = 0;
    uint16_t ipc_fd_count = 0;
    uint16_t output_len = 0;
};

enum class WkiRemoteSpawnResult : uint8_t {
    LOCAL = 0,
    REMOTE = 1,
    FAILED = 2,
};

struct WkiRemoteSpawnSpec {
    const char* const* argv = nullptr;
    const char* const* envp = nullptr;
    const char* cwd = nullptr;
};

struct WkiSharedElfCacheStats {
    uint64_t entries;
    uint64_t bytes;
    uint64_t max_entries;
    uint64_t max_bytes;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote compute subsystem. Called from wki_init().
void wki_remote_compute_init();

// Submitter side: submit a task with inline binary.
// Returns task_id on success, 0 on failure.
auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len,
                            const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map = nullptr,
                            uint16_t ipc_fd_count = 0) -> uint32_t;

// Submitter side: submit a task via VFS_REF (path-based delivery).
// The remote node loads the ELF from its VFS (typically via /wki/<hostname>/... mount).
// Returns task_id on success, 0 on failure.
auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path,
                             const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map = nullptr,
                             uint16_t ipc_fd_count = 0) -> uint32_t;

// Rich remote placement path used by spawn/exec code that already has the final
// argv/envp/cwd context. On success, the task is converted into a proxy.
auto wki_try_remote_spawn(ker::mod::sched::task::Task* task, const WkiRemoteSpawnSpec& spec) -> WkiRemoteSpawnResult;

// Mark a proxy task as fully parked in the wait queue.
// Used by execve-style handoff after the current task has actually blocked.
void wki_proxy_task_blocked(ker::mod::sched::task::Task* task);

// Submitter side: wait for a submitted task to complete.
// Returns 0 on success, -1 on timeout/error.
auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int;

// Submitter side: cancel a submitted task with the specified signal semantics.
void wki_task_cancel(uint32_t task_id, int signum);

// Load reporting: send current load to all peers.
// Called periodically from wki_peer_timer_tick().
void wki_load_report_send();

// Query cached remote load for a specific node.
auto wki_remote_node_load(uint16_t node_id) -> const RemoteNodeLoad*;

// Find the least-loaded remote node (or WKI_NODE_INVALID if none better).
// local_load: the caller's local load (0-1000) for comparison.
auto wki_least_loaded_node(uint16_t local_load) -> uint16_t;

// Fencing cleanup
void wki_remote_compute_cleanup_for_peer(uint16_t node_id);

// Release a shared cached ELF buffer acquired for remote execution.
// Returns true when the buffer was owned by the shared cache and must not be
// deleted by the caller.
auto wki_remote_compute_release_elf_buffer(uint8_t* buffer) -> bool;
auto wki_shared_elf_cache_stats() -> WkiSharedElfCacheStats;

// V2 A7.4: Forward an interrupt/termination signal to a remote task if the
// target is a WKI proxy. Called from kill()/process-group signal delivery.
// Returns true if signal was handled (forwarded).
auto wki_proxy_task_forward_signal(ker::mod::sched::task::Task* task, int signum) -> bool;

// Submitter side: resolve a remote execution PID back to the local proxy task.
// Returns a refcounted task pointer on an unambiguous match; caller must release.
auto wki_proxy_task_find_by_remote_pid_safe(uint64_t remote_pid) -> ker::mod::sched::task::Task*;

// Submitter side: read the runner node for a proxy task.
auto wki_proxy_task_remote_info(const ker::mod::sched::task::Task* task, uint16_t* target_node, char* hostname, size_t hostname_size)
    -> bool;
auto wki_remote_compute_diag_snapshot(WkiRemoteComputeDiagRow* rows, size_t capacity, WkiRemoteComputeDiagCounts* counts) -> size_t;

// Check running remote tasks for completion (called from timer tick).
// When a task exits, sends TASK_COMPLETE back to the submitter.
void wki_remote_compute_check_completions();

// Process deferred VFS_REF/RESOURCE_REF task submits.  Drains the pending
// queue inline (still called from the timer tick deferred section as a
// fallback).  The primary processing path is the dedicated compute submit
// kernel thread started by wki_remote_compute_start_submit_thread().
void wki_remote_compute_process_pending_submits();

// Start the dedicated kernel thread that processes VFS_REF/RESOURCE_REF
// task submits.  Must be called after the scheduler is running.
void wki_remote_compute_start_submit_thread();

// Wake the compute submit thread after queuing a new pending submit.
void wki_remote_compute_notify_pending_submit();

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_cleanup_marks_unready_proxy_failure() -> bool;
auto wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence() -> bool;
auto wki_remote_compute_selftest_task_wait_consumes_completed_row() -> bool;
#endif

// -----------------------------------------------------------------------------
// Internal - RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

// Receiver side: handle incoming task submission
void handle_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// Submitter side: handle accept/reject response
void handle_task_accept(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_task_reject(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// Submitter side: handle task completion
void handle_task_complete(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// Receiver side: handle cancel request
void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// Receiver side: handle incoming load report
void handle_load_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
