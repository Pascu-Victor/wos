#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/resource_contract.hpp>
#include <net/wki/service_contract.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <optional>
#include <span>

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
    // Submitter-local reservations bridge selection and SubmittedTask
    // publication. This field is never serialized on the wire.
    uint32_t placement_reservations = 0;
    // Balanced selections completed since this peer's last load report. Fast
    // tasks may finish before the next report or selector call; retaining this
    // sample-window debt keeps short bursts capacity-weighted.
    uint32_t balanced_assignments = 0;
    // Monotonic submitter-local selection sequence used only as a bounded
    // starvation guard among otherwise eligible balanced candidates.
    uint64_t balanced_last_assignment_seq = 0;
};

// -----------------------------------------------------------------------------
// Submitted task tracking (submitter side)
// -----------------------------------------------------------------------------

struct SubmittedTask {
    bool active = false;
    uint32_t task_id = 0;
    uint16_t target_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    WkiChannel* submit_channel = nullptr;
    uint32_t submit_channel_generation = 0;

    std::atomic<bool> response_pending{false};
    uint8_t accept_status = 0;  // TaskRejectReason
    uint8_t signal_version = 0;
    bool exec_handoff = false;
    bool exec_prepared = false;
    bool exec_cutover = false;
    uint64_t exec_remote_pid = 0;
    WkiWaitEntry* response_wait_entry = nullptr;  // V2 I-4: async wait for TASK_ACCEPT/REJECT
    // Handler ownership clears response_wait_entry before waking. Keep the
    // caller pinned separately until it refinds this row and consumes status.
    WkiWaitEntry* response_consumer_wait_entry = nullptr;

    std::atomic<bool> complete_pending{false};
    WkiWaitEntry* complete_wait_entry = nullptr;  // V2 I-4: async wait for TASK_COMPLETE
    WkiWaitEntry* complete_consumer_wait_entry = nullptr;
    int32_t exit_status = 0;
    uint64_t accepted_at_us = 0;
    uint64_t complete_received_at_us = 0;

    // V2 A7: Proxy task pointer - kept alive in WAITING state until remote completes.
    // A non-null slot owns exactly one Task lifetime reference. Completion
    // transfers that reference to its out-of-lock finalizer; ordinary result
    // cleanup releases it in place.
    ker::mod::sched::task::Task* local_task = nullptr;
    bool proxy_ready = false;
    // An accepted direct-submit ID remains a valid one-shot wait handle until
    // wki_try_remote_spawn transfers it to proxy ownership or wki_task_wait
    // consumes the terminal result. The owner is the submitting kernel task,
    // which may differ from local_task when a parent is publishing a child.
    bool result_handle_owned = false;
    ker::mod::sched::task::Task* result_owner_task = nullptr;
    bool reclaim_requested = false;
    // Completion can arrive before the exec handoff has parked the local
    // proxy. Keep an owned copy for wki_proxy_task_blocked() rather than a raw
    // task/File pointer across the compute lock.
    uint8_t* pending_proxy_output = nullptr;
    uint16_t pending_proxy_output_len = 0;
    std::optional<TaskResourceSnapshot> resource_baseline;
    std::optional<TaskResourceSnapshot> resource_completion;
    ker::mod::sched::resource::ServiceReportTarget service_report;
    bool service_report_pending = false;
    bool submission_started = false;

    void cancel_service_report(bool incomplete = true) {
        service_report.cancel(incomplete);
        service_report_pending = false;
    }

    WkiIpcChannels ipc_channels{};
    std::array<WkiIpcFdEntry, 16> ipc_fd_map = {};
    uint16_t ipc_fd_count = 0;
    // Pins for the exact local File identities represented by ipc_fd_map.
    // These move here before the interruptible TASK_ACCEPT wait so fatal task
    // cleanup can release them even when the submitter's stack is abandoned.
    std::array<WkiIpcTaskFdHandoff, 16> ipc_fd_handoff = {};
    uint16_t handoff_pin_count = 0;

    SubmittedTask() = default;
    ~SubmittedTask() {
        reset_local_task_ref();
        delete[] pending_proxy_output;
    }
    SubmittedTask(const SubmittedTask&) = delete;
    auto operator=(const SubmittedTask&) -> SubmittedTask& = delete;
    SubmittedTask(SubmittedTask&& o) noexcept
        : active(o.active),
          task_id(o.task_id),
          target_node(o.target_node),
          local_pid(o.local_pid),
          submit_channel(o.submit_channel),
          submit_channel_generation(o.submit_channel_generation),
          response_pending(o.response_pending.load(std::memory_order_relaxed)),
          accept_status(o.accept_status),
          signal_version(o.signal_version),
          exec_handoff(o.exec_handoff),
          exec_prepared(o.exec_prepared),
          exec_cutover(o.exec_cutover),
          exec_remote_pid(o.exec_remote_pid),
          response_wait_entry(o.response_wait_entry),
          response_consumer_wait_entry(o.response_consumer_wait_entry),
          complete_pending(o.complete_pending.load(std::memory_order_relaxed)),
          complete_wait_entry(o.complete_wait_entry),
          complete_consumer_wait_entry(o.complete_consumer_wait_entry),
          exit_status(o.exit_status),
          accepted_at_us(o.accepted_at_us),
          complete_received_at_us(o.complete_received_at_us),
          local_task(o.local_task),
          proxy_ready(o.proxy_ready),
          result_handle_owned(o.result_handle_owned),
          result_owner_task(o.result_owner_task),
          reclaim_requested(o.reclaim_requested),
          pending_proxy_output(o.pending_proxy_output),
          pending_proxy_output_len(o.pending_proxy_output_len),
          resource_baseline(o.resource_baseline),
          resource_completion(o.resource_completion),
          service_report(std::move(o.service_report)),
          service_report_pending(o.service_report_pending),
          submission_started(o.submission_started),
          ipc_channels(o.ipc_channels),
          ipc_fd_map(o.ipc_fd_map),
          ipc_fd_count(o.ipc_fd_count),
          ipc_fd_handoff(o.ipc_fd_handoff),
          handoff_pin_count(o.handoff_pin_count) {
        o.service_report_pending = false;
        o.submission_started = false;
        o.local_task = nullptr;
        o.pending_proxy_output = nullptr;
        o.pending_proxy_output_len = 0;
        o.ipc_fd_handoff = {};
        o.handoff_pin_count = 0;
    }
    auto operator=(SubmittedTask&& o) noexcept -> SubmittedTask& {
        if (this != &o) {
            reset_local_task_ref();
            active = o.active;
            task_id = o.task_id;
            target_node = o.target_node;
            local_pid = o.local_pid;
            submit_channel = o.submit_channel;
            submit_channel_generation = o.submit_channel_generation;
            response_pending.store(o.response_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            accept_status = o.accept_status;
            signal_version = o.signal_version;
            exec_handoff = o.exec_handoff;
            exec_prepared = o.exec_prepared;
            exec_cutover = o.exec_cutover;
            exec_remote_pid = o.exec_remote_pid;
            response_wait_entry = o.response_wait_entry;
            response_consumer_wait_entry = o.response_consumer_wait_entry;
            complete_pending.store(o.complete_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            complete_wait_entry = o.complete_wait_entry;
            complete_consumer_wait_entry = o.complete_consumer_wait_entry;
            exit_status = o.exit_status;
            accepted_at_us = o.accepted_at_us;
            complete_received_at_us = o.complete_received_at_us;
            local_task = o.local_task;
            o.local_task = nullptr;
            proxy_ready = o.proxy_ready;
            result_handle_owned = o.result_handle_owned;
            result_owner_task = o.result_owner_task;
            reclaim_requested = o.reclaim_requested;
            delete[] pending_proxy_output;
            pending_proxy_output = o.pending_proxy_output;
            pending_proxy_output_len = o.pending_proxy_output_len;
            resource_baseline = o.resource_baseline;
            resource_completion = o.resource_completion;
            service_report = std::move(o.service_report);
            service_report_pending = o.service_report_pending;
            submission_started = o.submission_started;
            o.service_report_pending = false;
            o.submission_started = false;
            o.pending_proxy_output = nullptr;
            o.pending_proxy_output_len = 0;
            ipc_channels = o.ipc_channels;
            ipc_fd_map = o.ipc_fd_map;
            ipc_fd_count = o.ipc_fd_count;
            ipc_fd_handoff = o.ipc_fd_handoff;
            handoff_pin_count = o.handoff_pin_count;
            o.ipc_fd_handoff = {};
            o.handoff_pin_count = 0;
        }
        return *this;
    }

    [[nodiscard]] auto set_local_task_ref(ker::mod::sched::task::Task* task) -> bool {
        if (local_task == task) {
            return true;
        }
        if (task != nullptr && !task->try_acquire_lifetime_ref()) {
            return false;
        }
        reset_local_task_ref();
        local_task = task;
        return true;
    }

    [[nodiscard]] auto take_local_task_ref() -> ker::mod::sched::task::Task* {
        auto* task = local_task;
        local_task = nullptr;
        return task;
    }

    void reset_local_task_ref() {
        auto* task = take_local_task_ref();
        if (task != nullptr) {
            task->release();
        }
    }
};

// -----------------------------------------------------------------------------
// Running remote task tracking (receiver side)
// -----------------------------------------------------------------------------

// D19: Output capture buffer for remote tasks (V2: increased from 1KB to 4KB)
constexpr uint16_t WKI_TASK_MAX_OUTPUT = 4096;

class TaskOutputCapture {
   public:
    TaskOutputCapture() = default;
    ~TaskOutputCapture();
    TaskOutputCapture(const TaskOutputCapture&) = delete;
    auto operator=(const TaskOutputCapture&) -> TaskOutputCapture& = delete;
    void retain();
    void release();
    // Input is kernel-owned memory (VFS bounces user writes before fops).
    void append(std::span<const uint8_t> bytes);
    auto size() -> uint16_t;
    // The returned view stays immutable and valid while the caller owns a ref.
    auto freeze() -> std::span<const uint8_t>;

   private:
    std::atomic<unsigned> refs{1};  // initial unpublished/monitor owner
    ker::mod::sys::Spinlock lock;
    std::array<uint8_t, WKI_TASK_MAX_OUTPUT> data = {};
    uint16_t len = 0;
    bool frozen = false;
};

struct RunningRemoteTask {
    bool active = false;
    bool published = false;
    bool exec_handoff = false;
    bool exec_committed = false;
    bool group_signal_ready = false;
    TaskExecSignals exec_signals{};
    bool accept_pending = false;
    bool discard_completion = false;
    bool termination_requested = false;
    bool resource_contract = false;
    bool root_completion_queued = false;
    bool service_report_inflight = false;
    uint64_t service_report_sequence = 0;
    uint64_t service_report_attempt_us = 0;
    ker::mod::sched::resource::RemoteServiceAccounting* service_accounting = nullptr;
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    uint64_t submit_session_epoch = 0;
    WkiChannel* submit_rx_channel = nullptr;
    uint32_t submit_rx_channel_generation = 0;
    ker::mod::sched::task::Task* task = nullptr;

    // D19: stdout/stderr capture
    TaskOutputCapture* output = nullptr;
    WkiIpcOutputCompletion* output_completion = nullptr;
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
    uint32_t channel_generation = 0;
    uint64_t submit_session_epoch = 0;
    bool active = false;
    bool response_pending = false;
    bool complete_pending = false;
    bool response_waiter_owned = false;
    bool response_consumer_owned = false;
    bool complete_waiter_owned = false;
    bool complete_consumer_owned = false;
    bool proxy_ready = false;
    bool has_local_task = false;
    bool published = false;
    bool accept_pending = false;
    bool discard_completion = false;
    bool termination_requested = false;
    bool result_handle_owned = false;
    bool has_result_owner = false;
    bool reclaim_requested = false;
    int32_t exit_status = 0;
    uint64_t accepted_age_us = 0;
    uint64_t complete_age_us = 0;
    uint16_t ipc_fd_count = 0;
    uint16_t ipc_handoff_pin_count = 0;
    uint16_t output_len = 0;
    uint16_t pending_proxy_output_len = 0;
};

enum class WkiRemoteSpawnResult : uint8_t {
    LOCAL = 0,
    REMOTE = 1,
    FAILED = 2,
};

// Reliable-RX ownership result for TASK_COMPLETE/TASK_CANCEL. RETRY means the
// channel must not advance or ACK the frame; DEFERRED and DISCARD mean the
// frame is consumed without synchronous handler dispatch.
enum class WkiComputeRxAdmission : uint8_t {
    INLINE,
    DEFERRED,
    DISCARD,
    RETRY,
};

// Borrowed only for a synchronous process-owned exec submission. The owned
// exec plan retains these strings/bytes until submission and callbacks finish;
// the current Task continues to describe its committed image.
struct WkiRemoteExecCandidate {
    const char* path = nullptr;
    std::span<const uint8_t> bytes{};
    bool complete = false;
    const char* target = nullptr;
    uint32_t target_flags = 0;
};

// Value-only native wait continuation. A handle is bound to the exact native
// Task and cannot outlive deferred kernel ownership by borrowing a syscall stack.
enum class WkiChildWaitState : uint8_t { LOCAL, PENDING, CLAIMED, COMPLETE, EMPTY, NO_CHILD, ERROR };
struct WkiChildWaitResult {
    WkiChildWaitState state{WkiChildWaitState::PENDING};
    int error{};
    uint64_t pid{}, user_time_us{}, system_time_us{};
    int32_t status{};
};
auto wki_child_wait_begin(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& waiter, int64_t pid, int32_t options,
                          uint64_t& handle) -> WkiChildWaitResult;
auto wki_child_wait_poll(ker::mod::sched::task::Task& waiter, uint64_t handle) -> WkiChildWaitResult;
auto wki_child_wait_commit(ker::mod::sched::task::Task& waiter, uint64_t handle) -> bool;
void wki_child_wait_close(ker::mod::sched::task::Task& waiter, uint64_t handle);
// After exec has closed native admission: cancel observations and wait for any
// detached commit/accounting step before capturing the next execution baseline.
auto wki_child_wait_quiesce(ker::mod::sched::task::Task& owner) -> bool;

struct WkiRemoteSpawnSpec {
    const char* const* argv = nullptr;
    const char* const* envp = nullptr;
    const char* cwd = nullptr;
    // Optional synchronous result: negative errno on FAILED, zero otherwise.
    int* error_out = nullptr;
    // Synchronous process-owned exec hooks. Neither callback runs in RX or
    // with compute/peer locks held. Spawn and legacy placement leave them null.
    int (*prepare)(ker::mod::sched::task::Task&, void*) = nullptr;
    void (*accepted)(ker::mod::sched::task::Task&, void*) = nullptr;
    // Irreversible process-owned transfer after receiver preparation. Fills
    // the final signal snapshot after closing/draining source admission.
    // Negative errno is permitted only before any irreversible publication.
    int (*cutover)(ker::mod::sched::task::Task&, uint32_t, uint64_t, TaskExecSignals&, void*) = nullptr;
    void* context = nullptr;
    const TaskExecSignals* exec_signals = nullptr;
    // A current executor can defer scheduler identity until its owned
    // preparation/rollback cleanup has finished. Accepted signal routing is
    // already published independently by the matching response handler.
    uint32_t* deferred_proxy_id = nullptr;
    // Borrowed for this synchronous call. Exec supplies its owned root/cwd
    // capture so both delivery modes encode one filesystem state.
    const ker::mod::sched::task::Task::FilesystemContext* filesystem = nullptr;
    const TaskExecCredentialTransition* exec_credentials = nullptr;
    // Spawn's original read identity; the private child already has the final
    // identity. Borrowed only for synchronous preparation and serialization.
    const ker::mod::security::Snapshot* image_credentials = nullptr;
    const WkiRemoteExecCandidate* exec_candidate = nullptr;
};

auto wki_exec_signal_snapshot(const ker::mod::sched::task::Task& task) -> TaskExecSignals;

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
// Returns task_id on success, 0 on failure; optional error_out is zero or negative errno.
auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len,
                            const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map = nullptr,
                            uint16_t ipc_fd_count = 0, WkiIpcTaskFdHandoff* ipc_fd_handoff = nullptr, uint8_t stdio_mask = UINT8_MAX,
                            int* error_out = nullptr, WkiIpcSubmissionGuard* preparation = nullptr,
                            const TaskExecSignals* exec_signals = nullptr, const WkiRemoteSpawnSpec* transfer = nullptr,
                            const ker::mod::sched::task::Task::FilesystemContext* filesystem = nullptr) -> uint32_t;

// Submitter side: submit a task via VFS_REF (path-based delivery).
// The remote node loads the ELF from its VFS (typically via /wki/<hostname>/... mount).
// Returns task_id on success, 0 on failure; optional error_out is zero or negative errno.
auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path,
                             const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                             const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map = nullptr,
                             uint16_t ipc_fd_count = 0, WkiIpcTaskFdHandoff* ipc_fd_handoff = nullptr, uint8_t stdio_mask = UINT8_MAX,
                             int* error_out = nullptr, WkiIpcSubmissionGuard* preparation = nullptr,
                             const TaskExecSignals* exec_signals = nullptr, const WkiRemoteSpawnSpec* transfer = nullptr,
                             const ker::mod::sched::task::Task::FilesystemContext* filesystem = nullptr) -> uint32_t;

// Rich remote placement path used by spawn/exec code that already has the final
// argv/envp/cwd context. On success, the task is converted into a proxy.
auto wki_try_remote_spawn(ker::mod::sched::task::Task* task, const WkiRemoteSpawnSpec& spec) -> WkiRemoteSpawnResult;

// Mark a proxy task as fully parked in the wait queue.
// Used by execve-style handoff after the current task has actually blocked.
void wki_proxy_task_blocked(ker::mod::sched::task::Task* task);

// Submitter side: wait for a submitted task to complete.
// Returns 0 on success, -1 on timeout/error.
auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int;

// Retire stack-wait ownership after generic task-exit cleanup has claimed or
// quiesced every still-linked WkiWaitEntry owned by this task.
void wki_remote_compute_cleanup_for_task(ker::mod::sched::task::Task* task);

// Submitter side: cancel a submitted task with the specified signal semantics.
auto wki_task_cancel(uint32_t task_id, int signum) -> bool;

// Load reporting: send current load to all peers.
// Called periodically from wki_peer_timer_tick().
void wki_load_report_send();

// Return the locally cached node load percentage on the 0-1000 scale used by
// LOAD_REPORT. Falls back to a queue-depth approximation until the first
// accounting interval has been sampled.
auto wki_local_node_load_pct() -> uint16_t;

// Query cached remote load for a specific node. Copies the row under the
// remote-compute lock so callers never retain a pointer into the load deque.
auto wki_remote_node_load_snapshot(uint16_t node_id, RemoteNodeLoad* out) -> bool;

// Fencing cleanup
// Retire receiver-side TASK_SUBMIT admission for a channel/session reset.
void wki_remote_compute_retire_submit_session(uint16_t node_id);
// Task-context cleanup after an ordinary peer epoch/channel reset. Only work
// bound to a retired resource-channel generation is terminalized.
void wki_remote_compute_cleanup_retired_for_peer(uint16_t node_id);
void wki_remote_compute_cleanup_for_peer(uint16_t node_id);

// Release a shared cached ELF buffer acquired for remote execution.
// Returns true when the buffer was owned by the shared cache and must not be
// deleted by the caller.
auto wki_remote_compute_release_elf_buffer(uint8_t* buffer) -> bool;
auto wki_shared_elf_cache_stats() -> WkiSharedElfCacheStats;

// A present result owns the remote terminal event, including refusal. Never
// fall back to the parked source frame or to lifecycle TASK_CANCEL.
// Borrows a retained lifecycle owner. Allocation-free coalescing enqueue only;
// daemon workers own forwarding/retry, never the producer's stack or identity.
void wki_defer_child_signal(ker::mod::sched::task::Task& target, uint16_t hops);
auto wki_proxy_task_parent_death(ker::mod::sched::task::Task& target, uint16_t hops = WKI_PARENT_DEATH_HOPS) -> std::optional<int>;

auto wki_proxy_task_terminal_signal(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum,
                                    uint16_t hops = WKI_TERMINAL_SIGNAL_HOPS) -> std::optional<int>;
// User-originated operation. A present result must never fall back to local
// proxy delivery or credential-free lifecycle cancellation.
auto wki_proxy_task_signal_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
                              const ker::mod::sched::session::Identity& caller_job, int signum) -> std::optional<int>;
auto wki_group_signal_as(const ker::mod::security::Snapshot& caller, const ker::mod::sched::session::Identity& caller_job, uint64_t pgid,
                         bool own_group, int signum) -> int;

// Submitter side: resolve a remote execution PID back to the local proxy task.
// Returns a refcounted task pointer on an unambiguous match; caller must release.
auto wki_proxy_task_find_by_remote_pid_safe(uint64_t remote_pid) -> ker::mod::sched::task::Task*;

// Submitter side: read the runner node for a proxy task.
auto wki_proxy_task_remote_info(const ker::mod::sched::task::Task* task, uint16_t* target_node, char* hostname, size_t hostname_size)
    -> bool;
// Resolve an already captured execution identity without reading a Task again.
auto wki_submitted_task_remote_info(uint32_t task_id, uint16_t* target_node, char* hostname, size_t hostname_size) -> bool;
auto wki_remote_compute_diag_snapshot(WkiRemoteComputeDiagRow* rows, size_t capacity, WkiRemoteComputeDiagCounts* counts) -> size_t;

// Check running remote tasks for completion (called from timer tick).
// When a task exits, sends TASK_COMPLETE back to the submitter.
void wki_remote_compute_check_completions();

// Wake completion monitoring when an exact receiver-side remote task reaches
// the same exit-ready publication point observed by local waiters.
void wki_remote_compute_notify_task_exit_ready(ker::mod::sched::task::Task* task);

// Process deferred TASK_SUBMIT messages. Drains the pending queue inline as a
// fallback; the primary path is the bounded compute-submit worker pool started
// by wki_remote_compute_start_submit_thread().
void wki_remote_compute_process_pending_submits();

// Start the bounded kernel-thread pool that processes all TASK_SUBMIT modes.
// Must be called after the scheduler is running.
void wki_remote_compute_start_submit_thread();

// Start the bounded peer-sharded pool that processes TASK_COMPLETE and
// TASK_CANCEL outside network RX/NAPI context.
void wki_remote_compute_start_rx_threads();

// Called with the reliable channel lock held. This function only validates
// and copies into preallocated storage; it never allocates, blocks, or wakes a
// task. Notify after dropping the channel lock when DEFERRED is returned.
auto wki_remote_compute_admit_rx(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation) -> WkiComputeRxAdmission;
// Own an authenticated submit-policy rejection before reliable RX consumes the
// request. RETRY leaves a valid request unconsumed; the worker retains the
// negative response until reliable enqueue or exact session retirement.
auto wki_remote_compute_admit_submit_denial_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                               uint32_t rx_channel_generation) -> WkiComputeRxAdmission;
void wki_remote_compute_notify_deferred_rx(uint16_t src_node);

// Wake the compute-submit workers after queuing a new pending submit.
void wki_remote_compute_notify_pending_submit();

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_cleanup_marks_unready_proxy_failure() -> bool;
auto wki_remote_compute_selftest_proxy_child_retirement() -> bool;
auto wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence() -> bool;
auto wki_remote_compute_selftest_task_wait_consumes_completed_row() -> bool;
auto wki_remote_compute_selftest_task_wait_timeout_preserves_successor() -> bool;
auto wki_remote_compute_selftest_task_exit_retires_wait_owners() -> bool;
auto wki_remote_compute_selftest_submit_buffers_follow_executor() -> bool;
auto wki_remote_compute_selftest_slot_allocation_failure() -> bool;
auto wki_remote_compute_selftest_failed_submit_restores_guard() -> bool;
auto wki_remote_compute_selftest_task_exit_releases_handoff_pins() -> bool;
auto wki_remote_compute_selftest_ipc_cleanup_keeps_channels() -> bool;
auto wki_remote_compute_selftest_submitted_slots_reclaim_safely() -> bool;
auto wki_remote_compute_selftest_task_id_wrap_is_safe() -> bool;
auto wki_remote_compute_selftest_load_snapshot_survives_cleanup() -> bool;
auto wki_remote_compute_selftest_placement_score_accounts_for_inflight() -> bool;
auto wki_remote_compute_selftest_balanced_score_accounts_for_capacity() -> bool;
auto wki_remote_compute_selftest_automatic_policy_is_workload_agnostic() -> bool;
auto wki_remote_compute_selftest_submit_policy_scope_restores_worker() -> bool;
auto wki_remote_compute_selftest_receiver_system_paths_are_local_by_default() -> bool;
auto wki_remote_compute_selftest_submit_context_lengths_are_checked() -> bool;
auto wki_remote_compute_selftest_resource_context_before_fetch() -> bool;
auto wki_remote_compute_selftest_full_credential_context() -> bool;
auto wki_remote_compute_selftest_filesystem_preparation() -> bool;
auto wki_remote_compute_selftest_cwd_submission_admission(const ker::mod::sched::task::Task::FilesystemContext& captured,
                                                          int expected_status, const ker::mod::security::Snapshot& credentials) -> bool;
auto wki_remote_compute_selftest_filesystem_wire() -> bool;
auto wki_remote_compute_selftest_installed_cwd(const ker::vfs::DirectoryViewRef& view, const ker::mod::security::Snapshot& credentials,
                                               const char* expected_path, const char* root_path = nullptr) -> bool;
auto wki_remote_compute_selftest_filesystem_context(const ker::mod::sched::task::Task::FilesystemContext* owned = nullptr) -> bool;
auto wki_remote_compute_selftest_submission_errors() -> bool;
auto wki_remote_compute_selftest_cloexec_commit_preserves_other_fds() -> bool;
auto wki_remote_compute_selftest_cache_content_is_verified() -> bool;
auto wki_remote_compute_selftest_stdio_presence_and_rollback() -> bool;
auto wki_remote_compute_selftest_replay_file_policy() -> bool;
auto wki_remote_compute_selftest_capture_lifetime_and_freeze() -> bool;
auto wki_remote_compute_selftest_user_signal_ownership() -> bool;
auto wki_remote_compute_selftest_exec_handoff_ownership() -> bool;
auto wki_remote_compute_selftest_exec_cutover_retirement() -> bool;
auto wki_remote_compute_selftest_accepted_signal_route() -> bool;
auto wki_remote_compute_selftest_group_signal_ownership() -> bool;
auto wki_remote_compute_selftest_group_route() -> bool;
auto wki_remote_compute_selftest_group_target_snapshot() -> bool;
auto wki_remote_compute_selftest_group_receivers() -> bool;
auto wki_remote_compute_selftest_resource_completion_is_session_scoped() -> bool;
auto wki_remote_compute_selftest_parent_death_admission() -> bool;
auto wki_remote_compute_selftest_child_authority_admission() -> bool;
auto wki_remote_compute_selftest_child_signal_delivery() -> bool;
auto wki_remote_compute_selftest_child_wait_transactions() -> bool;
auto wki_remote_compute_selftest_native_child_wait() -> bool;
auto wki_remote_compute_selftest_child_query_ownership() -> bool;
auto wki_remote_compute_selftest_child_query_wire() -> bool;
auto wki_remote_compute_selftest_child_cpu_observations() -> bool;
auto wki_remote_compute_selftest_child_claim_wire() -> bool;
auto wki_remote_compute_selftest_terminal_signal_admission() -> bool;
auto wki_remote_compute_selftest_signal_version_slot_ownership() -> bool;
auto wki_remote_compute_selftest_service_report_transport() -> bool;
auto wki_remote_compute_selftest_resource_rejection_is_session_scoped() -> bool;
auto wki_remote_compute_selftest_service_completion_counts_once() -> bool;
auto wki_remote_compute_selftest_submit_fragment_boundaries() -> bool;
auto wki_remote_compute_selftest_submit_worker_count_is_bounded() -> bool;
auto wki_remote_compute_selftest_completion_queue_preserves_ownership() -> bool;
auto wki_remote_compute_selftest_running_registry_compacts_stably() -> bool;
auto wki_remote_compute_selftest_running_publication_failure_unwinds() -> bool;
auto wki_remote_compute_selftest_final_publication_and_cleanup() -> bool;
auto wki_remote_compute_selftest_accept_retry_is_fair() -> bool;
auto wki_remote_compute_selftest_rejection_retry_is_bounded() -> bool;
auto wki_remote_compute_selftest_submit_cancel_is_session_scoped() -> bool;
auto wki_remote_compute_selftest_admitted_peer_identity() -> bool;
auto wki_remote_compute_selftest_rx_admission_is_bounded() -> bool;
auto wki_remote_compute_selftest_exit_ready_completion_wake_is_exact() -> bool;
#endif

// -----------------------------------------------------------------------------
// Internal - RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

// Receiver side: handle incoming task submission
void handle_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation);

// Submitter side: handle accept/reject response
void handle_task_signal_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation);
void handle_group_route_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation);
void handle_group_receiver_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                    uint32_t rx_channel_generation);
void handle_group_signal_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                  uint32_t rx_channel_generation);
void handle_task_exec_ready(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                            uint32_t generation);
void handle_task_exec_commit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                             uint32_t generation);
void handle_task_accept(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation);
void handle_task_service_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                uint32_t rx_channel_generation);
void handle_task_reject(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation);

// Submitter side: handle task completion
void handle_task_complete(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                          uint32_t rx_channel_generation);

// Receiver side: handle cancel request
void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation);

// Receiver side: handle incoming load report
void handle_load_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
