#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

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

    std::atomic<bool> response_pending{false};
    uint8_t accept_status = 0;  // TaskRejectReason
    uint64_t remote_pid = 0;
    WkiWaitEntry* response_wait_entry = nullptr;  // V2 I-4: async wait for TASK_ACCEPT/REJECT

    std::atomic<bool> complete_pending{false};
    WkiWaitEntry* complete_wait_entry = nullptr;  // V2 I-4: async wait for TASK_COMPLETE
    int32_t exit_status = 0;

    // V2§A7: Proxy task pointer — kept alive in WAITING state until remote completes
    ker::mod::sched::task::Task* local_task = nullptr;

    SubmittedTask() = default;
    SubmittedTask(const SubmittedTask&) = delete;
    auto operator=(const SubmittedTask&) -> SubmittedTask& = delete;
    SubmittedTask(SubmittedTask&& o) noexcept
        : active(o.active),
          task_id(o.task_id),
          target_node(o.target_node),
          response_pending(o.response_pending.load(std::memory_order_relaxed)),
          accept_status(o.accept_status),
          remote_pid(o.remote_pid),
          response_wait_entry(o.response_wait_entry),
          complete_pending(o.complete_pending.load(std::memory_order_relaxed)),
          complete_wait_entry(o.complete_wait_entry),
          exit_status(o.exit_status),
          local_task(o.local_task) {}
    auto operator=(SubmittedTask&& o) noexcept -> SubmittedTask& {
        if (this != &o) {
            active = o.active;
            task_id = o.task_id;
            target_node = o.target_node;
            response_pending.store(o.response_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            accept_status = o.accept_status;
            remote_pid = o.remote_pid;
            response_wait_entry = o.response_wait_entry;
            complete_pending.store(o.complete_pending.load(std::memory_order_relaxed), std::memory_order_relaxed);
            complete_wait_entry = o.complete_wait_entry;
            exit_status = o.exit_status;
            local_task = o.local_task;
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

    // D19: stdout/stderr capture
    TaskOutputCapture* output = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote compute subsystem. Called from wki_init().
void wki_remote_compute_init();

// Submitter side: submit a task with inline binary.
// Returns task_id on success, 0 on failure.
auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len, const void* args, uint16_t args_len) -> uint32_t;

// Submitter side: submit a task via VFS_REF (path-based delivery).
// The remote node loads the ELF from its VFS (typically via /wki/<hostname>/... mount).
// Returns task_id on success, 0 on failure.
auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path, const void* args, uint16_t args_len) -> uint32_t;

// Submitter side: wait for a submitted task to complete.
// Returns 0 on success, -1 on timeout/error.
auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int;

// Submitter side: cancel a submitted task.
void wki_task_cancel(uint32_t task_id);

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

// V2§A7.4: Forward signal to remote task if the target is a WKI proxy.
// Called from kill() syscall. Returns true if signal was handled (forwarded).
auto wki_proxy_task_forward_signal(ker::mod::sched::task::Task* task, int signum) -> bool;

// Check running remote tasks for completion (called from timer tick).
// When a task exits, sends TASK_COMPLETE back to the submitter.
void wki_remote_compute_check_completions();

// -----------------------------------------------------------------------------
// Internal — RX message handlers
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
