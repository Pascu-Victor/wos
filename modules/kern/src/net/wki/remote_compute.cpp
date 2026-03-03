#include "remote_compute.hpp"

#include <extern/elf.h>

#include <algorithm>
#include <cstring>
#include <deque>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/vfs.hpp>

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

ker::vfs::FileOperations g_capture_fops = {
    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    .vfs_open = nullptr,     .vfs_close = capture_close,   .vfs_read = nullptr,    .vfs_write = capture_write,
    .vfs_lseek = nullptr,    .vfs_isatty = capture_isatty, .vfs_readdir = nullptr, .vfs_readlink = nullptr,
    .vfs_truncate = nullptr, .vfs_poll_check = nullptr,
};

// -----------------------------------------------------------------------------
//  Check if this node has a VFS export covering the given path.
// If so, build the remote-accessible VFS_REF path:
//   local path /sbin/init  +  export "/"  → /wki/<our_hostname>/sbin/init
// Returns true if a VFS_REF path was built.
// -----------------------------------------------------------------------------

auto build_vfs_ref_path(uint16_t /*target_node*/, const char* local_path, char* out, size_t out_size) -> bool {
    if (local_path == nullptr || local_path[0] == '\0') {
        return false;
    }

    // Check if target node is connected and knows our hostname
    const char* our_hostname = g_wki.local_hostname;
    if (our_hostname[0] == '\0') {
        return false;
    }

    // Check that a VFS export on this node covers the requested path.
    // The simplest check: if we export "/" (root), any local path is coverable.
    // For more specific exports, the local_path must start with the export path.
    bool covered = false;
    wki_resource_foreach(
        [](const DiscoveredResource& /*res*/, void* /*ctx*/) {
            // We actually need to check our LOCAL exports, not discovered remote ones.
            // This visitor iterates discovered (remote) resources. We need a different approach.
        },
        nullptr);

    // Instead, directly check our local VFS exports.
    // Iterate VFS exports: if any export_path is a prefix of local_path, it's covered.
    // We can't iterate g_vfs_exports directly (it's in remote_vfs.cpp), but we can
    // check if we have a root "/" export by checking if local_path is accessible via VFS.
    // Simplification: if local_path starts with "/" and the file exists, we likely export
    // a VFS covering it. The remote node will resolve /wki/<hostname>/<relative_path>.

    // Check that the file actually exists
    int fd = ker::vfs::vfs_open(local_path, 0, 0);
    if (fd < 0) {
        return false;
    }
    ker::vfs::vfs_close(fd);
    covered = true;

    if (!covered) {
        return false;
    }

    // Build the VFS_REF path: /wki/<our_hostname>/<local_path without leading />
    const char* stripped = local_path;
    while (*stripped == '/') stripped++;

    snprintf(out, out_size, "/wki/%s/%s", our_hostname, stripped);
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
    // Guard: need ELF buffer for inline submit
    if (task->elfBuffer == nullptr || task->elfBufferSize == 0) {
        return false;
    }

    // Guard: don't re-remote-place tasks that were already submitted remotely
    // (prevents infinite bounce between nodes)
    if (std::strncmp(task->name, "wki-remote", 10) == 0) {
        return false;
    }

    // Compute local load (0-1000 scale)
    uint16_t local_load = 0;
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::getCoreCount());
    if (cpu_count > 0) {
        uint16_t total_runnable = 0;
        for (uint16_t c = 0; c < cpu_count; c++) {
            auto stats = ker::mod::sched::get_run_queue_stats(c);
            total_runnable += static_cast<uint16_t>(stats.active_task_count);
        }
        auto pct = static_cast<uint16_t>((static_cast<uint32_t>(total_runnable) * 1000U) / cpu_count);
        local_load = std::min<uint16_t>(pct, 1000);
    }

    // Only attempt remote if a significantly less loaded node exists
    s_compute_lock.lock();
    uint16_t best_node = wki_least_loaded_node(local_load);
    s_compute_lock.unlock();
    if (best_node == WKI_NODE_INVALID) {
        return false;
    }

    // ===  Delivery mode selection ===
    uint32_t tid = 0;

    // 1. If task prefers inline AND binary fits, use INLINE
    bool binary_fits = (sizeof(TaskSubmitPayload) + sizeof(uint32_t) + task->elfBufferSize) <= WKI_ETH_MAX_PAYLOAD;

    if (!task->wki_prefer_inline && task->exe_path[0] != '\0') {
        // 2. Task has a valid exe_path — try VFS_REF
        char vfs_ref_path[512] = {};
        if (build_vfs_ref_path(best_node, task->exe_path, vfs_ref_path, sizeof(vfs_ref_path))) {
            tid = wki_task_submit_vfs_ref(best_node, vfs_ref_path, nullptr, 0);
            if (tid != 0) {
                ker::mod::dbg::log("[WKI] D17: VFS_REF delivery for '%s' -> '%s'", task->exe_path, vfs_ref_path);
            }
        }
    }

    // 3. Fallback to INLINE if VFS_REF failed or wasn't attempted
    if (tid == 0 && binary_fits) {
        tid = wki_task_submit_inline(best_node, task->elfBuffer, static_cast<uint32_t>(task->elfBufferSize), nullptr, 0);
    }

    if (tid == 0) {
        return false;  // Both delivery modes failed, fall through to local
    }

    // ===  Proxy task lifecycle ===
    // Keep the local task alive as a proxy in WAITING state.
    // Parent can waitpid() on it; we'll wake it when TASK_COMPLETE arrives.

    // Clean up the ELF buffer — the task won't run locally
    delete[] task->elfBuffer;
    task->elfBuffer = nullptr;
    task->elfBufferSize = 0;

    // Mark this task as a WKI proxy
    task->wki_proxy_task_id = tid;

    // Store the proxy task pointer in the submitted task entry
    s_compute_lock.lock();
    SubmittedTask* st = find_submitted_task(tid);
    if (st != nullptr) {
        st->local_task = task;
        st->complete_pending = true;  // We're now waiting for TASK_COMPLETE
    }
    s_compute_lock.unlock();

    // Transition to WAITING — task won't be scheduled but stays alive for waitpid()
    task->schedQueue = ker::mod::sched::task::Task::SchedQueue::WAITING;

    ker::mod::dbg::log("[WKI] D17: Task '%s' (pid=0x%lx) placed as proxy on node 0x%04x (task_id=%u)", task->name, task->pid, best_node,
                       tid);
    return true;
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

// ===============================================================================
// Submitter Side — Task Submit (INLINE only in V1)
// ===============================================================================

auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len, const void* args, uint16_t args_len)
    -> uint32_t {
    if (binary == nullptr || binary_len == 0) {
        return 0;
    }

    // Check total size fits in WKI message
    // TaskSubmitPayload(16) + binary_len(4) + binary + args
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint32_t) + binary_len + args_len);
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
    st.local_task = nullptr;

    g_submitted_tasks.push_back(std::move(st));
    SubmittedTask* task_ptr = &g_submitted_tasks.back();
    s_compute_lock.unlock();

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
    submit->args_len = args_len;
    submit->argc = 0;
    submit->envc = 0;
    submit->cwd_len = 0;
    submit->reserved = 0;

    // INLINE format: {binary_len:u32, binary[binary_len], args[args_len]}
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &binary_len, sizeof(uint32_t));
    cursor += sizeof(uint32_t);
    memcpy(cursor, binary, binary_len);
    cursor += binary_len;
    if (args != nullptr && args_len > 0) {
        memcpy(cursor, args, args_len);
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
        return 0;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    task_ptr->response_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        task_ptr->response_pending.store(false, std::memory_order_relaxed);
        task_ptr->active = false;
        ker::mod::dbg::log("[WKI] Task submit timeout: task_id=%u target=0x%04x", task_id, target_node);
        return 0;
    }

    if (task_ptr->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        ker::mod::dbg::log("[WKI] Task rejected: task_id=%u status=%u", task_id, task_ptr->accept_status);
        task_ptr->active = false;
        return 0;
    }

    ker::mod::dbg::log("[WKI] Task accepted: task_id=%u remote_pid=%lu", task_id, task_ptr->remote_pid);
    return task_id;
}

// ===============================================================================
// Submitter Side — VFS_REF Submit (/ ===============================================================================

auto wki_task_submit_vfs_ref(uint16_t target_node, const char* vfs_path, const void* args, uint16_t args_len) -> uint32_t {
    if (vfs_path == nullptr || vfs_path[0] == '\0') {
        return 0;
    }

    auto path_len = static_cast<uint16_t>(std::strlen(vfs_path));
    if (path_len == 0) {
        return 0;
    }

    // TaskSubmitPayload(16) + path_len_field(2) + path + args
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint16_t) + path_len + args_len);
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
    st.local_task = nullptr;

    g_submitted_tasks.push_back(std::move(st));
    SubmittedTask* task_ptr = &g_submitted_tasks.back();
    s_compute_lock.unlock();

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
    submit->args_len = args_len;
    submit->argc = 0;
    submit->envc = 0;
    submit->cwd_len = 0;
    submit->reserved = 0;

    // VFS_REF format: {path_len:u16, path[path_len], args[args_len]}
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &path_len, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    memcpy(cursor, vfs_path, path_len);
    cursor += path_len;
    if (args != nullptr && args_len > 0) {
        memcpy(cursor, args, args_len);
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
        return 0;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    task_ptr->response_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        task_ptr->response_pending.store(false, std::memory_order_relaxed);
        task_ptr->active = false;
        ker::mod::dbg::log("[WKI] VFS_REF submit timeout: task_id=%u", task_id);
        return 0;
    }

    if (task_ptr->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        ker::mod::dbg::log("[WKI] VFS_REF task rejected: task_id=%u status=%u", task_id, task_ptr->accept_status);
        task_ptr->active = false;
        return 0;
    }

    ker::mod::dbg::log("[WKI] VFS_REF task accepted: task_id=%u remote_pid=%lu", task_id, task_ptr->remote_pid);
    return task_id;
}

// ===============================================================================
// Submitter Side — Wait for Completion
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
    s_compute_lock.unlock();

    int wait_rc = wki_wait_for_op(&wait, timeout_us);
    task->complete_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        task->complete_pending.store(false, std::memory_order_relaxed);
        return -1;
    }

    if (exit_status != nullptr) {
        *exit_status = task->exit_status;
    }

    // Clean up
    task->active = false;
    return 0;
}

// ===============================================================================
// Submitter Side — Cancel
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

    // Only forward SIGKILL(9) and SIGTERM(15) — other signals have no meaning on remote
    if (signum != 9 && signum != 15) {
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
    auto cpu_count = static_cast<uint16_t>(ker::mod::smt::getCoreCount());
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

        wki_send(peer->node_id, WKI_CHAN_RESOURCE, MsgType::LOAD_REPORT, &buf[0],
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

        // Collect proxy task for wake-up outside the lock
        if (t.local_task != nullptr && proxy_count < MAX_PROXIES) {
            proxy_tasks[proxy_count++] = t.local_task;
            t.local_task = nullptr;
        }

        t.active = false;
    }

    // Remove stale entries
    std::erase_if(g_submitted_tasks, [](const SubmittedTask& t) { return !t.active; });

    // Invalidate load cache for this peer
    std::erase_if(g_remote_loads, [node_id](const RemoteNodeLoad& rl) { return rl.node_id == node_id; });

    // Cancel running remote tasks submitted by this peer (they'll exit on their own,
    // but we won't be able to send TASK_COMPLETE back)
    for (auto& rt : g_running_remote_tasks) {
        if (rt.active && rt.submitter_node == node_id) {
            delete rt.output;
            rt.output = nullptr;
            rt.active = false;
        }
    }
    std::erase_if(g_running_remote_tasks, [](const RunningRemoteTask& rt) { return !rt.active; });

    s_compute_lock.unlock();

    // V2 I-4: Wake any blocked waiters (outside lock)
    for (size_t i = 0; i < waiter_count; i++) {
        wki_wake_op(waiters_to_wake[i], -1);
    }

    // Wake proxy tasks with error exit status (outside lock — scheduling operations)
    for (size_t i = 0; i < proxy_count; i++) {
        auto* proxy = proxy_tasks[i];
        proxy->exitStatus = -1;  // EFAULT equivalent
        proxy->hasExited = true;
        proxy->wki_proxy_task_id = 0;

        // Wake waitpid() waiters
        for (uint64_t j = 0; j < proxy->awaitee_on_exit_count; ++j) {
            uint64_t waiting_pid = proxy->awaitee_on_exit[j];
            auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(waiting_pid);
            if (waiting_task != nullptr) {
                if (!waiting_task->deferredTaskSwitch) {
                    waiting_task->context.regs.rax = proxy->pid;
                    if (waiting_task->waitStatusPhysAddr != 0) {
                        auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::getVirtPointer(waiting_task->waitStatusPhysAddr));
                        *status_ptr = -1;
                    }
                }
                uint64_t cpu = ker::mod::sched::get_least_loaded_cpu();
                ker::mod::sched::reschedule_task_for_cpu(cpu, waiting_task);
                waiting_task->release();
            }
        }
        proxy->awaitee_on_exit_count = 0;

        // Transition proxy to DEAD
        proxy->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
        proxy->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
        proxy->schedQueue = ker::mod::sched::task::Task::SchedQueue::DEAD_GC;
        ker::mod::sched::insert_into_dead_list(proxy);

        ker::mod::dbg::log("[WKI] Proxy task fenced: pid=0x%lx (peer 0x%04x)", proxy->pid, node_id);
    }
}

// ===============================================================================
// Receiver Side — Completion Monitoring
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

        auto* task = ker::mod::sched::find_task_by_pid(rt.local_pid);
        if (task == nullptr) {
            // Task was garbage collected — treat as exited with unknown status
            completed = true;
        } else if (task->hasExited) {
            exit_status = task->exitStatus;
            completed = true;
        }

        if (!completed) {
            continue;
        }

        if (num_completions < MAX_COMPLETIONS) {
            completions[num_completions++] = {rt.task_id, rt.submitter_node, rt.local_pid, exit_status, rt.output};
        } else {
            delete rt.output;
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

        ker::mod::dbg::log("[WKI] Remote task completed: task_id=%u pid=0x%lx exit=%d output=%u bytes", info.task_id, info.local_pid,
                           info.exit_status, out_len);

        delete info.output;
    }
}

// ===============================================================================
// Internal Helpers — ELF execution + VFS loading
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
    TaskRejectReason reject_reason = TaskRejectReason::ACCEPTED;
};

auto exec_elf_buffer(uint8_t* elf_buffer, uint32_t binary_len) -> ExecResult {
    ExecResult result;

    // Validate ELF magic
    if (binary_len < sizeof(Elf64_Ehdr)) {
        delete[] elf_buffer;
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    const auto* elf_hdr = reinterpret_cast<const Elf64_Ehdr*>(elf_buffer);
    if (elf_hdr->e_ident[EI_MAG0] != ELFMAG0 || elf_hdr->e_ident[EI_MAG1] != ELFMAG1 || elf_hdr->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_hdr->e_ident[EI_MAG3] != ELFMAG3) {
        delete[] elf_buffer;
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    // Allocate kernel stack
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::pageAlloc(KERNEL_STACK_SIZE));
    if (stack_base == 0) {
        delete[] elf_buffer;
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    uint64_t kernel_rsp = stack_base + KERNEL_STACK_SIZE;

    // Create the process task
    auto* new_task = new ker::mod::sched::task::Task(  // NOLINT(cppcoreguidelines-owning-memory)
        "wki-remote", reinterpret_cast<uint64_t>(elf_buffer), kernel_rsp, ker::mod::sched::task::TaskType::PROCESS);

    if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
        delete new_task;
        delete[] elf_buffer;
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    new_task->elfBuffer = elf_buffer;
    new_task->elfBufferSize = binary_len;

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
        new_task->fds[fd_idx] = capture_file;
    }

    // Note: Caller is responsible for calling post_task_balanced() after
    // setting up argv/envp/cwd on the user stack (    result.task = new_task;
    result.output = output_cap;
    return result;
}

// ---------------------------------------------------------------------------
// D14: Load ELF binary from a VFS path. Returns owned buffer + size.
// ---------------------------------------------------------------------------

struct VfsLoadResult {
    uint8_t* buffer = nullptr;
    uint32_t size = 0;
    TaskRejectReason reject_reason = TaskRejectReason::ACCEPTED;
};

auto load_elf_from_vfs_path(const char* path) -> VfsLoadResult {
    VfsLoadResult result;

    int fd = ker::vfs::vfs_open(path, 0, 0);
    if (fd < 0) {
        ker::mod::dbg::log("[WKI] VFS_REF: failed to open '%s'", path);
        result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
        return result;
    }

    ssize_t file_size = ker::vfs::vfs_lseek(fd, 0, 2);  // SEEK_END
    if (file_size <= 0) {
        ker::vfs::vfs_close(fd);
        result.reject_reason = TaskRejectReason::BINARY_NOT_FOUND;
        return result;
    }
    ker::vfs::vfs_lseek(fd, 0, 0);  // SEEK_SET

    auto* buf = new uint8_t[static_cast<size_t>(file_size)];  // NOLINT(cppcoreguidelines-owning-memory)
    size_t actual = 0;
    ker::vfs::vfs_read(fd, buf, static_cast<size_t>(file_size), &actual);
    ker::vfs::vfs_close(fd);

    if (static_cast<ssize_t>(actual) != file_size) {
        delete[] buf;
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        return result;
    }

    result.buffer = buf;
    result.size = static_cast<uint32_t>(file_size);
    return result;
}

}  // namespace

// ===============================================================================
// Receiver Side — RX Handlers
// ===============================================================================

namespace detail {

void handle_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskSubmitPayload)) {
        return;
    }

    const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(payload);
    const uint8_t* var_data = payload + sizeof(TaskSubmitPayload);
    auto var_len = static_cast<uint16_t>(payload_len - sizeof(TaskSubmitPayload));

    ker::mod::dbg::log("[WKI] Task submit received: task_id=%u from node=0x%04x mode=%u", submit->task_id, hdr->src_node,
                       submit->delivery_mode);

    uint8_t* elf_buffer = nullptr;
    uint32_t binary_len = 0;
    TaskRejectReason reject_reason = TaskRejectReason::ACCEPTED;
    std::array<char, 256> exe_path_buf = {};
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

            auto vfs_result = load_elf_from_vfs_path(path_buf.data());
            if (vfs_result.buffer == nullptr) {
                reject_reason = vfs_result.reject_reason;
                break;
            }
            elf_buffer = vfs_result.buffer;
            binary_len = vfs_result.size;
            std::strncpy(exe_path_buf.data(), path_buf.data(), exe_path_buf.size() - 1);
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

            // Try to load from VFS path directly — if the remote resource is already
            // mounted (e.g., via wki_remote_vfs_mount), the path is accessible.
            // Construct full path: "/remote_<node>_<resource>/<ref_path>" or just try ref_path.
            // For simplicity: try the path as-is first (works if already mounted at that path).
            auto vfs_result = load_elf_from_vfs_path(ref_path.data());
            if (vfs_result.buffer == nullptr) {
                ker::mod::dbg::log("[WKI] RESOURCE_REF: failed to load node=0x%04x res=%u path='%s'", ref_node, ref_resource,
                                   ref_path.data());
                reject_reason = TaskRejectReason::FETCH_FAILED;
                break;
            }
            elf_buffer = vfs_result.buffer;
            binary_len = vfs_result.size;
            std::strncpy(exe_path_buf.data(), ref_path.data(), exe_path_buf.size() - 1);
            break;
        }

        default:
            reject_reason = TaskRejectReason::FETCH_FAILED;
            break;
    }

    // If binary loading failed, reject
    if (elf_buffer == nullptr || reject_reason != TaskRejectReason::ACCEPTED) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(reject_reason);
        reject.remote_pid = 0;
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        return;
    }

    // Execute the ELF buffer (creates task but does NOT schedule it yet)
    ExecResult exec = exec_elf_buffer(elf_buffer, binary_len);
    if (exec.task == nullptr) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(exec.reject_reason);
        reject.remote_pid = 0;
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        return;
    }

    // -----------------------------------------------------------------------
    // et exe_path, cwd, and minimal argv/envp on the user stack
    // -----------------------------------------------------------------------
    auto* new_task = exec.task;

    // Set exe_path from the resolved path
    std::strncpy(new_task->exe_path, exe_path_buf.data(), ker::mod::sched::task::Task::EXE_PATH_MAX - 1);
    new_task->exe_path[ker::mod::sched::task::Task::EXE_PATH_MAX - 1] = '\0';

    // Set CWD from submit payload (V2 extended field)
    if (submit->cwd_len > 0) {
        // CWD data is at the end of the variable portion (after args + env strings)
        // For now: submitter sends cwd_len=0 → use default "/"
        // When cwd_len>0 the cwd string follows after argc+envc NUL-separated strings
    }
    // Default CWD is already "/" from task constructor

    // Set up user stack: argc / argv / envp / auxv (System V x86-64 ABI)
    {
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
            if (page_phys == 0) {
                return 0;
            }
            auto* dest = reinterpret_cast<uint8_t*>(addr::getVirtPointer(page_phys)) + page_off;
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
            if (page_phys == 0) {
                return 0;
            }
            auto* dest = reinterpret_cast<uint8_t*>(addr::getVirtPointer(page_phys)) + page_off;
            std::memcpy(dest, str, len);
            return vaddr;
        };

        // Push program name as argv[0]
        uint64_t argv0_addr = push_string(exe_path_buf.data());

        // Align to 16 bytes
        constexpr uint64_t ALIGN = 16;
        uint64_t cur = user_stack_top - stack_offset;
        uint64_t aligned = cur & ~(ALIGN - 1);
        stack_offset += (cur - aligned);

        // Auxiliary vector (minimal: AT_PAGESZ, AT_ENTRY, AT_NULL)
        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_EHDR = 33;

        std::array<uint64_t, 10> auxv = {AT_PAGESZ, paging::PAGE_SIZE,
                                         AT_ENTRY,  new_task->entry,
                                         AT_PHDR,   new_task->programHeaderAddr,
                                         AT_EHDR,   new_task->elfHeaderAddr,
                                         AT_NULL,   0};
        for (int j = static_cast<int>(auxv.size()) - 1; j >= 0; --j) {
            uint64_t val = auxv[static_cast<size_t>(j)];
            push_to_stack(&val, sizeof(uint64_t));
        }

        // envp: NULL terminator (no env vars sent yet)
        uint64_t null_val = 0;
        uint64_t envp_ptr = push_to_stack(&null_val, sizeof(uint64_t));

        // argv: {argv0, NULL}
        uint64_t argv_data[2] = {argv0_addr, 0};  // NOLINT
        uint64_t argv_ptr = push_to_stack(static_cast<const void*>(argv_data), 2 * sizeof(uint64_t));

        // argc
        uint64_t argc_val = 1;
        push_to_stack(&argc_val, sizeof(uint64_t));

        // Update user RSP and ABI registers
        new_task->context.frame.rsp = user_stack_top - stack_offset;
        new_task->context.regs.rdi = argc_val;
        new_task->context.regs.rsi = argv_ptr;
        new_task->context.regs.rdx = envp_ptr;
    }

    // Post to scheduler
    if (!ker::mod::sched::post_task_balanced(new_task)) {
        delete new_task;
        delete exec.output;
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_REJECT, &reject, sizeof(reject));
        return;
    }

    // Track for completion monitoring
    RunningRemoteTask rt;
    rt.active = true;
    rt.task_id = submit->task_id;
    rt.submitter_node = hdr->src_node;
    rt.local_pid = exec.task->pid;
    rt.output = exec.output;
    s_compute_lock.lock();
    g_running_remote_tasks.push_back(rt);
    s_compute_lock.unlock();

    // Send TASK_ACCEPT
    TaskResponsePayload accept = {};
    accept.task_id = submit->task_id;
    accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
    accept.remote_pid = exec.task->pid;
    wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_ACCEPT, &accept, sizeof(accept));

    ker::mod::dbg::log("[WKI] Remote task launched: task_id=%u pid=0x%lx on CPU %d mode=%u", submit->task_id, exec.task->pid,
                       exec.task->cpu, submit->delivery_mode);
}

void handle_task_accept(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
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

    task->response_pending.store(false, std::memory_order_release);
    WkiWaitEntry* waiter = task->response_wait_entry;
    s_compute_lock.unlock();

    if (waiter != nullptr) {
        wki_wake_op(waiter, 0);
    }
}

void handle_task_reject(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
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
}

void handle_task_complete(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskCompletePayload)) {
        return;
    }

    const auto* comp = reinterpret_cast<const TaskCompletePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(comp->task_id);
    if (task == nullptr) {
        s_compute_lock.unlock();
        return;
    }

    task->exit_status = comp->exit_status;

    // ===  Wake proxy task ===
    auto* proxy = task->local_task;
    task->local_task = nullptr;
    s_compute_lock.unlock();

    if (proxy != nullptr) {
        // Write captured output to serial log (if any)
        const uint8_t* output_data = payload + sizeof(TaskCompletePayload);
        uint16_t out_len = comp->output_len;
        if (out_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
            auto avail = static_cast<uint16_t>(payload_len - sizeof(TaskCompletePayload));
            if (out_len > avail) out_len = avail;
            ker::mod::dbg::log("[WKI] Task %u remote output (%u bytes):", comp->task_id, out_len);
            // Write output data to proxy's stdout FD if it has one
            if (proxy->fds[1] != nullptr) {
                auto* f = static_cast<ker::vfs::File*>(proxy->fds[1]);
                if (f->fops != nullptr && f->fops->vfs_write != nullptr) {
                    f->fops->vfs_write(f, output_data, out_len, 0);
                }
            }
        }

        // Set exit status on the proxy task
        proxy->exitStatus = comp->exit_status;
        proxy->hasExited = true;
        proxy->wki_proxy_task_id = 0;

        // Wake up any tasks waiting via waitpid() on this proxy
        for (uint64_t i = 0; i < proxy->awaitee_on_exit_count; ++i) {
            uint64_t waiting_pid = proxy->awaitee_on_exit[i];
            auto* waiting_task = ker::mod::sched::find_task_by_pid_safe(waiting_pid);
            if (waiting_task != nullptr) {
                // Set the return value (RAX = proxy's PID) and write exit status
                if (!waiting_task->deferredTaskSwitch) {
                    waiting_task->context.regs.rax = proxy->pid;
                    if (waiting_task->waitStatusPhysAddr != 0) {
                        auto* status_ptr = reinterpret_cast<int32_t*>(ker::mod::mm::addr::getVirtPointer(waiting_task->waitStatusPhysAddr));
                        *status_ptr = comp->exit_status;
                    }
                }
                uint64_t cpu = ker::mod::sched::get_least_loaded_cpu();
                ker::mod::sched::reschedule_task_for_cpu(cpu, waiting_task);
                waiting_task->release();
            }
        }
        proxy->awaitee_on_exit_count = 0;

        // Transition proxy to DEAD for GC
        proxy->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
        proxy->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
        proxy->schedQueue = ker::mod::sched::task::Task::SchedQueue::DEAD_GC;
        ker::mod::sched::insert_into_dead_list(proxy);

        ker::mod::dbg::log("[WKI] Proxy task pid=0x%lx completed: exit=%d (remote task_id=%u)", proxy->pid, comp->exit_status,
                           comp->task_id);
    } else {
        // No proxy task (legacy V1 path) — just log output
        if (comp->output_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
            ker::mod::dbg::log("[WKI] Task %u output (%u bytes)", comp->task_id, comp->output_len);
        }
    }

    s_compute_lock.lock();
    task->complete_pending.store(false, std::memory_order_release);
    WkiWaitEntry* complete_waiter = task->complete_wait_entry;
    task->active = false;
    s_compute_lock.unlock();

    if (complete_waiter != nullptr) {
        wki_wake_op(complete_waiter, 0);
    }
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
    TaskOutputCapture* output = rt->output;
    rt->output = nullptr;
    rt->active = false;
    s_compute_lock.unlock();

    auto* task = ker::mod::sched::find_task_by_pid(local_pid);
    if (task != nullptr && !task->hasExited) {
        // Best-effort force-kill: transition to EXITING, set exit status, then DEAD.
        // The scheduler will skip DEAD tasks and GC will reclaim resources.
        if (task->transitionState(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
            task->exitStatus = -9;
            task->hasExited = true;
            task->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
            task->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);

            ker::mod::dbg::log("[WKI] Task cancelled: task_id=%u pid=0x%lx", cancel->task_id, local_pid);
        }
    }

    // Send TASK_COMPLETE with exit_status=-9 (killed)
    // The completion monitor will also detect hasExited, but sending here is faster
    uint16_t out_len = (output != nullptr) ? output->len : static_cast<uint16_t>(0);
    auto msg_len = static_cast<uint16_t>(sizeof(TaskCompletePayload) + out_len);
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
    if (buf != nullptr) {
        auto* complete = reinterpret_cast<TaskCompletePayload*>(buf);
        complete->task_id = cancel->task_id;
        complete->exit_status = -9;
        complete->output_len = out_len;

        if (out_len > 0 && output != nullptr) {
            memcpy(buf + sizeof(TaskCompletePayload), output->data.data(), out_len);
        }

        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_COMPLETE, buf, msg_len);
        ker::mod::mm::dyn::kmalloc::free(buf);
    }

    delete output;
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
