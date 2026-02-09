#include "remote_compute.hpp"

#include <extern/elf.h>

#include <algorithm>
#include <cstring>
#include <deque>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
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

// ═══════════════════════════════════════════════════════════════════════════════
// Storage
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

std::deque<SubmittedTask> g_submitted_tasks;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteNodeLoad> g_remote_loads;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RunningRemoteTask> g_running_remote_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_task_id = 1;                           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_compute_initialized = false;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_load_report_us = 0;                    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_submitted_task(uint32_t task_id) -> SubmittedTask* {
    for (auto& t : g_submitted_tasks) {
        if (t.active && t.task_id == task_id) {
            return &t;
        }
    }
    return nullptr;
}

auto find_remote_load(uint16_t node_id) -> RemoteNodeLoad* {
    for (auto& rl : g_remote_loads) {
        if (rl.valid && rl.node_id == node_id) {
            return &rl;
        }
    }
    return nullptr;
}

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
    .vfs_open = nullptr,  .vfs_close = capture_close,   .vfs_read = nullptr,    .vfs_write = capture_write,
    .vfs_lseek = nullptr, .vfs_isatty = capture_isatty, .vfs_readdir = nullptr, .vfs_readlink = nullptr,
};

// -----------------------------------------------------------------------------
// D17: Scheduler auto-placement hook.
// Called from postTaskBalanced() when WKI is active and task is a PROCESS.
// Returns true if the task was submitted to a remote node (local task won't run).
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
            auto stats = ker::mod::sched::getRunQueueStats(c);
            total_runnable += static_cast<uint16_t>(stats.activeTaskCount);
        }
        auto pct = static_cast<uint16_t>((static_cast<uint32_t>(total_runnable) * 1000U) / cpu_count);
        local_load = std::min<uint16_t>(pct, 1000);
    }

    // Only attempt remote if a significantly less loaded node exists
    uint16_t best_node = wki_least_loaded_node(local_load);
    if (best_node == WKI_NODE_INVALID) {
        return false;
    }

    // Submit the ELF binary to the remote node
    uint32_t tid = wki_task_submit_inline(best_node, task->elfBuffer, static_cast<uint32_t>(task->elfBufferSize), nullptr, 0);
    if (tid == 0) {
        return false;  // Remote submission failed, fall through to local
    }

    // Task was submitted remotely.
    // The local Task object must survive until the caller finishes with it
    // (e.g., exec.cpp accesses task->pid after postTaskBalanced returns).
    // Transition it to DEAD so it never runs locally, and insert into the
    // scheduler's dead list so epoch-based GC can reclaim it.
    // TODO: Implement a proxy task that stays alive until the remote task
    // completes, enabling transparent waitpid() support (Phase 8).
    delete[] task->elfBuffer;
    task->elfBuffer = nullptr;
    task->elfBufferSize = 0;
    task->exitStatus = 0;
    task->hasExited = true;
    task->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
    task->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
    ker::mod::sched::insertIntoDeadList(task);

    ker::mod::dbg::log("[WKI] D17: Task '%s' remotely placed on node 0x%04x (task_id=%u)", task->name, best_node, tid);
    return true;
}

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Init
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_compute_init() {
    if (g_remote_compute_initialized) {
        return;
    }
    g_remote_compute_initialized = true;

    // D17: Register the remote placement hook with the scheduler
    ker::mod::sched::wki_try_remote_placement_fn = try_remote_placement;

    ker::mod::dbg::log("[WKI] Remote compute subsystem initialized");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Submitter Side — Task Submit (INLINE only in V1)
// ═══════════════════════════════════════════════════════════════════════════════

auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len, const void* args, uint16_t args_len)
    -> uint32_t {
    if (binary == nullptr || binary_len == 0) {
        return 0;
    }

    // Check total size fits in WKI message
    // TaskSubmitPayload(8) + binary_len(4) + binary + args
    auto total = static_cast<uint32_t>(sizeof(TaskSubmitPayload) + sizeof(uint32_t) + binary_len + args_len);
    if (total > WKI_ETH_MAX_PAYLOAD) {
        ker::mod::dbg::log("[WKI] Task binary too large for inline submit: %u bytes", binary_len);
        return 0;
    }

    uint32_t task_id = g_next_task_id++;

    // Create submitted task entry
    SubmittedTask st;
    st.active = true;
    st.task_id = task_id;
    st.target_node = target_node;
    st.response_pending = true;
    st.accept_status = 0;
    st.remote_pid = 0;
    st.complete_pending = false;
    st.exit_status = 0;

    g_submitted_tasks.push_back(st);

    // Build TASK_SUBMIT message
    auto msg_len = static_cast<uint16_t>(total);
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
    if (buf == nullptr) {
        g_submitted_tasks.pop_back();
        return 0;
    }

    auto* submit = reinterpret_cast<TaskSubmitPayload*>(buf);
    submit->task_id = task_id;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::INLINE);
    submit->reserved = 0;
    submit->args_len = args_len;

    // INLINE format: {binary_len:u32, binary[binary_len], args[args_len]}
    uint8_t* cursor = buf + sizeof(TaskSubmitPayload);
    memcpy(cursor, &binary_len, sizeof(uint32_t));
    cursor += sizeof(uint32_t);
    memcpy(cursor, binary, binary_len);
    cursor += binary_len;
    if (args != nullptr && args_len > 0) {
        memcpy(cursor, args, args_len);
    }

    int send_ret = wki_send(target_node, WKI_CHAN_RESOURCE, MsgType::TASK_SUBMIT, buf, msg_len);
    ker::mod::mm::dyn::kmalloc::free(buf);

    if (send_ret != WKI_OK) {
        g_submitted_tasks.pop_back();
        return 0;
    }

    // Spin-wait for accept/reject
    uint64_t deadline = wki_now_us() + WKI_TASK_SUBMIT_TIMEOUT_US;
    SubmittedTask* task_ptr = &g_submitted_tasks.back();

    while (task_ptr->response_pending) {
        asm volatile("mfence" ::: "memory");
        if (!task_ptr->response_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            task_ptr->response_pending = false;
            task_ptr->active = false;
            ker::mod::dbg::log("[WKI] Task submit timeout: task_id=%u target=0x%04x", task_id, target_node);
            return 0;
        }
        for (int i = 0; i < 1000; i++) {
            asm volatile("pause" ::: "memory");
        }
    }

    if (task_ptr->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        ker::mod::dbg::log("[WKI] Task rejected: task_id=%u status=%u", task_id, task_ptr->accept_status);
        task_ptr->active = false;
        return 0;
    }

    ker::mod::dbg::log("[WKI] Task accepted: task_id=%u remote_pid=%lu", task_id, task_ptr->remote_pid);
    return task_id;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Submitter Side — Wait for Completion
// ═══════════════════════════════════════════════════════════════════════════════

auto wki_task_wait(uint32_t task_id, int32_t* exit_status, uint64_t timeout_us) -> int {
    SubmittedTask* task = find_submitted_task(task_id);
    if (task == nullptr) {
        return -1;
    }

    task->complete_pending = true;

    uint64_t deadline = wki_now_us() + timeout_us;
    while (task->complete_pending) {
        asm volatile("mfence" ::: "memory");
        if (!task->complete_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            task->complete_pending = false;
            return -1;
        }
        for (int i = 0; i < 1000; i++) {
            asm volatile("pause" ::: "memory");
        }
    }

    if (exit_status != nullptr) {
        *exit_status = task->exit_status;
    }

    // Clean up
    task->active = false;
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Submitter Side — Cancel
// ═══════════════════════════════════════════════════════════════════════════════

void wki_task_cancel(uint32_t task_id) {
    SubmittedTask* task = find_submitted_task(task_id);
    if (task == nullptr) {
        return;
    }

    TaskCancelPayload cancel = {};
    cancel.task_id = task_id;

    wki_send(task->target_node, WKI_CHAN_RESOURCE, MsgType::TASK_CANCEL, &cancel, sizeof(cancel));

    task->active = false;
    task->complete_pending = false;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Load Reporting
// ═══════════════════════════════════════════════════════════════════════════════

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
        auto stats = ker::mod::sched::getRunQueueStats(c);
        auto cpu_load = static_cast<uint16_t>(stats.activeTaskCount + stats.waitQueueCount);
        per_cpu[c] = cpu_load;
        total_runnable += static_cast<uint16_t>(stats.activeTaskCount);
    }

    report.runnable_tasks = total_runnable;
    // avg_load_pct: scale 0-1000. Approximation: (total_runnable / num_cpus) * 1000, capped at 1000.
    if (report_cpus > 0 && total_runnable > 0) {
        uint32_t pct = (static_cast<uint32_t>(total_runnable) * 1000U) / report_cpus;
        report.avg_load_pct = static_cast<uint16_t>(std::min(pct, 1000U));
    } else {
        report.avg_load_pct = 0;
    }
    report.free_mem_pages = 0;  // TODO: buddy allocator free page count when API available

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

// ═══════════════════════════════════════════════════════════════════════════════
// Fencing Cleanup
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_compute_cleanup_for_peer(uint16_t node_id) {
    // Fail any submitted tasks targeting this peer
    for (auto& t : g_submitted_tasks) {
        if (!t.active || t.target_node != node_id) {
            continue;
        }

        if (t.response_pending) {
            t.accept_status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            t.response_pending = false;
        }
        if (t.complete_pending) {
            t.exit_status = -1;
            t.complete_pending = false;
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
}

// ═══════════════════════════════════════════════════════════════════════════════
// Receiver Side — Completion Monitoring
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_compute_check_completions() {
    if (!g_remote_compute_initialized) {
        return;
    }

    for (auto& rt : g_running_remote_tasks) {
        if (!rt.active) {
            continue;
        }

        int32_t exit_status = -1;
        bool completed = false;

        auto* task = ker::mod::sched::findTaskByPid(rt.local_pid);
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

        // D19: Build TASK_COMPLETE with captured output
        uint16_t out_len = (rt.output != nullptr) ? rt.output->len : static_cast<uint16_t>(0);
        auto msg_len = static_cast<uint16_t>(sizeof(TaskCompletePayload) + out_len);
        auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
        if (buf != nullptr) {
            auto* complete = reinterpret_cast<TaskCompletePayload*>(buf);
            complete->task_id = rt.task_id;
            complete->exit_status = exit_status;
            complete->output_len = out_len;

            if (out_len > 0 && rt.output != nullptr) {
                memcpy(buf + sizeof(TaskCompletePayload), rt.output->data.data(), out_len);
            }

            wki_send(rt.submitter_node, WKI_CHAN_RESOURCE, MsgType::TASK_COMPLETE, buf, msg_len);
            ker::mod::mm::dyn::kmalloc::free(buf);
        }

        ker::mod::dbg::log("[WKI] Remote task completed: task_id=%u pid=0x%lx exit=%d output=%u bytes", rt.task_id, rt.local_pid,
                           exit_status, out_len);

        delete rt.output;
        rt.output = nullptr;
        rt.active = false;
    }

    // Clean up inactive entries
    std::erase_if(g_running_remote_tasks, [](const RunningRemoteTask& rt) { return !rt.active; });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Internal Helpers — ELF execution + VFS loading
// ═══════════════════════════════════════════════════════════════════════════════

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

    // Post to scheduler
    if (!ker::mod::sched::postTaskBalanced(new_task)) {
        delete new_task;
        delete[] elf_buffer;
        delete output_cap;
        result.reject_reason = TaskRejectReason::OVERLOADED;
        return result;
    }

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

// ═══════════════════════════════════════════════════════════════════════════════
// Receiver Side — RX Handlers
// ═══════════════════════════════════════════════════════════════════════════════

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

    // Execute the ELF buffer (shared helper handles validation, task creation, scheduling)
    ExecResult exec = exec_elf_buffer(elf_buffer, binary_len);
    if (exec.task == nullptr) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(exec.reject_reason);
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
    g_running_remote_tasks.push_back(rt);

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

    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || !task->response_pending) {
        return;
    }

    task->accept_status = resp->status;
    task->remote_pid = resp->remote_pid;

    asm volatile("" ::: "memory");
    task->response_pending = false;
}

void handle_task_reject(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || !task->response_pending) {
        return;
    }

    task->accept_status = resp->status;
    task->remote_pid = 0;

    asm volatile("" ::: "memory");
    task->response_pending = false;
}

void handle_task_complete(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskCompletePayload)) {
        return;
    }

    const auto* comp = reinterpret_cast<const TaskCompletePayload*>(payload);

    SubmittedTask* task = find_submitted_task(comp->task_id);
    if (task == nullptr) {
        return;
    }

    task->exit_status = comp->exit_status;

    // D19: Log output if present (output data follows the TaskCompletePayload)
    if (comp->output_len > 0 && payload_len > sizeof(TaskCompletePayload)) {
        ker::mod::dbg::log("[WKI] Task %u output (%u bytes)", comp->task_id, comp->output_len);
    }

    asm volatile("" ::: "memory");
    task->complete_pending = false;
}

void handle_task_cancel(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(TaskCancelPayload)) {
        return;
    }

    const auto* cancel = reinterpret_cast<const TaskCancelPayload*>(payload);

    // D18: Find the running task and force-kill it
    RunningRemoteTask* rt = find_running_task(cancel->task_id, hdr->src_node);
    if (rt == nullptr) {
        ker::mod::dbg::log("[WKI] Task cancel: no matching running task task_id=%u from 0x%04x", cancel->task_id, hdr->src_node);
        return;
    }

    auto* task = ker::mod::sched::findTaskByPid(rt->local_pid);
    if (task != nullptr && !task->hasExited) {
        // Best-effort force-kill: transition to EXITING, set exit status, then DEAD.
        // The scheduler will skip DEAD tasks and GC will reclaim resources.
        if (task->transitionState(ker::mod::sched::task::TaskState::ACTIVE, ker::mod::sched::task::TaskState::EXITING)) {
            task->exitStatus = -9;
            task->hasExited = true;
            task->deathEpoch.store(ker::mod::sched::EpochManager::currentEpoch(), std::memory_order_release);
            task->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);

            ker::mod::dbg::log("[WKI] Task cancelled: task_id=%u pid=0x%lx", cancel->task_id, rt->local_pid);
        }
    }

    // Send TASK_COMPLETE with exit_status=-9 (killed)
    // The completion monitor will also detect hasExited, but sending here is faster
    uint16_t out_len = (rt->output != nullptr) ? rt->output->len : static_cast<uint16_t>(0);
    auto msg_len = static_cast<uint16_t>(sizeof(TaskCompletePayload) + out_len);
    auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(msg_len));
    if (buf != nullptr) {
        auto* complete = reinterpret_cast<TaskCompletePayload*>(buf);
        complete->task_id = cancel->task_id;
        complete->exit_status = -9;
        complete->output_len = out_len;

        if (out_len > 0 && rt->output != nullptr) {
            memcpy(buf + sizeof(TaskCompletePayload), rt->output->data.data(), out_len);
        }

        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::TASK_COMPLETE, buf, msg_len);
        ker::mod::mm::dyn::kmalloc::free(buf);
    }

    delete rt->output;
    rt->output = nullptr;
    rt->active = false;
}

void handle_load_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(LoadReportPayload)) {
        return;
    }

    const auto* report = reinterpret_cast<const LoadReportPayload*>(payload);

    // Find or create cache entry
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
}

}  // namespace detail

}  // namespace ker::net::wki
