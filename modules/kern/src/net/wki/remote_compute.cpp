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
#include <memory>
#include <net/wki/auth.hpp>
#include <net/wki/auth_policy.hpp>
#include <net/wki/chaos_workload.hpp>
#include <net/wki/child_query.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/resource_contract_model.hpp>
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
#include <platform/random/entropy.hpp>
#include <platform/sched/compartment_lifecycle.hpp>
#include <platform/sched/epoch.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/authorization.hpp>
#include <platform/security/compartment_wki.hpp>
#include <platform/security/credential_record.hpp>
#include <platform/security/exec_policy.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/process/process.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <type_traits>
#include <util/errno_name.hpp>
#include <util/hcf.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/content_match.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/filesystem_context.hpp>
#ifdef WOS_SELFTEST
#include <vfs/lookup.hpp>
#endif
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/mm/paging.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

#ifdef WOS_SELFTEST
namespace {
std::atomic<unsigned> s_task_submit_buffer_frees{0};
std::atomic<unsigned> s_output_capture_frees{0};
}  // namespace
#endif

// Wire storage remains owned by the executor even after IPC ownership moves
// into SubmittedTask. Reliable transport/fragment queues retain their own copies.
class TaskSubmitBuffer {
   public:
    TaskSubmitBuffer(ker::mod::sched::task::Task* source, size_t size, bool fail_allocation = false) {
        if (size == 0) return;
        executor = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : source;
        if (executor == nullptr) executor = source;
        if (executor != nullptr) {
            next = executor->pending_wki_submit_buffers;
            if (next != nullptr) next->previous = this;
            executor->pending_wki_submit_buffers = this;
        }
        bytes = fail_allocation ? nullptr : new (std::nothrow) uint8_t[size];
        if (bytes == nullptr) cancel();
    }
    ~TaskSubmitBuffer() { cancel(); }
    TaskSubmitBuffer(const TaskSubmitBuffer&) = delete;
    auto operator=(const TaskSubmitBuffer&) -> TaskSubmitBuffer& = delete;
    auto data() const -> uint8_t* { return bytes; }
    void cancel() {
        auto* owned = bytes;
        bytes = nullptr;
        if (owned != nullptr) {
            delete[] owned;
#ifdef WOS_SELFTEST
            s_task_submit_buffer_frees.fetch_add(1, std::memory_order_relaxed);
#endif
        }
        if (executor == nullptr) return;
        if (previous != nullptr)
            previous->next = next;
        else
            executor->pending_wki_submit_buffers = next;
        if (next != nullptr) next->previous = previous;
        executor = nullptr;
        previous = nullptr;
        next = nullptr;
    }

   private:
    uint8_t* bytes = nullptr;
    ker::mod::sched::task::Task* executor = nullptr;
    TaskSubmitBuffer* previous = nullptr;
    TaskSubmitBuffer* next = nullptr;
};

namespace {
// A grant transaction is never the reusable 32-bit SubmittedTask ID.
auto take_filesystem_submission_id(std::atomic<uint64_t>& next) -> uint64_t {
    auto value = next.load(std::memory_order_relaxed);
    while (value != 0) {
        auto const FOLLOWING = value == UINT64_MAX ? 0 : value + 1;
        if (next.compare_exchange_weak(value, FOLLOWING, std::memory_order_relaxed)) return value;
    }
    return 0;
}
std::atomic<uint64_t> g_next_filesystem_submission{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

// The executor owns the captured source and any owner-issued transfer Files
// until acceptance/failure. Keep remaining entries linked while one close waits.
class TaskFilesystemPreparation {
   public:
    using Task = ker::mod::sched::task::Task;
    TaskFilesystemPreparation(Task* source, const char* cwd, const Task::FilesystemContext* supplied,
                              const ker::mod::security::Snapshot* credentials = nullptr) {
        if (source == nullptr) {
            status_ = -ESRCH;
            return;
        }
        executor_ = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : source;
        if (executor_ == nullptr) executor_ = source;
        next_ = executor_->pending_wki_filesystems;
        if (next_ != nullptr) next_->previous_ = this;
        executor_->pending_wki_filesystems = this;
        owns_ = true;
        credentials_ = credentials != nullptr ? *credentials : source->credentials();
        if (supplied != nullptr)
            filesystem_ = *supplied;
        else if (!source->capture_filesystem_context(filesystem_)) {
            status_ = -ESRCH;
            return;
        }
        auto const* FILESYSTEM_OWNER = source->filesystem_owner();
        bool const CAPTURED_CWD = supplied != nullptr || (FILESYSTEM_OWNER != nullptr && cwd == FILESYSTEM_OWNER->cwd.data());
        const char* const SELECTED = CAPTURED_CWD ? filesystem_.cwd.data() : cwd;
        has_cwd_ = SELECTED != nullptr;
        if (SELECTED != nullptr) {
            size_t const LENGTH = std::strlen(SELECTED);
            if (LENGTH >= UINT16_MAX) {
                status_ = -E2BIG;
                return;
            }
            if (LENGTH >= cwd_.size()) {
                heap_cwd_.reset(new (std::nothrow) char[LENGTH + 1]);
                if (!heap_cwd_) {
                    status_ = -ENOMEM;
                    return;
                }
            }
            char* const DEST = heap_cwd_ ? heap_cwd_.get() : cwd_.data();
            std::memcpy(DEST, SELECTED, LENGTH + 1);
            if (supplied == nullptr && std::strcmp(DEST, filesystem_.cwd.data()) != 0) {
                // An explicit alternate cwd is text until separately resolved.
                // Never pair it with the source's unrelated retained object.
                filesystem_.cwd_anchor = {};
                filesystem_.cwd.fill('\0');
                filesystem_.cwd_len = 0;
                if (LENGTH < filesystem_.cwd.size()) {
                    std::memcpy(filesystem_.cwd.data(), DEST, LENGTH + 1);
                    filesystem_.cwd_len = static_cast<uint16_t>(LENGTH);
                }
            }
        }
        transaction_ = take_filesystem_submission_id(g_next_filesystem_submission);
        if (transaction_ == 0) status_ = -EOVERFLOW;
    }
    ~TaskFilesystemPreparation() { cancel(); }
    TaskFilesystemPreparation(const TaskFilesystemPreparation&) = delete;
    auto operator=(const TaskFilesystemPreparation&) -> TaskFilesystemPreparation& = delete;
    auto status() const -> int { return status_; }
    auto transaction() const -> uint64_t { return transaction_; }
    auto filesystem() const -> const Task::FilesystemContext& { return filesystem_; }
    auto credentials() const -> const ker::mod::security::Snapshot& { return credentials_; }
    auto cwd() const -> const char* { return owns_ && has_cwd_ ? (heap_cwd_ ? heap_cwd_.get() : cwd_.data()) : nullptr; }
    auto bind_target(uint16_t node, uint32_t boot) -> int {
        if (status_ != 0) return status_;
        if (node == WKI_NODE_INVALID || node == WKI_NODE_BROADCAST || boot == 0) return -EINVAL;
        if (target_ != WKI_NODE_INVALID) return -EBUSY;
        target_ = node;
        target_boot_ = boot;
        return 0;
    }
    // This checks local ownership/transaction consistency, not peer authority.
    // Wire admission and owner mapping must validate the grant independently.
    auto adopt_staging(size_t index, ker::vfs::File*& file, const VfsDirectoryGrant& grant) -> int {
        if (status_ != 0) return status_;
        if (index >= staging_.size() || file == nullptr || staging_.at(index) != nullptr || !wki_vfs_directory_grant_valid(grant) ||
            grant.submission_id != transaction_ || target_ == WKI_NODE_INVALID || grant.target_node != target_ ||
            grant.target_boot_epoch != target_boot_)
            return -EINVAL;
        if (std::find(staging_.begin(), staging_.end(), file) != staging_.end()) return -EINVAL;
        wire_ready_ = false;
        grants_.at(index) = grant;
        staging_.at(index) = std::exchange(file, nullptr);
        return 0;
    }
    auto finalize_wire(uint16_t flags) -> int {
        if (status_ != 0) return status_;
        TaskFilesystemContext candidate{.flags = flags, .submission_id = transaction_};
        if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0) candidate.root = grants_[0];
        if ((flags & WKI_TASK_FILESYSTEM_CWD_GRANT) != 0) candidate.cwd = grants_[1];
        if (!wki_task_filesystem_valid(candidate) || (((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0) != (staging_[0] != nullptr)) ||
            (((flags & WKI_TASK_FILESYSTEM_CWD_GRANT) != 0) != (staging_[1] != nullptr)))
            return -EINVAL;
        if ((flags & WKI_TASK_FILESYSTEM_ROOT_PATH) != 0) {
            if (filesystem_.root_anchor || !wki_task_legacy_root_path_valid(filesystem_.root, false)) return -ENOTSUP;
        } else if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) == 0 &&
                   (filesystem_.root_anchor || std::strcmp(filesystem_.root.data(), "/") != 0))
            return -ENOTSUP;
        if ((flags & WKI_TASK_FILESYSTEM_CWD_GRANT) == 0 && cwd() != nullptr && cwd()[0] != '\0' && std::strcmp(cwd(), "/") != 0)
            return -EINVAL;
        wire_ = candidate;
        wire_ready_ = true;
        return 0;
    }
    auto wire_context() const -> const TaskFilesystemContext* { return owns_ && wire_ready_ ? &wire_ : nullptr; }
    void hold_received_filesystem(Task::FilesystemContext&& filesystem) { received_ = std::move(filesystem); }
    auto received_filesystem() -> Task::FilesystemContext& { return received_; }
    auto received_view() -> ker::vfs::DirectoryViewRef& { return received_view_; }
    auto received_root_view() -> ker::vfs::DirectoryViewRef& { return received_root_view_; }
    auto received_prepared() -> ker::vfs::PreparedFilesystemContext& { return received_prepared_; }
    auto received_filesystem() const -> const Task::FilesystemContext& { return received_; }
    auto staged(size_t index) const -> ker::vfs::File* { return index < staging_.size() ? staging_.at(index) : nullptr; }
    void cancel() {
        if (!owns_) return;
        status_ = -ECANCELED;
        wire_ready_ = false;
        for (size_t index = 0; owns_ && index < staging_.size(); ++index) {
            auto* file = std::exchange(staging_.at(index), nullptr);
            grants_.at(index) = {};
            if (file != nullptr) ker::vfs::vfs_put_file(file);
        }
        // Assignment detaches each anchor before final release can wait. A
        // reentrant exit sees only the remaining anchors and staging Files.
        filesystem_.cwd_anchor = {};
        filesystem_.root_anchor = {};
        received_.cwd_anchor = {};
        received_.root_anchor = {};
        received_view_ = {};
        received_prepared_ = {};
        received_root_view_ = {};
        heap_cwd_.reset();
        if (!owns_) return;
        if (previous_ != nullptr)
            previous_->next_ = next_;
        else
            executor_->pending_wki_filesystems = next_;
        if (next_ != nullptr) next_->previous_ = previous_;
        executor_ = nullptr;
        previous_ = nullptr;
        next_ = nullptr;
        owns_ = false;
    }

   private:
    Task::FilesystemContext filesystem_;
    Task::FilesystemContext received_;
    ker::vfs::DirectoryViewRef received_view_;
    ker::vfs::DirectoryViewRef received_root_view_;
    ker::vfs::PreparedFilesystemContext received_prepared_;
    ker::mod::security::Snapshot credentials_;
    Task::PathBuffer cwd_{};
    std::unique_ptr<char[]> heap_cwd_;
    std::array<ker::vfs::File*, 2> staging_{};
    std::array<VfsDirectoryGrant, 2> grants_{};
    TaskFilesystemContext wire_{};
    bool wire_ready_ = false;
    Task* executor_ = nullptr;
    TaskFilesystemPreparation* previous_ = nullptr;
    TaskFilesystemPreparation* next_ = nullptr;
    uint64_t transaction_ = 0;
    uint16_t target_ = WKI_NODE_INVALID;
    uint32_t target_boot_ = 0;
    int status_ = 0;
    bool owns_ = false;
    bool has_cwd_ = false;
};

// ===============================================================================
// Storage
// ===============================================================================

namespace {

constexpr uint32_t WKI_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_LATENCY_DAEMON_NICE = -5;
constexpr uint16_t WKI_LOAD_REPORT_MAX_CPUS = 64;
constexpr size_t WKI_COMPUTE_SUBMIT_WORKER_MAX = 8;
constexpr size_t WKI_COMPUTE_SUBMIT_QUEUE_MAX = 64;
constexpr size_t WKI_COMPUTE_SUBMIT_CANCEL_MAX = WKI_COMPUTE_SUBMIT_QUEUE_MAX * 2;
constexpr size_t WKI_COMPUTE_RX_WORKER_MAX = 4;
// One full resource-channel receive window per worker. A shared pool lets a
// busy compile peer borrow unused capacity from idle shards while remaining
// bounded across four-node bursts.
constexpr size_t WKI_COMPUTE_RX_QUEUE_MAX = WKI_CREDITS_RESOURCE * WKI_COMPUTE_RX_WORKER_MAX;
constexpr size_t WKI_COMPUTE_RX_PAYLOAD_MAX = std::max(sizeof(TaskCompletePayload) + WKI_TASK_MAX_OUTPUT, WKI_ETH_MAX_PAYLOAD);
constexpr size_t WKI_COMPUTE_SUBMIT_FRAGMENT_MAX = WKI_COMPUTE_SUBMIT_QUEUE_MAX;
constexpr size_t WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX = WKI_ETH_MAX_PAYLOAD - sizeof(TaskSubmitFragmentPayload);
static_assert(WKI_COMPUTE_RX_QUEUE_MAX <= UINT16_MAX);
static_assert(WKI_COMPUTE_RX_PAYLOAD_MAX <= WKI_ETH_MAX_PAYLOAD);
static_assert(WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX > sizeof(TaskSubmitPayload));
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

    ~PeerLifecycleLease() { release(); }
    void release() { wki_peer_lifecycle_release(std::exchange(claimed_peer, nullptr)); }

    auto acquire(uint16_t node_id) -> bool { return acquire(wki_peer_find(node_id)); }

    // This noncopyable handle retains the exclusive claim until release(),
    // including boot/selftest context before scheduler task identity exists.
    auto owns(uint16_t node_id) const -> bool { return claimed_peer != nullptr && claimed_peer->node_id == node_id; }

    auto acquire(WkiPeer* peer) -> bool {
        if (claimed_peer != nullptr) {
            return false;
        }
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
    SubmittedTaskSlot* storage_next = nullptr;
    bool occupied = false;
};

// Append-only slot storage preserves addresses/order across reclamation. The
// free list reuses slots; growing the high-water mark must be a checked allocation.
SubmittedTaskSlot* s_submitted_task_head = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
SubmittedTaskSlot* s_submitted_task_tail = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_submitted_task_storage_count = 0;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<SubmittedTaskSlot*, WKI_SUBMITTED_TASK_INDEX_BUCKETS>
    s_submitted_task_index{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
SubmittedTaskSlot* s_submitted_task_free = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_submitted_task_count = 0;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::deque<RemoteNodeLoad> g_remote_loads;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
using RunningRemoteTasks = ker::util::SmallVec<RunningRemoteTask, 16>;
RunningRemoteTasks g_running_remote_tasks;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_task_accept_cursor = 0;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
struct PendingTaskCompletion {
    PendingTaskCompletion* next = nullptr;
    std::optional<TaskResourceSnapshot> resources;
    uint32_t task_id = 0;
    uint16_t submitter_node = WKI_NODE_INVALID;
    uint64_t local_pid = 0;
    int32_t exit_status = -1;
    TaskOutputCapture* output = nullptr;
    uint64_t submit_session_epoch = 0;
    WkiChannel* submit_rx_channel = nullptr;
    uint32_t submit_rx_channel_generation = 0;
};
// Detached send attempts own their node until success, retirement, or requeue.
PendingTaskCompletion* s_pending_completion_head = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
PendingTaskCompletion* s_pending_completion_tail = nullptr;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_completion_count = 0;                       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_next_task_id = 1;                                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_compute_initialized = false;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_last_load_report_us = 0;                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

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

auto is_placement_control_helper(const char* name) -> bool {
    if (name == nullptr || name[0] == '\0') {
        return false;
    }

    const char* base = name;
    for (const char* cursor = name; *cursor != '\0'; ++cursor) {
        if (*cursor == '/') {
            base = cursor + 1;
        }
    }

    constexpr std::array<const char*, 8> CONTROL_TOOLS = {
        "wkictl", "locally", "remotely", "anywhere", "homeward", "on", "forward", "wosid",
    };
    return std::ranges::any_of(CONTROL_TOOLS, [base](const char* tool) { return std::strcmp(base, tool) == 0; });
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
    g_last_local_cpu_accounting{};                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_last_local_cpu_accounting_valid{};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint16_t g_cached_local_load_pct{};               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_cached_local_load_update_us{};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint32_t g_balanced_local_reservations{};         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_balanced_local_sample_us{};            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_balanced_local_last_assignment_seq{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_balanced_assignment_seq{};             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint16_t g_balanced_placement_cursor{};           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_compute_lock;           // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Captured while the caller holds authenticated peer lifecycle admission.
// Queue storage carries values only, never peer pointers or cryptographic keys.
struct ComputePeerIdentity {
    uint32_t boot_epoch = 0;
    uint32_t auth_generation = 0;
    std::array<uint8_t, WKI_AUTH_SESSION_ID_SIZE> auth_session_id{};
    uint64_t policy = 0;
    uint16_t capabilities = 0;
    auto operator==(const ComputePeerIdentity&) const -> bool = default;
    [[nodiscard]] auto valid() const -> bool { return boot_epoch != 0 && auth_generation != 0; }
};

struct ComputeSubmitSessionToken {
    uint16_t node_id = WKI_NODE_INVALID;
    WkiChannel* rx_channel = nullptr;
    uint32_t rx_channel_generation = 0;
    uint64_t epoch = 0;
    ComputePeerIdentity identity{};
};

enum class RemoteComputeCleanupScope : uint8_t {
    ALL,
    RETIRED,
};

constexpr size_t CHILD_WAIT_LANES = 64;
constexpr size_t CHILD_WAIT_TRANSACTIONS = 128;

struct ComputeSubmitPeerSession {
    bool used = false;
    bool open = false;
    bool retiring = false;
    uint16_t node_id = WKI_NODE_INVALID;
    ComputeSubmitSessionToken current = {};
    WkiChannel* retired_channel = nullptr;
    uint32_t retired_channel_generation = 0;
    // A lane may be reused only with a larger sequence. Keep this compact
    // tombstone after ACK frees the expensive independent claim storage.
    std::array<uint64_t, CHILD_WAIT_LANES> child_wait_completed{};
    std::array<uint64_t, CHILD_WAIT_LANES> child_query_completed{};
};

std::array<ComputeSubmitPeerSession, WKI_MAX_PEERS>
    g_compute_submit_peer_sessions{};              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_next_compute_submit_session_epoch = 1;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_next_shared_elf_cache_load_token = 1;   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

namespace child_events = ker::syscall::process::child_events;

enum class ChildWaitCommand : uint8_t { BEGIN, COMMIT, RELEASE, ACK };
enum class ChildWaitPhase : uint8_t { PENDING, CLAIMED, DONE };

// Private continuation values, never a wire overlay. The future decoder must
// validate a versioned envelope; no Task/Event/channel pointer may be serialized.
struct ChildWaitRequest {
    uint64_t sequence = 0;
    child_events::QueueVersion queue_version{};
    uint32_t task_id = 0;
    uint64_t receiver_pid = 0;
    child_events::Selector selector{};
    int32_t options = 0;
    uint8_t lane = 0;
    auto operator==(const ChildWaitRequest&) const -> bool = default;
};
struct ChildWaitValue {
    uint64_t pid = 0;
    uint64_t process_group = 0;
    ker::mod::sched::session::Origin group_origin{};
    ker::mod::sched::task::ChildEventKind kind = ker::mod::sched::task::ChildEventKind::EXIT;
    ker::mod::sched::task::ChildEventAudience audience = ker::mod::sched::task::ChildEventAudience::PARENT;
    uint64_t user_time_us = 0;
    uint64_t system_time_us = 0;
    int32_t status = 0;
    auto operator==(const ChildWaitValue&) const -> bool = default;
};
struct ChildWaitReply {
    ChildWaitPhase phase = ChildWaitPhase::PENDING;
    child_events::ProbeResult probe = child_events::ProbeResult::WOULD_BLOCK;
    int error = 0;
    bool committed = false;
    ChildWaitValue value{};
};
struct ChildWaitTransaction {
    bool active = false;
    bool busy = false;
    bool retiring = false;
    ComputeSubmitSessionToken session{};
    ChildWaitRequest request{};
    ker::mod::sched::task::Task* owner = nullptr;
    child_events::Claim claim{};
    child_events::ClaimedEvent claimed{};
    ChildWaitReply reply{};
};
std::array<ChildWaitTransaction, CHILD_WAIT_TRANSACTIONS>
    s_child_wait_transactions{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_child_wait_cleanup_pending{false};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void retire_child_queries_for_session_locked(const ComputeSubmitSessionToken& session);
void retire_child_queries_for_task(ker::mod::sched::task::Task& task);
void retire_child_wait_transaction_locked(ChildWaitTransaction& transaction);
void retire_child_wait_transactions_for_task(ker::mod::sched::task::Task& task);

struct SubmittedIpcCleanup {
    WkiIpcChannels channels{};
    std::array<WkiIpcFdEntry, 16> map = {};
    uint16_t count = 0;
    uint16_t target_node = WKI_NODE_INVALID;
};

struct SubmittedIpcHandoffCleanup {
    std::array<WkiIpcTaskFdHandoff, 16> handoff = {};
    uint16_t count = 0;
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

// Transfer the exact File identity pins into durable submitted-task storage
// before publishing any stack-backed waiter. The caller's array is cleared as
// each pin moves so every path has one mechanically identifiable owner.
void remember_submitted_ipc_handoff(SubmittedTask& submitted, WkiIpcTaskFdHandoff* ipc_fd_handoff, uint16_t ipc_fd_count) {
    submitted.handoff_pin_count = 0;
    if (ipc_fd_handoff == nullptr || ipc_fd_count == 0) {
        return;
    }

    uint16_t const COPY_COUNT = std::min<uint16_t>(ipc_fd_count, static_cast<uint16_t>(submitted.ipc_fd_handoff.size()));
    for (uint16_t i = 0; i < COPY_COUNT; ++i) {
        submitted.ipc_fd_handoff.at(i) = ipc_fd_handoff[i];
        ipc_fd_handoff[i] = {};
    }
    submitted.handoff_pin_count = COPY_COUNT;
}

// Failed submission returns durable ownership to the executor before releasing
// the compute lock or consuming the row. This also covers blocking cancellation
// sends after a timeout. Peer cleanup may already have emptied both counts.
void restore_submitted_ipc_preparation_locked(SubmittedTask& submitted, WkiIpcTaskFdHandoff* handoff, uint16_t& count,
                                              WkiIpcSubmissionGuard& guard) {
    count = std::max(submitted.ipc_fd_count, submitted.handoff_pin_count);
    for (uint16_t i = 0; handoff != nullptr && i < submitted.handoff_pin_count; ++i) {
        handoff[i] = submitted.ipc_fd_handoff[i];
        submitted.ipc_fd_handoff[i] = {};
    }
    submitted.ipc_fd_count = 0;
    submitted.handoff_pin_count = 0;
    guard.arm();
}

// s_compute_lock must be held by the caller. File releases happen only after
// dropping it; clearing the row here transfers exact ownership to the snapshot.
auto take_submitted_ipc_handoff_locked(SubmittedTask* submitted) -> SubmittedIpcHandoffCleanup {
    SubmittedIpcHandoffCleanup cleanup = {};
    if (submitted == nullptr || submitted->handoff_pin_count == 0) {
        return cleanup;
    }

    cleanup.count = std::min<uint16_t>(submitted->handoff_pin_count, static_cast<uint16_t>(cleanup.handoff.size()));
    for (uint16_t i = 0; i < cleanup.count; ++i) {
        cleanup.handoff.at(i) = submitted->ipc_fd_handoff.at(i);
        submitted->ipc_fd_handoff.at(i) = {};
    }
    submitted->handoff_pin_count = 0;
    return cleanup;
}

void release_submitted_ipc_handoff(SubmittedIpcHandoffCleanup& cleanup) {
    wki_ipc_release_task_fd_handoff(cleanup.handoff.data(), cleanup.count);
    cleanup.count = 0;
}

auto submitted_ipc_cleanup_snapshot_locked(const SubmittedTask* submitted) -> SubmittedIpcCleanup {
    SubmittedIpcCleanup cleanup = {};
    if (submitted == nullptr || submitted->ipc_fd_count == 0) {
        return cleanup;
    }

    cleanup.channels = submitted->ipc_channels;
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
    wki_ipc_cleanup_exported_fds(cleanup.map.data(), cleanup.count, cleanup.target_node, cleanup.channels);
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

struct FragmentedTaskSubmit {
    ComputeSubmitSessionToken session{};
    bool active = false;
    uint16_t src_node = WKI_NODE_INVALID;
    WkiChannel* rx_channel = nullptr;
    uint32_t rx_channel_generation = 0;
    uint32_t task_id = 0;
    uint16_t total_len = 0;
    uint16_t received_len = 0;
    uint8_t* payload = nullptr;
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
std::array<FragmentedTaskSubmit, WKI_COMPUTE_SUBMIT_FRAGMENT_MAX>
    s_fragmented_task_submits{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_fragmented_task_submit_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
void release_fragmented_task_submits(uint16_t node_id, WkiChannel* channel = nullptr, uint32_t generation = 0);
std::array<ker::mod::sched::task::Task*, WKI_COMPUTE_SUBMIT_WORKER_MAX>
    s_compute_submit_tasks{};                        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<size_t> s_compute_submit_task_count{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct PendingUserSignal {
    ker::mod::sched::task::Task* owner = nullptr;
    WkiWaitEntry* response_wait = nullptr;
    WkiWaitEntry* consumer_wait = nullptr;
    WkiChannel* channel = nullptr;
    uint64_t request_id = 0;
    uint32_t task_id = 0;
    uint32_t generation = 0;
    uint16_t node = WKI_NODE_INVALID;
    int result = -EIO;
    bool group_operation = false;
    bool group_query = false;
    bool group_own_allowed = false;
    uint32_t source_boot = 0;
    ker::mod::sched::session::GroupCandidates groups{};
    bool route_operation = false;
    bool receiver_operation = false;
    GroupRouteRequest route_request{};
    GroupRouteResponse route_response{};
    GroupReceiverResponse receiver_response{};
};
std::array<PendingUserSignal, 64> s_user_signals{};
uint64_t s_next_user_signal = 0;

struct PendingComputeRx {
    bool child_reply_pending = false;
    ComputePeerIdentity identity{};
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

struct ProxyExitQueue {
    ker::mod::sched::task::Task* head = nullptr;
    ker::mod::sched::task::Task* tail = nullptr;

    // Caller owns the Task and queue serialization; no allocation after the
    // completion winner has moved the proxy to EXITING.
    auto push(ker::mod::sched::task::Task& task) -> bool {
        if (!task.try_acquire_lifetime_ref()) return false;
        task.wki_proxy_exit_next = nullptr;
        if (tail != nullptr)
            tail->wki_proxy_exit_next = &task;
        else
            head = &task;
        tail = &task;
        return true;
    }
    auto pop() -> ker::mod::sched::task::Task* {
        auto* task = head;
        if (task == nullptr) return nullptr;
        head = task->wki_proxy_exit_next;
        if (head == nullptr) tail = nullptr;
        task->wki_proxy_exit_next = nullptr;
        return task;
    }
};
ProxyExitQueue s_proxy_exit_queue;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_proxy_exit_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void notify_proxy_exit_work() {
    size_t const COUNT = s_compute_rx_task_count.load(std::memory_order_acquire);
    for (size_t i = 0; i < COUNT; ++i) ker::mod::sched::wake_task_from_event(s_compute_rx_tasks.at(i));
}

void enqueue_proxy_exit(ker::mod::sched::task::Task& task) {
    uint64_t const FLAGS = s_proxy_exit_lock.lock_irqsave();
    bool const RETAINED = s_proxy_exit_queue.push(task);
    s_proxy_exit_lock.unlock_irqrestore(FLAGS);
    if (!RETAINED) {
        ker::mod::dbg::panic_handler("WKI proxy retirement lost its retained Task");
        hcf();
    }
    notify_proxy_exit_work();
}

// Standard signals coalesce. One queued owner pin per Task bounds memory by
// the process population; popped work retains its pin independently of requeue.
struct ChildSignalQueue {
    using Task = ker::mod::sched::task::Task;
    Task* head = nullptr;
    Task* tail = nullptr;

    auto push(Task& task, uint16_t hops) -> bool {
        if (task.exit_in_progress || task.has_exited ||
            task.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE)
            return false;
        if (task.wki_child_signal_queued) {
            task.wki_child_signal_hops = std::max(task.wki_child_signal_hops, hops);
            return true;
        }
        if (!task.try_acquire_lifetime_ref()) return false;
        task.wki_child_signal_queued = true;
        task.wki_child_signal_hops = hops;
        task.wki_child_signal_next = nullptr;
        if (tail != nullptr)
            tail->wki_child_signal_next = &task;
        else
            head = &task;
        tail = &task;
        return true;
    }
    auto remove(Task& task) -> bool {
        Task* previous = nullptr;
        for (auto* cursor = head; cursor != nullptr; cursor = cursor->wki_child_signal_next) {
            if (cursor != &task) {
                previous = cursor;
                continue;
            }
            if (previous != nullptr)
                previous->wki_child_signal_next = task.wki_child_signal_next;
            else
                head = task.wki_child_signal_next;
            if (tail == &task) tail = previous;
            task.wki_child_signal_next = nullptr;
            task.wki_child_signal_queued = false;
            task.wki_child_signal_hops = 0;
            return true;  // transfers the queue's reference to the caller
        }
        return false;
    }
    auto pop(uint16_t& hops) -> Task* {
        auto* task = head;
        if (task != nullptr) {
            hops = task->wki_child_signal_hops;
            static_cast<void>(remove(*task));
        }
        return task;
    }
};
ChildSignalQueue s_child_signal_queue;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_child_signal_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void retire_child_signal_for_task(ker::mod::sched::task::Task& task) {
    uint64_t const FLAGS = s_child_signal_lock.lock_irqsave();
    bool const REMOVED = s_child_signal_queue.remove(task);
    s_child_signal_lock.unlock_irqrestore(FLAGS);
    if (REMOVED) task.release();
}

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
    return task != nullptr ? static_cast<uint32_t>(task->load_cpu()) : 0U;
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

auto task_rejection_errno(uint8_t status) -> int {
    switch (static_cast<TaskRejectReason>(status)) {
        case TaskRejectReason::ACCEPTED:
            return 0;
        case TaskRejectReason::OVERLOADED:
            return -EAGAIN;
        case TaskRejectReason::NO_MEM:
            return -ENOMEM;
        case TaskRejectReason::BINARY_NOT_FOUND:
            return -ENOENT;
        case TaskRejectReason::FETCH_FAILED:
            return -EIO;
        case TaskRejectReason::UNAUTHORIZED:
            return -EACCES;
        case TaskRejectReason::RESOURCE_NPROC:
            return -EAGAIN;
        case TaskRejectReason::RESOURCE_UNSUPPORTED:
            return -ENOTSUP;
        case TaskRejectReason::RESOURCE_INVALID:
            return -EINVAL;
        case TaskRejectReason::RESOURCE_CPU:
            return -EDQUOT;
    }
    return -EPROTO;
}

auto task_transport_errno(int result) -> int {
    switch (result) {
        case WKI_OK:
            return 0;
        case WKI_ERR_NO_MEM:
            return -ENOMEM;
        case WKI_ERR_NO_ROUTE:
            return -EHOSTUNREACH;
        case WKI_ERR_PEER_FENCED:
            return -EHOSTDOWN;
        case WKI_ERR_NO_CREDITS:
            return -EAGAIN;
        case WKI_ERR_TIMEOUT:
            return -ETIMEDOUT;
        case WKI_ERR_INVALID:
            return -EINVAL;
        case WKI_ERR_NOT_FOUND:
            return -ENOENT;
        case WKI_ERR_BUSY:
            return -EBUSY;
        case WKI_ERR_TX_FAILED:
            return -EIO;
        default:
            return -EIO;
    }
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

auto capture_exec_submit_channel(uint16_t node, SubmitterChannelToken& output) -> int {
    // Bind the receiver's loader contract to the channel generation actually
    // used for SUBMIT, including submissions with no inherited IPC descriptors.
    // A peer restart after this capture makes generation-bound sends fail.
    PeerLifecycleLease lifecycle;
    if (!lifecycle.acquire(node)) return -EAGAIN;
    if (wki_resource_generation_snapshot(node, ResourceType::COMPUTE, WKI_EXEC_HANDOFF_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(node, ResourceType::COMPUTE, WKI_PARENT_DEATH_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(node, ResourceType::COMPUTE, WKI_CHILD_SIGNAL_RESOURCE_ID) == 0)
        return -ENOTSUP;
    auto const CHANNEL = capture_submitter_channel(node);
    if (CHANNEL.channel == nullptr) return -EHOSTUNREACH;
    output = CHANNEL;
    return 0;
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

constexpr std::array<const char*, 7> WKI_RECEIVER_LOCAL_SYSTEM_PREFIXES = {
    "/usr", "/bin", "/sbin", "/lib", "/lib64", "/libexec", "/share",
};

auto task_vfs_rule_prefix_matches(const char* path, const char* prefix, size_t prefix_len) -> bool {
    if (path == nullptr || prefix == nullptr || prefix_len == 0 || std::strncmp(path, prefix, prefix_len) != 0) {
        return false;
    }
    if (prefix_len == 1 && prefix[0] == '/') {
        return true;
    }
    return path[prefix_len] == '\0' || path[prefix_len] == '/';
}

auto explicit_task_vfs_route_for_path(const ker::mod::sched::task::Task* task, const char* path, uint8_t* route_out) -> bool {
    if (task == nullptr || path == nullptr || route_out == nullptr) {
        return false;
    }

    bool found = false;
    uint16_t best_prefix_len = 0;
    for (const auto& rule : task->wki_vfs_rules) {
        if (rule.prefix_len == 0 || rule.prefix_len >= rule.prefix.size() ||
            !task_vfs_rule_prefix_matches(path, rule.prefix.data(), rule.prefix_len) || (found && rule.prefix_len <= best_prefix_len)) {
            continue;
        }
        found = true;
        best_prefix_len = rule.prefix_len;
        *route_out = rule.route;
    }
    return found;
}

auto receiver_local_system_path(const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }
    return std::ranges::any_of(WKI_RECEIVER_LOCAL_SYSTEM_PREFIXES,
                               [path](const char* prefix) { return task_vfs_rule_prefix_matches(path, prefix, std::strlen(prefix)); });
}

auto task_uses_receiver_local_path(const ker::mod::sched::task::Task* task, const char* path) -> bool {
    uint8_t explicit_route = 0;
    if (explicit_task_vfs_route_for_path(task, path, &explicit_route)) {
        return explicit_route == static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
    }
    if (receiver_local_system_path(path)) {
        return true;
    }
    return task_uses_local_vfs_route_for_path(task, path);
}

auto install_receiver_local_system_rules(ker::mod::sched::task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    for (const char* const PREFIX : WKI_RECEIVER_LOCAL_SYSTEM_PREFIXES) {
        size_t const PREFIX_LEN = std::strlen(PREFIX);
        uint8_t explicit_route = 0;
        if (explicit_task_vfs_route_for_path(task, PREFIX, &explicit_route)) {
            continue;
        }

        ker::mod::sched::task::WkiVfsRule rule{};
        std::memcpy(rule.prefix.data(), PREFIX, PREFIX_LEN);
        *std::next(rule.prefix.begin(), static_cast<ptrdiff_t>(PREFIX_LEN)) = '\0';
        rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
        rule.route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
        if (!task->wki_vfs_rules.push_back(rule)) {
            return false;
        }
    }
    return true;
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
auto publish_submitted_task_locked(SubmittedTask&& task, int* error_out = nullptr, bool fail_allocation = false) -> SubmittedTask* {
    if (error_out != nullptr) *error_out = 0;
    if (task.task_id == 0 || find_submitted_task_slot_locked(task.task_id) != nullptr) {
        if (error_out != nullptr) *error_out = -EINVAL;
        return nullptr;
    }

    SubmittedTaskSlot* slot = s_submitted_task_free;
    if (slot != nullptr) {
        s_submitted_task_free = slot->free_next;
        slot->free_next = nullptr;
    } else {
        slot = fail_allocation ? nullptr : new (std::nothrow) SubmittedTaskSlot{};
        if (slot == nullptr) {
            if (error_out != nullptr) *error_out = -ENOMEM;
            return nullptr;
        }
        if (s_submitted_task_tail != nullptr)
            s_submitted_task_tail->storage_next = slot;
        else
            s_submitted_task_head = slot;
        s_submitted_task_tail = slot;
        ++s_submitted_task_storage_count;
    }
    slot->task = std::move(task);

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
           task.local_task == nullptr && task.pending_proxy_output == nullptr && task.handoff_pin_count == 0 &&
           !task.service_report_pending;
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
    SubmittedIpcHandoffCleanup handoff_cleanup = {};
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
        handoff_cleanup = take_submitted_ipc_handoff_locked(submitted);
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
    release_submitted_ipc_handoff(handoff_cleanup);
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
    size_t retained = 0;
    for (size_t i = 0; i < g_running_remote_tasks.size(); ++i) {
        if (!g_running_remote_tasks[i].active) continue;
        if (retained != i) g_running_remote_tasks[retained] = g_running_remote_tasks[i];
        ++retained;
    }
    while (g_running_remote_tasks.size() > retained) g_running_remote_tasks.pop_back();
}

// The caller still owns the child's monitor/output until this succeeds.
auto publish_running_task_locked(const RunningRemoteTask& task, bool fail_allocation = false) -> bool {
    return !fail_allocation && g_running_remote_tasks.push_back(task);
}

// All queue helpers require s_compute_lock. Requeue never allocates.
void append_pending_completion_locked(PendingTaskCompletion* completion) {
    completion->next = nullptr;
    if (s_pending_completion_tail != nullptr) {
        s_pending_completion_tail->next = completion;
    } else {
        s_pending_completion_head = completion;
    }
    s_pending_completion_tail = completion;
    ++s_pending_completion_count;
}

auto queue_pending_completion_locked(const PendingTaskCompletion& info, bool fail_allocation = false) -> bool {
    auto* completion = fail_allocation ? nullptr : new (std::nothrow) PendingTaskCompletion(info);
    if (completion == nullptr) return false;
    append_pending_completion_locked(completion);
    return true;
}

auto pop_pending_completion_locked() -> PendingTaskCompletion* {
    auto* completion = s_pending_completion_head;
    if (completion == nullptr) return nullptr;
    s_pending_completion_head = completion->next;
    if (s_pending_completion_head == nullptr) s_pending_completion_tail = nullptr;
    completion->next = nullptr;
    --s_pending_completion_count;
    return completion;
}

void compact_pending_task_completions_locked() {
    auto** link = &s_pending_completion_head;
    s_pending_completion_tail = nullptr;
    while (*link != nullptr) {
        auto* completion = *link;
        if (completion->submitter_node == WKI_NODE_INVALID) {
            *link = completion->next;
            delete completion;
            --s_pending_completion_count;
        } else {
            s_pending_completion_tail = completion;
            link = &completion->next;
        }
    }
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

// Running/completion cleanup reconstructs this sealed local session key without
// an admission snapshot. New effects and queued submits validate the value too.
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
    retire_child_queries_for_session_locked(session);
    for (auto& transaction : s_child_wait_transactions) {
        if (transaction.active && compute_submit_session_token_matches(transaction.session, session))
            retire_child_wait_transaction_locked(transaction);
    }
    for (auto& task : g_running_remote_tasks) {
        if (task.active && running_task_session_matches(task, session)) {
            task.discard_completion = true;
        }
    }
}

// Callers retain authenticated peer lifecycle admission through capture. No
// peer/compute/channel lock is acquired here: fixed RX already holds channel.
auto compute_peer_identity(const WkiPeer& peer, uint16_t local_capabilities) -> ComputePeerIdentity {
    if (peer.state != PeerState::CONNECTED || !peer.auth_session.active || peer.remote_boot_epoch == 0 || peer.auth_session.generation == 0)
        return {};
    return {.boot_epoch = peer.remote_boot_epoch,
            .auth_generation = peer.auth_session.generation,
            .auth_session_id = peer.auth_session.session_id,
            .policy = peer.auth_session.policy,
            .capabilities = static_cast<uint16_t>(peer.capabilities & local_capabilities)};
}

auto capture_compute_peer_identity(uint16_t node_id) -> ComputePeerIdentity {
    auto const* peer = wki_peer_find(node_id);
    return peer != nullptr && peer->node_id == node_id ? compute_peer_identity(*peer, g_wki.capabilities) : ComputePeerIdentity{};
}

auto compute_rx_identity_matches(const PendingComputeRx& work, const WkiPeer& peer, uint16_t local_capabilities) -> bool {
    return work.identity.valid() && peer.node_id == work.hdr.src_node && work.identity == compute_peer_identity(peer, local_capabilities);
}

auto capture_compute_submit_session_locked(uint16_t node_id, WkiChannel* channel, uint32_t generation, const ComputePeerIdentity& identity)
    -> ComputeSubmitSessionToken {
    if (!identity.valid() || !compute_submit_channel_token_matches(node_id, channel, generation)) return {};

    auto* peer_session = get_or_create_compute_submit_peer_session_locked(node_id);
    if (peer_session == nullptr) {
        return {};
    }
    if (peer_session->retiring) {
        return {};
    }
    if (peer_session->open && peer_session->current.rx_channel == channel && peer_session->current.rx_channel_generation == generation) {
        // Same channel identity cannot silently adopt a different boot/auth
        // session. Normal retirement/channel rotation establishes a successor.
        return peer_session->current.identity == identity ? peer_session->current : ComputeSubmitSessionToken{};
    }
    if (!peer_session->open && peer_session->retired_channel == channel && peer_session->retired_channel_generation == generation) {
        return {};
    }
    if (peer_session->open) {
        retire_running_compute_submit_session_locked(peer_session->current);
        peer_session->retired_channel = peer_session->current.rx_channel;
        peer_session->retired_channel_generation = peer_session->current.rx_channel_generation;
    }

    peer_session->child_wait_completed.fill(0);
    peer_session->child_query_completed.fill(0);
    peer_session->current = {
        .node_id = node_id,
        .rx_channel = channel,
        .rx_channel_generation = generation,
        .epoch = next_nonzero_token(s_next_compute_submit_session_epoch),
        .identity = identity,
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

auto compute_submit_admission_is_current_locked(const ComputeSubmitSessionToken& session) -> bool {
    auto* peer_session = find_compute_submit_peer_session_locked(session.node_id);
    return session.identity.valid() && peer_session != nullptr && peer_session->current.identity == session.identity &&
           compute_submit_session_is_current_locked(session);
}

enum class ChildAuthorityAdmission : uint8_t {
    LOCAL,
    READY,
    AWAITING_ACCEPT,
    UNAVAILABLE,
};

// Local continuation state, never a wire layout. Session epochs and channel
// pointers belong to this kernel; peers exchange only task_id/receiver_pid.
struct ChildAuthorityRoute {
    ComputeSubmitSessionToken session{};
    uint32_t task_id{};
    uint64_t receiver_pid{};
};

// s_compute_lock held; the caller retains the canonical process Task. These
// primitives are connected only once the asynchronous wait engine owns cleanup.
[[maybe_unused]] auto capture_child_authority_route_locked(const ker::mod::sched::task::Task& receiver, ChildAuthorityRoute& route)
    -> ChildAuthorityAdmission {
    route = {};
    auto const STATE = receiver.state.load(std::memory_order_acquire);
    if (receiver.is_thread || receiver.exit_in_progress || receiver.has_exited || STATE == ker::mod::sched::task::TaskState::EXITING ||
        STATE == ker::mod::sched::task::TaskState::DEAD)
        return ChildAuthorityAdmission::UNAVAILABLE;
    for (auto const& running : g_running_remote_tasks) {
        if (running.task != &receiver) continue;
        if (!running.active) return ChildAuthorityAdmission::UNAVAILABLE;
        // A remotely spawned child starts its own family. Only exec carries
        // this logical parent's older queues through an upstream execution.
        if (!running.exec_handoff) return ChildAuthorityAdmission::LOCAL;
        ComputeSubmitSessionToken const SESSION{.node_id = running.submitter_node,
                                                .rx_channel = running.submit_rx_channel,
                                                .rx_channel_generation = running.submit_rx_channel_generation,
                                                .epoch = running.submit_session_epoch};
        if (running.discard_completion || running.root_completion_queued || running.termination_requested || running.task_id == 0 ||
            running.local_pid != receiver.pid || !compute_submit_session_is_current_locked(SESSION))
            return ChildAuthorityAdmission::UNAVAILABLE;
        if (!running.published || !running.exec_committed) return ChildAuthorityAdmission::AWAITING_ACCEPT;
        // ACCEPT may still be queued or in flight. The source resolves that
        // window; it must not turn a valid upstream family into local ECHILD.
        route = {.session = find_compute_submit_peer_session_locked(SESSION.node_id)->current,
                 .task_id = running.task_id,
                 .receiver_pid = running.local_pid};
        return ChildAuthorityAdmission::READY;
    }
    return ChildAuthorityAdmission::LOCAL;
}

// s_compute_lock and authenticated peer lifecycle admission held. A successful
// result transfers one Task ref to the continuation, never a borrowed row pointer.
auto acquire_child_authority_owner_locked(const ComputeSubmitSessionToken& session, uint32_t task_id, uint64_t receiver_pid,
                                          ker::mod::sched::task::Task*& owner) -> ChildAuthorityAdmission {
    owner = nullptr;
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if (task_id == 0 || receiver_pid == 0 || (session.identity.policy & REQUIRED) != REQUIRED ||
        (session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) == 0 || !compute_submit_admission_is_current_locked(session))
        return ChildAuthorityAdmission::UNAVAILABLE;
    auto* submitted = find_submitted_task(task_id);
    if (submitted == nullptr || !submitted->exec_handoff || submitted->target_node != session.node_id ||
        submitted->submit_channel != session.rx_channel || submitted->submit_channel_generation != session.rx_channel_generation ||
        submitted->local_task == nullptr || submitted->local_task->is_thread || submitted->local_task->exit_in_progress ||
        submitted->local_task->has_exited || !submitted->exec_prepared || submitted->exec_remote_pid != receiver_pid)
        return ChildAuthorityAdmission::UNAVAILABLE;
    if (!submitted->exec_cutover || submitted->response_pending.load(std::memory_order_acquire))
        return ChildAuthorityAdmission::AWAITING_ACCEPT;
    auto const EXECUTION = submitted->local_task->wki_execution_identity();
    if (submitted->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED) || EXECUTION.signal_task_id != task_id ||
        EXECUTION.remote_pid != receiver_pid || !submitted->local_task->try_acquire())
        return ChildAuthorityAdmission::UNAVAILABLE;
    owner = submitted->local_task;
    return ChildAuthorityAdmission::READY;
}

// No record pointer escapes these helpers. A slot number plus non-reused token
// permits late send completions without retaining the caller's stack or Task.
struct ChildQueryRequest {
    uint64_t sequence{};
    uint32_t task_id{};
    uint64_t receiver_pid{};
    child_query::Spec spec{};
    int32_t options{};
    uint16_t remaining_hops{};
    uint8_t lane{};
    auto operator==(const ChildQueryRequest&) const -> bool = default;
};
struct ChildQueryReply {
    bool ready{};
    child_query::Outcome outcome{};
    child_events::QueueVersion version{};
};
auto child_query_wire_origin(child_query::Origin origin) -> TaskSessionOrigin { return {.node = origin.node, .boot = origin.boot}; }
auto child_query_local_origin(const TaskSessionOrigin& origin) -> child_query::Origin { return {origin.node, origin.boot}; }
auto valid_child_query_origin(const TaskSessionOrigin& origin) -> bool {
    return origin.reserved == 0 && origin.node != WKI_NODE_BROADCAST && ((origin.node == 0) == (origin.boot == 0));
}

auto decode_child_query_request(const TaskChildQueryRequest& wire, ChildQueryRequest& request) -> bool {
    if (wire.version != WKI_CHILD_QUERY_VERSION || wire.reserved != 0 || wire.sequence == 0 || wire.task_id == 0 ||
        wire.receiver_pid == 0 || wire.generation == 0 || wire.generation > 4 || wire.lane >= CHILD_WAIT_LANES ||
        wire.operation > static_cast<uint8_t>(child_query::Operation::CLOSE) || wire.hops > child_query::MAX_HOPS ||
        (wire.flags & ~(WKI_CHILD_QUERY_ALIAS | WKI_CHILD_QUERY_OWN_GROUP)) != 0 || !valid_child_query_origin(wire.selector_origin) ||
        !valid_child_query_origin(wire.caller_origin) || !valid_child_query_origin(wire.own_group) ||
        !valid_child_query_origin(wire.selected_group))
        return false;
    constexpr int32_t OPTIONS = child_events::OPTION_NOHANG | child_events::OPTION_STOPPED | child_events::OPTION_EXITED |
                                child_events::OPTION_CONTINUED | child_events::OPTION_NOWAIT;
    bool const OWN = (wire.flags & WKI_CHILD_QUERY_OWN_GROUP) != 0;
    bool const GROUP = wire.selector != child_events::SELECT_ANY && (wire.selector & child_events::SELECT_PROCESS_GROUP) != 0;
    bool const PROBE = wire.operation == static_cast<uint8_t>(child_query::Operation::PROBE);
    auto const SELECTED = child_query_local_origin(wire.selected_group);
    auto const ORIGIN = child_query_local_origin(wire.own_group);
    if ((wire.options & ~OPTIONS) != 0 || (OWN ? !child_query::physical(ORIGIN) : !ORIGIN.local()) ||
        (PROBE ? !child_query::physical(SELECTED) : !SELECTED.local()))
        return false;
    request = {.sequence = wire.sequence,
               .task_id = wire.task_id,
               .receiver_pid = wire.receiver_pid,
               .spec = {.selector = {wire.selector, child_query_local_origin(wire.selector_origin)},
                        .group_alias = (wire.flags & WKI_CHILD_QUERY_ALIAS) != 0,
                        .caller_origin = child_query_local_origin(wire.caller_origin),
                        .own_group = OWN ? std::optional{ORIGIN} : std::nullopt},
               .options = wire.options,
               .remaining_hops = wire.hops,
               .lane = wire.lane};
    return child_query::valid_spec(request.spec) && (!PROBE || request.spec.group_alias) &&
           (!GROUP || request.spec.group_alias || child_query::physical(request.spec.selector.group_origin));
}

auto encode_child_query_request(const ChildQueryRequest& request, const child_query::Action& action) -> TaskChildQueryRequest {
    return {.sequence = request.sequence,
            .receiver_pid = request.receiver_pid,
            .selector = request.spec.selector.value,
            .task_id = request.task_id,
            .options = request.options,
            .hops = request.remaining_hops,
            .lane = request.lane,
            .operation = static_cast<uint8_t>(action.operation),
            .generation = action.generation,
            .flags = static_cast<uint8_t>((request.spec.group_alias ? WKI_CHILD_QUERY_ALIAS : 0) |
                                          (request.spec.own_group ? WKI_CHILD_QUERY_OWN_GROUP : 0)),
            .selector_origin = child_query_wire_origin(request.spec.selector.group_origin),
            .caller_origin = child_query_wire_origin(request.spec.caller_origin),
            .own_group = child_query_wire_origin(request.spec.own_group.value_or(child_query::Origin{})),
            .selected_group =
                child_query_wire_origin(action.operation == child_query::Operation::PROBE ? action.group : child_query::Origin{})};
}

auto encode_child_query_response(const TaskChildQueryRequest& request, const ChildQueryReply& reply) -> TaskChildQueryResponse {
    TaskChildQueryResponse wire{
        .request = request, .error = reply.ready ? reply.outcome.error : 0, .ready = static_cast<uint8_t>(reply.ready)};
    if (!reply.ready || reply.outcome.error != 0 || request.operation == static_cast<uint8_t>(child_query::Operation::CLOSE)) return wire;
    auto const& summary = reply.outcome.summary;
    wire.potential = static_cast<uint8_t>(summary.potential);
    wire.pid_flags = static_cast<uint8_t>((summary.pids.found ? 1 : 0) | (summary.pids.ambiguous ? 2 : 0));
    wire.group_flags = summary.groups.flags;
    wire.ready_depth = summary.ready_depth;
    wire.pid_origin = child_query_wire_origin(summary.pids.found ? summary.pids.origin : child_query::Origin{});
    wire.other_group = child_query_wire_origin(
        (summary.groups.flags & ker::mod::sched::session::GroupCandidates::OTHER) != 0 ? summary.groups.other : child_query::Origin{});
    wire.queue_version = reply.version.queue;
    wire.group_version = reply.version.groups;
    return wire;
}

auto decode_child_query_response(const TaskChildQueryResponse& wire, ChildQueryRequest& request, ChildQueryReply& reply) -> bool {
    using Groups = ker::mod::sched::session::GroupCandidates;
    if (!decode_child_query_request(wire.request, request) || wire.error > 0 || wire.error < -4095 || wire.ready > 1 ||
        wire.potential > 1 || wire.reserved != 0 || (wire.pid_flags & ~3U) != 0 || (wire.group_flags & ~15U) != 0 ||
        !valid_child_query_origin(wire.pid_origin) || !valid_child_query_origin(wire.other_group))
        return false;
    reply = {.ready = wire.ready != 0,
             .outcome = {.error = wire.error,
                         .summary = {.pids = {.found = (wire.pid_flags & 1) != 0,
                                              .ambiguous = (wire.pid_flags & 2) != 0,
                                              .origin = child_query_local_origin(wire.pid_origin)},
                                     .groups = {.flags = wire.group_flags, .other = child_query_local_origin(wire.other_group)},
                                     .potential = wire.potential != 0,
                                     .ready_depth = wire.ready_depth}},
             .version = {wire.queue_version, wire.group_version}};
    auto const CANONICAL = encode_child_query_response(wire.request, reply);
    if (std::memcmp(&CANONICAL, &wire, sizeof(wire)) != 0) return false;
    if (!reply.ready || reply.outcome.error != 0 || wire.request.operation == static_cast<uint8_t>(child_query::Operation::CLOSE))
        return true;
    auto const& summary = reply.outcome.summary;
    bool const PID = request.spec.selector.value > 0 && request.spec.selector.value < child_events::SELECT_PROCESS_GROUP;
    bool const ALIAS = request.spec.group_alias && wire.request.operation == static_cast<uint8_t>(child_query::Operation::INSPECT);
    if (wire.queue_version == 0 || wire.group_version == 0 || wire.queue_version == UINT64_MAX || wire.group_version == UINT64_MAX ||
        (summary.ready_depth != child_query::NO_READY && (!summary.potential || summary.ready_depth > request.remaining_hops)) ||
        (summary.pids.ambiguous && !summary.pids.found) || (summary.pids.found && !child_query::physical(summary.pids.origin)) ||
        (PID ? summary.pids.found != summary.potential : summary.pids.found) ||
        ((summary.groups.flags & Groups::AMBIGUOUS) != 0 && (summary.groups.flags & Groups::OTHER) == 0) ||
        ((summary.groups.flags & Groups::OTHER) != 0 && !child_query::physical(summary.groups.other)) ||
        ((summary.groups.flags & Groups::OWN) != 0 && !request.spec.own_group))
        return false;
    return ALIAS ? summary.ready_depth == child_query::NO_READY && summary.potential == (summary.groups.flags != 0)
                 : summary.groups.flags == 0;
}

struct ChildClaimRequest {
    uint64_t sequence{};
    uint32_t task_id{};
    uint64_t receiver_pid{};
    uint16_t depth{};
    uint8_t lane{};
    auto operator==(const ChildClaimRequest&) const -> bool = default;
};
struct ChildClaimAction {
    uint64_t query{};
    ChildWaitCommand command{ChildWaitCommand::BEGIN};
    uint8_t generation{};
    auto operator==(const ChildClaimAction&) const -> bool = default;
};
auto encode_child_claim_request(const ChildClaimRequest& request, const ChildClaimAction& action) -> TaskChildClaimRequest {
    return {.sequence = request.sequence,
            .receiver_pid = request.receiver_pid,
            .task_id = request.task_id,
            .depth = request.depth,
            .lane = request.lane,
            .command = static_cast<uint8_t>(action.command),
            .generation = action.generation};
}
auto decode_child_claim_request(const TaskChildClaimRequest& wire, ChildClaimRequest& request) -> bool {
    if (wire.version != WKI_CHILD_CLAIM_VERSION || wire.reserved != 0 || wire.reserved2 != 0 || wire.sequence == 0 || wire.task_id == 0 ||
        wire.receiver_pid == 0 || wire.lane >= CHILD_WAIT_LANES || wire.depth > child_query::MAX_HOPS || wire.generation == 0 ||
        wire.generation > 3 || wire.command > static_cast<uint8_t>(ChildWaitCommand::RELEASE))
        return false;
    request = {
        .sequence = wire.sequence, .task_id = wire.task_id, .receiver_pid = wire.receiver_pid, .depth = wire.depth, .lane = wire.lane};
    return true;
}
auto encode_child_claim_response(const TaskChildClaimRequest& request, const ChildWaitReply& reply,
                                 child_query::Origin local_origin = {g_wki.my_node_id, g_wki.local_boot_epoch}) -> TaskChildClaimResponse {
    TaskChildClaimResponse wire{.request = request,
                                .phase = static_cast<uint8_t>(reply.phase),
                                .probe = static_cast<uint8_t>(reply.probe),
                                .committed = static_cast<uint8_t>(reply.committed),
                                .error = reply.error};
    if (reply.phase == ChildWaitPhase::PENDING || reply.value.pid == 0) return wire;
    auto const& value = reply.value;
    wire.pid = value.pid;
    wire.process_group = value.process_group;
    wire.user_time_us = value.user_time_us;
    wire.system_time_us = value.system_time_us;
    wire.group_origin = child_query_wire_origin(value.group_origin.local() ? local_origin : value.group_origin);
    wire.status = value.status;
    wire.kind = static_cast<uint8_t>(value.kind);
    wire.audience = static_cast<uint8_t>(value.audience);
    return wire;
}
auto decode_child_claim_response(const TaskChildClaimResponse& wire, ChildClaimRequest& request, ChildWaitReply& reply) -> bool {
    using Kind = ker::mod::sched::task::ChildEventKind;
    using Audience = ker::mod::sched::task::ChildEventAudience;
    if (!decode_child_claim_request(wire.request, request) || wire.phase > static_cast<uint8_t>(ChildWaitPhase::DONE) ||
        wire.probe > static_cast<uint8_t>(child_events::ProbeResult::NO_CHILD) || wire.committed > 1 || wire.reserved != 0 ||
        wire.reserved2 != 0 || wire.error > 0 || wire.error < -4095 || wire.kind > static_cast<uint8_t>(Kind::CONTINUED) ||
        wire.audience > static_cast<uint8_t>(Audience::TRACER) || !valid_child_query_origin(wire.group_origin))
        return false;
    reply = {.phase = static_cast<ChildWaitPhase>(wire.phase),
             .probe = static_cast<child_events::ProbeResult>(wire.probe),
             .error = wire.error,
             .committed = wire.committed != 0,
             .value = {.pid = wire.pid,
                       .process_group = wire.process_group,
                       .group_origin = child_query_local_origin(wire.group_origin),
                       .kind = static_cast<Kind>(wire.kind),
                       .audience = static_cast<Audience>(wire.audience),
                       .user_time_us = wire.user_time_us,
                       .system_time_us = wire.system_time_us,
                       .status = wire.status}};
    auto const CANONICAL = encode_child_claim_response(wire.request, reply, {});
    if (std::memcmp(&CANONICAL, &wire, sizeof(wire)) != 0) return false;
    if (reply.phase == ChildWaitPhase::PENDING)
        return !reply.committed && (reply.error == 0 || reply.error == -EAGAIN) && reply.probe == child_events::ProbeResult::WOULD_BLOCK;
    if (reply.value.pid != 0 && (reply.value.process_group == 0 || !child_query::physical(reply.value.group_origin) ||
                                 reply.probe != child_events::ProbeResult::CLAIMED))
        return false;
    if (reply.probe == child_events::ProbeResult::CLAIMED && reply.value.pid == 0) return false;
    if (reply.phase == ChildWaitPhase::CLAIMED) return !reply.committed && reply.error == 0 && reply.value.pid != 0;
    return reply.committed ? reply.error == 0 && reply.value.pid != 0 : reply.error != 0 || reply.value.pid == 0;
}

enum class NativeChildPhase : uint8_t { INSPECT, PROBE, VALIDATE, CLAIM, CLAIMED, COMMIT, DONE };

struct ChildQueryRecord {
    bool active{}, busy{}, retiring{}, upstream_terminal{};
    uint64_t token{};
    ComputeSubmitSessionToken session{};
    ChildQueryRequest request{};
    ChildAuthorityRoute upstream{};
    child_query::Origin local_origin{};
    ker::mod::sched::task::Task* owner{};
    std::optional<child_query::Relay> relay{};
    int begin_error{};
    std::optional<child_query::Action> last_action{};
    uint64_t next_send_us{};
    std::optional<uint16_t> claim_depth{};
    ChildWaitReply claim_reply{};
    std::optional<ChildWaitValue> claim_value{};
    std::optional<ChildClaimAction> claim_action{};
    uint8_t claim_generation{};
    bool claim_issued{};
    uint64_t claim_next_send_us{};
    ker::mod::sched::task::Task* native_waiter{};
    NativeChildPhase native_phase{NativeChildPhase::INSPECT};
    WkiChildWaitResult native_result{};
    bool native_detached{}, native_claim_retired{true};
    uint64_t native_deadline_us{};
    child_events::Claim native_claim{};
    child_events::ClaimedEvent native_event{};
};
struct ChildQueryWork {
    uint8_t slot{};
    child_query::Action action{};
    ChildAuthorityRoute route{};
    ChildQueryRequest request{};
};
struct ChildClaimWork {
    uint8_t slot{};
    ChildClaimAction action{};
    ChildAuthorityRoute route{};
    ChildClaimRequest request{};
};
constexpr size_t CHILD_QUERY_RECORDS = CHILD_WAIT_LANES;
std::array<ChildQueryRecord, CHILD_QUERY_RECORDS> s_child_queries{};
uint64_t s_next_child_query_token = 1;
size_t s_child_query_cursor = 0;
size_t s_child_claim_cursor = 0;
size_t s_native_child_cursor = 0;
constexpr uint64_t CHILD_QUERY_RETRY_US = 10000;
std::atomic<bool> s_child_query_cleanup_pending{false};

auto child_query_error(int error) -> ChildQueryReply { return {.ready = true, .outcome = {.error = error}}; }

auto child_query_session_allowed_locked(const ComputeSubmitSessionToken& session) -> bool {
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    return (session.identity.policy & REQUIRED) == REQUIRED && (session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) != 0 &&
           compute_submit_admission_is_current_locked(session);
}

auto child_query_claim_matches_locked(const ChildQueryRecord& record, const ChildWaitTransaction& transaction) -> bool {
    return transaction.active && compute_submit_session_token_matches(record.session, transaction.session) &&
           record.request.lane == transaction.request.lane && record.request.sequence == transaction.request.sequence;
}
auto child_query_claim_pending_locked(const ChildQueryRecord& record) -> bool {
    return std::ranges::any_of(s_child_wait_transactions,
                               [&](const auto& transaction) { return child_query_claim_matches_locked(record, transaction); });
}
void retire_child_query_locked(ChildQueryRecord& record) {
    if (auto* peer = find_compute_submit_peer_session_locked(record.session.node_id);
        peer != nullptr && compute_submit_session_token_matches(peer->current, record.session)) {
        auto& completed = peer->child_query_completed.at(record.request.lane);
        completed = std::max(completed, record.request.sequence);
        auto& claim_completed = peer->child_wait_completed.at(record.request.lane);
        claim_completed = std::max(claim_completed, record.request.sequence);
    }
    for (auto& transaction : s_child_wait_transactions)
        if (child_query_claim_matches_locked(record, transaction)) retire_child_wait_transaction_locked(transaction);
    record.retiring = true;
    s_child_query_cleanup_pending.store(true, std::memory_order_release);
}

void retire_child_queries_for_session_locked(const ComputeSubmitSessionToken& session) {
    for (auto& record : s_child_queries) {
        if (!record.active) continue;
        if (compute_submit_session_token_matches(record.session, session)) retire_child_query_locked(record);
        if (compute_submit_session_token_matches(record.upstream.session, session)) {
            record.upstream_terminal = true;
            s_child_query_cleanup_pending.store(true, std::memory_order_release);
        }
    }
}

void retire_child_queries_for_task(ker::mod::sched::task::Task& task) {
    s_compute_lock.lock();
    for (auto& record : s_child_queries) {
        if (!record.active) continue;
        if (record.owner == &task)
            retire_child_query_locked(record);
        else if (record.native_waiter == &task) {
            record.native_detached = true;
            if (!record.busy && record.native_phase != NativeChildPhase::COMMIT) retire_child_query_locked(record);
        }
    }
    s_compute_lock.unlock();
    notify_proxy_exit_work();
}

// Only call when busy is false or owned by this executor. Invalidation flags
// may be set during a lifecycle snapshot, but never concurrently mutate Relay.
void advance_child_query_cleanup_locked(ChildQueryRecord& record) {
    if (!record.relay) return;
    if (record.upstream_terminal) {
        record.relay->upstream_lost();
        if (record.claim_action) {
            record.claim_reply = {.phase = ChildWaitPhase::DONE, .error = -EIO};
            record.claim_action.reset();  // commit may have happened; EIO is not rollback
        }
        record.upstream_terminal = false;
    }
    if (record.retiring) {
        record.claim_action.reset();
        record.relay->close();
    }
}

auto child_query_reply_locked(const ChildQueryRecord& record, child_query::Operation operation) -> ChildQueryReply {
    if (record.begin_error != 0 && operation != child_query::Operation::CLOSE) return child_query_error(record.begin_error);
    if (operation == child_query::Operation::CLOSE && child_query_claim_pending_locked(record)) return {};
    auto const RESULT = record.relay->outcome(operation);
    return {.ready = RESULT.has_value(), .outcome = RESULT.value_or(child_query::Outcome{}), .version = record.relay->version()};
}

auto local_child_query_selector(const ChildQueryRecord& record, std::optional<child_query::Origin> group = std::nullopt)
    -> child_events::Selector {
    auto selector = record.request.spec.selector;
    if (group) selector.group_origin = *group;
    if (selector.group_origin == record.local_origin) selector.group_origin = {};
    return selector;
}

// Private adapter entry: caller holds exactly this incoming peer's lifecycle
// lease, no spinlocks. Future wire decoding must validate the envelope first.
[[maybe_unused]] auto execute_child_query(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                          const ChildQueryRequest& request, child_query::Operation operation,
                                          child_query::Origin selected_group = {}, bool require_upstream_support = true)
    -> ChildQueryReply {
    using Operation = child_query::Operation;
    if (!lifecycle.owns(session.node_id)) return child_query_error(-EACCES);
    constexpr int32_t OPTIONS = child_events::OPTION_NOHANG | child_events::OPTION_STOPPED | child_events::OPTION_EXITED |
                                child_events::OPTION_CONTINUED | child_events::OPTION_NOWAIT;
    bool const GROUP =
        request.spec.selector.value != child_events::SELECT_ANY && (request.spec.selector.value & child_events::SELECT_PROCESS_GROUP) != 0;
    if (!child_query::valid_spec(request.spec) || operation > Operation::CLOSE || request.sequence == 0 || request.task_id == 0 ||
        request.receiver_pid == 0 || request.lane >= CHILD_WAIT_LANES || (request.options & ~OPTIONS) != 0 ||
        request.remaining_hops > child_query::MAX_HOPS ||
        (GROUP && !request.spec.group_alias && !child_query::physical(request.spec.selector.group_origin)) ||
        (operation == Operation::PROBE ? !child_query::physical(selected_group) : !selected_group.local()))
        return child_query_error(-EINVAL);
    if (!child_query::physical({g_wki.my_node_id, g_wki.local_boot_epoch})) return child_query_error(-ENODEV);
    s_compute_lock.lock();
    if (!child_query_session_allowed_locked(session)) {
        s_compute_lock.unlock();
        return child_query_error(-EACCES);
    }
    auto& completed = find_compute_submit_peer_session_locked(session.node_id)->child_query_completed.at(request.lane);
    ChildQueryRecord* record = nullptr;
    ChildQueryRecord* free = nullptr;
    for (auto& candidate : s_child_queries) {
        if (!candidate.active) {
            if (free == nullptr) free = &candidate;
        } else if (compute_submit_session_token_matches(candidate.session, session) && candidate.request.lane == request.lane) {
            record = &candidate;
        }
    }
    if (record != nullptr && record->request.sequence != request.sequence) {
        int const ERROR = request.sequence <= completed ? -ESTALE : -EAGAIN;
        s_compute_lock.unlock();
        return operation == Operation::CLOSE && ERROR == -ESTALE ? child_query_error(0) : child_query_error(ERROR);
    }
    if (record != nullptr && (record->session.identity != session.identity || record->request != request)) {
        s_compute_lock.unlock();
        return child_query_error(-EINVAL);
    }
    if (record == nullptr) {
        if (request.sequence <= completed || operation == Operation::CLOSE) {
            if (operation == Operation::CLOSE) completed = std::max(completed, request.sequence);
            s_compute_lock.unlock();
            return child_query_error(operation == Operation::CLOSE ? 0 : -ESTALE);
        }
        if (operation != Operation::INSPECT || free == nullptr) {
            s_compute_lock.unlock();
            return child_query_error(-EAGAIN);
        }
        if (s_next_child_query_token == UINT64_MAX) {
            s_compute_lock.unlock();
            return child_query_error(-EOVERFLOW);
        }
        ker::mod::sched::task::Task* owner = nullptr;
        auto const ADMISSION = acquire_child_authority_owner_locked(session, request.task_id, request.receiver_pid, owner);
        if (ADMISSION != ChildAuthorityAdmission::READY) {
            s_compute_lock.unlock();
            return ADMISSION == ChildAuthorityAdmission::AWAITING_ACCEPT ? ChildQueryReply{} : child_query_error(-ESRCH);
        }
        ChildAuthorityRoute upstream{};
        auto const ROUTE = capture_child_authority_route_locked(*owner, upstream);
        if (ROUTE != ChildAuthorityAdmission::READY && ROUTE != ChildAuthorityAdmission::LOCAL) {
            s_compute_lock.unlock();
            owner->release();
            return ROUTE == ChildAuthorityAdmission::AWAITING_ACCEPT ? ChildQueryReply{} : child_query_error(-EIO);
        }
        record = free;
        record->session = session;
        record->request = request;
        record->upstream = upstream;
        record->owner = owner;
        record->local_origin = {g_wki.my_node_id, g_wki.local_boot_epoch};
        record->token = s_next_child_query_token++;
        record->relay.emplace();
        record->active = true;
    }
    if (operation == Operation::CLOSE) retire_child_query_locked(*record);
    if (record->busy) {
        s_compute_lock.unlock();
        return ChildQueryReply{};
    }
    advance_child_query_cleanup_locked(*record);
    if (record->retiring) {
        auto const REPLY = child_query_reply_locked(*record, operation);
        s_compute_lock.unlock();
        notify_proxy_exit_work();
        return REPLY;
    }
    // Revalidate the exact accepted owner before local observations. A retained
    // Task pointer alone is insufficient after execution authority changes.
    ker::mod::sched::task::Task* current = nullptr;
    auto const ADMISSION = acquire_child_authority_owner_locked(session, request.task_id, request.receiver_pid, current);
    bool const CURRENT = ADMISSION == ChildAuthorityAdmission::READY && current == record->owner;
    if (current != nullptr) current->release();  // record retains a non-final pin
    if (!CURRENT) {
        retire_child_query_locked(*record);
        advance_child_query_cleanup_locked(*record);
        s_compute_lock.unlock();
        notify_proxy_exit_work();
        return child_query_error(-ESRCH);
    }
    record->busy = true;
    s_compute_lock.unlock();
    int error = 0;
    child_events::InspectionContext const CONTEXT{record->local_origin, request.spec.caller_origin, request.spec.own_group};
    if (record->relay->phase() == child_query::Phase::IDLE && record->begin_error == 0 && require_upstream_support &&
        record->upstream.session.epoch != 0 &&
        wki_resource_generation_snapshot(record->upstream.session.node_id, ResourceType::COMPUTE, WKI_CHILD_QUERY_RESOURCE_ID) == 0)
        record->begin_error = -ENOTSUP;  // no upstream action exists; CLOSE needs no upstream acknowledgment
    if (record->relay->phase() == child_query::Phase::IDLE && record->begin_error == 0) {
        auto const SNAPSHOT = child_events::inspect_queue(*record->owner, local_child_query_selector(*record), request.options, CONTEXT);
        record->begin_error =
            record->relay->begin(record->token, request.spec, SNAPSHOT, record->upstream.session.epoch != 0, request.remaining_hops);
    } else if (operation == Operation::PROBE) {
        auto const SNAPSHOT =
            child_events::inspect_queue(*record->owner, local_child_query_selector(*record, selected_group), request.options, CONTEXT);
        error = record->relay->probe(selected_group, SNAPSHOT);
    } else if (operation == Operation::VALIDATE) {
        auto const STATUS = child_events::validate_queue_version(*record->owner, record->relay->version());
        error = record->relay->validate(STATUS);
    }
    s_compute_lock.lock();
    advance_child_query_cleanup_locked(*record);
    auto const REPLY = error != 0 ? child_query_error(error) : child_query_reply_locked(*record, operation);
    record->busy = false;
    bool const RETIRING = record->retiring;
    s_compute_lock.unlock();
    if (RETIRING) notify_proxy_exit_work();
    return REPLY;
}

auto native_child_error(int error) -> WkiChildWaitResult { return {.state = WkiChildWaitState::ERROR, .error = error}; }
void finish_native_child(ChildQueryRecord& record, WkiChildWaitResult result) {
    record.native_result = result;
    record.native_phase = NativeChildPhase::DONE;
}
void schedule_native_child_claim(ChildQueryRecord& record, ChildWaitCommand command) {
    record.claim_reply = {};
    record.claim_action = ChildClaimAction{record.token, command, ++record.claim_generation};
    record.claim_issued = false;
    record.claim_next_send_us = 0;
}
void publish_native_child_value(ChildQueryRecord& record, const ChildWaitValue& value) {
    record.native_result = {.state = WkiChildWaitState::CLAIMED,
                            .pid = value.pid,
                            .user_time_us = value.user_time_us,
                            .system_time_us = value.system_time_us,
                            .status = value.status};
    record.native_phase = NativeChildPhase::CLAIMED;
}

// The worker owns busy across each bounded local operation. Network callbacks,
// caller poll/commit and other workers cannot touch these fields while busy.
// Exit/CLOSE may only mark flags, which the worker applies after reacquiring
// compute. No lifecycle operation runs under compute or an incoming peer lease.
void advance_native_child(ChildQueryRecord& record, bool require_upstream_support) {
    using Operation = child_query::Operation;
    using Phase = child_query::Phase;
    child_events::InspectionContext const CONTEXT{record.local_origin, record.request.spec.caller_origin, record.request.spec.own_group};
    auto fail = [&](int error) { finish_native_child(record, native_child_error(error)); };
    switch (record.native_phase) {
        case NativeChildPhase::INSPECT: {
            if (record.relay->phase() == Phase::IDLE) {
                if (require_upstream_support && (wki_resource_generation_snapshot(record.upstream.session.node_id, ResourceType::COMPUTE,
                                                                                  WKI_CHILD_QUERY_RESOURCE_ID) == 0 ||
                                                 wki_resource_generation_snapshot(record.upstream.session.node_id, ResourceType::COMPUTE,
                                                                                  WKI_CHILD_CLAIM_RESOURCE_ID) == 0)) {
                    fail(-ENOTSUP);
                    return;
                }
                auto const LOCAL =
                    child_events::inspect_queue(*record.owner, local_child_query_selector(record), record.request.options, CONTEXT);
                int const ERROR = record.relay->begin(record.token, record.request.spec, LOCAL, true, record.request.remaining_hops);
                if (ERROR != 0) fail(ERROR);
                return;
            }
            auto const OUTCOME = record.relay->outcome(Operation::INSPECT);
            if (!OUTCOME) return;
            if (OUTCOME->error != 0) {
                fail(OUTCOME->error);
                return;
            }
            record.native_phase = record.request.spec.group_alias ? NativeChildPhase::PROBE : NativeChildPhase::VALIDATE;
            return;
        }
        case NativeChildPhase::PROBE: {
            if (record.relay->phase() == Phase::INSPECTED) {
                auto group = record.relay->chosen_group();
                if (!group) {
                    auto const INSPECT = record.relay->outcome(Operation::INSPECT);
                    if (!INSPECT || INSPECT->summary.groups.flags != 0) {
                        fail(-EOVERFLOW);
                        return;
                    }
                    group = record.request.spec.caller_origin;  // validate an exact empty selection
                }
                auto const LOCAL =
                    child_events::inspect_queue(*record.owner, local_child_query_selector(record, group), record.request.options, CONTEXT);
                int const ERROR = record.relay->probe(*group, LOCAL);
                if (ERROR != 0) fail(ERROR);
                return;
            }
            auto const OUTCOME = record.relay->outcome(Operation::PROBE);
            if (!OUTCOME) return;
            if (OUTCOME->error != 0) {
                fail(OUTCOME->error);
                return;
            }
            record.native_phase = NativeChildPhase::VALIDATE;
            return;
        }
        case NativeChildPhase::VALIDATE: {
            if (record.relay->phase() == Phase::INSPECTED || record.relay->phase() == Phase::PROBED) {
                int const ERROR = record.relay->validate(child_events::validate_queue_version(*record.owner, record.relay->version()));
                if (ERROR != 0) fail(ERROR);
                return;
            }
            auto const OUTCOME = record.relay->outcome(Operation::VALIDATE);
            if (!OUTCOME) return;
            if (OUTCOME->error != 0) {
                fail(OUTCOME->error);
                return;
            }
            // Collision detection is over the complete, validated authority
            // chain and precedes both local and remote BEGIN operations.
            if (OUTCOME->summary.pids.ambiguous) {
                fail(-EOVERFLOW);
                return;
            }
            if (!OUTCOME->summary.potential) {
                finish_native_child(record, {.state = WkiChildWaitState::NO_CHILD});
                return;
            }
            if (OUTCOME->summary.ready_depth == child_query::NO_READY) {
                finish_native_child(record, {.state = WkiChildWaitState::EMPTY});
                return;
            }
            record.claim_depth = OUTCOME->summary.ready_depth;
            record.native_phase = NativeChildPhase::CLAIM;
            if (*record.claim_depth != 0) schedule_native_child_claim(record, ChildWaitCommand::BEGIN);
            return;
        }
        case NativeChildPhase::CLAIM: {
            if (*record.claim_depth == 0) {
                auto selector = record.relay->validated_selector();
                if (!selector) {
                    fail(-EPROTO);
                    return;
                }
                if (selector->group_origin == record.local_origin) selector->group_origin = {};
                auto const PROBE = child_events::try_claim_if_current(*record.owner, record.native_claim, *selector, record.request.options,
                                                                      record.relay->version(), record.native_event);
                if (PROBE.version != child_events::QueueVersionStatus::CURRENT) {
                    fail(PROBE.version == child_events::QueueVersionStatus::EXHAUSTED ? -EOVERFLOW : -EAGAIN);
                } else if (PROBE.probe != child_events::ProbeResult::CLAIMED) {
                    finish_native_child(record, {.state = PROBE.probe == child_events::ProbeResult::NO_CHILD ? WkiChildWaitState::NO_CHILD
                                                                                                             : WkiChildWaitState::EMPTY});
                } else {
                    record.native_claim_retired = false;
                    auto const& event = record.native_event;
                    publish_native_child_value(record, {.pid = event.subject_pid,
                                                        .user_time_us = event.user_time_us,
                                                        .system_time_us = event.system_time_us,
                                                        .status = event.status});
                }
                return;
            }
            auto const& REPLY = record.claim_reply;
            if (REPLY.phase == ChildWaitPhase::PENDING) return;
            if (REPLY.phase == ChildWaitPhase::CLAIMED)
                publish_native_child_value(record, REPLY.value);
            else if (REPLY.error != 0)
                fail(REPLY.error);
            else if (REPLY.committed)
                fail(-EPROTO);
            else
                finish_native_child(record, {.state = WkiChildWaitState::EMPTY});
            return;
        }
        case NativeChildPhase::COMMIT: {
            bool committed = false;
            int error = 0;
            if (*record.claim_depth == 0) {
                committed = child_events::commit_claim(*record.owner, record.native_claim, record.native_event,
                                                       (record.request.options & child_events::OPTION_NOWAIT) != 0);
                record.native_event = {};
                record.native_claim_retired = true;
                error = committed ? 0 : -EINTR;
            } else {
                auto const& REPLY = record.claim_reply;
                if (REPLY.phase == ChildWaitPhase::PENDING) return;
                committed = REPLY.phase == ChildWaitPhase::DONE && REPLY.committed && REPLY.error == 0;
                error = REPLY.error != 0 ? REPLY.error : -EPROTO;
                using namespace ker::mod::sched::task;
                if (committed && (record.request.options & child_events::OPTION_NOWAIT) == 0 && REPLY.value.kind == ChildEventKind::EXIT &&
                    REPLY.value.audience == ChildEventAudience::PARENT)
                    static_cast<void>(
                        child_events::import_live_reaped_cpu_times(*record.owner, REPLY.value.user_time_us, REPLY.value.system_time_us));
            }
            auto result = record.native_result;
            result.state = committed ? WkiChildWaitState::COMPLETE : WkiChildWaitState::ERROR;
            result.error = committed ? 0 : error;
            finish_native_child(record, result);
            return;
        }
        case NativeChildPhase::CLAIMED:
        case NativeChildPhase::DONE:
            return;
    }
}

auto run_one_native_child_wait(bool require_upstream_support = true) -> bool {
    uint64_t const NOW = wki_now_us();
    s_compute_lock.lock();
    for (size_t offset = 0; offset < s_child_queries.size(); ++offset) {
        size_t const INDEX = (s_native_child_cursor + offset) % s_child_queries.size();
        auto& record = s_child_queries[INDEX];
        if (!record.active || record.native_waiter == nullptr || record.busy) continue;
        advance_child_query_cleanup_locked(record);
        if (record.native_detached && record.native_phase != NativeChildPhase::COMMIT) retire_child_query_locked(record);
        if (record.retiring && record.native_claim_retired) continue;
        bool const CANCEL = record.retiring;
        if (!CANCEL && (record.native_phase == NativeChildPhase::CLAIMED || record.native_phase == NativeChildPhase::DONE)) continue;
        if (!CANCEL && record.native_phase != NativeChildPhase::COMMIT) {
            ChildAuthorityRoute current{};
            bool const MATCH = capture_child_authority_route_locked(*record.owner, current) == ChildAuthorityAdmission::READY &&
                               current.task_id == record.upstream.task_id && current.receiver_pid == record.upstream.receiver_pid &&
                               compute_submit_session_token_matches(current.session, record.upstream.session) &&
                               current.session.identity == record.upstream.session.identity;
            if (!MATCH || record.owner->resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0) {
                finish_native_child(record, native_child_error(MATCH ? -EINTR : -EIO));
                s_compute_lock.unlock();
                return true;
            }
        }
        // A terminal receipt is already known, even if this worker ran late.
        // Apply its observation once instead of turning it into an unknown outcome.
        bool const COMMIT_RECEIPT = record.native_phase == NativeChildPhase::COMMIT && record.claim_depth && *record.claim_depth != 0 &&
                                    record.claim_reply.phase != ChildWaitPhase::PENDING;
        if (!CANCEL && NOW >= record.native_deadline_us && !COMMIT_RECEIPT) {
            finish_native_child(record, native_child_error(record.native_phase == NativeChildPhase::COMMIT ? -EIO : -ETIMEDOUT));
            s_compute_lock.unlock();
            return true;
        }
        record.busy = true;
        s_native_child_cursor = (INDEX + 1) % s_child_queries.size();
        s_compute_lock.unlock();
        if (CANCEL) {
            child_events::cancel_claim(record.native_claim);
            record.native_event = {};
            record.native_claim_retired = true;
        } else
            advance_native_child(record, require_upstream_support);
        s_compute_lock.lock();
        advance_child_query_cleanup_locked(record);
        record.busy = false;
        s_compute_lock.unlock();
        return true;
    }
    s_compute_lock.unlock();
    return false;
}

// Future worker transport reserves one action, drops compute serialization,
// acquires that action's single upstream lease, and sends without waiting.
[[maybe_unused]] auto take_child_query_work(uint64_t now_us = 0) -> std::optional<ChildQueryWork> {
    s_compute_lock.lock();
    for (size_t offset = 0; offset < s_child_queries.size(); ++offset) {
        size_t const INDEX = (s_child_query_cursor + offset) % s_child_queries.size();
        auto& record = s_child_queries[INDEX];
        if (!record.active || record.busy || !record.relay) continue;
        advance_child_query_cleanup_locked(record);
        auto const PENDING = record.relay->pending_action();
        if (!PENDING) continue;
        if (record.last_action != PENDING) {
            record.last_action = PENDING;
            record.next_send_us = 0;
        }
        if (now_us != 0 && now_us < record.next_send_us) continue;
        if (now_us != 0) static_cast<void>(record.relay->retry(*PENDING));
        auto const ACTION = record.relay->take_action();
        if (!ACTION) continue;
        auto request = record.request;
        request.sequence = record.token;
        request.lane = static_cast<uint8_t>(INDEX);
        request.task_id = record.upstream.task_id;
        request.receiver_pid = record.upstream.receiver_pid;
        request.remaining_hops = static_cast<uint16_t>(request.remaining_hops - 1);
        ChildQueryWork const WORK{static_cast<uint8_t>(INDEX), *ACTION, record.upstream, request};
        s_child_query_cursor = (INDEX + 1) % s_child_queries.size();
        record.next_send_us = wki_future_deadline_us(now_us, CHILD_QUERY_RETRY_US);
        s_compute_lock.unlock();
        return WORK;
    }
    s_compute_lock.unlock();
    return std::nullopt;
}

auto child_query_work_matches_locked(const ChildQueryRecord& record, const ChildQueryWork& work) -> bool {
    return record.active && !record.busy && record.relay && record.token == work.action.query &&
           record.upstream.task_id == work.route.task_id && record.upstream.receiver_pid == work.route.receiver_pid &&
           compute_submit_session_token_matches(record.upstream.session, work.route.session) &&
           record.upstream.session.identity == work.route.session.identity;
}

[[maybe_unused]] auto retry_child_query_work(const ChildQueryWork& work) -> bool {
    if (work.slot >= s_child_queries.size()) return false;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(work.slot);
    bool const RETRIED = child_query_work_matches_locked(record, work) && record.relay->retry(work.action);
    s_compute_lock.unlock();
    return RETRIED;
}

[[maybe_unused]] auto accept_child_query_work(const PeerLifecycleLease& lifecycle, const ChildQueryWork& work,
                                              const child_query::Outcome& outcome) -> bool {
    if (!lifecycle.owns(work.route.session.node_id) || work.slot >= s_child_queries.size()) return false;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(work.slot);
    bool const ACCEPTED = child_query_work_matches_locked(record, work) && child_query_session_allowed_locked(work.route.session) &&
                          record.relay->accept(work.action, outcome);
    bool const RETIRING = ACCEPTED && record.retiring;
    s_compute_lock.unlock();
    if (RETIRING) notify_proxy_exit_work();
    return ACCEPTED;
}

// A pending upstream CLOSE is not reclaimable. Slot/token/session stay reserved
// until explicit acknowledgment or exact session termination, even after exit.
auto run_one_child_query_retirement() -> bool {
    if (!s_child_query_cleanup_pending.load(std::memory_order_acquire)) return false;
    ker::mod::sched::task::Task* owner = nullptr;
    ker::mod::sched::task::Task* waiter = nullptr;
    bool pending = false;
    s_compute_lock.lock();
    for (auto& record : s_child_queries) {
        if (!record.active) continue;
        pending |= record.retiring || record.upstream_terminal;
        if (record.busy) continue;
        advance_child_query_cleanup_locked(record);
        if (!record.retiring || !record.relay || record.relay->phase() != child_query::Phase::CLOSED ||
            child_query_claim_pending_locked(record) || !record.native_claim_retired)
            continue;
        owner = record.owner;
        waiter = record.native_waiter;
        record.native_waiter = nullptr;
        record.native_phase = NativeChildPhase::INSPECT;
        record.native_result = {};
        record.native_detached = false;
        record.native_deadline_us = 0;
        record.native_event = {};
        record.relay.reset();
        record.owner = nullptr;
        record.session = {};
        record.upstream = {};
        record.request = {};
        record.begin_error = 0;
        record.last_action.reset();
        record.next_send_us = 0;
        record.claim_depth.reset();
        record.claim_reply = {};
        record.claim_value.reset();
        record.claim_action.reset();
        record.claim_generation = 0;
        record.claim_issued = false;
        record.claim_next_send_us = 0;
        record.retiring = record.upstream_terminal = record.active = false;
        break;
    }
    if (!pending) s_child_query_cleanup_pending.store(false, std::memory_order_release);
    s_compute_lock.unlock();
    if (waiter != nullptr) waiter->release();
    if (owner != nullptr) owner->release();
    return owner != nullptr;
}

// Called only by compute workers outside any incoming peer lease. A true result
// requests a bounded worker sleep/poll, including while replies are outstanding.
auto run_one_child_query_send() -> bool {
    auto const WORK = take_child_query_work(wki_now_us());
    if (!WORK) {
        bool pending = false;
        s_compute_lock.lock();
        for (auto const& record : s_child_queries)
            if (record.active && !record.busy && record.relay && record.relay->pending_action()) {
                pending = true;
                break;
            }
        s_compute_lock.unlock();
        return pending;
    }
    PeerLifecycleLease lifecycle;
    if (!lifecycle.acquire(WORK->route.session.node_id)) return true;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(WORK->slot);
    bool const MATCH = child_query_work_matches_locked(record, *WORK) && record.relay->pending_action() == WORK->action;
    bool const CURRENT = MATCH && child_query_session_allowed_locked(WORK->route.session);
    if (MATCH && !CURRENT) {
        record.upstream_terminal = true;
        advance_child_query_cleanup_locked(record);
    }
    s_compute_lock.unlock();
    if (CURRENT) {
        auto const REQUEST = encode_child_query_request(WORK->request, WORK->action);
        static_cast<void>(wki_send_on_channel_generation(WORK->route.session.node_id, WORK->route.session.rx_channel,
                                                         WORK->route.session.rx_channel_generation, MsgType::TASK_CHILD_QUERY_REQ, &REQUEST,
                                                         sizeof(REQUEST)));
        // Timed application retries keep the same sequence/action. Reliable
        // transport acceptance is not completion or a cleanup acknowledgment.
    }
    return true;
}

auto receive_child_query_response(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                  const TaskChildQueryResponse& wire) -> bool {
    ChildQueryRequest request{};
    ChildQueryReply reply{};
    if (!lifecycle.owns(session.node_id) || !decode_child_query_response(wire, request, reply)) return false;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(request.lane);
    if (!record.active || record.busy || !record.relay || !record.last_action || record.token != request.sequence ||
        record.relay->pending_action() != record.last_action || !compute_submit_session_token_matches(record.upstream.session, session) ||
        record.upstream.session.identity != session.identity || !child_query_session_allowed_locked(session)) {
        s_compute_lock.unlock();
        return false;
    }
    auto expected = record.request;
    expected.sequence = record.token;
    expected.lane = request.lane;
    expected.task_id = record.upstream.task_id;
    expected.receiver_pid = record.upstream.receiver_pid;
    expected.remaining_hops = static_cast<uint16_t>(expected.remaining_hops - 1);
    auto const ENVELOPE = encode_child_query_request(expected, *record.last_action);
    if (std::memcmp(&ENVELOPE, &wire.request, sizeof(ENVELOPE)) != 0) {
        s_compute_lock.unlock();
        return false;
    }
    // Pending is deliberately not passed to Relay as an empty success. The
    // already reserved retry deadline drives another request without a hot loop.
    bool const ACCEPTED = !reply.ready || record.relay->accept(*record.last_action, reply.outcome);
    bool const RETIRING = ACCEPTED && record.retiring;
    s_compute_lock.unlock();
    if (RETIRING) notify_proxy_exit_work();
    return ACCEPTED;
}

auto execute_child_query_packet(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                const TaskChildQueryRequest& wire) -> TaskChildQueryResponse {
    ChildQueryRequest request{};
    auto reply = child_query_error(-EINVAL);
    if (decode_child_query_request(wire, request))
        reply = execute_child_query(lifecycle, session, request, static_cast<child_query::Operation>(wire.operation),
                                    child_query_local_origin(wire.selected_group));
    return encode_child_query_response(wire, reply);
}

auto child_wait_error(int error) -> ChildWaitReply {
    return {.phase = error == -EAGAIN ? ChildWaitPhase::PENDING : ChildWaitPhase::DONE, .error = error};
}

void retire_child_wait_transaction_locked(ChildWaitTransaction& transaction) {
    auto* peer = find_compute_submit_peer_session_locked(transaction.session.node_id);
    if (peer != nullptr && compute_submit_session_token_matches(peer->current, transaction.session)) {
        auto& completed = peer->child_wait_completed.at(transaction.request.lane);
        completed = std::max(completed, transaction.request.sequence);
    }
    transaction.retiring = true;
    s_child_wait_cleanup_pending.store(true, std::memory_order_release);
}

void retire_child_wait_transactions_for_task(ker::mod::sched::task::Task& task) {
    s_compute_lock.lock();
    for (auto& transaction : s_child_wait_transactions)
        if (transaction.active && transaction.owner == &task) retire_child_wait_transaction_locked(transaction);
    s_compute_lock.unlock();
    if (s_child_wait_cleanup_pending.load(std::memory_order_acquire)) notify_proxy_exit_work();
}

// No peer/compute lock crosses cancellation, waiter wakes or final Task release.
auto run_one_child_wait_retirement() -> bool {
    if (!s_child_wait_cleanup_pending.load(std::memory_order_acquire)) return false;
    ChildWaitTransaction* selected = nullptr;
    bool pending = false;
    s_compute_lock.lock();
    for (auto& transaction : s_child_wait_transactions) {
        if (!transaction.active || !transaction.retiring) continue;
        pending = true;
        if (!transaction.busy) {
            transaction.busy = true;
            selected = &transaction;
            break;
        }
    }
    if (!pending) s_child_wait_cleanup_pending.store(false, std::memory_order_release);
    s_compute_lock.unlock();
    if (selected == nullptr) return false;
    child_events::cancel_claim(selected->claim);
    auto* owner = selected->owner;
    s_compute_lock.lock();
    selected->claimed = {};
    selected->owner = nullptr;
    selected->session = {};
    selected->request = {};
    selected->reply = {};
    selected->retiring = false;
    selected->busy = false;
    selected->active = false;
    s_compute_lock.unlock();
    if (owner != nullptr) owner->release();
    return true;
}

// Caller owns an authenticated peer lifecycle lease, but no compute/channel/RX
// spinlock. It spans only bounded local claim effects, never a remote wait,
// usercopy or network send. No wire caller is enabled until the client protocol
// supplies stable lane sequencing, selector aggregation and fatal-wait cleanup.
[[maybe_unused]] auto execute_child_wait(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                         const ChildWaitRequest& request, ChildWaitCommand command) -> ChildWaitReply {
    if (!lifecycle.owns(session.node_id)) return child_wait_error(-EACCES);
    if (command > ChildWaitCommand::ACK || request.sequence == 0 ||
        (request.queue_version.queue == 0 || request.queue_version.groups == 0) || request.lane >= CHILD_WAIT_LANES ||
        request.task_id == 0 || request.receiver_pid == 0)
        return child_wait_error(-EINVAL);
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    s_compute_lock.lock();
    if ((session.identity.policy & REQUIRED) != REQUIRED || (session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) == 0 ||
        !compute_submit_admission_is_current_locked(session)) {
        s_compute_lock.unlock();
        return child_wait_error(-EACCES);
    }
    auto& completed = find_compute_submit_peer_session_locked(session.node_id)->child_wait_completed.at(request.lane);
    ChildWaitTransaction* transaction = nullptr;
    ChildWaitTransaction* free = nullptr;
    for (auto& candidate : s_child_wait_transactions) {
        if (!candidate.active) {
            if (free == nullptr) free = &candidate;
            continue;
        }
        if (compute_submit_session_token_matches(candidate.session, session) && candidate.request.lane == request.lane) {
            transaction = &candidate;
            break;
        }
    }
    if (transaction != nullptr && transaction->session.identity != session.identity) {
        s_compute_lock.unlock();
        return child_wait_error(-EACCES);
    }
    if (transaction != nullptr && transaction->request.sequence != request.sequence) {
        int const ERROR = request.sequence <= completed ? -ESTALE : -EAGAIN;
        s_compute_lock.unlock();
        return child_wait_error(ERROR);
    }
    if (transaction != nullptr && transaction->request != request) {
        s_compute_lock.unlock();
        return child_wait_error(-EINVAL);
    }
    if (transaction != nullptr && (transaction->busy || transaction->retiring)) {
        bool const RETIRED = transaction->retiring;
        s_compute_lock.unlock();
        return child_wait_error(RETIRED ? -ESTALE : -EAGAIN);
    }
    if (transaction == nullptr) {
        if (request.sequence <= completed) {
            s_compute_lock.unlock();
            return child_wait_error(-ESTALE);
        }
        // Cancel/ACK can beat a deferred BEGIN. Its watermark must survive so
        // that queued or delayed work cannot resurrect the abandoned request.
        if (command == ChildWaitCommand::ACK || command == ChildWaitCommand::RELEASE) {
            completed = request.sequence;
            s_compute_lock.unlock();
            return child_wait_error(command == ChildWaitCommand::RELEASE ? -ECANCELED : 0);
        }
        if (command != ChildWaitCommand::BEGIN || free == nullptr) {
            s_compute_lock.unlock();
            return child_wait_error(-EAGAIN);
        }
        ker::mod::sched::task::Task* owner = nullptr;
        auto const ADMISSION = acquire_child_authority_owner_locked(session, request.task_id, request.receiver_pid, owner);
        if (ADMISSION != ChildAuthorityAdmission::READY) {
            s_compute_lock.unlock();
            // No owner/claim storage is published before ACCEPT. The client
            // retains its immutable request and retries the PENDING response.
            return ADMISSION == ChildAuthorityAdmission::AWAITING_ACCEPT ? ChildWaitReply{} : child_wait_error(-ESRCH);
        }
        transaction = free;
        transaction->session = session;
        transaction->request = request;
        transaction->owner = owner;
        transaction->active = true;
    }
    if (command == ChildWaitCommand::ACK) {
        if (transaction->reply.phase != ChildWaitPhase::DONE) {
            s_compute_lock.unlock();
            return child_wait_error(-EAGAIN);
        }
        auto const REPLY = transaction->reply;
        retire_child_wait_transaction_locked(*transaction);
        s_compute_lock.unlock();
        notify_proxy_exit_work();
        return REPLY;
    }
    if (transaction->reply.phase == ChildWaitPhase::DONE ||
        (command == ChildWaitCommand::BEGIN && transaction->reply.phase == ChildWaitPhase::CLAIMED)) {
        auto const REPLY = transaction->reply;
        s_compute_lock.unlock();
        return REPLY;
    }
    if (command == ChildWaitCommand::COMMIT) {
        ker::mod::sched::task::Task* current_owner = nullptr;
        auto const ADMISSION = acquire_child_authority_owner_locked(session, request.task_id, request.receiver_pid, current_owner);
        bool const CURRENT = ADMISSION == ChildAuthorityAdmission::READY && current_owner == transaction->owner;
        if (current_owner != nullptr) current_owner->release();
        if (!CURRENT) {
            retire_child_wait_transaction_locked(*transaction);
            s_compute_lock.unlock();
            notify_proxy_exit_work();
            return child_wait_error(-ESRCH);
        }
    }
    transaction->busy = true;
    auto reply = transaction->reply;
    s_compute_lock.unlock();

    if (command == ChildWaitCommand::BEGIN) {
        auto const PROBE = child_events::try_claim_if_current(*transaction->owner, transaction->claim, request.selector, request.options,
                                                              request.queue_version, transaction->claimed);
        reply.probe = PROBE.probe;
        reply.phase = reply.probe == child_events::ProbeResult::CLAIMED ? ChildWaitPhase::CLAIMED : ChildWaitPhase::DONE;
        reply.error = PROBE.version == child_events::QueueVersionStatus::EXHAUSTED ? -EOVERFLOW
                      : PROBE.version == child_events::QueueVersionStatus::STALE   ? -EAGAIN
                      : reply.probe == child_events::ProbeResult::NO_CHILD         ? -ECHILD
                                                                                   : 0;
        auto const& event = transaction->claimed;
        reply.value = {.pid = event.subject_pid,
                       .process_group = event.process_group,
                       .group_origin = event.group_origin,
                       .kind = event.kind,
                       .audience = event.audience,
                       .user_time_us = event.user_time_us,
                       .system_time_us = event.system_time_us,
                       .status = event.status};
    } else if (command == ChildWaitCommand::COMMIT) {
        bool const COMMITTED = child_events::commit_claim(*transaction->owner, transaction->claim, transaction->claimed,
                                                          (request.options & child_events::OPTION_NOWAIT) != 0);
        reply.phase = ChildWaitPhase::DONE;
        reply.error = COMMITTED ? 0 : -EINTR;
        reply.committed = COMMITTED;
        transaction->claimed = {};  // cached reply contains values, never retired event storage
    } else {
        child_events::cancel_claim(transaction->claim);
        transaction->claimed = {};
        reply.phase = ChildWaitPhase::DONE;
        reply.error = -ECANCELED;
    }
    s_compute_lock.lock();
    transaction->reply = reply;
    transaction->busy = false;
    bool const RETIRING = transaction->retiring;
    s_compute_lock.unlock();
    if (RETIRING) notify_proxy_exit_work();
    return reply;
}

auto execute_query_claim(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session, const ChildClaimRequest& request,
                         ChildWaitCommand command, bool require_upstream_support = true) -> ChildWaitReply {
    if (!lifecycle.owns(session.node_id)) return child_wait_error(-EACCES);
    if (command > ChildWaitCommand::RELEASE || request.sequence == 0 || request.task_id == 0 || request.receiver_pid == 0 ||
        request.lane >= CHILD_WAIT_LANES || request.depth > child_query::MAX_HOPS)
        return child_wait_error(-EINVAL);
    s_compute_lock.lock();
    if (!child_query_session_allowed_locked(session)) {
        s_compute_lock.unlock();
        return child_wait_error(-EACCES);
    }
    ChildQueryRecord* record = nullptr;
    for (auto& candidate : s_child_queries)
        if (candidate.active && compute_submit_session_token_matches(candidate.session, session) &&
            candidate.request.lane == request.lane && candidate.request.sequence == request.sequence) {
            record = &candidate;
            break;
        }
    if (record == nullptr) {
        s_compute_lock.unlock();
        return child_wait_error(-ESTALE);
    }
    if (record->session.identity != session.identity || record->request.task_id != request.task_id ||
        record->request.receiver_pid != request.receiver_pid || (record->claim_depth && *record->claim_depth != request.depth)) {
        s_compute_lock.unlock();
        return child_wait_error(-EINVAL);
    }
    if (record->busy) {
        s_compute_lock.unlock();
        return {};
    }
    advance_child_query_cleanup_locked(*record);
    if (record->retiring) {
        s_compute_lock.unlock();
        return child_wait_error(-ECANCELED);
    }
    // A known terminal receipt is replayed without attempting another effect.
    if (record->claim_depth && (record->claim_reply.phase == ChildWaitPhase::DONE ||
                                (command == ChildWaitCommand::BEGIN && record->claim_reply.phase == ChildWaitPhase::CLAIMED))) {
        auto const REPLY = record->claim_reply;
        s_compute_lock.unlock();
        return REPLY;
    }
    if (record->claim_action && record->claim_action->command == command) {
        s_compute_lock.unlock();
        return {};
    }
    if (command == ChildWaitCommand::COMMIT && record->claim_reply.phase != ChildWaitPhase::CLAIMED) {
        s_compute_lock.unlock();
        return child_wait_error(-EAGAIN);
    }
    if (record->claim_action && command != ChildWaitCommand::RELEASE) {
        s_compute_lock.unlock();
        return {};
    }
    auto selected = record->relay->validated_selector();
    auto const VALIDATED = record->relay->outcome(child_query::Operation::VALIDATE);
    if (!selected || !VALIDATED || VALIDATED->error != 0) {
        int const ERROR = VALIDATED && VALIDATED->error != 0 ? VALIDATED->error : -EAGAIN;
        s_compute_lock.unlock();
        return child_wait_error(ERROR);
    }
    if (VALIDATED->summary.pids.ambiguous) {
        s_compute_lock.unlock();
        return child_wait_error(-EOVERFLOW);
    }
    if (VALIDATED->summary.ready_depth == child_query::NO_READY || VALIDATED->summary.ready_depth != request.depth) {
        int const ERROR = !VALIDATED->summary.potential ? -ECHILD : -EAGAIN;
        s_compute_lock.unlock();
        return {.phase = ChildWaitPhase::DONE, .error = ERROR};
    }
    ker::mod::sched::task::Task* current = nullptr;
    auto const ADMISSION = acquire_child_authority_owner_locked(session, request.task_id, request.receiver_pid, current);
    bool const CURRENT = ADMISSION == ChildAuthorityAdmission::READY && current == record->owner;
    if (current != nullptr) current->release();
    if (!CURRENT) {
        retire_child_query_locked(*record);
        advance_child_query_cleanup_locked(*record);
        s_compute_lock.unlock();
        notify_proxy_exit_work();
        return child_wait_error(-ESRCH);
    }
    if (selected->group_origin == record->local_origin) selected->group_origin = {};
    ChildWaitRequest const LOCAL{.sequence = request.sequence,
                                 .queue_version = record->relay->version(),
                                 .task_id = request.task_id,
                                 .receiver_pid = request.receiver_pid,
                                 .selector = *selected,
                                 .options = record->request.options,
                                 .lane = request.lane};
    record->claim_depth = request.depth;
    record->busy = true;
    s_compute_lock.unlock();
    bool const FORWARD = request.depth != 0;
    bool const SUPPORTED =
        !FORWARD || !require_upstream_support ||
        wki_resource_generation_snapshot(record->upstream.session.node_id, ResourceType::COMPUTE, WKI_CHILD_CLAIM_RESOURCE_ID) != 0;
    auto reply = FORWARD ? ChildWaitReply{} : execute_child_wait(lifecycle, session, LOCAL, command);
    s_compute_lock.lock();
    if (FORWARD) {
        if (record->retiring)
            reply = child_wait_error(-ECANCELED);
        else if (record->upstream_terminal)
            reply = child_wait_error(-EIO);
        else if (!SUPPORTED)
            reply = child_wait_error(-ENOTSUP);
        else {
            // BEGIN, optional COMMIT, optional RELEASE: at most three actions.
            ++record->claim_generation;
            record->claim_action = ChildClaimAction{record->token, command, record->claim_generation};
            record->claim_issued = false;
            record->claim_next_send_us = 0;
        }
    }
    record->claim_reply = reply;
    if (record->retiring) retire_child_query_locked(*record);  // catches a BEGIN reserved while busy
    advance_child_query_cleanup_locked(*record);
    record->busy = false;
    bool const RETIRING = record->retiring;
    s_compute_lock.unlock();
    if (RETIRING) notify_proxy_exit_work();
    return reply;
}

auto take_child_claim_work(uint64_t now_us) -> std::optional<ChildClaimWork> {
    s_compute_lock.lock();
    for (size_t offset = 0; offset < s_child_queries.size(); ++offset) {
        size_t const INDEX = (s_child_claim_cursor + offset) % s_child_queries.size();
        auto& record = s_child_queries[INDEX];
        if (!record.active || record.busy) continue;
        advance_child_query_cleanup_locked(record);
        if (record.retiring || !record.claim_action || !record.claim_depth || *record.claim_depth == 0 ||
            now_us < record.claim_next_send_us)
            continue;
        record.claim_issued = true;
        record.claim_next_send_us = wki_future_deadline_us(now_us, CHILD_QUERY_RETRY_US);
        s_child_claim_cursor = (INDEX + 1) % s_child_queries.size();
        ChildClaimWork const WORK{static_cast<uint8_t>(INDEX),
                                  *record.claim_action,
                                  record.upstream,
                                  {.sequence = record.token,
                                   .task_id = record.upstream.task_id,
                                   .receiver_pid = record.upstream.receiver_pid,
                                   .depth = static_cast<uint16_t>(*record.claim_depth - 1),
                                   .lane = static_cast<uint8_t>(INDEX)}};
        s_compute_lock.unlock();
        return WORK;
    }
    s_compute_lock.unlock();
    return std::nullopt;
}

auto child_claim_work_matches_locked(const ChildQueryRecord& record, const ChildClaimWork& work) -> bool {
    return record.active && !record.busy && !record.retiring && record.claim_action == work.action && record.claim_issued &&
           record.token == work.action.query && record.upstream.task_id == work.route.task_id &&
           record.upstream.receiver_pid == work.route.receiver_pid &&
           compute_submit_session_token_matches(record.upstream.session, work.route.session) &&
           record.upstream.session.identity == work.route.session.identity;
}
auto run_one_child_claim_send() -> bool {
    auto const WORK = take_child_claim_work(wki_now_us());
    if (!WORK) {
        bool pending = false;
        s_compute_lock.lock();
        for (auto const& record : s_child_queries)
            if (record.active && !record.busy && !record.retiring && record.claim_action) {
                pending = true;
                break;
            }
        s_compute_lock.unlock();
        return pending;
    }
    PeerLifecycleLease lifecycle;
    if (!lifecycle.acquire(WORK->route.session.node_id)) return true;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(WORK->slot);
    bool const MATCH = child_claim_work_matches_locked(record, *WORK);
    bool const CURRENT = MATCH && child_query_session_allowed_locked(WORK->route.session);
    if (MATCH && !CURRENT) {
        record.upstream_terminal = true;
        advance_child_query_cleanup_locked(record);
    }
    s_compute_lock.unlock();
    if (CURRENT) {
        auto const REQUEST = encode_child_claim_request(WORK->request, WORK->action);
        static_cast<void>(wki_send_on_channel_generation(WORK->route.session.node_id, WORK->route.session.rx_channel,
                                                         WORK->route.session.rx_channel_generation, MsgType::TASK_CHILD_CLAIM_REQ, &REQUEST,
                                                         sizeof(REQUEST)));
    }
    return true;
}

auto receive_child_claim_response(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                  const TaskChildClaimResponse& wire) -> bool {
    ChildClaimRequest request{};
    ChildWaitReply reply{};
    if (!lifecycle.owns(session.node_id) || !decode_child_claim_response(wire, request, reply)) return false;
    s_compute_lock.lock();
    auto& record = s_child_queries.at(request.lane);
    if (!record.active || record.busy || record.retiring || !record.claim_action || !record.claim_issued || !record.claim_depth ||
        *record.claim_depth == 0 || record.token != request.sequence ||
        !compute_submit_session_token_matches(record.upstream.session, session) || record.upstream.session.identity != session.identity ||
        !child_query_session_allowed_locked(session)) {
        s_compute_lock.unlock();
        return false;
    }
    ChildClaimRequest const EXPECTED{.sequence = record.token,
                                     .task_id = record.upstream.task_id,
                                     .receiver_pid = record.upstream.receiver_pid,
                                     .depth = static_cast<uint16_t>(*record.claim_depth - 1),
                                     .lane = request.lane};
    auto const ENVELOPE = encode_child_claim_request(EXPECTED, *record.claim_action);
    if (std::memcmp(&ENVELOPE, &wire.request, sizeof(ENVELOPE)) != 0) {
        s_compute_lock.unlock();
        return false;
    }
    if (reply.phase == ChildWaitPhase::PENDING) {
        s_compute_lock.unlock();
        return true;
    }
    if (reply.phase == ChildWaitPhase::CLAIMED && record.claim_action->command != ChildWaitCommand::BEGIN) {
        s_compute_lock.unlock();
        return false;
    }
    if (reply.value.pid != 0) {
        auto const SELECTOR = record.relay->validated_selector();
        if (!SELECTOR || !child_events::selector_matches(*SELECTOR, reply.value.pid, reply.value.process_group, reply.value.group_origin) ||
            (record.claim_value && *record.claim_value != reply.value)) {
            s_compute_lock.unlock();
            return false;
        }
        record.claim_value = reply.value;
    }
    // Values only: intermediate nodes never charge the child's CPU again.
    record.claim_reply = reply;
    record.claim_action.reset();
    s_compute_lock.unlock();
    return true;
}

auto execute_child_claim_packet(const PeerLifecycleLease& lifecycle, const ComputeSubmitSessionToken& session,
                                const TaskChildClaimRequest& wire) -> TaskChildClaimResponse {
    ChildClaimRequest request{};
    auto reply = child_wait_error(-EINVAL);
    if (decode_child_claim_request(wire, request))
        reply = execute_query_claim(lifecycle, session, request, static_cast<ChildWaitCommand>(wire.command));
    return encode_child_claim_response(wire, reply);
}

struct ChildSignalRoute {
    uint16_t node = WKI_NODE_INVALID;
    WkiChannel* channel = nullptr;
    uint32_t generation = 0;
    uint32_t task_id = 0;
    auto operator==(const ChildSignalRoute&) const -> bool = default;
};

// Source owner is retained by queued work. No event or PID lookup occurs here.
auto capture_child_signal_route_locked(ker::mod::sched::task::Task& target, ChildSignalRoute& route) -> ChildAuthorityAdmission {
    route = {};
    if (target.exit_in_progress || target.has_exited ||
        target.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE)
        return ChildAuthorityAdmission::UNAVAILABLE;
    auto const EXECUTION = target.wki_execution_identity();
    uint32_t const ID = EXECUTION.signal_task_id != 0 ? EXECUTION.signal_task_id : EXECUTION.proxy_task_id;
    if (ID == 0) return ChildAuthorityAdmission::LOCAL;
    auto* row = find_submitted_task(ID);
    if (row == nullptr || row->local_task != &target ||
        !compute_submit_channel_token_matches(row->target_node, row->submit_channel, row->submit_channel_generation))
        return ChildAuthorityAdmission::UNAVAILABLE;
    if (row->response_pending.load(std::memory_order_acquire)) return ChildAuthorityAdmission::AWAITING_ACCEPT;
    if (row->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED) || EXECUTION.signal_task_id != ID ||
        EXECUTION.remote_pid == 0 || (row->exec_handoff && (!row->exec_cutover || row->exec_remote_pid != EXECUTION.remote_pid)))
        return ChildAuthorityAdmission::UNAVAILABLE;
    route = {.node = row->target_node, .channel = row->submit_channel, .generation = row->submit_channel_generation, .task_id = ID};
    return ChildAuthorityAdmission::READY;
}

// true requests a later retry; no sleeping or RX recursion occurs under a lease.
auto forward_child_signal(ker::mod::sched::task::Task& target, uint16_t hops) -> bool {
    if (hops == 0 || hops > WKI_CHILD_SIGNAL_HOPS) return false;
    ChildSignalRoute route{};
    s_compute_lock.lock();
    auto const ADMISSION = capture_child_signal_route_locked(target, route);
    s_compute_lock.unlock();
    if (ADMISSION != ChildAuthorityAdmission::READY)
        return ADMISSION == ChildAuthorityAdmission::AWAITING_ACCEPT || ADMISSION == ChildAuthorityAdmission::LOCAL;
    PeerLifecycleLease lifecycle;
    if (!lifecycle.acquire(route.node)) return false;
    auto const IDENTITY = capture_compute_peer_identity(route.node);
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if (!IDENTITY.valid() || (IDENTITY.policy & REQUIRED) != REQUIRED || (IDENTITY.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) == 0 ||
        wki_resource_generation_snapshot(route.node, ResourceType::COMPUTE, WKI_CHILD_SIGNAL_RESOURCE_ID) == 0)
        return false;
    ChildSignalRoute current{};
    s_compute_lock.lock();
    auto const RECHECK = capture_child_signal_route_locked(target, current);
    s_compute_lock.unlock();
    if (RECHECK == ChildAuthorityAdmission::UNAVAILABLE) return false;
    if (RECHECK != ChildAuthorityAdmission::READY || current != route) return true;
    TaskChildSignal const REQUEST{.task_id = route.task_id, .hops = static_cast<uint16_t>(hops - 1)};
    int const SENT =
        wki_send_on_channel_generation(route.node, route.channel, route.generation, MsgType::TASK_CHILD_SIGNAL, &REQUEST, sizeof(REQUEST));
    // WKI_OK owns a reliable retransmit copy. Never send that logical event
    // again just because the initial physical transmit failed.
    return SENT != WKI_OK && SENT != WKI_ERR_PEER_FENCED && SENT != WKI_ERR_INVALID;
}

auto run_one_child_signal() -> bool {
    uint16_t hops = 0;
    uint64_t const FLAGS = s_child_signal_lock.lock_irqsave();
    auto* target = s_child_signal_queue.pop(hops);
    s_child_signal_lock.unlock_irqrestore(FLAGS);
    if (target == nullptr) return false;
    auto const LOCAL = ker::syscall::process::try_signal_child_event_local(*target);
    bool const RETRY = LOCAL.has_value() ? *LOCAL == -EAGAIN : forward_child_signal(*target, hops);
    if (RETRY) wki_defer_child_signal(*target, hops);
    target->release();
    if (RETRY) ker::mod::sched::kern_sleep_us(1000);
    return true;
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

// The submit worker retains exclusive peer lifecycle admission through the
// actual scheduler call. Check immediately before posting, including ordinary
// spawn and time spent allocating the private monitor.
template <typename Post>
auto post_admitted_remote_process(const ComputeSubmitSessionToken& session, uint32_t task_id, uint64_t deadline_us,
                                  const PeerLifecycleLease& lease, bool handoff_ready, Post&& post) -> bool {
    if (!handoff_ready || !lease.owns(session.node_id)) return false;
    s_compute_lock.lock();
    bool const CURRENT = compute_submit_admission_is_current_locked(session);
    int32_t const CANCEL = take_cancelled_task_submit_locked(session, task_id);
    bool const EXPIRED = deadline_us != 0 && wki_now_us() >= deadline_us;
    s_compute_lock.unlock();
    return CURRENT && CANCEL == 0 && !EXPIRED && post();
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

void release_output_capture(TaskOutputCapture* capture) {
    if (capture != nullptr) capture->release();
}

auto capture_write(ker::vfs::File* file, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (file == nullptr || file->private_data == nullptr || buf == nullptr || count == 0) {
        return 0;
    }
    auto* cap = static_cast<TaskOutputCapture*>(file->private_data);
    cap->append({static_cast<const uint8_t*>(buf), count});
    return static_cast<ssize_t>(count);  // report all bytes "written"
}

auto capture_close(ker::vfs::File* file) -> int {
    if (file == nullptr) return 0;
    auto* capture = static_cast<TaskOutputCapture*>(file->private_data);
    file->private_data = nullptr;
    release_output_capture(capture);
    return 0;
}
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

void close_proxy_fd_table(ker::mod::sched::task::Task* task, bool cloexec_only = false) {
    if (task == nullptr) {
        return;
    }

    for (unsigned fd = 0; fd < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++fd) {
        auto* file = static_cast<ker::vfs::File*>(nullptr);
        // Take the fd-table ownership reference. Retaining here would leave
        // proxy-owned pipe ends open and suppress EOF on the submitter side.
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        if (cloexec_only && !task->get_fd_cloexec(fd)) {
            task->fd_table_lock.unlock_irqrestore(IRQF);
            continue;
        }
        file = static_cast<ker::vfs::File*>(task->fd_table.lookup(fd));
        if (file == nullptr) {
            task->fd_table_lock.unlock_irqrestore(IRQF);
            continue;
        }
        task->clear_fd_cloexec(fd);
        task->fd_table.remove(fd);
        ker::vfs::fd_usage_changed_locked(*task);
        task->fd_table_lock.unlock_irqrestore(IRQF);
        ker::vfs::vfs_put_file(file);
    }
}

void destroy_unpublished_remote_process(ker::mod::sched::task::Task* owner, ker::mod::sched::task::Task* task, TaskOutputCapture* output,
                                        const char* reason, ker::mod::sched::resource::State* failure_observations = nullptr) {
    if (task != nullptr) {
        if (failure_observations != nullptr) {
            uint64_t const FLAGS = task->resource_lock.lock_irqsave();
            *failure_observations = task->resources;
            task->resource_lock.unlock_irqrestore(FLAGS);
        }
        ker::mod::dbg::log("[WKI] Retiring unpublished remote process pid=0x%lx reason=%s", task->pid, reason);
        if (!ker::mod::sched::task::destroy_owned_unpublished_process(owner, task)) [[unlikely]] {
            ker::mod::dbg::panic_handler("WKI remote compute: lost unpublished-process teardown ownership");
            hcf();
        }
    }
    release_output_capture(output);
}

// A failed monitor publication leaves both extra Task refs with the worker.
// Clear the private row, release those refs, then use the existing owner-linked
// teardown while VFS cleanup may block. The child has never been scheduled.
void discard_unpublished_running_task(ker::mod::sched::task::Task* owner, ker::mod::sched::task::Task* worker_ref,
                                      RunningRemoteTask& running, ker::mod::sched::resource::State* failure_observations = nullptr) {
    auto* child = running.task;
    auto* output = running.output;
    running.task = nullptr;
    running.output = nullptr;
    running.active = false;
    if (running.service_accounting != nullptr) {
        running.service_accounting->release();
        running.service_accounting = nullptr;
    }
    worker_ref->release();
    child->release();
    destroy_unpublished_remote_process(owner, child, output, "wki-submit-tracking", failure_observations);
}

// Consume the private monitor and its two extra Task references before
// owner-linked teardown. No peer lease or compute lock may cross final File
// puts, which can wait for responses from the admitted peer.
void discard_unposted_remote_process(ker::mod::sched::task::Task* owner, ker::mod::sched::task::Task* child,
                                     ker::mod::sched::task::Task* worker_ref, ker::mod::sched::task::Task* completion_ref,
                                     TaskOutputCapture* output, PeerLifecycleLease& publication_lease,
                                     ker::mod::sched::resource::State* failure_observations) {
    WkiIpcOutputCompletion* completion = nullptr;
    s_compute_lock.lock();
    for (auto& running : g_running_remote_tasks) {
        if (running.active && !running.published && running.task == completion_ref && running.local_pid == child->pid) {
            running.task = nullptr;
            output = std::exchange(running.output, nullptr);
            completion = std::exchange(running.output_completion, nullptr);
            running.active = false;
            if (running.service_accounting != nullptr) {
                running.service_accounting->release();
                running.service_accounting = nullptr;
            }
            break;
        }
    }
    compact_running_remote_tasks_locked();
    s_compute_lock.unlock();
    publication_lease.release();
    delete completion;
    worker_ref->release();
    completion_ref->release();
    destroy_unpublished_remote_process(owner, child, output, "wki-submit-post", failure_observations);
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
    auto* file = ker::vfs::vfs_get_file_retain(proxy, 1);
    if (file != nullptr) {
        [[maybe_unused]] ssize_t const WRITE_RESULT = ker::vfs::vfs_pwrite_file(file, output_data, output_len, 0);
#ifdef DEBUG_WKI_COMPUTE
        had_stdout = true;
        write_ret = WRITE_RESULT;
#endif
        ker::vfs::vfs_put_file(file);
    }

#ifdef DEBUG_WKI_COMPUTE
    ker::mod::dbg::log("[WKI] Task %u replay output: len=%u had_stdout=%d ret=%ld", task_id, output_len, had_stdout ? 1 : 0, write_ret);
#endif
}

// Caller has won proxy exit; apply all remote counters before publishing its
// immutable child event. Reaped counters are segment deltas, not policy CPU.
auto merge_proxy_resource_completion(ker::mod::sched::task::Task& proxy, const TaskResourceSnapshot& baseline,
                                     const TaskResourceSnapshot& completion) -> bool {
    uint64_t remote_cpu_us = 0;
    bool const SERVICE_REPORTS = (baseline.flags & WKI_RESOURCE_SERVICE_REPORTS) != 0;
    bool merged = false;
    {
        ker::mod::sched::resource::ServiceResourceMerge observation_guard(proxy);
        merged = resource_contract::merge_completion(baseline, completion, proxy.resources, ker::mod::sched::task::Task::FD_TABLE_SIZE,
                                                     remote_cpu_us);
        if (merged && SERVICE_REPORTS) observation_guard.exclude_reported_denials();
    }
    if (!merged) return false;
    using ker::mod::sched::resource::saturating_cpu_add;
    proxy.user_time_us = saturating_cpu_add(proxy.user_time_us, completion.cpu_user_time_us - baseline.cpu_user_time_us);
    proxy.system_time_us = saturating_cpu_add(proxy.system_time_us, completion.cpu_system_time_us - baseline.cpu_system_time_us);
    ker::syscall::process::child_events::add_reaped_cpu_times(proxy, completion.reaped_user_time_us, completion.reaped_system_time_us);
    if (!SERVICE_REPORTS) static_cast<void>(ker::mod::sched::resource::charge_service_cpu(proxy, remote_cpu_us));
    return true;
}

template <typename Enqueue>
void prepare_proxy_finalization(ker::mod::sched::task::Task* proxy, int32_t exit_status, const uint8_t* output_data, uint16_t output_len,
                                uint32_t task_id, const TaskResourceSnapshot* baseline, const TaskResourceSnapshot* completion,
                                Enqueue&& enqueue) {
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

    proxy->exit_in_progress.store(true, std::memory_order_release);

    if (baseline != nullptr && completion != nullptr && !merge_proxy_resource_completion(*proxy, *baseline, *completion)) {
        exit_status = -1;
    }

    int32_t const WAIT_STATUS = encode_remote_wait_status(exit_status);

    write_proxy_output(proxy, output_data, output_len, task_id);

    proxy->exit_status = WAIT_STATUS;
    proxy->set_wki_proxy_task_id(0);

    cleanup_proxy_resources(proxy);
    // The winner's output/resources are stable. Retirement can forward child
    // events, so leave it to local worker context outside every peer lease.
    enqueue(*proxy);
}

void finalize_proxy_task(ker::mod::sched::task::Task* proxy, int32_t exit_status, const uint8_t* output_data, uint16_t output_len,
                         uint32_t task_id, const TaskResourceSnapshot* baseline = nullptr,
                         const TaskResourceSnapshot* completion = nullptr) {
    prepare_proxy_finalization(proxy, exit_status, output_data, output_len, task_id, baseline, completion, enqueue_proxy_exit);
}

template <typename Publish>
void finish_proxy_exit(ker::mod::sched::task::Task& task, Publish&& publish) {
    auto* proxy = &task;
    if (ker::mod::sched::compartment::begin_init_retirement(task) != 0) [[unlikely]] {
        ker::mod::dbg::panic_handler("private proxy init: failed to retain retirement owners");
        hcf();
    }
    retire_child_queries_for_task(task);
    retire_child_wait_transactions_for_task(task);
    ker::syscall::process::child_events::prepare_process_exit(*proxy);
    proxy->process_charge.release();
    proxy->has_exited = true;
    proxy->exit_notify_ready.store(true, std::memory_order_release);

    // The proxy is now stable: output replay and all potentially blocking
    // resource cleanup completed, and the encoded status/rusage snapshot can
    // no longer change. Publish one immutable process child event before dead
    // list insertion; child-event waiter registration owns the publish/park
    // race and the scheduler only provides generic wakeup.
    proxy->death_epoch.store(ker::mod::sched::EpochManager::current_epoch(), std::memory_order_release);
    proxy->state.store(ker::mod::sched::task::TaskState::DEAD, std::memory_order_release);
    publish(*proxy);
}

auto run_one_proxy_exit() -> bool {
    uint64_t const FLAGS = s_proxy_exit_lock.lock_irqsave();
    auto* proxy = s_proxy_exit_queue.pop();
    s_proxy_exit_lock.unlock_irqrestore(FLAGS);
    if (proxy == nullptr) return false;
    finish_proxy_exit(*proxy, [](ker::mod::sched::task::Task& task) {
        if (!ker::syscall::process::child_events::publish_exit(task)) {
            ker::mod::dbg::panic_handler("WKI remote compute: failed to publish proxy child exit event");
            hcf();
        }
        ker::mod::sched::insert_into_dead_list(&task);
    });
    proxy->release();
    return true;
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
constexpr uint16_t WKI_TASK_RESOURCE_IDENTITY_SIZE = sizeof(WkiTaskIdentityContext) + sizeof(TaskResourceSnapshot);
constexpr uint16_t WKI_TASK_FULL_IDENTITY_SIZE =
    WKI_TASK_RESOURCE_IDENTITY_SIZE + ker::mod::security::credential_record::SIZE + sizeof(TaskJobOrigins) + sizeof(TaskExecSignals);
static_assert(ker::mod::security::credential_record::SIZE == TaskSignalRequest{}.credentials.size());
constexpr uint16_t WKI_TASK_ROUTED_IDENTITY_SIZE = WKI_TASK_FULL_IDENTITY_SIZE + sizeof(TaskSignalHome);
constexpr uint16_t WKI_TASK_FILESYSTEM_IDENTITY_SIZE = WKI_TASK_ROUTED_IDENTITY_SIZE + sizeof(TaskFilesystemContext);
auto has_routed_task_identity(uint16_t length) -> bool {
    return length == WKI_TASK_ROUTED_IDENTITY_SIZE || length == WKI_TASK_FILESYSTEM_IDENTITY_SIZE;
}
auto has_full_task_identity(uint16_t length) -> bool { return length == WKI_TASK_FULL_IDENTITY_SIZE || has_routed_task_identity(length); }
auto valid_submit_filesystem_identity(const TaskSubmitPayload& submit) -> bool {
    bool const PRESENT = (submit.reserved & WKI_TASK_SUBMIT_FILESYSTEM) != 0;
    return PRESENT == (submit.identity_len == WKI_TASK_FILESYSTEM_IDENTITY_SIZE) &&
           (!PRESENT || ((submit.reserved & WKI_TASK_SUBMIT_GROUP_HOME) != 0 &&
                         (submit.reserved & ~(WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION | WKI_TASK_SUBMIT_GROUP_HOME |
                                              WKI_TASK_SUBMIT_FILESYSTEM)) == 0));
}

auto valid_session_origin(const TaskSessionOrigin& origin) -> bool {
    return origin.reserved == 0 && ((origin.node == 0 && origin.boot == 0) || (origin.node != 0 && origin.boot != 0));
}

auto resolve_session_origin(const TaskSessionOrigin& origin, uint16_t source, uint32_t source_boot) -> ker::mod::sched::session::Origin {
    ker::mod::sched::session::Origin result{.node = origin.node == 0 ? source : origin.node,
                                            .boot = origin.node == 0 ? source_boot : origin.boot};
    if (result.node == g_wki.my_node_id && result.boot == g_wki.local_boot_epoch) result = {};
    return result;
}

auto has_resource_identity(uint16_t length) -> bool { return length == WKI_TASK_RESOURCE_IDENTITY_SIZE || has_full_task_identity(length); }

auto valid_task_identity_len(uint16_t identity_len) -> bool {
    return identity_len == 0 || identity_len == WkiTaskIdentityContext::V1_SIZE || identity_len == sizeof(WkiTaskIdentityContext) ||
           has_resource_identity(identity_len);
}

// Locate the policy without fetching/allocating executable bytes. Both forms
// of ELF construction must see validated limits before acquiring resources.
auto locate_submit_context(const TaskSubmitPayload& submit, const uint8_t* data, uint16_t length, const uint8_t*& context,
                           uint16_t& context_len) -> bool {
    size_t offset = 0;
    size_t source_length = 0;
    switch (static_cast<TaskDeliveryMode>(submit.delivery_mode)) {
        case TaskDeliveryMode::INLINE: {
            if (length < sizeof(uint32_t)) return false;
            uint32_t bytes = 0;
            std::memcpy(&bytes, data, sizeof(bytes));
            offset = sizeof(bytes);
            source_length = bytes;
            break;
        }
        case TaskDeliveryMode::VFS_REF:
        case TaskDeliveryMode::RESOURCE_REF: {
            offset = static_cast<TaskDeliveryMode>(submit.delivery_mode) == TaskDeliveryMode::VFS_REF ? 0 : 6;
            if (length < offset + sizeof(uint16_t)) return false;
            uint16_t bytes = 0;
            std::memcpy(&bytes, data + offset, sizeof(bytes));
            offset += sizeof(bytes);
            if (bytes >= 512) return false;
            source_length = bytes;
            break;
        }
        default:
            return false;
    }
    if (source_length == 0 || source_length > length - offset) return false;
    offset += source_length;
    context = data + offset;
    context_len = static_cast<uint16_t>(length - offset);
    return true;
}

auto resource_submission_admission(bool negotiated, uint16_t identity_length, bool full_credentials = false, bool routed_home = false,
                                   bool filesystem = false) -> TaskRejectReason {
    if (filesystem && (!routed_home || !full_credentials)) return TaskRejectReason::RESOURCE_UNSUPPORTED;
    uint16_t const EXPECTED = filesystem         ? WKI_TASK_FILESYSTEM_IDENTITY_SIZE
                              : routed_home      ? WKI_TASK_ROUTED_IDENTITY_SIZE
                              : full_credentials ? WKI_TASK_FULL_IDENTITY_SIZE
                                                 : WKI_TASK_RESOURCE_IDENTITY_SIZE;
    if (routed_home && !full_credentials) return TaskRejectReason::RESOURCE_UNSUPPORTED;
    return negotiated && identity_length == EXPECTED ? TaskRejectReason::ACCEPTED : TaskRejectReason::RESOURCE_UNSUPPORTED;
}

auto prepare_submitted_resources(const TaskSubmitPayload* submit, const uint8_t* var_data, uint16_t var_len, const uint8_t*& submit_context,
                                 uint16_t& submit_context_len, WkiTaskIdentityContext& submitted_identity, bool& has_submitted_identity,
                                 ker::mod::sched::resource::State& submitted_resources, uint8_t* stdio_mask = nullptr,
                                 bool* service_reports = nullptr, ker::mod::security::Snapshot* full_credentials = nullptr,
                                 TaskJobOrigins* job_origins = nullptr, TaskExecSignals* exec_signals = nullptr,
                                 TaskSignalHome* signal_home = nullptr, TaskFilesystemContext* filesystem_output = nullptr,
                                 TaskChildCpuObservation* child_observations = nullptr) -> TaskRejectReason {
    if (service_reports != nullptr) *service_reports = false;
    if (stdio_mask != nullptr) *stdio_mask = WKI_RESOURCE_STDIO_ALL;
    TaskRejectReason resource_reject = TaskRejectReason::ACCEPTED;
    TaskFilesystemContext filesystem_candidate{};
    TaskChildCpuObservation child_observations_candidate{};
    bool const HAS_FILESYSTEM = (submit->reserved & WKI_TASK_SUBMIT_FILESYSTEM) != 0;
    if (!locate_submit_context(*submit, var_data, var_len, submit_context, submit_context_len) || submit->ipc_fd_count > 16 ||
        !valid_task_identity_len(submit->identity_len) || !valid_submit_filesystem_identity(*submit)) {
        resource_reject = TaskRejectReason::RESOURCE_INVALID;
    } else {
        size_t const IPC_BYTES = static_cast<size_t>(submit->ipc_fd_count) * sizeof(WkiIpcFdEntry);
        size_t const IDENTITY_END = static_cast<size_t>(submit->args_len) + submit->identity_len;
        if (IPC_BYTES > submit_context_len || IDENTITY_END > submit_context_len - IPC_BYTES) {
            resource_reject = TaskRejectReason::RESOURCE_INVALID;
        } else {
            if (submit->identity_len != 0) {
                size_t const IDENTITY_BYTES = std::min<size_t>(submit->identity_len, sizeof(submitted_identity));
                std::memcpy(&submitted_identity, submit_context + submit->args_len, IDENTITY_BYTES);
                has_submitted_identity = true;
            }
            if (has_full_task_identity(submit->identity_len)) {
                ker::mod::security::Snapshot decoded;
                auto const* bytes = submit_context + submit->args_len + WKI_TASK_RESOURCE_IDENTITY_SIZE;
                int const ERROR =
                    ker::mod::security::credential_record::decode({bytes, ker::mod::security::credential_record::SIZE}, decoded);
                if (ERROR != 0) return ERROR == -ENOTSUP ? TaskRejectReason::RESOURCE_UNSUPPORTED : TaskRejectReason::RESOURCE_INVALID;
                TaskJobOrigins origins{};
                std::memcpy(&origins, bytes + ker::mod::security::credential_record::SIZE, sizeof(origins));
                if (!valid_session_origin(origins.session) || !valid_session_origin(origins.group))
                    return TaskRejectReason::RESOURCE_INVALID;
                TaskExecSignals signals{};
                std::memcpy(&signals, bytes + ker::mod::security::credential_record::SIZE + sizeof(origins), sizeof(signals));
                if (signals.version != WKI_EXEC_SIGNALS_VERSION) return TaskRejectReason::RESOURCE_UNSUPPORTED;
                if (!valid_exec_signals(signals)) return TaskRejectReason::RESOURCE_INVALID;
                if (job_origins != nullptr) *job_origins = origins;
                if (full_credentials != nullptr) *full_credentials = decoded;
                if (exec_signals != nullptr) *exec_signals = signals;
                if (has_routed_task_identity(submit->identity_len)) {
                    TaskSignalHome home{};
                    std::memcpy(&home, submit_context + submit->args_len + WKI_TASK_FULL_IDENTITY_SIZE, sizeof(home));
                    if (!valid_session_origin(home.origin) || home.origin.node == WKI_NODE_BROADCAST || home.serial == 0)
                        return TaskRejectReason::RESOURCE_INVALID;
                    if (signal_home != nullptr) *signal_home = home;
                }
            }
            if (HAS_FILESYSTEM) {
                std::memcpy(&filesystem_candidate, submit_context + submit->args_len + WKI_TASK_ROUTED_IDENTITY_SIZE,
                            sizeof(filesystem_candidate));
                if (!wki_task_filesystem_valid(filesystem_candidate)) return TaskRejectReason::RESOURCE_INVALID;
                if ((filesystem_candidate.flags & WKI_TASK_FILESYSTEM_ROOT_PATH) != 0) {
                    if (!wki_task_legacy_root_path_valid(submitted_identity.root)) return TaskRejectReason::RESOURCE_INVALID;
                } else if ((filesystem_candidate.flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) == 0 &&
                           submitted_identity.root != ker::mod::sched::task::Task::PathBuffer{'/', '\0'})
                    return TaskRejectReason::RESOURCE_INVALID;
                if ((filesystem_candidate.flags & WKI_TASK_FILESYSTEM_CWD_GRANT) == 0 && submit->cwd_len != 0 &&
                    (submit->cwd_len != 2 || submit->args_len < 2 || submit_context[submit->args_len - 2] != '/' ||
                     submit_context[submit->args_len - 1] != 0))
                    return TaskRejectReason::RESOURCE_INVALID;
                if (filesystem_output == nullptr) return TaskRejectReason::RESOURCE_UNSUPPORTED;
            }
            if (has_resource_identity(submit->identity_len)) {
                TaskResourceSnapshot record;
                auto const* bytes = submit_context + submit->args_len + sizeof(WkiTaskIdentityContext);
                if (!resource_contract::decode(bytes, sizeof(record), record, ker::mod::sched::task::Task::FD_TABLE_SIZE) ||
                    ((record.flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0 &&
                     ((submit->reserved & WKI_TASK_SUBMIT_EXEC_HANDOFF) == 0 || child_observations == nullptr)) ||
                    !resource_contract::restore(record, submitted_resources, ker::mod::sched::task::Task::FD_TABLE_SIZE,
                                                &child_observations_candidate))
                    resource_reject = TaskRejectReason::RESOURCE_INVALID;
                else if (resource_contract::cpu_exhausted(record))
                    resource_reject = TaskRejectReason::RESOURCE_CPU;
                else {
                    if (stdio_mask != nullptr)
                        *stdio_mask = static_cast<uint8_t>((record.flags & WKI_RESOURCE_STDIO_FLAGS) >> WKI_RESOURCE_STDIO_SHIFT);
                    if (service_reports != nullptr) *service_reports = (record.flags & WKI_RESOURCE_SERVICE_REPORTS) != 0;
                }
            }
        }
    }
    if (resource_reject == TaskRejectReason::ACCEPTED && stdio_mask != nullptr) {
        uint8_t fallback_mask = *stdio_mask;
        size_t const IPC_BYTES = static_cast<size_t>(submit->ipc_fd_count) * sizeof(WkiIpcFdEntry);
        // Optional VFS policy occupies the gap after identity. IPC is always
        // the validated tail, just as in receiver attachment below.
        const auto* ipc_data = submit_context + submit_context_len - IPC_BYTES;
        for (unsigned index = 0; index < submit->ipc_fd_count; ++index) {
            WkiIpcFdEntry entry;
            std::memcpy(&entry, ipc_data + index * sizeof(entry), sizeof(entry));
            if (entry.local_fd >= 3) continue;
            uint8_t const BIT = static_cast<uint8_t>(1U << entry.local_fd);
            if (has_resource_identity(submit->identity_len) && (*stdio_mask & BIT) == 0) return TaskRejectReason::RESOURCE_INVALID;
            fallback_mask &= static_cast<uint8_t>(~BIT);
        }
        *stdio_mask = fallback_mask;
    }
    if (resource_reject == TaskRejectReason::ACCEPTED && HAS_FILESYSTEM) *filesystem_output = filesystem_candidate;
    if (resource_reject == TaskRejectReason::ACCEPTED && child_observations != nullptr) *child_observations = child_observations_candidate;
    return resource_reject;
}

struct SubmitFilesystemContext {
    ker::mod::sched::task::Task::PathBuffer root{};
    const char* cwd = nullptr;
};

auto capture_submit_filesystem(const TaskFilesystemPreparation& preparation, SubmitFilesystemContext& output) -> bool {
    if (preparation.status() != 0) return false;
    auto const& filesystem = preparation.filesystem();
    // Exact object roots still require negotiated owner-grant serialization.
    if (filesystem.root_anchor) return false;
    SubmitFilesystemContext candidate;
    candidate.root = filesystem.root;
    candidate.cwd = preparation.cwd();
    output = candidate;
    return true;
}

struct FilesystemSubmitAdmission {
    SubmitterChannelToken channel{};
    ComputePeerIdentity identity{};
    uint64_t resource_generation = 0;
    uint32_t resource_id = 0;
};

auto filesystem_submit_admission_current(uint16_t target, const FilesystemSubmitAdmission& admitted) -> bool {
    PeerLifecycleLease lifecycle;
    if (!lifecycle.acquire(target)) return false;
    auto const CHANNEL = capture_submitter_channel(target);
    return admitted.identity.valid() && admitted.identity == capture_compute_peer_identity(target) &&
           CHANNEL.channel == admitted.channel.channel && CHANNEL.generation == admitted.channel.generation &&
           admitted.resource_generation != 0 &&
           admitted.resource_generation == wki_resource_generation_snapshot(target, ResourceType::COMPUTE, admitted.resource_id);
}

auto prepare_return_owner_cwd(TaskFilesystemPreparation& preparation, uint16_t target, FilesystemSubmitAdmission& output) -> int {
    auto* directory = ker::vfs::vfs_captured_remote_cwd(preparation.filesystem());
    if (directory == nullptr) return ker::vfs::vfs_validate_text_filesystem_submission(preparation.filesystem());
    // Legacy roots retain their explicit pathname/routing contract. Owned
    // roots and third-owner cwd need their own negotiated grant adapters.
    auto const& filesystem = preparation.filesystem();
    if (filesystem.root_anchor) return -ENOTSUP;
    bool const ROOT_PATH = std::strcmp(filesystem.root.data(), "/") != 0;
    if (ROOT_PATH && !wki_task_legacy_root_path_valid(filesystem.root, false)) return -ENOTSUP;
    if (wki_remote_vfs_directory_owner(directory) != target) return -ENOTSUP;
    FilesystemSubmitAdmission admitted;
    admitted.resource_id = ROOT_PATH ? WKI_ROOTED_RETURN_OWNER_CWD_RESOURCE_ID : WKI_RETURN_OWNER_CWD_RESOURCE_ID;
    {
        PeerLifecycleLease lifecycle;
        if (!lifecycle.acquire(target)) return -EHOSTUNREACH;
        admitted.resource_generation = wki_resource_generation_snapshot(target, ResourceType::COMPUTE, admitted.resource_id);
        if (admitted.resource_generation == 0) return -ENOTSUP;
        admitted.identity = capture_compute_peer_identity(target);
        admitted.channel = capture_submitter_channel(target);
        if (!admitted.identity.valid() || admitted.channel.channel == nullptr) return -EHOSTUNREACH;
        constexpr auto REQUIRED = WKI_CAP_RESOURCE_LIMITS | WKI_CAP_COMPUTE_CREDENTIALS;
        if ((admitted.identity.capabilities & REQUIRED) != REQUIRED) return -ENOTSUP;
    }
    int status = preparation.bind_target(target, admitted.identity.boot_epoch);
    if (status != 0) return status;
    ker::vfs::File* staging = nullptr;
    VfsDirectoryGrant grant{};
    // The captured cwd owns directory throughout the wait. The grant RPC has
    // its own executor-visible preparation; no lifecycle writer blocks RX.
    status = wki_remote_vfs_issue_directory_grant(directory, preparation.credentials(), target, admitted.identity.boot_epoch,
                                                  preparation.transaction(), {}, &staging, grant);
    if (status != 0) return status;
    status = preparation.adopt_staging(1, staging, grant);
    if (staging != nullptr) ker::vfs::vfs_put_file(staging);
    if (status != 0) return status;
    if (grant.owner_node != target || !filesystem_submit_admission_current(target, admitted)) return -ESTALE;
    status = preparation.finalize_wire(WKI_TASK_FILESYSTEM_CWD_GRANT | (ROOT_PATH ? WKI_TASK_FILESYSTEM_ROOT_PATH : 0));
    if (status == 0) output = admitted;
    return status;
}

// Produces private ownership only; the caller revalidates peer admission before
// publishing it. The executor-linked holder owns every completed preparation.
auto prepare_received_root_path_cwd(TaskFilesystemPreparation& owner, ker::mod::sched::task::Task& task,
                                    const ker::mod::security::Snapshot& credentials, const ker::mod::sched::task::Task::PathBuffer& root)
    -> int {
    if (owner.status() != 0) return owner.status();
    if (!wki_task_legacy_root_path_valid(root)) return -EINVAL;
    int status = ker::vfs::vfs_prepare_root_directory_view_as(task, credentials, root.data(), owner.received_root_view());
    if (status != 0) return status;
    if (owner.status() != 0) return owner.status();
    status =
        ker::vfs::vfs_prepare_filesystem_context(credentials, owner.received_root_view(), owner.received_view(), owner.received_prepared());
    if (status != 0) return status;
    if (owner.status() != 0) return owner.status();
    if (owner.received_prepared().filesystem().root != root) return -ESTALE;
    owner.received_filesystem() = owner.received_prepared().filesystem();
    // Retain the native root in preparation across loader waits, while the Task
    // keeps its declared legacy root and absolute-path routing behavior.
    owner.received_filesystem().root_anchor = {};
    return 0;
}

// Only captured or VFS-prepared contexts reach this private publication helper.
// All ownership is swapped under one lock and retired by the caller afterwards.
auto swap_task_filesystem(ker::mod::sched::task::Task& task, ker::mod::sched::task::Task::FilesystemContext& context) -> bool {
    if (task.filesystem_owner() != &task) return false;
    uint64_t const FLAGS = task.path_metadata_lock.lock_irqsave();
    task.root_anchor.swap(context.root_anchor);
    task.cwd_anchor.swap(context.cwd_anchor);
    std::swap(task.root, context.root);
    std::swap(task.cwd, context.cwd);
    std::swap(task.root_len, context.root_len);
    std::swap(task.cwd_len, context.cwd_len);
    task.path_metadata_lock.unlock_irqrestore(FLAGS);
    return true;
}

void fill_task_identity_context(const ker::mod::sched::task::Task* task, WkiTaskIdentityContext* identity,
                                const ker::mod::security::Snapshot& credentials, const ker::mod::sched::session::Identity& job,
                                const ker::mod::sched::task::Task::PathBuffer& root) {
    if (task == nullptr || identity == nullptr) {
        return;
    }

    identity->uid = credentials.user().real;
    identity->gid = credentials.group().real;
    identity->euid = credentials.user().effective;
    identity->egid = credentials.group().effective;
    identity->suid = credentials.user().saved;
    identity->sgid = credentials.group().saved;
    identity->umask = task->creation_mask();
    identity->session_id = job.session_id;
    identity->pgid = job.pgid;
    identity->controlling_tty = static_cast<int32_t>(job.terminal.index());
    uint64_t const PATH_FLAGS = task->path_metadata_lock.lock_irqsave();
    if (task->wki_submitter_hostname.front() != '\0') {
        std::strncpy(identity->submitter_hostname.data(), task->wki_submitter_hostname.data(), identity->submitter_hostname.size() - 1);
        identity->submitter_hostname.back() = '\0';
    }
    task->path_metadata_lock.unlock_irqrestore(PATH_FLAGS);
    std::strncpy(identity->root.data(), root.data(), identity->root.size() - 1);
    identity->root.back() = '\0';
}

auto prepare_submitted_identity(const WkiTaskIdentityContext& identity, uint64_t policy, ker::mod::security::Snapshot& mapped,
                                const ker::mod::security::Snapshot* full_credentials = nullptr) -> bool {
    if (std::find(identity.root.begin(), identity.root.end(), '\0') == identity.root.end() ||
        std::find(identity.submitter_hostname.begin(), identity.submitter_hostname.end(), '\0') == identity.submitter_hostname.end())
        return false;
    ker::mod::security::Snapshot supplied;
    if (full_credentials != nullptr) {
        supplied = *full_credentials;
        if (supplied.user() != ker::mod::security::Ids{identity.uid, identity.euid, identity.suid} ||
            supplied.group() != ker::mod::security::Ids{identity.gid, identity.egid, identity.sgid})
            return false;
    } else if (ker::mod::security::restore_legacy_remote_exec_identity({identity.uid, identity.euid, identity.suid},
                                                                       {identity.gid, identity.egid, identity.sgid}, supplied) != 0) {
        return false;
    }
    return auth_policy::map_compute_identity(policy, supplied, mapped) == 0;
}

auto decode_exec_credential_transition(const uint8_t* data, uint16_t& length, ker::mod::security::ExecIdentity& image) -> bool {
    if (length < sizeof(TaskExecCredentialTransition)) return false;
    TaskExecCredentialTransition wire{};
    std::memcpy(&wire, data + length - sizeof(wire), sizeof(wire));
    if (wire.version != WKI_EXEC_CREDENTIAL_VERSION || (wire.flags & ~(WKI_EXEC_CREDENTIAL_NOSUID | WKI_EXEC_CREDENTIAL_TRACED)) != 0 ||
        (wire.mode & ~07777U) != 0 || wire.uid == ker::mod::security::UNCHANGED_ID || wire.gid == ker::mod::security::UNCHANGED_ID)
        return false;
    image = {.uid = wire.uid,
             .gid = wire.gid,
             .set_uid = (wire.mode & 04000) != 0,
             .set_gid = (wire.mode & 02000) != 0,
             .nosuid = (wire.flags & WKI_EXEC_CREDENTIAL_NOSUID) != 0,
             .traced = (wire.flags & WKI_EXEC_CREDENTIAL_TRACED) != 0,
             .mode = wire.mode};
    length = static_cast<uint16_t>(length - sizeof(wire));
    return true;
}

auto prepare_spawn_read_credentials(const ker::mod::security::Snapshot& original, const ker::mod::security::Snapshot& final,
                                    const TaskExecCredentialTransition& wire, ker::mod::security::Snapshot& output) -> int {
    uint16_t length = sizeof(wire);
    ker::mod::security::ExecIdentity image;
    if (!decode_exec_credential_transition(reinterpret_cast<const uint8_t*>(&wire), length, image)) return -EINVAL;
    ker::mod::security::Snapshot prepared;
    if (original.prepare_exec(image, prepared) != 0 || prepared != final) return -EAGAIN;
    output = original;
    return 0;
}

auto select_submit_read_credentials(const ker::mod::security::Snapshot& current, const WkiRemoteSpawnSpec* transfer,
                                    ker::mod::security::Snapshot& output) -> int {
    if (transfer != nullptr) {
        if (transfer->exec_credentials == nullptr) return -EINVAL;
        if (transfer->cutover == nullptr) {
            if (transfer->image_credentials == nullptr) return -EINVAL;
            return prepare_spawn_read_credentials(*transfer->image_credentials, current, *transfer->exec_credentials, output);
        }
        if (transfer->image_credentials != nullptr) return -EINVAL;
    }
    output = current;
    return 0;
}

auto decode_submit_credential_transition(const TaskSubmitPayload& submit, const uint8_t* data, uint16_t& length,
                                         ker::mod::security::ExecIdentity& image) -> bool {
    auto const FLAGS = submit.reserved & (WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION);
    if (FLAGS == 0) return true;
    if (FLAGS == (WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION) || (submit.reserved & WKI_TASK_SUBMIT_GROUP_HOME) == 0 ||
        !has_full_task_identity(submit.identity_len))
        return false;
    return decode_exec_credential_transition(data, length, image);
}

auto prepare_submitted_exec_credentials(const ker::mod::security::Snapshot& original, const ker::mod::security::ExecIdentity& image,
                                        uint64_t policy, ker::mod::security::Snapshot& mapped) -> bool {
    ker::mod::security::Snapshot replacement;
    return original.prepare_exec(image, replacement) == 0 && auth_policy::map_compute_identity(policy, replacement, mapped) == 0;
}

auto apply_submitted_task_identity(ker::mod::sched::task::Task* task, const WkiTaskIdentityContext& identity,
                                   const ker::mod::security::Snapshot& mapped, ker::mod::sched::session::Origin origin,
                                   ker::mod::sched::session::Origin group_origin) -> bool {
    if (task == nullptr || task->credentials() != mapped || task->root_anchor) return false;

    (void)task->exchange_creation_mask(identity.umask);
    task->initialize_job_identity({.session_id = identity.session_id != 0 ? identity.session_id : task->pid,
                                   .pgid = identity.pgid != 0 ? identity.pgid : task->pid,
                                   .exec_completed = true,
                                   .origin = identity.session_id != 0 ? origin : ker::mod::sched::session::Origin{},
                                   .group_origin = identity.pgid != 0 ? group_origin : ker::mod::sched::session::Origin{}});
    // PTY indexes are node-local. Remote tasks use exported PTY fds rather
    // than a receiver-local controlling tty, so detach instead of accidentally
    // binding /dev/tty to a same-numbered PTY on the receiver.
    task->set_controlling_terminal({});
    if (identity.root.front() == '/') {
        std::strncpy(task->root.data(), identity.root.data(), task->root.size() - 1);
        task->root.back() = '\0';
        task->root_len = static_cast<uint16_t>(std::strlen(task->root.data()));
    }
    if (identity.submitter_hostname.front() != '\0') {
        std::strncpy(task->wki_submitter_hostname.data(), identity.submitter_hostname.data(), task->wki_submitter_hostname.size() - 1);
        task->wki_submitter_hostname.back() = '\0';
    }
    return true;
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
    TaskFilesystemPreparation* filesystem_owner = nullptr;

    ScopedSubmitVfsIdentity(ker::mod::sched::task::Task* current_task, const WkiTaskIdentityContext* identity,
                            const char* submitter_hostname, const uint8_t* policy_data, uint16_t policy_len,
                            TaskFilesystemPreparation* received_owner = nullptr)
        : task(current_task) {
        if (task == nullptr) {
            return;
        }

        // This compatibility scope installs path text only. Preserve an owned
        // root and all worker state until an exact-root grant adapter exists.
        auto const* owner = task->filesystem_owner();
        if (owner == nullptr) return;
        uint64_t const ROOT_FLAGS = owner->path_metadata_lock.lock_irqsave();
        bool const OWNED_ROOT = static_cast<bool>(owner->root_anchor);
        owner->path_metadata_lock.unlock_irqrestore(ROOT_FLAGS);
        if (OWNED_ROOT) return;

        if (received_owner != nullptr) {
            auto const& received = received_owner->received_filesystem();
            if (owner != task || received_owner->status() != 0 || received.root_anchor || !received.cwd_anchor || identity == nullptr ||
                received.root != identity->root ||
                (received.root != ker::mod::sched::task::Task::PathBuffer{'/', '\0'} && !wki_task_legacy_root_path_valid(received.root)))
                return;
            filesystem_owner = received_owner;
        }

        saved_vfs_rules = std::move(task->wki_vfs_rules);
        vfs_rules_installed = true;

        {
            uint64_t const IRQF = task->path_metadata_lock.lock_irqsave();
            saved_root = task->root;
            saved_root_len = task->root_len;
            saved_submitter = task->wki_submitter_hostname;
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

            task->path_metadata_lock.unlock_irqrestore(IRQF);
        }

        active = true;
        if (filesystem_owner) {
            auto installed = filesystem_owner->received_filesystem();
            if (!swap_task_filesystem(*task, installed)) return;
        }
        submitted_policy_valid = deserialize_task_vfs_rules(task, policy_data, policy_len);
    }

    [[nodiscard]] auto policy_valid() const -> bool { return submitted_policy_valid; }

    [[nodiscard]] auto install_filesystem_on(ker::mod::sched::task::Task& submitted_task) -> bool {
        if (!active || !submitted_policy_valid) return false;
        if (!filesystem_owner) return true;
        auto installed = filesystem_owner->received_filesystem();
        return swap_task_filesystem(submitted_task, installed);
    }

    [[nodiscard]] auto transfer_vfs_rules_to(ker::mod::sched::task::Task* submitted_task) -> bool {
        if (!active || !submitted_policy_valid || !vfs_rules_installed || task == nullptr || submitted_task == nullptr) {
            return false;
        }

        submitted_task->wki_vfs_rules = std::move(task->wki_vfs_rules);
        task->wki_vfs_rules = std::move(saved_vfs_rules);
        vfs_rules_installed = false;
        return install_receiver_local_system_rules(submitted_task);
    }

    ~ScopedSubmitVfsIdentity() {
        if (!active || task == nullptr) {
            return;
        }
        if (vfs_rules_installed) {
            task->wki_vfs_rules = std::move(saved_vfs_rules);
        }
        uint64_t const IRQF = task->path_metadata_lock.lock_irqsave();
        task->root = saved_root;
        task->root_len = saved_root_len;
        task->wki_submitter_hostname = saved_submitter;
        task->path_metadata_lock.unlock_irqrestore(IRQF);
        if (filesystem_owner && filesystem_owner->status() == 0) {
            auto restored = filesystem_owner->filesystem();
            static_cast<void>(swap_task_filesystem(*task, restored));
        }
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
                               SubmitContextInfo* info, bool resource_contract = false, bool full_credentials = false,
                               bool routed_home = false, const TaskFilesystemContext* filesystem = nullptr) -> bool {
    if (filesystem != nullptr &&
        (task == nullptr || !resource_contract || !full_credentials || !routed_home || !wki_task_filesystem_valid(*filesystem)))
        return false;
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
    if ((full_credentials && !resource_contract) || (routed_home && !full_credentials)) return false;
    uint16_t const RESOURCE_IDENTITY_SIZE = filesystem != nullptr ? WKI_TASK_FILESYSTEM_IDENTITY_SIZE
                                            : routed_home         ? WKI_TASK_ROUTED_IDENTITY_SIZE
                                            : full_credentials    ? WKI_TASK_FULL_IDENTITY_SIZE
                                                                  : WKI_TASK_RESOURCE_IDENTITY_SIZE;
    info->identity_len =
        task != nullptr ? (resource_contract ? RESOURCE_IDENTITY_SIZE : static_cast<uint16_t>(sizeof(WkiTaskIdentityContext))) : 0;
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

auto snapshot_task_resources(ker::mod::sched::task::Task* task, TaskResourceSnapshot& record, bool inherit_children = false) -> bool {
    ker::mod::sched::resource::Owner owner(task);
    if (owner.get() == nullptr) return false;
    uint64_t const FLAGS = owner.get()->resource_lock.lock_irqsave();
    record = resource_contract::snapshot(owner.get()->resources);
    owner.get()->resource_lock.unlock_irqrestore(FLAGS);
    if (inherit_children) {
        auto const CHILDREN = child_events::read_reaped_cpu_times(*owner.get());
        if (CHILDREN.user_time_us != 0 || CHILDREN.system_time_us != 0) {
            record.flags |= WKI_RESOURCE_CHILD_OBSERVATIONS;
            record.reaped_user_time_us = CHILDREN.user_time_us;
            record.reaped_system_time_us = CHILDREN.system_time_us;
        }
    }
    uint64_t const FD_FLAGS = task->fd_table_lock.lock_irqsave();
    for (unsigned fd = 0; fd < 3; ++fd) {
        if (task->fd_table.lookup(fd) != nullptr && !task->get_fd_cloexec(fd))
            record.flags |= static_cast<uint16_t>(1U << (WKI_RESOURCE_STDIO_SHIFT + fd));
    }
    task->fd_table_lock.unlock_irqrestore(FD_FLAGS);
    return true;
}

auto snapshot_exec_signals(const ker::mod::sched::task::Task& task) -> TaskExecSignals {
    TaskExecSignals state;
    auto const HANDLERS = task.signal_handlers_snapshot();
    for (size_t i = 0; i < HANDLERS.size(); ++i)
        if (HANDLERS.at(i).handler == 1) state.ignored |= uint64_t{1} << i;
    state.ignored &= ~WKI_EXEC_UNBLOCKABLE_SIGNALS;
    state.blocked = task.signal_mask_bits() & ~WKI_EXEC_UNBLOCKABLE_SIGNALS;
    state.pending = task.signal_delivery_pending_bits();
    state.parent_death_signal = task.parent_death_signal.load(std::memory_order_acquire);
    // Preparation is observational: rejection must leave the old image's
    // pending state intact. Arrivals after this capture still require the
    // independent exec signal-routing handoff, not a scheduler proxy flag.
    return state;
}

void install_exec_signals(ker::mod::sched::task::Task& task, const TaskExecSignals& state) {
    // Only an unpublished receiver. No old handler/restorer/stack pointer
    // crosses nodes, and pending storage does not replay generation/fanout.
    ker::mod::sched::task::Task::SignalHandlerTable handlers{};
    for (size_t i = 0; i < handlers.size(); ++i)
        if ((state.ignored & (uint64_t{1} << i)) != 0) handlers.at(i).handler = 1;
    task.initialize_signal_handlers(handlers);
    task.signal_mask_store(state.blocked);
    task.signal_pending_store(state.pending);
    task.parent_death_signal.store(state.parent_death_signal, std::memory_order_release);
}

void copy_submit_identity_data(uint8_t* dest, const ker::mod::sched::task::Task* task, const std::optional<TaskResourceSnapshot>& resources,
                               const ker::mod::security::Snapshot& credentials, const ker::mod::sched::task::Task::PathBuffer& root,
                               bool full_credentials = false, const TaskExecSignals* exec_signals = nullptr, bool routed_home = false,
                               const TaskFilesystemContext* filesystem = nullptr) {
    WkiTaskIdentityContext identity = {};
    auto const JOB = task != nullptr ? task->job_identity() : ker::mod::sched::session::Identity{};
    fill_task_identity_context(task, &identity, credentials, JOB, root);
    memcpy(dest, &identity, sizeof(identity));
    if (resources.has_value()) std::memcpy(dest + sizeof(identity), &*resources, sizeof(*resources));
    if (full_credentials && resources.has_value()) {
        auto const RECORD = ker::mod::security::credential_record::encode(credentials);
        std::memcpy(dest + WKI_TASK_RESOURCE_IDENTITY_SIZE, RECORD.data(), RECORD.size());
        TaskJobOrigins const ORIGINS{.session = {.node = JOB.origin.node, .reserved = 0, .boot = JOB.origin.boot},
                                     .group = {.node = JOB.group_origin.node, .reserved = 0, .boot = JOB.group_origin.boot}};
        std::memcpy(dest + WKI_TASK_RESOURCE_IDENTITY_SIZE + RECORD.size(), &ORIGINS, sizeof(ORIGINS));
        auto const SIGNALS = exec_signals != nullptr ? *exec_signals : (task != nullptr ? snapshot_exec_signals(*task) : TaskExecSignals{});
        std::memcpy(dest + WKI_TASK_RESOURCE_IDENTITY_SIZE + RECORD.size() + sizeof(ORIGINS), &SIGNALS, sizeof(SIGNALS));
        if (routed_home && task != nullptr) {
            auto const HOME = task->signal_home_identity();
            TaskSignalHome const WIRE{.origin = {.node = HOME.origin.node, .reserved = 0, .boot = HOME.origin.boot}, .serial = HOME.serial};
            std::memcpy(dest + WKI_TASK_FULL_IDENTITY_SIZE, &WIRE, sizeof(WIRE));
            if (filesystem != nullptr) std::memcpy(dest + WKI_TASK_ROUTED_IDENTITY_SIZE, filesystem, sizeof(*filesystem));
        }
    }
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

    if (task_uses_receiver_local_path(task, local_path)) {
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

auto wki_child_wait_begin(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& waiter, int64_t pid, int32_t options,
                          uint64_t& handle) -> WkiChildWaitResult {
    handle = 0;
    child_query::Spec spec{.caller_origin = {g_wki.my_node_id, g_wki.local_boot_epoch}};
    auto const OWN = child_events::selector_for_current_process_group(owner);
    if (pid > 0)
        spec.selector = static_cast<uint64_t>(pid);
    else if (pid == -1)
        spec.selector = child_events::SELECT_ANY;
    else if (pid == 0) {
        spec.selector = OWN;
        if (spec.selector.group_origin.local()) spec.selector.group_origin = spec.caller_origin;
    } else {
        if (pid == INT64_MIN) return native_child_error(-EINVAL);
        spec.selector = child_events::selector_for_process_group(static_cast<uint64_t>(-pid));
        spec.group_alias = true;
        if (spec.selector.value == OWN.value) spec.own_group = OWN.group_origin.local() ? spec.caller_origin : OWN.group_origin;
    }
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_OP_TIMEOUT_US);
    s_compute_lock.lock();
    ChildAuthorityRoute upstream{};
    auto const ADMISSION = capture_child_authority_route_locked(owner, upstream);
    if (ADMISSION != ChildAuthorityAdmission::READY) {
        s_compute_lock.unlock();
        if (ADMISSION == ChildAuthorityAdmission::LOCAL) return {.state = WkiChildWaitState::LOCAL};
        return ADMISSION == ChildAuthorityAdmission::AWAITING_ACCEPT ? WkiChildWaitResult{} : native_child_error(-EIO);
    }
    constexpr int32_t OPTIONS = child_events::OPTION_NOHANG | child_events::OPTION_STOPPED | child_events::OPTION_EXITED |
                                child_events::OPTION_CONTINUED | child_events::OPTION_NOWAIT;
    if (!child_query::valid_spec(spec) || (options & ~OPTIONS) != 0 ||
        owner.resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0) {
        s_compute_lock.unlock();
        return native_child_error(owner.resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0 ? -EINTR : -EINVAL);
    }
    ChildQueryRecord* available = nullptr;
    for (auto& record : s_child_queries) {
        if (record.active && record.native_waiter == &waiter && !record.retiring) {
            s_compute_lock.unlock();
            return native_child_error(-EBUSY);
        }
        if (!record.active && available == nullptr) available = &record;
    }
    if (available == nullptr || s_next_child_query_token == UINT64_MAX) {
        s_compute_lock.unlock();
        return native_child_error(available == nullptr ? -EAGAIN : -EOVERFLOW);
    }
    if (!owner.try_acquire()) {
        s_compute_lock.unlock();
        return native_child_error(-ESRCH);
    }
    if (!waiter.try_acquire()) {
        s_compute_lock.unlock();
        owner.release();
        return native_child_error(-ESRCH);
    }
    auto& record = *available;
    record.token = s_next_child_query_token++;
    record.request = {.sequence = record.token, .spec = spec, .options = options, .remaining_hops = child_query::MAX_HOPS};
    record.upstream = upstream;
    record.local_origin = spec.caller_origin;
    record.owner = &owner;
    record.native_waiter = &waiter;
    record.native_deadline_us = DEADLINE;
    record.relay.emplace();
    record.active = true;
    handle = record.token;
    s_compute_lock.unlock();
    notify_proxy_exit_work();
    return {};
}

auto wki_child_wait_poll(ker::mod::sched::task::Task& waiter, uint64_t handle) -> WkiChildWaitResult {
    s_compute_lock.lock();
    auto result = native_child_error(-ECANCELED);
    for (auto const& record : s_child_queries) {
        if (!record.active || record.token != handle || record.native_waiter != &waiter || record.retiring || record.native_detached)
            continue;
        result = record.busy ? WkiChildWaitResult{} : record.native_result;
        break;
    }
    s_compute_lock.unlock();
    return result;
}

auto wki_child_wait_commit(ker::mod::sched::task::Task& waiter, uint64_t handle) -> bool {
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_OP_TIMEOUT_US);
    bool committed = false;
    s_compute_lock.lock();
    for (auto& record : s_child_queries) {
        if (!record.active || record.token != handle || record.native_waiter != &waiter || record.retiring || record.native_detached ||
            record.busy || record.native_phase != NativeChildPhase::CLAIMED)
            continue;
        record.native_phase = NativeChildPhase::COMMIT;
        record.native_result.state = WkiChildWaitState::PENDING;
        record.native_deadline_us = DEADLINE;
        if (*record.claim_depth != 0) schedule_native_child_claim(record, ChildWaitCommand::COMMIT);
        committed = true;
        break;
    }
    s_compute_lock.unlock();
    if (committed) notify_proxy_exit_work();
    return committed;
}

void wki_child_wait_close(ker::mod::sched::task::Task& waiter, uint64_t handle) {
    s_compute_lock.lock();
    for (auto& record : s_child_queries) {
        if (!record.active || record.token != handle || record.native_waiter != &waiter) continue;
        record.native_detached = true;
        // A requested commit outlives its caller until receipt or terminal
        // failure, including its exactly-once local CPU observation.
        if (record.busy || record.native_phase != NativeChildPhase::COMMIT) {
            // Busy may be executing COMMIT; defer the phase decision to worker.
            if (!record.busy) retire_child_query_locked(record);
        }
        break;
    }
    s_compute_lock.unlock();
    notify_proxy_exit_work();
}

auto wki_child_wait_quiesce(ker::mod::sched::task::Task& owner) -> bool {
    bool quiesced = true;
    s_compute_lock.lock();
    for (auto& record : s_child_queries) {
        if (!record.active || record.owner != &owner || record.native_waiter == nullptr) continue;
        record.native_detached = true;
        if (record.busy) {
            quiesced = false;
            continue;
        }
        if (!record.retiring && record.native_phase == NativeChildPhase::COMMIT) {
            quiesced = false;
            continue;
        }
        retire_child_query_locked(record);
        if (!record.native_claim_retired) quiesced = false;
    }
    s_compute_lock.unlock();
    notify_proxy_exit_work();
    return quiesced;
}

TaskOutputCapture::~TaskOutputCapture() {
#ifdef WOS_SELFTEST
    s_output_capture_frees.fetch_add(1, std::memory_order_relaxed);
#endif
}

void TaskOutputCapture::retain() { refs.fetch_add(1, std::memory_order_relaxed); }

void TaskOutputCapture::release() {
    if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
}

void TaskOutputCapture::append(std::span<const uint8_t> bytes) {
    uint64_t const FLAGS = lock.lock_irqsave();
    if (!frozen) {
        size_t const COUNT = std::min(bytes.size(), data.size() - len);
        if (COUNT != 0) std::memcpy(data.data() + len, bytes.data(), COUNT);
        len = static_cast<uint16_t>(len + COUNT);
    }
    lock.unlock_irqrestore(FLAGS);
}

auto TaskOutputCapture::size() -> uint16_t {
    uint64_t const FLAGS = lock.lock_irqsave();
    uint16_t const SIZE = len;
    lock.unlock_irqrestore(FLAGS);
    return SIZE;
}

auto TaskOutputCapture::freeze() -> std::span<const uint8_t> {
    uint64_t const FLAGS = lock.lock_irqsave();
    frozen = true;
    uint16_t const SIZE = len;
    lock.unlock_irqrestore(FLAGS);
    return {data.data(), SIZE};
}

namespace {
auto wki_preferred_remote_node() -> uint16_t;

struct BalancedPlacement {
    uint16_t node_id = WKI_NODE_INVALID;
    bool found = false;
};

auto wki_balanced_node(uint16_t local_load, uint16_t local_cpus, uint16_t local_free_mem) -> BalancedPlacement;
void wki_release_preferred_remote_node(uint16_t node_id);
void wki_rollback_balanced_remote_assignment(uint16_t node_id);

class PreferredRemoteReservation {
   public:
    PreferredRemoteReservation() = default;
    PreferredRemoteReservation(const PreferredRemoteReservation&) = delete;
    auto operator=(const PreferredRemoteReservation&) -> PreferredRemoteReservation& = delete;
    ~PreferredRemoteReservation();

    void adopt(uint16_t selected_node_id) { node_id = selected_node_id; }

   private:
    uint16_t node_id = WKI_NODE_INVALID;
};

class BalancedRemoteAssignment {
   public:
    BalancedRemoteAssignment() = default;
    BalancedRemoteAssignment(const BalancedRemoteAssignment&) = delete;
    auto operator=(const BalancedRemoteAssignment&) -> BalancedRemoteAssignment& = delete;
    ~BalancedRemoteAssignment();

    void adopt(uint16_t selected_node_id) { node_id = selected_node_id; }
    void commit() { node_id = WKI_NODE_INVALID; }

   private:
    uint16_t node_id = WKI_NODE_INVALID;
};
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

auto wki_exec_signal_snapshot(const ker::mod::sched::task::Task& task) -> TaskExecSignals { return snapshot_exec_signals(task); }

auto wki_try_remote_spawn(ker::mod::sched::task::Task* task, const WkiRemoteSpawnSpec& spec) -> WkiRemoteSpawnResult {
    if (spec.deferred_proxy_id != nullptr) *spec.deferred_proxy_id = 0;
    int placement_error = -EHOSTUNREACH;
    auto finish_placement = [&](WkiRemoteSpawnResult result) {
        if (spec.error_out != nullptr) *spec.error_out = result == WkiRemoteSpawnResult::FAILED ? placement_error : 0;
        return result;
    };
    if (task != nullptr) {
        int const SCOPE_ADMISSION = ker::mod::security::compartment::remote_namespace_admission(task);
        if (SCOPE_ADMISSION != 0) {
            placement_error = SCOPE_ADMISSION;
            if (SCOPE_ADMISSION != -ENOTSUP) return finish_placement(WkiRemoteSpawnResult::FAILED);
            auto const* candidate = spec.exec_candidate;
            if (candidate != nullptr && (candidate->path == nullptr || candidate->target == nullptr)) {
                placement_error = -EINVAL;
                return finish_placement(WkiRemoteSpawnResult::FAILED);
            }
            auto const FLAGS = candidate != nullptr ? candidate->target_flags : task->wki_target_flags;
            auto const* target = candidate != nullptr ? candidate->target : task->wki_target_hostname.data();
            // Local execution preserves the actual subject and its owners.
            // Explicit named/remote/balanced placement is not portable yet,
            // even when the caller would otherwise allow a local fallback.
            if ((FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) == 0 &&
                (target[0] != '\0' || (FLAGS & (ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE |
                                                ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED)) != 0))
                return finish_placement(WkiRemoteSpawnResult::FAILED);
            return finish_placement(WkiRemoteSpawnResult::LOCAL);
        }
    }
    if (!g_remote_compute_initialized || task == nullptr) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "not-initialized");
        return finish_placement(WkiRemoteSpawnResult::LOCAL);
    }

    auto const* const CANDIDATE = spec.exec_candidate;
    if (CANDIDATE != nullptr && (CANDIDATE->path == nullptr || CANDIDATE->target == nullptr)) {
        placement_error = -EINVAL;
        return finish_placement(WkiRemoteSpawnResult::FAILED);
    }
    const char* const EXE_PATH = CANDIDATE != nullptr ? CANDIDATE->path : task->exe_path.data();
    const uint8_t* const BINARY = CANDIDATE != nullptr ? CANDIDATE->bytes.data() : task->elf_buffer;
    size_t const BINARY_SIZE = CANDIDATE != nullptr ? CANDIDATE->bytes.size() : task->elf_buffer_size;
    bool const BINARY_COMPLETE = CANDIDATE != nullptr ? CANDIDATE->complete : task->elf_buffer_complete;
    const char* const TARGET = CANDIDATE != nullptr ? CANDIDATE->target : task->wki_target_hostname.data();
    uint32_t const TARGET_FLAGS = CANDIDATE != nullptr ? CANDIDATE->target_flags : task->wki_target_flags;

    const bool HAS_EXE_PATH = EXE_PATH[0] != '\0';
    const bool HAS_INLINE_BINARY = BINARY_COMPLETE && BINARY != nullptr && BINARY_SIZE != 0;
    if (!HAS_EXE_PATH && !HAS_INLINE_BINARY) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "no-exec-context");
        return finish_placement(WkiRemoteSpawnResult::LOCAL);
    }

    // WKI_TARGET_FLAG_LOCAL: task is pinned to the local node.
    if ((TARGET_FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "local-flag");
        return finish_placement(WkiRemoteSpawnResult::LOCAL);
    }

    const bool EXPLICIT_TARGET = TARGET[0] != '\0';
    const bool STRICT_TARGET = EXPLICIT_TARGET && ((TARGET_FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool PREFER_REMOTE = !EXPLICIT_TARGET && ((TARGET_FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE) != 0);
    const bool STRICT_REMOTE = PREFER_REMOTE && ((TARGET_FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT) != 0);
    const bool BALANCED_PLACEMENT = !EXPLICIT_TARGET && ((TARGET_FLAGS & ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED) != 0);
    const bool AUTOMATIC_PLACEMENT = !EXPLICIT_TARGET && !PREFER_REMOTE && !BALANCED_PLACEMENT;
    const bool REMOTE_RECEIVER =
        task->wki_submitter_hostname.front() != '\0' && std::strcmp(task->wki_submitter_hostname.data(), wki_local_hostname()) != 0;

    if (AUTOMATIC_PLACEMENT) {
        // Placement controllers must run on the launch system so the policy
        // they apply describes one payload handoff. Migrating the controller
        // first creates an accidental proxy chain before it can request
        // local, named, remote, or balanced placement.
        if (is_placement_control_helper(task->name) || is_placement_control_helper(EXE_PATH)) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "placement-control-helper");
            return finish_placement(WkiRemoteSpawnResult::LOCAL);
        }

        bool stdin_is_pipe = false;
        uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
        auto* stdin_file = static_cast<ker::vfs::File*>(task->fd_table.lookup(0));
        stdin_is_pipe = ker::vfs::vfs_is_pipe_file(stdin_file);
        task->fd_table_lock.unlock_irqrestore(IRQF);
        if (stdin_is_pipe) {
            return finish_placement(WkiRemoteSpawnResult::LOCAL);
        }
    }

    // A task that was already submitted from another node stays on its receiver
    // for automatic follow-on execs. Explicit targets and remote-preferred
    // policies are an opt-in fan-out path used by distributed launchers.
    if (REMOTE_RECEIVER && !EXPLICIT_TARGET && !PREFER_REMOTE && !BALANCED_PLACEMENT) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "remote-receiver-auto");
        return finish_placement(WkiRemoteSpawnResult::LOCAL);
    }

    // Don't bounce tasks that are already remote-receiver instances.
    if (std::strncmp(task->name, "wki-remote", 10) == 0) {
        log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, "wki-remote-task");
        return finish_placement(WkiRemoteSpawnResult::LOCAL);
    }

    if (CANDIDATE == nullptr) task->wki_skip_legacy_placement = true;

    uint16_t best_node = WKI_NODE_INVALID;
    PreferredRemoteReservation preferred_reservation;
    BalancedRemoteAssignment balanced_assignment;
    if (EXPLICIT_TARGET) {
        const char* const TARGET_HOSTNAME = TARGET;
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
            return finish_placement(WkiRemoteSpawnResult::LOCAL);
        }
        if (node_id == WKI_NODE_INVALID) {
            WkiRemoteSpawnResult const RESULT = STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "target-not-found");
            return finish_placement(RESULT);
        }

        auto* peer = wki_peer_find(node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            WkiRemoteSpawnResult const RESULT = STRICT_TARGET ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "target-not-connected", node_id);
            return finish_placement(RESULT);
        }

        best_node = node_id;
    } else if (PREFER_REMOTE) {
        if (!wki_ipc_find_pipe_affinity_node(task, &best_node)) {
            s_compute_lock.lock();
            best_node = wki_preferred_remote_node();
            s_compute_lock.unlock();
            preferred_reservation.adopt(best_node);
        }
        if (best_node == WKI_NODE_INVALID) {
            WkiRemoteSpawnResult const RESULT = STRICT_REMOTE ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, "remote-preferred-no-node");
            return finish_placement(RESULT);
        }
    } else if (BALANCED_PLACEMENT || AUTOMATIC_PLACEMENT) {
        uint16_t const LOCAL_LOAD = compute_local_load();
        uint16_t const LOCAL_CPUS = std::max<uint16_t>(static_cast<uint16_t>(ker::mod::smt::get_core_count()), 1);
        uint16_t const LOCAL_FREE_MEM =
            static_cast<uint16_t>(std::min<uint64_t>(ker::mod::mm::phys::get_free_mem_pages() / 256ULL, UINT16_MAX));
        s_compute_lock.lock();
        BalancedPlacement const PLACEMENT = wki_balanced_node(LOCAL_LOAD, LOCAL_CPUS, LOCAL_FREE_MEM);
        s_compute_lock.unlock();
        if (!PLACEMENT.found) {
            WkiRemoteSpawnResult const RESULT = BALANCED_PLACEMENT ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
            log_spawn_diag(task, RESULT, AUTOMATIC_PLACEMENT ? "automatic-no-healthy-system" : "balanced-no-healthy-system");
            return finish_placement(RESULT);
        }
        best_node = PLACEMENT.node_id;
        balanced_assignment.adopt(best_node);
        if (best_node == WKI_NODE_INVALID) {
            log_spawn_diag(task, WkiRemoteSpawnResult::LOCAL, AUTOMATIC_PLACEMENT ? "automatic-local" : "balanced-local");
            return finish_placement(WkiRemoteSpawnResult::LOCAL);
        }
    }

    placement_error = ker::mod::sched::resource::service_remote_admission(*task);
    if (placement_error < 0) {
        auto const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "service-resource-admission", best_node);
        return finish_placement(RESULT);
    }
    if (!wki_peer_capability_negotiated(best_node, WKI_CAP_RESOURCE_LIMITS)) {
        placement_error = -ENOTSUP;
        auto const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "resource-capability", best_node);
        return finish_placement(RESULT);
    }
    if (!wki_peer_capability_negotiated(best_node, WKI_CAP_COMPUTE_CREDENTIALS) &&
        ker::mod::security::authorize_legacy_remote_exec(task->credentials()) != 0) {
        placement_error = -ENOTSUP;
        auto const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "credential-restrictions");
        return finish_placement(RESULT);
    }

    if (wki_resource_generation_snapshot(best_node, ResourceType::COMPUTE, WKI_EXEC_HANDOFF_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(best_node, ResourceType::COMPUTE, WKI_PARENT_DEATH_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(best_node, ResourceType::COMPUTE, WKI_CHILD_SIGNAL_RESOURCE_ID) == 0) {
        placement_error = -ENOTSUP;
        return finish_placement((STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL);
    }
    if (spec.prepare != nullptr) {
        placement_error = spec.prepare(*task, spec.context);
        if (placement_error != 0) return finish_placement(WkiRemoteSpawnResult::FAILED);
    }
    placement_error = 0;
    uint32_t tid = 0;
    bool const BINARY_FITS = HAS_INLINE_BINARY && (sizeof(TaskSubmitPayload) + sizeof(uint32_t) + BINARY_SIZE) <= WKI_ETH_MAX_PAYLOAD;

    // Export IPC fds (pipes, sockets) so they can be proxied on the remote node.
    // Must happen before task submission so the remote side can attach proxy fops.
    std::array<WkiIpcFdEntry, 16> ipc_fd_map = {};
    std::array<WkiIpcTaskFdHandoff, 16> ipc_fd_handoff = {};
    uint16_t ipc_fd_count = 0;
    uint8_t stdio_mask = 0;
    auto const IPC_CHANNELS = wki_ipc_capture_export_channels(best_node);
    WkiIpcSubmissionGuard ipc_guard(task, ipc_fd_map.data(), ipc_fd_handoff.data(), ipc_fd_count, best_node, IPC_CHANNELS);
    placement_error =
        wki_ipc_export_task_fds(task, best_node, IPC_CHANNELS, ipc_fd_map.data(), &ipc_fd_count, ipc_fd_handoff.data(), &stdio_mask);
    if (placement_error != 0) {
        WkiRemoteSpawnResult const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "fd-export-admission", best_node);
        return finish_placement(RESULT);
    }
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
        const char* local_path = EXE_PATH;
        if (EXE_PATH[0] != '/') {
            size_t const CWD_HINT = task->cwd_len;
            size_t const CWDLEN =
                (CWD_HINT > 0 && CWD_HINT < task->cwd.size() && task->cwd.at(CWD_HINT) == '\0') ? CWD_HINT : std::strlen(task->cwd.data());
            size_t const PATHLEN = std::strlen(EXE_PATH);
            bool const NEED_SEP = (CWDLEN > 1);
            size_t const TOTAL = CWDLEN + (NEED_SEP ? 1 : 0) + PATHLEN + 1;
            if (TOTAL <= abs_exe_path.size()) {
                std::memcpy(abs_exe_path.data(), task->cwd.data(), CWDLEN);
                if (NEED_SEP) {
                    *std::next(abs_exe_path.begin(), static_cast<ptrdiff_t>(CWDLEN)) = '/';
                    std::memcpy(abs_exe_path.data() + CWDLEN + 1, EXE_PATH, PATHLEN + 1);
                } else {
                    std::memcpy(abs_exe_path.data() + CWDLEN, EXE_PATH, PATHLEN + 1);
                }
                local_path = abs_exe_path.data();
            }
        }

        constexpr size_t VFS_REF_PATH_MAX = 5 + WKI_HOSTNAME_MAX + ABS_PATH_MAX;
        std::array<char, VFS_REF_PATH_MAX> vfs_ref_path = {};
        if (build_vfs_ref_path(best_node, local_path, vfs_ref_path.data(), vfs_ref_path.size(), task)) {
            vfs_ref_submit_attempted = true;
            tid =
                wki_task_submit_vfs_ref(best_node, vfs_ref_path.data(), spec.argv, spec.envp, spec.cwd, task, ipc_fd_map.data(),
                                        ipc_fd_count, ipc_fd_handoff.data(), stdio_mask, &placement_error, &ipc_guard, spec.exec_signals,
                                        (spec.cutover != nullptr || spec.image_credentials != nullptr) ? &spec : nullptr, spec.filesystem);
#ifdef WKI_DEBUG
            if (tid != 0) {
                ker::mod::dbg::log("[WKI] Remote spawn using VFS_REF '%s' -> '%s'", EXE_PATH, vfs_ref_path.data());
            }
#endif
        }
    }

    // Changing binary delivery cannot repair a rejected policy/admission or
    // establish that a timed-out execution never started.
    bool const CAN_RETRY_INLINE = !vfs_ref_submit_attempted || placement_error == -ENOENT || placement_error == -EIO ||
                                  placement_error == -E2BIG || placement_error == -ENAMETOOLONG;
    if (tid == 0 && BINARY_FITS && CAN_RETRY_INLINE) {
        if (vfs_ref_submit_attempted) {
            ipc_guard.cancel();
            ipc_fd_map = {};
            ipc_fd_handoff = {};
            ipc_fd_count = 0;
            ipc_guard.arm();
            placement_error = wki_ipc_export_task_fds(task, best_node, IPC_CHANNELS, ipc_fd_map.data(), &ipc_fd_count,
                                                      ipc_fd_handoff.data(), &stdio_mask);
            if (placement_error != 0) {
                WkiRemoteSpawnResult const RESULT =
                    (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
                log_spawn_diag(task, RESULT, "fd-export-admission", best_node);
                return finish_placement(RESULT);
            }
        }
        tid = wki_task_submit_inline(best_node, BINARY, static_cast<uint32_t>(BINARY_SIZE), spec.argv, spec.envp, spec.cwd, task,
                                     ipc_fd_map.data(), ipc_fd_count, ipc_fd_handoff.data(), stdio_mask, &placement_error, &ipc_guard,
                                     spec.exec_signals, (spec.cutover != nullptr || spec.image_credentials != nullptr) ? &spec : nullptr,
                                     spec.filesystem);
    }

    if (tid == 0) {
        if (placement_error == 0) placement_error = -ENOTSUP;
        ipc_guard.cancel();
        WkiRemoteSpawnResult const RESULT = (STRICT_TARGET || STRICT_REMOTE) ? WkiRemoteSpawnResult::FAILED : WkiRemoteSpawnResult::LOCAL;
        log_spawn_diag(task, RESULT, "submit-failed", best_node);
        return finish_placement(RESULT);
    }
    if (spec.cutover == nullptr && spec.accepted != nullptr) spec.accepted(*task, spec.context);
    // Acceptance commits exec. Close local CLOEXEC references before setting
    // proxy identity: a close may block and must not mark the proxy ready while
    // the current task is still performing exec cleanup. Rejections above keep
    // every original descriptor intact.
    close_proxy_fd_table(task, true);
    balanced_assignment.commit();

    bool const NEEDS_PROXY_PUBLICATION =
        task->sched_queue == ker::mod::sched::task::Task::sched_queue::NONE && task != ker::mod::sched::get_current_task();
    bool proxy_ready = false;
    if (NEEDS_PROXY_PUBLICATION) {
        // Publish the proxy identity before making the task discoverable.  A
        // signal may find the task as soon as post_task_waiting() inserts it in
        // the PID table; without these fields it could be mistaken for a local
        // process and woken to execute the ELF that is already running remotely.
        task->set_wki_proxy_task_id(tid);
        task->wki_proxy_task = true;
        task->set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
        proxy_ready = ker::mod::sched::post_task_waiting(task);
        if (!proxy_ready) {
            ker::syscall::process::child_events::abort_publication(*task);
            task->clear_wait_channel();
            task->wki_proxy_task = false;
            task->set_wki_proxy_task_id(0);
            abandon_submitted_task_after_proxy_publish_failure(tid);
            placement_error = -EAGAIN;
            log_spawn_diag(task, WkiRemoteSpawnResult::FAILED, "proxy-publication-failed", best_node);
            return finish_placement(WkiRemoteSpawnResult::FAILED);
        }
        if (!ker::syscall::process::child_events::commit_publication(*task)) [[unlikely]] {
            // post_task_waiting() already exposed the proxy and the remote
            // submit cannot be replayed safely. Preserve the REMOTE outcome;
            // the caller still owns a recovery reference until handoff.
            ker::mod::dbg::log("[WKI] proxy lifecycle commit completed concurrently: task_id=%u pid=0x%lx", tid, task->pid);
        }
    }

    // The process-owned exec plan retains its candidate and the old Task ELF
    // survives until proxy cleanup. Private spawn still transfers its own ELF.
    if (CANDIDATE == nullptr) {
        if (task->is_elf_buffer_shared) {
            release_cached_elf_buffer(task->elf_buffer);
        } else {
            delete[] task->elf_buffer;
        }
        task->elf_buffer = nullptr;
        task->elf_buffer_size = 0;
        task->elf_buffer_complete = true;
    }
    if (task->exec_image_file != nullptr) {
        ker::vfs::vfs_put_file(task->exec_image_file);
        task->exec_image_file = nullptr;
        task->exec_image_size = 0;
    }
    if (!NEEDS_PROXY_PUBLICATION && spec.deferred_proxy_id == nullptr) {
        task->set_wki_proxy_task_id(tid);
        task->wki_proxy_task = true;
    }

    bool completed_immediately = false;
    int32_t completed_exit_status = 0;
    uint8_t* completed_output = nullptr;
    uint16_t completed_output_len = 0;
    std::optional<TaskResourceSnapshot> resource_baseline;
    std::optional<TaskResourceSnapshot> resource_completion;
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
        if (st->resource_baseline && (st->resource_baseline->flags & WKI_RESOURCE_SERVICE_REPORTS) != 0)
            ker::mod::sched::resource::mark_service_remote_execution(*task);
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
            resource_baseline = st->resource_baseline;
            resource_completion = st->resource_completion;
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
        finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, tid,
                            resource_baseline ? &*resource_baseline : nullptr, resource_completion ? &*resource_completion : nullptr);
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
    if (spec.deferred_proxy_id != nullptr) *spec.deferred_proxy_id = tid;
    return finish_placement(WkiRemoteSpawnResult::REMOTE);
}

// ===============================================================================
// Submitter Side - Task Submit (INLINE only in V1)
// ===============================================================================

namespace {

[[maybe_unused]] constexpr auto task_submit_fragment_count(size_t total_len) -> size_t {
    return (total_len + WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX - 1) / WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX;
}

auto send_task_submit_payload(uint16_t target_node, const SubmitterChannelToken& channel, uint32_t task_id, const uint8_t* payload,
                              uint16_t payload_len) -> int {
    if (payload == nullptr || payload_len < sizeof(TaskSubmitPayload) || channel.channel == nullptr || channel.generation == 0) {
        return WKI_ERR_INVALID;
    }
    if (payload_len <= WKI_ETH_MAX_PAYLOAD) {
        return wki_send_on_channel_generation(target_node, channel.channel, channel.generation, MsgType::TASK_SUBMIT, payload, payload_len);
    }

    // Large exec contexts are uncommon, so keep the ordinary compiler-submit
    // path to one frame. Reliable-channel ordering makes the bounded fragments
    // arrive in exact offset order on the receiver's deferred worker.
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> frame{};
    size_t offset = 0;
    while (offset < payload_len) {
        size_t const CHUNK_LEN = std::min<size_t>(payload_len - offset, WKI_TASK_SUBMIT_FRAGMENT_DATA_MAX);
        auto* fragment = reinterpret_cast<TaskSubmitFragmentPayload*>(frame.data());
        fragment->task_id = task_id;
        fragment->total_len = payload_len;
        fragment->offset = static_cast<uint16_t>(offset);
        std::memcpy(frame.data() + sizeof(TaskSubmitFragmentPayload), payload + offset, CHUNK_LEN);
        auto const FRAME_LEN = static_cast<uint16_t>(sizeof(TaskSubmitFragmentPayload) + CHUNK_LEN);
        int const RC = wki_send_on_channel_generation(target_node, channel.channel, channel.generation, MsgType::TASK_SUBMIT_FRAGMENT,
                                                      frame.data(), FRAME_LEN);
        if (RC != WKI_OK) {
            return RC;
        }
        offset += CHUNK_LEN;
    }
    return WKI_OK;
}

template <typename Send, typename Current, typename Now, typename Pause>
auto retry_compute_control(uint64_t deadline_us, Send send, Current current, Now now, Pause pause) -> int {
    for (;;) {
        if (!current()) return WKI_ERR_NOT_FOUND;
        int const RESULT = send();
        switch (RESULT) {
            case WKI_ERR_NO_MEM:
            case WKI_ERR_NO_ROUTE:
            case WKI_ERR_NO_CREDITS:
            case WKI_ERR_NOT_FOUND:
            case WKI_ERR_BUSY:
            case WKI_ERR_TX_FAILED:
                break;
            default:
                return RESULT;
        }
        if (now() >= deadline_us) return RESULT;
        pause(deadline_us);
        if (now() >= deadline_us) return RESULT;
    }
}

auto wait_for_exec_submit(WkiWaitEntry& initial_wait, uint64_t timeout, uint32_t task_id, ker::mod::sched::task::Task& owner,
                          const WkiRemoteSpawnSpec* transfer, int& cutover_error) -> int {
    cutover_error = 0;
    int const INITIAL = wki_wait_for_op(&initial_wait, timeout);
    if (INITIAL != WKI_OK || transfer == nullptr || transfer->cutover == nullptr) return INITIAL;
    WkiWaitEntry committed_wait{};
    TaskExecCommit commit{};
    uint16_t node = WKI_NODE_INVALID;
    WkiChannel* channel = nullptr;
    uint32_t generation = 0;
    s_compute_lock.lock();
    auto* task = find_submitted_task_any(task_id);
    if (task == nullptr || !task->active || task->local_task != &owner) {
        s_compute_lock.unlock();
        return WKI_ERR_PEER_FENCED;
    }
    if (!task->exec_prepared) {
        s_compute_lock.unlock();
        return INITIAL;
    }
    if (task->response_consumer_wait_entry == &initial_wait) task->response_consumer_wait_entry = nullptr;
    task->response_wait_entry = &committed_wait;
    task->response_consumer_wait_entry = &committed_wait;
    task->response_pending.store(true, std::memory_order_release);
    task->exec_cutover = true;
    commit.identity = {.task_id = task_id, .remote_pid = task->exec_remote_pid};
    node = task->target_node;
    channel = task->submit_channel;
    generation = task->submit_channel_generation;
    s_compute_lock.unlock();

    // The durable consumer slot stays visible through every yield in source
    // cleanup. A callback error precedes publication and restores rollback;
    // after success only the replacement image may run.
    cutover_error = transfer->cutover(owner, task_id, commit.identity.remote_pid, commit.signals, transfer->context);
    if (cutover_error != 0) {
        s_compute_lock.lock();
        if (auto* row = find_submitted_task_any(task_id); row != nullptr) {
            row->exec_cutover = false;
            if (row->response_wait_entry == &committed_wait) row->response_wait_entry = nullptr;
            row->response_pending.store(false, std::memory_order_release);
        }
        bool const CLAIMED = wki_claim_op(&committed_wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(committed_wait, CLAIMED, WKI_ERR_BUSY);
        s_compute_lock.lock();
        if (auto* row = find_submitted_task_any(task_id); row != nullptr && row->response_consumer_wait_entry == &committed_wait)
            row->response_consumer_wait_entry = nullptr;
        s_compute_lock.unlock();
        return WKI_ERR_BUSY;
    }
    unsigned attempts = 0;
    int first_result = WKI_OK;
    uint64_t const SEND_DEADLINE = wki_future_deadline_us(wki_now_us(), 1'000'000);
    int const SENT = retry_compute_control(
        SEND_DEADLINE,
        [&]() {
            int const RESULT =
                wki_send_on_channel_generation(node, channel, generation, MsgType::TASK_EXEC_COMMIT, &commit, sizeof(commit));
            if (++attempts == 1) first_result = RESULT;
            return RESULT;
        },
        [&]() {
            s_compute_lock.lock();
            auto* row = find_submitted_task(task_id);
            bool const CURRENT = row != nullptr && row->local_task == &owner && row->exec_cutover && row->submit_channel == channel &&
                                 row->submit_channel_generation == generation;
            s_compute_lock.unlock();
            return CURRENT;
        },
        []() { return wki_now_us(); },
        [](uint64_t) {
            wki_spin_yield();
            ker::mod::sched::kern_yield();
        });
    if (attempts > 1 || SENT != WKI_OK)
        ker::mod::dbg::log("[WKI] Exec commit send: task_id=%u attempts=%u first=%d final=%d", task_id, attempts, first_result, SENT);
    int result = SENT;
    if (SENT == WKI_OK) {
        result = wki_wait_for_op(&committed_wait, timeout);
    } else {
        s_compute_lock.lock();
        if (auto* row = find_submitted_task_any(task_id); row != nullptr && row->response_wait_entry == &committed_wait)
            row->response_wait_entry = nullptr;
        bool const CLAIMED = wki_claim_op(&committed_wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(committed_wait, CLAIMED, SENT);
        // A matching response can win the send-failure/retirement race.
        // The claimant's completed result is authoritative after quiescence.
        result = committed_wait.result;
    }
    bool terminal = result != WKI_OK;
    s_compute_lock.lock();
    task = find_submitted_task_any(task_id);
    if (task != nullptr) {
        if (task->response_wait_entry == &committed_wait) task->response_wait_entry = nullptr;
        if (task->response_consumer_wait_entry == &committed_wait) task->response_consumer_wait_entry = nullptr;
        task->response_pending.store(false, std::memory_order_release);
        terminal |= task->accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        if (terminal) {
            task->active = false;
            task->exit_status = -1;
            task->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
            task->cancel_service_report(task->submission_started);
        }
    }
    s_compute_lock.unlock();
    if (terminal) {
        ker::mod::dbg::log("[WKI] Exec handoff terminal: task_id=%u send=%d wait=%d", task_id, SENT, result);
        static_cast<void>(send_task_cancel_request(node, task_id, WKI_SIGKILL_NUM, channel, generation));
    }
    return WKI_OK;
}

}  // namespace

auto wki_task_submit_inline(uint16_t target_node, const void* binary, uint32_t binary_len,
                            const char* const argv[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* const envp[],  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
                            const char* cwd, ker::mod::sched::task::Task* local_task, const WkiIpcFdEntry* ipc_fd_map,
                            uint16_t ipc_fd_count, WkiIpcTaskFdHandoff* ipc_fd_handoff, uint8_t stdio_mask, int* error_out,
                            WkiIpcSubmissionGuard* preparation, const TaskExecSignals* exec_signals, const WkiRemoteSpawnSpec* transfer,
                            const ker::mod::sched::task::Task::FilesystemContext* filesystem) -> uint32_t {
    if (error_out != nullptr) *error_out = 0;
    auto fail_submit = [&](int error) -> uint32_t {
        if (error_out != nullptr) *error_out = error;
        return 0;
    };
    if (ipc_fd_count > 16) return fail_submit(-EINVAL);
    WkiIpcSubmissionGuard ipc_guard(local_task, ipc_fd_map, ipc_fd_handoff, ipc_fd_count, target_node,
                                    preparation != nullptr ? preparation->channels() : WkiIpcChannels{});
    if (preparation != nullptr) preparation->disarm();
    auto cleanup_submit_ipc = [&]() { ipc_guard.cancel(); };
    if (binary == nullptr || binary_len == 0 || (ipc_fd_count != 0 && ipc_fd_map == nullptr) ||
        (stdio_mask != UINT8_MAX && stdio_mask > WKI_RESOURCE_STDIO_ALL)) {
        cleanup_submit_ipc();
        return fail_submit(-EINVAL);
    }

    if (local_task == nullptr) {
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    int const SCOPE_ADMISSION = ker::mod::security::compartment::remote_namespace_admission(local_task);
    if (SCOPE_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(SCOPE_ADMISSION);
    }
    ker::mod::security::Snapshot read_credentials;
    int const READ_CREDENTIAL_ADMISSION = select_submit_read_credentials(local_task->credentials(), transfer, read_credentials);
    if (READ_CREDENTIAL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(READ_CREDENTIAL_ADMISSION);
    }
    TaskFilesystemPreparation filesystem_preparation(local_task, cwd, filesystem, &read_credentials);
    if (filesystem_preparation.status() != 0) {
        cleanup_submit_ipc();
        return fail_submit(filesystem_preparation.status());
    }
    FilesystemSubmitAdmission filesystem_admission;
    int const FILESYSTEM_PREPARATION = prepare_return_owner_cwd(filesystem_preparation, target_node, filesystem_admission);
    if (FILESYSTEM_PREPARATION != 0) {
        cleanup_submit_ipc();
        return fail_submit(FILESYSTEM_PREPARATION);
    }
    SubmitFilesystemContext submit_filesystem;
    if (!capture_submit_filesystem(filesystem_preparation, submit_filesystem)) {
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    cwd = submit_filesystem.cwd;
    auto const SUBMIT_CREDENTIALS = filesystem_preparation.credentials();
    bool const FULL_CREDENTIALS = wki_peer_capability_negotiated(target_node, WKI_CAP_COMPUTE_CREDENTIALS);
    bool const ROUTED_HOME =
        FULL_CREDENTIALS && wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_GROUP_ROUTE_RESOURCE_ID) != 0;
    if (!ROUTED_HOME || local_task->signal_home_identity().serial == 0) {
        cleanup_submit_ipc();
        return fail_submit(ROUTED_HOME ? -EOVERFLOW : -ENOTSUP);
    }

    if (wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_EXEC_HANDOFF_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_PARENT_DEATH_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_CHILD_SIGNAL_RESOURCE_ID) == 0 ||
        (transfer != nullptr && (!FULL_CREDENTIALS || transfer->exec_credentials == nullptr))) {
        cleanup_submit_ipc();
        return fail_submit(-ENOTSUP);
    }
    int const CREDENTIAL_ADMISSION = FULL_CREDENTIALS ? 0 : ker::mod::security::authorize_legacy_remote_exec(SUBMIT_CREDENTIALS);
    if (CREDENTIAL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(CREDENTIAL_ADMISSION);
    }
    if (!wki_peer_capability_negotiated(target_node, WKI_CAP_RESOURCE_LIMITS)) {
        cleanup_submit_ipc();
        return fail_submit(-ENOTSUP);
    }
    int const SERVICE_ADMISSION = ker::mod::sched::resource::service_remote_admission(*local_task);
    if (SERVICE_ADMISSION < 0) {
        cleanup_submit_ipc();
        return fail_submit(SERVICE_ADMISSION);
    }
    std::optional<TaskResourceSnapshot> resources;
    {
        resources.emplace();
        if (!snapshot_task_resources(local_task, *resources, transfer != nullptr && transfer->cutover != nullptr)) {
            cleanup_submit_ipc();
            return fail_submit(-ESRCH);
        }
        if ((resources->flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0 &&
            wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_CHILD_CPU_RESOURCE_ID) == 0) {
            cleanup_submit_ipc();
            return fail_submit(-ENOTSUP);
        }
        if (stdio_mask != UINT8_MAX) {
            resources->flags =
                static_cast<uint16_t>((resources->flags & ~WKI_RESOURCE_STDIO_FLAGS) | (stdio_mask << WKI_RESOURCE_STDIO_SHIFT));
        }
    }
    if (resources && SERVICE_ADMISSION == 1) resources->flags |= WKI_RESOURCE_SERVICE_REPORTS;
    auto const* FILESYSTEM = filesystem_preparation.wire_context();
    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info, resources.has_value(), FULL_CREDENTIALS, ROUTED_HOME,
                                   FILESYSTEM)) {
        cleanup_submit_ipc();
        return fail_submit(-E2BIG);
    }

    // Check total size fits in WKI message:
    // TaskSubmitPayload + binary_len + binary + argv/envp/cwd + identity + policy + IPC fd entries.
    uint32_t const IPC_DATA_LEN = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    uint64_t const TOTAL = static_cast<uint64_t>(sizeof(TaskSubmitPayload)) + sizeof(uint32_t) + binary_len + context_info.data_len +
                           IPC_DATA_LEN + (transfer != nullptr ? sizeof(TaskExecCredentialTransition) : 0);
    if (TOTAL > UINT16_MAX) {
        ker::mod::dbg::log("[WKI] Task inline payload too large: binary=%u context=%u total=%llu", binary_len, context_info.data_len,
                           TOTAL);
        cleanup_submit_ipc();
        return fail_submit(-E2BIG);
    }
    auto const MSG_LEN = static_cast<uint16_t>(TOTAL);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> small_buf __attribute__((uninitialized));
    TaskSubmitBuffer large_buf(local_task, MSG_LEN > small_buf.size() ? MSG_LEN : 0);
    if (MSG_LEN > small_buf.size() && large_buf.data() == nullptr) {
        cleanup_submit_ipc();
        return fail_submit(-ENOMEM);
    }
    uint8_t* const BUF = large_buf.data() != nullptr ? large_buf.data() : small_buf.data();

    SubmitterChannelToken SUBMIT_CHANNEL;
    int const EXEC_CHANNEL_ADMISSION = capture_exec_submit_channel(target_node, SUBMIT_CHANNEL);
    if (EXEC_CHANNEL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(EXEC_CHANNEL_ADMISSION);
    }
    if (FILESYSTEM != nullptr && (filesystem_admission.channel.channel != SUBMIT_CHANNEL.channel ||
                                  filesystem_admission.channel.generation != SUBMIT_CHANNEL.generation ||
                                  !filesystem_submit_admission_current(target_node, filesystem_admission))) {
        cleanup_submit_ipc();
        return fail_submit(-ESTALE);
    }
    WkiChannelIdentity const IPC_SUBMIT_CHANNEL{SUBMIT_CHANNEL.channel, target_node, WKI_CHAN_RESOURCE, SUBMIT_CHANNEL.generation};
    if (!wki_ipc_exports_match_submit(ipc_fd_map, ipc_fd_count, target_node, ipc_guard.channels(), IPC_SUBMIT_CHANNEL)) {
        cleanup_submit_ipc();
        return fail_submit(-EHOSTUNREACH);
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = allocate_submitted_task_id_locked();

    // Create submitted task entry
    SubmittedTask st;
    st.resource_baseline = resources;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.submit_channel = SUBMIT_CHANNEL.channel;
    st.submit_channel_generation = SUBMIT_CHANNEL.generation;
    st.accept_status = 0;
    st.exec_handoff = transfer != nullptr && transfer->cutover != nullptr;
    st.complete_pending = false;
    st.exit_status = 0;
    if (!st.set_local_task_ref(local_task)) {
        s_compute_lock.unlock();
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    st.result_handle_owned = true;
    st.result_owner_task = ker::mod::sched::get_current_task();
    st.ipc_channels = ipc_guard.channels();
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);
    int publication_error = 0;
    auto* published = publish_submitted_task_locked(std::move(st), &publication_error);
    if (published == nullptr) {
        s_compute_lock.unlock();
        st.reset_local_task_ref();
        cleanup_submit_ipc();
        return fail_submit(publication_error);
    }
    if (resources && (resources->flags & WKI_RESOURCE_SERVICE_REPORTS) != 0) {
        int const BIND = published->service_report.bind(*local_task);
        if (BIND != 1) {
            published->ipc_fd_count = 0;  // executor guard still owns all exports
            published->active = false;
            static_cast<void>(consume_submitted_task_result_locked(published));
            s_compute_lock.unlock();
            cleanup_submit_ipc();
            return fail_submit(BIND < 0 ? BIND : -ESRCH);
        }
        published->service_report_pending = true;
    }
    // Move the pins only after the durable row exists. There is no blocking
    // operation between publication and relinquishing the executor's guard.
    remember_submitted_ipc_handoff(*published, ipc_fd_handoff, ipc_fd_count);
    ipc_guard.disarm();
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, binary_len, CALLSITE);

    // Build TASK_SUBMIT message. Every byte in the transmitted prefix is
    // assigned below; do not pattern-initialize the unused bounded tail.
    auto* submit = reinterpret_cast<TaskSubmitPayload*>(BUF);
    submit->task_id = TASK_ID;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::INLINE);
    submit->prefer_inline = 1;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->identity_len = context_info.identity_len;
    submit->ipc_fd_count = ipc_fd_count;
    submit->reserved = (transfer == nullptr            ? 0
                        : transfer->cutover != nullptr ? WKI_TASK_SUBMIT_EXEC_HANDOFF
                                                       : WKI_TASK_SUBMIT_SPAWN_TRANSITION) |
                       WKI_TASK_SUBMIT_GROUP_HOME | (FILESYSTEM != nullptr ? WKI_TASK_SUBMIT_FILESYSTEM : 0);

    // INLINE format: {binary_len:u32, binary[binary_len], argv/envp/cwd, identity, policy, ipc_fd_entries[]}
    uint8_t* cursor = BUF + sizeof(TaskSubmitPayload);
    memcpy(cursor, &binary_len, sizeof(uint32_t));
    cursor += sizeof(uint32_t);
    memcpy(cursor, binary, binary_len);
    cursor += binary_len;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.identity_len > 0) {
        copy_submit_identity_data(cursor, local_task, resources, SUBMIT_CREDENTIALS, submit_filesystem.root, FULL_CREDENTIALS, exec_signals,
                                  ROUTED_HOME, FILESYSTEM);
        cursor += context_info.identity_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, IPC_DATA_LEN);
    }
    if (transfer != nullptr) std::memcpy(cursor + IPC_DATA_LEN, transfer->exec_credentials, sizeof(TaskExecCredentialTransition));

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    s_compute_lock.lock();
    if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
        task_ptr->submission_started = true;
        task_ptr->response_wait_entry = &wait;
        task_ptr->response_consumer_wait_entry = &wait;
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = send_task_submit_payload(target_node, SUBMIT_CHANNEL, TASK_ID, BUF, MSG_LEN);

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
            task_ptr->cancel_service_report(task_ptr->submission_started);
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
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_submit_ipc();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, SEND_RET,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return fail_submit(task_transport_errno(SEND_RET));
    }

    int cutover_error = 0;
    int const WAIT_RC = wait_for_exec_submit(wait, WKI_OP_TIMEOUT_US, TASK_ID, *local_task, transfer, cutover_error);
    uint8_t accept_status = 0;
    SubmittedIpcHandoffCleanup handoff_cleanup = {};
    WkiIpcSubmissionGuard accepted_handoff(local_task, nullptr, handoff_cleanup.handoff.data(), handoff_cleanup.count, WKI_NODE_INVALID);
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
            task_ptr->cancel_service_report(task_ptr->submission_started);
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        static_cast<void>(
            send_task_cancel_request(target_node, TASK_ID, WKI_SIGKILL_NUM, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation));
        cleanup_submit_ipc();
        ker::mod::dbg::log("[WKI] Task submit wait failed: task_id=%u target=0x%04x rc=%d (%s)", TASK_ID, target_node, WAIT_RC,
                           errno_name(WAIT_RC));
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return fail_submit(cutover_error != 0 ? cutover_error : task_transport_errno(WAIT_RC));
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->active = false;
            task_ptr->cancel_service_report(task_ptr->submission_started);
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_submit_ipc();
        ker::mod::dbg::log("[WKI] Task rejected: task_id=%u status=%u", TASK_ID, accept_status);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_INLINE, target_node, TASK_ID,
                                -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status),
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), binary_len, CALLSITE);
        return fail_submit(task_ptr == nullptr ? -EIO : task_rejection_errno(accept_status));
    }
    handoff_cleanup = take_submitted_ipc_handoff_locked(task_ptr);
    s_compute_lock.unlock();

    uint16_t const EXPECTED_HANDOFFS = handoff_cleanup.count;
    uint16_t const COMMITTED_HANDOFFS = wki_ipc_commit_task_fd_handoff(local_task, handoff_cleanup.handoff.data(), handoff_cleanup.count);
    handoff_cleanup.count = 0;
    wki_ipc_activate_task_fds(ipc_fd_map, ipc_fd_count, target_node);
    if (COMMITTED_HANDOFFS != EXPECTED_HANDOFFS) {
        ker::mod::dbg::log("[WKI] Accepted inline IPC fd handoff mismatch: task_id=%u committed=%u expected=%u", TASK_ID,
                           COMMITTED_HANDOFFS, EXPECTED_HANDOFFS);
    }

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
                             uint16_t ipc_fd_count, WkiIpcTaskFdHandoff* ipc_fd_handoff, uint8_t stdio_mask, int* error_out,
                             WkiIpcSubmissionGuard* preparation, const TaskExecSignals* exec_signals, const WkiRemoteSpawnSpec* transfer,
                             const ker::mod::sched::task::Task::FilesystemContext* filesystem) -> uint32_t {
    if (error_out != nullptr) *error_out = 0;
    auto fail_submit = [&](int error) -> uint32_t {
        if (error_out != nullptr) *error_out = error;
        return 0;
    };
    if (ipc_fd_count > 16) return fail_submit(-EINVAL);
    WkiIpcSubmissionGuard ipc_guard(local_task, ipc_fd_map, ipc_fd_handoff, ipc_fd_count, target_node,
                                    preparation != nullptr ? preparation->channels() : WkiIpcChannels{});
    if (preparation != nullptr) preparation->disarm();
    auto cleanup_submit_ipc = [&]() { ipc_guard.cancel(); };
    if (vfs_path == nullptr || vfs_path[0] == '\0' || (ipc_fd_count != 0 && ipc_fd_map == nullptr) ||
        (stdio_mask != UINT8_MAX && stdio_mask > WKI_RESOURCE_STDIO_ALL)) {
        cleanup_submit_ipc();
        return fail_submit(-EINVAL);
    }

    size_t const PATH_LEN = std::strlen(vfs_path);
    if (PATH_LEN == 0 || PATH_LEN >= 512 || PATH_LEN > UINT16_MAX) {
        cleanup_submit_ipc();
        return fail_submit(-ENAMETOOLONG);
    }
    auto const PATH_LEN_WIRE = static_cast<uint16_t>(PATH_LEN);

    if (local_task == nullptr) {
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    int const SCOPE_ADMISSION = ker::mod::security::compartment::remote_namespace_admission(local_task);
    if (SCOPE_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(SCOPE_ADMISSION);
    }
    ker::mod::security::Snapshot read_credentials;
    int const READ_CREDENTIAL_ADMISSION = select_submit_read_credentials(local_task->credentials(), transfer, read_credentials);
    if (READ_CREDENTIAL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(READ_CREDENTIAL_ADMISSION);
    }
    TaskFilesystemPreparation filesystem_preparation(local_task, cwd, filesystem, &read_credentials);
    if (filesystem_preparation.status() != 0) {
        cleanup_submit_ipc();
        return fail_submit(filesystem_preparation.status());
    }
    FilesystemSubmitAdmission filesystem_admission;
    int const FILESYSTEM_PREPARATION = prepare_return_owner_cwd(filesystem_preparation, target_node, filesystem_admission);
    if (FILESYSTEM_PREPARATION != 0) {
        cleanup_submit_ipc();
        return fail_submit(FILESYSTEM_PREPARATION);
    }
    SubmitFilesystemContext submit_filesystem;
    if (!capture_submit_filesystem(filesystem_preparation, submit_filesystem)) {
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    cwd = submit_filesystem.cwd;
    auto const SUBMIT_CREDENTIALS = filesystem_preparation.credentials();
    bool const FULL_CREDENTIALS = wki_peer_capability_negotiated(target_node, WKI_CAP_COMPUTE_CREDENTIALS);
    bool const ROUTED_HOME =
        FULL_CREDENTIALS && wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_GROUP_ROUTE_RESOURCE_ID) != 0;
    if (!ROUTED_HOME || local_task->signal_home_identity().serial == 0) {
        cleanup_submit_ipc();
        return fail_submit(ROUTED_HOME ? -EOVERFLOW : -ENOTSUP);
    }

    if (wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_EXEC_HANDOFF_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_PARENT_DEATH_RESOURCE_ID) == 0 ||
        wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_CHILD_SIGNAL_RESOURCE_ID) == 0 ||
        (transfer != nullptr && (!FULL_CREDENTIALS || transfer->exec_credentials == nullptr))) {
        cleanup_submit_ipc();
        return fail_submit(-ENOTSUP);
    }
    int const CREDENTIAL_ADMISSION = FULL_CREDENTIALS ? 0 : ker::mod::security::authorize_legacy_remote_exec(SUBMIT_CREDENTIALS);
    if (CREDENTIAL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(CREDENTIAL_ADMISSION);
    }
    if (!wki_peer_capability_negotiated(target_node, WKI_CAP_RESOURCE_LIMITS)) {
        cleanup_submit_ipc();
        return fail_submit(-ENOTSUP);
    }
    int const SERVICE_ADMISSION = ker::mod::sched::resource::service_remote_admission(*local_task);
    if (SERVICE_ADMISSION < 0) {
        cleanup_submit_ipc();
        return fail_submit(SERVICE_ADMISSION);
    }
    std::optional<TaskResourceSnapshot> resources;
    {
        resources.emplace();
        if (!snapshot_task_resources(local_task, *resources, transfer != nullptr && transfer->cutover != nullptr)) {
            cleanup_submit_ipc();
            return fail_submit(-ESRCH);
        }
        if ((resources->flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0 &&
            wki_resource_generation_snapshot(target_node, ResourceType::COMPUTE, WKI_CHILD_CPU_RESOURCE_ID) == 0) {
            cleanup_submit_ipc();
            return fail_submit(-ENOTSUP);
        }
        if (stdio_mask != UINT8_MAX) {
            resources->flags =
                static_cast<uint16_t>((resources->flags & ~WKI_RESOURCE_STDIO_FLAGS) | (stdio_mask << WKI_RESOURCE_STDIO_SHIFT));
        }
    }
    if (resources && SERVICE_ADMISSION == 1) resources->flags |= WKI_RESOURCE_SERVICE_REPORTS;
    auto const* FILESYSTEM = filesystem_preparation.wire_context();
    SubmitContextInfo context_info = {};
    if (!build_submit_context_info(local_task, argv, envp, cwd, &context_info, resources.has_value(), FULL_CREDENTIALS, ROUTED_HOME,
                                   FILESYSTEM)) {
        cleanup_submit_ipc();
        return fail_submit(-E2BIG);
    }

    // TaskSubmitPayload + path_len + path + argv/envp/cwd + identity + policy + IPC fd entries.
    uint32_t const IPC_DATA_LEN = static_cast<uint32_t>(ipc_fd_count) * sizeof(WkiIpcFdEntry);
    uint64_t const TOTAL = static_cast<uint64_t>(sizeof(TaskSubmitPayload)) + sizeof(uint16_t) + PATH_LEN_WIRE + context_info.data_len +
                           IPC_DATA_LEN + (transfer != nullptr ? sizeof(TaskExecCredentialTransition) : 0);
    if (TOTAL > UINT16_MAX) {
        ker::mod::dbg::log("[WKI] VFS_REF submit context too large: path=%u context=%u ipc=%u total=%llu", PATH_LEN_WIRE,
                           context_info.data_len, IPC_DATA_LEN, TOTAL);
        cleanup_submit_ipc();
        return fail_submit(-E2BIG);
    }
    auto const MSG_LEN = static_cast<uint16_t>(TOTAL);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<uint8_t, WKI_ETH_MAX_PAYLOAD> small_buf __attribute__((uninitialized));
    TaskSubmitBuffer large_buf(local_task, MSG_LEN > small_buf.size() ? MSG_LEN : 0);
    if (MSG_LEN > small_buf.size() && large_buf.data() == nullptr) {
        cleanup_submit_ipc();
        return fail_submit(-ENOMEM);
    }
    uint8_t* const BUF = large_buf.data() != nullptr ? large_buf.data() : small_buf.data();

    SubmitterChannelToken SUBMIT_CHANNEL;
    int const EXEC_CHANNEL_ADMISSION = capture_exec_submit_channel(target_node, SUBMIT_CHANNEL);
    if (EXEC_CHANNEL_ADMISSION != 0) {
        cleanup_submit_ipc();
        return fail_submit(EXEC_CHANNEL_ADMISSION);
    }
    if (FILESYSTEM != nullptr && (filesystem_admission.channel.channel != SUBMIT_CHANNEL.channel ||
                                  filesystem_admission.channel.generation != SUBMIT_CHANNEL.generation ||
                                  !filesystem_submit_admission_current(target_node, filesystem_admission))) {
        cleanup_submit_ipc();
        return fail_submit(-ESTALE);
    }
    WkiChannelIdentity const IPC_SUBMIT_CHANNEL{SUBMIT_CHANNEL.channel, target_node, WKI_CHAN_RESOURCE, SUBMIT_CHANNEL.generation};
    if (!wki_ipc_exports_match_submit(ipc_fd_map, ipc_fd_count, target_node, ipc_guard.channels(), IPC_SUBMIT_CHANNEL)) {
        cleanup_submit_ipc();
        return fail_submit(-EHOSTUNREACH);
    }

    s_compute_lock.lock();
    uint32_t const TASK_ID = allocate_submitted_task_id_locked();

    SubmittedTask st;
    st.resource_baseline = resources;
    st.active = true;
    st.task_id = TASK_ID;
    st.target_node = target_node;
    st.response_pending = false;
    st.submit_channel = SUBMIT_CHANNEL.channel;
    st.submit_channel_generation = SUBMIT_CHANNEL.generation;
    st.accept_status = 0;
    st.exec_handoff = transfer != nullptr && transfer->cutover != nullptr;
    st.complete_pending = false;
    st.exit_status = 0;
    if (!st.set_local_task_ref(local_task)) {
        s_compute_lock.unlock();
        cleanup_submit_ipc();
        return fail_submit(-ESRCH);
    }
    st.local_pid = local_task != nullptr ? local_task->pid : 0;
    st.proxy_ready = false;
    st.result_handle_owned = true;
    st.result_owner_task = ker::mod::sched::get_current_task();
    st.ipc_channels = ipc_guard.channels();
    remember_submitted_ipc_fds(st, ipc_fd_map, ipc_fd_count);
    int publication_error = 0;
    auto* published = publish_submitted_task_locked(std::move(st), &publication_error);
    if (published == nullptr) {
        s_compute_lock.unlock();
        st.reset_local_task_ref();
        cleanup_submit_ipc();
        return fail_submit(publication_error);
    }
    if (resources && (resources->flags & WKI_RESOURCE_SERVICE_REPORTS) != 0) {
        int const BIND = published->service_report.bind(*local_task);
        if (BIND != 1) {
            published->ipc_fd_count = 0;  // executor guard still owns all exports
            published->active = false;
            static_cast<void>(consume_submitted_task_result_locked(published));
            s_compute_lock.unlock();
            cleanup_submit_ipc();
            return fail_submit(BIND < 0 ? BIND : -ESRCH);
        }
        published->service_report_pending = true;
    }
    // Move the pins only after the durable row exists. There is no blocking
    // operation between publication and relinquishing the executor's guard.
    remember_submitted_ipc_handoff(*published, ipc_fd_handoff, ipc_fd_count);
    ipc_guard.disarm();
    s_compute_lock.unlock();

    uint64_t const STARTED_US = wki_now_us();
    uint64_t const CALLSITE = WOS_PERF_CALLSITE();
    perf_record_compute_begin(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, PATH_LEN_WIRE, CALLSITE);

    // Every byte in the transmitted prefix is assigned below; do not
    // pattern-initialize the unused bounded tail.
    auto* submit = reinterpret_cast<TaskSubmitPayload*>(BUF);
    submit->task_id = TASK_ID;
    submit->delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::VFS_REF);
    submit->prefer_inline = 0;
    submit->args_len = context_info.args_len;
    submit->argc = context_info.argc;
    submit->envc = context_info.envc;
    submit->cwd_len = context_info.cwd_len;
    submit->identity_len = context_info.identity_len;
    submit->ipc_fd_count = ipc_fd_count;
    submit->reserved = (transfer == nullptr            ? 0
                        : transfer->cutover != nullptr ? WKI_TASK_SUBMIT_EXEC_HANDOFF
                                                       : WKI_TASK_SUBMIT_SPAWN_TRANSITION) |
                       WKI_TASK_SUBMIT_GROUP_HOME | (FILESYSTEM != nullptr ? WKI_TASK_SUBMIT_FILESYSTEM : 0);

    // VFS_REF format: {path_len:u16, path[path_len], argv/envp/cwd, identity, policy, ipc_fd_entries[]}
    uint8_t* cursor = BUF + sizeof(TaskSubmitPayload);
    memcpy(cursor, &PATH_LEN_WIRE, sizeof(uint16_t));
    cursor += sizeof(uint16_t);
    memcpy(cursor, vfs_path, PATH_LEN_WIRE);
    cursor += PATH_LEN_WIRE;
    if (context_info.args_len > 0) {
        copy_submit_context_data(cursor, argv, envp, cwd);
        cursor += context_info.args_len;
    }
    if (context_info.identity_len > 0) {
        copy_submit_identity_data(cursor, local_task, resources, SUBMIT_CREDENTIALS, submit_filesystem.root, FULL_CREDENTIALS, exec_signals,
                                  ROUTED_HOME, FILESYSTEM);
        cursor += context_info.identity_len;
    }
    if (context_info.policy_len > 0) {
        serialize_task_vfs_rules(cursor, local_task);
        cursor += context_info.policy_len;
    }
    if (ipc_fd_count > 0 && ipc_fd_map != nullptr) {
        memcpy(cursor, ipc_fd_map, IPC_DATA_LEN);
    }
    if (transfer != nullptr) std::memcpy(cursor + IPC_DATA_LEN, transfer->exec_credentials, sizeof(TaskExecCredentialTransition));

    // V2 I-4: Set up async wait entry before sending
    WkiWaitEntry wait = {};
    s_compute_lock.lock();
    if (auto* task_ptr = find_submitted_task_any(TASK_ID); task_ptr != nullptr) {
        task_ptr->submission_started = true;
        task_ptr->response_wait_entry = &wait;
        task_ptr->response_consumer_wait_entry = &wait;
        task_ptr->response_pending.store(true, std::memory_order_release);
    }
    s_compute_lock.unlock();

    int const SEND_RET = send_task_submit_payload(target_node, SUBMIT_CHANNEL, TASK_ID, BUF, MSG_LEN);

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
            task_ptr->cancel_service_report(task_ptr->submission_started);
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
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_submit_ipc();
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, SEND_RET,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return fail_submit(task_transport_errno(SEND_RET));
    }

    int cutover_error = 0;
    int const WAIT_RC = wait_for_exec_submit(wait, WKI_TASK_SUBMIT_VFS_TIMEOUT_US, TASK_ID, *local_task, transfer, cutover_error);
    perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::SUBMIT_WAIT_RETURN, target_node, TASK_ID, WAIT_RC, 0, WOS_PERF_CALLSITE());
    uint8_t accept_status = 0;
    SubmittedIpcHandoffCleanup handoff_cleanup = {};
    WkiIpcSubmissionGuard accepted_handoff(local_task, nullptr, handoff_cleanup.handoff.data(), handoff_cleanup.count, WKI_NODE_INVALID);
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
            task_ptr->cancel_service_report(task_ptr->submission_started);
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        static_cast<void>(
            send_task_cancel_request(target_node, TASK_ID, WKI_SIGKILL_NUM, SUBMIT_CHANNEL.channel, SUBMIT_CHANNEL.generation));
        cleanup_submit_ipc();
        ker::mod::dbg::log("[WKI] VFS_REF submit wait failed: task_id=%u rc=%d (%s) timeout_us=%llu", TASK_ID, WAIT_RC, errno_name(WAIT_RC),
                           WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID, WAIT_RC,
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return fail_submit(cutover_error != 0 ? cutover_error : task_transport_errno(WAIT_RC));
    }

    if (task_ptr == nullptr || accept_status != static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
        uint8_t* discarded_output = nullptr;
        if (task_ptr != nullptr) {
            task_ptr->active = false;
            task_ptr->cancel_service_report(task_ptr->submission_started);
            restore_submitted_ipc_preparation_locked(*task_ptr, ipc_fd_handoff, ipc_fd_count, ipc_guard);
            discarded_output = consume_submitted_task_result_locked(task_ptr);
        }
        s_compute_lock.unlock();
        delete[] discarded_output;
        cleanup_submit_ipc();
        perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::SUBMIT_REJECTION_LOG_BEGIN, target_node, TASK_ID,
                                  -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status), 0, WOS_PERF_CALLSITE());
        ker::mod::dbg::log("[WKI] VFS_REF task rejected: task_id=%u status=%u", TASK_ID, accept_status);
        perf_record_compute_point(ker::mod::perf::WkiPerfComputeOp::SUBMIT_REJECTION_LOG_END, target_node, TASK_ID,
                                  -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status), 0, WOS_PERF_CALLSITE());
        perf_record_compute_end(ker::mod::perf::WkiPerfComputeOp::SUBMIT_VFS_REF, target_node, TASK_ID,
                                -static_cast<int32_t>(accept_status == 0 ? 1 : accept_status),
                                static_cast<uint32_t>(wki_now_us() - STARTED_US), PATH_LEN_WIRE, CALLSITE);
        return fail_submit(task_ptr == nullptr ? -EIO : task_rejection_errno(accept_status));
    }
    handoff_cleanup = take_submitted_ipc_handoff_locked(task_ptr);
    s_compute_lock.unlock();

    uint16_t const EXPECTED_HANDOFFS = handoff_cleanup.count;
    uint16_t const COMMITTED_HANDOFFS = wki_ipc_commit_task_fd_handoff(local_task, handoff_cleanup.handoff.data(), handoff_cleanup.count);
    handoff_cleanup.count = 0;
    wki_ipc_activate_task_fds(ipc_fd_map, ipc_fd_count, target_node);
    if (COMMITTED_HANDOFFS != EXPECTED_HANDOFFS) {
        ker::mod::dbg::log("[WKI] Accepted VFS_REF IPC fd handoff mismatch: task_id=%u committed=%u expected=%u", TASK_ID,
                           COMMITTED_HANDOFFS, EXPECTED_HANDOFFS);
    }
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
    std::optional<TaskResourceSnapshot> resource_baseline;
    std::optional<TaskResourceSnapshot> resource_completion;
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
            resource_baseline = submitted->resource_baseline;
            resource_completion = submitted->resource_completion;
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
        finalize_proxy_task(completed_proxy_ref, completed_exit_status, completed_output, completed_output_len, TASK_ID,
                            resource_baseline ? &*resource_baseline : nullptr, resource_completion ? &*resource_completion : nullptr);
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

void retire_user_signals_for_task(ker::mod::sched::task::Task* owner) {
    for (;;) {
        PendingUserSignal* found = nullptr;
        WkiWaitEntry* wait = nullptr;
        bool claimed = false;
        s_compute_lock.lock();
        for (auto& slot : s_user_signals) {
            if (slot.owner == owner && owner != nullptr) {
                found = &slot;
                wait = slot.consumer_wait;
                slot.response_wait = nullptr;
                claimed = wki_claim_op(wait);
                break;
            }
        }
        s_compute_lock.unlock();
        if (found == nullptr) return;
        if (claimed)
            wki_finish_claimed_op(wait, WKI_ERR_PEER_FENCED);
        else
            wki_quiesce_claimed_op(wait);
        // Keep consumer ownership discoverable until the claimant's final
        // stack access. No successor can reuse this slot in the meantime.
        s_compute_lock.lock();
        *found = {};
        s_compute_lock.unlock();
    }
}

void wki_remote_compute_cleanup_for_task(ker::mod::sched::task::Task* exiting_task) {
    if (exiting_task != nullptr) {
        retire_child_signal_for_task(*exiting_task);
        retire_child_queries_for_task(*exiting_task);
        retire_child_wait_transactions_for_task(*exiting_task);
    }
    retire_user_signals_for_task(exiting_task);
    if (exiting_task != nullptr) ker::syscall::process::cleanup_signal_targets(*exiting_task);
    if (exiting_task == nullptr) {
        return;
    }
    // Filesystem preparation can own the same proxy that the abandoned
    // submit/ISSUE stack was using. Quiesce that wait before closing its pins.
    if (exiting_task->pending_wki_filesystems != nullptr) wki_remote_vfs_cleanup_for_task(exiting_task->pid);
    while (exiting_task->pending_wki_filesystems != nullptr) exiting_task->pending_wki_filesystems->cancel();
    while (exiting_task->pending_wki_submit_buffers != nullptr) exiting_task->pending_wki_submit_buffers->cancel();
    wki_ipc_cancel_submission_guards(*exiting_task);
    wki_ipc_cancel_fd_snapshots(*exiting_task);

    // A task owns at most one synchronous submit/wait at a time, but process
    // exit cleanup is rare and scanning to exhaustion also retires older
    // direct handles and any proxy row whose local representative is exiting.
    for (;;) {
        std::array<WkiWaitEntry*, 4> waiters = {};
        size_t waiter_count = 0;
        SubmittedIpcCleanup ipc_cleanup = {};
        SubmittedIpcHandoffCleanup handoff_cleanup = {};
        uint8_t* discarded_output = nullptr;
        uint16_t target_node = WKI_NODE_INVALID;
        uint32_t task_id = 0;
        WkiChannel* submit_channel = nullptr;
        uint32_t submit_channel_generation = 0;
        bool cancel_remote = false;
        bool found = false;

        s_compute_lock.lock();
        for (auto* slot_ptr = s_submitted_task_head; slot_ptr != nullptr; slot_ptr = slot_ptr->storage_next) {
            auto& slot = *slot_ptr;
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
            handoff_cleanup = take_submitted_ipc_handoff_locked(&submitted);
            submitted.exit_status = -1;
            submitted.active = false;
            submitted.cancel_service_report(submitted.submission_started);
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
        release_submitted_ipc_handoff(handoff_cleanup);
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
        if (!candidate->has_exited && task_process_owner_pid(candidate) == owner_pid &&
            (!candidate->is_thread || (signum < 18 || signum > 22))) {
            candidate->signal_generate_pending_mask(MASK);
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
            // A prepared receiver is still owned by its submit worker and
            // must never be woken into scheduler visibility by peer cleanup.
            if (!running.published) continue;
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

auto wki_proxy_task_signal_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
                              const ker::mod::sched::session::Identity& caller_job, int signum) -> std::optional<int> {
    auto const ROUTE = target.wki_execution_identity();
    uint32_t const TASK_ID = ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id;
    if (TASK_ID == 0) return std::nullopt;
    auto* owner = ker::mod::sched::get_current_task();
    if (owner == nullptr) return -ESRCH;
    if (signum < 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    uint64_t const ADMISSION_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    for (;;) {
        s_compute_lock.lock();
        auto* row = find_submitted_task(TASK_ID);
        bool const WAITING =
            row != nullptr && row->local_task == &target && row->exec_cutover && row->response_pending.load(std::memory_order_acquire);
        s_compute_lock.unlock();
        if (!WAITING) break;
        if (wki_now_us() >= ADMISSION_DEADLINE) return -ETIMEDOUT;
        sleep_until_us(ADMISSION_DEADLINE, 1000);
    }
    TaskSignalRequest request{};
    request.task_id = TASK_ID;
    request.signum = signum;
    request.session_id = caller_job.session_id;
    request.origin = {.node = caller_job.origin.node, .reserved = 0, .boot = caller_job.origin.boot};
    request.credentials = ker::mod::security::credential_record::encode(caller);
    WkiWaitEntry wait{};
    wait.diag_name = "task_signal";
    PendingUserSignal* pending = nullptr;
    s_compute_lock.lock();
    auto* submitted = find_submitted_task(TASK_ID);
    if (submitted == nullptr || submitted->local_task != &target) {
        s_compute_lock.unlock();
        return -ESRCH;
    }
    if (submitted->signal_version != WKI_TASK_SIGNAL_VERSION) {
        s_compute_lock.unlock();
        return -ENOTSUP;
    }
    if (s_next_user_signal == UINT64_MAX) {
        s_compute_lock.unlock();
        return -EOVERFLOW;
    }
    for (auto& slot : s_user_signals) {
        if (slot.owner == nullptr) {
            pending = &slot;
            break;
        }
    }
    if (pending == nullptr) {
        s_compute_lock.unlock();
        return -EAGAIN;
    }
    request.request_id = ++s_next_user_signal;
    *pending = {.owner = owner,
                .response_wait = &wait,
                .consumer_wait = &wait,
                .channel = submitted->submit_channel,
                .request_id = request.request_id,
                .task_id = TASK_ID,
                .generation = submitted->submit_channel_generation,
                .node = submitted->target_node};
    auto* channel = pending->channel;
    uint32_t const GENERATION = pending->generation;
    uint16_t const NODE = pending->node;
    wait.diag_peer = NODE;
    wait.diag_arg0 = request.request_id;
    wait.diag_resource_id = TASK_ID;
    s_compute_lock.unlock();
    int const SENT = wki_send_on_channel_generation(NODE, channel, GENERATION, MsgType::TASK_SIGNAL_REQ, &request, sizeof(request));
    int waited = SENT;
    if (SENT == WKI_OK)
        waited = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    else {
        s_compute_lock.lock();
        pending->response_wait = nullptr;
        bool const CLAIMED = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, CLAIMED, SENT);
    }
    s_compute_lock.lock();
    int const RESULT = waited == WKI_OK ? pending->result : task_transport_errno(waited);
    *pending = {};
    s_compute_lock.unlock();
    return RESULT;
}

namespace {
auto concrete_signal_origin(ker::mod::sched::session::Origin origin) -> TaskSessionOrigin {
    return {.node = origin.node == 0 ? g_wki.my_node_id : origin.node,
            .reserved = 0,
            .boot = origin.node == 0 ? g_wki.local_boot_epoch : origin.boot};
}
auto signal_home_matches(ker::mod::sched::task::Task& task, const TaskSignalHome& requested) -> bool {
    auto const HOME = task.signal_home_identity(false);
    auto const ORIGIN = concrete_signal_origin(HOME.origin);
    return HOME.serial != 0 && HOME.serial == requested.serial && ORIGIN.node == requested.origin.node &&
           ORIGIN.boot == requested.origin.boot;
}
void execute_group_route(const GroupRouteRequest& request, const ker::mod::security::Snapshot& caller,
                         const ker::mod::sched::session::Identity& caller_job, GroupRouteResponse& response) {
    response = {.request_id = request.request_id, .boot = g_wki.local_boot_epoch};
    if (request.operation == static_cast<uint8_t>(GroupRouteOperation::ENUMERATE)) {
        response.result = ker::syscall::process::enumerate_signal_anchors(request.cursor, request.ceiling, response);
        if (response.result != 0) {
            response.count = 0;
            response.ceiling = 0;
            response.anchors = {};
        }
        return;
    }
    auto* target = ker::mod::sched::find_task_by_pid_safe(request.target_pid);
    if (target == nullptr) {
        s_compute_lock.lock();
        for (auto& running : g_running_remote_tasks) {
            if (!running.active || running.published || running.discard_completion || running.root_completion_queued ||
                running.local_pid != request.target_pid || running.task == nullptr)
                continue;
            if (!signal_home_matches(*running.task, request.home)) break;
            if (!running.group_signal_ready) {
                response.kind = static_cast<uint8_t>(GroupRouteResult::RETRY);
                s_compute_lock.unlock();
                return;
            }
            if (running.task->try_acquire()) target = running.task;
            break;
        }
        s_compute_lock.unlock();
    }
    if (target == nullptr) {
        response.result = -ESRCH;
        return;
    }
    if (!signal_home_matches(*target, request.home)) {
        target->release();
        response.result = -ESRCH;
        return;
    }
    ker::mod::sched::session::Identity group{
        .pgid = request.pgid, .group_origin = resolve_session_origin(request.group_origin, g_wki.my_node_id, g_wki.local_boot_epoch)};
    ker::mod::sched::session::Identity current{};
    uint32_t redirect_id = 0;
    uint64_t redirect_pid = 0;
    bool const QUERY = request.operation == static_cast<uint8_t>(GroupRouteOperation::QUERY);
    auto const RESULT = ker::syscall::process::signal_group_target_as(*target, caller, caller_job, group, QUERY, request.signum, current,
                                                                      redirect_id, redirect_pid);
    if (RESULT.has_value()) {
        response.result = *RESULT;
        if (*RESULT == 0 && QUERY) {
            response.target_pgid = current.pgid;
            response.group_origin = concrete_signal_origin(current.group_origin);
        }
        return;
    }
    s_compute_lock.lock();
    auto* row = find_submitted_task(redirect_id);
    uint16_t const NODE = row != nullptr && row->active ? row->target_node : WKI_NODE_INVALID;
    s_compute_lock.unlock();
    uint32_t const BOOT = NODE != WKI_NODE_INVALID ? wki_peer_remote_boot_epoch_snapshot(NODE) : 0;
    if (NODE == WKI_NODE_INVALID || BOOT == 0 || redirect_pid == 0) {
        response.result = -ESRCH;
        return;
    }
    response.kind = static_cast<uint8_t>(GroupRouteResult::REDIRECT);
    response.destination = {.node = NODE, .boot = BOOT};
    response.target_pid = redirect_pid;
}

// Caller owns the running row's Task lifetime (normally s_compute_lock).
auto append_running_signal_receiver(const RunningRemoteTask& running, const GroupRouteRequest& request, GroupReceiverResponse& response)
    -> int {
    if (!running.active || running.task == nullptr || running.discard_completion || running.root_completion_queued ||
        (!running.published && !running.group_signal_ready) || running.task->has_exited ||
        running.task->type != ker::mod::sched::task::TaskType::PROCESS || running.task->is_thread ||
        running.task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE ||
        running.task->signal_admission_identity().remote)
        return 0;
    return ker::syscall::process::append_signal_receiver(*running.task, request.cursor, request.ceiling, response);
}

void collect_group_receivers(const GroupRouteRequest& request, GroupReceiverResponse& response) {
    response = {.request_id = request.request_id, .boot = g_wki.local_boot_epoch};
    response.result = ker::syscall::process::enumerate_signal_receivers(request.cursor, request.ceiling, response);
    if (response.result == 0) {
        // Scan published imports too: publication can move a Task into the
        // registry after the first snapshot. Receiver serial deduplicates overlap.
        s_compute_lock.lock();
        for (auto& running : g_running_remote_tasks) {
            response.result = append_running_signal_receiver(running, request, response);
            if (response.result != 0) break;
        }
        s_compute_lock.unlock();
    }
    if (response.result != 0) {
        response.count = 0;
        response.ceiling = 0;
        response.receivers = {};
    }
}

template <typename Response>
auto group_route_rpc(uint16_t node, uint32_t expected_boot, GroupRouteRequest request, const ker::mod::security::Snapshot& caller,
                     const ker::mod::sched::session::Identity& caller_job, Response& response) -> int {
    constexpr bool RECEIVERS = std::is_same_v<Response, GroupReceiverResponse>;
    if (node == g_wki.my_node_id) {
        if (expected_boot != g_wki.local_boot_epoch) return -ESTALE;
        if constexpr (RECEIVERS)
            collect_group_receivers(request, response);
        else
            execute_group_route(request, caller, caller_job, response);
        return response.result;
    }
    auto* owner = ker::mod::sched::get_current_task();
    if (owner == nullptr) return -ESRCH;
    WkiWaitEntry wait{};
    wait.diag_name = "group_route";
    wait.diag_peer = node;
    PendingUserSignal* pending = nullptr;
    SubmitterChannelToken token{};
    {
        PeerLifecycleLease lifecycle;
        if (!lifecycle.acquire(node)) return -EHOSTUNREACH;
        auto* peer = wki_peer_find(node);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) return -EHOSTUNREACH;
        if (expected_boot == 0 || wki_peer_remote_boot_epoch_snapshot(node) != expected_boot) return -ESTALE;
        constexpr uint32_t RESOURCE = RECEIVERS ? WKI_GROUP_RECEIVER_RESOURCE_ID : WKI_GROUP_ROUTE_RESOURCE_ID;
        if (wki_resource_generation_snapshot(node, ResourceType::COMPUTE, RESOURCE) == 0) return -ENOTSUP;
        token = capture_submitter_channel(node);
        if (token.channel == nullptr) return -ENOTCONN;
        s_compute_lock.lock();
        if (s_next_user_signal == UINT64_MAX) {
            s_compute_lock.unlock();
            return -EOVERFLOW;
        }
        for (auto& slot : s_user_signals)
            if (slot.owner == nullptr) {
                pending = &slot;
                break;
            }
        if (pending == nullptr) {
            s_compute_lock.unlock();
            return -EAGAIN;
        }
        request.request_id = ++s_next_user_signal;
        *pending = {.owner = owner,
                    .response_wait = &wait,
                    .consumer_wait = &wait,
                    .channel = token.channel,
                    .request_id = request.request_id,
                    .generation = token.generation,
                    .node = node,
                    .source_boot = expected_boot,
                    .route_operation = !RECEIVERS,
                    .receiver_operation = RECEIVERS,
                    .route_request = request};
        wait.diag_arg0 = request.request_id;
        s_compute_lock.unlock();
    }
    constexpr MsgType TYPE = RECEIVERS ? MsgType::GROUP_RECEIVER_REQ : MsgType::GROUP_ROUTE_REQ;
    int const SENT = wki_send_on_channel_generation(node, token.channel, token.generation, TYPE, &request, sizeof(request));
    int waited = SENT;
    if (SENT == WKI_OK)
        waited = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    else {
        s_compute_lock.lock();
        pending->response_wait = nullptr;
        bool const CLAIMED = wki_claim_op(&wait);
        s_compute_lock.unlock();
        finish_or_wait_for_cancelled_waiter(wait, CLAIMED, SENT);
        waited = wait.result;
    }
    s_compute_lock.lock();
    int const RESULT = waited == WKI_OK ? pending->result : task_transport_errno(waited);
    if (waited == WKI_OK) {
        if constexpr (RECEIVERS)
            response = pending->receiver_response;
        else
            response = pending->route_response;
    }
    *pending = {};
    s_compute_lock.unlock();
    return RESULT;
}

void group_signal_error(int result, int& error) {
    if (result != 0 && result != -ESRCH && (error == -ESRCH || result == -EPERM)) error = result;
}
}  // namespace

namespace {
struct GroupRouteTarget {
    TaskSignalHome home{};
    uint16_t node = 0;
    uint32_t boot = 0;
    uint64_t pid = 0;
};

// Selection is fixed before query/delivery. Later observations of the same
// logical process never replace its first route, including after a timeout.
class GroupRouteTargets {
   public:
    GroupRouteTargets() = default;
    GroupRouteTargets(const GroupRouteTargets&) = delete;
    auto operator=(const GroupRouteTargets&) -> GroupRouteTargets& = delete;
    ~GroupRouteTargets() { clear(); }

    auto view() const -> std::span<const GroupRouteTarget> { return {targets, count}; }
    auto insert(const GroupRouteTarget& target, bool fail_allocation = false) -> int {
        for (auto const& selected : view()) {
            if (selected.home.serial == target.home.serial && selected.home.origin.node == target.home.origin.node &&
                selected.home.origin.boot == target.home.origin.boot)
                return 0;
        }
        if (count == capacity) {
            if (capacity > SIZE_MAX / sizeof(GroupRouteTarget) / 2) return -EOVERFLOW;
            size_t const LARGER_CAPACITY = capacity * 2;
            auto* larger = fail_allocation ? nullptr : new (std::nothrow) GroupRouteTarget[LARGER_CAPACITY];
            if (larger == nullptr) return -ENOMEM;
            std::copy_n(targets, count, larger);
            auto* previous = targets;
            targets = larger;
            capacity = LARGER_CAPACITY;
            if (previous != inline_targets.data()) delete[] previous;
        }
        targets[count++] = target;
        return 0;
    }
    void clear() noexcept {
        auto* previous = targets;
        targets = inline_targets.data();
        count = 0;
        capacity = inline_targets.size();
        if (previous != inline_targets.data()) delete[] previous;
    }

   private:
    std::array<GroupRouteTarget, 32> inline_targets{};
    GroupRouteTarget* targets = inline_targets.data();
    size_t count = 0;
    size_t capacity = inline_targets.size();
};

auto group_signal_with_local_targets(const ker::mod::security::Snapshot& caller, const ker::mod::sched::session::Identity& caller_job,
                                     uint64_t pgid, bool own_group, int signum, std::span<ker::mod::sched::task::Task* const> local_targets,
                                     GroupRouteTargets& targets) -> int {
    using namespace ker::mod::sched;
    if (signum < 0 || std::cmp_greater(signum, task::Task::MAX_SIGNALS)) return -EINVAL;
    if (pgid == 0) return -ESRCH;
    auto* self = get_current_task();
    if (self == nullptr) return -ESRCH;
    auto const SELF_HOME = self->signal_home_identity();
    if (SELF_HOME.serial == 0) return -EOVERFLOW;
    auto const SELF_ORIGIN = concrete_signal_origin(SELF_HOME.origin);
    struct HomeNode {
        uint16_t node;
        uint32_t boot;
    };
    using Target = GroupRouteTarget;
    auto local_home = [](task::Task& target) {
        auto const HOME = target.signal_home_identity(false);
        return TaskSignalHome{.origin = concrete_signal_origin(HOME.origin), .serial = HOME.serial};
    };
    std::array<HomeNode, WKI_MAX_PEERS + 1> homes{};
    size_t count = 0;
    for (auto& peer : g_wki.peers) {
        peer.lock.lock();
        bool const CONNECTED = peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED;
        uint16_t const NODE = peer.node_id;
        uint32_t const BOOT = peer.remote_boot_epoch;
        peer.lock.unlock();
        if (!CONNECTED) continue;
        if (BOOT == 0) return -EACCES;
        if (wki_resource_generation_snapshot(NODE, ResourceType::COMPUTE, WKI_GROUP_ROUTE_RESOURCE_ID) == 0) return -ENOTSUP;
        homes[count++] = {.node = NODE, .boot = BOOT};
    }
    homes[count++] = {.node = g_wki.my_node_id, .boot = g_wki.local_boot_epoch};
    GroupRouteRequest base{.pgid = pgid,
                           .caller_pgid = caller_job.pgid,
                           .session_id = caller_job.session_id,
                           .session_origin = concrete_signal_origin(caller_job.origin),
                           .group_origin = concrete_signal_origin(caller_job.group_origin),
                           .credentials = ker::mod::security::credential_record::encode(caller)};
    auto collect = [&]() {
        int error = -ESRCH;
        // Start with the fixed local cohort, including imported receivers.
        // A retained PID/home is only a route: admission rechecks migration.
        for (auto* target : local_targets) {
            int const STORED = targets.insert(
                Target{.home = local_home(*target), .node = g_wki.my_node_id, .boot = g_wki.local_boot_epoch, .pid = target->pid});
            if (STORED != 0) return STORED;
        }
        // Prefer a capable node's current receivers over historical home
        // routes. Each node maps the original caller before enumeration.
        for (size_t i = 0; i < count; ++i) {
            auto const& receiver = homes[i];
            if (receiver.node == g_wki.my_node_id ||
                wki_resource_generation_snapshot(receiver.node, ResourceType::COMPUTE, WKI_GROUP_RECEIVER_RESOURCE_ID) == 0)
                continue;
            uint64_t cursor = 0;
            uint64_t ceiling = 0;
            for (;;) {
                auto request = base;
                request.operation = static_cast<uint8_t>(GroupRouteOperation::ENUMERATE);
                request.cursor = cursor;
                request.ceiling = ceiling;
                GroupReceiverResponse page{};
                int const RESULT = group_route_rpc(receiver.node, receiver.boot, request, caller, caller_job, page);
                if (RESULT != 0) {
                    group_signal_error(RESULT, error);
                    break;
                }
                ceiling = page.ceiling;
                for (size_t index = 0; index < page.count; ++index) {
                    auto const& CURRENT = page.receivers[index];
                    int const STORED =
                        targets.insert(Target{.home = CURRENT.home, .node = receiver.node, .boot = receiver.boot, .pid = CURRENT.pid});
                    if (STORED != 0) return STORED;
                }
                if (page.count == 0) break;
                cursor = page.receivers[page.count - 1].serial;
                if (page.count < page.receivers.size() || cursor >= ceiling) break;
            }
        }
        for (size_t i = 0; i < count; ++i) {
            auto const& home = homes[i];
            uint64_t cursor = 0;
            uint64_t ceiling = 0;
            for (;;) {
                auto request = base;
                request.operation = static_cast<uint8_t>(GroupRouteOperation::ENUMERATE);
                request.cursor = cursor;
                request.ceiling = ceiling;
                GroupRouteResponse page{};
                int const RESULT = group_route_rpc(home.node, home.boot, request, caller, caller_job, page);
                if (RESULT != 0) {
                    group_signal_error(RESULT, error);
                    break;
                }
                ceiling = page.ceiling;
                for (size_t index = 0; index < page.count; ++index) {
                    auto const& ANCHOR = page.anchors[index];
                    int const STORED = targets.insert(
                        Target{.home = {.origin = {.node = home.node, .reserved = 0, .boot = home.boot}, .serial = ANCHOR.serial},
                               .node = home.node,
                               .boot = home.boot,
                               .pid = ANCHOR.pid});
                    if (STORED != 0) return STORED;
                }
                if (page.count == 0) break;
                cursor = page.anchors[page.count - 1].serial;
                if (page.count < page.anchors.size() || cursor >= ceiling) break;
            }
        }
        return error;
    };
    int const ENUM_ERROR = collect();
    // Incomplete storage cannot define a target set. No query or signal has
    // run yet, so capacity failure has no partial process-control effect.
    if (ENUM_ERROR == -ENOMEM || ENUM_ERROR == -EOVERFLOW) return ENUM_ERROR;
    auto follow = [&](const Target& target, bool query, GroupRouteResponse& response) {
        auto request = base;
        request.operation = static_cast<uint8_t>(query ? GroupRouteOperation::QUERY : GroupRouteOperation::DELIVER);
        request.signum = query ? 0 : signum;
        request.home = target.home;
        request.target_pid = target.pid;
        uint16_t node = target.node;
        uint32_t boot = target.boot;
        uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
        unsigned redirects = 0;
        for (;;) {
            int const RESULT = group_route_rpc(node, boot, request, caller, caller_job, response);
            if (RESULT != 0) return RESULT;
            if (response.kind == static_cast<uint8_t>(GroupRouteResult::FINAL)) return 0;
            if (wki_now_us() >= DEADLINE) return -ETIMEDOUT;
            if (response.kind == static_cast<uint8_t>(GroupRouteResult::REDIRECT)) {
                if (++redirects > 64) return -ELOOP;
                node = response.destination.node;
                boot = response.destination.boot;
                request.target_pid = response.target_pid;
            } else {
                // An explicit retry response proves no admission/effect. A
                // transport timeout is never retried as an application call.
                sleep_until_us(DEADLINE, 1000);
            }
        }
    };
    if (!own_group) {
        session::GroupCandidates candidates;
        int query_error = -ESRCH;
        for (auto const& target : targets.view()) {
            GroupRouteResponse response{};
            int const RESULT = follow(target, true, response);
            if (RESULT != 0) {
                group_signal_error(RESULT, query_error);
                continue;
            }
            if (response.target_pgid != pgid) continue;
            auto const ORIGIN = resolve_session_origin(response.group_origin, target.node, target.boot);
            candidates.observe(ORIGIN, {}, pgid == caller_job.pgid ? std::optional{caller_job.group_origin} : std::nullopt);
        }
        group_signal_error(ENUM_ERROR, query_error);
        if ((candidates.flags & session::GroupCandidates::LOCAL) == 0 && query_error != -ESRCH) return query_error;
        auto const ORIGIN = candidates.resolve(pgid == caller_job.pgid ? std::optional{caller_job.group_origin} : std::nullopt);
        if (!ORIGIN) return -ESRCH;
        base.group_origin = concrete_signal_origin(*ORIGIN);
    }
    size_t accepted = 0;
    int error = -ESRCH;
    Target deferred_target{};
    auto deliver = [&](const Target& target) {
        GroupRouteResponse response{};
        int const RESULT = follow(target, false, response);
        accepted += RESULT == 0 ? 1 : 0;
        group_signal_error(RESULT, error);
    };
    for (auto const& target : targets.view()) {
        // A remotely running caller's home can be another node. Delay its
        // logical process, not merely this node's whole local pass.
        if (target.home.origin.node == SELF_ORIGIN.node && target.home.origin.boot == SELF_ORIGIN.boot &&
            target.home.serial == SELF_HOME.serial) {
            deferred_target = target;
        } else
            deliver(target);
    }
    group_signal_error(ENUM_ERROR, error);
    if (deferred_target.home.serial != 0) deliver(deferred_target);
    return accepted != 0 ? 0 : error;
}
}  // namespace

auto wki_group_signal_as(const ker::mod::security::Snapshot& caller, const ker::mod::sched::session::Identity& caller_job, uint64_t pgid,
                         bool own_group, int signum) -> int {
    if (signum < 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    if (pgid == 0 || ker::mod::sched::get_current_task() == nullptr) return -ESRCH;
    GroupRouteTargets targets;
    struct Context {
        const ker::mod::security::Snapshot& caller;
        const ker::mod::sched::session::Identity& job;
        uint64_t pgid;
        bool own;
        int signum;
        GroupRouteTargets& targets;
    } context{caller, caller_job, pgid, own_group, signum, targets};
    return ker::syscall::process::with_local_signal_targets(
        [](std::span<ker::mod::sched::task::Task* const> local_targets, void* opaque) {
            auto const& INPUT = *static_cast<Context*>(opaque);
            return group_signal_with_local_targets(INPUT.caller, INPUT.job, INPUT.pgid, INPUT.own, INPUT.signum, local_targets,
                                                   INPUT.targets);
        },
        &context, [](void* opaque) noexcept { static_cast<Context*>(opaque)->targets.clear(); });
}

void wki_defer_child_signal(ker::mod::sched::task::Task& target, uint16_t hops) {
    if (hops > WKI_CHILD_SIGNAL_HOPS) return;
    uint64_t const FLAGS = s_child_signal_lock.lock_irqsave();
    bool const QUEUED = s_child_signal_queue.push(target, hops);
    s_child_signal_lock.unlock_irqrestore(FLAGS);
    if (QUEUED) notify_proxy_exit_work();
}

auto wki_proxy_task_parent_death(ker::mod::sched::task::Task& target, uint16_t hops) -> std::optional<int> {
    auto const ROUTE = target.wki_execution_identity();
    uint32_t const TASK_ID = ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id;
    if (TASK_ID == 0) return std::nullopt;
    if (hops == 0 || hops > WKI_PARENT_DEATH_HOPS) return -ELOOP;
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    for (;;) {
        s_compute_lock.lock();
        auto* row = find_submitted_task(TASK_ID);
        bool const WAITING =
            row != nullptr && row->local_task == &target && row->exec_cutover && row->response_pending.load(std::memory_order_acquire);
        s_compute_lock.unlock();
        if (!WAITING) break;
        if (wki_now_us() >= DEADLINE) return -ETIMEDOUT;
        sleep_until_us(DEADLINE, 1000);
    }
    s_compute_lock.lock();
    auto* submitted = find_submitted_task(TASK_ID);
    if (submitted == nullptr || submitted->local_task != &target) {
        s_compute_lock.unlock();
        return -ESRCH;
    }
    uint16_t const NODE = submitted->target_node;
    auto* channel = submitted->submit_channel;
    uint32_t const GENERATION = submitted->submit_channel_generation;
    s_compute_lock.unlock();
    if (wki_resource_generation_snapshot(NODE, ResourceType::COMPUTE, WKI_PARENT_DEATH_RESOURCE_ID) == 0) return -ENOTSUP;
    TaskParentDeath const REQUEST{.task_id = TASK_ID, .hops = static_cast<uint16_t>(hops - 1)};
    int const SENT = wki_send_on_channel_generation(NODE, channel, GENERATION, MsgType::TASK_PARENT_DEATH, &REQUEST, sizeof(REQUEST));
    return SENT == WKI_OK ? 0 : -EHOSTUNREACH;
}

auto wki_proxy_task_terminal_signal(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum,
                                    uint16_t hops) -> std::optional<int> {
    auto const ROUTE = target.wki_execution_identity();
    uint32_t const TASK_ID = ROUTE.signal_task_id != 0 ? ROUTE.signal_task_id : ROUTE.proxy_task_id;
    if (TASK_ID == 0) return std::nullopt;
    if (hops == 0 || hops > WKI_TERMINAL_SIGNAL_HOPS) return -ELOOP;
    if (signum <= 0 || std::cmp_greater(signum, ker::mod::sched::task::Task::MAX_SIGNALS)) return -EINVAL;
    uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_TASK_SUBMIT_VFS_TIMEOUT_US);
    for (;;) {
        s_compute_lock.lock();
        auto* row = find_submitted_task(TASK_ID);
        bool const WAITING =
            row != nullptr && row->local_task == &target && row->exec_cutover && row->response_pending.load(std::memory_order_acquire);
        s_compute_lock.unlock();
        if (!WAITING) break;
        if (wki_now_us() >= DEADLINE) return -ETIMEDOUT;
        sleep_until_us(DEADLINE, 1000);
    }
    s_compute_lock.lock();
    auto* submitted = find_submitted_task(TASK_ID);
    if (submitted == nullptr || submitted->local_task != &target) {
        s_compute_lock.unlock();
        return -ESRCH;
    }
    uint16_t const NODE = submitted->target_node;
    auto* channel = submitted->submit_channel;
    uint32_t const GENERATION = submitted->submit_channel_generation;
    s_compute_lock.unlock();
    if (wki_resource_generation_snapshot(NODE, ResourceType::COMPUTE, WKI_TERMINAL_SIGNAL_RESOURCE_ID) == 0) return -ENOTSUP;
    TaskTerminalSignal const REQUEST{
        .task_id = TASK_ID,
        .signum = signum,
        .hops = static_cast<uint16_t>(hops - 1),
        .session_id = expected.session_id,
        .pgid = expected.pgid,
        .origins = {.session = concrete_signal_origin(expected.origin), .group = concrete_signal_origin(expected.group_origin)}};
    int const SENT = wki_send_on_channel_generation(NODE, channel, GENERATION, MsgType::TASK_TERMINAL_SIGNAL, &REQUEST, sizeof(REQUEST));
    return SENT == WKI_OK ? 0 : -EHOSTUNREACH;
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
    if (task == nullptr) return false;
    auto const IDENTITY = task->wki_execution_identity();
    return wki_submitted_task_remote_info(IDENTITY.proxy_task_id, target_node, hostname, hostname_size);
}

auto wki_submitted_task_remote_info(uint32_t task_id, uint16_t* target_node, char* hostname, size_t hostname_size) -> bool {
    if (task_id == 0) return false;

    uint16_t node = WKI_NODE_INVALID;
    s_compute_lock.lock();
    SubmittedTask const* submitted = find_submitted_task_any(task_id);
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
        std::array<char, WKI_HOSTNAME_MAX> peer_hostname{};
        if (wki_peer_copy_hostname(node, peer_hostname)) {
            std::strncpy(hostname, peer_hostname.data(), hostname_size - 1);
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

namespace {

// s_compute_lock must be held by caller.
auto active_submitted_tasks_for_node_locked(uint16_t node_id) -> size_t {
    size_t active = 0;
    for (const auto* slot_ptr = s_submitted_task_head; slot_ptr != nullptr; slot_ptr = slot_ptr->storage_next) {
        const auto& slot = *slot_ptr;
        if (slot.occupied && slot.task.active && slot.task.target_node == node_id) {
            active++;
        }
    }
    return active;
}

auto preferred_remote_placement_score(const RemoteNodeLoad& load, size_t active_submissions) -> uint64_t {
    uint64_t const CPUS = std::max<uint64_t>(load.num_cpus, 1);
    uint64_t const INFLIGHT = active_submissions + load.placement_reservations;
    uint64_t const INFLIGHT_PRESSURE = (INFLIGHT * 1000ULL) / CPUS;
    return static_cast<uint64_t>(load.avg_load_pct) + WKI_REMOTE_PLACEMENT_PENALTY + INFLIGHT_PRESSURE;
}

constexpr uint16_t WKI_BALANCED_FREE_MEM_RESERVE = 512;
constexpr uint16_t WKI_BALANCED_FREE_MEM_COMFORT = 4096;
constexpr uint64_t WKI_BALANCED_QUEUE_HEADROOM_NUMERATOR = 5;
constexpr uint64_t WKI_BALANCED_QUEUE_HEADROOM_DENOMINATOR = 4;

auto balanced_memory_eligible(uint16_t free_mem) -> bool { return free_mem >= WKI_BALANCED_FREE_MEM_RESERVE; }

auto balanced_memory_pressure(uint16_t free_mem) -> uint64_t {
    if (free_mem >= WKI_BALANCED_FREE_MEM_COMFORT) {
        return 0;
    }
    if (!balanced_memory_eligible(free_mem)) {
        return 1000;
    }
    constexpr uint64_t MAX_HEADROOM_PENALTY = 250;
    constexpr uint64_t HEADROOM_RANGE = WKI_BALANCED_FREE_MEM_COMFORT - WKI_BALANCED_FREE_MEM_RESERVE;
    return (static_cast<uint64_t>(WKI_BALANCED_FREE_MEM_COMFORT - free_mem) * MAX_HEADROOM_PENALTY) / HEADROOM_RANGE;
}

auto balanced_inflight_pressure(uint16_t num_cpus, uint64_t inflight) -> uint64_t {
    uint64_t const CPUS = std::max<uint64_t>(num_cpus, 1);
    // Round up so every system retains the advertised fractional headroom;
    // truncation disproportionately removes the reserve from smaller peers.
    uint64_t const QUEUE_CAPACITY =
        std::max<uint64_t>(((CPUS * WKI_BALANCED_QUEUE_HEADROOM_NUMERATOR) + WKI_BALANCED_QUEUE_HEADROOM_DENOMINATOR - 1) /
                               WKI_BALANCED_QUEUE_HEADROOM_DENOMINATOR,
                           1);
    // Keep a bounded runnable reserve for tasks that sleep on remote VFS or
    // IPC. The extra point makes a full reserve strictly more expensive than
    // a saturated candidate, so selection cannot grow beyond the reserve on
    // an equal-score tie.
    return inflight == 0 ? 0 : 1 + ((inflight * 1000ULL) / QUEUE_CAPACITY);
}

auto balanced_placement_score(uint16_t load_pct, uint16_t num_cpus, uint16_t free_mem, uint64_t inflight) -> uint64_t {
    uint64_t const INFLIGHT_PRESSURE = balanced_inflight_pressure(num_cpus, inflight);
    return std::max<uint64_t>(load_pct, INFLIGHT_PRESSURE) + balanced_memory_pressure(free_mem);
}

auto balanced_candidate_is_starved(uint64_t assignment_seq, uint64_t last_assignment_seq, uint64_t eligible_capacity,
                                   uint64_t candidate_capacity) -> bool {
    uint64_t const CAPACITY = std::max<uint64_t>(candidate_capacity, 1);
    uint64_t const STARVATION_WINDOW = std::max<uint64_t>((eligible_capacity + CAPACITY - 1) / CAPACITY, 1);
    return assignment_seq >= last_assignment_seq && assignment_seq - last_assignment_seq >= STARVATION_WINDOW;
}

// s_compute_lock must be held by caller. WKI_NODE_INVALID denotes the local
// node. Measured CPU utilization accounts for work that is actually consuming
// a system, while sample-window assignment debt bridges the delay until the
// next load report. Accepted remote submissions also bridge a report that
// resets assignment debt before those tasks finish. Use the larger signal
// rather than adding both: runnable placement controllers and accepted tasks
// otherwise describe the same future CPU work twice and make an idle submitter
// look saturated during a burst. A failed remote submission rolls its debt
// back through
// BalancedRemoteAssignment.
auto wki_balanced_node(uint16_t local_load, uint16_t local_cpus, uint16_t local_free_mem) -> BalancedPlacement {
    if (g_balanced_local_sample_us != g_cached_local_load_update_us) {
        g_balanced_local_sample_us = g_cached_local_load_update_us;
        g_balanced_local_reservations = 0;
    }

    struct Candidate {
        uint16_t node_id = WKI_NODE_INVALID;
        RemoteNodeLoad* remote_load = nullptr;
        uint64_t score = UINT64_MAX;
        uint64_t last_assignment_seq = 0;
        uint64_t capacity = 1;
    };
    std::array<Candidate, WKI_MAX_PEERS + 1> eligible = {};
    size_t eligible_count = 0;
    uint64_t eligible_capacity = 0;
    uint64_t const LOCAL_PRESSURE = g_balanced_local_reservations;
    if (balanced_memory_eligible(local_free_mem)) {
        eligible.at(eligible_count++) = {
            .node_id = WKI_NODE_INVALID,
            .remote_load = nullptr,
            .score = balanced_placement_score(local_load, local_cpus, local_free_mem, LOCAL_PRESSURE),
            .last_assignment_seq = g_balanced_local_last_assignment_seq,
            .capacity = std::max<uint16_t>(local_cpus, 1),
        };
        eligible_capacity += std::max<uint16_t>(local_cpus, 1);
    }
    uint64_t const NOW = wki_now_us();

    for (auto& rl : g_remote_loads) {
        if (!rl.valid || NOW - rl.last_update_us > (WKI_LOAD_REPORT_INTERVAL_US * 2) || !balanced_memory_eligible(rl.free_mem_pages)) {
            continue;
        }
        auto* peer = wki_peer_find(rl.node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            continue;
        }
        uint64_t const KNOWN_ACTIVE = active_submitted_tasks_for_node_locked(rl.node_id);
        uint64_t const REMOTE_PRESSURE = std::max<uint64_t>(KNOWN_ACTIVE, rl.balanced_assignments);
        uint64_t const SCORE = balanced_placement_score(rl.avg_load_pct, rl.num_cpus, rl.free_mem_pages, REMOTE_PRESSURE);
        if (eligible_count < eligible.size()) {
            eligible.at(eligible_count++) = {
                .node_id = rl.node_id,
                .remote_load = &rl,
                .score = SCORE,
                .last_assignment_seq = rl.balanced_last_assignment_seq,
                .capacity = std::max<uint16_t>(rl.num_cpus, 1),
            };
            eligible_capacity += std::max<uint16_t>(rl.num_cpus, 1);
        }
    }

    if (eligible_count == 0) {
        return {};
    }

    std::array<size_t, WKI_MAX_PEERS + 1> candidates = {};
    size_t candidate_count = 0;
    uint64_t oldest_age = 0;
    for (size_t i = 0; i < eligible_count; ++i) {
        Candidate const& candidate = eligible.at(i);
        if (!balanced_candidate_is_starved(g_balanced_assignment_seq, candidate.last_assignment_seq, eligible_capacity,
                                           candidate.capacity)) {
            continue;
        }
        uint64_t const AGE = g_balanced_assignment_seq - candidate.last_assignment_seq;
        if (AGE > oldest_age) {
            oldest_age = AGE;
            candidate_count = 0;
        }
        if (AGE == oldest_age) {
            candidates.at(candidate_count++) = i;
        }
    }

    if (candidate_count == 0) {
        uint64_t best_score = UINT64_MAX;
        for (size_t i = 0; i < eligible_count; ++i) {
            uint64_t const SCORE = eligible.at(i).score;
            if (SCORE < best_score) {
                best_score = SCORE;
                candidate_count = 0;
            }
            if (SCORE == best_score) {
                candidates.at(candidate_count++) = i;
            }
        }
    }

    Candidate& selected = eligible.at(candidates.at(g_balanced_placement_cursor % candidate_count));
    ++g_balanced_placement_cursor;
    if (g_balanced_assignment_seq == UINT64_MAX) {
        g_balanced_assignment_seq = 0;
        g_balanced_local_last_assignment_seq = 0;
        for (auto& load : g_remote_loads) {
            load.balanced_last_assignment_seq = 0;
        }
    }
    uint64_t const ASSIGNMENT_SEQ = ++g_balanced_assignment_seq;
    if (selected.node_id == WKI_NODE_INVALID) {
        g_balanced_local_last_assignment_seq = ASSIGNMENT_SEQ;
        if (g_balanced_local_reservations != UINT32_MAX) {
            ++g_balanced_local_reservations;
        }
        return {.node_id = WKI_NODE_INVALID, .found = true};
    }

    RemoteNodeLoad* const LOAD = selected.remote_load;
    if (LOAD != nullptr && LOAD->balanced_assignments != UINT32_MAX) {
        ++LOAD->balanced_assignments;
    }
    if (LOAD != nullptr) {
        LOAD->balanced_last_assignment_seq = ASSIGNMENT_SEQ;
    }
    return {.node_id = selected.node_id, .found = true};
}

// s_compute_lock must be held by caller. The returned node owns one transient
// reservation until PreferredRemoteReservation releases it on function exit.
auto wki_preferred_remote_node() -> uint16_t {
    std::array<RemoteNodeLoad*, WKI_MAX_PEERS> candidates = {};
    size_t candidate_count = 0;
    uint64_t best_score = UINT64_MAX;
    uint64_t const NOW = wki_now_us();

    for (auto& rl : g_remote_loads) {
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
        uint64_t const SCORE = preferred_remote_placement_score(rl, active_submitted_tasks_for_node_locked(rl.node_id));
        if (SCORE < best_score) {
            best_score = SCORE;
            candidate_count = 0;
        }
        if (SCORE == best_score && candidate_count < candidates.size()) {
            candidates.at(candidate_count++) = &rl;
        }
    }

    if (candidate_count != 0) {
        RemoteNodeLoad* const SELECTED = candidates.at(g_preferred_remote_cursor % candidate_count);
        ++g_preferred_remote_cursor;
        if (SELECTED->placement_reservations != UINT32_MAX) {
            SELECTED->placement_reservations++;
        }
        return SELECTED->node_id;
    }

    return WKI_NODE_INVALID;
}

void wki_release_preferred_remote_node(uint16_t node_id) {
    if (node_id == WKI_NODE_INVALID) {
        return;
    }

    s_compute_lock.lock();
    RemoteNodeLoad* const LOAD = find_remote_load(node_id);
    if (LOAD != nullptr && LOAD->placement_reservations != 0) {
        LOAD->placement_reservations--;
    }
    s_compute_lock.unlock();
}

PreferredRemoteReservation::~PreferredRemoteReservation() { wki_release_preferred_remote_node(node_id); }

void wki_rollback_balanced_remote_assignment(uint16_t node_id) {
    if (node_id == WKI_NODE_INVALID) {
        return;
    }

    s_compute_lock.lock();
    RemoteNodeLoad* const LOAD = find_remote_load(node_id);
    if (LOAD != nullptr && LOAD->balanced_assignments != 0) {
        LOAD->balanced_assignments--;
    }
    s_compute_lock.unlock();
}

BalancedRemoteAssignment::~BalancedRemoteAssignment() { wki_rollback_balanced_remote_assignment(node_id); }

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
        release_fragmented_task_submits(node_id, channel, generation);
    } else {
        release_fragmented_task_submits(node_id);
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
    if (s_child_wait_cleanup_pending.load(std::memory_order_acquire) || s_child_query_cleanup_pending.load(std::memory_order_acquire))
        notify_proxy_exit_work();
}

namespace {

auto submitted_task_matches_cleanup_locked(const SubmittedTask& submitted, uint16_t node_id, RemoteComputeCleanupScope scope) -> bool {
    if ((!submitted.active && !submitted.service_report_pending) || submitted.target_node != node_id) {
        return false;
    }
    return scope == RemoteComputeCleanupScope::ALL ||
           !compute_submit_channel_token_matches(node_id, submitted.submit_channel, submitted.submit_channel_generation);
}

void fail_user_signals_for_peer(uint16_t node_id, RemoteComputeCleanupScope scope) {
    std::array<WkiWaitEntry*, 64> waits{};
    size_t count = 0;
    s_compute_lock.lock();
    for (auto& slot : s_user_signals) {
        if (slot.owner != nullptr && slot.node == node_id &&
            (scope == RemoteComputeCleanupScope::ALL || !compute_submit_channel_token_matches(node_id, slot.channel, slot.generation))) {
            auto* wait = claim_and_clear_waiter_locked(slot.response_wait);
            if (wait != nullptr) waits[count++] = wait;
        }
    }
    s_compute_lock.unlock();
    for (size_t i = 0; i < count; ++i) wki_finish_claimed_op(waits[i], WKI_ERR_PEER_FENCED);
}

void fail_submitted_tasks_for_peer(uint16_t node_id, RemoteComputeCleanupScope scope) {
    fail_user_signals_for_peer(node_id, scope);
    constexpr size_t MAX_PROXIES_PER_BATCH = 64;
    constexpr size_t MAX_WAITERS_PER_BATCH = MAX_PROXIES_PER_BATCH * 2;
    constexpr size_t MAX_HANDOFF_CLEANUPS_PER_BATCH = 8;

    for (;;) {
        std::array<ker::mod::sched::task::Task*, MAX_PROXIES_PER_BATCH> proxy_tasks = {};
        std::array<uint32_t, MAX_PROXIES_PER_BATCH> proxy_task_ids = {};
        size_t proxy_count = 0;
        std::array<WkiWaitEntry*, MAX_WAITERS_PER_BATCH> waiters_to_finish = {};
        size_t waiter_count = 0;
        std::array<SubmittedIpcCleanup, MAX_PROXIES_PER_BATCH> ipc_cleanups = {};
        size_t ipc_cleanup_count = 0;
        std::array<SubmittedIpcHandoffCleanup, MAX_HANDOFF_CLEANUPS_PER_BATCH> handoff_cleanups = {};
        size_t handoff_cleanup_count = 0;
        bool drained_submitted = true;

        s_compute_lock.lock();

        // Fail submitted tasks targeting this peer in bounded batches. Each
        // stack waiter is claimed while s_compute_lock still owns the published
        // pointer so timeout and late-response paths cannot touch it after the
        // submitter unwinds.
        for (auto* slot_ptr = s_submitted_task_head; slot_ptr != nullptr; slot_ptr = slot_ptr->storage_next) {
            auto& slot = *slot_ptr;
            if (!slot.occupied) {
                continue;
            }
            auto& t = slot.task;
            if (!submitted_task_matches_cleanup_locked(t, node_id, scope)) {
                continue;
            }

            t.cancel_service_report(t.submission_started);
            if (!t.active) {
                reclaim_submitted_task_if_safe_locked(&t);
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
            size_t const NEEDED_HANDOFF_CLEANUPS = t.handoff_pin_count != 0 ? 1U : 0U;
            if (waiter_count + needed_waiters > waiters_to_finish.size() || proxy_count + NEEDED_PROXIES > proxy_tasks.size() ||
                ipc_cleanup_count + NEEDED_IPC_CLEANUPS > ipc_cleanups.size() ||
                handoff_cleanup_count + NEEDED_HANDOFF_CLEANUPS > handoff_cleanups.size()) {
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
            if (t.handoff_pin_count != 0) {
                handoff_cleanups.at(handoff_cleanup_count++) = take_submitted_ipc_handoff_locked(&t);
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
        for (size_t i = 0; i < handoff_cleanup_count; ++i) {
            release_submitted_ipc_handoff(handoff_cleanups.at(i));
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

    for (auto* node = s_pending_completion_head; node != nullptr; node = node->next) {
        auto& completion = *node;
        if (completion.submitter_node != node_id) {
            continue;
        }
        release_output_capture(completion.output);
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
    for (auto* node = s_pending_completion_head; node != nullptr; node = node->next) {
        auto& completion = *node;
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
        release_output_capture(completion.output);
        completion.output = nullptr;
        completion.submitter_node = WKI_NODE_INVALID;
    }
    compact_pending_task_completions_locked();
    gc_shared_elf_cache_locked(wki_now_us());
    s_compute_lock.unlock();
}

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_submission_errors() -> bool {
    constexpr std::array<int, 10> REJECTIONS{0, -EAGAIN, -ENOMEM, -ENOENT, -EIO, -EACCES, -EAGAIN, -ENOTSUP, -EINVAL, -EDQUOT};
    constexpr std::array<int, 10> TRANSPORT{0, -ENOMEM, -EHOSTUNREACH, -EHOSTDOWN, -EAGAIN, -ETIMEDOUT, -EINVAL, -ENOENT, -EBUSY, -EIO};
    bool ok = true;
    for (unsigned index = 0; index < REJECTIONS.size(); ++index) {
        ok &= task_rejection_errno(static_cast<uint8_t>(index)) == REJECTIONS[index];
        ok &= task_transport_errno(-static_cast<int>(index)) == TRANSPORT[index];
    }
    ok &= task_rejection_errno(UINT8_MAX) == -EPROTO && task_transport_errno(-999) == -EIO;
    int error = 123;
    ok &= wki_task_submit_inline(WKI_NODE_INVALID, nullptr, 0, nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, UINT8_MAX,
                                 &error) == 0 &&
          error == -EINVAL;
    error = 123;
    ok &= wki_task_submit_vfs_ref(WKI_NODE_INVALID, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, UINT8_MAX, &error) ==
              0 &&
          error == -EINVAL;
    std::array<char, 513> long_path;
    long_path.fill('x');
    long_path.back() = '\0';
    error = 123;
    ok &= wki_task_submit_vfs_ref(WKI_NODE_INVALID, long_path.data(), nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, UINT8_MAX,
                                  &error) == 0 &&
          error == -ENAMETOOLONG;
    // Invalid counts must be rejected before dereferencing an IPC array.
    error = 123;
    ok &= wki_task_submit_inline(WKI_NODE_INVALID, nullptr, 0, nullptr, nullptr, nullptr, nullptr, nullptr, 17, nullptr, UINT8_MAX,
                                 &error) == 0 &&
          error == -EINVAL;
    constexpr std::array<uint8_t, 1> BINARY{};
    error = 123;
    ok &= wki_task_submit_inline(WKI_NODE_INVALID, BINARY.data(), BINARY.size(), nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr,
                                 UINT8_MAX, &error) == 0 &&
          error == -ESRCH;
    error = 123;
    ok &= wki_task_submit_vfs_ref(WKI_NODE_INVALID, "/bin/true", nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr, UINT8_MAX,
                                  &error) == 0 &&
          error == -ESRCH;
    ker::mod::sched::task::Task policy_owner{};
    auto const BEFORE = resource_contract::snapshot(policy_owner.resources);
    error = 123;
    ok &= wki_task_submit_inline(WKI_NODE_INVALID, BINARY.data(), BINARY.size(), nullptr, nullptr, nullptr, &policy_owner, nullptr, 0,
                                 nullptr, UINT8_MAX, &error) == 0 &&
          error == -ENOTSUP;
    error = 123;
    ok &= wki_task_submit_vfs_ref(WKI_NODE_INVALID, "/bin/true", nullptr, nullptr, nullptr, &policy_owner, nullptr, 0, nullptr, UINT8_MAX,
                                  &error) == 0 &&
          error == -ENOTSUP;
    auto const AFTER = resource_contract::snapshot(policy_owner.resources);
    ok &= std::memcmp(&BEFORE, &AFTER, sizeof(BEFORE)) == 0 && policy_owner.fd_table.empty();
    // Exercise the real submission entrypoints for omitted credential fields.
    // An invalid target ensures denied cases cannot transmit or wait for peers.
    for (unsigned variant = 0; variant < 4; ++variant) {
        auto credentials = ker::mod::security::Snapshot::bootstrap_root();
        constexpr std::array<uint32_t, 1> GROUPS{3000};
        if ((variant & 1U) != 0)
            ok &= credentials.prepare({.change = ker::mod::security::Change::SET_GROUPS, .groups = GROUPS}, credentials) == 0;
        if ((variant & 2U) != 0) ok &= credentials.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, credentials) == 0;
        ok &= credentials.prepare({.change = ker::mod::security::Change::SET_DUMPABLE, .dumpable = (variant & 2U) != 0}, credentials) == 0;
        ok &= ker::mod::security::authorize_legacy_remote_exec(credentials) == -ENOTSUP;
        ok &= ker::mod::sched::resource::initialize_credentials(policy_owner, credentials) == 0;
        auto const RESOURCE_BEFORE = resource_contract::snapshot(policy_owner.resources);
        error = 123;
        ok &= wki_task_submit_inline(WKI_NODE_INVALID, BINARY.data(), BINARY.size(), nullptr, nullptr, nullptr, &policy_owner, nullptr, 0,
                                     nullptr, UINT8_MAX, &error) == 0 &&
              error == -ENOTSUP;
        error = 123;
        ok &= wki_task_submit_vfs_ref(WKI_NODE_INVALID, "/bin/true", nullptr, nullptr, nullptr, &policy_owner, nullptr, 0, nullptr,
                                      UINT8_MAX, &error) == 0 &&
              error == -ENOTSUP;
        auto const RESOURCE_AFTER = resource_contract::snapshot(policy_owner.resources);
        ok &= std::memcmp(&RESOURCE_BEFORE, &RESOURCE_AFTER, sizeof(RESOURCE_BEFORE)) == 0 && policy_owner.fd_table.empty() &&
              policy_owner.credentials() == credentials;
    }
    for (bool negotiated : {false, true}) {
        for (uint16_t identity_length :
             {uint16_t{0}, uint16_t{sizeof(WkiTaskIdentityContext)}, uint16_t{WKI_TASK_RESOURCE_IDENTITY_SIZE - 1},
              uint16_t{WKI_TASK_RESOURCE_IDENTITY_SIZE}, uint16_t{WKI_TASK_RESOURCE_IDENTITY_SIZE + 1}}) {
            auto const EXPECTED = negotiated && identity_length == WKI_TASK_RESOURCE_IDENTITY_SIZE ? TaskRejectReason::ACCEPTED
                                                                                                   : TaskRejectReason::RESOURCE_UNSUPPORTED;
            ok &= resource_submission_admission(negotiated, identity_length) == EXPECTED;
        }
    }
    WkiRemoteSpawnSpec spec{};
    spec.error_out = &error;
    ok &= wki_try_remote_spawn(nullptr, spec) == WkiRemoteSpawnResult::LOCAL && error == 0;
    WkiTaskIdentityContext identity{};
    ker::mod::security::Snapshot mapped;
    auto const DENIED_BEFORE = mapped;
    constexpr uint64_t TRUST_ROOT =
        wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN) | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    ok &= !prepare_submitted_identity(identity, wki_policy_bits(WkiPolicy::COMPUTE), mapped) && mapped == DENIED_BEFORE;
    ok &= prepare_submitted_identity(identity, TRUST_ROOT, mapped) && mapped == ker::mod::security::Snapshot::bootstrap_root();
    auto const MAPPED_BEFORE = mapped;
    identity.suid = UINT32_MAX;
    ok &= !prepare_submitted_identity(identity, TRUST_ROOT, mapped) && mapped == MAPPED_BEFORE;
    identity.suid = 0;
    identity.root.fill('x');
    ok &= !prepare_submitted_identity(identity, TRUST_ROOT, mapped) && mapped == MAPPED_BEFORE;
    identity.root.fill('\0');
    identity.submitter_hostname.fill('x');
    ok &= !prepare_submitted_identity(identity, TRUST_ROOT, mapped) && mapped == MAPPED_BEFORE;
    return ok;
}

auto wki_remote_compute_selftest_cloexec_commit_preserves_other_fds() -> bool {
    ker::mod::sched::task::Task task{};
    auto* keep = new (std::nothrow) ker::vfs::File{};
    auto* close = new (std::nothrow) ker::vfs::File{};
    if (keep == nullptr || close == nullptr) {
        delete keep;
        delete close;
        return false;
    }
    keep->refcount.store(1, std::memory_order_release);
    close->refcount.store(1, std::memory_order_release);
    keep->fops = &g_stdin_null_fops;
    close->fops = &g_stdin_null_fops;
    static_cast<void>(task.fd_table.insert(0, keep));
    static_cast<void>(task.fd_table.insert(1, close));
    task.set_fd_cloexec(1);
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    task.resources.commit_extent(NOFILE, 2);
    close_proxy_fd_table(&task, true);
    bool const PRESERVED = task.fd_table.lookup(0) == keep && task.fd_table.lookup(1) == nullptr && !task.get_fd_cloexec(1) &&
                           task.resources.entries[NOFILE].current == 1 && task.resources.entries[NOFILE].peak == 2;
    close_proxy_fd_table(&task);
    return PRESERVED && task.fd_table.empty() && task.resources.entries[NOFILE].current == 0;
}

auto wki_remote_compute_selftest_filesystem_wire() -> bool {
    using Task = ker::mod::sched::task::Task;
    Task owner{};
    owner.initialize_job_identity({.session_id = 51, .pgid = 52});
    auto credentials = owner.credentials();
    if (credentials.prepare({.change = ker::mod::security::Change::NO_NEW_PRIVILEGES}, credentials) != 0 ||
        credentials.prepare({.change = ker::mod::security::Change::SET_GID, .real = 2000}, credentials) != 0 ||
        credentials.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, credentials) != 0)
        return false;
    std::optional<TaskResourceSnapshot> limits{resource_contract::snapshot(owner.resources)};
    VfsDirectoryGrant root{.version = WKI_VFS_DIRECTORY_GRANT_VERSION,
                           .owner_node = 11,
                           .source_node = 12,
                           .target_node = 13,
                           .resource_id = 1,
                           .owner_incarnation = {2, 3},
                           .source_boot_epoch = 4,
                           .target_boot_epoch = 5,
                           .owner_grant_id = 6,
                           .submission_id = 77};
    auto cwd = root;
    cwd.owner_node = 21;  // The codec cannot assume a common root/cwd owner.
    cwd.resource_id = 2;
    cwd.owner_grant_id = 7;
    constexpr std::array<const char*, 2> ARGV{"program", nullptr};
    constexpr std::array<const char*, 2> ENVP{"NAME=value", nullptr};
    auto fail = [](int line) {
        ker::mod::dbg::logger<"ktest">::error("compute filesystem wire invariant failed at remote_compute.cpp:%d", line);
        return false;
    };
    for (const char* path : {"", "relative", "/", "//", "/rootfs/", "/./rootfs", "/rootfs/..", "/rootfs//tmp"}) {
        Task::PathBuffer root_path{};
        std::strcpy(root_path.data(), path);
        if (wki_task_legacy_root_path_valid(root_path)) return fail(__LINE__);
    }
    Task::PathBuffer root_path{};
    std::strcpy(root_path.data(), "/rootfs/.hidden");
    if (!wki_task_legacy_root_path_valid(root_path)) return fail(__LINE__);
    root_path.back() = 'x';
    if (wki_task_legacy_root_path_valid(root_path) || !wki_task_legacy_root_path_valid(root_path, false)) return fail(__LINE__);
    root_path.fill('x');
    root_path.front() = '/';
    if (wki_task_legacy_root_path_valid(root_path, false)) return fail(__LINE__);
    for (uint16_t flags : {uint16_t{4}, uint16_t{5}, uint16_t{7}}) {
        TaskFilesystemContext invalid{.flags = flags, .submission_id = 77};
        if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0) invalid.root = root;
        if ((flags & WKI_TASK_FILESYSTEM_CWD_GRANT) != 0) invalid.cwd = cwd;
        if (wki_task_filesystem_valid(invalid)) return fail(__LINE__);
    }
    for (uint16_t flags : {uint16_t{0}, WKI_TASK_FILESYSTEM_ROOT_GRANT, WKI_TASK_FILESYSTEM_CWD_GRANT, uint16_t{3}, uint16_t{6}}) {
        TaskFilesystemContext wire{.flags = flags, .submission_id = 77};
        if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0) wire.root = root;
        if ((flags & WKI_TASK_FILESYSTEM_CWD_GRANT) != 0) wire.cwd = cwd;
        if (!wki_task_filesystem_valid(wire) || !wki_task_filesystem_matches_admission(wire, 12, 4, 13, 5)) return fail(__LINE__);
        if (flags != 0 &&
            (wki_task_filesystem_matches_admission(wire, 12, 6, 13, 5) || wki_task_filesystem_matches_admission(wire, 12, 4, 13, 6) ||
             wki_task_filesystem_matches_admission(wire, 14, 4, 13, 5)))
            return fail(__LINE__);
        auto bad = wire;
        ++bad.version;
        if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        bad = wire;
        bad.flags |= 0x8000;
        if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        bad = wire;
        bad.reserved = 1;
        if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        bad = wire;
        bad.submission_id = 0;
        if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        bad = wire;
        if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0)
            ++bad.root.submission_id;
        else
            bad.root.version = 1;
        if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        if (flags == 3) {
            bad = wire;
            ++bad.cwd.target_boot_epoch;
            if (wki_task_filesystem_valid(bad)) return fail(__LINE__);
        }
        const char* const CWD = (flags & WKI_TASK_FILESYSTEM_CWD_GRANT) != 0 ? "/display-cwd" : "/";
        owner.root.fill('\0');
        std::strcpy(owner.root.data(), (flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) != 0
                                           ? "/display-root"
                                           : ((flags & WKI_TASK_FILESYSTEM_ROOT_PATH) != 0 ? "/rootfs" : "/"));
        SubmitContextInfo info{};
        if (!build_submit_context_info(&owner, ARGV.data(), ENVP.data(), CWD, &info, true, true, true, &wire) ||
            info.identity_len != WKI_TASK_FILESYSTEM_IDENTITY_SIZE ||
            resource_submission_admission(true, info.identity_len, true, true, true) != TaskRejectReason::ACCEPTED ||
            resource_submission_admission(true, info.identity_len, true, true, false) != TaskRejectReason::RESOURCE_UNSUPPORTED ||
            resource_submission_admission(true, info.identity_len, true, false, true) != TaskRejectReason::RESOURCE_UNSUPPORTED)
            return fail(__LINE__);
        for (auto mode : {TaskDeliveryMode::INLINE, TaskDeliveryMode::VFS_REF, TaskDeliveryMode::RESOURCE_REF}) {
            std::array<uint8_t, 4096> bytes{};
            TaskSubmitPayload submit{.task_id = 1,
                                     .delivery_mode = static_cast<uint8_t>(mode),
                                     .args_len = info.args_len,
                                     .argc = info.argc,
                                     .envc = info.envc,
                                     .cwd_len = info.cwd_len,
                                     .identity_len = info.identity_len,
                                     .reserved = WKI_TASK_SUBMIT_GROUP_HOME | WKI_TASK_SUBMIT_FILESYSTEM};
            size_t const PREFIX = mode == TaskDeliveryMode::INLINE ? 4 : (mode == TaskDeliveryMode::VFS_REF ? 2 : 8);
            uint32_t const ONE = 1;
            if (mode == TaskDeliveryMode::INLINE)
                std::memcpy(bytes.data(), &ONE, sizeof(ONE));
            else
                std::memcpy(bytes.data() + PREFIX - 2, &ONE, 2);
            auto* context_bytes = bytes.data() + PREFIX + 1;
            copy_submit_context_data(context_bytes, ARGV.data(), ENVP.data(), CWD);
            copy_submit_identity_data(context_bytes + info.args_len, &owner, limits, credentials, owner.root, true, nullptr, true, &wire);
            uint16_t const LENGTH = static_cast<uint16_t>(PREFIX + 1 + info.data_len);
            TaskFilesystemContext decoded{.submission_id = 999};
            const uint8_t* context = nullptr;
            uint16_t context_len = 0;
            WkiTaskIdentityContext identity{};
            bool has_identity = false;
            ker::mod::sched::resource::State restored;
            ker::mod::security::Snapshot decoded_credentials;
            TaskSignalHome home{};
            auto prepare = [&](uint16_t length, TaskFilesystemContext* output) {
                return prepare_submitted_resources(&submit, bytes.data(), length, context, context_len, identity, has_identity, restored,
                                                   nullptr, nullptr, &decoded_credentials, nullptr, nullptr, &home, output);
            };
            if (!valid_submit_filesystem_identity(submit) || prepare(LENGTH, &decoded) != TaskRejectReason::ACCEPTED || !has_identity ||
                decoded_credentials != credentials || home.serial == 0 || std::memcmp(&decoded, &wire, sizeof(wire)) != 0 ||
                context != context_bytes)
                return fail(__LINE__);
            auto const BEFORE = decoded;
            for (uint16_t missing = 1; missing <= sizeof(TaskFilesystemContext); ++missing) {
                if (prepare(static_cast<uint16_t>(LENGTH - missing), &decoded) != TaskRejectReason::RESOURCE_INVALID ||
                    std::memcmp(&decoded, &BEFORE, sizeof(decoded)) != 0)
                    return fail(__LINE__);
            }
            if (prepare(LENGTH, nullptr) != TaskRejectReason::RESOURCE_UNSUPPORTED) return fail(__LINE__);
            submit.reserved = WKI_TASK_SUBMIT_GROUP_HOME;
            if (valid_submit_filesystem_identity(submit) || prepare(LENGTH, &decoded) != TaskRejectReason::RESOURCE_INVALID)
                return fail(__LINE__);
            submit.reserved = WKI_TASK_SUBMIT_FILESYSTEM;
            if (valid_submit_filesystem_identity(submit) || prepare(LENGTH, &decoded) != TaskRejectReason::RESOURCE_INVALID)
                return fail(__LINE__);
            submit.reserved |= WKI_TASK_SUBMIT_GROUP_HOME;
            --submit.identity_len;
            if (valid_submit_filesystem_identity(submit) || prepare(LENGTH, &decoded) != TaskRejectReason::RESOURCE_INVALID)
                return fail(__LINE__);
            ++submit.identity_len;
            auto* suffix = context_bytes + info.args_len + WKI_TASK_ROUTED_IDENTITY_SIZE;
            bad = wire;
            bad.reserved = 1;
            std::memcpy(suffix, &bad, sizeof(bad));
            if (prepare(LENGTH, &decoded) != TaskRejectReason::RESOURCE_INVALID || std::memcmp(&decoded, &BEFORE, sizeof(decoded)) != 0)
                return fail(__LINE__);
            std::memcpy(suffix, &wire, sizeof(wire));
            if ((flags & WKI_TASK_FILESYSTEM_ROOT_GRANT) == 0) {
                auto* identity_bytes = context_bytes + info.args_len;
                identity.root[1] = '/';
                std::memcpy(identity_bytes, &identity, sizeof(identity));
                if (prepare(LENGTH, &decoded) != TaskRejectReason::RESOURCE_INVALID) return fail(__LINE__);
            }
        }
    }
    return true;
}

auto wki_remote_compute_selftest_cwd_submission_admission(const ker::mod::sched::task::Task::FilesystemContext& captured,
                                                          int expected_status, const ker::mod::security::Snapshot& credentials) -> bool {
    using Task = ker::mod::sched::task::Task;
    Task source{};
    source.pid = 0x7FFF12A1;
    if (ker::mod::sched::resource::initialize_credentials(source, credentials) != 0) return false;
    auto const BEFORE = captured;
    auto const CREDENTIALS = source.credentials();
    TaskFilesystemPreparation preparation(&source, captured.cwd.data(), &captured);
    if (preparation.status() != 0) return false;
    FilesystemSubmitAdmission admission;
    admission.resource_generation = 123;
    for (unsigned attempt = 0; attempt < 2; ++attempt) {
        if (prepare_return_owner_cwd(preparation, 13, admission) != expected_status || admission.resource_generation != 123 ||
            admission.channel.channel != nullptr || admission.identity.valid() || preparation.wire_context() != nullptr ||
            preparation.staged(0) != nullptr || preparation.staged(1) != nullptr || preparation.credentials() != CREDENTIALS ||
            preparation.filesystem().root_anchor.get() != BEFORE.root_anchor.get() ||
            preparation.filesystem().cwd_anchor.get() != BEFORE.cwd_anchor.get() || preparation.filesystem().root != BEFORE.root ||
            preparation.filesystem().cwd != BEFORE.cwd || source.credentials() != CREDENTIALS)
            return false;
    }
    // No target was bound and cancellation leaves the caller's owners intact.
    if (preparation.bind_target(13, 7) != 0) return false;
    preparation.cancel();
    return captured.root == BEFORE.root && captured.cwd == BEFORE.cwd && captured.root_anchor.get() == BEFORE.root_anchor.get() &&
           captured.cwd_anchor.get() == BEFORE.cwd_anchor.get() && !preparation.filesystem().root_anchor &&
           !preparation.filesystem().cwd_anchor;
}

auto wki_remote_compute_selftest_filesystem_preparation() -> bool {
    using Task = ker::mod::sched::task::Task;
    std::atomic<uint64_t> boundary{UINT64_MAX - 1};
    if (take_filesystem_submission_id(boundary) != UINT64_MAX - 1 || take_filesystem_submission_id(boundary) != UINT64_MAX ||
        take_filesystem_submission_id(boundary) != 0 || take_filesystem_submission_id(boundary) != 0)
        return false;
    Task executor{};
    Task unrelated{};
    executor.pid = 0x7FFF10A1;
    unrelated.pid = executor.pid + 1;
    std::strcpy(executor.root.data(), "/captured");
    executor.root_len = 9;
    std::strcpy(executor.cwd.data(), "/captured/cwd");
    executor.cwd_len = 13;
    std::array<char, 513> text{};
    text[0] = '/';
    std::fill(text.begin() + 1, text.end() - 1, 'p');
    auto const ORIGINAL = text;
    TaskFilesystemPreparation first(&executor, text.data(), nullptr);
    TaskFilesystemPreparation other(&executor, executor.cwd.data(), nullptr);
    if (first.status() != 0 || other.status() != 0 || first.transaction() == 0 || other.transaction() == first.transaction() ||
        executor.pending_wki_filesystems != &other || first.filesystem().cwd_anchor || first.filesystem().cwd_len != 0)
        return false;
    text[1] = 'x';
    std::strcpy(executor.root.data(), "/replacement");
    std::strcpy(executor.cwd.data(), "/replacement/cwd");
    SubmitFilesystemContext selected;
    if (!capture_submit_filesystem(first, selected) || std::strcmp(selected.root.data(), "/captured") != 0 ||
        std::strcmp(selected.cwd, ORIGINAL.data()) != 0 || std::strcmp(other.cwd(), "/captured/cwd") != 0)
        return false;
    constexpr uint16_t TARGET = 13;
    constexpr uint32_t BOOT = 7;
    if (first.bind_target(0, BOOT) != -EINVAL || first.bind_target(TARGET, BOOT) != 0 || first.bind_target(TARGET, BOOT) != -EBUSY ||
        other.bind_target(TARGET, BOOT) != 0)
        return false;
    struct CloseState {
        Task* executor;
        TaskFilesystemPreparation* first;
        unsigned closed = 0;
        bool reenter = true;
        bool ok = true;
    } state{&executor, &first};
    ker::vfs::FileOperations operations{};
    operations.vfs_close = +[](ker::vfs::File* file) -> int {
        auto* state = static_cast<CloseState*>(file->private_data);
        ++state->closed;
        if (state->reenter) {
            state->reenter = false;
            // Normal cancellation has removed this File, but another staging
            // File and another preparation remain discoverable during close.
            state->ok &= state->first->staged(0) == nullptr && state->first->staged(1) != nullptr;
            wki_remote_compute_cleanup_for_task(state->executor);
            state->ok &= state->executor->pending_wki_filesystems == nullptr && state->closed == 3;
        }
        return 0;
    };
    struct Cleanup {
        TaskFilesystemPreparation* first;
        TaskFilesystemPreparation* other;
        ~Cleanup() {
            other->cancel();
            first->cancel();
        }
    } cleanup{&first, &other};
    auto stage = [&](TaskFilesystemPreparation& preparation, size_t index, uint64_t grant_id) -> bool {
        auto release = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
        std::unique_ptr<ker::vfs::File, decltype(release)> owner(new (std::nothrow) ker::vfs::File{}, release);
        if (!owner) return false;
        owner->refcount = 1;
        owner->fops = &operations;
        owner->fs_type = ker::vfs::FSType::DEVFS;
        owner->open_flags = ker::vfs::O_NO_CACHE;
        owner->private_data = &state;
        auto* file = owner.get();
        VfsDirectoryGrant grant{.version = WKI_VFS_DIRECTORY_GRANT_VERSION,
                                .owner_node = 11,
                                .source_node = 12,
                                .target_node = TARGET,
                                .resource_id = 1,
                                .owner_incarnation = {1, 1},
                                .source_boot_epoch = 1,
                                .target_boot_epoch = BOOT,
                                .owner_grant_id = grant_id,
                                .submission_id = preparation.transaction()};
        auto wrong = grant;
        wrong.submission_id = preparation.transaction() + 1;
        if (preparation.adopt_staging(index, file, wrong) != -EINVAL || file != owner.get() || preparation.staged(index) != nullptr)
            return false;
        wrong = grant;
        ++wrong.target_boot_epoch;
        if (preparation.adopt_staging(index, file, wrong) != -EINVAL || file != owner.get()) return false;
        if (preparation.adopt_staging(index, file, grant) != 0 || file != nullptr) return false;
        static_cast<void>(owner.release());
        return true;
    };
    // Disable reentry until all three Files are staged, including failure cleanup.
    state.reenter = false;
    if (!stage(first, 0, 1) || !stage(first, 1, 2) || !stage(other, 0, 3)) return false;
    if (first.finalize_wire(WKI_TASK_FILESYSTEM_ROOT_GRANT | WKI_TASK_FILESYSTEM_CWD_GRANT) != 0 || first.wire_context() == nullptr ||
        first.wire_context()->submission_id != first.transaction() || other.finalize_wire(WKI_TASK_FILESYSTEM_ROOT_GRANT) != -EINVAL ||
        other.wire_context() != nullptr)
        return false;
    wki_remote_compute_cleanup_for_task(&unrelated);
    if (state.closed != 0 || executor.pending_wki_filesystems != &other) return false;
    state.reenter = true;
    first.cancel();
    first.cancel();
    wki_remote_compute_cleanup_for_task(&executor);
    return state.ok && state.closed == 3 && first.wire_context() == nullptr && other.wire_context() == nullptr && first.cwd() == nullptr &&
           other.cwd() == nullptr && first.status() == -ECANCELED && other.status() == -ECANCELED &&
           executor.pending_wki_filesystems == nullptr;
}

auto wki_remote_compute_selftest_installed_cwd(const ker::vfs::DirectoryViewRef& view, const ker::mod::security::Snapshot& credentials,
                                               const char* expected_path, const char* root_path) -> bool {
    using Task = ker::mod::sched::task::Task;
    auto check = [](bool okay, int line) {
        if (!okay) ker::mod::dbg::logger<"wki">::warn("return-owner cwd check failed at %d", line);
        return okay;
    };
    Task worker{}, child{};
    worker.pid = 0x7FFF1201;
    child.pid = worker.pid + 1;
    Task::FilesystemContext received, previous;
    TaskFilesystemPreparation prepared_owner(&worker, nullptr, nullptr);
    Task::PathBuffer expected_root{'/', '\0'};
    if (root_path != nullptr) {
        std::strcpy(expected_root.data(), root_path);
        prepared_owner.received_view() = view;
        if (!check(prepare_received_root_path_cwd(prepared_owner, worker, credentials, expected_root) == 0, __LINE__)) return false;
        received = prepared_owner.received_filesystem();
    } else if (!check(ker::vfs::vfs_prepare_global_cwd(credentials, view, received) == 0, __LINE__)) {
        return false;
    }
    if (!check(ker::vfs::vfs_prepare_global_cwd(credentials, view, previous) == 0, __LINE__)) return false;
    if (!check(!received.root_anchor && received.cwd_anchor && received.root == expected_root &&
                   std::strcmp(received.cwd.data(), expected_path) == 0,
               __LINE__))
        return false;
    worker.cwd_anchor = previous.cwd_anchor;
    worker.cwd = previous.cwd;
    worker.cwd_len = previous.cwd_len;
    std::strcpy(worker.wki_submitter_hostname.data(), "saved-worker");
    auto const SAVED_HOST = worker.wki_submitter_hostname;
    auto* const SAVED_CWD = worker.cwd_anchor.get();
    auto* const RECEIVED_CWD = received.cwd_anchor.get();
    WkiTaskIdentityContext identity{};
    identity.root = expected_root;
    {
        TaskFilesystemPreparation received_owner(&worker, nullptr, nullptr);
        received_owner.hold_received_filesystem(std::move(received));
        ScopedSubmitVfsIdentity scope(&worker, &identity, "submitter", nullptr, 0, &received_owner);
        if (!check(scope.policy_valid() && !received.cwd_anchor && worker.cwd_anchor.get() == RECEIVED_CWD && !worker.root_anchor &&
                       scope.install_filesystem_on(child) && child.cwd_anchor.get() == RECEIVED_CWD && child.root == expected_root,
                   __LINE__))
            return false;
        for (auto* task : {&worker, &child}) {
            std::array<char, Task::CWD_MAX> name{};
            if (!check(ker::vfs::vfs_getcwd_for_task(task, name.data(), name.size(), nullptr, &credentials) == 0 &&
                           std::strcmp(name.data(), expected_path) == 0,
                       __LINE__))
                return false;
            ker::vfs::LookupHandle relative;
            if (!check(ker::vfs::vfs_acquire_lookup(task, ker::vfs::AT_FDCWD, ".", ker::vfs::LookupIntent::STAT,
                                                    ker::vfs::LookupFollowPolicy::FOLLOW_FINAL, true, false, &relative, 3, false,
                                                    &credentials) == 0 &&
                           relative.directory_view() && relative.directory_view().root() == view.root() &&
                           relative.directory_view().directory() == view.directory() &&
                           relative.mount_policy().read_only == view.policy().read_only &&
                           relative.mount_policy().nosuid == view.policy().nosuid,
                       __LINE__))
                return false;
            ker::vfs::LookupHandle absolute;
            if (!check(ker::vfs::vfs_acquire_lookup(task, ker::vfs::AT_FDCWD, "/", ker::vfs::LookupIntent::STAT,
                                                    ker::vfs::LookupFollowPolicy::FOLLOW_FINAL, true, false, &absolute, 3, false,
                                                    &credentials) == 0 &&
                           !absolute.directory_view(),
                       __LINE__))
                return false;
        }
    }
    if (!check(worker.cwd_anchor.get() == SAVED_CWD && worker.cwd == previous.cwd && worker.wki_submitter_hostname == SAVED_HOST &&
                   child.cwd_anchor.get() == RECEIVED_CWD,
               __LINE__))
        return false;
    Task::FilesystemContext invalid;
    if (!check(child.capture_filesystem_context(invalid), __LINE__)) return false;
    // A received native view must not become an unrestricted pathname on a
    // subsequent remote exec, including when both inherited policy bits are off.
    if (!check(wki_remote_compute_selftest_cwd_submission_admission(invalid, -ENOTSUP, credentials), __LINE__)) return false;
    for (bool root_label : {false, true}) {
        auto stale = invalid;
        auto& label = root_label ? stale.root : stale.cwd;
        label[1] = 'x';
        label[2] = '\0';
        if (!check(wki_remote_compute_selftest_cwd_submission_admission(stale, -ESTALE, credentials), __LINE__)) return false;
    }
    {
        // Explicitly selecting another cwd has the existing text-routing
        // contract; the previous directory's anchor must not be paired with it.
        TaskFilesystemPreparation alternate(&child, "/", nullptr);
        FilesystemSubmitAdmission admission;
        if (!check(alternate.status() == 0 && !alternate.filesystem().cwd_anchor &&
                       prepare_return_owner_cwd(alternate, 13, admission) == 0 && alternate.wire_context() == nullptr,
                   __LINE__))
            return false;
    }
    uint8_t const TRUNCATED_POLICY = 1;
    {
        TaskFilesystemPreparation received_owner(&worker, nullptr, nullptr);
        received_owner.hold_received_filesystem(std::move(invalid));
        ScopedSubmitVfsIdentity scope(&worker, &identity, "rejected", &TRUNCATED_POLICY, 1, &received_owner);
        if (!check(!scope.policy_valid() && !scope.install_filesystem_on(child), __LINE__)) return false;
    }
    {
        TaskFilesystemPreparation abandoned(&worker, nullptr, nullptr);
        if (!check(child.capture_filesystem_context(abandoned.received_filesystem()), __LINE__)) return false;
        abandoned.received_view() = view;
        if (root_path != nullptr && !check(prepare_received_root_path_cwd(abandoned, worker, credentials, expected_root) == 0, __LINE__))
            return false;
        wki_remote_compute_cleanup_for_task(&worker);
        if (!check(abandoned.status() == -ECANCELED && !abandoned.received_filesystem().cwd_anchor && !abandoned.received_view() &&
                       !abandoned.received_root_view() && !abandoned.received_prepared() && worker.pending_wki_filesystems == nullptr,
                   __LINE__))
            return false;
        wki_remote_compute_cleanup_for_task(&worker);
    }
    return check(worker.cwd_anchor.get() == SAVED_CWD && worker.cwd == previous.cwd && worker.wki_submitter_hostname == SAVED_HOST &&
                     !worker.root_anchor && child.cwd_anchor.get() == RECEIVED_CWD,
                 __LINE__);
}

auto wki_remote_compute_selftest_filesystem_context(const ker::mod::sched::task::Task::FilesystemContext* owned) -> bool {
    using Task = ker::mod::sched::task::Task;
    Task owner{};
    Task worker{};
    owner.pid = 0x7FFF1011;
    worker.pid = owner.pid + 1;
    worker.owner_pid = owner.pid;
    worker.is_thread = true;
    std::strcpy(owner.root.data(), "/captured-root");
    owner.root_len = 14;
    std::strcpy(owner.cwd.data(), "/captured-root/cwd");
    owner.cwd_len = 18;
    std::strcpy(worker.root.data(), "/obsolete-worker-root");
    worker.root_len = 21;
    SubmitFilesystemContext selected;
    std::strcpy(selected.root.data(), "/unchanged");
    selected.cwd = "/explicit";
    auto const BEFORE = selected;
    TaskFilesystemPreparation unbound(&worker, nullptr, nullptr);
    bool ok = !capture_submit_filesystem(unbound, selected) && selected.root == BEFORE.root && selected.cwd == BEFORE.cwd;
    unbound.cancel();
    if (!ker::mod::sched::resource::bind_thread_owner(worker, owner)) return false;
    (void)owner.exchange_creation_mask(0077);
    Task::FilesystemContext captured;
    if (!worker.capture_filesystem_context(captured)) return false;
    // A subsequent filesystem transition must not change the root paired with
    // exec's captured cwd, even when the executor is an older process thread.
    auto const FLAGS = owner.path_metadata_lock.lock_irqsave();
    std::strcpy(owner.root.data(), "/new-root");
    owner.root_len = 9;
    std::strcpy(owner.cwd.data(), "/new-root/cwd");
    owner.cwd_len = 13;
    owner.path_metadata_lock.unlock_irqrestore(FLAGS);
    auto const CREDENTIALS = worker.credentials();
    std::optional<TaskResourceSnapshot> limits{resource_contract::snapshot(owner.resources)};
    for (bool supplied : {false, true}) {
        for (auto* task : {&owner, &worker}) {
            TaskFilesystemPreparation preparation(task, "/explicit", supplied ? &captured : nullptr);
            ok &= capture_submit_filesystem(preparation, selected);
            auto const& ROOT = supplied ? captured.root : owner.root;
            auto const* CWD = supplied ? captured.cwd.data() : "/explicit";
            ok &= selected.root == ROOT && std::strcmp(selected.cwd, CWD) == 0;
            SubmitContextInfo info{};
            ok &= build_submit_context_info(task, nullptr, nullptr, selected.cwd, &info, true, true);
            for (auto mode : {TaskDeliveryMode::INLINE, TaskDeliveryMode::VFS_REF}) {
                std::array<uint8_t, 2048> bytes{};
                TaskSubmitPayload submit{};
                submit.delivery_mode = static_cast<uint8_t>(mode);
                submit.args_len = info.args_len;
                submit.cwd_len = info.cwd_len;
                submit.identity_len = info.identity_len;
                size_t const PREFIX = mode == TaskDeliveryMode::INLINE ? 4 : 2;
                uint32_t const ONE = 1;
                std::memcpy(bytes.data(), &ONE, PREFIX);
                auto* context_bytes = bytes.data() + PREFIX + 1;
                copy_submit_context_data(context_bytes, nullptr, nullptr, selected.cwd);
                copy_submit_identity_data(context_bytes + info.args_len, task, limits, CREDENTIALS, selected.root, true);
                const uint8_t* context = nullptr;
                uint16_t context_len = 0;
                WkiTaskIdentityContext identity{};
                bool has_identity = false;
                ker::mod::sched::resource::State restored;
                ker::mod::security::Snapshot decoded;
                auto const LENGTH = static_cast<uint16_t>(PREFIX + 1 + info.data_len);
                ok &= prepare_submitted_resources(&submit, bytes.data(), LENGTH, context, context_len, identity, has_identity, restored,
                                                  nullptr, nullptr, &decoded) == TaskRejectReason::ACCEPTED;
                ok &= has_identity && identity.umask == 0077 && std::strcmp(identity.root.data(), ROOT.data()) == 0 &&
                      decoded == CREDENTIALS && context == context_bytes &&
                      std::strcmp(reinterpret_cast<const char*>(context_bytes), CWD) == 0;
                auto receiver = std::make_unique<Task>();
                ok &= apply_submitted_task_identity(receiver.get(), identity, CREDENTIALS, {}, {}) && receiver->creation_mask() == 0077;
                (void)receiver->exchange_creation_mask(0);
                ok &= worker.creation_mask() == 0077;
                auto const TERMINATOR = std::find(identity.root.begin(), identity.root.end(), '\0');
                ok &= std::all_of(TERMINATOR, identity.root.end(), [](char byte) { return byte == '\0'; });
            }
        }
    }
    if (owned != nullptr) {
        if (!owned->root_anchor || !owned->cwd_anchor) return false;
        auto const PRESERVED = selected;
        owner.root_anchor = owned->root_anchor;
        for (auto* task : {&owner, &worker}) {
            TaskFilesystemPreparation current(task, "/explicit", nullptr);
            TaskFilesystemPreparation supplied(task, "/explicit", owned);
            ok &= current.filesystem().root_anchor.get() == owned->root_anchor.get() &&
                  supplied.filesystem().root_anchor.get() == owned->root_anchor.get() &&
                  supplied.filesystem().cwd_anchor.get() == owned->cwd_anchor.get();
            ok &= !capture_submit_filesystem(current, selected) && selected.root == PRESERVED.root && selected.cwd == PRESERVED.cwd;
            ok &= !capture_submit_filesystem(supplied, selected) && selected.root == PRESERVED.root && selected.cwd == PRESERVED.cwd;
            wki_remote_compute_cleanup_for_task(task);
            ok &= !current.filesystem().root_anchor && !supplied.filesystem().root_anchor && !supplied.filesystem().cwd_anchor;
        }
        auto const ORIGINAL_ROOT = owner.root;
        auto const ORIGINAL_UMASK = owner.creation_mask();
        auto const ORIGINAL_JOB = owner.job_identity();
        auto const ORIGINAL_SUBMITTER = owner.wki_submitter_hostname;
        WkiTaskIdentityContext identity{};
        std::strcpy(identity.root.data(), "/text-replacement");
        identity.umask = 077;
        ok &= !apply_submitted_task_identity(&owner, identity, CREDENTIALS, {}, {}) && owner.root == ORIGINAL_ROOT &&
              owner.creation_mask() == ORIGINAL_UMASK && owner.job_identity().session_id == ORIGINAL_JOB.session_id &&
              owner.job_identity().pgid == ORIGINAL_JOB.pgid && owner.job_identity().terminal == ORIGINAL_JOB.terminal &&
              owner.job_identity().terminal_generation == ORIGINAL_JOB.terminal_generation &&
              owner.job_identity().exec_completed == ORIGINAL_JOB.exec_completed && owner.job_identity().origin == ORIGINAL_JOB.origin &&
              owner.job_identity().group_origin == ORIGINAL_JOB.group_origin;
        {
            ScopedSubmitVfsIdentity scope(&owner, &identity, "replacement-host", nullptr, 0);
            ok &= !scope.policy_valid() && !scope.active && !scope.vfs_rules_installed && owner.root == ORIGINAL_ROOT &&
                  owner.wki_submitter_hostname == ORIGINAL_SUBMITTER && owner.root_anchor.get() == owned->root_anchor.get();
        }
        owner.root_anchor = {};
        TaskFilesystemPreparation legacy(&worker, "/explicit", nullptr);
        ok &= capture_submit_filesystem(legacy, selected);
    }
    return ok && worker.credentials() == CREDENTIALS && std::strcmp(worker.root.data(), "/obsolete-worker-root") == 0;
}

namespace {
auto selftest_exec_credential_transition() -> bool {
    using namespace ker::mod;
    bool ok = true;
    auto original = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, 2> GROUPS{3000, 3001};
    ok &= original.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, original) == 0;
    ok &= original.prepare({.change = security::Change::SET_RESGID, .real = 2002, .effective = 2002, .saved = 2000}, original) == 0;
    ok &= original.prepare({.change = security::Change::SET_RESUID, .real = 1002, .effective = 1002, .saved = 1000}, original) == 0;
    constexpr uint64_t TRUST = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    for (unsigned suppression = 0; suppression < 8; ++suppression) {
        auto caller = original;
        if ((suppression & 1) != 0) ok &= caller.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, caller) == 0;
        auto const BEFORE = caller;
        for (uint32_t mode : {0755U, 04755U, 02755U, 06755U}) {
            TaskExecCredentialTransition wire{.flags = static_cast<uint16_t>(suppression >> 1), .uid = 1003, .gid = 2003, .mode = mode};
            // Unaligned tail follows arbitrary prior data; stripping preserves it.
            std::array<uint8_t, sizeof(wire) + 3> bytes{0x31, 0x32, 0x33};
            std::memcpy(bytes.data() + 3, &wire, sizeof(wire));
            uint16_t length = bytes.size();
            security::ExecIdentity image;
            ok &= decode_exec_credential_transition(bytes.data(), length, image) && length == 3 && bytes[0] == 0x31;
            security::Snapshot replacement;
            ok &= prepare_submitted_exec_credentials(caller, image, TRUST, replacement);
            security::Snapshot read = security::Snapshot::bootstrap_root();
            ok &= prepare_spawn_read_credentials(caller, replacement, wire, read) == 0 && read == caller;
            auto const READ_BEFORE = read;
            ok &=
                prepare_spawn_read_credentials(caller, security::Snapshot::bootstrap_root(), wire, read) == -EAGAIN && read == READ_BEFORE;
            for (uint16_t flag : {WKI_TASK_SUBMIT_EXEC_HANDOFF, WKI_TASK_SUBMIT_SPAWN_TRANSITION}) {
                TaskSubmitPayload submit{};
                submit.reserved = flag | WKI_TASK_SUBMIT_GROUP_HOME;
                submit.identity_len = WKI_TASK_ROUTED_IDENTITY_SIZE;
                length = bytes.size();
                ok &= decode_submit_credential_transition(submit, bytes.data(), length, image) && length == 3;
                submit.reserved |= WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION;
                length = bytes.size();
                ok &= !decode_submit_credential_transition(submit, bytes.data(), length, image) && length == bytes.size();
                submit.reserved = flag;
                ok &= !decode_submit_credential_transition(submit, bytes.data(), length, image) && length == bytes.size();
            }
            uint32_t const UID = suppression == 0 && (mode & 04000) != 0 ? 1003 : 1002;
            uint32_t const GID = suppression == 0 && (mode & 02000) != 0 ? 2003 : 2002;
            ok &= replacement.user() == security::Ids{1002, UID, UID} && replacement.group() == security::Ids{2002, GID, GID};
            std::array<uint64_t, 10> const EXPECTED_AUXV{11, 1002, 12, UID, 13, 2002, 14, GID, 23, UID != 1002 || GID != 2002};
            ok &= security::exec_policy::identity_auxv(replacement) == EXPECTED_AUXV;
            ok &= replacement.authority_ceiling() == caller.authority_ceiling() &&
                  replacement.no_new_privileges() == caller.no_new_privileges();
            ok &= replacement.has_group(3000) && replacement.has_group(3001) && caller == BEFORE;
            security::Snapshot changed;
            ok &= replacement.prepare({.change = security::Change::SET_EUID, .effective = 1000}, changed) == -EPERM;
            ok &= replacement.prepare({.change = security::Change::SET_EGID, .effective = 2000}, changed) == -EPERM;
        }
    }
    security::Snapshot mapped = original;
    // Original non-root identity is admissible, but a root set-ID replacement
    // needs independent root mapping. Rejection cannot overwrite the output.
    ok &= auth_policy::map_compute_identity(TRUST, original, mapped) == 0;
    ok &= !prepare_submitted_exec_credentials(original, {.uid = 0, .set_uid = true}, TRUST, mapped) && mapped == original;
    ok &= prepare_submitted_exec_credentials(original, {.uid = 0, .set_uid = true}, TRUST | wki_policy_bits(WkiPolicy::IDENTITY_ROOT),
                                             mapped);
    ok &= mapped.user() == security::Ids{1002, 0, 0};
    TaskExecCredentialTransition wire{.uid = 1003, .gid = 2003, .mode = 06755};
    std::array<uint8_t, sizeof(wire)> bytes{};
    for (unsigned invalid = 0; invalid < 5; ++invalid) {
        auto broken = wire;
        if (invalid == 0) broken.version++;
        if (invalid == 1) broken.flags = 4;
        if (invalid == 2) broken.mode |= 0100000;
        if (invalid == 3) broken.uid = security::UNCHANGED_ID;
        if (invalid == 4) broken.gid = security::UNCHANGED_ID;
        std::memcpy(bytes.data(), &broken, sizeof(broken));
        uint16_t length = bytes.size();
        security::ExecIdentity image{.uid = 77};
        ok &= !decode_exec_credential_transition(bytes.data(), length, image) && length == bytes.size() && image.uid == 77;
    }
    for (uint16_t length = 0; length < sizeof(wire); ++length) {
        auto remaining = length;
        security::ExecIdentity image{};
        ok &= !decode_exec_credential_transition(bytes.data(), remaining, image) && remaining == length;
    }
    return ok;
}
}  // namespace

auto wki_remote_compute_selftest_full_credential_context() -> bool {
    namespace security = ker::mod::security;
    bool ok = selftest_exec_credential_transition();
    for (bool resources : {false, true}) {
        for (bool full : {false, true}) {
            for (uint16_t length : {uint16_t{0}, WKI_TASK_RESOURCE_IDENTITY_SIZE, WKI_TASK_FULL_IDENTITY_SIZE,
                                    uint16_t{WKI_TASK_FULL_IDENTITY_SIZE - 1}, uint16_t{WKI_TASK_FULL_IDENTITY_SIZE + 1}}) {
                bool const ALLOWED = resources && length == (full ? WKI_TASK_FULL_IDENTITY_SIZE : WKI_TASK_RESOURCE_IDENTITY_SIZE);
                ok &= resource_submission_admission(resources, length, full) ==
                      (ALLOWED ? TaskRejectReason::ACCEPTED : TaskRejectReason::RESOURCE_UNSUPPORTED);
            }
        }
    }
    auto credentials = security::Snapshot::bootstrap_root();
    constexpr std::array<uint32_t, security::MAX_GROUPS> GROUPS{3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007};
    ok &= credentials.prepare({.change = security::Change::SET_GROUPS, .groups = GROUPS}, credentials) == 0;
    ok &= credentials.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, credentials) == 0;
    ok &= credentials.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 3}, credentials) == 0;
    ok &= credentials.prepare({.change = security::Change::SET_GID, .real = 2000}, credentials) == 0;
    ok &= credentials.prepare({.change = security::Change::SET_UID, .real = 1000}, credentials) == 0;
    ok &= credentials.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, credentials) == 0;
    ker::mod::sched::task::Task owner{};
    std::strcpy(owner.root.data(), "/");
    owner.root_len = 1;
    owner.initialize_job_identity(
        {.session_id = 51, .pgid = 52, .origin = {.node = 0x7B42, .boot = 9}, .group_origin = {.node = 0x7B43, .boot = 10}});
    ker::mod::sched::task::Task::SignalHandlerTable handlers{};
    handlers.at(9) = {.handler = 1, .flags = 0xFFFFFFFF, .restorer = 0x1234, .mask = 7};
    handlers.at(11) = {.handler = 0x5678, .flags = 0xFFFFFFFF, .restorer = 0x9ABC, .mask = 8};
    handlers.at(63).handler = 1;
    owner.initialize_signal_handlers(handlers);
    constexpr uint64_t BLOCKED = (uint64_t{1} << 11) | (uint64_t{1} << 63);
    owner.signal_mask_store(BLOCKED);
    owner.signal_pending_store(BLOCKED);
    owner.parent_death_signal.store(12);
    auto const OWNER_BEFORE = owner.credentials();
    SubmitContextInfo info{};
    ok &= build_submit_context_info(&owner, nullptr, nullptr, nullptr, &info, true, true) &&
          info.identity_len == WKI_TASK_FULL_IDENTITY_SIZE && info.data_len == WKI_TASK_FULL_IDENTITY_SIZE;
    constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
    ok &= owner.resources.set(AS, {4096, 8192}, true, owner.FD_TABLE_SIZE) == 0;
    std::optional<TaskResourceSnapshot> limits{resource_contract::snapshot(owner.resources)};
    constexpr uint64_t TRUST = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    for (bool routed : {false, true}) {
        ok &= build_submit_context_info(&owner, nullptr, nullptr, nullptr, &info, true, true, routed);
        ok &= info.identity_len == (routed ? WKI_TASK_ROUTED_IDENTITY_SIZE : WKI_TASK_FULL_IDENTITY_SIZE);
        ok &= resource_submission_admission(true, info.identity_len, true, routed) == TaskRejectReason::ACCEPTED;
        for (auto mode : {TaskDeliveryMode::INLINE, TaskDeliveryMode::VFS_REF, TaskDeliveryMode::RESOURCE_REF}) {
            std::array<uint8_t, 2048> bytes{};
            TaskSubmitPayload submit{};
            submit.delivery_mode = static_cast<uint8_t>(mode);
            submit.identity_len = info.identity_len;
            size_t const PREFIX = mode == TaskDeliveryMode::INLINE ? 4 : (mode == TaskDeliveryMode::VFS_REF ? 2 : 8);
            uint32_t const ONE = 1;
            if (mode == TaskDeliveryMode::INLINE)
                std::memcpy(bytes.data(), &ONE, sizeof(ONE));
            else
                std::memcpy(bytes.data() + PREFIX - 2, &ONE, 2);
            auto* identity_bytes = bytes.data() + PREFIX + 1;
            copy_submit_identity_data(identity_bytes, &owner, limits, credentials, owner.root, true, nullptr, routed);
            uint16_t const LENGTH = static_cast<uint16_t>(PREFIX + 1 + info.identity_len);
            const uint8_t* context = nullptr;
            uint16_t context_len = 0;
            WkiTaskIdentityContext identity{};
            bool has_identity = false;
            security::Snapshot decoded;
            TaskJobOrigins origins{};
            TaskExecSignals signals{};
            TaskSignalHome home{};
            ker::mod::sched::resource::State restored;
            auto prepare = [&](uint16_t length) {
                return prepare_submitted_resources(&submit, bytes.data(), length, context, context_len, identity, has_identity, restored,
                                                   nullptr, nullptr, &decoded, &origins, &signals, &home);
            };
            ok &= prepare(LENGTH) == TaskRejectReason::ACCEPTED && has_identity && decoded == credentials &&
                  restored.entries[AS].limit.soft == 4096 && restored.entries[AS].limit.hard == 8192;
            ok &= origins.session.node == 0x7B42 && origins.session.boot == 9 && origins.group.node == 0x7B43 && origins.group.boot == 10;
            ok &= signals.blocked == BLOCKED && signals.pending == BLOCKED && signals.ignored == ((uint64_t{1} << 9) | (uint64_t{1} << 63));
            ok &= signals.parent_death_signal == 12;
            if (routed) {
                auto const HOME = owner.signal_home_identity();
                ok &= home.serial == HOME.serial && home.origin.node == HOME.origin.node && home.origin.boot == HOME.origin.boot;
                auto* home_bytes = identity_bytes + WKI_TASK_FULL_IDENTITY_SIZE;
                TaskSignalHome const SAVED = home;
                home.origin.reserved = 1;
                std::memcpy(home_bytes, &home, sizeof(home));
                ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
                home = SAVED;
                home.serial = 0;
                std::memcpy(home_bytes, &home, sizeof(home));
                ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
                std::memcpy(home_bytes, &SAVED, sizeof(SAVED));
                ok &= prepare(LENGTH) == TaskRejectReason::ACCEPTED && decoded == credentials;
            }
            ker::mod::sched::task::Task receiver{};
            install_exec_signals(receiver, signals);
            ok &= receiver.parent_death_signal.load() == 12;
            auto const IMPORTED = receiver.signal_handlers_snapshot();
            for (size_t i = 0; i < IMPORTED.size(); ++i)
                ok &= IMPORTED.at(i).handler == (i == 9 || i == 63 ? 1U : 0U) && IMPORTED.at(i).flags == 0 &&
                      IMPORTED.at(i).restorer == 0 && IMPORTED.at(i).mask == 0;
            ok &= receiver.signal_mask_bits() == BLOCKED && receiver.signal_pending_bits() == BLOCKED && receiver.sigaltstack_flags == 2 &&
                  receiver.sigaltstack_sp == 0 && receiver.sigaltstack_size == 0 && !receiver.in_signal_handler;
            auto* signal_bytes =
                identity_bytes + WKI_TASK_RESOURCE_IDENTITY_SIZE + security::credential_record::SIZE + sizeof(TaskJobOrigins);
            for (size_t offset : {size_t{2}, size_t{4}, size_t{9}, size_t{18}}) {
                // Reserved fields, blocked KILL and ignored STOP cannot be imported.
                signal_bytes[offset] ^= offset == 18 ? 4 : 1;
                ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
                signal_bytes[offset] ^= offset == 18 ? 4 : 1;
            }
            signal_bytes[32] = 65;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
            signal_bytes[32] = 12;
            signal_bytes[36] = 1;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
            signal_bytes[36] = 0;
            signal_bytes[0] = 1;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_UNSUPPORTED;
            signal_bytes[0] = WKI_EXEC_SIGNALS_VERSION + 1;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_UNSUPPORTED;
            signal_bytes[0] = WKI_EXEC_SIGNALS_VERSION;
            auto* origin_bytes = identity_bytes + WKI_TASK_RESOURCE_IDENTITY_SIZE + security::credential_record::SIZE;
            for (size_t offset : {size_t{0}, sizeof(TaskSessionOrigin)}) {
                origin_bytes[offset + 2] = 1;  // reserved bytes never grant an origin
                ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
                origin_bytes[offset + 2] = 0;
            }
            security::Snapshot mapped;
            ok &= prepare_submitted_identity(identity, TRUST, mapped, &decoded) && mapped == credentials;
            auto const MAPPED_BEFORE = mapped;
            // Every duplicate ID must agree with the canonical record.
            for (size_t slot = 0; slot < 6; ++slot) {
                identity_bytes[slot * sizeof(uint32_t)] ^= 1;
                ok &= prepare(LENGTH) == TaskRejectReason::ACCEPTED;
                ok &= !prepare_submitted_identity(identity, TRUST, mapped, &decoded) && mapped == MAPPED_BEFORE;
                identity_bytes[slot * sizeof(uint32_t)] ^= 1;
            }
            // Every truncated credential/origin suffix fails before image work.
            for (uint16_t missing = 1; missing <= security::credential_record::SIZE + sizeof(TaskJobOrigins) + sizeof(TaskExecSignals);
                 ++missing)
                ok &= prepare(static_cast<uint16_t>(LENGTH - missing)) == TaskRejectReason::RESOURCE_INVALID;
            auto* record = identity_bytes + WKI_TASK_RESOURCE_IDENTITY_SIZE;
            record[6] = 1;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
            record[6] = 0;
            record[0] = 2;
            ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_UNSUPPORTED;
            record[0] = security::credential_record::VERSION;
            submit.identity_len++;
            ok &= prepare(static_cast<uint16_t>(LENGTH + 1)) == TaskRejectReason::RESOURCE_INVALID;
        }
    }
    auto const ORIGINAL = owner.signal_handlers_snapshot();
    return ok && owner.credentials() == OWNER_BEFORE && owner.fd_table.empty() && owner.signal_mask_bits() == BLOCKED &&
           owner.signal_pending_bits() == BLOCKED && ORIGINAL.at(9).flags == 0xFFFFFFFF && ORIGINAL.at(11).handler == 0x5678;
}

auto wki_remote_compute_selftest_resource_context_before_fetch() -> bool {
    bool ok = true;
    for (auto mode : {TaskDeliveryMode::INLINE, TaskDeliveryMode::VFS_REF, TaskDeliveryMode::RESOURCE_REF}) {
        std::array<uint8_t, 2048> bytes{};
        TaskSubmitPayload submit{};
        submit.delivery_mode = static_cast<uint8_t>(mode);
        submit.identity_len = WKI_TASK_RESOURCE_IDENTITY_SIZE;
        size_t const PREFIX = mode == TaskDeliveryMode::INLINE ? 4 : (mode == TaskDeliveryMode::VFS_REF ? 2 : 8);
        if (mode == TaskDeliveryMode::INLINE) {
            uint32_t const LENGTH = 1;
            std::memcpy(bytes.data(), &LENGTH, sizeof(LENGTH));
        } else {
            uint16_t const LENGTH = 1;
            std::memcpy(bytes.data() + PREFIX - sizeof(LENGTH), &LENGTH, sizeof(LENGTH));
        }
        WkiTaskIdentityContext identity{};
        identity.uid = 77;
        std::memcpy(bytes.data() + PREFIX + 1, &identity, sizeof(identity));
        ker::mod::sched::resource::State source;
        constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
        ok &= source.set(AS, {4096, 8192}, true, 256) == 0;
        auto record = resource_contract::snapshot(source);
        auto* suffix = bytes.data() + PREFIX + 1 + sizeof(identity);
        std::memcpy(suffix, &record, sizeof(record));
        auto const LENGTH = static_cast<uint16_t>(PREFIX + 1 + submit.identity_len);
        const uint8_t* context = nullptr;
        uint16_t context_len = 0;
        WkiTaskIdentityContext decoded{};
        bool has_identity = false;
        ker::mod::sched::resource::State destination;
        uint8_t stdio_mask = UINT8_MAX;
        auto prepare = [&](uint16_t length) {
            return prepare_submitted_resources(&submit, bytes.data(), length, context, context_len, decoded, has_identity, destination,
                                               &stdio_mask);
        };
        ok &= prepare(LENGTH) == TaskRejectReason::ACCEPTED;
        ok &= has_identity && decoded.uid == 77 && destination.entries[AS].limit.soft == 4096;
        ok &= context == bytes.data() + PREFIX + 1 && context_len == submit.identity_len;
        ok &= stdio_mask == 0;
        // Deliberately descriptor-shaped policy bytes must never be parsed as
        // IPC. The real appended entry is stdout, separated by that policy.
        record.flags = WKI_RESOURCE_STDIO_FLAGS;
        std::memcpy(suffix, &record, sizeof(record));
        constexpr auto LOCAL = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
        constexpr auto HOST = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST);
        constexpr std::array<uint8_t, 12> POLICY{2, 0, LOCAL, 0, 1, 0, '/', HOST, 0, 1, 0, '/'};
        ker::mod::sched::task::Task policy_task{};
        ok &= deserialize_task_vfs_rules(&policy_task, POLICY.data(), POLICY.size());
        std::memcpy(bytes.data() + LENGTH, POLICY.data(), POLICY.size());
        WkiIpcFdEntry entry{};
        entry.local_fd = 1;
        std::memcpy(bytes.data() + LENGTH + POLICY.size(), &entry, sizeof(entry));
        submit.ipc_fd_count = 1;
        auto const WITH_POLICY = static_cast<uint16_t>(LENGTH + POLICY.size() + sizeof(entry));
        ok &= prepare(WITH_POLICY) == TaskRejectReason::ACCEPTED && stdio_mask == 5;
        record.flags = 0;
        std::memcpy(suffix, &record, sizeof(record));
        ok &= prepare(WITH_POLICY) == TaskRejectReason::RESOURCE_INVALID;
        submit.ipc_fd_count = 0;
        ok &= prepare(LENGTH - 1) == TaskRejectReason::RESOURCE_INVALID;
        submit.ipc_fd_count = 17;
        ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
        submit.ipc_fd_count = 0;
        record.version++;
        std::memcpy(suffix, &record, sizeof(record));
        ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_INVALID;
        record.version = WKI_RESOURCE_VERSION;
        record.entries[static_cast<unsigned>(ker::abi::resource::Id::CPU)].limit = {0, 0};
        std::memcpy(suffix, &record, sizeof(record));
        ok &= prepare(LENGTH) == TaskRejectReason::RESOURCE_CPU;
    }
    return ok;
}

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

auto wki_remote_compute_selftest_signal_version_slot_ownership() -> bool {
    constexpr uint32_t FIRST_ID = 0x22FA51;
    constexpr uint32_t NEXT_ID = FIRST_ID + 1;
    bool ok = true;
    for (uint8_t version : {uint8_t{0}, WKI_TASK_SIGNAL_VERSION, uint8_t{255}}) {
        SubmittedTask source;
        source.signal_version = version;
        SubmittedTask moved(std::move(source));
        SubmittedTask assigned;
        assigned.signal_version = static_cast<uint8_t>(version + 1);
        assigned = std::move(moved);
        ok &= assigned.signal_version == version;
    }
    ker::mod::sched::task::Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.publish_accepted_signal_route(NEXT_ID, 101);
    SubmittedTask first;
    first.active = true;
    first.task_id = FIRST_ID;
    SubmittedTask next;
    next.active = true;
    next.task_id = NEXT_ID;
    bool const RETAINED = next.set_local_task_ref(&target);
    if (!RETAINED) return false;
    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(FIRST_ID);
    remove_submitted_task_for_selftest_locked(NEXT_ID);
    auto* accepted = publish_submitted_task_locked(std::move(first));
    if (accepted == nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    // Simulate negotiation after publication, then exercise actual reclaim
    // and free-list reuse rather than resetting a detached test value.
    accepted->signal_version = WKI_TASK_SIGNAL_VERSION;
    remove_submitted_task_for_selftest_locked(FIRST_ID);
    ok &= accepted->signal_version == 0;
    auto* replacement = publish_submitted_task_locked(std::move(next));
    bool const UNNEGOTIATED = replacement == accepted && replacement != nullptr && replacement->signal_version == 0;
    ok &= UNNEGOTIATED;
    s_compute_lock.unlock();
    // Boot-time KTEST has no current scheduler task. It verifies the actual
    // slot state used by the public version gate; live fixtures exercise the
    // caller-owned RPC path once the scheduler is running.
    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(NEXT_ID);
    s_compute_lock.unlock();
    return ok && target.ref_count.load() == 1;
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

auto wki_remote_compute_selftest_proxy_child_retirement() -> bool {
    using namespace ker::mod::sched;
    namespace events = ker::syscall::process::child_events;
    bool ok = true;
    for (bool exited : {false, true}) {
        auto observer = std::make_unique<task::Task>();
        auto proxy = std::make_unique<task::Task>();
        auto child = std::make_unique<task::Task>();
        auto other = std::make_unique<task::Task>();
        observer->pid = 0x7B7C00;
        // No live init is touched: PID1's exit explicitly discards/unlinks.
        proxy->pid = 1;
        child->pid = 0x7B7C01;
        for (auto* value : {observer.get(), proxy.get(), child.get(), other.get()}) {
            value->type = task::TaskType::PROCESS;
            value->state.store(task::TaskState::EXITING);
        }
        if (!events::begin_publication(*observer, *proxy) || !events::commit_publication(*proxy) ||
            !events::begin_publication(*proxy, *child) || !events::commit_publication(*child))
            return false;
        if (exited) {
            child->exit_status = 37 << 8;
            ok &= events::publish_exit(*child);
        }
        events::ClaimedEvent before{};
        ok &= events::claim_or_register(*observer, *observer, proxy->pid, 0, true, before) == events::ProbeResult::WOULD_BLOCK;
        ProxyExitQueue queue;
        unsigned dispatches = 0;
        proxy->state.store(task::TaskState::ACTIVE);
        auto enqueue = [&](task::Task& selected) {
            ++dispatches;
            ok &= &selected == proxy.get() && selected.exit_in_progress.load() && !selected.has_exited;
            ok &= !selected.exit_notify_ready.load() && selected.state.load() == task::TaskState::EXITING;
            ok &= selected.child_list_head == child.get();
            ok &= events::diagnostics(*observer).owned_event_count == 0;
            ok &= queue.push(selected);
        };
        prepare_proxy_finalization(proxy.get(), 0, nullptr, 0, 0x7B7C01, nullptr, nullptr, enqueue);
        prepare_proxy_finalization(proxy.get(), -9, nullptr, 0, 0x7B7C01, nullptr, nullptr, enqueue);
        ok &= dispatches == 1 && proxy->exit_status == 0 && queue.push(*other);
        auto* selected = queue.pop();
        ok &= selected == proxy.get() && selected->wki_proxy_exit_next == nullptr;
        bool published = false;
        finish_proxy_exit(*selected, [&](task::Task& retiring) {
            published = true;
            ok &= retiring.child_list_head == nullptr && retiring.child_event_head == nullptr;
            ok &= child->child_parent == nullptr && child->ref_count.load() == 1;
            ok &= child->child_membership_state.load() == task::ChildMembershipState::UNLINKED;
            ok &= retiring.has_exited && retiring.exit_notify_ready.load() && retiring.state.load() == task::TaskState::DEAD;
            ok &= events::publish_exit(retiring);
        });
        selected->release();
        auto* second = queue.pop();
        ok &= second == other.get() && queue.pop() == nullptr && queue.head == nullptr && queue.tail == nullptr;
        if (second != nullptr) second->release();
        events::ClaimedEvent after{};
        bool const CLAIMED = events::claim_or_register(*observer, *observer, proxy->pid, 0, false, after) == events::ProbeResult::CLAIMED;
        ok &= published && CLAIMED && after.status == 0;
        if (CLAIMED) ok &= events::commit_claim(*observer, *observer, after, false);
        events::cancel_wait(*observer);
        ok &= proxy->ref_count.load() == 1 && observer->ref_count.load() == 1 && other->ref_count.load() == 1;
    }
    return ok;
}

auto wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence() -> bool {
    using ChildMembershipState = ker::mod::sched::task::ChildMembershipState;
    using ProbeResult = ker::syscall::process::child_events::ProbeResult;

    ker::mod::sched::task::Task parent{};
    ker::mod::sched::task::Task proxy{};

    parent.pid = 0x7A21;
    proxy.pid = 0x7A22;
    proxy.initialize_job_identity({.pgid = 0x7A20});
    proxy.exit_status = 0x1234;
    proxy.user_time_us = 123;
    proxy.system_time_us = 456;
    proxy.child_user_time_us = 11;
    proxy.child_system_time_us = 13;
    static_cast<void>(proxy.resources.charge_cpu(100, 40));
    auto const BASELINE = resource_contract::snapshot(proxy.resources);
    ker::mod::sched::resource::State remote;
    if (!resource_contract::restore(BASELINE, remote, 256)) return false;
    static_cast<void>(remote.charge_cpu(300, 200));
    auto completion = resource_contract::snapshot(remote);
    completion.reaped_user_time_us = 17;
    completion.reaped_system_time_us = 19;
    auto bad = completion;
    bad.cpu_user_time_us = 0;
    if (merge_proxy_resource_completion(proxy, BASELINE, bad) || proxy.user_time_us != 123 || proxy.system_time_us != 456 ||
        proxy.child_user_time_us != 11 || proxy.child_system_time_us != 13 || proxy.resources.cpu_time_us != 100)
        return false;
    if (!merge_proxy_resource_completion(proxy, BASELINE, completion) || proxy.user_time_us != 323 || proxy.system_time_us != 556 ||
        proxy.child_user_time_us != 28 || proxy.child_system_time_us != 32 || proxy.resources.cpu_time_us != 400)
        return false;

    if (!ker::syscall::process::child_events::begin_publication(parent, proxy)) {
        return false;
    }
    if (!ker::syscall::process::child_events::commit_publication(proxy)) {
        ker::syscall::process::child_events::abort_publication(proxy);
        return false;
    }

    ker::syscall::process::child_events::ClaimedEvent before_publish{};
    bool const REGISTERED_BEFORE_PUBLISH = ker::syscall::process::child_events::claim_or_register(
                                               parent, parent, proxy.pid, 0, true, before_publish) == ProbeResult::WOULD_BLOCK;

    // These stack-local Tasks are lifecycle fixtures, not scheduler tasks.
    // Prevent the generic event wake from publishing the parent as runnable.
    parent.state.store(ker::mod::sched::task::TaskState::EXITING, std::memory_order_release);
    bool const PUBLISHED = ker::syscall::process::child_events::publish_exit(proxy);

    ker::syscall::process::child_events::ClaimedEvent first_claim{};
    bool const FIRST_CLAIMED =
        ker::syscall::process::child_events::claim_or_register(parent, parent, proxy.pid, 0, false, first_claim) == ProbeResult::CLAIMED;
    bool const SNAPSHOT_PRESERVED = FIRST_CLAIMED && first_claim.subject_pid == proxy.pid &&
                                    first_claim.process_group == proxy.job_identity().pgid && first_claim.status == proxy.exit_status &&
                                    first_claim.user_time_us == 351 && first_claim.system_time_us == 588;
    uint64_t const FIRST_COOKIE = first_claim.claim_cookie;
    ker::syscall::process::child_events::release_claim(parent, first_claim);

    ker::syscall::process::child_events::ClaimedEvent retry_claim{};
    bool const RETRY_CLAIMED =
        ker::syscall::process::child_events::claim_or_register(parent, parent, proxy.pid, 0, false, retry_claim) == ProbeResult::CLAIMED;
    bool const RETRY_IS_SAME_EVENT = RETRY_CLAIMED && retry_claim.node == first_claim.node && retry_claim.claim_cookie != FIRST_COOKIE &&
                                     retry_claim.status == first_claim.status;
    bool const CONSUMED = RETRY_CLAIMED && ker::syscall::process::child_events::commit_claim(parent, parent, retry_claim, false);

    ker::syscall::process::child_events::ClaimedEvent stale_claim{};
    bool const REJECTED_STALE_SPECIFIC_WAIT = ker::syscall::process::child_events::claim_or_register(
                                                  parent, parent, proxy.pid + 1, 0, false, stale_claim) == ProbeResult::NO_CHILD;

    return REGISTERED_BEFORE_PUBLISH && PUBLISHED && SNAPSHOT_PRESERVED && RETRY_IS_SAME_EVENT && CONSUMED &&
           REJECTED_STALE_SPECIFIC_WAIT && proxy.child_membership_state.load(std::memory_order_acquire) == ChildMembershipState::UNLINKED &&
           parent.child_user_time_us == 351 && parent.child_system_time_us == 588;
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

auto wki_remote_compute_selftest_submit_buffers_follow_executor() -> bool {
    ker::mod::sched::task::Task executor{};
    unsigned const FREES = s_task_submit_buffer_frees.load();
    bool ok = true;
    {
        TaskSubmitBuffer first(&executor, WKI_ETH_MAX_PAYLOAD + 1);
        TaskSubmitBuffer second(&executor, UINT16_MAX);
        if (first.data() == nullptr || second.data() == nullptr) return false;
        first.data()[0] = 0x31;
        first.data()[WKI_ETH_MAX_PAYLOAD] = 0x32;
        second.data()[UINT16_MAX - 1] = 0x33;
        ok &= executor.pending_wki_submit_buffers == &second;
        {
            TaskSubmitBuffer empty(&executor, 0);
            TaskSubmitBuffer failed(&executor, UINT16_MAX, true);
            ok &= empty.data() == nullptr && failed.data() == nullptr && executor.pending_wki_submit_buffers == &second &&
                  s_task_submit_buffer_frees.load() == FREES;
        }
        uint16_t count = 0;
        WkiIpcSubmissionGuard ipc(&executor, nullptr, nullptr, count, WKI_NODE_INVALID);
        ipc.disarm();
        ok &= executor.pending_wki_submissions == nullptr && executor.pending_wki_submit_buffers == &second;
        ok &= first.data()[0] == 0x31 && first.data()[WKI_ETH_MAX_PAYLOAD] == 0x32 && second.data()[UINT16_MAX - 1] == 0x33;
        // Removing a non-head allocation must preserve the remaining node.
        first.cancel();
        ok &= first.data() == nullptr && executor.pending_wki_submit_buffers == &second && s_task_submit_buffer_frees.load() == FREES + 1;
        wki_remote_compute_cleanup_for_task(&executor);
        ok &= second.data() == nullptr && executor.pending_wki_submit_buffers == nullptr && s_task_submit_buffer_frees.load() == FREES + 2;
    }
    return ok && s_task_submit_buffer_frees.load() == FREES + 2;
}

auto wki_remote_compute_selftest_slot_allocation_failure() -> bool {
    ker::mod::sched::task::Task subject{};
    SubmittedTask input;
    if (!input.set_local_task_ref(&subject)) return false;
    input.active = true;
    input.ipc_fd_count = 1;
    input.ipc_fd_map[0].resource_id = 0x7B27;
    input.resource_baseline.emplace();
    input.resource_baseline->cpu_time_us = 123;
    s_compute_lock.lock();
    input.task_id = allocate_submitted_task_id_locked();
    uint32_t const ID = input.task_id;
    size_t const COUNT = s_submitted_task_count;
    size_t const STORAGE = s_submitted_task_storage_count;
    auto* const HEAD = s_submitted_task_head;
    auto* const TAIL = s_submitted_task_tail;
    auto* const FREE = s_submitted_task_free;
    // Force the same empty-free-list allocation branch used on real growth.
    // No concurrent pool user can observe this test's temporary free-list view.
    s_submitted_task_free = nullptr;
    int error = 0;
    auto* failed = publish_submitted_task_locked(std::move(input), &error, true);
    bool ok = failed == nullptr && error == -ENOMEM && s_submitted_task_count == COUNT && s_submitted_task_storage_count == STORAGE &&
              s_submitted_task_head == HEAD && s_submitted_task_tail == TAIL && s_submitted_task_free == nullptr &&
              find_submitted_task_any(ID) == nullptr && input.task_id == ID && input.local_task == &subject &&
              subject.ref_count.load() == 2 && input.ipc_fd_count == 1 && input.ipc_fd_map[0].resource_id == 0x7B27 &&
              input.resource_baseline.has_value() && input.resource_baseline->cpu_time_us == 123;
    SubmittedTask invalid;
    ok &= publish_submitted_task_locked(std::move(invalid), &error, true) == nullptr && error == -EINVAL;
    auto* row = publish_submitted_task_locked(std::move(input), &error);
    s_submitted_task_free = FREE;
    if (row == nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto* const SLOT = find_submitted_task_slot_locked(ID);
    ok &= error == 0 && s_submitted_task_storage_count == STORAGE + 1 && s_submitted_task_count == COUNT + 1 &&
          s_submitted_task_tail == SLOT && SLOT->storage_next == nullptr &&
          (TAIL != nullptr ? TAIL->storage_next == SLOT && s_submitted_task_head == HEAD : s_submitted_task_head == SLOT);
    invalid.task_id = ID;
    ok &= publish_submitted_task_locked(std::move(invalid), &error) == nullptr && error == -EINVAL;
    ok &= input.local_task == nullptr && row->local_task == &subject && subject.ref_count.load() == 2;
    row->active = false;
    row->ipc_fd_count = 0;
    auto* output = consume_submitted_task_result_locked(row);
    ok &= output == nullptr && subject.ref_count.load() == 1 && s_submitted_task_count == COUNT && s_submitted_task_free == SLOT &&
          SLOT->free_next == FREE;
    // Allocation failure injection must not reject a reusable slot.
    SubmittedTask reuse;
    reuse.task_id = allocate_submitted_task_id_locked();
    auto* reused = publish_submitted_task_locked(std::move(reuse), &error, true);
    ok &= reused == row && error == 0 && s_submitted_task_storage_count == STORAGE + 1;
    auto* reused_output = consume_submitted_task_result_locked(reused);
    ok &= reused_output == nullptr && s_submitted_task_count == COUNT;
    s_compute_lock.unlock();
    delete[] output;
    delete[] reused_output;
    return ok;
}

auto wki_remote_compute_selftest_failed_submit_restores_guard() -> bool {
    bool ok = true;
    for (uint16_t number : {uint16_t{0}, uint16_t{1}, uint16_t{16}}) {
        ker::mod::sched::task::Task executor{};
        std::array<ker::vfs::File, 16> files{};
        std::array<WkiIpcFdEntry, 16> map{};
        std::array<WkiIpcTaskFdHandoff, 16> handoff{};
        uint16_t count = number;
        for (uint16_t i = 0; i < count; ++i) {
            files[i].refcount.store(2, std::memory_order_release);
            handoff[i] = {.local_fd = i, .file = &files[i]};
        }
        WkiIpcSubmissionGuard guard(&executor, map.data(), handoff.data(), count, WKI_NODE_INVALID);
        SubmittedTask submitted;
        s_compute_lock.lock();
        submitted.task_id = allocate_submitted_task_id_locked();
        submitted.active = true;
        submitted.result_owner_task = &executor;
        submitted.result_handle_owned = true;
        remember_submitted_ipc_fds(submitted, map.data(), count);
        auto* row = publish_submitted_task_locked(std::move(submitted));
        if (row == nullptr) {
            s_compute_lock.unlock();
            return false;
        }
        remember_submitted_ipc_handoff(*row, handoff.data(), count);
        guard.disarm();
        ok &= executor.pending_wki_submissions == nullptr && row->handoff_pin_count == number;
        for (uint16_t i = 0; i < number; ++i) ok &= handoff[i].file == nullptr && row->ipc_fd_handoff[i].file == &files[i];
        // The same production handoff is used by send failure, timeout and
        // rejection before they consume their result row and unlock.
        restore_submitted_ipc_preparation_locked(*row, handoff.data(), count, guard);
        ok &= executor.pending_wki_submissions == &guard && row->ipc_fd_count == 0 && row->handoff_pin_count == 0;
        row->active = false;
        auto* output = consume_submitted_task_result_locked(row);
        s_compute_lock.unlock();
        delete[] output;
        for (uint16_t i = 0; i < number; ++i) ok &= handoff[i].file == &files[i] && files[i].refcount.load() == 2;
        wki_remote_compute_cleanup_for_task(&executor);
        ok &= executor.pending_wki_submissions == nullptr;
        for (uint16_t i = 0; i < number; ++i) ok &= handoff[i].file == nullptr && files[i].refcount.load() == 1;
    }
    return ok;
}

auto wki_remote_compute_selftest_ipc_cleanup_keeps_channels() -> bool {
    WkiChannel control{};
    WkiChannel data{};
    constexpr uint16_t PEER = 0x7AA2;
    WkiIpcChannels const CHANNELS{{&control, PEER, WKI_CHAN_RESOURCE, 11}, {&data, PEER, WKI_CHAN_IPC_DATA, 12}};
    SubmittedTask original;
    original.ipc_channels = CHANNELS;
    original.target_node = PEER;
    std::array<WkiIpcFdEntry, 1> map{};
    map[0].resource_id = 0x7AA20001;
    remember_submitted_ipc_fds(original, map.data(), 1);
    SubmittedTask moved(std::move(original));
    SubmittedTask assigned;
    assigned = std::move(moved);
    s_compute_lock.lock();
    auto const cleanup = submitted_ipc_cleanup_snapshot_locked(&assigned);
    assigned.ipc_channels = {};
    assigned.ipc_fd_count = 0;
    s_compute_lock.unlock();
    return cleanup.target_node == PEER && cleanup.count == 1 && cleanup.map[0].resource_id == map[0].resource_id &&
           cleanup.channels.control.channel == &control && cleanup.channels.control.generation == 11 &&
           cleanup.channels.control.peer_node_id == PEER && cleanup.channels.control.channel_id == WKI_CHAN_RESOURCE &&
           cleanup.channels.data.channel == &data && cleanup.channels.data.generation == 12 && cleanup.channels.data.peer_node_id == PEER &&
           cleanup.channels.data.channel_id == WKI_CHAN_IPC_DATA;
}

auto wki_remote_compute_selftest_task_exit_releases_handoff_pins() -> bool {
    constexpr uint32_t TASK_ID = 0xC10023U;
    constexpr uint16_t LOCAL_FD = 7;
    ker::mod::sched::task::Task exiting_task = {};
    ker::vfs::File pinned_file = {};
    // Model one ordinary fd owner plus the temporary identity pin. Cleanup
    // must release exactly the latter without destroying this stack fixture.
    pinned_file.refcount.store(2, std::memory_order_release);
    std::array<WkiIpcTaskFdHandoff, 1> handoff = {
        WkiIpcTaskFdHandoff{.local_fd = LOCAL_FD, .file = &pinned_file},
    };

    SubmittedTask submitted = {};
    submitted.active = false;
    submitted.task_id = TASK_ID;
    submitted.target_node = WKI_NODE_INVALID;
    submitted.result_handle_owned = true;
    submitted.result_owner_task = &exiting_task;
    remember_submitted_ipc_handoff(submitted, handoff.data(), static_cast<uint16_t>(handoff.size()));
    bool const MOVED_TO_DURABLE_ROW =
        handoff.front().file == nullptr && submitted.handoff_pin_count == 1 && submitted.ipc_fd_handoff.front().file == &pinned_file;

    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID);
    bool const PUBLISHED = publish_submitted_task_locked(std::move(submitted)) != nullptr;
    s_compute_lock.unlock();
    if (!PUBLISHED) {
        return false;
    }

    wki_remote_compute_cleanup_for_task(&exiting_task);

    s_compute_lock.lock();
    bool const ROW_RETIRED = find_submitted_task_any(TASK_ID) == nullptr;
    remove_submitted_task_for_selftest_locked(TASK_ID);
    s_compute_lock.unlock();

    return MOVED_TO_DURABLE_ROW && ROW_RETIRED && pinned_file.refcount.load(std::memory_order_acquire) == 1;
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
    size_t const HIGH_WATER_AFTER_FIRST = s_submitted_task_storage_count;
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
    slot_reused = reused != nullptr && s_submitted_task_storage_count == HIGH_WATER_AFTER_FIRST;
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

auto wki_remote_compute_selftest_placement_score_accounts_for_inflight() -> bool {
    RemoteNodeLoad load = {};
    load.num_cpus = 8;
    load.avg_load_pct = 100;

    uint64_t const IDLE_SCORE = preferred_remote_placement_score(load, 0);
    uint64_t const ONE_ACTIVE_SCORE = preferred_remote_placement_score(load, 1);

    load.placement_reservations = 1;
    uint64_t const ONE_RESERVED_SCORE = preferred_remote_placement_score(load, 0);

    load.num_cpus = 4;
    uint64_t const FOUR_CPU_RESERVED_SCORE = preferred_remote_placement_score(load, 0);

    return ONE_ACTIVE_SCORE > IDLE_SCORE && ONE_RESERVED_SCORE == ONE_ACTIVE_SCORE && FOUR_CPU_RESERVED_SCORE > ONE_RESERVED_SCORE;
}

auto wki_remote_compute_selftest_balanced_score_accounts_for_capacity() -> bool {
    uint64_t const IDLE = balanced_placement_score(100, 8, 4096, 0);
    uint64_t const ONE_ACTIVE = balanced_placement_score(100, 8, 4096, 1);
    uint64_t const FOUR_CPU_ACTIVE = balanced_placement_score(100, 4, 4096, 1);
    uint64_t const OBSERVED_BUSY = balanced_placement_score(500, 8, 4096, 0);
    uint64_t const BUSY_WITH_SMALL_DEBT = balanced_placement_score(500, 8, 4096, 1);
    uint64_t const MEMORY_PRESSURE = balanced_placement_score(100, 8, WKI_BALANCED_FREE_MEM_RESERVE, 0);
    uint64_t const SATURATED_AT_CAPACITY = balanced_placement_score(1000, 8, 4096, 8);
    uint64_t const SATURATED_AT_HEADROOM = balanced_placement_score(1000, 8, 4096, 10);
    uint64_t const SATURATED_BEYOND_HEADROOM = balanced_placement_score(1000, 8, 4096, 11);
    uint64_t const SEVEN_CPU_BELOW_HEADROOM = balanced_placement_score(1000, 7, 4096, 8);
    uint64_t const SEVEN_CPU_AT_HEADROOM = balanced_placement_score(1000, 7, 4096, 9);

    return ONE_ACTIVE > IDLE && FOUR_CPU_ACTIVE > ONE_ACTIVE && BUSY_WITH_SMALL_DEBT == OBSERVED_BUSY && MEMORY_PRESSURE > IDLE &&
           SATURATED_AT_CAPACITY == 1000 && SATURATED_AT_HEADROOM > SATURATED_AT_CAPACITY &&
           SATURATED_BEYOND_HEADROOM > SATURATED_AT_HEADROOM && SEVEN_CPU_BELOW_HEADROOM == 1000 &&
           SEVEN_CPU_AT_HEADROOM > SEVEN_CPU_BELOW_HEADROOM && balanced_candidate_is_starved(4, 0, 28, 7) &&
           !balanced_candidate_is_starved(3, 0, 28, 7) && balanced_candidate_is_starved(28, 0, 28, 1) &&
           !balanced_candidate_is_starved(27, 0, 28, 1) && balanced_candidate_is_starved(2, 0, 28, 27) &&
           !balanced_candidate_is_starved(1, 0, 28, 27) && balanced_memory_eligible(WKI_BALANCED_FREE_MEM_RESERVE) &&
           !balanced_memory_eligible(WKI_BALANCED_FREE_MEM_RESERVE - 1);
}

auto wki_remote_compute_selftest_automatic_policy_is_workload_agnostic() -> bool {
    return is_placement_control_helper("wkictl") && is_placement_control_helper("/usr/bin/anywhere") &&
           !is_placement_control_helper("/usr/bin/clang") && !is_placement_control_helper("/usr/bin/ninja") &&
           !is_placement_control_helper("/usr/bin/git") && !is_placement_control_helper("/workspace/build/CMakeFiles/cmTC_probe");
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
        uint8_t submitted_lib_route = 0;
        scope_state_ok = POLICY_INSTALLED && TRANSFERRED && WORKER_RULES_RESTORED &&
                         explicit_task_vfs_route_for_path(&submitted, POLICY_PREFIX, &submitted_lib_route) &&
                         submitted_lib_route == static_cast<uint8_t>(WkiVfsRoute::LOCAL) &&
                         task_uses_receiver_local_path(&submitted, "/usr/bin/clang");
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

auto wki_remote_compute_selftest_receiver_system_paths_are_local_by_default() -> bool {
    using ker::mod::sched::task::Task;
    using ker::mod::sched::task::WkiVfsRoute;
    using ker::mod::sched::task::WkiVfsRule;

    auto add_rule = [](Task* task, const char* prefix, WkiVfsRoute route) -> bool {
        WkiVfsRule rule{};
        size_t const PREFIX_LEN = std::strlen(prefix);
        if (PREFIX_LEN == 0 || PREFIX_LEN >= rule.prefix.size()) {
            return false;
        }
        std::memcpy(rule.prefix.data(), prefix, PREFIX_LEN);
        *std::next(rule.prefix.begin(), static_cast<ptrdiff_t>(PREFIX_LEN)) = '\0';
        rule.prefix_len = static_cast<uint16_t>(PREFIX_LEN);
        rule.route = static_cast<uint8_t>(route);
        return task->wki_vfs_rules.push_back(rule);
    };

    Task defaults{};
    if (!install_receiver_local_system_rules(&defaults) || !task_uses_receiver_local_path(&defaults, "/usr/bin/clang") ||
        !task_uses_receiver_local_path(&defaults, "/bin/bash") || !task_uses_receiver_local_path(&defaults, "/lib/libc.so") ||
        receiver_local_system_path("/workspace/input.cpp")) {
        return false;
    }

    Task explicit_host{};
    if (!add_rule(&explicit_host, "/", WkiVfsRoute::HOST) || !install_receiver_local_system_rules(&explicit_host) ||
        explicit_host.wki_vfs_rules.size() != 1 || task_uses_receiver_local_path(&explicit_host, "/usr/bin/clang")) {
        return false;
    }

    Task mixed{};
    if (!add_rule(&mixed, "/usr", WkiVfsRoute::HOST) || !install_receiver_local_system_rules(&mixed) ||
        task_uses_receiver_local_path(&mixed, "/usr/bin/clang") || !task_uses_receiver_local_path(&mixed, "/bin/bash")) {
        return false;
    }

    return true;
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
    for (const auto* slot_ptr = s_submitted_task_head; slot_ptr != nullptr; slot_ptr = slot_ptr->storage_next) {
        const auto& slot = *slot_ptr;
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
        row.channel_generation = task.submit_channel_generation;
        row.active = task.active;
        row.response_pending = task.response_pending.load(std::memory_order_relaxed);
        row.complete_pending = task.complete_pending.load(std::memory_order_relaxed);
        row.response_waiter_owned = task.response_wait_entry != nullptr;
        row.response_consumer_owned = task.response_consumer_wait_entry != nullptr;
        row.complete_waiter_owned = task.complete_wait_entry != nullptr;
        row.complete_consumer_owned = task.complete_consumer_wait_entry != nullptr;
        row.proxy_ready = task.proxy_ready;
        row.has_local_task = task.local_task != nullptr;
        row.result_handle_owned = task.result_handle_owned;
        row.has_result_owner = task.result_owner_task != nullptr;
        row.reclaim_requested = task.reclaim_requested;
        row.exit_status = task.exit_status;
        row.accepted_age_us = task.accepted_at_us != 0 && NOW_US >= task.accepted_at_us ? NOW_US - task.accepted_at_us : 0;
        row.complete_age_us =
            task.complete_received_at_us != 0 && NOW_US >= task.complete_received_at_us ? NOW_US - task.complete_received_at_us : 0;
        row.ipc_fd_count = task.ipc_fd_count;
        row.ipc_handoff_pin_count = task.handoff_pin_count;
        row.pending_proxy_output_len = task.pending_proxy_output_len;
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
        row.channel_generation = task.submit_rx_channel_generation;
        row.submit_session_epoch = task.submit_session_epoch;
        row.active = task.active;
        row.has_local_task = task.task != nullptr;
        row.published = task.published;
        row.accept_pending = task.accept_pending;
        row.discard_completion = task.discard_completion;
        row.termination_requested = task.termination_requested;
        row.output_len = task.output != nullptr ? task.output->size() : 0;
        append_row(row);
    }

    local_counts.pending_completions = s_pending_completion_count;
    for (const auto* node = s_pending_completion_head; node != nullptr; node = node->next) {
        const auto& completion = *node;
        if (completion.submitter_node == WKI_NODE_INVALID) {
            continue;
        }

        WkiRemoteComputeDiagRow row{};
        row.kind = WkiRemoteComputeDiagKind::PENDING_COMPLETE;
        row.task_id = completion.task_id;
        row.peer_node = completion.submitter_node;
        row.local_pid = completion.local_pid;
        row.channel_generation = completion.submit_rx_channel_generation;
        row.submit_session_epoch = completion.submit_session_epoch;
        row.active = true;
        row.exit_status = completion.exit_status;
        row.output_len = completion.output != nullptr ? completion.output->size() : 0;
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

auto running_remote_task_accounting_ready(const RunningRemoteTask& running, const ker::mod::sched::task::Task& task) -> bool {
    return task.exit_notify_ready.load(std::memory_order_acquire) && task.has_exited &&
           (!running.resource_contract || (task.resource_cpu_threads.load(std::memory_order_acquire) == 0 &&
                                           task.resource_deferred_observations.load(std::memory_order_acquire) == 0));
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
        accept.reserved[0] = WKI_TASK_SIGNAL_VERSION;
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

namespace {
// Called only after completion/output ownership was queued or discarded.
void release_running_root_locked(RunningRemoteTask& rt) {
    delete rt.output_completion;
    rt.output_completion = nullptr;
    if (rt.task != nullptr) {
        rt.task->release();
        rt.task = nullptr;
    }
    rt.output = nullptr;
    rt.root_completion_queued = true;
    if (rt.discard_completion && rt.service_accounting != nullptr) {
        rt.service_accounting->release();
        rt.service_accounting = nullptr;
    }
    rt.active = rt.service_accounting != nullptr;
}

void send_service_reports() {
    uint64_t const NOW = wki_now_us();
    s_compute_lock.lock();
    size_t remaining = g_running_remote_tasks.size();
    s_compute_lock.unlock();
    while (remaining-- != 0) {
        TaskServiceReport report{};
        ComputeSubmitSessionToken session{};
        bool found = false;
        s_compute_lock.lock();
        for (auto& rt : g_running_remote_tasks) {
            if (!rt.active || !rt.published || rt.accept_pending || rt.discard_completion || rt.service_accounting == nullptr ||
                rt.service_report_inflight || (rt.service_report_sequence != 0 && NOW - rt.service_report_attempt_us < 100000))
                continue;
            // A single in-flight attempt owns this sequence across the unlocked
            // send. Each retry takes a newer cumulative snapshot/sequence.
            auto const OBSERVATION = rt.service_accounting->snapshot();
            bool const FINAL = rt.root_completion_queued && OBSERVATION.members == 0 && OBSERVATION.pending_remote_children == 0;
            if (rt.service_report_sequence == UINT64_MAX) continue;
            report = service_contract::encode(rt.task_id, ++rt.service_report_sequence, OBSERVATION, FINAL);
            rt.service_report_attempt_us = NOW;
            rt.service_report_inflight = true;
            session = {.node_id = rt.submitter_node,
                       .rx_channel = rt.submit_rx_channel,
                       .rx_channel_generation = rt.submit_rx_channel_generation,
                       .epoch = rt.submit_session_epoch};
            found = true;
            break;
        }
        s_compute_lock.unlock();
        if (!found) break;
        int const RESULT = wki_send_on_channel_generation(session.node_id, session.rx_channel, session.rx_channel_generation,
                                                          MsgType::TASK_SERVICE_REPORT, &report, sizeof(report));
        s_compute_lock.lock();
        auto* rt = find_running_task(report.task_id, session);
        if (rt != nullptr && rt->service_report_inflight && rt->service_report_sequence == report.sequence) {
            rt->service_report_inflight = false;
            // WKI_OK transfers a retransmittable copy to the exact channel.
            // Session retirement independently marks the source incomplete.
            if (RESULT == WKI_OK && (report.flags & WKI_SERVICE_REPORT_FINAL) != 0) {
                rt->service_accounting->release();
                rt->service_accounting = nullptr;
                rt->active = false;
            }
        }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();
    }
}
}  // namespace

void wki_remote_compute_check_completions() {
    if (!g_remote_compute_initialized) {
        return;
    }

    retry_pending_task_accepts();
    send_service_reports();

    s_compute_lock.lock();

    for (auto& rt : g_running_remote_tasks) {
        if (!running_remote_task_completion_eligible(rt)) {
            continue;
        }

        if (rt.root_completion_queued) {
            if (rt.discard_completion) {
                if (rt.service_accounting != nullptr) rt.service_accounting->release();
                rt.service_accounting = nullptr;
                rt.active = false;
            }
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
        } else if (running_remote_task_accounting_ready(rt, *task)) {
            // Wait for the same waitpid-visible point as local parents so
            // remote stdout/stderr proxy closes cannot be overtaken by
            // TASK_COMPLETE.
            exit_status = normalize_local_exit_status_for_wire(task->exit_status);
            completed = true;
        }

        if (!completed) {
            continue;
        }

        // The monitor snapshot is finite even if inherited capture Files
        // survive root exit. Later writes keep their File-owned buffer alive,
        // but cannot mutate the completion payload or any retry of it.
        if (rt.output != nullptr) static_cast<void>(rt.output->freeze());

        // No peer lifecycle lease is held by this timer scan. Receipt RX and
        // home pipe flushing remain free to progress between scans. This
        // checks a fixed accepted prefix, never every inherited writer's EOF.
        if (!rt.discard_completion && rt.output_completion != nullptr && !rt.output_completion->ready()) continue;

        if (rt.discard_completion) {
            release_output_capture(rt.output);
        } else {
            std::optional<TaskResourceSnapshot> resources;
            if (rt.resource_contract && task != nullptr) {
                resources.emplace();
                uint64_t const FLAGS = task->resource_lock.lock_irqsave();
                *resources = resource_contract::snapshot(task->resources);
                task->resource_lock.unlock_irqrestore(FLAGS);
                // Exit and retired thread CPU contributions make reaped-child totals
                // stable. Export only this segment; imported observations were
                // already charged by an earlier/source queue and must stay excluded.
                resources->reaped_user_time_us = task->child_user_time_us;
                resources->reaped_system_time_us = task->child_system_time_us;
            }
            if (!queue_pending_completion_locked(PendingTaskCompletion{.resources = resources,
                                                                       .task_id = rt.task_id,
                                                                       .submitter_node = rt.submitter_node,
                                                                       .local_pid = rt.local_pid,
                                                                       .exit_status = exit_status,
                                                                       .output = rt.output,
                                                                       .submit_session_epoch = rt.submit_session_epoch,
                                                                       .submit_rx_channel = rt.submit_rx_channel,
                                                                       .submit_rx_channel_generation = rt.submit_rx_channel_generation})) {
                // Keep the monitor ref, capture, and resource state together
                // until a later timer tick can preserve the completion.
                continue;
            }
        }
        release_running_root_locked(rt);
    }

    // Clean up inactive entries
    compact_running_remote_tasks_locked();

    s_compute_lock.unlock();

    // Process queued completions without lock. Keep entries queued until the
    // submitter actually accepts TASK_COMPLETE so transient transport stalls
    // cannot strand waitpid()/proxy waiters.
    s_compute_lock.lock();
    size_t completions_remaining = s_pending_completion_count;
    s_compute_lock.unlock();
    while (completions_remaining-- > 0) {
        s_compute_lock.lock();
        std::unique_ptr<PendingTaskCompletion> pending(pop_pending_completion_locked());
        s_compute_lock.unlock();
        if (!pending) break;
        auto& info = *pending;

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
            release_output_capture(info.output);
            continue;
        }

        // D19: Build TASK_COMPLETE with captured output
        std::span<const uint8_t> const OUTPUT = info.output != nullptr ? info.output->freeze() : std::span<const uint8_t>{};
        auto const OUT_LEN = static_cast<uint16_t>(OUTPUT.size());
        uint16_t const RESOURCE_LEN = info.resources.has_value() ? sizeof(TaskResourceSnapshot) : 0;
        auto const MSG_LEN = static_cast<uint16_t>(sizeof(TaskCompletePayload) + OUT_LEN + RESOURCE_LEN);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): only the exact initialized prefix is transmitted.
        std::array<uint8_t, WKI_COMPUTE_RX_PAYLOAD_MAX> buf __attribute__((uninitialized));
        auto* complete = reinterpret_cast<TaskCompletePayload*>(buf.data());
        complete->task_id = info.task_id;
        complete->exit_status = info.exit_status;
        complete->output_len = OUT_LEN;
        complete->reserved = RESOURCE_LEN;

        if (OUT_LEN > 0 && info.output != nullptr) {
            memcpy(buf.data() + sizeof(TaskCompletePayload), OUTPUT.data(), OUT_LEN);
        }

        if (info.resources.has_value()) {
            std::memcpy(buf.data() + sizeof(TaskCompletePayload) + OUT_LEN, &*info.resources, RESOURCE_LEN);
        }

        int const SEND_RESULT = wki_send_on_channel_generation(
            info.submitter_node, info.submit_rx_channel, info.submit_rx_channel_generation, MsgType::TASK_COMPLETE, buf.data(), MSG_LEN);

        if (SEND_RESULT != WKI_OK) {
#if WKI_DEBUG
            uint32_t const TASK_ID = info.task_id;
            uint64_t const LOCAL_PID = info.local_pid;
            uint16_t const SUBMITTER_NODE = info.submitter_node;
#endif
            s_compute_lock.lock();
            session_current = compute_submit_session_is_current_locked(SESSION);
            if (session_current) {
                append_pending_completion_locked(pending.release());
            }
            s_compute_lock.unlock();
            if (!session_current) {
                release_output_capture(info.output);
                continue;
            }
#if WKI_DEBUG
            ker::mod::dbg::log("[WKI] TASK_COMPLETE send deferred: task_id=%u pid=0x%lx submitter=0x%04x status=%d", TASK_ID, LOCAL_PID,
                               SUBMITTER_NODE, SEND_RESULT);
#endif
            continue;
        }

#if WKI_DEBUG
        ker::mod::dbg::log("[WKI] Remote task completed: task_id=%u pid=0x%lx exit=%d output=%u bytes", info.task_id, info.local_pid,
                           info.exit_status, OUT_LEN);
#endif
        release_output_capture(info.output);
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

// The child is unpublished. Prepare every File before taking its FD lock,
// then install inherited representations atomically, even with NOFILE lowered
// below their numeric slots. No new descriptor is being opened by the user.
auto install_remote_stdio(ker::mod::sched::task::Task& task, uint8_t stdio_mask, TaskOutputCapture*& output, int fail_insert_fd = -1)
    -> bool {
    if (stdio_mask > WKI_RESOURCE_STDIO_ALL) return false;
    std::unique_ptr<TaskOutputCapture> capture;
    if ((stdio_mask & 6U) != 0) {
        capture.reset(new (std::nothrow) TaskOutputCapture());
        if (!capture) return false;
    }
    std::array<std::unique_ptr<ker::vfs::File>, 3> files;
    for (unsigned fd = 0; fd < files.size(); ++fd) {
        if ((stdio_mask & (1U << fd)) == 0) continue;
        auto& file = files[fd];
        file.reset(new (std::nothrow) ker::vfs::File());
        if (!file) return false;
        file->fd = static_cast<int>(fd);
        file->private_data = fd == 0 ? nullptr : capture.get();
        file->fops = fd == 0 ? &g_stdin_null_fops : &g_capture_fops;
        file->fs_type = ker::vfs::FSType::TMPFS;
        file->refcount = 1;
        file->open_flags = fd == 0 ? 0 : 1;
    }
    uint64_t const FLAGS = task.fd_table_lock.lock_irqsave();
    for (unsigned fd = 0; fd < files.size(); ++fd) {
        if (files[fd] && (task.fd_table.lookup(fd) != nullptr || task.fd_table.reserved(fd))) {
            task.fd_table_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (unsigned fd = 0; fd < files.size(); ++fd) {
        if (!files[fd]) continue;
        if (static_cast<int>(fd) == fail_insert_fd || !task.fd_table.insert(fd, files[fd].get())) {
            for (unsigned previous = 0; previous < fd; ++previous) {
                if (files[previous]) task.fd_table.remove(previous);
            }
            task.fd_table_lock.unlock_irqrestore(FLAGS);
            return false;
        }
    }
    for (unsigned fd = 0; fd < files.size(); ++fd) {
        if (files[fd]) {
            // All inserts succeeded. The unpublished task now owns the File;
            // its final close, including after fork/import replacement, owns
            // one capture reference independent of the monitor.
            if (fd != 0) capture->retain();
            task.clear_fd_cloexec(fd);
            (void)files[fd].release();
        }
    }
    ker::vfs::fd_usage_changed_locked(task);
    task.fd_table_lock.unlock_irqrestore(FLAGS);
    output = capture.release();
    return true;
}

auto finish_remote_exec_task(ker::mod::sched::task::Task* new_task, uint8_t stdio_mask,
                             ker::mod::sched::resource::State& failure_observations) -> ExecResult {
    ExecResult result;
    if (new_task == nullptr) {
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    if (!install_remote_stdio(*new_task, stdio_mask, result.output)) {
        destroy_unpublished_remote_process(ker::mod::sched::get_current_task(), new_task, nullptr, "wki-exec-stdio", &failure_observations);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    // Caller prepares argv/envp/cwd before scheduler publication.
    result.task = new_task;
    return result;
}

auto exec_elf_buffer(uint8_t* elf_buffer, uint32_t binary_len, bool shared_elf_buffer, ker::mod::sched::resource::State& resources,
                     const ker::mod::security::Snapshot& credentials, uint8_t stdio_mask) -> ExecResult {
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

    ker::mod::sched::resource::ProcessReservation process_reservation(resources, credentials.user().real);
    if (process_reservation.result() != 0) {
        release_loaded_elf_buffer(elf_buffer, shared_elf_buffer);
        result.reject_reason = TaskRejectReason::RESOURCE_NPROC;
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

    // Construct an empty recovery-owned task before applying the submitted
    // policy and mapping either ELF or stack pages.
    auto* new_task = new ker::mod::sched::task::Task(  // NOLINT(cppcoreguidelines-owning-memory)
        "wki-remote", 0, 0, KERNEL_RSP, ker::mod::sched::task::TaskType::PROCESS);

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

    new_task->resources = resources;
    if (ker::mod::sched::resource::initialize_credentials(*new_task, credentials) != 0 || !process_reservation.attach(*new_task) ||
        !new_task->initialize_process_image(elf_buffer, binary_len)) {
        destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-construction", &resources);
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }
    int interpreter_error = 0;
    if (!ker::mod::sched::task::complete_unpublished_process_construction(new_task, &interpreter_error)) {
        destroy_unpublished_remote_process(OWNER, new_task, nullptr, "wki-exec-interpreter", &resources);
        result.reject_reason = interpreter_error == -EACCES ? TaskRejectReason::UNAUTHORIZED : TaskRejectReason::FETCH_FAILED;
        return result;
    }

    return finish_remote_exec_task(new_task, stdio_mask, resources);
}

auto exec_elf_file(ker::vfs::File* owned_file, uint32_t binary_len, const ker::vfs::Stat& file_stat,
                   ker::mod::sched::resource::State& resources, const ker::mod::security::Snapshot& credentials, uint8_t stdio_mask)
    -> ExecResult {
    ker::mod::sched::resource::ProcessReservation process_reservation(resources, credentials.user().real);
    if (process_reservation.result() != 0) {
        ker::vfs::vfs_put_file(owned_file);
        ExecResult result;
        result.reject_reason = TaskRejectReason::RESOURCE_NPROC;
        return result;
    }
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::kernel_stack_alloc("wki_file_exec_kstack"));
    if (stack_base == 0) {
        ker::vfs::vfs_put_file(owned_file);
        ExecResult result;
        result.reject_reason = TaskRejectReason::NO_MEM;
        return result;
    }

    int interpreter_error = 0;
    auto* task = ker::syscall::process::create_file_backed_process_task("wki-remote", owned_file, binary_len, file_stat,
                                                                        stack_base + ker::mod::mm::KERNEL_STACK_SIZE, resources,
                                                                        credentials, process_reservation, &resources, &interpreter_error);
    if (task == nullptr && interpreter_error == -EACCES) {
        ExecResult result;
        result.reject_reason = TaskRejectReason::UNAUTHORIZED;
        return result;
    }
    return finish_remote_exec_task(task, stdio_mask, resources);
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

auto load_elf_from_vfs_path(ker::mod::sched::task::Task& path_context, const char* path, uint16_t submitter_node, uint32_t correlation,
                            const ComputeSubmitSessionToken& session, const ker::mod::security::Snapshot& credentials,
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

    auto* const CURRENT_TASK = &path_context;
    auto const REQUEST_CREDENTIALS = credentials;
    auto release_file = [](ker::vfs::File* file) { ker::vfs::vfs_put_file(file); };
    std::unique_ptr<ker::vfs::File, decltype(release_file)> source(nullptr, release_file);
    auto source_rejection = [](int error) {
        if (error == -EACCES || error == -EPERM) return TaskRejectReason::UNAUTHORIZED;
        if (error == -ENOTSUP) return TaskRejectReason::RESOURCE_UNSUPPORTED;
        if (error == -EAGAIN) return TaskRejectReason::OVERLOADED;
        return TaskRejectReason::BINARY_NOT_FOUND;
    };
    auto source_metadata_rejection = [&](int error) {
        return error == -EACCES || error == -EPERM || error == -ENOTSUP ? source_rejection(error) : TaskRejectReason::FETCH_FAILED;
    };

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

    // Every request, including a shared-cache hit, first acquires its own
    // read capability using the mapped credential. Metadata comes from that
    // retained object, never from a separate ambient pathname lookup.
    auto admit_source = [&]() -> int {
        source.reset();
        ker::vfs::File* opened = nullptr;
        int const ERROR =
            ker::vfs::vfs_open_read_file_as(*CURRENT_TASK, REQUEST_CREDENTIALS, resolved_path, !using_disconnected_host_fallback, &opened);
        source.reset(opened);
        return ERROR;
    };
    int const OPEN_ERROR = admit_source();
    if (OPEN_ERROR != 0) {
        result.reject_reason = source_rejection(OPEN_ERROR);
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }
    ker::vfs::Stat statbuf = {};
    int const STAT_ERROR = ker::vfs::vfs_fstat_file(source.get(), &statbuf);
    if (STAT_ERROR != 0 || statbuf.st_size <= 0) {
        result.reject_reason = source_rejection(STAT_ERROR);
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }
    if (std::cmp_greater(statbuf.st_size, UINT32_MAX)) {
        result.reject_reason = TaskRejectReason::FETCH_FAILED;
        load_measure.finish(perf_compute_reject_status(result.reject_reason));
        return result;
    }

    // A complete large executable buffer recreates the same high-order buddy
    // allocation that local exec avoids. Keep the opened file instead; the
    // receiver will read bounded ELF metadata now and fault immutable PT_LOAD
    // pages from this retained handle on demand.
    if (std::cmp_greater_equal(statbuf.st_size, WKI_FILE_BACKED_ELF_MIN_SIZE)) {
        if (!request_is_current()) {
            result.reject_reason = TaskRejectReason::OVERLOADED;
            load_measure.finish(perf_compute_reject_status(result.reject_reason));
            return result;
        }
        if (ker::syscall::process::supports_file_backed_process(source.get(), static_cast<size_t>(statbuf.st_size))) {
            ker::vfs::Stat post_open_stat = {};
            int const POST_OPEN_STAT_ERROR = ker::vfs::vfs_fstat_file(source.get(), &post_open_stat);
            if (POST_OPEN_STAT_ERROR != 0 || !shared_elf_freshness_matches(statbuf, post_open_stat)) {
                result.reject_reason = source_metadata_rejection(POST_OPEN_STAT_ERROR);
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            result.file = source.release();
            result.size = static_cast<uint32_t>(statbuf.st_size);
            result.file_stat = statbuf;
            result.reject_reason = TaskRejectReason::ACCEPTED;
            result.shared = false;
            load_measure.finish(0, result.size);
            return result;
        }
    }

    // Keep the admitted source and cache/single-flight identity fixed to the
    // path whose freshness key was captured. A retry reopens that same source
    // with the same credential; permission failure never selects another file.
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
            // Pin the immutable candidate before dropping the lock. Metadata
            // merely chose it; compare against the newly admitted source File.
            cached->refcount++;
            cached->last_used_us = wki_now_us();
            auto* const CACHED_BUFFER = cached->buffer;
            uint32_t const CACHED_SIZE = cached->size;
            s_compute_lock.unlock();
            constexpr size_t SCRATCH_SIZE = 64 * 1024;
            std::unique_ptr<uint8_t[]> scratch(new (std::nothrow) uint8_t[SCRATCH_SIZE]);
            bool const MATCH =
                scratch && ker::vfs::content_matches(
                               std::span<const uint8_t>(CACHED_BUFFER, CACHED_SIZE), std::span<uint8_t>(scratch.get(), SCRATCH_SIZE),
                               [&](uint8_t* bytes, size_t count, size_t offset) {
                                   return ker::vfs::vfs_pread_file(source.get(), bytes, count, static_cast<off_t>(offset));
                               },
                               request_is_current);
            if (MATCH) {
                result.buffer = CACHED_BUFFER;
                result.size = CACHED_SIZE;
                result.shared = true;
                load_measure.finish(0, result.size);
                return result;
            }
            s_compute_lock.lock();
            if (auto* stale = find_shared_elf_cache_by_buffer_locked(CACHED_BUFFER); stale != nullptr) {
                stale->valid = false;
            }
            s_compute_lock.unlock();
            release_cached_elf_buffer(CACHED_BUFFER);
            continue;
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
            int const RETRY_OPEN_ERROR = admit_source();
            if (RETRY_OPEN_ERROR == -EACCES || RETRY_OPEN_ERROR == -EPERM || RETRY_OPEN_ERROR == -ENOTSUP) {
                fail_inflight_load();
                result.reject_reason = source_rejection(RETRY_OPEN_ERROR);
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            ker::vfs::Stat retry_stat = {};
            int const RETRY_STAT_ERROR = RETRY_OPEN_ERROR != 0 ? RETRY_OPEN_ERROR : ker::vfs::vfs_fstat_file(source.get(), &retry_stat);
            if (RETRY_STAT_ERROR == -EACCES || RETRY_STAT_ERROR == -EPERM || RETRY_STAT_ERROR == -ENOTSUP) {
                fail_inflight_load();
                result.reject_reason = source_metadata_rejection(RETRY_STAT_ERROR);
                load_measure.finish(perf_compute_reject_status(result.reject_reason));
                return result;
            }
            if (RETRY_STAT_ERROR != 0 || retry_stat.st_size <= 0) {
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
            ssize_t const READ_RET = ker::vfs::vfs_pread_file(source.get(), buf + total_read, CHUNK, static_cast<off_t>(total_read));

            if (READ_RET < 0 || READ_RET == 0) {
                if (++idle_retries >= WKI_VFS_LOAD_IDLE_RETRIES) {
                    break;
                }
                continue;
            }

            total_read += static_cast<size_t>(READ_RET);
            idle_retries = 0;
        }

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
    int const POST_READ_STAT_ERROR = ker::vfs::vfs_fstat_file(source.get(), &post_read_stat);
    if (POST_READ_STAT_ERROR != 0 || !shared_elf_freshness_matches(cache_key, post_read_stat)) {
        fail_inflight_load();
        delete[] buf;
        result.reject_reason = source_metadata_rejection(POST_READ_STAT_ERROR);
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
    // Only our exact loader marker may receive the freshly read bytes.
    // Another same-metadata buffer has not been content-validated here.

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

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_cache_content_is_verified() -> bool {
    constexpr uint16_t NODE = 0x7A27;
    constexpr char PATH[] = "/tmp/ktest_compute_cache_content";
    constexpr std::array<uint8_t, 8> BYTES{0x7f, 'E', 'L', 'F', 1, 2, 3, 4};
    ker::mod::sched::task::Task worker{};
    std::strcpy(worker.root.data(), "/");
    worker.root_len = 1;
    std::strcpy(worker.cwd.data(), "/");
    worker.cwd_len = 1;
    auto const CREDENTIALS = ker::mod::security::Snapshot::bootstrap_root();
    ker::vfs::vfs_mkdir("/tmp", 0755);
    auto* file = ker::vfs::vfs_open_file(PATH, ker::vfs::O_CREAT | ker::vfs::O_EXCL | 2, 0644);
    if (file == nullptr) return false;
    ker::vfs::Stat stat{};
    bool ok = file->fops != nullptr && file->fops->vfs_write != nullptr &&
              file->fops->vfs_write(file, BYTES.data(), BYTES.size(), 0) == static_cast<ssize_t>(BYTES.size());
    file->open_flags |= ker::vfs::O_NO_CACHE;
    ok = ok && ker::vfs::vfs_fstat_file(file, &stat) == 0;
    ker::vfs::vfs_put_file(file);
    if (!ok) {
        ker::vfs::vfs_unlink(PATH);
        return false;
    }
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 1;
    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE) != nullptr) {
        s_compute_lock.unlock();
        ker::vfs::vfs_unlink(PATH);
        return false;
    }
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 1, {.boot_epoch = 1, .auth_generation = 1});
    SharedElfCacheEntry stale{};
    stale.valid = true;
    stale.submitter_node = NODE;
    stale.session = SESSION;
    stale.load_token = next_nonzero_token(s_next_shared_elf_cache_load_token);
    std::strncpy(stale.path.data(), PATH, stale.path.size() - 1);
    stale.freshness = stat;
    stale.size = BYTES.size();
    stale.buffer = new uint8_t[BYTES.size()];
    std::memcpy(stale.buffer, BYTES.data(), BYTES.size());
    stale.buffer[BYTES.size() - 1] ^= 1;
    g_shared_elf_cache.push_back(stale);
    s_compute_lock.unlock();
    auto first = load_elf_from_vfs_path(worker, PATH, NODE, 1, SESSION, CREDENTIALS, 0);
    ok =
        first.shared && first.buffer != nullptr && first.size == BYTES.size() && std::memcmp(first.buffer, BYTES.data(), BYTES.size()) == 0;
    auto second = load_elf_from_vfs_path(worker, PATH, NODE, 2, SESSION, CREDENTIALS, 0);
    ok = ok && second.shared && second.buffer == first.buffer && second.size == BYTES.size();
    release_loaded_elf_buffer(first.buffer, first.shared);
    release_loaded_elf_buffer(second.buffer, second.shared);
    ker::vfs::vfs_put_file(first.file);
    ker::vfs::vfs_put_file(second.file);
    s_compute_lock.lock();
    for (auto& entry : g_shared_elf_cache) {
        if (entry.submitter_node == NODE) {
            ok = ok && entry.refcount == 0 && !entry.loading;
            entry.valid = false;
        }
    }
    gc_shared_elf_cache_locked(wki_now_us());
    if (auto* peer = find_compute_submit_peer_session_locked(NODE); peer != nullptr) *peer = {};
    s_compute_lock.unlock();
    ok = ker::vfs::vfs_unlink(PATH) == 0 && ok;
    return ok;
}

auto wki_remote_compute_selftest_replay_file_policy() -> bool {
    namespace vfs = ker::vfs;
    using ker::mod::sched::task::Task;
    struct State {
        Task* task{};
        std::array<uint8_t, 4> bytes{};
        unsigned writes{};
        unsigned closes{};
        bool unlocked = true;
        bool retained = true;
    };
    vfs::FileOperations operations{};
    operations.vfs_write = [](vfs::File* file, const void* data, size_t count, size_t offset) -> ssize_t {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.writes;
        state.retained = file->refcount.load() == 2;
        if (!state.task->fd_table_lock.try_lock()) {
            state.unlocked = false;
            return -EIO;
        }
        state.task->fd_table_lock.unlock();
        if (!s_compute_lock.try_lock()) {
            state.unlocked = false;
            return -EIO;
        }
        s_compute_lock.unlock();
        if (offset != 0 || count != state.bytes.size()) return -EINVAL;
        if (vfs::vfs_release_fd(state.task, 1) != 0) return -EIO;
        vfs::vfs_put_file(file);  // Retire the table's reference during replay.
        state.retained = state.retained && state.closes == 0 && file->refcount.load() == 1;
        std::memcpy(state.bytes.data(), data, count);
        return static_cast<ssize_t>(count);
    };
    operations.vfs_close = [](vfs::File* file) -> int {
        auto& state = *static_cast<State*>(file->private_data);
        ++state.closes;
        if (!state.task->fd_table_lock.try_lock())
            state.unlocked = false;
        else
            state.task->fd_table_lock.unlock();
        if (!s_compute_lock.try_lock())
            state.unlocked = false;
        else
            s_compute_lock.unlock();
        state.retained = state.retained && file->pos == 7;
        return 0;
    };
    constexpr std::array<uint8_t, 4> DATA{'k', 'e', 'e', 'p'};
    for (unsigned mode = 0; mode <= vfs::FILE_ACCESS_SEARCH_ONLY; ++mode) {
        for (bool readonly : {false, true}) {
            Task task{};
            State state{.task = &task};
            auto* file = new vfs::File;
            file->fs_type = vfs::FSType::SOCKET;
            file->fops = &operations;
            file->private_data = &state;
            file->open_flags = static_cast<int>(mode);
            file->mount_policy.read_only = readonly;
            file->refcount.store(1, std::memory_order_relaxed);
            file->pos = 7;
            if (!task.fd_table.insert(1, file)) {
                vfs::vfs_put_file(file);
                return false;
            }
            struct Cleanup {
                Task* task;
                ~Cleanup() { close_proxy_fd_table(task); }
            } cleanup{&task};
            write_proxy_output(&task, DATA.data(), DATA.size(), 0);
            bool const ALLOWED = (mode == 1 || mode == 2) && !readonly;
            if (ALLOWED) {
                if (state.writes != 1 || state.closes != 1 || state.bytes != DATA || !task.fd_table.empty()) return false;
            } else {
                if (state.writes != 0 || state.closes != 0 || state.bytes != std::array<uint8_t, 4>{} || task.fd_table.lookup(1) != file ||
                    file->refcount.load() != 1 || file->pos != 7)
                    return false;
                close_proxy_fd_table(&task);
                if (state.closes != 1) return false;
            }
            if (!state.unlocked || !state.retained) return false;
        }
    }
    return true;
}

auto wki_remote_compute_selftest_capture_lifetime_and_freeze() -> bool {
    unsigned const FREES_BEFORE = s_output_capture_frees.load(std::memory_order_acquire);
    ker::mod::sched::task::Task root{};
    TaskOutputCapture* output = nullptr;
    if (!install_remote_stdio(root, 6, output)) return false;
    auto* out_file = static_cast<ker::vfs::File*>(root.fd_table.lookup(1));
    auto* err_file = static_cast<ker::vfs::File*>(root.fd_table.lookup(2));
    // Fork/dup retains File objects, not another monitor reference.
    ker::vfs::vfs_retain_file(out_file);
    ker::vfs::vfs_retain_file(err_file);
    bool ok = capture_write(out_file, "root", 4, 0) == 4 && capture_write(err_file, "-err", 4, 0) == 4;
    auto const SNAPSHOT = output->freeze();
    ok &= SNAPSHOT.size() == 8 && std::memcmp(SNAPSHOT.data(), "root-err", 8) == 0;
    close_proxy_fd_table(&root);
    release_output_capture(output);
    output = nullptr;
    ok &= s_output_capture_frees.load(std::memory_order_acquire) == FREES_BEFORE;
    ok &= capture_write(out_file, "late", 4, 0) == 4 && capture_write(err_file, "writer", 6, 0) == 6;
    ok &= SNAPSHOT.size() == 8 && std::memcmp(SNAPSHOT.data(), "root-err", 8) == 0;
    ker::vfs::vfs_put_file(out_file);
    ok &= s_output_capture_frees.load(std::memory_order_acquire) == FREES_BEFORE;
    ker::vfs::vfs_put_file(err_file);
    ok &= s_output_capture_frees.load(std::memory_order_acquire) == FREES_BEFORE + 1;

    // Conversely, closing every File leaves the pending monitor payload alive.
    if (!install_remote_stdio(root, 2, output)) return false;
    out_file = static_cast<ker::vfs::File*>(root.fd_table.lookup(1));
    std::array<uint8_t, WKI_TASK_MAX_OUTPUT + 32> bytes{};
    bytes.fill(0x5A);
    ok &= capture_write(out_file, bytes.data(), bytes.size(), 0) == static_cast<ssize_t>(bytes.size());
    close_proxy_fd_table(&root);
    ok &= output->size() == WKI_TASK_MAX_OUTPUT;
    auto const FULL = output->freeze();
    ok &= FULL.size() == WKI_TASK_MAX_OUTPUT && std::ranges::all_of(FULL, [](uint8_t byte) { return byte == 0x5A; });
    output->append({reinterpret_cast<const uint8_t*>("ignored"), 7});
    ok &= output->size() == WKI_TASK_MAX_OUTPUT && FULL.front() == 0x5A && FULL.back() == 0x5A;
    release_output_capture(output);
    output = nullptr;
    ok &= s_output_capture_frees.load(std::memory_order_acquire) == FREES_BEFORE + 2;
    ok &= !install_remote_stdio(root, 6, output, 2) && output == nullptr && root.fd_table.empty();
    ok &= s_output_capture_frees.load(std::memory_order_acquire) == FREES_BEFORE + 3;
    return ok;
}

auto wki_remote_compute_selftest_stdio_presence_and_rollback() -> bool {
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    bool ok = true;
    for (uint8_t mask = 0; mask <= WKI_RESOURCE_STDIO_ALL; ++mask) {
        ker::mod::sched::task::Task task{};
        ok &= task.resources.set(NOFILE, {0, 0}, true, task.FD_TABLE_SIZE) == 0;
        TaskOutputCapture* output = nullptr;
        if (!install_remote_stdio(task, mask, output)) return false;
        unsigned count = 0;
        for (unsigned fd = 0; fd < 3; ++fd) {
            auto* file = static_cast<ker::vfs::File*>(task.fd_table.lookup(fd));
            bool const PRESENT = (mask & (1U << fd)) != 0;
            ok &= (file != nullptr) == PRESENT;
            if (PRESENT) {
                ++count;
                ok &= file != nullptr && file->open_flags == (fd == 0 ? 0 : 1);
            }
        }
        ok &= (output != nullptr) == ((mask & 6U) != 0);
        ok &= task.resources.entries[NOFILE].current == count && task.resources.entries[NOFILE].peak == count &&
              task.resources.entries[NOFILE].denied == 0;
        // The source snapshot filters descriptor CLOEXEC without changing the
        // actual open table. Its flags survive an unaligned wire decode.
        task.set_fd_cloexec(1);
        TaskResourceSnapshot record;
        ok &= snapshot_task_resources(&task, record);
        uint8_t const EXPECTED = mask & static_cast<uint8_t>(~2U);
        ok &= ((record.flags & WKI_RESOURCE_STDIO_FLAGS) >> WKI_RESOURCE_STDIO_SHIFT) == EXPECTED;
        close_proxy_fd_table(&task);
        release_output_capture(output);
        ok &= task.fd_table.empty() && task.resources.entries[NOFILE].current == 0;
    }
    ker::mod::sched::task::Task task{};
    task.set_fd_cloexec(0);
    TaskOutputCapture* output = nullptr;
    ok &= !install_remote_stdio(task, WKI_RESOURCE_STDIO_ALL, output, 2);
    ok &= task.fd_table.empty() && output == nullptr && task.get_fd_cloexec(0) && task.resources.entries[NOFILE].current == 0 &&
          task.resources.entries[NOFILE].peak == 0 && task.resources.entries[NOFILE].denied == 0;
    // A reserved inherited target is a conflict; it must not be overwritten.
    int owner = 0;
    ok &= task.fd_table.reserve(2, 3, &owner) == 2;
    ok &= !install_remote_stdio(task, WKI_RESOURCE_STDIO_ALL, output);
    ok &= task.fd_table.empty() && task.fd_table.owned_by(2, &owner) && task.resources.entries[NOFILE].peak == 0;
    ok &= task.fd_table.cancel(2, &owner);
    return ok;
}
#endif

// ===============================================================================
// Receiver Side - RX Handlers
// ===============================================================================

// Internal implementation: runs the full TASK_SUBMIT handler including
// any blocking VFS reads.  Must only be called from a context where
// wki_spin_yield() / inline NAPI draining are safe (i.e. NOT from inside
// the NAPI poll dispatch path).
namespace {
// Submission workers retain the rejection bytes until reliable TX accepts them.
// Route admission can fail transiently while another task owns the peer gate;
// the channel's authentication build can likewise temporarily reject a send.
// Never sleep in RX, hold a lock across backoff, or retry an accepted sequence.

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
    TaskJobOrigins submitted_origins{};
    TaskExecSignals submitted_signals{};
    ker::mod::security::Snapshot submitted_credentials;
    bool has_submitted_identity = false;
    ker::mod::sched::resource::State submitted_resources;
    bool resource_observations_valid = false;
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
        auto send_bytes = [&](const void* bytes, uint16_t length) -> int {
            auto send = [&]() {
                return wki_send_on_channel_generation(src_node, session.rx_channel, session.rx_channel_generation, type, bytes, length);
            };
            if (type != MsgType::TASK_REJECT) return send();
            constexpr uint64_t REJECT_RETRY_US = 1'000'000;
            constexpr uint64_t REJECT_BACKOFF_US = 1'000;
            uint64_t const RETRY_DEADLINE = std::min(deadline_us, wki_future_deadline_us(wki_now_us(), REJECT_RETRY_US));
            return retry_compute_control(
                RETRY_DEADLINE, send,
                [&]() {
                    s_compute_lock.lock();
                    bool const CURRENT = compute_submit_admission_is_current_locked(session);
                    s_compute_lock.unlock();
                    return CURRENT;
                },
                []() { return wki_now_us(); }, [](uint64_t until) { sleep_until_us(until, REJECT_BACKOFF_US); });
        };
        if (type == MsgType::TASK_REJECT && resource_observations_valid) {
            std::array<uint8_t, sizeof(TaskResponsePayload) + sizeof(TaskResourceSnapshot)> bytes{};
            auto header = response;
            header.reserved[0] = static_cast<uint8_t>(sizeof(TaskResourceSnapshot) & 0xFFU);
            header.reserved[1] = static_cast<uint8_t>(sizeof(TaskResourceSnapshot) >> 8);
            header.reserved[2] = 0;
            auto const OBSERVATIONS = resource_contract::snapshot(submitted_resources);
            std::memcpy(bytes.data(), &header, sizeof(header));
            std::memcpy(bytes.data() + sizeof(header), &OBSERVATIONS, sizeof(OBSERVATIONS));
            return send_bytes(bytes.data(), bytes.size());
        }
        return send_bytes(&response, sizeof(response));
    };

    auto submit_deadline_expired = [&]() -> bool { return deadline_us != 0 && wki_now_us() >= deadline_us; };
    s_compute_lock.lock();
    bool const SESSION_CURRENT = compute_submit_admission_is_current_locked(session);
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

    // Refuse incompatible execution before any ELF fetch or admission.
    // Disabling advertisement never restores a legacy resource-policy bypass.
    uint8_t stdio_mask = WKI_RESOURCE_STDIO_ALL;
    bool service_reports = false;
    TaskSignalHome submitted_home{};
    bool const ROUTED_HOME = (submit->reserved & WKI_TASK_SUBMIT_GROUP_HOME) != 0;
    bool const FILESYSTEM = (submit->reserved & WKI_TASK_SUBMIT_FILESYSTEM) != 0;
    TaskFilesystemContext submitted_filesystem{};
    TaskChildCpuObservation submitted_child_observations{};
    bool const CREDENTIAL_TRANSITION = (submit->reserved & (WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION)) != 0;
    ker::mod::security::ExecIdentity submitted_exec_image{};
    auto resource_reject =
        resource_submission_admission((session.identity.capabilities & WKI_CAP_RESOURCE_LIMITS) != 0, submit->identity_len,
                                      (session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) != 0, ROUTED_HOME, FILESYSTEM);
    if (!valid_submit_filesystem_identity(*submit) ||
        (submit->reserved & ~(WKI_TASK_SUBMIT_EXEC_HANDOFF | WKI_TASK_SUBMIT_SPAWN_TRANSITION | WKI_TASK_SUBMIT_GROUP_HOME |
                              WKI_TASK_SUBMIT_FILESYSTEM)) != 0 ||
        (submit->reserved != 0 && (session.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) == 0))
        resource_reject = TaskRejectReason::RESOURCE_INVALID;
    if (!decode_submit_credential_transition(*submit, var_data, var_len, submitted_exec_image))
        resource_reject = TaskRejectReason::RESOURCE_INVALID;
    if (resource_reject == TaskRejectReason::ACCEPTED) {
        resource_reject = prepare_submitted_resources(submit, var_data, var_len, submit_context, submit_context_len, submitted_identity,
                                                      has_submitted_identity, submitted_resources, &stdio_mask, &service_reports,
                                                      &submitted_credentials, &submitted_origins, &submitted_signals, &submitted_home,
                                                      &submitted_filesystem, &submitted_child_observations);
    }
    if (resource_reject == TaskRejectReason::ACCEPTED && FILESYSTEM) {
        bool const BOUND = wki_task_filesystem_matches_admission(submitted_filesystem, src_node, session.identity.boot_epoch,
                                                                 g_wki.my_node_id, g_wki.local_boot_epoch);
        if (!BOUND)
            resource_reject = TaskRejectReason::RESOURCE_INVALID;
        else if ((submitted_filesystem.flags != WKI_TASK_FILESYSTEM_CWD_GRANT &&
                  submitted_filesystem.flags != (WKI_TASK_FILESYSTEM_CWD_GRANT | WKI_TASK_FILESYSTEM_ROOT_PATH)) ||
                 submitted_filesystem.cwd.owner_node != g_wki.my_node_id)
            resource_reject = TaskRejectReason::RESOURCE_UNSUPPORTED;
    }
    resource_observations_valid = has_resource_identity(submit->identity_len) &&
                                  (resource_reject == TaskRejectReason::ACCEPTED || resource_reject == TaskRejectReason::RESOURCE_CPU);
    if (resource_reject == TaskRejectReason::ACCEPTED) {
        size_t const IPC_BYTES = static_cast<size_t>(submit->ipc_fd_count) * sizeof(WkiIpcFdEntry);
        const auto* entries = reinterpret_cast<const WkiIpcFdEntry*>(submit_context + submit_context_len - IPC_BYTES);
        int const IPC_ADMISSION = wki_ipc_validate_task_fds(entries, submit->ipc_fd_count, src_node);
        if (IPC_ADMISSION != 0)
            resource_reject = IPC_ADMISSION == -EACCES ? TaskRejectReason::UNAUTHORIZED : TaskRejectReason::RESOURCE_INVALID;
    }
    if (resource_reject == TaskRejectReason::RESOURCE_CPU)
        submitted_resources.deny(static_cast<unsigned>(ker::abi::resource::Id::CPU), ker::abi::resource::Denial::HARD_LIMIT);
    if (resource_reject != TaskRejectReason::ACCEPTED) {
        TaskResponsePayload reject{};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(resource_reject);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(resource_reject));
        return;
    }

    // The channel/session above authenticates the request. Numeric identity
    // requires an independent, immutable boot-policy mapping before any source
    // fetch, process accounting reservation, or image construction.
    ker::mod::security::Snapshot mapped_credentials;
    ker::mod::security::Snapshot replacement_credentials;
    if (!has_submitted_identity ||
        !prepare_submitted_identity(submitted_identity, session.identity.policy, mapped_credentials,
                                    has_full_task_identity(submit->identity_len) ? &submitted_credentials : nullptr) ||
        (CREDENTIAL_TRANSITION && !prepare_submitted_exec_credentials(submitted_credentials, submitted_exec_image, session.identity.policy,
                                                                      replacement_credentials))) {
        TaskResponsePayload reject{};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::UNAUTHORIZED);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::UNAUTHORIZED));
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
    }

    auto* current_task = ker::mod::sched::get_current_task();
    std::optional<TaskFilesystemPreparation> received_owner;
    if (FILESYSTEM) {
        received_owner.emplace(current_task, nullptr, nullptr);
        auto& view = received_owner->received_view();
        int status = -ESTALE;
        auto admission_current = [&]() {
            PeerLifecycleLease lifecycle;
            if (!lifecycle.acquire(src_node) || capture_compute_peer_identity(src_node) != session.identity) return false;
            s_compute_lock.lock();
            bool const CURRENT = compute_submit_admission_is_current_locked(session);
            s_compute_lock.unlock();
            return CURRENT;
        };
        if (received_owner->status() == 0 && admission_current()) {
            WkiDirectoryGrantSource const SOURCE{.node = src_node,
                                                 .boot_epoch = session.identity.boot_epoch,
                                                 .submission_id = submitted_filesystem.submission_id,
                                                 .identity_policy = session.identity.policy};
            // Only private ownership is produced here. Even a native XFS view
            // may wait for a proxied block device: keep lifecycle writers out
            // of these calls and revalidate before installing any Task state.
            status = wki_remote_vfs_adopt_directory_grant_native(submitted_credentials, SOURCE, submitted_filesystem.cwd, view);
            if (status == 0 && (submitted_filesystem.flags & WKI_TASK_FILESYSTEM_ROOT_PATH) != 0) {
                status = prepare_received_root_path_cwd(*received_owner, *current_task, mapped_credentials, submitted_identity.root);
            } else if (status == 0) {
                status = ker::vfs::vfs_prepare_global_cwd(mapped_credentials, view, received_owner->received_filesystem());
            }
            if (status == 0 && (received_owner->status() != 0 || !admission_current())) status = -ESTALE;
        }
        view = {};
        if (status != 0) {
            received_owner->cancel();
            TaskResponsePayload reject{};
            reject.task_id = submit->task_id;
            auto const REASON = status == -EACCES || status == -EPERM ? TaskRejectReason::UNAUTHORIZED
                                : status == -ENOTSUP                  ? TaskRejectReason::RESOURCE_UNSUPPORTED
                                                                      : TaskRejectReason::RESOURCE_INVALID;
            reject.status = static_cast<uint8_t>(REASON);
            send_submit_response(MsgType::TASK_REJECT, reject);
            handle_measure.finish(perf_compute_reject_status(static_cast<TaskRejectReason>(reject.status)));
            return;
        }
    }
    const char* effective_submitter_hostname = (has_submitted_identity && submitted_identity.submitter_hostname.front() != '\0')
                                                   ? submitted_identity.submitter_hostname.data()
                                                   : wki_peer_get_hostname(hdr->src_node);
    // Source and interpreter reads share the submitted root/routes. Install
    // and validate them before fetching bytes, then transfer the same policy
    // into the unpublished task after construction succeeds.
    ScopedSubmitVfsIdentity submit_vfs_identity(current_task, has_submitted_identity ? &submitted_identity : nullptr,
                                                effective_submitter_hostname, policy_cursor, policy_len,
                                                received_owner ? &*received_owner : nullptr);
    if (reject_reason != TaskRejectReason::ACCEPTED || !submit_vfs_identity.policy_valid()) {
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

            auto vfs_result = load_elf_from_vfs_path(*current_task, path_buf.data(), hdr->src_node, submit->task_id, session,
                                                     mapped_credentials, deadline_us);
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
            auto vfs_result = load_elf_from_vfs_path(*current_task, ref_path.data(), hdr->src_node, submit->task_id, session,
                                                     mapped_credentials, deadline_us);
            if (vfs_result.buffer == nullptr && vfs_result.file == nullptr) {
                ker::mod::dbg::log("[WKI] RESOURCE_REF: failed to load node=0x%04x res=%u path='%s'", ref_node, ref_resource,
                                   ref_path.data());
                reject_reason = vfs_result.reject_reason;
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

    // Test-only C1 publication gate. This executes exclusively on the
    // receiver submit daemon after all input/source validation, before exec
    // construction or scheduler publication, and with no compute/peer lock or
    // lifecycle lease held. TASK_CANCEL remains on the independent compute-RX
    // worker and publishes its exact-session marker while this task waits.
    int const PUBLISH_WAIT_STATUS = wki_chaos_workload_compute_publish_wait(deadline_us);
    s_compute_lock.lock();
    bool const POST_HOLD_SESSION_CURRENT = compute_submit_admission_is_current_locked(session);
    int32_t const POST_HOLD_CANCEL_SIGNAL = take_cancelled_task_submit_locked(session, submit->task_id);
    s_compute_lock.unlock();
    bool const POST_HOLD_DEADLINE_EXPIRED = submit_deadline_expired();
    if (PUBLISH_WAIT_STATUS != 0 || !POST_HOLD_SESSION_CURRENT || POST_HOLD_CANCEL_SIGNAL != 0 || POST_HOLD_DEADLINE_EXPIRED) {
        release_elf_source();
        if (POST_HOLD_SESSION_CURRENT) {
            TaskResponsePayload reject = {};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            reject.remote_pid = 0;
            send_submit_response(MsgType::TASK_REJECT, reject);
        }
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    ExecResult const EXEC =
        elf_file != nullptr
            ? exec_elf_file(elf_file, binary_len, elf_file_stat, submitted_resources, mapped_credentials, stdio_mask)
            : exec_elf_buffer(elf_buffer, binary_len, elf_buffer_shared, submitted_resources, mapped_credentials, stdio_mask);
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
    child_events::import_reaped_cpu_times(*new_task, submitted_child_observations.user_time_us,
                                          submitted_child_observations.system_time_us);

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
    if (ROUTED_HOME) {
        auto const ORIGIN = submitted_home.origin.node == 0
                                ? ker::mod::sched::session::Origin{.node = src_node, .boot = session.identity.boot_epoch}
                                : ker::mod::sched::session::Origin{.node = submitted_home.origin.node, .boot = submitted_home.origin.boot};
        parse_ok &= new_task->adopt_signal_home({.origin = ORIGIN, .serial = submitted_home.serial});
    }
    new_task->hide_group_signals_until_publication();

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
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-parse", &submitted_resources);

        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::FETCH_FAILED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit parse failed: task_id=%u", submit->task_id);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::FETCH_FAILED), binary_len);
        return;
    }

    uint32_t const SOURCE_BOOT = session.identity.boot_epoch;
    if (!apply_submitted_task_identity(new_task, submitted_identity, mapped_credentials,
                                       resolve_session_origin(submitted_origins.session, hdr->src_node, SOURCE_BOOT),
                                       resolve_session_origin(submitted_origins.group, hdr->src_node, SOURCE_BOOT)) ||
        !submit_vfs_identity.install_filesystem_on(*new_task)) {
        delete[] argv_strings;
        delete[] envp_strings;
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-population", &submitted_resources);
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
        return;
    }

    install_exec_signals(*new_task, submitted_signals);

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
    if (!FILESYSTEM) {
        memcpy(new_task->cwd.data(), task_cwd, cwd_copy_len);
        *std::next(new_task->cwd.begin(), static_cast<ptrdiff_t>(cwd_copy_len)) = '\0';
        new_task->cwd_len = static_cast<uint16_t>(cwd_copy_len);
    }

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
            if (!ker::mod::sched::threading::ensure_stack_backing(new_task->thread, new_task->pagemap, vaddr, END, new_task)) {
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

        uint64_t const AT_EXECFN_ADDR = push_string(new_task->exe_path.data());
        if (AT_EXECFN_ADDR == 0) {
            delete[] argv_addrs;
            delete[] envp_addrs;
            return false;
        }

        std::array<uint8_t, 16> at_random{};
        if (!ker::mod::random::entropy::get_bytes(at_random.data(), at_random.size())) {
            delete[] argv_addrs;
            delete[] envp_addrs;
            return false;
        }
        uint64_t const AT_RANDOM_ADDR = push_to_stack(at_random.data(), at_random.size());
        std::memset(at_random.data(), 0, at_random.size());
        if (AT_RANDOM_ADDR == 0) {
            delete[] argv_addrs;
            delete[] envp_addrs;
            return false;
        }
        new_task->at_random_addr = AT_RANDOM_ADDR;

        // Align to 16 bytes
        constexpr uint64_t ALIGN = 16;
        uint64_t const CUR = user_stack_top - stack_offset;
        uint64_t const ALIGNED = CUR & ~(ALIGN - 1);
        stack_offset += (CUR - ALIGNED);

        size_t const AUXV_QWORDS = 18 + ker::mod::security::exec_policy::IDENTITY_AUXV_WORDS + (new_task->interp_base != 0 ? 2 : 0);
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
        constexpr uint64_t AT_RANDOM = 25;
        constexpr uint64_t AT_EXECFN = 31;
        constexpr uint64_t AT_EHDR = 33;

        std::array<uint64_t, 32> auxv = {};
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
        append_auxv(AT_RANDOM, AT_RANDOM_ADDR);
        append_auxv(AT_EXECFN, AT_EXECFN_ADDR);
        if (new_task->interp_base != 0) {
            append_auxv(AT_BASE, new_task->interp_base);
        }
        auto const IDENTITY_AUXV =
            ker::mod::security::exec_policy::identity_auxv(CREDENTIAL_TRANSITION ? replacement_credentials : mapped_credentials);
        for (size_t i = 0; i < IDENTITY_AUXV.size(); i += 2) append_auxv(IDENTITY_AUXV[i], IDENTITY_AUXV[i + 1]);
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
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-stack", &submitted_resources);

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
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-expired", &submitted_resources);

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

    // The private guard owns receipt refs until monitor publication succeeds.
    // It retains no File and unwinds every rejection without delaying close.
    std::unique_ptr<WkiIpcOutputCompletion> output_completion;
    // Attach IPC proxy fds if the submitter exported any.
    if (submit->ipc_fd_count > 0 && submit_context != nullptr) {
        // IPC fd entries are appended after argv/envp/cwd in the submit context.
        // Find them by skipping past submit_context_len - (ipc_fd_count * sizeof(WkiIpcFdEntry)).
        uint16_t const IPC_DATA_SIZE = submit->ipc_fd_count * sizeof(WkiIpcFdEntry);
        if (submit_context_len >= IPC_DATA_SIZE) {
            const auto* ipc_entries = reinterpret_cast<const WkiIpcFdEntry*>(submit_context + submit_context_len - IPC_DATA_SIZE);
            WkiChannelIdentity const IPC_CONTROL_CHANNEL{
                .channel = session.rx_channel,
                .peer_node_id = src_node,
                .channel_id = WKI_CHAN_RESOURCE,
                .generation = session.rx_channel_generation,
            };
            auto const IPC_DATA_CHANNEL = wki_ipc_capture_data_channel(IPC_CONTROL_CHANNEL);
            output_completion.reset(new (std::nothrow) WkiIpcOutputCompletion());
            if (!output_completion || !wki_ipc_attach_task_fds(new_task, ipc_entries, submit->ipc_fd_count, src_node, IPC_CONTROL_CHANNEL,
                                                               IPC_DATA_CHANNEL, output_completion.get())) {
                destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-ipc-import", &submitted_resources);
                TaskResponsePayload reject{};
                reject.task_id = submit->task_id;
                reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
                send_submit_response(MsgType::TASK_REJECT, reject);
                handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
                return;
            }
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
    if (service_reports) {
        auto* accounting = ker::mod::sched::resource::RemoteServiceAccounting::create();
        bool const ATTACHED = accounting != nullptr && accounting->attach(*new_task, &submitted_resources);
        if (accounting != nullptr) accounting->release();
        if (!ATTACHED) {
            destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-service", &submitted_resources);
            TaskResponsePayload reject{};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
            send_submit_response(MsgType::TASK_REJECT, reject);
            handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
            return;
        }
    }
    // Fetch, interpreter loading, filesystem and inherited-FD preparation all
    // used the original admitted identity. Publish the admitted replacement
    // exactly once while this Task is still private and cannot execute.
    if (CREDENTIAL_TRANSITION &&
        ker::mod::sched::resource::publish_exec_credentials(*new_task, mapped_credentials, replacement_credentials) != 0) {
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-exec-credentials", &submitted_resources);
        TaskResponsePayload reject{};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::RESOURCE_INVALID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::RESOURCE_INVALID), binary_len);
        return;
    }
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
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-ref", &submitted_resources);
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
    rt.exec_handoff = (submit->reserved & WKI_TASK_SUBMIT_EXEC_HANDOFF) != 0;
    rt.accept_pending = false;
    rt.task_id = submit->task_id;
    rt.submitter_node = hdr->src_node;
    rt.local_pid = LAUNCHED_PID;
    rt.submit_session_epoch = session.epoch;
    rt.submit_rx_channel = session.rx_channel;
    rt.submit_rx_channel_generation = session.rx_channel_generation;
    rt.task = completion_task_ref;
    rt.resource_contract = has_resource_identity(submit->identity_len);
    rt.output = EXEC.output;
    rt.output_completion = output_completion.get();

    // Linearize scheduler publication with peer epoch/disconnect cleanup.
    // Lifecycle teardown owns the same lease before retiring this session, so
    // it either invalidates us before this final check or runs after the task
    // and its monitor row have been published consistently.
    PeerLifecycleLease publication_lease;
    if (!publication_lease.acquire(src_node)) {
        worker_task_ref->release();
        completion_task_ref->release();
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, "wki-submit-peer", &submitted_resources);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }

    s_compute_lock.lock();
    bool const FINAL_SESSION_CURRENT = compute_submit_admission_is_current_locked(session);
    int32_t const FINAL_CANCEL_SIGNAL = take_cancelled_task_submit_locked(session, submit->task_id);
    bool const FINAL_DEADLINE_EXPIRED = submit_deadline_expired();
    if (!FINAL_SESSION_CURRENT || FINAL_CANCEL_SIGNAL != 0 || FINAL_DEADLINE_EXPIRED) {
        s_compute_lock.unlock();
        // Private teardown may await VFS pin cancellation from this peer.
        // Its RX responses need the lifecycle gate released first.
        publication_lease.release();
        worker_task_ref->release();
        completion_task_ref->release();
        const char* failure_reason = "wki-submit-expired";
        if (!FINAL_SESSION_CURRENT) {
            failure_reason = "wki-submit-session";
        } else if (FINAL_CANCEL_SIGNAL != 0) {
            failure_reason = "wki-submit-cancel";
        }
        destroy_unpublished_remote_process(current_task, new_task, EXEC.output, failure_reason, &submitted_resources);
        if (FINAL_SESSION_CURRENT) {
            TaskResponsePayload reject = {};
            reject.task_id = submit->task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            send_submit_response(MsgType::TASK_REJECT, reject);
        }
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }
    if (service_reports) {
        rt.service_accounting = new_task->service_member.remote;
        rt.service_accounting->retain();
    }
    if (!publish_running_task_locked(rt)) {
        s_compute_lock.unlock();
        publication_lease.release();
        discard_unpublished_running_task(current_task, worker_task_ref, rt, &submitted_resources);
        TaskResponsePayload reject{};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::NO_MEM), binary_len);
        return;
    }
    static_cast<void>(output_completion.release());
    s_compute_lock.unlock();

    bool handoff_ready = true;
    if (rt.exec_handoff) {
        // Prepared construction owns this unpublished task. Do not hold a
        // lifecycle lease across the source's commit wait: fencing marks the
        // row discarded and the submit worker performs unpublished cleanup.
        publication_lease.release();
        TaskExecReady const READY{.task_id = submit->task_id, .remote_pid = LAUNCHED_PID};
        uint64_t const READY_DEADLINE = std::min(deadline_us, wki_future_deadline_us(wki_now_us(), 1'000'000));
        handoff_ready = retry_compute_control(
                            READY_DEADLINE,
                            [&]() {
                                return wki_send_on_channel_generation(src_node, session.rx_channel, session.rx_channel_generation,
                                                                      MsgType::TASK_EXEC_READY, &READY, sizeof(READY));
                            },
                            [&]() {
                                s_compute_lock.lock();
                                bool const CURRENT = compute_submit_admission_is_current_locked(session);
                                s_compute_lock.unlock();
                                return CURRENT;
                            },
                            []() { return wki_now_us(); }, [](uint64_t until) { sleep_until_us(until, 1000); }) == WKI_OK;
        bool committed = false;
        TaskExecSignals final_signals{};
        while (handoff_ready && !submit_deadline_expired()) {
            s_compute_lock.lock();
            handoff_ready =
                compute_submit_admission_is_current_locked(session) && take_cancelled_task_submit_locked(session, submit->task_id) == 0;
            bool found = false;
            for (auto& running : g_running_remote_tasks) {
                if (running.active && !running.published && running.task == completion_task_ref && running.local_pid == LAUNCHED_PID) {
                    found = true;
                    handoff_ready &= !running.discard_completion;
                    committed = running.exec_committed;
                    final_signals = running.exec_signals;
                    break;
                }
            }
            handoff_ready &= found;
            s_compute_lock.unlock();
            if (!handoff_ready || committed) break;
            sleep_until_us(deadline_us, 1000);
        }
        if (handoff_ready && committed) {
            install_exec_signals(*new_task, final_signals);
            s_compute_lock.lock();
            for (auto& running : g_running_remote_tasks) {
                if (running.active && running.task == new_task && running.task_id == submit->task_id &&
                    running_task_session_matches(running, session) && !running.discard_completion)
                    running.group_signal_ready = true;
            }
            s_compute_lock.unlock();
        }
        // The diagnostic commit-stage witness holds only the owning submit
        // daemon. RX and group requests keep running; no lifecycle lease or
        // compute lock is held. Revalidate cancellation/session after release.
        if (handoff_ready && committed)
            handoff_ready = wki_chaos_workload_compute_publish_wait(deadline_us, ComputePublishStage::COMMIT) == 0;
        handoff_ready = handoff_ready && committed && !submit_deadline_expired() && publication_lease.acquire(src_node);
        if (handoff_ready) {
            s_compute_lock.lock();
            handoff_ready =
                compute_submit_admission_is_current_locked(session) && take_cancelled_task_submit_locked(session, submit->task_id) == 0;
            s_compute_lock.unlock();
        }
        if (!handoff_ready)
            ker::mod::dbg::log("[WKI] Exec prepared wait failed: task_id=%u committed=%d expired=%d", submit->task_id, committed,
                               submit_deadline_expired());
    }

    // The unpublished row owns the monitor reference/output but is invisible
    // to cancel and completion readers until scheduler publication returns.
    bool const POSTED = post_admitted_remote_process(session, submit->task_id, deadline_us, publication_lease, handoff_ready,
                                                     [&]() { return ker::mod::sched::post_task_balanced(new_task); });
    if (POSTED) {
        new_task->publish_group_signal_visibility();
        if (new_task->signal_pending_bits() != 0) ker::mod::sched::wake_task_for_signal(new_task);
    }

    if (POSTED && !ker::mod::sched::task::release_unpublished_process(current_task, new_task)) [[unlikely]] {
        ker::mod::dbg::panic_handler("WKI remote compute: lost receiver child ownership after scheduler publication");
        hcf();
    }
    if (!POSTED) {
        discard_unposted_remote_process(current_task, new_task, worker_task_ref, completion_task_ref, EXEC.output, publication_lease,
                                        &submitted_resources);
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        reject.remote_pid = 0;
        ker::mod::dbg::log("[WKI] Task submit post failed: task_id=%u pid=0x%lx", submit->task_id, LAUNCHED_PID);
        send_submit_response(MsgType::TASK_REJECT, reject);
        handle_measure.finish(perf_compute_reject_status(TaskRejectReason::OVERLOADED), binary_len);
        return;
    }
    bool send_accept = false;
    int32_t post_cancel_signal = 0;
    s_compute_lock.lock();
    RunningRemoteTask* published_row = nullptr;
    for (auto& running : g_running_remote_tasks) {
        if (running.active && !running.published && running.task == completion_task_ref && running.local_pid == LAUNCHED_PID) {
            published_row = &running;
            break;
        }
    }
    if (published_row != nullptr) {
        published_row->published = true;
        bool const CURRENT = compute_submit_admission_is_current_locked(session);
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
    launched_cpu = static_cast<int>(new_task->load_cpu());
#endif

    // Send TASK_ACCEPT
    TaskResponsePayload accept = {};
    accept.task_id = submit->task_id;
    accept.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
    accept.remote_pid = LAUNCHED_PID;
    accept.reserved[0] = WKI_TASK_SIGNAL_VERSION;
    if (post_cancel_signal != 0) {
        uint64_t const OWNER_PID = task_process_owner_pid(new_task);
        size_t const SIGNALED = queue_signal_for_process_tasks(OWNER_PID, post_cancel_signal);
        if (SIGNALED == 0 && !new_task->has_exited && post_cancel_signal > 0 &&
            std::cmp_less_equal(post_cancel_signal, ker::mod::sched::task::Task::MAX_SIGNALS)) {
            new_task->signal_generate_pending_mask(1ULL << (post_cancel_signal - 1));
            ker::mod::sched::wake_task_for_signal(new_task);
        }
    } else if (send_accept) {
        int const ACCEPT_RESULT = send_submit_response(MsgType::TASK_ACCEPT, accept);
        s_compute_lock.lock();
        if (auto* running = find_running_task(submit->task_id, session); running != nullptr) {
            if (ACCEPT_RESULT == WKI_OK) {
                running->accept_pending = false;
            } else if (!compute_submit_admission_is_current_locked(session)) {
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

    if (type == MsgType::TASK_CHILD_CLAIM_REQ || type == MsgType::TASK_CHILD_CLAIM_RESP) {
        bool const REQUEST = type == MsgType::TASK_CHILD_CLAIM_REQ;
        size_t const SIZE = REQUEST ? sizeof(TaskChildClaimRequest) : sizeof(TaskChildClaimResponse);
        if (payload == nullptr || payload_len != SIZE) return WkiComputeRxAdmission::DISCARD;
        ChildClaimRequest request{};
        bool valid = false;
        if (REQUEST) {
            TaskChildClaimRequest wire{};
            std::memcpy(&wire, payload, sizeof(wire));
            valid = decode_child_claim_request(wire, request);
        } else {
            TaskChildClaimResponse wire{};
            ChildWaitReply reply{};
            std::memcpy(&wire, payload, sizeof(wire));
            valid = decode_child_claim_response(wire, request, reply);
        }
        if (!valid) return WkiComputeRxAdmission::DISCARD;
        *copy_len = static_cast<uint16_t>(SIZE);
        return WkiComputeRxAdmission::DEFERRED;
    }
    if (type == MsgType::TASK_CHILD_QUERY_REQ || type == MsgType::TASK_CHILD_QUERY_RESP) {
        bool const REQUEST = type == MsgType::TASK_CHILD_QUERY_REQ;
        size_t const SIZE = REQUEST ? sizeof(TaskChildQueryRequest) : sizeof(TaskChildQueryResponse);
        if (payload == nullptr || payload_len != SIZE) return WkiComputeRxAdmission::DISCARD;
        ChildQueryRequest request{};
        bool valid = false;
        if (REQUEST) {
            TaskChildQueryRequest wire{};
            std::memcpy(&wire, payload, sizeof(wire));
            valid = decode_child_query_request(wire, request);
        } else {
            TaskChildQueryResponse wire{};
            ChildQueryReply reply{};
            std::memcpy(&wire, payload, sizeof(wire));
            valid = decode_child_query_response(wire, request, reply);
        }
        if (!valid) return WkiComputeRxAdmission::DISCARD;
        *copy_len = static_cast<uint16_t>(SIZE);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::GROUP_ROUTE_REQ || type == MsgType::GROUP_RECEIVER_REQ) {
        if (payload == nullptr || payload_len != sizeof(GroupRouteRequest)) return WkiComputeRxAdmission::DISCARD;
        GroupRouteRequest request{};
        std::memcpy(&request, payload, sizeof(request));
        bool const ENUMERATE = request.operation == static_cast<uint8_t>(GroupRouteOperation::ENUMERATE);
        bool const DELIVER = request.operation == static_cast<uint8_t>(GroupRouteOperation::DELIVER);
        if (type == MsgType::GROUP_RECEIVER_REQ && !ENUMERATE) return WkiComputeRxAdmission::DISCARD;
        if (request.request_id == 0 || request.version != WKI_GROUP_ROUTE_VERSION || request.reserved != 0 || request.pgid == 0 ||
            request.operation > static_cast<uint8_t>(GroupRouteOperation::DELIVER) || request.signum < 0 ||
            std::cmp_greater(request.signum, ker::mod::sched::task::Task::MAX_SIGNALS) || (!DELIVER && request.signum != 0) ||
            !valid_session_origin(request.session_origin) || request.session_origin.node == WKI_NODE_BROADCAST ||
            !valid_session_origin(request.group_origin) || request.group_origin.node == WKI_NODE_BROADCAST)
            return WkiComputeRxAdmission::DISCARD;
        if (ENUMERATE) {
            if (request.home.serial != 0 || request.home.origin.node != 0 || request.home.origin.boot != 0 ||
                request.home.origin.reserved != 0 || request.target_pid != 0 ||
                (request.cursor != 0 && (request.ceiling == 0 || request.cursor > request.ceiling)))
                return WkiComputeRxAdmission::DISCARD;
        } else if (request.home.serial == 0 || request.home.origin.node == 0 || request.home.origin.node == WKI_NODE_BROADCAST ||
                   !valid_session_origin(request.home.origin) || request.target_pid == 0 || request.cursor != 0 || request.ceiling != 0)
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::GROUP_SIGNAL_REQ) {
        if (payload == nullptr || payload_len != sizeof(GroupSignalRequest)) return WkiComputeRxAdmission::DISCARD;
        GroupSignalRequest request{};
        std::memcpy(&request, payload, sizeof(request));
        bool const QUERY = request.operation == static_cast<uint8_t>(GroupSignalOperation::QUERY);
        if (request.request_id == 0 || request.version != WKI_GROUP_SIGNAL_VERSION || request.reserved != 0 || request.pgid == 0 ||
            request.operation > static_cast<uint8_t>(GroupSignalOperation::DELIVER) || request.signum < 0 ||
            std::cmp_greater(request.signum, ker::mod::sched::task::Task::MAX_SIGNALS) || (QUERY && request.signum != 0) ||
            !valid_session_origin(request.session_origin) || !valid_session_origin(request.group_origin) ||
            request.session_origin.node == WKI_NODE_BROADCAST || request.group_origin.node == WKI_NODE_BROADCAST)
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_COMPLETE) {
        if (payload == nullptr || payload_len < sizeof(TaskCompletePayload)) {
            return WkiComputeRxAdmission::DISCARD;
        }
        TaskCompletePayload complete = {};
        std::memcpy(&complete, payload, sizeof(complete));
        size_t const EXPECTED_LEN = sizeof(TaskCompletePayload) + complete.output_len + complete.reserved;
        if ((complete.reserved != 0 && complete.reserved != sizeof(TaskResourceSnapshot)) || complete.output_len > WKI_TASK_MAX_OUTPUT ||
            EXPECTED_LEN > payload_len) {
            return WkiComputeRxAdmission::DISCARD;
        }
        *copy_len = static_cast<uint16_t>(EXPECTED_LEN);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_CHILD_SIGNAL) {
        if (payload == nullptr || payload_len != sizeof(TaskChildSignal)) return WkiComputeRxAdmission::DISCARD;
        TaskChildSignal request{};
        std::memcpy(&request, payload, sizeof(request));
        if (request.task_id == 0 || request.version != WKI_CHILD_SIGNAL_VERSION || request.hops >= WKI_CHILD_SIGNAL_HOPS)
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_PARENT_DEATH) {
        if (payload == nullptr || payload_len != sizeof(TaskParentDeath)) return WkiComputeRxAdmission::DISCARD;
        TaskParentDeath request{};
        std::memcpy(&request, payload, sizeof(request));
        if (request.task_id == 0 || request.version != WKI_PARENT_DEATH_VERSION || request.hops >= WKI_PARENT_DEATH_HOPS)
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_TERMINAL_SIGNAL) {
        if (payload == nullptr || payload_len != sizeof(TaskTerminalSignal)) return WkiComputeRxAdmission::DISCARD;
        TaskTerminalSignal request{};
        std::memcpy(&request, payload, sizeof(request));
        auto concrete = [](const TaskSessionOrigin& origin) {
            return valid_session_origin(origin) && origin.node != 0 && origin.node != WKI_NODE_BROADCAST;
        };
        if (request.task_id == 0 || request.version != WKI_TERMINAL_SIGNAL_VERSION || request.reserved != 0 ||
            request.hops >= WKI_TERMINAL_SIGNAL_HOPS || request.signum <= 0 ||
            std::cmp_greater(request.signum, ker::mod::sched::task::Task::MAX_SIGNALS) || request.session_id == 0 || request.pgid == 0 ||
            !concrete(request.origins.session) || !concrete(request.origins.group))
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
        return WkiComputeRxAdmission::DEFERRED;
    }

    if (type == MsgType::TASK_SIGNAL_REQ) {
        if (payload == nullptr || payload_len != sizeof(TaskSignalRequest)) return WkiComputeRxAdmission::DISCARD;
        TaskSignalRequest request{};
        std::memcpy(&request, payload, sizeof(request));
        if (request.request_id == 0 || request.task_id == 0 || request.signum < 0 ||
            std::cmp_greater(request.signum, ker::mod::sched::task::Task::MAX_SIGNALS) || !valid_session_origin(request.origin))
            return WkiComputeRxAdmission::DISCARD;
        *copy_len = sizeof(request);
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

    if (type == MsgType::TASK_SUBMIT_FRAGMENT) {
        if (payload == nullptr || payload_len <= sizeof(TaskSubmitFragmentPayload)) {
            return WkiComputeRxAdmission::DISCARD;
        }
        TaskSubmitFragmentPayload fragment = {};
        std::memcpy(&fragment, payload, sizeof(fragment));
        size_t const CHUNK_LEN = payload_len - sizeof(TaskSubmitFragmentPayload);
        if (fragment.task_id == 0 || fragment.total_len <= WKI_ETH_MAX_PAYLOAD || fragment.offset >= fragment.total_len ||
            std::cmp_greater(CHUNK_LEN, fragment.total_len - fragment.offset)) {
            return WkiComputeRxAdmission::DISCARD;
        }
        if (fragment.offset == 0) {
            if (CHUNK_LEN < sizeof(TaskSubmitPayload)) {
                return WkiComputeRxAdmission::DISCARD;
            }
            TaskSubmitPayload submit = {};
            std::memcpy(&submit, payload + sizeof(TaskSubmitFragmentPayload), sizeof(submit));
            if (submit.task_id != fragment.task_id || !valid_submit_filesystem_identity(submit)) {
                return WkiComputeRxAdmission::DISCARD;
            }
        }
        *copy_len = payload_len;
        return WkiComputeRxAdmission::DEFERRED;
    }

    return WkiComputeRxAdmission::INLINE;
}

void release_fragmented_task_submits(uint16_t node_id, WkiChannel* channel, uint32_t generation) {
    std::array<uint8_t*, WKI_COMPUTE_SUBMIT_FRAGMENT_MAX> released = {};
    size_t released_count = 0;

    s_fragmented_task_submit_lock.lock();
    for (auto& fragmented : s_fragmented_task_submits) {
        if (!fragmented.active || fragmented.src_node != node_id) {
            continue;
        }
        if (channel != nullptr && (fragmented.rx_channel != channel || fragmented.rx_channel_generation != generation)) {
            continue;
        }
        released.at(released_count++) = fragmented.payload;
        fragmented = {};
    }
    s_fragmented_task_submit_lock.unlock();

    for (size_t i = 0; i < released_count; ++i) {
        delete[] released.at(i);
    }
}

void queue_admitted_task_submit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len,
                                const ComputeSubmitSessionToken& SESSION) {
    s_compute_lock.lock();
    bool const CURRENT = compute_submit_admission_is_current_locked(SESSION);
    s_compute_lock.unlock();
    if (!CURRENT) return;
    const auto* submit = reinterpret_cast<const TaskSubmitPayload*>(payload);
    auto mode = static_cast<TaskDeliveryMode>(submit->delivery_mode);
    auto reject_submit = [&](TaskRejectReason reason) {
        TaskResponsePayload reject = {};
        reject.task_id = submit->task_id;
        reject.status = static_cast<uint8_t>(reason);
        reject.remote_pid = 0;
        wki_send_on_channel_generation(hdr->src_node, SESSION.rx_channel, SESSION.rx_channel_generation, MsgType::TASK_REJECT, &reject,
                                       sizeof(reject));
    };

    if (!valid_submit_filesystem_identity(*submit)) {
        reject_submit(TaskRejectReason::RESOURCE_INVALID);
        return;
    }

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

void handle_task_submit_fragment_work(const WkiHeader& hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                      uint32_t rx_channel_generation, const ComputePeerIdentity& identity) {
    s_compute_lock.lock();
    auto const SESSION = capture_compute_submit_session_locked(hdr.src_node, rx_channel, rx_channel_generation, identity);
    s_compute_lock.unlock();
    if (SESSION.epoch == 0) return;
    TaskSubmitFragmentPayload fragment = {};
    std::memcpy(&fragment, payload, sizeof(fragment));
    auto const* chunk = payload + sizeof(TaskSubmitFragmentPayload);
    auto const CHUNK_LEN = static_cast<uint16_t>(payload_len - sizeof(TaskSubmitFragmentPayload));

    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    std::unique_ptr<uint8_t[]> allocation;
    if (fragment.offset == 0) {
        allocation.reset(new (std::nothrow) uint8_t[fragment.total_len]);
        if (allocation == nullptr) {
            TaskResponsePayload reject = {};
            reject.task_id = fragment.task_id;
            reject.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
            wki_send_on_channel_generation(hdr.src_node, rx_channel, rx_channel_generation, MsgType::TASK_REJECT, &reject, sizeof(reject));
            return;
        }
    }

    uint8_t* completed_payload = nullptr;
    s_fragmented_task_submit_lock.lock();
    FragmentedTaskSubmit* entry = nullptr;
    for (auto& candidate : s_fragmented_task_submits) {
        if (candidate.active && candidate.src_node == hdr.src_node && candidate.rx_channel == rx_channel &&
            candidate.rx_channel_generation == rx_channel_generation && candidate.task_id == fragment.task_id &&
            compute_submit_session_token_matches(candidate.session, SESSION) && candidate.session.identity == SESSION.identity) {
            entry = &candidate;
            break;
        }
    }

    if (entry == nullptr && fragment.offset == 0) {
        for (auto& candidate : s_fragmented_task_submits) {
            if (!candidate.active) {
                entry = &candidate;
                candidate.session = SESSION;
                candidate.active = true;
                candidate.src_node = hdr.src_node;
                candidate.rx_channel = rx_channel;
                candidate.rx_channel_generation = rx_channel_generation;
                candidate.task_id = fragment.task_id;
                candidate.total_len = fragment.total_len;
                candidate.received_len = 0;
                candidate.payload = allocation.release();
                break;
            }
        }
    }

    bool const ACCEPT = entry != nullptr && entry->total_len == fragment.total_len && entry->received_len == fragment.offset &&
                        CHUNK_LEN <= static_cast<uint16_t>(entry->total_len - entry->received_len);
    bool const CAPACITY_REJECTED = entry == nullptr && fragment.offset == 0;
    if (ACCEPT) {
        std::memcpy(entry->payload + entry->received_len, chunk, CHUNK_LEN);
        entry->received_len = static_cast<uint16_t>(entry->received_len + CHUNK_LEN);
        if (entry->received_len == entry->total_len) {
            completed_payload = entry->payload;
            *entry = {};
        }
    }
    s_fragmented_task_submit_lock.unlock();

    if (CAPACITY_REJECTED) {
        TaskResponsePayload reject = {};
        reject.task_id = fragment.task_id;
        reject.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
        wki_send_on_channel_generation(hdr.src_node, rx_channel, rx_channel_generation, MsgType::TASK_REJECT, &reject, sizeof(reject));
        return;
    }
    if (completed_payload != nullptr) {
        queue_admitted_task_submit(&hdr, completed_payload, fragment.total_len, SESSION);
        delete[] completed_payload;
    }
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

auto authorize_user_signal(const PendingComputeRx& work) -> TaskSignalResponse {
    TaskSignalRequest request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    TaskSignalResponse response{.request_id = request.request_id, .task_id = request.task_id, .result = -ESRCH};
    ker::mod::security::Snapshot supplied, caller;
    response.result = ker::mod::security::credential_record::decode(request.credentials, supplied);
    if (response.result != 0) return response;
    uint64_t const POLICY = work.identity.policy;
    response.result = auth_policy::map_compute_identity(POLICY, supplied, caller);
    if (response.result != 0) return response;
    uint32_t const SOURCE_BOOT = work.identity.boot_epoch;
    if (SOURCE_BOOT == 0) {
        response.result = -EACCES;
        return response;
    }
    ker::mod::sched::task::Task* target = nullptr;
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    auto* running = SESSION.epoch != 0 ? find_running_task(request.task_id, SESSION) : nullptr;
    if (running != nullptr && running->published && !running->root_completion_queued && running->task != nullptr &&
        running->task->try_acquire())
        target = running->task;
    s_compute_lock.unlock();
    if (target == nullptr) {
        response.result = -ESRCH;
        return response;
    }
    ker::mod::sched::session::Identity job{.session_id = request.session_id,
                                           .origin = resolve_session_origin(request.origin, work.hdr.src_node, SOURCE_BOOT)};
    // signal_as consumes the exact retained row target, including across a
    // further remote handoff or fatal worker cleanup. No PID fallback exists.
    response.result = ker::syscall::process::signal_as(*target, caller, job, request.signum);
    return response;
}

auto deliver_child_signal(const PendingComputeRx& work) -> int {
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    if ((work.identity.policy & REQUIRED) != REQUIRED || (work.identity.capabilities & WKI_CAP_COMPUTE_CREDENTIALS) == 0) return -EACCES;
    TaskChildSignal request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    ker::mod::sched::task::Task* target = nullptr;
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    auto* running = SESSION.epoch != 0 ? find_running_task(request.task_id, SESSION) : nullptr;
    if (running != nullptr && running->published && !running->discard_completion && !running->termination_requested &&
        !running->root_completion_queued && running->task != nullptr && running->task->try_acquire())
        target = running->task;
    s_compute_lock.unlock();
    if (target == nullptr) return -ESRCH;
    // This exact published execution owns only a SIGCHLD notification. Further
    // hops enqueue outside this peer lease; never recursively wait on RX here.
    ker::syscall::process::signal_child_event(*target, request.hops);
    target->release();
    return 0;
}

auto deliver_parent_death(const PendingComputeRx& work) -> int {
    TaskParentDeath request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    ker::mod::sched::task::Task* target = nullptr;
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    auto* running = SESSION.epoch != 0 ? find_running_task(request.task_id, SESSION) : nullptr;
    if (running != nullptr && running->published && !running->root_completion_queued && running->task != nullptr &&
        running->task->try_acquire())
        target = running->task;
    s_compute_lock.unlock();
    if (target == nullptr) return -ESRCH;
    // Only this accepted execution chain may forward the lifecycle event.
    // The current child registration selects the signal, including disabling it.
    return ker::syscall::process::signal_parent_death(*target, request.hops);
}

auto deliver_terminal_signal(const PendingComputeRx& work) -> int {
    TaskTerminalSignal request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    ker::mod::sched::task::Task* target = nullptr;
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    auto* running = SESSION.epoch != 0 ? find_running_task(request.task_id, SESSION) : nullptr;
    if (running != nullptr && !running->root_completion_queued && running->task != nullptr && running->task->try_acquire())
        target = running->task;
    s_compute_lock.unlock();
    if (target == nullptr) return -ESRCH;
    ker::mod::sched::session::Identity const EXPECTED{
        .session_id = request.session_id,
        .pgid = request.pgid,
        .origin = resolve_session_origin(request.origins.session, work.hdr.src_node, work.identity.boot_epoch),
        .group_origin = resolve_session_origin(request.origins.group, work.hdr.src_node, work.identity.boot_epoch)};
    // The accepted-task grant names an exact retained object. Current receiver
    // membership decides the event; there is no PID or lifecycle-cancel fallback.
    return ker::syscall::process::signal_terminal_as(*target, EXPECTED, request.signum, request.hops);
}

auto map_group_caller(const PendingComputeRx& work, const GroupRouteRequest& request, ker::mod::security::Snapshot& caller,
                      ker::mod::sched::session::Identity& job) -> int {
    ker::mod::security::Snapshot supplied;
    int const DECODED = ker::mod::security::credential_record::decode(request.credentials, supplied);
    if (DECODED != 0) return DECODED;
    uint64_t const POLICY = work.identity.policy;
    int const MAPPED = auth_policy::map_compute_identity(POLICY, supplied, caller);
    if (MAPPED != 0) return MAPPED;
    uint32_t const BOOT = work.identity.boot_epoch;
    if (BOOT == 0) return -EACCES;
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    s_compute_lock.unlock();
    if (SESSION.epoch == 0) return -ESRCH;
    job = {.session_id = request.session_id,
           .pgid = request.caller_pgid,
           .origin = resolve_session_origin(request.session_origin, work.hdr.src_node, BOOT),
           .group_origin = resolve_session_origin(request.group_origin, work.hdr.src_node, BOOT)};
    return 0;
}

auto authorize_group_route(const PendingComputeRx& work) -> GroupRouteResponse {
    GroupRouteRequest request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    GroupRouteResponse response{.request_id = request.request_id, .boot = g_wki.local_boot_epoch};
    ker::mod::security::Snapshot caller;
    ker::mod::sched::session::Identity job{};
    response.result = map_group_caller(work, request, caller, job);
    if (response.result == 0) execute_group_route(request, caller, job, response);
    return response;
}

auto authorize_group_receivers(const PendingComputeRx& work) -> GroupReceiverResponse {
    GroupRouteRequest request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    GroupReceiverResponse response{.request_id = request.request_id, .boot = g_wki.local_boot_epoch};
    ker::mod::security::Snapshot caller;
    ker::mod::sched::session::Identity job{};
    response.result = map_group_caller(work, request, caller, job);
    if (response.result == 0) collect_group_receivers(request, response);
    return response;
}

auto authorize_group_signal(const PendingComputeRx& work) -> GroupSignalResponse {
    GroupSignalRequest request{};
    std::memcpy(&request, work.payload.data(), sizeof(request));
    GroupSignalResponse response{.request_id = request.request_id, .result = -ESRCH, .version = WKI_GROUP_SIGNAL_VERSION};
    ker::mod::security::Snapshot supplied, caller;
    response.result = ker::mod::security::credential_record::decode(request.credentials, supplied);
    if (response.result != 0) return response;
    uint64_t const POLICY = work.identity.policy;
    response.result = auth_policy::map_compute_identity(POLICY, supplied, caller);
    if (response.result != 0) return response;
    uint32_t const SOURCE_BOOT = work.identity.boot_epoch;
    if (SOURCE_BOOT == 0) {
        response.result = -EACCES;
        return response;
    }
    s_compute_lock.lock();
    auto const SESSION =
        capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel, work.rx_channel_generation, work.identity);
    s_compute_lock.unlock();
    if (SESSION.epoch == 0) {
        response.result = -ESRCH;
        return response;
    }
    ker::mod::sched::session::Identity job{.session_id = request.session_id,
                                           .pgid = request.caller_pgid,
                                           .origin = resolve_session_origin(request.session_origin, work.hdr.src_node, SOURCE_BOOT),
                                           .group_origin = resolve_session_origin(request.group_origin, work.hdr.src_node, SOURCE_BOOT)};
    if (request.operation == static_cast<uint8_t>(GroupSignalOperation::QUERY)) {
        ker::mod::sched::session::GroupCandidates groups;
        auto const NAMESPACE = resolve_session_origin({}, work.hdr.src_node, SOURCE_BOOT);
        response.result = ker::syscall::process::query_group_local(request.pgid, NAMESPACE, job, groups);
        if (response.result == 0) {
            response.flags = groups.flags;
            response.other = {.node = groups.other.node, .boot = groups.other.boot};
        }
    } else {
        ker::mod::sched::session::Identity const GROUP{.pgid = request.pgid, .group_origin = job.group_origin};
        response.result = ker::syscall::process::signal_group_local_as(caller, job, GROUP, request.signum);
    }
    return response;
}

void requeue_compute_rx_slot(size_t worker_index, uint16_t slot_index) {
    s_compute_rx_lock.lock();
    auto& shard = s_compute_rx_shards.at(worker_index);
    // This still-owned in-flight slot is one of the fixed pool, so the other
    // queued slots cannot occupy every position in this shard.
    shard.slot_indices.at(shard.tail) = slot_index;
    shard.tail = (shard.tail + 1) % WKI_COMPUTE_RX_QUEUE_MAX;
    ++shard.count;
    s_compute_rx_lock.unlock();
}

[[noreturn]] void compute_rx_worker_loop(size_t worker_index) {
    for (;;) {
        // One local retirement per RX iteration bounds queue monopolization.
        // No PeerLifecycleLease or scheduler lock crosses this work.
        bool const RETIRED = run_one_proxy_exit();
        bool const SIGNALED = run_one_child_signal();
        bool const CLAIM_RETIRED = run_one_child_wait_retirement();
        bool const NATIVE_WORK = run_one_native_child_wait();
        bool const QUERY_RETIRED = run_one_child_query_retirement();
        bool const QUERY_WORK = run_one_child_query_send();
        bool const CLAIM_WORK = run_one_child_claim_send();
        uint16_t slot_index = 0;
        if (!dequeue_compute_rx(worker_index, &slot_index)) {
            if (NATIVE_WORK || QUERY_WORK || CLAIM_WORK)
                ker::mod::sched::kern_sleep_us(1000);
            else if (!RETIRED && !SIGNALED && !CLAIM_RETIRED && !QUERY_RETIRED)
                ker::mod::sched::kern_block();
            continue;
        }

        auto& work = s_compute_rx_slots.at(slot_index);
        bool retry_reply = false;
        {
            PeerLifecycleLease lifecycle;
            if (lifecycle.acquire(work.hdr.src_node)) {
                WkiPeer const* peer = wki_peer_find(work.hdr.src_node);
                if (peer != nullptr && compute_rx_identity_matches(work, *peer, g_wki.capabilities) &&
                    compute_submit_channel_token_matches(work.hdr.src_node, work.rx_channel, work.rx_channel_generation)) {
                    auto const WORK_TYPE = static_cast<MsgType>(work.type);
                    if (WORK_TYPE == MsgType::TASK_REJECT) {
                        // This fixed slot owns only a submit-policy denial,
                        // never an admitted execution. Transient enqueue
                        // refusal cannot discard the already ACKed decision.
                        retry_reply = wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                                     MsgType::TASK_REJECT, work.payload.data(), work.payload_len) != WKI_OK;
                    } else if (WORK_TYPE == MsgType::TASK_CHILD_CLAIM_REQ || WORK_TYPE == MsgType::TASK_CHILD_CLAIM_RESP) {
                        s_compute_lock.lock();
                        auto const SESSION = capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel,
                                                                                   work.rx_channel_generation, work.identity);
                        s_compute_lock.unlock();
                        if (SESSION.epoch != 0) {
                            if (WORK_TYPE == MsgType::TASK_CHILD_CLAIM_REQ) {
                                TaskChildClaimRequest request{};
                                std::memcpy(&request, work.payload.data(), sizeof(request));
                                auto const RESPONSE = execute_child_claim_packet(lifecycle, SESSION, request);
                                std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                                work.payload_len = sizeof(RESPONSE);
                                work.type = static_cast<uint8_t>(MsgType::TASK_CHILD_CLAIM_RESP);
                                work.child_reply_pending = true;
                            }
                            if (work.child_reply_pending) {
                                retry_reply = wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                                             MsgType::TASK_CHILD_CLAIM_RESP, work.payload.data(),
                                                                             work.payload_len) != WKI_OK;
                            } else {
                                TaskChildClaimResponse response{};
                                std::memcpy(&response, work.payload.data(), sizeof(response));
                                static_cast<void>(receive_child_claim_response(lifecycle, SESSION, response));
                            }
                        }
                    } else if (WORK_TYPE == MsgType::TASK_CHILD_QUERY_REQ || WORK_TYPE == MsgType::TASK_CHILD_QUERY_RESP) {
                        s_compute_lock.lock();
                        auto const SESSION = capture_compute_submit_session_locked(work.hdr.src_node, work.rx_channel,
                                                                                   work.rx_channel_generation, work.identity);
                        s_compute_lock.unlock();
                        if (SESSION.epoch != 0) {
                            if (WORK_TYPE == MsgType::TASK_CHILD_QUERY_REQ) {
                                TaskChildQueryRequest request{};
                                std::memcpy(&request, work.payload.data(), sizeof(request));
                                auto const RESPONSE = execute_child_query_packet(lifecycle, SESSION, request);
                                std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                                work.payload_len = sizeof(RESPONSE);
                                work.type = static_cast<uint8_t>(MsgType::TASK_CHILD_QUERY_RESP);
                                work.child_reply_pending = true;
                            }
                            if (work.child_reply_pending) {
                                retry_reply = wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                                             MsgType::TASK_CHILD_QUERY_RESP, work.payload.data(),
                                                                             work.payload_len) != WKI_OK;
                            } else {
                                TaskChildQueryResponse response{};
                                std::memcpy(&response, work.payload.data(), sizeof(response));
                                static_cast<void>(receive_child_query_response(lifecycle, SESSION, response));
                            }
                        }
                    } else if (WORK_TYPE == MsgType::TASK_COMPLETE) {
                        detail::handle_task_complete(&work.hdr, work.payload.data(), work.payload_len, work.rx_channel,
                                                     work.rx_channel_generation);
                    } else if (WORK_TYPE == MsgType::TASK_CANCEL) {
                        detail::handle_task_cancel(&work.hdr, work.payload.data(), work.payload_len, work.rx_channel,
                                                   work.rx_channel_generation);
                    } else if (WORK_TYPE == MsgType::TASK_CHILD_SIGNAL) {
                        static_cast<void>(deliver_child_signal(work));
                    } else if (WORK_TYPE == MsgType::TASK_PARENT_DEATH) {
                        static_cast<void>(deliver_parent_death(work));
                    } else if (WORK_TYPE == MsgType::TASK_TERMINAL_SIGNAL) {
                        static_cast<void>(deliver_terminal_signal(work));
                    } else if (WORK_TYPE == MsgType::TASK_SIGNAL_REQ || WORK_TYPE == MsgType::TASK_SIGNAL_RESP) {
                        if (WORK_TYPE == MsgType::TASK_SIGNAL_REQ) {
                            auto const RESPONSE = authorize_user_signal(work);
                            std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                            work.payload_len = sizeof(RESPONSE);
                            work.type = static_cast<uint8_t>(MsgType::TASK_SIGNAL_RESP);
                        }
                        // The fixed work slot owns the completed decision
                        // until reliable enqueue or exact session retirement.
                        // Retrying this reply never delivers the signal twice.
                        retry_reply =
                            wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                           MsgType::TASK_SIGNAL_RESP, work.payload.data(), work.payload_len) != WKI_OK;
                    } else if (WORK_TYPE == MsgType::GROUP_RECEIVER_REQ || WORK_TYPE == MsgType::GROUP_RECEIVER_RESP) {
                        if (WORK_TYPE == MsgType::GROUP_RECEIVER_REQ) {
                            auto const RESPONSE = authorize_group_receivers(work);
                            std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                            work.payload_len = sizeof(RESPONSE);
                            work.type = static_cast<uint8_t>(MsgType::GROUP_RECEIVER_RESP);
                        }
                        retry_reply =
                            wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                           MsgType::GROUP_RECEIVER_RESP, work.payload.data(), work.payload_len) != WKI_OK;
                    } else if (WORK_TYPE == MsgType::GROUP_ROUTE_REQ || WORK_TYPE == MsgType::GROUP_ROUTE_RESP) {
                        if (WORK_TYPE == MsgType::GROUP_ROUTE_REQ) {
                            auto const RESPONSE = authorize_group_route(work);
                            std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                            work.payload_len = sizeof(RESPONSE);
                            work.type = static_cast<uint8_t>(MsgType::GROUP_ROUTE_RESP);
                        }
                        retry_reply =
                            wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                           MsgType::GROUP_ROUTE_RESP, work.payload.data(), work.payload_len) != WKI_OK;
                    } else if (WORK_TYPE == MsgType::GROUP_SIGNAL_REQ || WORK_TYPE == MsgType::GROUP_SIGNAL_RESP) {
                        if (WORK_TYPE == MsgType::GROUP_SIGNAL_REQ) {
                            auto const RESPONSE = authorize_group_signal(work);
                            std::memcpy(work.payload.data(), &RESPONSE, sizeof(RESPONSE));
                            work.payload_len = sizeof(RESPONSE);
                            work.type = static_cast<uint8_t>(MsgType::GROUP_SIGNAL_RESP);
                        }
                        retry_reply =
                            wki_send_on_channel_generation(work.hdr.src_node, work.rx_channel, work.rx_channel_generation,
                                                           MsgType::GROUP_SIGNAL_RESP, work.payload.data(), work.payload_len) != WKI_OK;
                    } else if (WORK_TYPE == MsgType::TASK_SUBMIT_FRAGMENT) {
                        handle_task_submit_fragment_work(work.hdr, work.payload.data(), work.payload_len, work.rx_channel,
                                                         work.rx_channel_generation, work.identity);
                    }
                }
            }
        }
        if (retry_reply) {
            requeue_compute_rx_slot(worker_index, slot_index);
            ker::mod::sched::kern_sleep_us(1000);
        } else
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

        // The notifier preserves an event-before-park token even if this
        // worker is preempted after observing an empty queue. kern_block()
        // consumes that token instead of sleeping with a queued TASK_SUBMIT.
        s_pending_submit_lock.lock();
        bool const EMPTY = s_pending_task_submit_count == 0;
        s_pending_submit_lock.unlock();
        if (EMPTY) {
            ker::mod::sched::kern_block();
        }
    }
}

}  // namespace

namespace {

auto queue_compute_rx_work(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t copy_len, WkiChannel* rx_channel,
                           uint32_t rx_channel_generation) -> WkiComputeRxAdmission {
    if (hdr == nullptr || rx_channel == nullptr || rx_channel_generation == 0 || hdr->channel_id != WKI_CHAN_RESOURCE ||
        rx_channel->channel_id != WKI_CHAN_RESOURCE) {
        return WkiComputeRxAdmission::DISCARD;
    }

    auto const IDENTITY = capture_compute_peer_identity(hdr->src_node);
    if (!IDENTITY.valid()) return WkiComputeRxAdmission::DISCARD;

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
    slot.child_reply_pending = false;
    slot.identity = IDENTITY;
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

}  // namespace

auto wki_remote_compute_admit_rx(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation) -> WkiComputeRxAdmission {
    uint16_t copy_len = 0;
    WkiComputeRxAdmission const CLASSIFICATION = classify_compute_rx_payload(type, payload, payload_len, &copy_len);
    if (CLASSIFICATION != WkiComputeRxAdmission::DEFERRED) {
        return CLASSIFICATION;
    }
    return queue_compute_rx_work(type, hdr, payload, copy_len, rx_channel, rx_channel_generation);
}

auto wki_remote_compute_admit_submit_denial_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                               uint32_t rx_channel_generation) -> WkiComputeRxAdmission {
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(uint32_t) ||
        (hdr->msg_type != static_cast<uint8_t>(MsgType::TASK_SUBMIT) &&
         hdr->msg_type != static_cast<uint8_t>(MsgType::TASK_SUBMIT_FRAGMENT))) {
        return WkiComputeRxAdmission::DISCARD;
    }
    TaskResponsePayload response{};
    std::memcpy(&response.task_id, payload, sizeof(response.task_id));
    if (response.task_id == 0) return WkiComputeRxAdmission::DISCARD;
    response.status = static_cast<uint8_t>(TaskRejectReason::UNAUTHORIZED);
    auto const ADMISSION = queue_compute_rx_work(MsgType::TASK_REJECT, hdr, reinterpret_cast<const uint8_t*>(&response), sizeof(response),
                                                 rx_channel, rx_channel_generation);
    // A valid denied request must not be ACKed without an owned decision,
    // including when its peer/channel snapshot cannot currently be captured.
    return ADMISSION == WkiComputeRxAdmission::DISCARD ? WkiComputeRxAdmission::RETRY : ADMISSION;
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
    notify_proxy_exit_work();
    ker::mod::dbg::log("[WKI] Compute RX pool started (%u/%u workers)", static_cast<unsigned>(started_workers),
                       static_cast<unsigned>(TARGET_WORKERS));
}

void wki_remote_compute_start_submit_thread() {
    if (s_compute_submit_task_count.load(std::memory_order_acquire) != 0) {
        return;
    }

    constexpr std::array<const char*, WKI_COMPUTE_SUBMIT_WORKER_MAX> WORKER_NAMES = {
        "wki_compute0", "wki_compute1", "wki_compute2", "wki_compute3", "wki_compute4", "wki_compute5", "wki_compute6", "wki_compute7",
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
        ker::mod::sched::wake_task_from_event(s_compute_submit_tasks.at(i));
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

auto wki_remote_compute_selftest_submit_fragment_boundaries() -> bool {
    constexpr uint32_t TASK_ID = 0x12A5U;
    std::array<uint8_t, sizeof(TaskSubmitFragmentPayload) + sizeof(TaskSubmitPayload)> first_frame = {};
    TaskSubmitFragmentPayload fragment = {
        .task_id = TASK_ID,
        .total_len = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD + 1),
        .offset = 0,
    };
    TaskSubmitPayload submit = {};
    submit.task_id = TASK_ID;
    std::memcpy(first_frame.data(), &fragment, sizeof(fragment));
    std::memcpy(first_frame.data() + sizeof(fragment), &submit, sizeof(submit));

    uint16_t copy_len = 0;
    bool const FIRST_VALID =
        classify_compute_rx_payload(MsgType::TASK_SUBMIT_FRAGMENT, first_frame.data(), static_cast<uint16_t>(first_frame.size()),
                                    &copy_len) == WkiComputeRxAdmission::DEFERRED &&
        copy_len == first_frame.size();
    submit.task_id++;
    std::memcpy(first_frame.data() + sizeof(fragment), &submit, sizeof(submit));
    bool const MISMATCH_REJECTED =
        classify_compute_rx_payload(MsgType::TASK_SUBMIT_FRAGMENT, first_frame.data(), static_cast<uint16_t>(first_frame.size()),
                                    &copy_len) == WkiComputeRxAdmission::DISCARD;

    return FIRST_VALID && MISMATCH_REJECTED && task_submit_fragment_count(WKI_ETH_MAX_PAYLOAD + 1) == 2 &&
           task_submit_fragment_count(UINT16_MAX) == 8;
}

auto wki_remote_compute_selftest_submit_worker_count_is_bounded() -> bool {
    return compute_submit_worker_target(0) == 1 && compute_submit_worker_target(1) == 1 && compute_submit_worker_target(2) == 2 &&
           compute_submit_worker_target(WKI_COMPUTE_SUBMIT_WORKER_MAX) == WKI_COMPUTE_SUBMIT_WORKER_MAX &&
           compute_submit_worker_target(WKI_COMPUTE_SUBMIT_WORKER_MAX + 1) == WKI_COMPUTE_SUBMIT_WORKER_MAX &&
           compute_submit_worker_target(256) == WKI_COMPUTE_SUBMIT_WORKER_MAX;
}

auto wki_remote_compute_selftest_rx_admission_is_bounded() -> bool {
    std::array<uint8_t, sizeof(TaskCompletePayload) + WKI_TASK_MAX_OUTPUT> complete_bytes = {};
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

auto wki_remote_compute_selftest_service_completion_counts_once() -> bool {
    namespace resource = ker::mod::sched::resource;
    constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
    constexpr unsigned AS_ROW = ker::abi::service_resource::RESOURCE_COUNT - 1;
    bool ok = true;
    for (bool report_first : {false, true}) {
        auto* accounting = resource::RemoteServiceAccounting::create();
        if (accounting == nullptr) return false;
        {
            ker::mod::sched::task::Task proxy{};
            proxy.type = ker::mod::sched::task::TaskType::PROCESS;
            ok &= accounting->attach(proxy);
            resource::charge_cpu(&proxy, 100, 40);
            auto baseline = resource_contract::snapshot(proxy.resources);
            baseline.flags |= WKI_RESOURCE_SERVICE_REPORTS;
            resource::State receiver;
            ok &= resource_contract::restore(baseline, receiver, proxy.FD_TABLE_SIZE);
            static_cast<void>(receiver.charge_cpu(300, 200));
            receiver.deny(AS, resource::Denial::SOFT_LIMIT);
            auto const COMPLETION = resource_contract::snapshot(receiver);
            resource::ServiceReportTarget target;
            ok &= target.bind(proxy) == 1;
            resource::RemoteServiceObservation report{};
            report.cpu_time_us = 300;
            report.usage[AS_ROW].denied = 1;
            report.usage[AS_ROW].reason = resource::Denial::SOFT_LIMIT;
            if (report_first) ok &= target.apply(1, report, true) == resource::ServiceReportResult::APPLIED;
            auto bad = COMPLETION;
            bad.cpu_time_us = 0;
            ok &= !merge_proxy_resource_completion(proxy, baseline, bad) && proxy.service_member.denied_baseline[AS_ROW] == 0;
            ok &= merge_proxy_resource_completion(proxy, baseline, COMPLETION);
            auto observed = accounting->snapshot();
            ok &= proxy.resources.cpu_time_us == 400 && proxy.resources.entries[AS].denied == 1 &&
                  observed.cpu_time_us == (report_first ? 400 : 100) && observed.usage[AS_ROW].denied == (report_first ? 1 : 0);
            if (!report_first) ok &= target.apply(1, report, true) == resource::ServiceReportResult::APPLIED;
            observed = accounting->snapshot();
            ok &= observed.cpu_time_us == 400 && observed.usage[AS_ROW].denied == 1;
        }
        auto const FINAL = accounting->snapshot();
        ok &= FINAL.members == 0 && FINAL.pending_remote_children == 0 && FINAL.cpu_time_us == 400 && FINAL.usage[AS_ROW].denied == 1 &&
              FINAL.flags == 0;
        accounting->release();
    }
    return ok;
}

auto wki_remote_compute_selftest_service_report_transport() -> bool {
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::Task;
    constexpr uint32_t TASK_ID = 0x22AA01;
    constexpr uint16_t NODE = 0x7A22;
    constexpr uint32_t GENERATION = 19;
    bool ok = true;
    for (unsigned ending = 0; ending < 3; ++ending) {
        auto* accounting = resource::RemoteServiceAccounting::create();
        if (accounting == nullptr) return false;
        {
            Task source{};
            source.type = ker::mod::sched::task::TaskType::PROCESS;
            ok &= accounting->attach(source) && resource::service_remote_admission(source) == 1;
            WkiWaitEntry wait{};
            SubmittedTask row{};
            row.active = true;
            row.response_wait_entry = &wait;
            row.response_pending = true;
            row.task_id = TASK_ID;
            row.target_node = NODE;
            row.submit_channel_generation = GENERATION;
            row.exit_status = 17;
            row.result_handle_owned = true;
            row.submission_started = true;
            row.reclaim_requested = true;
            ok &= row.service_report.bind(source) == 1;
            row.service_report_pending = true;
            s_compute_lock.lock();
            remove_submitted_task_for_selftest_locked(TASK_ID);
            auto* published = publish_submitted_task_locked(std::move(row));
            s_compute_lock.unlock();
            if (published == nullptr) {
                accounting->release();
                return false;
            }
            WkiHeader header{};
            header.src_node = NODE;
            TaskResponsePayload response{};
            response.task_id = TASK_ID;
            if (ending == 2) {
                response.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
                detail::handle_task_reject(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), nullptr, GENERATION);
                s_compute_lock.lock();
                auto* current = find_submitted_task_any(TASK_ID);
                ok &= current != nullptr && !current->service_report_pending;
                if (current != nullptr) {
                    current->active = false;
                    static_cast<void>(consume_submitted_task_result_locked(current));
                }
                s_compute_lock.unlock();
                ok &= accounting->snapshot().flags == 0 && accounting->snapshot().pending_remote_children == 0;
                accounting->release();  // member retains the remaining ref until scope exit
                continue;
            }
            response.status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
            detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), nullptr, GENERATION);
            s_compute_lock.lock();
            auto* accepted = find_submitted_task_any(TASK_ID);
            ok &= accepted != nullptr && accepted->service_report_pending;
            if (accepted != nullptr) accepted->active = false;
            s_compute_lock.unlock();
            resource::RemoteServiceObservation observation{};
            observation.cpu_time_us = 300;
            observation.members = 1;
            observation.live_processes = 1;
            auto report = service_contract::encode(TASK_ID, 1, observation, false);
            auto deliver = [&](uint32_t generation, uint16_t length = sizeof(TaskServiceReport)) {
                // Exercise the same unaligned packet path as production RX.
                std::array<uint8_t, sizeof(TaskServiceReport) + 1> bytes{};
                std::memcpy(bytes.data() + 1, &report, sizeof(report));
                detail::handle_task_service_report(&header, bytes.data() + 1, length, nullptr, generation);
            };
            deliver(GENERATION + 1);
            header.src_node++;
            deliver(GENERATION);
            header.src_node = NODE;
            deliver(GENERATION, sizeof(report) - 1);
            ok &= accounting->snapshot().cpu_time_us == 0;
            deliver(GENERATION);
            deliver(GENERATION);
            ok &= accounting->snapshot().cpu_time_us == 300 && accounting->snapshot().pending_remote_children == 1;
            report.sequence = 2;
            report.cpu_time_us = 299;
            deliver(GENERATION);
            ok &= accounting->snapshot().cpu_time_us == 300;
            if (ending == 1) {
                // An inactive root with a still-owned result must retain status.
                fail_submitted_tasks_for_peer(NODE, RemoteComputeCleanupScope::ALL);
                s_compute_lock.lock();
                auto* current = find_submitted_task_any(TASK_ID);
                ok &= current != nullptr && current->exit_status == 17 && !current->service_report_pending;
                if (current != nullptr) static_cast<void>(consume_submitted_task_result_locked(current));
                s_compute_lock.unlock();
                ok &= accounting->snapshot().flags == ker::abi::service_resource::ACCOUNTING_INCOMPLETE;
            } else {
                s_compute_lock.lock();
                auto* current = find_submitted_task_any(TASK_ID);
                if (current != nullptr) static_cast<void>(consume_submitted_task_result_locked(current));
                ok &= find_submitted_task_any(TASK_ID) != nullptr;  // stream alone pins row
                s_compute_lock.unlock();
                observation.cpu_time_us = 400;
                observation.members = 0;
                observation.live_processes = 0;
                report = service_contract::encode(TASK_ID, 3, observation, true);
                deliver(GENERATION);
                deliver(GENERATION);  // reclaimed row cannot reapply a final delta
                ok &= accounting->snapshot().cpu_time_us == 400 && accounting->snapshot().flags == 0;
            }
            s_compute_lock.lock();
            ok &= find_submitted_task_any(TASK_ID) == nullptr;
            remove_submitted_task_for_selftest_locked(TASK_ID);
            s_compute_lock.unlock();
            ok &= accounting->snapshot().pending_remote_children == 0;
        }
        accounting->release();
    }
    // Receiver monitor drops its root Task ref while descendant membership
    // independently keeps the accounting domain alive and non-final.
    auto* accounting = resource::RemoteServiceAccounting::create();
    if (accounting == nullptr) return false;
    RunningRemoteTask running{};
    running.active = true;
    running.published = true;
    running.service_accounting = accounting;  // transfer create's monitor ref
    {
        Task descendant{};
        descendant.type = ker::mod::sched::task::TaskType::PROCESS;
        {
            Task root{};
            root.type = ker::mod::sched::task::TaskType::PROCESS;
            ok &= accounting->attach(root);
            resource::inherit_service(descendant, root);
            ok &= root.try_acquire_lifetime_ref();
            running.task = &root;
            s_compute_lock.lock();
            release_running_root_locked(running);
            s_compute_lock.unlock();
            ok &= running.active && running.root_completion_queued && running.task == nullptr;
            root.has_exited = true;
            root.state.store(ker::mod::sched::task::TaskState::EXITING);
            root.exit_notify_ready.store(true, std::memory_order_release);
            ok &= accounting->snapshot().members == 1;  // descendant, not retained root metadata
        }
        ok &= accounting->snapshot().members == 1;
        static_cast<void>(resource::charge_service_cpu(descendant, 123));
        constexpr auto AS = static_cast<unsigned>(resource::Id::AS);
        constexpr auto AS_ROW = ker::abi::service_resource::RESOURCE_COUNT - 1;
        descendant.resources.commit_extent(AS, 8192);
        descendant.resources.deny(AS, resource::Denial::SOFT_LIMIT);
        descendant.state.store(ker::mod::sched::task::TaskState::EXITING);
        descendant.cpu_accounting_finalized.store(true);
        descendant.has_exited = true;
        ok &= accounting->snapshot().members == 1;  // accounting alone is not exit readiness
        descendant.resource_cpu_threads.store(1);
        descendant.exit_notify_ready.store(true, std::memory_order_release);
        ok &= accounting->snapshot().members == 1;  // active contributor still gates finality
        descendant.resource_cpu_threads.store(0, std::memory_order_release);
        descendant.resource_deferred_observations.store(1);
        ok &= accounting->snapshot().members == 1;  // deferred core output can still update peaks
        descendant.resource_deferred_observations.store(0, std::memory_order_release);
        auto const FINAL = accounting->snapshot();
        ok &= FINAL.members == 0 && FINAL.live_processes == 0 && FINAL.cpu_time_us == 123 && FINAL.usage[AS_ROW].current == 0 &&
              FINAL.usage[AS_ROW].peak == 8192 && FINAL.usage[AS_ROW].denied == 1 && FINAL.flags == 0;
    }
    auto const RECLAIMED = accounting->snapshot();
    ok &= RECLAIMED.members == 0 && RECLAIMED.cpu_time_us == 123 &&
          RECLAIMED.usage[ker::abi::service_resource::RESOURCE_COUNT - 1].denied == 1;
    running.discard_completion = true;
    s_compute_lock.lock();
    release_running_root_locked(running);
    s_compute_lock.unlock();
    ok &= !running.active && running.service_accounting == nullptr;
    return ok;
}

auto wki_remote_compute_selftest_resource_rejection_is_session_scoped() -> bool {
    constexpr uint32_t TASK_ID = 0x22BEF0;
    constexpr uint16_t NODE = 0x7A22;
    constexpr uint32_t GENERATION = 11;
    constexpr unsigned AS = static_cast<unsigned>(ker::abi::resource::Id::AS);
    ker::mod::sched::task::Task subject{};
    subject.type = ker::mod::sched::task::TaskType::PROCESS;
    subject.resources.commit_extent(AS, 123);
    auto const BASELINE = resource_contract::snapshot(subject.resources);
    ker::mod::sched::resource::State remote;
    if (!resource_contract::restore(BASELINE, remote, subject.FD_TABLE_SIZE)) return false;
    remote.commit_extent(AS, 4096);
    remote.deny(AS, ker::abi::resource::Denial::SOFT_LIMIT);
    auto record = resource_contract::snapshot(remote);
    WkiWaitEntry wait{};
    SubmittedTask submitted{};
    submitted.active = true;
    submitted.task_id = TASK_ID;
    submitted.target_node = NODE;
    submitted.submit_channel_generation = GENERATION;
    submitted.result_handle_owned = true;
    submitted.resource_baseline = BASELINE;
    submitted.response_pending.store(true, std::memory_order_release);
    submitted.response_wait_entry = &wait;
    if (!submitted.set_local_task_ref(&subject)) return false;
    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID);
    bool const PUBLISHED = publish_submitted_task_locked(std::move(submitted)) != nullptr;
    s_compute_lock.unlock();
    if (!PUBLISHED) return false;

    std::array<uint8_t, sizeof(TaskResponsePayload) + sizeof(TaskResourceSnapshot)> bytes{};
    TaskResponsePayload response{};
    response.task_id = TASK_ID;
    response.status = static_cast<uint8_t>(TaskRejectReason::NO_MEM);
    response.reserved[0] = static_cast<uint8_t>(sizeof(record) & 0xFFU);
    response.reserved[1] = static_cast<uint8_t>(sizeof(record) >> 8);
    auto encode = [&]() {
        std::memcpy(bytes.data(), &response, sizeof(response));
        std::memcpy(bytes.data() + sizeof(response), &record, sizeof(record));
    };
    auto unchanged = [&]() {
        auto const NOW = resource_contract::snapshot(subject.resources);
        return std::memcmp(&NOW, &BASELINE, sizeof(NOW)) == 0 && wait.state.load() == WkiWaitEntry::PENDING;
    };
    WkiHeader header{};
    header.src_node = NODE;
    encode();
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION + 1);
    bool ok = unchanged();
    header.src_node++;
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    ok &= unchanged();
    header.src_node = NODE;
    detail::handle_task_reject(&header, bytes.data(), bytes.size() - 1, nullptr, GENERATION);
    ok &= unchanged();
    response.reserved[2] = 1;
    encode();
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    ok &= unchanged();
    response.reserved[2] = 0;
    record.entries[AS].limit.soft--;
    encode();
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    ok &= unchanged();
    record.entries[AS].limit.soft++;
    encode();
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    ok &= wait.state.load() == WkiWaitEntry::DONE && subject.resources.entries[AS].denied == 1 &&
          subject.resources.entries[AS].peak == 4096 && subject.resources.entries[AS].current == 123;
    record.entries[AS].denied++;
    encode();
    detail::handle_task_reject(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    ok &= subject.resources.entries[AS].denied == 1;
    s_compute_lock.lock();
    auto* tracked = find_submitted_task_any(TASK_ID);
    ok &= tracked != nullptr && !tracked->response_pending.load() && tracked->accept_status == response.status;
    remove_submitted_task_for_selftest_locked(TASK_ID);
    s_compute_lock.unlock();
    return ok && subject.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_resource_completion_is_session_scoped() -> bool {
    constexpr uint32_t TASK_ID = 0x22BEEF;
    constexpr uint16_t NODE = 0x7A22;
    constexpr uint32_t GENERATION = 11;
    ker::mod::sched::resource::State state;
    static_cast<void>(state.charge_cpu(100));
    SubmittedTask first;
    first.active = true;
    first.task_id = TASK_ID;
    first.target_node = NODE;
    first.submit_channel_generation = GENERATION;
    first.result_handle_owned = true;
    first.resource_baseline = resource_contract::snapshot(state);
    static_cast<void>(state.charge_cpu(300));
    first.resource_completion = resource_contract::snapshot(state);
    SubmittedTask moved(std::move(first));
    SubmittedTask submitted;
    submitted = std::move(moved);
    bool ok = submitted.resource_baseline && submitted.resource_baseline->cpu_time_us == 100 && submitted.resource_completion &&
              submitted.resource_completion->cpu_time_us == 400;
    submitted.resource_completion.reset();
    s_compute_lock.lock();
    remove_submitted_task_for_selftest_locked(TASK_ID);
    bool const PUBLISHED = publish_submitted_task_locked(std::move(submitted)) != nullptr;
    s_compute_lock.unlock();
    if (!PUBLISHED) return false;

    std::array<uint8_t, sizeof(TaskCompletePayload) + sizeof(TaskResourceSnapshot)> bytes{};
    TaskCompletePayload complete{};
    complete.task_id = TASK_ID;
    complete.reserved = sizeof(TaskResourceSnapshot);
    std::memcpy(bytes.data(), &complete, sizeof(complete));
    auto record = resource_contract::snapshot(state);
    std::memcpy(bytes.data() + sizeof(complete), &record, sizeof(record));
    uint16_t copied = 0;
    ok &= classify_compute_rx_payload(MsgType::TASK_COMPLETE, bytes.data(), bytes.size(), &copied) == WkiComputeRxAdmission::DEFERRED &&
          copied == bytes.size();
    ok &= classify_compute_rx_payload(MsgType::TASK_COMPLETE, bytes.data(), bytes.size() - 1, &copied) == WkiComputeRxAdmission::DISCARD;
    WkiHeader header{};
    header.src_node = NODE;
    detail::handle_task_complete(&header, bytes.data(), bytes.size(), nullptr, GENERATION + 1);
    s_compute_lock.lock();
    auto* tracked = find_submitted_task_any(TASK_ID);
    ok &= tracked != nullptr && tracked->active && !tracked->resource_completion;
    s_compute_lock.unlock();
    record.cpu_time_us = 99;
    std::memcpy(bytes.data() + sizeof(complete), &record, sizeof(record));
    detail::handle_task_complete(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    s_compute_lock.lock();
    tracked = find_submitted_task_any(TASK_ID);
    ok &= tracked != nullptr && tracked->active && !tracked->resource_completion;
    s_compute_lock.unlock();
    record.cpu_time_us = 400;
    record.cpu_system_time_us = 400;
    std::memcpy(bytes.data() + sizeof(complete), &record, sizeof(record));
    detail::handle_task_complete(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    // A duplicate cannot overwrite the first terminal snapshot.
    record.cpu_time_us = 700;
    record.cpu_system_time_us = 700;
    std::memcpy(bytes.data() + sizeof(complete), &record, sizeof(record));
    detail::handle_task_complete(&header, bytes.data(), bytes.size(), nullptr, GENERATION);
    s_compute_lock.lock();
    tracked = find_submitted_task_any(TASK_ID);
    ok &= tracked != nullptr && !tracked->active && tracked->resource_completion && tracked->resource_completion->cpu_time_us == 400;
    remove_submitted_task_for_selftest_locked(TASK_ID);
    s_compute_lock.unlock();
    return ok;
}

auto wki_remote_compute_selftest_completion_queue_preserves_ownership() -> bool {
    auto* output = new (std::nothrow) TaskOutputCapture{};
    if (output == nullptr) return false;
    output->append({reinterpret_cast<const uint8_t*>("wki!"), 4});
    PendingTaskCompletion info{};
    info.task_id = 1;
    info.submitter_node = 0x7B29;
    info.local_pid = 0x12345;
    info.exit_status = 17;
    info.output = output;
    info.submit_session_epoch = 12;
    info.submit_rx_channel_generation = 31;
    info.resources = resource_contract::snapshot(ker::mod::sched::resource::State{});
    info.resources->cpu_time_us = 123456;
    info.resources->cpu_system_time_us = 123456;
    info.resources->entries[static_cast<unsigned>(ker::abi::resource::Id::AS)].peak = 0x100000;
    auto const EXPECTED_RESOURCES = *info.resources;
    constexpr size_t COUNT = 69;
    s_compute_lock.lock();
    auto* saved_head = s_pending_completion_head;
    auto* saved_tail = s_pending_completion_tail;
    size_t const SAVED_COUNT = s_pending_completion_count;
    s_pending_completion_head = nullptr;
    s_pending_completion_tail = nullptr;
    s_pending_completion_count = 0;
    bool ok = !queue_pending_completion_locked(info, true) && s_pending_completion_head == nullptr &&
              s_pending_completion_tail == nullptr && s_pending_completion_count == 0 && info.output == output;
    bool output_queued = false;
    for (size_t i = 0; i < COUNT; ++i) {
        info.task_id = static_cast<uint32_t>(i + 1);
        info.output = i == 0 ? output : nullptr;
        if (!queue_pending_completion_locked(info)) {
            ok = false;
            break;
        }
        if (i == 0) output_queued = true;
    }
    size_t const QUEUED = s_pending_completion_count;
    auto* const HEAD = s_pending_completion_head;
    auto* const TAIL = s_pending_completion_tail;
    ok &= !queue_pending_completion_locked(info, true) && s_pending_completion_count == QUEUED && s_pending_completion_head == HEAD &&
          s_pending_completion_tail == TAIL;
    // Retry a full FIFO rotation. Every retry uses the same detached node.
    for (size_t i = 0; i < QUEUED; ++i) {
        auto* node = pop_pending_completion_locked();
        if (node == nullptr) {
            ok = false;
            break;
        }
        ok &= node->task_id == i + 1 && node->next == nullptr && s_pending_completion_count == QUEUED - 1 && node->resources &&
              std::memcmp(&*node->resources, &EXPECTED_RESOURCES, sizeof(EXPECTED_RESOURCES)) == 0;
        append_pending_completion_locked(node);
        ok &= s_pending_completion_tail == node && s_pending_completion_count == QUEUED;
    }
    auto* detached = pop_pending_completion_locked();
    // Match peer cleanup's invalidation/compaction while one send is detached.
    for (auto* node = s_pending_completion_head; node != nullptr; node = node->next) {
        node->submitter_node = WKI_NODE_INVALID;
    }
    compact_pending_task_completions_locked();
    ok &= s_pending_completion_head == nullptr && s_pending_completion_tail == nullptr && s_pending_completion_count == 0;
    if (detached != nullptr) {
        ok &= detached == HEAD && detached->task_id == 1 && detached->output == output && detached->output->size() == 4 &&
              std::memcmp(detached->output->freeze().data(), "wki!", 4) == 0 && detached->submit_session_epoch == 12 &&
              detached->submit_rx_channel_generation == 31 && detached->local_pid == 0x12345 && detached->exit_status == 17 &&
              detached->resources && std::memcmp(&*detached->resources, &EXPECTED_RESOURCES, sizeof(EXPECTED_RESOURCES)) == 0;
        append_pending_completion_locked(detached);
        ok &= s_pending_completion_head == detached && s_pending_completion_tail == detached && s_pending_completion_count == 1;
        auto* retired = pop_pending_completion_locked();
        ok &= retired == detached && s_pending_completion_head == nullptr && s_pending_completion_tail == nullptr &&
              s_pending_completion_count == 0;
        release_output_capture(retired->output);
        delete retired;
    } else {
        ok = false;
    }
    if (!output_queued) release_output_capture(output);
    s_pending_completion_head = saved_head;
    s_pending_completion_tail = saved_tail;
    s_pending_completion_count = SAVED_COUNT;
    s_compute_lock.unlock();
    return ok;
}

auto wki_remote_compute_selftest_running_registry_compacts_stably() -> bool {
    RunningRemoteTasks rows;
    constexpr size_t COUNT = 69;
    for (size_t i = 0; i < COUNT; ++i) {
        RunningRemoteTask row{};
        row.active = i % 2 == 0;
        row.task_id = static_cast<uint32_t>(i + 1);
        row.local_pid = 0x7000 + i;
        if (!rows.push_back(row)) return false;
    }
    RunningRemoteTasks saved;
    s_compute_lock.lock();
    std::swap(saved, g_running_remote_tasks);
    std::swap(rows, g_running_remote_tasks);
    compact_running_remote_tasks_locked();
    bool ok = g_running_remote_tasks.size() == (COUNT + 1) / 2;
    for (size_t i = 0; i < g_running_remote_tasks.size(); ++i) {
        ok &= g_running_remote_tasks[i].active && g_running_remote_tasks[i].task_id == i * 2 + 1 &&
              g_running_remote_tasks[i].local_pid == 0x7000 + i * 2;
        g_running_remote_tasks[i].active = false;
    }
    compact_running_remote_tasks_locked();
    ok &= g_running_remote_tasks.empty();
    std::swap(rows, g_running_remote_tasks);
    std::swap(saved, g_running_remote_tasks);
    s_compute_lock.unlock();
    return ok;
}

auto wki_remote_compute_selftest_running_publication_failure_unwinds() -> bool {
    using ker::mod::sched::resource::ProcessReservation;
    using ker::mod::sched::resource::State;
    constexpr uint32_t UID = 0x7B28;
    ker::mod::sched::task::Task owner{};
    ker::mod::sched::task::Task observer{};
    if (ker::mod::sched::resource::initialize_real_uid(observer, UID) != 0) return false;
    ker::abi::resource::Usage population{};
    ker::mod::sched::resource::copy_process_observation(observer, population);
    if (population.current != 0) return false;
    auto* child = new (std::nothrow) ker::mod::sched::task::Task{};
    auto* output = new (std::nothrow) TaskOutputCapture{};
    if (child == nullptr || output == nullptr) {
        delete child;
        release_output_capture(output);
        return false;
    }
    if (ker::mod::sched::resource::initialize_real_uid(*child, UID) != 0) {
        delete child;
        release_output_capture(output);
        return false;
    }
    if (!ker::mod::sched::task::claim_unpublished_process(&owner, child)) {
        delete child;
        release_output_capture(output);
        return false;
    }
    State policy;
    policy.entries[static_cast<unsigned>(ker::abi::resource::Id::NPROC)].limit = {1, 1};
    ProcessReservation charge(policy, UID);
    if (charge.result() != 0 || !charge.attach(*child)) {
        destroy_unpublished_remote_process(&owner, child, output, "ktest-tracking-charge");
        return false;
    }
    ker::vfs::LocalPipePerfSnapshot before{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(before);
    std::array<int, 2> descriptors{-1, -1};
    if (ker::vfs::vfs_selftest_create_pipe(*child, descriptors.data()) != 0) {
        destroy_unpublished_remote_process(&owner, child, output, "ktest-tracking-pipe");
        return false;
    }
    bool const MONITOR_REF = child->try_acquire();
    bool const WORKER_REF = MONITOR_REF && child->try_acquire();
    if (!WORKER_REF) {
        if (MONITOR_REF) child->release();
        destroy_unpublished_remote_process(&owner, child, output, "ktest-tracking-refs");
        return false;
    }
    ker::mod::sched::resource::copy_process_observation(observer, population);
    bool ok = population.current == 1 && child->ref_count.load() == 4 && child->fd_table.size() == 2 && !child->scheduler_published.load();
    RunningRemoteTask running{};
    running.active = true;
    running.task_id = 0x7B28;
    running.task = child;
    running.output = output;
    s_compute_lock.lock();
    size_t const ROWS = g_running_remote_tasks.size();
    ok &= !publish_running_task_locked(running, true) && g_running_remote_tasks.size() == ROWS && running.active && running.task == child &&
          running.output == output && child->ref_count.load() == 4;
    s_compute_lock.unlock();
    discard_unpublished_running_task(&owner, child, running);
    ker::vfs::LocalPipePerfSnapshot after{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(after);
    ker::mod::sched::resource::copy_process_observation(observer, population);
    ok &= !running.active && running.task == nullptr && running.output == nullptr && owner.owned_unpublished_process.load() == nullptr &&
          !owner.unpublished_teardown_in_progress.load() && population.current == 0 && after.active_pipes == before.active_pipes &&
          after.capacity_bytes == before.capacity_bytes && after.buffered_bytes == before.buffered_bytes;
    ProcessReservation retry(policy, UID);
    ok &= retry.result() == 0;
    return ok;
}

auto wki_remote_compute_selftest_final_publication_and_cleanup() -> bool {
    using namespace ker::mod;
    using Task = sched::task::Task;
    using sched::resource::ProcessReservation;
    using sched::resource::RemoteServiceAccounting;
    using sched::resource::State;
    enum class Scenario {
        NO_LEASE,
        NOT_READY,
        RETIRED,
        IDENTITY_CHANGED,
        CHANNEL_REUSED,
        CANCELLED,
        OTHER_CANCEL,
        EXPIRED,
        POST_REFUSED,
        ACCEPTED
    };
    constexpr uint16_t NODE = 0x7A29;
    constexpr uint32_t TASK_ID = 0xC10401;
    constexpr uint32_t UID = 0x7B29;
    if (wki_peer_find(NODE) != nullptr) return false;
    s_compute_lock.lock();
    bool const UNUSED = find_compute_submit_peer_session_locked(NODE) == nullptr;
    s_compute_lock.unlock();
    if (!UNUSED) return false;
    bool ok = true;
    for (auto scenario : {Scenario::NO_LEASE, Scenario::NOT_READY, Scenario::RETIRED, Scenario::IDENTITY_CHANGED, Scenario::CHANNEL_REUSED,
                          Scenario::CANCELLED, Scenario::OTHER_CANCEL, Scenario::EXPIRED, Scenario::POST_REFUSED, Scenario::ACCEPTED}) {
        bool case_ok = true;
        WkiPeer peer{};
        peer.node_id = NODE;
        WkiChannel channel{};
        channel.active = true;
        channel.peer_node_id = NODE;
        channel.channel_id = WKI_CHAN_RESOURCE;
        channel.generation = 7;
        ComputePeerIdentity const IDENTITY{.boot_epoch = 17,
                                           .auth_generation = 4,
                                           .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                                           .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
        s_compute_lock.lock();
        auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 7, IDENTITY);
        s_compute_lock.unlock();
        struct SessionCleanup {
            ~SessionCleanup() {
                s_compute_lock.lock();
                if (auto* row = find_compute_submit_peer_session_locked(NODE); row != nullptr) *row = {};
                for (auto& cancellation : g_cancelled_task_submits) {
                    if (cancellation.active && cancellation.session.node_id == NODE) cancellation = {};
                }
                s_compute_lock.unlock();
            }
        } session_cleanup;
        if (SESSION.epoch == 0) return false;
        Task owner{};
        Task observer{};
        if (sched::resource::initialize_real_uid(observer, UID) != 0) return false;
        ker::abi::resource::Usage population{};
        sched::resource::copy_process_observation(observer, population);
        if (population.current != 0) return false;
        auto* child = new (std::nothrow) Task{};
        auto* output = new (std::nothrow) TaskOutputCapture{};
        if (child == nullptr || output == nullptr) {
            delete child;
            release_output_capture(output);
            return false;
        }
        child->type = sched::task::TaskType::PROCESS;
        child->pid = TASK_ID;
        auto credentials = security::Snapshot::bootstrap_root();
        if (credentials.prepare({.change = security::Change::SET_RESGID, .real = UID, .effective = UID, .saved = UID}, credentials) != 0 ||
            credentials.prepare({.change = security::Change::SET_RESUID, .real = UID, .effective = UID, .saved = UID}, credentials) != 0 ||
            credentials.prepare({.change = security::Change::NO_NEW_PRIVILEGES}, credentials) != 0 ||
            credentials.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, credentials) != 0 ||
            sched::resource::initialize_credentials(*child, credentials) != 0 || !sched::task::claim_unpublished_process(&owner, child)) {
            delete child;
            release_output_capture(output);
            return false;
        }
        State policy;
        policy.entries[static_cast<unsigned>(ker::abi::resource::Id::NPROC)].limit = {1, 1};
        ProcessReservation charge(policy, UID);
        if (charge.result() != 0 || !charge.attach(*child)) {
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-charge");
            return false;
        }
        ker::vfs::LocalPipePerfSnapshot before{};
        ker::vfs::vfs_get_local_pipe_perf_snapshot(before);
        std::array<int, 2> fds{-1, -1};
        if (ker::vfs::vfs_selftest_create_pipe(*child, fds.data()) != 0) {
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-pipe");
            return false;
        }
        struct CloseObservation {
            Task* owner;
            Task* child;
            WkiPeer* peer;
            security::Snapshot credentials;
            unsigned closes = 0;
            bool safe = true;
        } observation{&owner, child, &peer, credentials};
        ker::vfs::FileOperations fops{};
        fops.vfs_close = [](ker::vfs::File* file) -> int {
            auto& observed = *static_cast<CloseObservation*>(file->private_data);
            ++observed.closes;
            observed.safe &= observed.owner->owned_unpublished_process.load() == observed.child &&
                             observed.owner->unpublished_teardown_in_progress.load() &&
                             observed.child->credentials() == observed.credentials && !observed.child->scheduler_published.load();
            bool const RX = wki_peer_lifecycle_rx_try_acquire(observed.peer);
            observed.safe &= RX;
            if (RX) wki_peer_lifecycle_rx_release(observed.peer);
            if (s_compute_lock.try_lock()) {
                for (auto const& running : g_running_remote_tasks) observed.safe &= !running.active || running.task != observed.child;
                s_compute_lock.unlock();
            } else {
                observed.safe = false;
            }
            return 0;
        };
        auto* file = new (std::nothrow) ker::vfs::File{};
        if (file == nullptr) {
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-file");
            return false;
        }
        file->refcount.store(1);
        file->fops = &fops;
        file->private_data = &observation;
        if (!child->fd_table.insert(2, file)) {
            ker::vfs::vfs_put_file(file);
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-fd");
            return false;
        }
        child->resources.commit_extent(static_cast<unsigned>(ker::abi::resource::Id::NOFILE), 3);
        // Entirely private, unregistered proxy: no other thread can access it.
        ProxyIpcState proxy{};
        proxy.active.store(true);
        proxy.message_write_flow_control = true;
        auto release_accounting = [](RemoteServiceAccounting* accounting) {
            if (accounting != nullptr) accounting->release();
        };
        std::unique_ptr<RemoteServiceAccounting, decltype(release_accounting)> accounting(RemoteServiceAccounting::create(),
                                                                                          release_accounting);
        std::unique_ptr<WkiIpcOutputCompletion> receipt(new (std::nothrow) WkiIpcOutputCompletion);
        if (!accounting || !receipt || !accounting->attach(*child)) {
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-accounting");
            return false;
        }
        receipt->retain_locked(&proxy);
        bool const MONITOR_REF = child->try_acquire();
        bool const WORKER_REF = MONITOR_REF && child->try_acquire();
        if (!WORKER_REF) {
            if (MONITOR_REF) child->release();
            destroy_unpublished_remote_process(&owner, child, output, "ktest-publication-refs");
            return false;
        }
        RunningRemoteTask running{};
        running.active = true;
        running.task_id = TASK_ID;
        running.local_pid = child->pid;
        running.task = child;
        running.output = output;
        running.output_completion = receipt.get();
        running.service_accounting = accounting.get();
        accounting->retain();
        running.submitter_node = NODE;
        running.submit_session_epoch = SESSION.epoch;
        running.submit_rx_channel = &channel;
        running.submit_rx_channel_generation = 7;
        s_compute_lock.lock();
        bool const TRACKED = publish_running_task_locked(running);
        s_compute_lock.unlock();
        if (!TRACKED) {
            discard_unpublished_running_task(&owner, child, running);
            return false;
        }
        static_cast<void>(receipt.release());
        PeerLifecycleLease lease;
        bool const LEASED = scenario == Scenario::NO_LEASE || lease.acquire(&peer);
        case_ok &= LEASED && proxy.completion_refs == 1 && proxy.refcount.load() == 2 && accounting->snapshot().members == 1;
        s_compute_lock.lock();
        if (scenario == Scenario::RETIRED) retire_compute_submit_session_locked(NODE, &channel, 7);
        if (scenario == Scenario::IDENTITY_CHANGED) find_compute_submit_peer_session_locked(NODE)->current.identity.policy = 0;
        if (scenario == Scenario::CHANNEL_REUSED) channel.generation = 8;
        if (scenario == Scenario::CANCELLED || scenario == Scenario::OTHER_CANCEL)
            record_cancelled_task_submit_locked(SESSION, scenario == Scenario::CANCELLED ? TASK_ID : TASK_ID + 1, WKI_SIGKILL_NUM);
        s_compute_lock.unlock();
        unsigned posts = 0;
        bool const RESULT = post_admitted_remote_process(
            SESSION, TASK_ID, scenario == Scenario::EXPIRED ? uint64_t{1} : 0, lease, scenario != Scenario::NOT_READY, [&]() {
                ++posts;
                case_ok &= lease.owns(NODE) && child->credentials() == credentials && owner.owned_unpublished_process.load() == child;
                if (s_compute_lock.try_lock())
                    s_compute_lock.unlock();
                else
                    ok = false;
                return scenario != Scenario::POST_REFUSED;
            });
        bool const CALLED = scenario == Scenario::ACCEPTED || scenario == Scenario::OTHER_CANCEL || scenario == Scenario::POST_REFUSED;
        case_ok &= posts == static_cast<unsigned>(CALLED) && RESULT == (CALLED && scenario != Scenario::POST_REFUSED) &&
                   child->credentials() == credentials && !child->scheduler_published.load();
        State observations;
        // The callback deliberately never schedules this synthetic child.
        // Both refusal and successful admission controls use real private teardown.
        discard_unposted_remote_process(&owner, child, child, child, output, lease, &observations);
        ker::vfs::LocalPipePerfSnapshot after{};
        ker::vfs::vfs_get_local_pipe_perf_snapshot(after);
        sched::resource::copy_process_observation(observer, population);
        case_ok &= observation.closes == 1 && observation.safe && owner.owned_unpublished_process.load() == nullptr &&
                   !owner.unpublished_teardown_in_progress.load() && population.current == 0 && after.active_pipes == before.active_pipes &&
                   after.capacity_bytes == before.capacity_bytes && after.buffered_bytes == before.buffered_bytes &&
                   proxy.completion_refs == 0 && proxy.refcount.load() == 1 && accounting->snapshot().members == 0 && !lease.owns(NODE);
        ProcessReservation retry(policy, UID);
        case_ok &= retry.result() == 0;
        if (!case_ok)
            dbg::logger<"wki-selftest">::error(
                "receiver publication scenario=%u posts=%u result=%d closes=%u safe=%d population=%llu receipts=%u refs=%d members=%llu",
                static_cast<unsigned>(scenario), posts, RESULT, observation.closes, observation.safe, population.current,
                proxy.completion_refs, proxy.refcount.load(), accounting->snapshot().members);
        ok &= case_ok;
    }
    return ok;
}

auto wki_remote_compute_selftest_rejection_retry_is_bounded() -> bool {
    constexpr std::array TRANSIENT = {WKI_ERR_NO_MEM,    WKI_ERR_NO_ROUTE, WKI_ERR_NO_CREDITS,
                                      WKI_ERR_NOT_FOUND, WKI_ERR_BUSY,     WKI_ERR_TX_FAILED};
    bool ok = true;
    for (int error : TRANSIENT) {
        unsigned sends = 0;
        unsigned sleeps = 0;
        uint64_t now = 0;
        int const RESULT = retry_compute_control(
            3, [&]() { return ++sends == 1 ? error : WKI_OK; }, []() { return true; }, [&]() { return now; },
            [&](uint64_t) {
                ++sleeps;
                ++now;
            });
        ok &= RESULT == WKI_OK && sends == 2 && sleeps == 1;
    }
    for (int result : {WKI_OK, WKI_ERR_INVALID, WKI_ERR_PEER_FENCED}) {
        unsigned sends = 0;
        unsigned sleeps = 0;
        int const RESULT = retry_compute_control(
            3,
            [&]() {
                ++sends;
                return result;
            },
            []() { return true; }, []() { return uint64_t{0}; }, [&](uint64_t) { ++sleeps; });
        ok &= RESULT == result && sends == 1 && sleeps == 0;
    }
    unsigned sends = 0;
    uint64_t now = 0;
    int const EXHAUSTED = retry_compute_control(
        3,
        [&]() {
            ++sends;
            return WKI_ERR_NO_ROUTE;
        },
        []() { return true; }, [&]() { return now; }, [&](uint64_t) { ++now; });
    ok &= EXHAUSTED == WKI_ERR_NO_ROUTE && sends == 3 && now == 3;
    sends = 0;
    bool current = true;
    int const RETIRED = retry_compute_control(
        3,
        [&]() {
            ++sends;
            return WKI_ERR_NOT_FOUND;
        },
        [&]() { return current; }, []() { return uint64_t{0}; }, [&](uint64_t) { current = false; });
    ok &= RETIRED == WKI_ERR_NOT_FOUND && sends == 1;
    int const STALE = retry_compute_control(
        3,
        [&]() {
            ++sends;
            return WKI_OK;
        },
        [&]() { return current; }, []() { return uint64_t{0}; }, [](uint64_t) {});
    ok &= STALE == WKI_ERR_NOT_FOUND && sends == 1;
    unsigned sleeps = 0;
    sends = 0;
    int const EXPIRED = retry_compute_control(
        0,
        [&]() {
            ++sends;
            return WKI_ERR_NO_ROUTE;
        },
        []() { return true; }, []() { return uint64_t{0}; }, [&](uint64_t) { ++sleeps; });
    return ok && EXPIRED == WKI_ERR_NO_ROUTE && sends == 1 && sleeps == 0;
}

auto wki_remote_compute_selftest_accept_retry_is_fair() -> bool {
    constexpr size_t ROW_COUNT = WKI_COMPUTE_SUBMIT_QUEUE_MAX + 2;
    RunningRemoteTasks test_rows;
    for (size_t i = 0; i < ROW_COUNT; ++i) {
        RunningRemoteTask row{};
        row.active = true;
        row.published = true;
        row.accept_pending = true;
        row.task_id = static_cast<uint32_t>(i + 1);
        row.submitter_node = i < WKI_COMPUTE_SUBMIT_QUEUE_MAX ? 0x7A31 : 0x7A32;
        row.local_pid = i + 1;
        if (!test_rows.push_back(row)) return false;
    }

    std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX> first = {};
    std::array<PendingTaskAcceptAttempt, WKI_COMPUTE_SUBMIT_QUEUE_MAX> second = {};
    RunningRemoteTasks saved_rows;

    s_compute_lock.lock();
    std::swap(saved_rows, g_running_remote_tasks);
    std::swap(test_rows, g_running_remote_tasks);
    size_t const SAVED_CURSOR = s_pending_task_accept_cursor;
    s_pending_task_accept_cursor = 0;
    size_t const FIRST_COUNT = collect_pending_task_accept_attempts_locked(first);
    size_t const SECOND_COUNT = collect_pending_task_accept_attempts_locked(second);
    s_pending_task_accept_cursor = SAVED_CURSOR;
    std::swap(test_rows, g_running_remote_tasks);
    std::swap(saved_rows, g_running_remote_tasks);
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

    // A retained thread's metadata must not hold remote wait hostage after
    // its final CPU contribution. Use a real owner binding so ref lifetime
    // and accounting retirement are exercised independently.
    ker::mod::sched::resource::LifetimeOwner retained_thread;
    if (!retained_thread.attach(&exact_task)) return false;
    row.resource_contract = false;
    bool const LEGACY_READY = running_remote_task_accounting_ready(row, exact_task);
    row.resource_contract = true;
    bool const CPU_PENDING_BLOCKED = !running_remote_task_accounting_ready(row, exact_task);
    retained_thread.finish_cpu();
    bool const METADATA_RETAINED_READY = exact_task.resource_thread_owners.load() == 1 && exact_task.resource_cpu_threads.load() == 0 &&
                                         running_remote_task_accounting_ready(row, exact_task);
    exact_task.resource_deferred_observations.store(1);
    bool const DEFERRED_OUTPUT_BLOCKED = !running_remote_task_accounting_ready(row, exact_task);
    exact_task.resource_deferred_observations.store(0, std::memory_order_release);
    bool const OUTPUT_FINISHED_READY = running_remote_task_accounting_ready(row, exact_task);
    exact_task.exit_notify_ready.store(false, std::memory_order_release);
    bool const CLEANUP_PENDING_BLOCKED = !running_remote_task_accounting_ready(row, exact_task);
    exact_task.exit_notify_ready.store(true, std::memory_order_release);
    exact_task.has_exited = false;
    bool const LIVE_TASK_BLOCKED = !running_remote_task_accounting_ready(row, exact_task);

    return EXACT_MATCH && INACTIVE_REJECTED && OTHER_TASK_REJECTED && OTHER_PID_REJECTED && UNREADY_REJECTED && UNPUBLISHED_INELIGIBLE &&
           ACCEPT_PENDING_INELIGIBLE && ACCEPTED_ELIGIBLE && DISCARD_ELIGIBLE && LEGACY_READY && CPU_PENDING_BLOCKED &&
           METADATA_RETAINED_READY && DEFERRED_OUTPUT_BLOCKED && OUTPUT_FINISHED_READY && CLEANUP_PENDING_BLOCKED && LIVE_TASK_BLOCKED;
}

auto wki_remote_compute_selftest_admitted_peer_identity() -> bool {
    constexpr uint16_t NODE = 0x7A28;
    constexpr uint32_t TASK = 0xC10301;
    constexpr uint16_t CAPS = WKI_CAP_COMPUTE_CREDENTIALS | WKI_CAP_RESOURCE_LIMITS;
    WkiPeer peer{};
    peer.node_id = NODE;
    peer.state = PeerState::CONNECTED;
    peer.remote_boot_epoch = 17;
    peer.capabilities = CAPS;
    peer.auth_session.active = true;
    peer.auth_session.generation = 4;
    peer.auth_session.session_id[0] = 9;
    peer.auth_session.policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    PendingComputeRx work{};
    work.hdr.src_node = NODE;
    work.identity = compute_peer_identity(peer, CAPS);
    auto const CAPTURED = work.identity;
    if (!CAPTURED.valid() || CAPTURED.boot_epoch != 17 || CAPTURED.auth_generation != 4 || CAPTURED.auth_session_id[0] != 9 ||
        !compute_rx_identity_matches(work, peer, CAPS))
        return false;
    for (unsigned field = 0; field < 8; ++field) {
        switch (field) {
            case 0:
                ++peer.remote_boot_epoch;
                break;
            case 1:
                ++peer.auth_session.generation;
                break;
            case 2:
                ++peer.auth_session.session_id[0];
                break;
            case 3:
                peer.auth_session.policy = 0;
                break;
            case 4:
                peer.capabilities = 0;
                break;
            case 5:
                peer.auth_session.active = false;
                break;
            case 6:
                peer.state = PeerState::FENCED;
                break;
            default:
                ++peer.node_id;
                break;
        }
        if (compute_rx_identity_matches(work, peer, CAPS) || work.identity != CAPTURED) return false;
        peer.remote_boot_epoch = CAPTURED.boot_epoch;
        peer.auth_session.generation = CAPTURED.auth_generation;
        peer.auth_session.session_id = CAPTURED.auth_session_id;
        peer.auth_session.policy = CAPTURED.policy;
        peer.capabilities = CAPS;
        peer.auth_session.active = true;
        peer.state = PeerState::CONNECTED;
        peer.node_id = NODE;
    }
    if (compute_rx_identity_matches(work, peer, 0)) return false;
    peer.remote_boot_epoch = 0;
    if (compute_peer_identity(peer, CAPS).valid()) return false;
    peer.remote_boot_epoch = 17;
    peer.auth_session.generation = 0;
    if (compute_peer_identity(peer, CAPS).valid()) return false;
    peer.auth_session.generation = 4;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;
    s_compute_lock.lock();
    bool const UNUSED = find_compute_submit_peer_session_locked(NODE) == nullptr;
    s_compute_lock.unlock();
    if (!UNUSED) return false;
    struct Cleanup {
        ~Cleanup() {
            release_fragmented_task_submits(NODE);
            s_compute_lock.lock();
            if (auto* row = find_compute_submit_peer_session_locked(NODE); row != nullptr) *row = {};
            s_compute_lock.unlock();
        }
    } cleanup;
    s_compute_lock.lock();
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 7, CAPTURED);
    bool const CURRENT = compute_submit_admission_is_current_locked(SESSION);
    auto changed = CAPTURED;
    ++changed.boot_epoch;
    auto altered_token = SESSION;
    altered_token.identity = changed;
    bool const MIXED_DENIED = capture_compute_submit_session_locked(NODE, &channel, 7, changed).epoch == 0 &&
                              !compute_submit_admission_is_current_locked(altered_token) &&
                              compute_submit_admission_is_current_locked(SESSION);
    // Cleanup keys retain their existing semantics; they confer no new identity.
    ComputeSubmitSessionToken const KEY{.node_id = NODE, .rx_channel = &channel, .rx_channel_generation = 7, .epoch = SESSION.epoch};
    bool const KEY_ONLY = compute_submit_session_is_current_locked(KEY) && !compute_submit_admission_is_current_locked(KEY);
    s_compute_lock.unlock();
    if (!CURRENT || !MIXED_DENIED || !KEY_ONLY) return false;
    // Admission identity survives a delayed construction's origin resolution.
    peer.remote_boot_epoch = 18;
    auto const ORIGIN = resolve_session_origin({}, NODE, SESSION.identity.boot_epoch);
    if (ORIGIN.boot != 17 || ORIGIN.node != NODE || SESSION.identity != CAPTURED) return false;
    std::array<uint8_t, sizeof(TaskSubmitFragmentPayload) + sizeof(TaskSubmitPayload)> bytes{};
    TaskSubmitFragmentPayload fragment{.task_id = TASK, .total_len = WKI_ETH_MAX_PAYLOAD + 1};
    TaskSubmitPayload submit{.task_id = TASK};
    std::memcpy(bytes.data(), &fragment, sizeof(fragment));
    std::memcpy(bytes.data() + sizeof(fragment), &submit, sizeof(submit));
    WkiHeader header{.src_node = NODE, .channel_id = WKI_CHAN_RESOURCE};
    handle_task_submit_fragment_work(header, bytes.data(), bytes.size(), &channel, 7, CAPTURED);
    auto received = [&]() -> uint16_t {
        uint16_t result = 0;
        s_fragmented_task_submit_lock.lock();
        for (auto const& entry : s_fragmented_task_submits) {
            if (entry.active && entry.src_node == NODE && entry.task_id == TASK && entry.session.identity == CAPTURED)
                result = entry.received_len;
        }
        s_fragmented_task_submit_lock.unlock();
        return result;
    };
    if (received() != sizeof(submit)) return false;
    fragment.offset = sizeof(submit);
    std::memcpy(bytes.data(), &fragment, sizeof(fragment));
    handle_task_submit_fragment_work(header, bytes.data(), sizeof(fragment) + 1, &channel, 7, changed);
    if (received() != sizeof(submit)) return false;
    handle_task_submit_fragment_work(header, bytes.data(), sizeof(fragment) + 1, &channel, 7, CAPTURED);
    if (received() != sizeof(submit) + 1) return false;
    s_compute_lock.lock();
    retire_compute_submit_session_locked(NODE, &channel, 7);
    bool const RETIRED = !compute_submit_admission_is_current_locked(SESSION);
    s_compute_lock.unlock();
    fragment.offset = sizeof(submit) + 1;
    std::memcpy(bytes.data(), &fragment, sizeof(fragment));
    handle_task_submit_fragment_work(header, bytes.data(), sizeof(fragment) + 1, &channel, 7, CAPTURED);
    if (!RETIRED || received() != sizeof(submit) + 1) return false;
    release_fragmented_task_submits(NODE, &channel, 7);
    return received() == 0;
}

auto wki_remote_compute_selftest_submit_cancel_is_session_scoped() -> bool {
    constexpr uint32_t TASK_ID = 0xC10101U;
    constexpr uint16_t NODE_ID = 0x7A26;
    constexpr uint64_t TRUSTED_POLICY =
        wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN) | wki_policy_bits(WkiPolicy::IDENTITY_ROOT);
    ComputePeerIdentity const IDENTITY{
        .boot_epoch = 1, .auth_generation = 1, .policy = TRUSTED_POLICY, .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE_ID;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;

    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE_ID) != nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto const SESSION_A = capture_compute_submit_session_locked(NODE_ID, &channel, 7, IDENTITY);
    auto const REUSED = capture_compute_submit_session_locked(NODE_ID, &channel, 7, IDENTITY);
    bool const CAPTURED = SESSION_A.epoch != 0 && SESSION_A.identity.policy == TRUSTED_POLICY &&
                          SESSION_A.identity.capabilities == WKI_CAP_COMPUTE_CREDENTIALS &&
                          REUSED.identity.capabilities == SESSION_A.identity.capabilities &&
                          compute_submit_session_token_matches(SESSION_A, REUSED) && REUSED.identity.policy == TRUSTED_POLICY;
    retire_compute_submit_session_locked(NODE_ID, &channel, 7);
    bool const RETIRED = !compute_submit_session_is_current_locked(SESSION_A) &&
                         capture_compute_submit_session_locked(NODE_ID, &channel, 7, IDENTITY).epoch == 0;
    channel.generation = 8;
    auto const SESSION_B = capture_compute_submit_session_locked(NODE_ID, &channel, 8, {.boot_epoch = 2, .auth_generation = 2});
    bool const ROTATED = SESSION_B.epoch != 0 && SESSION_B.identity.policy == 0 && SESSION_A.identity.policy == TRUSTED_POLICY &&
                         !compute_submit_session_is_current_locked(SESSION_A) && compute_submit_session_is_current_locked(SESSION_B);
    record_cancelled_task_submit_locked(SESSION_A, TASK_ID, WKI_SIGKILL_NUM);
    bool const EXACT_MATCH = find_cancelled_task_submit_locked(SESSION_A, TASK_ID) != nullptr;
    bool const OTHER_SESSION_CLEAR = find_cancelled_task_submit_locked(SESSION_B, TASK_ID) == nullptr;
    bool const CONSUMED = take_cancelled_task_submit_locked(SESSION_A, TASK_ID) == WKI_SIGKILL_NUM;
    bool const CLEARED = find_cancelled_task_submit_locked(SESSION_A, TASK_ID) == nullptr;
    if (auto* peer_session = find_compute_submit_peer_session_locked(NODE_ID); peer_session != nullptr) {
        *peer_session = {};
    }
    s_compute_lock.unlock();
    return CAPTURED && RETIRED && ROTATED && EXACT_MATCH && OTHER_SESSION_CLEAR && CONSUMED && CLEARED;
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

    auto const IDENTITY = capture_compute_peer_identity(hdr->src_node);
    s_compute_lock.lock();
    ComputeSubmitSessionToken const SESSION =
        capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation, IDENTITY);
    s_compute_lock.unlock();
    if (SESSION.epoch == 0) {
        return;
    }

    queue_admitted_task_submit(hdr, payload, payload_len, SESSION);
}

void handle_task_signal_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(TaskSignalResponse)) return;
    TaskSignalResponse response{};
    std::memcpy(&response, payload, sizeof(response));
    if (response.request_id == 0 || response.task_id == 0 || response.result > 0 || response.result < -4095) return;
    WkiWaitEntry* wait = nullptr;
    s_compute_lock.lock();
    for (auto& slot : s_user_signals) {
        if (slot.owner != nullptr && !slot.group_operation && slot.request_id == response.request_id && slot.task_id == response.task_id &&
            slot.node == hdr->src_node && slot.channel == rx_channel && slot.generation == rx_channel_generation) {
            wait = claim_and_clear_waiter_locked(slot.response_wait);
            if (wait != nullptr) slot.result = response.result;
            break;
        }
    }
    s_compute_lock.unlock();
    if (wait != nullptr) wki_finish_claimed_op(wait, WKI_OK);
}

void handle_group_receiver_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                    uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(GroupReceiverResponse)) return;
    GroupReceiverResponse response{};
    std::memcpy(&response, payload, sizeof(response));
    if (response.request_id == 0 || response.version != WKI_GROUP_RECEIVER_VERSION || response.reserved != 0 || response.boot == 0 ||
        response.result > 0 || response.result < -4095 || response.count > response.receivers.size())
        return;
    WkiWaitEntry* wait = nullptr;
    s_compute_lock.lock();
    for (auto& slot : s_user_signals) {
        if (slot.owner == nullptr || !slot.receiver_operation || slot.request_id != response.request_id || slot.node != hdr->src_node ||
            slot.channel != rx_channel || slot.generation != rx_channel_generation || slot.source_boot != response.boot)
            continue;
        auto const& request = slot.route_request;
        bool valid = true;
        for (size_t i = response.count; i < response.receivers.size(); ++i) {
            auto const& entry = response.receivers[i];
            valid &= entry.serial == 0 && entry.pid == 0 && entry.home.serial == 0 && entry.home.origin.node == 0 &&
                     entry.home.origin.boot == 0 && entry.home.origin.reserved == 0;
        }
        if (response.result != 0) {
            valid &= response.count == 0 && response.ceiling == 0;
        } else {
            valid &= response.ceiling >= request.cursor && (request.ceiling == 0 || request.ceiling == response.ceiling);
            uint64_t previous = request.cursor;
            for (size_t i = 0; i < response.count; ++i) {
                auto const& entry = response.receivers[i];
                valid &= entry.serial > previous && entry.serial <= response.ceiling && entry.pid != 0 && entry.home.serial != 0 &&
                         entry.home.origin.node != 0 && entry.home.origin.node != WKI_NODE_BROADCAST &&
                         valid_session_origin(entry.home.origin);
                previous = entry.serial;
            }
        }
        if (!valid) break;
        wait = claim_and_clear_waiter_locked(slot.response_wait);
        if (wait != nullptr) {
            slot.result = response.result;
            slot.receiver_response = response;
        }
        break;
    }
    s_compute_lock.unlock();
    if (wait != nullptr) wki_finish_claimed_op(wait, WKI_OK);
}

void handle_group_route_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                 uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(GroupRouteResponse)) return;
    GroupRouteResponse response{};
    std::memcpy(&response, payload, sizeof(response));
    if (response.request_id == 0 || response.version != WKI_GROUP_ROUTE_VERSION || response.reserved != 0 || response.boot == 0 ||
        response.result > 0 || response.result < -4095 || response.kind > static_cast<uint8_t>(GroupRouteResult::RETRY) ||
        response.count > response.anchors.size())
        return;
    auto zero_origin = [](const TaskSessionOrigin& origin) { return origin.node == 0 && origin.boot == 0 && origin.reserved == 0; };
    auto concrete = [](const TaskSessionOrigin& origin) {
        return origin.node != 0 && origin.node != WKI_NODE_BROADCAST && origin.boot != 0 && origin.reserved == 0;
    };
    WkiWaitEntry* wait = nullptr;
    s_compute_lock.lock();
    for (auto& slot : s_user_signals) {
        if (slot.owner == nullptr || !slot.route_operation || slot.request_id != response.request_id || slot.node != hdr->src_node ||
            slot.channel != rx_channel || slot.generation != rx_channel_generation || slot.source_boot != response.boot)
            continue;
        auto const& request = slot.route_request;
        bool const ENUMERATE = request.operation == static_cast<uint8_t>(GroupRouteOperation::ENUMERATE);
        bool const QUERY = request.operation == static_cast<uint8_t>(GroupRouteOperation::QUERY);
        bool const FINAL = response.kind == static_cast<uint8_t>(GroupRouteResult::FINAL);
        bool valid = true;
        for (size_t i = response.count; i < response.anchors.size(); ++i)
            valid &= response.anchors[i].serial == 0 && response.anchors[i].pid == 0;
        if (response.result != 0) {
            valid &= FINAL && response.count == 0 && response.ceiling == 0 && response.target_pgid == 0 &&
                     zero_origin(response.group_origin) && zero_origin(response.destination) && response.target_pid == 0;
        } else if (ENUMERATE) {
            valid &= FINAL && response.ceiling >= request.cursor && (request.ceiling == 0 || request.ceiling == response.ceiling) &&
                     response.target_pgid == 0 && zero_origin(response.group_origin) && zero_origin(response.destination) &&
                     response.target_pid == 0;
            uint64_t previous = request.cursor;
            for (size_t i = 0; i < response.count; ++i) {
                valid &=
                    response.anchors[i].serial > previous && response.anchors[i].serial <= response.ceiling && response.anchors[i].pid != 0;
                previous = response.anchors[i].serial;
            }
        } else {
            valid &= response.count == 0 && response.ceiling == 0;
            if (FINAL) {
                valid &= zero_origin(response.destination) && response.target_pid == 0 &&
                         (QUERY ? concrete(response.group_origin) : (response.target_pgid == 0 && zero_origin(response.group_origin)));
            } else {
                valid &= response.target_pgid == 0 && zero_origin(response.group_origin);
                if (response.kind == static_cast<uint8_t>(GroupRouteResult::REDIRECT))
                    valid &= concrete(response.destination) && response.target_pid != 0;
                else
                    valid &= zero_origin(response.destination) && response.target_pid == 0;
            }
        }
        if (!valid) break;
        wait = claim_and_clear_waiter_locked(slot.response_wait);
        if (wait != nullptr) {
            slot.result = response.result;
            slot.route_response = response;
        }
        break;
    }
    s_compute_lock.unlock();
    if (wait != nullptr) wki_finish_claimed_op(wait, WKI_OK);
}

void handle_group_signal_response(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                  uint32_t rx_channel_generation) {
    using ker::mod::sched::session::GroupCandidates;
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(GroupSignalResponse)) return;
    GroupSignalResponse response{};
    std::memcpy(&response, payload, sizeof(response));
    constexpr uint8_t FLAGS = GroupCandidates::LOCAL | GroupCandidates::OWN | GroupCandidates::OTHER | GroupCandidates::AMBIGUOUS;
    bool const OTHER = (response.flags & GroupCandidates::OTHER) != 0;
    bool const ZERO_ORIGIN = response.other.node == 0 && response.other.boot == 0;
    if (response.request_id == 0 || response.version != WKI_GROUP_SIGNAL_VERSION || response.reserved != 0 || response.result > 0 ||
        response.result < -4095 || (response.flags & ~FLAGS) != 0 || !valid_session_origin(response.other) ||
        response.other.node == WKI_NODE_BROADCAST || (!OTHER && !ZERO_ORIGIN) ||
        ((response.flags & GroupCandidates::AMBIGUOUS) != 0 && !OTHER) || (response.result != 0 && response.flags != 0))
        return;
    WkiWaitEntry* wait = nullptr;
    s_compute_lock.lock();
    for (auto& slot : s_user_signals) {
        if (slot.owner == nullptr || !slot.group_operation || slot.request_id != response.request_id || slot.node != hdr->src_node ||
            slot.channel != rx_channel || slot.generation != rx_channel_generation)
            continue;
        if ((!slot.group_query && response.flags != 0) || (!slot.group_own_allowed && (response.flags & GroupCandidates::OWN) != 0)) break;
        wait = claim_and_clear_waiter_locked(slot.response_wait);
        if (wait != nullptr) {
            slot.result = response.result;
            slot.groups.flags = response.flags;
            if (OTHER) slot.groups.other = resolve_session_origin(response.other, hdr->src_node, slot.source_boot);
        }
        break;
    }
    s_compute_lock.unlock();
    if (wait != nullptr) wki_finish_claimed_op(wait, WKI_OK);
}

void handle_task_exec_ready(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                            uint32_t generation) {
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(TaskExecReady)) return;
    TaskExecReady ready{};
    std::memcpy(&ready, payload, sizeof(ready));
    if (ready.version != WKI_EXEC_HANDOFF_VERSION || ready.reserved != 0 || ready.task_id == 0 || ready.remote_pid == 0) return;
    WkiWaitEntry* waiter = nullptr;
    s_compute_lock.lock();
    auto* task = find_submitted_task(ready.task_id);
    if (task != nullptr && task->exec_handoff && !task->exec_prepared && !task->exec_cutover && task->target_node == hdr->src_node &&
        task->submit_channel == rx_channel && task->submit_channel_generation == generation &&
        task->response_pending.load(std::memory_order_acquire)) {
        waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
        if (waiter != nullptr) {
            task->exec_prepared = true;
            task->exec_remote_pid = ready.remote_pid;
            task->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        }
        task->response_pending.store(false, std::memory_order_release);
    }
    s_compute_lock.unlock();
    if (waiter != nullptr) wki_finish_claimed_op(waiter, WKI_OK);
}

void handle_task_exec_commit(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                             uint32_t generation) {
    if (hdr == nullptr || payload == nullptr || payload_len != sizeof(TaskExecCommit)) return;
    TaskExecCommit commit{};
    std::memcpy(&commit, payload, sizeof(commit));
    auto const& identity = commit.identity;
    if (identity.version != WKI_EXEC_HANDOFF_VERSION || identity.reserved != 0 || identity.task_id == 0 || identity.remote_pid == 0 ||
        !valid_exec_signals(commit.signals))
        return;
    s_compute_lock.lock();
    auto* peer = find_compute_submit_peer_session_locked(hdr->src_node);
    if (peer != nullptr && peer->open && !peer->retiring && peer->current.rx_channel == rx_channel &&
        peer->current.rx_channel_generation == generation) {
        for (auto& running : g_running_remote_tasks) {
            if (running.active && !running.published && running.exec_handoff && !running.exec_committed && !running.discard_completion &&
                running.task_id == identity.task_id && running.local_pid == identity.remote_pid &&
                running_task_session_matches(running, peer->current)) {
                running.exec_signals = commit.signals;
                running.exec_committed = true;
                break;
            }
        }
    }
    s_compute_lock.unlock();
}

void handle_task_accept(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || (task->exec_handoff && (!task->exec_cutover || resp->remote_pid != task->exec_remote_pid)) ||
        task->target_node != hdr->src_node || task->submit_channel != rx_channel ||
        task->submit_channel_generation != rx_channel_generation || !task->response_pending.load(std::memory_order_acquire)) {
        s_compute_lock.unlock();
        return;
    }

    WkiWaitEntry* waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
    if (waiter != nullptr) {
        task->accept_status = resp->status;
        task->signal_version = resp->reserved[1] == 0 && resp->reserved[2] == 0 ? resp->reserved[0] : 0;
        task->accepted_at_us = wki_now_us();
        task->complete_received_at_us = 0;
        if (task->local_task != nullptr && resp->status == static_cast<uint8_t>(TaskRejectReason::ACCEPTED)) {
            task->local_task->publish_accepted_signal_route(resp->task_id, resp->remote_pid);
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

void handle_task_service_report(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                                uint32_t rx_channel_generation) {
    if (hdr == nullptr) return;
    TaskServiceReport report{};
    ker::mod::sched::resource::RemoteServiceObservation observation{};
    if (!service_contract::decode(payload, payload_len, report, observation)) return;
    s_compute_lock.lock();
    auto* task = find_submitted_task_any(report.task_id);
    if (task != nullptr && task->service_report_pending && task->target_node == hdr->src_node && task->submit_channel == rx_channel &&
        task->submit_channel_generation == rx_channel_generation) {
        bool const FINAL = (report.flags & WKI_SERVICE_REPORT_FINAL) != 0;
        auto const RESULT = task->service_report.apply(report.sequence, observation, FINAL);
        if (FINAL && RESULT == ker::mod::sched::resource::ServiceReportResult::APPLIED) {
            task->cancel_service_report(false);
            reclaim_submitted_task_if_safe_locked(task);
        }
    }
    s_compute_lock.unlock();
}

void handle_task_reject(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                        uint32_t rx_channel_generation) {
    if (hdr == nullptr || payload_len < sizeof(TaskResponsePayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const TaskResponsePayload*>(payload);
    uint16_t const RESOURCE_LEN = static_cast<uint16_t>(resp->reserved[0]) | (static_cast<uint16_t>(resp->reserved[1]) << 8);
    if (resp->reserved[2] != 0 || sizeof(TaskResponsePayload) + RESOURCE_LEN != payload_len ||
        (RESOURCE_LEN != 0 && RESOURCE_LEN != sizeof(TaskResourceSnapshot)) ||
        resp->status == static_cast<uint8_t>(TaskRejectReason::ACCEPTED))
        return;
    std::optional<TaskResourceSnapshot> observations;
    if (RESOURCE_LEN != 0) {
        observations.emplace();
        if (!resource_contract::decode(payload + sizeof(TaskResponsePayload), RESOURCE_LEN, *observations,
                                       ker::mod::sched::task::Task::FD_TABLE_SIZE))
            return;
    }

    s_compute_lock.lock();
    SubmittedTask* task = find_submitted_task(resp->task_id);
    if (task == nullptr || task->target_node != hdr->src_node || task->submit_channel != rx_channel ||
        task->submit_channel_generation != rx_channel_generation || !task->response_pending.load(std::memory_order_acquire)) {
        s_compute_lock.unlock();
        return;
    }

    if (observations && (!task->resource_baseline || !resource_contract::valid_rejection(*task->resource_baseline, *observations,
                                                                                         ker::mod::sched::task::Task::FD_TABLE_SIZE))) {
        s_compute_lock.unlock();
        return;
    }

    WkiWaitEntry* waiter = claim_and_clear_waiter_locked(task->response_wait_entry);
    if (waiter != nullptr) {
        if (observations && task->local_task != nullptr) {
            auto* owner = task->local_task->is_thread ? task->local_task->resource_owner.get() : task->local_task;
            if (owner != nullptr) {
                uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
                static_cast<void>(resource_contract::merge_rejection(*task->resource_baseline, *observations, owner->resources,
                                                                     ker::mod::sched::task::Task::FD_TABLE_SIZE));
                owner->resource_lock.unlock_irqrestore(FLAGS);
            }
        }
        task->cancel_service_report(false);
        task->accept_status = resp->status;
        // A refused exec retains the identity of the still-running image.
        // Only an unpublished ordinary spawn has no prior remote identity.
        if (task->local_task != nullptr && !task->exec_handoff) {
            task->local_task->set_wki_remote_pid(0);
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
    size_t const EXPECTED_LEN = sizeof(TaskCompletePayload) + comp->output_len + comp->reserved;
    if (EXPECTED_LEN != payload_len || comp->output_len > WKI_TASK_MAX_OUTPUT ||
        (comp->reserved != 0 && comp->reserved != sizeof(TaskResourceSnapshot)))
        return;
    std::optional<TaskResourceSnapshot> resource_completion;
    if (comp->reserved != 0) {
        resource_completion.emplace();
        if (!resource_contract::decode(payload + sizeof(TaskCompletePayload) + comp->output_len, comp->reserved, *resource_completion,
                                       ker::mod::sched::task::Task::FD_TABLE_SIZE))
            return;
    }
    std::optional<TaskResourceSnapshot> resource_baseline;
    uint16_t const OUTPUT_LEN = comp->output_len;
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
    if (task == nullptr || (task->exec_handoff && !task->exec_cutover) || task->target_node != hdr->src_node ||
        task->submit_channel != rx_channel || task->submit_channel_generation != rx_channel_generation) {
        s_compute_lock.unlock();
        delete[] pending_output;
        ker::mod::dbg::log("[WKI] TASK_COMPLETE: no submitted task for task_id=%u", comp->task_id);
        return;
    }

    if (task->resource_baseline.has_value() != resource_completion.has_value() ||
        (task->resource_baseline && !resource_contract::valid_completion(*task->resource_baseline, *resource_completion,
                                                                         ker::mod::sched::task::Task::FD_TABLE_SIZE))) {
        s_compute_lock.unlock();
        delete[] pending_output;
        return;
    }
    resource_baseline = task->resource_baseline;
    task->resource_completion = resource_completion;

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
            if (task->local_task != nullptr) {
                auto const IDENTITY = task->local_task->wki_execution_identity();
                task->local_task->publish_accepted_signal_route(comp->task_id, IDENTITY.remote_pid);
            }
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
            finalize_proxy_task(proxy, comp->exit_status, output_data, OUTPUT_LEN, comp->task_id,
                                resource_baseline ? &*resource_baseline : nullptr, resource_completion ? &*resource_completion : nullptr);
        } else {
            finalize_proxy_task(proxy, comp->exit_status, nullptr, 0, comp->task_id, resource_baseline ? &*resource_baseline : nullptr,
                                resource_completion ? &*resource_completion : nullptr);
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
    auto const IDENTITY = capture_compute_peer_identity(hdr->src_node);
    s_compute_lock.lock();
    ComputeSubmitSessionToken const SESSION =
        capture_compute_submit_session_locked(hdr->src_node, rx_channel, rx_channel_generation, IDENTITY);
    RunningRemoteTask* rt = SESSION.epoch != 0 ? find_running_task(cancel->task_id, SESSION) : nullptr;
    if (rt == nullptr) {
        if (SESSION.epoch != 0 && SIGNUM > 0 && std::cmp_less_equal(SIGNUM, ker::mod::sched::task::Task::MAX_SIGNALS)) {
            record_cancelled_task_submit_locked(SESSION, cancel->task_id, SIGNUM);
        }
        s_compute_lock.unlock();
        ker::mod::dbg::log("[WKI] Task cancel queued before publication: task_id=%u from 0x%04x", cancel->task_id, hdr->src_node);
        return;
    }

    // Report-only rows outlive the root. Its PID may already be reused.
    if (rt->root_completion_queued) {
        s_compute_lock.unlock();
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
            task->signal_generate_pending_mask(1ULL << (SIGNUM - 1));
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
    rl->balanced_assignments = 0;
    s_compute_lock.unlock();
}

}  // namespace detail

#ifdef WOS_SELFTEST
auto wki_remote_compute_selftest_exec_cutover_retirement() -> bool {
    using namespace ker::mod;
    constexpr uint16_t NODE = 0x7BF4;
    if (wki_peer_find(NODE) != nullptr) return false;
    bool ok = true;
    for (unsigned mode = 0; mode < 6; ++mode) {
        sched::task::Task source{};
        struct Scenario {
            unsigned mode{};
            security::Snapshot before{};
            security::Snapshot after{};
            unsigned calls{};
            bool waiter_owned{};
        } scenario{.mode = mode};
        auto original = security::Snapshot::bootstrap_root();
        if (original.prepare({.change = security::Change::SET_RESGID, .real = 2002, .effective = 2002, .saved = 2000}, original) != 0 ||
            original.prepare({.change = security::Change::SET_RESUID, .real = 1002, .effective = 1002, .saved = 1000}, original) != 0 ||
            sched::resource::initialize_credentials(source, original) != 0 ||
            original.prepare_exec({.uid = 1003, .gid = 2003, .set_uid = true, .set_gid = true, .mode = 06755}, scenario.after) != 0)
            return false;
        scenario.before = source.credentials();
        WkiChannel channel{};
        WkiWaitEntry initial{};
        initial.result = mode == 0 ? WKI_ERR_TIMEOUT : WKI_OK;
        initial.state.store(WkiWaitEntry::DONE, std::memory_order_release);
        SubmittedTask submitted;
        submitted.active = true;
        submitted.exec_handoff = mode != 5;
        submitted.exec_prepared = mode != 0 && mode != 5;
        submitted.exec_remote_pid = 77;
        submitted.target_node = NODE;
        submitted.submit_channel = &channel;
        submitted.submit_channel_generation = 53;
        submitted.response_consumer_wait_entry = &initial;
        if (!submitted.set_local_task_ref(&source)) return false;
        s_compute_lock.lock();
        submitted.task_id = allocate_submitted_task_id_locked();
        uint32_t const ID = submitted.task_id;
        auto* stored = publish_submitted_task_locked(std::move(submitted));
        s_compute_lock.unlock();
        if (stored == nullptr) return false;

        WkiRemoteSpawnSpec transfer;
        transfer.context = &scenario;
        transfer.cutover = [](sched::task::Task& task, uint32_t id, uint64_t, TaskExecSignals&, void* context) -> int {
            auto& state = *static_cast<Scenario*>(context);
            ++state.calls;
            s_compute_lock.lock();
            auto* row = find_submitted_task_any(id);
            state.waiter_owned = row != nullptr && row->exec_cutover && row->response_pending.load() &&
                                 row->response_wait_entry != nullptr && row->response_consumer_wait_entry == row->response_wait_entry;
            s_compute_lock.unlock();
            if (state.mode == 4) {
                int const result = sched::resource::publish_exec_credentials(task, state.before, state.after);
                if (result != 0) return result;
            }
            if (state.mode == 3 || state.mode == 4) fail_submitted_tasks_for_peer(NODE, RemoteComputeCleanupScope::ALL);
            return state.mode == 4 ? 0 : -EAGAIN;
        };
        if (mode == 1) fail_submitted_tasks_for_peer(NODE, RemoteComputeCleanupScope::ALL);
        if (mode == 5) transfer.cutover = nullptr;
        int cutover_error = 123;
        int const result = wait_for_exec_submit(initial, 1000, ID, source, &transfer, cutover_error);
        bool const called = mode >= 2 && mode <= 4;
        int const expected = mode == 0 ? WKI_ERR_TIMEOUT : mode == 1 ? WKI_ERR_PEER_FENCED : mode <= 3 ? WKI_ERR_BUSY : WKI_OK;
        ok &= result == expected && cutover_error == (mode == 2 || mode == 3 ? -EAGAIN : 0);
        ok &= scenario.calls == static_cast<unsigned>(called) && (!called || scenario.waiter_owned);
        ok &= source.credentials() == (mode == 4 ? scenario.after : scenario.before) && !source.scheduler_published.load();
        s_compute_lock.lock();
        ok &= find_submitted_task_any(ID) == stored && stored->response_wait_entry == nullptr && !stored->response_pending.load();
        ok &= stored->response_consumer_wait_entry == (called ? nullptr : &initial);
        ok &= stored->exec_cutover == (mode == 4);
        if (mode == 1 || mode == 3 || mode == 4) ok &= !stored->active && stored->exit_status == -1;
        remove_submitted_task_for_selftest_locked(ID);
        ok &= find_submitted_task_any(ID) == nullptr;
        s_compute_lock.unlock();
        ok &= source.ref_count.load() == 1;
    }
    return ok;
}

auto wki_remote_compute_selftest_exec_handoff_ownership() -> bool {
    using ker::mod::sched::task::Task;
    constexpr uint16_t NODE = 0x7B72;
    constexpr uint32_t GENERATION = 47;
    // Refusing an onward exec must retain the still-running receiver identity.
    // Exercise the actual response handler, including its generation guard.
    for (bool exec : {false, true}) {
        Task old_image{};
        old_image.set_wki_remote_pid(7);
        WkiChannel reject_channel{};
        WkiWaitEntry rejected_wait{};
        SubmittedTask rejected;
        rejected.active = true;
        rejected.exec_handoff = exec;
        rejected.target_node = NODE;
        rejected.submit_channel = &reject_channel;
        rejected.submit_channel_generation = GENERATION;
        rejected.response_wait_entry = &rejected_wait;
        rejected.response_consumer_wait_entry = &rejected_wait;
        rejected.response_pending.store(true);
        if (!rejected.set_local_task_ref(&old_image)) return false;
        s_compute_lock.lock();
        rejected.task_id = allocate_submitted_task_id_locked();
        uint32_t const REJECT_ID = rejected.task_id;
        auto* row = publish_submitted_task_locked(std::move(rejected));
        s_compute_lock.unlock();
        if (row == nullptr) return false;
        WkiHeader reject_header{};
        reject_header.src_node = NODE;
        TaskResponsePayload rejection{.task_id = REJECT_ID, .status = static_cast<uint8_t>(TaskRejectReason::UNAUTHORIZED)};
        detail::handle_task_reject(&reject_header, reinterpret_cast<const uint8_t*>(&rejection), sizeof(rejection), &reject_channel,
                                   GENERATION - 1);
        bool retained = rejected_wait.state.load() == WkiWaitEntry::PENDING && old_image.wki_execution_identity().remote_pid == 7;
        detail::handle_task_reject(&reject_header, reinterpret_cast<const uint8_t*>(&rejection), sizeof(rejection), &reject_channel,
                                   GENERATION);
        wki_quiesce_claimed_op(&rejected_wait);
        retained &= rejected_wait.state.load() == WkiWaitEntry::DONE && old_image.wki_execution_identity().remote_pid == (exec ? 7 : 0);
        s_compute_lock.lock();
        retained &= !row->response_pending.load() && row->accept_status == rejection.status;
        remove_submitted_task_for_selftest_locked(REJECT_ID);
        s_compute_lock.unlock();
        if (!retained || old_image.ref_count.load() != 1) return false;
    }
    Task source{}, receiver{};
    WkiChannel channel{};
    WkiWaitEntry ready_wait{}, accepted_wait{};
    SubmittedTask submitted;
    submitted.active = true;
    submitted.exec_handoff = true;
    submitted.target_node = NODE;
    submitted.submit_channel = &channel;
    submitted.submit_channel_generation = GENERATION;
    submitted.response_wait_entry = &ready_wait;
    submitted.response_consumer_wait_entry = &ready_wait;
    submitted.response_pending.store(true);
    if (!submitted.set_local_task_ref(&source)) return false;
    s_compute_lock.lock();
    submitted.task_id = allocate_submitted_task_id_locked();
    uint32_t const ID = submitted.task_id;
    auto* stored = publish_submitted_task_locked(std::move(submitted));
    s_compute_lock.unlock();
    if (stored == nullptr) return false;
    WkiHeader header{};
    header.src_node = NODE;
    TaskExecReady ready{.task_id = ID, .remote_pid = 77};
    auto receive_ready = [&](uint16_t length = sizeof(TaskExecReady), uint32_t generation = GENERATION) {
        detail::handle_task_exec_ready(&header, reinterpret_cast<const uint8_t*>(&ready), length, &channel, generation);
    };
    receive_ready(sizeof(ready) - 1);
    receive_ready(sizeof(ready), GENERATION - 1);
    ready.version = 2;
    receive_ready();
    ready.version = WKI_EXEC_HANDOFF_VERSION;
    ready.reserved = 1;
    receive_ready();
    ready.reserved = 0;
    ready.remote_pid = 0;
    receive_ready();
    ready.remote_pid = 77;
    header.src_node = NODE + 1;
    receive_ready();
    header.src_node = NODE;
    TaskResponsePayload accept{.task_id = ID, .remote_pid = 77};
    detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&accept), sizeof(accept), &channel, GENERATION);
    TaskCompletePayload complete{.task_id = ID};
    detail::handle_task_complete(&header, reinterpret_cast<const uint8_t*>(&complete), sizeof(complete), &channel, GENERATION);
    bool ok = ready_wait.state.load() == WkiWaitEntry::PENDING && !stored->exec_prepared && stored->active &&
              source.wki_execution_identity().signal_task_id == 0;
    receive_ready();
    wki_quiesce_claimed_op(&ready_wait);
    ok &= stored->exec_prepared && stored->exec_remote_pid == 77 && !stored->exec_cutover &&
          source.wki_execution_identity().signal_task_id == 0;
    ready.remote_pid = 88;
    receive_ready();
    ok &= stored->exec_remote_pid == 77;

    s_compute_lock.lock();
    stored->exec_cutover = true;
    stored->response_wait_entry = &accepted_wait;
    stored->response_consumer_wait_entry = &accepted_wait;
    stored->response_pending.store(true);
    s_compute_lock.unlock();
    accept.remote_pid = 88;
    detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&accept), sizeof(accept), &channel, GENERATION);
    ok &= accepted_wait.state.load() == WkiWaitEntry::PENDING && source.wki_execution_identity().signal_task_id == 0;
    accept.remote_pid = 77;
    accept.reserved[0] = WKI_TASK_SIGNAL_VERSION;
    detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&accept), sizeof(accept), &channel, GENERATION);
    wki_quiesce_claimed_op(&accepted_wait);
    ok &= source.wki_execution_identity().signal_task_id == ID && source.wki_execution_identity().remote_pid == 77;

    s_compute_lock.lock();
    ComputeSubmitPeerSession saved{};
    if (auto* previous = find_compute_submit_peer_session_locked(NODE); previous != nullptr) saved = *previous;
    auto* peer = get_or_create_compute_submit_peer_session_locked(NODE);
    bool published = false;
    if (peer != nullptr) {
        peer->open = true;
        peer->retiring = false;
        peer->current = {.node_id = NODE, .rx_channel = &channel, .rx_channel_generation = GENERATION, .epoch = 71};
        RunningRemoteTask running;
        running.active = true;
        running.exec_handoff = true;
        running.task_id = ID;
        running.submitter_node = NODE;
        running.local_pid = 77;
        running.submit_session_epoch = 71;
        running.submit_rx_channel = &channel;
        running.submit_rx_channel_generation = GENERATION;
        running.task = &receiver;
        published = publish_running_task_locked(running);
    }
    s_compute_lock.unlock();
    ok &= published;
    if (published) {
        TaskExecCommit commit{.identity = {.task_id = ID, .remote_pid = 77}, .signals = {.pending = uint64_t{1} << 11}};
        auto receive_commit = [&](uint16_t length = sizeof(TaskExecCommit), uint32_t generation = GENERATION) {
            detail::handle_task_exec_commit(&header, reinterpret_cast<const uint8_t*>(&commit), length, &channel, generation);
        };
        auto committed = [&]() {
            bool value = false;
            s_compute_lock.lock();
            for (auto const& row : g_running_remote_tasks)
                if (row.active && row.task == &receiver) value = row.exec_committed;
            s_compute_lock.unlock();
            return value;
        };
        receive_commit(sizeof(commit) - 1);
        receive_commit(sizeof(commit), GENERATION - 1);
        commit.identity.remote_pid = 78;
        receive_commit();
        commit.identity.remote_pid = 77;
        commit.signals.blocked = WKI_EXEC_UNBLOCKABLE_SIGNALS;
        receive_commit();
        commit.signals.blocked = 0;
        s_compute_lock.lock();
        peer->retiring = true;
        s_compute_lock.unlock();
        receive_commit();
        s_compute_lock.lock();
        peer->retiring = false;
        s_compute_lock.unlock();
        ok &= !committed();
        receive_commit();
        ok &= committed() && !receiver.scheduler_published.load() && receiver.signal_pending_bits() == 0;
        commit.signals.pending = uint64_t{1} << 12;
        receive_commit();
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks) {
            if (row.active && row.task == &receiver) {
                ok &= row.exec_signals.pending == (uint64_t{1} << 11) && !row.published;
                row.task = nullptr;
                row.active = false;
            }
        }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();
    }
    s_compute_lock.lock();
    if (peer != nullptr) *peer = saved;
    stored->response_wait_entry = nullptr;
    stored->response_consumer_wait_entry = nullptr;
    stored->response_pending.store(false);
    remove_submitted_task_for_selftest_locked(ID);
    s_compute_lock.unlock();
    return ok && source.ref_count.load() == 1 && receiver.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_accepted_signal_route() -> bool {
    using ker::mod::sched::task::Task;
    constexpr uint16_t NODE = 0x7B61;
    bool ok = true;
    for (unsigned mode = 0; mode < 4; ++mode) {
        Task subject{};
        subject.type = ker::mod::sched::task::TaskType::PROCESS;
        subject.pid = 0x7B610001;
        WkiChannel channel{};
        WkiWaitEntry wait{};
        SubmittedTask row;
        row.active = true;
        row.target_node = NODE;
        row.submit_channel = &channel;
        row.submit_channel_generation = 41;
        row.response_pending.store(true);
        row.response_wait_entry = &wait;
        row.response_consumer_wait_entry = &wait;
        if (!row.set_local_task_ref(&subject)) return false;
        s_compute_lock.lock();
        row.task_id = allocate_submitted_task_id_locked();
        uint32_t const ID = row.task_id;
        auto* stored = publish_submitted_task_locked(std::move(row));
        s_compute_lock.unlock();
        if (stored == nullptr) return false;
        WkiHeader header{};
        header.src_node = NODE;
        TaskResponsePayload response{};
        response.task_id = ID;
        response.remote_pid = 77;
        response.reserved[0] = WKI_TASK_SIGNAL_VERSION;
        auto accept = [&](uint16_t length, uint32_t generation) {
            detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&response), length, &channel, generation);
        };
        accept(sizeof(response) - 1, 41);
        accept(sizeof(response), 40);
        header.src_node = NODE + 1;
        accept(sizeof(response), 41);
        header.src_node = NODE;
        response.task_id = 0;
        accept(sizeof(response), 41);
        response.task_id = ID;
        ok &= subject.wki_execution_identity().signal_task_id == 0 && wait.state.load() == WkiWaitEntry::PENDING;
        if (mode == 0) {
            accept(sizeof(response), 41);
        } else if (mode == 1) {
            TaskCompletePayload complete{};
            complete.task_id = ID;
            detail::handle_task_complete(&header, reinterpret_cast<const uint8_t*>(&complete), sizeof(complete), &channel, 41);
        } else if (mode == 2) {
            response.status = static_cast<uint8_t>(TaskRejectReason::OVERLOADED);
            detail::handle_task_reject(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), &channel, 41);
        } else {
            wki_wake_op(&wait, WKI_ERR_TIMEOUT);
            accept(sizeof(response), 41);  // A lost waiter claim cannot publish.
        }
        auto const ROUTE = subject.wki_execution_identity();
        ok &= ROUTE.signal_task_id == (mode < 2 ? ID : 0) && ROUTE.proxy_task_id == 0 && !subject.wki_proxy_task &&
              subject.sched_queue == Task::sched_queue::NONE && subject.signal_pending_bits() == 0;
        if (mode == 0) ok &= ROUTE.remote_pid == 77;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            auto const ADMISSION = subject.signal_admission_identity();
            ok &= ADMISSION.remote == (mode < 2) && ADMISSION.credentials == subject.credentials() &&
                  ADMISSION.job.pgid == subject.job_identity().pgid;
        }
        response.remote_pid = 88;
        accept(sizeof(response), 41);
        ok &= subject.wki_execution_identity().remote_pid == ROUTE.remote_pid;
        wki_quiesce_claimed_op(&wait);
        s_compute_lock.lock();
        stored->active = false;
        stored->response_pending.store(false);
        stored->response_wait_entry = nullptr;
        stored->response_consumer_wait_entry = nullptr;
        auto* output = consume_submitted_task_result_locked(stored);
        s_compute_lock.unlock();
        delete[] output;
        if (mode < 2) {
            subject.set_wki_proxy_task_id(0);
            ok &= subject.wki_execution_identity().signal_task_id == ID;
            auto const RESULT = wki_proxy_task_signal_as(subject, subject.credentials(), subject.job_identity(), 0);
            ok &= RESULT.has_value() && *RESULT == -ESRCH;
        }
        ok &= subject.ref_count.load() == 1 && subject.signal_pending_bits() == 0;
    }
    return ok;
}

auto wki_remote_compute_selftest_user_signal_ownership() -> bool {
    using ker::mod::sched::task::Task;
    Task owner{};
    WkiChannel channel{};
    PendingUserSignal* slot = nullptr;
    s_compute_lock.lock();
    for (auto& candidate : s_user_signals) {
        if (candidate.owner == nullptr) {
            slot = &candidate;
            break;
        }
    }
    s_compute_lock.unlock();
    if (slot == nullptr) return false;
    constexpr uint16_t NODE = 0x7B31;
    bool ok = true;
    for (unsigned mode = 0; mode < 3; ++mode) {
        WkiWaitEntry wait{};
        s_compute_lock.lock();
        *slot = {.owner = &owner,
                 .response_wait = &wait,
                 .consumer_wait = &wait,
                 .channel = &channel,
                 .request_id = 0xABCDEF,
                 .task_id = 99,
                 .generation = 27,
                 .node = NODE};
        s_compute_lock.unlock();
        WkiHeader hdr{};
        hdr.src_node = NODE;
        TaskSignalResponse response{.request_id = 0xABCDEF, .task_id = 99, .result = -EPERM};
        auto deliver = [&](uint32_t generation) {
            detail::handle_task_signal_response(&hdr, reinterpret_cast<const uint8_t*>(&response), sizeof(response), &channel, generation);
        };
        deliver(26);
        hdr.src_node = NODE + 1;
        deliver(27);
        hdr.src_node = NODE;
        ++response.request_id;
        deliver(27);
        --response.request_id;
        response.result = 1;
        deliver(27);
        response.result = -EPERM;
        ok &= wait.state.load() == WkiWaitEntry::PENDING && slot->response_wait == &wait;
        if (mode == 0) {
            deliver(27);
            ok &= wait.state.load() == WkiWaitEntry::DONE && wait.result == WKI_OK && slot->result == -EPERM;
            response.result = 0;
            deliver(27);
            ok &= slot->result == -EPERM;
        } else if (mode == 1) {
            fail_user_signals_for_peer(NODE, RemoteComputeCleanupScope::ALL);
            ok &= wait.state.load() == WkiWaitEntry::DONE && wait.result == WKI_ERR_PEER_FENCED;
        } else {
            wki_wake_op(&wait, WKI_ERR_TIMEOUT);
            deliver(27);
            ok &= wait.result == WKI_ERR_TIMEOUT && slot->result == -EIO;
        }
        ok &= slot->consumer_wait == &wait;
        retire_user_signals_for_task(&owner);
        ok &= slot->owner == nullptr && slot->consumer_wait == nullptr;
        deliver(27);
        ok &= slot->owner == nullptr;
    }
    // The actual admission helper consumes exactly one lookup reference and
    // leaves every signal/ptrace bit unchanged on denial (including signal0).
    Task target{};
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.pid = 0x7B310001;
    target.initialize_job_identity({.session_id = 41, .origin = {.node = NODE, .boot = 7}});
    ker::mod::security::Snapshot caller;
    ok &= ker::mod::security::Snapshot::bootstrap_root().prepare({.change = ker::mod::security::Change::SET_UID, .real = 100}, caller) == 0;
    auto call = [&](int sig) {
        if (!target.try_acquire()) return -ESRCH;
        return ker::syscall::process::signal_as(target, caller, {.session_id = 41}, sig);
    };
    auto const PENDING = target.signal_pending_bits();
    ok &= call(0) == -EPERM && call(10) == -EPERM && call(18) == -EPERM;
    ok &= target.signal_pending_bits() == PENDING && target.ref_count.load() == 1;
    ok &= ker::mod::sched::resource::initialize_credentials(target, caller) == 0;
    ok &= call(0) == 0 && target.signal_pending_bits() == PENDING && target.ref_count.load() == 1;
    ok &= valid_session_origin({0, 0, 0}) && valid_session_origin({NODE, 0, 7}) && !valid_session_origin({NODE, 0, 0}) &&
          !valid_session_origin({0, 0, 7}) && !valid_session_origin({NODE, 1, 7});
    auto const LOCAL = ker::mod::sched::session::Identity{.session_id = 41};
    auto const FOREIGN = ker::mod::sched::session::Identity{.session_id = 41, .origin = {.node = NODE, .boot = 7}};
    ok &= !ker::mod::sched::session::same_session(LOCAL, FOREIGN) && ker::mod::sched::session::same_session(FOREIGN, FOREIGN);
    return ok;
}
auto wki_remote_compute_selftest_child_cpu_observations() -> bool {
    using Task = ker::mod::sched::task::Task;
    using State = ker::mod::sched::task::TaskState;
    auto tasks = std::make_unique<Task[]>(5);
    auto& parent = tasks[0];
    auto& source = tasks[1];
    auto& receiver = tasks[2];
    auto& onward = tasks[3];
    auto& child = tasks[4];
    for (size_t i = 0; i < 5; ++i) {
        tasks[i].pid = i == 0 ? 1 : 0x7B830000 + i;
        tasks[i].type = ker::mod::sched::task::TaskType::PROCESS;
        tasks[i].state.store(State::EXITING);
    }
    bool ok = true;
    auto check = [&](bool value, int line = __builtin_LINE()) {
        if (!value) ker::mod::dbg::logger<"wki">::error("Child CPU observation check failed at line %d", line);
        ok &= value;
    };
    child_events::add_reaped_cpu_times(source, 11, 13);
    child_events::import_reaped_cpu_times(source, 17, 19);
    TaskResourceSnapshot baseline;
    check(snapshot_task_resources(&source, baseline) && baseline.reaped_user_time_us == 0 && baseline.reaped_system_time_us == 0);
    check(snapshot_task_resources(&source, baseline, true) && baseline.reaped_user_time_us == 28 && baseline.reaped_system_time_us == 32 &&
          (baseline.flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0);

    // Actual resource-suffix decoder: missing negotiation/exec context cannot
    // silently discard the child observation, and failed decoding preserves it.
    std::array<uint8_t, 5 + WKI_TASK_RESOURCE_IDENTITY_SIZE> bytes{};
    uint32_t const IMAGE_LENGTH = 1;
    std::memcpy(bytes.data(), &IMAGE_LENGTH, sizeof(IMAGE_LENGTH));
    std::memcpy(bytes.data() + 5 + sizeof(WkiTaskIdentityContext), &baseline, sizeof(baseline));
    TaskSubmitPayload submit{};
    submit.delivery_mode = static_cast<uint8_t>(TaskDeliveryMode::INLINE);
    submit.identity_len = WKI_TASK_RESOURCE_IDENTITY_SIZE;
    const uint8_t* context = nullptr;
    uint16_t context_len{};
    WkiTaskIdentityContext identity{};
    bool has_identity{};
    TaskChildCpuObservation observation{31, 37};
    auto prepare = [&](TaskChildCpuObservation* output) {
        return prepare_submitted_resources(&submit, bytes.data(), bytes.size(), context, context_len, identity, has_identity,
                                           receiver.resources, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, output);
    };
    check(prepare(&observation) == TaskRejectReason::RESOURCE_INVALID && observation.user_time_us == 31);
    submit.reserved = WKI_TASK_SUBMIT_EXEC_HANDOFF;
    check(prepare(nullptr) == TaskRejectReason::RESOURCE_INVALID);
    check(prepare(&observation) == TaskRejectReason::ACCEPTED && observation.user_time_us == 28 && observation.system_time_us == 32);
    child_events::import_reaped_cpu_times(receiver, observation.user_time_us, observation.system_time_us);
    check(receiver.child_user_time_us == 0 && receiver.child_system_time_us == 0);

    // A remotely observed reap is charged at its real queue, then observed at
    // the receiver. This simulates the future initiating client's one receipt.
    child.resources.cpu_user_time_us = 5;
    child.resources.cpu_system_time_us = 7;
    child.resources.cpu_time_us = 12;
    check(child_events::begin_publication(source, child) && child_events::commit_publication(child));
    child.has_exited = true;
    child.exit_notify_ready.store(true);
    check(child_events::publish_exit(child));
    source.state.store(State::ACTIVE);
    child_events::Claim claim;
    child_events::ClaimedEvent event;
    check(child_events::try_claim(source, claim, child.pid, 0, event) == child_events::ProbeResult::CLAIMED);
    check(child_events::commit_claim(source, claim, event, false));
    child_events::import_reaped_cpu_times(receiver, event.user_time_us, event.system_time_us);
    child_events::add_reaped_cpu_times(receiver, 3, 4);
    TaskResourceSnapshot onward_baseline;
    check(snapshot_task_resources(&receiver, onward_baseline, true) && onward_baseline.reaped_user_time_us == 36 &&
          onward_baseline.reaped_system_time_us == 43);
    TaskChildCpuObservation onward_observation;
    check(resource_contract::restore(onward_baseline, onward.resources, Task::FD_TABLE_SIZE, &onward_observation));
    child_events::import_reaped_cpu_times(onward, onward_observation.user_time_us, onward_observation.system_time_us);
    child_events::add_reaped_cpu_times(onward, 7, 11);
    auto completion = resource_contract::snapshot(onward.resources);
    completion.reaped_user_time_us = onward.child_user_time_us;
    completion.reaped_system_time_us = onward.child_system_time_us;
    check(merge_proxy_resource_completion(receiver, onward_baseline, completion));
    completion = resource_contract::snapshot(receiver.resources);
    completion.reaped_user_time_us = receiver.child_user_time_us;
    completion.reaped_system_time_us = receiver.child_system_time_us;
    check(completion.reaped_user_time_us == 10 && completion.reaped_system_time_us == 15);
    check(merge_proxy_resource_completion(source, baseline, completion));
    for (auto* task : {&source, &receiver, &onward}) {
        auto const TOTAL = child_events::read_reaped_cpu_times(*task);
        check(TOTAL.user_time_us == 43 && TOTAL.system_time_us == 54);
    }
    check(source.child_user_time_us == 26 && source.imported_child_user_time_us == 17);

    // User-visible observations and immutable EXIT rusage include both parts;
    // WNOWAIT does not charge either part to the parent.
    ker::mod::sched::resource::CpuObservation visible{};
    check(ker::mod::sched::resource::copy_cpu_observation(source, visible) && visible.child_user_time_us == 43 &&
          visible.child_system_time_us == 54);
    source.resources.cpu_user_time_us = 100;
    source.resources.cpu_system_time_us = 200;
    source.resources.cpu_time_us = 300;
    source.state.store(State::EXITING);
    check(child_events::begin_publication(parent, source) && child_events::commit_publication(source));
    source.has_exited = true;
    source.exit_notify_ready.store(true);
    check(child_events::publish_exit(source));
    parent.state.store(State::ACTIVE);
    check(child_events::try_claim(parent, claim, source.pid, 0, event) == child_events::ProbeResult::CLAIMED && event.user_time_us == 143 &&
          event.system_time_us == 254);
    check(child_events::commit_claim(parent, claim, event, true) && parent.child_user_time_us == 0);
    check(child_events::try_claim(parent, claim, source.pid, 0, event) == child_events::ProbeResult::CLAIMED);
    check(child_events::commit_claim(parent, claim, event, false) && parent.child_user_time_us == 143 &&
          parent.child_system_time_us == 254);
    child_events::import_reaped_cpu_times(onward, UINT64_MAX, UINT64_MAX);
    auto const SATURATED = child_events::read_reaped_cpu_times(onward);
    check(SATURATED.user_time_us == UINT64_MAX && SATURATED.system_time_us == UINT64_MAX && onward.child_user_time_us == 7);
    parent.state.store(State::EXITING);
    child_events::prepare_process_exit(parent);
    child_events::prepare_process_exit(source);
    for (size_t i = 0; i < 5; ++i) check(tasks[i].ref_count.load() == 1);
    return ok;
}

auto wki_remote_compute_selftest_child_query_wire() -> bool {
    using Operation = child_query::Operation;
    ChildQueryRequest request{.sequence = 19,
                              .task_id = 23,
                              .receiver_pid = 29,
                              .spec = {.selector = 31, .caller_origin = {37, 41}},
                              .remaining_hops = 4,
                              .lane = 7};
    child_query::Action action{.query = 19, .generation = 1, .operation = Operation::INSPECT};
    auto const WIRE = encode_child_query_request(request, action);
    ChildQueryRequest decoded{};
    bool ok = decode_child_query_request(WIRE, decoded) && decoded == request;
    auto check = [&](bool value, int line = __builtin_LINE()) {
        if (!value) ker::mod::dbg::logger<"wki">::error("Child-query wire check failed at line %d", line);
        ok &= value;
    };
    uint16_t copied = 0;
    check(classify_compute_rx_payload(MsgType::TASK_CHILD_QUERY_REQ, reinterpret_cast<const uint8_t*>(&WIRE), sizeof(WIRE), &copied) ==
              WkiComputeRxAdmission::DEFERRED &&
          copied == sizeof(WIRE));
    for (uint16_t length : {uint16_t{0}, uint16_t{sizeof(WIRE) - 1}, uint16_t{sizeof(WIRE) + 1}})
        check(classify_compute_rx_payload(MsgType::TASK_CHILD_QUERY_REQ, reinterpret_cast<const uint8_t*>(&WIRE), length, &copied) ==
              WkiComputeRxAdmission::DISCARD);
    for (unsigned field = 0; field < 16; ++field) {
        auto bad = WIRE;
        switch (field) {
            case 0:
                ++bad.version;
                break;
            case 1:
                bad.reserved = 1;
                break;
            case 2:
                bad.sequence = 0;
                break;
            case 3:
                bad.receiver_pid = 0;
                break;
            case 4:
                bad.task_id = 0;
                break;
            case 5:
                bad.generation = 0;
                break;
            case 6:
                bad.generation = 5;
                break;
            case 7:
                bad.operation = 4;
                break;
            case 8:
                bad.flags = 4;
                break;
            case 9:
                bad.lane = 64;
                break;
            case 10:
                bad.hops = 65;
                break;
            case 11:
                bad.caller_origin.boot = 0;
                break;
            case 12:
                bad.caller_origin.node = WKI_NODE_BROADCAST;
                break;
            case 13:
                bad.selector_origin.reserved = 1;
                break;
            case 14:
                bad.options = 0x40000000;
                break;
            case 15:
                bad.selected_group = {.node = 2, .boot = 3};
                break;
        }
        check(!decode_child_query_request(bad, decoded));
    }
    ChildQueryReply reply{.ready = true,
                          .outcome = {.summary = {.pids = {.found = true, .origin = {43, 47}}, .potential = true, .ready_depth = 2}},
                          .version = {53, 59}};
    auto const RESPONSE = encode_child_query_response(WIRE, reply);
    ChildQueryReply result{};
    check(decode_child_query_response(RESPONSE, decoded, result) && result.ready && result.version == reply.version &&
          result.outcome.summary.ready_depth == 2 && result.outcome.summary.pids.origin == reply.outcome.summary.pids.origin);
    check(classify_compute_rx_payload(MsgType::TASK_CHILD_QUERY_RESP, reinterpret_cast<const uint8_t*>(&RESPONSE), sizeof(RESPONSE),
                                      &copied) == WkiComputeRxAdmission::DEFERRED &&
          copied == sizeof(RESPONSE));
    for (unsigned field = 0; field < 12; ++field) {
        auto bad = RESPONSE;
        switch (field) {
            case 0:
                bad.ready = 2;
                break;
            case 1:
                bad.potential = 2;
                break;
            case 2:
                bad.error = 1;
                break;
            case 3:
                bad.error = -4096;
                break;
            case 4:
                bad.queue_version = 0;
                break;
            case 5:
                bad.group_version = UINT64_MAX;
                break;
            case 6:
                bad.ready_depth = 5;
                break;
            case 7:
                bad.pid_flags = 2;
                break;
            case 8:
                bad.pid_origin.boot = 0;
                break;
            case 9:
                bad.group_flags = 1;
                break;
            case 10:
                bad.ready = 0;
                break;
            case 11:
                bad.reserved = 1;
                break;
        }
        check(!decode_child_query_response(bad, decoded, result));
    }
    auto pending = encode_child_query_response(WIRE, {});
    check(decode_child_query_response(pending, decoded, result) && !result.ready && !result.outcome.summary.potential);
    auto error = encode_child_query_response(WIRE, child_query_error(-EOVERFLOW));
    check(decode_child_query_response(error, decoded, result) && result.ready && result.outcome.error == -EOVERFLOW);
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    check(auth_policy::required_permissions(MsgType::TASK_CHILD_QUERY_REQ, &WIRE, sizeof(WIRE)) == REQUIRED);
    check(auth_policy::required_permissions(MsgType::TASK_CHILD_QUERY_RESP, &RESPONSE, sizeof(RESPONSE)) == REQUIRED);
    request.spec = {.selector = child_events::selector_for_process_group(77),
                    .group_alias = true,
                    .caller_origin = {37, 41},
                    .own_group = child_query::Origin{43, 47}};
    action.operation = Operation::PROBE;
    action.generation = 2;
    action.group = {43, 47};
    auto group = encode_child_query_request(request, action);
    check(decode_child_query_request(group, decoded) && decoded == request);
    action.operation = Operation::CLOSE;
    action.generation = 3;
    auto close = encode_child_query_request(request, action);
    check(decode_child_query_request(close, decoded) && child_query_local_origin(close.selected_group).local());
    auto closed = encode_child_query_response(close, child_query_error(0));
    check(decode_child_query_response(closed, decoded, result) && result.ready && result.version == child_events::QueueVersion{});
    return ok;
}

auto wki_remote_compute_selftest_child_claim_wire() -> bool {
    ChildClaimRequest const REQUEST{.sequence = 19, .task_id = 23, .receiver_pid = 29, .depth = 4, .lane = 7};
    auto const WIRE = encode_child_claim_request(REQUEST, {.query = 19, .generation = 1});
    ChildClaimRequest decoded{};
    bool ok = decode_child_claim_request(WIRE, decoded) && decoded == REQUEST;
    auto check = [&](bool value, int line = __builtin_LINE()) {
        if (!value) ker::mod::dbg::logger<"wki">::error("Child-claim wire check failed at line %d", line);
        ok &= value;
    };
    uint16_t copied{};
    check(classify_compute_rx_payload(MsgType::TASK_CHILD_CLAIM_REQ, reinterpret_cast<const uint8_t*>(&WIRE), sizeof(WIRE), &copied) ==
              WkiComputeRxAdmission::DEFERRED &&
          copied == sizeof(WIRE));
    check(classify_compute_rx_payload(MsgType::TASK_CHILD_CLAIM_REQ, reinterpret_cast<const uint8_t*>(&WIRE), sizeof(WIRE), nullptr) ==
          WkiComputeRxAdmission::DISCARD);
    for (uint16_t length : {uint16_t{0}, uint16_t{sizeof(WIRE) - 1}, uint16_t{sizeof(WIRE) + 1}})
        check(classify_compute_rx_payload(MsgType::TASK_CHILD_CLAIM_REQ, reinterpret_cast<const uint8_t*>(&WIRE), length, &copied) ==
              WkiComputeRxAdmission::DISCARD);
    for (unsigned field = 0; field < 11; ++field) {
        auto bad = WIRE;
        switch (field) {
            case 0:
                ++bad.version;
                break;
            case 1:
                bad.reserved = 1;
                break;
            case 2:
                bad.reserved2 = 1;
                break;
            case 3:
                bad.sequence = 0;
                break;
            case 4:
                bad.receiver_pid = 0;
                break;
            case 5:
                bad.task_id = 0;
                break;
            case 6:
                bad.generation = 0;
                break;
            case 7:
                bad.generation = 4;
                break;
            case 8:
                bad.command = static_cast<uint8_t>(ChildWaitCommand::ACK);
                break;
            case 9:
                bad.lane = 64;
                break;
            case 10:
                bad.depth = 65;
                break;
        }
        check(!decode_child_claim_request(bad, decoded));
    }
    ChildWaitReply reply{
        .phase = ChildWaitPhase::CLAIMED,
        .probe = child_events::ProbeResult::CLAIMED,
        .value = {.pid = 31, .process_group = 37, .group_origin = {41, 43}, .user_time_us = 47, .system_time_us = 53, .status = 37 << 8}};
    auto const RESPONSE = encode_child_claim_response(WIRE, reply);
    ChildWaitReply result{};
    check(decode_child_claim_response(RESPONSE, decoded, result) && result.value == reply.value && result.phase == reply.phase);
    check(classify_compute_rx_payload(MsgType::TASK_CHILD_CLAIM_RESP, reinterpret_cast<const uint8_t*>(&RESPONSE), sizeof(RESPONSE),
                                      &copied) == WkiComputeRxAdmission::DEFERRED &&
          copied == sizeof(RESPONSE));
    for (unsigned field = 0; field < 15; ++field) {
        auto bad = RESPONSE;
        switch (field) {
            case 0:
                bad.phase = 3;
                break;
            case 1:
                bad.probe = 3;
                break;
            case 2:
                bad.committed = 2;
                break;
            case 3:
                bad.error = 1;
                break;
            case 4:
                bad.error = -4096;
                break;
            case 5:
                bad.reserved = 1;
                break;
            case 6:
                bad.reserved2 = 1;
                break;
            case 7:
                bad.pid = 0;
                break;
            case 8:
                bad.process_group = 0;
                break;
            case 9:
                bad.group_origin.boot = 0;
                break;
            case 10:
                bad.group_origin.reserved = 1;
                break;
            case 11:
                bad.kind = 4;
                break;
            case 12:
                bad.audience = 2;
                break;
            case 13:
                bad.phase = static_cast<uint8_t>(ChildWaitPhase::PENDING);
                break;
            case 14:
                bad.phase = static_cast<uint8_t>(ChildWaitPhase::DONE);
                break;
        }
        check(!decode_child_claim_response(bad, decoded, result));
    }
    reply.phase = ChildWaitPhase::DONE;
    reply.committed = true;
    check(decode_child_claim_response(encode_child_claim_response(WIRE, reply), decoded, result) && result.committed);
    reply.committed = false;
    reply.error = -ECANCELED;
    check(decode_child_claim_response(encode_child_claim_response(WIRE, reply), decoded, result) && result.error == -ECANCELED);
    for (int error : {0, -EAGAIN, -EOVERFLOW, -ESTALE})
        check(decode_child_claim_response(encode_child_claim_response(WIRE, child_wait_error(error)), decoded, result));
    auto pending = encode_child_claim_response(WIRE, {});
    check(decode_child_claim_response(pending, decoded, result) && result.phase == ChildWaitPhase::PENDING);
    pending.user_time_us = 1;
    check(!decode_child_claim_response(pending, decoded, result));
    constexpr uint64_t REQUIRED = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
    check(auth_policy::required_permissions(MsgType::TASK_CHILD_CLAIM_REQ, &WIRE, sizeof(WIRE)) == REQUIRED);
    check(auth_policy::required_permissions(MsgType::TASK_CHILD_CLAIM_RESP, &RESPONSE, sizeof(RESPONSE)) == REQUIRED);
    return ok;
}

auto wki_remote_compute_selftest_native_child_wait() -> bool {
    using Task = ker::mod::sched::task::Task;
    using State = ker::mod::sched::task::TaskState;
    using Operation = child_query::Operation;
    constexpr uint16_t LOCAL = 0x7B84, UP = 0x7B83;
    uint16_t const SAVED_NODE = g_wki.my_node_id;
    uint32_t const SAVED_BOOT = g_wki.local_boot_epoch;
    g_wki.my_node_id = LOCAL;
    g_wki.local_boot_epoch = 53;
    auto tasks = std::make_unique<Task[]>(3);
    auto& owner = tasks[0];
    auto& child = tasks[1];
    auto& waiter = tasks[2];
    owner.pid = 1;
    child.pid = 0x7B840123;
    waiter.pid = 0x7B840124;
    for (auto& task : std::span(tasks.get(), 3)) {
        task.type = ker::mod::sched::task::TaskType::PROCESS;
        task.state.store(State::EXITING);
    }
    child_events::update_process_group(child, 77);
    child.exit_status = 37 << 8;
    child.resources.cpu_user_time_us = 17;
    bool ok = child_events::begin_publication(owner, child) && child_events::commit_publication(child);
    auto check = [&](bool value, int line = __builtin_LINE()) {
        if (!value) ker::mod::dbg::logger<"wki">::error("Native child-wait check failed at line %d", line);
        ok &= value;
    };
    child.has_exited = true;
    child.exit_notify_ready.store(true);
    check(child_events::publish_exit(child));
    owner.state.store(State::ACTIVE);
    waiter.state.store(State::ACTIVE);
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = UP;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 59;
    WkiPeer peer{};
    peer.node_id = UP;
    ComputePeerIdentity identity{.boot_epoch = 61,
                                 .auth_generation = 7,
                                 .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                                 .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    s_compute_lock.lock();
    auto const SESSION = capture_compute_submit_session_locked(UP, &channel, channel.generation, identity);
    RunningRemoteTask running{};
    running.active = running.exec_handoff = running.published = running.exec_committed = true;
    running.task_id = 0x7B8301;
    running.submitter_node = UP;
    running.local_pid = owner.pid;
    running.submit_session_epoch = SESSION.epoch;
    running.submit_rx_channel = &channel;
    running.submit_rx_channel_generation = channel.generation;
    running.task = &owner;
    bool const PIN = owner.try_acquire_lifetime_ref();
    bool const INSERTED = PIN && SESSION.epoch != 0 && publish_running_task_locked(running);
    s_compute_lock.unlock();
    check(INSERTED);
    PeerLifecycleLease lease;
    auto answer_query = [&](const ChildQueryWork& work, child_query::Summary summary) {
        check(lease.acquire(&peer));
        auto wire = encode_child_query_request(work.request, work.action);
        check(receive_child_query_response(
            lease, SESSION, encode_child_query_response(wire, {.ready = true, .outcome = {.summary = summary}, .version = {11, 13}})));
        lease.release();
    };
    auto answer_claim = [&](const ChildClaimWork& work, ChildWaitReply reply) {
        check(lease.acquire(&peer));
        auto wire = encode_child_claim_request(work.request, work.action);
        auto response = encode_child_claim_response(wire, reply);
        check(receive_child_claim_response(lease, SESSION, response));
        check(!receive_child_claim_response(lease, SESSION, response));
        lease.release();
    };
    auto close = [&](uint64_t handle) {
        wki_child_wait_close(waiter, handle);
        for (unsigned i = 0; i < 8; ++i) {
            static_cast<void>(run_one_native_child_wait(false));
            if (auto work = take_child_query_work()) {
                check(work->action.operation == Operation::CLOSE);
                answer_query(*work, {});
            }
            static_cast<void>(run_one_child_query_retirement());
        }
        check(wki_child_wait_poll(waiter, handle).error == -ECANCELED);
        check(waiter.ref_count.load() == 1);
    };
    if (INSERTED) {
        // Collision; local WNOWAIT; abandoned local claim; remote WNOWAIT;
        // detached remote commit; then wait-any can reap the colliding local PID.
        for (unsigned scenario = 0; scenario < 6; ++scenario) {
            bool const REMOTE = scenario == 3 || scenario == 4;
            int64_t const PID = scenario == 5 ? -1 : static_cast<int64_t>(child.pid + (REMOTE ? 1 : 0));
            int32_t const OPTIONS = scenario == 1 || scenario == 3 ? child_events::OPTION_NOWAIT : 0;
            uint64_t handle = 0;
            auto result = wki_child_wait_begin(owner, waiter, PID, OPTIONS, handle);
            check(result.state == WkiChildWaitState::PENDING && handle != 0 && waiter.ref_count.load() == 2);
            child_query::Summary summary{};
            if (scenario == 0 || REMOTE || scenario == 5) {
                summary.potential = true;
                summary.ready_depth = 0;
                if (scenario != 5) summary.pids = {.found = true, .origin = {UP, 61}};
            }
            for (unsigned step = 0; step < 16; ++step) {
                static_cast<void>(run_one_native_child_wait(false));
                if (auto work = take_child_query_work()) {
                    check(work->action.operation == Operation::INSPECT || work->action.operation == Operation::VALIDATE);
                    answer_query(*work, summary);
                }
                result = wki_child_wait_poll(waiter, handle);
                if (result.state != WkiChildWaitState::PENDING ||
                    (REMOTE && s_child_queries.end() != std::ranges::find_if(s_child_queries, [&](auto const& record) {
                                   return record.active && record.token == handle && record.claim_action.has_value();
                               })))
                    break;
            }
            if (scenario == 0) {
                check(result.state == WkiChildWaitState::ERROR && result.error == -EOVERFLOW);
                check(!take_child_claim_work(1000));
                check(child_events::inspect_queue(owner, child.pid, 0).ready);
                check(child_events::read_reaped_cpu_times(owner).user_time_us == 0);
                close(handle);
                continue;
            }
            ChildWaitReply receipt{.phase = ChildWaitPhase::CLAIMED,
                                   .probe = child_events::ProbeResult::CLAIMED,
                                   .value = {.pid = child.pid + 1,
                                             .process_group = 77,
                                             .group_origin = {UP, 61},
                                             .user_time_us = 29,
                                             .system_time_us = 3,
                                             .status = 37 << 8}};
            if (REMOTE) {
                auto work = take_child_claim_work(1000);
                check(work && work->action.command == ChildWaitCommand::BEGIN && work->request.depth == 0);
                if (work) answer_claim(*work, receipt);
                check(run_one_native_child_wait(false));
                result = wki_child_wait_poll(waiter, handle);
            }
            check(result.state == WkiChildWaitState::CLAIMED && result.pid == static_cast<uint64_t>(REMOTE ? child.pid + 1 : child.pid));
            if (scenario == 2) {
                close(handle);  // models usercopy failure: no commit requested
                check(child_events::inspect_queue(owner, child.pid, 0).ready);
                continue;
            }
            check(wki_child_wait_commit(waiter, handle));
            check(!wki_child_wait_commit(waiter, handle));
            if (REMOTE) {
                auto work = take_child_claim_work(20000);
                check(work && work->action.command == ChildWaitCommand::COMMIT);
                if (scenario == 4) {
                    retire_child_queries_for_task(waiter);
                    check(!wki_child_wait_quiesce(owner));
                    check(waiter.ref_count.load() == 2);
                }
                receipt.phase = ChildWaitPhase::DONE;
                receipt.committed = true;
                if (work) answer_claim(*work, receipt);
                if (scenario == 4) {
                    // A received commit remains authoritative after the local deadline.
                    s_compute_lock.lock();
                    for (auto& record : s_child_queries)
                        if (record.active && record.token == handle) record.native_deadline_us = 0;
                    s_compute_lock.unlock();
                }
            }
            check(run_one_native_child_wait(false));
            if (scenario != 4)
                check(wki_child_wait_poll(waiter, handle).state == WkiChildWaitState::COMPLETE);
            else
                check(wki_child_wait_quiesce(owner));
            close(handle);
            uint64_t const EXPECTED = scenario < 4 ? 0 : scenario == 4 ? 29 : 46;
            check(child_events::read_reaped_cpu_times(owner).user_time_us == EXPECTED);
            check(child_events::inspect_queue(owner, child.pid, 0).ready == (scenario != 5));
        }
        owner.exit_notify_ready.store(true);
        check(!child_events::import_live_reaped_cpu_times(owner, 100, 100));
        check(child_events::read_reaped_cpu_times(owner).user_time_us == 46);
        owner.exit_notify_ready.store(false);
    }
    s_compute_lock.lock();
    retire_compute_submit_session_locked(UP, &channel, channel.generation);
    s_compute_lock.unlock();
    retire_child_queries_for_task(owner);
    while (run_one_native_child_wait(false) || run_one_child_query_retirement()) {
    }
    s_compute_lock.lock();
    for (auto& row : g_running_remote_tasks)
        if (row.task == &owner) {
            row.task = nullptr;
            row.active = false;
        }
    compact_running_remote_tasks_locked();
    if (auto* stored = find_compute_submit_peer_session_locked(UP); stored != nullptr) *stored = {};
    s_compute_lock.unlock();
    if (PIN) owner.release();
    owner.state.store(State::EXITING);
    child_events::prepare_process_exit(owner);
    g_wki.my_node_id = SAVED_NODE;
    g_wki.local_boot_epoch = SAVED_BOOT;
    return ok && owner.ref_count.load() == 1 && child.ref_count.load() == 1 && waiter.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_child_query_ownership() -> bool {
    // KTEST executes during scheduler setup, before WKI init assigns a node.
    // Other CPUs are parked and no compute workers exist yet. Restore both
    // fields so this fixture does not change any later test or boot identity.
    struct LocalIdentityFixture {
        uint16_t saved_node = g_wki.my_node_id;
        uint32_t saved_boot = g_wki.local_boot_epoch;
        LocalIdentityFixture() {
            g_wki.my_node_id = 0x7B82;
            g_wki.local_boot_epoch = 47;
        }
        ~LocalIdentityFixture() {
            g_wki.my_node_id = saved_node;
            g_wki.local_boot_epoch = saved_boot;
        }
    } local_identity;

    using Task = ker::mod::sched::task::Task;
    using State = ker::mod::sched::task::TaskState;
    using Operation = child_query::Operation;
    constexpr uint16_t DOWN = 0x7B80, UP = 0x7B81;
    auto tasks = std::make_unique<Task[]>(2);
    auto& owner = tasks[0];
    auto& child = tasks[1];
    owner.pid = 1;
    child.pid = 0x7B801234;
    owner.type = child.type = ker::mod::sched::task::TaskType::PROCESS;
    owner.state.store(State::EXITING);
    child.state.store(State::EXITING);
    child_events::update_process_group(child, 77);
    child.exit_status = 37 << 8;
    child.resources.cpu_user_time_us = 17;
    bool ok = child_events::begin_publication(owner, child) && child_events::commit_publication(child);
    auto check = [&](bool value, int line = __builtin_LINE()) {
        if (!value) ker::mod::dbg::logger<"wki">::error("Child-query ownership check failed at line %d", line);
        ok &= value;
    };
    check(ok);

    child.has_exited = true;
    child.exit_notify_ready.store(true);
    check(child_events::publish_exit(child));
    owner.state.store(State::ACTIVE);
    std::array<WkiChannel, 2> channels{};
    std::array<WkiPeer, 2> peers{};
    std::array<ComputeSubmitSessionToken, 2> sessions{};
    ComputePeerIdentity identity{.boot_epoch = 41,
                                 .auth_generation = 5,
                                 .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                                 .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    s_compute_lock.lock();
    for (size_t i = 0; i < 2; ++i) {
        auto& channel = channels[i];
        peers[i].node_id = channel.peer_node_id = i == 0 ? DOWN : UP;
        channel.active = true;
        channel.channel_id = WKI_CHAN_RESOURCE;
        channel.generation = 43;
        check(find_compute_submit_peer_session_locked(peers[i].node_id) == nullptr);
        sessions[i] = capture_compute_submit_session_locked(peers[i].node_id, &channel, channel.generation, identity);
        check(sessions[i].epoch != 0);
    }
    SubmittedTask submitted;
    submitted.active = submitted.exec_handoff = submitted.exec_prepared = submitted.exec_cutover = true;
    submitted.exec_remote_pid = 0x7B808000;
    submitted.target_node = DOWN;
    submitted.submit_channel = &channels[0];
    submitted.submit_channel_generation = channels[0].generation;
    submitted.task_id = allocate_submitted_task_id_locked();
    uint32_t const ID = submitted.task_id;
    bool const REF = submitted.set_local_task_ref(&owner);
    auto* row = REF ? publish_submitted_task_locked(std::move(submitted)) : nullptr;
    s_compute_lock.unlock();
    PeerLifecycleLease lease;
    check(lease.acquire(&peers[0]));
    bool running_ref = false;
    if (row != nullptr) {
        owner.publish_accepted_signal_route(ID, row->exec_remote_pid);
        child_query::Origin const LOCAL{g_wki.my_node_id, g_wki.local_boot_epoch};
        ChildQueryRequest request{.sequence = 1,
                                  .task_id = ID,
                                  .receiver_pid = row->exec_remote_pid,
                                  .spec = {.selector = child.pid, .caller_origin = LOCAL},
                                  .remaining_hops = 4,
                                  .lane = 3};
        auto call = [&](Operation op, child_query::Origin group = {}) {
            return execute_child_query(lease, sessions[0], request, op, group, false);
        };
        auto locate = [&]() -> ChildQueryRecord* {
            for (auto& record : s_child_queries)
                if (record.active && record.request == request && compute_submit_session_token_matches(record.session, sessions[0]))
                    return &record;
            return nullptr;
        };
        auto drain = [&] {
            lease.release();
            while (run_one_child_wait_retirement() || run_one_child_query_retirement()) {
            }
            check(lease.acquire(&peers[0]));
        };
        uint32_t const REFS = owner.ref_count.load();
        row->response_pending.store(true);
        check(!call(Operation::INSPECT).ready && owner.ref_count.load() == REFS && locate() == nullptr);
        row->response_pending.store(false);
        row->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        auto reply = call(Operation::INSPECT);
        check(reply.ready && reply.outcome.error == 0 && reply.outcome.summary.ready_depth == 0 && reply.outcome.summary.pids.found &&
              reply.outcome.summary.pids.origin == LOCAL && owner.ref_count.load() == REFS + 1);
        auto const VERSION = reply.version;
        auto wire = encode_child_query_request(request, {.query = 1, .generation = 1, .operation = Operation::INSPECT});
        auto packet_reply = execute_child_query_packet(lease, sessions[0], wire);
        ChildQueryRequest packet_request{};
        ChildQueryReply packet_result{};
        check(decode_child_query_response(packet_reply, packet_request, packet_result) && packet_request == request &&
              packet_result.ready && packet_result.version == VERSION && packet_result.outcome.summary.pids.found);

        check(call(Operation::INSPECT).version == VERSION && owner.ref_count.load() == REFS + 1);
        auto changed = request;
        ++changed.options;
        check(execute_child_query(lease, sessions[0], changed, Operation::INSPECT).outcome.error == -EINVAL);
        auto stale = sessions[0];
        ++stale.identity.auth_generation;
        check(execute_child_query(lease, stale, request, Operation::VALIDATE).outcome.error == -EACCES);
        PeerLifecycleLease unowned;
        check(execute_child_query(unowned, sessions[0], request, Operation::INSPECT).outcome.error == -EACCES);
        child_events::Claim claim;
        child_events::ClaimedEvent event{};
        check(child_events::try_claim(owner, claim, child.pid, 0, event) == child_events::ProbeResult::CLAIMED);
        child_events::cancel_claim(claim);
        check(call(Operation::VALIDATE).outcome.error == -EAGAIN);
        check(call(Operation::CLOSE).ready);
        drain();
        check(locate() == nullptr && owner.ref_count.load() == REFS);
        check(call(Operation::INSPECT).outcome.error == -ESTALE && call(Operation::CLOSE).outcome.error == 0);
        ++request.sequence;
        check(call(Operation::CLOSE).outcome.error == 0);  // close before initial inspect
        check(call(Operation::INSPECT).outcome.error == -ESTALE);
        ++request.sequence;
        request.spec.selector = child_events::SELECT_ANY;
        check(call(Operation::INSPECT).outcome.error == 0 && call(Operation::VALIDATE).outcome.error == 0);
        check(call(Operation::CLOSE).ready);
        drain();
        // The query owns one conditional claim, including its final receipt.
        // WNOWAIT allows the same child to exercise later relay tests.
        for (unsigned scenario = 0; scenario < 4; ++scenario) {
            ++request.sequence;
            request.options = child_events::OPTION_NOWAIT;
            check(call(Operation::INSPECT).outcome.error == 0);
            ChildClaimRequest claim_request{
                .sequence = request.sequence, .task_id = ID, .receiver_pid = request.receiver_pid, .lane = request.lane};
            auto claim_call = [&](ChildWaitCommand command) {
                auto const wire =
                    encode_child_claim_request(claim_request, {.query = request.sequence, .command = command, .generation = 1});
                return execute_child_claim_packet(lease, sessions[0], wire);
            };
            check(claim_call(ChildWaitCommand::BEGIN).error == -EAGAIN);
            check(call(Operation::VALIDATE).outcome.error == 0);
            if (scenario == 3) {
                check(child_events::try_claim(owner, claim, child.pid, 0, event) == child_events::ProbeResult::CLAIMED);
                child_events::cancel_claim(claim);
                auto stale = claim_call(ChildWaitCommand::BEGIN);
                check(stale.phase == static_cast<uint8_t>(ChildWaitPhase::DONE) && stale.error == -EAGAIN);
            } else {
                auto begun = claim_call(ChildWaitCommand::BEGIN);
                check(begun.phase == static_cast<uint8_t>(ChildWaitPhase::CLAIMED) && begun.pid == child.pid && begun.user_time_us == 17 &&
                      begun.group_origin.node == LOCAL.node);
                auto replay = claim_call(ChildWaitCommand::BEGIN);
                check(std::memcmp(&begun, &replay, sizeof(begun)) == 0);
                auto wrong = claim_request;
                ++wrong.depth;
                check(execute_query_claim(lease, sessions[0], wrong, ChildWaitCommand::BEGIN).error == -EINVAL);
                if (scenario == 0) {
                    auto committed = claim_call(ChildWaitCommand::COMMIT);
                    check(committed.committed == 1 && committed.error == 0 && committed.pid == child.pid);
                    check(claim_call(ChildWaitCommand::COMMIT).committed == 1);
                    check(claim_call(ChildWaitCommand::RELEASE).committed == 1);
                } else if (scenario == 1) {
                    check(claim_call(ChildWaitCommand::RELEASE).error == -ECANCELED);
                    check(claim_call(ChildWaitCommand::BEGIN).error == -ECANCELED);
                }
            }
            // Even a terminal claim receipt owns storage until final close.
            check(!call(Operation::CLOSE).ready);
            check(claim_call(ChildWaitCommand::BEGIN).error == -ECANCELED);
            drain();
            check(locate() == nullptr && call(Operation::CLOSE).ready && owner.ref_count.load() == REFS);
            check(claim_call(ChildWaitCommand::BEGIN).error == -ESTALE);
            check(child_events::inspect_queue(owner, child.pid, 0).ready && child_events::read_reaped_cpu_times(owner).user_time_us == 0);
        }
        request.options = 0;
        // Exact local physical groups must localize before queue lookup.
        child_events::update_process_group(owner, 77);
        ++request.sequence;
        request.spec.selector = child_events::selector_for_process_group(77, LOCAL);
        reply = call(Operation::INSPECT);
        check(reply.ready && reply.outcome.summary.ready_depth == 0);
        check(call(Operation::CLOSE).ready);
        drain();
        ++request.sequence;
        request.spec.selector.group_origin = {};
        request.spec.group_alias = true;
        reply = call(Operation::INSPECT);
        check(reply.ready && reply.outcome.summary.groups.flags != 0 && reply.outcome.summary.ready_depth == child_query::NO_READY);
        check(call(Operation::VALIDATE).outcome.error == -EAGAIN);
        reply = call(Operation::PROBE, LOCAL);
        check(reply.ready && reply.outcome.summary.ready_depth == 0);
        check(call(Operation::PROBE, {UP, 41}).outcome.error == -EINVAL);
        check(call(Operation::VALIDATE).outcome.error == 0);
        // Exit can mark a record while a local executor owns busy. Reclamation
        // must wait for that executor to stop touching the retained storage.
        auto* busy = locate();
        if (busy != nullptr) {
            s_compute_lock.lock();
            busy->busy = true;
            s_compute_lock.unlock();
            retire_child_queries_for_task(owner);
            drain();
            check(locate() == busy && owner.ref_count.load() == REFS + 1);
            s_compute_lock.lock();
            busy->busy = false;
            s_compute_lock.unlock();
            drain();
            check(locate() == nullptr && owner.ref_count.load() == REFS);
        } else
            ok = false;
        ++request.sequence;
        request.spec.group_alias = false;
        request.spec.selector = child.pid;
        uint64_t const SAVED_TOKEN = s_next_child_query_token;
        s_next_child_query_token = UINT64_MAX;
        check(call(Operation::INSPECT).outcome.error == -EOVERFLOW && owner.ref_count.load() == REFS);
        s_next_child_query_token = SAVED_TOKEN;
        // A real running-exec row makes this owner a relay to another session.
        RunningRemoteTask running{};
        running.active = running.exec_handoff = running.published = running.exec_committed = true;
        running.task_id = 0x7B8101;
        running.submitter_node = UP;
        running.local_pid = owner.pid;
        running.submit_session_epoch = sessions[1].epoch;
        running.submit_rx_channel = &channels[1];
        running.submit_rx_channel_generation = channels[1].generation;
        running.task = &owner;
        running_ref = owner.try_acquire_lifetime_ref();
        s_compute_lock.lock();
        bool const INSERTED = running_ref && publish_running_task_locked(running);
        s_compute_lock.unlock();
        check(INSERTED);
        if (INSERTED) {
            // The normal packet entry refuses an unadvertised upstream before
            // creating any outgoing action; a fresh query can retry later.
            ++request.sequence;
            auto unsupported = execute_child_query_packet(
                lease, sessions[0], encode_child_query_request(request, {.query = 1, .generation = 1, .operation = Operation::INSPECT}));
            check(unsupported.ready == 1 && unsupported.error == -ENOTSUP && !take_child_query_work());
            check(call(Operation::CLOSE).ready);
            drain();
            // Inject two supported retained requests, then use actual encoded
            // replies and the production response receiver to drive one chain.
            ++request.sequence;
            check(!call(Operation::INSPECT).ready);
            auto sibling = request;
            sibling.lane = 4;
            check(!execute_child_query(lease, sessions[0], sibling, Operation::INSPECT, {}, false).ready);
            auto first = take_child_query_work(100000);
            check(first.has_value());
            if (first) check(retry_child_query_work(*first));
            auto second = take_child_query_work(100000);
            check(first && second && first->slot != second->slot);
            check(!take_child_query_work(100001));
            // Both are overdue; a timed retry remains the same logical action.
            auto retry = take_child_query_work(110000);
            check(retry && first && retry->slot == first->slot && retry->action == first->action);
            if (first && second) {
                lease.release();
                check(lease.acquire(&peers[1]));
                for (auto const& work : {*first, *second}) {
                    auto const wire = encode_child_query_request(work.request, work.action);
                    auto pending = encode_child_query_response(wire, {});
                    check(receive_child_query_response(lease, sessions[1], pending));
                    ChildQueryReply upstream{.ready = true, .version = {11, 13}};
                    auto response = encode_child_query_response(wire, upstream);
                    auto tampered = response;
                    ++tampered.request.generation;
                    check(!receive_child_query_response(lease, sessions[1], tampered));
                    tampered = response;
                    ++tampered.request.options;
                    check(!receive_child_query_response(lease, sessions[1], tampered));
                    check(receive_child_query_response(lease, sessions[1], response));
                    check(!receive_child_query_response(lease, sessions[1], response));
                }
                lease.release();
                check(lease.acquire(&peers[0]));
                auto collected = call(Operation::INSPECT);
                check(collected.ready && collected.outcome.summary.ready_depth == 0 && collected.outcome.summary.pids.found);
                check(!call(Operation::VALIDATE).ready);
                auto validate = take_child_query_work(120000);
                check(validate && validate->action.operation == Operation::VALIDATE);
                if (validate) {
                    lease.release();
                    check(lease.acquire(&peers[1]));
                    auto wire = encode_child_query_request(validate->request, validate->action);
                    auto response = encode_child_query_response(wire, {.ready = true, .version = {11, 13}});
                    check(receive_child_query_response(lease, sessions[1], response));
                    lease.release();
                    check(lease.acquire(&peers[0]));
                    check(call(Operation::VALIDATE).ready && call(Operation::VALIDATE).outcome.summary.pids.found);
                }
            }
            check(!call(Operation::CLOSE).ready);
            check(!execute_child_query(lease, sessions[0], sibling, Operation::CLOSE, {}, false).ready);
            lease.release();
            check(lease.acquire(&peers[1]));
            for (unsigned i = 0; i < 2; ++i) {
                auto close = take_child_query_work(130000);
                check(close && close->action.operation == Operation::CLOSE);
                if (close) {
                    auto wire = encode_child_query_request(close->request, close->action);
                    check(receive_child_query_response(lease, sessions[1], encode_child_query_response(wire, child_query_error(0))));
                }
            }
            lease.release();
            check(lease.acquire(&peers[0]));
            drain();
            check(locate() == nullptr && owner.ref_count.load() == REFS + 1);
            // Exercise the production value codec and retained response adapter
            // with an upstream-selected event and with a cross-node PID collision.
            for (unsigned scenario = 0; scenario < 3; ++scenario) {
                ++request.sequence;
                request.spec.selector = scenario == 2 ? child.pid : child.pid + 1;
                check(!call(Operation::INSPECT).ready);
                child_query::Summary summary{.pids = {.found = true, .origin = {UP, 41}}, .potential = true, .ready_depth = 0};
                auto answer_query = [&](Operation operation, child_query::Summary value) {
                    auto work = take_child_query_work();
                    check(work && work->action.operation == operation);
                    if (!work) return;
                    lease.release();
                    check(lease.acquire(&peers[1]));
                    auto wire = encode_child_query_request(work->request, work->action);
                    auto response = encode_child_query_response(wire, {.ready = true, .outcome = {.summary = value}, .version = {11, 13}});
                    check(receive_child_query_response(lease, sessions[1], response));
                    lease.release();
                    check(lease.acquire(&peers[0]));
                };
                answer_query(Operation::INSPECT, summary);
                check(call(Operation::INSPECT).ready);
                check(!call(Operation::VALIDATE).ready);
                answer_query(Operation::VALIDATE, summary);
                check(call(Operation::VALIDATE).ready);
                ChildClaimRequest claim_request{.sequence = request.sequence,
                                                .task_id = ID,
                                                .receiver_pid = request.receiver_pid,
                                                .depth = static_cast<uint16_t>(scenario == 2 ? 0 : 1),
                                                .lane = request.lane};
                auto claim_call = [&](ChildWaitCommand command) {
                    return execute_query_claim(lease, sessions[0], claim_request, command, false);
                };
                if (scenario == 2) {
                    check(call(Operation::VALIDATE).outcome.summary.pids.ambiguous);
                    check(claim_call(ChildWaitCommand::BEGIN).error == -EOVERFLOW && !take_child_claim_work(1000));
                } else {
                    check(claim_call(ChildWaitCommand::BEGIN).phase == ChildWaitPhase::PENDING);
                    auto work = take_child_claim_work(1000);
                    check(work && work->request.depth == 0 && work->request.task_id == running.task_id);
                    check(!take_child_claim_work(1001));
                    auto retry = take_child_claim_work(11000);
                    check(work && retry && retry->action == work->action && retry->request == work->request);
                    if (work) {
                        ChildWaitReply upstream{.phase = ChildWaitPhase::CLAIMED,
                                                .probe = child_events::ProbeResult::CLAIMED,
                                                .value = {.pid = child.pid + 1,
                                                          .process_group = 77,
                                                          .group_origin = {UP, 41},
                                                          .user_time_us = 29,
                                                          .status = 37 << 8}};
                        auto wire = encode_child_claim_request(work->request, work->action);
                        auto response = encode_child_claim_response(wire, upstream);
                        lease.release();
                        check(lease.acquire(&peers[1]));
                        check(receive_child_claim_response(lease, sessions[1], encode_child_claim_response(wire, {})));
                        auto bad = response;
                        ++bad.request.generation;
                        check(!receive_child_claim_response(lease, sessions[1], bad));
                        bad = response;
                        ++bad.pid;
                        check(!receive_child_claim_response(lease, sessions[1], bad));
                        check(receive_child_claim_response(lease, sessions[1], response));
                        check(!receive_child_claim_response(lease, sessions[1], response));
                        lease.release();
                        check(lease.acquire(&peers[0]));
                        check(claim_call(ChildWaitCommand::BEGIN).phase == ChildWaitPhase::CLAIMED);
                        auto command = scenario == 0 ? ChildWaitCommand::COMMIT : ChildWaitCommand::RELEASE;
                        check(claim_call(command).phase == ChildWaitPhase::PENDING);
                        auto finish = take_child_claim_work(21000);
                        check(finish && finish->action.command == command && finish->action.generation == 2);
                        if (finish) {
                            auto envelope = encode_child_claim_request(finish->request, finish->action);
                            lease.release();
                            check(lease.acquire(&peers[1]));
                            check(!receive_child_claim_response(lease, sessions[1], encode_child_claim_response(envelope, upstream)));
                            upstream.phase = ChildWaitPhase::DONE;
                            upstream.committed = scenario == 0;
                            upstream.error = scenario == 0 ? 0 : -ECANCELED;
                            auto bad_value = upstream;
                            ++bad_value.value.user_time_us;
                            check(!receive_child_claim_response(lease, sessions[1], encode_child_claim_response(envelope, bad_value)));
                            check(receive_child_claim_response(lease, sessions[1], encode_child_claim_response(envelope, upstream)));
                            lease.release();
                            check(lease.acquire(&peers[0]));
                            auto receipt = claim_call(ChildWaitCommand::RELEASE);
                            check(receipt.phase == ChildWaitPhase::DONE && receipt.committed == (scenario == 0) &&
                                  receipt.value.user_time_us == 29 && !take_child_claim_work(31000));
                        }
                    }
                }
                check(!call(Operation::CLOSE).ready);
                answer_query(Operation::CLOSE, {});
                drain();
                check(locate() == nullptr && owner.ref_count.load() == REFS + 1);
                check(child_events::inspect_queue(owner, child.pid, 0).ready &&
                      child_events::read_reaped_cpu_times(owner).user_time_us == 0);
            }
            ++request.sequence;
            request.spec.selector = child.pid;
            reply = call(Operation::INSPECT);
            check(!reply.ready && owner.ref_count.load() == REFS + 2);
            auto work = take_child_query_work();
            check(work.has_value());
            if (work) {
                auto const OLD = *work;
                check(work->route.task_id == running.task_id && work->request.task_id == running.task_id &&
                      work->request.receiver_pid == owner.pid && work->request.remaining_hops == 3 &&
                      work->request.sequence == work->action.query && work->request.lane == work->slot);
                check(!take_child_query_work().has_value() && retry_child_query_work(*work));
                work = take_child_query_work();
                check(work && work->action == OLD.action);
                // CLOSE overtakes the reserved send; it cannot release the pin.
                check(!call(Operation::CLOSE).ready);
                drain();
                check(locate() != nullptr && owner.ref_count.load() == REFS + 2);
                lease.release();
                check(lease.acquire(&peers[1]));
                check(!accept_child_query_work(lease, OLD, {}));
                auto close = take_child_query_work();
                check(close && close->action.operation == Operation::CLOSE);
                if (close) {
                    auto bad = *close;
                    ++bad.route.session.epoch;
                    check(!accept_child_query_work(lease, bad, {}));
                    check(accept_child_query_work(lease, *close, {.error = -EAGAIN}));
                    auto retry = take_child_query_work();
                    check(retry && retry->action == close->action);
                    check(accept_child_query_work(lease, *close, {}));
                }
                lease.release();
                check(lease.acquire(&peers[0]));
                drain();
                check(locate() == nullptr && owner.ref_count.load() == REFS + 1);
                ++request.sequence;
                check(!call(Operation::INSPECT).ready);
                auto next = take_child_query_work();
                check(next && next->action.query != OLD.action.query && !retry_child_query_work(OLD));
                // Downstream termination keeps the upstream close and Task pin.
                s_compute_lock.lock();
                retire_child_queries_for_session_locked(sessions[0]);
                s_compute_lock.unlock();
                drain();
                check(locate() != nullptr && !call(Operation::CLOSE).ready);
                // Exact upstream session retirement permits final reclamation.
                s_compute_lock.lock();
                retire_compute_submit_session_locked(UP, &channels[1], channels[1].generation);
                s_compute_lock.unlock();
                drain();
                check(locate() == nullptr && owner.ref_count.load() == REFS + 1);
                if (next) check(!retry_child_query_work(*next));
            }
        }
        check(child_events::inspect_queue(owner, child.pid, 0).ready && child_events::read_reaped_cpu_times(owner).user_time_us == 0);
    } else
        ok = false;
    lease.release();
    s_compute_lock.lock();
    for (auto const& session : sessions) retire_child_queries_for_session_locked(session);
    s_compute_lock.unlock();
    retire_child_queries_for_task(owner);
    while (run_one_child_wait_retirement() || run_one_child_query_retirement()) {
    }
    s_compute_lock.lock();
    for (auto& running : g_running_remote_tasks)
        if (running.task == &owner) {
            running.task = nullptr;
            running.active = false;
        }
    compact_running_remote_tasks_locked();
    if (row != nullptr) remove_submitted_task_for_selftest_locked(ID);
    for (auto const& session : sessions)
        if (auto* stored = find_compute_submit_peer_session_locked(session.node_id); stored != nullptr) *stored = {};
    s_compute_lock.unlock();
    if (running_ref) owner.release();
    owner.state.store(State::EXITING);
    child_events::prepare_process_exit(owner);
    return ok && owner.ref_count.load() == 1 && child.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_child_wait_transactions() -> bool {
    using Task = ker::mod::sched::task::Task;
    using State = ker::mod::sched::task::TaskState;
    using Command = ChildWaitCommand;
    using Phase = ChildWaitPhase;
    constexpr uint16_t NODE = 0x7B7F;
    constexpr uint32_t GENERATION = 29;
    auto tasks = std::make_unique<Task[]>(5);
    auto& owner = tasks[0];
    owner.pid = 1;  // fixture exit discards instead of adopting into a live init
    bool ok = true;
    for (size_t i = 0; i < 5; ++i) {
        auto& task = tasks[i];
        task.type = ker::mod::sched::task::TaskType::PROCESS;
        if (i != 0) task.pid = 0x7B7F1000 + i;
        task.state.store(State::EXITING);
    }
    for (size_t i = 1; i < 5; ++i) {
        auto& child = tasks[i];
        child.resources.cpu_user_time_us = i * 7;
        child.resources.cpu_system_time_us = i * 3;
        child.exit_status = static_cast<int32_t>(i << 8);
        ok &= child_events::begin_publication(owner, child) && child_events::commit_publication(child);
        child.has_exited = true;
        child.exit_notify_ready.store(true);
        ok &= child_events::publish_exit(child);
    }
    owner.state.store(State::ACTIVE);
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = GENERATION;
    ComputePeerIdentity identity{.boot_epoch = 31,
                                 .auth_generation = 3,
                                 .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                                 .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    WkiPeer peer{};
    peer.node_id = NODE;
    PeerLifecycleLease lease;
    ok &= lease.acquire(&peer);
    s_compute_lock.lock();
    bool const UNUSED = find_compute_submit_peer_session_locked(NODE) == nullptr;
    auto const SESSION = UNUSED ? capture_compute_submit_session_locked(NODE, &channel, GENERATION, identity) : ComputeSubmitSessionToken{};
    SubmittedTask submitted;
    submitted.active = submitted.exec_handoff = submitted.exec_prepared = submitted.exec_cutover = true;
    submitted.exec_remote_pid = 0x7B7F2000;
    submitted.target_node = NODE;
    submitted.submit_channel = &channel;
    submitted.submit_channel_generation = GENERATION;
    submitted.task_id = allocate_submitted_task_id_locked();
    uint32_t const ID = submitted.task_id;
    bool const REF = submitted.set_local_task_ref(&owner);
    auto* row = UNUSED && REF ? publish_submitted_task_locked(std::move(submitted)) : nullptr;
    s_compute_lock.unlock();
    ok &= row != nullptr && SESSION.epoch != 0;
    if (row != nullptr && SESSION.epoch != 0) {
        owner.publish_accepted_signal_route(ID, row->exec_remote_pid);
        auto request = [&](uint8_t lane, uint64_t sequence, size_t child, int32_t options = 0) {
            return ChildWaitRequest{.sequence = sequence,
                                    .queue_version = child_events::inspect_queue(owner, tasks[child].pid, options).version,
                                    .task_id = ID,
                                    .receiver_pid = row->exec_remote_pid,
                                    .selector = tasks[child].pid,
                                    .options = options,
                                    .lane = lane};
        };
        auto call = [&](const ChildWaitRequest& req, Command command) { return execute_child_wait(lease, SESSION, req, command); };
        auto locate = [&](const ChildWaitRequest& req) -> ChildWaitTransaction* {
            for (auto& slot : s_child_wait_transactions)
                if (slot.active && slot.request == req && compute_submit_session_token_matches(slot.session, SESSION)) return &slot;
            return nullptr;
        };
        auto drain = [&] {
            // Cleanup requires no peer lease, just like the actual daemon loop.
            lease.release();
            while (run_one_child_wait_retirement()) {
            }
            ok &= lease.acquire(&peer);
        };
        auto ack = [&](const ChildWaitRequest& req) {
            static_cast<void>(call(req, Command::ACK));
            drain();
        };
        auto a = request(0, 1, 1);
        uint32_t const INITIAL_REFS = owner.ref_count.load();
        s_compute_lock.lock();
        row->response_pending.store(true);
        s_compute_lock.unlock();
        auto reply = call(a, Command::BEGIN);
        ok &= reply.phase == Phase::PENDING && reply.error == 0 && owner.ref_count.load() == INITIAL_REFS;
        s_compute_lock.lock();
        ok &= locate(a) == nullptr;
        row->response_pending.store(false);
        row->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        s_compute_lock.unlock();
        PeerLifecycleLease unowned;
        ok &= execute_child_wait(unowned, SESSION, a, Command::BEGIN).error == -EACCES;
        auto invalid = a;
        invalid.lane = CHILD_WAIT_LANES;
        ok &= call(invalid, Command::BEGIN).error == -EINVAL;
        invalid = a;
        invalid.sequence = 0;
        ok &= call(invalid, Command::BEGIN).error == -EINVAL;
        invalid = a;
        invalid.queue_version = {};
        ok &= call(invalid, Command::BEGIN).error == -EINVAL;
        ok &= call(a, static_cast<Command>(UINT8_MAX)).error == -EINVAL;
        auto stale = SESSION;
        ++stale.identity.auth_generation;
        ok &= execute_child_wait(lease, stale, a, Command::BEGIN).error == -EACCES;

        reply = call(a, Command::BEGIN);
        ok &= reply.phase == Phase::CLAIMED && reply.value.pid == tasks[1].pid && reply.value.status == 256 &&
              reply.value.user_time_us == 7 && reply.value.system_time_us == 3 && owner.ref_count.load() == INITIAL_REFS + 1;
        s_compute_lock.lock();
        auto* transaction = locate(a);
        uint64_t const COOKIE = transaction != nullptr ? transaction->claimed.claim_cookie : 0;
        s_compute_lock.unlock();
        ok &= COOKIE != 0;
        reply = call(a, Command::BEGIN);
        ok &= reply.phase == Phase::CLAIMED && owner.ref_count.load() == INITIAL_REFS + 1;
        s_compute_lock.lock();
        ok &= locate(a) != nullptr && locate(a)->claimed.claim_cookie == COOKIE;
        s_compute_lock.unlock();
        invalid = a;
        invalid.selector = tasks[2].pid;
        ok &= call(invalid, Command::BEGIN).error == -EINVAL;
        invalid = a;
        ++invalid.queue_version.queue;
        ok &= call(invalid, Command::BEGIN).error == -EINVAL;
        invalid = a;
        ++invalid.sequence;
        ok &= call(invalid, Command::BEGIN).error == -EAGAIN;
        for (unsigned changed = 0; changed < 4; ++changed) {
            auto changed_session = SESSION;
            if (changed == 0) changed_session.identity.policy &= ~wki_policy_bits(WkiPolicy::COMPUTE);
            if (changed == 1) changed_session.identity.policy &= ~wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
            if (changed == 2) changed_session.identity.capabilities = 0;
            if (changed == 3) ++changed_session.identity.auth_generation;
            s_compute_lock.lock();
            find_compute_submit_peer_session_locked(NODE)->current.identity = changed_session.identity;
            s_compute_lock.unlock();
            ok &= execute_child_wait(lease, changed_session, a, Command::COMMIT).error == -EACCES;
            s_compute_lock.lock();
            find_compute_submit_peer_session_locked(NODE)->current.identity = SESSION.identity;
            ok &= locate(a) != nullptr && locate(a)->claimed.claim_cookie == COOKIE;
            s_compute_lock.unlock();
        }
        auto b = request(1, 1, 1);
        reply = call(b, Command::BEGIN);
        ok &= reply.phase == Phase::DONE && reply.probe == child_events::ProbeResult::WOULD_BLOCK && reply.value.pid == 0;
        ack(b);
        reply = call(a, Command::COMMIT);
        ok &= reply.committed && reply.error == 0 && reply.value.pid == tasks[1].pid && owner.child_user_time_us == 7;
        for (Command command : {Command::COMMIT, Command::BEGIN, Command::RELEASE}) {
            reply = call(a, command);
            ok &= reply.committed && reply.value.pid == tasks[1].pid && owner.child_user_time_us == 7 && owner.child_system_time_us == 3;
        }
        ack(a);
        ok &= call(a, Command::BEGIN).error == -ESTALE;
        ok &= call(a, Command::COMMIT).error == -ESTALE;
        a = request(0, 2, 2);
        reply = call(a, Command::BEGIN);
        ok &= reply.phase == Phase::CLAIMED && reply.value.pid == tasks[2].pid;
        reply = call(a, Command::RELEASE);
        ok &= reply.phase == Phase::DONE && !reply.committed && reply.error == -ECANCELED;
        ok &= call(a, Command::COMMIT).error == -ECANCELED && owner.child_user_time_us == 7;
        ack(a);
        a = request(0, 3, 2, child_events::OPTION_NOWAIT);
        ok &= call(a, Command::BEGIN).phase == Phase::CLAIMED;
        ok &= call(a, Command::COMMIT).committed && owner.child_user_time_us == 7;
        ok &= call(a, Command::COMMIT).committed && owner.child_user_time_us == 7;
        ack(a);
        a = request(0, 4, 2);
        ok &= call(a, Command::BEGIN).phase == Phase::CLAIMED;
        ok &= call(a, Command::COMMIT).committed && owner.child_user_time_us == 21 && owner.child_system_time_us == 9;
        ack(a);

        // Discovery followed by a claim/release ABA must not acquire a new
        // transaction against the old snapshot, even though readiness matches.
        auto stale_query = request(3, 1, 3);
        child_events::Claim concurrent{};
        child_events::ClaimedEvent observed{};
        ok &= child_events::try_claim(owner, concurrent, tasks[4].pid, 0, observed) == child_events::ProbeResult::CLAIMED;
        child_events::release_claim(concurrent, observed);
        reply = call(stale_query, Command::BEGIN);
        ok &= reply.phase == Phase::DONE && reply.error == -EAGAIN && !reply.committed && reply.value.pid == 0;
        ok &= tasks[3].child_parent_exit_event.claimed_by == nullptr && owner.child_user_time_us == 21;
        ok &= call(stale_query, Command::BEGIN).error == -EAGAIN && call(stale_query, Command::COMMIT).error == -EAGAIN;
        ack(stale_query);
        ok &= call(stale_query, Command::BEGIN).error == -ESTALE;
        stale_query = request(3, 2, 3);
        ok &= call(stale_query, Command::BEGIN).phase == Phase::CLAIMED;
        ok &= call(stale_query, Command::RELEASE).error == -ECANCELED;
        ack(stale_query);

        // Cancellation can arrive before a deferred BEGIN or after a claim.
        auto canceled = request(2, 10, 3);
        ok &= call(canceled, Command::RELEASE).error == -ECANCELED;
        ok &= call(canceled, Command::BEGIN).error == -ESTALE;
        canceled = request(2, 11, 3);
        ok &= call(canceled, Command::ACK).error == 0;
        ok &= call(canceled, Command::BEGIN).error == -ESTALE;
        canceled = request(2, 12, 3);
        ok &= call(canceled, Command::BEGIN).phase == Phase::CLAIMED;
        s_compute_lock.lock();
        transaction = locate(canceled);
        ok &= transaction != nullptr;
        if (transaction != nullptr) {
            transaction->busy = true;
            retire_child_wait_transaction_locked(*transaction);
        }
        s_compute_lock.unlock();
        lease.release();
        ok &= !run_one_child_wait_retirement();
        s_compute_lock.lock();
        if (transaction != nullptr) {
            ok &= transaction->claim.event != nullptr;
            transaction->busy = false;
        }
        s_compute_lock.unlock();
        while (run_one_child_wait_retirement()) {
        }
        ok &= lease.acquire(&peer);
        ok &= call(canceled, Command::BEGIN).error == -ESTALE;
        canceled = request(2, 13, 3);
        ok &= call(canceled, Command::BEGIN).phase == Phase::CLAIMED;
        // A retained claim is not a perpetual execution grant.
        s_compute_lock.lock();
        row->active = false;
        s_compute_lock.unlock();
        ok &= call(canceled, Command::COMMIT).error == -ESRCH && owner.child_user_time_us == 21;
        drain();
        s_compute_lock.lock();
        row->active = true;
        s_compute_lock.unlock();
        canceled = request(2, 14, 3);
        ok &= call(canceled, Command::BEGIN).phase == Phase::CLAIMED;
        lease.release();
        retire_child_wait_transactions_for_task(owner);
        while (run_one_child_wait_retirement()) {
        }
        ok &= lease.acquire(&peer);
        ok &= call(canceled, Command::BEGIN).error == -ESTALE;

        // Session retirement releases a live claim, while a replacement session
        // has independent replay watermarks and cannot use the old grant.
        canceled = request(2, 15, 3);
        ok &= call(canceled, Command::BEGIN).phase == Phase::CLAIMED;
        s_compute_lock.lock();
        retire_compute_submit_session_locked(NODE, &channel, GENERATION);
        s_compute_lock.unlock();
        ok &= call(canceled, Command::COMMIT).error == -EACCES;
        drain();
        channel.generation = GENERATION + 1;
        s_compute_lock.lock();
        auto const NEXT = capture_compute_submit_session_locked(NODE, &channel, GENERATION + 1, identity);
        ok &= NEXT.epoch != SESSION.epoch && find_compute_submit_peer_session_locked(NODE)->child_wait_completed[2] == 0;
        s_compute_lock.unlock();
        // The submitted row still belongs to the old generation.
        ok &= execute_child_wait(lease, NEXT, canceled, Command::BEGIN).error == -ESRCH;
    }
    lease.release();
    retire_child_wait_transactions_for_task(owner);
    while (run_one_child_wait_retirement()) {
    }
    owner.state.store(State::EXITING);
    child_events::prepare_process_exit(owner);
    s_compute_lock.lock();
    if (row != nullptr) remove_submitted_task_for_selftest_locked(ID);
    if (UNUSED)
        if (auto* session = find_compute_submit_peer_session_locked(NODE); session != nullptr) *session = {};
    s_compute_lock.unlock();
    for (size_t i = 0; i < 5; ++i) ok &= tasks[i].ref_count.load() == 1;
    return ok;
}

auto wki_remote_compute_selftest_child_authority_admission() -> bool {
    using ker::mod::sched::task::Task;
    using Admission = ChildAuthorityAdmission;
    constexpr uint16_t NODE = 0x7B7D;
    constexpr uint32_t GENERATION = 19;
    auto tasks = std::make_unique<Task[]>(3);
    auto& source = tasks[0];
    auto& receiver = tasks[1];
    auto& same_pid = tasks[2];
    source.type = receiver.type = same_pid.type = ker::mod::sched::task::TaskType::PROCESS;
    source.pid = 0x7B7D0001;
    receiver.pid = same_pid.pid = 0x7B7D0002;
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = GENERATION;
    ComputePeerIdentity identity{.boot_epoch = 23,
                                 .auth_generation = 2,
                                 .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                                 .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE) != nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, GENERATION, identity);
    SubmittedTask submitted;
    submitted.active = submitted.exec_handoff = true;
    submitted.target_node = NODE;
    submitted.submit_channel = &channel;
    submitted.submit_channel_generation = GENERATION;
    submitted.task_id = allocate_submitted_task_id_locked();
    uint32_t const ID = submitted.task_id;
    bool const SOURCE_REF = submitted.set_local_task_ref(&source);
    auto* stored = SOURCE_REF ? publish_submitted_task_locked(std::move(submitted)) : nullptr;
    RunningRemoteTask running{};
    running.active = running.exec_handoff = true;
    running.task_id = ID;
    running.submitter_node = NODE;
    running.local_pid = receiver.pid;
    running.submit_session_epoch = SESSION.epoch;
    running.submit_rx_channel = &channel;
    running.submit_rx_channel_generation = GENERATION;
    running.task = &receiver;
    bool const RECEIVER_REF = receiver.try_acquire_lifetime_ref();
    bool const INSERTED = RECEIVER_REF && publish_running_task_locked(running);
    s_compute_lock.unlock();
    bool ok = SESSION.epoch != 0 && stored != nullptr && INSERTED;
    if (ok) {
        auto source_probe_key = [&](Admission expected, const ComputeSubmitSessionToken& session, uint32_t id, uint64_t pid) {
            Task* owner = nullptr;
            s_compute_lock.lock();
            auto const RESULT = acquire_child_authority_owner_locked(session, id, pid, owner);
            bool const MATCH = RESULT == expected && owner == (expected == Admission::READY ? &source : nullptr) &&
                               source.ref_count.load() == (expected == Admission::READY ? 3U : 2U);
            s_compute_lock.unlock();
            if (owner != nullptr) owner->release();
            return MATCH;
        };
        auto source_probe = [&](Admission expected) { return source_probe_key(expected, SESSION, ID, receiver.pid); };
        auto mutate_source = [&](auto change) {
            s_compute_lock.lock();
            change(*stored);
            s_compute_lock.unlock();
        };
        auto receiver_probe = [&](Admission expected, Task& task) {
            ChildAuthorityRoute route{};
            s_compute_lock.lock();
            auto const RESULT = capture_child_authority_route_locked(task, route);
            s_compute_lock.unlock();
            return RESULT == expected &&
                   (expected == Admission::READY
                        ? route.task_id == ID && route.receiver_pid == receiver.pid &&
                              compute_submit_session_token_matches(route.session, SESSION) && route.session.identity == SESSION.identity
                        : route.task_id == 0 && route.receiver_pid == 0 && route.session.epoch == 0);
        };
        auto mutate_running = [&](auto change) {
            s_compute_lock.lock();
            for (auto& row : g_running_remote_tasks)
                if (row.task == &receiver) change(row);
            s_compute_lock.unlock();
        };
        ok &= receiver_probe(Admission::LOCAL, same_pid);
        ok &= receiver_probe(Admission::AWAITING_ACCEPT, receiver);
        mutate_running([](auto& row) { row.published = row.exec_committed = row.accept_pending = true; });
        ok &= receiver_probe(Admission::READY, receiver);  // ACCEPT can still be in flight.
        ok &= source_probe(Admission::UNAVAILABLE);
        mutate_source([&](auto& row) {
            row.exec_prepared = true;
            row.exec_remote_pid = receiver.pid;
        });
        ok &= source_probe(Admission::AWAITING_ACCEPT);
        mutate_source([](auto& row) {
            row.exec_cutover = true;
            row.response_pending.store(true);
        });
        ok &= source_probe(Admission::AWAITING_ACCEPT);
        WkiWaitEntry accepted{};
        mutate_source([&](auto& row) { row.response_wait_entry = row.response_consumer_wait_entry = &accepted; });
        WkiHeader header{};
        header.src_node = NODE;
        TaskResponsePayload response{.task_id = ID, .remote_pid = receiver.pid};
        detail::handle_task_accept(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), &channel, GENERATION);
        wki_quiesce_claimed_op(&accepted);
        mutate_source([](auto& row) { row.response_consumer_wait_entry = nullptr; });
        ok &= source_probe(Admission::READY) && !stored->proxy_ready;
        ok &= source_probe_key(Admission::UNAVAILABLE, SESSION, ID + 1, receiver.pid);
        ok &= source_probe_key(Admission::UNAVAILABLE, SESSION, ID, receiver.pid + 1);
        for (unsigned field = 0; field < 7; ++field) {
            auto stale = SESSION;
            if (field == 0) ++stale.node_id;
            if (field == 1) ++stale.rx_channel_generation;
            if (field == 2) ++stale.epoch;
            if (field == 3) ++stale.identity.boot_epoch;
            if (field == 4) ++stale.identity.auth_generation;
            if (field == 5) stale.identity.auth_session_id[0] ^= 1;
            if (field == 6) stale.rx_channel = nullptr;
            ok &= source_probe_key(Admission::UNAVAILABLE, stale, ID, receiver.pid);
        }
        // Match the current session identity while withholding one permission:
        // these must fail policy, not merely an identity-mismatch guard.
        for (unsigned permission = 0; permission < 3; ++permission) {
            auto denied = SESSION;
            if (permission == 0) denied.identity.policy &= ~wki_policy_bits(WkiPolicy::COMPUTE);
            if (permission == 1) denied.identity.policy &= ~wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
            if (permission == 2) denied.identity.capabilities = 0;
            s_compute_lock.lock();
            find_compute_submit_peer_session_locked(NODE)->current.identity = denied.identity;
            s_compute_lock.unlock();
            ok &= source_probe_key(Admission::UNAVAILABLE, denied, ID, receiver.pid);
        }
        s_compute_lock.lock();
        find_compute_submit_peer_session_locked(NODE)->current.identity = SESSION.identity;
        s_compute_lock.unlock();
        mutate_source([](auto& row) { row.exec_handoff = false; });
        ok &= source_probe(Admission::UNAVAILABLE);
        mutate_source([](auto& row) { row.exec_handoff = true; });
        source.exit_in_progress = true;
        ok &= source_probe(Admission::UNAVAILABLE);
        source.exit_in_progress = false;
        mutate_source([](auto& row) { row.active = false; });
        ok &= source_probe(Admission::UNAVAILABLE);
        mutate_source([](auto& row) { row.active = true; });
        mutate_running([](auto& row) { row.exec_handoff = false; });
        ok &= receiver_probe(Admission::LOCAL, receiver);
        mutate_running([](auto& row) {
            row.exec_handoff = true;
            row.root_completion_queued = true;
        });
        ok &= receiver_probe(Admission::UNAVAILABLE, receiver);
        mutate_running([](auto& row) {
            row.root_completion_queued = false;
            ++row.submit_session_epoch;
        });
        ok &= receiver_probe(Admission::UNAVAILABLE, receiver);
        mutate_running([](auto& row) { --row.submit_session_epoch; });
        s_compute_lock.lock();
        auto* peer = find_compute_submit_peer_session_locked(NODE);
        peer->retiring = true;
        s_compute_lock.unlock();
        ok &= source_probe(Admission::UNAVAILABLE) && receiver_probe(Admission::UNAVAILABLE, receiver);
        s_compute_lock.lock();
        peer->retiring = false;
        s_compute_lock.unlock();
        ok &= source_probe(Admission::READY) && receiver_probe(Admission::READY, receiver);
    }
    s_compute_lock.lock();
    for (auto& row : g_running_remote_tasks)
        if (row.task == &receiver) {
            row.task = nullptr;
            row.active = false;
        }
    compact_running_remote_tasks_locked();
    if (stored != nullptr) remove_submitted_task_for_selftest_locked(ID);
    if (auto* peer = find_compute_submit_peer_session_locked(NODE); peer != nullptr) *peer = {};
    s_compute_lock.unlock();
    if (RECEIVER_REF) receiver.release();
    return ok && source.ref_count.load() == 1 && receiver.ref_count.load() == 1 && same_pid.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_child_signal_delivery() -> bool {
    constexpr uint16_t NODE = 0x7B7E;
    constexpr uint32_t ID = 0x7B7E01;
    TaskChildSignal request{.task_id = ID, .hops = 3};
    uint16_t copied = 0;
    auto classify = [&](uint16_t length = sizeof(TaskChildSignal)) {
        return classify_compute_rx_payload(MsgType::TASK_CHILD_SIGNAL, reinterpret_cast<const uint8_t*>(&request), length, &copied);
    };
    bool ok = classify() == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    for (uint16_t length = 0; length < sizeof(request); ++length) ok &= classify(length) == WkiComputeRxAdmission::DISCARD;
    ok &= classify(sizeof(request) + 1) == WkiComputeRxAdmission::DISCARD;
    auto const VALID = request;
    request.task_id = 0;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request = VALID;
    ++request.version;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request = VALID;
    request.hops = WKI_CHILD_SIGNAL_HOPS;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request.hops = 0;
    ok &= classify() == WkiComputeRxAdmission::DEFERRED;
    request = VALID;
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B7E0002;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.hide_group_signals_until_publication();
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;
    PendingComputeRx work{};
    work.hdr.src_node = NODE;
    work.rx_channel = &channel;
    work.rx_channel_generation = 7;
    work.identity = {.boot_epoch = 9,
                     .auth_generation = 1,
                     .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                     .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    std::memcpy(work.payload.data(), &request, sizeof(request));
    work.payload_len = sizeof(request);
    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE) != nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 7, work.identity);
    RunningRemoteTask running{};
    running.active = running.published = true;
    running.task_id = ID;
    running.submitter_node = NODE;
    running.local_pid = target.pid;
    running.submit_session_epoch = SESSION.epoch;
    running.submit_rx_channel = &channel;
    running.submit_rx_channel_generation = 7;
    running.task = &target;
    bool const INSERTED = SESSION.epoch != 0 && publish_running_task_locked(running);
    s_compute_lock.unlock();
    if (INSERTED) {
        ok &= deliver_child_signal(work) == 0 && target.signal_pending_bits() == (uint64_t{1} << 16);
        ok &= deliver_child_signal(work) == 0 && target.signal_pending_bits() == (uint64_t{1} << 16);
        target.signal_clear_pending_mask(~uint64_t{0});
        for (unsigned denied = 0; denied < 3; ++denied) {
            auto const IDENTITY = work.identity;
            if (denied == 0) work.identity.policy &= ~wki_policy_bits(WkiPolicy::COMPUTE);
            if (denied == 1) work.identity.policy &= ~wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN);
            if (denied == 2) work.identity.capabilities = 0;
            ok &= deliver_child_signal(work) == -EACCES && target.signal_pending_bits() == 0;
            work.identity = IDENTITY;
        }
        auto const IDENTITY = work.identity;
        ++work.identity.boot_epoch;
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.identity = IDENTITY;
        ++work.identity.auth_generation;
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.identity = IDENTITY;
        work.identity.auth_session_id[0] ^= 1;
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.identity = IDENTITY;
        work.rx_channel_generation = 6;
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.rx_channel_generation = 7;
        request.task_id++;
        std::memcpy(work.payload.data(), &request, sizeof(request));
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        request = VALID;
        std::memcpy(work.payload.data(), &request, sizeof(request));
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) row.root_completion_queued = true;
        s_compute_lock.unlock();
        ok &= deliver_child_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) {
                row.task = nullptr;
                row.active = false;
            }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();
    }
    s_compute_lock.lock();
    if (auto* peer = find_compute_submit_peer_session_locked(NODE); peer != nullptr) *peer = {};
    s_compute_lock.unlock();
    using Task = ker::mod::sched::task::Task;
    auto queued = std::make_unique<Task[]>(3);
    ChildSignalQueue queue;
    uint16_t hops = 0;
    ok &= queue.push(queued[0], 3) && queue.push(queued[0], 8) && queued[0].ref_count.load() == 2;
    ok &= queue.push(queued[1], 4) && queue.push(queued[2], 5);
    ok &= queue.remove(queued[1]);
    queued[1].release();
    auto* inflight = queue.pop(hops);
    ok &= inflight == &queued[0] && hops == 8 && !queued[0].wki_child_signal_queued;
    ok &= queue.push(queued[0], 2) && queued[0].ref_count.load() == 3;
    ok &= queue.push(queued[0], 8) && queued[0].ref_count.load() == 3;
    if (inflight != nullptr) inflight->release();
    auto* next = queue.pop(hops);
    ok &= next == &queued[2] && hops == 5;
    if (next != nullptr) next->release();
    next = queue.pop(hops);
    ok &= next == &queued[0] && hops == 8;
    if (next != nullptr) next->release();
    ok &= queue.head == nullptr && queue.tail == nullptr && !queue.remove(queued[0]);
    queued[0].exit_in_progress = true;
    ok &= !queue.push(queued[0], 2);
    for (size_t i = 0; i < 3; ++i) ok &= queued[i].ref_count.load() == 1 && !queued[i].wki_child_signal_queued;

    // Real submitted row: same-number Task has no authority, pending ACCEPT
    // defers, channel retirement drops, and no effect appears on the proxy.
    Task source{}, same_pid{};
    source.pid = same_pid.pid = 0x7B7E0003;
    s_compute_lock.lock();
    SubmittedTask submitted;
    submitted.active = submitted.exec_handoff = true;
    submitted.exec_cutover = submitted.exec_prepared = true;
    submitted.exec_remote_pid = target.pid;
    submitted.target_node = NODE;
    submitted.submit_channel = &channel;
    submitted.submit_channel_generation = 7;
    submitted.task_id = allocate_submitted_task_id_locked();
    uint32_t const SUBMIT_ID = submitted.task_id;
    bool const REF = submitted.set_local_task_ref(&source);
    auto* row = REF ? publish_submitted_task_locked(std::move(submitted)) : nullptr;
    s_compute_lock.unlock();
    ok &= row != nullptr;
    if (row != nullptr) {
        source.set_wki_proxy_task_id(SUBMIT_ID);
        auto probe = [&](Task& subject, ChildAuthorityAdmission expected) {
            ChildSignalRoute route{};
            s_compute_lock.lock();
            auto const RESULT = capture_child_signal_route_locked(subject, route);
            s_compute_lock.unlock();
            return RESULT == expected &&
                   (expected == ChildAuthorityAdmission::READY
                        ? route.node == NODE && route.channel == &channel && route.generation == 7 && route.task_id == SUBMIT_ID
                        : route.task_id == 0);
        };
        s_compute_lock.lock();
        row->response_pending.store(true);
        s_compute_lock.unlock();
        ok &= probe(source, ChildAuthorityAdmission::AWAITING_ACCEPT);
        same_pid.publish_accepted_signal_route(SUBMIT_ID, target.pid);
        ok &= probe(same_pid, ChildAuthorityAdmission::UNAVAILABLE);
        source.publish_accepted_signal_route(SUBMIT_ID, target.pid);
        s_compute_lock.lock();
        row->response_pending.store(false);
        row->accept_status = static_cast<uint8_t>(TaskRejectReason::ACCEPTED);
        s_compute_lock.unlock();
        ok &= probe(source, ChildAuthorityAdmission::READY);
        channel.generation = 8;
        ok &= probe(source, ChildAuthorityAdmission::UNAVAILABLE);
        channel.generation = 7;
        source.exit_in_progress = true;
        ok &= probe(source, ChildAuthorityAdmission::UNAVAILABLE);
        source.exit_in_progress = false;
        // Production queue cleanup is idempotent and releases only its own pin.
        wki_defer_child_signal(source, 4);
        ok &= source.ref_count.load() == 3;
        retire_child_signal_for_task(source);
        retire_child_signal_for_task(source);
        ok &= source.ref_count.load() == 2 && source.signal_pending_bits() == 0;
        s_compute_lock.lock();
        remove_submitted_task_for_selftest_locked(SUBMIT_ID);
        s_compute_lock.unlock();
    }
    ok &= source.ref_count.load() == 1;
    return ok && INSERTED && target.ref_count.load() == 1 && target.local_signal_admissions_drained();
}

auto wki_remote_compute_selftest_parent_death_admission() -> bool {
    constexpr uint16_t NODE = 0x7B7B;
    constexpr uint32_t ID = 0x7B7B01;
    TaskParentDeath request{.task_id = ID, .hops = 3};
    uint16_t copied = 0;
    auto classify = [&](uint16_t length = sizeof(TaskParentDeath)) {
        return classify_compute_rx_payload(MsgType::TASK_PARENT_DEATH, reinterpret_cast<const uint8_t*>(&request), length, &copied);
    };
    bool ok = classify() == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    for (uint16_t length = 0; length < sizeof(request); ++length) ok &= classify(length) == WkiComputeRxAdmission::DISCARD;
    ok &= classify(sizeof(request) + 1) == WkiComputeRxAdmission::DISCARD;
    auto const VALID = request;
    request.task_id = 0;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request = VALID;
    ++request.version;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request = VALID;
    request.hops = WKI_PARENT_DEATH_HOPS;
    ok &= classify() == WkiComputeRxAdmission::DISCARD;
    request.hops = 0;
    ok &= classify() == WkiComputeRxAdmission::DEFERRED;
    request = VALID;
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B7B0002;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.hide_group_signals_until_publication();
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;
    PendingComputeRx work{};
    work.hdr.src_node = NODE;
    work.rx_channel = &channel;
    work.rx_channel_generation = 7;
    work.identity = {.boot_epoch = 9,
                     .auth_generation = 1,
                     .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                     .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    std::memcpy(work.payload.data(), &request, sizeof(request));
    work.payload_len = sizeof(request);
    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE) != nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 7, work.identity);
    RunningRemoteTask running{};
    running.active = running.published = true;
    running.task_id = ID;
    running.submitter_node = NODE;
    running.local_pid = target.pid;
    running.submit_session_epoch = SESSION.epoch;
    running.submit_rx_channel = &channel;
    running.submit_rx_channel_generation = 7;
    running.task = &target;
    bool const INSERTED = SESSION.epoch != 0 && publish_running_task_locked(running);
    s_compute_lock.unlock();
    if (INSERTED) {
        for (uint32_t registration : {10U, 0U, 12U}) {
            target.parent_death_signal.store(registration);
            ok &= deliver_parent_death(work) == 0;
            ok &= target.signal_pending_bits() == (registration ? uint64_t{1} << (registration - 1) : 0);
            target.signal_clear_pending_mask(~uint64_t{0});
        }
        work.rx_channel_generation = 6;
        ok &= deliver_parent_death(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.rx_channel_generation = 7;
        request.task_id++;
        std::memcpy(work.payload.data(), &request, sizeof(request));
        ok &= deliver_parent_death(work) == -ESRCH && target.signal_pending_bits() == 0;
        request = VALID;
        std::memcpy(work.payload.data(), &request, sizeof(request));
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) row.root_completion_queued = true;
        s_compute_lock.unlock();
        ok &= deliver_parent_death(work) == -ESRCH && target.signal_pending_bits() == 0;
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) {
                row.task = nullptr;
                row.active = false;
            }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();
    }
    s_compute_lock.lock();
    if (auto* peer = find_compute_submit_peer_session_locked(NODE); peer != nullptr) *peer = {};
    s_compute_lock.unlock();
    return ok && INSERTED && target.ref_count.load() == 1 && target.local_signal_admissions_drained();
}

auto wki_remote_compute_selftest_terminal_signal_admission() -> bool {
    constexpr uint16_t NODE = 0x7B79;
    constexpr uint32_t ID = 0x7B7901;
    TaskTerminalSignal request{.task_id = ID,
                               .signum = 2,
                               .hops = 3,
                               .session_id = 41,
                               .pgid = 42,
                               .origins = {.session = {.node = NODE, .boot = 9}, .group = {.node = NODE, .boot = 9}}};
    uint16_t copied = 0;
    auto classify = [&](uint16_t length = sizeof(TaskTerminalSignal)) {
        return classify_compute_rx_payload(MsgType::TASK_TERMINAL_SIGNAL, reinterpret_cast<const uint8_t*>(&request), length, &copied);
    };
    bool ok = classify() == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    for (uint16_t length = 0; length < sizeof(request); ++length) ok &= classify(length) == WkiComputeRxAdmission::DISCARD;
    ok &= classify(sizeof(request) + 1) == WkiComputeRxAdmission::DISCARD;
    auto const VALID = request;
    for (unsigned mutation = 0; mutation < 12; ++mutation) {
        request = VALID;
        switch (mutation) {
            case 0:
                request.task_id = 0;
                break;
            case 1:
                ++request.version;
                break;
            case 2:
                request.reserved = 1;
                break;
            case 3:
                request.hops = WKI_TERMINAL_SIGNAL_HOPS;
                break;
            case 4:
                request.signum = 0;
                break;
            case 5:
                request.signum = 65;
                break;
            case 6:
                request.session_id = 0;
                break;
            case 7:
                request.pgid = 0;
                break;
            case 8:
                request.origins.session.node = 0;
                break;
            case 9:
                request.origins.session.boot = 0;
                break;
            case 10:
                request.origins.group.node = WKI_NODE_BROADCAST;
                break;
            case 11:
                request.origins.group.reserved = 1;
                break;
        }
        ok &= classify() == WkiComputeRxAdmission::DISCARD;
    }
    request = VALID;
    ker::mod::sched::task::Task target{};
    target.pid = 0x7B790002;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.hide_group_signals_until_publication();
    auto const JOB = ker::mod::sched::session::Identity{
        .session_id = 41, .pgid = 42, .origin = {.node = NODE, .boot = 9}, .group_origin = {.node = NODE, .boot = 9}};
    target.initialize_job_identity(JOB);
    WkiChannel channel{};
    channel.active = true;
    channel.peer_node_id = NODE;
    channel.channel_id = WKI_CHAN_RESOURCE;
    channel.generation = 7;
    PendingComputeRx work{};
    work.hdr.src_node = NODE;
    work.rx_channel = &channel;
    work.rx_channel_generation = 7;
    work.identity = {.boot_epoch = 9,
                     .auth_generation = 1,
                     .policy = wki_policy_bits(WkiPolicy::COMPUTE) | wki_policy_bits(WkiPolicy::IDENTITY_SAME_DOMAIN),
                     .capabilities = WKI_CAP_COMPUTE_CREDENTIALS};
    std::memcpy(work.payload.data(), &request, sizeof(request));
    work.payload_len = sizeof(request);
    s_compute_lock.lock();
    if (find_compute_submit_peer_session_locked(NODE) != nullptr) {
        s_compute_lock.unlock();
        return false;
    }
    auto const SESSION = capture_compute_submit_session_locked(NODE, &channel, 7, work.identity);
    RunningRemoteTask running{};
    running.active = running.published = true;
    running.task_id = ID;
    running.submitter_node = NODE;
    running.local_pid = target.pid;
    running.submit_session_epoch = SESSION.epoch;
    running.submit_rx_channel = &channel;
    running.submit_rx_channel_generation = 7;
    running.task = &target;
    bool const INSERTED = SESSION.epoch != 0 && publish_running_task_locked(running);
    s_compute_lock.unlock();
    if (INSERTED) {
        ok &= deliver_terminal_signal(work) == 0 && target.signal_pending_bits() == 2;
        target.signal_clear_pending_mask(~uint64_t{0});
        auto changed = JOB;
        changed.group_origin.boot++;
        target.initialize_job_identity(changed);
        ok &= deliver_terminal_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        target.initialize_job_identity(JOB);
        work.rx_channel_generation = 6;
        ok &= deliver_terminal_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        work.rx_channel_generation = 7;
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) row.root_completion_queued = true;
        s_compute_lock.unlock();
        ok &= deliver_terminal_signal(work) == -ESRCH && target.signal_pending_bits() == 0;
        s_compute_lock.lock();
        for (auto& row : g_running_remote_tasks)
            if (row.task == &target) {
                row.task = nullptr;
                row.active = false;
            }
        compact_running_remote_tasks_locked();
        s_compute_lock.unlock();
    }
    s_compute_lock.lock();
    if (auto* peer = find_compute_submit_peer_session_locked(NODE); peer != nullptr) *peer = {};
    s_compute_lock.unlock();
    return ok && INSERTED && target.ref_count.load() == 1 && target.local_signal_admissions_drained();
}

auto wki_remote_compute_selftest_group_target_snapshot() -> bool {
    GroupRouteTargets selected;
    auto target = [](uint64_t serial) {
        return GroupRouteTarget{.home = {.origin = {.node = 7, .boot = 11}, .serial = serial}, .node = 9, .boot = 13, .pid = 1000 + serial};
    };
    bool ok = true;
    for (uint64_t i = 1; i <= 32; ++i) ok &= selected.insert(target(i)) == 0;
    ok &= selected.view().size() == 32 && selected.insert(target(33), true) == -ENOMEM && selected.view().size() == 32;
    auto migrated = target(1);
    migrated.node = 17;
    migrated.boot = 19;
    migrated.pid = 2001;
    // A duplicate never allocates or replaces a selected route. This remains
    // true when that route later returns denial or an uncertain transport result.
    ok &= selected.insert(migrated, true) == 0 && selected.view().size() == 32 && selected.view()[0].node == 9 &&
          selected.view()[0].boot == 13 && selected.view()[0].pid == 1001;
    for (uint64_t i = 33; i <= 70; ++i) ok &= selected.insert(target(i)) == 0;
    ok &= selected.view().size() == 70;
    for (size_t i = 0; i < selected.view().size(); ++i) ok &= selected.view()[i].home.serial == i + 1 && selected.view()[i].pid == 1001 + i;
    // Serial alone is insufficient: both node and boot are identity components.
    migrated.home.origin.node = 8;
    ok &= selected.insert(migrated) == 0 && selected.view().size() == 71;
    migrated.home.origin.node = 7;
    migrated.home.origin.boot = 12;
    ok &= selected.insert(migrated) == 0 && selected.view().size() == 72;
    selected.clear();
    selected.clear();
    ok &= selected.view().empty() && selected.insert(target(73), true) == 0 && selected.view().size() == 1 &&
          selected.view()[0].home.serial == 73;
    return ok;
}

auto wki_remote_compute_selftest_group_receivers() -> bool {
    constexpr uint16_t NODE = 0x7B73;
    constexpr uint32_t BOOT = 83;
    constexpr uint32_t GENERATION = 84;
    ker::mod::sched::task::Task receiver{};
    receiver.type = ker::mod::sched::task::TaskType::PROCESS;
    receiver.pid = 77;
    bool ok = receiver.adopt_signal_home({.origin = {.node = 7, .boot = 8}, .serial = 9});
    RunningRemoteTask running{};
    running.active = true;
    running.task = &receiver;
    running.local_pid = receiver.pid;
    GroupRouteRequest enumeration{};
    auto visible = [&]() {
        GroupReceiverResponse page{};
        bool good = append_running_signal_receiver(running, enumeration, page) == 0;
        if (page.count != 0)
            good &= page.count == 1 && page.receivers[0].pid == receiver.pid && page.receivers[0].home.origin.node == 7 &&
                    page.receivers[0].home.origin.boot == 8 && page.receivers[0].home.serial == 9;
        ok &= good;
        return page.count != 0;
    };
    ok &= !visible();
    running.group_signal_ready = true;
    ok &= visible();
    running.published = true;
    running.group_signal_ready = false;
    ok &= visible();
    receiver.is_thread = true;
    ok &= !visible();
    receiver.is_thread = false;
    running.root_completion_queued = true;
    ok &= !visible();
    running.root_completion_queued = false;
    running.discard_completion = true;
    ok &= !visible();
    running.discard_completion = false;
    receiver.publish_accepted_signal_route(0x7B7302, 78);
    ok &= !visible();
    GroupRouteRequest request{.request_id = 0x7B7301, .pgid = 77, .cursor = 2, .ceiling = 9};
    uint16_t copied = 0;
    auto admit = [&](uint16_t length = sizeof(GroupRouteRequest)) {
        return classify_compute_rx_payload(MsgType::GROUP_RECEIVER_REQ, reinterpret_cast<const uint8_t*>(&request), length, &copied);
    };
    ok &= admit() == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    for (uint16_t length = 0; length < sizeof(request); ++length) ok &= admit(length) == WkiComputeRxAdmission::DISCARD;
    ok &= admit(sizeof(request) + 1) == WkiComputeRxAdmission::DISCARD;
    request.operation = static_cast<uint8_t>(GroupRouteOperation::QUERY);
    request.home = {.origin = {.node = NODE, .reserved = 0, .boot = BOOT}, .serial = 1};
    request.target_pid = 5;
    request.cursor = request.ceiling = 0;
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.operation = static_cast<uint8_t>(GroupRouteOperation::DELIVER);
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request = {.request_id = 0x7B7301, .pgid = 77, .cursor = 2, .ceiling = 9};
    WkiChannel channel{}, foreign_channel{};
    ker::mod::sched::task::Task owner{};
    WkiHeader header{.src_node = NODE};
    for (unsigned mode = 0; mode < 4; ++mode) {
        WkiWaitEntry wait{};
        PendingUserSignal* slot = nullptr;
        s_compute_lock.lock();
        for (auto& candidate : s_user_signals)
            if (candidate.owner == nullptr) {
                slot = &candidate;
                break;
            }
        if (slot != nullptr)
            *slot = {.owner = &owner,
                     .response_wait = &wait,
                     .consumer_wait = &wait,
                     .channel = &channel,
                     .request_id = request.request_id,
                     .generation = GENERATION,
                     .node = NODE,
                     .source_boot = BOOT,
                     .receiver_operation = true,
                     .route_request = request};
        s_compute_lock.unlock();
        if (slot == nullptr) return false;
        GroupReceiverResponse response{.request_id = request.request_id, .count = 2, .boot = BOOT, .ceiling = 9};
        response.receivers[0] = {.serial = 3, .pid = 40, .home = {.origin = {.node = 7, .reserved = 0, .boot = 8}, .serial = 9}};
        response.receivers[1] = {.serial = 5, .pid = 41, .home = {.origin = {.node = 9, .reserved = 0, .boot = 10}, .serial = 9}};
        auto receive = [&](WkiChannel* received_channel, uint32_t generation) {
            detail::handle_group_receiver_response(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), received_channel,
                                                   generation);
        };
        receive(&foreign_channel, GENERATION);
        receive(&channel, GENERATION - 1);
        --response.boot;
        receive(&channel, GENERATION);
        ++response.boot;
        ++header.src_node;
        receive(&channel, GENERATION);
        --header.src_node;
        ++response.request_id;
        receive(&channel, GENERATION);
        --response.request_id;
        slot->receiver_operation = false;
        receive(&channel, GENERATION);
        slot->receiver_operation = true;
        auto const VALID = response;
        for (unsigned mutation = 0; mutation < 9; ++mutation) {
            response = VALID;
            switch (mutation) {
                case 0:
                    response.version = 2;
                    break;
                case 1:
                    response.receivers[1].serial = 3;
                    break;
                case 2:
                    response.receivers[0].home.origin.boot = 0;
                    break;
                case 3:
                    response.receivers[0].home.serial = 0;
                    break;
                case 4:
                    response.receivers[2].pid = 42;
                    break;
                case 5:
                    response.ceiling = 10;
                    break;
                case 6:
                    response.count = WKI_GROUP_RECEIVER_PAGE + 1;
                    break;
                case 7:
                    response.result = -EACCES;
                    break;
                case 8:
                    response.reserved = 1;
                    break;
            }
            receive(&channel, GENERATION);
            ok &= wait.state.load() == WkiWaitEntry::PENDING && slot->response_wait == &wait;
        }
        response = VALID;
        if (mode == 1)
            fail_user_signals_for_peer(NODE, RemoteComputeCleanupScope::ALL);
        else if (mode == 2)
            wki_wake_op(&wait, WKI_ERR_TIMEOUT);
        else if (mode == 3)
            response = {.request_id = request.request_id, .result = -EACCES, .boot = BOOT};
        receive(&channel, GENERATION);
        wki_quiesce_claimed_op(&wait);
        ok &= wait.state.load() == WkiWaitEntry::DONE && slot->consumer_wait == &wait;
        if (mode == 0) {
            ok &= slot->result == 0 && slot->receiver_response.count == 2 && slot->receiver_response.receivers[1].home.origin.node == 9;
            response = {.request_id = request.request_id, .result = -EPERM, .boot = BOOT};
            receive(&channel, GENERATION);
            ok &= slot->result == 0;
        } else if (mode == 1)
            ok &= wait.result == WKI_ERR_PEER_FENCED;
        else if (mode == 2)
            ok &= wait.result == WKI_ERR_TIMEOUT && slot->result == -EIO;
        else
            ok &= slot->result == -EACCES && slot->receiver_response.count == 0;
        retire_user_signals_for_task(&owner);
        receive(&channel, GENERATION);
        ok &= slot->owner == nullptr && slot->consumer_wait == nullptr;
    }
    return ok && owner.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_group_route() -> bool {
    constexpr uint16_t NODE = 0x7B72;
    constexpr uint32_t BOOT = 81;
    constexpr uint32_t GENERATION = 82;
    uint16_t copied = 0;
    GroupRouteRequest request{.request_id = 0x7B7201, .pgid = 77};
    auto admit = [&]() {
        return classify_compute_rx_payload(MsgType::GROUP_ROUTE_REQ, reinterpret_cast<const uint8_t*>(&request), sizeof(request), &copied);
    };
    bool ok = admit() == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    request.reserved = 1;
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.reserved = 0;
    request.operation = static_cast<uint8_t>(GroupRouteOperation::DELIVER);
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.home = {.origin = {.node = NODE, .boot = BOOT}, .serial = 4};
    request.target_pid = 17;
    ok &= admit() == WkiComputeRxAdmission::DEFERRED;
    request.home.origin.boot = 0;
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.home.origin.boot = BOOT;
    request.cursor = 1;
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.cursor = 0;
    request.operation = static_cast<uint8_t>(GroupRouteOperation::QUERY);
    request.signum = 12;
    ok &= admit() == WkiComputeRxAdmission::DISCARD;
    request.signum = 0;
    WkiChannel channel{};
    ker::mod::sched::task::Task owner{};
    WkiHeader header{.src_node = NODE};
    auto exercise = [&](GroupRouteOperation operation) {
        WkiWaitEntry wait{};
        PendingUserSignal* slot = nullptr;
        s_compute_lock.lock();
        for (auto& candidate : s_user_signals)
            if (candidate.owner == nullptr) {
                slot = &candidate;
                break;
            }
        if (slot != nullptr) {
            *slot = {
                .owner = &owner,
                .response_wait = &wait,
                .consumer_wait = &wait,
                .channel = &channel,
                .request_id = request.request_id,
                .generation = GENERATION,
                .node = NODE,
                .source_boot = BOOT,
                .route_operation = true,
                .route_request = {
                    .request_id = request.request_id, .operation = static_cast<uint8_t>(operation), .pgid = 77, .cursor = 2, .ceiling = 9}};
        }
        s_compute_lock.unlock();
        if (slot == nullptr) return false;
        GroupRouteResponse response{.request_id = request.request_id, .boot = BOOT};
        if (operation == GroupRouteOperation::ENUMERATE) {
            response.count = 2;
            response.ceiling = 9;
            response.anchors[0] = {.serial = 3, .pid = 40};
            response.anchors[1] = {.serial = 5, .pid = 41};
        } else if (operation == GroupRouteOperation::QUERY) {
            response.target_pgid = 77;
            response.group_origin = {.node = NODE, .boot = BOOT};
        }
        auto receive = [&](uint32_t generation = GENERATION) {
            detail::handle_group_route_response(&header, reinterpret_cast<const uint8_t*>(&response), sizeof(response), &channel,
                                                generation);
        };
        receive(GENERATION - 1);
        response.boot--;
        receive();
        response.boot++;
        response.reserved = 1;
        receive();
        response.reserved = 0;
        bool good = wait.state.load() == WkiWaitEntry::PENDING;
        if (operation == GroupRouteOperation::ENUMERATE) {
            response.anchors[1].serial = 3;
            receive();
            good &= wait.state.load() == WkiWaitEntry::PENDING;
            response.anchors[1].serial = 5;
        }
        receive();
        wki_quiesce_claimed_op(&wait);
        good &=
            wait.state.load() == WkiWaitEntry::DONE && slot->result == 0 && slot->response_wait == nullptr && slot->consumer_wait == &wait;
        response.result = -EPERM;
        receive();
        good &= slot->result == 0;
        s_compute_lock.lock();
        *slot = {};
        s_compute_lock.unlock();
        return good;
    };
    ok &= exercise(GroupRouteOperation::ENUMERATE) && exercise(GroupRouteOperation::QUERY) && exercise(GroupRouteOperation::DELIVER);
    return ok && owner.ref_count.load() == 1;
}

auto wki_remote_compute_selftest_group_signal_ownership() -> bool {
    using ker::mod::sched::session::GroupCandidates;
    using ker::mod::sched::task::Task;
    GroupSignalRequest valid{.request_id = 1, .version = WKI_GROUP_SIGNAL_VERSION, .pgid = 77, .caller_pgid = 77};
    auto request = valid;
    uint16_t copied = 0;
    auto admit = [&](uint16_t length) {
        return classify_compute_rx_payload(MsgType::GROUP_SIGNAL_REQ, reinterpret_cast<const uint8_t*>(&request), length, &copied);
    };
    bool ok = admit(sizeof(request)) == WkiComputeRxAdmission::DEFERRED && copied == sizeof(request);
    for (uint16_t length = 0; length < sizeof(request); ++length) ok &= admit(length) == WkiComputeRxAdmission::DISCARD;
    ok &= admit(sizeof(request) + 1) == WkiComputeRxAdmission::DISCARD;
    for (unsigned mutation = 0; mutation < 10; ++mutation) {
        request = valid;
        switch (mutation) {
            case 0:
                request.request_id = 0;
                break;
            case 1:
                request.version = 2;
                break;
            case 2:
                request.reserved = 1;
                break;
            case 3:
                request.operation = 2;
                break;
            case 4:
                request.signum = 10;
                break;  // QUERY cannot deliver.
            case 5:
                request.pgid = 0;
                break;
            case 6:
                request.session_origin.reserved = 1;
                break;
            case 7:
                request.group_origin = {.node = 7};
                break;
            case 8:
                request.group_origin = {.node = WKI_NODE_BROADCAST, .boot = 1};
                break;
            case 9:
                request.operation = 1;
                request.signum = 65;
                break;
        }
        ok &= admit(sizeof(request)) == WkiComputeRxAdmission::DISCARD;
    }
    request = valid;
    request.operation = static_cast<uint8_t>(GroupSignalOperation::DELIVER);
    request.signum = 10;
    ok &= admit(sizeof(request)) == WkiComputeRxAdmission::DEFERRED;

    Task owner{};
    WkiChannel channel{};
    PendingUserSignal* slot = nullptr;
    s_compute_lock.lock();
    for (auto& candidate : s_user_signals)
        if (candidate.owner == nullptr) {
            slot = &candidate;
            break;
        }
    s_compute_lock.unlock();
    if (slot == nullptr) return false;
    constexpr uint16_t NODE = 0x7B51;
    for (unsigned mode = 0; mode < 4; ++mode) {
        WkiWaitEntry wait{};
        s_compute_lock.lock();
        *slot = {.owner = &owner,
                 .response_wait = &wait,
                 .consumer_wait = &wait,
                 .channel = &channel,
                 .request_id = 0xABCDEF,
                 .generation = 27,
                 .node = NODE,
                 .group_operation = true,
                 .group_query = true,
                 .source_boot = 7};
        s_compute_lock.unlock();
        WkiHeader hdr{};
        hdr.src_node = NODE;
        GroupSignalResponse response{
            .request_id = 0xABCDEF, .result = 0, .version = WKI_GROUP_SIGNAL_VERSION, .flags = GroupCandidates::OTHER};
        auto deliver = [&](uint32_t generation) {
            detail::handle_group_signal_response(&hdr, reinterpret_cast<const uint8_t*>(&response), sizeof(response), &channel, generation);
        };
        deliver(26);
        hdr.src_node = NODE + 1;
        deliver(27);
        hdr.src_node = NODE;
        ++response.request_id;
        deliver(27);
        --response.request_id;
        response.reserved = 1;
        deliver(27);
        response.reserved = 0;
        response.version = 2;
        deliver(27);
        response.version = WKI_GROUP_SIGNAL_VERSION;
        response.other.reserved = 1;
        deliver(27);
        response.other.reserved = 0;
        response.flags = 16;
        deliver(27);
        response.flags = GroupCandidates::OWN;
        deliver(27);  // No own alias was admitted.
        response.flags = GroupCandidates::OTHER;
        response.result = -EPERM;
        deliver(27);
        response.result = 0;  // Error cannot claim group observations.
        slot->group_query = false;
        deliver(27);
        slot->group_query = true;
        slot->group_operation = false;
        deliver(27);
        slot->group_operation = true;
        ok &= wait.state.load() == WkiWaitEntry::PENDING && slot->response_wait == &wait && slot->groups.flags == 0;
        if (mode == 0) {
            deliver(27);
            ok &= wait.state.load() == WkiWaitEntry::DONE && slot->result == 0 && slot->groups.flags == GroupCandidates::OTHER &&
                  slot->groups.other.node == NODE && slot->groups.other.boot == 7;
            response.flags = GroupCandidates::LOCAL;
            deliver(27);
            ok &= slot->groups.flags == GroupCandidates::OTHER;
        } else if (mode == 1) {
            fail_user_signals_for_peer(NODE, RemoteComputeCleanupScope::ALL);
            ok &= wait.state.load() == WkiWaitEntry::DONE && wait.result == WKI_ERR_PEER_FENCED;
        } else if (mode == 2) {
            wki_wake_op(&wait, WKI_ERR_TIMEOUT);
            deliver(27);
            ok &= wait.result == WKI_ERR_TIMEOUT && slot->groups.flags == 0;
        }
        retire_user_signals_for_task(&owner);
        ok &= slot->owner == nullptr && slot->consumer_wait == nullptr;
        if (mode == 3) ok &= wait.state.load() == WkiWaitEntry::DONE && wait.result == WKI_ERR_PEER_FENCED;
        deliver(27);
        ok &= slot->owner == nullptr;
    }
    return ok;
}
#endif

}  // namespace ker::net::wki
