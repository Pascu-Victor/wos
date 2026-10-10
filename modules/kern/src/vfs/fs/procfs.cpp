#include "procfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/ahci.hpp>
#include <dev/block_request.hpp>
#include <dev/virtio/virtio_net.hpp>
#include <memory>
#include <minimalist_malloc/mini_malloc.hpp>
#include <net/backlog.hpp>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/tcp.hpp>
#include <new>
#include <optional>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/ktime/process_timer.hpp>
#include <platform/loader/runtime_images.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/memacc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <platform/mm/reclaim_policy.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/preemption_diagnostics.hpp>
#include <platform/sched/preemption_policy.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/resource_process.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/authorization.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/usercopy.hpp>
#include <string_view>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>
#include <vfs/advisory.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "net/wki/auth.hpp"
#include "net/wki/blk_ring.hpp"
#include "net/wki/chaos.hpp"
#include "net/wki/chaos_model.hpp"
#include "net/wki/chaos_workload.hpp"
#include "net/wki/dev_proxy.hpp"
#include "net/wki/dev_server.hpp"
#include "net/wki/remotable.hpp"
#include "net/wki/remote_compute.hpp"
#include "net/wki/remote_ipc.hpp"
#include "net/wki/remote_net.hpp"
#include "net/wki/remote_vfs.hpp"
#include "net/wki/routing.hpp"
#include "net/wki/secure_rdma.hpp"
#include "net/wki/wire.hpp"
#include "net/wki/wki.hpp"
#include "platform/mm/paging.hpp"
#include "platform/sched/scheduler.hpp"
#include "release.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/filesystem_context.hpp"

namespace ker::vfs::procfs {

#ifdef WOS_SELFTEST
void (*mounts_snapshot_test_hook)(ker::mod::sched::task::Task*) = nullptr;
#endif

namespace {

using ker::mod::security::DiagnosticAccess;
using ProcTask = ker::mod::sched::task::Task;
namespace pid_ns = ker::mod::sched::pid_ns;

constexpr size_t MAX_WKI_NAPI_BUF = 65536;

constexpr auto diagnostic_access(ProcNodeType type) -> DiagnosticAccess {
    switch (type) {
        case ProcNodeType::ROOT_DIR:
        case ProcNodeType::PID_DIR:
        case ProcNodeType::TASK_DIR:
        case ProcNodeType::TASK_TID_DIR:
        case ProcNodeType::SELF_LINK:
        case ProcNodeType::EXE_LINK:
        case ProcNodeType::CWD_LINK:
        case ProcNodeType::ROOT_LINK:
        case ProcNodeType::FD_DIR:
        case ProcNodeType::FD_LINK:
        case ProcNodeType::STATUS_FILE:
        case ProcNodeType::MOUNTS_FILE:
        case ProcNodeType::STAT_FILE:
        case ProcNodeType::STATM_FILE:
        case ProcNodeType::CMDLINE_FILE:
        case ProcNodeType::MAPS_FILE:
        case ProcNodeType::IMAGES_FILE:
        case ProcNodeType::UPTIME_FILE:
        case ProcNodeType::TIMEKEEPING_FILE:
        case ProcNodeType::CPU_STAT_FILE:
        case ProcNodeType::LOADAVG_FILE:
        case ProcNodeType::MEMINFO_FILE:
        case ProcNodeType::VERSION_FILE:
        case ProcNodeType::KCPUSTAT_FILE:
        case ProcNodeType::WKI_LAUNCHER_FILE:
        case ProcNodeType::WKI_RUNNER_FILE:
        case ProcNodeType::WKI_REMOTE_PID_FILE:
        case ProcNodeType::WKI_DIR:
        case ProcNodeType::WKI_PEERS_FILE:
        case ProcNodeType::WKI_AUTH_FILE:
        case ProcNodeType::MEMACC_DIR:
        case ProcNodeType::MEMACC_TRACK_DIR:
        case ProcNodeType::MEMACC_RECLAIM_DIR:
        case ProcNodeType::MEMACC_SUMMARY_FILE:
        case ProcNodeType::MEMACC_ZONES_FILE:
        case ProcNodeType::MEMACC_ALLOC_TOTALS_FILE:
        case ProcNodeType::MEMACC_FEATURES_FILE:
        case ProcNodeType::MEMACC_RECLAIM_COORDINATOR_FILE:
        case ProcNodeType::LIMITS_FILE:
            return DiagnosticAccess::PUBLIC_READ;
        case ProcNodeType::KPERFSTATUS_FILE:
        case ProcNodeType::KPERF_FILE:
        case ProcNodeType::KWKISTAT_FILE:
        case ProcNodeType::KCPUSTATE_FILE:
        case ProcNodeType::KCONTSTAT_FILE:
        case ProcNodeType::KIPCSTAT_FILE:
        case ProcNodeType::WKI_NETDIAG_FILE:
        case ProcNodeType::WKI_NAPI_FILE:
        case ProcNodeType::BLOCK_QUEUE_FILE:
        case ProcNodeType::WKI_PIPES_FILE:
        case ProcNodeType::MEMACC_PROCS_FILE:
        case ProcNodeType::MEMACC_DEAD_FILE:
        case ProcNodeType::MEMACC_SLABS_FILE:
        case ProcNodeType::MEMACC_KMALLOC_LIVE_FILE:
        case ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE:
        case ProcNodeType::MEMACC_PAGE_CALLERS_FILE:
            return DiagnosticAccess::KERNEL_READ;
        case ProcNodeType::KPERFCTL_FILE:
        case ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE:
        case ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE:
        case ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE:
        case ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE:
        case ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE:
        case ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE:
        case ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE:
            return DiagnosticAccess::KERNEL_CONTROL;
        case ProcNodeType::WKI_CHAOS_FILE:
        case ProcNodeType::WKI_CHAOS_WORKLOAD_FILE:
            return DiagnosticAccess::WKI_CONTROL;
        default:
            return DiagnosticAccess::DENY;
    }
}

auto current_procfs_credentials() -> ker::mod::security::Snapshot {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return task != nullptr ? task->credentials() : ker::mod::security::Snapshot{};
}

auto current_procfs_pid() -> uint64_t {
    auto* caller = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return caller != nullptr ? ker::mod::sched::task::process_pid(*caller) : 0;
}

constexpr auto private_procfs_node(ProcNodeType type) -> bool {
    switch (type) {
        case ProcNodeType::FD_DIR:
        case ProcNodeType::FD_LINK:
        case ProcNodeType::EXE_LINK:
        case ProcNodeType::CWD_LINK:
        case ProcNodeType::ROOT_LINK:
        case ProcNodeType::CMDLINE_FILE:
        case ProcNodeType::MAPS_FILE:
        case ProcNodeType::IMAGES_FILE:
            return true;
        default:
            return false;
    }
}

constexpr auto private_view_node(ProcNodeType type) -> bool {
    switch (type) {
        case ProcNodeType::ROOT_DIR:
        case ProcNodeType::PID_DIR:
        case ProcNodeType::TASK_DIR:
        case ProcNodeType::TASK_TID_DIR:
        case ProcNodeType::SELF_LINK:
        case ProcNodeType::EXE_LINK:
        case ProcNodeType::CWD_LINK:
        case ProcNodeType::ROOT_LINK:
        case ProcNodeType::FD_DIR:
        case ProcNodeType::FD_LINK:
        case ProcNodeType::STATUS_FILE:
        case ProcNodeType::MOUNTS_FILE:
        case ProcNodeType::STAT_FILE:
        case ProcNodeType::STATM_FILE:
        case ProcNodeType::CMDLINE_FILE:
        case ProcNodeType::MAPS_FILE:
        case ProcNodeType::IMAGES_FILE:
        case ProcNodeType::LIMITS_FILE:
        case ProcNodeType::WKI_LAUNCHER_FILE:
        case ProcNodeType::WKI_RUNNER_FILE:
        case ProcNodeType::WKI_REMOTE_PID_FILE:
        case ProcNodeType::UPTIME_FILE:
        case ProcNodeType::TIMEKEEPING_FILE:
        case ProcNodeType::CPU_STAT_FILE:
        case ProcNodeType::LOADAVG_FILE:
        case ProcNodeType::MEMINFO_FILE:
        case ProcNodeType::VERSION_FILE:
            return true;
        default:
            return false;
    }
}

auto task_in_pid_view(const ProcTask& task, const pid_ns::NamespaceRef& view) -> bool {
    if (view.is_host()) return true;
    auto const* scope = task.pid_namespace();
    return scope != nullptr && *scope == view && task.namespace_tid() != 0;
}

// Bounded value observation only: safe at retained VFS result publication.
// No registry join, allocation, blocking, File release or remote discovery.
auto procfs_view_policy(const ProcFileData& data) -> int {
    if (data.pid_view.is_host()) return 0;
    if (data.pid_view.closed()) return -EIDRM;
    if (!private_view_node(data.node.type)) return -ENOTSUP;
    if (data.target != nullptr) {
        if (!task_in_pid_view(*data.target, data.pid_view)) return -ESRCH;
        auto const* owner = data.target->credential_owner();
        if (owner == nullptr) return -ESRCH;
        auto const IDENTITY = owner->wki_execution_identity();
        auto const THREAD_IDENTITY = data.target->wki_execution_identity();
        if (owner->wki_proxy_task || data.target->wki_proxy_task || IDENTITY.remote_pid != 0 || IDENTITY.proxy_task_id != 0 ||
            IDENTITY.signal_task_id != 0 || THREAD_IDENTITY.remote_pid != 0 || THREAD_IDENTITY.proxy_task_id != 0 ||
            THREAD_IDENTITY.signal_task_id != 0)
            return -ENOTSUP;
    }
    return data.pid_view.closed() ? -EIDRM : 0;
}

auto authorize_private_procfs(const ker::mod::security::Snapshot& caller, uint64_t caller_pid, const ProcFileData& data,
                              ker::mod::security::CredentialStore::Observation& observation) -> int {
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    if (data.target == nullptr || data.target->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE)
        return -ESRCH;
    observation = data.target->credential_observation();
    bool const SAME_PROCESS = caller_pid != 0 && caller_pid == ker::mod::sched::task::process_pid(*data.target);
    return ker::mod::security::may_access_process_private(caller, observation.credentials, SAME_PROCESS) ? 0 : -EACCES;
}

auto revalidate_private_procfs(const ProcFileData& data, const ker::mod::security::CredentialStore::Observation& before) -> int {
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    if (data.target == nullptr || data.target->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE)
        return -ESRCH;
    return data.target->credential_observation().generation == before.generation ? 0 : -EAGAIN;
}

constexpr uint64_t NS_PER_SEC = 1000000000ULL;
constexpr uint64_t NS_PER_US = 1000ULL;
constexpr uint64_t PROC_CLK_TCK = 100ULL;
constexpr uint64_t US_PER_PROC_TICK = 1000000ULL / PROC_CLK_TCK;
constexpr uint64_t KIB = 1024ULL;

enum class ProcRootReaddirIndex : uint8_t {
    SELF = 2,
    MOUNTS,
    UPTIME,
    VERSION,
    STAT,
    LOADAVG,
    MEMINFO,
    WKI,
    MEMACC,
    TIMEKEEPING,
    BLOCKQ,
    FIRST_PID,
};

constexpr auto proc_root_readdir_index(ProcRootReaddirIndex index) -> size_t { return static_cast<size_t>(index); }

constexpr auto proc_root_readdir_next_index(ProcRootReaddirIndex index) -> uint64_t {
    return static_cast<uint64_t>(proc_root_readdir_index(index) + 1);
}

uint64_t g_procfs_creation_epoch_ns = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Helper: int to decimal string
void int_to_str(uint64_t val, char* buf, size_t bufsz) {
    if (bufsz == 0) {
        return;
    }
    std::array<char, 24> tmp{};
    int pos = 0;
    if (val == 0) {
        tmp[pos++] = '0';
    } else {
        while (val > 0 && pos < 22) {
            tmp[pos++] = static_cast<char>('0' + static_cast<unsigned char>(val % 10));
            val /= 10;
        }
    }
    // reverse
    int const LEN = pos;
    for (int i = 0; i < LEN && static_cast<size_t>(i) < bufsz - 1; ++i) {
        buf[i] = tmp[LEN - 1 - i];
    }
    buf[std::cmp_less(LEN, bufsz) ? LEN : static_cast<int>(bufsz - 1)] = '\0';
}

auto proc_task_state_char(const ProcTask& task) -> char {
    auto const TASK_STATE = task.state.load(std::memory_order_acquire);
    if (TASK_STATE == ker::mod::sched::task::TaskState::DEAD || TASK_STATE == ker::mod::sched::task::TaskState::EXITING ||
        task.has_exited) {
        return 'Z';
    }
    if (task.sched_queue == ProcTask::sched_queue::RUNNABLE) {
        return 'R';
    }
    if (task.sched_queue == ProcTask::sched_queue::WAITING) {
        return task.wait_channel != nullptr ? 'D' : 'S';
    }
    return 'S';
}

auto proc_task_state_label(char state) -> const char* {
    switch (state) {
        case 'Z':
            return "Z (zombie)";
        case 'R':
            return "R (running)";
        case 'D':
            return "D (blocked)";
        default:
            return "S (sleeping)";
    }
}

struct ProcTaskStats {
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    uint64_t child_user_time_us{};
    uint64_t child_system_time_us{};
    uint64_t thread_count{1};
    char state{'S'};
};

auto collect_proc_task_stats(ProcTask& task, bool thread_view, bool local_view = false) -> ProcTaskStats {
    ProcTaskStats stats{
        .user_time_us = task.user_time_us,
        .system_time_us = task.system_time_us,
        .thread_count = 1,
        .state = proc_task_state_char(task),
    };
    if (thread_view || task.is_thread || task.type != ker::mod::sched::task::TaskType::PROCESS) {
        return stats;
    }

    ker::mod::sched::resource::CpuObservation observed{};
    if (ker::mod::sched::resource::copy_cpu_observation(task, observed)) {
        stats.user_time_us = observed.user_time_us;
        stats.system_time_us = observed.system_time_us;
        stats.child_user_time_us = observed.child_user_time_us;
        stats.child_system_time_us = observed.child_system_time_us;
    }
    // Registry membership determines live state/count, never lifetime CPU.
    uint64_t const GROUP_PID = ker::mod::sched::task::process_pid(task);
    uint64_t thread_count = 0;
    bool any_running = false;
    bool any_blocked = false;
    bool any_sleeping = false;
    bool any_zombie = false;

    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* candidate = ker::mod::sched::get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::same_thread_group(*candidate, GROUP_PID) ||
            (local_view &&
             (!ker::mod::sched::task::same_pid_namespace(*candidate, task) || candidate->credential_owner() != task.credential_owner()))) {
            candidate->release();
            continue;
        }

        thread_count++;
        char const STATE = proc_task_state_char(*candidate);
        any_running = any_running || STATE == 'R';
        any_blocked = any_blocked || STATE == 'D';
        any_sleeping = any_sleeping || STATE == 'S';
        any_zombie = any_zombie || STATE == 'Z';
        candidate->release();
    }

    if (thread_count == 0) {
        return stats;
    }

    stats.thread_count = thread_count;
    if (any_running) {
        stats.state = 'R';
    } else if (any_blocked) {
        stats.state = 'D';
    } else if (any_sleeping) {
        stats.state = 'S';
    } else if (any_zombie) {
        stats.state = 'Z';
    }
    return stats;
}

auto process_visible_task_at(size_t index, const pid_ns::NamespaceRef* view = nullptr) -> ProcTask* {
    size_t seen = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::process_visible(*task) || (view != nullptr && !task_in_pid_view(*task, *view))) {
            task->release();
            continue;
        }
        if (seen == index) {
            return task;
        }
        seen++;
        task->release();
    }
    return nullptr;
}

auto thread_group_task_at(uint64_t group_pid, size_t index, const pid_ns::NamespaceRef* view = nullptr, const ProcTask* group = nullptr)
    -> ProcTask* {
    size_t seen = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::same_thread_group(*task, group_pid) ||
            (view != nullptr && !view->is_host() &&
             (!task_in_pid_view(*task, *view) || group == nullptr || task->credential_owner() != group->credential_owner()))) {
            task->release();
            continue;
        }
        if (seen == index) {
            return task;
        }
        seen++;
        task->release();
    }
    return nullptr;
}

auto task_group_pid_for(uint64_t pid, uint64_t& group_pid) -> bool {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return false;
    }
    group_pid = ker::mod::sched::task::process_pid(*task);
    task->release();
    return true;
}

auto task_fd_file_retain(ProcTask* task, uint64_t fd) -> File* {
    if (task == nullptr || fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return nullptr;
    }
    return ker::vfs::vfs_get_file_retain(task, static_cast<int>(fd));
}

auto authorized_task_fd_retain(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, ProcTask& target, uint64_t fd,
                               int& result) -> File* {
    auto const BEFORE = target.credential_observation();
    bool const SAME_PROCESS = caller_pid != 0 && ker::mod::sched::task::process_pid(target) == caller_pid;
    if (!ker::mod::security::may_access_process_private(credentials, BEFORE.credentials, SAME_PROCESS)) {
        result = -EACCES;
        return nullptr;
    }
    auto* file = task_fd_file_retain(&target, fd);
    auto const AFTER = target.credential_observation();
    if (AFTER.generation != BEFORE.generation) {
        if (file != nullptr) ker::vfs::vfs_put_file(file);
        result = -EAGAIN;
        return nullptr;
    }
    result = file != nullptr ? 0 : -ENOENT;
    return file;
}

auto task_fd_at(ProcTask& target, size_t index, uint64_t& fd_out) -> bool {
    // A thread's process owner is retained for its entire lifetime.
    auto* task = target.is_thread ? target.resource_owner.get() : &target;
    if (task == nullptr) return false;
    size_t seen = 0;
    bool found = false;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    for (uint64_t fd = 0; fd < ProcTask::FD_TABLE_SIZE; ++fd) {
        if (task->fd_table.lookup(fd) == nullptr) continue;
        if (seen == index) {
            fd_out = fd;
            found = true;
            break;
        }
        ++seen;
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);
    return found;
}

auto parse_fd_component(const char* s, uint64_t& fd_out) -> bool {
    if (s == nullptr || s[0] == '\0') {
        return false;
    }
    uint64_t fd = 0;
    for (const char* p = s; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(*p - '0');
        if (fd > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        fd = (fd * 10) + DIGIT;
    }
    if (fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return false;
    }
    fd_out = fd;
    return true;
}

auto procfs_copy_link_target(const char* target, char* buf, size_t bufsz) -> ssize_t {
    if (target == nullptr) {
        return -ENOENT;
    }
    size_t len = std::strlen(target);
    len = std::min(len, bufsz);
    for (size_t i = 0; i < len; ++i) {
        buf[i] = target[i];
    }
    return static_cast<ssize_t>(len);
}

auto procfs_copy_task_visible_path(const char* root, const char* path, char* buf, size_t bufsz) -> ssize_t {
    if (path == nullptr || path[0] == '\0') {
        return -ENOENT;
    }

    const char* target = path;
    size_t const ROOT_LEN = std::strlen(root);
    size_t const PATH_LEN = std::strlen(path);
    if (ROOT_LEN > 1 && PATH_LEN >= ROOT_LEN && std::strncmp(path, root, ROOT_LEN) == 0 &&
        (path[ROOT_LEN] == '\0' || path[ROOT_LEN] == '/')) {
        target = path + ROOT_LEN;
        if (target[0] == '\0') {
            target = "/";
        }
    }
    return procfs_copy_link_target(target, buf, bufsz);
}

void fill_numeric_dirent(DirEntry* buf, size_t count, uint64_t pid) {
    buf->d_ino = pid + 100;
    buf->d_off = count + 1;
    buf->d_reclen = sizeof(DirEntry);
    buf->d_type = DT_DIR;
    int_to_str(pid, buf->d_name.data(), buf->d_name.size());
}

void fill_fd_dirent(DirEntry* buf, size_t count, uint64_t fd) {
    buf->d_ino = 100000 + fd;
    buf->d_off = count + 1;
    buf->d_reclen = sizeof(DirEntry);
    buf->d_type = DT_LNK;
    int_to_str(fd, buf->d_name.data(), buf->d_name.size());
}

auto procfs_readdir_private(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const ProcFileData& data, DirEntry* buf,
                            size_t count) -> int {
    ker::mod::security::CredentialStore::Observation before{};
    int const POLICY = authorize_private_procfs(credentials, caller_pid, data, before);
    if (POLICY != 0) return POLICY;
    DirEntry staged{};
    int result = 0;
    if (count < 2) {
        staged.d_ino = 1;
        staged.d_off = count + 1;
        staged.d_reclen = sizeof(DirEntry);
        staged.d_type = DT_DIR;
        staged.d_name[0] = '.';
        if (count == 1) staged.d_name[1] = '.';
    } else {
        uint64_t fd = 0;
        if (task_fd_at(*data.target, count - 2, fd))
            fill_fd_dirent(&staged, count, fd);
        else
            result = -ENOENT;
    }
    int const VALID = revalidate_private_procfs(data, before);
    if (VALID != 0) return VALID;
    if (result == 0) *buf = staged;
    return result;
}

auto procfs_readdir_in_view(File* f, DirEntry* buf, size_t count) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    if (pfd->node.type == ProcNodeType::FD_DIR)
        return procfs_readdir_private(current_procfs_credentials(), current_procfs_pid(), *pfd, buf, count);

    // Synthetic . and ..
    if (count == 0) {
        buf->d_ino = 1;
        buf->d_off = 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '\0';
        return 0;
    }
    if (count == 1) {
        buf->d_ino = 1;
        buf->d_off = 2;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '.';
        buf->d_name[2] = '\0';
        return 0;
    }

    if (pfd->node.type == ProcNodeType::ROOT_DIR) {
        // Private cookies are dense: unavailable global trees cannot terminate
        // iteration before scoped process names. Host keeps its existing order.
        if (!pfd->pid_view.is_host()) {
            struct Entry {
                const char* name;
                uint8_t type;
            };
            constexpr std::array<Entry, 8> ENTRIES{{
                {"self", DT_LNK},
                {"mounts", DT_REG},
                {"uptime", DT_REG},
                {"version", DT_REG},
                {"stat", DT_REG},
                {"loadavg", DT_REG},
                {"meminfo", DT_REG},
                {"timekeeping", DT_REG},
            }};
            if (count - 2 < ENTRIES.size()) {
                auto const& entry = ENTRIES[count - 2];
                buf->d_ino = count;
                buf->d_off = count + 1;
                buf->d_reclen = sizeof(DirEntry);
                buf->d_type = entry.type;
                std::strcpy(buf->d_name.data(), entry.name);
                return 0;
            }
            auto* task = process_visible_task_at(count - 2 - ENTRIES.size(), &pfd->pid_view);
            if (task == nullptr) return -ENOENT;
            uint64_t const PID = task->namespace_tid();
            task->release();
            fill_numeric_dirent(buf, count, PID);
            return 0;
        }
        // /proc root: fixed entries then PID directories
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::SELF)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::SELF);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::SELF);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "self", 5);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::MOUNTS)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::MOUNTS);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::MOUNTS);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "mounts", 7);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::TIMEKEEPING)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::TIMEKEEPING);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::TIMEKEEPING);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "timekeeping", 12);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::UPTIME)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::UPTIME);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::UPTIME);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "uptime", 7);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::VERSION)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::VERSION);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::VERSION);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "version", 8);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::STAT)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::STAT);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::STAT);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "stat", 5);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::LOADAVG)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::LOADAVG);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::LOADAVG);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "loadavg", 8);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::MEMINFO)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::MEMINFO);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::MEMINFO);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "meminfo", 8);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::WKI)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::WKI);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::WKI);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "wki", 4);
            return 0;
        }
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::MEMACC)) {
            buf->d_ino = 22;
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::MEMACC);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "memacc", 7);
            return 0;
        }
        // PID directories from active task list. Threads and WKI proxy tasks
        // stay hidden here; real threads are exposed under /proc/<pid>/task/<tid>.
        if (count == proc_root_readdir_index(ProcRootReaddirIndex::BLOCKQ)) {
            buf->d_ino = proc_root_readdir_index(ProcRootReaddirIndex::BLOCKQ);
            buf->d_off = proc_root_readdir_next_index(ProcRootReaddirIndex::BLOCKQ);
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "blockq", 7);
            return 0;
        }
        size_t const PID_INDEX = count - proc_root_readdir_index(ProcRootReaddirIndex::FIRST_PID);
        auto* task = process_visible_task_at(PID_INDEX);
        if (task == nullptr) {
            return -ENOENT;
        }
        uint64_t const PID = task->pid;
        task->release();
        fill_numeric_dirent(buf, count, PID);
        return 0;
    }

    if (pfd->node.type == ProcNodeType::PID_DIR || pfd->node.type == ProcNodeType::TASK_TID_DIR) {
        // /proc/<pid> and /proc/<pid>/task/<tid>: index 2 = "stat", 3 = "status", 4 = "statm",
        // 5 = "cmdline", 6 = "exe", 7 = "cwd", 8 = "root", 9 = "fd", 10 = "wki_launcher",
        // 11 = "wki_runner", 12 = "wki_remote_pid", 13 = "maps". Top-level process dirs
        // additionally expose "task" at index 14 and append "images" at index 15;
        // thread directories append their image-catalog alias at index 14.
        if (count == 2) {
            buf->d_ino = 10;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "stat", 5);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 11;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "status", 7);
            return 0;
        }
        if (count == 4) {
            buf->d_ino = 12;
            buf->d_off = 5;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "statm", 6);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 13;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "cmdline", 8);
            return 0;
        }
        if (count == 6) {
            buf->d_ino = 14;
            buf->d_off = 7;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "exe", 4);
            return 0;
        }
        if (count == 7) {
            buf->d_ino = 24;
            buf->d_off = 8;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "cwd", 4);
            return 0;
        }
        if (count == 8) {
            buf->d_ino = 25;
            buf->d_off = 9;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "root", 5);
            return 0;
        }
        if (count == 9) {
            buf->d_ino = 26;
            buf->d_off = 10;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "fd", 3);
            return 0;
        }
        if (count == 10) {
            buf->d_ino = 15;
            buf->d_off = 11;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_launcher", 13);
            return 0;
        }
        if (count == 11) {
            buf->d_ino = 16;
            buf->d_off = 12;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_runner", 11);
            return 0;
        }
        if (count == 12) {
            buf->d_ino = 17;
            buf->d_off = 13;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "wki_remote_pid", 15);
            return 0;
        }
        if (count == 13) {
            buf->d_ino = 18;
            buf->d_off = 14;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "maps", 5);
            return 0;
        }
        if (pfd->node.type == ProcNodeType::PID_DIR && count == 14) {
            buf->d_ino = 19;
            buf->d_off = 15;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_DIR;
            std::memcpy(buf->d_name.data(), "task", 5);
            return 0;
        }
        bool const IS_IMAGES_ENTRY =
            (pfd->node.type == ProcNodeType::PID_DIR && count == 15) || (pfd->node.type == ProcNodeType::TASK_TID_DIR && count == 14);
        if (IS_IMAGES_ENTRY) {
            buf->d_ino = 27;
            buf->d_off = count + 1;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "images", 7);
            return 0;
        }
        bool const IS_LIMITS_ENTRY =
            (pfd->node.type == ProcNodeType::PID_DIR && count == 16) || (pfd->node.type == ProcNodeType::TASK_TID_DIR && count == 15);
        if (IS_LIMITS_ENTRY) {
            buf->d_ino = 28;
            buf->d_off = count + 1;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "limits", 7);
            return 0;
        }
        return -ENOENT;  // No more entries
    }

    if (pfd->node.type == ProcNodeType::TASK_DIR) {
        auto* task = thread_group_task_at(pfd->node.pid, count - 2, &pfd->pid_view, pfd->target);
        if (task == nullptr) {
            return -ENOENT;
        }
        uint64_t const TID = pfd->pid_view.is_host() ? task->pid : task->namespace_tid();
        task->release();
        fill_numeric_dirent(buf, count, TID);
        return 0;
    }

    if (pfd->node.type == ProcNodeType::WKI_DIR) {
        if (count == 2) {
            buf->d_ino = 20;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "peers", 6);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 21;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "netdiag", 8);
            return 0;
        }
        if (count == 4) {
            buf->d_ino = 23;
            buf->d_off = 5;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "pipes", 6);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 24;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "chaos", 6);
            return 0;
        }
        if (count == 6) {
            buf->d_ino = 25;
            buf->d_off = 7;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "chaos_workload", 15);
            return 0;
        }
        if (count == 7) {
            buf->d_ino = 26;
            buf->d_off = 8;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "auth", 5);
            return 0;
        }
        if (count == 8) {
            buf->d_ino = 27;
            buf->d_off = 9;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "napi", 5);
            return 0;
        }
        return -ENOENT;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_DIR) {
        struct Entry {
            const char* name;
            uint8_t type;
        };
        constexpr std::array<Entry, 12> ENTRIES{{
            {.name = "summary", .type = DT_REG},
            {.name = "zones", .type = DT_REG},
            {.name = "procs", .type = DT_REG},
            {.name = "dead", .type = DT_REG},
            {.name = "alloc_totals", .type = DT_REG},
            {.name = "slabs", .type = DT_REG},
            {.name = "kmalloc_live", .type = DT_REG},
            {.name = "kmalloc_callers", .type = DT_REG},
            {.name = "page_callers", .type = DT_REG},
            {.name = "features", .type = DT_REG},
            {.name = "track", .type = DT_DIR},
            {.name = "reclaim", .type = DT_DIR},
        }};
        size_t const INDEX = count - 2;
        if (INDEX >= ENTRIES.size()) {
            return -ENOENT;
        }
        buf->d_ino = 30 + INDEX;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = ENTRIES.at(INDEX).type;
        std::strncpy(buf->d_name.data(), ENTRIES.at(INDEX).name, buf->d_name.size() - 1);
        buf->d_name[buf->d_name.size() - 1] = '\0';
        return 0;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_TRACK_DIR) {
        if (count == 2) {
            buf->d_ino = 40;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "page_callers", 13);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 41;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "kmalloc_debug", 14);
            return 0;
        }
        return -ENOENT;
    }

    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_DIR) {
        constexpr std::array<const char*, 6> ENTRIES{
            "buffer_cache", "packet_pool", "xfs_inode", "file_mmap_cache", "anonymous_swap", "coordinator",
        };
        size_t const INDEX = count - 2;
        if (INDEX >= ENTRIES.size()) {
            return -ENOENT;
        }
        buf->d_ino = 42 + INDEX;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_REG;
        std::strncpy(buf->d_name.data(), ENTRIES.at(INDEX), buf->d_name.size() - 1);
        buf->d_name[buf->d_name.size() - 1] = '\0';
        return 0;
    }

    return -ENOENT;
}

// Helper to parse a PID from a path component
auto procfs_readdir(File* f, DirEntry* buf, size_t count) -> int {
    if (f == nullptr || f->private_data == nullptr) return -EBADF;
    if (buf == nullptr) return -EINVAL;
    auto const& data = *static_cast<const ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    DirEntry candidate{};
    int const RESULT = procfs_readdir_in_view(f, &candidate, count);
    int const VALID = procfs_view_policy(data);
    if (VALID != 0) return VALID;
    if (RESULT == 0) *buf = candidate;
    return RESULT;
}

auto parse_pid(const char* s) -> int64_t {
    if (s == nullptr || *s == '\0') {
        return -EINVAL;
    }
    int64_t val = 0;
    for (const char* p = s; *p != 0; ++p) {
        if (*p < '0' || *p > '9') {
            return -EINVAL;
        }
        val = (val * 10) + (*p - '0');
    }
    return val;
}

class ProcPagemapPin {
   public:
    explicit ProcPagemapPin(ProcTask& task) : task_(task) { (void)task_.try_acquire_usercopy_pagemap(pagemap); }
    ~ProcPagemapPin() {
        if (pagemap != nullptr) task_.release_usercopy_pagemap();
    }
    ProcPagemapPin(const ProcPagemapPin&) = delete;
    auto operator=(const ProcPagemapPin&) -> ProcPagemapPin& = delete;
    ker::mod::mm::paging::PageTable* pagemap{};

   private:
    ProcTask& task_;
};

// Caller retains the Task; the VM guard serializes exec/mapping publication,
// and the pin prevents exit from retiring this task's address space mid-walk.
auto collect_proc_memory(ProcTask& task) -> ker::mod::mm::virt::UserMemoryStats {
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    ProcPagemapPin pin(task);
    return ker::mod::mm::virt::collect_user_memory_stats(pin.pagemap);
}

struct ProcNameSnapshot {
    ProcTask::ExePathBuffer executable{};
    ProcTask::ExePathBuffer short_name{};
};

// Caller holds the VM publication guard, protecting dynamically owned name.
auto copy_proc_names_locked(const ProcTask& task) -> ProcNameSnapshot {
    ProcNameSnapshot names{};
    uint64_t const IRQF = task.path_metadata_lock.lock_irqsave();
    names.executable = task.exe_path;
    task.path_metadata_lock.unlock_irqrestore(IRQF);
    const char* short_name = task.name;
    if (short_name == nullptr || short_name[0] == '\0') {
        short_name = names.executable.data();
        for (const char* p = short_name; *p != '\0'; ++p)
            if (*p == '/') short_name = p + 1;
    }
    if (short_name[0] == '\0') short_name = "unknown";
    std::strncpy(names.short_name.data(), short_name, names.short_name.size() - 1);
    return names;
}

auto copy_proc_public_comm(ProcTask& task) -> ProcTask::ExePathBuffer {
    ProcNameSnapshot names{};
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        names = copy_proc_names_locked(task);
    }
    const char* comm = names.executable.data();
    for (const char* p = comm; *p != '\0'; ++p)
        if (*p == '/') comm = p + 1;
    if (comm[0] == '\0') comm = names.short_name.data();
    ProcTask::ExePathBuffer result{};
    size_t length = 0;
    while (comm[length] != '\0' && length < result.size() - 1) {
        auto const BYTE = static_cast<unsigned char>(comm[length]);
        result[length] = BYTE < 0x20 || BYTE == 0x7f ? '?' : comm[length];
        ++length;
    }
    return result;
}

// Public generators borrow the Task retained by ProcFileData. A numeric PID
// must not reselect the subject between lookup/open and first content generation.
// Generate content for /proc/<pid>/status
auto generate_status(ProcTask* task, char* buf, size_t bufsz, bool thread_view, bool local_view = false) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) {
        return 0;
    }
    auto const MEM = collect_proc_memory(*task);
    auto const STATS = collect_proc_task_stats(*task, thread_view, local_view);
    uint64_t const PROCESS_PID =
        local_view ? ker::mod::sched::task::namespace_process_pid(*task) : ker::mod::sched::task::process_pid(*task);

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    append("Name:\t");
    auto const COMM = copy_proc_public_comm(*task);
    append(COMM.data());
    append("\nTgid:\t");
    append_int(PROCESS_PID);
    append("\nPid:\t");
    append_int(local_view ? task->namespace_tid() : task->pid);
    append("\nPPid:\t");
    append_int(local_view ? ker::syscall::process::child_events::namespace_parent_pid(*task) : task->parent_pid);
    append("\nUid:\t");
    auto const CREDENTIALS = task->credentials();
    append_int(CREDENTIALS.user().real);
    append("\t");
    append_int(CREDENTIALS.user().effective);
    append("\t");
    append_int(CREDENTIALS.user().saved);
    append("\t");
    append_int(CREDENTIALS.user().real);
    append("\nGid:\t");
    append_int(CREDENTIALS.group().real);
    append("\t");
    append_int(CREDENTIALS.group().effective);
    append("\t");
    append_int(CREDENTIALS.group().saved);
    append("\t");
    append_int(CREDENTIALS.group().real);
    // Every credential row uses the same canonical process snapshot.
    append("\nGroups:\t");
    bool first_group = true;
    for (auto group : CREDENTIALS.groups()) {
        if (!first_group) append(" ");
        append_int(group);
        first_group = false;
    }
    append("\nNoNewPrivs:\t");
    append_int(CREDENTIALS.no_new_privileges());
    append("\nWosAuthorityCeiling:\t");
    append_int(CREDENTIALS.authority_ceiling());
    append("\nWosServicePrivileges:\t");
    append_int(CREDENTIALS.service_privileges());
    append("\nWosServicePrivilegesLocked:\t");
    append_int(CREDENTIALS.service_privileges_locked());
    append("\nThreads:\t");
    append_int(STATS.thread_count);
    append("\nVmSize:\t");
    append_int(MEM.virtual_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nVmRSS:\t");
    append_int(MEM.resident_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nVmSwap:\t");
    append_int(MEM.swapped_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nRssShmem:\t");
    append_int(MEM.shared_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");
    append("\nVmPTE:\t");
    append_int(MEM.page_table_pages * (ker::mod::mm::paging::PAGE_SIZE / KIB));
    append(" kB");

    // State
    append("\nState:\t");
    append(proc_task_state_label(STATS.state));

    // Scheduling info
    append("\nCpu:\t");
    append_int(task->cpu);
    append("\nSchedQueue:\t");
    switch (task->sched_queue) {
        case ker::mod::sched::task::Task::sched_queue::NONE:
            append("NONE");
            break;
        case ker::mod::sched::task::Task::sched_queue::RUNNABLE:
            append("RUNNABLE");
            break;
        case ker::mod::sched::task::Task::sched_queue::WAITING:
            append("WAITING");
            break;
        case ker::mod::sched::task::Task::sched_queue::DEAD_GC:
            append("DEAD_GC");
            break;
    }

    // Wait channel
    append("\nWchan:\t");
    if (task->wait_channel != nullptr) {
        append(task->wait_channel);
    } else {
        append("-");
    }

    // Blocking and process-child lifecycle state.
    auto const CHILD_DIAGNOSTICS = ker::syscall::process::child_events::diagnostics(*task, local_view);
    append("\nDeferredSwitch:\t");
    append(task->deferred_task_switch ? "1" : "0");
    append("\nVoluntaryBlock:\t");
    append(task->is_voluntary_blocked() ? "1" : "0");
    append("\nWaitingForPid:\t");
    append_int(CHILD_DIAGNOSTICS.registered_selector);
    append("\nWaitpidCompletionClaimed:\t");
    append(CHILD_DIAGNOSTICS.claim_active ? "1" : "0");
    append("\nWaitpidLastRepairUs:\t");
    append_int(0);
    append("\nChildEventCount:\t");
    append_int(CHILD_DIAGNOSTICS.owned_event_count);
    append("\nChildWaiterCount:\t");
    append_int(CHILD_DIAGNOSTICS.owned_waiter_count);
    append("\nChildMembershipState:\t");
    append_int(static_cast<uint64_t>(CHILD_DIAGNOSTICS.membership));
    append("\nExitInProgress:\t");
    append(task->exit_in_progress ? "1" : "0");
    append("\nExitNotifyReady:\t");
    append(task->exit_notify_ready.load(std::memory_order_acquire) ? "1" : "0");
    append("\nWaitedOn:\t");
    append(task->has_exited && CHILD_DIAGNOSTICS.membership == ker::mod::sched::task::ChildMembershipState::UNLINKED ? "1" : "0");
    append("\nWakeupPending:\t");
    append(task->wakeup_pending.load(std::memory_order_acquire) ? "1" : "0");

    // Signals
    append("\nSigPnd:\t");
    append_int(task->signal_pending_bits());
    append("\nSigBlk:\t");
    append_int(task->signal_mask_bits());
    append("\nInSigHandler:\t");
    append(task->in_signal_handler ? "1" : "0");

    // Terminal
    append("\nTTY:\t");
    auto const JOB_SNAPSHOT = task->job_identity_snapshot();
    auto const& JOB = JOB_SNAPSHOT.internal;
    int const TTY_INDEX = JOB.terminal.index();
    if (TTY_INDEX >= 0) {
        append_int(static_cast<uint64_t>(TTY_INDEX));
    } else {
        append("-1");
    }
    append("\nPGID:\t");
    append_int(local_view ? JOB_SNAPSHOT.visible.pgid : JOB.pgid);
    append("\nSID:\t");
    append_int(local_view ? JOB_SNAPSHOT.visible.session_id : JOB.session_id);

    // Time accounting
    append("\nUserTime:\t");
    append_int(STATS.user_time_us);
    append(" us");
    append("\nSysTime:\t");
    append_int(STATS.system_time_us);
    append(" us");
    append("\nStartTime:\t");
    append_int(task->start_time_us);
    append(" us");

    append("\n");

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/stat (Linux-compatible format)
// Format: pid (comm) state ppid pgid sid tty tpgid flags minflt cminflt majflt cmajflt
//         utime stime cutime cstime priority nice nthreads itrealvalue starttime vsize rss ...
auto generate_stat(ProcTask* task, char* buf, size_t bufsz, bool thread_view, bool local_view = false) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) {
        return 0;
    }
    auto const MEM = collect_proc_memory(*task);
    auto const STATS = collect_proc_task_stats(*task, thread_view, local_view);

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    auto const COMM = copy_proc_public_comm(*task);

    // pid (comm) state ppid pgid sid tty_nr tpgid flags
    append_int(local_view ? task->namespace_tid() : task->pid);
    append(" (");
    append(COMM.data());
    append(") ");
    buf[off++] = STATS.state;
    append(" ");
    append_int(local_view ? ker::syscall::process::child_events::namespace_parent_pid(*task) : task->parent_pid);  // ppid
    append(" ");
    auto const JOB_SNAPSHOT = task->job_identity_snapshot();
    auto const& JOB = JOB_SNAPSHOT.internal;
    append_int(local_view ? JOB_SNAPSHOT.visible.pgid : JOB.pgid);  // pgrp
    append(" ");
    append_int(local_view ? JOB_SNAPSHOT.visible.session_id : JOB.session_id);  // session
    append(" ");
    int const TTY_INDEX = JOB.terminal.index();
    append_int(TTY_INDEX >= 0 ? static_cast<uint64_t>(TTY_INDEX) : 0);  // tty_nr
    append(" ");
    append_int(0);  // tpgid
    append(" ");
    append_int(0);  // flags
    append(" ");

    // minflt cminflt majflt cmajflt
    append("0 ");  // minflt
    append("0 ");  // cminflt
    append("0 ");  // majflt
    append("0 ");  // cmajflt

    // utime stime cutime cstime (in clock ticks, CLK_TCK=100, so 1 tick=10000us)
    append_int(STATS.user_time_us / 10000);  // utime
    append(" ");
    append_int(STATS.system_time_us / 10000);  // stime
    append(" ");
    append_int(STATS.child_user_time_us / 10000);
    append(" ");
    append_int(STATS.child_system_time_us / 10000);
    append(" ");

    // priority nice num_threads itrealvalue starttime
    append_int(static_cast<uint64_t>(20 + task->sched_nice));  // priority
    append(" ");
    if (task->sched_nice < 0) {
        append("-");
        append_int(static_cast<uint64_t>(-task->sched_nice));
    } else {
        append_int(static_cast<uint64_t>(task->sched_nice));
    }
    append(" ");
    append_int(STATS.thread_count);  // num_threads
    append(" ");
    append("0 ");                             // itrealvalue
    append_int(task->start_time_us / 10000);  // starttime (in ticks since boot)
    append(" ");

    // vsize rss
    append_int(MEM.virtual_pages * ker::mod::mm::paging::PAGE_SIZE);  // vsize
    append(" ");
    append_int(MEM.resident_pages);  // rss
    append(" ");

    // rlim signal blocked sigignore sigcatch wchan nswap cnswap exit_signal processor
    append("0 ");                             // rlim
    append_int(task->signal_pending_bits());  // signal (pending)
    append(" ");
    append_int(task->signal_mask_bits());  // blocked
    append(" ");
    append("0 ");  // sigignore
    append("0 ");  // sigcatch
    // wchan: kernel wait channel name (0 if not blocked)
    if (task->wait_channel != nullptr) {
        append(task->wait_channel);
    } else {
        append("0");
    }
    append(" ");
    append("0 ");           // nswap
    append("0 ");           // cnswap
    append("0 ");           // exit_signal
    append_int(task->cpu);  // processor

    append("\n");
    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/statm.
// Format: size resident shared text lib data dt (all in pages).
auto generate_statm(ProcTask* task, char* buf, size_t bufsz) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) {
        return 0;
    }
    auto const MEM = collect_proc_memory(*task);

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    append_int(MEM.virtual_pages);
    append(" ");
    append_int(MEM.resident_pages);
    append(" ");
    append_int(MEM.shared_pages);
    append(" 0 0 0 0\n");
    buf[off] = '\0';
    return off;
}

auto procfs_pte_raw(const ker::mod::mm::paging::PageTableEntry& entry) -> uint64_t {
    uint64_t raw = 0;
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}

auto procfs_page_table_from_entry(const ker::mod::mm::paging::PageTableEntry& entry) -> ker::mod::mm::paging::PageTable* {
    uint64_t const PHYS = static_cast<uint64_t>(entry.frame) << ker::mod::mm::paging::PAGE_SHIFT;
    return reinterpret_cast<ker::mod::mm::paging::PageTable*>(ker::mod::mm::addr::get_virt_pointer(PHYS));
}

auto procfs_is_reserved_leaf(const ker::mod::mm::paging::PageTableEntry& entry) -> bool {
    return entry.present == 0 && (procfs_pte_raw(entry) & ker::mod::mm::paging::PAGE_RESERVED) != 0U;
}

auto procfs_map_perms(const ker::mod::mm::paging::PageTableEntry& entry) -> std::array<char, 5> {
    if (procfs_is_reserved_leaf(entry)) {
        return {'-', '-', '-', 'p', '\0'};
    }

    uint64_t const RAW = procfs_pte_raw(entry);
    return {
        'r',
        ((entry.writable != 0) || ((RAW & ker::mod::mm::paging::PAGE_COW) != 0U)) ? 'w' : '-',
        entry.no_execute == 0 ? 'x' : '-',
        (RAW & ker::mod::mm::paging::PAGE_SHARED) != 0U ? 's' : 'p',
        '\0',
    };
}

auto read_proc_runtime_image_memory(void* opaque, uint64_t address, void* destination, size_t size) -> bool {
    auto* task = static_cast<ker::mod::sched::task::Task*>(opaque);
    return task != nullptr && ker::mod::sys::usercopy::copy_from_task_mapped(*task, address, destination, size);
}

auto snapshot_proc_runtime_images(ker::mod::sched::task::Task& task,
                                  std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES>& images, size_t& count,
                                  bool include_kernel = false) -> ker::loader::runtime::SnapshotStatus {
    ker::mod::sched::task::Task::ExePathBuffer exe_path{};
    uint64_t const IRQF = task.path_metadata_lock.lock_irqsave();
    exe_path = task.exe_path;
    task.path_metadata_lock.unlock_irqrestore(IRQF);
    ker::loader::runtime::SnapshotSource const SOURCE{
        .program_header_addr = task.program_header_addr,
        .program_header_count = task.program_header_count,
        .program_header_ent_size = task.program_header_ent_size,
        .main_elf_header_addr = task.elf_header_addr,
        .main_load_base = task.image_load_base,
        .main_image_start = task.image_vaddr_start,
        .main_image_end = task.image_vaddr_end,
        .main_entry = task.entry,
        .interpreter_base = task.interp_base,
        .interpreter_image_start = task.interp_vaddr_start,
        .interpreter_image_end = task.interp_vaddr_end,
        .main_path = exe_path.data(),
        .interpreter_path = "/lib/ld.so",
    };
    auto status = ker::loader::runtime::snapshot_images(read_proc_runtime_image_memory, &task, SOURCE, images, count);
    if (include_kernel && !ker::loader::runtime::append_kernel_image(images, count)) {
        status =
            count >= images.size() ? ker::loader::runtime::SnapshotStatus::TRUNCATED : ker::loader::runtime::SnapshotStatus::INCONSISTENT;
    }
    return status;
}

auto runtime_image_status_name(ker::loader::runtime::SnapshotStatus status) -> const char* {
    using SnapshotStatus = ker::loader::runtime::SnapshotStatus;
    switch (status) {
        case SnapshotStatus::COMPLETE:
            return "complete";
        case SnapshotStatus::STATIC_IMAGE:
            return "static";
        case SnapshotStatus::UNAVAILABLE:
            return "unavailable";
        case SnapshotStatus::INCONSISTENT:
            return "inconsistent";
        case SnapshotStatus::TRUNCATED:
            return "truncated";
    }
    return "unavailable";
}

auto encode_runtime_image_path(const std::array<char, ker::loader::runtime::IMAGE_PATH_MAX>& path)
    -> std::array<char, (ker::loader::runtime::IMAGE_PATH_MAX * 3) + 1> {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    std::array<char, (ker::loader::runtime::IMAGE_PATH_MAX * 3) + 1> encoded{};
    size_t out = 0;
    for (char raw : path) {
        auto const BYTE = static_cast<uint8_t>(raw);
        if (BYTE == 0) {
            break;
        }
        if (BYTE <= static_cast<uint8_t>(' ') || BYTE == static_cast<uint8_t>('%') || BYTE >= 0x7fU) {
            encoded.at(out++) = '%';
            encoded.at(out++) = HEX_DIGITS.at(BYTE >> 4U);
            encoded.at(out++) = HEX_DIGITS.at(BYTE & 0x0fU);
        } else {
            encoded.at(out++) = raw;
        }
    }
    return encoded;
}

// Generate the authoritative runtime image catalog for profilers and debuggers.
auto generate_images(ProcTask* task, char* buf, size_t bufsz, bool include_kernel) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES> images{};
    size_t image_count = 0;
    ker::loader::runtime::SnapshotStatus status = ker::loader::runtime::SnapshotStatus::UNAVAILABLE;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        ProcPagemapPin pin(*task);
        if (pin.pagemap != nullptr) {
            status = snapshot_proc_runtime_images(*task, images, image_count, include_kernel);
        }
    }

    size_t off = 0;
    auto append_format = [&](const char* format, auto... args) {
        if (off >= bufsz - 1) {
            return;
        }
        int const LEN = std::snprintf(buf + off, bufsz - off, format, args...);
        if (LEN > 0) {
            off += std::min(static_cast<size_t>(LEN), bufsz - off - 1);
        }
    };

    append_format("status=%s count=%llu\n", runtime_image_status_name(status), static_cast<unsigned long long>(image_count));
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (size_t i = 0; i < image_count; ++i) {
        auto const& image = images.at(i);
        auto const ENCODED_PATH = encode_runtime_image_path(image.path);
        std::array<char, (ker::loader::runtime::BUILD_ID_MAX * 2) + 2> build_id{};
        if (image.build_id_size == 0) {
            build_id.at(0) = '-';
        } else {
            size_t const BUILD_ID_SIZE = std::min<size_t>(image.build_id_size, ker::loader::runtime::BUILD_ID_MAX);
            for (size_t byte = 0; byte < BUILD_ID_SIZE; ++byte) {
                build_id.at(byte * 2) = HEX_DIGITS.at(image.build_id.at(byte) >> 4U);
                build_id.at((byte * 2) + 1) = HEX_DIGITS.at(image.build_id.at(byte) & 0x0fU);
            }
        }
        append_format(
            "image base=0x%016llx start=0x%016llx end=0x%016llx text_start=0x%016llx text_end=0x%016llx "
            "entry=0x%016llx dynamic=0x%016llx flags=0x%08x build_id=%s path=%s\n",
            static_cast<unsigned long long>(image.load_base), static_cast<unsigned long long>(image.image_start),
            static_cast<unsigned long long>(image.image_end), static_cast<unsigned long long>(image.text_start),
            static_cast<unsigned long long>(image.text_end), static_cast<unsigned long long>(image.entry),
            static_cast<unsigned long long>(image.dynamic_addr), image.flags, build_id.data(), ENCODED_PATH.data());
    }
    buf[off] = '\0';
    return off;
}

// Generate Linux-style /proc/<pid>/maps rows for sanitizer runtimes.
auto generate_maps(ProcTask* task, char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    using ker::mod::mm::paging::PAGE_SIZE;
    using ker::mod::mm::paging::PageTableEntry;

    constexpr size_t USER_PML4_ENTRIES = 256;
    constexpr uint64_t PAGES_PER_2M = 512;
    constexpr uint64_t PAGES_PER_1G = 512 * PAGES_PER_2M;
    constexpr uint64_t PML4_SHIFT = 39;
    constexpr uint64_t PML3_SHIFT = 30;
    constexpr uint64_t PML2_SHIFT = 21;
    constexpr uint64_t PML1_SHIFT = 12;

    std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES> images{};
    size_t image_count = 0;

    size_t off = 0;
    uint64_t range_start = 0;
    uint64_t range_end = 0;
    std::array<char, 5> range_perms{};
    size_t range_image = ker::loader::runtime::MAX_IMAGES;
    bool have_range = false;

    auto append_range = [&]() {
        if (!have_range || off >= bufsz - 1) {
            return;
        }
        auto const* image = range_image < image_count ? &images.at(range_image) : nullptr;
        const char* const IMAGE_PATH = image != nullptr ? image->path.data() : "";
        bool const HAS_PATH = IMAGE_PATH[0] != '\0';
        // RuntimeImage has authoritative extents but not PT_LOAD p_offset.
        // Keep the legacy field parseable without presenting a virtual offset
        // as a file offset.
        constexpr uint64_t MAP_OFFSET = 0;
        int const LEN =
            std::snprintf(buf + off, bufsz - off, "%012llx-%012llx %s %08llx 00:00 0%s%s\n", static_cast<unsigned long long>(range_start),
                          static_cast<unsigned long long>(range_end), range_perms.data(), static_cast<unsigned long long>(MAP_OFFSET),
                          HAS_PATH ? " " : "", HAS_PATH ? IMAGE_PATH : "");
        if (LEN <= 0) {
            return;
        }
        off += std::min(static_cast<size_t>(LEN), bufsz - off - 1);
    };

    auto image_at = [&](uint64_t address) -> size_t {
        for (size_t i = 0; i < image_count; ++i) {
            auto const& image = images.at(i);
            if ((image.flags & ker::loader::runtime::IMAGE_EXACT_MAPPING) != 0U && image.image_start < image.image_end &&
                address >= image.image_start && address < image.image_end) {
                return i;
            }
        }
        return ker::loader::runtime::MAX_IMAGES;
    };

    auto next_image_boundary = [&](uint64_t address, uint64_t end) -> uint64_t {
        uint64_t next = end;
        for (size_t i = 0; i < image_count; ++i) {
            auto const& image = images.at(i);
            if ((image.flags & ker::loader::runtime::IMAGE_EXACT_MAPPING) == 0U || image.image_start >= image.image_end) {
                continue;
            }
            if (image.image_start > address && image.image_start < next) {
                next = image.image_start;
            }
            if (image.image_end > address && image.image_end < next) {
                next = image.image_end;
            }
        }
        return next;
    };

    auto emit_segment = [&](uint64_t start, uint64_t end, const std::array<char, 5>& perms, size_t image_index) {
        if (have_range && range_end == start && range_perms == perms && range_image == image_index) {
            range_end = end;
            return;
        }

        append_range();
        range_start = start;
        range_end = end;
        range_perms = perms;
        range_image = image_index;
        have_range = true;
    };

    auto emit_leaf = [&](uint64_t vaddr, uint64_t page_count, const PageTableEntry& entry) {
        if (page_count > (UINT64_MAX - vaddr) / PAGE_SIZE) {
            return;
        }
        auto const PERMS = procfs_map_perms(entry);
        uint64_t const END = vaddr + (page_count * PAGE_SIZE);
        uint64_t current = vaddr;
        while (current < END) {
            uint64_t const NEXT = next_image_boundary(current, END);
            emit_segment(current, NEXT, PERMS, image_at(current));
            current = NEXT;
        }
    };

    bool has_pagemap = false;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        ProcPagemapPin pin(*task);
        auto* pagemap = pin.pagemap;
        if (pagemap != nullptr) {
            has_pagemap = true;
            (void)snapshot_proc_runtime_images(*task, images, image_count);

            for (size_t i4 = 0; i4 < USER_PML4_ENTRIES; ++i4) {
                const auto& pml4e = pagemap->entries.at(i4);
                if (pml4e.present == 0) {
                    continue;
                }

                auto* pml3 = procfs_page_table_from_entry(pml4e);
                for (size_t i3 = 0; i3 < pml3->entries.size(); ++i3) {
                    const auto& pml3e = pml3->entries.at(i3);
                    if (pml3e.present == 0) {
                        continue;
                    }
                    uint64_t const VADDR_1G = (static_cast<uint64_t>(i4) << PML4_SHIFT) | (static_cast<uint64_t>(i3) << PML3_SHIFT);
                    if (pml3e.pagesize != 0) {
                        if (pml3e.user != 0) {
                            emit_leaf(VADDR_1G, PAGES_PER_1G, pml3e);
                        }
                        continue;
                    }

                    auto* pml2 = procfs_page_table_from_entry(pml3e);
                    for (size_t i2 = 0; i2 < pml2->entries.size(); ++i2) {
                        const auto& pml2e = pml2->entries.at(i2);
                        if (pml2e.present == 0) {
                            continue;
                        }
                        uint64_t const VADDR_2M = VADDR_1G | (static_cast<uint64_t>(i2) << PML2_SHIFT);
                        if (pml2e.pagesize != 0) {
                            if (pml2e.user != 0) {
                                emit_leaf(VADDR_2M, PAGES_PER_2M, pml2e);
                            }
                            continue;
                        }

                        auto* pml1 = procfs_page_table_from_entry(pml2e);
                        for (size_t i1 = 0; i1 < pml1->entries.size(); ++i1) {
                            const auto& pte = pml1->entries.at(i1);
                            if (pte.present == 0 && !procfs_is_reserved_leaf(pte)) {
                                continue;
                            }
                            if (pte.present != 0 && pte.user == 0) {
                                continue;
                            }
                            uint64_t const VADDR = VADDR_2M | (static_cast<uint64_t>(i1) << PML1_SHIFT);
                            emit_leaf(VADDR, 1, pte);
                        }
                    }
                }
            }
        }
    }

    if (!has_pagemap) {
        buf[0] = '\0';
        return 0;
    }

    append_range();
    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/cmdline (NUL-separated argv)
auto generate_cmdline(ProcTask* task, char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) return 0;
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    ProcTask::ExePathBuffer exe_path{};
    uint64_t const IRQF = task->path_metadata_lock.lock_irqsave();
    exe_path = task->exe_path;
    task->path_metadata_lock.unlock_irqrestore(IRQF);
    // Use exe_path as fallback (most processes won't have a saved argv)
    const auto* path = exe_path.data();
    if (path[0] == '\0') {
        path = (task->name != nullptr) ? task->name : "";
    }
    size_t len = strlen(path);
    if (len >= bufsz) {
        len = bufsz - 1;
    }
    memcpy(buf, path, len);
    buf[len] = '\0';  // NUL terminator (cmdline ends with NUL)
    return len + 1;   // Include the trailing NUL as part of content length
}

// The retained subject also retains its canonical filesystem owner. Copy only
// values here: capturing directory ownership could run a final File close in
// callers that already hold publication or content locks.
auto copy_proc_filesystem_root(const ProcTask& task, ProcTask::PathBuffer& output) -> int {
    auto const* owner = task.filesystem_owner();
    if (owner == nullptr) return -ESRCH;
    uint64_t const IRQF = owner->path_metadata_lock.lock_irqsave();
    output = owner->root;
    owner->path_metadata_lock.unlock_irqrestore(IRQF);
    return 0;
}

// Generate content for /proc/mounts into caller-owned scratch from one validated root/mount observation.
auto generate_mounts_for_task(ProcTask* task, char* buf, size_t bufsz) -> ssize_t {
    if (bufsz == 0) return 0;
    FilesystemRootMetadata root;
    int const CAPTURE = vfs_capture_root_metadata(task, root);
    if (CAPTURE != 0) return CAPTURE;
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };

    // Strip the task root prefix so mount points appear task-root-relative,
    // matching what POSIX processes expect (e.g. "/dev" not "/rootfs/dev").
    const char* task_root = "/";
    size_t root_len = 1;
    if (root.path()[0] == '/' && root.path()[1] != '\0') {
        task_root = root.path();
        root_len = std::strlen(task_root);
    }

    // Iterate mount table
    for (size_t i = 0; i < ker::vfs::get_mount_count(root.mount_scope()); ++i) {
        ker::vfs::MountSnapshot mount_snapshot{};
        if (!ker::vfs::get_mount_snapshot_at(i, &mount_snapshot, root.mount_scope())) {
            continue;
        }
        const char* snapshot_path = static_cast<const char*>(mount_snapshot.path);
        const char* snapshot_fstype = static_cast<const char*>(mount_snapshot.fstype);
        const char* mount_path = (snapshot_path[0] != '\0') ? snapshot_path : "/";

        // Compute the task-relative display path by stripping the task root prefix.
        const char* display_path = mount_path;
        size_t const MOUNT_PATH_LEN = std::strlen(mount_path);
        bool const INSIDE_ROOT = root_len == 1 || (MOUNT_PATH_LEN >= root_len && std::strncmp(mount_path, task_root, root_len) == 0 &&
                                                   (mount_path[root_len] == '/' || mount_path[root_len] == '\0'));
        if (!root.mount_scope().is_host() && !INSIDE_ROOT) continue;
        if (root_len > 1 && INSIDE_ROOT) {
            display_path = mount_path + root_len;
            if (display_path[0] == '\0') {
                display_path = "/";
            }
        }

        append((snapshot_fstype[0] != '\0') ? snapshot_fstype : "none");
        append(" ");
        append(display_path);
        append(" ");
        append((snapshot_fstype[0] != '\0') ? snapshot_fstype : "none");
        append((mount_snapshot.read_only || (INSIDE_ROOT && root.read_only())) ? " ro" : " rw");
        if (mount_snapshot.nosuid || (INSIDE_ROOT && root.nosuid())) append(",nosuid");
        append(" 0 0\n");
    }

#ifdef WOS_SELFTEST
    if (mounts_snapshot_test_hook != nullptr) mounts_snapshot_test_hook(task);
#endif
    if (!vfs_root_metadata_current(task, root)) return -EAGAIN;
    buf[off] = '\0';
    return static_cast<ssize_t>(off);
}

auto generate_timekeeping(char* buf, size_t bufsz) -> size_t {
    auto const SOURCE = ker::mod::tsc::get_status();
    auto const WALL = ker::mod::rtc::status();
    auto const TIMERS = ker::mod::time::process_timer::status();
    auto const NOW = ker::mod::time::get_monotonic_ns();
    int const LENGTH = snprintf(
        buf, bufsz,
        "version=1\nsource=%s\ninvariant_tsc=%u\nfrequency_hz=%llu\nresolution_ns=%llu\n"
        "conversion_multiplier=%llu\nconversion_divisor=%llu\ncalibration_spread_ppm=%llu\nwatchdog_deviation_ns=%llu\n"
        "kvm_registered_cpus=%llu\nkvm_expected_cpus=%llu\nkvm_watchdog_checks=%llu\n"
        "rtc_valid=%u\nwall_steps=%llu\nwall_slews=%llu\nwall_rejected=%llu\nwall_pending_ns=%lld\nslew_limit_ppm=500\n"
        "process_timer_real_depth=%llu\nprocess_timer_wake_depth=%llu\nprocess_timer_expirations=%llu\n"
        "process_timer_late_us_max=%llu\nprocess_timer_work_budget=%llu\nclockevent=x2apic-oneshot\n",
        ker::mod::tsc::state_name(SOURCE.state), SOURCE.invariant_tsc ? 1U : 0U, static_cast<unsigned long long>(SOURCE.frequency_hz),
        static_cast<unsigned long long>(SOURCE.resolution_ns), static_cast<unsigned long long>(SOURCE.conversion_multiplier),
        static_cast<unsigned long long>(SOURCE.conversion_divisor), static_cast<unsigned long long>(SOURCE.calibration_spread_ppm),
        static_cast<unsigned long long>(SOURCE.watchdog_deviation_ns), static_cast<unsigned long long>(SOURCE.kvm_registered_cpus),
        static_cast<unsigned long long>(SOURCE.kvm_expected_cpus), static_cast<unsigned long long>(SOURCE.kvm_watchdog_checks),
        WALL.rtc_valid ? 1U : 0U, static_cast<unsigned long long>(WALL.steps), static_cast<unsigned long long>(WALL.slews),
        static_cast<unsigned long long>(WALL.rejected), static_cast<long long>(WALL.correction_ns - WALL.applied(NOW)),
        static_cast<unsigned long long>(TIMERS.real_depth), static_cast<unsigned long long>(TIMERS.wake_depth),
        static_cast<unsigned long long>(TIMERS.expirations), static_cast<unsigned long long>(TIMERS.max_late_us),
        static_cast<unsigned long long>(TIMERS.work_budget));
    return LENGTH <= 0 || bufsz == 0 ? 0 : std::min(static_cast<size_t>(LENGTH), bufsz - 1);
}

// Generate content for /proc/uptime
// Format: "<uptime_seconds> <idle_seconds>\n" with two decimal places
auto generate_uptime(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    uint64_t const UPTIME_US = ker::mod::time::get_us();
    uint64_t const UPTIME_SEC = UPTIME_US / 1000000ULL;
    uint64_t const UPTIME_FRAC = (UPTIME_US % 1000000ULL) / 10000ULL;  // two decimal places

    append_int(UPTIME_SEC);
    append(".");
    // Zero-pad the fractional part to two digits
    if (UPTIME_FRAC < 10) {
        append("0");
    }
    append_int(UPTIME_FRAC);

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    uint64_t idle_us = 0;
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        idle_us += ker::mod::sched::get_cpu_accounting_snapshot(cpu).idle_us;
    }
    uint64_t const IDLE_AVG_US = idle_us / cpu_count;
    uint64_t const IDLE_SEC = IDLE_AVG_US / 1000000ULL;
    uint64_t const IDLE_FRAC = (IDLE_AVG_US % 1000000ULL) / 10000ULL;

    append(" ");
    append_int(IDLE_SEC);
    append(".");
    if (IDLE_FRAC < 10) {
        append("0");
    }
    append_int(IDLE_FRAC);
    append("\n");

    buf[off] = '\0';
    return off;
}

struct ProcViewTaskCounts {
    uint64_t runnable{};
    uint64_t total{};
    uint64_t blocked{};
    uint64_t last_name{};  // highest currently observed local PID/TID
};

auto collect_view_task_counts(const pid_ns::NamespaceRef& view) -> ProcViewTaskCounts {
    ProcViewTaskCounts result{};
    uint32_t const COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) continue;
        if (task_in_pid_view(*task, view) && !task->wki_proxy_task) {
            ++result.total;
            char const STATE = proc_task_state_char(*task);
            if (STATE == 'R') ++result.runnable;
            if (STATE == 'D') ++result.blocked;
            result.last_name = std::max(result.last_name, task->namespace_tid());
        }
        task->release();
    }
    return result;
}

auto generate_cpu_stat(char* buf, size_t bufsz, const pid_ns::NamespaceRef* view = nullptr) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_ticks = [&](uint64_t us) { append_int(us / US_PER_PROC_TICK); };

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }

    ker::mod::sched::CpuAccountingSnapshot total{};
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        auto const SNAP = ker::mod::sched::get_cpu_accounting_committed_snapshot(cpu);
        total.user_us += SNAP.user_us;
        total.nice_us += SNAP.nice_us;
        total.system_us += SNAP.system_us;
        total.idle_us += SNAP.idle_us;
        total.iowait_us += SNAP.iowait_us;
        total.irq_us += SNAP.irq_us;
        total.softirq_us += SNAP.softirq_us;
        total.steal_us += SNAP.steal_us;
    }

    auto append_cpu_row = [&](const char* prefix, uint64_t cpu_no, const ker::mod::sched::CpuAccountingSnapshot& snap, bool numbered) {
        append(prefix);
        if (numbered) {
            append_int(cpu_no);
        }
        append("  ");
        append_ticks(snap.user_us);
        append(" ");
        append_ticks(snap.nice_us);
        append(" ");
        append_ticks(snap.system_us);
        append(" ");
        append_ticks(snap.idle_us);
        append(" ");
        append_ticks(snap.iowait_us);
        append(" ");
        append_ticks(snap.irq_us);
        append(" ");
        append_ticks(snap.softirq_us);
        append(" ");
        append_ticks(snap.steal_us);
        append(" 0 0\n");
    };

    append_cpu_row("cpu", 0, total, false);
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        append_cpu_row("cpu", cpu, ker::mod::sched::get_cpu_accounting_committed_snapshot(cpu), true);
    }

    auto const LOAD = ker::mod::sched::get_load_average_snapshot();
    append("procs_running ");
    bool const PRIVATE = view != nullptr && !view->is_host();
    auto const COUNTS = PRIVATE ? collect_view_task_counts(*view) : ProcViewTaskCounts{};
    append_int(PRIVATE ? COUNTS.runnable : LOAD.runnable_tasks);
    append("\nprocs_blocked ");
    append_int(PRIVATE ? COUNTS.blocked : LOAD.uninterruptible_tasks);
    append("\n");

    buf[off] = '\0';
    return off;
}

auto generate_loadavg(char* buf, size_t bufsz, const pid_ns::NamespaceRef* view = nullptr) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_load = [&](uint64_t milli) {
        append_int(milli / 1000ULL);
        append(".");
        uint64_t const FRAC = (milli % 1000ULL) / 10ULL;
        if (FRAC < 10) {
            append("0");
        }
        append_int(FRAC);
    };

    auto const LOAD = ker::mod::sched::get_load_average_snapshot();
    append_load(LOAD.load1_milli);
    append(" ");
    append_load(LOAD.load5_milli);
    append(" ");
    append_load(LOAD.load15_milli);
    append(" ");
    bool const PRIVATE = view != nullptr && !view->is_host();
    auto const COUNTS = PRIVATE ? collect_view_task_counts(*view) : ProcViewTaskCounts{};
    append_int(PRIVATE ? COUNTS.runnable : LOAD.runnable_tasks);
    append("/");
    append_int(PRIVATE ? COUNTS.total : LOAD.total_tasks);
    append(" ");
    append_int(PRIVATE ? COUNTS.last_name : LOAD.last_pid);
    append("\n");

    buf[off] = '\0';
    return off;
}

auto generate_meminfo(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };
    auto append_kb_line = [&](const char* name, uint64_t bytes) {
        append(name);
        append(":\t");
        append_int(bytes / KIB);
        append(" kB\n");
    };

    uint64_t const TOTAL = ker::mod::mm::phys::get_total_mem_bytes();
    uint64_t const FREE = ker::mod::mm::phys::get_free_mem_bytes();
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    uint64_t const BUFFERS = BCACHE.total_bytes;
    uint64_t const CACHED = FILE_CACHE.bytes + ELF_CACHE.bytes;
    uint64_t const AVAILABLE = std::min<uint64_t>(TOTAL, FREE + BCACHE.clean_bytes + CACHED);

    uint64_t page_table_bytes = 0;
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!task->is_thread) {
            auto const MEM = collect_proc_memory(*task);
            page_table_bytes += MEM.page_table_pages * ker::mod::mm::paging::PAGE_SIZE;
        }
        task->release();
    }
    ker::mod::mm::swap::SwapStats swap_stats{};
    ker::mod::mm::swap::get_stats(&swap_stats);

    append_kb_line("MemTotal", TOTAL);
    append_kb_line("MemFree", FREE);
    append_kb_line("MemAvailable", AVAILABLE);
    append_kb_line("Buffers", BUFFERS);
    append_kb_line("Cached", CACHED);
    append_kb_line("SwapCached", swap_stats.cached_bytes);
    append_kb_line("Dirty", BCACHE.dirty_bytes);
    append_kb_line("Writeback", 0);
    append_kb_line("PageTables", page_table_bytes);
    append_kb_line("SwapTotal", swap_stats.total_bytes);
    append_kb_line("SwapFree", swap_stats.free_bytes);

    buf[off] = '\0';
    return off;
}

auto procfs_local_wki_hostname() -> std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> {
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> hostname{};
    (void)ker::net::wki::wki_peer_copy_hostname(ker::net::wki::g_wki.my_node_id, hostname);
    return hostname;
}

auto format_procfs_hostname(const char* hostname, char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) return 0;
    if (bufsz == 1) {
        buf[0] = '\0';
        return 0;
    }
    if (hostname[0] == '\0') hostname = "local";
    size_t const LENGTH = std::min(std::strlen(hostname), bufsz - 2);
    std::memcpy(buf, hostname, LENGTH);
    buf[LENGTH] = '\n';
    buf[LENGTH + 1] = '\0';
    return LENGTH + 1;
}

// Public placement metadata is a copied observation, never remote authority.
auto generate_wki_launcher(ProcTask* task, char* buf, size_t bufsz, bool local_view = false) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) return 0;
    if (local_view) {
        auto const LOCAL = procfs_local_wki_hostname();
        return format_procfs_hostname(LOCAL.data(), buf, bufsz);
    }
    auto const IDENTITY = task->wki_execution_identity();
    auto const LOCAL = procfs_local_wki_hostname();
    return format_procfs_hostname(IDENTITY.submitter_hostname.front() != '\0' ? IDENTITY.submitter_hostname.data() : LOCAL.data(), buf,
                                  bufsz);
}

auto generate_wki_runner(ProcTask* task, char* buf, size_t bufsz, bool local_view = false) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) return 0;
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> runner{};
    if (!local_view) (void)ker::net::wki::wki_proxy_task_remote_info(task, nullptr, runner.data(), runner.size());
    if (runner.front() == '\0') runner = procfs_local_wki_hostname();
    return format_procfs_hostname(runner.data(), buf, bufsz);
}

auto task_wki_remote_pid(uint64_t pid, const ProcTask::WkiExecutionIdentity& identity,
                         const std::array<char, ker::net::wki::WKI_HOSTNAME_MAX>& local_hostname) -> uint64_t {
    if (identity.remote_pid != 0) return identity.remote_pid;
    if (identity.submitter_hostname.front() != '\0' && local_hostname.front() != '\0' &&
        std::strcmp(identity.submitter_hostname.data(), local_hostname.data()) != 0)
        return pid;
    return 0;
}

auto generate_wki_remote_pid(ProcTask* task, char* buf, size_t bufsz, bool local_view = false) -> size_t {
    if (task == nullptr || task->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) return 0;
    auto const IDENTITY = task->wki_execution_identity();
    uint64_t const PID = task->pid;
    auto const LOCAL = procfs_local_wki_hostname();
    int const LENGTH =
        std::snprintf(buf, bufsz, "%llu\n", static_cast<unsigned long long>(local_view ? 0 : task_wki_remote_pid(PID, IDENTITY, LOCAL)));
    if (LENGTH < 0) {
        if (bufsz != 0) buf[0] = '\0';
        return 0;
    }
    return bufsz != 0 ? std::min(static_cast<size_t>(LENGTH), bufsz - 1) : 0;
}

auto generate_wki_peers(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    struct PeerProcSnapshot {
        std::array<char, net::wki::WKI_HOSTNAME_MAX> hostname = {};
        uint16_t node_id = ker::net::wki::WKI_NODE_INVALID;
        ker::net::wki::PeerState state = ker::net::wki::PeerState::UNKNOWN;
        uint64_t last_heartbeat = 0;
        uint64_t last_rx_activity = 0;
    };

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s != '\0' && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_row = [&](const std::array<char, net::wki::WKI_HOSTNAME_MAX>& hostname, uint64_t node_id, bool connected, uint64_t cpus,
                          uint64_t load_pct, uint64_t last_update_us, bool local) {
        if (off >= bufsz - 1) {
            return;
        }
        int const LEN =
            std::snprintf(buf + off, bufsz - off, "%s %llu %u %llu %llu %llu %u\n", hostname[0] != '\0' ? hostname.data() : "unknown",
                          static_cast<unsigned long long>(node_id), connected ? 1U : 0U, static_cast<unsigned long long>(cpus),
                          static_cast<unsigned long long>(load_pct), static_cast<unsigned long long>(last_update_us), local ? 1U : 0U);
        if (LEN <= 0) {
            return;
        }
        off += std::min(static_cast<size_t>(LEN), bufsz - off - 1);
    };

    append("hostname node_id connected cpus load_pct last_update_us local\n");

    uint64_t const CPU_COUNT = std::max<uint64_t>(ker::mod::smt::get_core_count(), 1);
    uint64_t const LOCAL_LOAD = ker::net::wki::wki_local_node_load_pct();
    uint16_t const LOCAL_NODE = ker::net::wki::g_wki.my_node_id != ker::net::wki::WKI_NODE_INVALID ? ker::net::wki::g_wki.my_node_id : 0;
    auto const LOCAL_HOSTNAME = procfs_local_wki_hostname();
    append_row(LOCAL_HOSTNAME, LOCAL_NODE, true, CPU_COUNT, LOCAL_LOAD, ker::net::wki::wki_now_us(), true);

    std::array<PeerProcSnapshot, ker::net::wki::WKI_MAX_PEERS> peer_rows{};
    size_t peer_count = 0;

    uint64_t const FLAGS = ker::net::wki::g_wki.peer_lock.lock_irqsave();
    for (const auto& peer : ker::net::wki::g_wki.peers) {
        if (peer.node_id == ker::net::wki::WKI_NODE_INVALID) {
            continue;
        }
        if (peer_count >= peer_rows.size()) {
            break;
        }

        auto& row = peer_rows.at(peer_count++);
        row.hostname = peer.hostname;
        row.node_id = peer.node_id;
        row.state = peer.state;
        row.last_heartbeat = peer.last_heartbeat;
        row.last_rx_activity = peer.last_rx_activity;
    }
    ker::net::wki::g_wki.peer_lock.unlock_irqrestore(FLAGS);

    for (size_t i = 0; i < peer_count; ++i) {
        const auto& row = peer_rows.at(i);
        ker::net::wki::RemoteNodeLoad load = {};
        bool const HAS_LOAD = ker::net::wki::wki_remote_node_load_snapshot(row.node_id, &load);
        uint64_t const CPUS = HAS_LOAD && load.num_cpus != 0 ? load.num_cpus : 0;
        uint64_t const LOAD_PCT = HAS_LOAD ? load.avg_load_pct : 0;
        uint64_t const LAST_UPDATE = HAS_LOAD ? load.last_update_us : std::max(row.last_heartbeat, row.last_rx_activity);
        append_row(row.hostname, row.node_id, row.state == ker::net::wki::PeerState::CONNECTED, CPUS, LOAD_PCT, LAST_UPDATE, false);
    }

    buf[off] = '\0';
    return off;
}

auto generate_wki_auth(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }
    size_t off = 0;
    int const HEADER = std::snprintf(
        buf, bufsz, "protocol secure_only local_node ready\n%u 1 %u %u\npeer active key_id generation policy tx_counter rx_high\n",
        ker::net::wki::WKI_VERSION, ker::net::wki::wki_auth_local_node_id(), ker::net::wki::wki_auth_ready() ? 1U : 0U);
    if (HEADER <= 0) {
        return 0;
    }
    off = std::min(static_cast<size_t>(HEADER), bufsz - 1);

    auto* peers = new (std::nothrow) ker::net::wki::WkiPeerDiag[ker::net::wki::WKI_PEER_DIAG_MAX];
    if (peers == nullptr) {
        buf[off] = '\0';
        return off;
    }
    size_t const COUNT = ker::net::wki::wki_peer_diag_snapshot(peers, ker::net::wki::WKI_PEER_DIAG_MAX);
    for (size_t index = 0; index < COUNT && off < bufsz - 1; ++index) {
        auto const& peer = peers[index];
        int const LENGTH = std::snprintf(buf + off, bufsz - off, "%u %u %u %u 0x%llx %llu %llu\n", peer.node_id, peer.auth_active ? 1U : 0U,
                                         peer.auth_key_id, peer.auth_generation, static_cast<unsigned long long>(peer.auth_policy),
                                         static_cast<unsigned long long>(peer.auth_tx_counter),
                                         static_cast<unsigned long long>(peer.auth_rx_counter_high));
        if (LENGTH <= 0) {
            break;
        }
        off += std::min(static_cast<size_t>(LENGTH), bufsz - off - 1);
    }
    delete[] peers;
    buf[off] = '\0';
    return off;
}

// Generate content for /proc/version
auto generate_version(char* buf, size_t bufsz) -> size_t;

// Generate a one-line status string for /proc/kperfctl
auto generate_kperfctl(char* buf, size_t bufsz) -> size_t {
    const char* state = ker::mod::perf::is_enabled() ? "enabled\n" : "disabled\n";
    size_t len = strlen(state);
    if (len >= bufsz) {
        len = bufsz - 1;
    }
    memcpy(buf, state, len);
    buf[len] = '\0';
    return len;
}

// Forward declarations for helpers defined below
static void append_dec64(char*& p, const char* end, uint64_t v);
static void append_sconst(char*& p, const char* end, const char* s);
static void append_char(char*& p, const char* end, char c);

// Generate content for /proc/kcontstat
// Shows per-subsystem container aggregate statistics (non-destructive).
auto generate_kcontstat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    for (size_t i = 1; i < ker::mod::perf::PERF_SUBSYSTEM_COUNT; ++i) {
        auto s = ker::mod::perf::get_subsystem_stats(static_cast<ker::mod::perf::PerfSubsystem>(i));
        uint64_t const INS = s.inserts;
        uint64_t const REM = s.removes;
        uint64_t const RES = s.resizes;
        uint64_t const OOM = s.oom_failures;
        uint64_t const PEAK = s.peak_count;
        uint64_t const CUR = s.current_count;

        // Skip subsystems with no activity
        if (INS == 0 && REM == 0 && RES == 0 && OOM == 0) {
            continue;
        }

        append_sconst(p, end, "subsys=");
        append_sconst(p, end, ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(i)));
        append_sconst(p, end, " inserts=");
        append_dec64(p, end, INS);
        append_sconst(p, end, " removes=");
        append_dec64(p, end, REM);
        append_sconst(p, end, " resizes=");
        append_dec64(p, end, RES);
        append_sconst(p, end, " oom=");
        append_dec64(p, end, OOM);
        append_sconst(p, end, " peak=");
        append_dec64(p, end, PEAK);
        append_sconst(p, end, " current=");
        append_dec64(p, end, CUR);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_kwkistat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    std::array<ker::mod::perf::WkiPerfSummarySnapshot, ker::mod::perf::WKI_PERF_SUMMARY_BUCKETS> rows{};
    size_t const ROW_COUNT = ker::mod::perf::get_wki_summary_snapshots(rows.data(), rows.size());

    for (size_t i = 0; i < ROW_COUNT; ++i) {
        const auto& row = rows[i];
        if (row.calls == 0) {
            continue;
        }

        uint64_t const AVG = row.calls != 0 ? (row.total_latency_us / row.calls) : 0;
        append_sconst(p, end, "scope=");
        append_sconst(p, end, ker::mod::perf::wki_scope_name(static_cast<ker::mod::perf::WkiPerfScope>(row.scope)));
        append_sconst(p, end, " op=");
        append_sconst(p, end, ker::mod::perf::wki_op_name(static_cast<ker::mod::perf::WkiPerfScope>(row.scope), row.op));
        append_sconst(p, end, " peer=");
        append_dec64(p, end, row.peer);
        append_sconst(p, end, " channel=");
        append_dec64(p, end, row.channel);
        append_sconst(p, end, " calls=");
        append_dec64(p, end, row.calls);
        append_sconst(p, end, " errors=");
        append_dec64(p, end, row.errors);
        append_sconst(p, end, " retries=");
        append_dec64(p, end, row.retries);
        append_sconst(p, end, " bytes=");
        append_dec64(p, end, row.bytes);
        append_sconst(p, end, " samples=");
        append_dec64(p, end, row.latency_samples);
        append_sconst(p, end, " total_us=");
        append_dec64(p, end, row.total_latency_us);
        append_sconst(p, end, " avg_us=");
        append_dec64(p, end, AVG);
        append_sconst(p, end, " max_us=");
        append_dec64(p, end, row.max_latency_us);
        append_sconst(p, end, " p50_us=");
        append_dec64(p, end, row.p50_us);
        append_sconst(p, end, " p95_us=");
        append_dec64(p, end, row.p95_us);
        append_sconst(p, end, " p99_us=");
        append_dec64(p, end, row.p99_us);
        append_sconst(p, end, " p999_us=");
        append_dec64(p, end, row.p999_us);
        append_sconst(p, end, " p9999_us=");
        append_dec64(p, end, row.p9999_us);
        append_sconst(p, end, " p99999_us=");
        append_dec64(p, end, row.p99999_us);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_kipcstat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::net::wki::WkiIpcPerfSnapshot snapshot{};
    ker::net::wki::wki_ipc_get_perf_snapshot(snapshot);
    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);

    append_sconst(p, end, "exports=");
    append_dec64(p, end, snapshot.exports);
    append_sconst(p, end, " proxies=");
    append_dec64(p, end, snapshot.proxies);
    append_sconst(p, end, " pump_tasks=");
    append_dec64(p, end, snapshot.pump_tasks);
    append_sconst(p, end, " ring_bytes=");
    append_dec64(p, end, snapshot.proxy_ring_bytes);
    append_sconst(p, end, " ring_used=");
    append_dec64(p, end, snapshot.proxy_ring_used_bytes);
    append_sconst(p, end, " blocked_readers=");
    append_dec64(p, end, snapshot.blocked_readers);
    append_sconst(p, end, " poll_waiters=");
    append_dec64(p, end, snapshot.poll_waiters);
    append_sconst(p, end, " pending_deliveries=");
    append_dec64(p, end, snapshot.pending_deliveries);
    append_sconst(p, end, " pending_chunks=");
    append_dec64(p, end, snapshot.pending_chunks);
    append_sconst(p, end, " pending_bytes=");
    append_dec64(p, end, snapshot.pending_bytes);
    append_sconst(p, end, " export_backlogs=");
    append_dec64(p, end, snapshot.export_backlogs);
    append_sconst(p, end, " export_backlog_chunks=");
    append_dec64(p, end, snapshot.export_backlog_chunks);
    append_sconst(p, end, " export_backlog_bytes=");
    append_dec64(p, end, snapshot.export_backlog_bytes);
    append_sconst(p, end, " export_close_pending=");
    append_dec64(p, end, snapshot.export_close_pending);
    append_sconst(p, end, " export_close_waiting_for_bytes=");
    append_dec64(p, end, snapshot.export_close_waiting_for_bytes);
    append_sconst(p, end, " export_flush_queue=");
    append_dec64(p, end, snapshot.export_flush_queue);
    append_sconst(p, end, " proxy_close_queue=");
    append_dec64(p, end, snapshot.proxy_close_queue);
    append_sconst(p, end, " proxy_close_attempts=");
    append_dec64(p, end, snapshot.proxy_close_attempts);
    append_sconst(p, end, " dev_op_queue=");
    append_dec64(p, end, snapshot.dev_op_queue);
    append_sconst(p, end, " dev_op_payload_bytes=");
    append_dec64(p, end, snapshot.dev_op_payload_bytes);
    append_sconst(p, end, " proxy_write_payload_bytes=");
    append_dec64(p, end, snapshot.proxy_write_payload_bytes);
    append_sconst(p, end, " proxy_write_no_credit_waits=");
    append_dec64(p, end, snapshot.proxy_write_no_credit_waits);
    append_sconst(p, end, " proxy_write_block_us=");
    append_dec64(p, end, snapshot.proxy_write_block_us);
    append_sconst(p, end, " proxy_pipe_rdma_full_waits=");
    append_dec64(p, end, snapshot.proxy_pipe_rdma_full_waits);
    append_sconst(p, end, " proxy_ring_full_waits=");
    append_dec64(p, end, snapshot.proxy_ring_full_waits);
    append_sconst(p, end, " proxy_ring_full_bytes=");
    append_dec64(p, end, snapshot.proxy_ring_full_bytes);
    append_sconst(p, end, " pipe_payload_bytes=");
    append_dec64(p, end, snapshot.pipe_payload_bytes);
    append_sconst(p, end, " approx_alloc_bytes=");
    append_dec64(p, end, snapshot.approx_alloc_bytes);
    append_sconst(p, end, " local_pipe_active=");
    append_dec64(p, end, local_pipe.active_pipes);
    append_sconst(p, end, " local_pipe_created=");
    append_dec64(p, end, local_pipe.created_since_reset);
    append_sconst(p, end, " local_pipe_peak=");
    append_dec64(p, end, local_pipe.peak_pipes);
    append_sconst(p, end, " local_pipe_capacity=");
    append_dec64(p, end, local_pipe.capacity_bytes);
    append_sconst(p, end, " local_pipe_peak_capacity=");
    append_dec64(p, end, local_pipe.peak_capacity_bytes);
    append_sconst(p, end, " local_pipe_buffered=");
    append_dec64(p, end, local_pipe.buffered_bytes);
    append_sconst(p, end, " local_pipe_reader_waiters=");
    append_dec64(p, end, local_pipe.reader_waiters);
    append_sconst(p, end, " local_pipe_writer_waiters=");
    append_dec64(p, end, local_pipe.writer_waiters);
    append_sconst(p, end, " local_pipe_poll_waiters=");
    append_dec64(p, end, local_pipe.poll_waiters);
    append_sconst(p, end, " local_pipe_direct_writes=");
    append_dec64(p, end, local_pipe.direct_writes);
    append_sconst(p, end, " local_pipe_read_closed=");
    append_dec64(p, end, local_pipe.read_closed);
    append_sconst(p, end, " local_pipe_write_closed=");
    append_dec64(p, end, local_pipe.write_closed);
    append_sconst(p, end, " local_pipe_approx_alloc_bytes=");
    append_dec64(p, end, local_pipe.approx_alloc_bytes);
    if (p + 1 < end) {
        *p++ = '\n';
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Hex helper
void append_hex64(char*& p, const char* end, uint64_t v) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    if (p + 18 >= end) {
        return;
    }
    *p++ = '0';
    *p++ = 'x';
    for (int i = 60; i >= 0; i -= 4) {
        *p++ = HEX_DIGITS[static_cast<size_t>((v >> i) & 0xf)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

void append_dec64(char*& p, const char* end, uint64_t v) {
    std::array<char, 22> tmp{};
    int n = 0;
    if (v == 0) {
        tmp[static_cast<size_t>(n++)] = '0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    } else {
        while (v > 0 && static_cast<size_t>(n) < tmp.size() - 1) {
            tmp[static_cast<size_t>(n++)] =
                static_cast<char>('0' + (v % 10));  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            v /= 10;
        }
    }
    // reverse into output
    if (p + n >= end) {
        return;
    }
    for (int i = n - 1; i >= 0; --i) {
        *p++ = tmp[static_cast<size_t>(i)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

void append_sdec64(char*& p, const char* end, int64_t v) {
    if (v < 0) {
        append_char(p, end, '-');
        append_dec64(p, end, static_cast<uint64_t>(-v));
        return;
    }
    append_dec64(p, end, static_cast<uint64_t>(v));
}

void append_sconst(char*& p, const char* end, const char* s) {
    while (((*s) != 0) && p + 1 < end) {
        *p++ = *s++;
    }
}

void append_char(char*& p, const char* end, char c) {
    if (p + 1 < end) {
        *p++ = c;
    }
}

void append_bool01(char*& p, const char* end, bool v) { append_dec64(p, end, v ? 1U : 0U); }

void append_hex16(char*& p, const char* end, uint16_t v) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    if (p + 6 >= end) {
        return;
    }
    *p++ = '0';
    *p++ = 'x';
    for (int i = 12; i >= 0; i -= 4) {
        *p++ = HEX_DIGITS[static_cast<size_t>((v >> i) & 0xf)];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
}

auto remote_compute_diag_kind_name(ker::net::wki::WkiRemoteComputeDiagKind kind) -> const char* {
    switch (kind) {
        case ker::net::wki::WkiRemoteComputeDiagKind::UNKNOWN:
            return "unknown";
        case ker::net::wki::WkiRemoteComputeDiagKind::SUBMITTED:
            return "submitted";
        case ker::net::wki::WkiRemoteComputeDiagKind::RUNNING:
            return "running";
        case ker::net::wki::WkiRemoteComputeDiagKind::PENDING_COMPLETE:
            return "pending_complete";
    }
    return "unknown";
}

auto ipc_diag_kind_name(ker::net::wki::WkiIpcDiagKind kind) -> const char* {
    switch (kind) {
        case ker::net::wki::WkiIpcDiagKind::UNKNOWN:
            return "unknown";
        case ker::net::wki::WkiIpcDiagKind::EXPORT:
            return "export";
        case ker::net::wki::WkiIpcDiagKind::PROXY:
            return "proxy";
        case ker::net::wki::WkiIpcDiagKind::EXPORT_BACKLOG:
            return "export_backlog";
        case ker::net::wki::WkiIpcDiagKind::PROXY_CLOSE:
            return "proxy_close";
        case ker::net::wki::WkiIpcDiagKind::PENDING_DELIVERY:
            return "pending_delivery";
        case ker::net::wki::WkiIpcDiagKind::DEV_OP_WORK:
            return "dev_op_work";
        case ker::net::wki::WkiIpcDiagKind::PEER_CLEANUP:
            return "peer_cleanup";
    }
    return "unknown";
}

auto resource_diag_kind_name(ker::net::wki::WkiResourceDiagKind kind) -> const char* {
    switch (kind) {
        case ker::net::wki::WkiResourceDiagKind::DISCOVERED:
            return "discovered";
        case ker::net::wki::WkiResourceDiagKind::PENDING_VFS_MOUNT:
            return "pending_vfs_mount";
        case ker::net::wki::WkiResourceDiagKind::PENDING_NET_ATTACH:
            return "pending_net_attach";
        case ker::net::wki::WkiResourceDiagKind::PENDING_RX:
            return "pending_rx";
    }
    return "unknown";
}

auto chaos_direction_name(ker::net::wki::WkiChaosDirection direction) -> const char* {
    switch (direction) {
        case ker::net::wki::WkiChaosDirection::TX:
            return "tx";
        case ker::net::wki::WkiChaosDirection::RX:
            return "rx";
    }
    return "unknown";
}

auto chaos_surface_name(ker::net::wki::WkiChaosSurface surface) -> const char* {
    switch (surface) {
        case ker::net::wki::WkiChaosSurface::FRAME:
            return "frame";
        case ker::net::wki::WkiChaosSurface::BLOCK_DOORBELL:
            return "blk_doorbell";
        case ker::net::wki::WkiChaosSurface::BLOCK_SQE:
            return "blk_sqe";
    }
    return "unknown";
}

auto chaos_block_origin_name(ker::net::wki::WkiChaosBlockOrigin origin) -> const char* {
    switch (origin) {
        case ker::net::wki::WkiChaosBlockOrigin::PROXY:
            return "proxy";
        case ker::net::wki::WkiChaosBlockOrigin::SERVER:
            return "server";
    }
    return "unknown";
}

auto chaos_block_lane_name(ker::net::wki::WkiChaosBlockLane lane) -> const char* {
    switch (lane) {
        case ker::net::wki::WkiChaosBlockLane::IVSHMEM:
            return "ivshmem";
        case ker::net::wki::WkiChaosBlockLane::ROCE:
            return "roce";
    }
    return "unknown";
}

auto chaos_action_name(ker::net::wki::WkiChaosAction action) -> const char* {
    switch (action) {
        case ker::net::wki::WkiChaosAction::PASS:
            return "pass";
        case ker::net::wki::WkiChaosAction::DROP:
            return "drop";
        case ker::net::wki::WkiChaosAction::DUPLICATE:
            return "duplicate";
        case ker::net::wki::WkiChaosAction::DELAY:
            return "delay";
        case ker::net::wki::WkiChaosAction::REORDER:
            return "reorder";
        case ker::net::wki::WkiChaosAction::CORRUPT:
            return "corrupt";
        case ker::net::wki::WkiChaosAction::FAIL:
            return "fail";
        case ker::net::wki::WkiChaosAction::PARTITION:
            return "partition";
        case ker::net::wki::WkiChaosAction::RELEASE:
            return "release";
    }
    return "unknown";
}

auto chaos_corrupt_mode_name(ker::net::wki::WkiChaosCorruptMode mode) -> const char* {
    switch (mode) {
        case ker::net::wki::WkiChaosCorruptMode::CHECKSUM:
            return "checksum";
        case ker::net::wki::WkiChaosCorruptMode::PAYLOAD_XOR:
            return "payload";
    }
    return "unknown";
}

auto chaos_outcome_name(ker::net::wki::WkiChaosOutcome outcome) -> const char* {
    switch (outcome) {
        case ker::net::wki::WkiChaosOutcome::PASSED:
            return "passed";
        case ker::net::wki::WkiChaosOutcome::DROPPED:
            return "dropped";
        case ker::net::wki::WkiChaosOutcome::DUPLICATED:
            return "duplicated";
        case ker::net::wki::WkiChaosOutcome::QUEUED:
            return "queued";
        case ker::net::wki::WkiChaosOutcome::FAILED:
            return "failed";
        case ker::net::wki::WkiChaosOutcome::RELEASED:
            return "released";
        case ker::net::wki::WkiChaosOutcome::QUEUE_OVERFLOW:
            return "queue_overflow";
        case ker::net::wki::WkiChaosOutcome::STREAM_OVERFLOW:
            return "stream_overflow";
        case ker::net::wki::WkiChaosOutcome::TRANSPORT_REMOVED:
            return "transport_removed";
        case ker::net::wki::WkiChaosOutcome::CORRUPTED:
            return "corrupted";
        case ker::net::wki::WkiChaosOutcome::COALESCED:
            return "coalesced";
        case ker::net::wki::WkiChaosOutcome::HEALED:
            return "healed";
    }
    return "unknown";
}

void append_block_ring_diag(char*& p, const char* end, const ker::net::wki::BlkRingGeometry& geometry,
                            const ker::net::wki::BlkRingIndices& indices, bool geometry_valid, bool indices_valid) {
    append_sconst(p, end, " ring_ready=");
    append_dec64(p, end, geometry.server_ready);
    append_sconst(p, end, " sq_depth=");
    append_dec64(p, end, geometry.sq_depth);
    append_sconst(p, end, " cq_depth=");
    append_dec64(p, end, geometry.cq_depth);
    append_sconst(p, end, " slots=");
    append_dec64(p, end, geometry.data_slot_count);
    append_sconst(p, end, " slot_size=");
    append_dec64(p, end, geometry.data_slot_size);
    append_sconst(p, end, " block_size=");
    append_dec64(p, end, geometry.block_size);
    append_sconst(p, end, " total_blocks=");
    append_dec64(p, end, geometry.total_blocks);
    append_sconst(p, end, " sq_head=");
    append_dec64(p, end, indices.sq_head);
    append_sconst(p, end, " sq_tail=");
    append_dec64(p, end, indices.sq_tail);
    append_sconst(p, end, " cq_head=");
    append_dec64(p, end, indices.cq_head);
    append_sconst(p, end, " cq_tail=");
    append_dec64(p, end, indices.cq_tail);
    append_sconst(p, end, " geometry_valid=");
    append_bool01(p, end, geometry_valid);
    append_sconst(p, end, " indices_valid=");
    append_bool01(p, end, indices_valid);
}

auto ipc_resource_type_name(uint16_t raw_type) -> const char* {
    switch (static_cast<ker::net::wki::ResourceType>(raw_type)) {
        case ker::net::wki::ResourceType::BLOCK:
            return "block";
        case ker::net::wki::ResourceType::CHAR:
            return "char";
        case ker::net::wki::ResourceType::NET:
            return "net";
        case ker::net::wki::ResourceType::VFS:
            return "vfs";
        case ker::net::wki::ResourceType::COMPUTE:
            return "compute";
        case ker::net::wki::ResourceType::CUSTOM:
            return "custom";
        case ker::net::wki::ResourceType::IPC_PIPE:
            return "ipc_pipe";
        case ker::net::wki::ResourceType::IPC_EVENTFD:
            return "ipc_eventfd";
        case ker::net::wki::ResourceType::IPC_PTY:
            return "ipc_pty";
        case ker::net::wki::ResourceType::IPC_FUTEX:
            return "ipc_futex";
        case ker::net::wki::ResourceType::IPC_EPOLL:
            return "ipc_epoll";
        case ker::net::wki::ResourceType::IPC_SOCKET:
            return "ipc_socket";
    }
    return "unknown";
}

void append_ipv4(char*& p, const char* end, uint32_t ip) {
    append_dec64(p, end, (ip >> 24U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, (ip >> 16U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, (ip >> 8U) & 0xffU);
    append_char(p, end, '.');
    append_dec64(p, end, ip & 0xffU);
}

void append_tcp_endpoint(char*& p, const char* end, const ker::net::SocketEndpoint& endpoint) {
    if (endpoint.is_ipv6() && !endpoint.is_v4_mapped()) {
        constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        append_char(p, end, '[');
        for (size_t i = 0; i < endpoint.address.size(); i += 2) {
            if (i != 0) {
                append_char(p, end, ':');
            }
            uint16_t const WORD = static_cast<uint16_t>((static_cast<uint16_t>(endpoint.address.at(i)) << 8U) | endpoint.address.at(i + 1));
            for (int shift = 12; shift >= 0; shift -= 4) {
                append_char(p, end, HEX_DIGITS.at((WORD >> shift) & 0xfU));
            }
        }
        if (endpoint.scope_id != 0) {
            append_char(p, end, '%');
            append_dec64(p, end, endpoint.scope_id);
        }
        append_char(p, end, ']');
    } else {
        append_ipv4(p, end, endpoint.ipv4_address().to_host_order());
    }
    append_char(p, end, ':');
    append_dec64(p, end, endpoint.port);
}

auto peer_state_name(ker::net::wki::PeerState state) -> const char* {
    switch (state) {
        case ker::net::wki::PeerState::UNKNOWN:
            return "UNKNOWN";
        case ker::net::wki::PeerState::HELLO_SENT:
            return "HELLO_SENT";
        case ker::net::wki::PeerState::CONNECTED:
            return "CONNECTED";
        case ker::net::wki::PeerState::FENCED:
            return "FENCED";
        case ker::net::wki::PeerState::RECONNECTING:
            return "RECONNECTING";
    }
    return "?";
}

auto tcp_state_name(uint8_t state) -> const char* {
    switch (static_cast<ker::net::proto::TcpState>(state)) {
        case ker::net::proto::TcpState::CLOSED:
            return "CLOSED";
        case ker::net::proto::TcpState::LISTEN:
            return "LISTEN";
        case ker::net::proto::TcpState::SYN_SENT:
            return "SYN_SENT";
        case ker::net::proto::TcpState::SYN_RECEIVED:
            return "SYN_RECEIVED";
        case ker::net::proto::TcpState::ESTABLISHED:
            return "ESTABLISHED";
        case ker::net::proto::TcpState::FIN_WAIT_1:
            return "FIN_WAIT_1";
        case ker::net::proto::TcpState::FIN_WAIT_2:
            return "FIN_WAIT_2";
        case ker::net::proto::TcpState::CLOSE_WAIT:
            return "CLOSE_WAIT";
        case ker::net::proto::TcpState::CLOSING:
            return "CLOSING";
        case ker::net::proto::TcpState::LAST_ACK:
            return "LAST_ACK";
        case ker::net::proto::TcpState::TIME_WAIT:
            return "TIME_WAIT";
    }
    return "?";
}

auto priority_name(uint8_t priority) -> const char* {
    return priority == static_cast<uint8_t>(ker::net::wki::PriorityClass::THROUGHPUT) ? "THROUGHPUT" : "LATENCY";
}

auto napi_state_name(uint8_t state) -> const char* {
    switch (static_cast<ker::net::NapiState>(state)) {
        case ker::net::NapiState::IDLE:
            return "IDLE";
        case ker::net::NapiState::SCHEDULED:
            return "SCHEDULED";
        case ker::net::NapiState::POLLING:
            return "POLLING";
        case ker::net::NapiState::DISABLED:
            return "DISABLED";
    }
    return "?";
}

void append_netdev_name(char*& p, const char* end, const std::array<char, ker::net::NETDEV_NAME_LEN>& name) {
    for (char c : name) {
        if (c == '\0') {
            return;
        }
        append_char(p, end, c);
    }
}

void append_memacc_key(char*& p, const char* end, const char* key) {
    append_char(p, end, ' ');
    append_sconst(p, end, key);
    append_char(p, end, '=');
}

void append_memacc_dec(char*& p, const char* end, const char* key, uint64_t value) {
    append_memacc_key(p, end, key);
    append_dec64(p, end, value);
}

void append_memacc_sdec(char*& p, const char* end, const char* key, int64_t value) {
    append_memacc_key(p, end, key);
    append_sdec64(p, end, value);
}

void append_memacc_hex(char*& p, const char* end, const char* key, uint64_t value) {
    append_memacc_key(p, end, key);
    append_hex64(p, end, value);
}

void append_memacc_bool(char*& p, const char* end, const char* key, bool value) { append_memacc_dec(p, end, key, value ? 1U : 0U); }

void append_percent_byte(char*& p, const char* end, uint8_t value) {
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    if (p + 3 >= end) {
        return;
    }
    *p++ = '%';
    *p++ = HEX_DIGITS.at((value >> 4U) & 0xfU);
    *p++ = HEX_DIGITS.at(value & 0xfU);
}

void append_memacc_str(char*& p, const char* end, const char* key, const char* value) {
    append_memacc_key(p, end, key);
    if (value == nullptr || value[0] == '\0') {
        append_sconst(p, end, "-");
        return;
    }
    while (*value != '\0' && p + 1 < end) {
        auto const CH = static_cast<uint8_t>(*value);
        if (CH <= static_cast<uint8_t>(' ') || CH == static_cast<uint8_t>('%') || CH == static_cast<uint8_t>('=')) {
            append_percent_byte(p, end, CH);
        } else {
            append_char(p, end, *value);
        }
        value++;
    }
}

void append_memacc_feature_row(char*& p, const char* end, const char* name, bool available, bool enabled, bool default_enabled,
                               uint64_t generation) {
    append_sconst(p, end, "feature");
    append_memacc_str(p, end, "name", name);
    append_memacc_bool(p, end, "available", available);
    append_memacc_bool(p, end, "default", default_enabled);
    append_memacc_bool(p, end, "enabled", enabled);
    append_memacc_dec(p, end, "generation", generation);
    append_char(p, end, '\n');
}

auto task_type_name(ker::mod::sched::task::TaskType type) -> const char* {
    switch (type) {
        case ker::mod::sched::task::TaskType::DAEMON:
            return "daemon";
        case ker::mod::sched::task::TaskType::PROCESS:
            return "process";
        case ker::mod::sched::task::TaskType::IDLE:
            return "idle";
    }
    return "unknown";
}

auto task_queue_name(enum ker::mod::sched::task::Task::sched_queue queue) -> const char* {
    switch (queue) {
        case ker::mod::sched::task::Task::sched_queue::NONE:
            return "none";
        case ker::mod::sched::task::Task::sched_queue::RUNNABLE:
            return "runnable";
        case ker::mod::sched::task::Task::sched_queue::WAITING:
            return "waiting";
        case ker::mod::sched::task::Task::sched_queue::DEAD_GC:
            return "dead_gc";
    }
    return "unknown";
}

auto task_state_name(const ker::mod::sched::task::Task* task) -> const char* {
    if (task == nullptr) {
        return "unknown";
    }
    auto const STATE = task->state.load(std::memory_order_acquire);
    if (STATE == ker::mod::sched::task::TaskState::DEAD || task->has_exited) {
        return "dead";
    }
    if (STATE == ker::mod::sched::task::TaskState::EXITING) {
        return "exiting";
    }
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::RUNNABLE) {
        return "running";
    }
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING) {
        return task->wait_channel != nullptr ? "blocked" : "sleeping";
    }
    return "sleeping";
}

constexpr uint64_t MEMACC_PAGE_BYTES = ker::mod::mm::paging::PAGE_SIZE;

auto pages_to_bytes(uint64_t pages) -> uint64_t { return pages * MEMACC_PAGE_BYTES; }

// Caller holds SharedVmemPublicationGuard, keeping image metadata and pagemap
// publication coherent while the bounded userspace loader catalog is read.
auto task_memacc_layout_locked(ker::mod::sched::task::Task& task) -> ker::mod::mm::memacc::UserMemoryLayout {
    static_assert(ker::mod::mm::memacc::UserMemoryLayout::MAX_IMAGE_RANGES >= ker::loader::runtime::MAX_IMAGES);
    std::array<ker::loader::runtime::RuntimeImage, ker::loader::runtime::MAX_IMAGES> images{};
    size_t image_count = 0;
    (void)snapshot_proc_runtime_images(task, images, image_count);

    ker::mod::mm::memacc::UserMemoryLayout layout{};
    for (size_t i = 0; i < image_count && layout.image_count < layout.images.size(); ++i) {
        auto const& image = images.at(i);
        if ((image.flags & ker::loader::runtime::IMAGE_EXACT_MAPPING) == 0U || image.image_start >= image.image_end) {
            continue;
        }
        layout.images.at(layout.image_count++) = {.start = image.image_start, .end = image.image_end};
    }
    return layout;
}

struct MemaccProcessTotals {
    uint64_t task_count;
    uint64_t process_count;
    uint64_t kernel_task_count;
    uint64_t virtual_bytes;
    uint64_t resident_bytes;
    uint64_t swapped_bytes;
    uint64_t swap_inflight_bytes;
    uint64_t shared_bytes;
    uint64_t pte_bytes;
    uint64_t code_bytes;
    uint64_t heap_bytes;
    uint64_t mmap_bytes;
    uint64_t stack_bytes;
    uint64_t low_address_bytes;
    uint64_t high_runtime_bytes;
    uint64_t rw_bytes;
    uint64_t rx_bytes;
    uint64_t ro_bytes;
};

void add_process_totals(MemaccProcessTotals& totals, const ker::mod::mm::memacc::UserMemoryBreakdown& mem) {
    totals.virtual_bytes += pages_to_bytes(mem.virtual_pages);
    totals.resident_bytes += pages_to_bytes(mem.resident_pages);
    totals.shared_bytes += pages_to_bytes(mem.shared_pages);
    totals.pte_bytes += pages_to_bytes(mem.page_table_pages);
    totals.code_bytes += pages_to_bytes(mem.code_pages);
    totals.heap_bytes += pages_to_bytes(mem.heap_pages);
    totals.mmap_bytes += pages_to_bytes(mem.mmap_pages);
    totals.stack_bytes += pages_to_bytes(mem.stack_pages);
    totals.low_address_bytes += pages_to_bytes(mem.low_address_pages);
    totals.high_runtime_bytes += pages_to_bytes(mem.high_runtime_pages);
    totals.rw_bytes += pages_to_bytes(mem.rw_pages);
    totals.rx_bytes += pages_to_bytes(mem.rx_pages);
    totals.ro_bytes += pages_to_bytes(mem.ro_pages);
}

auto collect_memacc_process_totals() -> MemaccProcessTotals {
    MemaccProcessTotals totals{};
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        totals.task_count++;
        if (task->type != ker::mod::sched::task::TaskType::PROCESS) {
            totals.kernel_task_count++;
        }
        if (ker::mod::sched::task::process_visible(*task)) {
            totals.process_count++;
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            ProcPagemapPin pin(*task);
            auto const LAYOUT = pin.pagemap != nullptr ? task_memacc_layout_locked(*task) : ker::mod::mm::memacc::UserMemoryLayout{};
            add_process_totals(totals, ker::mod::mm::memacc::collect_user_memory_breakdown(pin.pagemap, LAYOUT));
            auto const SWAP_MEM = ker::mod::mm::virt::collect_user_memory_stats(pin.pagemap);
            totals.swapped_bytes += pages_to_bytes(SWAP_MEM.swapped_pages);
            totals.swap_inflight_bytes += pages_to_bytes(SWAP_MEM.swap_inflight_pages);
        }
        task->release();
    }
    return totals;
}

auto physical_reclaimability_name(ker::mod::mm::phys::PhysicalOwnerReclaimability reclaimability) -> const char* {
    using Reclaimability = ker::mod::mm::phys::PhysicalOwnerReclaimability;
    switch (reclaimability) {
        case Reclaimability::OWNER_TEARDOWN:
            return "owner_teardown";
        case Reclaimability::PRESSURE_RECLAIM:
            return "pressure_reclaim";
        case Reclaimability::PERMANENT:
            return "permanent_reserve";
        case Reclaimability::DIAGNOSTIC_LIFETIME:
            return "diagnostic_lifetime";
    }
    ker::mod::dbg::panic_handler("invalid physical owner reclaimability");
}

auto generate_memacc_summary(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::mod::mm::phys::PhysicalBalanceSnapshot balance{};
    ker::mod::mm::phys::get_physical_balance_snapshot(balance);
    auto const PROC = collect_memacc_process_totals();
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);
    ker::mod::mm::dyn::kmalloc::KmallocTrackedTotals kmalloc{};
    ker::mod::mm::dyn::kmalloc::get_tracked_alloc_breakdown(kmalloc);
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
    ker::mod::mm::virt::AnonymousSwapStatsSnapshot anonymous_swap{};
    ker::mod::mm::virt::get_anonymous_swap_stats_snapshot(anonymous_swap);

    uint64_t const KMALLOC_BYTES = kmalloc.medium_bytes + kmalloc.large_bytes;
    uint64_t const KMALLOC_DEBUG_BYTES = KMALLOC_DEBUG.block_bytes;
    uint64_t const MINI_BYTES = ker::mod::mm::mini_malloc::mini_get_total_slab_bytes();
    uint64_t const LOGICAL_ALLOCATOR_BYTES = KMALLOC_BYTES + MINI_BYTES + KMALLOC_DEBUG_BYTES;
    uint64_t const LOGICAL_CACHE_BYTES = static_cast<uint64_t>(BCACHE.total_bytes) + FILE_CACHE.bytes + ELF_CACHE.bytes +
                                         ipc.approx_alloc_bytes + local_pipe.approx_alloc_bytes;
    uint64_t const TOTAL_PAGES = balance.managed_pages + balance.per_cpu_page_cache_reserve_pages;
    uint64_t const IDENTITY_PAGES = balance.identity_pages + balance.per_cpu_page_cache_reserve_pages;
    uint64_t const USED_PAGES = TOTAL_PAGES - balance.free_pages;
    uint64_t firmware_reserve_pages = 0;
    for (const auto& reserve : balance.firmware_reserves) {
        firmware_reserve_pages += reserve.pages;
    }

    append_sconst(p, end, "summary");
    append_memacc_dec(p, end, "schema", 2);
    append_memacc_dec(p, end, "total_pages", TOTAL_PAGES);
    append_memacc_dec(p, end, "total_bytes", pages_to_bytes(TOTAL_PAGES));
    append_memacc_dec(p, end, "free_pages", balance.free_pages);
    append_memacc_dec(p, end, "free_bytes", pages_to_bytes(balance.free_pages));
    append_memacc_dec(p, end, "used_pages", USED_PAGES);
    append_memacc_dec(p, end, "used_bytes", pages_to_bytes(USED_PAGES));
    append_memacc_dec(p, end, "owner_pages", balance.owner_pages);
    append_memacc_dec(p, end, "owner_bytes", pages_to_bytes(balance.owner_pages));
    append_memacc_dec(p, end, "allocator_metadata_pages", balance.allocator_metadata_pages);
    append_memacc_dec(p, end, "allocator_metadata_bytes", pages_to_bytes(balance.allocator_metadata_pages));
    append_memacc_dec(p, end, "zone_descriptor_pages", balance.zone_descriptor_pages);
    append_memacc_dec(p, end, "zone_descriptor_bytes", pages_to_bytes(balance.zone_descriptor_pages));
    append_memacc_dec(p, end, "per_cpu_page_cache_reserve_pages", balance.per_cpu_page_cache_reserve_pages);
    append_memacc_dec(p, end, "per_cpu_page_cache_reserve_bytes", pages_to_bytes(balance.per_cpu_page_cache_reserve_pages));
    append_memacc_dec(p, end, "identity_pages", IDENTITY_PAGES);
    append_memacc_dec(p, end, "identity_bytes", pages_to_bytes(IDENTITY_PAGES));
    append_memacc_dec(p, end, "firmware_reserve_pages", firmware_reserve_pages);
    append_memacc_dec(p, end, "firmware_reserve_bytes", pages_to_bytes(firmware_reserve_pages));
    append_memacc_dec(p, end, "physical_address_pages", TOTAL_PAGES + firmware_reserve_pages);
    append_memacc_dec(p, end, "physical_address_identity_pages", IDENTITY_PAGES + firmware_reserve_pages);
    append_memacc_dec(p, end, "identity_mismatch_pages", balance.identity_mismatch_pages);
    append_memacc_dec(p, end, "untracked_unreclaimable_pages", balance.untracked_unreclaimable_pages);
    append_memacc_dec(p, end, "processes", PROC.process_count);
    append_memacc_dec(p, end, "tasks", PROC.task_count);
    append_memacc_dec(p, end, "kernel_tasks", PROC.kernel_task_count);
    append_memacc_dec(p, end, "process_virtual_bytes", PROC.virtual_bytes);
    append_memacc_dec(p, end, "process_rss_bytes", PROC.resident_bytes);
    append_memacc_dec(p, end, "process_swap_bytes", PROC.swapped_bytes);
    append_memacc_dec(p, end, "process_swap_inflight_bytes", PROC.swap_inflight_bytes);
    append_memacc_dec(p, end, "process_shared_bytes", PROC.shared_bytes);
    append_memacc_dec(p, end, "process_pte_bytes", PROC.pte_bytes);
    append_memacc_dec(p, end, "process_code_bytes", PROC.code_bytes);
    append_memacc_dec(p, end, "process_heap_bytes", PROC.heap_bytes);
    append_memacc_dec(p, end, "process_mmap_bytes", PROC.mmap_bytes);
    append_memacc_dec(p, end, "process_stack_bytes", PROC.stack_bytes);
    append_memacc_dec(p, end, "process_low_address_bytes", PROC.low_address_bytes);
    append_memacc_dec(p, end, "process_high_runtime_bytes", PROC.high_runtime_bytes);
    append_memacc_dec(p, end, "process_rw_bytes", PROC.rw_bytes);
    append_memacc_dec(p, end, "process_rx_bytes", PROC.rx_bytes);
    append_memacc_dec(p, end, "process_ro_bytes", PROC.ro_bytes);
    append_memacc_dec(p, end, "logical_allocator_bytes", LOGICAL_ALLOCATOR_BYTES);
    append_memacc_dec(p, end, "logical_kmalloc_bytes", KMALLOC_BYTES);
    append_memacc_dec(p, end, "logical_kmalloc_debug_pool_bytes", KMALLOC_DEBUG_BYTES);
    append_memacc_dec(p, end, "logical_mini_slab_bytes", MINI_BYTES);
    append_memacc_dec(p, end, "logical_cache_bytes", LOGICAL_CACHE_BYTES);
    append_char(p, end, '\n');

    append_sconst(p, end, "anonymous_swap");
    append_memacc_dec(p, end, "resident_candidates", anonymous_swap.resident_candidates);
    append_memacc_dec(p, end, "swapped_pages", anonymous_swap.swapped_pages);
    append_memacc_dec(p, end, "pageout_inflight", anonymous_swap.pageout_inflight);
    append_memacc_dec(p, end, "pagein_inflight", anonymous_swap.pagein_inflight);
    append_memacc_dec(p, end, "pageout_attempts", anonymous_swap.pageout_attempts);
    append_memacc_dec(p, end, "pageout_successes", anonymous_swap.pageout_successes);
    append_memacc_dec(p, end, "pageout_failures", anonymous_swap.pageout_failures);
    append_memacc_dec(p, end, "pagein_attempts", anonymous_swap.pagein_attempts);
    append_memacc_dec(p, end, "pagein_successes", anonymous_swap.pagein_successes);
    append_memacc_dec(p, end, "pagein_failures", anonymous_swap.pagein_failures);
    append_memacc_dec(p, end, "pagein_corruption", anonymous_swap.pagein_corruption);
    append_memacc_dec(p, end, "pageout_latency_us", anonymous_swap.pageout_latency_us);
    append_memacc_dec(p, end, "pageout_latency_max_us", anonymous_swap.pageout_latency_max_us);
    append_memacc_dec(p, end, "pagein_latency_us", anonymous_swap.pagein_latency_us);
    append_memacc_dec(p, end, "pagein_latency_max_us", anonymous_swap.pagein_latency_max_us);
    append_char(p, end, '\n');

    for (const auto& descriptor : ker::mod::mm::phys::physical_owner_descriptors()) {
        size_t const OWNER_IDX = static_cast<size_t>(descriptor.owner);
        const auto& owner = balance.owners.at(OWNER_IDX);
        append_sconst(p, end, "physical_owner");
        append_memacc_str(p, end, "name", descriptor.name);
        append_memacc_dec(p, end, "pages", owner.pages);
        append_memacc_dec(p, end, "bytes", pages_to_bytes(owner.pages));
        append_memacc_dec(p, end, "objects", owner.objects);
        append_memacc_str(p, end, "lifetime", descriptor.lifetime);
        append_memacc_str(p, end, "reclaimability", physical_reclaimability_name(descriptor.reclaimability));
        append_memacc_str(p, end, "scaling_bound", descriptor.scaling_bound);
        append_char(p, end, '\n');
    }
    auto append_fixed_owner = [&](const char* name, uint64_t pages, uint64_t objects, const char* lifetime, const char* bound) {
        append_sconst(p, end, "physical_owner");
        append_memacc_str(p, end, "name", name);
        append_memacc_dec(p, end, "pages", pages);
        append_memacc_dec(p, end, "bytes", pages_to_bytes(pages));
        append_memacc_dec(p, end, "objects", objects);
        append_memacc_str(p, end, "lifetime", lifetime);
        append_memacc_str(p, end, "reclaimability", "permanent_reserve");
        append_memacc_str(p, end, "scaling_bound", bound);
        append_char(p, end, '\n');
    };
    append_fixed_owner("physical_allocator_embedded_metadata", balance.allocator_metadata_pages, balance.zone_count, "boot_to_shutdown",
                       "fixed_bytes_per_managed_page");
    append_fixed_owner("physical_zone_descriptors", balance.zone_descriptor_pages, balance.zone_descriptor_pages, "boot_to_shutdown",
                       "one_page_per_managed_zone");
    append_fixed_owner("per_cpu_physical_page_cache_metadata", balance.per_cpu_page_cache_reserve_pages,
                       balance.per_cpu_page_cache_reserve_objects, "boot_to_shutdown", "fixed_bytes_per_configured_cpu");
    for (const auto& descriptor : ker::mod::mm::phys::physical_reserve_descriptors()) {
        const auto& reserve = balance.firmware_reserves.at(static_cast<size_t>(descriptor.kind));
        append_fixed_owner(descriptor.name, reserve.pages, reserve.objects, descriptor.lifetime, descriptor.scaling_bound);
    }

    append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                              ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                              ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_feature_row(p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(),
                              ker::mod::mm::dyn::kmalloc::debug_info_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(),
                              ker::mod::mm::dyn::kmalloc::debug_info_generation());

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_zones(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ZONES = 32;
    std::array<ker::mod::mm::phys::ZoneSnapshot, MAX_ZONES> zones{};
    size_t const ROWS = ker::mod::mm::phys::snapshot_zones(zones.data(), zones.size());
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& zone = zones.at(i);
        append_sconst(p, end, "zone");
        append_memacc_dec(p, end, "id", zone.zone_num);
        append_memacc_str(p, end, "name", zone.name);
        append_memacc_hex(p, end, "start", zone.start);
        append_memacc_dec(p, end, "len_bytes", zone.len);
        append_memacc_dec(p, end, "page_count", zone.page_count);
        append_memacc_dec(p, end, "total_pages", zone.total_pages);
        append_memacc_dec(p, end, "usable_pages", zone.usable_pages);
        append_memacc_dec(p, end, "free_pages", zone.free_pages);
        append_memacc_dec(p, end, "metadata_pages", zone.metadata_pages);
        append_memacc_dec(p, end, "scanned_free_pages", zone.scanned_free_pages);
        append_memacc_dec(p, end, "cached_order0_pages", zone.cached_order0_pages);
        append_memacc_bool(p, end, "has_allocator", zone.has_allocator);
        append_memacc_bool(p, end, "invalid_allocator", zone.invalid_allocator);
        append_memacc_bool(p, end, "bad_order", zone.bad_order);
        append_memacc_bool(p, end, "free_count_mismatch", zone.free_count_mismatch);
        append_char(p, end, '\n');

        for (size_t order = 0; order < zone.buddy_order_counts.size(); ++order) {
            uint64_t const BLOCKS = zone.buddy_order_counts.at(order);
            if (BLOCKS == 0) {
                continue;
            }
            uint64_t const PAGES = BLOCKS * (uint64_t{1} << order);
            append_sconst(p, end, "buddy");
            append_memacc_dec(p, end, "zone", zone.zone_num);
            append_memacc_dec(p, end, "order", order);
            append_memacc_dec(p, end, "free_blocks", BLOCKS);
            append_memacc_dec(p, end, "free_pages", PAGES);
            append_memacc_dec(p, end, "free_bytes", pages_to_bytes(PAGES));
            append_char(p, end, '\n');
        }
    }
    if (ROWS == MAX_ZONES) {
        append_sconst(p, end, "truncated file=zones");
        append_memacc_dec(p, end, "max_rows", MAX_ZONES);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_procs(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::process_visible(*task)) {
            task->release();
            continue;
        }

        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
            ProcPagemapPin pin(*task);
            auto const LAYOUT = pin.pagemap != nullptr ? task_memacc_layout_locked(*task) : ker::mod::mm::memacc::UserMemoryLayout{};
            auto const MEM = ker::mod::mm::memacc::collect_user_memory_breakdown(pin.pagemap, LAYOUT);
            auto const SWAP_MEM = ker::mod::mm::virt::collect_user_memory_stats(pin.pagemap);
            append_sconst(p, end, "proc");
            append_memacc_dec(p, end, "pid", task->pid);
            append_memacc_dec(p, end, "ppid", task->parent_pid);
            auto const CREDENTIALS = task->credentials();
            append_memacc_dec(p, end, "uid", CREDENTIALS.user().real);
            append_memacc_dec(p, end, "gid", CREDENTIALS.group().real);
            append_memacc_str(p, end, "state", task_state_name(task));
            append_memacc_str(p, end, "queue", task_queue_name(task->sched_queue));
            append_memacc_dec(p, end, "cpu", task->cpu);
            append_memacc_str(p, end, "type", task_type_name(task->type));
            auto const NAMES = copy_proc_names_locked(*task);
            append_memacc_str(p, end, "name", NAMES.short_name.data());
            append_memacc_str(p, end, "cmd", NAMES.executable[0] != '\0' ? NAMES.executable.data() : NAMES.short_name.data());
            append_memacc_dec(p, end, "virt_bytes", pages_to_bytes(MEM.virtual_pages));
            append_memacc_dec(p, end, "rss_bytes", pages_to_bytes(MEM.resident_pages));
            append_memacc_dec(p, end, "swap_bytes", pages_to_bytes(SWAP_MEM.swapped_pages));
            append_memacc_dec(p, end, "swap_inflight_bytes", pages_to_bytes(SWAP_MEM.swap_inflight_pages));
            append_memacc_dec(p, end, "shr_bytes", pages_to_bytes(MEM.shared_pages));
            append_memacc_dec(p, end, "pte_bytes", pages_to_bytes(MEM.page_table_pages));
            append_memacc_dec(p, end, "code_bytes", pages_to_bytes(MEM.code_pages));
            append_memacc_dec(p, end, "heap_bytes", pages_to_bytes(MEM.heap_pages));
            append_memacc_dec(p, end, "mmap_bytes", pages_to_bytes(MEM.mmap_pages));
            append_memacc_dec(p, end, "stack_bytes", pages_to_bytes(MEM.stack_pages));
            append_memacc_dec(p, end, "low_address_bytes", pages_to_bytes(MEM.low_address_pages));
            append_memacc_dec(p, end, "high_runtime_bytes", pages_to_bytes(MEM.high_runtime_pages));
            append_memacc_dec(p, end, "rw_bytes", pages_to_bytes(MEM.rw_pages));
            append_memacc_dec(p, end, "rx_bytes", pages_to_bytes(MEM.rx_pages));
            append_memacc_dec(p, end, "ro_bytes", pages_to_bytes(MEM.ro_pages));
            append_memacc_hex(p, end, "pagemap", reinterpret_cast<uint64_t>(pin.pagemap));
            append_char(p, end, '\n');
        }
        task->release();
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_dead(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }

    constexpr size_t BATCH = 64;
    std::array<uint64_t, BATCH> pids{};
    std::array<uint32_t, BATCH> refcounts{};
    for (uint64_t cpu = 0; cpu < cpu_count; ++cpu) {
        auto const STATS = ker::mod::sched::get_run_queue_stats(cpu);
        append_sconst(p, end, "queue");
        append_memacc_dec(p, end, "cpu", cpu);
        append_memacc_dec(p, end, "run", STATS.active_task_count);
        append_memacc_dec(p, end, "wait", STATS.wait_queue_count);
        append_memacc_dec(p, end, "dead", STATS.expired_task_count);
        append_char(p, end, '\n');

        size_t const DEAD_COUNT = ker::mod::sched::get_dead_task_count(cpu);
        for (size_t start = 0; start < DEAD_COUNT; start += BATCH) {
            size_t const ROWS = ker::mod::sched::get_expired_task_refcounts(cpu, pids.data(), refcounts.data(), pids.size(), start);
            for (size_t i = 0; i < ROWS; ++i) {
                append_sconst(p, end, "dead_task");
                append_memacc_dec(p, end, "cpu", cpu);
                append_memacc_dec(p, end, "pid", pids.at(i));
                append_memacc_dec(p, end, "refcount", refcounts.at(i));
                append_char(p, end, '\n');
            }
            if (ROWS == 0) {
                break;
            }
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_alloc_totals(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    ker::mod::mm::phys::AllocStatsSnapshot phys{};
    ker::mod::mm::phys::get_alloc_stats_snapshot(phys);
    append_sconst(p, end, "phys");
    append_memacc_dec(p, end, "total_allocated_bytes", phys.total_allocated_bytes);
    append_memacc_dec(p, end, "total_freed_bytes", phys.total_freed_bytes);
    append_memacc_dec(p, end, "live_allocated_bytes", phys.live_allocated_bytes);
    append_memacc_dec(p, end, "alloc_count", phys.alloc_count);
    append_memacc_dec(p, end, "free_count", phys.free_count);
    append_memacc_dec(p, end, "total_mem_bytes", phys.total_mem_bytes);
    append_memacc_dec(p, end, "free_mem_bytes", phys.free_mem_bytes);
    append_memacc_dec(p, end, "total_allocated_pages", phys.total_allocated_pages);
    append_memacc_dec(p, end, "total_freed_pages", phys.total_freed_pages);
    append_memacc_dec(p, end, "live_allocated_pages", phys.live_allocated_pages);
    append_memacc_dec(p, end, "current_free_pages", phys.current_free_pages);
    append_memacc_dec(p, end, "alloc_operation_count", phys.alloc_count);
    append_memacc_dec(p, end, "free_operation_count", phys.free_count);
    append_char(p, end, '\n');

    ker::mod::mm::dyn::kmalloc::KmallocTrackedTotals kmalloc{};
    ker::mod::mm::dyn::kmalloc::get_tracked_alloc_breakdown(kmalloc);
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
    append_sconst(p, end, "kmalloc");
    append_memacc_dec(p, end, "medium_count", kmalloc.medium_count);
    append_memacc_dec(p, end, "medium_bytes", kmalloc.medium_bytes);
    append_memacc_dec(p, end, "large_count", kmalloc.large_count);
    append_memacc_dec(p, end, "large_bytes", kmalloc.large_bytes);
    append_memacc_dec(p, end, "debug_pool_blocks", KMALLOC_DEBUG.block_count);
    append_memacc_dec(p, end, "debug_pool_bytes", KMALLOC_DEBUG.block_bytes);
    append_memacc_dec(p, end, "debug_capacity", KMALLOC_DEBUG.capacity);
    append_memacc_dec(p, end, "debug_active", KMALLOC_DEBUG.active);
    append_memacc_dec(p, end, "debug_high_water", KMALLOC_DEBUG.high_water);
    append_memacc_dec(p, end, "debug_dropped", KMALLOC_DEBUG.dropped);
    append_memacc_dec(p, end, "total_count", kmalloc.medium_count + kmalloc.large_count);
    append_memacc_dec(p, end, "total_bytes", kmalloc.medium_bytes + kmalloc.large_bytes);
    append_memacc_dec(p, end, "accounted_bytes", kmalloc.medium_bytes + kmalloc.large_bytes + KMALLOC_DEBUG.block_bytes);
    append_char(p, end, '\n');

    append_sconst(p, end, "mini_slab");
    append_memacc_dec(p, end, "total_bytes", ker::mod::mm::mini_malloc::mini_get_total_slab_bytes());
    append_char(p, end, '\n');

    auto const BCACHE = ker::vfs::buffer_cache_stats();
    append_sconst(p, end, "buffer_cache");
    append_memacc_dec(p, end, "buffers", BCACHE.total_buffers);
    append_memacc_dec(p, end, "dirty_buffers", BCACHE.dirty_buffers);
    append_memacc_dec(p, end, "total_bytes", BCACHE.total_bytes);
    append_memacc_dec(p, end, "clean_bytes", BCACHE.clean_bytes);
    append_memacc_dec(p, end, "dirty_bytes", BCACHE.dirty_bytes);
    append_memacc_dec(p, end, "max_bytes", BCACHE.max_bytes);
    append_memacc_dec(p, end, "dirty_bdevs", BCACHE.dirty_bdevs);
    append_memacc_dec(p, end, "dirty_target_bytes", BCACHE.dirty_target_bytes);
    append_memacc_dec(p, end, "dirty_hard_bytes", BCACHE.dirty_hard_bytes);
    append_memacc_dec(p, end, "dirty_waiters", BCACHE.dirty_waiters);
    append_memacc_dec(p, end, "hits", BCACHE.hits);
    append_memacc_dec(p, end, "misses", BCACHE.misses);
    append_memacc_dec(p, end, "disk_read_calls", BCACHE.disk_read_calls);
    append_memacc_dec(p, end, "disk_read_bytes", BCACHE.disk_read_bytes);
    append_memacc_dec(p, end, "metadata_disk_read_calls", BCACHE.metadata_disk_read_calls);
    append_memacc_dec(p, end, "metadata_disk_read_bytes", BCACHE.metadata_disk_read_bytes);
    append_memacc_dec(p, end, "data_disk_read_calls", BCACHE.data_disk_read_calls);
    append_memacc_dec(p, end, "data_disk_read_bytes", BCACHE.data_disk_read_bytes);
    append_memacc_dec(p, end, "range_copy_attempts", BCACHE.range_copy_attempts);
    append_memacc_dec(p, end, "range_copy_cover_hits", BCACHE.range_copy_cover_hits);
    append_memacc_dec(p, end, "range_copy_overlap_hits", BCACHE.range_copy_overlap_hits);
    append_memacc_dec(p, end, "range_copy_no_state", BCACHE.range_copy_no_state);
    append_memacc_dec(p, end, "range_copy_no_overlap", BCACHE.range_copy_no_overlap);
    append_memacc_dec(p, end, "range_copy_incomplete", BCACHE.range_copy_incomplete);
    append_memacc_dec(p, end, "range_copy_overflow", BCACHE.range_copy_overflow);
    append_memacc_dec(p, end, "range_copy_degraded", BCACHE.range_copy_degraded);
    append_char(p, end, '\n');

    std::array<ker::vfs::BufferCacheBdevStats, 16> bcache_bdevs{};
    size_t const BCACHE_BDEVS = ker::vfs::buffer_cache_bdev_stats(bcache_bdevs.data(), bcache_bdevs.size());
    size_t const BCACHE_BDEV_ROWS = std::min(BCACHE_BDEVS, bcache_bdevs.size());
    for (size_t i = 0; i < BCACHE_BDEV_ROWS; ++i) {
        auto const& row = bcache_bdevs.at(i);
        append_sconst(p, end, "buffer_cache_bdev");
        append_memacc_str(p, end, "name", row.name);
        append_memacc_dec(p, end, "dirty_buffers", row.dirty_buffers);
        append_memacc_dec(p, end, "dirty_bytes", row.dirty_bytes);
        append_memacc_dec(p, end, "oldest_dirty_epoch", row.oldest_dirty_epoch);
        append_char(p, end, '\n');
    }

    ker::vfs::VfsCachePerfSnapshot vfs_cache{};
    ker::vfs::vfs_get_cache_perf_snapshot(vfs_cache);
    append_sconst(p, end, "vfs_cache");
    append_memacc_dec(p, end, "metadata_hits", vfs_cache.metadata_hits);
    append_memacc_dec(p, end, "metadata_misses", vfs_cache.metadata_misses);
    append_memacc_dec(p, end, "metadata_stores", vfs_cache.metadata_stores);
    append_memacc_dec(p, end, "metadata_miss_empty", vfs_cache.metadata_miss_empty);
    append_memacc_dec(p, end, "metadata_miss_invalidated", vfs_cache.metadata_miss_invalidated);
    append_memacc_dec(p, end, "metadata_miss_stale_generation", vfs_cache.metadata_miss_stale_generation);
    append_memacc_dec(p, end, "metadata_miss_conflict", vfs_cache.metadata_miss_conflict);
    append_memacc_dec(p, end, "metadata_path_invalidations", vfs_cache.metadata_path_invalidations);
    append_memacc_dec(p, end, "metadata_generation_resets", vfs_cache.metadata_generation_resets);
    append_memacc_dec(p, end, "existence_hits", vfs_cache.existence_hits);
    append_memacc_dec(p, end, "existence_misses", vfs_cache.existence_misses);
    append_memacc_dec(p, end, "existence_stores", vfs_cache.existence_stores);
    append_memacc_dec(p, end, "symlink_hits", vfs_cache.symlink_hits);
    append_memacc_dec(p, end, "symlink_misses", vfs_cache.symlink_misses);
    append_memacc_dec(p, end, "symlink_stores", vfs_cache.symlink_stores);
    append_memacc_dec(p, end, "stream_hits", vfs_cache.stream_hits);
    append_memacc_dec(p, end, "stream_misses", vfs_cache.stream_misses);
    append_memacc_dec(p, end, "stream_backend_reads", vfs_cache.stream_backend_reads);
    append_memacc_dec(p, end, "stream_backend_bytes", vfs_cache.stream_backend_bytes);
    append_memacc_dec(p, end, "stream_copied_bytes", vfs_cache.stream_copied_bytes);
    append_memacc_dec(p, end, "stream_invalidate_empty_skips", vfs_cache.stream_invalidate_empty_skips);
    append_memacc_dec(p, end, "fstat_snapshot_hits", vfs_cache.fstat_snapshot_hits);
    append_memacc_dec(p, end, "fstat_snapshot_misses", vfs_cache.fstat_snapshot_misses);
    append_memacc_dec(p, end, "fstat_snapshot_stores", vfs_cache.fstat_snapshot_stores);
    append_memacc_dec(p, end, "fstat_snapshot_miss_uncacheable", vfs_cache.fstat_snapshot_miss_uncacheable);
    append_memacc_dec(p, end, "fstat_snapshot_miss_bad_args", vfs_cache.fstat_snapshot_miss_bad_args);
    append_memacc_dec(p, end, "fstat_snapshot_miss_no_cache", vfs_cache.fstat_snapshot_miss_no_cache);
    append_memacc_dec(p, end, "fstat_snapshot_miss_pathless", vfs_cache.fstat_snapshot_miss_pathless);
    append_memacc_dec(p, end, "fstat_snapshot_miss_fs", vfs_cache.fstat_snapshot_miss_fs);
    append_memacc_dec(p, end, "fstat_snapshot_miss_empty", vfs_cache.fstat_snapshot_miss_empty);
    append_memacc_dec(p, end, "fstat_snapshot_miss_generation", vfs_cache.fstat_snapshot_miss_generation);
    append_memacc_dec(p, end, "fstat_snapshot_miss_invalidated", vfs_cache.fstat_snapshot_miss_invalidated);
    append_char(p, end, '\n');

    ker::vfs::xfs::XfsDentryCacheStats xfs_dentry{};
    ker::vfs::xfs::xfs_dentry_cache_stats(xfs_dentry);
    append_sconst(p, end, "xfs_dentry_cache");
    append_memacc_dec(p, end, "hits", xfs_dentry.hits);
    append_memacc_dec(p, end, "misses", xfs_dentry.misses);
    append_memacc_dec(p, end, "stores", xfs_dentry.stores);
    append_memacc_dec(p, end, "invalidations", xfs_dentry.invalidations);
    append_char(p, end, '\n');

    ker::vfs::xfs::XfsInodeCacheStats xfs_inode{};
    ker::vfs::xfs::xfs_inode_cache_stats(xfs_inode);
    append_sconst(p, end, "xfs_inode_cache");
    append_memacc_dec(p, end, "idle", xfs_inode.idle_inodes);
    append_memacc_dec(p, end, "retain_limit", xfs_inode.retain_limit);
    append_memacc_dec(p, end, "reclaim_attempts", xfs_inode.reclaim_attempts);
    append_memacc_dec(p, end, "reclaim_runs", xfs_inode.reclaim_runs);
    append_memacc_dec(p, end, "reclaim_busy_skips", xfs_inode.reclaim_busy_skips);
    append_memacc_dec(p, end, "reclaim_buckets", xfs_inode.reclaim_buckets_scanned);
    append_memacc_dec(p, end, "reclaim_victims", xfs_inode.reclaim_victims);
    append_memacc_dec(p, end, "reclaim_us", xfs_inode.reclaim_us);
    append_memacc_dec(p, end, "reclaim_max_us", xfs_inode.reclaim_max_us);
    append_memacc_dec(p, end, "reclaim_cursor", xfs_inode.reclaim_cursor);
    append_memacc_dec(p, end, "reclaim_deferred_releases", xfs_inode.reclaim_deferred_releases);
    append_char(p, end, '\n');

    auto const FILE_CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    append_sconst(p, end, "file_cache");
    append_memacc_dec(p, end, "pages", FILE_CACHE.pages);
    append_memacc_dec(p, end, "bytes", FILE_CACHE.bytes);
    append_memacc_dec(p, end, "capacity_pages", FILE_CACHE.capacity_pages);
    append_char(p, end, '\n');

    auto const ELF_CACHE = ker::net::wki::wki_shared_elf_cache_stats();
    append_sconst(p, end, "wki_elf_cache");
    append_memacc_dec(p, end, "entries", ELF_CACHE.entries);
    append_memacc_dec(p, end, "bytes", ELF_CACHE.bytes);
    append_memacc_dec(p, end, "max_entries", ELF_CACHE.max_entries);
    append_memacc_dec(p, end, "max_bytes", ELF_CACHE.max_bytes);
    append_char(p, end, '\n');

    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    append_sconst(p, end, "wki_ipc");
    append_memacc_dec(p, end, "exports", ipc.exports);
    append_memacc_dec(p, end, "proxies", ipc.proxies);
    append_memacc_dec(p, end, "pump_tasks", ipc.pump_tasks);
    append_memacc_dec(p, end, "ring_bytes", ipc.proxy_ring_bytes);
    append_memacc_dec(p, end, "ring_used_bytes", ipc.proxy_ring_used_bytes);
    append_memacc_dec(p, end, "pending_chunks", ipc.pending_chunks);
    append_memacc_dec(p, end, "pending_bytes", ipc.pending_bytes);
    append_memacc_dec(p, end, "export_backlog_bytes", ipc.export_backlog_bytes);
    append_memacc_dec(p, end, "export_close_pending", ipc.export_close_pending);
    append_memacc_dec(p, end, "export_close_waiting_for_bytes", ipc.export_close_waiting_for_bytes);
    append_memacc_dec(p, end, "proxy_close_queue", ipc.proxy_close_queue);
    append_memacc_dec(p, end, "proxy_close_attempts", ipc.proxy_close_attempts);
    append_memacc_dec(p, end, "dev_op_payload_bytes", ipc.dev_op_payload_bytes);
    append_memacc_dec(p, end, "approx_alloc_bytes", ipc.approx_alloc_bytes);
    append_char(p, end, '\n');

    ker::vfs::LocalPipePerfSnapshot local_pipe{};
    ker::vfs::vfs_get_local_pipe_perf_snapshot(local_pipe);
    append_sconst(p, end, "local_pipe");
    append_memacc_dec(p, end, "active_pipes", local_pipe.active_pipes);
    append_memacc_dec(p, end, "created", local_pipe.created_since_reset);
    append_memacc_dec(p, end, "peak_pipes", local_pipe.peak_pipes);
    append_memacc_dec(p, end, "capacity_bytes", local_pipe.capacity_bytes);
    append_memacc_dec(p, end, "buffered_bytes", local_pipe.buffered_bytes);
    append_memacc_dec(p, end, "reader_waiters", local_pipe.reader_waiters);
    append_memacc_dec(p, end, "writer_waiters", local_pipe.writer_waiters);
    append_memacc_dec(p, end, "poll_waiters", local_pipe.poll_waiters);
    append_memacc_dec(p, end, "approx_alloc_bytes", local_pipe.approx_alloc_bytes);
    append_char(p, end, '\n');

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_slabs(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_SLABS = 16;
    std::array<ker::mod::mm::mini_malloc::MiniSlabStats, MAX_SLABS> slabs{};
    size_t const ROWS = ker::mod::mm::mini_malloc::mini_collect_slab_stats(slabs.data(), slabs.size());
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& slab = slabs.at(i);
        append_sconst(p, end, "slab");
        append_memacc_str(p, end, "class", slab.name);
        append_memacc_dec(p, end, "object_size", slab.object_size);
        append_memacc_dec(p, end, "slabs", slab.slab_count);
        append_memacc_dec(p, end, "total_blocks", slab.total_blocks);
        append_memacc_dec(p, end, "free_blocks", slab.free_blocks);
        append_memacc_dec(p, end, "used_blocks", slab.total_blocks >= slab.free_blocks ? slab.total_blocks - slab.free_blocks : 0);
        append_memacc_dec(p, end, "page_bytes", slab.page_bytes);
        append_memacc_dec(p, end, "used_object_bytes",
                          (slab.total_blocks >= slab.free_blocks ? slab.total_blocks - slab.free_blocks : 0) * slab.object_size);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_kmalloc_live(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ROWS = 2048;
    auto* rows = new (std::nothrow) ker::mod::mm::dyn::kmalloc::KmallocLiveAlloc[MAX_ROWS];
    if (rows == nullptr) {
        append_sconst(p, end, "error file=kmalloc_live reason=oom\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }
    size_t total_rows = 0;
    size_t const ROWS = ker::mod::mm::dyn::kmalloc::snapshot_live_allocs(rows, MAX_ROWS, total_rows);
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& row = rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "kmalloc_live");
        append_memacc_str(p, end, "tier", row.tier);
        append_memacc_hex(p, end, "addr", row.addr);
        append_memacc_dec(p, end, "size", row.size);
        append_memacc_bool(p, end, "has_debug", row.has_debug);
        if (row.has_debug) {
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_str(p, end, "tag", row.tag);
        }
        append_char(p, end, '\n');
    }
    if (ROWS < total_rows) {
        append_sconst(p, end, "truncated file=kmalloc_live");
        append_memacc_dec(p, end, "rows", ROWS);
        append_memacc_dec(p, end, "total_rows", total_rows);
        append_char(p, end, '\n');
    }
    delete[] rows;
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_kmalloc_callers(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    constexpr size_t MAX_ROWS = 512;
    auto* rows = new (std::nothrow) ker::mod::mm::dyn::kmalloc::KmallocCallerStat[MAX_ROWS];
    if (rows == nullptr) {
        append_sconst(p, end, "error file=kmalloc_callers reason=oom\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }

    size_t total_rows = 0;
    size_t const ROWS = ker::mod::mm::dyn::kmalloc::snapshot_caller_stats(rows, MAX_ROWS, total_rows);
    for (size_t i = 0; i < ROWS; ++i) {
        const auto& row = rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "kmalloc_caller");
        append_memacc_str(p, end, "tier", row.tier);
        append_memacc_dec(p, end, "count", row.count);
        append_memacc_dec(p, end, "bytes", row.bytes);
        append_memacc_bool(p, end, "has_debug", row.has_debug);
        if (row.has_debug) {
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_str(p, end, "tag", row.tag);
        }
        append_char(p, end, '\n');
    }
    if (ROWS < total_rows) {
        append_sconst(p, end, "truncated file=kmalloc_callers");
        append_memacc_dec(p, end, "rows", ROWS);
        append_memacc_dec(p, end, "total_rows", total_rows);
        append_char(p, end, '\n');
    }

    delete[] rows;
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_page_callers(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    append_sconst(p, end, "page_callers_status");
    append_memacc_bool(p, end, "available", ker::mod::mm::phys::page_caller_stats_available());
    append_memacc_bool(p, end, "enabled", ker::mod::mm::phys::page_caller_stats_enabled());
    append_memacc_dec(p, end, "generation", ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_str(p, end, "mode", "live");
    append_char(p, end, '\n');

    constexpr size_t MAX_ROWS = 64;
    std::array<ker::mod::mm::phys::CallerPageStat, MAX_ROWS> rows{};
    size_t total_rows = 0;
    if (ker::mod::mm::phys::snapshot_page_caller_stats(rows.data(), rows.size(), total_rows)) {
        size_t const ROWS = std::min(total_rows, rows.size());
        for (size_t i = 0; i < ROWS; ++i) {
            const auto& row = rows.at(i);
            append_sconst(p, end, "page_caller");
            append_memacc_hex(p, end, "caller", row.caller);
            append_memacc_dec(p, end, "pages", row.pages);
            append_memacc_dec(p, end, "bytes", pages_to_bytes(row.pages));
            append_char(p, end, '\n');
        }
        if (ROWS < total_rows) {
            append_sconst(p, end, "truncated file=page_callers");
            append_memacc_dec(p, end, "rows", ROWS);
            append_memacc_dec(p, end, "total_rows", total_rows);
            append_char(p, end, '\n');
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_features(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                              ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                              ker::mod::mm::phys::page_caller_stats_generation());
    append_memacc_feature_row(p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(),
                              ker::mod::mm::dyn::kmalloc::debug_info_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(),
                              ker::mod::mm::dyn::kmalloc::debug_info_generation());
    auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
    append_sconst(p, end, "kmalloc_debug_pool");
    append_memacc_dec(p, end, "blocks", KMALLOC_DEBUG.block_count);
    append_memacc_dec(p, end, "bytes", KMALLOC_DEBUG.block_bytes);
    append_memacc_dec(p, end, "capacity", KMALLOC_DEBUG.capacity);
    append_memacc_dec(p, end, "active", KMALLOC_DEBUG.active);
    append_memacc_dec(p, end, "high_water", KMALLOC_DEBUG.high_water);
    append_memacc_dec(p, end, "dropped", KMALLOC_DEBUG.dropped);
    append_char(p, end, '\n');

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_track_state(ProcNodeType type, char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    if (type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE) {
        append_memacc_feature_row(p, end, "page_callers", ker::mod::mm::phys::page_caller_stats_available(),
                                  ker::mod::mm::phys::page_caller_stats_enabled(), ker::mod::mm::phys::page_caller_stats_default_enabled(),
                                  ker::mod::mm::phys::page_caller_stats_generation());
    } else {
        append_memacc_feature_row(
            p, end, "kmalloc_debug", ker::mod::mm::dyn::kmalloc::debug_info_available(), ker::mod::mm::dyn::kmalloc::debug_info_enabled(),
            ker::mod::mm::dyn::kmalloc::debug_info_default_enabled(), ker::mod::mm::dyn::kmalloc::debug_info_generation());
        auto const KMALLOC_DEBUG = ker::mod::mm::dyn::kmalloc::debug_info_stats();
        append_sconst(p, end, "kmalloc_debug_pool");
        append_memacc_dec(p, end, "blocks", KMALLOC_DEBUG.block_count);
        append_memacc_dec(p, end, "bytes", KMALLOC_DEBUG.block_bytes);
        append_memacc_dec(p, end, "capacity", KMALLOC_DEBUG.capacity);
        append_memacc_dec(p, end, "active", KMALLOC_DEBUG.active);
        append_memacc_dec(p, end, "high_water", KMALLOC_DEBUG.high_water);
        append_memacc_dec(p, end, "dropped", KMALLOC_DEBUG.dropped);
        append_char(p, end, '\n');
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_buffer_cache(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto const BCACHE = ker::vfs::buffer_cache_stats();
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "buffer_cache");
    append_memacc_dec(p, end, "total_bytes", BCACHE.total_bytes);
    append_memacc_dec(p, end, "clean_bytes", BCACHE.clean_bytes);
    append_memacc_dec(p, end, "dirty_bytes", BCACHE.dirty_bytes);
    append_memacc_dec(p, end, "buffers", BCACHE.total_buffers);
    append_memacc_dec(p, end, "dirty_buffers", BCACHE.dirty_buffers);
    append_memacc_dec(p, end, "default_target_bytes", 0);
    append_memacc_dec(p, end, "dirty_target_bytes", BCACHE.dirty_target_bytes);
    append_memacc_dec(p, end, "dirty_hard_bytes", BCACHE.dirty_hard_bytes);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_packet_pool(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto const POOL = ker::net::pkt_pool_snapshot();
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "packet_pool");
    append_memacc_dec(p, end, "capacity", POOL.capacity);
    append_memacc_dec(p, end, "chunk_count", POOL.chunk_count);
    append_memacc_dec(p, end, "baseline_capacity", POOL.baseline_capacity);
    append_memacc_dec(p, end, "active_capacity", POOL.active_capacity);
    append_memacc_dec(p, end, "free", POOL.free);
    append_memacc_dec(p, end, "used", POOL.used);
    append_memacc_dec(p, end, "draining_buffers", POOL.draining_buffers);
    append_memacc_dec(p, end, "draining_free", POOL.draining_free);
    append_memacc_dec(p, end, "buffer_size", POOL.buffer_size);
    append_memacc_dec(p, end, "object_size", POOL.object_size);
    append_memacc_dec(p, end, "physical_pages_per_buffer", POOL.physical_pages_per_buffer);
    append_memacc_dec(p, end, "physical_bytes_per_buffer", POOL.physical_pages_per_buffer * ker::mod::mm::paging::PAGE_SIZE);
    append_memacc_dec(p, end, "total_bytes", POOL.capacity * POOL.object_size);
    append_memacc_dec(p, end, "active_bytes", POOL.active_capacity * POOL.object_size);
    append_memacc_dec(p, end, "free_bytes", POOL.free * POOL.object_size);
    append_memacc_dec(p, end, "used_bytes", POOL.used * POOL.object_size);
    append_memacc_dec(p, end, "draining_bytes", POOL.draining_buffers * POOL.object_size);
    append_memacc_dec(p, end, "draining_free_bytes", POOL.draining_free * POOL.object_size);
    append_memacc_dec(p, end, "rx_reserve", POOL.rx_reserve);
    append_memacc_dec(p, end, "tx_reserve", ker::net::PKT_POOL_TX_RESERVE);
    append_memacc_dec(p, end, "permanent_reserve", POOL.baseline_capacity);
    append_memacc_dec(p, end, "grow_chunk", POOL.grow_chunk);
    append_memacc_dec(p, end, "minimum_reclaim_quantum_buffers", POOL.grow_chunk);
    append_memacc_dec(p, end, "minimum_reclaim_quantum_pages", POOL.grow_chunk * POOL.physical_pages_per_buffer);
    append_memacc_bool(p, end, "reclaim_chunk_size_variable", true);
    append_memacc_dec(p, end, "default_target_capacity", std::max(POOL.baseline_capacity, POOL.used + POOL.rx_reserve + POOL.grow_chunk));
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_xfs_inode(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    ker::vfs::xfs::XfsInodeCacheStats stats{};
    ker::vfs::xfs::xfs_inode_cache_stats(stats);
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "xfs_inode");
    append_memacc_dec(p, end, "idle_inodes", stats.idle_inodes);
    append_memacc_dec(p, end, "retain_limit", stats.retain_limit);
    append_memacc_dec(p, end, "reclaim_victims", stats.reclaim_victims);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_file_mmap_cache(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto const CACHE = ker::syscall::vmem::file_mmap_cache_stats();
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "file_mmap_cache");
    append_memacc_dec(p, end, "pages", CACHE.pages);
    append_memacc_dec(p, end, "bytes", CACHE.bytes);
    append_memacc_dec(p, end, "capacity_pages", CACHE.capacity_pages);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_anonymous_swap(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;
    ker::mod::mm::virt::AnonymousSwapStatsSnapshot stats{};
    ker::mod::mm::virt::get_anonymous_swap_stats_snapshot(stats);
    append_sconst(p, end, "reclaim");
    append_memacc_str(p, end, "name", "anonymous_swap");
    append_memacc_dec(p, end, "resident_candidates", stats.resident_candidates);
    append_memacc_dec(p, end, "swapped_pages", stats.swapped_pages);
    append_memacc_dec(p, end, "pageout_inflight", stats.pageout_inflight);
    append_memacc_dec(p, end, "pagein_inflight", stats.pagein_inflight);
    append_memacc_dec(p, end, "pageout_successes", stats.pageout_successes);
    append_memacc_dec(p, end, "pagein_successes", stats.pagein_successes);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

auto generate_memacc_reclaim_coordinator(char* buf, size_t bufsz) -> size_t {
    namespace reclaim = ker::mod::mm::reclaim;
    char* p = buf;
    char const* end = buf + bufsz - 1;

    reclaim::GlobalStatsSnapshot global{};
    reclaim::get_global_stats(global);
    uint64_t attempts = reclaim::saturating_add(global.direct_attempts, global.background_attempts);
    attempts = reclaim::saturating_add(attempts, global.explicit_attempts);

    append_sconst(p, end, "reclaim_coordinator");
    append_memacc_dec(p, end, "schema", 1);
    append_memacc_str(p, end, "pressure", reclaim::pressure_name(global.current_pressure));
    append_memacc_dec(p, end, "attempts", attempts);
    append_memacc_dec(p, end, "direct_attempts", global.direct_attempts);
    append_memacc_dec(p, end, "background_attempts", global.background_attempts);
    append_memacc_dec(p, end, "explicit_attempts", global.explicit_attempts);
    append_memacc_dec(p, end, "explicit_requests", global.explicit_requests);
    append_memacc_dec(p, end, "explicit_completions", global.explicit_completions);
    append_memacc_dec(p, end, "explicit_rejections", global.explicit_rejections);
    append_memacc_dec(p, end, "scans", global.callbacks);
    append_memacc_dec(p, end, "callbacks", global.callbacks);
    append_memacc_dec(p, end, "scanned_pages", global.scanned_pages);
    append_memacc_dec(p, end, "scanned_bytes", global.scanned_bytes);
    append_memacc_dec(p, end, "scanned_objects", global.scanned_objects);
    append_memacc_dec(p, end, "reported_pages", global.reported_pages);
    append_memacc_dec(p, end, "reported_bytes", global.reported_bytes);
    append_memacc_dec(p, end, "reported_objects", global.reported_objects);
    append_memacc_dec(p, end, "reclaimed_pages", global.reclaimed_pages);
    append_memacc_dec(p, end, "reclaimed_bytes", reclaim::saturating_mul(global.reclaimed_pages, ker::mod::mm::paging::PAGE_SIZE));
    append_memacc_dec(p, end, "no_progress", global.no_progress);
    append_memacc_dec(p, end, "failures", global.failures);
    append_memacc_dec(p, end, "recursion_avoided", global.recursion_avoided);
    append_memacc_dec(p, end, "lease_contention", global.lease_contention);
    append_memacc_dec(p, end, "unsafe_context_skips", global.unsafe_context_skips);
    append_memacc_dec(p, end, "capability_skips", global.capability_skips);
    append_memacc_dec(p, end, "cooldown_skips", global.cooldown_skips);
    append_memacc_dec(p, end, "latency_us", global.latency_us);
    append_memacc_dec(p, end, "latency_max_us", global.latency_max_us);
    append_memacc_dec(p, end, "low_transitions", global.low_transitions);
    append_memacc_dec(p, end, "critical_transitions", global.critical_transitions);
    append_memacc_dec(p, end, "recovered_transitions", global.recovered_transitions);
    append_memacc_dec(p, end, "worker_wakeups", global.worker_wakeups);
    append_memacc_dec(p, end, "worker_no_progress", global.worker_no_progress);
    append_char(p, end, '\n');

    std::array<reclaim::ZoneWatermarkSnapshot, reclaim::MAX_WATERMARK_ZONES> zones{};
    size_t const ZONE_ROWS = reclaim::snapshot_zone_watermarks(zones.data(), zones.size(), 0);
    for (size_t i = 0; i < ZONE_ROWS; ++i) {
        auto const& zone = zones.at(i);
        append_sconst(p, end, "zone_watermark");
        append_memacc_dec(p, end, "zone", zone.zone);
        append_memacc_dec(p, end, "total_pages", zone.total_pages);
        append_memacc_dec(p, end, "free_pages", zone.free_pages);
        append_memacc_dec(p, end, "critical_pages", zone.critical_pages);
        append_memacc_dec(p, end, "low_pages", zone.low_pages);
        append_memacc_dec(p, end, "high_pages", zone.high_pages);
        append_memacc_sdec(p, end, "largest_free_order", zone.largest_free_order);
        append_memacc_dec(p, end, "requested_order", zone.requested_order);
        append_memacc_str(p, end, "pressure", reclaim::pressure_name(zone.level));
        append_char(p, end, '\n');
    }

    std::array<reclaim::ShrinkerStatsSnapshot, reclaim::MAX_SHRINKERS> shrinkers{};
    size_t const SHRINKER_ROWS = reclaim::snapshot_shrinker_stats(shrinkers.data(), shrinkers.size());
    for (size_t i = 0; i < SHRINKER_ROWS; ++i) {
        auto const& shrinker = shrinkers.at(i);
        append_sconst(p, end, "reclaim_shrinker");
        append_memacc_str(p, end, "name", shrinker.name);
        append_memacc_str(p, end, "unit", reclaim::unit_name(shrinker.unit));
        append_memacc_str(p, end, "scan_unit", reclaim::unit_name(shrinker.scan_unit));
        if (std::strcmp(shrinker.name, "packet_pool") == 0) {
            append_memacc_str(p, end, "unit_kind", "chunks");
            append_memacc_str(p, end, "scan_unit_kind", "chunks");
            append_memacc_dec(p, end, "minimum_quantum_pages",
                              ker::net::PKT_POOL_GROW_CHUNK * ker::net::pkt_pool_snapshot().physical_pages_per_buffer);
            append_memacc_bool(p, end, "variable_quantum", true);
        }
        append_memacc_dec(p, end, "rank", shrinker.rank);
        append_memacc_str(p, end, "min_priority", reclaim::priority_name(shrinker.min_priority));
        append_memacc_dec(p, end, "capabilities", shrinker.capabilities);
        append_memacc_dec(p, end, "count_calls", shrinker.count_calls);
        append_memacc_dec(p, end, "scan_calls", shrinker.scan_calls);
        append_memacc_dec(p, end, "scanned_units", shrinker.scanned_units);
        append_memacc_dec(p, end, "reclaimed_units", shrinker.reclaimed_units);
        append_memacc_dec(p, end, "reclaimed_pages", shrinker.reclaimed_pages);
        append_memacc_dec(p, end, "reclaimed_bytes", reclaim::saturating_mul(shrinker.reclaimed_pages, ker::mod::mm::paging::PAGE_SIZE));
        append_memacc_dec(p, end, "no_progress", shrinker.no_progress);
        append_memacc_dec(p, end, "failures", shrinker.failures);
        append_memacc_dec(p, end, "context_skips", shrinker.context_skips);
        append_memacc_dec(p, end, "cooldown_skips", shrinker.cooldown_skips);
        append_memacc_dec(p, end, "latency_us", shrinker.latency_us);
        append_memacc_dec(p, end, "latency_max_us", shrinker.latency_max_us);
        append_memacc_dec(p, end, "count_reclaimable", shrinker.last_count.reclaimable);
        append_memacc_dec(p, end, "count_dirty", shrinker.last_count.dirty);
        append_memacc_dec(p, end, "count_pinned", shrinker.last_count.pinned);
        append_memacc_dec(p, end, "count_reserved", shrinker.last_count.reserved);
        append_memacc_dec(p, end, "count_unreclaimable", shrinker.last_count.unreclaimable);
        append_char(p, end, '\n');
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

void append_napi_diag(char*& p, const char* end, const ker::net::NapiDiagSnapshot& diag) {
    append_memacc_bool(p, end, "napi_diag", diag.enabled);
    if (!diag.enabled) return;
    constexpr std::array<const char*, ker::net::NAPI_SCHEDULE_SOURCES> SCHEDULE_NAMES = {"unspecified", "irq", "rearm", "startup"};
    constexpr std::array<const char*, ker::net::NAPI_POLL_SOURCES> POLL_NAMES = {"worker", "inline", "rescue"};
    auto append_source = [&](const char* key, const char* source, uint64_t value) {
        append_char(p, end, ' ');
        append_sconst(p, end, key);
        append_char(p, end, '_');
        append_sconst(p, end, source);
        append_char(p, end, '=');
        append_dec64(p, end, value);
    };
    for (size_t i = 0; i < SCHEDULE_NAMES.size(); ++i) {
        append_source("sched", SCHEDULE_NAMES.at(i), diag.schedules.at(i));
        append_source("accepted", SCHEDULE_NAMES.at(i), diag.accepted.at(i));
    }
    for (size_t i = 0; i < POLL_NAMES.size(); ++i) {
        append_source("poll", POLL_NAMES.at(i), diag.polls.at(i));
        append_source("rxwork", POLL_NAMES.at(i), diag.rx_work.at(i));
        append_source("rxzero", POLL_NAMES.at(i), diag.rx_zero.at(i));
        append_source("txreclaimed", POLL_NAMES.at(i), diag.tx_reclaimed.at(i));
    }
    append_memacc_dec(p, end, "sched_coalesced", diag.coalesced);
    append_memacc_dec(p, end, "sched_polling_reject", diag.polling_rejects);
    append_memacc_dec(p, end, "sched_disabled_reject", diag.disabled_rejects);
    append_memacc_dec(p, end, "park_calls", diag.park_calls);
    append_memacc_dec(p, end, "park_empty_returns", diag.park_empty_returns);
    append_memacc_dec(p, end, "contention_sleeps", diag.contention_sleeps);
    append_memacc_dec(p, end, "inline_refused", diag.inline_refused);
    append_memacc_dec(p, end, "budget_handoffs", diag.budget_handoffs);
}

void append_virtqueue_diag(char*& p, const char* end, const char* prefix, const ker::dev::virtio::VirtqueueDiagSnapshot& q) {
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_size=");
    append_dec64(p, end, q.size);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_free=");
    append_dec64(p, end, q.num_free);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_inflight=");
    append_dec64(p, end, q.size >= q.num_free ? q.size - q.num_free : 0);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_pending=");
    append_dec64(p, end, q.pending);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_mapped=");
    append_dec64(p, end, q.mapped);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_avail_idx=");
    append_dec64(p, end, q.avail_idx);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_used_idx=");
    append_dec64(p, end, q.used_idx);
    append_char(p, end, ' ');
    append_sconst(p, end, prefix);
    append_sconst(p, end, "_last_used=");
    append_dec64(p, end, q.last_used_idx);
}

auto generate_wki_chaos(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }

    char* p = buf;
    char const* end = buf + bufsz - 1;
    auto* rules = new (std::nothrow) ker::net::wki::WkiChaosRuleRow[ker::net::wki::WKI_CHAOS_MAX_RULES];
    auto* trace = new (std::nothrow) ker::net::wki::WkiChaosTraceRow[ker::net::wki::WKI_CHAOS_MAX_TRACE_ROWS];
    if (rules == nullptr || trace == nullptr) {
        append_sconst(p, end, "wki_chaos_snapshot allocation_failed=1\n");
        delete[] rules;
        delete[] trace;
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }

    ker::net::wki::WkiChaosSnapshot snapshot{};
    size_t rule_count = 0;
    size_t trace_count = 0;
    if (!ker::net::wki::wki_chaos_capture(&snapshot, rules, ker::net::wki::WKI_CHAOS_MAX_RULES, &rule_count, trace,
                                          ker::net::wki::WKI_CHAOS_MAX_TRACE_ROWS, &trace_count)) {
        append_sconst(p, end, "wki_chaos supported=0 snapshot_failed=1\n");
        delete[] rules;
        delete[] trace;
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }

    append_sconst(p, end, "wki_chaos schema=");
    append_dec64(p, end, snapshot.schema_version);
    append_sconst(p, end, " supported=");
    append_bool01(p, end, snapshot.supported);
    append_sconst(p, end, " runtime_control=");
    append_bool01(p, end, snapshot.runtime_control_allowed);
    append_sconst(p, end, " enabled=");
    append_bool01(p, end, snapshot.enabled);
    append_sconst(p, end, " seed=");
    append_dec64(p, end, snapshot.seed);
    append_sconst(p, end, " rules=");
    append_dec64(p, end, snapshot.active_rule_count);
    append_sconst(p, end, " queued=");
    append_dec64(p, end, snapshot.queued_frame_count + snapshot.queued_block_event_count);
    append_sconst(p, end, " queued_frames=");
    append_dec64(p, end, snapshot.queued_frame_count);
    append_sconst(p, end, " queued_block_events=");
    append_dec64(p, end, snapshot.queued_block_event_count);
    append_sconst(p, end, " trace_rows=");
    append_dec64(p, end, snapshot.trace_count);
    append_sconst(p, end, " first_event=");
    append_dec64(p, end, snapshot.trace_first_event_id);
    append_sconst(p, end, " last_event=");
    append_dec64(p, end, snapshot.trace_last_event_id);
    append_sconst(p, end, " queue_overflow=");
    append_bool01(p, end, snapshot.queue_overflow);
    append_sconst(p, end, " trace_overflow=");
    append_bool01(p, end, snapshot.trace_overflow);
    append_sconst(p, end, " stream_overflow=");
    append_bool01(p, end, snapshot.stream_overflow);
    append_sconst(p, end, " invalid=");
    append_bool01(p, end, snapshot.invalid);
    append_char(p, end, '\n');

    const auto& counters = snapshot.counters;
    append_sconst(p, end, "wki_chaos_counters observed=");
    append_dec64(p, end, counters.observed);
    append_sconst(p, end, " matched=");
    append_dec64(p, end, counters.matched);
    append_sconst(p, end, " passed=");
    append_dec64(p, end, counters.passed);
    append_sconst(p, end, " dropped=");
    append_dec64(p, end, counters.dropped);
    append_sconst(p, end, " duplicated=");
    append_dec64(p, end, counters.duplicated);
    append_sconst(p, end, " delayed=");
    append_dec64(p, end, counters.delayed);
    append_sconst(p, end, " reordered=");
    append_dec64(p, end, counters.reordered);
    append_sconst(p, end, " corrupted=");
    append_dec64(p, end, counters.corrupted);
    append_sconst(p, end, " failed=");
    append_dec64(p, end, counters.failed);
    append_sconst(p, end, " released=");
    append_dec64(p, end, counters.released);
    append_sconst(p, end, " coalesced=");
    append_dec64(p, end, counters.coalesced);
    append_sconst(p, end, " secure_pressure_drops=");
    append_dec64(p, end, snapshot.secure_pressure_drops);
    append_char(p, end, '\n');

    for (size_t i = 0; i < rule_count; ++i) {
        const auto& rule = rules[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "wki_chaos_rule id=");
        append_dec64(p, end, rule.id);
        append_sconst(p, end, " active=");
        append_bool01(p, end, rule.active);
        append_sconst(p, end, " surface=");
        append_sconst(p, end, chaos_surface_name(rule.surface));
        append_sconst(p, end, " direction=");
        append_sconst(p, end, chaos_direction_name(rule.direction));
        append_sconst(p, end, " action=");
        append_sconst(p, end, chaos_action_name(rule.action));
        append_sconst(p, end, " match_transport=");
        append_bool01(p, end, rule.match_transport);
        append_sconst(p, end, " transport=");
        append_dec64(p, end, rule.transport_id);
        append_sconst(p, end, " match_neighbor=");
        append_bool01(p, end, rule.match_neighbor);
        append_sconst(p, end, " neighbor=");
        append_hex16(p, end, rule.neighbor);
        append_sconst(p, end, " match_src=");
        append_bool01(p, end, rule.match_src);
        append_sconst(p, end, " src=");
        append_hex16(p, end, rule.src_node);
        append_sconst(p, end, " match_dst=");
        append_bool01(p, end, rule.match_dst);
        append_sconst(p, end, " dst=");
        append_hex16(p, end, rule.dst_node);
        append_sconst(p, end, " match_channel=");
        append_bool01(p, end, rule.match_channel);
        append_sconst(p, end, " channel=");
        append_dec64(p, end, rule.channel_id);
        append_sconst(p, end, " match_type=");
        append_bool01(p, end, rule.match_type);
        append_sconst(p, end, " type=");
        append_dec64(p, end, rule.msg_type);
        append_sconst(p, end, " match_op=");
        append_bool01(p, end, rule.match_op);
        append_sconst(p, end, " op=");
        append_hex16(p, end, rule.op_id);
        append_sconst(p, end, " match_origin=");
        append_bool01(p, end, rule.match_block_origin);
        append_sconst(p, end, " origin=");
        append_sconst(p, end, chaos_block_origin_name(rule.block_origin));
        append_sconst(p, end, " match_lane=");
        append_bool01(p, end, rule.match_block_lane);
        append_sconst(p, end, " lane=");
        append_sconst(p, end, chaos_block_lane_name(rule.block_lane));
        append_sconst(p, end, " match_zone=");
        append_bool01(p, end, rule.match_zone);
        append_sconst(p, end, " zone=");
        append_dec64(p, end, rule.zone_id);
        append_sconst(p, end, " match_resource=");
        append_bool01(p, end, rule.match_resource);
        append_sconst(p, end, " resource=");
        append_dec64(p, end, rule.resource_id);
        append_sconst(p, end, " match_ring_generation=");
        append_bool01(p, end, rule.match_ring_generation);
        append_sconst(p, end, " ring_generation=");
        append_dec64(p, end, rule.ring_generation);
        append_sconst(p, end, " match_ring_index=");
        append_bool01(p, end, rule.match_ring_index);
        append_sconst(p, end, " ring_index=");
        append_dec64(p, end, rule.ring_index);
        append_sconst(p, end, " match_cookie=");
        append_bool01(p, end, rule.match_cookie);
        append_sconst(p, end, " cookie=");
        append_dec64(p, end, rule.operation_cookie);
        append_sconst(p, end, " match_blk_op=");
        append_bool01(p, end, rule.match_block_opcode);
        append_sconst(p, end, " blk_op=");
        append_dec64(p, end, rule.block_opcode);
        append_sconst(p, end, " match_seq=");
        append_bool01(p, end, rule.match_seq);
        append_sconst(p, end, " seq=");
        append_dec64(p, end, rule.seq_num);
        append_sconst(p, end, " match_occurrence=");
        append_bool01(p, end, rule.match_occurrence);
        append_sconst(p, end, " occurrence=");
        append_dec64(p, end, rule.occurrence);
        append_sconst(p, end, " after=");
        append_dec64(p, end, rule.after);
        append_sconst(p, end, " every=");
        append_dec64(p, end, rule.every);
        append_sconst(p, end, " limit=");
        append_dec64(p, end, rule.limit);
        append_sconst(p, end, " chance_permyriad=");
        append_dec64(p, end, rule.chance_permyriad);
        append_sconst(p, end, " corrupt_mode=");
        append_sconst(p, end, chaos_corrupt_mode_name(rule.corrupt_mode));
        append_sconst(p, end, " payload_offset=");
        append_dec64(p, end, rule.payload_offset);
        append_sconst(p, end, " payload_xor=");
        append_dec64(p, end, rule.payload_xor);
        append_sconst(p, end, " sqe_offset=");
        append_dec64(p, end, rule.sqe_offset);
        append_sconst(p, end, " sqe_xor=");
        append_dec64(p, end, rule.sqe_xor);
        append_sconst(p, end, " applied=");
        append_dec64(p, end, rule.applied);
        append_char(p, end, '\n');
    }

    for (size_t i = 0; i < trace_count; ++i) {
        const auto& row = trace[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "wki_chaos_trace event=");
        append_dec64(p, end, row.event_id);
        append_sconst(p, end, " release_order=");
        append_dec64(p, end, row.release_order);
        append_sconst(p, end, " queued_event=");
        append_dec64(p, end, row.queued_event_id);
        append_sconst(p, end, " surface=");
        append_sconst(p, end, chaos_surface_name(row.surface));
        if (row.surface == ker::net::wki::WkiChaosSurface::FRAME) {
            append_sconst(p, end, " direction=");
            append_sconst(p, end, chaos_direction_name(row.key.direction));
            append_sconst(p, end, " transport=");
            append_dec64(p, end, row.key.transport_id);
            append_sconst(p, end, " neighbor=");
            append_hex16(p, end, row.key.neighbor);
            append_sconst(p, end, " src=");
            append_hex16(p, end, row.key.src_node);
            append_sconst(p, end, " dst=");
            append_hex16(p, end, row.key.dst_node);
            append_sconst(p, end, " channel=");
            append_dec64(p, end, row.key.channel_id);
            append_sconst(p, end, " type=");
            append_dec64(p, end, row.key.msg_type);
            append_sconst(p, end, " seq=");
            append_dec64(p, end, row.key.seq_num);
            append_sconst(p, end, " ack=");
            append_dec64(p, end, row.key.ack_num);
            append_sconst(p, end, " len=");
            append_dec64(p, end, row.key.frame_len);
            append_sconst(p, end, " payload_len=");
            append_dec64(p, end, row.key.payload_len);
            append_sconst(p, end, " frame_valid=");
            append_bool01(p, end, row.key.frame_valid);
            append_sconst(p, end, " dev_op_valid=");
            append_bool01(p, end, row.key.dev_op_valid);
            append_sconst(p, end, " op=");
            append_hex16(p, end, row.key.dev_op_id);
            append_sconst(p, end, " occurrence=");
            append_dec64(p, end, row.key.stream_occurrence);
            append_sconst(p, end, " stable_hash=");
            append_hex64(p, end, row.key.stable_hash);
        } else {
            append_sconst(p, end, " direction=");
            append_sconst(p, end, chaos_direction_name(row.block_key.direction));
            append_sconst(p, end, " origin=");
            append_sconst(p, end, chaos_block_origin_name(row.block_key.origin));
            append_sconst(p, end, " lane=");
            append_sconst(p, end, chaos_block_lane_name(row.block_key.lane));
            append_sconst(p, end, " transport=");
            append_dec64(p, end, row.block_key.transport_id);
            append_sconst(p, end, " neighbor=");
            append_hex16(p, end, row.block_key.neighbor);
            append_sconst(p, end, " zone=");
            append_dec64(p, end, row.block_key.zone_id);
            append_sconst(p, end, " resource=");
            append_dec64(p, end, row.block_key.resource_id);
            append_sconst(p, end, " ring_generation=");
            append_dec64(p, end, row.block_key.ring_generation);
            append_sconst(p, end, " ring_index=");
            append_dec64(p, end, row.block_key.ring_index);
            append_sconst(p, end, " cookie=");
            append_dec64(p, end, row.block_key.operation_cookie);
            append_sconst(p, end, " blk_op=");
            append_dec64(p, end, row.block_key.block_opcode);
            append_sconst(p, end, " attach_cookie=");
            append_dec64(p, end, row.block_key.attach_cookie);
            append_sconst(p, end, " channel_generation=");
            append_dec64(p, end, row.block_key.channel_generation);
            append_sconst(p, end, " owner_boot=");
            append_dec64(p, end, row.block_key.owner_boot_epoch);
            append_sconst(p, end, " incarnation=");
            append_dec64(p, end, row.block_key.resource_incarnation);
            append_sconst(p, end, " descriptor_len=");
            append_dec64(p, end, row.block_key.descriptor_len);
            append_sconst(p, end, " delivery_deadline_us=");
            append_dec64(p, end, row.block_key.delivery_deadline_us);
            append_sconst(p, end, " occurrence=");
            append_dec64(p, end, row.block_key.stream_occurrence);
            append_sconst(p, end, " stable_hash=");
            append_hex64(p, end, row.block_key.stable_hash);
        }
        append_sconst(p, end, " rule=");
        append_dec64(p, end, row.rule_id);
        append_sconst(p, end, " action=");
        append_sconst(p, end, chaos_action_name(row.action));
        append_sconst(p, end, " outcome=");
        append_sconst(p, end, chaos_outcome_name(row.outcome));
        append_sconst(p, end, " transport_result=");
        append_sdec64(p, end, row.transport_result);
        append_sconst(p, end, " coalesced=");
        append_dec64(p, end, row.coalesced_count);
        append_sconst(p, end, " checksum_before=");
        append_dec64(p, end, row.checksum_before);
        append_sconst(p, end, " checksum_after=");
        append_dec64(p, end, row.checksum_after);
        append_sconst(p, end, " corrupt_mode=");
        append_sconst(p, end, chaos_corrupt_mode_name(row.corruption.mode));
        append_sconst(p, end, " payload_offset=");
        append_dec64(p, end, row.corruption.payload_offset);
        append_sconst(p, end, " payload_xor=");
        append_dec64(p, end, row.corruption.payload_xor);
        append_sconst(p, end, " byte_before=");
        append_dec64(p, end, row.corruption.byte_before);
        append_sconst(p, end, " byte_after=");
        append_dec64(p, end, row.corruption.byte_after);
        append_sconst(p, end, " payload_changed=");
        append_bool01(p, end, row.corruption.payload_changed);
        append_sconst(p, end, " sqe_offset=");
        append_dec64(p, end, row.block_corruption.sqe_offset);
        append_sconst(p, end, " sqe_xor=");
        append_dec64(p, end, row.block_corruption.sqe_xor);
        append_sconst(p, end, " sqe_byte_before=");
        append_dec64(p, end, row.block_corruption.byte_before);
        append_sconst(p, end, " sqe_byte_after=");
        append_dec64(p, end, row.block_corruption.byte_after);
        append_sconst(p, end, " sqe_changed=");
        append_bool01(p, end, row.block_corruption.changed);
        append_sconst(p, end, " local_boot=");
        append_dec64(p, end, row.local_boot_epoch);
        append_sconst(p, end, " peer_boot=");
        append_dec64(p, end, row.peer_boot_epoch);
        append_sconst(p, end, " peer_channel_epoch=");
        append_dec64(p, end, row.peer_channel_epoch);
        append_char(p, end, '\n');
    }
    delete[] rules;
    delete[] trace;
    append_sconst(p, end, "wki_chaos_end complete=1\n");
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

void append_virtio_net_diag(char*& p, const char* end, const ker::dev::virtio::VirtIONetDiagSnapshot& row) {
    append_sconst(p, end, "virtio_net name=");
    append_netdev_name(p, end, row.name);
    append_sconst(p, end, " ifindex=");
    append_dec64(p, end, row.ifindex);
    append_sconst(p, end, " pair=");
    append_dec64(p, end, row.pair);
    append_sconst(p, end, " active=");
    append_bool01(p, end, row.active);
    append_sconst(p, end, " pairs=");
    append_dec64(p, end, row.num_queue_pairs);
    append_sconst(p, end, " configured=");
    append_dec64(p, end, row.configured_queue_pairs);
    append_sconst(p, end, " msix=");
    append_bool01(p, end, row.msix_enabled);
    append_sconst(p, end, " vector=");
    append_dec64(p, end, row.irq_vector);
    append_sconst(p, end, " hdr_size=");
    append_dec64(p, end, row.hdr_size);
    append_sconst(p, end, " napi_state=");
    append_sconst(p, end, napi_state_name(row.napi_state));
    append_sconst(p, end, " napi_work=");
    append_bool01(p, end, row.napi_has_work);
    append_sconst(p, end, " worker_pid=");
    append_dec64(p, end, row.napi_worker_pid);
    append_sconst(p, end, " worker_cpu=");
    append_dec64(p, end, row.napi_worker_cpu);
    append_sconst(p, end, " polls=");
    append_dec64(p, end, row.napi_polls);
    append_sconst(p, end, " completes=");
    append_dec64(p, end, row.napi_completes);
    append_virtqueue_diag(p, end, "rx", row.rx);
    append_virtqueue_diag(p, end, "tx", row.tx);
    append_napi_diag(p, end, row.napi_diag);
    append_char(p, end, '\n');
}

auto generate_wki_napi(char* buf, size_t bufsz) -> size_t {
    if (buf == nullptr || bufsz == 0) return 0;
    char* p = buf;
    char const* end = buf + bufsz - 1;
    // Reserve the footer before copying any rows. Never publish a partial row
    // or claim a complete snapshot when the body or a staged row cannot fit.
    constexpr size_t FOOTER_RESERVE = 96;
    if (bufsz <= FOOTER_RESERVE) {
        append_sconst(p, end, "wki_napi_end complete=0\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }
    char const* body_end = end - FOOTER_RESERVE;
    constexpr char HEADER[] = "wki_napi version=1 scope=virtio_admitted\n";
    bool complete = sizeof(HEADER) - 1 <= static_cast<size_t>(body_end - p);
    if (complete) {
        std::memcpy(p, HEADER, sizeof(HEADER) - 1);
        p += sizeof(HEADER) - 1;
    }

    // The driver statically bounds its registry to at most 32 queue pairs.
    // Closed admission excludes retiring devices; the snapshot owns its data.
    std::array<ker::dev::virtio::VirtIONetDiagSnapshot, ker::dev::virtio::VIRTIO_NET_DIAG_MAX_ROWS> rows{};
    size_t const SNAPSHOT_ROWS = ker::dev::virtio::virtio_net_diag_snapshot(rows.data(), rows.size());
    size_t written_rows = 0;
    std::array<char, 4096> row_buf{};
    for (size_t i = 0; complete && i < SNAPSHOT_ROWS; ++i) {
        char* row_p = row_buf.data();
        char const* row_end = row_buf.data() + row_buf.size() - 1;
        append_virtio_net_diag(row_p, row_end, rows.at(i));
        size_t const ROW_BYTES = static_cast<size_t>(row_p - row_buf.data());
        if (row_p + 1 >= row_end || ROW_BYTES > static_cast<size_t>(body_end - p)) {
            complete = false;
            break;
        }
        std::memcpy(p, row_buf.data(), ROW_BYTES);
        p += ROW_BYTES;
        ++written_rows;
    }
    append_sconst(p, end, "wki_napi_end complete=");
    append_bool01(p, end, complete);
    append_sconst(p, end, " rows=");
    append_dec64(p, end, written_rows);
    append_sconst(p, end, " snapshot_rows=");
    append_dec64(p, end, SNAPSHOT_ROWS);
    append_char(p, end, '\n');
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Covers all retained snapshot rows at maximal numeric/string widths. Other
// snapshot caps/allocation failures remain explicit; this is not an atomic
// observation across subsystems. Only netdiag uses this per-open allocation.
constexpr size_t MAX_WKI_NETDIAG_BUF = 4 * 1024 * 1024;

// Ordinary procfs read only: authenticate timer RIPs against the relocated
// kernel without adding work to the IRQ recorder or routing receive interval.
void append_routing_timer_kernel_metadata(char*& p, const char* end) {
    wos::abi::KernelDebugMetadata metadata{};
    bool const VALID =
        ker::loader::runtime::kernel_debug_metadata(metadata) && (metadata.flags & wos::abi::KERNEL_DEBUG_METADATA_EXACT_MAPPING) != 0;
    append_sconst(p, end, " kernel_meta_valid=");
    append_bool01(p, end, VALID);
    append_sconst(p, end, " kernel_load_base=");
    append_dec64(p, end, VALID ? metadata.load_base : 0);
    append_sconst(p, end, " kernel_text_start=");
    append_dec64(p, end, VALID ? metadata.text_start : 0);
    append_sconst(p, end, " kernel_text_end=");
    append_dec64(p, end, VALID ? metadata.text_end : 0);
    append_sconst(p, end, " kernel_build_id=");
    if (!VALID || (metadata.flags & wos::abi::KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT) == 0 || metadata.build_id_size == 0 ||
        metadata.build_id_size > sizeof(metadata.build_id)) {
        append_char(p, end, '-');
        return;
    }
    constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (size_t i = 0; i < metadata.build_id_size; ++i) {
        uint8_t const BYTE = metadata.build_id[i];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        append_char(p, end, HEX_DIGITS.at(BYTE >> 4U));
        append_char(p, end, HEX_DIGITS.at(BYTE & 0xfU));
    }
}

// Aggregate network/WKI state. Readers must require the completion footer.
auto generate_wki_netdiag(char* buf, size_t bufsz) -> size_t {  // NOLINT(readability-function-size)
    if (bufsz == 0) {
        return 0;
    }

    char* p = buf;
    char const* end = buf + bufsz - 1;

    auto pool = ker::net::pkt_pool_snapshot();
    append_sconst(p, end, "packet_pool capacity=");
    append_dec64(p, end, pool.capacity);
    append_sconst(p, end, " baseline=");
    append_dec64(p, end, pool.baseline_capacity);
    append_sconst(p, end, " active_capacity=");
    append_dec64(p, end, pool.active_capacity);
    append_sconst(p, end, " free=");
    append_dec64(p, end, pool.free);
    append_sconst(p, end, " used=");
    append_dec64(p, end, pool.used);
    append_sconst(p, end, " draining=");
    append_dec64(p, end, pool.draining_buffers);
    append_sconst(p, end, " draining_free=");
    append_dec64(p, end, pool.draining_free);
    append_sconst(p, end, " rx_reserve=");
    append_dec64(p, end, pool.rx_reserve);
    append_sconst(p, end, " grow_chunk=");
    append_dec64(p, end, pool.grow_chunk);
    append_sconst(p, end, " buf_size=");
    append_dec64(p, end, pool.buffer_size);
    append_sconst(p, end, " headroom=");
    append_dec64(p, end, pool.headroom);
    append_sconst(p, end, " tx_refused=");
    append_dec64(p, end, pool.tx_refused);
    append_sconst(p, end, " expanding=");
    append_bool01(p, end, pool.expand_in_progress);
    append_char(p, end, '\n');

    ker::net::wki::WkiSecureRdmaDiagnostics secure{};
    bool const SECURE_COMPLETE = ker::net::wki::wki_secure_rdma_diagnostics(secure);
    append_sconst(p, end, "wki_secure_rdma complete=");
    append_bool01(p, end, SECURE_COMPLETE);
    if (SECURE_COMPLETE) {
        append_sconst(p, end, " queued_records=");
        append_dec64(p, end, secure.queued_records);
        append_sconst(p, end, " live_regions=");
        append_dec64(p, end, secure.live_regions);
        append_sconst(p, end, " region_references=");
        append_dec64(p, end, secure.region_references);
        append_sconst(p, end, " region_claims=");
        append_dec64(p, end, secure.region_claims);
        append_sconst(p, end, " live_operations=");
        append_dec64(p, end, secure.live_operations);
        append_sconst(p, end, " live_keys=");
        append_dec64(p, end, secure.live_keys);
        append_sconst(p, end, " replay_windows=");
        append_dec64(p, end, secure.replay_windows);
        append_sconst(p, end, " consumer_pins=");
        append_dec64(p, end, secure.consumer_pins);
        append_sconst(p, end, " pending_revocations=");
        append_dec64(p, end, secure.pending_revocations);
    }
    append_char(p, end, '\n');
    if (SECURE_COMPLETE) {
        for (size_t index = 0; index < secure.client_completions.size(); ++index) {
            append_sconst(p, end, "wki_secure_rdma_client transport=");
            append_dec64(p, end, index / 4 + 1);
            append_sconst(p, end, " service=");
            append_dec64(p, end, index % 4 + 1);
            append_sconst(p, end, " completions=");
            append_dec64(p, end, secure.client_completions[index]);
            append_sconst(p, end, " bytes=");
            append_dec64(p, end, secure.client_bytes[index]);
            append_sconst(p, end, " read_completions=");
            append_dec64(p, end, secure.client_read_completions[index]);
            append_sconst(p, end, " read_bytes=");
            append_dec64(p, end, secure.client_read_bytes[index]);
            append_sconst(p, end, " write_completions=");
            append_dec64(p, end, secure.client_write_completions[index]);
            append_sconst(p, end, " write_bytes=");
            append_dec64(p, end, secure.client_write_bytes[index]);
            append_char(p, end, '\n');
        }
        constexpr std::array REASONS{"none",       "length",         "version",   "field",     "identity", "session",
                                     "domain",     "authentication", "counter",   "duplicate", "too_old",  "grant",
                                     "permission", "range",          "operation", "fragment",  "retired",  "pressure"};
        static_assert(REASONS.size() == static_cast<size_t>(ker::net::wki::rdma_protocol::Drop::COUNT));
        for (size_t index = 0; index < REASONS.size(); ++index) {
            append_sconst(p, end, "wki_secure_rdma_drop reason=");
            append_sconst(p, end, REASONS[index]);
            append_sconst(p, end, " count=");
            append_dec64(p, end, secure.drops[index]);
            append_char(p, end, '\n');
        }
    }

    ker::net::BacklogSnapshot backlog{};
    ker::net::backlog_get_snapshot(backlog);
    append_sconst(p, end, "backlog ready=");
    append_bool01(p, end, backlog.ready);
    append_sconst(p, end, " cpus=");
    append_dec64(p, end, backlog.num_cpus);
    append_sconst(p, end, " queues=");
    append_dec64(p, end, backlog.queue_count);
    append_sconst(p, end, " queued=");
    append_dec64(p, end, backlog.total_queued);
    append_char(p, end, '\n');
    for (size_t i = 0; i < backlog.queue_count; ++i) {
        const auto& row = backlog.queues.at(i);
        append_sconst(p, end, "backlog_cpu cpu=");
        append_dec64(p, end, row.cpu);
        append_sconst(p, end, " queued=");
        append_dec64(p, end, row.queued);
        append_sconst(p, end, " handler_pid=");
        append_dec64(p, end, row.handler_pid);
        append_sconst(p, end, " handler_cpu=");
        append_dec64(p, end, row.handler_cpu);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.handler_active);
        append_sconst(p, end, " consumer=");
        append_bool01(p, end, row.consumer_active);
        append_sconst(p, end, " owner_pid=");
        append_dec64(p, end, row.consumer_owner_pid);
        append_sconst(p, end, " owner_cpu=");
        append_dec64(p, end, row.consumer_owner_cpu);
        append_sconst(p, end, " owner_site=");
        append_hex64(p, end, row.consumer_owner_site);
        append_sconst(p, end, " hold_us=");
        append_dec64(p, end, row.consumer_hold_us);
        append_sconst(p, end, " stage=");
        append_dec64(p, end, row.consumer_stage);
        append_sconst(p, end, " batch=");
        append_dec64(p, end, row.consumer_batch);
        append_sconst(p, end, " normal=");
        append_dec64(p, end, row.consumer_normal);
        append_sconst(p, end, " wki=");
        append_dec64(p, end, row.consumer_wki);
        append_char(p, end, '\n');
    }

    std::array<ker::net::NetDeviceSnapshot, ker::net::MAX_NET_DEVICES> devs{};
    size_t const DEV_COUNT = ker::net::netdev_snapshot(devs.data(), devs.size());
    for (size_t i = 0; i < DEV_COUNT; ++i) {
        const auto& dev = devs.at(i);
        append_sconst(p, end, "netdev name=");
        append_netdev_name(p, end, dev.name);
        append_sconst(p, end, " ifindex=");
        append_dec64(p, end, dev.ifindex);
        append_sconst(p, end, " state=");
        append_sconst(p, end, dev.state != 0 ? "up" : "down");
        append_sconst(p, end, " mtu=");
        append_dec64(p, end, dev.mtu);
        append_sconst(p, end, " txqlen=");
        append_dec64(p, end, dev.tx_queue_len);
        append_sconst(p, end, " wki_transport=");
        append_bool01(p, end, dev.wki_transport);
        append_sconst(p, end, " rx_forward=");
        append_bool01(p, end, dev.wki_rx_forward);
        append_sconst(p, end, " remotable=");
        append_bool01(p, end, dev.remotable);
        append_sconst(p, end, " rx_packets=");
        append_dec64(p, end, dev.rx_packets);
        append_sconst(p, end, " rx_bytes=");
        append_dec64(p, end, dev.rx_bytes);
        append_sconst(p, end, " rx_dropped=");
        append_dec64(p, end, dev.rx_dropped);
        append_sconst(p, end, " tx_packets=");
        append_dec64(p, end, dev.tx_packets);
        append_sconst(p, end, " tx_bytes=");
        append_dec64(p, end, dev.tx_bytes);
        append_sconst(p, end, " tx_dropped=");
        append_dec64(p, end, dev.tx_dropped);
        append_char(p, end, '\n');
    }

    std::array<ker::dev::virtio::VirtIONetDiagSnapshot, ker::dev::virtio::VIRTIO_NET_DIAG_MAX_ROWS> virtio_rows{};
    size_t const VIRTIO_COUNT = ker::dev::virtio::virtio_net_diag_snapshot(virtio_rows.data(), virtio_rows.size());
    for (size_t i = 0; i < VIRTIO_COUNT; ++i) {
        append_virtio_net_diag(p, end, virtio_rows.at(i));
    }
    if (VIRTIO_COUNT == virtio_rows.size()) {
        append_sconst(p, end, "virtio_net_truncated max=");
        append_dec64(p, end, virtio_rows.size());
        append_char(p, end, '\n');
    }

    std::array<ker::net::proto::TcpListenerSnapshot, ker::net::proto::TCP_LISTENER_SNAPSHOT_MAX> listeners{};
    size_t const LISTENER_COUNT = ker::net::proto::tcp_listener_snapshot(listeners.data(), listeners.size());
    for (size_t i = 0; i < LISTENER_COUNT; ++i) {
        const auto& listener = listeners.at(i);
        append_sconst(p, end, "tcp_listener local=");
        append_tcp_endpoint(p, end, listener.local);
        append_sconst(p, end, " state=");
        append_sconst(p, end, tcp_state_name(listener.state));
        append_sconst(p, end, " owner_pid=");
        append_dec64(p, end, listener.owner_pid);
        append_sconst(p, end, " accept_queue=");
        append_dec64(p, end, listener.accept_queue);
        append_sconst(p, end, " backlog=");
        append_dec64(p, end, static_cast<uint64_t>(listener.backlog));
        append_sconst(p, end, " rcvbuf_used=");
        append_dec64(p, end, listener.rcvbuf_used);
        append_sconst(p, end, " rcvbuf_capacity=");
        append_dec64(p, end, listener.rcvbuf_capacity);
        append_sconst(p, end, " rcv_wnd=");
        append_dec64(p, end, listener.rcv_wnd);
        append_sconst(p, end, " refcnt=");
        append_dec64(p, end, listener.refcount);
        append_char(p, end, '\n');
    }
    if (LISTENER_COUNT == listeners.size()) {
        append_sconst(p, end, "tcp_listener_truncated max=");
        append_dec64(p, end, listeners.size());
        append_char(p, end, '\n');
    }

    std::array<ker::net::proto::TcpConnSnapshot, ker::net::proto::TCP_CONN_SNAPSHOT_MAX> tcp_conns{};
    size_t const TCP_CONN_COUNT = ker::net::proto::tcp_conn_snapshot(tcp_conns.data(), tcp_conns.size());
    for (size_t i = 0; i < TCP_CONN_COUNT; ++i) {
        const auto& conn = tcp_conns.at(i);
        append_sconst(p, end, "tcp_conn local=");
        append_tcp_endpoint(p, end, conn.local);
        append_sconst(p, end, " remote=");
        append_tcp_endpoint(p, end, conn.remote);
        append_sconst(p, end, " state=");
        append_sconst(p, end, tcp_state_name(conn.state));
        append_sconst(p, end, " owner_pid=");
        append_dec64(p, end, conn.owner_pid);
        append_sconst(p, end, " rcvbuf_used=");
        append_dec64(p, end, conn.rcvbuf_used);
        append_sconst(p, end, " rcvbuf_capacity=");
        append_dec64(p, end, conn.rcvbuf_capacity);
        append_sconst(p, end, " rcv_nxt=");
        append_dec64(p, end, conn.rcv_nxt);
        append_sconst(p, end, " rcv_wnd=");
        append_dec64(p, end, conn.rcv_wnd);
        append_sconst(p, end, " snd_una=");
        append_dec64(p, end, conn.snd_una);
        append_sconst(p, end, " snd_nxt=");
        append_dec64(p, end, conn.snd_nxt);
        append_sconst(p, end, " snd_wnd=");
        append_dec64(p, end, conn.snd_wnd);
        append_sconst(p, end, " ooo_bytes=");
        append_dec64(p, end, conn.ooo_bytes);
        append_sconst(p, end, " sack=");
        append_bool01(p, end, conn.sack_permitted);
        append_sconst(p, end, " sender_enabled=");
        append_bool01(p, end, conn.sender_enabled);
        append_sconst(p, end, " cwnd=");
        append_dec64(p, end, conn.cwnd);
        append_sconst(p, end, " ssthresh=");
        append_dec64(p, end, conn.ssthresh);
        append_sconst(p, end, " flight=");
        append_dec64(p, end, conn.flight);
        append_sconst(p, end, " dupacks=");
        append_dec64(p, end, conn.duplicate_acks);
        append_sconst(p, end, " recovery=");
        append_bool01(p, end, conn.in_recovery);
        append_sconst(p, end, " recovery_reason=");
        append_dec64(p, end, conn.recovery_reason);
        append_sconst(p, end, " rto_ms=");
        append_dec64(p, end, conn.rto_ms);
        append_sconst(p, end, " retransmit_packets=");
        append_dec64(p, end, conn.retransmits);
        append_sconst(p, end, " recovery_requests=");
        append_dec64(p, end, conn.recovery_requests);
        append_sconst(p, end, " timeouts=");
        append_dec64(p, end, conn.timeouts);
        append_sconst(p, end, " sack_ranges=");
        append_dec64(p, end, conn.sack_ranges);
        append_sconst(p, end, " sack_blocks=");
        append_dec64(p, end, conn.sack_blocks);
        append_sconst(p, end, " sack_drops=");
        append_dec64(p, end, conn.sack_drops);
        append_sconst(p, end, " persist_probes=");
        append_dec64(p, end, conn.persist_probes);
        append_sconst(p, end, " persist_deadline=");
        append_dec64(p, end, conn.persist_deadline);
        append_sconst(p, end, " refcnt=");
        append_dec64(p, end, conn.refcount);
        append_char(p, end, '\n');
    }
    if (TCP_CONN_COUNT == tcp_conns.size()) {
        append_sconst(p, end, "tcp_conn_truncated max=");
        append_dec64(p, end, tcp_conns.size());
        append_char(p, end, '\n');
    }

    ker::net::wki::WkiIpcPerfSnapshot ipc{};
    ker::net::wki::wki_ipc_get_perf_snapshot(ipc);
    append_sconst(p, end, "wki_ipc exports=");
    append_dec64(p, end, ipc.exports);
    append_sconst(p, end, " proxies=");
    append_dec64(p, end, ipc.proxies);
    append_sconst(p, end, " active_pumps=");
    append_dec64(p, end, ipc.pump_tasks);
    append_sconst(p, end, " ring_used=");
    append_dec64(p, end, ipc.proxy_ring_used_bytes);
    append_sconst(p, end, " pending_deliveries=");
    append_dec64(p, end, ipc.pending_deliveries);
    append_sconst(p, end, " pending_bytes=");
    append_dec64(p, end, ipc.pending_bytes);
    append_sconst(p, end, " export_flush_queue=");
    append_dec64(p, end, ipc.export_flush_queue);
    append_sconst(p, end, " export_close_pending=");
    append_dec64(p, end, ipc.export_close_pending);
    append_sconst(p, end, " export_close_waiting_for_bytes=");
    append_dec64(p, end, ipc.export_close_waiting_for_bytes);
    append_sconst(p, end, " proxy_close_queue=");
    append_dec64(p, end, ipc.proxy_close_queue);
    append_sconst(p, end, " dev_op_queue=");
    append_dec64(p, end, ipc.dev_op_queue);
    append_char(p, end, '\n');

    ker::net::wki::WkiIpcDiagCounts ipc_diag_counts{};
    std::array<ker::net::wki::WkiIpcDiagRow, ker::net::wki::WKI_IPC_DIAG_MAX> ipc_diag_rows{};
    size_t const IPC_DIAG_ROW_COUNT = ker::net::wki::wki_ipc_diag_snapshot(ipc_diag_rows.data(), ipc_diag_rows.size(), &ipc_diag_counts);
    append_sconst(p, end, "wki_ipc_diag_counts exports=");
    append_dec64(p, end, ipc_diag_counts.exports);
    append_sconst(p, end, " active_exports=");
    append_dec64(p, end, ipc_diag_counts.active_exports);
    append_sconst(p, end, " proxies=");
    append_dec64(p, end, ipc_diag_counts.proxies);
    append_sconst(p, end, " active_proxies=");
    append_dec64(p, end, ipc_diag_counts.active_proxies);
    append_sconst(p, end, " export_backlogs=");
    append_dec64(p, end, ipc_diag_counts.export_backlogs);
    append_sconst(p, end, " proxy_close_queue=");
    append_dec64(p, end, ipc_diag_counts.proxy_close_queue);
    append_sconst(p, end, " pending_deliveries=");
    append_dec64(p, end, ipc_diag_counts.pending_deliveries);
    append_sconst(p, end, " dev_op_work=");
    append_dec64(p, end, ipc_diag_counts.dev_op_work);
    append_sconst(p, end, " peer_cleanup_slots=");
    append_dec64(p, end, ipc_diag_counts.peer_cleanup_slots);
    append_sconst(p, end, " truncated=");
    append_dec64(p, end, ipc_diag_counts.truncated);
    append_char(p, end, '\n');
    for (size_t i = 0; i < IPC_DIAG_ROW_COUNT; ++i) {
        const auto& row = ipc_diag_rows.at(i);
        append_sconst(p, end, "wki_ipc_diag kind=");
        append_sconst(p, end, ipc_diag_kind_name(row.kind));
        append_sconst(p, end, " resource=");
        append_dec64(p, end, row.resource_id);
        append_sconst(p, end, " type=");
        append_sconst(p, end, ipc_resource_type_name(row.res_type));
        append_sconst(p, end, " peer=");
        append_hex16(p, end, row.peer_node);
        append_sconst(p, end, " ch=");
        append_dec64(p, end, row.assigned_channel);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.active);
        append_sconst(p, end, " file=");
        append_bool01(p, end, row.has_file);
        append_sconst(p, end, " flags=");
        append_dec64(p, end, static_cast<uint32_t>(row.open_flags));
        append_sconst(p, end, " pump_running=");
        append_bool01(p, end, row.pump_running);
        append_sconst(p, end, " pump_queued=");
        append_bool01(p, end, row.pump_queued);
        append_sconst(p, end, " pump_task=");
        append_bool01(p, end, row.has_pump_task);
        append_sconst(p, end, " pipe_bytes_received=");
        append_dec64(p, end, row.pipe_bytes_received);
        append_sconst(p, end, " proxy_bytes_written=");
        append_dec64(p, end, row.proxy_bytes_written);
        append_sconst(p, end, " ring_used=");
        append_dec64(p, end, row.ring_used);
        append_sconst(p, end, " ring_capacity=");
        append_dec64(p, end, row.ring_capacity);
        append_sconst(p, end, " write_closed=");
        append_bool01(p, end, row.write_closed);
        append_sconst(p, end, " blocked_reader=");
        append_dec64(p, end, row.blocked_reader_pid);
        append_sconst(p, end, " poll_waiters=");
        append_dec64(p, end, row.poll_waiters);
        append_sconst(p, end, " backlog_bytes=");
        append_dec64(p, end, row.backlog_bytes);
        append_sconst(p, end, " backlog_chunks=");
        append_dec64(p, end, row.backlog_chunks);
        append_sconst(p, end, " close_pending=");
        append_bool01(p, end, row.close_pending);
        append_sconst(p, end, " close_has_expected_bytes=");
        append_bool01(p, end, row.close_has_expected_bytes);
        append_sconst(p, end, " close_expected_bytes=");
        append_dec64(p, end, row.close_expected_bytes);
        append_sconst(p, end, " op=");
        append_dec64(p, end, row.op_id);
        append_sconst(p, end, " msg_size=");
        append_dec64(p, end, row.msg_size);
        append_sconst(p, end, " attempts=");
        append_dec64(p, end, row.attempts);
        append_sconst(p, end, " seq=");
        append_dec64(p, end, row.sequence);
        append_sconst(p, end, " refs=");
        append_sdec64(p, end, row.refcount);
        append_sconst(p, end, " pending_wait=");
        append_bool01(p, end, row.has_pending_wait);
        append_sconst(p, end, " pending_op=");
        append_dec64(p, end, row.pending_wait_op);
        append_sconst(p, end, " pending_cookie=");
        append_dec64(p, end, row.pending_wait_cookie);
        append_sconst(p, end, " cleanup_epoch=");
        append_dec64(p, end, row.cleanup_epoch);
        append_sconst(p, end, " cleanup_active=");
        append_bool01(p, end, row.cleanup_active);
        append_sconst(p, end, " fenced=");
        append_bool01(p, end, row.fenced);
        append_sconst(p, end, " credits=");
        append_dec64(p, end, row.message_write_credits);
        append_sconst(p, end, " write_error=");
        append_sdec64(p, end, row.message_write_error);
        append_sconst(p, end, " write_waiters=");
        append_dec64(p, end, row.message_write_waiters);
        append_sconst(p, end, " rdma=");
        append_bool01(p, end, row.pipe_rdma_enabled);
        append_sconst(p, end, " rdma_writer=");
        append_bool01(p, end, row.pipe_rdma_writer_active);
        append_char(p, end, '\n');
    }

    ker::net::wki::WkiRemoteComputeDiagCounts compute_counts{};
    std::array<ker::net::wki::WkiRemoteComputeDiagRow, ker::net::wki::WKI_REMOTE_COMPUTE_DIAG_MAX> compute_rows{};
    size_t const COMPUTE_ROW_COUNT =
        ker::net::wki::wki_remote_compute_diag_snapshot(compute_rows.data(), compute_rows.size(), &compute_counts);
    append_sconst(p, end, "wki_compute submitted=");
    append_dec64(p, end, compute_counts.submitted_total);
    append_sconst(p, end, " submitted_active=");
    append_dec64(p, end, compute_counts.submitted_active);
    append_sconst(p, end, " running=");
    append_dec64(p, end, compute_counts.running_total);
    append_sconst(p, end, " running_active=");
    append_dec64(p, end, compute_counts.running_active);
    append_sconst(p, end, " pending_complete=");
    append_dec64(p, end, compute_counts.pending_completions);
    append_sconst(p, end, " truncated=");
    append_dec64(p, end, compute_counts.truncated);
    append_char(p, end, '\n');
    for (size_t i = 0; i < COMPUTE_ROW_COUNT; ++i) {
        const auto& row = compute_rows.at(i);
        append_sconst(p, end, "wki_compute_task kind=");
        append_sconst(p, end, remote_compute_diag_kind_name(row.kind));
        append_sconst(p, end, " task=");
        append_dec64(p, end, row.task_id);
        append_sconst(p, end, " peer=");
        append_hex16(p, end, row.peer_node);
        append_sconst(p, end, " local_pid=");
        append_dec64(p, end, row.local_pid);
        append_sconst(p, end, " local_task=");
        append_hex64(p, end, row.local_task_ptr);
        append_sconst(p, end, " ch_gen=");
        append_dec64(p, end, row.channel_generation);
        append_sconst(p, end, " session_epoch=");
        append_dec64(p, end, row.submit_session_epoch);
        append_sconst(p, end, " active=");
        append_bool01(p, end, row.active);
        append_sconst(p, end, " response_pending=");
        append_bool01(p, end, row.response_pending);
        append_sconst(p, end, " complete_pending=");
        append_bool01(p, end, row.complete_pending);
        append_sconst(p, end, " response_waiter=");
        append_bool01(p, end, row.response_waiter_owned);
        append_sconst(p, end, " response_consumer=");
        append_bool01(p, end, row.response_consumer_owned);
        append_sconst(p, end, " complete_waiter=");
        append_bool01(p, end, row.complete_waiter_owned);
        append_sconst(p, end, " complete_consumer=");
        append_bool01(p, end, row.complete_consumer_owned);
        append_sconst(p, end, " proxy_ready=");
        append_bool01(p, end, row.proxy_ready);
        append_sconst(p, end, " has_local_task=");
        append_bool01(p, end, row.has_local_task);
        append_sconst(p, end, " published=");
        append_bool01(p, end, row.published);
        append_sconst(p, end, " accept_pending=");
        append_bool01(p, end, row.accept_pending);
        append_sconst(p, end, " discard_completion=");
        append_bool01(p, end, row.discard_completion);
        append_sconst(p, end, " terminate=");
        append_bool01(p, end, row.termination_requested);
        append_sconst(p, end, " result_owned=");
        append_bool01(p, end, row.result_handle_owned);
        append_sconst(p, end, " result_owner=");
        append_bool01(p, end, row.has_result_owner);
        append_sconst(p, end, " reclaim=");
        append_bool01(p, end, row.reclaim_requested);
        append_sconst(p, end, " exit=");
        append_sdec64(p, end, row.exit_status);
        append_sconst(p, end, " accepted_age_us=");
        append_dec64(p, end, row.accepted_age_us);
        append_sconst(p, end, " complete_age_us=");
        append_dec64(p, end, row.complete_age_us);
        append_sconst(p, end, " ipc_fds=");
        append_dec64(p, end, row.ipc_fd_count);
        append_sconst(p, end, " ipc_handoff_pins=");
        append_dec64(p, end, row.ipc_handoff_pin_count);
        append_sconst(p, end, " output_len=");
        append_dec64(p, end, row.output_len);
        append_sconst(p, end, " pending_output_len=");
        append_dec64(p, end, row.pending_proxy_output_len);
        append_char(p, end, '\n');
    }

    ker::net::wki::WkiResourceDiagCounts resource_counts{};
    std::array<ker::net::wki::WkiResourceDiagRow, ker::net::wki::WKI_RESOURCE_DIAG_MAX> resource_rows{};
    size_t const RESOURCE_ROW_COUNT =
        ker::net::wki::wki_resource_diag_snapshot(resource_rows.data(), resource_rows.size(), &resource_counts);
    append_sconst(p, end, "wki_resource_counts discovered=");
    append_dec64(p, end, resource_counts.discovered);
    append_sconst(p, end, " pending_vfs_mounts=");
    append_dec64(p, end, resource_counts.pending_vfs_mounts);
    append_sconst(p, end, " pending_net_attaches=");
    append_dec64(p, end, resource_counts.pending_net_attaches);
    append_sconst(p, end, " pending_rx=");
    append_dec64(p, end, resource_counts.pending_rx);
    append_sconst(p, end, " truncated=");
    append_dec64(p, end, resource_counts.truncated);
    append_char(p, end, '\n');
    for (size_t i = 0; i < RESOURCE_ROW_COUNT; ++i) {
        const auto& row = resource_rows.at(i);
        append_sconst(p, end, "wki_resource kind=");
        append_sconst(p, end, resource_diag_kind_name(row.kind));
        append_sconst(p, end, " node=");
        append_hex16(p, end, row.node_id);
        append_sconst(p, end, " type=");
        append_sconst(p, end, ipc_resource_type_name(static_cast<uint16_t>(row.resource_type)));
        append_sconst(p, end, " id=");
        append_dec64(p, end, row.resource_id);
        append_sconst(p, end, " generation=");
        append_dec64(p, end, row.generation);
        append_sconst(p, end, " owner_boot=");
        append_dec64(p, end, row.owner_incarnation.owner_boot_epoch);
        append_sconst(p, end, " incarnation=");
        append_dec64(p, end, row.owner_incarnation.resource_incarnation);
        append_sconst(p, end, " flags=");
        append_dec64(p, end, row.flags);
        append_sconst(p, end, " valid=");
        append_bool01(p, end, row.valid);
        append_sconst(p, end, " force_remount=");
        append_bool01(p, end, row.force_remount);
        append_sconst(p, end, " retry=");
        append_dec64(p, end, row.retry_count);
        append_sconst(p, end, " next_attempt_us=");
        append_dec64(p, end, row.next_attempt_us);
        append_sconst(p, end, " msg_type=");
        append_dec64(p, end, static_cast<uint8_t>(row.msg_type));
        append_sconst(p, end, " ch=");
        append_dec64(p, end, row.channel_id);
        append_sconst(p, end, " ch_gen=");
        append_dec64(p, end, row.channel_generation);
        append_sconst(p, end, " seq=");
        append_dec64(p, end, row.sequence);
        append_sconst(p, end, " payload_len=");
        append_dec64(p, end, row.payload_len);
        append_char(p, end, '\n');
    }

    auto* net_proxy_rows = new (std::nothrow) ker::net::wki::WkiRemoteNetDiagRow[ker::net::wki::WKI_REMOTE_NET_DIAG_MAX];
    if (net_proxy_rows != nullptr) {
        size_t net_proxy_total = 0;
        size_t const NET_PROXY_COUNT =
            ker::net::wki::wki_remote_net_diag_snapshot(net_proxy_rows, ker::net::wki::WKI_REMOTE_NET_DIAG_MAX, &net_proxy_total);
        append_sconst(p, end, "wki_net_proxy_counts total=");
        append_dec64(p, end, net_proxy_total);
        append_sconst(p, end, " returned=");
        append_dec64(p, end, NET_PROXY_COUNT);
        append_sconst(p, end, " truncated=");
        append_bool01(p, end, net_proxy_total > NET_PROXY_COUNT);
        append_char(p, end, '\n');
        for (size_t i = 0; i < NET_PROXY_COUNT; ++i) {
            const auto& row = net_proxy_rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_net_proxy owner=");
            append_hex16(p, end, row.owner_node);
            append_sconst(p, end, " resource=");
            append_dec64(p, end, row.resource_id);
            append_sconst(p, end, " generation=");
            append_dec64(p, end, row.resource_generation);
            append_sconst(p, end, " ch=");
            append_dec64(p, end, row.assigned_channel);
            append_sconst(p, end, " ch_gen=");
            append_dec64(p, end, row.channel_generation);
            append_sconst(p, end, " refs=");
            append_dec64(p, end, row.refs);
            append_sconst(p, end, " active=");
            append_bool01(p, end, row.active);
            append_sconst(p, end, " attaching=");
            append_bool01(p, end, row.attaching);
            append_sconst(p, end, " registered=");
            append_bool01(p, end, row.netdev_registered);
            append_sconst(p, end, " published=");
            append_bool01(p, end, row.ever_published);
            append_sconst(p, end, " epoch_reset=");
            append_bool01(p, end, row.epoch_reset_pending);
            append_sconst(p, end, " cleanup_started=");
            append_bool01(p, end, row.cleanup_started);
            append_sconst(p, end, " cleanup_complete=");
            append_bool01(p, end, row.cleanup_complete);
            append_sconst(p, end, " retiring=");
            append_bool01(p, end, row.retiring);
            append_sconst(p, end, " detail_complete=");
            append_bool01(p, end, row.detail_complete);
            append_sconst(p, end, " op_pending=");
            append_bool01(p, end, row.op_pending);
            append_sconst(p, end, " op_id=");
            append_dec64(p, end, row.op_expected_id);
            append_sconst(p, end, " op_seq=");
            append_dec64(p, end, row.op_expected_seq);
            append_sconst(p, end, " op_waiter=");
            append_bool01(p, end, row.op_waiter_owned);
            append_sconst(p, end, " attach_pending=");
            append_bool01(p, end, row.attach_pending);
            append_sconst(p, end, " attach_waiter=");
            append_bool01(p, end, row.attach_waiter_owned);
            append_sconst(p, end, " attach_cookie=");
            append_dec64(p, end, row.attach_cookie);
            append_sconst(p, end, " expected_cookie=");
            append_dec64(p, end, row.attach_expected_cookie);
            append_sconst(p, end, " peer_boot=");
            append_dec64(p, end, row.binding_peer_boot_epoch);
            append_sconst(p, end, " detach_pending=");
            append_bool01(p, end, row.detach_pending);
            append_sconst(p, end, " detach_retry=");
            append_bool01(p, end, row.detach_retry_in_progress);
            append_sconst(p, end, " detach_cookie=");
            append_dec64(p, end, row.detach_attach_cookie);
            append_sconst(p, end, " detach_boot=");
            append_dec64(p, end, row.detach_peer_boot_epoch);
            append_sconst(p, end, " credits=");
            append_dec64(p, end, row.rx_credits_remaining);
            append_sconst(p, end, " rx_packets=");
            append_dec64(p, end, row.rx_packets);
            append_sconst(p, end, " tx_packets=");
            append_dec64(p, end, row.tx_packets);
            append_sconst(p, end, " drops=");
            append_dec64(p, end, row.rx_dropped + row.tx_dropped);
            append_char(p, end, '\n');
        }
        delete[] net_proxy_rows;
    } else {
        append_sconst(p, end, "wki_net_proxy_counts allocation_failed=1\n");
    }

    auto* block_proxy_rows = new (std::nothrow) ker::net::wki::WkiDevProxyDiagRow[ker::net::wki::WKI_DEV_PROXY_DIAG_MAX];
    if (block_proxy_rows != nullptr) {
        size_t block_proxy_total = 0;
        size_t const BLOCK_PROXY_COUNT =
            ker::net::wki::wki_dev_proxy_diag_snapshot(block_proxy_rows, ker::net::wki::WKI_DEV_PROXY_DIAG_MAX, &block_proxy_total);
        append_sconst(p, end, "wki_block_proxy_counts total=");
        append_dec64(p, end, block_proxy_total);
        append_sconst(p, end, " returned=");
        append_dec64(p, end, BLOCK_PROXY_COUNT);
        append_sconst(p, end, " truncated=");
        append_bool01(p, end, block_proxy_total > BLOCK_PROXY_COUNT);
        append_char(p, end, '\n');
        for (size_t i = 0; i < BLOCK_PROXY_COUNT; ++i) {
            const auto& row = block_proxy_rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_block_proxy owner=");
            append_hex16(p, end, row.owner_node);
            append_sconst(p, end, " resource=");
            append_dec64(p, end, row.resource_id);
            append_sconst(p, end, " generation=");
            append_dec64(p, end, row.resource_generation);
            append_sconst(p, end, " owner_boot=");
            append_dec64(p, end, row.binding_incarnation.owner_boot_epoch);
            append_sconst(p, end, " incarnation=");
            append_dec64(p, end, row.binding_incarnation.resource_incarnation);
            append_sconst(p, end, " ch=");
            append_dec64(p, end, row.assigned_channel);
            append_sconst(p, end, " ch_gen=");
            append_dec64(p, end, row.channel_generation);
            append_sconst(p, end, " lifecycle_detail=");
            append_bool01(p, end, row.lifecycle_detail_complete);
            append_sconst(p, end, " io_detail=");
            append_bool01(p, end, row.io_detail_complete);
            append_sconst(p, end, " active=");
            append_bool01(p, end, row.active);
            append_sconst(p, end, " fenced=");
            append_bool01(p, end, row.fenced);
            append_sconst(p, end, " published=");
            append_bool01(p, end, row.ever_published);
            append_sconst(p, end, " epoch_reset=");
            append_bool01(p, end, row.epoch_reset_pending);
            append_sconst(p, end, " cleanup=");
            append_bool01(p, end, row.cleanup_in_progress);
            append_sconst(p, end, " resume_pending=");
            append_bool01(p, end, row.resume_pending);
            append_sconst(p, end, " resume_active=");
            append_bool01(p, end, row.resume_in_progress);
            append_sconst(p, end, " resume_after_detach=");
            append_bool01(p, end, row.resume_after_detach);
            append_sconst(p, end, " detach_confirmed=");
            append_bool01(p, end, row.resume_detach_confirmed);
            append_sconst(p, end, " op_pending=");
            append_bool01(p, end, row.op_pending);
            append_sconst(p, end, " op_id=");
            append_dec64(p, end, row.op_expected_id);
            append_sconst(p, end, " op_seq=");
            append_dec64(p, end, row.op_expected_seq);
            append_sconst(p, end, " op_waiter=");
            append_bool01(p, end, row.op_waiter_owned);
            append_sconst(p, end, " attach_pending=");
            append_bool01(p, end, row.attach_pending);
            append_sconst(p, end, " attach_waiter=");
            append_bool01(p, end, row.attach_waiter_owned);
            append_sconst(p, end, " binding_cookie=");
            append_dec64(p, end, row.binding_attach_cookie);
            append_sconst(p, end, " detach_pending=");
            append_bool01(p, end, row.detach_pending);
            append_sconst(p, end, " detach_cookie=");
            append_dec64(p, end, row.detach_attach_cookie);
            append_sconst(p, end, " rdma=");
            append_bool01(p, end, row.rdma_attached);
            append_sconst(p, end, " roce=");
            append_bool01(p, end, row.rdma_roce);
            append_sconst(p, end, " zone=");
            append_dec64(p, end, row.rdma_zone_id);
            append_sconst(p, end, " data_slots=");
            append_hex64(p, end, row.data_slot_bitmap);
            append_sconst(p, end, " tags=");
            append_hex64(p, end, row.tag_bitmap);
            append_block_ring_diag(p, end, row.ring_geometry, row.ring_indices, row.ring_geometry_valid, row.ring_indices_valid);
            append_sconst(p, end, " bulk=");
            append_bool01(p, end, row.bulk_capable);
            append_sconst(p, end, " bulk_max=");
            append_dec64(p, end, row.bulk_max_transfer);
            append_sconst(p, end, " secure_compatible=");
            append_bool01(p, end, row.secure_block_compatible);
            append_sconst(p, end, " secure_uncertain=");
            append_bool01(p, end, row.secure_block_write_uncertain);
            append_sconst(p, end, " secure_attempts=");
            append_dec64(p, end, row.secure_block_attempts);
            append_sconst(p, end, " secure_completions=");
            append_dec64(p, end, row.secure_block_completions);
            append_sconst(p, end, " secure_bytes=");
            append_dec64(p, end, row.secure_block_bytes);
            append_sconst(p, end, " secure_fallbacks=");
            append_dec64(p, end, row.secure_block_fallbacks);
            append_sconst(p, end, " secure_transport=");
            append_dec64(p, end, static_cast<uint8_t>(row.secure_block_transport));
            append_char(p, end, '\n');
        }
        delete[] block_proxy_rows;
    } else {
        append_sconst(p, end, "wki_block_proxy_counts allocation_failed=1\n");
    }

    auto* server_rows = new (std::nothrow) ker::net::wki::WkiDevServerDiagRow[ker::net::wki::WKI_DEV_SERVER_DIAG_MAX];
    if (server_rows != nullptr) {
        size_t server_total = 0;
        size_t const SERVER_COUNT =
            ker::net::wki::wki_dev_server_diag_snapshot(server_rows, ker::net::wki::WKI_DEV_SERVER_DIAG_MAX, &server_total);
        append_sconst(p, end, "wki_server_binding_counts total=");
        append_dec64(p, end, server_total);
        append_sconst(p, end, " returned=");
        append_dec64(p, end, SERVER_COUNT);
        append_sconst(p, end, " truncated=");
        append_bool01(p, end, server_total > SERVER_COUNT);
        append_char(p, end, '\n');
        for (size_t i = 0; i < SERVER_COUNT; ++i) {
            const auto& row = server_rows[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_server_binding consumer=");
            append_hex16(p, end, row.consumer_node);
            append_sconst(p, end, " type=");
            append_sconst(p, end, ipc_resource_type_name(static_cast<uint16_t>(row.resource_type)));
            append_sconst(p, end, " resource=");
            append_dec64(p, end, row.resource_id);
            append_sconst(p, end, " owner_boot=");
            append_dec64(p, end, row.resource_incarnation.owner_boot_epoch);
            append_sconst(p, end, " incarnation=");
            append_dec64(p, end, row.resource_incarnation.resource_incarnation);
            append_sconst(p, end, " cookie=");
            append_dec64(p, end, row.attach_cookie);
            append_sconst(p, end, " ch=");
            append_dec64(p, end, row.assigned_channel);
            append_sconst(p, end, " ch_gen=");
            append_dec64(p, end, row.channel_generation);
            append_sconst(p, end, " refs=");
            append_dec64(p, end, row.refs);
            append_sconst(p, end, " active=");
            append_bool01(p, end, row.active);
            append_sconst(p, end, " retiring=");
            append_bool01(p, end, row.retiring);
            append_sconst(p, end, " epoch_reset=");
            append_bool01(p, end, row.epoch_reset_pending);
            append_sconst(p, end, " cleanup_pending=");
            append_bool01(p, end, row.detach_cleanup_pending);
            append_sconst(p, end, " cleanup_claimed=");
            append_bool01(p, end, row.detach_cleanup_claimed);
            append_sconst(p, end, " readonly=");
            append_bool01(p, end, row.block_read_only);
            append_sconst(p, end, " writer_lease=");
            append_bool01(p, end, row.block_writer_lease_owned);
            append_sconst(p, end, " secure_context=");
            append_bool01(p, end, row.block_secure_context);
            append_sconst(p, end, " native_busy=");
            append_bool01(p, end, row.block_native_busy);
            append_sconst(p, end, " rdma=");
            append_bool01(p, end, row.blk_rdma_active);
            append_sconst(p, end, " roce=");
            append_bool01(p, end, row.blk_roce);
            append_sconst(p, end, " zone_pending=");
            append_bool01(p, end, row.blk_zone_pending);
            append_sconst(p, end, " sq_notified=");
            append_bool01(p, end, row.blk_sq_notified);
            append_sconst(p, end, " poll_active=");
            append_bool01(p, end, row.blk_poll_active);
            append_sconst(p, end, " zone=");
            append_dec64(p, end, row.blk_zone_id);
            append_sconst(p, end, " ring_stable=");
            append_bool01(p, end, row.ring_snapshot_stable);
            append_block_ring_diag(p, end, row.ring_geometry, row.ring_indices, row.ring_geometry_valid, row.ring_indices_valid);
            append_sconst(p, end, " vfs_anchor=");
            append_bool01(p, end, row.vfs_lane_anchor);
            append_sconst(p, end, " export_revision=");
            append_dec64(p, end, row.vfs_export_publication_revision);
            append_sconst(p, end, " seen_revision=");
            append_dec64(p, end, row.vfs_export_revision_seen);
            append_sconst(p, end, " nic_open=");
            append_bool01(p, end, row.net_nic_opened);
            append_sconst(p, end, " net_credits=");
            append_dec64(p, end, row.net_rx_credits);
            append_char(p, end, '\n');
        }
        delete[] server_rows;
    } else {
        append_sconst(p, end, "wki_server_binding_counts allocation_failed=1\n");
    }

    // Independent scalar observations. Neither subsystem lock crosses the
    // other snapshot or formatting, and no File/Task identity is exposed.
    auto const ADVISORY = ker::vfs::vfs_advisory_diag_snapshot();
    auto const CLIENT_ADVISORY = ker::net::wki::wki_remote_vfs_client_advisory_diag_snapshot();
    append_sconst(p, end, "wki_vfs_advisory version=1");
    auto advisory_field = [&](const char* name, uint64_t value) {
        append_sconst(p, end, name);
        append_dec64(p, end, value);
    };
    advisory_field(" locks=", ADVISORY.locks);
    advisory_field(" associations=", ADVISORY.associations);
    advisory_field(" scopes=", ADVISORY.scopes);
    advisory_field(" closed_scopes=", ADVISORY.closed_scopes);
    advisory_field(" wait_receipts=", ADVISORY.wait_receipts);
    advisory_field(" pending=", ADVISORY.pending);
    advisory_field(" client_scopes=", CLIENT_ADVISORY.scopes);
    advisory_field(" client_scope_cleanup=", CLIENT_ADVISORY.scope_cleanup);
    advisory_field(" client_scope_users=", CLIENT_ADVISORY.scope_users);
    advisory_field(" client_scope_rpc_held=", CLIENT_ADVISORY.scope_rpc_held);
    advisory_field(" client_operations=", CLIENT_ADVISORY.operations);
    advisory_field(" client_started=", CLIENT_ADVISORY.started);
    advisory_field(" client_cancelling=", CLIENT_ADVISORY.cancelling);
    advisory_field(" client_acknowledging=", CLIENT_ADVISORY.acknowledging);
    advisory_field(" client_active_operations=", CLIENT_ADVISORY.active_operations);
    advisory_field(" client_task_owners_present=", CLIENT_ADVISORY.task_owners_present ? 1U : 0U);
    append_char(p, end, '\n');

    ker::net::wki::WkiRemoteVfsExecDiag exec_views{};
    ker::net::wki::wki_remote_vfs_exec_diag_snapshot(&exec_views);
    append_sconst(p, end, "wki_vfs_exec capacity=");
    append_dec64(p, end, exec_views.capacity);
    append_sconst(p, end, " owned=");
    append_dec64(p, end, exec_views.owned);
    append_sconst(p, end, " pending=");
    append_dec64(p, end, exec_views.pending);
    append_sconst(p, end, " retrying=");
    append_dec64(p, end, exec_views.retrying);
    append_sconst(p, end, " owner_views=");
    append_dec64(p, end, exec_views.owner_views);
    append_sconst(p, end, " attempts=");
    append_dec64(p, end, exec_views.attempts);
    append_sconst(p, end, " failed_attempts=");
    append_dec64(p, end, exec_views.failed_attempts);
    append_sconst(p, end, " cancelled=");
    append_dec64(p, end, exec_views.cancelled);
    append_sconst(p, end, " retirement_handoffs=");
    append_dec64(p, end, exec_views.retirement_handoffs);
    append_char(p, end, '\n');

    ker::net::wki::WkiRemoteVfsServerDiag vfs_server{};
    ker::net::wki::wki_remote_vfs_server_diag_snapshot(&vfs_server);
    append_sconst(p, end, "wki_vfs_server fd_entries=");
    append_dec64(p, end, vfs_server.fd_entries);
    append_sconst(p, end, " active=");
    append_dec64(p, end, vfs_server.active_fds);
    append_sconst(p, end, " retiring=");
    append_dec64(p, end, vfs_server.retiring_fds);
    append_sconst(p, end, " opens_total=");
    append_dec64(p, end, vfs_server.opens_total);
    append_sconst(p, end, " close_retirements_total=");
    append_dec64(p, end, vfs_server.close_retirements_total);
    append_sconst(p, end, " async_close_completions=");
    append_dec64(p, end, vfs_server.async_close_completions);
    append_sconst(p, end, " async_close_misses=");
    append_dec64(p, end, vfs_server.async_close_misses);
    append_char(p, end, '\n');

    std::array<ker::net::wki::WkiRemoteVfsProxyDiag, ker::net::wki::WKI_REMOTE_VFS_PROXY_DIAG_MAX> vfs_proxies{};
    size_t const VFS_PROXY_COUNT = ker::net::wki::wki_remote_vfs_proxy_diag_snapshot(vfs_proxies.data(), vfs_proxies.size());
    for (size_t i = 0; i < VFS_PROXY_COUNT; ++i) {
        const auto& proxy = vfs_proxies.at(i);
        append_sconst(p, end, "wki_vfs_proxy owner=");
        append_hex16(p, end, proxy.owner_node);
        append_sconst(p, end, " res_id=");
        append_dec64(p, end, proxy.resource_id);
        append_sconst(p, end, " ch=");
        append_dec64(p, end, proxy.assigned_channel);
        append_sconst(p, end, " ch_gen=");
        append_dec64(p, end, proxy.assigned_channel_generation);
        append_sconst(p, end, " res_gen=");
        append_dec64(p, end, proxy.resource_generation);
        append_sconst(p, end, " owner_boot=");
        append_dec64(p, end, proxy.owner_boot_epoch);
        append_sconst(p, end, " incarnation=");
        append_dec64(p, end, proxy.resource_incarnation);
        append_sconst(p, end, " binding_boot=");
        append_dec64(p, end, proxy.binding_peer_boot_epoch);
        append_sconst(p, end, " group=");
        append_dec64(p, end, proxy.mount_group_id);
        append_sconst(p, end, " lane=");
        append_dec64(p, end, proxy.lane_index);
        append_sconst(p, end, " lanes=");
        append_dec64(p, end, proxy.lane_count);
        append_sconst(p, end, " anchor=");
        append_bool01(p, end, proxy.lane_anchor);
        append_sconst(p, end, " lanes_ready=");
        append_bool01(p, end, proxy.lanes_ready);
        append_sconst(p, end, " active=");
        append_bool01(p, end, proxy.active);
        append_sconst(p, end, " epoch_reset=");
        append_bool01(p, end, proxy.epoch_reset_pending);
        append_sconst(p, end, " op_pending=");
        append_bool01(p, end, proxy.op_pending);
        append_sconst(p, end, " op_id=");
        append_dec64(p, end, proxy.op_expected_id);
        append_sconst(p, end, " op_seq=");
        append_dec64(p, end, proxy.op_expected_seq);
        append_sconst(p, end, " op_gen=");
        append_dec64(p, end, proxy.op_generation);
        append_sconst(p, end, " op_waiter=");
        append_dec64(p, end, proxy.op_waiter_pid);
        append_sconst(p, end, " retiring_waiter=");
        append_dec64(p, end, proxy.op_retiring_waiter_pid);
        append_sconst(p, end, " slot_waiters=");
        append_dec64(p, end, proxy.op_slot_waiter_count);
        append_sconst(p, end, " attach_pending=");
        append_bool01(p, end, proxy.attach_pending);
        append_sconst(p, end, " attach_cookie=");
        append_dec64(p, end, proxy.attach_expected_cookie);
        append_sconst(p, end, " binding_cookie=");
        append_dec64(p, end, proxy.binding_attach_cookie);
        append_sconst(p, end, " detach_pending=");
        append_bool01(p, end, proxy.detach_pending);
        append_sconst(p, end, " detach_retry=");
        append_bool01(p, end, proxy.detach_retry_in_progress);
        append_sconst(p, end, " detach_boot=");
        append_dec64(p, end, proxy.detach_peer_boot_epoch);
        append_sconst(p, end, " open_refs=");
        append_dec64(p, end, proxy.open_file_refs);
        append_sconst(p, end, " lifecycle_refs=");
        append_dec64(p, end, proxy.lifecycle_refs);
        append_sconst(p, end, " destroy_when_idle=");
        append_bool01(p, end, proxy.destroy_when_idle);
        append_sconst(p, end, " mounted=");
        append_bool01(p, end, proxy.mount_configured);
        append_sconst(p, end, " mount_released=");
        append_bool01(p, end, proxy.mount_released);
        append_sconst(p, end, " resources_releasing=");
        append_bool01(p, end, proxy.resources_releasing);
        append_sconst(p, end, " resources_released=");
        append_bool01(p, end, proxy.resources_released);
        append_sconst(p, end, " rdma=");
        append_bool01(p, end, proxy.rdma_capable);
        append_sconst(p, end, " bulk_rdma=");
        append_bool01(p, end, proxy.bulk_rdma_capable);
        append_sconst(p, end, " mount=");
        append_sconst(p, end, proxy.local_mount_path.data());
        append_char(p, end, '\n');
    }
    if (VFS_PROXY_COUNT == vfs_proxies.size()) {
        append_sconst(p, end, "wki_vfs_proxy_truncated max=");
        append_dec64(p, end, vfs_proxies.size());
        append_char(p, end, '\n');
    }

    // Routing counters are copied before formatting. No routing lock crosses
    // buffer output or allocation; existing diagnostic rows remain unchanged.
    auto const ROUTING = ker::net::wki::wki_routing_diag_snapshot();
    append_sconst(p, end, "wki_routing version=1 now_us=");
    append_dec64(p, end, ker::net::wki::wki_now_us());
    append_sconst(p, end, " accepted=");
    append_dec64(p, end, ROUTING.accepted);
    append_sconst(p, end, " malformed=");
    append_dec64(p, end, ROUTING.malformed);
    append_sconst(p, end, " stale=");
    append_dec64(p, end, ROUTING.stale);
    append_sconst(p, end, " full=");
    append_dec64(p, end, ROUTING.full);
    append_sconst(p, end, " coalesced=");
    append_dec64(p, end, ROUTING.coalesced);
    append_sconst(p, end, " computations=");
    append_dec64(p, end, ROUTING.computations);
    append_sconst(p, end, " obsolete=");
    append_dec64(p, end, ROUTING.obsolete);
    append_sconst(p, end, " publications=");
    append_dec64(p, end, ROUTING.publications);
    append_sconst(p, end, " flood_retries=");
    append_dec64(p, end, ROUTING.flood_retries);
    append_sconst(p, end, " max_rx_us=");
    append_dec64(p, end, ROUTING.max_rx_us);
    append_sconst(p, end, " max_spf_us=");
    append_dec64(p, end, ROUTING.max_spf_us);
    append_sconst(p, end, " leases_expired=");
    append_dec64(p, end, ROUTING.leases_expired);
    append_sconst(p, end, " lease_rejected=");
    append_dec64(p, end, ROUTING.lease_rejected);
    append_sconst(p, end, " lease_requests=");
    append_dec64(p, end, ROUTING.lease_requests);
    append_sconst(p, end, " lease_responses=");
    append_dec64(p, end, ROUTING.lease_responses);
    append_char(p, end, '\n');

    append_sconst(p, end, "wki_routing_rx version=1 enabled=");
    append_bool01(p, end, ROUTING.rx_timing_enabled);
    if (ROUTING.rx_timing_enabled) {
        auto const& rx = ROUTING.max_rx_sample;
        append_sconst(p, end, " total_us=");
        append_dec64(p, end, rx.total_us);
        append_sconst(p, end, " started_us=");
        append_dec64(p, end, rx.started_us);
        append_sconst(p, end, " cpu_start=");
        append_dec64(p, end, rx.cpu_start);
        append_sconst(p, end, " cpu_end=");
        append_dec64(p, end, rx.cpu_end);
        append_sconst(p, end, " source=");
        append_dec64(p, end, rx.source);
        append_sconst(p, end, " origin=");
        append_dec64(p, end, rx.origin);
        append_sconst(p, end, " sequence=");
        append_dec64(p, end, rx.sequence);
        append_sconst(p, end, " payload_len=");
        append_dec64(p, end, rx.payload_len);
        append_sconst(p, end, " message=");
        append_dec64(p, end, rx.message);
        append_sconst(p, end, " outcome=");
        append_dec64(p, end, rx.outcome);
        append_sconst(p, end, " stage_mask=");
        append_dec64(p, end, rx.stage_mask);
        append_sconst(p, end, " notified=");
        append_bool01(p, end, rx.notified);
        append_sconst(p, end, " validation_us=");
        append_dec64(p, end, rx.stages_us.at(0));
        append_sconst(p, end, " origin_us=");
        append_dec64(p, end, rx.stages_us.at(1));
        append_sconst(p, end, " prepare_us=");
        append_dec64(p, end, rx.stages_us.at(2));
        append_sconst(p, end, " lock_wait_us=");
        append_dec64(p, end, rx.stages_us.at(3));
        append_sconst(p, end, " apply_us=");
        append_dec64(p, end, rx.stages_us.at(4));
        append_sconst(p, end, " tail_us=");
        append_dec64(p, end, rx.stages_us.at(5));
    }
    append_char(p, end, '\n');

    // Same maximum RX sample as the unchanged version-1 row above. These are
    // elapsed subspans; lock/poke time overlaps reschedule time. Counter reads
    // observe the wake's start CPU, including unrelated execution on that CPU.
    append_sconst(p, end, "wki_routing_wake version=1 enabled=");
    append_bool01(p, end, ROUTING.rx_timing_enabled);
    if (ROUTING.rx_timing_enabled) {
        auto const& rx = ROUTING.max_rx_sample;
        auto const& wake = rx.wake;
        append_sconst(p, end, " rx_started_us=");
        append_dec64(p, end, rx.started_us);
        append_sconst(p, end, " recorded=");
        append_bool01(p, end, wake.recorded);
        append_sconst(p, end, " started_us=");
        append_dec64(p, end, wake.started_us);
        append_sconst(p, end, " total_us=");
        append_dec64(p, end, wake.total_us);
        append_sconst(p, end, " target_us=");
        append_dec64(p, end, wake.target_us);
        append_sconst(p, end, " prepare_us=");
        append_dec64(p, end, wake.prepare_us);
        append_sconst(p, end, " admission_us=");
        append_dec64(p, end, wake.admission_us);
        append_sconst(p, end, " reschedule_us=");
        append_dec64(p, end, wake.reschedule_us);
        append_sconst(p, end, " rq_wait_us=");
        append_dec64(p, end, wake.rq_wait_us);
        append_sconst(p, end, " rq_body_us=");
        append_dec64(p, end, wake.rq_body_us);
        append_sconst(p, end, " rq_return_us=");
        append_dec64(p, end, wake.rq_return_us);
        append_sconst(p, end, " poke_us=");
        append_dec64(p, end, wake.poke_us);
        append_sconst(p, end, " cpu_start=");
        append_dec64(p, end, wake.cpu_start);
        append_sconst(p, end, " cpu_end=");
        append_dec64(p, end, wake.cpu_end);
        append_sconst(p, end, " target_cpu=");
        append_dec64(p, end, wake.target_cpu);
        append_sconst(p, end, " leader=");
        append_bool01(p, end, wake.leader);
        append_sconst(p, end, " passes=");
        append_dec64(p, end, wake.passes);
        append_sconst(p, end, " locks=");
        append_dec64(p, end, wake.locks);
        append_sconst(p, end, " scans=");
        append_dec64(p, end, wake.scans);
        append_sconst(p, end, " pokes=");
        append_dec64(p, end, wake.pokes);
        append_sconst(p, end, " counters_valid=");
        append_bool01(p, end, wake.counters_valid);
        append_sconst(p, end, " irq_start_us=");
        append_dec64(p, end, wake.irq_start_us);
        append_sconst(p, end, " irq_end_us=");
        append_dec64(p, end, wake.irq_end_us);
        append_sconst(p, end, " timer_start=");
        append_dec64(p, end, wake.timer_start);
        append_sconst(p, end, " timer_end=");
        append_dec64(p, end, wake.timer_end);
    }
    append_char(p, end, '\n');

    if (ROUTING.rx_timing_enabled) {
        auto const& rx = ROUTING.max_rx_sample;
        auto const& wake = rx.wake;
        auto const& trace = ROUTING.max_rx_timer_trace;
        append_sconst(p, end, "wki_routing_timer_trace version=1 rx_started_us=");
        append_dec64(p, end, rx.started_us);
        append_sconst(p, end, " enabled=");
        append_bool01(p, end, trace.enabled);
        append_sconst(p, end, " cpu=");
        append_dec64(p, end, wake.cpu_start);
        append_sconst(p, end, " waker_valid=");
        append_bool01(p, end, wake.waker_valid);
        append_sconst(p, end, " waker_pid=");
        append_dec64(p, end, wake.waker_pid);
        append_sconst(p, end, " waker_type=");
        append_dec64(p, end, wake.waker_type);
        append_sconst(p, end, " waker_pinned=");
        append_bool01(p, end, wake.waker_pinned);
        append_sconst(p, end, " start=");
        append_dec64(p, end, trace.start_sequence);
        append_sconst(p, end, " end=");
        append_dec64(p, end, trace.end_sequence);
        append_sconst(p, end, " count=");
        append_dec64(p, end, trace.count);
        append_sconst(p, end, " omitted=");
        append_dec64(p, end, trace.omitted);
        append_sconst(p, end, " missing=");
        append_dec64(p, end, trace.missing);
        append_sconst(p, end, " complete=");
        append_bool01(p, end, trace.complete());
        append_routing_timer_kernel_metadata(p, end);
        append_char(p, end, '\n');
        for (size_t i = 0; i < trace.count; ++i) {
            auto const& event = trace.events.at(i);
            append_sconst(p, end, "wki_routing_timer_event version=1 rx_started_us=");
            append_dec64(p, end, rx.started_us);
            append_sconst(p, end, " cpu=");
            append_dec64(p, end, wake.cpu_start);
            append_sconst(p, end, " sequence=");
            append_dec64(p, end, event.sequence);
            append_sconst(p, end, " at_us=");
            append_dec64(p, end, event.at_us);
            append_sconst(p, end, " kind=");
            append_dec64(p, end, event.metadata & 0xffU);
            append_sconst(p, end, " pid=");
            append_dec64(p, end, event.pid);
            append_sconst(p, end, " value=");
            append_dec64(p, end, event.value);
            append_sconst(p, end, " task_type=");
            append_dec64(p, end, (event.metadata >> 8U) & 0xffU);
            append_sconst(p, end, " pinned=");
            append_bool01(p, end, ((event.metadata >> 16U) & 1U) != 0);
            append_sconst(p, end, " detail=");
            append_dec64(p, end, event.metadata >> 32U);
            append_char(p, end, '\n');
        }
    }

    auto* origins = new (std::nothrow) ker::net::wki::WkiRoutingOriginDiag[ker::net::wki::WKI_MAX_PEERS];
    if (origins != nullptr) {
        size_t const ORIGIN_COUNT = ker::net::wki::wki_routing_origin_diag_snapshot(origins, ker::net::wki::WKI_MAX_PEERS);
        for (size_t i = 0; i < ORIGIN_COUNT; ++i) {
            auto const& origin = origins[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_routing_origin version=1 origin=");
            append_hex16(p, end, origin.origin_node);
            append_sconst(p, end, " valid=");
            append_bool01(p, end, origin.valid);
            append_sconst(p, end, " fenced=");
            append_bool01(p, end, origin.fenced);
            append_sconst(p, end, " sequence_known=");
            append_bool01(p, end, origin.sequence_known);
            append_sconst(p, end, " sequence=");
            append_dec64(p, end, origin.lsa_seq);
            append_sconst(p, end, " received_us=");
            append_dec64(p, end, origin.received_time_us);
            append_sconst(p, end, " revision=");
            append_dec64(p, end, origin.revision);
            append_sconst(p, end, " completed_generation=");
            append_dec64(p, end, origin.authenticated_generation);
            append_sconst(p, end, " completed_boot_epoch=");
            append_dec64(p, end, origin.authenticated_boot_epoch);
            append_sconst(p, end, " completed_capabilities=");
            append_dec64(p, end, origin.authenticated_capabilities);
            append_sconst(p, end, " completed_identity_valid=");
            append_bool01(p, end, origin.authenticated_identity_valid);
            append_sconst(p, end, " hint_valid=");
            append_bool01(p, end, origin.hint_valid);
            append_sconst(p, end, " hint_sequence=");
            append_dec64(p, end, origin.hint_sequence);
            append_sconst(p, end, " hint_received_us=");
            append_dec64(p, end, origin.hint_received_time_us);
            append_sconst(p, end, " discovery_expired=");
            append_bool01(p, end, origin.discovery_expired);
            append_sconst(p, end, " replay_generation=");
            append_dec64(p, end, origin.origin_replay_generation);
            append_sconst(p, end, " replay_pending=");
            append_bool01(p, end, origin.origin_replay_pending);
            append_sconst(p, end, " replay_retry=");
            append_bool01(p, end, origin.origin_replay_retry);
            append_sconst(p, end, " lease_generation=");
            append_dec64(p, end, origin.lease_session_generation);
            append_sconst(p, end, " lease_bound=");
            append_bool01(p, end, origin.lease_bound);
            append_sconst(p, end, " lease_extended_required=");
            append_bool01(p, end, origin.lease_extended_required);
            append_sconst(p, end, " lease_withdrawn=");
            append_bool01(p, end, origin.lease_withdrawn);
            append_sconst(p, end, " lease_expires_us=");
            append_dec64(p, end, origin.lease_expires_us);
            append_sconst(p, end, " neighbor_count=");
            append_dec64(p, end, origin.num_neighbors);
            append_sconst(p, end, " neighbors=");
            for (size_t n = 0; n < std::min<size_t>(origin.num_neighbors, origin.neighbors.size()); ++n) {
                if (n != 0) {
                    append_char(p, end, ',');
                }
                auto const& neighbor = origin.neighbors.at(n);
                append_hex16(p, end, neighbor.node_id);
                append_char(p, end, ':');
                append_dec64(p, end, neighbor.link_cost);
                append_char(p, end, ':');
                append_dec64(p, end, neighbor.transport_mtu);
            }
            append_char(p, end, '\n');
        }
        delete[] origins;
    } else {
        append_sconst(p, end, "wki_routing_origin allocation_failed=1\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }

    auto* routes = new (std::nothrow) ker::net::wki::RoutingEntry[ker::net::wki::WKI_MAX_PEERS];
    if (routes != nullptr) {
        size_t const ROUTE_COUNT = ker::net::wki::wki_routing_snapshot(routes, ker::net::wki::WKI_MAX_PEERS);
        for (size_t i = 0; i < ROUTE_COUNT; ++i) {
            const auto& route = routes[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_route dst=");
            append_hex16(p, end, route.dst_node);
            append_sconst(p, end, " next_hop=");
            append_hex16(p, end, route.next_hop);
            append_sconst(p, end, " cost=");
            append_dec64(p, end, route.cost);
            append_sconst(p, end, " hops=");
            append_dec64(p, end, route.hop_count);
            append_sconst(p, end, " generation=");
            append_dec64(p, end, route.publication_generation);
            append_sconst(p, end, " hop_boot_epoch=");
            append_dec64(p, end, route.next_hop_boot_epoch);
            append_sconst(p, end, " hop_auth_generation=");
            append_dec64(p, end, route.next_hop_auth_generation);
            append_sconst(p, end, " lease_expires_us=");
            append_dec64(p, end, route.lease_expires_us);
            append_char(p, end, '\n');
        }
        delete[] routes;
    } else {
        append_sconst(p, end, "wki_route allocation_failed=1\n");
    }

    auto* peers = new (std::nothrow) ker::net::wki::WkiPeerDiag[ker::net::wki::WKI_PEER_DIAG_MAX];
    if (peers != nullptr) {
        size_t const PEER_COUNT = ker::net::wki::wki_peer_diag_snapshot(peers, ker::net::wki::WKI_PEER_DIAG_MAX);
        for (size_t i = 0; i < PEER_COUNT; ++i) {
            const auto& peer = peers[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_peer_lifecycle peer=");
            append_hex16(p, end, peer.node_id);
            append_sconst(p, end, " state=");
            append_sconst(p, end, peer_state_name(peer.state));
            append_sconst(p, end, " direct=");
            append_bool01(p, end, peer.direct);
            append_sconst(p, end, " next_hop=");
            append_hex16(p, end, peer.next_hop);
            append_sconst(p, end, " hops=");
            append_dec64(p, end, peer.hop_count);
            append_sconst(p, end, " local_channel_epoch=");
            append_dec64(p, end, peer.local_channel_epoch);
            append_sconst(p, end, " remote_channel_epoch=");
            append_dec64(p, end, peer.remote_channel_epoch);
            append_sconst(p, end, " remote_boot_epoch=");
            append_dec64(p, end, peer.remote_boot_epoch);
            append_sconst(p, end, " replacement=");
            append_hex16(p, end, peer.replacement_node_id);
            append_sconst(p, end, " lifecycle=");
            append_dec64(p, end, peer.lifecycle_state);
            append_sconst(p, end, " compute_cleanup=");
            append_bool01(p, end, peer.compute_reset_cleanup_pending);
            append_sconst(p, end, " vfs_rebind=");
            append_bool01(p, end, peer.vfs_reset_rebind_pending);
            append_sconst(p, end, " block_resume=");
            append_bool01(p, end, peer.block_resume_pending);
            append_sconst(p, end, " invalidate_discovery=");
            append_bool01(p, end, peer.vfs_reset_invalidate_discovery);
            append_sconst(p, end, " owner_reboot=");
            append_bool01(p, end, peer.vfs_reset_owner_reboot_proven);
            append_sconst(p, end, " advert_request=");
            append_dec64(p, end, peer.resource_advert_request);
            append_sconst(p, end, " advert_active=");
            append_dec64(p, end, peer.resource_advert_active_request);
            append_sconst(p, end, " advert_index=");
            append_dec64(p, end, peer.resource_advert_index);
            append_sconst(p, end, " advert_stage=");
            append_dec64(p, end, peer.resource_advert_stage);
            append_sconst(p, end, " heartbeat_us=");
            append_dec64(p, end, peer.last_heartbeat);
            append_sconst(p, end, " rx_us=");
            append_dec64(p, end, peer.last_rx_activity);
            append_sconst(p, end, " tx_us=");
            append_dec64(p, end, peer.last_tx_activity);
            append_sconst(p, end, " fence_defer_us=");
            append_dec64(p, end, peer.fence_defer_until_us);
            append_sconst(p, end, " connected_us=");
            append_dec64(p, end, peer.connected_time);
            append_sconst(p, end, " hello_us=");
            append_dec64(p, end, peer.hello_sent_time);
            append_char(p, end, '\n');
        }
        delete[] peers;
    } else {
        append_sconst(p, end, "wki_peer_lifecycle allocation_failed=1\n");
    }

    auto* waits = new (std::nothrow) ker::net::wki::WkiWaitDiag[ker::net::wki::WKI_WAIT_DIAG_MAX];
    if (waits != nullptr) {
        size_t const WAIT_COUNT = ker::net::wki::wki_wait_diag_snapshot(waits, ker::net::wki::WKI_WAIT_DIAG_MAX);
        for (size_t i = 0; i < WAIT_COUNT; ++i) {
            const auto& wait = waits[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            append_sconst(p, end, "wki_wait entry=");
            append_hex64(p, end, wait.entry_address);
            append_sconst(p, end, " state=");
            append_dec64(p, end, wait.state);
            append_sconst(p, end, " retirement_pending=");
            append_bool01(p, end, wait.retirement_pending);
            append_sconst(p, end, " task=");
            append_dec64(p, end, wait.task_pid);
            append_sconst(p, end, " deadline_us=");
            append_dec64(p, end, wait.deadline_us);
            append_sconst(p, end, " result=");
            append_sdec64(p, end, wait.result);
            append_sconst(p, end, " name=");
            append_sconst(p, end, wait.diag_name.data());
            append_sconst(p, end, " callsite=");
            append_hex64(p, end, wait.diag_callsite);
            append_sconst(p, end, " arg0=");
            append_dec64(p, end, wait.diag_arg0);
            append_sconst(p, end, " arg1=");
            append_dec64(p, end, wait.diag_arg1);
            append_sconst(p, end, " resource=");
            append_dec64(p, end, wait.diag_resource_id);
            append_sconst(p, end, " op=");
            append_dec64(p, end, wait.diag_op_id);
            append_sconst(p, end, " peer=");
            append_hex16(p, end, wait.diag_peer);
            append_sconst(p, end, " ch=");
            append_dec64(p, end, wait.diag_channel);
            append_sconst(p, end, " cookie=");
            append_dec64(p, end, wait.diag_cookie);
            append_char(p, end, '\n');
        }
        if (WAIT_COUNT == ker::net::wki::WKI_WAIT_DIAG_MAX) {
            append_sconst(p, end, "wki_wait_truncated max=");
            append_dec64(p, end, ker::net::wki::WKI_WAIT_DIAG_MAX);
            append_char(p, end, '\n');
        }
        delete[] waits;
    } else {
        append_sconst(p, end, "wki_wait allocation_failed=1\n");
    }

    auto* channels = new (std::nothrow) ker::net::wki::WkiChannelDiag[ker::net::wki::WKI_CHANNEL_POOL_SIZE];
    if (channels == nullptr) {
        append_sconst(p, end, "wki_channel allocation_failed=1\n");
        *p = '\0';
        return static_cast<size_t>(p - buf);
    }
    size_t const CHANNEL_COUNT = ker::net::wki::wki_channel_diag_snapshot(channels, ker::net::wki::WKI_CHANNEL_POOL_SIZE);
    for (size_t i = 0; i < CHANNEL_COUNT; ++i) {
        const auto& ch = channels[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        append_sconst(p, end, "wki_channel peer=");
        append_hex16(p, end, ch.peer_node);
        append_sconst(p, end, " state=");
        append_sconst(p, end, peer_state_name(ch.peer_state));
        append_sconst(p, end, " direct=");
        append_bool01(p, end, ch.peer_direct);
        append_sconst(p, end, " next_hop=");
        append_hex16(p, end, ch.next_hop);
        append_sconst(p, end, " hops=");
        append_dec64(p, end, ch.hop_count);
        append_sconst(p, end, " ch=");
        append_dec64(p, end, ch.channel_id);
        append_sconst(p, end, " priority=");
        append_sconst(p, end, priority_name(ch.priority));
        append_sconst(p, end, " active=");
        append_bool01(p, end, ch.active);
        append_sconst(p, end, " generation=");
        append_dec64(p, end, ch.generation);
        append_sconst(p, end, " tx_seq=");
        append_dec64(p, end, ch.tx_seq);
        append_sconst(p, end, " tx_ack=");
        append_dec64(p, end, ch.tx_ack);
        append_sconst(p, end, " rx_seq=");
        append_dec64(p, end, ch.rx_seq);
        append_sconst(p, end, " rx_ack=");
        append_dec64(p, end, ch.rx_ack_pending);
        append_sconst(p, end, " ack_pending=");
        append_bool01(p, end, ch.ack_pending);
        append_sconst(p, end, " tx_credits=");
        append_dec64(p, end, ch.tx_credits);
        append_sconst(p, end, " rx_credits=");
        append_dec64(p, end, ch.rx_credits);
        append_sconst(p, end, " retransmit_count=");
        append_dec64(p, end, ch.retransmit_count);
        append_sconst(p, end, " retransmit_active=");
        append_bool01(p, end, ch.retransmit_in_progress);
        append_sconst(p, end, " retransmit_head_seq=");
        append_dec64(p, end, ch.retransmit_head_seq);
        append_sconst(p, end, " retransmit_head_type=");
        append_dec64(p, end, ch.retransmit_head_type);
        append_sconst(p, end, " retransmit_head_retries=");
        append_dec64(p, end, ch.retransmit_head_retries);
        append_sconst(p, end, " retransmit_rto_us=");
        append_dec64(p, end, ch.retransmit_rto_us);
        append_sconst(p, end, " retransmit_deadline_us=");
        append_dec64(p, end, ch.retransmit_deadline_us);
        append_sconst(p, end, " retransmit_head_send_us=");
        append_dec64(p, end, ch.retransmit_head_send_time_us);
        append_sconst(p, end, " fast_retransmit_seq=");
        append_dec64(p, end, ch.fast_retransmit_seq);
        append_sconst(p, end, " dup_acks=");
        append_dec64(p, end, ch.duplicate_ack_count);
        append_sconst(p, end, " reorder_count=");
        append_dec64(p, end, ch.reorder_count);
        append_sconst(p, end, " reorder_head_seq=");
        append_dec64(p, end, ch.reorder_head_seq);
        append_sconst(p, end, " reorder_head_type=");
        append_dec64(p, end, ch.reorder_head_type);
        append_sconst(p, end, " dispatch_waiters=");
        append_dec64(p, end, ch.rx_dispatch_waiter_count);
        append_sconst(p, end, " retransmits=");
        append_dec64(p, end, ch.retransmits);
        append_sconst(p, end, " bytes_tx=");
        append_dec64(p, end, ch.bytes_sent);
        append_sconst(p, end, " bytes_rx=");
        append_dec64(p, end, ch.bytes_received);
        append_char(p, end, '\n');
    }
    // This buffer covers the entire fixed channel pool, including every
    // allocated slot; equality with its capacity does not omit a row.
    delete[] channels;

    // The host runner requires this terminal row. If the bounded append
    // buffer fills first, the otherwise well-formed prefix is explicitly
    // incomplete rather than being mistaken for a full state snapshot.
    append_sconst(p, end, "wki_netdiag_end complete=1\n");
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

class PerfEventTextWriter {
   public:
    PerfEventTextWriter(char*& cursor, char* end) : cursor_(cursor), end_(end) {}

    void append(char value) {
        if (!complete_) {
            return;
        }
        if (cursor_ >= end_) {
            complete_ = false;
            return;
        }
        *cursor_++ = value;
    }

    void append(const char* value) {
        while (complete_ && *value != '\0') {
            append(*value++);
        }
    }

    void append_dec(uint64_t value) {
        std::array<char, 20> digits{};
        size_t count = 0;
        do {
            digits.at(count++) = static_cast<char>('0' + (value % 10U));
            value /= 10U;
        } while (value != 0);
        while (count > 0) {
            append(digits.at(--count));
        }
    }

    void append_signed(int64_t value) {
        if (value < 0) {
            append('-');
            append_dec(0U - static_cast<uint64_t>(value));
            return;
        }
        append_dec(static_cast<uint64_t>(value));
    }

    void append_hex(uint64_t value) {
        constexpr std::array<char, 16> HEX_DIGITS{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        append('0');
        append('x');
        for (int shift = 60; shift >= 0; shift -= 4) {
            append(HEX_DIGITS.at(static_cast<size_t>((value >> shift) & 0xfU)));
        }
    }

    [[nodiscard]] auto complete() const -> bool { return complete_; }

   private:
    char*& cursor_;
    char* end_;
    bool complete_ = true;
};

void append_perf_callsite(PerfEventTextWriter& writer, uint64_t callsite) {
    if (callsite == 0) {
        writer.append('?');
        return;
    }

    if (callsite >= 0xffff800000000000ULL && (callsite & (alignof(ker::mod::perf::PerfCallsiteInfo) - 1)) == 0) {
        const auto* info = reinterpret_cast<const ker::mod::perf::PerfCallsiteInfo*>(callsite);
        if (info->magic == ker::mod::perf::PERF_CALLSITE_MAGIC && info->file != nullptr && info->line != 0) {
            const char* base = info->file;
            for (const char* cur = info->file; *cur != '\0'; ++cur) {
                if (*cur == '/') {
                    base = cur + 1;
                }
            }
            writer.append(base);
            writer.append(':');
            writer.append_dec(info->line);
            return;
        }
    }

    writer.append_hex(callsite);
}

// Generate content for /proc/kperf
// Drains the kernel perf ring buffer and formats each event as one text line.
// Format:
//   S <ts_ns> <cpu> <pid> <rip_hex>   <lag_v>  <flags>    SAMPLE
//   X <ts_ns> <cpu> <prev_pid> <next_pid> <lag_v> <flags> <run_us> <callsite>   SWITCH
//   W <ts_ns> <cpu> <pid> <wake_at_us> <sleep_us> <flags> <callsite> <wait_channel> WAKE
//   B <ts_ns> <cpu> <pid> <wake_at_us> <run_us>   <flags> <callsite> <wait_channel> SLEEP
//   K <ts_ns> <cpu> <pid> <scope> <op> <phase> <peer> <channel> <corr> <status> <aux> <callsite>
struct PerfTextOutput {
    char* cursor;
    char* end;
};

auto append_perf_event(const ker::mod::perf::PerfEvent& ev, void* context) -> bool {
    auto& output = *static_cast<PerfTextOutput*>(context);
    char* const EVENT_START = output.cursor;
    PerfEventTextWriter writer(output.cursor, output.end);
    // Determine event letter
    char letter = '?';
    switch (static_cast<ker::mod::perf::PerfEventType>(ev.type)) {
        case ker::mod::perf::PerfEventType::SAMPLE:
            letter = 'S';
            break;
        case ker::mod::perf::PerfEventType::SWITCH:
            letter = 'X';
            break;
        case ker::mod::perf::PerfEventType::WAKE:
            letter = 'W';
            break;
        case ker::mod::perf::PerfEventType::SLEEP:
            letter = 'B';
            break;
        case ker::mod::perf::PerfEventType::CONTAINER_STAT:
            letter = 'C';
            break;
        case ker::mod::perf::PerfEventType::WKI:
            letter = 'K';
            break;
    }
    writer.append(letter);
    writer.append(' ');
    writer.append_dec(ev.ts_ns);
    writer.append(' ');
    writer.append_dec(ev.cpu);
    writer.append(' ');
    // Type-specific fields
    if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::SAMPLE) {
        writer.append_dec(ev.pid);
        writer.append(' ');
        writer.append_hex(ev.data);  // RIP
        writer.append(' ');
        writer.append_signed(ev.lag_v);
        writer.append(' ');
        writer.append_dec(ev.flags);
    } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::SWITCH) {
        writer.append_dec(ev.pid);  // prev
        writer.append(' ');
        writer.append_dec(ev.data);  // next
        writer.append(' ');
        writer.append_dec(static_cast<uint64_t>(ev.lag_v >= 0 ? ev.lag_v : 0U));
        writer.append(' ');
        writer.append_dec(ev.flags);
        writer.append(' ');
        writer.append_dec(ev.aux);
        writer.append(' ');
        append_perf_callsite(writer, ev.callsite);
    } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::CONTAINER_STAT) {
        // CONTAINER_STAT: C <ts> <cpu> <pid> <subsys_name> <flags> <count> <capacity> <callsite>
        auto const SUBSYS_ID = static_cast<uint8_t>(ev.data >> 32);
        writer.append_dec(ev.pid);
        writer.append(' ');
        writer.append(ker::mod::perf::subsystem_name(static_cast<ker::mod::perf::PerfSubsystem>(SUBSYS_ID)));
        writer.append(' ');
        writer.append_dec(ev.flags);
        writer.append(' ');
        writer.append_signed(ev.lag_v);
        writer.append(' ');
        writer.append_dec(ev.aux);
        writer.append(' ');
        append_perf_callsite(writer, ev.callsite);
    } else if (static_cast<ker::mod::perf::PerfEventType>(ev.type) == ker::mod::perf::PerfEventType::WKI) {
        ker::mod::perf::WkiPerfScope scope = ker::mod::perf::WkiPerfScope::NONE;
        ker::mod::perf::WkiPerfPhase phase = ker::mod::perf::WkiPerfPhase::POINT;
        uint8_t op = 0;
        uint16_t peer = 0;
        uint16_t channel = 0;
        ker::mod::perf::wki_unpack_event_data(ev.data, scope, op, phase, peer, channel);

        writer.append_dec(ev.pid);
        writer.append(' ');
        writer.append(ker::mod::perf::wki_scope_name(scope));
        writer.append(' ');
        writer.append(ker::mod::perf::wki_op_name(scope, op));
        writer.append(' ');
        writer.append(ker::mod::perf::wki_phase_name(phase));
        writer.append(' ');
        writer.append_dec(peer);
        writer.append(' ');
        writer.append_dec(channel);
        writer.append(' ');
        writer.append_dec(ker::mod::perf::wki_unpack_trace_correlation(ev.lag_v));
        writer.append(' ');
        writer.append_signed(ker::mod::perf::wki_unpack_trace_status(ev.lag_v));
        writer.append(' ');
        writer.append_dec(ev.aux);
        writer.append(' ');
        if (scope == ker::mod::perf::WkiPerfScope::LOCAL_VMEM ||
            (scope == ker::mod::perf::WkiPerfScope::LOCAL_LOADER &&
             (op == static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_MAIN) ||
              op == static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_INTERP)))) {
            writer.append_hex(ev.callsite);
        } else {
            append_perf_callsite(writer, ev.callsite);
        }
    } else {
        // WAKE or SLEEP
        writer.append_dec(ev.pid);
        writer.append(' ');
        writer.append_dec(ev.data);  // wakeAtUs
        writer.append(' ');
        writer.append_dec(ev.aux);
        writer.append(' ');
        writer.append_dec(ev.flags);
        writer.append(' ');
        append_perf_callsite(writer, ev.callsite);
        writer.append(' ');
        auto const* wait_channel = reinterpret_cast<const char*>(static_cast<uint64_t>(ev.lag_v));
        writer.append(wait_channel != nullptr ? wait_channel : "-");
    }
    writer.append('\n');
    if (!writer.complete()) {
        output.cursor = EVENT_START;
        return false;
    }
    return true;
}

auto generate_kperf(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }
    PerfTextOutput output{buf, buf + bufsz - 1};
    // Each cursor advances only after a complete line fits. The sink holds one
    // ring lock for one event, formats into this bounded buffer, and takes no
    // other locks. Rejected events remain queued for the next snapshot.
    while (ker::mod::perf::drain_events_to(append_perf_event, &output, 64, UINT32_MAX) == 64) {
    }
    *output.cursor = '\0';
    return static_cast<size_t>(output.cursor - buf);
}

auto generate_kperfstatus(char* buf, size_t bufsz) -> size_t {
    if (bufsz == 0) {
        return 0;
    }
    auto const STATUS = ker::mod::perf::get_capture_status();
    char* cursor = buf;
    PerfEventTextWriter writer(cursor, buf + bufsz - 1);
    writer.append("perf_capture version=1 session=");
    writer.append_dec(STATUS.session);
    writer.append(" control_version=");
    writer.append_dec(STATUS.control_version);
    writer.append(" enabled=");
    writer.append_dec(STATUS.enabled ? 1 : 0);
    writer.append(" mask=");
    writer.append_dec(STATUS.mask);
    writer.append(" cpus=");
    writer.append_dec(STATUS.cpus);
    writer.append(" written=");
    writer.append_dec(STATUS.written);
    writer.append(" consumed=");
    writer.append_dec(STATUS.consumed);
    writer.append(" overwritten=");
    writer.append_dec(STATUS.overwritten);
    writer.append(" pending=");
    writer.append_dec(STATUS.pending);
    writer.append('\n');
    if (!writer.complete()) {
        cursor = buf;
    }
    *cursor = '\0';
    return static_cast<size_t>(cursor - buf);
}

// Generate content for /proc/kcpustat
// Shows per-CPU aggregate scheduler counters (non-destructive).
auto generate_kcpustat(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }
    cpu_count = std::min(cpu_count, ker::mod::perf::get_num_perf_cpus());

    for (uint64_t c = 0; c < cpu_count; ++c) {
        auto s = ker::mod::perf::get_cpu_stats(static_cast<uint32_t>(c));
        auto sched = ker::mod::sched::get_scheduler_trace_stats(c);
        auto const TIMERS = ker::mod::sched::get_run_queue_stats(c);
        auto const CLOCKEVENT = ker::mod::sys::context_switch::clockevent_status(c);
        auto mm_destroy = ker::mod::mm::virt::get_destroy_user_space_stats(c);
        ker::mod::mm::phys::PageRefStatsSnapshot ref_stats{};
        ker::mod::mm::phys::PageCacheStatsSnapshot page_cache_stats{};
        ker::mod::mm::virt::PageTablePoolStatsSnapshot page_table_pool_stats{};
        ker::mod::mm::virt::OwnedFrameStatsSnapshot owned_frame_stats{};
        if (c == 0) {
            ker::mod::mm::phys::get_page_ref_stats_snapshot(ref_stats);
            ker::mod::mm::phys::get_page_cache_stats_snapshot(page_cache_stats);
            ker::mod::mm::virt::get_page_table_pool_stats_snapshot(page_table_pool_stats);
            ker::mod::mm::virt::get_owned_frame_stats_snapshot(owned_frame_stats);
        }
        append_sconst(p, end, "cpu=");
        append_dec64(p, end, c);
        append_sconst(p, end, " ctx=");
        append_dec64(p, end, s.ctx_switches);
        append_sconst(p, end, " preempt=");
        append_dec64(p, end, s.preemptions);
        append_sconst(p, end, " yield=");
        append_dec64(p, end, s.yields);
        append_sconst(p, end, " sleep=");
        append_dec64(p, end, s.sleeps);
        append_sconst(p, end, " wake=");
        append_dec64(p, end, s.wakes);
        append_sconst(p, end, " sample=");
        append_dec64(p, end, s.samples);
        append_sconst(p, end, " fast_skip=");
        append_dec64(p, end, s.fastpath_skips);
        append_sconst(p, end, " ring_write=");
        append_dec64(p, end, s.ring_writes);
        append_sconst(p, end, " timer_irq=");
        append_dec64(p, end, sched.scheduler_timer_interrupts);
        append_sconst(p, end, " sched_arm=");
        append_dec64(p, end, sched.scheduler_timer_arms);
        append_sconst(p, end, " sched_disarm=");
        append_dec64(p, end, sched.scheduler_timer_disarms);
        append_sconst(p, end, " arm_wait=");
        append_dec64(p, end, sched.scheduler_timer_arm_wait_deadline);
        append_sconst(p, end, " arm_itimer=");
        append_dec64(p, end, sched.scheduler_timer_arm_itimer);
        append_sconst(p, end, " arm_vol=");
        append_dec64(p, end, sched.scheduler_timer_arm_voluntary);
        append_sconst(p, end, " arm_idlework=");
        append_dec64(p, end, sched.scheduler_timer_arm_idle_work);
        append_sconst(p, end, " arm_runq=");
        append_dec64(p, end, sched.scheduler_timer_arm_runqueue);
        append_sconst(p, end, " arm_comp=");
        append_dec64(p, end, sched.scheduler_timer_arm_competitor);
        append_sconst(p, end, " idle_arm=");
        append_dec64(p, end, sched.idle_timer_arms);
        append_sconst(p, end, " idle_disarm=");
        append_dec64(p, end, sched.idle_timer_disarms);
        append_sconst(p, end, " idle_wake=");
        append_dec64(p, end, sched.idle_timer_wakeups);
        append_sconst(p, end, " wake_ipi=");
        append_dec64(p, end, sched.wake_ipis_sent);
        append_sconst(p, end, " wake_coal=");
        append_dec64(p, end, sched.wake_ipis_coalesced);
        append_sconst(p, end, " local_resched=");
        append_dec64(p, end, sched.local_reschedule_requests);
        append_sconst(p, end, " local_poke=");
        append_dec64(p, end, sched.local_reschedule_timer_pokes);
        append_sconst(p, end, " slow_scan=");
        append_dec64(p, end, sched.slow_reschedule_scans);
        append_sconst(p, end, " wait_scan=");
        append_dec64(p, end, sched.wait_list_scan_iterations);
        append_sconst(p, end, " wait_pass=");
        append_dec64(p, end, sched.wait_list_scan_passes);
        append_sconst(p, end, " wait_max=");
        append_dec64(p, end, sched.wait_list_scan_max);
        append_sconst(p, end, " timer_wake=");
        append_dec64(p, end, sched.timer_expired_wakeups);
        append_sconst(p, end, " timer_queue_depth=");
        append_dec64(p, end, TIMERS.timer_queue_depth);
        append_sconst(p, end, " timer_extractions=");
        append_dec64(p, end, TIMERS.timer_extractions);
        append_sconst(p, end, " timer_budget_exhaustions=");
        append_dec64(p, end, TIMERS.timer_budget_exhaustions);
        append_sconst(p, end, " timer_late_us_max=");
        append_dec64(p, end, TIMERS.timer_late_us_max);
        append_sconst(p, end, " timer_irq_budget=");
        append_dec64(p, end, TIMERS.timer_irq_budget);
        append_sconst(p, end, " clockevent_initialized=");
        append_dec64(p, end, CLOCKEVENT.initialized ? 1 : 0);
        append_sconst(p, end, " clockevent_sample_ticks=");
        append_dec64(p, end, CLOCKEVENT.calibration.sample_ticks);
        append_sconst(p, end, " clockevent_sample_us=");
        append_dec64(p, end, CLOCKEVENT.calibration.sample_us);
        append_sconst(p, end, " clockevent_resolution_ns=");
        append_dec64(p, end, CLOCKEVENT.calibration.resolution_ns());
        append_sconst(p, end, " clockevent_min_us=");
        append_dec64(p, end, CLOCKEVENT.calibration.minimum_delay_us());
        append_sconst(p, end, " clockevent_max_us=");
        append_dec64(p, end, CLOCKEVENT.calibration.maximum_delay_us());
        append_sconst(p, end, " clockevent_quantum_us=");
        append_dec64(p, end, CLOCKEVENT.quantum_us);
        append_sconst(p, end, " gc_pass=");
        append_dec64(p, end, sched.gc_passes_triggered);
        append_sconst(p, end, " gc_reclaim=");
        append_dec64(p, end, sched.gc_tasks_reclaimed);
        append_sconst(p, end, " gc_us=");
        append_dec64(p, end, sched.gc_work_us_total);
        append_sconst(p, end, " gc_max_us=");
        append_dec64(p, end, sched.gc_work_us_max);
        append_sconst(p, end, " gc_task_us=");
        append_dec64(p, end, sched.gc_task_us_total);
        append_sconst(p, end, " gc_task_max=");
        append_dec64(p, end, sched.gc_task_us_max);
        append_sconst(p, end, " gc_detach_us=");
        append_dec64(p, end, sched.gc_detach_us_total);
        append_sconst(p, end, " gc_detach_max=");
        append_dec64(p, end, sched.gc_detach_us_max);
        append_sconst(p, end, " gc_pm_us=");
        append_dec64(p, end, sched.gc_pagemap_us_total);
        append_sconst(p, end, " gc_pm_max=");
        append_dec64(p, end, sched.gc_pagemap_us_max);
        append_sconst(p, end, " gc_thr_us=");
        append_dec64(p, end, sched.gc_thread_us_total);
        append_sconst(p, end, " gc_thr_max=");
        append_dec64(p, end, sched.gc_thread_us_max);
        append_sconst(p, end, " gc_misc_us=");
        append_dec64(p, end, sched.gc_misc_us_total);
        append_sconst(p, end, " gc_misc_max=");
        append_dec64(p, end, sched.gc_misc_us_max);
        append_sconst(p, end, " gc_dbg_us=");
        append_dec64(p, end, sched.gc_debug_us_total);
        append_sconst(p, end, " gc_dbg_max=");
        append_dec64(p, end, sched.gc_debug_us_max);
        append_sconst(p, end, " gc_defer_queued=");
        append_dec64(p, end, sched.gc_deferred_queued);
        append_sconst(p, end, " gc_defer_done=");
        append_dec64(p, end, sched.gc_deferred_completed);
        append_sconst(p, end, " gc_defer_depth=");
        append_dec64(p, end, sched.gc_deferred_depth);
        append_sconst(p, end, " gc_defer_depth_max=");
        append_dec64(p, end, sched.gc_deferred_depth_max);
        append_sconst(p, end, " gc_defer_slices=");
        append_dec64(p, end, sched.gc_deferred_slices);
        append_sconst(p, end, " gc_defer_slice_done=");
        append_dec64(p, end, sched.gc_deferred_slices_completed);
        append_sconst(p, end, " gc_defer_wait_max_us=");
        append_dec64(p, end, sched.gc_deferred_oldest_wait_us_max);
        append_sconst(p, end, " gc_idle_boost_pass=");
        append_dec64(p, end, sched.gc_idle_boost_passes);
        append_sconst(p, end, " gc_foreground_pass=");
        append_dec64(p, end, sched.gc_foreground_passes);
        append_sconst(p, end, " dus_calls=");
        append_dec64(p, end, mm_destroy.calls);
        append_sconst(p, end, " dus_collect_us=");
        append_dec64(p, end, mm_destroy.collect_frames_us_total);
        append_sconst(p, end, " dus_collect_max=");
        append_dec64(p, end, mm_destroy.collect_frames_us_max);
        append_sconst(p, end, " dus_data_us=");
        append_dec64(p, end, mm_destroy.free_data_us_total);
        append_sconst(p, end, " dus_data_max=");
        append_dec64(p, end, mm_destroy.free_data_us_max);
        append_sconst(p, end, " dus_pt_us=");
        append_dec64(p, end, mm_destroy.free_pt_us_total);
        append_sconst(p, end, " dus_pt_max=");
        append_dec64(p, end, mm_destroy.free_pt_us_max);
        append_sconst(p, end, " dus_tlb_us=");
        append_dec64(p, end, mm_destroy.tlb_flush_us_total);
        append_sconst(p, end, " dus_tlb_max=");
        append_dec64(p, end, mm_destroy.tlb_flush_us_max);
        append_sconst(p, end, " dus_leaf=");
        append_dec64(p, end, mm_destroy.data_leaf_entries_visited);
        append_sconst(p, end, " dus_refdec=");
        append_dec64(p, end, mm_destroy.data_pages_ref_decremented);
        append_sconst(p, end, " dus_freed=");
        append_dec64(p, end, mm_destroy.data_pages_freed);
        append_sconst(p, end, " dus_ptrefdec=");
        append_dec64(p, end, mm_destroy.page_table_pages_ref_decremented);
        append_sconst(p, end, " dus_ptfree=");
        append_dec64(p, end, mm_destroy.page_table_pages_freed);
        append_sconst(p, end, " dus_huge_skip=");
        append_dec64(p, end, mm_destroy.skipped_huge_pages);
        append_sconst(p, end, " dus_unknown_skip=");
        append_dec64(p, end, mm_destroy.skipped_unknown_frames);
        append_sconst(p, end, " dus_slab_skip=");
        append_dec64(p, end, mm_destroy.skipped_slab_alloc_frames);
        append_sconst(p, end, " dus_medium_skip=");
        append_dec64(p, end, mm_destroy.skipped_medium_alloc_frames);
        append_sconst(p, end, " dus_kmalloc_large_skip=");
        append_dec64(p, end, mm_destroy.skipped_kmalloc_large_alloc_frames);
        append_sconst(p, end, " dus_alias_skip=");
        append_dec64(p, end, mm_destroy.skipped_page_table_aliases);
        append_sconst(p, end, " dus_corrupt=");
        append_dec64(p, end, mm_destroy.skipped_corrupt_entries);
        append_sconst(p, end, " dus_corrupt_skip=");
        append_dec64(p, end, mm_destroy.skipped_corrupt_entries);
        append_sconst(p, end, " dus_magic_unknown_reads=");
        append_dec64(p, end, mm_destroy.magic_unknown_probe_reads);
        append_sconst(p, end, " dus_magic_unknown_slab=");
        append_dec64(p, end, mm_destroy.magic_unknown_slab_hits);
        append_sconst(p, end, " dus_magic_unknown_medium=");
        append_dec64(p, end, mm_destroy.magic_unknown_medium_hits);
        append_sconst(p, end, " dus_magic_unknown_kmalloc_large=");
        append_dec64(p, end, mm_destroy.magic_unknown_kmalloc_large_hits);
        append_sconst(p, end, " pa_ref_inc=");
        append_dec64(p, end, ref_stats.inc_ops);
        append_sconst(p, end, " pa_ref_inc_retry=");
        append_dec64(p, end, ref_stats.inc_cas_retries);
        append_sconst(p, end, " pa_ref_add=");
        append_dec64(p, end, ref_stats.add_ops);
        append_sconst(p, end, " pa_ref_add_refs=");
        append_dec64(p, end, ref_stats.add_refs);
        append_sconst(p, end, " pa_ref_add_retry=");
        append_dec64(p, end, ref_stats.add_cas_retries);
        append_sconst(p, end, " pa_ref_dec=");
        append_dec64(p, end, ref_stats.dec_ops);
        append_sconst(p, end, " pa_ref_dec_retry=");
        append_dec64(p, end, ref_stats.dec_cas_retries);
        append_sconst(p, end, " pa_ref_zero=");
        append_dec64(p, end, ref_stats.dec_zero_candidates);
        append_sconst(p, end, " pa_ref_zero_freed=");
        append_dec64(p, end, ref_stats.dec_zero_pages_freed);
        append_sconst(p, end, " pa_ref_zero_bad=");
        append_dec64(p, end, ref_stats.dec_zero_validation_failed);
        append_sconst(p, end, " pa_ref_batch=");
        append_dec64(p, end, ref_stats.batch_calls);
        append_sconst(p, end, " pa_ref_batch_pages=");
        append_dec64(p, end, ref_stats.batch_pages);
        append_sconst(p, end, " pa_ref_batch_zero=");
        append_dec64(p, end, ref_stats.batch_zero_candidates);
        append_sconst(p, end, " pa_ref_batch_runs=");
        append_dec64(p, end, ref_stats.batch_free_runs);
        append_sconst(p, end, " pa_ref_batch_freed=");
        append_dec64(p, end, ref_stats.batch_pages_freed);
        append_sconst(p, end, " pa_cache_enabled=");
        append_dec64(p, end, page_cache_stats.enabled);
        append_sconst(p, end, " pa_cache_capacity=");
        append_dec64(p, end, page_cache_stats.capacity);
        append_sconst(p, end, " pa_cache_cached=");
        append_dec64(p, end, page_cache_stats.cached_pages);
        append_sconst(p, end, " pa_cache_hit=");
        append_dec64(p, end, page_cache_stats.alloc_hits);
        append_sconst(p, end, " pa_cache_miss=");
        append_dec64(p, end, page_cache_stats.alloc_misses);
        append_sconst(p, end, " pa_cache_refill=");
        append_dec64(p, end, page_cache_stats.refill_calls);
        append_sconst(p, end, " pa_cache_refill_pages=");
        append_dec64(p, end, page_cache_stats.refill_pages);
        append_sconst(p, end, " pa_cache_free=");
        append_dec64(p, end, page_cache_stats.free_hits);
        append_sconst(p, end, " pa_cache_free_miss=");
        append_dec64(p, end, page_cache_stats.free_misses);
        append_sconst(p, end, " pa_cache_drain=");
        append_dec64(p, end, page_cache_stats.drain_calls);
        append_sconst(p, end, " pa_cache_drain_pages=");
        append_dec64(p, end, page_cache_stats.drain_pages);
        append_sconst(p, end, " pa_cache_stale=");
        append_dec64(p, end, page_cache_stats.stale_entries);
        append_sconst(p, end, " pt_pool_capacity=");
        append_dec64(p, end, page_table_pool_stats.capacity);
        append_sconst(p, end, " pt_pool_cached=");
        append_dec64(p, end, page_table_pool_stats.cached_pages);
        append_sconst(p, end, " pt_pool_hit=");
        append_dec64(p, end, page_table_pool_stats.alloc_hits);
        append_sconst(p, end, " pt_pool_miss=");
        append_dec64(p, end, page_table_pool_stats.alloc_misses);
        append_sconst(p, end, " pt_pool_release=");
        append_dec64(p, end, page_table_pool_stats.releases);
        append_sconst(p, end, " pt_pool_reject=");
        append_dec64(p, end, page_table_pool_stats.rejects);
        append_sconst(p, end, " of_capacity=");
        append_dec64(p, end, owned_frame_stats.capacity);
        append_sconst(p, end, " of_entries=");
        append_dec64(p, end, owned_frame_stats.entries);
        append_sconst(p, end, " of_track=");
        append_dec64(p, end, owned_frame_stats.track_attempts);
        append_sconst(p, end, " of_added=");
        append_dec64(p, end, owned_frame_stats.track_added);
        append_sconst(p, end, " of_replaced=");
        append_dec64(p, end, owned_frame_stats.track_replaced);
        append_sconst(p, end, " of_skipped=");
        append_dec64(p, end, owned_frame_stats.track_skipped);
        append_sconst(p, end, " of_conflict=");
        append_dec64(p, end, owned_frame_stats.track_conflicts);
        append_sconst(p, end, " of_probe_fail=");
        append_dec64(p, end, owned_frame_stats.track_probe_failures);
        append_sconst(p, end, " of_untrack=");
        append_dec64(p, end, owned_frame_stats.untrack_attempts);
        append_sconst(p, end, " of_removed=");
        append_dec64(p, end, owned_frame_stats.untrack_removed);
        append_sconst(p, end, " of_missed=");
        append_dec64(p, end, owned_frame_stats.untrack_missed);
        append_sconst(p, end, " of_purge=");
        append_dec64(p, end, owned_frame_stats.purge_calls);
        append_sconst(p, end, " of_purge_removed=");
        append_dec64(p, end, owned_frame_stats.purge_removed);
        append_sconst(p, end, " lb_push=");
        append_dec64(p, end, sched.load_balance_pushes);
        if (p + 1 < end) {
            *p++ = '\n';
        }
    }
    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Generate content for /proc/kcpustate
// Shows a locked snapshot of each CPU's current task and queue state.
auto generate_kcpustate(char* buf, size_t bufsz) -> size_t {
    char* p = buf;
    char const* end = buf + bufsz - 1;

    uint64_t cpu_count = ker::mod::smt::get_core_count();
    if (cpu_count == 0) {
        cpu_count = 1;
    }

    auto const LOAD = ker::mod::sched::get_load_average_snapshot();
    append_sconst(p, end, "loadavg_state");
    append_memacc_dec(p, end, "load1_milli", LOAD.load1_milli);
    append_memacc_dec(p, end, "load5_milli", LOAD.load5_milli);
    append_memacc_dec(p, end, "load15_milli", LOAD.load15_milli);
    append_memacc_dec(p, end, "active", LOAD.active_for_load_tasks);
    append_memacc_dec(p, end, "runnable", LOAD.runnable_tasks);
    append_memacc_dec(p, end, "uninterruptible", LOAD.uninterruptible_tasks);
    append_memacc_dec(p, end, "runq", LOAD.runnable_queue_tasks);
    append_memacc_dec(p, end, "runq_voluntary", LOAD.runnable_voluntary_tasks);
    append_memacc_dec(p, end, "waiting", LOAD.waiting_tasks);
    append_memacc_dec(p, end, "dead", LOAD.dead_or_exiting_tasks);
    append_memacc_dec(p, end, "ptrace_stopped", LOAD.ptrace_stopped_tasks);
    append_memacc_dec(p, end, "total", LOAD.total_tasks);
    append_memacc_dec(p, end, "last_pid", LOAD.last_pid);
    append_char(p, end, '\n');

    for (uint64_t c = 0; c < cpu_count; ++c) {
        auto state = ker::mod::sched::get_scheduler_cpu_state(c);
        append_sconst(p, end, "cpu_state");
        append_memacc_dec(p, end, "cpu", state.cpu_no);
        append_memacc_bool(p, end, "idle", state.is_idle);
        append_memacc_dec(p, end, "runq", state.runnable_count);
        append_memacc_dec(p, end, "waitq", state.wait_queue_count);
        append_memacc_dec(p, end, "cur_pid", state.current_pid);
        append_memacc_str(p, end, "cur_name", state.current_name);
        append_memacc_str(p, end, "cur_type", task_type_name(static_cast<ker::mod::sched::task::TaskType>(state.current_type)));
        append_memacc_bool(p, end, "cur_vblk", state.current_voluntary_block);
        append_memacc_bool(p, end, "cur_wblk", state.current_wants_block);
        append_memacc_bool(p, end, "cur_pinned", state.current_cpu_pinned);
        append_memacc_dec(p, end, "task_cpu", state.current_task_cpu);
        append_memacc_str(p, end, "frame_class",
                          ker::mod::sched::task::saved_frame_class_name(
                              static_cast<ker::mod::sched::task::SavedFrameClass>(state.current_saved_frame_class)));
        append_memacc_str(p, end, "preempt_frame",
                          ker::mod::sched::task::saved_frame_class_name(
                              static_cast<ker::mod::sched::task::SavedFrameClass>(state.current_preemption_frame_class)));
        append_memacc_str(p, end, "preempt_reason",
                          ker::mod::sched::kernel_preemption_block_reason_name(
                              static_cast<ker::mod::sched::KernelPreemptionBlockReason>(state.current_preemption_reason)));
        append_memacc_dec(p, end, "preempt_source_cpu", state.current_preemption_source_cpu);
        append_memacc_dec(p, end, "preempt_target_cpu", state.current_preemption_target_cpu);
        append_memacc_dec(p, end, "preempt_depth", state.current_preempt_depth);
        append_memacc_bool(p, end, "preempt_pending", state.current_preempt_pending);
        append_memacc_dec(p, end, "preempt_max_us", state.current_preempt_max_us);
        append_memacc_hex(p, end, "preempt_owner", state.current_preempt_owner);
        append_memacc_dec(p, end, "preempt_start_us", state.current_preempt_start_us);
        append_memacc_dec(p, end, "migration_depth", state.current_migration_depth);
        append_memacc_dec(p, end, "migration_owner_cpu", state.current_migration_owner_cpu);
        append_memacc_dec(p, end, "migration_max_us", state.current_migration_max_us);
        append_memacc_hex(p, end, "migration_owner", state.current_migration_owner);
        append_memacc_dec(p, end, "migration_start_us", state.current_migration_start_us);
        append_memacc_str(p, end, "migration_frame",
                          ker::mod::sched::task::saved_frame_class_name(
                              static_cast<ker::mod::sched::task::SavedFrameClass>(state.current_migration_frame_class)));
        append_memacc_str(p, end, "migration_reason",
                          ker::mod::sched::migration_rejection_reason_name(
                              static_cast<ker::mod::sched::MigrationRejectionReason>(state.current_migration_reason)));
        append_memacc_dec(p, end, "migration_source_cpu", state.current_migration_source_cpu);
        append_memacc_dec(p, end, "migration_target_cpu", state.current_migration_target_cpu);
        append_memacc_bool(p, end, "scheduler_transition", state.current_scheduler_transition);
        append_char(p, end, '\n');
    }

    *p = '\0';
    return static_cast<size_t>(p - buf);
}

// Generate content for /proc/version
auto generate_version(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };

    append(ker::release::NAME);
    append(" version ");
    append(ker::release::VERSION);
    append(" (");
    append(ker::release::BUILDER);
    append(") (");
    append(ker::release::COMPILER);
    append(") #1 ");
    append(ker::release::SMP);
    append("\n");

    buf[off] = '\0';
    return off;
}

// Procfs stores generated content in the File's private_data as ProcFileData.
auto generate_blockq(char* buffer, size_t capacity) -> size_t {
    constexpr size_t MAX_QUEUES = 64;
    auto* rows = new (std::nothrow) ker::dev::BlockQueueSnapshot[MAX_QUEUES];
    if (rows == nullptr) return 0;
    size_t const COUNT = ker::dev::block_queue_snapshots({rows, MAX_QUEUES});
    char* cursor = buffer;
    char const* end = buffer + capacity - 1;
    append_sconst(cursor, end, "blockq version=1 capacity=64 count=");
    append_dec64(cursor, end, COUNT);
    append_char(cursor, end, '\n');
    for (size_t i = 0; i < COUNT; ++i) {
        auto const& row = rows[i];
        append_sconst(cursor, end, "device=");
        append_sconst(cursor, end, row.name.data());
        auto field = [&](const char* key, uint64_t value) {
            append_char(cursor, end, ' ');
            append_sconst(cursor, end, key);
            append_char(cursor, end, '=');
            append_dec64(cursor, end, value);
        };
        field("generation", row.generation);
        field("open", row.admission_open);
        field("async_open", row.async_admission_open);
        field("worker", row.worker_present);
        field("caller_driven", row.caller_driven);
        field("depth", row.limits.depth);
        field("owned", row.owned_requests);
        field("owned_bytes", row.owned_bytes);
        field("queued", row.queued);
        field("inflight", row.issued);
        field("inflight_bytes", row.issued_bytes);
        field("completing", row.completing);
        field("submitted", row.stats.submitted);
        field("completed", row.stats.completed);
        field("released", row.stats.released);
        field("backpressure", row.stats.admission_rejections);
        field("timeouts", row.stats.timeouts);
        field("resets", row.stats.resets);
        field("stale", row.stats.stale_completions);
        field("flushes", row.flushes);
        field("request_high", row.stats.request_high_water);
        field("byte_high", row.stats.byte_high_water);
        field("latency_us", row.latency_us);
        field("max_latency_us", row.max_latency_us);
#ifdef WOS_BLOCK_WAIT_DIAGNOSTICS
        auto const& wait = row.wait_diagnostics;
        field("wait_bursts", wait.bursts);
        field("wait_completed", wait.completed);
        field("wait_no_context", wait.no_context);
        field("wait_mandatory_busy", wait.mandatory_busy);
        field("wait_disallowed", wait.disallowed);
        field("wait_clock_regression", wait.clock_regression);
        field("wait_budget", wait.budget);
        field("wait_deadline", wait.deadline);
        field("wait_extra_busy", wait.extra_busy);
        field("wait_count_cap", wait.count_cap);
        field("wait_extra_passes", wait.extra_passes);
        field("wait_count_cap_under_budget", wait.count_cap_under_budget);
        field("wait_cap_elapsed_us", wait.cap_elapsed_us);
        field("wait_cap_elapsed_max_us", wait.cap_elapsed_max_us);
        field("wait_park_attempts", wait.park_attempts);
#endif
        ker::dev::ahci::AhciAsyncSnapshot hardware{};
        if (ker::dev::ahci::ahci_async_snapshot(ker::dev::block_device_find_by_name(row.name.data()), hardware)) {
            field("ahci_depth", hardware.depth);
            field("ahci_ncq", hardware.ncq);
            field("ahci_active", hardware.active);
            field("ahci_high", hardware.high_water);
            field("hardware_active", hardware.hardware_active);
            field("hardware_high", hardware.hardware_high_water);
            field("pxci", hardware.current_ci);
            field("pxsact", hardware.current_sact);
            field("hba_cap", hardware.hba_cap);
            field("identify75", hardware.identify_queue_depth);
            field("identify76", hardware.identify_sata_caps);
            field("identify83", hardware.identify_command_sets);
            field("ahci_resets", hardware.resets);
            field("ahci_quarantines", hardware.quarantines);
            field("ahci_offline", hardware.offline);
        }
        append_sconst(cursor, end, " latency_log2_us=");
        for (size_t bucket = 0; bucket < row.latency_histogram.size(); ++bucket) {
            if (bucket != 0) append_char(cursor, end, ',');
            append_dec64(cursor, end, row.latency_histogram[bucket]);
        }
        append_char(cursor, end, '\n');
    }
    *cursor = '\0';
    delete[] rows;
    return static_cast<size_t>(cursor - buffer);
}

// --- FileOperations ---

// File retains the target; a user thread's immutable LifetimeOwner retains its
// canonical process. Read and seek must admit the same subject and caller.
auto authorize_procfs_limits(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const ProcFileData& data,
                             ProcTask*& process) -> int {
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    auto* subject = data.target;
    if (subject == nullptr || subject->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) return -ESRCH;
    auto* owner = subject->credential_owner();
    if (owner == nullptr || owner->state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE) return -ESRCH;
    if (!credentials.allows(ker::mod::security::Authority::RESOURCE_LIMIT) && (caller_pid == 0 || caller_pid != owner->pid)) return -EACCES;
    process = owner;
    return 0;
}

auto procfs_read_limits(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, ProcFileData& data, void* buf, size_t count,
                        size_t offset) -> ssize_t {
    using namespace ker::mod::sched::resource;
    ker::mod::sys::MutexGuard content_guard(data.limits_content_lock);
    std::array<Usage, ker::abi::resource::COUNT> snapshot{};
    ProcTask* process = nullptr;
    int const POLICY = authorize_procfs_limits(credentials, caller_pid, data, process);
    if (POLICY != 0) return POLICY;
    if (data.content == nullptr) {
        uint64_t const FD_FLAGS = process->fd_table_lock.lock_irqsave();
        uint64_t const FLAGS = process->resource_lock.lock_irqsave();
        auto& fd = process->resources.entries[static_cast<unsigned>(Id::NOFILE)];
        fd.current = process->fd_table.size() + process->fd_table.reserved_count();
        fd.peak = std::max(fd.peak, fd.current);
        snapshot = process->resources.entries;
        process->resource_lock.unlock_irqrestore(FLAGS);
        process->fd_table_lock.unlock_irqrestore(FD_FLAGS);
        // Completed VM transactions publish AS observations. Do not
        // acquire the VM mutex here: file mmap may read procfs while
        // already holding it, including this File's content mutex.
        copy_address_space_observation(*data.target, snapshot[static_cast<unsigned>(Id::AS)]);
        copy_process_observation(*data.target, snapshot[static_cast<unsigned>(Id::NPROC)]);
    }
    if (data.content == nullptr) {
        constexpr size_t CAPACITY = 4096;
        data.content = new (std::nothrow) char[CAPACITY];
        if (data.content == nullptr) {
            return -ENOMEM;
        }
        size_t off = 0;
        auto append = [&](const char* text) {
            while (*text != '\0' && off + 1 < CAPACITY) {
                data.content[off++] = *text++;
            }
        };
        auto number = [&](uint64_t value, bool limit = false) {
            if (limit && value == INFINITY_VALUE) {
                append("infinity");
            } else {
                std::array<char, 24> digits{};
                int_to_str(value, digits.data(), digits.size());
                append(digits.data());
            }
        };
        struct Row {
            Id id;
            const char* name;
            const char* units;
            bool enforced;
        };
        constexpr std::array<Row, 7> ROWS{{
            {Id::CPU, "CPU", "seconds", true},
            {Id::FSIZE, "FSIZE", "bytes", true},
            {Id::STACK, "STACK", "bytes", true},
            {Id::CORE, "CORE", "bytes", true},
            {Id::NPROC, "NPROC", "tasks", true},
            {Id::NOFILE, "NOFILE", "descriptors", true},
            {Id::AS, "AS", "bytes", true},
        }};
        append("Resource Soft Hard Current Peak Denied Reason Units Enforcement\n");
        for (auto const& row : ROWS) {
            auto const& entry = snapshot[static_cast<unsigned>(row.id)];
            append(row.name);
            append(" ");
            number(entry.limit.soft, true);
            append(" ");
            number(entry.limit.hard, true);
            append(" ");
            number(entry.current);
            append(" ");
            number(entry.peak);
            append(" ");
            number(entry.denied);
            append(" ");
            switch (entry.reason) {
                case Denial::NONE:
                    append("none");
                    break;
                case Denial::HARD_LIMIT:
                    append("hard_limit");
                    break;
                case Denial::SOFT_LIMIT:
                    append("soft_limit");
                    break;
                case Denial::OVERFLOW:
                    append("overflow");
                    break;
            }
            append(" ");
            append(row.units);
            append(row.enforced ? " enforced\n" : " pending\n");
        }
        data.content[off] = '\0';
        data.content_len = off;
    }
    int const VALID_VIEW = procfs_view_policy(data);
    if (VALID_VIEW != 0) return VALID_VIEW;
    if (offset >= data.content_len) {
        return 0;
    }
    count = std::min(count, data.content_len - offset);
    std::memcpy(buf, data.content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_read_private(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, ProcFileData& data, void* buf, size_t count,
                         size_t offset) -> ssize_t {
    ker::mod::security::CredentialStore::Observation before{};
    int const POLICY = authorize_private_procfs(credentials, caller_pid, data, before);
    if (POLICY != 0) return POLICY;
    ker::mod::sys::MutexGuard content_guard(data.limits_content_lock);
    if (data.content == nullptr || data.content_credential_generation != before.generation) {
        bool const LARGE = data.node.type == ProcNodeType::MAPS_FILE || data.node.type == ProcNodeType::IMAGES_FILE;
        size_t const CAPACITY = LARGE ? 262144 : 4096;
        auto* staged = new (std::nothrow) char[CAPACITY];
        if (staged == nullptr) return -ENOMEM;
        size_t length = 0;
        switch (data.node.type) {
            case ProcNodeType::CMDLINE_FILE:
                length = generate_cmdline(data.target, staged, CAPACITY);
                break;
            case ProcNodeType::MAPS_FILE:
                length = generate_maps(data.target, staged, CAPACITY);
                break;
            case ProcNodeType::IMAGES_FILE:
                length = generate_images(data.target, staged, CAPACITY,
                                         data.pid_view.is_host() && credentials.allows(ker::mod::security::Authority::KERNEL_DEBUG));
                break;
            case ProcNodeType::EXE_LINK: {
                uint64_t const IRQF = data.target->path_metadata_lock.lock_irqsave();
                length = static_cast<size_t>(procfs_copy_link_target(data.target->exe_path.data(), staged, CAPACITY - 1));
                data.target->path_metadata_lock.unlock_irqrestore(IRQF);
                staged[length] = '\0';
                break;
            }
            default:
                staged[0] = '\0';
                break;
        }
        int const VALID = revalidate_private_procfs(data, before);
        if (VALID != 0 || (LARGE && length == 0)) {
            delete[] staged;
            return VALID != 0 ? VALID : -ESRCH;
        }
        delete[] data.content;
        data.content = staged;
        data.content_len = length;
        data.content_credential_generation = before.generation;
    } else {
        int const VALID = revalidate_private_procfs(data, before);
        if (VALID != 0) return VALID;
    }
    if (offset >= data.content_len) return 0;
    count = std::min(count, data.content_len - offset);
    std::memcpy(buf, data.content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_read_with_credentials(const ker::mod::security::Snapshot& credentials, File* f, void* buf, size_t count, size_t offset,
                                  uint64_t caller_pid = 0, ProcTask* caller_task = nullptr) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(*pfd);
    if (VIEW != 0) return VIEW;
    if (pfd->node.type == ProcNodeType::MOUNTS_FILE) {
        if ((caller_task == nullptr && caller_pid != 0) ||
            (caller_task != nullptr && ker::mod::sched::task::process_pid(*caller_task) != caller_pid))
            return -ESRCH;
        return procfs_read_mounts_as(caller_task, credentials, f, buf, count, offset);
    }
    auto const ACCESS = diagnostic_access(pfd->node.type);
    int const POLICY = ker::mod::security::authorize_diagnostic(credentials, ACCESS, false);
    if (POLICY != 0) return POLICY;
    if (private_procfs_node(pfd->node.type)) return procfs_read_private(credentials, caller_pid, *pfd, buf, count, offset);
    if (pfd->node.type == ProcNodeType::LIMITS_FILE) {
        return procfs_read_limits(credentials, caller_pid, *pfd, buf, count, offset);
    }
    ker::mod::sys::MutexGuard content_guard(pfd->limits_content_lock);

    // Lazily generate content
    if (pfd->content == nullptr) {
        constexpr size_t MAX_PROCFS_BUF = 4096;
        constexpr size_t MAX_KPERF_BUF = 65536;  // 64 KiB for event streams
        constexpr size_t MAX_BLOCKQ_BUF = 131072;
        constexpr size_t MAX_MEMACC_BUF = 262144;
        bool const IS_RUNTIME_MAP = pfd->node.type == ProcNodeType::MAPS_FILE || pfd->node.type == ProcNodeType::IMAGES_FILE;
        bool const IS_KPERF = (pfd->node.type == ProcNodeType::KPERF_FILE || pfd->node.type == ProcNodeType::KWKISTAT_FILE ||
                               pfd->node.type == ProcNodeType::KCPUSTAT_FILE || pfd->node.type == ProcNodeType::KCPUSTATE_FILE ||
                               pfd->node.type == ProcNodeType::KCONTSTAT_FILE || pfd->node.type == ProcNodeType::KIPCSTAT_FILE ||
                               pfd->node.type == ProcNodeType::WKI_NETDIAG_FILE || pfd->node.type == ProcNodeType::WKI_NAPI_FILE ||
                               pfd->node.type == ProcNodeType::WKI_PIPES_FILE || pfd->node.type == ProcNodeType::CPU_STAT_FILE ||
                               pfd->node.type == ProcNodeType::BLOCK_QUEUE_FILE);
        bool const IS_LARGE_WKI = pfd->node.type == ProcNodeType::WKI_CHAOS_FILE;
        bool const IS_MEMACC =
            (pfd->node.type == ProcNodeType::MEMACC_SUMMARY_FILE || pfd->node.type == ProcNodeType::MEMACC_ZONES_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_PROCS_FILE || pfd->node.type == ProcNodeType::MEMACC_DEAD_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_ALLOC_TOTALS_FILE || pfd->node.type == ProcNodeType::MEMACC_SLABS_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_KMALLOC_LIVE_FILE || pfd->node.type == ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_PAGE_CALLERS_FILE || pfd->node.type == ProcNodeType::MEMACC_FEATURES_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE ||
             pfd->node.type == ProcNodeType::MEMACC_RECLAIM_COORDINATOR_FILE);
        size_t alloc_sz = MAX_PROCFS_BUF;
        if (pfd->node.type == ProcNodeType::WKI_NETDIAG_FILE) {
            alloc_sz = MAX_WKI_NETDIAG_BUF;
        } else if (pfd->node.type == ProcNodeType::WKI_NAPI_FILE) {
            alloc_sz = MAX_WKI_NAPI_BUF;
        } else if (pfd->node.type == ProcNodeType::BLOCK_QUEUE_FILE) {
            alloc_sz = MAX_BLOCKQ_BUF;
        } else if (IS_MEMACC || IS_RUNTIME_MAP || IS_LARGE_WKI) {
            alloc_sz = MAX_MEMACC_BUF;
        } else if (IS_KPERF) {
            alloc_sz = MAX_KPERF_BUF;
        }
        pfd->content = new (std::nothrow) char[alloc_sz];
        if (pfd->content == nullptr) {
            return -ENOMEM;
        }

        switch (pfd->node.type) {
            case ProcNodeType::STATUS_FILE:
                pfd->content_len =
                    generate_status(pfd->target, pfd->content, MAX_PROCFS_BUF, pfd->node.thread_view, !pfd->pid_view.is_host());
                break;
            case ProcNodeType::STAT_FILE:
                pfd->content_len =
                    generate_stat(pfd->target, pfd->content, MAX_PROCFS_BUF, pfd->node.thread_view, !pfd->pid_view.is_host());
                break;
            case ProcNodeType::STATM_FILE:
                pfd->content_len = generate_statm(pfd->target, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::UPTIME_FILE:
                pfd->content_len = generate_uptime(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::TIMEKEEPING_FILE:
                pfd->content_len = generate_timekeeping(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::CPU_STAT_FILE:
                pfd->content_len = generate_cpu_stat(pfd->content, MAX_KPERF_BUF, &pfd->pid_view);
                break;
            case ProcNodeType::LOADAVG_FILE:
                pfd->content_len = generate_loadavg(pfd->content, MAX_PROCFS_BUF, &pfd->pid_view);
                break;
            case ProcNodeType::MEMINFO_FILE:
                pfd->content_len = generate_meminfo(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::VERSION_FILE:
                pfd->content_len = generate_version(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KPERF_FILE:
                pfd->content_len = generate_kperf(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KWKISTAT_FILE:
                pfd->content_len = generate_kwkistat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KCPUSTAT_FILE:
                pfd->content_len = generate_kcpustat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KCPUSTATE_FILE:
                pfd->content_len = generate_kcpustate(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::KPERFCTL_FILE:
                pfd->content_len = generate_kperfctl(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KPERFSTATUS_FILE:
                pfd->content_len = generate_kperfstatus(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::KCONTSTAT_FILE:
                pfd->content_len = generate_kcontstat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::BLOCK_QUEUE_FILE:
                pfd->content_len = generate_blockq(pfd->content, MAX_BLOCKQ_BUF);
                break;
            case ProcNodeType::KIPCSTAT_FILE:
                pfd->content_len = generate_kipcstat(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::WKI_LAUNCHER_FILE:
                pfd->content_len = generate_wki_launcher(pfd->target, pfd->content, MAX_PROCFS_BUF, !pfd->pid_view.is_host());
                break;
            case ProcNodeType::WKI_RUNNER_FILE:
                pfd->content_len = generate_wki_runner(pfd->target, pfd->content, MAX_PROCFS_BUF, !pfd->pid_view.is_host());
                break;
            case ProcNodeType::WKI_REMOTE_PID_FILE:
                pfd->content_len = generate_wki_remote_pid(pfd->target, pfd->content, MAX_PROCFS_BUF, !pfd->pid_view.is_host());
                break;
            case ProcNodeType::WKI_PEERS_FILE:
                pfd->content_len = generate_wki_peers(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_AUTH_FILE:
                pfd->content_len = generate_wki_auth(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::WKI_NAPI_FILE:
                pfd->content_len = generate_wki_napi(pfd->content, MAX_WKI_NAPI_BUF);
                break;
            case ProcNodeType::WKI_NETDIAG_FILE:
                pfd->content_len = generate_wki_netdiag(pfd->content, MAX_WKI_NETDIAG_BUF);
                break;
            case ProcNodeType::WKI_PIPES_FILE:
                pfd->content_len = ker::vfs::vfs_generate_local_pipe_diag(pfd->content, MAX_KPERF_BUF);
                break;
            case ProcNodeType::WKI_CHAOS_FILE:
                pfd->content_len = generate_wki_chaos(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::WKI_CHAOS_WORKLOAD_FILE:
                pfd->content_len = ker::net::wki::wki_chaos_workload_snapshot(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_SUMMARY_FILE:
                pfd->content_len = generate_memacc_summary(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_ZONES_FILE:
                pfd->content_len = generate_memacc_zones(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_PROCS_FILE:
                pfd->content_len = generate_memacc_procs(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_DEAD_FILE:
                pfd->content_len = generate_memacc_dead(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_ALLOC_TOTALS_FILE:
                pfd->content_len = generate_memacc_alloc_totals(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_SLABS_FILE:
                pfd->content_len = generate_memacc_slabs(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_KMALLOC_LIVE_FILE:
                pfd->content_len = generate_memacc_kmalloc_live(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE:
                pfd->content_len = generate_memacc_kmalloc_callers(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_PAGE_CALLERS_FILE:
                pfd->content_len = generate_memacc_page_callers(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_FEATURES_FILE:
                pfd->content_len = generate_memacc_features(pfd->content, MAX_MEMACC_BUF);
                break;
            case ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE:
            case ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE:
                pfd->content_len = generate_memacc_track_state(pfd->node.type, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE:
                pfd->content_len = generate_memacc_reclaim_buffer_cache(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE:
                pfd->content_len = generate_memacc_reclaim_packet_pool(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE:
                pfd->content_len = generate_memacc_reclaim_xfs_inode(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE:
                pfd->content_len = generate_memacc_reclaim_file_mmap_cache(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE:
                pfd->content_len = generate_memacc_reclaim_anonymous_swap(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MEMACC_RECLAIM_COORDINATOR_FILE:
                pfd->content_len = generate_memacc_reclaim_coordinator(pfd->content, MAX_MEMACC_BUF);
                break;
            default:
                pfd->content[0] = '\0';
                pfd->content_len = 0;
                break;
        }
    }

    auto requires_live_task_content = [](ProcNodeType type) -> bool {
        switch (type) {
            case ProcNodeType::STATUS_FILE:
            case ProcNodeType::STAT_FILE:
            case ProcNodeType::STATM_FILE:
            case ProcNodeType::MAPS_FILE:
            case ProcNodeType::IMAGES_FILE:
            case ProcNodeType::WKI_LAUNCHER_FILE:
            case ProcNodeType::WKI_RUNNER_FILE:
            case ProcNodeType::WKI_REMOTE_PID_FILE:
                return true;
            default:
                return false;
        }
    };
    if (pfd->content_len == 0 && requires_live_task_content(pfd->node.type)) {
        delete[] pfd->content;
        pfd->content = nullptr;
        return -ESRCH;
    }

    int const VALID_VIEW = procfs_view_policy(*pfd);
    if (VALID_VIEW != 0) return VALID_VIEW;
    if (offset >= pfd->content_len) {
        return 0;
    }
    size_t const AVAIL = pfd->content_len - offset;
    count = std::min(count, AVAIL);
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return procfs_read_with_credentials(current_procfs_credentials(), f, buf, count, offset, current_procfs_pid(), task);
}

auto procfs_ascii_space(char c) -> bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }

auto procfs_command_equals(const char* s, size_t count, const char* expected) -> bool {
    while (count > 0 && procfs_ascii_space(*s)) {
        s++;
        count--;
    }
    while (count > 0 && procfs_ascii_space(s[count - 1])) {
        count--;
    }

    size_t expected_len = 0;
    while (expected[expected_len] != '\0') {
        expected_len++;
    }
    if (count != expected_len) {
        return false;
    }
    for (size_t i = 0; i < count; ++i) {
        char lhs = s[i];
        char rhs = expected[i];
        if (lhs >= 'A' && lhs <= 'Z') {
            lhs = static_cast<char>(lhs - 'A' + 'a');
        }
        if (rhs >= 'A' && rhs <= 'Z') {
            rhs = static_cast<char>(rhs - 'A' + 'a');
        }
        if (lhs != rhs) {
            return false;
        }
    }
    return true;
}

auto procfs_parse_u64_trimmed(const char* s, size_t count, uint64_t& out) -> bool {
    while (count > 0 && procfs_ascii_space(*s)) {
        s++;
        count--;
    }
    while (count > 0 && procfs_ascii_space(s[count - 1])) {
        count--;
    }
    if (count == 0) {
        return false;
    }

    uint64_t value = 0;
    for (size_t i = 0; i < count; ++i) {
        char const CH = s[i];
        if (CH < '0' || CH > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(CH - '0');
        if (value > (UINT64_MAX - DIGIT) / 10) {
            return false;
        }
        value = (value * 10) + DIGIT;
    }
    out = value;
    return true;
}

auto procfs_write_memacc_track(ProcNodeType type, const char* s, size_t count) -> ssize_t {
    bool const PAGE_CALLERS = type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE;
    bool const AVAILABLE =
        PAGE_CALLERS ? ker::mod::mm::phys::page_caller_stats_available() : ker::mod::mm::dyn::kmalloc::debug_info_available();
    if (!AVAILABLE) {
        return -ENOTSUP;
    }

    if (procfs_command_equals(s, count, "on") || procfs_command_equals(s, count, "1")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_reset();
            ker::mod::mm::phys::page_caller_stats_set_enabled(true);
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_reset();
            ker::mod::mm::dyn::kmalloc::debug_info_set_enabled(true);
        }
    } else if (procfs_command_equals(s, count, "off") || procfs_command_equals(s, count, "0")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_set_enabled(false);
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_set_enabled(false);
        }
    } else if (procfs_command_equals(s, count, "reset")) {
        if (PAGE_CALLERS) {
            ker::mod::mm::phys::page_caller_stats_reset();
        } else {
            ker::mod::mm::dyn::kmalloc::debug_info_reset();
        }
    } else if (procfs_command_equals(s, count, "status")) {
        // No-op convenience for scripts that use one path for reads and writes.
    } else {
        return -EINVAL;
    }

    return static_cast<ssize_t>(count);
}

auto procfs_request_explicit_reclaim(std::string_view shrinker, uint64_t target_pages, uint64_t budget_units = 0) -> bool {
    ker::mod::mm::reclaim::ReclaimRequest const REQUEST{
        .context = ker::mod::mm::reclaim::ReclaimContext::EXPLICIT,
        .priority = ker::mod::mm::reclaim::ReclaimPriority::CRITICAL,
        .target_pages = std::max<uint64_t>(target_pages, 1),
        .budget_units = budget_units,
        .may_block = true,
        .may_io = true,
        .may_allocate = true,
    };
    return ker::mod::mm::reclaim::request_explicit(REQUEST, shrinker);
}

auto procfs_write_memacc_reclaim_buffer_cache(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t target_bytes = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        target_bytes = 0;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, target_bytes)) {
        return -EINVAL;
    }

    if (target_bytes > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    auto const BEFORE = ker::vfs::buffer_cache_stats();
    if (BEFORE.total_bytes > target_bytes) {
        uint64_t const TO_RECLAIM = BEFORE.total_bytes - target_bytes;
        uint64_t const TARGET_PAGES =
            (TO_RECLAIM / ker::mod::mm::paging::PAGE_SIZE) + ((TO_RECLAIM % ker::mod::mm::paging::PAGE_SIZE) != 0 ? 1 : 0);
        if (!procfs_request_explicit_reclaim("buffer_cache", TARGET_PAGES, TO_RECLAIM)) {
            return -EBUSY;
        }
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_packet_pool(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    constexpr size_t GROW_PREFIX_LEN = 5;
    if (count >= GROW_PREFIX_LEN && std::memcmp(s, "grow=", GROW_PREFIX_LEN) == 0) {
        uint64_t grow_count = 0;
        if (!procfs_parse_u64_trimmed(s + GROW_PREFIX_LEN, count - GROW_PREFIX_LEN, grow_count)) {
            return -EINVAL;
        }
        if (grow_count == 0 || grow_count > ker::net::PKT_POOL_DIAGNOSTIC_GROW_MAX) {
            return -ERANGE;
        }
        if (!ker::net::pkt_pool_populate_reclaimable(static_cast<size_t>(grow_count))) {
            return -ENOMEM;
        }
        return static_cast<ssize_t>(count);
    }

    uint64_t target_capacity = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        target_capacity = 0;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, target_capacity)) {
        return -EINVAL;
    }

    if (target_capacity > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    auto const REQUESTED_CAPACITY = static_cast<size_t>(target_capacity);
    auto const BEFORE = ker::net::pkt_pool_snapshot();
    size_t const TARGET_CAPACITY = std::max(REQUESTED_CAPACITY, BEFORE.baseline_capacity);
    if (BEFORE.capacity > TARGET_CAPACITY) {
        size_t const BUFFERS = BEFORE.capacity - TARGET_CAPACITY;
        uint64_t const CHUNKS = (BUFFERS + ker::net::PKT_POOL_GROW_CHUNK - 1) / ker::net::PKT_POOL_GROW_CHUNK;
        uint64_t const TARGET_PAGES = ker::mod::mm::reclaim::saturating_mul(
            CHUNKS, static_cast<uint64_t>(ker::net::PKT_POOL_GROW_CHUNK * BEFORE.physical_pages_per_buffer));
        if (!procfs_request_explicit_reclaim("packet_pool", TARGET_PAGES, CHUNKS)) {
            return -EBUSY;
        }
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_xfs_inode(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t max_inodes = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        max_inodes = SIZE_MAX;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, max_inodes)) {
        return -EINVAL;
    }
    if (max_inodes > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    ker::vfs::xfs::XfsInodeCacheStats before{};
    ker::vfs::xfs::xfs_inode_cache_stats(before);
    uint64_t const BUDGET = std::min(max_inodes, before.idle_inodes);
    if (BUDGET != 0 && !procfs_request_explicit_reclaim("xfs_inode", BUDGET, BUDGET)) {
        return -EBUSY;
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_file_mmap_cache(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t max_pages = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all") || procfs_command_equals(s, count, "reset")) {
        max_pages = SIZE_MAX;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, max_pages)) {
        return -EINVAL;
    }
    if (max_pages > static_cast<uint64_t>(SIZE_MAX)) {
        return -EOVERFLOW;
    }
    auto const BEFORE = ker::syscall::vmem::file_mmap_cache_stats();
    uint64_t const BUDGET = std::min(max_pages, BEFORE.pages);
    if (BUDGET != 0 && !procfs_request_explicit_reclaim("file_mmap_cache", BUDGET, BUDGET)) {
        return -EBUSY;
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write_memacc_reclaim_anonymous_swap(const char* s, size_t count) -> ssize_t {
    if (count == 0) {
        return 0;
    }

    uint64_t max_pages = 0;
    if (procfs_command_equals(s, count, "drop") || procfs_command_equals(s, count, "all")) {
        max_pages = UINT64_MAX;
    } else if (procfs_command_equals(s, count, "status")) {
        return static_cast<ssize_t>(count);
    } else if (!procfs_parse_u64_trimmed(s, count, max_pages) || max_pages == 0) {
        return -EINVAL;
    }
    if (!procfs_request_explicit_reclaim("anonymous_swap", max_pages, max_pages)) {
        return -EBUSY;
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write_with_credentials(const ker::mod::security::Snapshot& credentials, File* f, const void* buf, size_t count) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(*pfd);
    if (VIEW != 0) return VIEW;
    int const POLICY = ker::mod::security::authorize_diagnostic(credentials, diagnostic_access(pfd->node.type), true);
    if (POLICY != 0) return POLICY;
    ker::mod::sys::MutexGuard content_guard(pfd->limits_content_lock);

    if (pfd->node.type == ProcNodeType::WKI_CHAOS_WORKLOAD_FILE) {
        if (count == 0) {
            return 0;
        }
        int const RESULT = ker::net::wki::wki_chaos_workload_configure(static_cast<const char*>(buf), count);
        if (RESULT < 0) {
            return RESULT;
        }
        delete[] pfd->content;
        pfd->content = nullptr;
        pfd->content_len = 0;
        return static_cast<ssize_t>(count);
    }
    if (pfd->node.type == ProcNodeType::WKI_CHAOS_FILE) {
        if (count == 0) {
            return 0;
        }
        const char* command = static_cast<const char*>(buf);
        bool const ALWAYS_ALLOWED =
            (count >= 5 && std::memcmp(command, "clear", 5) == 0) || (count >= 7 && std::memcmp(command, "disable", 7) == 0);
        ker::net::wki::WkiChaosSnapshot snapshot{};
        if (!ALWAYS_ALLOWED && (!ker::net::wki::wki_chaos_snapshot(&snapshot) || !snapshot.runtime_control_allowed)) {
            return -EPERM;
        }
        int const RESULT = ker::net::wki::wki_chaos_configure(command, count);
        if (RESULT != ker::net::wki::WKI_OK) {
            if (RESULT == ker::net::wki::WKI_ERR_BUSY) {
                return -EBUSY;
            }
            if (RESULT == ker::net::wki::WKI_ERR_NOT_FOUND) {
                return -ENOENT;
            }
            if (RESULT == ker::net::wki::WKI_ERR_NO_MEM) {
                return -ENOSPC;
            }
            return -EINVAL;
        }
        delete[] pfd->content;
        pfd->content = nullptr;
        pfd->content_len = 0;
        return static_cast<ssize_t>(count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE || pfd->node.type == ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE) {
        return procfs_write_memacc_track(pfd->node.type, static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE) {
        return procfs_write_memacc_reclaim_buffer_cache(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE) {
        return procfs_write_memacc_reclaim_packet_pool(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE) {
        return procfs_write_memacc_reclaim_xfs_inode(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE) {
        return procfs_write_memacc_reclaim_file_mmap_cache(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type == ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE) {
        return procfs_write_memacc_reclaim_anonymous_swap(static_cast<const char*>(buf), count);
    }
    if (pfd->node.type != ProcNodeType::KPERFCTL_FILE) {
        return -EPERM;
    }
    if (count == 0) {
        return 0;
    }

    // Accept "enable", "enable <filter>", "mask <filter>", or "disable"
    const char* s = static_cast<const char*>(buf);
    if (count >= 6 && memcmp(s, "enable", 6) == 0) {
        uint16_t mask = ker::mod::perf::PERF_MASK_ALL;
        // Check for optional filter: "enable switch,wake,container"
        if (count > 7 && s[6] == ' ') {
            uint16_t const MASK = ker::mod::perf::parse_event_mask(s + 7, count - 7);
            if (MASK == 0) {
                return -EINVAL;
            }
            mask = MASK;
        }
        // Reset ring buffers on fresh enable so report only sees the new session
        ker::vfs::vfs_reset_local_pipe_perf_counters();
        ker::mod::perf::start_recording(mask);
    } else if (count >= 4 && memcmp(s, "mask", 4) == 0) {
        // Change mask without resetting rings or toggling enable: "mask switch,container"
        if (count > 5 && s[4] == ' ') {
            uint16_t const MASK = ker::mod::perf::parse_event_mask(s + 5, count - 5);
            if (MASK == 0) {
                return -EINVAL;
            }
            ker::mod::perf::set_event_mask(MASK);
        } else {
            return -EINVAL;
        }
    } else if (count >= 7 && memcmp(s, "disable", 7) == 0) {
        ker::mod::perf::disable();
    } else {
        return -EINVAL;
    }
    return static_cast<ssize_t>(count);
}

auto procfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    return procfs_write_with_credentials(current_procfs_credentials(), f, buf, count);
}

auto procfs_close(File* f) -> int {
    if (f == nullptr) {
        return -EINVAL;
    }
    if (f->private_data != nullptr) {
        auto* pfd = static_cast<ProcFileData*>(f->private_data);
        if (pfd->content != nullptr) {
            delete[] pfd->content;
            pfd->content = nullptr;
        }
        delete pfd;
        f->private_data = nullptr;
    }
    return 0;
}

auto procfs_lseek_with_credentials(const ker::mod::security::Snapshot& CREDENTIALS, uint64_t caller_pid, File* f, off_t offset, int whence,
                                   ProcTask* caller_task = nullptr) -> off_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(*pfd);
    if (VIEW != 0) return VIEW;
    if (pfd->node.type == ProcNodeType::MOUNTS_FILE) {
        if ((caller_task == nullptr && caller_pid != 0) ||
            (caller_task != nullptr && ker::mod::sched::task::process_pid(*caller_task) != caller_pid))
            return -ESRCH;
        return procfs_lseek_mounts_as(caller_task, CREDENTIALS, f, offset, whence);
    }
    auto const ACCESS = diagnostic_access(pfd->node.type);
    int const POLICY = ker::mod::security::authorize_diagnostic(CREDENTIALS, ACCESS, false);
    if (POLICY != 0) return POLICY;
    bool const PRIVATE = private_procfs_node(pfd->node.type);
    ker::mod::security::CredentialStore::Observation before{};
    if (PRIVATE) {
        int const RESULT = authorize_private_procfs(CREDENTIALS, caller_pid, *pfd, before);
        if (RESULT != 0) return RESULT;
    }
    if (pfd->node.type == ProcNodeType::LIMITS_FILE) {
        ProcTask* process = nullptr;
        int const RESULT = authorize_procfs_limits(CREDENTIALS, caller_pid, *pfd, process);
        if (RESULT != 0) return RESULT;
    }
    ker::mod::sys::MutexGuard content_guard(pfd->limits_content_lock);
    off_t new_pos = 0;
    switch (whence) {
        case 0:
            new_pos = offset;
            break;  // SEEK_SET
        case 1:
            new_pos = f->pos + offset;
            break;  // SEEK_CUR
        case 2:
            if (PRIVATE && pfd->content != nullptr && pfd->content_credential_generation != before.generation) return -EAGAIN;
            new_pos = static_cast<off_t>(pfd->content_len) + offset;
            break;  // SEEK_END
        default:
            return -EINVAL;
    }
    if (new_pos < 0) {
        return -EINVAL;
    }
    if (PRIVATE) {
        int const VALID = revalidate_private_procfs(*pfd, before);
        if (VALID != 0) return VALID;
    }
    int const VALID_VIEW = procfs_view_policy(*pfd);
    if (VALID_VIEW != 0) return VALID_VIEW;
    f->pos = new_pos;
    return new_pos;
}

auto procfs_lseek(File* f, off_t offset, int whence) -> off_t {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return procfs_lseek_with_credentials(current_procfs_credentials(), current_procfs_pid(), f, offset, whence, task);
}

auto procfs_readlink_as_impl(File* f, char* buf, size_t bufsz, const ker::mod::security::Snapshot& CREDENTIALS, uint64_t CALLER_PID,
                             uint64_t* target_generation = nullptr, bool allow_remote_cwd = false,
                             DirectoryViewRef* directory_view = nullptr, const ProcTask* caller_task = nullptr) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(*pfd);
    if (VIEW != 0) return VIEW;
    if (!pfd->pid_view.is_host()) allow_remote_cwd = false;
    if (pfd->node.type != ProcNodeType::EXE_LINK && pfd->node.type != ProcNodeType::SELF_LINK && pfd->node.type != ProcNodeType::CWD_LINK &&
        pfd->node.type != ProcNodeType::ROOT_LINK && pfd->node.type != ProcNodeType::FD_LINK) {
        return -EINVAL;
    }

    if (pfd->node.type == ProcNodeType::SELF_LINK) {
        if (CALLER_PID == 0) return -ESRCH;
        std::array<char, 24> tmp{};
        uint64_t reader_pid = CALLER_PID;
        if (!pfd->pid_view.is_host()) {
            if (caller_task == nullptr || ker::mod::sched::task::process_pid(*caller_task) != CALLER_PID) return -ESRCH;
            if (!task_in_pid_view(*caller_task, pfd->pid_view)) return -EXDEV;
            reader_pid = ker::mod::sched::task::namespace_process_pid(*caller_task);
            if (reader_pid == 0) return -ESRCH;
        }
        int_to_str(reader_pid, tmp.data(), tmp.size());
        std::array<char, 64> link{};
        size_t off = 0;
        // Private self is relative to this exact procfs mount, including a
        // mount below a narrower directory view. Host spelling is unchanged.
        if (pfd->pid_view.is_host()) {
            memcpy(link.data(), "/proc/", 6);
            off = 6;
        }
        for (const char* p = tmp.data(); (*p != 0) && off < link.size() - 1; ++p, ++off) {
            link[off] = *p;
        }
        link[off] = '\0';
        size_t len = off;
        len = std::min(len, bufsz);
        int const VALID_VIEW = procfs_view_policy(*pfd);
        if (VALID_VIEW != 0) return VALID_VIEW;
        if (target_generation != nullptr) *target_generation = 0;
        memcpy(buf, link.data(), len);
        return static_cast<ssize_t>(len);
    }

    ker::mod::security::CredentialStore::Observation before{};
    int const POLICY = authorize_private_procfs(CREDENTIALS, CALLER_PID, *pfd, before);
    if (POLICY != 0) return POLICY;
    auto* task = pfd->target;
    auto const* subject_credentials = !pfd->pid_view.is_host() || allow_remote_cwd ? &CREDENTIALS : nullptr;
    auto const RECOVERY = allow_remote_cwd ? DirectoryRecoveryMode::ALLOW_REMOTE : DirectoryRecoveryMode::LOCAL_ONLY;
    DirectoryViewRef candidate_view;
    std::array<char, 512> staged{};
    ssize_t result = 0;
    if (pfd->node.type == ProcNodeType::CWD_LINK) {
        size_t length = 0;
        int const CWD = ker::vfs::vfs_getcwd_for_task(task, staged.data(), staged.size(), &length, subject_credentials,
                                                      directory_view != nullptr ? &candidate_view : nullptr, RECOVERY);
        result = CWD == 0 ? static_cast<ssize_t>(length) : CWD;
    } else if (pfd->node.type == ProcNodeType::ROOT_LINK) {
        size_t length = 0;
        int const ROOT = ker::vfs::vfs_getroot_for_task(task, staged.data(), staged.size(), &length, subject_credentials,
                                                        directory_view != nullptr ? &candidate_view : nullptr, RECOVERY);
        result = ROOT == 0 ? static_cast<ssize_t>(length) : ROOT;
    } else if (pfd->node.type == ProcNodeType::EXE_LINK) {
        uint64_t const IRQF = task->path_metadata_lock.lock_irqsave();
        result = procfs_copy_link_target(task->exe_path.data(), staged.data(), staged.size());
        task->path_metadata_lock.unlock_irqrestore(IRQF);
    } else {
        int fd_result = 0;
        auto* file = authorized_task_fd_retain(CREDENTIALS, CALLER_PID, *task, pfd->node.fd, fd_result);
        if (file == nullptr) return fd_result;
        bool directory_text = false;
        if (directory_view != nullptr && file->is_directory) {
            result = ker::vfs::vfs_directory_view_from_file_for_task(*task, CREDENTIALS, file, candidate_view);
        } else if (file->is_directory) {
            size_t length = 0;
            result = ker::vfs::vfs_directory_path_for_task(*task, file, staged.data(), staged.size(), &length,
                                                           allow_remote_cwd ? &CREDENTIALS : nullptr);
            directory_text = result != ker::vfs::DIRECTORY_PATH_LEGACY;
            if (!directory_text)
                result = 0;
            else if (result == 0)
                result = static_cast<ssize_t>(length);
        }
        // Failed owned-root capture cannot fall back to a saved pathname.
        if (result == 0 && !candidate_view && !directory_text) {
            ProcTask::PathBuffer root{};
            int const COPY = copy_proc_filesystem_root(*task, root);
            if (COPY != 0) {
                ker::vfs::vfs_put_file(file);
                return COPY;
            }
            std::array<char, 512> path{};
            uint64_t const PATH_IRQF = file->path_metadata_lock.lock_irqsave();
            bool const HAS_PATH = file->vfs_path != nullptr && file->vfs_path[0] != '\0';
            bool too_long = false;
            if (HAS_PATH) {
                size_t length = 0;
                while (length < path.size() && file->vfs_path[length] != '\0') ++length;
                too_long = length == path.size();
                if (!too_long) std::memcpy(path.data(), file->vfs_path, length + 1);
            }
            file->path_metadata_lock.unlock_irqrestore(PATH_IRQF);
            if (too_long)
                result = -ENAMETOOLONG;
            else if (HAS_PATH)
                result = procfs_copy_task_visible_path(root.data(), path.data(), staged.data(), staged.size());
            else {
                if (ker::vfs::vfs_is_pipe_file(file)) {
                    std::snprintf(staged.data(), staged.size(), "pipe:[%llu]", static_cast<unsigned long long>(pfd->node.fd));
                } else if (ker::vfs::vfs_is_socket_file(file)) {
                    std::snprintf(staged.data(), staged.size(), "socket:[%llu]", static_cast<unsigned long long>(pfd->node.fd));
                } else if (ker::vfs::vfs_is_epoll_file(file)) {
                    std::snprintf(staged.data(), staged.size(), "anon_inode:[eventpoll]");
                } else {
                    std::snprintf(staged.data(), staged.size(), "anon_inode:[%llu]", static_cast<unsigned long long>(pfd->node.fd));
                }
                result = static_cast<ssize_t>(std::strlen(staged.data()));
            }
        }
        ker::vfs::vfs_put_file(file);
    }
    int const VALID = revalidate_private_procfs(*pfd, before);
    if (VALID != 0) return VALID;
    if (result < 0) return result;
    if (directory_view != nullptr) *directory_view = std::move(candidate_view);
    size_t const LENGTH = std::min(static_cast<size_t>(result), bufsz);
    if (target_generation != nullptr) *target_generation = before.generation;
    std::memcpy(buf, staged.data(), LENGTH);
    return static_cast<ssize_t>(LENGTH);
}

auto procfs_readlink(File* f, char* buf, size_t bufsz) -> ssize_t {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return procfs_readlink_as_impl(f, buf, bufsz, current_procfs_credentials(), current_procfs_pid(), nullptr, false, nullptr, task);
}

FileOperations procfs_fops_instance = {
    .vfs_open = nullptr,
    .vfs_close = procfs_close,
    .vfs_read = procfs_read,
    .vfs_write = procfs_write,
    .vfs_lseek = procfs_lseek,
    .vfs_isatty = nullptr,
    .vfs_readdir = procfs_readdir,
    .vfs_readlink = procfs_readlink,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

}  // namespace

ProcFileData::~ProcFileData() {
    if (target != nullptr) target->release();
}

auto get_procfs_fops() -> FileOperations* { return &procfs_fops_instance; }

auto procfs_read_mounts_as(ker::mod::sched::task::Task* task, const ker::mod::security::Snapshot& credentials, File* file, void* buffer,
                           size_t count, size_t offset) -> ssize_t {
    if (file == nullptr || file->private_data == nullptr) return -EINVAL;
    auto const& data = *static_cast<ProcFileData*>(file->private_data);
    if (data.node.type != ProcNodeType::MOUNTS_FILE) return -EINVAL;
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    int const POLICY = ker::mod::security::authorize_diagnostic(credentials, diagnostic_access(data.node.type), false);
    if (POLICY != 0) return POLICY;
    if (buffer == nullptr && count != 0) return -EFAULT;
    // No persistent content or directory ownership: another reader/root must
    // never inherit this descriptor's first caller's cached mount names.
    std::array<char, 4096> staged{};
    ssize_t const LENGTH = generate_mounts_for_task(task, staged.data(), staged.size());
    if (LENGTH < 0) return LENGTH;
    int const VALID_VIEW = procfs_view_policy(data);
    if (VALID_VIEW != 0) return VALID_VIEW;
    if (offset >= static_cast<size_t>(LENGTH)) return 0;
    count = std::min(count, static_cast<size_t>(LENGTH) - offset);
    if (count != 0) std::memcpy(buffer, staged.data() + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_lseek_mounts_as(ker::mod::sched::task::Task* task, const ker::mod::security::Snapshot& credentials, File* file, off_t offset,
                            int whence) -> off_t {
    if (file == nullptr || file->private_data == nullptr) return -EINVAL;
    auto& data = *static_cast<ProcFileData*>(file->private_data);
    if (data.node.type != ProcNodeType::MOUNTS_FILE) return -EINVAL;
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    int const POLICY = ker::mod::security::authorize_diagnostic(credentials, diagnostic_access(data.node.type), false);
    if (POLICY != 0) return POLICY;
    if (whence < 0 || whence > 2) return -EINVAL;
    ssize_t length = 0;
    if (whence == 2) {
        std::array<char, 4096> staged{};
        length = generate_mounts_for_task(task, staged.data(), staged.size());
        if (length < 0) return length;
    }
    // The generator has already retired root ownership before cursor locking.
    ker::mod::sys::MutexGuard content_guard(data.limits_content_lock);
    off_t const POSITION = (whence == 0 ? 0 : whence == 1 ? file->pos : static_cast<off_t>(length)) + offset;
    if (POSITION < 0) return -EINVAL;
    int const VALID_VIEW = procfs_view_policy(data);
    if (VALID_VIEW != 0) return VALID_VIEW;
    file->pos = POSITION;
    return POSITION;
}

auto procfs_readlink_as(File* file, char* buffer, size_t capacity, const ker::mod::security::Snapshot& credentials, uint64_t caller_pid,
                        uint64_t* target_generation, bool allow_remote_cwd, const ProcTask* caller_task) -> ssize_t {
    return procfs_readlink_as_impl(file, buffer, capacity, credentials, caller_pid, target_generation, allow_remote_cwd, nullptr,
                                   caller_task);
}

auto procfs_readlink_view_as(File* file, char* buffer, size_t capacity, const ker::mod::security::Snapshot& credentials,
                             uint64_t caller_pid, DirectoryViewRef& directory_view, uint64_t* target_generation,
                             const ProcTask* caller_task) -> ssize_t {
    DirectoryViewRef candidate;
    ssize_t const RESULT =
        procfs_readlink_as_impl(file, buffer, capacity, credentials, caller_pid, target_generation, true, &candidate, caller_task);
    if (RESULT >= 0) directory_view = std::move(candidate);
    return RESULT;
}

auto procfs_revalidate_readlink_result(File* file, uint64_t target_generation) -> int {
    if (file == nullptr || file->private_data == nullptr) return -EINVAL;
    auto const& data = *static_cast<const ProcFileData*>(file->private_data);
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    if (data.node.type == ProcNodeType::SELF_LINK) return target_generation == 0 ? 0 : -EAGAIN;
    if (data.node.type != ProcNodeType::EXE_LINK && data.node.type != ProcNodeType::CWD_LINK && data.node.type != ProcNodeType::ROOT_LINK &&
        data.node.type != ProcNodeType::FD_LINK)
        return -EINVAL;
    ker::mod::security::CredentialStore::Observation before{};
    before.generation = target_generation;
    return revalidate_private_procfs(data, before);
}

namespace {

auto task_start_epoch_ns(const ker::mod::sched::task::Task* task) -> uint64_t {
    if (task == nullptr || task->start_time_us == 0) {
        return 0;
    }

    uint64_t const NOW_US = ker::mod::time::get_us();
    uint64_t const NOW_EPOCH_NS = ker::mod::time::get_epoch_ns();
    if (task->start_time_us >= NOW_US) {
        return NOW_EPOCH_NS;
    }

    uint64_t const AGE_NS = (NOW_US - task->start_time_us) * NS_PER_US;
    return (NOW_EPOCH_NS > AGE_NS) ? (NOW_EPOCH_NS - AGE_NS) : NOW_EPOCH_NS;
}

auto procfs_node_creation_epoch_ns(const ker::mod::sched::task::Task* owner_task) -> uint64_t {
    uint64_t const TASK_EPOCH_NS = task_start_epoch_ns(owner_task);
    if (TASK_EPOCH_NS != 0) {
        return TASK_EPOCH_NS;
    }

    if (g_procfs_creation_epoch_ns != 0) {
        return g_procfs_creation_epoch_ns;
    }

    return ker::mod::time::get_epoch_ns();
}

void set_stat_timestamps(Stat* statbuf, uint64_t epoch_ns) {
    if (epoch_ns == 0) {
        epoch_ns = ker::mod::time::get_epoch_ns();
    }

    Timespec const TS{
        .tv_sec = static_cast<int64_t>(epoch_ns / NS_PER_SEC),
        .tv_nsec = static_cast<int64_t>(epoch_ns % NS_PER_SEC),
    };
    statbuf->st_atim = TS;
    statbuf->st_mtim = TS;
    statbuf->st_ctim = TS;
}

}  // namespace

auto procfs_fill_stat(File* f, Stat* statbuf, dev_t dev_id, uint64_t* target_generation) -> int {
    if (f == nullptr || statbuf == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }

    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    int const VIEW = procfs_view_policy(*pfd);
    if (VIEW != 0) return VIEW;
    Stat candidate{};
    auto* output = &candidate;
    auto* owner_task = pfd->target;
    ker::mod::security::CredentialStore::Observation owner{};
    owner.credentials = ker::mod::security::Snapshot::bootstrap_root();
    if (owner_task != nullptr) owner = owner_task->credential_observation();

    std::memset(output, 0, sizeof(Stat));
    output->st_dev = dev_id;
    output->st_ino = 1;
    output->st_nlink = 1;
    auto const& OWNER_CREDENTIALS = owner.credentials;
    output->st_uid = OWNER_CREDENTIALS.user().real;
    output->st_gid = OWNER_CREDENTIALS.group().real;
    output->st_rdev = 0;
    output->st_size = 0;
    output->st_blksize = 4096;
    output->st_blocks = 0;

    if (f->is_directory) {
        output->st_mode = S_IFDIR | 0555;
    } else if (pfd->node.type == ProcNodeType::EXE_LINK || pfd->node.type == ProcNodeType::SELF_LINK ||
               pfd->node.type == ProcNodeType::CWD_LINK || pfd->node.type == ProcNodeType::ROOT_LINK ||
               pfd->node.type == ProcNodeType::FD_LINK) {
        output->st_mode = S_IFLNK | 0777;
    } else if (pfd->node.type == ProcNodeType::KPERFCTL_FILE || pfd->node.type == ProcNodeType::WKI_CHAOS_FILE ||
               pfd->node.type == ProcNodeType::WKI_CHAOS_WORKLOAD_FILE || pfd->node.type == ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE ||
               pfd->node.type == ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE) {
        output->st_mode = S_IFREG | 0644;
    } else {
        output->st_mode = S_IFREG | 0444;
    }

    auto const ACCESS = diagnostic_access(pfd->node.type);
    if (ACCESS == DiagnosticAccess::KERNEL_READ) output->st_mode = S_IFREG | 0400;
    if (ACCESS == DiagnosticAccess::KERNEL_CONTROL || ACCESS == DiagnosticAccess::WKI_CONTROL) output->st_mode = S_IFREG | 0600;

    set_stat_timestamps(output, procfs_node_creation_epoch_ns(owner_task));
    int const VALID_VIEW = procfs_view_policy(*pfd);
    if (VALID_VIEW != 0) return VALID_VIEW;
    *statbuf = candidate;
    if (target_generation != nullptr) *target_generation = owner.generation;
    return 0;
}

auto procfs_revalidate_lookup_result(const File* file) -> int {
    if (file == nullptr || file->fs_type != FSType::PROCFS || file->private_data == nullptr) return -EBADF;
    return procfs_view_policy(*static_cast<const ProcFileData*>(file->private_data));
}

auto procfs_revalidate_metadata_result(const File* file, uint64_t target_generation) -> int {
    if (file == nullptr || file->private_data == nullptr) return -EINVAL;
    auto const& data = *static_cast<const ProcFileData*>(file->private_data);
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    auto const* target = data.target;
    // A retained metadata object survives target exit. Unlike private content,
    // it does not require an ACTIVE task or authorize access to task resources.
    uint64_t const GENERATION = target != nullptr ? target->credential_observation().generation : 0;
    return GENERATION == target_generation ? 0 : -EAGAIN;
}

namespace {
struct ProcFdPath {
    uint64_t pid{};
    uint64_t group_pid{};
    uint64_t fd{};
    bool self{};
    bool thread_alias{};
};

auto parse_proc_fd_path(const char* path, ProcFdPath& parsed) -> bool {
    if (path == nullptr) return false;
    while (*path == '/') ++path;
    auto parse_component = [](const char*& text, uint64_t& value) -> bool {
        const char* slash = text;
        while (*slash != '\0' && *slash != '/') ++slash;
        if (*slash != '/') return false;
        size_t const LENGTH = static_cast<size_t>(slash - text);
        std::array<char, 24> component{};
        if (LENGTH == 0 || LENGTH >= component.size()) return false;
        std::memcpy(component.data(), text, LENGTH);
        int64_t const ID = parse_pid(component.data());
        if (ID < 0) return false;
        value = static_cast<uint64_t>(ID);
        text = slash + 1;
        return true;
    };
    if (std::strncmp(path, "self/", 5) == 0) {
        parsed.self = true;
        path += 5;
    } else if (!parse_component(path, parsed.pid)) {
        return false;
    }
    if (std::strncmp(path, "task/", 5) == 0) {
        parsed.thread_alias = true;
        parsed.group_pid = parsed.pid;
        path += 5;
        if (!parse_component(path, parsed.pid)) return false;
    }
    return std::strncmp(path, "fd/", 3) == 0 && parse_fd_component(path + 3, parsed.fd);
}
}  // namespace

auto procfs_path_is_fd_link(const char* path) -> bool {
    ProcFdPath parsed{};
    return parse_proc_fd_path(path, parsed);
}

auto procfs_open_fd_link_path(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const char* path, int* result_out,
                              ProcTask* caller_task) -> File* {
    if (result_out == nullptr) return nullptr;
    *result_out = 0;
    ProcFdPath parsed{};
    if (!parse_proc_fd_path(path, parsed)) return nullptr;
    if (caller_task != nullptr) {
        auto const* view = caller_task->pid_namespace();
        if (view == nullptr || ker::mod::sched::task::process_pid(*caller_task) != caller_pid) {
            *result_out = -ESRCH;
            return nullptr;
        }
        if (!view->is_host()) {
            auto release = [](File* file) { ker::vfs::vfs_put_file(file); };
            std::unique_ptr<File, decltype(release)> origin(procfs_lookup_path_as(path, caller_pid, 0, caller_task, result_out), release);
            if (!origin) return nullptr;
            origin->fops = get_procfs_fops();
            return procfs_open_fd_lookup_as(credentials, caller_pid, origin.get(), result_out, nullptr);
        }
    }
    if (parsed.self) {
        if (caller_pid == 0) {
            *result_out = -ESRCH;
            return nullptr;
        }
        if (parsed.thread_alias)
            parsed.group_pid = caller_pid;
        else
            parsed.pid = caller_pid;
    }
    auto* task = ker::mod::sched::find_task_by_pid_safe(parsed.pid);
    if (task == nullptr) {
        *result_out = -ENOENT;
        return nullptr;
    }
    if (parsed.thread_alias && !ker::mod::sched::task::same_thread_group(*task, parsed.group_pid)) {
        task->release();
        *result_out = -ENOENT;
        return nullptr;
    }
    auto* file = authorized_task_fd_retain(credentials, caller_pid, *task, parsed.fd, *result_out);
    task->release();
    return file;
}

// --- Open a procfs path ---

auto procfs_lookup_is_symlink(const File* file) -> bool {
    if (file == nullptr || file->private_data == nullptr) return false;
    auto const TYPE = static_cast<const ProcFileData*>(file->private_data)->node.type;
    return TYPE == ProcNodeType::SELF_LINK || TYPE == ProcNodeType::EXE_LINK || TYPE == ProcNodeType::CWD_LINK ||
           TYPE == ProcNodeType::ROOT_LINK || TYPE == ProcNodeType::FD_LINK;
}

namespace {
auto authorize_procfs_open_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* file, int flags,
                              bool require_directory, uint64_t* target_generation) -> int {
    if ((flags & 3) == 3 || (flags & (O_CREAT | O_DIRECTORY)) == (O_CREAT | O_DIRECTORY)) return -EINVAL;
    if (file == nullptr || file->private_data == nullptr) return -ENOENT;
    if ((flags & O_NOFOLLOW) != 0 && procfs_lookup_is_symlink(file)) return -ELOOP;
    auto const* data = static_cast<const ProcFileData*>(file->private_data);
    int const VIEW = procfs_view_policy(*data);
    if (VIEW != 0) return VIEW;
    bool const REQUIRE_DIRECTORY = require_directory || (flags & O_DIRECTORY) != 0;
    int policy = 0;
    if ((flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL))
        policy = -EEXIST;
    else if (REQUIRE_DIRECTORY && !file->is_directory)
        policy = -ENOTDIR;
    else if (file->is_directory && ((flags & 3) != 0 || (flags & O_TRUNC) != 0))
        policy = -EISDIR;
    else {
        if ((flags & 3) != 1) policy = ker::mod::security::authorize_diagnostic(credentials, diagnostic_access(data->node.type), false);
        if (policy == 0 && ((flags & 3) != 0 || (flags & O_TRUNC) != 0))
            policy = ker::mod::security::authorize_diagnostic(credentials, diagnostic_access(data->node.type), true);
    }
    uint64_t generation = 0;
    if (policy == 0 && private_procfs_node(data->node.type)) {
        ker::mod::security::CredentialStore::Observation observation{};
        policy = authorize_private_procfs(credentials, caller_pid, *data, observation);
        generation = observation.generation;
    }
    if (policy == 0 && target_generation != nullptr) *target_generation = generation;
    return policy;
}
}  // namespace

auto procfs_revalidate_open_result(const File* file, uint64_t target_generation) -> int {
    if (file == nullptr || file->private_data == nullptr) return -EINVAL;
    auto const& data = *static_cast<const ProcFileData*>(file->private_data);
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    if (!private_procfs_node(data.node.type)) return target_generation == 0 ? 0 : -EAGAIN;
    ker::mod::security::CredentialStore::Observation before{};
    before.generation = target_generation;
    return revalidate_private_procfs(data, before);
}

auto procfs_lookup_is_fd_link(const File* file) -> bool {
    return file != nullptr && file->private_data != nullptr &&
           static_cast<const ProcFileData*>(file->private_data)->node.type == ProcNodeType::FD_LINK;
}

auto procfs_open_fd_lookup_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* retained, int* result_out,
                              uint64_t* target_generation) -> File* {
    if (result_out == nullptr) return nullptr;
    if (!procfs_lookup_is_fd_link(retained)) {
        *result_out = -EINVAL;
        return nullptr;
    }
    auto const& data = *static_cast<const ProcFileData*>(retained->private_data);
    ker::mod::security::CredentialStore::Observation before{};
    *result_out = authorize_private_procfs(credentials, caller_pid, data, before);
    if (*result_out != 0) return nullptr;
    auto* file = authorized_task_fd_retain(credentials, caller_pid, *data.target, data.node.fd, *result_out);
    if (file == nullptr) return nullptr;
    int const VALID = revalidate_private_procfs(data, before);
    if (VALID != 0) {
        ker::vfs::vfs_put_file(file);
        *result_out = VALID;
        return nullptr;
    }
    if (target_generation != nullptr) *target_generation = before.generation;
    return file;
}

auto procfs_open_lookup_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* retained, int flags,
                           bool require_directory, int* result_out, uint64_t* target_generation) -> File* {
    auto fail = [result_out](int error) -> File* {
        if (result_out != nullptr) *result_out = error;
        return nullptr;
    };
    uint64_t generation = 0;
    int const POLICY = authorize_procfs_open_as(credentials, caller_pid, retained, flags, require_directory, &generation);
    if (POLICY != 0) return fail(POLICY);
    auto const& source = *static_cast<const ProcFileData*>(retained->private_data);
    auto* data = new ProcFileData{};
    if (data == nullptr) return fail(-ENOMEM);
    data->node = source.node;
    int const RETAIN = source.pid_view.retain(data->pid_view);
    if (RETAIN != 0) {
        delete data;
        return fail(RETAIN);
    }
    if (source.target != nullptr) {
        if (!source.target->try_acquire()) {
            delete data;
            return fail(-ESRCH);
        }
        data->target = source.target;
    }
    auto* file = new File{};
    if (file == nullptr) {
        delete data;
        return fail(-ENOMEM);
    }
    file->private_data = data;
    file->fd = -1;
    file->is_directory = retained->is_directory;
    file->fs_type = FSType::PROCFS;
    file->refcount = 1;
    file->open_flags = flags;
    file->fops = get_procfs_fops();
    int const VALID = procfs_revalidate_open_result(file, generation);
    if (VALID != 0) {
        procfs_close(file);
        delete file;
        return fail(VALID);
    }
    if (target_generation != nullptr) *target_generation = generation;
    if (result_out != nullptr) *result_out = 0;
    return file;
}

auto procfs_open_path(const ker::mod::security::Snapshot& credentials, const char* path, int flags, int mode, int* result_out,
                      bool require_directory) -> File* {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return procfs_open_path_as(credentials, current_procfs_pid(), path, flags, mode, result_out, require_directory, task);
}

auto procfs_open_path_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const char* path, int flags, int mode,
                         int* result_out, bool require_directory, ProcTask* caller_task) -> File* {
    (void)mode;
    auto fail = [result_out](int error) -> File* {
        if (result_out != nullptr) *result_out = error;
        return nullptr;
    };
    if ((flags & 3) == 3 || (flags & (O_CREAT | O_DIRECTORY)) == (O_CREAT | O_DIRECTORY)) return fail(-EINVAL);
    int lookup_result = -ENOENT;
    auto* file = procfs_lookup_path_as(path, caller_pid, flags, caller_task, &lookup_result);
    if (file == nullptr) return fail(lookup_result);
    uint64_t generation = 0;
    int const POLICY = authorize_procfs_open_as(credentials, caller_pid, file, flags, require_directory, &generation);
    int const VALID = POLICY == 0 ? procfs_revalidate_open_result(file, generation) : POLICY;
    if (VALID != 0) {
        procfs_close(file);
        delete file;
        return fail(VALID);
    }
    if (result_out != nullptr) *result_out = 0;
    return file;
}

namespace {
auto private_directory_policy(const File* file) -> int {
    if (file == nullptr || file->fs_type != FSType::PROCFS || file->private_data == nullptr) return -EBADF;
    if (!file->is_directory) return -ENOTDIR;
    auto const& data = *static_cast<const ProcFileData*>(file->private_data);
    if (data.pid_view.is_host()) return -ENOTSUP;
    int const VIEW = procfs_view_policy(data);
    if (VIEW != 0) return VIEW;
    switch (data.node.type) {
        case ProcNodeType::ROOT_DIR:
            return data.target == nullptr ? 0 : -EBADF;
        case ProcNodeType::PID_DIR:
        case ProcNodeType::TASK_DIR:
        case ProcNodeType::TASK_TID_DIR:
        case ProcNodeType::FD_DIR:
            return data.target != nullptr && data.node.pid == data.target->pid &&
                           data.target->state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE
                       ? 0
                       : -ESRCH;
        default:
            return -ENOTDIR;
    }
}

auto make_private_component(const pid_ns::NamespaceRef& view, ProcNodeType type, ProcTask* target, bool directory, bool thread_view,
                            uint64_t fd, int& error) -> File* {
    auto data = std::unique_ptr<ProcFileData>(new (std::nothrow) ProcFileData{});
    if (!data) {
        error = -ENOMEM;
        return nullptr;
    }
    error = view.retain(data->pid_view);
    if (error != 0) return nullptr;
    if (target != nullptr) {
        if (!target->try_acquire()) {
            error = -ESRCH;
            return nullptr;
        }
        data->target = target;
    }
    data->node = {.type = type, .pid = target != nullptr ? target->pid : 0, .fd = fd, .thread_view = thread_view};
    error = procfs_view_policy(*data);
    if (error != 0) return nullptr;
    auto file = std::unique_ptr<File>(new (std::nothrow) File{});
    if (!file) {
        error = -ENOMEM;
        return nullptr;
    }
    file->private_data = data.release();
    file->fd = -1;
    file->is_directory = directory;
    file->fs_type = FSType::PROCFS;
    file->refcount = 1;
    // This is metadata. Its caller supplies operations and authorizes the
    // eventual access just like the ordinary parser's retained result.
    error = 0;
    return file.release();
}
}  // namespace

auto procfs_directory_path(const File* directory, char* buffer, size_t capacity) -> int {
    if (buffer == nullptr || capacity == 0) return -EINVAL;
    int const POLICY = private_directory_policy(directory);
    if (POLICY != 0) return POLICY;
    auto const& data = *static_cast<const ProcFileData*>(directory->private_data);
    std::array<char, 96> staged{};
    int length = 0;
    if (data.node.type == ProcNodeType::ROOT_DIR) {
        staged[0] = '/';
        length = 1;
    } else {
        auto const TID = data.target->namespace_tid();
        auto const GROUP = ker::mod::sched::task::namespace_process_pid(*data.target);
        if (TID == 0 || GROUP == 0) return -ESRCH;
        bool const THREAD_PATH =
            data.node.type == ProcNodeType::TASK_TID_DIR || (data.node.type == ProcNodeType::FD_DIR && data.node.thread_view);
        if (THREAD_PATH) {
            length = std::snprintf(staged.data(), staged.size(), "/%llu/task/%llu%s", static_cast<unsigned long long>(GROUP),
                                   static_cast<unsigned long long>(TID), data.node.type == ProcNodeType::FD_DIR ? "/fd" : "");
        } else {
            length = std::snprintf(staged.data(), staged.size(), "/%llu%s",
                                   static_cast<unsigned long long>(data.node.type == ProcNodeType::TASK_DIR ? GROUP : TID),
                                   data.node.type == ProcNodeType::TASK_DIR ? "/task"
                                   : data.node.type == ProcNodeType::FD_DIR ? "/fd"
                                                                            : "");
        }
    }
    if (length < 0 || static_cast<size_t>(length) >= staged.size()) return -ENAMETOOLONG;
    if (static_cast<size_t>(length) + 1 > capacity) return -ERANGE;
    int const CURRENT = private_directory_policy(directory);
    if (CURRENT != 0) return CURRENT;
    std::memcpy(buffer, staged.data(), static_cast<size_t>(length) + 1);
    return 0;
}

auto procfs_lookup_component_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* directory,
                                const char* component, ProcTask* caller_task, int* result_out) -> File* {
    auto fail = [result_out](int error) -> File* {
        if (result_out != nullptr) *result_out = error;
        return nullptr;
    };
    int const POLICY = private_directory_policy(directory);
    if (POLICY != 0) return fail(POLICY);
    auto const& source = *static_cast<const ProcFileData*>(directory->private_data);
    if (caller_task == nullptr || ker::mod::sched::task::process_pid(*caller_task) != caller_pid) return fail(-ESRCH);
    auto const* scope = caller_task->pid_namespace();
    if (scope == nullptr) return fail(-ESRCH);
    if (scope->closed()) return fail(-EIDRM);
    if (*scope != source.pid_view || !task_in_pid_view(*caller_task, source.pid_view)) return fail(-EXDEV);
    if (component == nullptr) return fail(-EINVAL);
    constexpr size_t COMPONENT_LIMIT = 256;
    size_t length = 0;
    while (length < COMPONENT_LIMIT && component[length] != '\0') {
        if (component[length] == '/') return fail(-EINVAL);
        ++length;
    }
    if (length == COMPONENT_LIMIT) return fail(-ENAMETOOLONG);
    int error = -ENOENT;
    File* candidate = nullptr;
    auto select = [&](ProcNodeType type, ProcTask* target, bool is_directory, bool thread_view = false, uint64_t fd = 0) {
        candidate = make_private_component(source.pid_view, type, target, is_directory, thread_view, fd, error);
    };
    if (component[0] == '\0' || std::strcmp(component, ".") == 0) {
        select(source.node.type, source.target, true, source.node.thread_view);
    } else if (std::strcmp(component, "..") == 0) {
        // The VFS handles a procfs-root exit using its exact native cover.
        if (source.node.type == ProcNodeType::ROOT_DIR || source.node.type == ProcNodeType::PID_DIR)
            select(ProcNodeType::ROOT_DIR, nullptr, true);
        else if (source.node.type == ProcNodeType::TASK_TID_DIR)
            select(ProcNodeType::TASK_DIR, source.target->credential_owner(), true);
        else if (source.node.type == ProcNodeType::TASK_DIR)
            select(ProcNodeType::PID_DIR, source.target->credential_owner(), true);
        else
            select(source.node.thread_view ? ProcNodeType::TASK_TID_DIR : ProcNodeType::PID_DIR, source.target, true);
    } else if (source.node.type == ProcNodeType::ROOT_DIR) {
        // A root entry is a new scoped selection. All deeper directories use
        // their retained target instead of joining again through numeric text.
        candidate = procfs_lookup_path_as(component, caller_pid, 0, caller_task, &error);
    } else if (source.node.type == ProcNodeType::TASK_DIR) {
        int64_t const ID = parse_pid(component);
        if (ID > 0) {
            ProcTask* target = nullptr;
            auto* group = source.target->credential_owner();
            if (group != nullptr && static_cast<uint64_t>(ID) == group->namespace_tid())
                target = group->try_acquire() ? group : nullptr;
            else
                target = ker::mod::sched::find_task_by_pid_namespace_safe(source.pid_view, static_cast<uint64_t>(ID));
            if (target != nullptr) {
                if (target->credential_owner() == group && task_in_pid_view(*target, source.pid_view))
                    select(ProcNodeType::TASK_TID_DIR, target, true);
                target->release();
            }
        }
    } else if (source.node.type == ProcNodeType::FD_DIR) {
        uint64_t fd = 0;
        if (parse_fd_component(component, fd)) {
            ker::mod::security::CredentialStore::Observation observation{};
            error = authorize_private_procfs(credentials, caller_pid, source, observation);
            if (error == 0) {
                auto* description = task_fd_file_retain(source.target, fd);
                if (description != nullptr) {
                    // This possible last remote close stays outside publication.
                    ker::vfs::vfs_put_file(description);
                    error = revalidate_private_procfs(source, observation);
                    if (error == 0) select(ProcNodeType::FD_LINK, source.target, false, source.node.thread_view, fd);
                } else {
                    error = -ENOENT;
                }
            }
        }
    } else {
        bool const THREAD = source.node.type == ProcNodeType::TASK_TID_DIR;
        struct Child {
            const char* name;
            ProcNodeType type;
        };
        constexpr std::array<Child, 13> CHILDREN{{
            {"exe", ProcNodeType::EXE_LINK},
            {"cwd", ProcNodeType::CWD_LINK},
            {"root", ProcNodeType::ROOT_LINK},
            {"fd", ProcNodeType::FD_DIR},
            {"status", ProcNodeType::STATUS_FILE},
            {"stat", ProcNodeType::STAT_FILE},
            {"statm", ProcNodeType::STATM_FILE},
            {"cmdline", ProcNodeType::CMDLINE_FILE},
            {"maps", ProcNodeType::MAPS_FILE},
            {"images", ProcNodeType::IMAGES_FILE},
            {"limits", ProcNodeType::LIMITS_FILE},
            {"wki_launcher", ProcNodeType::WKI_LAUNCHER_FILE},
            {"wki_runner", ProcNodeType::WKI_RUNNER_FILE},
        }};
        if (!THREAD && std::strcmp(component, "task") == 0) {
            select(ProcNodeType::TASK_DIR, source.target->credential_owner(), true);
        } else if (std::strcmp(component, "wki_remote_pid") == 0) {
            select(ProcNodeType::WKI_REMOTE_PID_FILE, source.target, false);
        } else {
            for (auto const& child : CHILDREN) {
                if (std::strcmp(component, child.name) != 0) continue;
                bool const DIR = child.type == ProcNodeType::FD_DIR;
                select(child.type, source.target, DIR,
                       THREAD && (DIR || child.type == ProcNodeType::STATUS_FILE || child.type == ProcNodeType::STAT_FILE));
                break;
            }
        }
    }
    if (candidate == nullptr) return fail(error);
    int const CURRENT = private_directory_policy(directory);
    auto const& selected = *static_cast<const ProcFileData*>(candidate->private_data);
    int const FINAL = CURRENT != 0 ? CURRENT : selected.pid_view != source.pid_view ? -EXDEV : procfs_view_policy(selected);
    if (FINAL != 0) {
        procfs_close(candidate);
        delete candidate;
        return fail(FINAL);
    }
    if (result_out != nullptr) *result_out = 0;
    return candidate;
}

auto procfs_lookup_path(const char* path, int flags) -> File* {
    auto* task = ker::mod::sched::can_query_current_task() ? ker::mod::sched::get_current_task() : nullptr;
    return procfs_lookup_path_as(path, current_procfs_pid(), flags, task);
}

auto procfs_lookup_path_as(const char* path, uint64_t caller_pid, int flags, ProcTask* caller_task, int* result_out) -> File* {
    int lookup_error = -ENOENT;
    if (result_out != nullptr) *result_out = lookup_error;
    auto fail = [&](int error) -> File* {
        lookup_error = error;
        if (result_out != nullptr) *result_out = error;
        return nullptr;
    };
    if (path == nullptr) return nullptr;
    pid_ns::NamespaceRef const HOST_VIEW;
    auto const* view = caller_task != nullptr ? caller_task->pid_namespace() : &HOST_VIEW;
    if (view == nullptr || (caller_task != nullptr && ker::mod::sched::task::process_pid(*caller_task) != caller_pid)) return fail(-ESRCH);
    if (view->closed()) return fail(-EIDRM);
    bool const PRIVATE_VIEW = !view->is_host();
    uint64_t const SELF_PID = PRIVATE_VIEW ? ker::mod::sched::task::namespace_process_pid(*caller_task) : caller_pid;
    if (PRIVATE_VIEW && SELF_PID == 0) return fail(-ESRCH);

    // Skip leading /
    while (*path == '/') {
        path++;
    }

    auto release_target = [](ProcTask* task) {
        if (task != nullptr) task->release();
    };
    std::unique_ptr<ProcTask, decltype(release_target)> selected(nullptr, release_target);
    uint64_t selected_name = 0;
    auto retain_target = [&](uint64_t pid) -> ProcTask* {
        if (view->closed()) {
            fail(-EIDRM);
            return nullptr;
        }
        if (!selected || selected_name != pid) {
            auto* owner = caller_task != nullptr ? caller_task->credential_owner() : nullptr;
            auto* target =
                owner != nullptr && pid == SELF_PID && (PRIVATE_VIEW || owner->pid == caller_pid) && task_in_pid_view(*owner, *view)
                    ? (owner->try_acquire() ? owner : nullptr)
                    : ker::mod::sched::find_task_by_pid_namespace_safe(*view, pid);
            if (target == nullptr) return nullptr;
            selected.reset(target);
            selected_name = pid;
        }
        return selected->try_acquire() ? selected.get() : nullptr;
    };
    auto make_file = [&](ProcNodeType type, uint64_t pid, bool is_dir, bool thread_view = false, uint64_t fd = 0) -> File* {
        if (view->closed()) return fail(-EIDRM);
        if (PRIVATE_VIEW && !private_view_node(type)) return fail(-ENOTSUP);
        auto* pfd = new ProcFileData{};
        if (pfd == nullptr) return fail(-ENOMEM);
        int const RETAIN = view->retain(pfd->pid_view);
        if (RETAIN != 0) {
            delete pfd;
            return fail(RETAIN);
        }
        pfd->node.type = type;
        pfd->node.pid = pid;
        pfd->node.fd = fd;
        pfd->node.thread_view = thread_view;
        // Bind public ownership metadata at lookup too. SELF_LINK can represent
        // an explicit synthetic caller without a registered Task.
        if (pid != 0 || private_procfs_node(type)) {
            pfd->target = retain_target(pid);
            if (pfd->target == nullptr && (PRIVATE_VIEW || type != ProcNodeType::SELF_LINK)) {
                delete pfd;
                return fail(lookup_error);
            }
            if (pfd->target != nullptr) pfd->node.pid = pfd->target->pid;
        }
        pfd->content = nullptr;
        pfd->content_len = 0;

        auto* f = new File{};
        if (f == nullptr) {
            delete pfd;
            return fail(-ENOMEM);
        }
        f->private_data = pfd;
        f->fd = -1;
        f->pos = 0;
        f->is_directory = is_dir;
        f->fs_type = FSType::PROCFS;
        f->refcount = 1;
        f->open_flags = flags;
        f->fd_flags = 0;
        f->fops = nullptr;  // set by caller
        int const VALID = procfs_view_policy(*pfd);
        if (VALID != 0) {
            delete pfd;
            delete f;
            return fail(VALID);
        }
        if (result_out != nullptr) *result_out = 0;
        return f;
    };
    auto pid_exists = [&retain_target](uint64_t pid) -> bool {
        auto* task = retain_target(pid);
        if (task == nullptr) {
            return false;
        }
        task->release();
        return true;
    };
    auto make_fd_link = [&](uint64_t pid, const char* fd_text) -> File* {
        uint64_t fd = 0;
        if (!parse_fd_component(fd_text, fd)) return nullptr;
        auto* target = retain_target(pid);
        if (target == nullptr) return nullptr;
        auto* description = task_fd_file_retain(target, fd);
        target->release();
        if (description == nullptr) return nullptr;
        ker::vfs::vfs_put_file(description);
        return make_file(ProcNodeType::FD_LINK, pid, false, false, fd);
    };
    auto open_task_subpath = [&](uint64_t group_pid, const char* subpath) -> File* {
        std::unique_ptr<ProcTask, decltype(release_target)> group(retain_target(group_pid), release_target);
        if (!group) return nullptr;
        uint64_t const PHYSICAL_GROUP = ker::mod::sched::task::process_pid(*group);
        if (subpath == nullptr || *subpath == '\0') {
            return make_file(ProcNodeType::TASK_DIR, group_pid, true);
        }

        const char* slash = nullptr;
        for (const char* p = subpath; *p != 0; ++p) {
            if (*p == '/') {
                slash = p;
                break;
            }
        }

        std::array<char, 24> tid_str{};
        size_t const TID_LEN = (slash == nullptr) ? std::strlen(subpath) : static_cast<size_t>(slash - subpath);
        if (TID_LEN == 0 || TID_LEN >= tid_str.size()) {
            return nullptr;
        }
        memcpy(tid_str.data(), subpath, TID_LEN);
        tid_str[TID_LEN] = '\0';
        int64_t const TID = parse_pid(tid_str.data());
        if (TID < 0) {
            return nullptr;
        }

        auto const TID_VALUE = static_cast<uint64_t>(TID);
        auto* target = retain_target(TID_VALUE);
        if (target == nullptr) return nullptr;
        bool const MEMBER = ker::mod::sched::task::same_thread_group(*target, PHYSICAL_GROUP) &&
                            (!PRIVATE_VIEW || target->credential_owner() == group->credential_owner());
        target->release();
        if (!MEMBER) return nullptr;

        if (slash == nullptr) {
            return make_file(ProcNodeType::TASK_TID_DIR, TID_VALUE, true);
        }

        const char* task_sub = slash + 1;
        if (strcmp(task_sub, "exe") == 0) {
            return make_file(ProcNodeType::EXE_LINK, TID_VALUE, false);
        }
        if (strcmp(task_sub, "cwd") == 0) {
            return make_file(ProcNodeType::CWD_LINK, TID_VALUE, false);
        }
        if (strcmp(task_sub, "root") == 0) {
            return make_file(ProcNodeType::ROOT_LINK, TID_VALUE, false);
        }
        if (strcmp(task_sub, "fd") == 0) {
            return make_file(ProcNodeType::FD_DIR, TID_VALUE, true, PRIVATE_VIEW);
        }
        if (strncmp(task_sub, "fd/", 3) == 0) {
            return make_fd_link(TID_VALUE, task_sub + 3);
        }
        if (strcmp(task_sub, "limits") == 0) {
            return make_file(ProcNodeType::LIMITS_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "status") == 0) {
            return make_file(ProcNodeType::STATUS_FILE, TID_VALUE, false, true);
        }
        if (strcmp(task_sub, "stat") == 0) {
            return make_file(ProcNodeType::STAT_FILE, TID_VALUE, false, true);
        }
        if (strcmp(task_sub, "statm") == 0) {
            return make_file(ProcNodeType::STATM_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "cmdline") == 0) {
            return make_file(ProcNodeType::CMDLINE_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "maps") == 0) {
            return make_file(ProcNodeType::MAPS_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "images") == 0) {
            return make_file(ProcNodeType::IMAGES_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_launcher") == 0) {
            return make_file(ProcNodeType::WKI_LAUNCHER_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_runner") == 0) {
            return make_file(ProcNodeType::WKI_RUNNER_FILE, TID_VALUE, false);
        }
        if (strcmp(task_sub, "wki_remote_pid") == 0) {
            return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, TID_VALUE, false);
        }
        return nullptr;
    };

    // /proc (root)
    if (*path == '\0') {
        return make_file(ProcNodeType::ROOT_DIR, 0, true);
    }

    // /proc/mounts
    if (strcmp(path, "mounts") == 0) {
        return make_file(ProcNodeType::MOUNTS_FILE, 0, false);
    }

    // /proc/uptime
    if (strcmp(path, "timekeeping") == 0) {
        return make_file(ProcNodeType::TIMEKEEPING_FILE, 0, false);
    }
    if (strcmp(path, "uptime") == 0) {
        return make_file(ProcNodeType::UPTIME_FILE, 0, false);
    }

    // /proc/stat
    if (strcmp(path, "stat") == 0) {
        return make_file(ProcNodeType::CPU_STAT_FILE, 0, false);
    }

    // /proc/loadavg
    if (strcmp(path, "loadavg") == 0) {
        return make_file(ProcNodeType::LOADAVG_FILE, 0, false);
    }

    // /proc/meminfo
    if (strcmp(path, "meminfo") == 0) {
        return make_file(ProcNodeType::MEMINFO_FILE, 0, false);
    }

    // /proc/version
    if (strcmp(path, "version") == 0) {
        return make_file(ProcNodeType::VERSION_FILE, 0, false);
    }

    // /proc/kperf - drain kernel perf ring buffer as text events
    if (strcmp(path, "kperf") == 0) {
        return make_file(ProcNodeType::KPERF_FILE, 0, false);
    }

    // /proc/kperfstatus - recording session, loss and consumer counters
    if (strcmp(path, "blockq") == 0) return make_file(ProcNodeType::BLOCK_QUEUE_FILE, 0, false);

    if (strcmp(path, "kperfstatus") == 0) {
        return make_file(ProcNodeType::KPERFSTATUS_FILE, 0, false);
    }

    // /proc/kwkistat - recording-scoped WKI summary statistics
    if (strcmp(path, "kwkistat") == 0) {
        return make_file(ProcNodeType::KWKISTAT_FILE, 0, false);
    }

    // /proc/kcpustat - per-CPU aggregate scheduler statistics
    if (strcmp(path, "kcpustat") == 0) {
        return make_file(ProcNodeType::KCPUSTAT_FILE, 0, false);
    }

    // /proc/kcpustate - per-CPU scheduler current/runqueue state
    if (strcmp(path, "kcpustate") == 0) {
        return make_file(ProcNodeType::KCPUSTATE_FILE, 0, false);
    }

    // /proc/kperfctl - write "enable"/"disable" to start/stop perf recording
    if (strcmp(path, "kperfctl") == 0) {
        return make_file(ProcNodeType::KPERFCTL_FILE, 0, false);
    }

    // /proc/kcontstat - per-subsystem container statistics
    if (strcmp(path, "kcontstat") == 0) {
        return make_file(ProcNodeType::KCONTSTAT_FILE, 0, false);
    }

    // /proc/kipcstat - WKI IPC memory/queue snapshot
    if (strcmp(path, "kipcstat") == 0) {
        return make_file(ProcNodeType::KIPCSTAT_FILE, 0, false);
    }

    // /proc/wki
    if (strcmp(path, "wki") == 0) {
        return make_file(ProcNodeType::WKI_DIR, 0, true);
    }

    // /proc/wki/peers
    if (strcmp(path, "wki/peers") == 0) {
        return make_file(ProcNodeType::WKI_PEERS_FILE, 0, false);
    }

    if (strcmp(path, "wki/auth") == 0) {
        return make_file(ProcNodeType::WKI_AUTH_FILE, 0, false);
    }

    // /proc/wki/napi - complete bounded virtio queue diagnostics
    if (strcmp(path, "wki/napi") == 0) {
        return make_file(ProcNodeType::WKI_NAPI_FILE, 0, false);
    }

    // /proc/wki/netdiag
    if (strcmp(path, "wki/netdiag") == 0) {
        return make_file(ProcNodeType::WKI_NETDIAG_FILE, 0, false);
    }

    // /proc/wki/pipes
    if (strcmp(path, "wki/pipes") == 0) {
        return make_file(ProcNodeType::WKI_PIPES_FILE, 0, false);
    }

    // /proc/wki/chaos
    if (strcmp(path, "wki/chaos") == 0) {
        return make_file(ProcNodeType::WKI_CHAOS_FILE, 0, false);
    }

    // /proc/wki/chaos_workload
    if (strcmp(path, "wki/chaos_workload") == 0) {
        return make_file(ProcNodeType::WKI_CHAOS_WORKLOAD_FILE, 0, false);
    }

    // /proc/memacc structured memory accounting diagnostics
    if (strcmp(path, "memacc") == 0) {
        return make_file(ProcNodeType::MEMACC_DIR, 0, true);
    }
    if (strcmp(path, "memacc/summary") == 0) {
        return make_file(ProcNodeType::MEMACC_SUMMARY_FILE, 0, false);
    }
    if (strcmp(path, "memacc/zones") == 0) {
        return make_file(ProcNodeType::MEMACC_ZONES_FILE, 0, false);
    }
    if (strcmp(path, "memacc/procs") == 0) {
        return make_file(ProcNodeType::MEMACC_PROCS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/dead") == 0) {
        return make_file(ProcNodeType::MEMACC_DEAD_FILE, 0, false);
    }
    if (strcmp(path, "memacc/alloc_totals") == 0) {
        return make_file(ProcNodeType::MEMACC_ALLOC_TOTALS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/slabs") == 0) {
        return make_file(ProcNodeType::MEMACC_SLABS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/kmalloc_live") == 0) {
        return make_file(ProcNodeType::MEMACC_KMALLOC_LIVE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/kmalloc_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_KMALLOC_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/page_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_PAGE_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/features") == 0) {
        return make_file(ProcNodeType::MEMACC_FEATURES_FILE, 0, false);
    }
    if (strcmp(path, "memacc/track") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_DIR, 0, true);
    }
    if (strcmp(path, "memacc/reclaim") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_DIR, 0, true);
    }
    if (strcmp(path, "memacc/track/page_callers") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_PAGE_CALLERS_FILE, 0, false);
    }
    if (strcmp(path, "memacc/track/kmalloc_debug") == 0) {
        return make_file(ProcNodeType::MEMACC_TRACK_KMALLOC_DEBUG_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/buffer_cache") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_BUFFER_CACHE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/packet_pool") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_PACKET_POOL_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/xfs_inode") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_XFS_INODE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/file_mmap_cache") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/anonymous_swap") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE, 0, false);
    }
    if (strcmp(path, "memacc/reclaim/coordinator") == 0) {
        return make_file(ProcNodeType::MEMACC_RECLAIM_COORDINATOR_FILE, 0, false);
    }

    // /proc/self -> symlink to /proc/<pid>
    if (strcmp(path, "self") == 0) {
        return make_file(ProcNodeType::SELF_LINK, SELF_PID, false);
    }
    // /proc/self/exe
    if (strcmp(path, "self/exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, SELF_PID, false);
    }
    // /proc/self/cwd
    if (strcmp(path, "self/cwd") == 0) {
        return make_file(ProcNodeType::CWD_LINK, SELF_PID, false);
    }
    // /proc/self/root
    if (strcmp(path, "self/root") == 0) {
        return make_file(ProcNodeType::ROOT_LINK, SELF_PID, false);
    }
    // /proc/self/fd[/<fd>]
    if (strcmp(path, "self/fd") == 0) {
        return make_file(ProcNodeType::FD_DIR, SELF_PID, true);
    }
    if (strncmp(path, "self/fd/", 8) == 0) {
        return make_fd_link(SELF_PID, path + 8);
    }
    // /proc/self/status
    if (strcmp(path, "self/limits") == 0) {
        return make_file(ProcNodeType::LIMITS_FILE, SELF_PID, false);
    }
    if (strcmp(path, "self/status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, SELF_PID, false);
    }
    // /proc/self/stat
    if (strcmp(path, "self/stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, SELF_PID, false);
    }
    // /proc/self/statm
    if (strcmp(path, "self/statm") == 0) {
        return make_file(ProcNodeType::STATM_FILE, SELF_PID, false);
    }
    // /proc/self/cmdline
    if (strcmp(path, "self/cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, SELF_PID, false);
    }
    // /proc/self/maps
    if (strcmp(path, "self/maps") == 0) {
        return make_file(ProcNodeType::MAPS_FILE, SELF_PID, false);
    }
    // /proc/self/images
    if (strcmp(path, "self/images") == 0) {
        return make_file(ProcNodeType::IMAGES_FILE, SELF_PID, false);
    }
    // /proc/self/wki_launcher
    if (strcmp(path, "self/wki_launcher") == 0) {
        return make_file(ProcNodeType::WKI_LAUNCHER_FILE, SELF_PID, false);
    }
    // /proc/self/wki_runner
    if (strcmp(path, "self/wki_runner") == 0) {
        return make_file(ProcNodeType::WKI_RUNNER_FILE, SELF_PID, false);
    }
    // /proc/self/wki_remote_pid
    if (strcmp(path, "self/wki_remote_pid") == 0) {
        return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, SELF_PID, false);
    }
    // /proc/self/task[/<tid>/...]
    if (strcmp(path, "self/task") == 0) {
        return open_task_subpath(SELF_PID, "");
    }
    if (strncmp(path, "self/task/", 10) == 0) {
        return open_task_subpath(SELF_PID, path + 10);
    }

    // /proc/<pid>
    // Find first / in path
    const char* slash = nullptr;
    for (const char* p = path; *p != 0; ++p) {
        if (*p == '/') {
            slash = p;
            break;
        }
    }

    if (slash == nullptr) {
        // Just /proc/<pid>
        int64_t const PID = parse_pid(path);
        if (PID < 0) {
            return nullptr;
        }
        // Check if task exists
        if (!pid_exists(static_cast<uint64_t>(PID))) {
            return nullptr;
        }
        return make_file(ProcNodeType::PID_DIR, static_cast<uint64_t>(PID), true);
    }

    // /proc/<pid>/<subpath>
    std::array<char, 24> pid_str{};
    auto pid_len = static_cast<size_t>(slash - path);
    if (pid_len >= pid_str.size()) {
        return nullptr;
    }
    memcpy(pid_str.data(), path, pid_len);
    pid_str[pid_len] = '\0';
    int64_t const PID = parse_pid(pid_str.data());
    if (PID < 0) {
        return nullptr;
    }
    if (!pid_exists(static_cast<uint64_t>(PID))) {
        return nullptr;
    }

    const char* sub = slash + 1;
    auto* group_target = retain_target(static_cast<uint64_t>(PID));
    if (group_target == nullptr) return nullptr;
    uint64_t const GROUP_PID =
        PRIVATE_VIEW ? ker::mod::sched::task::namespace_process_pid(*group_target) : ker::mod::sched::task::process_pid(*group_target);
    group_target->release();
    if (strcmp(sub, "task") == 0) {
        return open_task_subpath(GROUP_PID, "");
    }
    if (strncmp(sub, "task/", 5) == 0) {
        return open_task_subpath(GROUP_PID, sub + 5);
    }
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "cwd") == 0) {
        return make_file(ProcNodeType::CWD_LINK, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "root") == 0) {
        return make_file(ProcNodeType::ROOT_LINK, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "fd") == 0) {
        return make_file(ProcNodeType::FD_DIR, static_cast<uint64_t>(PID), true);
    }
    if (strncmp(sub, "fd/", 3) == 0) {
        return make_fd_link(static_cast<uint64_t>(PID), sub + 3);
    }
    if (strcmp(sub, "limits") == 0) {
        return make_file(ProcNodeType::LIMITS_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "statm") == 0) {
        return make_file(ProcNodeType::STATM_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "maps") == 0) {
        return make_file(ProcNodeType::MAPS_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "images") == 0) {
        return make_file(ProcNodeType::IMAGES_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_launcher") == 0) {
        return make_file(ProcNodeType::WKI_LAUNCHER_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_runner") == 0) {
        return make_file(ProcNodeType::WKI_RUNNER_FILE, static_cast<uint64_t>(PID), false);
    }
    if (strcmp(sub, "wki_remote_pid") == 0) {
        return make_file(ProcNodeType::WKI_REMOTE_PID_FILE, static_cast<uint64_t>(PID), false);
    }

    return nullptr;  // Unknown procfs path
}

#ifdef WOS_SELFTEST
auto selftest_limits_authorization() -> bool {
    namespace security = ker::mod::security;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, restricted_root;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root) != 0)
        return false;
    ProcTask target{};
    target.pid = 0x7B46;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    auto& descriptors = target.resources.entries[static_cast<unsigned>(ker::abi::resource::Id::NOFILE)];
    descriptors.current = 17;
    descriptors.peak = 23;
    auto const INITIAL_REFS = target.ref_count.load();
    bool passed = true;
    {
        ProcFileData data{};
        data.target = &target;
        target.ref_count.fetch_add(1);
        // The retained object is absent from the registry. Numeric path text
        // cannot substitute another subject at read time.
        data.node = {.type = ProcNodeType::LIMITS_FILE, .pid = target.pid + 1};
        File file{};
        file.private_data = &data;
        std::array<char, 64> output{};
        output.fill('S');
        auto const SENTINEL = output;
        auto const BEFORE = target.resources.entries;
        for (auto const& caller : {user, restricted_root}) {
            passed &= procfs_read_with_credentials(caller, &file, output.data(), output.size(), 0, target.pid + 1) == -EACCES;
            passed &= output == SENTINEL && data.content == nullptr && data.content_len == 0;
            passed &= std::memcmp(BEFORE.data(), target.resources.entries.data(), sizeof(BEFORE)) == 0;
        }
        passed &= procfs_read_with_credentials(user, &file, output.data(), output.size(), 0, 0) == -EACCES;
        passed &= output == SENTINEL && data.content == nullptr;
        passed &= procfs_read_with_credentials(user, &file, output.data(), output.size(), 0, target.pid) > 0;
        passed &= std::memcmp(output.data(), "Resource", 8) == 0;
        passed &= descriptors.current == 0 && descriptors.peak == 23;
        auto* const CACHE = data.content;
        size_t const LENGTH = data.content_len;
        auto const CACHED_OUTPUT = output;
        auto const ACCOUNTING = target.resources.entries;
        output = SENTINEL;
        for (auto const& caller : {user, restricted_root}) {
            for (size_t offset : {size_t{0}, LENGTH}) {
                for (size_t count : {size_t{0}, output.size()}) {
                    passed &= procfs_read_with_credentials(caller, &file, output.data(), count, offset, target.pid + 1) == -EACCES;
                    passed &= output == SENTINEL && data.content == CACHE && data.content_len == LENGTH;
                    passed &= std::memcmp(ACCOUNTING.data(), target.resources.entries.data(), sizeof(ACCOUNTING)) == 0;
                }
            }
        }
        passed &= procfs_read_with_credentials(ROOT, &file, output.data(), output.size(), 0, target.pid + 1) > 0;
        passed &= output == CACHED_OUTPUT;
        passed &= procfs_read_with_credentials(restricted_root, &file, output.data(), output.size(), 0, target.pid) > 0;
        passed &= output == CACHED_OUTPUT;
        passed &= procfs_read_with_credentials(user, &file, output.data(), output.size(), LENGTH, target.pid) == 0;
        target.state.store(ker::mod::sched::task::TaskState::DEAD);
        output = SENTINEL;
        passed &= procfs_read_with_credentials(ROOT, &file, output.data(), output.size(), 0, target.pid + 1) == -ESRCH;
        passed &= output == SENTINEL && data.content == CACHE && data.content_len == LENGTH;
        target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        delete[] data.content;
        data.content = nullptr;
        file.private_data = nullptr;
    }
    return passed && target.ref_count.load() == INITIAL_REFS;
}

auto selftest_limits_seek_authorization() -> bool {
    namespace security = ker::mod::security;
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::TaskState;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, restricted_root;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root) != 0)
        return false;
    ProcTask process{};
    process.pid = 424250;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    auto const PROCESS_REFS = process.ref_count.load();
    bool passed = true;
    {
        ProcTask thread{};
        thread.pid = process.pid + 1;
        thread.owner_pid = process.pid;
        thread.is_thread = true;
        thread.type = ker::mod::sched::task::TaskType::PROCESS;
        if (!resource::bind_thread_owner(thread, process)) return false;
        constexpr auto NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
        process.resources.entries[NOFILE].peak = 123;
        thread.resources.entries[NOFILE].current = 456;
        auto const THREAD_RESOURCES = thread.resources.entries;
        for (auto* subject : {&process, &thread}) {
            auto const SUBJECT_REFS = subject->ref_count.load();
            {
                ProcFileData data{};
                data.target = subject;
                subject->ref_count.fetch_add(1);
                data.node = {.type = ProcNodeType::LIMITS_FILE, .pid = UINT64_MAX - 1};
                File file{};
                file.private_data = &data;
                for (bool cached : {false, true}) {
                    if (cached) {
                        std::array<char, 64> output{};
                        // Both objects are unregistered: a thread must use its
                        // retained process owner, not look up owner_pid again.
                        passed &= procfs_read_with_credentials(user, &file, output.data(), output.size(), 0, process.pid) > 0;
                        passed &= std::memcmp(output.data(), "Resource", 8) == 0;
                    }
                    auto const ACCOUNTING = process.resources.entries;
                    auto* const CACHE = data.content;
                    auto const LENGTH = data.content_len;
                    for (auto const& caller : {ROOT, user, restricted_root}) {
                        for (uint64_t caller_pid : {uint64_t{0}, process.pid, thread.pid, process.pid + 2}) {
                            bool const ALLOWED = caller == ROOT || caller_pid == process.pid;
                            for (int whence : {0, 1, 2, 3}) {
                                file.pos = 7;
                                off_t const EXPECTED = !ALLOWED      ? -EACCES
                                                       : whence == 3 ? -EINVAL
                                                       : whence == 0 ? 2
                                                       : whence == 1 ? 9
                                                                     : static_cast<off_t>(LENGTH) + 2;
                                passed &= procfs_lseek_with_credentials(caller, caller_pid, &file, 2, whence) == EXPECTED;
                                passed &= file.pos == (EXPECTED < 0 ? 7 : EXPECTED);
                                passed &= data.content == CACHE && data.content_len == LENGTH;
                                passed &= std::memcmp(ACCOUNTING.data(), process.resources.entries.data(), sizeof(ACCOUNTING)) == 0;
                            }
                        }
                    }
                    file.pos = 7;
                    passed &= procfs_lseek_with_credentials(user, process.pid, &file, -1, 0) == -EINVAL && file.pos == 7;
                    // The same retained target must be live for read and seek,
                    // including a cached descriptor passed to another caller.
                    for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
                        subject->state.store(state);
                        for (int whence : {0, 1, 2}) {
                            passed &= procfs_lseek_with_credentials(ROOT, 0, &file, 0, whence) == -ESRCH && file.pos == 7;
                        }
                        char output = 'S';
                        passed &= procfs_read_with_credentials(ROOT, &file, &output, 0, LENGTH) == -ESRCH && output == 'S';
                        passed &= data.content == CACHE && data.content_len == LENGTH;
                    }
                    subject->state.store(TaskState::ACTIVE);
                    if (subject == &thread) {
                        for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
                            process.state.store(state);
                            passed &= procfs_lseek_with_credentials(ROOT, 0, &file, 0, 2) == -ESRCH && file.pos == 7;
                            char output = 'S';
                            passed &= procfs_read_with_credentials(ROOT, &file, &output, 1, 0) == -ESRCH && output == 'S';
                            passed &= data.content == CACHE && data.content_len == LENGTH;
                        }
                        process.state.store(TaskState::ACTIVE);
                    }
                    passed &= std::memcmp(THREAD_RESOURCES.data(), thread.resources.entries.data(), sizeof(THREAD_RESOURCES)) == 0;
                }
                delete[] data.content;
                data.content = nullptr;
                file.private_data = nullptr;
            }
            passed &= subject->ref_count.load() == SUBJECT_REFS;
        }
        ProcTask unbound{};
        unbound.is_thread = true;
        unbound.owner_pid = process.pid;
        for (auto* subject : {static_cast<ProcTask*>(nullptr), &unbound}) {
            ProcFileData data{};
            data.target = subject;
            if (subject != nullptr) subject->ref_count.fetch_add(1);
            data.node.type = ProcNodeType::LIMITS_FILE;
            File file{};
            file.private_data = &data;
            file.pos = 7;
            char output = 'S';
            passed &= procfs_read_with_credentials(ROOT, &file, &output, 0, 0) == -ESRCH && output == 'S';
            passed &= procfs_lseek_with_credentials(ROOT, 0, &file, 0, 2) == -ESRCH && file.pos == 7;
            passed &= data.content == nullptr && data.content_len == 0;
            file.private_data = nullptr;
        }
    }
    return passed && process.ref_count.load() == PROCESS_REFS;
}

auto selftest_wki_identity_snapshots() -> bool {
    ProcTask target{};
    target.pid = 0x7B45;
    target.wki_submitter_hostname.fill('x');
    target.set_wki_remote_pid(123);
    target.set_wki_proxy_task_id(456);
    auto const OBSERVED = target.wki_execution_identity();
    bool passed = OBSERVED.remote_pid == 123 && OBSERVED.proxy_task_id == 456 && OBSERVED.submitter_hostname.back() == '\0' &&
                  target.wki_submitter_hostname.back() == 'x';
    target.set_wki_remote_pid(0);
    target.set_wki_proxy_task_id(0);
    passed &= OBSERVED.remote_pid == 123 && target.wki_execution_identity().remote_pid == 0;
    ProcTask::WkiExecutionIdentity identity{};
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> local{};
    std::memcpy(local.data(), "here", 5);
    passed &= task_wki_remote_pid(target.pid, identity, local) == 0;
    std::memcpy(identity.submitter_hostname.data(), "there", 6);
    passed &= task_wki_remote_pid(target.pid, identity, local) == target.pid;
    identity.remote_pid = 17;
    passed &= task_wki_remote_pid(target.pid, identity, local) == 17;
    identity.remote_pid = 0;
    std::memcpy(identity.submitter_hostname.data(), "here", 5);
    passed &= task_wki_remote_pid(target.pid, identity, local) == 0;

    std::array<char, 8> output{};
    output.fill('S');
    passed &= format_procfs_hostname("abc", output.data(), 0) == 0 && output[0] == 'S';
    passed &= format_procfs_hostname("abc", output.data(), 1) == 0 && output[0] == '\0' && output[1] == 'S';
    passed &= format_procfs_hostname("abc", output.data(), 2) == 1 && output[0] == '\n' && output[1] == '\0' && output[2] == 'S';
    passed &= format_procfs_hostname("abc", output.data(), 4) == 3 && std::strcmp(output.data(), "ab\n") == 0 && output[4] == 'S';
    passed &= format_procfs_hostname("", output.data(), output.size()) == 6 && std::strcmp(output.data(), "local\n") == 0;

    // Kernel selftests precede WKI initialization. Publish and restore only
    // the local name, never a peer/session/channel entry.
    if (ker::net::wki::g_wki.initialized) return false;
    auto& state = ker::net::wki::g_wki;
    uint64_t flags = state.peer_lock.lock_irqsave();
    auto const SAVED = state.local_hostname;
    state.local_hostname.fill('\0');
    std::memcpy(state.local_hostname.data(), "snapshot", 9);
    state.peer_lock.unlock_irqrestore(flags);
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> copied{};
    passed &= ker::net::wki::wki_peer_copy_hostname(state.my_node_id, copied);
    flags = state.peer_lock.lock_irqsave();
    state.local_hostname = SAVED;
    state.peer_lock.unlock_irqrestore(flags);
    passed &= std::strcmp(copied.data(), "snapshot") == 0;
    copied.fill('S');
    passed &= !ker::net::wki::wki_peer_copy_hostname(ker::net::wki::WKI_NODE_BROADCAST, copied) && copied[0] == 'S';

    for (auto type : {ProcNodeType::WKI_LAUNCHER_FILE, ProcNodeType::WKI_RUNNER_FILE, ProcNodeType::WKI_REMOTE_PID_FILE}) {
        ProcFileData data{};
        data.node = {.type = type, .pid = UINT64_MAX - 1, .fd = 0, .thread_view = false};
        File file{};
        file.private_data = &data;
        output.fill('S');
        passed &= procfs_read_with_credentials(ker::mod::security::Snapshot{}, &file, output.data(), output.size(), 0) == -ESRCH;
        passed &= output[0] == 'S' && data.content == nullptr && data.content_len == 0;
        file.private_data = nullptr;
    }
    return passed;
}

auto selftest_public_retained_subject() -> bool {
    namespace security = ker::mod::security;
    using ker::mod::sched::task::TaskState;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, restricted_root;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root) != 0)
        return false;
    ProcTask target{};
    target.pid = 424242;
    target.type = ker::mod::sched::task::TaskType::PROCESS;
    if (ker::mod::sched::resource::initialize_credentials(target, user) != 0) return false;
    std::memcpy(target.exe_path.data(), "/private/retained-public", sizeof("/private/retained-public"));
    std::memcpy(target.wki_submitter_hostname.data(), "owner-test", sizeof("owner-test"));
    target.set_wki_remote_pid(77);
    auto const INITIAL_REFS = target.ref_count.load();
    std::array<char, 128> runner{};
    auto const LOCAL = procfs_local_wki_hostname();
    (void)format_procfs_hostname(LOCAL.data(), runner.data(), runner.size());
    bool passed = true;
    // The child is never registered. Exercise real lookup and open, including
    // process/thread aliases, with public readers of different authority.
    for (auto const* prefix : {"self", "424242", "self/task/424242", "424242/task/424242"}) {
        for (auto const* name : {"status", "stat", "statm", "wki_launcher", "wki_runner", "wki_remote_pid"}) {
            for (auto const& caller : {ROOT, user, restricted_root}) {
                std::array<char, 96> path{};
                (void)std::snprintf(path.data(), path.size(), "%s/%s", prefix, name);
                auto* lookup = procfs_lookup_path_as(path.data(), target.pid, 0, &target);
                if (lookup == nullptr) {
                    passed = false;
                    continue;
                }
                int error = -EINVAL;
                uint64_t generation = UINT64_MAX;
                auto* file = procfs_open_lookup_as(caller, target.pid, lookup, 0, false, &error, &generation);
                procfs_close(lookup);
                delete lookup;
                if (file == nullptr) {
                    passed = false;
                    continue;
                }
                auto& data = *static_cast<ProcFileData*>(file->private_data);
                passed &= error == 0 && generation == 0 && data.target == &target;
                // Even altered path metadata must not change the retained subject.
                data.node.pid = UINT64_MAX - 1;
                std::array<char, 4096> output{};
                auto const LENGTH = procfs_read_with_credentials(caller, file, output.data(), output.size() - 1, 0, target.pid + 1);
                passed &= LENGTH > 0 && target.ref_count.load() == INITIAL_REFS + 1;
                if (std::strcmp(name, "status") == 0) {
                    passed &= std::strstr(output.data(), "Name:\tretained-public\nTgid:\t424242\nPid:\t424242\n") != nullptr;
                    passed &= std::strstr(output.data(), "\nUid:\t1000\t1000\t1000\t1000\n") != nullptr;
                    passed &= std::strstr(output.data(), "/private/") == nullptr;
                } else if (std::strcmp(name, "stat") == 0) {
                    passed &= std::strncmp(output.data(), "424242 (retained-public) ", sizeof("424242 (retained-public) ") - 1) == 0;
                } else {
                    const char* expected = "0 0 0 0 0 0 0\n";
                    if (std::strcmp(name, "wki_launcher") == 0) expected = "owner-test\n";
                    if (std::strcmp(name, "wki_runner") == 0) expected = runner.data();
                    if (std::strcmp(name, "wki_remote_pid") == 0) expected = "77\n";
                    passed &= std::strcmp(output.data(), expected) == 0;
                }
                auto const CACHED = output;
                auto* const CACHE = data.content;
                for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
                    target.state.store(state);
                    output.fill('\0');
                    passed &= procfs_read_with_credentials(caller, file, output.data(), output.size() - 1, 0) == LENGTH;
                    passed &= output == CACHED && data.content == CACHE;
                    output.fill('S');
                    passed &= procfs_read_with_credentials(caller, file, output.data(), output.size(), data.content_len) == 0;
                    passed &= procfs_read_with_credentials(caller, file, output.data(), 0, 0) == 0 && output.front() == 'S';
                }
                // An uncached dead/missing subject still fails without output,
                // even at EOF or with a zero count; no empty cache is retained.
                delete[] data.content;
                data.content = nullptr;
                data.content_len = 0;
                for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
                    target.state.store(state);
                    for (bool missing : {false, true}) {
                        data.target = missing ? nullptr : &target;
                        for (size_t count : {size_t{0}, output.size()}) {
                            for (size_t offset : {size_t{0}, output.size()}) {
                                output.fill('S');
                                auto const SENTINEL = output;
                                passed &= procfs_read_with_credentials(caller, file, output.data(), count, offset) == -ESRCH;
                                passed &= output == SENTINEL && data.content == nullptr && data.content_len == 0;
                            }
                        }
                    }
                }
                data.target = &target;
                target.state.store(TaskState::ACTIVE);
                procfs_close(file);
                delete file;
                passed &= target.ref_count.load() == INITIAL_REFS && target.usercopy_pagemap_accesses.load() == 0;
            }
        }
    }
    return passed;
}

auto selftest_public_snapshots() -> bool {
    ProcTask target{};
    std::memcpy(target.exe_path.data(), "/private/place/tool\nUid:\t0", sizeof("/private/place/tool\nUid:\t0"));
    auto comm = copy_proc_public_comm(target);
    bool passed = std::strcmp(comm.data(), "tool?Uid:?0") == 0;
    target.exe_path.fill('\0');
    target.name = "worker\rname";
    comm = copy_proc_public_comm(target);
    passed &= std::strcmp(comm.data(), "worker?name") == 0;
    target.name = nullptr;
    comm = copy_proc_public_comm(target);
    passed &= std::strcmp(comm.data(), "unknown") == 0;
    auto const ACTIVE = collect_proc_memory(target);
    passed &= ACTIVE.virtual_pages == 0 && ACTIVE.resident_pages == 0 && ACTIVE.page_table_pages == 0;
    target.state.store(ker::mod::sched::task::TaskState::DEAD);
    auto const EXITED = collect_proc_memory(target);
    passed &= EXITED.virtual_pages == 0 && EXITED.resident_pages == 0 && EXITED.page_table_pages == 0;
    passed &= target.usercopy_pagemap_accesses.load() == 0;
    return passed;
}

auto selftest_canonical_filesystem_root() -> bool {
    namespace security = ker::mod::security;
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::TaskState;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0) return false;
    ProcTask process{};
    process.pid = 424260;
    if (resource::initialize_credentials(process, ROOT) != 0) return false;
    auto const PROCESS_REFS = process.ref_count.load();
    bool passed = true;
    auto check = [](bool condition, unsigned line) {
        if (!condition) ker::mod::dbg::logger<"procfs">::error("canonical root selftest failed at line %u", line);
        return condition;
    };
    {
        ProcTask thread{};
        thread.pid = process.pid + 1;
        thread.owner_pid = process.pid;
        thread.is_thread = true;
        std::strcpy(thread.root.data(), "/obsolete-thread-root");
        thread.root_len = std::strlen(thread.root.data());
        ProcTask::PathBuffer unbound{};
        unbound.fill('S');
        auto const BEFORE = unbound;
        passed &= check(copy_proc_filesystem_root(thread, unbound) == -ESRCH && unbound == BEFORE, __LINE__);
        if (!resource::bind_thread_owner(thread, process)) return false;
        File description{};
        description.refcount = 1;
        description.open_flags = 2 | O_APPEND;
        description.pos = 37;
        std::array<char, 256> physical{};
        description.vfs_path = physical.data();
        if (!process.fd_table.insert(13, &description)) return false;
        struct Remove {
            ProcTask& task;
            ~Remove() { task.fd_table.remove(13); }
        } remove{process};
        {
            ProcTask unbound_thread{};
            unbound_thread.pid = process.pid + 3;
            unbound_thread.is_thread = true;
            if (!unbound_thread.fd_table.insert(13, &description)) return false;
            unbound_thread.set_fd_cloexec(13);
            for (uint64_t owner_pid : {uint64_t{0}, unbound_thread.pid, process.pid}) {
                unbound_thread.owner_pid = owner_pid;
                passed &=
                    check(vfs_get_file_retain(&unbound_thread, 13) == nullptr && vfs_alloc_fd(&unbound_thread, &description) == -ESRCH &&
                              vfs_release_fd(&unbound_thread, 13) == -ESRCH && unbound_thread.fd_table.lookup(13) == &description &&
                              unbound_thread.get_fd_cloexec(13) && description.refcount.load() == 1,
                          __LINE__);
            }
            unbound_thread.fd_table.remove(13);
        }
        File allocated{};
        allocated.refcount = 1;
        int const ALLOCATED_FD = vfs_alloc_fd(&thread, &allocated);
        if (ALLOCATED_FD < 0) return false;
        passed &= check(process.fd_table.lookup(ALLOCATED_FD) == &allocated && thread.fd_table.empty(), __LINE__);
        auto* acquired = vfs_get_file_retain(&thread, ALLOCATED_FD);
        passed &= check(acquired == &allocated && allocated.refcount.load() == 2, __LINE__);
        if (acquired != nullptr) vfs_put_file(acquired);
        passed &= check(vfs_release_fd(&thread, ALLOCATED_FD) == 0 && process.fd_table.lookup(ALLOCATED_FD) == nullptr &&
                            allocated.refcount.load() == 1,
                        __LINE__);
        for (auto const* root : {"/proc", "/proc/changed-root", "/"}) {
            uint64_t const IRQF = process.path_metadata_lock.lock_irqsave();
            std::strcpy(process.root.data(), root);
            process.root_len = std::strlen(root);
            process.path_metadata_lock.unlock_irqrestore(IRQF);
            std::snprintf(physical.data(), physical.size(), "%s%sitem", root, std::strcmp(root, "/") == 0 ? "" : "/");
            for (auto* subject : {&process, &thread}) {
                auto const SUBJECT_REFS = subject->ref_count.load();
                {
                    ProcFileData data{};
                    data.target = subject;
                    subject->ref_count.fetch_add(1);
                    // An unregistered retained subject must not be reselected
                    // from either this deliberately wrong PID or owner_pid.
                    data.node.pid = UINT64_MAX - 1;
                    data.node.fd = 13;
                    File link{};
                    link.private_data = &data;
                    for (auto type : {ProcNodeType::ROOT_LINK, ProcNodeType::FD_LINK}) {
                        data.node.type = type;
                        const char* expected = type == ProcNodeType::ROOT_LINK ? root : "/item";
                        for (auto const& caller : {ROOT, user}) {
                            for (uint64_t caller_pid : {process.pid, process.pid + 2}) {
                                bool const ALLOWED = caller == ROOT || caller_pid == process.pid;
                                for (size_t capacity : {size_t{0}, size_t{1}, size_t{3}, size_t{128}}) {
                                    std::array<char, 128> output{};
                                    output.fill('S');
                                    auto const SENTINEL = output;
                                    uint64_t generation = UINT64_MAX;
                                    auto const COUNT = std::min(capacity, std::strlen(expected));
                                    auto const RESULT =
                                        procfs_readlink_as_impl(&link, output.data(), capacity, caller, caller_pid, &generation);
                                    if (ALLOWED) {
                                        passed &= check(RESULT == static_cast<ssize_t>(COUNT) &&
                                                            std::memcmp(output.data(), expected, COUNT) == 0 &&
                                                            generation == process.credential_observation().generation,
                                                        __LINE__);
                                        for (size_t i = COUNT; i < output.size(); ++i) passed &= check(output[i] == 'S', __LINE__);
                                    } else {
                                        passed &= check(RESULT == -EACCES && output == SENTINEL && generation == UINT64_MAX, __LINE__);
                                    }
                                    passed &=
                                        check(description.refcount.load() == 1 && description.pos == 37 &&
                                                  description.open_flags == (2 | O_APPEND) && process.fd_table.lookup(13) == &description,
                                              __LINE__);
                                }
                            }
                        }
                        for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
                            subject->state.store(state);
                            std::array<char, 128> output{};
                            output.fill('S');
                            auto const SENTINEL = output;
                            uint64_t generation = UINT64_MAX;
                            passed &= check(
                                procfs_readlink_as_impl(&link, output.data(), output.size(), ROOT, process.pid, &generation) == -ESRCH &&
                                    output == SENTINEL && generation == UINT64_MAX,
                                __LINE__);
                            subject->state.store(TaskState::ACTIVE);
                        }
                    }
                }
                passed &= check(subject->ref_count.load() == SUBJECT_REFS, __LINE__);
            }
            std::array<char, 8192> owner_mounts{}, thread_mounts{}, global_mounts{};
            auto const OWNER_SIZE = generate_mounts_for_task(&process, owner_mounts.data(), owner_mounts.size());
            auto const THREAD_SIZE = generate_mounts_for_task(&thread, thread_mounts.data(), thread_mounts.size());
            auto const GLOBAL_SIZE = generate_mounts_for_task(nullptr, global_mounts.data(), global_mounts.size());
            passed &= check(OWNER_SIZE != 0 && OWNER_SIZE == THREAD_SIZE && owner_mounts == thread_mounts && GLOBAL_SIZE != 0, __LINE__);
            if (std::strcmp(root, "/proc") == 0) passed &= check(owner_mounts != global_mounts, __LINE__);
            if (std::strcmp(root, "/") == 0) passed &= check(owner_mounts == global_mounts, __LINE__);
            char sentinel = 'S';
            passed &= check(generate_mounts_for_task(&thread, &sentinel, 0) == 0 && sentinel == 'S', __LINE__);
        }
        for (auto state : {TaskState::EXITING, TaskState::DEAD}) {
            process.state.store(state);
            passed &= check(vfs_get_file_retain(&thread, 13) == nullptr && vfs_alloc_fd(&thread, &description) == -ESRCH &&
                                vfs_release_fd(&thread, 13) == -ESRCH && process.fd_table.lookup(13) == &description &&
                                description.refcount.load() == 1,
                            __LINE__);
            process.state.store(TaskState::ACTIVE);
        }
        passed &= check(std::strcmp(thread.root.data(), "/obsolete-thread-root") == 0 && description.refcount.load() == 1, __LINE__);
    }
    return passed && process.ref_count.load() == PROCESS_REFS;
}

auto selftest_private_authorization() -> bool {
    namespace security = ker::mod::security;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, dumpable;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        user.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = true}, dumpable) != 0)
        return false;
    ProcTask target{};
    target.pid = 0x7B44;
    target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    if (ker::mod::sched::resource::initialize_credentials(target, ROOT) != 0) return false;
    auto const INITIAL_REFS = target.ref_count.load();
    bool passed = true;
    {
        ProcFileData data{};
        data.target = &target;
        target.ref_count.fetch_add(1);
        data.node = {.type = ProcNodeType::CMDLINE_FILE, .pid = target.pid};
        data.content = new char[5];
        if (data.content == nullptr) return false;
        std::memcpy(data.content, "keep", 5);
        data.content_len = 4;
        data.content_credential_generation = target.credential_observation().generation;
        auto* const ORIGINAL_CACHE = data.content;
        std::array<char, 4> output{'s', 'a', 'f', 'e'};
        for (auto type : {ProcNodeType::CMDLINE_FILE, ProcNodeType::MAPS_FILE, ProcNodeType::IMAGES_FILE, ProcNodeType::EXE_LINK,
                          ProcNodeType::CWD_LINK, ProcNodeType::ROOT_LINK, ProcNodeType::FD_LINK, ProcNodeType::FD_DIR}) {
            data.node.type = type;
            passed &= procfs_read_private(user, target.pid + 1, data, output.data(), output.size(), 0) == -EACCES;
            passed &= std::memcmp(output.data(), "safe", 4) == 0 && data.content == ORIGINAL_CACHE && data.content_len == 4;
        }
        DirEntry entry{};
        entry.d_ino = 0xCAFE;
        for (size_t index : {size_t{0}, size_t{1}, size_t{2}}) {
            passed &= procfs_readdir_private(user, target.pid + 1, data, &entry, index) == -EACCES;
            passed &= entry.d_ino == 0xCAFE;
        }
        passed &= procfs_readdir_private(user, target.pid, data, &entry, 0) == 0 && entry.d_name[0] == '.';
        data.node.type = ProcNodeType::CMDLINE_FILE;
        passed &= procfs_read_private(user, target.pid, data, output.data(), output.size(), 0) == 4;
        passed &= std::memcmp(output.data(), "keep", 4) == 0;
        std::memcpy(target.exe_path.data(), "/new", 5);
        passed &= ker::mod::sched::resource::initialize_credentials(target, dumpable) == 0;
        passed &= procfs_read_private(dumpable, target.pid + 1, data, output.data(), output.size(), 0) == 4;
        passed &= std::memcmp(output.data(), "/new", 4) == 0 && data.content_len == 5;
        passed &= data.content_credential_generation == target.credential_observation().generation;
        auto* const NEW_CACHE = data.content;
        passed &= ker::mod::sched::resource::initialize_credentials(target, user) == 0;
        output = {'s', 'a', 'f', 'e'};
        passed &= procfs_read_private(user, target.pid + 1, data, output.data(), output.size(), 0) == -EACCES;
        passed &= std::memcmp(output.data(), "safe", 4) == 0 && data.content == NEW_CACHE;
        passed &= procfs_read_private(user, target.pid, data, output.data(), output.size(), 0) == 4;
        target.state.store(ker::mod::sched::task::TaskState::DEAD);
        output = {'s', 'a', 'f', 'e'};
        passed &= procfs_read_private(ROOT, target.pid + 1, data, output.data(), output.size(), 0) == -ESRCH;
        passed &= std::memcmp(output.data(), "safe", 4) == 0;
        target.state.store(ker::mod::sched::task::TaskState::ACTIVE);
        delete[] data.content;
        data.content = nullptr;
    }
    passed &= target.ref_count.load() == INITIAL_REFS;
    return passed;
}

auto selftest_fd_authorization() -> bool {
    namespace security = ker::mod::security;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0) return false;
    ProcTask target{};
    target.pid = 0x7B43;
    auto* file = new File{};
    if (file == nullptr) return false;
    file->refcount = 1;
    int const FD = ker::vfs::vfs_alloc_fd(&target, file);
    if (FD < 0) {
        delete file;
        return false;
    }
    int result = 0;
    auto* acquired = authorized_task_fd_retain(user, target.pid + 1, target, FD, result);
    bool passed = acquired == nullptr && result == -EACCES && file->refcount.load() == 1;
    if (acquired != nullptr) ker::vfs::vfs_put_file(acquired);
    acquired = authorized_task_fd_retain(ROOT, target.pid + 1, target, FD, result);
    passed &= acquired == file && result == 0 && file->refcount.load() == 2;
    if (acquired != nullptr) ker::vfs::vfs_put_file(acquired);
    acquired = authorized_task_fd_retain(user, target.pid, target, FD, result);
    passed &= acquired == file && result == 0;
    if (acquired != nullptr) ker::vfs::vfs_put_file(acquired);
    passed &= file->refcount.load() == 1;
    passed &= ker::vfs::vfs_release_fd(&target, FD) == 0;
    ker::vfs::vfs_put_file(file);
    return passed;
}

auto selftest_global_authorization() -> bool {
    namespace security = ker::mod::security;
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot user, restricted_root;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        ROOT.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = 0}, restricted_root) != 0)
        return false;
    bool passed = true;
    bool const PERF_ENABLED = ker::mod::perf::is_enabled();
    auto const PERF_MASK = ker::mod::perf::get_event_mask();
    for (const char* path :
         {"kperfctl", "wki/chaos", "wki/chaos_workload", "memacc/track/page_callers", "memacc/track/kmalloc_debug",
          "memacc/reclaim/buffer_cache", "memacc/reclaim/packet_pool", "memacc/reclaim/xfs_inode", "memacc/reclaim/file_mmap_cache",
          "memacc/reclaim/anonymous_swap", "kperf", "kperfstatus", "kcpustate", "memacc/kmalloc_live", "wki/napi"}) {
        int result = 0;
        auto* file = procfs_open_path(ROOT, path, 0, 0, &result);
        if (file == nullptr || result != 0) return false;
        auto* data = static_cast<ProcFileData*>(file->private_data);
        std::array<char, 4> output{'s', 'a', 'f', 'e'};
        passed &= procfs_read_with_credentials(user, file, output.data(), output.size(), 0) == -EACCES;
        passed &= data->content == nullptr && data->content_len == 0;
        if (std::strcmp(path, "wki/napi") == 0) {
            passed &= procfs_read_with_credentials(ROOT, file, output.data(), output.size(), 0) == 4;
            passed &= std::memcmp(output.data(), "wki_", 4) == 0;
            auto* const SNAPSHOT = data->content;
            output = {'s', 'a', 'f', 'e'};
            passed &= procfs_read_with_credentials(user, file, output.data(), output.size(), 0) == -EACCES;
            passed &= procfs_read_with_credentials(restricted_root, file, output.data(), output.size(), 0) == -EACCES;
            passed &= std::memcmp(output.data(), "safe", 4) == 0 && data->content == SNAPSHOT;
            int restricted_result = 0;
            auto* restricted_file = procfs_open_path(restricted_root, path, 0, 0, &restricted_result);
            passed &= restricted_file == nullptr && restricted_result == -EACCES;
            if (restricted_file != nullptr) {
                procfs_close(restricted_file);
                delete restricted_file;
            }
            delete[] data->content;
            data->content = nullptr;
            data->content_len = 0;
        }
        data->content = new char[4];
        if (data->content == nullptr) {
            procfs_close(file);
            delete file;
            return false;
        }
        memcpy(data->content, "keep", 4);
        data->content_len = 4;
        auto* const CONTENT = data->content;
        passed &= procfs_read_with_credentials(user, file, output.data(), output.size(), 0) == -EACCES;
        passed &= memcmp(output.data(), "safe", 4) == 0;
        for (const char* command : {"enable", "disable", "clear", "reset", "drop", "status"}) {
            auto const EXPECTED = diagnostic_access(data->node.type) == DiagnosticAccess::KERNEL_READ ? -EPERM : -EACCES;
            passed &= procfs_write_with_credentials(user, file, command, strlen(command)) == EXPECTED;
            passed &= procfs_write_with_credentials(user, file, command, 0) == EXPECTED;
        }
        passed &= data->content == CONTENT && data->content_len == 4 && memcmp(data->content, "keep", 4) == 0;
        passed &= procfs_read_with_credentials(ROOT, file, output.data(), output.size(), 0) == 4;
        passed &= memcmp(output.data(), "keep", 4) == 0;
        procfs_close(file);
        delete file;
        file = procfs_open_path(user, path, 0, 0, &result);
        passed &= file == nullptr && result == -EACCES;
        if (file != nullptr) {
            procfs_close(file);
            delete file;
        }
    }
    passed &= ker::mod::perf::is_enabled() == PERF_ENABLED && ker::mod::perf::get_event_mask() == PERF_MASK;
    auto* public_file = procfs_open_path(user, "version", 0, 0);
    if (public_file == nullptr) return false;
    std::array<char, 32> output{};
    passed &= procfs_read_with_credentials(user, public_file, output.data(), output.size(), 0) > 0;
    procfs_close(public_file);
    delete public_file;
    return passed;
}

auto selftest_cpu_observation() -> bool {
    ProcTask process{};
    process.pid = 0x7B42;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    process.user_time_us = 3;
    process.system_time_us = 5;
    process.child_user_time_us = 90000;
    process.child_system_time_us = 110000;
    static_cast<void>(process.resources.charge_cpu(100000, 70000));
    // This task is absent from the live registry, like retained post-exit
    // metadata. Process history must survive, while thread view stays local.
    auto const GROUP = collect_proc_task_stats(process, false);
    auto const THREAD = collect_proc_task_stats(process, true);
    return GROUP.user_time_us == 70000 && GROUP.system_time_us == 30000 && GROUP.child_user_time_us == 90000 &&
           GROUP.child_system_time_us == 110000 && THREAD.user_time_us == 3 && THREAD.system_time_us == 5;
}
#endif

void procfs_init() {
    if (g_procfs_creation_epoch_ns == 0) {
        g_procfs_creation_epoch_ns = ker::mod::time::get_epoch_ns();
    }

    // Mount procfs at /proc
    ker::vfs::mount_filesystem("/proc", "procfs", nullptr);
}

}  // namespace ker::vfs::procfs
