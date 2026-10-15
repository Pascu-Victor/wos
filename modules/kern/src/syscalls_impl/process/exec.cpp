#include "exec.hpp"

#ifdef WOS_SELFTEST
#include <test/compartment_lifecycle_runtime.hpp>
#endif

#include <abi/callnums/perf_trace.h>
#include <abi/callnums/process.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>
#include <extern/elf.h>

#include <abi/compartment.hpp>
#include <net/wki/dev_proxy.hpp>
#include <platform/sched/native_creation_policy.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/resource_as.hpp>
#include <platform/sched/resource_process.hpp>
#include <platform/sched/session.hpp>
#include <platform/security/compartment_host.hpp>

// #define EXEC_DEBUG

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <memory>
#include <net/wki/remote_compute.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/ktime/process_timer.hpp>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/user_layout.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/random/entropy.hpp>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/lazy_vmem.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/compartment_wki.hpp>
#include <platform/security/exec_policy.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/signal.hpp>
#include <platform/sys/usercopy.hpp>
#include <platform/sys/userspace.hpp>
#include <span>
#include <string_view>
#include <syscalls_impl/multiproc/threadControl.hpp>
#include <util/hcf.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/fd_reservation.hpp>
#include <vfs/file.hpp>
#include <vfs/filesystem_context.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "net/wki/wki.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/threading.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/process.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "vfs/stat.hpp"
namespace ker::syscall::process {

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp,
                        const ker::abi::process::SpawnOptions* spawn_options, int shebang_depth,
                        const ker::mod::security::Snapshot& admitted_credentials, ker::mod::mm::numa::Policy memory_policy) -> uint64_t;
auto wos_proc_execve_impl(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr, int shebang_depth)
    -> uint64_t;

constexpr int MAX_SHEBANG_DEPTH = 4;
constexpr size_t EXEC_PATH_MAX = 512;
constexpr size_t EXEC_ARG_BYTES_MAX = static_cast<size_t>(2) * 1024 * 1024;
constexpr size_t EXEC_ARG_SMALL_BYTES = 1024;
constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGSTOP = 19;
constexpr uint64_t MAX_SPAWN_ACTIONS = 32;
constexpr size_t AT_RANDOM_BYTES = 16;
constexpr uint64_t EXEC_DEBUG_STAGING_BIT = 1ULL << 63U;
using exec_log = ker::mod::dbg::logger<"exec">;

auto exec_cmdline_has_token(const char* cmdline, const char* token) -> bool {
    if (cmdline == nullptr || token == nullptr || token[0] == '\0') {
        return false;
    }

    size_t const TOKEN_LEN = std::strlen(token);
    const char* cursor = cmdline;
    while (*cursor != '\0') {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') {
            ++cursor;
        }
        if (*cursor == '\0') {
            break;
        }

        const char* const START = cursor;
        size_t len = 0;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && *cursor != '\n') {
            ++cursor;
            ++len;
        }

        if (len == TOKEN_LEN && std::strncmp(START, token, TOKEN_LEN) == 0) {
            return true;
        }
    }
    return false;
}

auto exec_lazy_file_segments_enabled() -> bool {
    static std::atomic<int> s_enabled{-1};
    int const CACHED = s_enabled.load(std::memory_order_acquire);
    if (CACHED >= 0) {
        return CACHED != 0;
    }

    const char* const CMDLINE = ker::init::get_kernel_cmdline();
    bool const DISABLED =
        exec_cmdline_has_token(CMDLINE, "vmem.no_lazy_file_mmap") || exec_cmdline_has_token(CMDLINE, "exec.no_lazy_file_segments");
    int const VALUE = DISABLED ? 0 : 1;
    int expected = -1;
    if (s_enabled.compare_exchange_strong(expected, VALUE, std::memory_order_acq_rel, std::memory_order_acquire)) {
        exec_log::info("exec lazy file segments %s", DISABLED ? "disabled by cmdline" : "enabled");
        return !DISABLED;
    }

    return expected != 0;
}

void pin_completed_exec_locally(ker::mod::sched::task::Task& task) {
    task.wki_target_hostname.front() = '\0';
    // One-shot placement selects the system for the payload process tree, not
    // merely for the first executable in a wrapper chain.  Clearing to
    // automatic here lets a locally selected payload's fork/exec descendants
    // move independently, while remotely selected payloads stay receiver-local.
    // Keep both outcomes symmetric by pinning the successful payload locally.
    // A later explicit setwkitarget() call can still opt back into fan-out.
    task.wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL;
    // The rich exec path already made this task's placement decision. Prevent
    // scheduler publication from attempting the same placement again.
    task.wki_skip_legacy_placement = true;
}

void consume_successful_one_shot_wki_target(ker::mod::sched::task::Task* task, ker::net::wki::WkiRemoteSpawnResult result) {
    if (task == nullptr || result == ker::net::wki::WkiRemoteSpawnResult::FAILED ||
        (task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_ONESHOT) == 0) {
        return;
    }

    pin_completed_exec_locally(*task);
}

using FdSnapshot = std::array<uint64_t, ker::mod::sched::task::Task::FD_TABLE_SIZE>;
using LazyVmemKind = ker::mod::sched::task::LazyVmemKind;
using LazyVmemRange = ker::mod::sched::task::LazyVmemRange;
using LazyVmemRangeVec = ker::mod::sched::task::LazyVmemRangeVec;

#ifdef WOS_SELFTEST
std::atomic<bool> g_exec_selftest_force_fd_clone_insert_failure{false};
std::atomic<bool> g_exec_selftest_force_stdio_insert_failure{false};
std::atomic<int> g_exec_selftest_close_count{0};
ker::mod::sched::task::Task* g_exec_selftest_cleanup_owner{};
bool g_exec_selftest_cleanup_observed{};
bool g_exec_selftest_expect_admission_closed{};

auto exec_selftest_close(vfs::File*) -> int {
    g_exec_selftest_close_count.fetch_add(1, std::memory_order_relaxed);
    if (auto* owner = g_exec_selftest_cleanup_owner; owner != nullptr) {
        auto* prepared = owner->owned_exec_preparation.load(std::memory_order_acquire);
        bool const GUARDED = owner->exec_preparation_teardown_in_progress.load(std::memory_order_acquire);
        cleanup_exec_preparation(*owner);
        g_exec_selftest_cleanup_observed =
            GUARDED && prepared != nullptr && owner->owned_exec_preparation.load(std::memory_order_acquire) == prepared &&
            owner->exec_preparation_teardown_in_progress.load(std::memory_order_acquire) &&
            owner->process_exit_requested.load(std::memory_order_acquire) &&
            (!g_exec_selftest_expect_admission_closed ||
             (owner->credential_owner() != nullptr && owner->credential_owner()->resource_exec_admission_closed));
    }
    return 0;
}

vfs::FileOperations g_exec_selftest_fops = {
    .vfs_open = nullptr,
    .vfs_close = exec_selftest_close,
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

auto exec_selftest_make_file() -> vfs::File* {
    auto* file = new vfs::File{};
    if (file == nullptr) {
        return nullptr;
    }
    file->refcount.store(1, std::memory_order_relaxed);
    file->fops = &g_exec_selftest_fops;
    return file;
}
#endif

void record_local_proc_event(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, ker::mod::perf::WkiPerfPhase phase,
                             uint32_t correlation, int32_t status, uint32_t aux, uint64_t callsite) {
    if (task == nullptr) {
        return;
    }

    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), task->pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), phase, 0, 0, correlation, status,
                                     aux, callsite);
}

auto clamp_perf_aux(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

auto check_exec_permission_from_stat(const ker::mod::security::Snapshot& credentials, const vfs::Stat& statbuf) -> int {
    // Retain exec's requirement for at least one execute bit even for a caller
    // with DAC override; directory search has different central semantics.
    return ker::mod::security::authorize_file_access(credentials,
                                                     {statbuf.st_uid, statbuf.st_gid, static_cast<uint32_t>(statbuf.st_mode & 07777)}, 1);
}

struct LocalProcStage {
    uint32_t correlation{};
    uint64_t started_us{};
    uint64_t subject_pid{};
    uint64_t bytes{};
    uint64_t capture_session = ker::mod::perf::PERF_ANY_SESSION;
    ker::mod::perf::WkiPerfLocalProcOp op{};
    bool active{};
};

struct NativeExecCompletion {
    size_t trace_depth{};
    uint64_t subject_pid{};
};

auto begin_local_proc_stage_for_subject(uint64_t subject_pid, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
    -> LocalProcStage {
    uint64_t const SESSION =
        ker::mod::perf::current_wki_capture_session(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op));
    if (SESSION == ker::mod::perf::PERF_ANY_SESSION) {
        return {};
    }
    LocalProcStage const STAGE = {
        .correlation = ker::mod::perf::next_wki_trace_correlation(),
        .started_us = ker::mod::time::get_us(),
        .subject_pid = subject_pid,
        .capture_session = SESSION,
        .op = op,
        .active = true,
    };
    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), subject_pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op),
                                     ker::mod::perf::WkiPerfPhase::BEGIN, 0, 0, STAGE.correlation, 0, aux, callsite, SESSION);
    return STAGE;
}

auto begin_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
    -> LocalProcStage {
    return task != nullptr ? begin_local_proc_stage_for_subject(task->pid, op, aux, callsite) : LocalProcStage{};
}

auto end_local_proc_stage(ker::mod::sched::task::Task* /*task*/, ker::mod::perf::WkiPerfLocalProcOp op, LocalProcStage& stage,
                          int32_t status, uint64_t bytes, uint64_t callsite) -> uint32_t {
    if (!std::exchange(stage.active, false)) {
        return 0;
    }
    if (stage.capture_session !=
        ker::mod::perf::current_wki_capture_session(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op))) {
        return 0;
    }
    uint64_t const NOW = ker::mod::time::get_us();
    uint32_t const ELAPSED_US = clamp_perf_aux(NOW >= stage.started_us ? NOW - stage.started_us : 0);
    // Completion may run on the canonical owner after the preparing worker
    // has retired. Subject identity is a value, never a borrowed Task pointer.
    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), stage.subject_pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::END,
                                     0, 0, stage.correlation, status, ELAPSED_US, callsite, stage.capture_session);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), 0, 0, status, ELAPSED_US, true,
                                       0, bytes, stage.capture_session);
    return ELAPSED_US;
}

// Every admitted recursive attempt remains open through actual image completion.
// One pending substage per depth covers aborts that abandon loader stacks.
// Moving these fixed values follows existing request/image ownership transfers.
struct ExecTraceState {
    std::array<LocalProcStage, MAX_SHEBANG_DEPTH + 1> attempts{};
    std::array<LocalProcStage, MAX_SHEBANG_DEPTH + 1> pending{};

    void begin(ker::mod::sched::task::Task* task, size_t depth, uint64_t callsite) {
        auto& attempt = attempts.at(depth);
        if (!attempt.active)
            attempt = begin_local_proc_stage(task, ker::mod::perf::WkiPerfLocalProcOp::EXECVE, static_cast<uint32_t>(depth), callsite);
    }

    auto stage(ker::mod::sched::task::Task* task, size_t depth, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
        -> LocalProcStage& {
        auto& slot = pending.at(depth);
        slot = begin_local_proc_stage(task, op, aux, callsite);
        return slot;
    }

    void finish(ker::mod::sched::task::Task* task, int32_t status, uint64_t callsite) {
        for (size_t depth = attempts.size(); depth != 0; --depth) {
            auto& slot = pending.at(depth - 1);
            end_local_proc_stage(task, slot.op, slot, status, slot.bytes, callsite);
            auto& attempt = attempts.at(depth - 1);
            end_local_proc_stage(task, ker::mod::perf::WkiPerfLocalProcOp::EXECVE, attempt, status, attempt.bytes, callsite);
        }
    }
};

void arm_native_exec_user_return(ker::mod::sched::task::Task& task, const LocalProcStage& attempt, uint64_t initial_rip) {
    task.exec_image_trace = {
        .subject_pid = attempt.subject_pid,
        .initial_rip = initial_rip,
        .capture_session = attempt.capture_session,
        .correlation = attempt.correlation,
        .pending = attempt.active,
    };
}

template <typename T, size_t N>
auto fixed_slot(std::array<T, N>& values, size_t index) -> T& {
    // Callers validate logical extents before indexing fixed kernel buffers.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return values[index];
}

#ifdef WOS_SELFTEST
void release_task_fd_table_files(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->fd_table.for_each([](uint64_t /*key*/, void* val) {
        if (val != nullptr) {
            vfs::vfs_put_file(static_cast<vfs::File*>(val));
        }
    });
}
#endif

auto clone_exec_fd_table_checked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child, bool preserve_cloexec = false)
    -> bool {
    if (parent == nullptr || child == nullptr) {
        return false;
    }

    ker::mod::sched::resource::Owner fd_owner(parent);
    parent = fd_owner.get();
    if (parent == nullptr) {
        return false;
    }

    bool ok = true;
    uint64_t const IRQF = parent->fd_table_lock.lock_irqsave();
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (!ok || val == nullptr) {
            return;
        }
        bool const CLOEXEC = parent->get_fd_cloexec(static_cast<unsigned>(key));
        if (CLOEXEC && !preserve_cloexec) {
            return;
        }

        auto* parent_file = static_cast<vfs::File*>(val);
        parent_file->refcount.fetch_add(1, std::memory_order_acq_rel);
#ifdef WOS_SELFTEST
        if (g_exec_selftest_force_fd_clone_insert_failure.load(std::memory_order_relaxed)) {
            parent_file->refcount.fetch_sub(1, std::memory_order_acq_rel);
            ok = false;
            return;
        }
#endif
        if (!child->fd_table.insert(key, parent_file)) {
            parent_file->refcount.fetch_sub(1, std::memory_order_acq_rel);
            ok = false;
        } else if (CLOEXEC) {
            child->set_fd_cloexec(static_cast<unsigned>(key));
        }
    });
    parent->fd_table_lock.unlock_irqrestore(IRQF);
    return ok;
}

auto install_exec_fd_file_checked(ker::mod::sched::task::Task* task, unsigned fd, vfs::File* file) -> bool {
    if (task == nullptr || file == nullptr) {
        vfs::vfs_put_file(file);
        return false;
    }

    bool inserted = false;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
#ifdef WOS_SELFTEST
    bool const FORCE_FAILURE = g_exec_selftest_force_stdio_insert_failure.load(std::memory_order_relaxed);
#else
    bool const FORCE_FAILURE = false;
#endif
    if (!FORCE_FAILURE && fd < vfs::fd_limit_locked(*task) && task->fd_table.lookup(fd) == nullptr) {
        inserted = task->fd_table.insert(fd, file);
        if (inserted) {
            file->fd = static_cast<int>(fd);
            task->clear_fd_cloexec(fd);
        }
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    if (!inserted) {
        vfs::vfs_put_file(file);
    }
    return inserted;
}

auto spawn_fd_valid(int32_t fd) -> bool { return fd >= 0 && std::cmp_less(fd, ker::mod::sched::task::Task::FD_TABLE_SIZE); }

auto spawn_put_file(vfs::File* file) -> void {
    if (file != nullptr) {
        vfs::vfs_put_file(file);
    }
}

auto spawn_close_fd(ker::mod::sched::task::Task* task, int32_t fd) -> bool {
    if (task == nullptr || !spawn_fd_valid(fd)) {
        return true;
    }

    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* existing = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
    if (existing != nullptr) {
        task->fd_table.remove(static_cast<uint64_t>(fd));
        task->clear_fd_cloexec(static_cast<unsigned>(fd));
        vfs::fd_usage_changed_locked(*task);
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    spawn_put_file(existing);
    return true;
}

auto spawn_dup2_fd(ker::mod::sched::task::Task* task, int32_t srcfd, int32_t dstfd) -> int {
    if (task == nullptr) return -ESRCH;
    if (!spawn_fd_valid(srcfd) || !spawn_fd_valid(dstfd)) return -EBADF;

    auto* displaced = static_cast<vfs::File*>(nullptr);
    int result = -EBADF;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    if (srcfd != dstfd && static_cast<uint64_t>(dstfd) >= vfs::fd_limit_locked(*task)) {
        vfs::fd_limit_denied_locked(*task);
        task->fd_table_lock.unlock_irqrestore(IRQF);
        return -EBADF;
    }
    auto* src = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(srcfd)));
    if (src != nullptr) {
        if (srcfd == dstfd) {
            task->clear_fd_cloexec(static_cast<unsigned>(dstfd));
            result = 0;
        } else {
            src->refcount.fetch_add(1, std::memory_order_acq_rel);
            displaced = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(dstfd)));
            bool const INSERTED = task->fd_table.insert(static_cast<uint64_t>(dstfd), src);
            if (INSERTED) {
                result = 0;
                vfs::fd_usage_changed_locked(*task);
                task->clear_fd_cloexec(static_cast<unsigned>(dstfd));
            } else {
                src->refcount.fetch_sub(1, std::memory_order_acq_rel);
                displaced = nullptr;
                result = -EBUSY;  // A bounded fixed-table slot can only reject a reservation.
            }
        }
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    spawn_put_file(displaced);
    return result;
}

// Only called for an exclusively owned, unpublished child. Ordered OPEN closes
// its destination before resolving the path, so a full table can reuse that slot.
// Failed construction discards this private table; parent descriptions survive.
auto spawn_open_fd(ker::mod::sched::task::Task* task, const ker::abi::process::SpawnFdAction& action) -> int {
    if (task == nullptr) return -ESRCH;
    if (!spawn_fd_valid(action.fd)) return -EBADF;
    if (action.path == nullptr) return -EINVAL;
    {
        uint64_t const FLAGS = task->fd_table_lock.lock_irqsave();
        bool const ALLOWED = static_cast<uint64_t>(action.fd) < vfs::fd_limit_locked(*task);
        if (!ALLOWED) vfs::fd_limit_denied_locked(*task);
        task->fd_table_lock.unlock_irqrestore(FLAGS);
        if (!ALLOWED) return -EBADF;
    }
    spawn_close_fd(task, action.fd);
    int const OPENED = vfs::vfs_openat(task, vfs::AT_FDCWD, action.path, action.oflag, static_cast<int>(action.mode));
    if (OPENED < 0) return OPENED;
    if (OPENED == action.fd) return 0;

    // VFS reserved and installed OPENED before returning. The destination is
    // empty, bounded and unreserved in this private fixed table. Moving the
    // description cannot allocate, block or fail after create/truncate succeeds.
    uint64_t const FLAGS = task->fd_table_lock.lock_irqsave();
    auto* file = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(OPENED)));
    bool const CLOEXEC = task->get_fd_cloexec(static_cast<unsigned>(OPENED));
    bool const MOVED = file != nullptr && task->fd_table.lookup(static_cast<uint64_t>(action.fd)) == nullptr &&
                       task->fd_table.insert(static_cast<uint64_t>(action.fd), file);
    if (MOVED) {
        task->fd_table.remove(static_cast<uint64_t>(OPENED));
        task->clear_fd_cloexec(static_cast<unsigned>(OPENED));
        if (CLOEXEC)
            task->set_fd_cloexec(static_cast<unsigned>(action.fd));
        else
            task->clear_fd_cloexec(static_cast<unsigned>(action.fd));
        file->fd = action.fd;
        vfs::fd_usage_changed_locked(*task);
    }
    task->fd_table_lock.unlock_irqrestore(FLAGS);
    if (!MOVED) {
        ker::mod::dbg::panic_handler("spawn_open_fd: unpublished descriptor transfer lost ownership");
        hcf();
    }
    return 0;
}

auto apply_spawn_options(ker::mod::sched::task::Task* task, const ker::abi::process::SpawnOptions* options, int* error_out = nullptr)
    -> bool {
    if (error_out != nullptr) {
        *error_out = 0;
    }
    if (options == nullptr) {
        return true;
    }
    if (options->size != sizeof(ker::abi::process::SpawnOptions) || options->version != ker::abi::process::SPAWN_OPTIONS_VERSION ||
        options->reserved0 != 0 || options->reserved1 != 0) {
        return false;
    }
    if ((options->flags & ~ker::abi::process::SPAWN_SUPPORTED_FLAGS) != 0) {
        return false;
    }

    if (options->action_count > MAX_SPAWN_ACTIONS || (options->action_count != 0 && options->actions == nullptr)) {
        return false;
    }

    // Reject a foreign/nonexistent group before OPEN actions can create or
    // truncate paths. The unpublished task owns any successful group change.
    if ((options->flags & ker::abi::process::SPAWN_FLAG_SETPGROUP) != 0) {
        int const GROUP_RESULT = ker::mod::sched::session::prepare_spawn_group(*task, options->pgroup);
        if (GROUP_RESULT != 0) {
            if (error_out != nullptr) {
                *error_out = GROUP_RESULT;
            }
            return false;
        }
    }

    for (uint64_t i = 0; i < options->action_count; ++i) {
        auto const& action = options->actions[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        bool ok = false;
        switch (static_cast<ker::abi::process::SpawnFdActionType>(action.type)) {
            case ker::abi::process::SpawnFdActionType::CLOSE:
                ok = spawn_close_fd(task, action.fd);
                break;
            case ker::abi::process::SpawnFdActionType::DUP2: {
                int const RESULT = spawn_dup2_fd(task, action.srcfd, action.fd);
                if (RESULT != 0 && error_out != nullptr) *error_out = RESULT;
                ok = RESULT == 0;
                break;
            }
            case ker::abi::process::SpawnFdActionType::OPEN: {
                int const RESULT = spawn_open_fd(task, action);
                if (RESULT != 0 && error_out != nullptr) *error_out = RESULT;
                ok = RESULT == 0;
                break;
            }
            default:
                ok = false;
                break;
        }
        if (!ok) {
            return false;
        }
    }

    if ((options->flags & ker::abi::process::SPAWN_FLAG_SETSIGMASK) != 0) {
        uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
        task->signal_mask_store(options->sig_mask & ~UNBLOCKABLE);
        ker::mod::sys::signal::sync_task_signal_mask_cache(task);
    }

    return true;
}

auto ensure_exec_stdio_fallbacks(ker::mod::sched::task::Task* task) -> bool {
    // User threads retain their process owner; only that table is visible to
    // VFS. Unpublished process construction continues to use its own table.
    task = task != nullptr ? task->credential_owner() : nullptr;
    if (task == nullptr) {
        return false;
    }

    // Live private views preserve only inherited or explicitly granted FDs.
    // Closed or unbound views refuse before any Host device or table effect.
    int const SCOPE = ker::mod::security::compartment::host_namespace_admission(task);
    if (SCOPE != 0) return SCOPE == -EPERM;

    constexpr int STDIN_OPEN_FLAGS = 0;   // O_RDONLY
    constexpr int STDOUT_OPEN_FLAGS = 1;  // O_WRONLY

    auto const CREDENTIALS = task->credentials();
    for (unsigned fd = 0; fd < 3; ++fd) {
        {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            bool const PRESENT = task->fd_table.lookup(fd) != nullptr || fd >= vfs::fd_limit_locked(*task);
            task->fd_table_lock.unlock_irqrestore(IRQF);
            if (PRESENT) {
                continue;
            }
        }

        int const OPEN_FLAGS = fd == 0 ? STDIN_OPEN_FLAGS : STDOUT_OPEN_FLAGS;
        vfs::File* new_file = vfs::devfs::devfs_open_path(CREDENTIALS, "/dev/console", OPEN_FLAGS, 0);
        if (new_file == nullptr) {
            continue;
        }
        new_file->fops = vfs::devfs::get_devfs_fops();
        new_file->refcount.store(1, std::memory_order_relaxed);
        if (!install_exec_fd_file_checked(task, fd, new_file)) {
            return false;
        }
    }
    return true;
}

auto collect_cloexec_fds_locked(ker::mod::sched::task::Task* task, FdSnapshot& fds) -> size_t {
    // Match vfs_close's process table, including a retired main-thread owner.
    task = task != nullptr ? task->credential_owner() : nullptr;
    if (task == nullptr) {
        return 0;
    }

    size_t fd_count = 0;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    task->fd_table.for_each([&](uint64_t key, void* val) {
        if (val == nullptr || key >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
            return;
        }
        if (task->get_fd_cloexec(static_cast<unsigned>(key)) && fd_count < fds.size()) {
            fixed_slot(fds, fd_count++) = key;
        }
    });
    task->fd_table_lock.unlock_irqrestore(IRQF);
    return fd_count;
}

auto close_spawn_cloexec_fds(ker::mod::sched::task::Task* task) -> void {
    for (;;) {
        FdSnapshot fds{};
        size_t const FD_COUNT = collect_cloexec_fds_locked(task, fds);
        if (FD_COUNT == 0) {
            return;
        }
        for (size_t i = 0; i < FD_COUNT; ++i) {
            spawn_close_fd(task, static_cast<int32_t>(fixed_slot(fds, i)));
        }
    }
}

struct SpawnOptionsSnapshot {
    ker::abi::process::SpawnOptions options{};
    std::array<ker::abi::process::SpawnFdAction, MAX_SPAWN_ACTIONS> actions{};
    std::array<char*, MAX_SPAWN_ACTIONS> owned_paths{};

    ~SpawnOptionsSnapshot() {
        for (auto* path : owned_paths) {
            delete[] path;
        }
    }
};

// A syscall entry snapshot owns every byte and pointer that the loader is
// allowed to traverse. The two-megabyte combined strings/pointer budget
// matches the ARG_MAX value advertised by mlibc and bounds both hostile
// unterminated vectors and allocator use.
struct ExecArgumentsSnapshot {
    std::array<char, EXEC_PATH_MAX> path{};
    char* strings{};
    // Completed strings retain their addresses when a later string needs the large arena.
    char* small_strings{};
    const char** argv{};
    const char** envp{};
    ker::util::SmallVec<uint64_t, 16> input_string_addrs;
    size_t strings_used{};
    size_t budget_used{};

    ~ExecArgumentsSnapshot() {
        delete[] argv;
        delete[] envp;
        delete[] strings;
        delete[] small_strings;
    }

    ExecArgumentsSnapshot() = default;
    ExecArgumentsSnapshot(const ExecArgumentsSnapshot&) = delete;
    auto operator=(const ExecArgumentsSnapshot&) -> ExecArgumentsSnapshot& = delete;
};

auto snapshot_exec_string(ker::mod::sched::task::Task& task, uint64_t string_addr, ExecArgumentsSnapshot& snapshot, const char*& out)
    -> int {
    size_t const REMAINING = EXEC_ARG_BYTES_MAX - snapshot.budget_used;
    size_t copied = 0;
    for (;;) {
        if (copied == REMAINING) return -E2BIG;
        if (snapshot.strings == nullptr && snapshot.strings_used + copied == EXEC_ARG_SMALL_BYTES) {
            // Publish ownership before the next faulting copy. Move only this
            // unfinished prefix; earlier strings remain in the retained small arena.
            snapshot.strings = new (std::nothrow) char[EXEC_ARG_BYTES_MAX];
            if (snapshot.strings == nullptr) return -ENOMEM;
            std::memcpy(snapshot.strings, snapshot.small_strings + snapshot.strings_used, copied);
            snapshot.strings_used = 0;
        }
        char* const STORAGE = snapshot.strings != nullptr ? snapshot.strings : snapshot.small_strings;
        size_t const CAPACITY = snapshot.strings != nullptr ? EXEC_ARG_BYTES_MAX : EXEC_ARG_SMALL_BYTES;
        uint64_t current = 0;
        if (__builtin_add_overflow(string_addr, static_cast<uint64_t>(copied), &current)) return -EFAULT;
        size_t const PAGE_REMAINING = ker::mod::mm::paging::PAGE_SIZE - (current & (ker::mod::mm::paging::PAGE_SIZE - 1));
        size_t const CHUNK = std::min({REMAINING - copied, CAPACITY - snapshot.strings_used - copied, PAGE_REMAINING});
        char* const DEST = STORAGE + snapshot.strings_used + copied;
        auto const COPY = ker::mod::sys::usercopy::copy_from_task_partial(task, current, DEST, CHUNK);
        if (COPY.bytes_copied == 0) return -EFAULT;
        auto const* const NUL = static_cast<const char*>(std::memchr(DEST, '\0', COPY.bytes_copied));
        if (NUL != nullptr) {
            copied += static_cast<size_t>(NUL - DEST) + 1;
            out = STORAGE + snapshot.strings_used;
            snapshot.strings_used += copied;
            snapshot.budget_used += copied;
            return 0;
        }
        copied += COPY.bytes_copied;
        if (COPY.fault) return -EFAULT;
    }
}

auto snapshot_exec_vector(ker::mod::sched::task::Task& task, uint64_t vector_addr, ExecArgumentsSnapshot& snapshot, const char**& out)
    -> int {
    if (vector_addr == 0) {
        if (snapshot.budget_used > EXEC_ARG_BYTES_MAX - sizeof(uint64_t)) {
            return -E2BIG;
        }
        snapshot.budget_used += sizeof(uint64_t);
        out = nullptr;
        return 0;
    }

    auto& string_addrs = snapshot.input_string_addrs;
    string_addrs.clear();
    size_t index = 0;
    bool terminated = false;
    while (!terminated) {
        if (snapshot.budget_used > EXEC_ARG_BYTES_MAX - sizeof(uint64_t)) {
            return -E2BIG;
        }

        uint64_t offset = 0;
        uint64_t entry_addr = 0;
        if (__builtin_mul_overflow(static_cast<uint64_t>(index), static_cast<uint64_t>(sizeof(uint64_t)), &offset) ||
            __builtin_add_overflow(vector_addr, offset, &entry_addr)) {
            return -EFAULT;
        }

        std::array<uint64_t, 16> captured;
        size_t const PAGE_REMAINING = ker::mod::mm::paging::PAGE_SIZE - (entry_addr & (ker::mod::mm::paging::PAGE_SIZE - 1));
        // Batch only the inline prefix. Keep the original per-entry copy at
        // every spill: public SmallVec capacity does not describe its grow check.
        size_t const BEFORE_SPILL = string_addrs.size() < captured.size() ? captured.size() - string_addrs.size() + 1 : 1;
        size_t const BATCH_COUNT = std::min({captured.size(), PAGE_REMAINING / sizeof(uint64_t),
                                             (EXEC_ARG_BYTES_MAX - snapshot.budget_used) / sizeof(uint64_t), BEFORE_SPILL});
        ker::mod::sys::usercopy::CopyResult copy{};
        size_t count = BATCH_COUNT;
        if (count <= 1) {
            // Keep one-entry copying at spill growth and unaligned page crossings.
            count = 1;
            if (!ker::mod::sys::usercopy::copy_value_from_task(task, entry_addr, captured.at(0))) return -EFAULT;
            copy.bytes_copied = sizeof(uint64_t);
        } else {
            // Never cross a user page or read past the first possible SmallVec
            // growth failure. The copy releases every pin before push_back.
            copy = ker::mod::sys::usercopy::copy_from_task_partial(task, entry_addr, captured.data(), count * sizeof(uint64_t));
        }
        size_t const COMPLETE = copy.bytes_copied / sizeof(uint64_t);
        for (size_t entry = 0; entry < COMPLETE; ++entry) {
            uint64_t const STRING_ADDR = captured.at(entry);
            snapshot.budget_used += sizeof(uint64_t);
            ++index;
            if (STRING_ADDR == 0) {
                terminated = true;
                break;
            }
            if (!string_addrs.push_back(STRING_ADDR)) return -ENOMEM;
        }
        // A complete terminating word wins over a fault in later captured bytes.
        if (!terminated && (copy.fault || COMPLETE != count)) return -EFAULT;
    }

    size_t const COUNT = string_addrs.size();
    out = new (std::nothrow) const char*[COUNT + 1];
    if (out == nullptr) {
        return -ENOMEM;
    }

    if (COUNT != 0 && snapshot.small_strings == nullptr && snapshot.strings == nullptr) {
        snapshot.small_strings = new (std::nothrow) char[EXEC_ARG_SMALL_BYTES];
        if (snapshot.small_strings == nullptr) return -ENOMEM;
    }
    for (size_t index = 0; index < COUNT; ++index) {
        int const RESULT = snapshot_exec_string(task, string_addrs.at(index), snapshot, out[index]);
        if (RESULT != 0) return RESULT;
    }
    out[COUNT] = nullptr;
    return 0;
}

auto snapshot_exec_arguments(ker::mod::sched::task::Task& task, uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr,
                             ExecArgumentsSnapshot& snapshot) -> int {
    auto const PATH_STATUS =
        ker::mod::sys::usercopy::copy_cstring_from_task_status(task, path_addr, snapshot.path.data(), snapshot.path.size());
    if (PATH_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT) {
        return -EFAULT;
    }
    if (PATH_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::TOO_LONG) {
        return -ENAMETOOLONG;
    }

    int result = snapshot_exec_vector(task, argv_addr, snapshot, snapshot.argv);
    if (result == 0) {
        result = snapshot_exec_vector(task, envp_addr, snapshot, snapshot.envp);
    }
    return result;
}

auto snapshot_spawn_options(ker::mod::sched::task::Task& parent, uint64_t user_options_addr, SpawnOptionsSnapshot& snapshot) -> int {
    if (user_options_addr == 0 || !ker::mod::sys::usercopy::copy_value_from_task(parent, user_options_addr, snapshot.options)) {
        return -EFAULT;
    }
    if (snapshot.options.size != sizeof(ker::abi::process::SpawnOptions) ||
        snapshot.options.version != ker::abi::process::SPAWN_OPTIONS_VERSION || snapshot.options.reserved0 != 0 ||
        snapshot.options.reserved1 != 0 || (snapshot.options.flags & ~ker::abi::process::SPAWN_SUPPORTED_FLAGS) != 0 ||
        snapshot.options.action_count > MAX_SPAWN_ACTIONS || (snapshot.options.action_count != 0 && snapshot.options.actions == nullptr)) {
        return -EINVAL;
    }

    uint64_t const ACTION_COUNT = snapshot.options.action_count;
    if (ACTION_COUNT != 0 &&
        !ker::mod::sys::usercopy::copy_from_task(parent, reinterpret_cast<uint64_t>(snapshot.options.actions), snapshot.actions.data(),
                                                 ACTION_COUNT * sizeof(ker::abi::process::SpawnFdAction))) {
        return -EFAULT;
    }

    for (uint64_t i = 0; i < ACTION_COUNT; ++i) {
        auto& action = fixed_slot(snapshot.actions, static_cast<size_t>(i));
        switch (static_cast<ker::abi::process::SpawnFdActionType>(action.type)) {
            case ker::abi::process::SpawnFdActionType::CLOSE:
            case ker::abi::process::SpawnFdActionType::DUP2:
                action.path = nullptr;
                break;
            case ker::abi::process::SpawnFdActionType::OPEN: {
                auto* path = new (std::nothrow) char[EXEC_PATH_MAX];
                if (path == nullptr) {
                    return -ENOMEM;
                }
                auto const PATH_STATUS = ker::mod::sys::usercopy::copy_cstring_from_task_status(
                    parent, reinterpret_cast<uint64_t>(action.path), path, EXEC_PATH_MAX);
                if (PATH_STATUS != ker::mod::sys::usercopy::CStringCopyStatus::COMPLETE) {
                    delete[] path;
                    return PATH_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT ? -EFAULT : -ENAMETOOLONG;
                }
                fixed_slot(snapshot.owned_paths, static_cast<size_t>(i)) = path;
                action.path = path;
                break;
            }
            default:
                return -EINVAL;
        }
    }

    snapshot.options.actions = ACTION_COUNT != 0 ? snapshot.actions.data() : nullptr;
    return 0;
}

inline auto local_wki_hostname() -> const char* { return std::begin(ker::net::wki::g_wki.local_hostname); }

auto copy_exec_path(const char* path, std::array<char, EXEC_PATH_MAX>& out) -> int {
    if (path == nullptr) {
        return -EFAULT;
    }

    for (size_t i = 0; i < out.size(); ++i) {
        char const C = path[i];
        fixed_slot(out, i) = C;
        if (C == '\0') {
            return i == 0 ? -ENOENT : 0;
        }
    }

    fixed_slot(out, out.size() - 1) = '\0';
    return -ENAMETOOLONG;
}

struct ShebangInfo {
    std::array<char, ker::mod::sched::task::Task::EXE_PATH_MAX> interpreter = {};
    std::array<char, 256> argument = {};
    bool has_argument = false;
};

auto allocate_kernel_stack() -> uint64_t {
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::kernel_stack_alloc("exec_kstack"));
    if (stack_base == 0) {
        return 0;
    }

    return stack_base + ker::mod::mm::KERNEL_STACK_SIZE;
}

auto read_file_fully(vfs::File* fd, uint8_t* dst, size_t size, const char* path) -> ssize_t {
    if (dst == nullptr) {
        return -EINVAL;
    }
    size_t total = 0;
    int consecutive_errors = 0;
    constexpr int MAX_CONSECUTIVE_ERRORS = 3;

    while (total < size) {
        ssize_t const RC = vfs::vfs_pread_file(fd, dst + total, size - total, static_cast<off_t>(total));
        if (RC > 0) {
            total += static_cast<size_t>(RC);
            consecutive_errors = 0;
            continue;
        }
        if (RC == 0) {
            exec_log::warn("exec: unexpected EOF while reading '%s' at %llu/%llu bytes", path, static_cast<unsigned long long>(total),
                           static_cast<unsigned long long>(size));
            return static_cast<ssize_t>(total);
        }

        consecutive_errors++;
        if (consecutive_errors >= MAX_CONSECUTIVE_ERRORS) {
            exec_log::warn("exec: read failed for '%s' at %llu/%llu bytes rc=%lld after %d attempts", path,
                           static_cast<unsigned long long>(total), static_cast<unsigned long long>(size), static_cast<long long>(RC),
                           MAX_CONSECUTIVE_ERRORS);
            return RC;
        }
    }

    return static_cast<ssize_t>(total);
}

constexpr size_t EXEC_SPARSE_ELF_MIN_SIZE = static_cast<size_t>(64) * 1024;
constexpr size_t EXEC_SHEBANG_PROBE_SIZE = 4096;
constexpr size_t EXEC_FILE_BACKED_ELF_MIN_SIZE = static_cast<size_t>(8) * 1024 * 1024;
constexpr uint64_t EXEC_FILE_BACKED_METADATA_MAX = static_cast<uint64_t>(4) * 1024 * 1024;

struct ExecImageReadResult {
    ssize_t bytes_read{};
    int status{};
    size_t shebang_probe_size{};
};

auto exec_min_size(size_t lhs, size_t rhs) -> size_t { return lhs < rhs ? lhs : rhs; }

auto exec_range_in_file(size_t file_size, uint64_t offset, uint64_t size) -> bool {
    return offset <= file_size && size <= (static_cast<uint64_t>(file_size) - offset);
}

auto exec_table_size(uint64_t count, uint64_t entry_size, uint64_t& out) -> bool {
    if (entry_size != 0 && count > UINT64_MAX / entry_size) {
        return false;
    }
    out = count * entry_size;
    return true;
}

struct ExecFileReadContext {
    vfs::File* file{};
    uint64_t logical_size{};
    int error{};
};

struct PreparedFileBackedElf {
    loader::elf::ElfFileView view{};
    ExecFileReadContext read_context{};
    Elf64_Phdr* program_headers{};
    Elf64_Shdr* section_headers{};
    char* section_names{};
    uint8_t* task_metadata{};
    size_t task_metadata_size{};
    size_t bytes_read{};

    PreparedFileBackedElf() = default;
    PreparedFileBackedElf(const PreparedFileBackedElf&) = delete;
    auto operator=(const PreparedFileBackedElf&) -> PreparedFileBackedElf& = delete;

    ~PreparedFileBackedElf() { reset(); }

    void reset() {
        delete[] std::exchange(program_headers, nullptr);
        delete[] std::exchange(section_headers, nullptr);
        delete[] std::exchange(section_names, nullptr);
        delete[] std::exchange(task_metadata, nullptr);
        view = {};
        read_context = {};
        task_metadata_size = 0;
        bytes_read = 0;
    }

    auto take_task_metadata() -> uint8_t* {
        uint8_t* const RESULT = task_metadata;
        task_metadata = nullptr;
        return RESULT;
    }
};

auto read_exec_file_exact(vfs::File* file, uint64_t logical_size, uint64_t offset, void* destination, size_t size, int* error = nullptr)
    -> bool {
    if (error != nullptr) *error = 0;
    if (file == nullptr || destination == nullptr || offset > logical_size || size > logical_size - offset ||
        offset > static_cast<uint64_t>(INT64_MAX)) {
        if (error != nullptr) *error = -EIO;
        return false;
    }

    auto* out = static_cast<uint8_t*>(destination);
    size_t total = 0;
    int consecutive_errors = 0;
    constexpr int MAX_CONSECUTIVE_ERRORS = 3;
    while (total < size) {
        uint64_t const READ_OFFSET = offset + total;
        if (READ_OFFSET > static_cast<uint64_t>(INT64_MAX)) {
            if (error != nullptr) *error = -EIO;
            return false;
        }
        ssize_t const RC = vfs::vfs_pread_file(file, out + total, size - total, static_cast<off_t>(READ_OFFSET));
        if (RC > 0) {
            total += static_cast<size_t>(RC);
            consecutive_errors = 0;
            continue;
        }
        if (RC == 0) {
            if (error != nullptr) *error = -EIO;
            return false;
        }
        if (++consecutive_errors >= MAX_CONSECUTIVE_ERRORS) {
            if (error != nullptr) *error = static_cast<int>(RC);
            return false;
        }
    }
    return true;
}

auto exec_file_read_at(void* context, uint64_t offset, void* destination, size_t size) -> bool {
    auto* source = static_cast<ExecFileReadContext*>(context);
    if (source == nullptr || source->file == nullptr) {
        return false;
    }
    return read_exec_file_exact(source->file, source->logical_size, offset, destination, size, &source->error);
}

enum class PrepareFileBackedElfResult : uint8_t {
    READY,
    FALLBACK,
    INVALID,
    OUT_OF_MEMORY,
    IO_ERROR,
};

auto prepare_file_backed_elf(vfs::File* file, size_t file_size, PreparedFileBackedElf& out,
                             mod::mm::user_layout::ImageRole role = mod::mm::user_layout::ImageRole::MAIN,
                             size_t main_minimum_size = EXEC_FILE_BACKED_ELF_MIN_SIZE) -> PrepareFileBackedElfResult {
    bool const INTERPRETER = role == mod::mm::user_layout::ImageRole::INTERPRETER;
    if (file == nullptr || file_size < sizeof(Elf64_Ehdr) ||
        (INTERPRETER ? file_size <= EXEC_SPARSE_ELF_MIN_SIZE || file->exec_read_view == nullptr : file_size < main_minimum_size)) {
        return PrepareFileBackedElfResult::FALLBACK;
    }

    int* const READ_ERROR = INTERPRETER ? &out.read_context.error : nullptr;
    Elf64_Ehdr header{};
    if (!read_exec_file_exact(file, file_size, 0, &header, sizeof(header), READ_ERROR)) {
        return PrepareFileBackedElfResult::IO_ERROR;
    }
    out.bytes_read += sizeof(header);
    if (header.e_ident[EI_MAG0] != ELFMAG0 || header.e_ident[EI_MAG1] != ELFMAG1 || header.e_ident[EI_MAG2] != ELFMAG2 ||
        header.e_ident[EI_MAG3] != ELFMAG3 || header.e_ident[EI_CLASS] != ELFCLASS64 || header.e_phnum == 0 ||
        header.e_phentsize != sizeof(Elf64_Phdr)) {
        return PrepareFileBackedElfResult::FALLBACK;
    }

    // A compact view must not accept offsets rejected by the old contiguous parser.
    if (INTERPRETER &&
        ((header.e_phoff % alignof(Elf64_Phdr)) != 0U || (header.e_shnum != 0 && (header.e_shoff % alignof(Elf64_Shdr)) != 0U))) {
        return PrepareFileBackedElfResult::FALLBACK;
    }

    uint64_t phdr_bytes = 0;
    if (!exec_table_size(header.e_phnum, header.e_phentsize, phdr_bytes) || phdr_bytes > EXEC_FILE_BACKED_METADATA_MAX ||
        !exec_range_in_file(file_size, header.e_phoff, phdr_bytes)) {
        return PrepareFileBackedElfResult::INVALID;
    }
    out.program_headers = new Elf64_Phdr[header.e_phnum];
    if (out.program_headers == nullptr) {
        return PrepareFileBackedElfResult::OUT_OF_MEMORY;
    }
    if (!read_exec_file_exact(file, file_size, header.e_phoff, out.program_headers, static_cast<size_t>(phdr_bytes), READ_ERROR)) {
        return PrepareFileBackedElfResult::IO_ERROR;
    }
    out.bytes_read += static_cast<size_t>(phdr_bytes);

    bool has_interp = false;
    for (Elf64_Half i = 0; i < header.e_phnum; ++i) {
        has_interp = has_interp || out.program_headers[i].p_type == PT_INTERP;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }
    // The loader's static relocation path intentionally still requires the
    // complete image. Large dynamically linked programs are the fragmented-
    // memory failure mode this bounded source removes.
    if (!has_interp) {
        if (!INTERPRETER) return PrepareFileBackedElfResult::FALLBACK;
    }

    if (header.e_shnum != 0) {
        uint64_t shdr_bytes = 0;
        if (header.e_shentsize != sizeof(Elf64_Shdr) || header.e_shstrndx >= header.e_shnum ||
            !exec_table_size(header.e_shnum, header.e_shentsize, shdr_bytes) || shdr_bytes > EXEC_FILE_BACKED_METADATA_MAX ||
            !exec_range_in_file(file_size, header.e_shoff, shdr_bytes)) {
            return (INTERPRETER && shdr_bytes > EXEC_FILE_BACKED_METADATA_MAX) ? PrepareFileBackedElfResult::FALLBACK
                                                                               : PrepareFileBackedElfResult::INVALID;
        }
        out.section_headers = new Elf64_Shdr[header.e_shnum];
        if (out.section_headers == nullptr) {
            return PrepareFileBackedElfResult::OUT_OF_MEMORY;
        }
        if (!read_exec_file_exact(file, file_size, header.e_shoff, out.section_headers, static_cast<size_t>(shdr_bytes), READ_ERROR)) {
            return PrepareFileBackedElfResult::IO_ERROR;
        }
        out.bytes_read += static_cast<size_t>(shdr_bytes);

        if (!INTERPRETER || header.e_shstrndx != SHN_UNDEF) {
            const Elf64_Shdr& shstrtab = out.section_headers[header.e_shstrndx];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            if (shstrtab.sh_size > EXEC_FILE_BACKED_METADATA_MAX || !exec_range_in_file(file_size, shstrtab.sh_offset, shstrtab.sh_size)) {
                return (INTERPRETER && shstrtab.sh_size > EXEC_FILE_BACKED_METADATA_MAX) ? PrepareFileBackedElfResult::FALLBACK
                                                                                         : PrepareFileBackedElfResult::INVALID;
            }
            if (shstrtab.sh_size != 0) {
                out.section_names = new char[static_cast<size_t>(shstrtab.sh_size)];
                if (out.section_names == nullptr) {
                    return PrepareFileBackedElfResult::OUT_OF_MEMORY;
                }
                if (!read_exec_file_exact(file, file_size, shstrtab.sh_offset, out.section_names, static_cast<size_t>(shstrtab.sh_size),
                                          READ_ERROR)) {
                    return PrepareFileBackedElfResult::IO_ERROR;
                }
                out.bytes_read += static_cast<size_t>(shstrtab.sh_size);
            }
            out.view.section_names_size = shstrtab.sh_size;
        }
    }

    if (!INTERPRETER) {
        if (header.e_phoff > EXEC_FILE_BACKED_METADATA_MAX || phdr_bytes > EXEC_FILE_BACKED_METADATA_MAX - header.e_phoff) {
            return PrepareFileBackedElfResult::INVALID;
        }
        out.task_metadata_size = std::max(static_cast<size_t>(header.e_phoff + phdr_bytes), sizeof(Elf64_Ehdr));
        out.task_metadata = new uint8_t[out.task_metadata_size]{};
        if (out.task_metadata == nullptr) {
            return PrepareFileBackedElfResult::OUT_OF_MEMORY;
        }
        std::memcpy(out.task_metadata, &header, sizeof(header));
        std::memcpy(out.task_metadata + header.e_phoff, out.program_headers, static_cast<size_t>(phdr_bytes));
    }

    out.view.elf_header = header;
    out.view.program_headers = out.program_headers;
    out.view.section_headers = out.section_headers;
    out.view.section_names = out.section_names;
    out.read_context = {.file = file, .logical_size = file_size};
    out.view.read_at = exec_file_read_at;
    out.view.read_context = &out.read_context;
    out.view.logical_size = file_size;
    out.view.contiguous_base = nullptr;
    out.view.source_metadata_stable = INTERPRETER;
    return PrepareFileBackedElfResult::READY;
}

auto read_file_range_fully(vfs::File* fd, uint8_t* dst, size_t file_size, uint64_t offset, uint64_t size, const char* path,
                           size_t& bytes_read) -> int;

auto exec_lazy_file_page_range(const Elf64_Phdr& ph, uint64_t page_no, uint64_t& file_offset_out, uint64_t& vaddr_out) -> bool {
    if (ph.p_type != PT_LOAD || (ph.p_flags & PF_W) != 0U || ph.p_filesz == 0 || ph.p_memsz < ph.p_filesz || page_no == 0) {
        return false;
    }

    uint64_t const FIRST_PAGE_OFFSET = ph.p_vaddr & (ker::mod::mm::virt::PAGE_SIZE - 1);
    uint64_t const ALIGNED_START_VA = ph.p_vaddr & ~(ker::mod::mm::virt::PAGE_SIZE - 1);
    uint64_t const PAGE_VA = ALIGNED_START_VA + (page_no * ker::mod::mm::virt::PAGE_SIZE);
    uint64_t const DST_IN_PAGE = (page_no == 0) ? FIRST_PAGE_OFFSET : 0;
    uint64_t const ROOM_IN_PAGE = ker::mod::mm::virt::PAGE_SIZE - DST_IN_PAGE;

    uint64_t bytes_before_this_page = 0;
    if (page_no != 0) {
        bytes_before_this_page = (ker::mod::mm::virt::PAGE_SIZE - FIRST_PAGE_OFFSET) + ((page_no - 1) * ker::mod::mm::virt::PAGE_SIZE);
    }
    if (bytes_before_this_page >= ph.p_filesz) {
        return false;
    }

    uint64_t const REMAINING_IN_FILE = ph.p_filesz - bytes_before_this_page;
    uint64_t const COPY_SIZE = REMAINING_IN_FILE < ROOM_IN_PAGE ? REMAINING_IN_FILE : ROOM_IN_PAGE;
    if (DST_IN_PAGE != 0 || COPY_SIZE != ker::mod::mm::virt::PAGE_SIZE || ph.p_offset > UINT64_MAX - bytes_before_this_page) {
        return false;
    }

    file_offset_out = ph.p_offset + bytes_before_this_page;
    vaddr_out = PAGE_VA;
    return true;
}

auto exec_pt_load_page_overlaps_other_segment(const Elf64_Phdr* program_headers, Elf64_Half count, Elf64_Half owner_index,
                                              uint64_t page_vaddr) -> bool {
    if (program_headers == nullptr) {
        return false;
    }
    uint64_t const PAGE_END = page_vaddr + ker::mod::mm::virt::PAGE_SIZE;
    for (Elf64_Half i = 0; i < count; ++i) {
        const Elf64_Phdr& ph = program_headers[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (i == owner_index || ph.p_type != PT_LOAD || ph.p_memsz == 0) {
            continue;
        }
        uint64_t const START = ph.p_vaddr & ~(ker::mod::mm::virt::PAGE_SIZE - 1);
        uint64_t const END = (ph.p_vaddr + ph.p_memsz + ker::mod::mm::virt::PAGE_SIZE - 1) & ~(ker::mod::mm::virt::PAGE_SIZE - 1);
        if (page_vaddr < END && PAGE_END > START) {
            return true;
        }
    }
    return false;
}

auto read_exec_load_segment_for_loader(vfs::File* fd, uint8_t* dst, size_t file_size, const Elf64_Phdr& ph,
                                       const Elf64_Phdr* program_headers, Elf64_Half ph_count, Elf64_Half ph_index, const char* path,
                                       bool allow_lazy_file_pages, size_t& bytes_read) -> int {
    if (!allow_lazy_file_pages || ph.p_type != PT_LOAD || (ph.p_flags & PF_W) != 0U || ph.p_filesz == 0 || ph.p_memsz < ph.p_filesz) {
        return read_file_range_fully(fd, dst, file_size, ph.p_offset, ph.p_filesz, path, bytes_read);
    }

    uint64_t const SEG_END = ph.p_vaddr + ph.p_memsz;
    uint64_t const START_PAGE_ADDR = ph.p_vaddr & ~(ker::mod::mm::virt::PAGE_SIZE - 1);
    uint64_t const END_PAGE_ADDR = (SEG_END + ker::mod::mm::virt::PAGE_SIZE - 1) & ~(ker::mod::mm::virt::PAGE_SIZE - 1);
    size_t const NUM_PAGES = (END_PAGE_ADDR - START_PAGE_ADDR) / ker::mod::mm::virt::PAGE_SIZE;
    uint64_t const FIRST_PAGE_OFFSET = ph.p_vaddr & (ker::mod::mm::virt::PAGE_SIZE - 1);

    for (uint64_t page_no = 0; page_no < NUM_PAGES; ++page_no) {
        uint64_t bytes_before_this_page = 0;
        if (page_no != 0) {
            bytes_before_this_page = (ker::mod::mm::virt::PAGE_SIZE - FIRST_PAGE_OFFSET) + ((page_no - 1) * ker::mod::mm::virt::PAGE_SIZE);
        }
        if (bytes_before_this_page >= ph.p_filesz) {
            continue;
        }

        uint64_t const DST_IN_PAGE = (page_no == 0) ? FIRST_PAGE_OFFSET : 0;
        uint64_t const ROOM_IN_PAGE = ker::mod::mm::virt::PAGE_SIZE - DST_IN_PAGE;
        uint64_t const REMAINING_IN_FILE = ph.p_filesz - bytes_before_this_page;
        uint64_t const COPY_SIZE = REMAINING_IN_FILE < ROOM_IN_PAGE ? REMAINING_IN_FILE : ROOM_IN_PAGE;
        uint64_t lazy_file_offset = 0;
        uint64_t lazy_vaddr = 0;
        if (exec_lazy_file_page_range(ph, page_no, lazy_file_offset, lazy_vaddr) &&
            !exec_pt_load_page_overlaps_other_segment(program_headers, ph_count, ph_index, lazy_vaddr)) {
            continue;
        }

        if (ph.p_offset > UINT64_MAX - bytes_before_this_page) {
            return -EINVAL;
        }
        int const RET = read_file_range_fully(fd, dst, file_size, ph.p_offset + bytes_before_this_page, COPY_SIZE, path, bytes_read);
        if (RET < 0) {
            return RET;
        }
    }
    return 0;
}

void release_lazy_file_refs(LazyVmemRangeVec& ranges) { ker::mod::sched::task::release_lazy_vmem_range_refs(ranges); }

auto append_exec_lazy_file_ranges(LazyVmemRangeVec& out, const loader::elf::ElfLazyLoadRangeVec& loader_ranges, vfs::File* file,
                                  const vfs::Stat& st) -> bool {
    if (file == nullptr) {
        return loader_ranges.empty();
    }
    for (const auto& loader_range : loader_ranges) {
        if (loader_range.size == 0 || loader_range.vaddr > UINT64_MAX - loader_range.size) {
            return false;
        }
        LazyVmemRange const RANGE{.start = loader_range.vaddr,
                                  .end = loader_range.vaddr + loader_range.size,
                                  .prot = loader_range.prot,
                                  .flags = loader_range.flags,
                                  .kind = LazyVmemKind::FILE_BACKED,
                                  .file = file,
                                  .file_offset = loader_range.file_offset,
                                  .file_dev = st.st_dev,
                                  .file_ino = st.st_ino,
                                  .file_size = st.st_size > 0 ? static_cast<uint64_t>(st.st_size) : 0,
                                  .file_mtime_sec = st.st_mtim.tv_sec,
                                  .file_mtime_nsec = st.st_mtim.tv_nsec,
                                  .file_ctime_sec = st.st_ctim.tv_sec,
                                  .file_ctime_nsec = st.st_ctim.tv_nsec};
        if (!out.push_back(RANGE)) {
            return false;
        }
        vfs::vfs_retain_file(file);
    }
    return true;
}

void swap_exec_lazy_ranges(ker::mod::sched::task::Task* task, LazyVmemRangeVec& new_ranges, LazyVmemRangeVec& old_ranges) {
    if (task == nullptr) {
        return;
    }

    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    old_ranges = std::move(task->lazy_vmem_ranges);
    task->lazy_vmem_ranges = std::move(new_ranges);
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
}

auto prepare_execve_file_backed_elf(const ker::mod::sched::task::Task& task, vfs::File* file, size_t file_size, PreparedFileBackedElf& out,
                                    bool lazy_file_segments) -> PrepareFileBackedElfResult {
    if (file_size <= EXEC_SPARSE_ELF_MIN_SIZE || file_size >= EXEC_FILE_BACKED_ELF_MIN_SIZE || !lazy_file_segments || file == nullptr ||
        file->exec_read_view == nullptr || task.wki_prefer_inline) {
        return prepare_file_backed_elf(file, file_size, out);
    }

    // This image is already pinned and too large for inline WKI delivery.
    // Reuse bounded metadata instead of reserving a full logical-file buffer.
    // Keep preferred-inline and non-lazy execution on their existing paths.
    auto const RESULT = prepare_file_backed_elf(file, file_size, out, mod::mm::user_layout::ImageRole::MAIN, EXEC_SPARSE_ELF_MIN_SIZE + 1);
    auto const& HEADER = out.view.elf_header;
    if (RESULT == PrepareFileBackedElfResult::READY && (HEADER.e_phoff % alignof(Elf64_Phdr)) == 0U &&
        (HEADER.e_shnum == 0 || (HEADER.e_shoff % alignof(Elf64_Shdr)) == 0U)) {
        // The syscall-local scratch outlives synchronous loading. Let the
        // debug registry copy its names/rows before that scratch is retired.
        out.view.source_metadata_stable = true;
        return RESULT;
    }

    // The optional compact path must not introduce a rejection for metadata
    // accepted by the original parser, or keep partial scratch on fallback.
    out.reset();
    return PrepareFileBackedElfResult::FALLBACK;
}

// A synchronous, borrowed view of a fully prepared native image. It contains
// no process identity, kernel stack, syscall scratch, or scheduler ownership.
// The preparation owner retains every pointer until commit returns.
// This view is intentionally non-assignable; its ownership slots cannot be reseated.
// NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members)
struct NativeExecImage {
    uint64_t entry;
    uint64_t program_header_addr;
    uint64_t elf_header_addr;
    uint64_t image_load_base;
    uint64_t image_vaddr_start;
    uint64_t image_vaddr_end;
    uint16_t program_header_count;
    uint16_t program_header_ent_size;
    uint8_t* elf_buffer;
    size_t elf_buffer_size;
    bool elf_buffer_complete;
    vfs::File* exec_image_file;
    uint64_t exec_image_size;
    uint64_t interp_base;
    uint64_t interp_vaddr_start;
    uint64_t interp_vaddr_end;
    uint64_t at_random_addr;
    uint64_t mmap_next;
    char*& name;
    const char* path;
    uint64_t debug_staging_pid;
    uint64_t initial_rip;
    uint64_t initial_rsp;
    const ker::mod::cpu::GPRegs& registers;
    ker::mod::mm::paging::PageTable* pagemap;
    ker::mod::sched::threading::Thread* thread;
    LazyVmemRangeVec& lazy_ranges;
    const vfs::Stat& stat;
    bool nosuid{};
    ker::mod::mm::numa::Policy memory_policy{};
    bool one_shot_wki_target{};
    ker::mod::security::Snapshot admitted_credentials{};
    ker::mod::security::Snapshot stack_credentials{};
};
// NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

// Detached old storage stays in the task-owned exec transaction until cleanup.
// Its pagemap cannot be reclaimed before activation of the replacement root.
struct RetiredExecImage {
    uint8_t* elf_buffer{};
    vfs::File* exec_image_file{};
    ker::mod::sched::threading::Thread* thread{};
    const char* name{};
    ker::mod::mm::paging::PageTable* pagemap{};
    bool pagemap_has_other_publishers{};
    LazyVmemRangeVec lazy_ranges;
};

}  // namespace

// Worker candidates own storage independently of the preparing stack. The
// synchronous canonical owner may borrow its current immutable syscall input.
// Neither form replaces the current Task image before canonical cutover.
struct RemoteExecPlan {
    using Task = ker::mod::sched::task::Task;
    ker::mod::sched::resource::ExecAdmission admission;
    ker::mod::security::Snapshot credentials;
    ker::mod::security::Snapshot replacement_credentials;
    ker::mod::security::ExecIdentity image_identity;
    ker::net::wki::TaskExecCredentialTransition credential_transition;
    ker::net::wki::TaskExecSignals signals{};
    Task* actor{};
    WorkerExecHandoff* handoff{};
    uint8_t* bytes{};
    size_t size{};
    bool complete{};
    const char** argv{};
    const char** envp{};
    char* strings{};
    // Only a synchronous exec by its canonical owner may borrow its current
    // immutable request/loader storage. Worker handoffs keep independent copies.
    bool uses_current_input{};
    std::span<const uint8_t> current_bytes{};
    const char* const* current_argv{};
    const char* const* current_envp{};
    std::array<const char*, 1> empty_arguments{};
    std::array<char, Task::EXE_PATH_MAX> path{};
    Task::FilesystemContext filesystem;
    std::array<char, Task::WKI_TARGET_HOSTNAME_MAX> target{};
    uint32_t target_flags{};
    bool committed{};

    ~RemoteExecPlan() {
        admission.finish();
        filesystem.root_anchor = {};
        filesystem.cwd_anchor = {};
        if (actor != nullptr) actor->release();
        delete[] bytes;
        delete[] argv;
        delete[] envp;
        delete[] strings;
    }

    static auto measure_arguments(const char* const* input_argv, const char* const* input_envp, size_t& argc, size_t& envc, size_t& total)
        -> bool {
        argc = envc = total = 0;
        auto measure = [&](const char* const* input, size_t& count) {
            if (input == nullptr) return true;
            while (input[count] != nullptr) {
                size_t const LENGTH = strnlen(input[count], EXEC_ARG_BYTES_MAX);
                if (LENGTH == EXEC_ARG_BYTES_MAX || total > EXEC_ARG_BYTES_MAX - LENGTH - 1 || count == EXEC_ARG_BYTES_MAX / sizeof(char*))
                    return false;
                total += LENGTH + 1;
                ++count;
            }
            return true;
        };
        if (!measure(input_argv, argc) || !measure(input_envp, envc) || (argc + envc + 2) > (EXEC_ARG_BYTES_MAX - total) / sizeof(char*))
            return false;
        return true;
    }

    auto copy_arguments(const char* const* input_argv, const char* const* input_envp) -> bool {
        size_t argc = 0, envc = 0, total = 0;
        if (!measure_arguments(input_argv, input_envp, argc, envc, total)) return false;
        argv = new (std::nothrow) const char*[argc + 1]{};
        envp = new (std::nothrow) const char*[envc + 1]{};
        strings = new (std::nothrow) char[total != 0 ? total : 1];
        if (argv == nullptr || envp == nullptr || strings == nullptr) return false;
        char* cursor = strings;
        auto copy = [&](const char* const* input, const char** output, size_t count) {
            for (size_t i = 0; i < count; ++i) {
                size_t const LENGTH = std::strlen(input[i]) + 1;
                std::memcpy(cursor, input[i], LENGTH);
                output[i] = cursor;
                cursor += LENGTH;
            }
        };
        copy(input_argv, argv, argc);
        copy(input_envp, envp, envc);
        return true;
    }

    auto borrow_current_input(const uint8_t* elf, size_t elf_size, const char* const* input_argv, const char* const* input_envp) -> bool {
        size_t argc = 0, envc = 0, total = 0;
        if (!measure_arguments(input_argv, input_envp, argc, envc, total)) return false;
        uses_current_input = true;
        current_bytes = {elf, elf_size};
        current_argv = input_argv != nullptr ? input_argv : empty_arguments.data();
        current_envp = input_envp != nullptr ? input_envp : empty_arguments.data();
        return true;
    }

    auto arguments() const -> const char* const* { return uses_current_input ? current_argv : argv; }
    auto environment() const -> const char* const* { return uses_current_input ? current_envp : envp; }

    auto candidate() const -> ker::net::wki::WkiRemoteExecCandidate {
        return {.path = path.data(),
                .bytes = uses_current_input ? current_bytes : std::span<const uint8_t>{bytes, size},
                .complete = complete,
                .target = target.data(),
                .target_flags = target_flags};
    }

    // Caller has passed every rejecting cutover check under the VM publication
    // guard. Publish text/routing alongside the replacement credentials.
    void publish_metadata(Task& owner) const {
        uint64_t const FLAGS = owner.path_metadata_lock.lock_irqsave();
        owner.exe_path = path;
        owner.wki_target_hostname = target;
        owner.wki_target_flags = target_flags;
        owner.wki_skip_legacy_placement = true;
        consume_successful_one_shot_wki_target(&owner, ker::net::wki::WkiRemoteSpawnResult::REMOTE);
        owner.path_metadata_lock.unlock_irqrestore(FLAGS);
    }
};

// The waiting worker retains its request and CPU contribution until result is
// published. After acknowledgement the owner must never dereference this row.
struct WorkerExecHandoff {
    ~WorkerExecHandoff();
    bool remote{};
    ExecRequestArguments* remote_request{};
    ker::mod::sched::task::Task* worker{};
    NativeExecCompletion completion{};
    uint64_t signal_mask{};
    std::atomic<int> result{-EINPROGRESS};
};

// Native exec publishes this owner before faulting usercopy. Recursion never
// owns a forwarded vector or its inline strings only on an abandoned stack.
namespace {
// Every owner here survives an abandoned creation stack through the executor's
// existing arguments slot. A prepared child must retire before this root does.
struct NativeCompartmentUserInputs {
    NativeCompartmentSpec spec;
#ifdef WOS_SELFTEST
    NativeConstructionToken fault_token{};
#endif
    std::array<char, abi::compartment::PATH_BYTES_MAX> executable{};
    std::array<std::array<char, abi::compartment::PATH_BYTES_MAX>, abi::compartment::ASSET_MAX> paths{};
    std::array<NativeCompartmentAsset, abi::compartment::ASSET_MAX> assets{};
    std::array<NativeCompartmentDescriptor, abi::compartment::DESCRIPTOR_MAX> descriptors{};
    std::array<abi::compartment::Asset, abi::compartment::ASSET_MAX> asset_input{};
    std::array<abi::compartment::Descriptor, abi::compartment::DESCRIPTOR_MAX> descriptor_input{};
    std::array<abi::compartment::String, abi::compartment::VECTOR_MAX> argv_input{}, envp_input{};
    std::array<std::string_view, abi::compartment::VECTOR_MAX> argv{}, envp{};
    std::unique_ptr<char[]> strings;
    ~NativeCompartmentUserInputs() {
        for (auto& asset : assets)
            if (asset.source != nullptr) vfs::vfs_put_file(asset.source);
        for (auto& descriptor : descriptors)
            if (descriptor.source != nullptr) vfs::vfs_put_file(descriptor.source);
    }
};
struct NativeCompartmentPreparation {
    static constexpr size_t ASSET_MAX = 64;
    static constexpr size_t DESCRIPTOR_MAX = 256;
    struct Asset {
        vfs::File* source{};
        std::array<char, EXEC_PATH_MAX> path{};
        uint32_t mode{};
        vfs::Stat stat{};
    };
    struct Descriptor {
        vfs::File* source{};
        unsigned destination{};
    };
    mod::sched::task::Task& executor;
#ifdef WOS_SELFTEST
    NativeConstructionToken fault_token{};
#endif
    mod::sched::task::Task* creator{};
    mod::sched::task::Task* child{};
    int actor_nice{};
    int creator_nice{};
    int inherited_nice{};
    uint32_t inherited_weight{};
    bool priority_initialized{};
    mod::sched::resource::NativeCreatorPolicy policy;
    mod::sched::resource::State resources;
    std::unique_ptr<mod::sched::resource::ProcessReservation> reservation;
    NativeProcessContext context;
    vfs::PreparedMount mount;
    std::unique_ptr<mod::sched::task::Task> populator;
    vfs::File* root_pin{};
    vfs::DirectoryViewRef view;
    vfs::PreparedFilesystemContext filesystem;
    std::array<Asset, ASSET_MAX> assets{};
    std::array<Descriptor, DESCRIPTOR_MAX> descriptors{};
    size_t asset_count{};
    size_t descriptor_count{};
    std::unique_ptr<uint8_t[]> buffer;
    vfs::File* target{};
    vfs::File* executable{};
    vfs::Stat executable_stat{};
    uint64_t kernel_rsp{};
    std::array<std::string_view, DESCRIPTOR_MAX> argv{}, envp{};
    NativeProcessArguments arguments;
    uint64_t root_bytes{};
    bool published{};
    std::unique_ptr<NativeCompartmentUserInputs> user_inputs;
#ifdef WOS_SELFTEST
    explicit NativeCompartmentPreparation(mod::sched::task::Task& owner, NativeConstructionToken token)
        : executor(owner), fault_token(token) {
        context.fault_token = token;
    }
#else
    explicit NativeCompartmentPreparation(mod::sched::task::Task& owner) : executor(owner) {}
#endif
    ~NativeCompartmentPreparation();
};

// Nice belongs to the executing Task, while policy and lifetime belong to its
// exact canonical process. Call with VM publication held and no spinlocks.
auto creation_priority_source_status(mod::sched::task::Task& actor, mod::sched::task::Task* creator) -> int {
    using namespace ker::mod;
    if (creator == nullptr || actor.credential_owner() != creator || creator->is_thread || creator->credential_owner() != creator ||
        actor.type != sched::task::TaskType::PROCESS || creator->type != sched::task::TaskType::PROCESS)
        return -ESRCH;
    for (auto* subject : {&actor, creator}) {
        if (subject->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE || subject->has_exited ||
            subject->exit_in_progress.load(std::memory_order_acquire) || subject->exit_notify_ready.load(std::memory_order_acquire) ||
            subject->gc_queued.load(std::memory_order_acquire))
            return -ECANCELED;
    }
    if (actor.is_thread && actor.pagemap != creator->pagemap) return -ESRCH;
    return 0;
}

auto capture_native_creation_priority(NativeCompartmentPreparation& root) -> int {
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    if (int const STATUS = creation_priority_source_status(root.executor, root.creator); STATUS != 0) return STATUS;
    int const ACTOR = root.executor.sched_nice;
    int const CREATOR = root.creator->sched_nice;
    if (ACTOR < -20 || ACTOR > 19 || CREATOR < -20 || CREATOR > 19) return -EINVAL;
    root.actor_nice = ACTOR;
    root.creator_nice = CREATOR;
    root.inherited_nice = std::max(ACTOR, CREATOR);
    return 0;
}

// VM publication is already held by initialization/final publication. A copied
// worker field cannot replace the retained canonical source or its current image.
auto revalidate_native_creation_priority(const NativeCompartmentPreparation& root) -> int {
    if (int const STATUS = creation_priority_source_status(root.executor, root.creator); STATUS != 0) return STATUS;
    return root.executor.sched_nice == root.actor_nice && root.creator->sched_nice == root.creator_nice ? 0 : -EAGAIN;
}

auto initialize_native_creation_priority(NativeCompartmentPreparation& root, mod::sched::task::Task& child) -> int {
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    if (int const STATUS = revalidate_native_creation_priority(root); STATUS != 0) return STATUS;
    int const STATUS = mod::sched::initialize_unpublished_task_priority(root.executor, child, root.inherited_nice);
    if (STATUS == 0) {
        root.inherited_weight = child.sched_weight;
        root.priority_initialized = true;
    }
    return STATUS;
}
}  // namespace

class ExecRequestArguments {
   public:
    ExecArgumentsSnapshot snapshot;
    ker::mod::security::Snapshot admitted_credentials;
    ker::mod::mm::numa::Policy memory_policy{};
    NativeExecCompletion completion;
    ExecTraceState trace;
    WorkerExecHandoff handoff;
    std::unique_ptr<RemoteExecPlan> remote;
    uint32_t remote_proxy_id{};
    bool remote_worker_committed{};
    std::unique_ptr<NativeCompartmentPreparation> compartment;
    bool native_compartment_request{};
#ifdef WOS_SELFTEST
    NativeConstructionToken fault_token{};
#endif

    // Scratch for each recursive native loader invocation. Persistent executable
    // bytes leave prepared_elf through take_task_metadata(); pagemap/Thread belong
    // to the image transaction. Native construction also owns its interpreter
    // File here; ordinary exec keeps that File in its existing transaction.
    struct NativeScratch {
#ifdef WOS_SELFTEST
        NativeConstructionToken fault_token{};
#endif
        PreparedFileBackedElf prepared_elf;
        PreparedFileBackedElf prepared_interp;
        uint8_t* interp_buf{};
        vfs::File* interp_file{};
        loader::elf::ElfLazyLoadRangeVec main_lazy_ranges;
        loader::elf::ElfLazyLoadRangeVec interp_lazy_ranges;
        uint64_t* argv_addrs{};
        uint64_t* envp_addrs{};
        size_t argv_count{};
        size_t envp_count{};
        void reset_interp_storage() {
            prepared_interp.reset();
            delete[] std::exchange(interp_buf, nullptr);
            if (auto* file = std::exchange(interp_file, nullptr); file != nullptr) vfs::vfs_put_file(file);
        }
        ~NativeScratch() {
            reset_interp_storage();
            delete[] argv_addrs;
            delete[] envp_addrs;
        }
    };

    auto native_scratch(int depth) -> NativeScratch* {
        if (depth < 0 || depth > MAX_SHEBANG_DEPTH) return nullptr;
        return &m_native_scratch.at(static_cast<size_t>(depth));
    }

    auto prepare_shebang(const char* script_path, const char* const* argv, size_t argv_count, const ShebangInfo& shebang, int depth,
                         const char*& interpreter, const char**& forwarded) -> int {
        if (depth < 0 || depth >= MAX_SHEBANG_DEPTH) return -ELOOP;
        auto& frame = m_frames.at(static_cast<size_t>(depth));
        if (frame.argv != nullptr) return -EBUSY;
        int const PATH_RESULT = copy_exec_path(script_path, frame.script_path);
        if (PATH_RESULT < 0) return PATH_RESULT;
        frame.shebang = shebang;
        size_t const COUNT = 2 + (argv_count > 0 ? argv_count - 1 : 0) + (shebang.has_argument ? 1 : 0);
        frame.argv = new (std::nothrow) const char*[COUNT + 1];
        if (frame.argv == nullptr) return -ENOMEM;
        size_t index = 0;
        frame.argv[index++] = frame.shebang.interpreter.data();
        if (frame.shebang.has_argument) frame.argv[index++] = frame.shebang.argument.data();
        frame.argv[index++] = frame.script_path.data();
        for (size_t i = 1; i < argv_count; ++i) frame.argv[index++] = argv[i];
        frame.argv[index] = nullptr;
        interpreter = frame.shebang.interpreter.data();
        forwarded = frame.argv;
        return 0;
    }

#ifdef WOS_SELFTEST
    uint32_t* selftest_destructions = nullptr;
    ~ExecRequestArguments() {
        if (selftest_destructions != nullptr) ++*selftest_destructions;
    }
#endif

   private:
    struct ShebangFrame {
        ShebangInfo shebang;
        std::array<char, EXEC_PATH_MAX> script_path{};
        const char** argv{};
        ~ShebangFrame() { delete[] argv; }
    };
    std::array<ShebangFrame, MAX_SHEBANG_DEPTH> m_frames;
    std::array<NativeScratch, MAX_SHEBANG_DEPTH + 1> m_native_scratch;
};

WorkerExecHandoff::~WorkerExecHandoff() { delete remote_request; }

// Owns prepared/retired storage and temporary admission independently of the
// preparing stack. The Task slot survives publication through final cleanup.
// VFS/MM releases may block during normal cleanup or ACTIVE executor exit.
// This is not a cross-task handoff protocol.
class PreparedNativeExecImage {
   public:
    ExecTraceState trace;
    PreparedNativeExecImage(const NativeExecImage& image, uint64_t origin_pid)
        : m_name(std::exchange(image.name, nullptr)),
          m_registers(image.registers),
          m_stat(image.stat),
          m_lazy_ranges(std::move(image.lazy_ranges)),
          m_origin_pid(origin_pid),
          m_image{
              .entry = image.entry,
              .program_header_addr = image.program_header_addr,
              .elf_header_addr = image.elf_header_addr,
              .image_load_base = image.image_load_base,
              .image_vaddr_start = image.image_vaddr_start,
              .image_vaddr_end = image.image_vaddr_end,
              .program_header_count = image.program_header_count,
              .program_header_ent_size = image.program_header_ent_size,
              .elf_buffer = image.elf_buffer,
              .elf_buffer_size = image.elf_buffer_size,
              .elf_buffer_complete = image.elf_buffer_complete,
              .exec_image_file = image.exec_image_file,
              .exec_image_size = image.exec_image_size,
              .interp_base = image.interp_base,
              .interp_vaddr_start = image.interp_vaddr_start,
              .interp_vaddr_end = image.interp_vaddr_end,
              .at_random_addr = image.at_random_addr,
              .mmap_next = image.mmap_next,
              .name = m_name,
              .path = m_path.data(),
              .debug_staging_pid = image.debug_staging_pid,
              .initial_rip = image.initial_rip,
              .initial_rsp = image.initial_rsp,
              .registers = m_registers,
              .pagemap = image.pagemap,
              .thread = image.thread,
              .lazy_ranges = m_lazy_ranges,
              .stat = m_stat,
              .nosuid = image.nosuid,
              .memory_policy = image.memory_policy,
              .one_shot_wki_target = image.one_shot_wki_target,
              .admitted_credentials = image.admitted_credentials,
              .stack_credentials = image.stack_credentials,
          } {
        // Native staging has already validated the complete bounded path.
        std::memcpy(m_path.data(), image.path, std::strlen(image.path) + 1);
    }
    ~PreparedNativeExecImage() { discard(); }
    PreparedNativeExecImage(const PreparedNativeExecImage&) = delete;
    auto operator=(const PreparedNativeExecImage&) -> PreparedNativeExecImage& = delete;

    [[nodiscard]] auto view() const -> const NativeExecImage& { return m_image; }
    [[nodiscard]] auto admission() -> ker::mod::sched::resource::ExecAdmission& { return m_admission; }
    [[nodiscard]] auto retired() -> RetiredExecImage& { return m_retired; }
    void release_after_commit() {
        if (m_phase != Phase::PREPARED) {
            ker::mod::dbg::panic_handler("exec image publication repeated");
            hcf();
        }
        m_phase = Phase::PUBLISHED;
    }
    // Called only after the replacement CR3/FPU/TLS state is active. Fatal
    // recovery stays deferred between publication and this transition.
    void mark_activated() {
        if (m_phase != Phase::PUBLISHED) {
            ker::mod::dbg::panic_handler("exec activation without image publication");
            hcf();
        }
        m_phase = Phase::ACTIVE;
    }
    void discard() {
        if (m_phase == Phase::DISCARDED) {
            return;
        }
        if (m_phase == Phase::PUBLISHED) {
            ker::mod::dbg::panic_handler("exec cleanup before replacement root activation");
            hcf();
        }
        auto const PHASE = std::exchange(m_phase, Phase::DISCARDED);
        if (PHASE == Phase::ACTIVE) {
            discard_retired();
            // Final shared state and old-image cleanup finish before reopening
            // temporary admission. Permanent process exit admission is separate.
            m_admission.finish();
            return;
        }
        // Failed publication leaves the original image intact and can admit
        // new workers while private preparation cleanup blocks.
        m_admission.finish();
        if (m_image.debug_staging_pid != 0) {
            loader::debug::unregister_process(m_image.debug_staging_pid);
        }
        if (m_image.exec_image_file != nullptr) {
            vfs::vfs_put_file(m_image.exec_image_file);
        }
        release_lazy_file_refs(m_lazy_ranges);
        if (m_image.pagemap != nullptr) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(m_image.pagemap);
            ker::mod::mm::virt::destroy_user_space(m_image.pagemap, m_origin_pid, m_name != nullptr ? m_name : m_path.data(),
                                                   "exec-prepared-image-abort");
            ker::mod::mm::virt::release_pagemap(m_image.pagemap, m_origin_pid);
        }
        if (m_image.thread != nullptr) {
            m_image.thread->tls_phys_ptr = 0;
            m_image.thread->stack_phys_ptr = 0;
            ker::mod::sched::threading::destroy_thread(m_image.thread);
        }
        delete[] m_name;
        m_name = nullptr;
        delete[] m_image.elf_buffer;
    }

   private:
    enum class Phase : uint8_t { PREPARED, PUBLISHED, ACTIVE, DISCARDED };

    void discard_retired() {
        delete[] m_retired.name;
        release_lazy_file_refs(m_retired.lazy_ranges);
        if (m_retired.elf_buffer != nullptr) {
            if (!ker::net::wki::wki_remote_compute_release_elf_buffer(m_retired.elf_buffer)) {
                delete[] m_retired.elf_buffer;
            }
        }
        if (m_retired.exec_image_file != nullptr) {
            vfs::vfs_put_file(m_retired.exec_image_file);
        }
        // Preserve last-publisher GC ownership of an old shared root. The
        // private replacement is already active and is never freed here.
        if (m_retired.pagemap != nullptr && m_retired.pagemap != m_image.pagemap && !m_retired.pagemap_has_other_publishers) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(m_retired.pagemap);
            ker::mod::mm::virt::destroy_user_space(m_retired.pagemap, m_origin_pid, m_path.data(), "exec-old-image");
            ker::mod::mm::virt::release_pagemap(m_retired.pagemap, m_origin_pid);
        }
        if (m_retired.thread != nullptr && m_retired.thread != m_image.thread) {
            m_retired.thread->tls_phys_ptr = 0;
            m_retired.thread->stack_phys_ptr = 0;
            ker::mod::sched::threading::destroy_thread(m_retired.thread);
        }
    }

    std::array<char, EXEC_PATH_MAX> m_path{};
    char* m_name{};
    ker::mod::cpu::GPRegs m_registers{};
    vfs::Stat m_stat{};
    LazyVmemRangeVec m_lazy_ranges;
    uint64_t m_origin_pid{};
    Phase m_phase = Phase::PREPARED;
    NativeExecImage m_image;
    RetiredExecImage m_retired;
    ker::mod::sched::resource::ExecAdmission m_admission;
};

namespace {
void cleanup_exec_preparation_with_status(ker::mod::sched::task::Task& task, int32_t trace_status) {
    bool expected = false;
    if (!task.exec_preparation_teardown_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }
    ExecTraceState image_trace;
    ExecTraceState argument_trace;
    auto* prepared = task.owned_exec_preparation.load(std::memory_order_acquire);
    if (prepared != nullptr) {
        image_trace = std::exchange(prepared->trace, {});
        // Keep recovery ownership published until every potentially blocking
        // release has finished. Reentrant fatal/group exits must stay pending.
        prepared->discard();
        if (task.owned_exec_preparation.exchange(nullptr, std::memory_order_acq_rel) != prepared) {
            ker::mod::dbg::panic_handler("exec preparation owner changed during teardown");
            hcf();
        }
        delete prepared;
    }
    auto* arguments = task.owned_exec_arguments.load(std::memory_order_acquire);
    if (arguments != nullptr) {
        argument_trace = std::exchange(arguments->trace, {});
        delete arguments;
        if (task.owned_exec_arguments.exchange(nullptr, std::memory_order_acq_rel) != arguments) {
            ker::mod::dbg::panic_handler("exec argument owner changed during teardown");
            hcf();
        }
    }
    // Releases and request destruction have finished while fatal recovery is
    // still deferred. Close the final substage before its enclosing attempts.
    image_trace.finish(&task, trace_status, WOS_PERF_CALLSITE());
    argument_trace.finish(&task, trace_status, WOS_PERF_CALLSITE());
    task.exec_preparation_teardown_in_progress.store(false, std::memory_order_release);
}
}  // namespace

void cleanup_exec_preparation(ker::mod::sched::task::Task& task) { cleanup_exec_preparation_with_status(task, -ECANCELED); }

namespace {
auto adopt_exec_arguments(ker::mod::sched::task::Task& task, std::unique_ptr<ExecRequestArguments>& arguments) -> bool {
    if (!arguments || task.exit_in_progress.load(std::memory_order_acquire) || task.has_exited ||
        task.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE ||
        task.exec_preparation_teardown_in_progress.load(std::memory_order_acquire))
        return false;
    ExecRequestArguments* expected = nullptr;
    if (!task.owned_exec_arguments.compare_exchange_strong(expected, arguments.get(), std::memory_order_acq_rel)) return false;
    static_cast<void>(arguments.release());
    return true;
}

auto adopt_exec_preparation(ker::mod::sched::task::Task& task, std::unique_ptr<PreparedNativeExecImage>& prepared) -> bool {
    if (!prepared || task.exit_in_progress.load(std::memory_order_acquire) || task.has_exited ||
        task.state.load(std::memory_order_acquire) != ker::mod::sched::task::TaskState::ACTIVE ||
        task.exec_preparation_teardown_in_progress.load(std::memory_order_acquire)) {
        return false;
    }
    PreparedNativeExecImage* expected = nullptr;
    if (!task.owned_exec_preparation.compare_exchange_strong(expected, prepared.get(), std::memory_order_acq_rel)) {
        return false;
    }
    // No blocking or cross-task access between publication and stack disarm.
    static_cast<void>(prepared.release());
    return true;
}

auto finish_native_exec_image(ker::mod::sched::task::Task* task, PreparedNativeExecImage* PREPARED, NativeExecCompletion completion,
                              WorkerExecHandoff* handoff = nullptr) -> uint64_t;

void acknowledge_worker_exec(ker::mod::sched::task::Task& owner, WorkerExecHandoff& handoff, int result) {
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    owner.active_exec_handoff.store(nullptr, std::memory_order_release);
    handoff.result.store(result, std::memory_order_release);
    owner.resource_lock.unlock_irqrestore(FLAGS);
}

struct RemoteExecOutcome {
    ker::net::wki::WkiRemoteSpawnResult result = ker::net::wki::WkiRemoteSpawnResult::FAILED;
    int error = -EAGAIN;
    WorkerExecHandoff* pending{};
};

auto prepare_remote_exec_owner(ker::mod::sched::task::Task& owner, void* context) -> int {
    auto& plan = *static_cast<RemoteExecPlan*>(context);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (owner.credentials() != plan.credentials) return -EAGAIN;
        if (!plan.admission.owns(owner) && !plan.admission.begin(owner)) return -EAGAIN;
        plan.image_identity.traced = owner.ptrace_traced;
        ker::mod::security::Snapshot before;
        int const ERROR = ker::mod::sched::resource::prepare_exec_credentials(owner, plan.image_identity, before,
                                                                              plan.replacement_credentials, &plan.credentials);
        if (ERROR != 0) return ERROR;
        plan.credential_transition = {
            .flags = static_cast<uint16_t>((plan.image_identity.nosuid ? ker::net::wki::WKI_EXEC_CREDENTIAL_NOSUID : 0) |
                                           (plan.image_identity.traced ? ker::net::wki::WKI_EXEC_CREDENTIAL_TRACED : 0)),
            .uid = plan.image_identity.uid,
            .gid = plan.image_identity.gid,
            .mode = plan.image_identity.mode & 07777U,
        };
    }
    if (!plan.admission.quiesce(owner)) return -EAGAIN;
    {
        // The remote wire contract has no interval timer state. Admission
        // excludes new setters while this snapshot rejects a lossy transfer.
        uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
        bool const TIMERS_ACTIVE = ker::mod::time::process_timer::has_active_locked(owner);
        owner.resource_lock.unlock_irqrestore(FLAGS);
        if (TIMERS_ACTIVE) return -EBUSY;
    }
    // Detached wait commits must publish their observation before the exec snapshot.
    if (!ker::net::wki::wki_child_wait_quiesce(owner)) return -EAGAIN;
    // An already-entered credential syscall may have finished during quiesce.
    // The submission must still carry the executable's admitted snapshot.
    if (owner.credentials() != plan.credentials) return -EAGAIN;
    plan.signals = ker::net::wki::wki_exec_signal_snapshot(*plan.actor);
    // The retained process owns the parent/child relationship, including when
    // a native worker supplies the replacement image's thread signal state.
    plan.signals.parent_death_signal = owner.parent_death_signal.load(std::memory_order_acquire);
    if (plan.actor != &owner) plan.signals.pending |= owner.signal_delivery_pending_bits();
    return 0;
}

void accept_remote_exec_owner(ker::mod::sched::task::Task& owner, void* context) {
    auto& plan = *static_cast<RemoteExecPlan*>(context);
    plan.committed = true;
    owner.main_thread_retired.store(false, std::memory_order_release);
    if (auto* handoff = std::exchange(plan.handoff, nullptr); handoff != nullptr) {
        // The original worker still owns loader/File cleanup. Let it unwind
        // and retire before waiting for all old-image CPU contributions.
        acknowledge_worker_exec(owner, *handoff, 1);
    }
    retire_exec_siblings(owner);
}

template <typename Wait>
void capture_remote_exec_cutover_signals(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& actor, uint32_t task_id,
                                         uint64_t remote_pid, ker::net::wki::TaskExecSignals& signals, Wait&& wait) {
    owner.publish_accepted_signal_route(task_id, remote_pid);
    // Positive-PID signal admission belongs to the exact target Task. The
    // execing worker's in-flight effects must also finish before its snapshot.
    // Its old TID retires here: only the owner matches SubmittedTask::local_task
    // and can forward signals to the replacement process.
    if (&actor != &owner) actor.publish_accepted_signal_route(task_id, remote_pid);
    while (!owner.local_signal_admissions_drained() || !actor.local_signal_admissions_drained()) wait();
    signals = ker::net::wki::wki_exec_signal_snapshot(actor);
    signals.parent_death_signal = owner.parent_death_signal.load(std::memory_order_acquire);
    if (&actor != &owner) signals.pending |= owner.signal_delivery_pending_bits();
}

auto cutover_remote_exec_owner(ker::mod::sched::task::Task& owner, uint32_t task_id, uint64_t remote_pid,
                               ker::net::wki::TaskExecSignals& signals, void* context) -> int {
    auto& plan = *static_cast<RemoteExecPlan*>(context);
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!plan.admission.owns(owner) || !plan.admission.quiesced(owner) || owner.ptrace_traced != plan.image_identity.traced)
            return -EAGAIN;
        int const ERROR = ker::mod::sched::resource::publish_exec_credentials(owner, plan.credentials, plan.replacement_credentials);
        if (ERROR != 0) return ERROR;
        plan.publish_metadata(owner);
    }
    plan.committed = true;
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::image_committed(owner, true);
#endif
    capture_remote_exec_cutover_signals(owner, *plan.actor, task_id, remote_pid, signals, [] { ker::mod::sched::kern_yield(); });
    accept_remote_exec_owner(owner, context);
    return 0;
}

auto run_remote_exec_owner(ker::mod::sched::task::Task& owner, ExecRequestArguments& request, WorkerExecHandoff* handoff)
    -> RemoteExecOutcome {
    auto& plan = *request.remote;
    bool const WORKER_HANDOFF = handoff != nullptr;
    plan.handoff = handoff;
    auto const CANDIDATE = plan.candidate();
    RemoteExecOutcome outcome;
    ker::net::wki::WkiRemoteSpawnSpec const SPEC{
        .argv = plan.arguments(),
        .envp = plan.environment(),
        .cwd = plan.filesystem.cwd.data(),
        .error_out = &outcome.error,
        .prepare = prepare_remote_exec_owner,
        .accepted = accept_remote_exec_owner,
        .cutover = cutover_remote_exec_owner,
        .context = &plan,
        .exec_signals = &plan.signals,
        .deferred_proxy_id = &request.remote_proxy_id,
        .filesystem = &plan.filesystem,
        .exec_credentials = &plan.credential_transition,
        .exec_candidate = &CANDIDATE,
    };
    outcome.result = ker::net::wki::wki_try_remote_spawn(&owner, SPEC);
    outcome.pending = plan.handoff;
    // No scheduler proxy identity yet: rollback/argument/actor ownership can
    // safely finish even if a cleanup later gains a blocking release.
    request.remote.reset();
    if (WORKER_HANDOFF && outcome.result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
        // Acceptance may already have released the worker and its request. The
        // claimed holder retains the stage until canonical submission finishes.
        for (auto& stage : request.trace.pending) {
            if (stage.active && stage.op == ker::mod::perf::WkiPerfLocalProcOp::REMOTE_SPAWN)
                end_local_proc_stage(&owner, stage.op, stage, static_cast<int32_t>(outcome.result), stage.bytes, WOS_PERF_CALLSITE());
        }
    }
    return outcome;
}

void publish_remote_exec_proxy(ker::mod::sched::task::Task& owner, uint32_t id) {
    owner.set_wki_proxy_task_id(id);
    owner.wki_proxy_task = true;
    owner.deferred_task_switch = true;
    owner.yield_switch = false;
    owner.set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
}

auto try_process_remote_exec(ker::mod::sched::task::Task& actor, const uint8_t* elf, size_t elf_size, bool complete, const char* path,
                             const char* const* argv, const char* const* envp, const ker::vfs::Stat& stat, bool nosuid, int& error)
    -> ker::net::wki::WkiRemoteSpawnResult {
    using ker::net::wki::WkiRemoteSpawnResult;
    using namespace ker::mod;
    error = 0;
    if ((actor.wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) return WkiRemoteSpawnResult::LOCAL;
    auto* request = actor.owned_exec_arguments.load(std::memory_order_acquire);
    auto* owner = actor.credential_owner();
    if (request == nullptr || owner == nullptr) {
        error = -ESRCH;
        return WkiRemoteSpawnResult::FAILED;
    }
    auto plan = std::unique_ptr<RemoteExecPlan>(new (std::nothrow) RemoteExecPlan);
    if (!plan) {
        error = -ENOMEM;
        return WkiRemoteSpawnResult::FAILED;
    }
    if (&actor == owner) {
        // The canonical owner's syscall does not return or release its input
        // until run_remote_exec_owner finishes. No worker can retire that input.
        if (!plan->borrow_current_input(elf, elf_size, argv, envp)) {
            error = -ENOMEM;
            return WkiRemoteSpawnResult::FAILED;
        }
    } else {
        if (!plan->copy_arguments(argv, envp)) {
            error = -ENOMEM;
            return WkiRemoteSpawnResult::FAILED;
        }
        plan->bytes = new (std::nothrow) uint8_t[elf_size];
        if (plan->bytes == nullptr) {
            error = -ENOMEM;
            return WkiRemoteSpawnResult::FAILED;
        }
        std::memcpy(plan->bytes, elf, elf_size);
    }
    if (!actor.try_acquire()) {
        error = -ENOMEM;
        return WkiRemoteSpawnResult::FAILED;
    }
    plan->actor = &actor;
    plan->credentials = request->admitted_credentials;
    plan->image_identity = {.uid = stat.st_uid,
                            .gid = stat.st_gid,
                            .set_uid = (stat.st_mode & 04000) != 0,
                            .set_gid = (stat.st_mode & 02000) != 0,
                            .nosuid = nosuid,
                            .mode = stat.st_mode};
    plan->size = elf_size;
    plan->complete = complete;
    std::memcpy(plan->path.data(), path, std::min(std::strlen(path), plan->path.size() - 1));
    if (!actor.capture_filesystem_context(plan->filesystem)) {
        error = -ESRCH;
        return WkiRemoteSpawnResult::FAILED;
    }
    plan->target = actor.wki_target_hostname;
    plan->target_flags = actor.wki_target_flags;
    if (&actor == owner) {
        request->remote = std::move(plan);
        auto const OUTCOME = run_remote_exec_owner(actor, *request, nullptr);
        error = OUTCOME.error;
        return OUTCOME.result;
    }
    auto holder = std::unique_ptr<ExecRequestArguments>(new (std::nothrow) ExecRequestArguments);
    if (!holder) {
        error = -ENOMEM;
        return WkiRemoteSpawnResult::FAILED;
    }
    holder->remote = std::move(plan);
    auto& handoff = request->handoff;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!holder->remote->admission.begin(actor)) {
            error = -EAGAIN;
            return WkiRemoteSpawnResult::FAILED;
        }
        uint64_t const LOCK = owner->resource_lock.lock_irqsave();
        bool const AVAILABLE = !owner->exit_in_progress.load(std::memory_order_acquire) && !owner->resource_cpu_admission_closed &&
                               owner->pending_exec_handoff.load(std::memory_order_relaxed) == nullptr &&
                               owner->active_exec_handoff.load(std::memory_order_relaxed) == nullptr &&
                               owner->owned_exec_arguments.load(std::memory_order_acquire) == nullptr &&
                               owner->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE;
        if (AVAILABLE) {
            handoff.remote = true;
            handoff.worker = &actor;
            handoff.remote_request = holder.release();
            owner->pending_exec_handoff.store(&handoff, std::memory_order_release);
        }
        owner->resource_lock.unlock_irqrestore(LOCK);
        if (!AVAILABLE) {
            error = -EAGAIN;
            return WkiRemoteSpawnResult::FAILED;
        }
    }
    sched::wake_task_for_signal(owner);
    sched::wake_task_from_event(owner);
    int result;
    {
        sched::resource::ExecSafeWait safe_wait(actor);
        while ((result = handoff.result.load(std::memory_order_acquire)) == -EINPROGRESS) sched::kern_yield();
        if (result != 1) safe_wait.wait_for_release();
    }
    request->remote_worker_committed = result == 1;
    // Optional remote placement can fall through to the existing native
    // preparation/handoff in this same request. Its waiter starts afresh.
    handoff.remote = false;
    handoff.result.store(-EINPROGRESS, std::memory_order_release);
    if (result < 0) {
        error = result;
        return WkiRemoteSpawnResult::FAILED;
    }
    return result == 1 ? WkiRemoteSpawnResult::REMOTE : WkiRemoteSpawnResult::LOCAL;
}

auto claim_pending_exec_handoff(ker::mod::sched::task::Task& owner) -> WorkerExecHandoff* {
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    auto* handoff = owner.pending_exec_handoff.load(std::memory_order_acquire);
    if (handoff == nullptr || owner.owned_exec_preparation.load(std::memory_order_acquire) != nullptr) {
        owner.resource_lock.unlock_irqrestore(FLAGS);
        return nullptr;
    }
    if (handoff->remote) {
        if (owner.owned_exec_arguments.load(std::memory_order_acquire) != nullptr) {
            owner.resource_lock.unlock_irqrestore(FLAGS);
            return nullptr;
        }
        auto* holder = std::exchange(handoff->remote_request, nullptr);
        if (handoff->worker != nullptr) {
            auto* worker_request = handoff->worker->owned_exec_arguments.load(std::memory_order_acquire);
            if (worker_request != nullptr) holder->trace = std::exchange(worker_request->trace, {});
        }
        owner.owned_exec_arguments.store(holder, std::memory_order_release);
    } else {
        auto* prepared = handoff->worker->owned_exec_preparation.exchange(nullptr, std::memory_order_acq_rel);
        owner.owned_exec_preparation.store(prepared, std::memory_order_release);
    }
    owner.pending_exec_handoff.store(nullptr, std::memory_order_release);
    owner.active_exec_handoff.store(handoff, std::memory_order_release);
    owner.resource_lock.unlock_irqrestore(FLAGS);
    return handoff;
}

auto handoff_worker_exec(ker::mod::sched::task::Task& worker, PreparedNativeExecImage& prepared, ExecRequestArguments& request)
    -> uint64_t {
    using namespace ker::mod;
    auto* owner = worker.credential_owner();
    if (owner == nullptr || owner == &worker) return static_cast<uint64_t>(-ESRCH);
    auto& handoff = request.handoff;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        security::Snapshot before, after;
        auto const& image = prepared.view();
        int const ERROR = sched::resource::prepare_exec_credentials(worker,
                                                                    {.uid = image.stat.st_uid,
                                                                     .gid = image.stat.st_gid,
                                                                     .set_uid = (image.stat.st_mode & 04000) != 0,
                                                                     .set_gid = (image.stat.st_mode & 02000) != 0,
                                                                     .nosuid = image.nosuid,
                                                                     .traced = owner->ptrace_traced,
                                                                     .mode = image.stat.st_mode},
                                                                    before, after, &image.admitted_credentials);
        if (ERROR != 0) return static_cast<uint64_t>(ERROR);
        // The canonical owner rendezvous excludes all old-image contributors
        // before any authority change, including this explicit waiting worker.
        if (!prepared.admission().begin(worker)) return static_cast<uint64_t>(-EAGAIN);
        uint64_t const FLAGS = owner->resource_lock.lock_irqsave();
        bool const AVAILABLE = !owner->exit_in_progress.load(std::memory_order_acquire) && !owner->resource_cpu_admission_closed &&
                               owner->pending_exec_handoff.load(std::memory_order_relaxed) == nullptr &&
                               owner->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE;
        if (AVAILABLE) {
            handoff.worker = &worker;
            handoff.completion = request.completion;
            handoff.signal_mask = worker.signal_mask_bits();
            owner->pending_exec_handoff.store(&handoff, std::memory_order_release);
        }
        owner->resource_lock.unlock_irqrestore(FLAGS);
        if (!AVAILABLE) return static_cast<uint64_t>(-EAGAIN);
    }
    sched::wake_task_for_signal(owner);
    sched::wake_task_from_event(owner);
    int result;
    {
        sched::resource::ExecSafeWait safe_wait(worker);
        while ((result = handoff.result.load(std::memory_order_acquire)) == -EINPROGRESS) {
            // Kernel wait frames defer fatal delivery. Owner exit cancels this row
            // before waiting for our CPU contribution; successful acceptance wakes
            // this explicit retirement path before owner sibling cleanup waits.
            sched::kern_yield();
        }
        // A later exec may start after this request was rejected. Withdraw
        // atomically with observing its rendezvous release before unwinding.
        if (result != 0) safe_wait.wait_for_release();
    }
    if (result == 0) {
        ker::syscall::multiproc::wos_thread_exit_current();
    }
    return static_cast<uint64_t>(result);
}

auto prepare_native_exec_quiescence(ker::mod::sched::task::Task& task, PreparedNativeExecImage& prepared) -> int {
    using namespace ker::mod;
    bool change = false;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        auto const& image = prepared.view();
        security::Snapshot before, after;
        int const ERROR = sched::resource::prepare_exec_credentials(task,
                                                                    {.uid = image.stat.st_uid,
                                                                     .gid = image.stat.st_gid,
                                                                     .set_uid = (image.stat.st_mode & 04000) != 0,
                                                                     .set_gid = (image.stat.st_mode & 02000) != 0,
                                                                     .nosuid = image.nosuid,
                                                                     .traced = task.ptrace_traced,
                                                                     .mode = image.stat.st_mode},
                                                                    before, after, &image.admitted_credentials);
        if (ERROR != 0) return ERROR;
        change = before.user().effective != after.user().effective || before.group().effective != after.group().effective;
        if (change && !prepared.admission().owns(task) && !prepared.admission().begin(task)) return -EAGAIN;
    }
    // Siblings may be unwinding syscalls that need the VM/action locks.
    if (change && !prepared.admission().quiesce(task)) return -EAGAIN;
    return 0;
}

// The subject is the current executor. This is image publication only: no
// CR3/TLS activation, sibling retirement, descriptor close, or user return.
// An asynchronous owner handoff also requires executor quiescence and tracked
// cancellation/fatal-exit ownership; prepared storage alone is insufficient.
auto commit_native_exec_image(ker::mod::sched::task::Task* task, PreparedNativeExecImage& prepared) -> int {
    using namespace ker::mod;
    auto const& image = prepared.view();
    auto& retired = prepared.retired();
    auto& exec_admission = prepared.admission();
    bool commit_published = false;
    int credential_error = 0;
    // Same lock order as sigaction's potentially faulting output copy.
    mod::sys::MutexGuard action_guard(task->signal_action_mutex());
    // Prevent a new CLONE_VM/thread publisher from appearing between the
    // sibling snapshot and the root swap. Existing siblings keep the old
    // address space alive; exec must never tear a shared root out from
    // under their execution or stable usercopy pins.
    ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
    retired.pagemap_has_other_publishers = mod::sched::task_has_live_pagemap_sibling(task);
    security::Snapshot before_credentials;
    security::Snapshot after_credentials;
    credential_error = sched::resource::prepare_exec_credentials(*task,
                                                                 {.uid = image.stat.st_uid,
                                                                  .gid = image.stat.st_gid,
                                                                  .set_uid = (image.stat.st_mode & 04000) != 0,
                                                                  .set_gid = (image.stat.st_mode & 02000) != 0,
                                                                  .nosuid = image.nosuid,
                                                                  .traced = task->ptrace_traced,
                                                                  .mode = image.stat.st_mode},
                                                                 before_credentials, after_credentials, &image.admitted_credentials);
    // The loader must see the same credentials that commit publishes. Tracing
    // can change set-ID suppression while the private stack is being built.
    if (credential_error == 0 && after_credentials != image.stack_credentials) credential_error = -EAGAIN;
    // Live credential-owner executors must remain in safe waits. Retained
    // non-CPU Task refs and separate CLONE_VM credential stores do not gain IDs.
    if (credential_error == 0 &&
        (task->is_thread || (task->resource_cpu_threads.load(std::memory_order_acquire) != 0 && !exec_admission.quiesced(*task))) &&
        (before_credentials.user().effective != after_credentials.user().effective ||
         before_credentials.group().effective != after_credentials.group().effective)) {
        credential_error = -EAGAIN;
    }
    if (credential_error == 0 && !exec_admission.owns(*task) && !exec_admission.begin(*task)) {
        credential_error = -EAGAIN;
    }
    if (credential_error == 0) {
        commit_published = loader::debug::publish_staged_process(image.debug_staging_pid, task->pid, image.name);
    }
    if (commit_published) {
        retired.elf_buffer = task->elf_buffer;
        retired.exec_image_file = task->exec_image_file;
        retired.thread = task->thread;
        task->mark_exec_completed();
        task->publish_memory_policy_snapshot(image.memory_policy);
        task->entry = image.entry;
        task->program_header_addr = image.program_header_addr;
        task->elf_header_addr = image.elf_header_addr;
        task->image_load_base = image.image_load_base;
        task->image_vaddr_start = image.image_vaddr_start;
        task->image_vaddr_end = image.image_vaddr_end;
        task->program_header_count = image.program_header_count;
        task->program_header_ent_size = image.program_header_ent_size;
        task->elf_buffer = image.elf_buffer;
        task->elf_buffer_size = image.elf_buffer_size;
        task->is_elf_buffer_shared = false;
        task->elf_buffer_complete = image.elf_buffer_complete;
        task->exec_image_file = image.exec_image_file;
        task->exec_image_size = image.exec_image_size;
        task->interp_base = image.interp_base;
        task->interp_vaddr_start = image.interp_vaddr_start;
        task->interp_vaddr_end = image.interp_vaddr_end;
        task->at_random_addr = image.at_random_addr;
        task->mmap_next.store(image.mmap_next, std::memory_order_relaxed);

        size_t path_len = std::strlen(image.path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        {
            uint64_t const PATH_IRQF = task->path_metadata_lock.lock_irqsave();
            task->exe_path.fill('\0');
            std::memcpy(task->exe_path.data(), image.path, path_len);
            retired.name = task->name;
            task->name = std::exchange(image.name, nullptr);

            int const PUBLISH_RESULT = sched::resource::publish_exec_credentials(*task, before_credentials, after_credentials);
            // Local placement is only a proposal until the image commits. Apply
            // the preparing actor's one-shot choice to the canonical payload,
            // including worker exec; every earlier refusal preserves routing.
            if (PUBLISH_RESULT == 0 && image.one_shot_wki_target) pin_completed_exec_locally(*task);
            task->path_metadata_lock.unlock_irqrestore(PATH_IRQF);
            if (PUBLISH_RESULT != 0) {
                dbg::panic_handler("exec credential publication changed under the VM publication guard");
                hcf();
            }
        }

        task->context.frame.rip = image.initial_rip;
        task->context.frame.rsp = image.initial_rsp;
        task->context.frame.ss = 0x1b;
        task->context.frame.cs = 0x23;
        task->context.frame.flags = 0x202;
        task->context.frame.int_num = 0;
        task->context.frame.err_code = 0;
        ker::mod::sys::context_switch::record_saved_frame_class(task, task->context.frame,
                                                                ker::mod::sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);

        // Match the fresh-process entry contract used by wos_asm_enter_usermode:
        // startup code consumes argc/argv/envp from the initial stack, not GPRs.
        task->context.regs = image.registers;

        retired.pagemap = task->replace_pagemap_after_usercopy_quiescence(image.pagemap);
        task->thread = image.thread;
        mod::sched::threading::record_stack_usage(*task);
        swap_exec_lazy_ranges(task, image.lazy_ranges, retired.lazy_ranges);
        // The Task owns every new-image resource before commit locks release.
        // Exit recovery may remove the wrapper but must never discard this image.
        prepared.release_after_commit();
        (void)mod::sched::resource::observe_address_space(*task);
#ifdef WOS_SELFTEST
        ker::test::lifecycle60::image_committed(*task, false);
#endif
    }
    if (commit_published) {
        return 0;
    }
    return credential_error != 0 ? credential_error : -EIO;
}

auto read_file_range_fully(vfs::File* fd, uint8_t* dst, size_t file_size, uint64_t offset, uint64_t size, const char* path,
                           size_t& bytes_read) -> int {
    if (dst == nullptr || !exec_range_in_file(file_size, offset, size)) {
        return -EINVAL;
    }

    uint64_t total = 0;
    int consecutive_errors = 0;
    constexpr int MAX_CONSECUTIVE_ERRORS = 3;

    while (total < size) {
        uint64_t const READ_OFFSET = offset + total;
        auto const REMAINING = static_cast<size_t>(size - total);
        auto* out = dst + static_cast<size_t>(READ_OFFSET);
        ssize_t const RC = vfs::vfs_pread_file(fd, out, REMAINING, static_cast<off_t>(READ_OFFSET));
        if (RC > 0) {
            total += static_cast<uint64_t>(RC);
            bytes_read += static_cast<size_t>(RC);
            consecutive_errors = 0;
            continue;
        }
        if (RC == 0) {
            exec_log::warn("exec: unexpected EOF while reading '%s' at offset %llu (%llu/%llu bytes)", path,
                           static_cast<unsigned long long>(READ_OFFSET), static_cast<unsigned long long>(total),
                           static_cast<unsigned long long>(size));
            return -EIO;
        }

        consecutive_errors++;
        if (consecutive_errors >= MAX_CONSECUTIVE_ERRORS) {
            exec_log::warn("exec: range read failed for '%s' at offset %llu (%llu/%llu bytes) rc=%lld after %d attempts", path,
                           static_cast<unsigned long long>(READ_OFFSET), static_cast<unsigned long long>(total),
                           static_cast<unsigned long long>(size), static_cast<long long>(RC), MAX_CONSECUTIVE_ERRORS);
            return static_cast<int>(RC);
        }
    }

    return 0;
}

auto read_full_exec_image(vfs::File* fd, uint8_t* dst, size_t file_size, const char* path) -> ExecImageReadResult {
    ssize_t const BYTES_READ = read_file_fully(fd, dst, file_size, path);
    if (BYTES_READ < 0) {
        return {.bytes_read = 0, .status = static_cast<int>(BYTES_READ)};
    }
    if (std::cmp_not_equal(BYTES_READ, file_size)) {
        return {.bytes_read = BYTES_READ, .status = -EIO};
    }
    return {
        .bytes_read = BYTES_READ,
        .status = 0,
        .shebang_probe_size = file_size,
    };
}

auto read_exec_section_if_needed(vfs::File* fd, uint8_t* dst, size_t file_size, const Elf64_Shdr& section, const char* section_name,
                                 const char* path, bool read_relocation_metadata, size_t& bytes_read) -> int {
    if (!read_relocation_metadata) {
        return 0;
    }
    bool const READ_BY_TYPE = section.sh_type == SHT_STRTAB || section.sh_type == SHT_SYMTAB || section.sh_type == SHT_DYNSYM ||
                              section.sh_type == SHT_REL || section.sh_type == SHT_RELA;
    bool const READ_RELR_BY_NAME =
        section_name != nullptr && (std::strcmp(section_name, ".relr") == 0 || std::strcmp(section_name, ".relr.dyn") == 0);
    if (!READ_BY_TYPE && !READ_RELR_BY_NAME) {
        return 0;
    }
    return read_file_range_fully(fd, dst, file_size, section.sh_offset, section.sh_size, path, bytes_read);
}

auto read_sparse_elf_image(vfs::File* fd, uint8_t* dst, size_t file_size, const char* path, size_t initial_bytes_read,
                           bool read_static_relocation_metadata, bool allow_lazy_file_segments) -> ExecImageReadResult {
    size_t bytes_read = initial_bytes_read;
    int ret = read_file_range_fully(fd, dst, file_size, 0, sizeof(Elf64_Ehdr), path, bytes_read);
    if (ret < 0) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
    }

    const auto* elf_header = reinterpret_cast<const Elf64_Ehdr*>(dst);
    if (elf_header->e_ident[EI_MAG0] != ELFMAG0 || elf_header->e_ident[EI_MAG1] != ELFMAG1 || elf_header->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_header->e_ident[EI_MAG3] != ELFMAG3) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = 0};
    }
    if (elf_header->e_phentsize != sizeof(Elf64_Phdr) || elf_header->e_shentsize != sizeof(Elf64_Shdr) || elf_header->e_phnum == 0 ||
        elf_header->e_shnum == 0 || elf_header->e_shstrndx >= elf_header->e_shnum) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = -ENOEXEC};
    }

    uint64_t phdr_bytes = 0;
    uint64_t shdr_bytes = 0;
    if (!exec_table_size(elf_header->e_phnum, elf_header->e_phentsize, phdr_bytes) ||
        !exec_table_size(elf_header->e_shnum, elf_header->e_shentsize, shdr_bytes) ||
        !exec_range_in_file(file_size, elf_header->e_phoff, phdr_bytes) ||
        !exec_range_in_file(file_size, elf_header->e_shoff, shdr_bytes)) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = -ENOEXEC};
    }

    ret = read_file_range_fully(fd, dst, file_size, elf_header->e_phoff, phdr_bytes, path, bytes_read);
    if (ret < 0) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
    }
    ret = read_file_range_fully(fd, dst, file_size, elf_header->e_shoff, shdr_bytes, path, bytes_read);
    if (ret < 0) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
    }

    const auto* program_headers = reinterpret_cast<const Elf64_Phdr*>(dst + elf_header->e_phoff);
    const auto* section_headers = reinterpret_cast<const Elf64_Shdr*>(dst + elf_header->e_shoff);
    const Elf64_Shdr& shstrtab = section_headers[elf_header->e_shstrndx];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    if (!exec_range_in_file(file_size, shstrtab.sh_offset, shstrtab.sh_size)) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = -ENOEXEC};
    }
    ret = read_file_range_fully(fd, dst, file_size, shstrtab.sh_offset, shstrtab.sh_size, path, bytes_read);
    if (ret < 0) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
    }
    const char* const SECTION_NAMES = reinterpret_cast<const char*>(dst + shstrtab.sh_offset);

    bool has_dynamic_interp = false;
    for (Elf64_Half i = 0; i < elf_header->e_phnum; ++i) {
        const Elf64_Phdr& ph = program_headers[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        has_dynamic_interp = has_dynamic_interp || ph.p_type == PT_INTERP;
    }
    bool const LAZY_FILE_SEGMENTS = allow_lazy_file_segments && (has_dynamic_interp || !read_static_relocation_metadata);
    for (Elf64_Half i = 0; i < elf_header->e_phnum; ++i) {
        const Elf64_Phdr& ph = program_headers[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (ph.p_filesz == 0) {
            continue;
        }
        if (ph.p_type == PT_LOAD) {
            ret = read_exec_load_segment_for_loader(fd, dst, file_size, ph, program_headers, elf_header->e_phnum, i, path,
                                                    LAZY_FILE_SEGMENTS, bytes_read);
            if (ret < 0) {
                return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
            }
            continue;
        }
        if (ph.p_type != PT_NOTE && ph.p_type != PT_INTERP) {
            continue;
        }
        ret = read_file_range_fully(fd, dst, file_size, ph.p_offset, ph.p_filesz, path, bytes_read);
        if (ret < 0) {
            return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
        }
    }

    bool const READ_RELOCATION_METADATA = read_static_relocation_metadata && !has_dynamic_interp;
    for (Elf64_Half i = 0; i < elf_header->e_shnum; ++i) {
        const Elf64_Shdr& section = section_headers[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        const char* section_name = "";
        if (section.sh_name < shstrtab.sh_size) {
            section_name = SECTION_NAMES + section.sh_name;
        }
        ret = read_exec_section_if_needed(fd, dst, file_size, section, section_name, path, READ_RELOCATION_METADATA, bytes_read);
        if (ret < 0) {
            return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = ret};
        }
    }

    return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = 0};
}

auto read_exec_image_for_loader(vfs::File* fd, uint8_t* dst, size_t file_size, const char* path,
                                bool read_static_relocation_metadata = true, bool allow_lazy_file_segments = false) -> ExecImageReadResult {
    if (file_size <= EXEC_SPARSE_ELF_MIN_SIZE) {
        return read_full_exec_image(fd, dst, file_size, path);
    }

    size_t bytes_read = 0;
    size_t const PROBE_SIZE = exec_min_size(file_size, EXEC_SHEBANG_PROBE_SIZE);
    int const PROBE_RET = read_file_range_fully(fd, dst, file_size, 0, PROBE_SIZE, path, bytes_read);
    if (PROBE_RET < 0) {
        return {.bytes_read = static_cast<ssize_t>(bytes_read), .status = PROBE_RET};
    }

    bool const USE_LAZY_FILE_SEGMENTS = allow_lazy_file_segments && exec_lazy_file_segments_enabled();
    ExecImageReadResult result =
        read_sparse_elf_image(fd, dst, file_size, path, bytes_read, read_static_relocation_metadata, USE_LAZY_FILE_SEGMENTS);
    result.shebang_probe_size = PROBE_SIZE;
    return result;
}

auto parse_shebang_line(const uint8_t* file_data, size_t file_size, ShebangInfo* out) -> bool {
    if (file_data == nullptr || out == nullptr || file_size < 2 || file_data[0] != '#' || file_data[1] != '!') {
        return false;
    }

    size_t pos = 2;
    while (pos < file_size && (file_data[pos] == ' ' || file_data[pos] == '\t')) {
        pos++;
    }

    size_t const INTERP_BEGIN = pos;
    while (pos < file_size && file_data[pos] != '\n' && file_data[pos] != '\r' && file_data[pos] != ' ' && file_data[pos] != '\t') {
        pos++;
    }

    size_t const INTERP_LEN = pos - INTERP_BEGIN;
    if (INTERP_LEN == 0 || INTERP_LEN >= out->interpreter.size()) {
        return false;
    }
    std::memcpy(out->interpreter.data(), file_data + INTERP_BEGIN, INTERP_LEN);
    fixed_slot(out->interpreter, INTERP_LEN) = '\0';

    while (pos < file_size && (file_data[pos] == ' ' || file_data[pos] == '\t')) {
        pos++;
    }

    size_t const ARG_BEGIN = pos;
    while (pos < file_size && file_data[pos] != '\n' && file_data[pos] != '\r') {
        pos++;
    }

    while (pos > ARG_BEGIN && (file_data[pos - 1] == ' ' || file_data[pos - 1] == '\t')) {
        pos--;
    }

    size_t const ARG_LEN = pos - ARG_BEGIN;
    if (ARG_LEN == 0) {
        return true;
    }
    if (ARG_LEN >= out->argument.size()) {
        return false;
    }

    std::memcpy(out->argument.data(), file_data + ARG_BEGIN, ARG_LEN);
    fixed_slot(out->argument, ARG_LEN) = '\0';
    out->has_argument = true;
    return true;
}

template <typename ExecFn>
auto exec_shebang_script(const char* script_path, const char* const* argv, const char* const* envp, size_t argv_count,
                         const ShebangInfo& shebang, int shebang_depth, ExecFn exec_fn, ExecRequestArguments* request = nullptr)
    -> uint64_t {
    if (request != nullptr) {
        const char* interpreter = nullptr;
        const char** forwarded = nullptr;
        int const RESULT = request->prepare_shebang(script_path, argv, argv_count, shebang, shebang_depth, interpreter, forwarded);
        if (RESULT < 0) return static_cast<uint64_t>(RESULT);
        // The recursive native commit can destroy request before returning.
        // Nothing in this frame may dereference it or free forwarded afterward.
        return exec_fn(interpreter, forwarded, envp, shebang_depth + 1);
    }
    size_t const FORWARDED_ARGS = argv_count > 0 ? (argv_count - 1) : 0;
    size_t const NEW_ARGC = 2 + FORWARDED_ARGS + (shebang.has_argument ? 1 : 0);
    auto** shebang_argv = new const char*[NEW_ARGC + 1];
    if (shebang_argv == nullptr) {
        return 0;
    }

    size_t idx = 0;
    shebang_argv[idx++] = shebang.interpreter.data();
    if (shebang.has_argument) {
        shebang_argv[idx++] = shebang.argument.data();
    }
    shebang_argv[idx++] = script_path;
    for (size_t i = 1; i < argv_count; ++i) {
        shebang_argv[idx++] = argv[i];
    }
    shebang_argv[idx] = nullptr;

    uint64_t const RC = exec_fn(shebang.interpreter.data(), shebang_argv, envp, shebang_depth + 1);
    delete[] shebang_argv;
    return RC;
}

}  // namespace

#ifdef WOS_SELFTEST
namespace {
auto make_exec_selftest_owned_image(bool& consumed, ker::mod::mm::numa::Policy memory_policy = {}, uint64_t staging_pid = 0)
    -> std::unique_ptr<PreparedNativeExecImage> {
    std::unique_ptr<PreparedNativeExecImage> prepared;
    {
        auto* file = exec_selftest_make_file();
        auto* buffer = new (std::nothrow) uint8_t[8]{};
        char* name = new (std::nothrow) char[8]{};
        if (file == nullptr || buffer == nullptr || name == nullptr) {
            if (file != nullptr) vfs::vfs_put_file(file);
            delete[] buffer;
            delete[] name;
            return nullptr;
        }
        std::memcpy(name, "owned", 6);
        std::array<char, EXEC_PATH_MAX> path{};
        std::memcpy(path.data(), "/owned/image", 13);
        ker::mod::cpu::GPRegs registers{};
        registers.rdi = 0x123456;
        vfs::Stat stat{};
        stat.st_uid = 37;
        LazyVmemRangeVec ranges;
        LazyVmemRange range{};
        range.kind = LazyVmemKind::FILE_BACKED;
        range.file = file;
        if (!ranges.push_back(range)) {
            vfs::vfs_put_file(file);
            delete[] buffer;
            delete[] name;
            return nullptr;
        }
        vfs::vfs_retain_file(file);
        NativeExecImage const IMAGE{
            .entry = 0,
            .program_header_addr = 0,
            .elf_header_addr = 0,
            .image_load_base = 0,
            .image_vaddr_start = 0,
            .image_vaddr_end = 0,
            .program_header_count = 0,
            .program_header_ent_size = 0,
            .elf_buffer = buffer,
            .elf_buffer_size = 8,
            .elf_buffer_complete = true,
            .exec_image_file = file,
            .exec_image_size = 8,
            .interp_base = 0,
            .interp_vaddr_start = 0,
            .interp_vaddr_end = 0,
            .at_random_addr = 0,
            .mmap_next = 0,
            .name = name,
            .path = path.data(),
            .debug_staging_pid = staging_pid,
            .initial_rip = 0,
            .initial_rsp = 0,
            .registers = registers,
            .pagemap = nullptr,
            .thread = nullptr,
            .lazy_ranges = ranges,
            .stat = stat,
            .nosuid = true,
            .memory_policy = memory_policy,
            .admitted_credentials = ker::mod::security::Snapshot::bootstrap_root(),
            .stack_credentials = ker::mod::security::Snapshot::bootstrap_root(),
        };
        prepared.reset(new (std::nothrow) PreparedNativeExecImage(IMAGE, 0x7B60));
        if (!prepared) {
            release_lazy_file_refs(ranges);
            vfs::vfs_put_file(file);
            delete[] buffer;
            delete[] name;
            return nullptr;
        }
        consumed = name == nullptr && ranges.empty();
        // Destroy/overwrite all borrowed staging metadata before inspection.
        path.fill('x');
        registers.rdi = 0;
        stat.st_uid = 0;
        memory_policy = {};
    }
    return prepared;
}
}  // namespace

namespace {
ker::mod::sched::task::Task exec_arguments_selftest_owner;  // NOLINT
ker::mod::sched::task::Task exec_snapshot_selftest_owner;   // NOLINT
ker::mod::sched::task::Task exec_handoff_selftest_worker;   // NOLINT
}  // namespace

auto exec_selftest_argument_snapshot_arenas_and_faults() -> bool {
    namespace mm = ker::mod::mm;
    constexpr uint64_t USER_BASE = 0x40000000ULL;
    constexpr size_t PAGE_BYTES = mm::paging::PAGE_SIZE;
    constexpr size_t LONG_OFFSET = PAGE_BYTES - 1;
    constexpr size_t LONG_BYTES = 2000;
    auto* root = mm::virt::create_pagemap();
    if (root == nullptr) return false;
    mm::virt::copy_kernel_mappings_to(root);
    auto& owner = exec_snapshot_selftest_owner;
    owner.type = ker::mod::sched::task::TaskType::PROCESS;
    owner.usercopy_pagemap_closing.store(false);
    owner.pagemap = root;
    std::array<uint8_t*, 3> pages{};
    size_t mapped = 0;
    bool ok = true;
    for (auto& page : pages) {
        page = static_cast<uint8_t*>(
            mm::phys::page_alloc_with_reclaim_may_fail(mm::PhysicalPageOwner::SELFTEST, PAGE_BYTES, "exec-snapshot-selftest"));
        if (page == nullptr) {
            ok = false;
            break;
        }
        std::memset(page, 0, PAGE_BYTES);
        auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<mm::addr::vaddr_t>(page)));
        mm::virt::map_page(root, USER_BASE + mapped * PAGE_BYTES, PHYSICAL, mm::paging::page_types::USER);
        ++mapped;
    }
    if (ok) {
        // Populate target pages through their HHDM aliases; the snapshot itself
        // exercises ordinary Task usercopy, including real page pins and SMAP.
        auto store = [&pages](size_t offset, const void* source, size_t size) {
            auto* bytes = static_cast<const uint8_t*>(source);
            for (size_t i = 0; i < size; ++i) pages.at((offset + i) / PAGE_BYTES)[(offset + i) % PAGE_BYTES] = bytes[i];
        };
        constexpr char PATH[] = "/bin/true";
        constexpr char FIRST[] = "first";
        constexpr char LAST[] = "last";
        constexpr char ENV[] = "K=value";
        constexpr std::array<uint64_t, 4> ARGV{USER_BASE + 0x300, USER_BASE + LONG_OFFSET, USER_BASE + 2 * PAGE_BYTES + 300, 0};
        constexpr std::array<uint64_t, 2> ENVP{USER_BASE + 2 * PAGE_BYTES + 360, 0};
        constexpr std::array<uint64_t, 2> SHORT_ARGV{USER_BASE + 0x300, 0};
        constexpr std::array<uint64_t, 2> BAD_ARGV{USER_BASE + 3 * PAGE_BYTES, 0};
        store(16, PATH, sizeof(PATH));
        store(0x100, ARGV.data(), sizeof(ARGV));
        store(0x150, ENVP.data(), sizeof(ENVP));
        store(0x200, SHORT_ARGV.data(), sizeof(SHORT_ARGV));
        store(0x240, BAD_ARGV.data(), sizeof(BAD_ARGV));
        store(0x300, FIRST, sizeof(FIRST));
        for (size_t i = 0; i < LONG_BYTES; ++i) pages.at((LONG_OFFSET + i) / PAGE_BYTES)[(LONG_OFFSET + i) % PAGE_BYTES] = 'a';
        store(2 * PAGE_BYTES + 300, LAST, sizeof(LAST));
        store(2 * PAGE_BYTES + 360, ENV, sizeof(ENV));
        {
            ExecArgumentsSnapshot short_args;
            int const RESULT = snapshot_exec_arguments(owner, USER_BASE + 16, USER_BASE + 0x200, 0, short_args);
            ok &= RESULT == 0 && short_args.small_strings != nullptr && short_args.strings == nullptr &&
                  short_args.budget_used == 3 * sizeof(uint64_t) + sizeof(FIRST);
            if (RESULT == 0) ok &= short_args.argv[1] == nullptr && std::strcmp(short_args.argv[0], FIRST) == 0;
        }
        {
            ExecArgumentsSnapshot arguments;
            int const RESULT = snapshot_exec_arguments(owner, USER_BASE + 16, USER_BASE + 0x100, USER_BASE + 0x150, arguments);
            ok &= RESULT == 0 && arguments.small_strings != nullptr && arguments.strings != nullptr &&
                  arguments.budget_used == 6 * sizeof(uint64_t) + sizeof(FIRST) + LONG_BYTES + 1 + sizeof(LAST) + sizeof(ENV);
            for (auto* page : pages) std::memset(page, 0, PAGE_BYTES);
            if (RESULT == 0) {
                ok &= std::strcmp(arguments.path.data(), PATH) == 0 && std::strcmp(arguments.argv[0], FIRST) == 0 &&
                      std::strcmp(arguments.argv[2], LAST) == 0 && arguments.argv[3] == nullptr &&
                      std::strcmp(arguments.envp[0], ENV) == 0 && arguments.envp[1] == nullptr;
                for (size_t i = 0; i < LONG_BYTES; ++i) ok &= arguments.argv[1][i] == 'a';
                ok &= arguments.argv[1][LONG_BYTES] == '\0';
            }
        }
        store(16, PATH, sizeof(PATH));
        store(0x200, SHORT_ARGV.data(), sizeof(SHORT_ARGV));
        store(0x240, BAD_ARGV.data(), sizeof(BAD_ARGV));
        {
            ExecArgumentsSnapshot invalid;
            ok &= snapshot_exec_arguments(owner, USER_BASE + 16, USER_BASE + 0x240, 0, invalid) == -EFAULT;
        }
        {
            ExecArgumentsSnapshot empty;
            ok &= snapshot_exec_arguments(owner, USER_BASE + 16, 0, 0, empty) == 0 && empty.argv == nullptr && empty.envp == nullptr &&
                  empty.small_strings == nullptr && empty.strings == nullptr && empty.budget_used == 2 * sizeof(uint64_t);
        }
        {
            ExecArgumentsSnapshot exhausted;
            exhausted.budget_used = EXEC_ARG_BYTES_MAX - sizeof(uint64_t);
            ok &= snapshot_exec_vector(owner, USER_BASE + 0x200, exhausted, exhausted.argv) == -E2BIG && exhausted.small_strings == nullptr;
        }
        store(0x300, FIRST, sizeof(FIRST));
        {
            ExecArgumentsSnapshot last_null;
            ok &= snapshot_exec_vector(owner, USER_BASE + 3 * PAGE_BYTES - sizeof(uint64_t), last_null, last_null.argv) == 0 &&
                  last_null.argv != nullptr && last_null.argv[0] == nullptr && last_null.budget_used == sizeof(uint64_t);
        }
        for (size_t offset : {3 * PAGE_BYTES - sizeof(SHORT_ARGV), PAGE_BYTES - 3}) {
            store(offset, SHORT_ARGV.data(), sizeof(SHORT_ARGV));
            ExecArgumentsSnapshot boundary;
            int const RESULT = snapshot_exec_vector(owner, USER_BASE + offset, boundary, boundary.argv);
            ok &= RESULT == 0 && boundary.budget_used == sizeof(SHORT_ARGV) + sizeof(FIRST);
            if (RESULT == 0) ok &= boundary.argv[1] == nullptr && std::strcmp(boundary.argv[0], FIRST) == 0;
        }
        {
            std::array<uint64_t, 41> many{};
            many.fill(USER_BASE + 0x300);
            many.back() = 0;
            store(2 * PAGE_BYTES + 512, many.data(), sizeof(many));
            ExecArgumentsSnapshot spilled;
            int const RESULT = snapshot_exec_vector(owner, USER_BASE + 2 * PAGE_BYTES + 512, spilled, spilled.argv);
            ok &= RESULT == 0 && spilled.budget_used == sizeof(many) + 40 * sizeof(FIRST) && spilled.input_string_addrs.size() == 40;
            if (RESULT == 0) {
                for (size_t i = 0; i < 40; ++i) ok &= std::strcmp(spilled.argv[i], FIRST) == 0;
                ok &= spilled.argv[40] == nullptr;
            }
        }
        {
            store(3 * PAGE_BYTES - 3, SHORT_ARGV.data(), 3);
            ExecArgumentsSnapshot partial_word;
            ok &= snapshot_exec_vector(owner, USER_BASE + 3 * PAGE_BYTES - 3, partial_word, partial_word.argv) == -EFAULT &&
                  partial_word.budget_used == 0 && partial_word.argv == nullptr;
        }
        {
            store(3 * PAGE_BYTES - sizeof(uint64_t), SHORT_ARGV.data(), sizeof(uint64_t));
            ExecArgumentsSnapshot before_unmapped;
            before_unmapped.budget_used = EXEC_ARG_BYTES_MAX - sizeof(uint64_t);
            ok &= snapshot_exec_vector(owner, USER_BASE + 3 * PAGE_BYTES - sizeof(uint64_t), before_unmapped, before_unmapped.argv) ==
                      -E2BIG &&
                  before_unmapped.budget_used == EXEC_ARG_BYTES_MAX && before_unmapped.argv == nullptr;
        }
    }
    // Every snapshot and usercopy pin has retired before unmapping the private
    // root. This fake Task never becomes runnable or changes the active CR3.
    ok &= owner.usercopy_pagemap_accesses.load() == 0;
    for (size_t i = 0; i < mapped; ++i) mm::virt::unmap_page(root, USER_BASE + i * PAGE_BYTES);
    ok &= owner.detach_pagemap_after_usercopy_quiescence() == root;
    mm::virt::destroy_user_space(root, 0, "exec-snapshot-selftest", "argument-arenas");
    mm::virt::release_pagemap(root);
    return ok;
}

auto exec_selftest_request_arguments_recover_partial_and_recursive_storage() -> bool {
    auto& owner = exec_arguments_selftest_owner;
    owner.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    owner.exit_in_progress.store(false);
    owner.has_exited = false;
    uint32_t destructions = 0;
    std::unique_ptr<ExecRequestArguments> pending(new (std::nothrow) ExecRequestArguments);
    std::unique_ptr<ExecRequestArguments> rejected(new (std::nothrow) ExecRequestArguments);
    if (!pending || !rejected) return false;
    pending->selftest_destructions = &destructions;
    auto* arguments = pending.get();
    if (!adopt_exec_arguments(owner, pending)) return false;
    bool ok = !pending && owner.owned_exec_arguments.load() == arguments && !adopt_exec_arguments(owner, rejected) && rejected != nullptr;
    auto& snapshot = arguments->snapshot;
    snapshot.argv = new (std::nothrow) const char*[3]{};
    snapshot.envp = new (std::nothrow) const char*[1]{};
    snapshot.strings = new (std::nothrow) char[16]{};
    snapshot.small_strings = new (std::nothrow) char[EXEC_ARG_SMALL_BYTES]{};
    if (!snapshot.argv || !snapshot.envp || !snapshot.strings || !snapshot.small_strings) {
        cleanup_exec_preparation(owner);
        return false;
    }
    std::memcpy(snapshot.strings, "tail", 5);
    snapshot.argv[0] = snapshot.path.data();
    snapshot.argv[1] = snapshot.strings;
    for (uint64_t i = 0; i < 40; ++i) ok &= snapshot.input_string_addrs.push_back(i + 1);
    const char* interpreter = nullptr;
    const char** forwarded = nullptr;
    {
        ShebangInfo shebang{};
        std::memcpy(shebang.interpreter.data(), "/interp", 8);
        std::memcpy(shebang.argument.data(), "option", 7);
        shebang.has_argument = true;
        std::array<char, EXEC_PATH_MAX> script{};
        std::memcpy(script.data(), "/script", 8);
        ok &= arguments->prepare_shebang(script.data(), snapshot.argv, 2, shebang, 0, interpreter, forwarded) == 0;
        shebang.interpreter.fill('x');
        shebang.argument.fill('x');
        script.fill('x');
    }
    ok &= interpreter != nullptr && std::strcmp(interpreter, "/interp") == 0 && forwarded != nullptr;
    if (forwarded != nullptr) {
        ok &= std::strcmp(forwarded[0], "/interp") == 0 && std::strcmp(forwarded[1], "option") == 0 &&
              std::strcmp(forwarded[2], "/script") == 0 && std::strcmp(forwarded[3], "tail") == 0 && forwarded[4] == nullptr;
    }
    owner.process_exit_requested.store(true);
    owner.exec_preparation_teardown_in_progress.store(true);
    cleanup_exec_preparation(owner);
    ok &= destructions == 0 && owner.owned_exec_arguments.load() == arguments && !adopt_exec_arguments(owner, rejected);
    owner.exec_preparation_teardown_in_progress.store(false);
    ShebangInfo nested{};
    std::memcpy(nested.interpreter.data(), "/nested", 8);
    bool called = false;
    if (forwarded != nullptr) {
        uint64_t const RESULT = exec_shebang_script(
            "/outer", forwarded, snapshot.envp, 4, nested, 1,
            [&owner, &called](const char* path, const char* const* argv, const char* const*, int depth) -> uint64_t {
                called = depth == 2 && std::strcmp(path, "/nested") == 0 && std::strcmp(argv[1], "/outer") == 0 &&
                         std::strcmp(argv[2], "option") == 0 && std::strcmp(argv[3], "/script") == 0 && std::strcmp(argv[4], "tail") == 0 &&
                         argv[5] == nullptr;
                cleanup_exec_preparation(owner);
                return 42;
            },
            arguments);
        ok &= RESULT == 42 && called;
    }
    cleanup_exec_preparation(owner);
    ok &= destructions == 1 && owner.owned_exec_arguments.load() == nullptr && owner.process_exit_requested.load();
    // Recover an incomplete second snapshot, including a spilled input-address
    // vector, without ever constructing a native image or env/string storage.
    rejected->selftest_destructions = &destructions;
    arguments = rejected.get();
    if (!adopt_exec_arguments(owner, rejected)) return false;
    arguments->snapshot.argv = new (std::nothrow) const char*[2]{};
    ok &= arguments->snapshot.argv != nullptr;
    for (uint64_t i = 0; i < 40; ++i) ok &= arguments->snapshot.input_string_addrs.push_back(i + 1);
    cleanup_exec_preparation(owner);
    cleanup_exec_preparation(owner);
    ok &= destructions == 2 && !owner.owned_exec_arguments.load() && !owner.exec_preparation_teardown_in_progress.load();
    owner.process_exit_requested.store(false);
    return ok;
}

auto exec_selftest_request_scratch_preserves_transferred_metadata() -> bool {
    auto& owner = exec_arguments_selftest_owner;
    owner.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    owner.exit_in_progress.store(false);
    owner.has_exited = false;
    uint32_t destructions = 0;
    std::unique_ptr<ExecRequestArguments> pending(new (std::nothrow) ExecRequestArguments);
    if (!pending) return false;
    pending->selftest_destructions = &destructions;
    auto* request = pending.get();
    if (!adopt_exec_arguments(owner, pending)) return false;
    bool ok = request->native_scratch(-1) == nullptr && request->native_scratch(MAX_SHEBANG_DEPTH + 1) == nullptr;
    for (int depth = 0; depth <= MAX_SHEBANG_DEPTH; ++depth) {
        auto* scratch = request->native_scratch(depth);
        auto& elf = scratch->prepared_elf;
        elf.program_headers = new (std::nothrow) Elf64_Phdr[2]{};
        elf.section_headers = new (std::nothrow) Elf64_Shdr[2]{};
        elf.section_names = new (std::nothrow) char[16]{};
        elf.task_metadata = new (std::nothrow) uint8_t[64]{};
        scratch->argv_addrs = new (std::nothrow) uint64_t[40]{};
        // The first frame deliberately has a partially built address vector pair.
        if (depth != 0) scratch->envp_addrs = new (std::nothrow) uint64_t[40]{};
        if (!elf.program_headers || !elf.section_headers || !elf.section_names || !elf.task_metadata || !scratch->argv_addrs ||
            (depth != 0 && !scratch->envp_addrs)) {
            cleanup_exec_preparation(owner);
            return false;
        }
        auto& interp = scratch->prepared_interp;
        if ((depth % 2) == 0) {
            interp.program_headers = new (std::nothrow) Elf64_Phdr[2]{};
            interp.section_headers = new (std::nothrow) Elf64_Shdr[2]{};
            // Leave the first interpreter frame partially prepared.
            if (depth != 0) interp.section_names = new (std::nothrow) char[16]{};
            if (!interp.program_headers || !interp.section_headers || (depth != 0 && !interp.section_names)) {
                cleanup_exec_preparation(owner);
                return false;
            }
            interp.view.program_headers = interp.program_headers;
            interp.view.read_context = &interp.read_context;
        } else {
            scratch->interp_buf = new (std::nothrow) uint8_t[128]{};
            if (!scratch->interp_buf) {
                cleanup_exec_preparation(owner);
                return false;
            }
            scratch->interp_buf[0] = static_cast<uint8_t>(depth + 1);
        }
        elf.task_metadata[0] = static_cast<uint8_t>(depth + 1);
        elf.view.program_headers = elf.program_headers;
        elf.view.read_context = &elf.read_context;
        for (uint64_t index = 0; index < 40; ++index) {
            loader::elf::ElfLazyLoadRange const RANGE{.vaddr = index + static_cast<uint64_t>(depth) * 100};
            ok &= scratch->main_lazy_ranges.push_back(RANGE);
            ok &= scratch->interp_lazy_ranges.push_back(RANGE);
        }
        if (depth != 0) ok &= scratch != request->native_scratch(depth - 1);
    }
    for (int depth = 0; depth <= MAX_SHEBANG_DEPTH; ++depth) {
        auto* scratch = request->native_scratch(depth);
        auto const& elf = scratch->prepared_elf;
        ok &= elf.task_metadata[0] == depth + 1 && elf.view.program_headers == elf.program_headers &&
              elf.view.read_context == &elf.read_context && scratch->main_lazy_ranges.size() == 40 &&
              scratch->interp_lazy_ranges.size() == 40;
        auto const& interp = scratch->prepared_interp;
        if ((depth % 2) == 0) {
            ok &= interp.view.program_headers == interp.program_headers && interp.view.read_context == &interp.read_context &&
                  interp.task_metadata == nullptr && scratch->interp_buf == nullptr;
        } else {
            ok &= scratch->interp_buf[0] == depth + 1 && interp.program_headers == nullptr;
        }
    }
    auto* first = request->native_scratch(0);
    first->reset_interp_storage();
    first->reset_interp_storage();
    ok &= first->prepared_interp.program_headers == nullptr && first->prepared_interp.view.read_context == nullptr &&
          first->interp_buf == nullptr;
    auto& transferred = request->native_scratch(MAX_SHEBANG_DEPTH)->prepared_elf;
    std::unique_ptr<uint8_t[]> image_metadata(transferred.take_task_metadata());
    ok &= transferred.task_metadata == nullptr;
    owner.exec_preparation_teardown_in_progress.store(true);
    cleanup_exec_preparation(owner);
    ok &= destructions == 0 && owner.owned_exec_arguments.load() == request;
    owner.exec_preparation_teardown_in_progress.store(false);
    cleanup_exec_preparation(owner);
    cleanup_exec_preparation(owner);
    return ok && destructions == 1 && owner.owned_exec_arguments.load() == nullptr && !owner.exec_preparation_teardown_in_progress.load() &&
           image_metadata[0] == MAX_SHEBANG_DEPTH + 1;
}

auto exec_selftest_worker_handoff_claim_and_cancel() -> bool {
    auto& owner = exec_arguments_selftest_owner;
    auto& worker = exec_handoff_selftest_worker;
    owner.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    worker.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    owner.exit_in_progress.store(false);
    worker.exit_in_progress.store(false);
    owner.has_exited = worker.has_exited = false;
    owner.resource_cpu_admission_closed = false;
    g_exec_selftest_close_count.store(0);
    namespace numa = ker::mod::mm::numa;
    constexpr numa::Policy OWNER_POLICY{.nodes = 1, .mode = numa::Mode::PREFERRED};
    constexpr numa::Policy WORKER_POLICY{.nodes = 1, .mode = numa::Mode::BIND};
    if (!owner.selftest_set_memory_policy(OWNER_POLICY) || !worker.selftest_set_memory_policy(WORKER_POLICY)) return false;
    bool consumed = false;
    bool ok = true;
    for (bool const CLAIM : {false, true}) {
        auto image = make_exec_selftest_owned_image(consumed, worker.memory_policy_snapshot());
        if (!image) return false;
        auto* raw = image.get();
        if (!image->admission().begin(owner)) return false;
        ok &= image->admission().owns(owner) && !image->admission().owns(worker) &&
              !ker::mod::sched::resource::close_idle_thread_cpu_admission(owner);
        if (!adopt_exec_preparation(worker, image)) return false;
        WorkerExecHandoff handoff;
        handoff.worker = &worker;
        owner.pending_exec_handoff.store(&handoff);
        if (CLAIM) {
            ok &= claim_pending_exec_handoff(owner) == &handoff && !worker.owned_exec_preparation.load() &&
                  owner.owned_exec_preparation.load() == raw && owner.active_exec_handoff.load() == &handoff &&
                  !owner.pending_exec_handoff.load();
        }
        ok &= raw->view().memory_policy == WORKER_POLICY && owner.memory_policy_snapshot() == OWNER_POLICY;
        cancel_pending_exec_handoff(owner);
        ok &= handoff.result.load() == -ECANCELED && !owner.pending_exec_handoff.load() && !owner.active_exec_handoff.load();
        // Pending cancellation keeps worker ownership; claimed cancellation
        // keeps owner ownership. Exit cleans only the corresponding Task slot.
        ok &= (CLAIM ? owner.owned_exec_preparation.load() : worker.owned_exec_preparation.load()) == raw;
        cleanup_exec_preparation(owner);
        cleanup_exec_preparation(worker);
        ok &= !owner.resource_exec_admission_closed && owner.memory_policy_snapshot() == OWNER_POLICY;
    }
    ok &= g_exec_selftest_close_count.load() == 2 && ker::mod::sched::resource::close_idle_thread_cpu_admission(owner);
    owner.resource_cpu_admission_closed = false;
    owner.publish_memory_policy_snapshot({});
    worker.publish_memory_policy_snapshot({});
    return ok;
}

auto exec_selftest_owned_image_outlives_staging_and_discards_once() -> bool {
    g_exec_selftest_close_count.store(0, std::memory_order_relaxed);
    bool consumed = false;
    constexpr ker::mod::mm::numa::Policy POLICY{.nodes = 1, .mode = ker::mod::mm::numa::Mode::PREFERRED};
    auto prepared = make_exec_selftest_owned_image(consumed, POLICY);
    if (!prepared) return false;
    auto const& IMAGE = prepared->view();
    bool const SNAPSHOT_OK = consumed && std::strcmp(IMAGE.path, "/owned/image") == 0 && std::strcmp(IMAGE.name, "owned") == 0 &&
                             IMAGE.registers.rdi == 0x123456 && IMAGE.stat.st_uid == 37 && IMAGE.lazy_ranges.size() == 1 &&
                             IMAGE.exec_image_file->refcount.load() == 2 && IMAGE.nosuid && IMAGE.memory_policy == POLICY &&
                             IMAGE.admitted_credentials == ker::mod::security::Snapshot::bootstrap_root();
    prepared->discard();
    prepared->discard();
    prepared.reset();
    return SNAPSHOT_OK && g_exec_selftest_close_count.load(std::memory_order_relaxed) == 1;
}

auto exec_selftest_worker_policy_reaches_canonical_commit() -> bool {
    using ker::mod::sched::task::Task;
    namespace numa = ker::mod::mm::numa;
    auto owner = std::unique_ptr<Task>{new (std::nothrow) Task{}};
    auto worker = std::unique_ptr<Task>{new (std::nothrow) Task{}};
    if (!owner || !worker) return false;
    owner->pid = ker::mod::sched::task::get_next_pid();
    owner->type = ker::mod::sched::task::TaskType::PROCESS;
    worker->is_thread = true;
    worker->type = ker::mod::sched::task::TaskType::PROCESS;
    worker->owner_pid = owner->pid;
    if (!ker::mod::sched::resource::bind_thread_owner(*worker, *owner)) return false;
    constexpr numa::Policy OWNER_POLICY{.nodes = 1, .mode = numa::Mode::PREFERRED};
    constexpr numa::Policy WORKER_POLICY{.nodes = 1, .mode = numa::Mode::BIND};
    if (!owner->selftest_set_memory_policy(OWNER_POLICY) || !worker->selftest_set_memory_policy(WORKER_POLICY)) return false;
    bool consumed = false;
    uint64_t const STAGING_PID = owner->pid | EXEC_DEBUG_STAGING_BIT;
    auto prepared = make_exec_selftest_owned_image(consumed, worker->memory_policy_snapshot(), STAGING_PID);
    if (!prepared || !prepared->admission().begin(*worker)) return false;
    auto* const IMAGE = prepared.get();
    if (!adopt_exec_preparation(*worker, prepared)) return false;
    WorkerExecHandoff handoff;
    handoff.worker = worker.get();
    owner->pending_exec_handoff.store(&handoff);
    bool ok = claim_pending_exec_handoff(*owner) == &handoff && !worker->owned_exec_preparation.load() &&
              owner->owned_exec_preparation.load() == IMAGE;
    if (!ok) {
        cancel_pending_exec_handoff(*owner);
        cleanup_exec_preparation(*worker);
        cleanup_exec_preparation(*owner);
        return false;
    }
    // The real publication helper must preserve A when its actual debug-stage
    // prerequisite is absent, then install B when that prerequisite is present.
    ok = commit_native_exec_image(owner.get(), *IMAGE) == -EIO && owner->memory_policy_snapshot() == OWNER_POLICY;
    loader::debug::register_process(STAGING_PID, IMAGE->view().name, 0, 0);
    int const COMMITTED = commit_native_exec_image(owner.get(), *IMAGE);
    ok = COMMITTED == 0 && owner->memory_policy_snapshot() == WORKER_POLICY && worker->memory_policy_snapshot() == WORKER_POLICY && ok;
    if (COMMITTED == 0) {
        // This existing metadata-only image fixture never enters userspace or
        // switches CR3. Complete its ownership phase after the real Task commit.
        IMAGE->mark_activated();
        acknowledge_worker_exec(*owner, handoff, 0);
    } else {
        cancel_pending_exec_handoff(*owner);
    }
    cleanup_exec_preparation(*owner);
    cleanup_exec_preparation(*worker);
    if (COMMITTED == 0) {
        loader::debug::unregister_process(owner->pid);
        ker::mod::sched::task::release_lazy_vmem_ranges(*owner);
        vfs::vfs_put_file(std::exchange(owner->exec_image_file, nullptr));
        delete[] std::exchange(owner->elf_buffer, nullptr);
        delete[] std::exchange(owner->name, nullptr);
    }
    return ok && !owner->owned_exec_preparation.load() && !owner->active_exec_handoff.load();
}

auto exec_selftest_tracked_image_cleanup_is_reentrant_and_exclusive() -> bool {
    g_exec_selftest_close_count.store(0, std::memory_order_relaxed);
    bool consumed = false;
    auto first = make_exec_selftest_owned_image(consumed);
    auto second = make_exec_selftest_owned_image(consumed);
    if (!first || !second) return false;
    ker::mod::sched::task::Task owner{};
    auto* const FIRST = first.get();
    auto* const SECOND = second.get();
    bool ok = adopt_exec_preparation(owner, first) && !first && owner.owned_exec_preparation.load() == FIRST;
    ok = !adopt_exec_preparation(owner, second) && second.get() == SECOND && owner.owned_exec_preparation.load() == FIRST && ok;
    owner.process_exit_requested.store(true, std::memory_order_release);
    g_exec_selftest_cleanup_observed = false;
    g_exec_selftest_cleanup_owner = &owner;
    cleanup_exec_preparation(owner);
    g_exec_selftest_cleanup_owner = nullptr;
    ok = g_exec_selftest_cleanup_observed && !owner.owned_exec_preparation.load() && !owner.exec_preparation_teardown_in_progress.load() &&
         g_exec_selftest_close_count.load() == 1 && ok;
    cleanup_exec_preparation(owner);
    ok = g_exec_selftest_close_count.load() == 1 && ok;
    owner.exit_in_progress.store(true, std::memory_order_release);
    ok = !adopt_exec_preparation(owner, second) && second.get() == SECOND && ok;
    owner.exit_in_progress.store(false, std::memory_order_release);
    owner.exec_preparation_teardown_in_progress.store(true, std::memory_order_release);
    ok = !adopt_exec_preparation(owner, second) && second.get() == SECOND && ok;
    owner.exec_preparation_teardown_in_progress.store(false, std::memory_order_release);
    ok = adopt_exec_preparation(owner, second) && !second && ok;
    cleanup_exec_preparation(owner);
    cleanup_exec_preparation(owner);
    return ok && !owner.owned_exec_preparation.load() && !owner.exec_preparation_teardown_in_progress.load() &&
           owner.process_exit_requested.load() && g_exec_selftest_close_count.load() == 2;
}

auto exec_selftest_transaction_recovers_admission_and_retired_storage() -> bool {
    namespace resource = ker::mod::sched::resource;
    using ker::mod::sched::task::Task;
    g_exec_selftest_close_count.store(0, std::memory_order_relaxed);
    Task process{};
    process.pid = 0x7B63;
    Task worker{};
    worker.is_thread = true;
    worker.owner_pid = process.pid;
    if (!resource::bind_thread_owner(worker, process)) {
        return false;
    }
    bool consumed = false;
    auto aborted = make_exec_selftest_owned_image(consumed);
    if (!aborted) {
        return false;
    }
    auto* const ABORTED = aborted.get();
    if (!adopt_exec_preparation(worker, aborted)) {
        return false;
    }
    bool ok = ABORTED->admission().begin(worker) && process.resource_exec_admission_closed;
    worker.exit_in_progress.store(true, std::memory_order_release);
    cleanup_exec_preparation(worker);
    cleanup_exec_preparation(worker);
    ok = !process.resource_exec_admission_closed && !worker.owned_exec_preparation.load() && g_exec_selftest_close_count.load() == 1 && ok;
    worker.exit_in_progress.store(false, std::memory_order_release);
    {
        Task fresh{};
        fresh.is_thread = true;
        fresh.owner_pid = process.pid;
        ok = resource::bind_thread_owner(fresh, process) && ok;
    }

    auto transaction = make_exec_selftest_owned_image(consumed);
    auto old_image = make_exec_selftest_owned_image(consumed);
    if (!transaction || !old_image) {
        return false;
    }
    // Transfer the replacement resources to an independent image owner, as
    // publication does to Task. Only the detached old resources may be freed.
    std::unique_ptr<PreparedNativeExecImage> published(new (std::nothrow) PreparedNativeExecImage(transaction->view(), process.pid));
    if (!published) {
        return false;
    }
    transaction->release_after_commit();
    transaction->mark_activated();
    auto& retired = transaction->retired();
    auto const& OLD = old_image->view();
    retired.name = std::exchange(OLD.name, nullptr);
    retired.elf_buffer = OLD.elf_buffer;
    retired.exec_image_file = OLD.exec_image_file;
    retired.lazy_ranges = std::move(OLD.lazy_ranges);
    old_image->release_after_commit();
    old_image->mark_activated();
    old_image.reset();
    auto* const TRANSACTION = transaction.get();
    if (!adopt_exec_preparation(worker, transaction)) {
        return false;
    }
    ok = TRANSACTION->admission().begin(worker) && ok;
    ok = resource::close_thread_cpu_admission(worker) && ok;
    worker.exit_in_progress.store(true, std::memory_order_release);
    worker.process_exit_requested.store(true, std::memory_order_release);
    g_exec_selftest_cleanup_observed = false;
    g_exec_selftest_expect_admission_closed = true;
    g_exec_selftest_cleanup_owner = &worker;
    cleanup_exec_preparation(worker);
    g_exec_selftest_cleanup_owner = nullptr;
    g_exec_selftest_expect_admission_closed = false;
    ok = g_exec_selftest_cleanup_observed && !worker.owned_exec_preparation.load() &&
         !worker.exec_preparation_teardown_in_progress.load() && !process.resource_exec_admission_closed &&
         process.resource_cpu_admission_closed && worker.process_exit_requested.load() && g_exec_selftest_close_count.load() == 2 && ok;
    auto const& IMAGE = published->view();
    ok = IMAGE.exec_image_file->refcount.load() == 2 && IMAGE.elf_buffer_size == 8 && std::strcmp(IMAGE.name, "owned") == 0 &&
         IMAGE.lazy_ranges.size() == 1 && ok;
    cleanup_exec_preparation(worker);
    ok = g_exec_selftest_close_count.load() == 2 && ok;
    published.reset();
    return ok && g_exec_selftest_close_count.load() == 3;
}

auto exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure() -> bool {
    ker::mod::sched::task::Task parent{};
    ker::mod::sched::task::Task child{};
    ker::vfs::File inherited{};
    ker::vfs::File cloexec{};
    inherited.refcount.store(1, std::memory_order_relaxed);
    cloexec.refcount.store(1, std::memory_order_relaxed);

    constexpr uint64_t INHERITED_FD = 5;
    constexpr uint64_t CLOEXEC_FD = 7;
    bool ok = parent.fd_table.insert(INHERITED_FD, &inherited) && parent.fd_table.insert(CLOEXEC_FD, &cloexec);
    parent.set_fd_cloexec(static_cast<unsigned>(CLOEXEC_FD));

    ok = ok && clone_exec_fd_table_checked(&parent, &child);
    ok = ok && child.fd_table.lookup(INHERITED_FD) == &inherited && child.fd_table.lookup(CLOEXEC_FD) == nullptr &&
         inherited.refcount.load(std::memory_order_relaxed) == 2 && cloexec.refcount.load(std::memory_order_relaxed) == 1;
    release_task_fd_table_files(&child);
    ok = ok && inherited.refcount.load(std::memory_order_relaxed) == 1;

    ker::mod::sched::task::Task failed_child{};
    g_exec_selftest_force_fd_clone_insert_failure.store(true, std::memory_order_relaxed);
    bool const CLONED = clone_exec_fd_table_checked(&parent, &failed_child);
    g_exec_selftest_force_fd_clone_insert_failure.store(false, std::memory_order_relaxed);
    ok = ok && !CLONED && failed_child.fd_table.empty() && inherited.refcount.load(std::memory_order_relaxed) == 1 &&
         cloexec.refcount.load(std::memory_order_relaxed) == 1;

    parent.fd_table.remove(INHERITED_FD);
    parent.fd_table.remove(CLOEXEC_FD);
    return ok;
}

auto exec_selftest_stdio_insert_failure_closes_file() -> bool {
    g_exec_selftest_close_count.store(0, std::memory_order_relaxed);
    g_exec_selftest_force_stdio_insert_failure.store(false, std::memory_order_relaxed);

    ker::mod::sched::task::Task task{};
    auto* success_file = exec_selftest_make_file();
    if (success_file == nullptr) {
        return false;
    }

    constexpr unsigned FD = 0;
    task.set_fd_cloexec(FD);
    bool ok = install_exec_fd_file_checked(&task, FD, success_file) && task.fd_table.lookup(FD) == success_file && !task.get_fd_cloexec(FD);

    auto* installed = static_cast<vfs::File*>(task.fd_table.remove(FD));
    ok = ok && installed == success_file;
    vfs::vfs_put_file(installed);

    auto* failed_file = exec_selftest_make_file();
    if (failed_file == nullptr) {
        return false;
    }
    g_exec_selftest_force_stdio_insert_failure.store(true, std::memory_order_relaxed);
    bool const INSERTED = install_exec_fd_file_checked(&task, FD, failed_file);
    g_exec_selftest_force_stdio_insert_failure.store(false, std::memory_order_relaxed);

    ok = ok && !INSERTED && task.fd_table.lookup(FD) == nullptr && g_exec_selftest_close_count.load(std::memory_order_relaxed) == 2;
    return ok;
}

auto exec_selftest_cloexec_snapshot_collects_marked_fds() -> bool {
    ker::mod::sched::task::Task task{};
    task.pid = 0x7B90;
    ker::vfs::File fd0{};
    ker::vfs::File fd1{};
    ker::vfs::File fd2{};
    ker::vfs::File fd255{};

    bool ok = task.fd_table.insert(0, &fd0) && task.fd_table.insert(1, &fd1) && task.fd_table.insert(2, &fd2) &&
              task.fd_table.insert(255, &fd255);
    task.set_fd_cloexec(0);
    task.set_fd_cloexec(255);

    FdSnapshot snapshot{};
    size_t const COUNT = collect_cloexec_fds_locked(&task, snapshot);
    bool saw0 = false;
    bool saw255 = false;
    for (size_t i = 0; i < COUNT; ++i) {
        saw0 = saw0 || fixed_slot(snapshot, i) == 0;
        saw255 = saw255 || fixed_slot(snapshot, i) == 255;
    }

    ker::mod::sched::task::Task worker{};
    worker.is_thread = true;
    worker.pid = task.pid + 1;
    worker.owner_pid = task.pid;
    ok &= collect_cloexec_fds_locked(&worker, snapshot) == 0 && !ensure_exec_stdio_fallbacks(&worker);
    ok &= ker::mod::sched::resource::bind_thread_owner(worker, task);
    // Poison the obsolete per-thread table/bitmap. Exec must use only the
    // retained owner, including after the main thread has retired.
    ok &= worker.fd_table.insert(7, &fd1);
    worker.set_fd_cloexec(7);
    task.main_thread_retired.store(true);
    size_t const WORKER_COUNT = collect_cloexec_fds_locked(&worker, snapshot);
    bool worker_saw0 = false;
    bool worker_saw255 = false;
    for (size_t i = 0; i < WORKER_COUNT; ++i) {
        worker_saw0 |= fixed_slot(snapshot, i) == 0;
        worker_saw255 |= fixed_slot(snapshot, i) == 255;
    }
    ok &= WORKER_COUNT == 2 && worker_saw0 && worker_saw255 && ensure_exec_stdio_fallbacks(&worker) && worker.fd_table.size() == 1;
    worker.fd_table.remove(7);

    task.fd_table.remove(0);
    task.fd_table.remove(1);
    task.fd_table.remove(2);
    task.fd_table.remove(255);
    return ok && COUNT == 2 && saw0 && saw255;
}

auto exec_selftest_spawn_dup2_consumes_cloexec_source() -> bool {
    ker::mod::sched::task::Task parent{};
    ker::mod::sched::task::Task child{};
    ker::vfs::File source{};
    ker::vfs::File displaced{};
    source.refcount.store(1, std::memory_order_relaxed);
    displaced.refcount.store(1, std::memory_order_relaxed);

    constexpr int32_t SOURCE_FD = 7;
    constexpr int32_t DESTINATION_FD = 1;
    child.pid = 42;
    bool ok = parent.fd_table.insert(SOURCE_FD, &source) && parent.fd_table.insert(DESTINATION_FD, &displaced);
    parent.set_fd_cloexec(SOURCE_FD);
    ok = ok && clone_exec_fd_table_checked(&parent, &child, true) && child.get_fd_cloexec(SOURCE_FD);

    ker::abi::process::SpawnFdAction action{
        .type = static_cast<uint32_t>(ker::abi::process::SpawnFdActionType::DUP2),
        .fd = DESTINATION_FD,
        .srcfd = SOURCE_FD,
    };
    ker::abi::process::SpawnOptions options{
        .size = sizeof(ker::abi::process::SpawnOptions),
        .version = ker::abi::process::SPAWN_OPTIONS_VERSION,
        .flags = ker::abi::process::SPAWN_FLAG_SETPGROUP,
        .pgroup = 0,
        .actions = &action,
        .action_count = 1,
    };
    options.pgroup = 0x7fffffff;
    int error = 0;
    ok = ok && !apply_spawn_options(&child, &options, &error) && error == -EPERM && child.fd_table.lookup(DESTINATION_FD) == &displaced &&
         child.fd_table.lookup(SOURCE_FD) == &source && child.get_fd_cloexec(SOURCE_FD) &&
         source.refcount.load(std::memory_order_relaxed) == 2;
    options.pgroup = 0;
    options.flags = 0;
    auto const BEFORE = child.credentials();
    action.srcfd = 31;
    ok &= !apply_spawn_options(&child, &options, &error) && error == -EBADF && child.fd_table.lookup(DESTINATION_FD) == &displaced &&
          child.get_fd_cloexec(SOURCE_FD) && source.refcount.load(std::memory_order_relaxed) == 2 &&
          displaced.refcount.load(std::memory_order_relaxed) == 2;
    action.fd = 31;
    ok &= !apply_spawn_options(&child, &options, &error) && error == -EBADF && child.fd_table.size() == 2;
    action.srcfd = SOURCE_FD;
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    if (child.resources.set(NOFILE, {16, 16}, true, 256) != 0) ok = false;
    ok &= !apply_spawn_options(&child, &options, &error) && error == -EBADF && child.fd_table.size() == 2;
    // Same-descriptor actions preserve inherited handles above a lowered limit,
    // but clear CLOEXEC as posix_spawn DUP2 requires.
    if (child.resources.set(NOFILE, {4, 16}, true, 256) != 0) ok = false;
    action.fd = SOURCE_FD;
    ok &= apply_spawn_options(&child, &options, &error) && error == 0 && !child.get_fd_cloexec(SOURCE_FD) &&
          source.refcount.load(std::memory_order_relaxed) == 2;
    child.set_fd_cloexec(SOURCE_FD);
    int reservation_owner{};
    uint64_t const RESERVED = child.fd_table.reserve(2, 4, &reservation_owner);
    action.fd = 2;
    ok &= RESERVED == 2 && !apply_spawn_options(&child, &options, &error) && error == -EBUSY && child.fd_table.lookup(2) == nullptr &&
          child.fd_table.reserved(2) && child.get_fd_cloexec(SOURCE_FD) && source.refcount.load(std::memory_order_relaxed) == 2;
    ok &= child.fd_table.cancel(RESERVED, &reservation_owner);
    action.fd = DESTINATION_FD;
    ok &= child.credentials() == BEFORE && child.fd_table.reserved_count() == 0;
    options.flags = ker::abi::process::SPAWN_FLAG_SETPGROUP;
    ok = ok && apply_spawn_options(&child, &options);
    close_spawn_cloexec_fds(&child);
    ok = ok && child.fd_table.lookup(SOURCE_FD) == nullptr && child.fd_table.lookup(DESTINATION_FD) == &source &&
         !child.get_fd_cloexec(DESTINATION_FD) && child.job_identity().pgid == child.pid &&
         source.refcount.load(std::memory_order_relaxed) == 2 && displaced.refcount.load(std::memory_order_relaxed) == 1;

    release_task_fd_table_files(&child);
    ok = ok && source.refcount.load(std::memory_order_relaxed) == 1;
    parent.fd_table.remove(SOURCE_FD);
    parent.fd_table.remove(DESTINATION_FD);
    return ok;
}

auto exec_selftest_spawn_open_uses_private_child_authority() -> bool {
    using namespace ker::mod;
    constexpr const char* DIR = "/tmp/ktest_spawn_open";
    constexpr const char* PATH = "/tmp/ktest_spawn_open/file";
    sched::task::Task child{};
    child.pid = 0x7B91;
    std::strcpy(child.root.data(), "/");
    std::strcpy(child.cwd.data(), DIR);
    struct Cleanup {
        sched::task::Task& task;
        vfs::File* source{};
        ~Cleanup() {
            release_task_fd_table_files(&task);
            if (source) vfs::vfs_put_file(source);
            vfs::vfs_unlink("/tmp/ktest_spawn_open/file");
            vfs::vfs_rmdir("/tmp/ktest_spawn_open/private");
            vfs::vfs_rmdir("/tmp/ktest_spawn_open");
        }
    } cleanup{child};
    vfs::vfs_mkdir("/tmp", 0755);
    if (vfs::vfs_mkdir(DIR, 0755) != 0 || vfs::vfs_mkdir("/tmp/ktest_spawn_open/private", 0700) != 0) return false;
    cleanup.source = vfs::vfs_open_file(PATH, vfs::O_CREAT | vfs::O_EXCL | 2, 0600);
    if (!cleanup.source) return false;
    auto user = security::Snapshot::bootstrap_root();
    if (user.prepare({.change = security::Change::SET_UID, .real = 1000}, user) != 0 ||
        sched::resource::initialize_credentials(child, user) != 0)
        return false;
    ker::abi::process::SpawnFdAction action{.type = static_cast<uint32_t>(ker::abi::process::SpawnFdActionType::OPEN),
                                            .fd = 8,
                                            .oflag = vfs::O_LOCAL | vfs::O_TRUNC | 1,
                                            .path = "file"};
    bool ok = spawn_open_fd(&child, action) == -EACCES && child.fd_table.empty();
    if (vfs::vfs_chmod(PATH, 0666) != 0) return false;
    action.path = "private/../file";
    ok &= spawn_open_fd(&child, action) == -EACCES && child.fd_table.empty();
    action.path = "file";
    action.oflag = vfs::O_LOCAL | vfs::O_CLOEXEC;
    ok &=
        spawn_open_fd(&child, action) == 0 && child.fd_table.size() == 1 && child.fd_table.lookup(8) != nullptr && child.get_fd_cloexec(8);
    action.fd = 9;
    action.path = "/proc/self/fd/8";
    action.oflag = vfs::O_LOCAL;
    ok &= spawn_open_fd(&child, action) == 0 && child.fd_table.lookup(9) == child.fd_table.lookup(8);
    spawn_close_fd(&child, 9);
    close_spawn_cloexec_fds(&child);
    ok &= child.fd_table.empty() && child.credentials() == user;
    ok &= spawn_open_fd(&child, action) == -ENOENT && child.fd_table.empty();
    action.path = "file";
    // Full table replacement must reserve the destination released by OPEN,
    // rather than fail after a successful create/truncate needing another slot.
    constexpr unsigned NOFILE = static_cast<unsigned>(ker::abi::resource::Id::NOFILE);
    if (child.resources.set(NOFILE, {4, 4}, true, 256) != 0) return false;
    for (unsigned fd = 0; fd < 4; ++fd) {
        vfs::vfs_retain_file(cleanup.source);
        if (!child.fd_table.insert(fd, cleanup.source)) {
            vfs::vfs_put_file(cleanup.source);
            return false;
        }
    }
    action.fd = 4;
    ok &= spawn_open_fd(&child, action) == -EBADF && child.fd_table.size() == 4;
    action.fd = 3;
    action.oflag = vfs::O_LOCAL;
    ok &= spawn_open_fd(&child, action) == 0 && child.fd_table.size() == 4 && child.fd_table.lookup(3) != cleanup.source &&
          !child.get_fd_cloexec(3);
    ok &= child.fd_table.reserved_count() == 0 && child.credentials() == user;
    return ok;
}

auto exec_selftest_spawn_procfs_failure_drains_child(ker::mod::sched::task::Task& parent) -> bool {
    using namespace ker::mod;
    abi::resource::Usage before{}, after{};
    sched::resource::copy_process_observation(parent, before);
    auto candidate = std::make_unique<sched::task::Task>();
    if (!candidate) return false;
    candidate->pid = sched::task::get_next_pid();
    candidate->type = sched::task::TaskType::PROCESS;
    auto const CREDENTIALS = parent.credentials();
    sched::resource::ProcessReservation reservation(&parent, CREDENTIALS.user().real);
    if (reservation.result() != 0 || !sched::resource::inherit(*candidate, parent) ||
        sched::resource::initialize_credentials(*candidate, CREDENTIALS) != 0 || !reservation.attach(*candidate))
        return false;
    sched::task::Task::FilesystemContext filesystem;
    if (!parent.capture_filesystem_context(filesystem)) return false;
    candidate->root = filesystem.root;
    candidate->cwd = filesystem.cwd;
    candidate->root_len = filesystem.root_len;
    candidate->cwd_len = filesystem.cwd_len;
    candidate->root_anchor = std::move(filesystem.root_anchor);
    candidate->cwd_anchor = std::move(filesystem.cwd_anchor);
    if (!sched::task::claim_unpublished_process(&parent, candidate.get())) return false;
    auto* child = candidate.release();
    std::array<abi::process::SpawnFdAction, 3> actions{};
    actions[0] = {.type = static_cast<uint32_t>(abi::process::SpawnFdActionType::OPEN), .fd = 30, .path = "/proc/self/status"};
    actions[1] = {.type = static_cast<uint32_t>(abi::process::SpawnFdActionType::DUP2), .fd = 31, .srcfd = 30};
    actions[2] = {.type = static_cast<uint32_t>(abi::process::SpawnFdActionType::OPEN), .fd = 32, .path = "/missing-spawn-rollback-path"};
    abi::process::SpawnOptions options{.size = sizeof(abi::process::SpawnOptions),
                                       .version = abi::process::SPAWN_OPTIONS_VERSION,
                                       .actions = actions.data(),
                                       .action_count = actions.size()};
    int error = 0;
    bool const FAILED = !apply_spawn_options(child, &options, &error);
    auto* source = static_cast<vfs::File*>(child->fd_table.lookup(30));
    bool const EXACT = FAILED && error == -ENOENT && source != nullptr && source == child->fd_table.lookup(31) &&
                       source->fs_type == vfs::FSType::PROCFS && source->refcount.load() == 2 && child->fd_table.size() == 2 &&
                       child->fd_table.reserved_count() == 0 && child->ref_count.load() == 3;
    bool const DESTROYED = sched::task::destroy_owned_unpublished_process(&parent, child);
    if (!DESTROYED) {
        // Test failure recovery only: the fixture owns every descriptor and
        // has no external alias. Preserve the production refusal, then drain
        // this fixture explicitly so a failed assertion cannot leak its Task.
        release_task_fd_table_files(child);
        static_cast<void>(sched::task::destroy_owned_unpublished_process(&parent, child));
    }
    sched::resource::copy_process_observation(parent, after);
    return EXACT && DESTROYED && parent.owned_unpublished_process.load() == nullptr && !parent.unpublished_teardown_in_progress.load() &&
           before.current == after.current;
}

auto exec_selftest_one_shot_wki_target_consumes_only_on_success() -> bool {
    ker::mod::sched::task::Task one_shot{};
    std::strncpy(one_shot.wki_target_hostname.data(), "wos-2", one_shot.wki_target_hostname.size() - 1);
    one_shot.wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_STRICT | ker::mod::sched::task::Task::WKI_TARGET_FLAG_ONESHOT;
    consume_successful_one_shot_wki_target(&one_shot, ker::net::wki::WkiRemoteSpawnResult::LOCAL);

    ker::mod::sched::task::Task persistent{};
    persistent.wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED;
    consume_successful_one_shot_wki_target(&persistent, ker::net::wki::WkiRemoteSpawnResult::REMOTE);

    ker::mod::sched::task::Task failed{};
    failed.wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE | ker::mod::sched::task::Task::WKI_TARGET_FLAG_ONESHOT;
    consume_successful_one_shot_wki_target(&failed, ker::net::wki::WkiRemoteSpawnResult::FAILED);

    return one_shot.wki_target_hostname.front() == '\0' &&
           one_shot.wki_target_flags == ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL && one_shot.wki_skip_legacy_placement &&
           persistent.wki_target_flags == ker::mod::sched::task::Task::WKI_TARGET_FLAG_BALANCED &&
           failed.wki_target_flags ==
               (ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE | ker::mod::sched::task::Task::WKI_TARGET_FLAG_ONESHOT);
}
#endif

auto supports_file_backed_process(vfs::File* file, size_t file_size) -> bool {
    PreparedFileBackedElf prepared_elf;
    return prepare_file_backed_elf(file, file_size, prepared_elf) == PrepareFileBackedElfResult::READY;
}

#ifdef WOS_SELFTEST
namespace {
std::atomic<NativeConstructionFault> g_native_construction_fault{NativeConstructionFault::NONE};
uint32_t g_native_construction_scratch_destructions{};
}  // namespace
void exec_selftest_fail_native_construction_once(NativeConstructionFault fault) {
    g_native_construction_fault.store(fault, std::memory_order_release);
}
auto exec_selftest_take_native_construction_fault(NativeConstructionFault fault) -> bool {
    return g_native_construction_fault.compare_exchange_strong(fault, NativeConstructionFault::NONE, std::memory_order_acq_rel);
}
auto exec_selftest_take_native_construction_fault(NativeConstructionFault fault, NativeConstructionToken token) noexcept -> bool {
    if (token.actor != nullptr && token.cookie != 0 && exec_selftest_consume_scoped_native_construction_fault(token, fault)) return true;
    // Existing preboot tests retain their exact global fault semantics. The
    // scheduled controller never arms this independent boot-only selector.
    return exec_selftest_take_native_construction_fault(fault);
}
auto exec_selftest_native_construction_scratch_destructions() -> uint32_t { return g_native_construction_scratch_destructions; }
#endif

namespace {
// Defers fatal stack abandonment, without holding a lock or disabling IRQs.
// Private construction's sole executor owns the recovery slot throughout.
class NativeConstructionExecutorGuard {
   public:
    explicit NativeConstructionExecutorGuard(mod::sched::task::Task& owner) : m_owner(&owner) {
        bool expected = false;
        m_acquired = owner.unpublished_teardown_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }
    ~NativeConstructionExecutorGuard() {
        if (m_acquired) m_owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
    }
    NativeConstructionExecutorGuard(const NativeConstructionExecutorGuard&) = delete;
    auto operator=(const NativeConstructionExecutorGuard&) -> NativeConstructionExecutorGuard& = delete;
    [[nodiscard]] auto acquired() const -> bool { return m_acquired; }
    // The receiving input transaction assumes this exact flag without a gap.
    // Failure leaves ownership here; only the native builder may use this seam.
    auto transfer_to(mod::sched::task::Task& owner) -> bool {
        if (!m_acquired || m_owner != &owner || !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire)) return false;
        m_acquired = false;
        return true;
    }

   private:
    mod::sched::task::Task* m_owner;
    bool m_acquired{};
};

auto capture_native_arguments(const NativeProcessArguments* input, ExecRequestArguments& request,
                              ExecRequestArguments::NativeScratch& scratch) -> int {
#ifdef WOS_SELFTEST
    scratch.fault_token = request.fault_token;
#endif
    if (input == nullptr) return 0;
    auto const PATH = input->executable_path;
    if (PATH.empty() || PATH.data() == nullptr || PATH.front() != '/' || PATH.size() >= EXEC_PATH_MAX ||
        std::memchr(PATH.data(), '\0', PATH.size()) != nullptr)
        return -EINVAL;
    constexpr size_t COUNT_MAX = 256;
    if (input->argv.size() > COUNT_MAX || input->envp.size() > COUNT_MAX) return -E2BIG;
    if ((!input->argv.empty() && input->argv.data() == nullptr) || (!input->envp.empty() && input->envp.data() == nullptr)) return -EINVAL;
    size_t budget = (input->argv.size() + input->envp.size() + 2) * sizeof(uint64_t);
    size_t bytes = 0;
    for (auto const vector : {input->argv, input->envp}) {
        for (auto const text : vector) {
            if (text.size() >= EXEC_ARG_BYTES_MAX - budget) return -E2BIG;
            if (!text.empty() && (text.data() == nullptr || std::memchr(text.data(), '\0', text.size()) != nullptr)) return -EINVAL;
            budget += text.size() + 1;
            bytes += text.size() + 1;
        }
    }
    auto& snapshot = request.snapshot;
    std::memcpy(snapshot.path.data(), PATH.data(), PATH.size());
    snapshot.argv = new (std::nothrow) const char*[input->argv.size() + 1]{};
    if (snapshot.argv == nullptr) return -ENOMEM;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::ARGUMENT_POINTERS, request.fault_token)) return -ENOMEM;
#endif
    snapshot.envp = new (std::nothrow) const char*[input->envp.size() + 1]{};
    if (snapshot.envp == nullptr) return -ENOMEM;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::ARGUMENT_ARENA, request.fault_token)) return -ENOMEM;
#endif
    snapshot.strings = new (std::nothrow) char[bytes != 0 ? bytes : 1];
    if (snapshot.strings == nullptr) return -ENOMEM;
    char* cursor = snapshot.strings;
    auto copy = [&](std::span<const std::string_view> source, const char** destination) {
        for (size_t i = 0; i < source.size(); ++i) {
            destination[i] = cursor;
            if (!source[i].empty()) std::memcpy(cursor, source[i].data(), source[i].size());
            cursor[source[i].size()] = '\0';
            cursor += source[i].size() + 1;
        }
    };
    copy(input->argv, snapshot.argv);
    copy(input->envp, snapshot.envp);
    snapshot.strings_used = bytes;
    snapshot.budget_used = budget;
    scratch.argv_count = input->argv.size();
    scratch.envp_count = input->envp.size();
    return 0;
}

auto prepare_native_interpreter(mod::sched::task::Task& child, ExecRequestArguments::NativeScratch& scratch) -> int {
    if (child.pending_interp_path.front() == '\0') return 0;
    if (mod::security::compartment::remote_namespace_admission(&child) != -ENOTSUP) return -EIDRM;
    auto const CREDENTIALS = child.credentials();
    int result = vfs::vfs_open_file_as(child, CREDENTIALS, child.pending_interp_path.data(), 0, &scratch.interp_file);
    if (result != 0) return result;
    if (scratch.interp_file == nullptr || scratch.interp_file->fops != vfs::tmpfs::get_tmpfs_fops() || scratch.interp_file->is_directory)
        return -ENOTSUP;
    result = vfs::vfs_stabilize_exec_file(&scratch.interp_file);
    if (result != 0) return result;
    vfs::Stat stat{};
    result = vfs::vfs_fstat_file(scratch.interp_file, &stat);
    if (result != 0) return result;
    result = check_exec_permission_from_stat(CREDENTIALS, stat);
    if (result != 0) return result;
    if (stat.st_size <= 0) return -ENOEXEC;
    auto const SIZE = static_cast<size_t>(stat.st_size);
    auto& prepared = scratch.prepared_interp;
    auto const PREPARED = prepare_file_backed_elf(scratch.interp_file, SIZE, prepared, mod::mm::user_layout::ImageRole::INTERPRETER);
    if (PREPARED == PrepareFileBackedElfResult::OUT_OF_MEMORY) return -ENOMEM;
    if (PREPARED == PrepareFileBackedElfResult::IO_ERROR) return prepared.read_context.error != 0 ? prepared.read_context.error : -EIO;
    if (PREPARED == PrepareFileBackedElfResult::INVALID) return -ENOEXEC;
    if (PREPARED != PrepareFileBackedElfResult::READY) {
        constexpr size_t BUFFERED_IMAGE_MAX = 8 * 1024 * 1024;
        if (SIZE > BUFFERED_IMAGE_MAX) return -E2BIG;
        prepared.reset();
#ifdef WOS_SELFTEST
        if (exec_selftest_take_native_construction_fault(NativeConstructionFault::INTERPRETER_BUFFER, scratch.fault_token)) return -ENOMEM;
#endif
        scratch.interp_buf = new (std::nothrow) uint8_t[SIZE];
        if (scratch.interp_buf == nullptr) return -ENOMEM;
        int read_error = 0;
        if (!read_exec_file_exact(scratch.interp_file, SIZE, 0, scratch.interp_buf, SIZE, &read_error))
            return read_error != 0 ? read_error : -EIO;
    }
    loader::elf::ElfLoadOptions const OPTIONS{
        .register_special_symbols = false,
        .base_address = 0,
        .lazy_file_ranges = nullptr,
        .debug_registry_pid = 0,
        .image_role = mod::mm::user_layout::ImageRole::INTERPRETER,
        .policy_owner = &child,
    };
    auto const IMAGE = PREPARED == PrepareFileBackedElfResult::READY
                           ? loader::elf::load_elf(prepared.view, child.pagemap, child.pid, "ld.so", OPTIONS)
                           : loader::elf::load_elf(scratch.interp_buf, SIZE, child.pagemap, child.pid, "ld.so", OPTIONS);
    if (IMAGE.entry_point == 0) return prepared.read_context.error != 0 ? prepared.read_context.error : -ENOEXEC;
    child.context.frame.rip = IMAGE.entry_point;
    child.interp_base = IMAGE.load_base;
    child.interp_vaddr_start = IMAGE.image_start;
    child.interp_vaddr_end = IMAGE.image_end;
    child.pending_interp_path.fill('\0');
    (void)mod::sched::resource::observe_address_space(child);
    return 0;
}

auto prepare_native_stack(mod::sched::task::Task& child, const ExecArgumentsSnapshot& snapshot,
                          ExecRequestArguments::NativeScratch& scratch) -> int {
    if (snapshot.path.front() == '\0') return 0;
    auto* thread = child.thread;
    if (thread == nullptr || child.pagemap == nullptr || thread->stack <= thread->stack_base_virt) return -EINVAL;
    uint64_t const TOP = thread->stack;
    uint64_t const CAPACITY = TOP - thread->stack_base_virt;
    uint64_t offset = 0;
    int error = 0;
    auto push = [&](const void* data, size_t bytes) -> uint64_t {
        if (offset > CAPACITY || bytes > CAPACITY - offset) {
            error = -E2BIG;
            return 0;
        }
        uint64_t const ADDRESS = TOP - offset - bytes;
        if (!mod::sched::threading::ensure_stack_backing(thread, child.pagemap, ADDRESS, ADDRESS + bytes, &child)) {
            error = -ENOMEM;
            return 0;
        }
        auto const* source = static_cast<const uint8_t*>(data);
        size_t copied = 0;
        while (copied < bytes) {
            uint64_t const VIRTUAL = ADDRESS + copied;
            uint64_t const PHYSICAL = mod::mm::virt::translate(child.pagemap, VIRTUAL);
            if (PHYSICAL == mod::mm::virt::PADDR_INVALID) {
                error = -EFAULT;
                return 0;
            }
            size_t const CHUNK = std::min(bytes - copied, mod::mm::paging::PAGE_SIZE - (VIRTUAL % mod::mm::paging::PAGE_SIZE));
            std::memcpy(mod::mm::addr::get_virt_pointer(PHYSICAL), source + copied, CHUNK);
            copied += CHUNK;
        }
        offset += bytes;
        return ADDRESS;
    };
    scratch.argv_addrs = new (std::nothrow) uint64_t[scratch.argv_count + 1]{};
    if (scratch.argv_addrs == nullptr) return -ENOMEM;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::STACK_ADDRESSES, scratch.fault_token)) return -ENOMEM;
#endif
    scratch.envp_addrs = new (std::nothrow) uint64_t[scratch.envp_count + 1]{};
    if (scratch.envp_addrs == nullptr) return -ENOMEM;
    for (size_t i = 0; i < scratch.argv_count; ++i) {
        scratch.argv_addrs[i] = push(snapshot.argv[i], std::strlen(snapshot.argv[i]) + 1);
        if (error != 0) return error;
    }
    for (size_t i = 0; i < scratch.envp_count; ++i) {
        scratch.envp_addrs[i] = push(snapshot.envp[i], std::strlen(snapshot.envp[i]) + 1);
        if (error != 0) return error;
    }
    uint64_t const EXECFN = push(snapshot.path.data(), std::strlen(snapshot.path.data()) + 1);
    if (error != 0) return error;
    std::array<uint8_t, AT_RANDOM_BYTES> random{};
    if (!mod::random::entropy::get_bytes(random.data(), random.size())) return -EIO;
    uint64_t const RANDOM = push(random.data(), random.size());
    std::memset(random.data(), 0, random.size());
    if (error != 0) return error;
    constexpr size_t AUXV_WORDS = 18 + mod::security::exec_policy::IDENTITY_AUXV_WORDS;
    std::array<uint64_t, AUXV_WORDS> auxv{};
    size_t words = 0;
    auto pair = [&](uint64_t key, uint64_t value) {
        auxv.at(words++) = key;
        auxv.at(words++) = value;
    };
    pair(6, mod::mm::paging::PAGE_SIZE);
    pair(9, child.entry);
    pair(3, child.program_header_addr);
    pair(4, child.program_header_ent_size);
    pair(5, child.program_header_count);
    if (child.interp_base != 0) pair(7, child.interp_base);
    pair(25, RANDOM);
    pair(31, EXECFN);
    for (auto const value : mod::security::exec_policy::identity_auxv(child.credentials())) auxv.at(words++) = value;
    pair(0, 0);
    // Align the completed argc/argv/envp/auxv area, including its parity.
    offset += (TOP - offset) % 16;
    size_t const STRUCTURED_WORDS = words + scratch.argv_count + scratch.envp_count + 3;
    if ((STRUCTURED_WORDS % 2) != 0) offset += sizeof(uint64_t);
    if (offset > CAPACITY || STRUCTURED_WORDS > (CAPACITY - offset) / sizeof(uint64_t)) return -E2BIG;
    push(auxv.data(), words * sizeof(uint64_t));
    if (error != 0) return error;
    uint64_t const ENVP = push(scratch.envp_addrs, (scratch.envp_count + 1) * sizeof(uint64_t));
    if (error != 0) return error;
    uint64_t const ARGV = push(scratch.argv_addrs, (scratch.argv_count + 1) * sizeof(uint64_t));
    if (error != 0) return error;
    uint64_t const ARGC = scratch.argv_count;
    uint64_t const RSP = push(&ARGC, sizeof(ARGC));
    if (error != 0) return error;
    child.at_random_addr = RANDOM;
    child.context.frame.rsp = RSP;
    child.context.regs.rdi = ARGC;
    child.context.regs.rsi = ARGV;
    child.context.regs.rdx = ENVP;
    (void)mod::sched::resource::observe_address_space(child);
    return 0;
}

auto create_native_process_task_impl(mod::sched::task::Task* owner, const NativeProcessContext& context, const char* name,
                                     vfs::File* owned_file, size_t file_size, const vfs::Stat& file_stat, uint64_t kernel_rsp,
                                     const mod::sched::resource::State& resources, const mod::security::Snapshot& credentials,
                                     mod::sched::resource::ProcessReservation& reservation,
                                     mod::sched::resource::State* failure_observations, int* construction_error,
                                     mod::mm::numa::Policy memory_policy, NativeConstructionExecutorGuard* transferred_guard = nullptr)
    -> mod::sched::task::Task* {
    if (construction_error != nullptr) *construction_error = 0;
    bool input_guard = false;
    auto refuse_inputs = [&](int error) -> mod::sched::task::Task* {
        if (construction_error != nullptr) *construction_error = error;
        if (owned_file != nullptr) vfs::vfs_put_file(owned_file);
        if (kernel_rsp != 0) mod::mm::phys::page_free(reinterpret_cast<void*>(kernel_rsp - mod::mm::KERNEL_STACK_SIZE));
        if (input_guard) owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
        return nullptr;
    };
    if (owner == nullptr) return refuse_inputs(-ESRCH);
    bool expected = false;
    bool const ACQUIRED = transferred_guard != nullptr
                              ? transferred_guard->transfer_to(*owner)
                              : owner->unpublished_teardown_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    if (!ACQUIRED) return refuse_inputs(-EBUSY);
    input_guard = true;
    if (owner->exit_in_progress.load(std::memory_order_acquire) || owner->has_exited ||
        owner->state.load(std::memory_order_acquire) != mod::sched::task::TaskState::ACTIVE)
        return refuse_inputs(-ECANCELED);
    if (owner->owned_unpublished_process.load(std::memory_order_acquire) != nullptr) return refuse_inputs(-EBUSY);
    if (context.ipc.closed() || context.mounts.closed() || context.pids.closed()) return refuse_inputs(-EIDRM);
    if (context.ipc.is_host() || context.mounts.is_host() || context.pids.is_host() || context.filesystem == nullptr)
        return refuse_inputs(-EINVAL);
    if ((context.service_creator == nullptr) != (context.creator_policy == nullptr)) return refuse_inputs(-EINVAL);
    if (name == nullptr || owned_file == nullptr || kernel_rsp < mod::mm::KERNEL_STACK_SIZE || file_stat.st_size <= 0 ||
        static_cast<size_t>(file_stat.st_size) != file_size)
        return refuse_inputs(-EINVAL);
    if (owned_file->fops != vfs::tmpfs::get_tmpfs_fops() || owned_file->is_directory) return refuse_inputs(-ENOTSUP);
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::TASK, context.fault_token)) return refuse_inputs(-ENOMEM);
#endif
    auto* child = new (std::nothrow) mod::sched::task::Task{};
    if (child == nullptr) return refuse_inputs(-ENOMEM);
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::allocated_attempt_task(context.fault_token, child);
#endif
    if (!mod::sched::task::claim_unpublished_process(owner, child)) {
        delete child;
        return refuse_inputs(-EBUSY);
    }
    child->publish_memory_policy_snapshot(memory_policy);
    // Task allocation can enter reclaim. Fatal exit stays deferred until the
    // slot owns the child and all input ownership has moved off this stack.
    child->exec_image_file = std::exchange(owned_file, nullptr);
    child->exec_image_size = file_size;
    child->context.syscall_kernel_stack = std::exchange(kernel_rsp, 0);
    owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
    input_guard = false;
    auto abort_child = [&](int error) -> mod::sched::task::Task* {
        if (construction_error != nullptr) *construction_error = error;
        if (failure_observations != nullptr) {
            uint64_t const FLAGS = child->resource_lock.lock_irqsave();
            *failure_observations = child->resources;
            child->resource_lock.unlock_irqrestore(FLAGS);
        }
        if (!mod::sched::task::destroy_owned_unpublished_process(owner, child)) [[unlikely]] {
            mod::dbg::panic_handler("native construction: lost child recovery ownership");
            hcf();
        }
        return nullptr;
    };
#ifdef WOS_SELFTEST
    if (int const ERROR = child->initialize_unpublished_process_skeleton(*owner, name, context.fault_token); ERROR != 0)
        return abort_child(ERROR);
#else
    if (int const ERROR = child->initialize_unpublished_process_skeleton(*owner, name); ERROR != 0) return abort_child(ERROR);
#endif
    child->resources = resources;
    if (int const ERROR = mod::sched::resource::initialize_credentials(*child, credentials); ERROR != 0) return abort_child(ERROR);
    if (!reservation.attach(*child)) return abort_child(-EAGAIN);
    if (!child->initialize_ipc_namespace(context.ipc) || !child->initialize_mount_namespace(context.mounts) ||
        !child->initialize_pid_namespace(context.pids))
        return abort_child(-EIDRM);
    if (int const ERROR = vfs::vfs_install_unpublished_filesystem_context(*child, *context.filesystem); ERROR != 0)
        return abort_child(ERROR);
    if (context.service_creator != nullptr) {
        if (int const ERROR =
                mod::sched::resource::attach_native_creation_service(*child, *context.service_creator, *context.creator_policy);
            ERROR != 0)
            return abort_child(ERROR);
    }

    // The lease wrapper has temporary backend owners of its own. Defer fatal
    // exit until it has published the replacement File or retired its candidate.
    expected = false;
    if (!owner->unpublished_teardown_in_progress.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        return abort_child(-EBUSY);
    int const STABLE = vfs::vfs_stabilize_exec_file(&child->exec_image_file);
    owner->unpublished_teardown_in_progress.store(false, std::memory_order_release);
    if (STABLE != 0) return abort_child(STABLE);
    vfs::Stat fresh{};
    if (int const ERROR = vfs::vfs_fstat_file(child->exec_image_file, &fresh); ERROR != 0) return abort_child(ERROR);
    if (fresh.st_size <= 0 || static_cast<size_t>(fresh.st_size) != file_size) return abort_child(-EAGAIN);
    if (int const ERROR = check_exec_permission_from_stat(credentials, fresh); ERROR != 0) return abort_child(ERROR);
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::SCRATCH, context.fault_token)) return abort_child(-ENOMEM);
#endif
    ExecRequestArguments* request = nullptr;
    ExecRequestArguments::NativeScratch* scratch = nullptr;
    int arguments_error = 0;
    {
        NativeConstructionExecutorGuard guard(*owner);
        if (!guard.acquired()) {
            arguments_error = -EBUSY;
        } else {
            std::unique_ptr<ExecRequestArguments> pending(new (std::nothrow) ExecRequestArguments);
            if (!pending) {
                arguments_error = -ENOMEM;
            } else {
                pending->memory_policy = memory_policy;
#ifdef WOS_SELFTEST
                pending->selftest_destructions = &g_native_construction_scratch_destructions;
#endif
#ifdef WOS_SELFTEST
                pending->fault_token = context.fault_token;
#endif
                request = pending.get();
                scratch = request->native_scratch(0);
                arguments_error =
                    adopt_exec_arguments(*child, pending) ? capture_native_arguments(context.arguments, *request, *scratch) : -EBUSY;
            }
        }
    }
    if (arguments_error != 0) return abort_child(arguments_error);
    // Memory-only parser tables and lazy staging now survive an abandoned stack
    // inside the exclusively owned child, alongside its File/stack/pagemap.
    auto& prepared = scratch->prepared_elf;
    PrepareFileBackedElfResult const PREPARED = prepare_file_backed_elf(child->exec_image_file, file_size, prepared);
    if (PREPARED == PrepareFileBackedElfResult::OUT_OF_MEMORY) return abort_child(-ENOMEM);
    if (PREPARED == PrepareFileBackedElfResult::IO_ERROR) return abort_child(-EIO);
    if (PREPARED == PrepareFileBackedElfResult::INVALID) return abort_child(-ENOEXEC);
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::RECOVERY_AFTER_READ, context.fault_token)) {
        // Exercise the same transferred owner-slot cleanup used by ACTIVE exit.
        auto* recovered = mod::sched::task::take_unpublished_process(owner);
        if (recovered != child || !mod::sched::task::destroy_unpublished_process(recovered)) [[unlikely]] {
            mod::dbg::panic_handler("native construction: fatal recovery failed");
            hcf();
        }
        if (construction_error != nullptr) *construction_error = -ECANCELED;
        return nullptr;
    }
#endif
    bool initialized = false;
    if (PREPARED == PrepareFileBackedElfResult::READY) {
        child->elf_buffer = prepared.take_task_metadata();
        child->elf_buffer_size = prepared.task_metadata_size;
        child->elf_buffer_complete = false;
        loader::elf::ElfLoadOptions const OPTIONS{
            .register_special_symbols = true,
            .base_address = 0,
            .lazy_file_ranges = exec_lazy_file_segments_enabled() ? &scratch->main_lazy_ranges : nullptr,
        };
        initialized = child->initialize_process_image(prepared.view, OPTIONS) &&
                      append_exec_lazy_file_ranges(child->lazy_vmem_ranges, scratch->main_lazy_ranges, child->exec_image_file, fresh);
    } else {
        constexpr size_t BUFFERED_IMAGE_MAX = 8 * 1024 * 1024;
        if (file_size > BUFFERED_IMAGE_MAX) return abort_child(-E2BIG);
        prepared.reset();
#ifdef WOS_SELFTEST
        if (exec_selftest_take_native_construction_fault(NativeConstructionFault::BUFFER, context.fault_token)) return abort_child(-ENOMEM);
#endif
        child->elf_buffer = new (std::nothrow) uint8_t[file_size];
        if (child->elf_buffer == nullptr) return abort_child(-ENOMEM);
        child->elf_buffer_size = file_size;
        child->elf_buffer_complete = true;
        int read_error = 0;
        if (!read_exec_file_exact(child->exec_image_file, file_size, 0, child->elf_buffer, file_size, &read_error))
            return abort_child(read_error != 0 ? read_error : -EIO);
        initialized = child->initialize_process_image(child->elf_buffer, file_size);
    }
    if (!initialized) return abort_child(-ENOEXEC);
    auto guarded = [&](auto&& operation) -> int {
        NativeConstructionExecutorGuard guard(*owner);
        if (!guard.acquired()) return -EBUSY;
        return operation();
    };
    if (int const ERROR = guarded([&] { return prepare_native_interpreter(*child, *scratch); }); ERROR != 0) return abort_child(ERROR);
#ifdef WOS_SELFTEST
    auto recover_if_requested = [&](NativeConstructionFault fault) -> bool {
        if (!exec_selftest_take_native_construction_fault(fault, context.fault_token)) return false;
        auto* recovered = mod::sched::task::take_unpublished_process(owner);
        if (recovered != child || !mod::sched::task::destroy_unpublished_process(recovered)) [[unlikely]] {
            mod::dbg::panic_handler("native launch: transferred recovery failed");
            hcf();
        }
        if (construction_error != nullptr) *construction_error = -ECANCELED;
        return true;
    };
    if (recover_if_requested(NativeConstructionFault::RECOVERY_INTERPRETER_READ)) return nullptr;
#endif
    if (int const ERROR = guarded([&] { return prepare_native_stack(*child, request->snapshot, *scratch); }); ERROR != 0)
        return abort_child(ERROR);
#ifdef WOS_SELFTEST
    if (recover_if_requested(NativeConstructionFault::RECOVERY_STACK_BUILT)) return nullptr;
#endif
    if (mod::security::compartment::remote_namespace_admission(child) != -ENOTSUP) return abort_child(-EIDRM);
    // Interpreter File release can block; keep the executing owner protected
    // until the child-owned scratch has retired completely.
    if (int const ERROR = guarded([&] {
            cleanup_exec_preparation(*child);
            return 0;
        });
        ERROR != 0)
        return abort_child(ERROR);
    return child;
}

NativeCompartmentPreparation::~NativeCompartmentPreparation() {
    if (child != nullptr && executor.owned_unpublished_process.load(std::memory_order_acquire) == child) [[unlikely]] {
        mod::dbg::panic_handler("native creation: root retired before unpublished child");
        hcf();
    }
    auto drop = [](vfs::File*& file) {
        if (auto* owned = std::exchange(file, nullptr); owned != nullptr) vfs::vfs_put_file(owned);
    };
    drop(target);
    drop(executable);
    for (auto& asset : assets) drop(asset.source);
    for (auto& descriptor : descriptors) drop(descriptor.source);
    if (kernel_rsp != 0) mod::mm::phys::page_free(reinterpret_cast<void*>(kernel_rsp - mod::mm::KERNEL_STACK_SIZE));
    // The root pin prevents init retirement from freeing prepared mount state.
    vfs::finish_prepared_mount(&mount);
    filesystem = {};
    view = {};
    drop(root_pin);
    populator.reset();
    if (!published && !context.mounts.is_host()) {
        int const RETIRED = vfs::retire_mount_namespace(context.mounts);
        if (RETIRED != 0) [[unlikely]] {
            mod::dbg::panic_handler("native creation: private root retirement failed");
            hcf();
        }
    }
    context.mounts.reset();
    if (!published) context.ipc.close();
    context.ipc.reset();
    if (!published) context.pids.close();
    context.pids.reset();
    reservation.reset();
    if (creator != nullptr) creator->release();
}

auto validate_native_asset_path(std::string_view path) -> int {
    if (path.size() < 2 || path.size() >= EXEC_PATH_MAX || path.data() == nullptr || path.front() != '/' || path.back() == '/' ||
        std::memchr(path.data(), '\0', path.size()) != nullptr)
        return -EINVAL;
    size_t start = 1, depth = 0;
    for (size_t end = 1; end <= path.size(); ++end) {
        if (end != path.size() && path[end] != '/') continue;
        auto const NAME = path.substr(start, end - start);
        if (NAME.empty() || NAME == "." || NAME == "..") return -EINVAL;
        if (NAME.size() >= 256 || ++depth > 16) return -ENAMETOOLONG;
        start = end + 1;
    }
    return 0;
}

auto native_regular_capability(vfs::File* file, bool readable, vfs::Stat& stat) -> int {
    if (file == nullptr) return -EBADF;
    int const ACCESS = file->open_flags & 3;
    if (ACCESS > 2 || (readable && ACCESS == 1)) return -EBADF;
    if (file->is_directory || !((file->fs_type == vfs::FSType::TMPFS && file->fops == vfs::tmpfs::get_tmpfs_fops()) ||
                                (file->fs_type == vfs::FSType::XFS && file->fops == vfs::xfs::get_xfs_fops())))
        return -ENOTSUP;
    if (!readable && file->fs_type == vfs::FSType::XFS) {
        auto* mount = vfs::xfs::xfs_file_mount_context(file);
        int const TRANSPORT = net::wki::wki_dev_proxy_block_transport_admission(mount != nullptr ? mount->device : nullptr);
        if (TRANSPORT != 0) return TRANSPORT;
    }
    int const STATUS = vfs::vfs_fstat_file(file, &stat);
    if (STATUS != 0) return STATUS;
    return (stat.st_mode & vfs::S_IFMT) == vfs::S_IFREG && stat.st_size >= 0 ? 0 : -ENOTSUP;
}

auto capture_native_compartment_inputs(NativeCompartmentPreparation& root, ExecRequestArguments& request, const NativeCompartmentSpec& spec)
    -> int {
    using Preparation = NativeCompartmentPreparation;
    if (spec.assets.empty() || spec.assets.size() > Preparation::ASSET_MAX || spec.assets.data() == nullptr ||
        spec.descriptors.size() > Preparation::DESCRIPTOR_MAX || (!spec.descriptors.empty() && spec.descriptors.data() == nullptr) ||
        spec.root_bytes < 4096 || spec.root_bytes > 32 * 1024 * 1024 || spec.root_bytes % 4096 != 0)
        return -EINVAL;
    root.root_bytes = spec.root_bytes;
    {
        // This temporary retained thread owner is released while the executor
        // guard is held; only root.creator survives the image-building call.
        mod::sched::resource::Owner owner(&root.executor);
        auto* source = owner.get();
        if (source == nullptr || !source->try_acquire()) return -ESRCH;
        root.creator = source;
    }
    if (int const STATUS = capture_native_creation_priority(root); STATUS != 0) return STATUS;
    if (int const STATUS = mod::sched::resource::capture_native_creator_policy(*root.creator, spec.policy, root.policy); STATUS != 0)
        return STATUS;
    for (size_t i = 0; i < root.policy.admitted.limits.size(); ++i) root.resources.entries[i].limit = root.policy.admitted.limits[i];
    // Keep the public process image name complete; Task's observation field is
    // smaller than the private argument/import path buffer. Never truncate it.
    if (spec.arguments.executable_path.size() >= root.creator->exe_path.size()) return -ENAMETOOLONG;
    auto* scratch = request.native_scratch(0);
    int const ARGUMENTS = capture_native_arguments(&spec.arguments, request, *scratch);
    if (ARGUMENTS != 0) return ARGUMENTS;
    if (int const PATH = validate_native_asset_path(spec.arguments.executable_path); PATH != 0) return PATH;
    for (size_t i = 0; i < scratch->argv_count; ++i) root.argv[i] = request.snapshot.argv[i];
    for (size_t i = 0; i < scratch->envp_count; ++i) root.envp[i] = request.snapshot.envp[i];
    root.arguments = {.executable_path = request.snapshot.path.data(),
                      .argv = {root.argv.data(), scratch->argv_count},
                      .envp = {root.envp.data(), scratch->envp_count}};
    bool executable = false;
    for (auto const& input : spec.assets) {
        if (int const PATH = validate_native_asset_path(input.destination); PATH != 0) return PATH;
        if ((input.mode & ~0777U) != 0) return -EINVAL;
        for (size_t i = 0; i < root.asset_count; ++i)
            if (input.destination == root.assets[i].path.data()) return -EEXIST;
        auto& asset = root.assets[root.asset_count++];
        std::memcpy(asset.path.data(), input.destination.data(), input.destination.size());
        asset.mode = input.mode;
        if (input.source == nullptr) return -EBADF;
        vfs::vfs_retain_file(input.source);
        asset.source = input.source;
        executable |= input.destination == root.arguments.executable_path;
    }
    if (!executable) return -ENOENT;
    auto const NOFILE = root.policy.admitted.limits[static_cast<unsigned>(mod::sched::resource::Id::NOFILE)].soft;
    for (auto const& input : spec.descriptors) {
        if (input.destination >= mod::sched::task::Task::FD_TABLE_SIZE || input.destination >= NOFILE) return -EMFILE;
        for (size_t i = 0; i < root.descriptor_count; ++i)
            if (input.destination == root.descriptors[i].destination) return -EEXIST;
        if (input.source == nullptr) return -EBADF;
        auto& descriptor = root.descriptors[root.descriptor_count++];
        descriptor.destination = input.destination;
        vfs::vfs_retain_file(input.source);
        descriptor.source = input.source;
    }
    return 0;
}

auto capture_user_native_compartment_inputs(NativeCompartmentPreparation& root, ExecRequestArguments& request, uint64_t address) -> int {
    namespace wire = abi::compartment;
    static_assert(wire::CREATE_OPERATION == static_cast<uint64_t>(abi::process::procmgmt_ops::COMPARTMENT_CREATE));
    static_assert(wire::LIMIT_COUNT == abi::resource::COUNT);
    wire::Spec input;
    if (!mod::sys::usercopy::copy_value_from_task(root.executor, address, input)) return -EFAULT;
    if (input.size != sizeof(input) || input.version != wire::VERSION || input.flags != 0 || input.reserved[0] != 0 ||
        input.reserved[1] != 0 || input.dumpable > 1 ||
        (input.network != wire::Network::DENIED && input.network != wire::Network::SHARED_HOST) || input.asset_count == 0 ||
        input.asset_count > wire::ASSET_MAX || input.descriptor_count > wire::DESCRIPTOR_MAX || input.root_bytes < 4096 ||
        input.root_bytes > wire::ROOT_BYTES_MAX || input.root_bytes % 4096 != 0)
        return -EINVAL;
    if (input.argc > wire::VECTOR_MAX || input.envc > wire::VECTOR_MAX) return -E2BIG;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::USER_INPUT_STATE, root.fault_token)) return -ENOMEM;
#endif
    root.user_inputs.reset(new (std::nothrow) NativeCompartmentUserInputs);
    if (!root.user_inputs) return -ENOMEM;
    auto& stage = *root.user_inputs;
#ifdef WOS_SELFTEST
    stage.fault_token = root.fault_token;
#endif
    auto copy_records = [&](uint64_t source, auto& records, uint64_t count) -> int {
        return count == 0 || mod::sys::usercopy::copy_from_task(root.executor, source, records.data(), count * sizeof(records[0]))
                   ? 0
                   : -EFAULT;
    };
    auto copy_path = [&](wire::String text, auto& destination) -> int {
        if (text.bytes == 0) return -EINVAL;
        if (text.bytes >= destination.size()) return -ENAMETOOLONG;
        if (!mod::sys::usercopy::copy_from_task(root.executor, text.address, destination.data(), text.bytes)) return -EFAULT;
        if (std::memchr(destination.data(), '\0', text.bytes) != nullptr) return -EINVAL;
        destination[text.bytes] = '\0';
        return 0;
    };
    int result = copy_path(input.executable, stage.executable);
    if (result == 0) result = copy_records(input.assets, stage.asset_input, input.asset_count);
    if (result == 0) result = copy_records(input.descriptors, stage.descriptor_input, input.descriptor_count);
    if (result == 0) result = copy_records(input.argv, stage.argv_input, input.argc);
    if (result == 0) result = copy_records(input.envp, stage.envp_input, input.envc);
    if (result != 0) return result;
    for (size_t i = 0; i < input.asset_count; ++i) {
        auto const& asset = stage.asset_input[i];
        if (asset.reserved != 0) return -EINVAL;
        if (int const PATH = copy_path(asset.destination, stage.paths[i]); PATH != 0) return PATH;
        stage.assets[i].destination = {stage.paths[i].data(), static_cast<size_t>(asset.destination.bytes)};
        stage.assets[i].mode = asset.mode;
        stage.assets[i].source = vfs::vfs_get_file_retain(&root.executor, asset.source_fd);
        if (stage.assets[i].source == nullptr) return -EBADF;
    }
    for (size_t i = 0; i < input.descriptor_count; ++i) {
        auto const& descriptor = stage.descriptor_input[i];
        if (descriptor.reserved != 0) return -EINVAL;
        stage.descriptors[i].destination = descriptor.destination;
        stage.descriptors[i].source = vfs::vfs_get_file_retain(&root.executor, descriptor.source_fd);
        if (stage.descriptors[i].source == nullptr) return -EBADF;
    }
    size_t budget = (input.argc + input.envc + 2) * sizeof(uint64_t);
    size_t bytes = 0;
    for (auto vector : {std::span(stage.argv_input.data(), input.argc), std::span(stage.envp_input.data(), input.envc)}) {
        for (auto text : vector) {
            if (text.bytes >= wire::ARGUMENT_BYTES_MAX - budget) return -E2BIG;
            budget += text.bytes + 1;
            bytes += text.bytes + 1;
        }
    }
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::USER_ARGUMENT_ARENA, stage.fault_token)) return -ENOMEM;
#endif
    stage.strings.reset(new (std::nothrow) char[bytes != 0 ? bytes : 1]);
    if (!stage.strings) return -ENOMEM;
    char* cursor = stage.strings.get();
    auto copy_vector = [&](auto const& source, auto& destination, size_t count) -> int {
        for (size_t i = 0; i < count; ++i) {
            auto const TEXT = source[i];
            if (TEXT.bytes != 0 && !mod::sys::usercopy::copy_from_task(root.executor, TEXT.address, cursor, TEXT.bytes)) return -EFAULT;
            if (std::memchr(cursor, '\0', TEXT.bytes) != nullptr) return -EINVAL;
            cursor[TEXT.bytes] = '\0';
            destination[i] = {cursor, static_cast<size_t>(TEXT.bytes)};
            cursor += TEXT.bytes + 1;
        }
        return 0;
    };
    result = copy_vector(stage.argv_input, stage.argv, input.argc);
    if (result == 0) result = copy_vector(stage.envp_input, stage.envp, input.envc);
    if (result != 0) return result;
    stage.spec.policy.authority_ceiling = input.authority_ceiling;
    stage.spec.policy.service_privileges = input.service_privileges;
    stage.spec.policy.dumpable = input.dumpable != 0;
    stage.spec.policy.network = input.network == wire::Network::DENIED ? mod::security::compartment::Network::DENIED
                                                                       : mod::security::compartment::Network::SHARED_HOST;
    for (size_t i = 0; i < input.limits.size(); ++i) stage.spec.policy.limits[i] = {input.limits[i].soft, input.limits[i].hard};
    stage.spec.arguments = {.executable_path = {stage.executable.data(), static_cast<size_t>(input.executable.bytes)},
                            .argv = {stage.argv.data(), input.argc},
                            .envp = {stage.envp.data(), input.envc}};
    stage.spec.assets = {stage.assets.data(), input.asset_count};
    stage.spec.descriptors = {stage.descriptors.data(), input.descriptor_count};
    stage.spec.root_bytes = input.root_bytes;
    result = capture_native_compartment_inputs(root, request, stage.spec);
    // The native request now owns its own strings and File references. Drop
    // user staging while the executor guard still protects blocking releases.
    root.user_inputs.reset();
    return result;
}

auto populate_native_compartment_root(NativeCompartmentPreparation& root) -> int {
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_RESERVATION, root.fault_token)) return -ENOMEM;
#endif
    // The private validated limits include both the live creator and requested
    // NPROC limit. The reservation is owned before root allocation or import IO.
    root.reservation.reset(new (std::nothrow)
                               mod::sched::resource::ProcessReservation(root.resources, root.policy.admitted.credentials.user().real));
    if (!root.reservation) return -ENOMEM;
    if (root.reservation->result() != 0) return root.reservation->result();
    // Inherited descriptions must be admitted before any asset import I/O.
    for (size_t i = 0; i < root.descriptor_count; ++i) {
        vfs::Stat stat{};
        int const STATUS = native_regular_capability(root.descriptors[i].source, false, stat);
        if (STATUS != 0) return STATUS;
    }
    uint64_t bytes = 0;
    uint64_t const FSIZE = root.policy.admitted.limits[static_cast<unsigned>(mod::sched::resource::Id::FSIZE)].soft;
    for (size_t i = 0; i < root.asset_count; ++i) {
        auto& asset = root.assets[i];
        if (int const STATUS = native_regular_capability(asset.source, true, asset.stat); STATUS != 0) return STATUS;
        if (static_cast<uint64_t>(asset.stat.st_size) > FSIZE) return -EFBIG;
        if (static_cast<uint64_t>(asset.stat.st_size) > root.root_bytes) return -E2BIG;
        // Stable native descriptions pin a lease/copy. Mutable source writes
        // cannot change bytes halfway through an admitted root import.
        if (int const STATUS = vfs::vfs_stabilize_exec_file(&asset.source); STATUS != 0) return STATUS;
        if (int const STATUS = vfs::vfs_fstat_file(asset.source, &asset.stat); STATUS != 0) return STATUS;
        if (asset.stat.st_size < 0 || static_cast<uint64_t>(asset.stat.st_size) > FSIZE) return -EFBIG;
        if (static_cast<uint64_t>(asset.stat.st_size) > root.root_bytes) return -E2BIG;
        uint64_t const PAGES = (static_cast<uint64_t>(asset.stat.st_size) + 4095) / 4096;
        if (!mod::sched::resource::add_extent(bytes, PAGES, 4096) || bytes > root.root_bytes) return -E2BIG;
    }
    int status = vfs::mount_ns::create_namespace(root.context.mounts);
    if (status == 0) status = mod::sched::ipc::create_namespace(root.context.ipc);
    if (status == 0)
        status = mod::sched::pid_ns::create_namespace(*root.creator->pid_namespace(), root.policy.admitted.network, root.context.pids);
    if (status != 0) return status;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_ROOT, root.fault_token)) return -ENOMEM;
#endif
    status = vfs::prepare_mount_filesystem("/", "tmpfs", nullptr, 0, "size=32M", nullptr, nullptr, &root.mount,
                                           vfs::MountPathKind::RETAINED_PHYSICAL, root.context.mounts, &root.policy.admitted.credentials);
    if (status == 0) status = vfs::publish_prepared_mount(&root.mount, "/");
    vfs::finish_prepared_mount(&root.mount);
    if (status != 0) return status;
    root.populator.reset(new (std::nothrow) mod::sched::task::Task{});
    if (!root.populator) return -ENOMEM;
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::allocated_attempt_task(root.fault_token, root.populator.get());
#endif
    root.populator->pid = mod::sched::task::get_next_pid();
    root.populator->type = mod::sched::task::TaskType::PROCESS;
    status = mod::sched::resource::initialize_credentials(*root.populator, root.policy.admitted.credentials);
    if (status != 0) return status;
    if (!root.populator->initialize_mount_namespace(root.context.mounts)) return -EIDRM;
    // Provisioning uses explicit manifest modes, never temporary root authority.
    // The service receives its creator's observed umask after construction.
    static_cast<void>(root.populator->exchange_creation_mask(0));
    status = vfs::vfs_pin_private_mount_root_as(root.context.mounts, root.policy.admitted.credentials, &root.root_pin);
    if (status == 0)
        status = vfs::vfs_prepare_directory_view(root.policy.admitted.credentials, root.root_pin, root.root_pin, {}, root.view);
    if (status != 0) return status;
#ifdef WOS_SELFTEST
    if (exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_IMPORT_BUFFER, root.fault_token)) return -ENOMEM;
#endif
    constexpr size_t CHUNK = 8192;
    root.buffer.reset(new (std::nothrow) uint8_t[CHUNK]);
    if (!root.buffer) return -ENOMEM;
    for (size_t i = 0; i < root.asset_count; ++i) {
        auto& asset = root.assets[i];
        std::array<char, EXEC_PATH_MAX> directory{};
        for (size_t end = 1; asset.path[end] != '\0'; ++end) {
            if (asset.path[end] != '/') continue;
            std::memcpy(directory.data(), asset.path.data(), end);
            directory[end] = '\0';
            status = vfs::vfs_mkdirat(root.populator.get(), vfs::AT_FDCWD, directory.data(), 0755);
            if (status != 0 && status != -EEXIST) return status;
        }
        status = vfs::vfs_open_directory_view_as(*root.populator, root.policy.admitted.credentials, root.view, asset.path.data() + 1,
                                                 vfs::O_CREAT | vfs::O_EXCL | 2, static_cast<int>(asset.mode), &root.target);
        if (status != 0) return status;
        size_t const SIZE = static_cast<size_t>(asset.stat.st_size);
        for (size_t offset = 0; offset < SIZE;) {
#ifdef WOS_SELFTEST
            if (exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_IMPORT_READ, root.fault_token)) return -EIO;
#endif
            size_t const COUNT = std::min(CHUNK, SIZE - offset);
            ssize_t const READ = vfs::vfs_pread_file(asset.source, root.buffer.get(), COUNT, static_cast<off_t>(offset));
            if (READ < 0) return static_cast<int>(READ);
            if (READ == 0 || static_cast<size_t>(READ) > COUNT) return -EIO;
            ssize_t const WRITTEN =
                vfs::vfs_pwrite_file(root.target, root.buffer.get(), static_cast<size_t>(READ), static_cast<off_t>(offset));
            if (WRITTEN < 0) return static_cast<int>(WRITTEN);
            if (WRITTEN != READ) return -EIO;
            offset += static_cast<size_t>(READ);
        }
        vfs::vfs_put_file(std::exchange(root.target, nullptr));
        vfs::vfs_put_file(std::exchange(asset.source, nullptr));
    }
    status = vfs::vfs_prepare_filesystem_context(root.policy.admitted.credentials, root.view, root.filesystem);
    if (status == 0)
        status = vfs::vfs_open_directory_view_as(*root.populator, root.policy.admitted.credentials, root.view,
                                                 root.arguments.executable_path.data() + 1, 0, 0, &root.executable);
    if (status == 0) status = vfs::vfs_fstat_file(root.executable, &root.executable_stat);
    if (status != 0) return status;
    root.kernel_rsp = reinterpret_cast<uint64_t>(mod::mm::phys::kernel_stack_alloc("native_compartment"));
    if (root.kernel_rsp == 0) return -ENOMEM;
    root.kernel_rsp += mod::mm::KERNEL_STACK_SIZE;
    root.context.filesystem = &root.filesystem;
    root.context.arguments = &root.arguments;
    root.context.service_creator = root.creator;
    root.context.creator_policy = &root.policy;
    return mod::sched::resource::revalidate_native_creator_policy(*root.creator, root.policy);
}

auto create_file_backed_process_task_impl(const char* name, vfs::File* owned_file, size_t file_size, const vfs::Stat& file_stat,
                                          uint64_t kernel_rsp, const mod::sched::resource::State& resources,
                                          const mod::security::Snapshot& credentials, mod::sched::resource::ProcessReservation& reservation,
                                          mod::sched::resource::State* failure_observations, int* interpreter_error,
                                          mod::sched::task::Task* explicit_owner, const NativeProcessContext* native_context,
                                          bool observe_current_owner) -> mod::sched::task::Task* {
    if (interpreter_error != nullptr) *interpreter_error = 0;
    auto release_inputs = [&]() {
        if (owned_file != nullptr) {
            vfs::vfs_put_file(owned_file);
            owned_file = nullptr;
        }
        if (kernel_rsp != 0) {
            mod::mm::phys::page_free(reinterpret_cast<void*>(kernel_rsp - mod::mm::KERNEL_STACK_SIZE));
            kernel_rsp = 0;
        }
    };

    if (!observe_current_owner && (explicit_owner == nullptr || native_context == nullptr)) {
        if (interpreter_error != nullptr) *interpreter_error = -ESRCH;
        release_inputs();
        return nullptr;
    }
    if (native_context != nullptr) {
        int const ERROR = native_context->ipc.closed() || native_context->mounts.closed() || native_context->pids.closed() ? -EIDRM
                          : native_context->ipc.is_host() || native_context->mounts.is_host() || native_context->pids.is_host() ||
                                  native_context->filesystem == nullptr
                              ? -EINVAL
                              : 0;
        if (ERROR != 0) {
            if (interpreter_error != nullptr) *interpreter_error = ERROR;
            release_inputs();
            return nullptr;
        }
    }

    PreparedFileBackedElf prepared_elf;
    if (name == nullptr || owned_file == nullptr || kernel_rsp == 0 ||
        prepare_file_backed_elf(owned_file, file_size, prepared_elf) != PrepareFileBackedElfResult::READY) {
        release_inputs();
        return nullptr;
    }

    auto* new_task = new mod::sched::task::Task(name, 0, 0, kernel_rsp, mod::sched::task::TaskType::PROCESS);  // NOLINT
    if (new_task == nullptr) {
        release_inputs();
        return nullptr;
    }
    kernel_rsp = 0;

    new_task->elf_buffer = prepared_elf.take_task_metadata();
    new_task->elf_buffer_size = prepared_elf.task_metadata_size;
    new_task->is_elf_buffer_shared = false;
    new_task->elf_buffer_complete = false;
    new_task->exec_image_file = owned_file;
    new_task->exec_image_size = file_size;
    owned_file = nullptr;

    auto* const OWNER = observe_current_owner ? mod::sched::get_current_task() : explicit_owner;
    if (OWNER == nullptr || !mod::sched::task::claim_unpublished_process(OWNER, new_task)) [[unlikely]] {
        mod::dbg::panic_handler("file-backed exec: cannot publish child recovery ownership");
        hcf();
    }

    new_task->resources = resources;
    auto preserve_failure_observations = [&]() {
        if (failure_observations == nullptr) return;
        uint64_t const FLAGS = new_task->resource_lock.lock_irqsave();
        *failure_observations = new_task->resources;
        new_task->resource_lock.unlock_irqrestore(FLAGS);
    };
    if (mod::sched::resource::initialize_credentials(*new_task, credentials) != 0 || !reservation.attach(*new_task)) {
        preserve_failure_observations();
        if (!mod::sched::task::destroy_owned_unpublished_process(OWNER, new_task)) [[unlikely]] {
            mod::dbg::panic_handler("file-backed exec: lost child ownership after process admission failure");
            hcf();
        }
        return nullptr;
    }

    if (native_context != nullptr) {
        int context_error = -EIDRM;
        if (new_task->initialize_ipc_namespace(native_context->ipc) && new_task->initialize_mount_namespace(native_context->mounts) &&
            new_task->initialize_pid_namespace(native_context->pids))
            context_error = vfs::vfs_install_unpublished_filesystem_context(*new_task, *native_context->filesystem);
        if (context_error != 0) {
            if (interpreter_error != nullptr) *interpreter_error = context_error;
            preserve_failure_observations();
            if (!mod::sched::task::destroy_owned_unpublished_process(OWNER, new_task)) [[unlikely]] {
                mod::dbg::panic_handler("file-backed exec: lost child ownership after native context failure");
                hcf();
            }
            return nullptr;
        }
    }

    loader::elf::ElfLazyLoadRangeVec loader_lazy_ranges;
    loader::elf::ElfLoadOptions const LOAD_OPTIONS{
        .register_special_symbols = true,
        .base_address = 0,
        .lazy_file_ranges = exec_lazy_file_segments_enabled() ? &loader_lazy_ranges : nullptr,
    };
    if (!new_task->initialize_process_image(prepared_elf.view, LOAD_OPTIONS) ||
        !append_exec_lazy_file_ranges(new_task->lazy_vmem_ranges, loader_lazy_ranges, new_task->exec_image_file, file_stat) ||
        !mod::sched::task::complete_unpublished_process_construction(new_task, interpreter_error, explicit_owner, observe_current_owner) ||
        (native_context != nullptr && mod::security::compartment::remote_namespace_admission(new_task) != -ENOTSUP)) {
        preserve_failure_observations();
        if (!mod::sched::task::destroy_owned_unpublished_process(OWNER, new_task)) [[unlikely]] {
            mod::dbg::panic_handler("file-backed exec: lost child recovery ownership during teardown");
            hcf();
        }
        return nullptr;
    }
    return new_task;
}
}  // namespace

auto create_file_backed_process_task(const char* name, vfs::File* owned_file, size_t file_size, const vfs::Stat& file_stat,
                                     uint64_t kernel_rsp, const mod::sched::resource::State& resources,
                                     const mod::security::Snapshot& credentials, mod::sched::resource::ProcessReservation& reservation,
                                     mod::sched::resource::State* failure_observations, int* interpreter_error) -> mod::sched::task::Task* {
    // WKI receiver path: machine-local node policies are not transported.
    // Its newly constructed Task deliberately keeps the default LOCAL value.
    return create_file_backed_process_task_impl(name, owned_file, file_size, file_stat, kernel_rsp, resources, credentials, reservation,
                                                failure_observations, interpreter_error, nullptr, nullptr, true);
}

auto create_file_backed_process_task_for_owner(mod::sched::task::Task* owner, const NativeProcessContext& context, const char* name,
                                               vfs::File* owned_file, size_t file_size, const vfs::Stat& file_stat, uint64_t kernel_rsp,
                                               const mod::sched::resource::State& resources, const mod::security::Snapshot& credentials,
                                               mod::sched::resource::ProcessReservation& reservation,
                                               mod::sched::resource::State* failure_observations, int* construction_error)
    -> mod::sched::task::Task* {
    auto const MEMORY_POLICY = owner != nullptr ? owner->memory_policy_snapshot() : mod::mm::numa::Policy{};
    return create_native_process_task_impl(owner, context, name, owned_file, file_size, file_stat, kernel_rsp, resources, credentials,
                                           reservation, failure_observations, construction_error, MEMORY_POLICY);
}

auto abort_native_compartment_preparation(mod::sched::task::Task& executor, mod::sched::task::Task* child) -> bool {
    auto* request = executor.owned_exec_arguments.load(std::memory_order_acquire);
    if (request == nullptr || !request->native_compartment_request) return false;
    auto* root = request->compartment.get();
    if (child == nullptr && root != nullptr) child = root->child;
    if (root != nullptr && root->child != child) return false;
    if (child != nullptr && !mod::sched::task::destroy_owned_unpublished_process(&executor, child)) return false;
    if (root != nullptr) root->child = nullptr;
    cleanup_exec_preparation(executor);
    return executor.owned_exec_arguments.load(std::memory_order_acquire) == nullptr;
}

static auto prepare_native_compartment_inputs_for_owner(mod::sched::task::Task& executor, const NativeCompartmentSpec* spec,
                                                        uint64_t user_address, int& error
#ifdef WOS_SELFTEST
                                                        ,
                                                        NativeConstructionToken fault_token = {}
#endif
                                                        ) -> mod::sched::task::Task* {
    error = 0;
    ExecRequestArguments* request = nullptr;
    mod::sched::task::Task* child = nullptr;
    {
        NativeConstructionExecutorGuard guard(executor);
        if (!guard.acquired()) {
            error = -EBUSY;
            return nullptr;
        }
        if (executor.exit_in_progress.load(std::memory_order_acquire) || executor.has_exited ||
            executor.state.load(std::memory_order_acquire) != mod::sched::task::TaskState::ACTIVE) {
            error = -ECANCELED;
            return nullptr;
        }
        if (executor.owned_unpublished_process.load(std::memory_order_acquire) != nullptr ||
            executor.owned_exec_arguments.load(std::memory_order_acquire) != nullptr ||
            executor.owned_exec_preparation.load(std::memory_order_acquire) != nullptr) {
            error = -EBUSY;
            return nullptr;
        }
        std::unique_ptr<ExecRequestArguments> pending(new (std::nothrow) ExecRequestArguments);
        if (!pending) {
            error = -ENOMEM;
            return nullptr;
        }
        pending->native_compartment_request = true;
        pending->memory_policy = executor.memory_policy_snapshot();
#ifdef WOS_SELFTEST
        pending->fault_token = fault_token;
#endif
        request = pending.get();
        if (!adopt_exec_arguments(executor, pending)) {
            error = -EBUSY;
            return nullptr;
        }
#ifdef WOS_SELFTEST
        if (exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_STATE, request->fault_token)) error = -ENOMEM;
#endif
        if (error == 0) {
#ifdef WOS_SELFTEST
            request->compartment.reset(new (std::nothrow) NativeCompartmentPreparation(executor, request->fault_token));
#else
            request->compartment.reset(new (std::nothrow) NativeCompartmentPreparation(executor));
#endif
            if (!request->compartment) error = -ENOMEM;
        }
        if (error == 0)
            error = spec != nullptr ? capture_native_compartment_inputs(*request->compartment, *request, *spec)
                                    : capture_user_native_compartment_inputs(*request->compartment, *request, user_address);
        if (error == 0) error = populate_native_compartment_root(*request->compartment);
#ifdef WOS_SELFTEST
        if (error == 0 &&
            exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_RECOVERY_IMPORT, request->fault_token))
            error = -ECANCELED;
#endif
        if (error == 0) {
            auto& root = *request->compartment;
            // Inputs stay owned until this exact guard is transferred to the
            // builder. Its child slot adopts both before fatal exit can resume.
            child = create_native_process_task_impl(
                &executor, root.context, request->snapshot.path.data(), std::exchange(root.executable, nullptr),
                static_cast<size_t>(root.executable_stat.st_size), root.executable_stat, std::exchange(root.kernel_rsp, 0), root.resources,
                root.policy.admitted.credentials, *root.reservation, nullptr, &error, request->memory_policy, &guard);
            root.child = child;
            if (child == nullptr && error == 0) error = -ENOEXEC;
        }
    }
    if (error == 0) {
        NativeConstructionExecutorGuard guard(executor);
        if (!guard.acquired()) {
            error = -EBUSY;
        } else {
            auto& root = *request->compartment;
            error = initialize_native_creation_priority(root, *child);
            if (error == 0) {
                child->initialize_job_identity({.session_id = child->pid, .pgid = child->pid}, mod::sched::session::Names{1, 1});
                static_cast<void>(child->exchange_creation_mask(root.policy.creation_mask));
                std::memcpy(child->exe_path.data(), request->snapshot.path.data(), root.arguments.executable_path.size() + 1);
            }
            for (size_t i = 0; i < root.descriptor_count && error == 0; ++i) {
                auto& descriptor = root.descriptors[i];
                vfs::FdReservation slot(child, descriptor.destination);
                if (slot.result() < 0) {
                    error = slot.result();
                } else if (static_cast<unsigned>(slot.result()) != descriptor.destination) {
                    error = -EBUSY;
                } else {
                    int const INSTALLED = slot.install(descriptor.source);
                    if (INSTALLED < 0)
                        error = INSTALLED;
                    else
                        descriptor.source = nullptr;
                }
#ifdef WOS_SELFTEST
                if (error == 0 &&
                    exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_DESCRIPTORS, root.fault_token))
                    error = -ENOMEM;
#endif
            }
            if (error == 0) error = mod::sched::resource::revalidate_native_creator_policy(*root.creator, root.policy);
            // Imported sources, staging buffers and the populator are retired
            // before success; root/cwd ownership waits for actual publication.
            if (error == 0) {
                root.buffer.reset();
                root.populator.reset();
                root.reservation.reset();
            }
        }
    }
#ifdef WOS_SELFTEST
    if (error == 0 &&
        exec_selftest_take_native_construction_fault(NativeConstructionFault::CREATION_RECOVERY_BUILT, request->fault_token)) {
        auto* recovered = mod::sched::task::take_unpublished_process(&executor);
        if (recovered != child || !mod::sched::task::destroy_unpublished_process(recovered)) [[unlikely]] {
            mod::dbg::panic_handler("native creation: transferred child recovery failed");
            hcf();
        }
        request->compartment->child = nullptr;
        child = nullptr;
        error = -ECANCELED;
    }
#endif
    if (error != 0) {
        if (!abort_native_compartment_preparation(executor, child)) [[unlikely]] {
            mod::dbg::panic_handler("native creation: lost preparation ownership");
            hcf();
        }
        return nullptr;
    }
    return child;
}

auto prepare_native_compartment_for_owner(mod::sched::task::Task& executor, const NativeCompartmentSpec& spec, int& error
#ifdef WOS_SELFTEST
                                          ,
                                          NativeConstructionToken fault_token
#endif
                                          ) -> mod::sched::task::Task* {
#ifdef WOS_SELFTEST
    return prepare_native_compartment_inputs_for_owner(executor, &spec, 0, error, fault_token);
#else
    return prepare_native_compartment_inputs_for_owner(executor, &spec, 0, error);
#endif
}

auto publish_native_compartment_for_owner(mod::sched::task::Task& executor, mod::sched::task::Task* child) -> int64_t {
    int result = 0;
    uint64_t pid = 0;
    {
        NativeConstructionExecutorGuard recovery(executor);
        if (!recovery.acquired()) return -EBUSY;
        auto* request = executor.owned_exec_arguments.load(std::memory_order_acquire);
        auto* root = request != nullptr && request->native_compartment_request ? request->compartment.get() : nullptr;
        if (root == nullptr || root->creator == nullptr || &root->executor != &executor || child == nullptr || root->child != child ||
            root->published || executor.owned_unpublished_process.load(std::memory_order_acquire) != child)
            return -EINVAL;
        bool membership = false;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            if (executor.exit_in_progress.load(std::memory_order_acquire) || executor.has_exited ||
                executor.state.load(std::memory_order_acquire) != mod::sched::task::TaskState::ACTIVE)
                result = -ECANCELED;
            else if (mod::power::shutdown_in_progress())
                result = -ESHUTDOWN;
            else
                result = mod::sched::resource::revalidate_native_creator_policy(*root->creator, root->policy);
            if (result == 0) result = revalidate_native_creation_priority(*root);
            if (result == 0 &&
                (!root->priority_initialized || child->sched_nice != root->inherited_nice || child->sched_weight != root->inherited_weight))
                result = -EAGAIN;
            if (result == 0 && (child->scheduler_published.load(std::memory_order_acquire) || child->pid_namespace() == nullptr ||
                                child->mount_namespace() == nullptr || child->ipc_namespace() == nullptr ||
                                *child->pid_namespace() != root->context.pids || *child->mount_namespace() != root->context.mounts ||
                                *child->ipc_namespace() != root->context.ipc || child->namespace_tid() != 1))
                result = -EINVAL;
            if (result == 0 && (root->context.pids.closed() || root->context.mounts.closed() || root->context.ipc.closed()))
                result = -EIDRM;
            if (result == 0 && (child->pid == 0 || child->pid > INT64_MAX)) result = -EOVERFLOW;
            if (result == 0) {
                pid = child->pid;
                membership = child_events::begin_publication(*root->creator, *child);
                if (!membership) result = -ECANCELED;
            }
#ifdef WOS_SELFTEST
            if (result == 0 &&
                exec_selftest_take_native_construction_fault(NativeConstructionFault::PUBLICATION_BEFORE_POST, root->fault_token))
                result = -EAGAIN;
#endif
            if (result == 0) {
                uint64_t cpu = mod::cpu::current_cpu();
#ifdef WOS_SELFTEST
                if (exec_selftest_take_native_construction_fault(NativeConstructionFault::PUBLICATION_INVALID_CPU, root->fault_token))
                    cpu = UINT64_MAX;
#endif
                if (!mod::sched::post_task_for_cpu(cpu, child)) result = -EAGAIN;
            }
            if (result == 0) {
                // Post can run/exit immediately. The slot still pins the child;
                // parent exit may already have transferred its event audience.
                static_cast<void>(child_events::commit_publication(*child));
                root->published = true;
                root->child = nullptr;
                if (!mod::sched::task::release_unpublished_process(&executor, child)) [[unlikely]] {
                    mod::dbg::panic_handler("native publication: lost published child slot");
                    hcf();
                }
            }
        }
        if (result != 0 && membership) child_events::abort_publication(*child);
    }
    if (result != 0) return result;
    // Preparation File releases may block; transferred scopes stay live and
    // cleanup keeps its own persistent/fatal recovery owner until destruction.
    cleanup_exec_preparation(executor);
    return static_cast<int64_t>(pid);
}

auto create_native_compartment_for_subject(mod::sched::task::Task* executor, uint64_t spec_address) -> int64_t {
    int const SCOPE = mod::security::compartment::host_namespace_admission(executor);
    if (SCOPE != 0) return SCOPE;
    if (mod::power::shutdown_in_progress()) return -ESHUTDOWN;
#ifdef WOS_SELFTEST
    NativeConstructionToken fault_token{};
#endif
#ifdef WOS_SELFTEST
    // The controller owns attempt/death/disarm state; fatal usercopy can abandon
    // this stack. Do not install a stack-only cleanup guard or infer an actor.
    fault_token = exec_selftest_begin_native_construction_attempt(*executor);
#endif
    int error = 0;
#ifdef WOS_SELFTEST
    auto* child = prepare_native_compartment_inputs_for_owner(*executor, nullptr, spec_address, error, fault_token);
#else
    auto* child = prepare_native_compartment_inputs_for_owner(*executor, nullptr, spec_address, error);
#endif
#ifdef WOS_SELFTEST
    if (child == nullptr) {
        int64_t const RESULT = error != 0 ? error : -ENOEXEC;
        ker::test::lifecycle60::create_returned(fault_token, RESULT);
        return RESULT;
    }
#else
    if (child == nullptr) return error != 0 ? error : -ENOEXEC;
#endif
    int64_t const RESULT = publish_native_compartment_for_owner(*executor, child);
    if (RESULT < 0 && !abort_native_compartment_preparation(*executor, child)) [[unlikely]] {
        mod::dbg::panic_handler("native public creation: lost preparation ownership");
        hcf();
    }
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::create_returned(fault_token, RESULT);
#endif
    return RESULT;
}

auto wos_proc_exec(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    auto* parent = ker::mod::sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    auto const ADMITTED_CREDENTIALS = parent->credentials();
    auto const MEMORY_POLICY = parent->memory_policy_snapshot();
    ExecArgumentsSnapshot snapshot;
    int const SNAPSHOT_RESULT = snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, snapshot);
    if (SNAPSHOT_RESULT < 0) {
        return static_cast<uint64_t>(SNAPSHOT_RESULT);
    }
    return wos_proc_exec_impl(snapshot.path.data(), snapshot.argv, snapshot.envp, nullptr, 0, ADMITTED_CREDENTIALS, MEMORY_POLICY);
}

auto wos_proc_spawn(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, uint64_t options_addr) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    auto* parent = ker::mod::sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    auto const ADMITTED_CREDENTIALS = parent->credentials();
    auto const MEMORY_POLICY = parent->memory_policy_snapshot();
    ExecArgumentsSnapshot arguments;
    int const ARGUMENT_RESULT = snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, arguments);
    if (ARGUMENT_RESULT < 0) {
        return static_cast<uint64_t>(ARGUMENT_RESULT);
    }
    if (options_addr == 0) {
        return wos_proc_exec_impl(arguments.path.data(), arguments.argv, arguments.envp, nullptr, 0, ADMITTED_CREDENTIALS, MEMORY_POLICY);
    }

    auto* snapshot = new (std::nothrow) SpawnOptionsSnapshot;
    if (snapshot == nullptr) {
        return static_cast<uint64_t>(-ENOMEM);
    }
    int const OPTIONS_RESULT = snapshot_spawn_options(*parent, options_addr, *snapshot);
    if (OPTIONS_RESULT < 0) {
        delete snapshot;
        return static_cast<uint64_t>(OPTIONS_RESULT);
    }
    uint64_t const RESULT = wos_proc_exec_impl(arguments.path.data(), arguments.argv, arguments.envp, &snapshot->options, 0,
                                               ADMITTED_CREDENTIALS, MEMORY_POLICY);
    delete snapshot;
    return RESULT;
}

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp,
                        const ker::abi::process::SpawnOptions* spawn_options, int shebang_depth,
                        const ker::mod::security::Snapshot& admitted_credentials, ker::mod::mm::numa::Policy memory_policy) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    std::string_view const STR(path, std::strlen(path));
    size_t argv_count = 0;
    if (argv != nullptr) {
        while (argv[argv_count] != nullptr) {
            argv_count++;
        }
    }

    size_t envp_count = 0;
    if (envp != nullptr) {
        while (envp[envp_count] != nullptr) {
            envp_count++;
        }
    }

    using namespace ker::mod;

    auto* parent_task = sched::get_current_task();
    if (parent_task == nullptr) {
        dbg::log("wos_proc_exec: No current task");
        return 0;
    }
    auto const PARENT_CREDENTIALS = admitted_credentials;
    uint64_t const PARENT_PID = sched::task::process_pid(*parent_task);

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Loading '%.*s'", static_cast<int>(STR.size()), STR.data());
#endif

    vfs::File* exec_file = nullptr;
    int const OPEN_RESULT = vfs::vfs_open_file_as(*parent_task, PARENT_CREDENTIALS, path, 0, &exec_file);
    if (OPEN_RESULT != 0) {
        dbg::log("wos_proc_exec: Failed to open file '%.*s'", static_cast<int>(STR.size()), STR.data());
        return static_cast<uint64_t>(OPEN_RESULT);
    }

    bool const EXEC_NOSUID = vfs::file_mount_nosuid(*exec_file);
    vfs::Stat exec_stat{};
    int const PIN_RET = vfs::vfs_stabilize_exec_file(&exec_file);
    int const STAT_RET = PIN_RET == 0 ? vfs::vfs_fstat_file(exec_file, &exec_stat) : PIN_RET;
    if (STAT_RET < 0) {
        dbg::log("wos_proc_exec: Failed to stat file '%.*s'", static_cast<int>(STR.size()), STR.data());
        vfs::vfs_put_file(exec_file);
        return static_cast<uint64_t>(STAT_RET);
    }

    int const ACCESS_RET = check_exec_permission_from_stat(PARENT_CREDENTIALS, exec_stat);
    if (ACCESS_RET < 0) {
        dbg::log("wos_proc_exec: Execute permission denied for '%.*s'", static_cast<int>(STR.size()), STR.data());
        vfs::vfs_put_file(exec_file);
        return static_cast<uint64_t>(ACCESS_RET);
    }

    ssize_t const FILE_SIZE = exec_stat.st_size;
    if (FILE_SIZE <= 0) {
        dbg::log("wos_proc_exec: Invalid file size: %d", FILE_SIZE);
        vfs::vfs_put_file(exec_file);
        return 0;
    }

    auto release_exec_file_once = [&]() {
        if (exec_file != nullptr) {
            vfs::vfs_put_file(exec_file);
            exec_file = nullptr;
        }
    };

    PreparedFileBackedElf prepared_elf;
    PrepareFileBackedElfResult const PREPARED_RESULT = prepare_file_backed_elf(exec_file, static_cast<size_t>(FILE_SIZE), prepared_elf);
    bool const FILE_BACKED_ELF = PREPARED_RESULT == PrepareFileBackedElfResult::READY;
    if (PREPARED_RESULT == PrepareFileBackedElfResult::INVALID || PREPARED_RESULT == PrepareFileBackedElfResult::IO_ERROR ||
        PREPARED_RESULT == PrepareFileBackedElfResult::OUT_OF_MEMORY) {
        dbg::log("wos_proc_exec: Failed to prepare bounded ELF source");
        release_exec_file_once();
        return 0;
    }

    auto* elf_buffer = FILE_BACKED_ELF ? prepared_elf.take_task_metadata() : new uint8_t[FILE_SIZE];
    size_t const ELF_BUFFER_SIZE = FILE_BACKED_ELF ? prepared_elf.task_metadata_size : static_cast<size_t>(FILE_SIZE);
    bool const ELF_BUFFER_COMPLETE = !FILE_BACKED_ELF;
    if (elf_buffer == nullptr) {
        dbg::log("wos_proc_exec: Failed to allocate buffer");
        release_exec_file_once();
        return 0;
    }

    uint32_t const ELF_READ_CORR = perf::next_wki_trace_correlation();
    uint64_t const ELF_READ_STARTED_US = time::get_us();
    record_local_proc_event(parent_task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::BEGIN, ELF_READ_CORR, 0,
                            clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    ExecImageReadResult const READ_RESULT = FILE_BACKED_ELF
                                                ? ExecImageReadResult{.bytes_read = static_cast<ssize_t>(prepared_elf.bytes_read),
                                                                      .status = 0,
                                                                      .shebang_probe_size = sizeof(Elf64_Ehdr)}
                                                : read_exec_image_for_loader(exec_file, elf_buffer, static_cast<size_t>(FILE_SIZE), path);
    uint32_t const ELF_READ_US = clamp_perf_aux(time::get_us() - ELF_READ_STARTED_US);
    int32_t const ELF_READ_STATUS = READ_RESULT.status;
    record_local_proc_event(parent_task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::END, ELF_READ_CORR, ELF_READ_STATUS,
                            ELF_READ_US, WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::ELF_READ), 0, 0,
                             ELF_READ_STATUS, ELF_READ_US, true, 0,
                             READ_RESULT.bytes_read > 0 ? static_cast<uint64_t>(READ_RESULT.bytes_read) : 0);

    if (READ_RESULT.status < 0) {
        dbg::log("wos_proc_exec: Failed to read file completely");
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    // Add memory barrier after reading to ensure visibility
    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    size_t const SHEBANG_BYTES = READ_RESULT.shebang_probe_size != 0 ? READ_RESULT.shebang_probe_size : static_cast<size_t>(FILE_SIZE);
    if (parse_shebang_line(elf_buffer, SHEBANG_BYTES, &shebang)) {
        release_exec_file_once();
        delete[] elf_buffer;
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return 0;
        }
        return exec_shebang_script(path, argv, envp, argv_count, shebang, shebang_depth,
                                   [spawn_options, &PARENT_CREDENTIALS, memory_policy](const char* interp, const char* const* argv2,
                                                                                       const char* const* envp2, int depth) -> uint64_t {
                                       return wos_proc_exec_impl(interp, argv2, envp2, spawn_options, depth, PARENT_CREDENTIALS,
                                                                 memory_policy);
                                   });
    }

    auto* elf_header = reinterpret_cast<Elf64_Ehdr*>(elf_buffer);

    if (elf_header->e_ident[EI_MAG0] != ELFMAG0 || elf_header->e_ident[EI_MAG1] != ELFMAG1 || elf_header->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_header->e_ident[EI_MAG3] != ELFMAG3) {
        dbg::log("wos_proc_exec: Not a valid ELF file");
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    if (elf_header->e_ident[EI_CLASS] != ELFCLASS64) {
        dbg::log("wos_proc_exec: Not a 64-bit ELF");
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    const char* process_name = STR.data();
    if (size_t const SLASH = STR.rfind('/'); SLASH != std::string_view::npos) {
        process_name = STR.data() + SLASH + 1;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Creating task for '%s', parent PID: %x", process_name, PARENT_PID);
#endif

    if (parent_task->owned_unpublished_process.load(std::memory_order_acquire) != nullptr) {
        dbg::log("wos_proc_exec: parent PID %x already owns an unpublished process", PARENT_PID);
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    sched::resource::ProcessReservation process_reservation(parent_task, PARENT_CREDENTIALS.user().real);
    if (process_reservation.result() < 0) {
        release_exec_file_once();
        delete[] elf_buffer;
        return static_cast<uint64_t>(process_reservation.result());
    }
    uint64_t const KERNEL_RSP = allocate_kernel_stack();
    if (KERNEL_RSP == 0) {
        dbg::log("wos_proc_exec: Failed to allocate kernel stack");
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    // DIAGNOSTIC: Detect stack corruption during Task constructor
    // Save known canary values on stack, check after constructor
    volatile uint64_t canary1 = 0xDEAD'BEEF'CAFE'BABEULL;  // NOLINT
    volatile uint64_t canary2 = 0x1234'5678'9ABC'DEF0ULL;  // NOLINT

    auto* new_task = new sched::task::Task(process_name, 0, 0, KERNEL_RSP, sched::task::TaskType::PROCESS);

    // Check canaries for stack corruption
    if (canary1 != 0xDEAD'BEEF'CAFE'BABEULL || canary2 != 0x1234'5678'9ABC'DEF0ULL) {  // NOLINT
        dbg::log("STACK CORRUPTION DETECTED in exec!");
        dbg::log("  canary1=%lx (expect DEADBEEFCAFEBABE)", canary1);
        dbg::log("  canary2=%lx (expect 123456789ABCDEF0)", canary2);
        dbg::log("  newTask=%p, &canary1=%p, &canary2=%p", new_task, &canary1, &canary2);
        dbg::log("  stack RSP approx %p, kernelRsp=%lx", &new_task, KERNEL_RSP);
    }

    // Also check if newTask is suspiciously not in HHDM range
    auto task_addr = reinterpret_cast<uintptr_t>(new_task);
    if (task_addr != 0 && (task_addr < 0xffff800000000000ULL || task_addr >= 0xffff900000000000ULL)) {
        dbg::log("EXEC BUG: operator new returned non-HHDM ptr: %p", new_task);
        dbg::log("  expected range: 0xffff800000000000 - 0xffff900000000000");
        dbg::log("  &newTask on stack = %p, kernelRsp = %lx", &new_task, KERNEL_RSP);
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    if (new_task == nullptr) {
        ker::mod::mm::phys::page_free(reinterpret_cast<void*>(KERNEL_RSP - ker::mod::mm::KERNEL_STACK_SIZE));
        release_exec_file_once();
        delete[] elf_buffer;
        return 0;
    }

    new_task->publish_memory_policy_snapshot(memory_policy);

    // The constructor deliberately performs no PT_INTERP VFS I/O. Transfer
    // every caller-owned resource and publish fatal-exit recovery before the
    // post-construction interpreter stage can enter a voluntary wait.
    new_task->elf_buffer = elf_buffer;
    new_task->elf_buffer_size = ELF_BUFFER_SIZE;
    new_task->is_elf_buffer_shared = false;
    new_task->elf_buffer_complete = ELF_BUFFER_COMPLETE;
    if (FILE_BACKED_ELF) {
        new_task->exec_image_file = exec_file;
        new_task->exec_image_size = static_cast<uint64_t>(FILE_SIZE);
        exec_file = nullptr;
    } else {
        release_exec_file_once();
    }
    if (!sched::task::claim_unpublished_process(parent_task, new_task)) {
        dbg::panic_handler("wos_proc_exec: current task already owns an unpublished process");
        hcf();
    }
    uint64_t const CHILD_PID = new_task->pid;
    auto cleanup_unpublished_task = [&]() {
        child_events::abort_publication(*new_task);
        if (!sched::task::destroy_owned_unpublished_process(parent_task, new_task)) {
            dbg::panic_handler("wos_proc_exec: lost unpublished child ownership during teardown");
            hcf();
        }
    };

    int priority_error = 0;
    {
        // Ordinary creation inherits the actor at this observation point.
        // Release before loader IO and WKI placement; later changes to the
        // parent do not retroactively change this exclusively owned child.
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        priority_error = creation_priority_source_status(*parent_task, parent_task->credential_owner());
        if (priority_error == 0)
            priority_error = sched::initialize_unpublished_task_priority(*parent_task, *new_task, parent_task->sched_nice);
    }
    if (priority_error != 0) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(priority_error);
    }

    if (!sched::resource::inherit(*new_task, *parent_task)) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(-ESRCH);
    }
    if (sched::resource::initialize_credentials(*new_task, PARENT_CREDENTIALS) != 0 || !process_reservation.attach(*new_task)) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(-EAGAIN);
    }
    uint64_t const CHILD_VISIBLE_PID = new_task->namespace_tid();

    // PT_INTERP construction can resolve paths and block. Install the exact
    // inherited roots while fatal-exit recovery still owns this child.
    {
        ker::mod::sched::task::Task::FilesystemContext filesystem;
        if (!parent_task->capture_filesystem_context(filesystem)) {
            cleanup_unpublished_task();
            return 0;
        }
        new_task->cwd = filesystem.cwd;
        new_task->cwd_len = filesystem.cwd_len;
        new_task->root = filesystem.root;
        new_task->root_len = filesystem.root_len;
        new_task->root_anchor = std::move(filesystem.root_anchor);
        new_task->cwd_anchor = std::move(filesystem.cwd_anchor);
    }

    if (FILE_BACKED_ELF) {
        loader::elf::ElfLazyLoadRangeVec loader_lazy_ranges;
        loader::elf::ElfLoadOptions const LOAD_OPTIONS{
            .register_special_symbols = true,
            .base_address = 0,
            .lazy_file_ranges = exec_lazy_file_segments_enabled() ? &loader_lazy_ranges : nullptr,
        };
        if (!new_task->initialize_process_image(prepared_elf.view, LOAD_OPTIONS) ||
            !append_exec_lazy_file_ranges(new_task->lazy_vmem_ranges, loader_lazy_ranges, new_task->exec_image_file, exec_stat)) {
            dbg::log("wos_proc_exec: Failed to initialize file-backed process image");
            cleanup_unpublished_task();
            return 0;
        }
    }

    if (!FILE_BACKED_ELF &&
        !new_task->initialize_process_image(reinterpret_cast<const uint8_t*>(new_task->elf_buffer), static_cast<size_t>(FILE_SIZE))) {
        cleanup_unpublished_task();
        return 0;
    }

    if (new_task->thread == nullptr || new_task->pagemap == nullptr || new_task->entry == 0) {
        dbg::log("wos_proc_exec: Failed to create task (OOM or ELF load failure)");
        cleanup_unpublished_task();
        return 0;
    }
    int interpreter_error = 0;
    if (!sched::task::complete_unpublished_process_construction(new_task, &interpreter_error)) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(interpreter_error);
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task constructor completed successfully");
    dbg::log("wos_proc_exec: Entry point = 0x%x, RIP = 0x%x", new_task->entry, new_task->context.frame.rip);
#endif

    // Inherit process execution context from the parent before applying
    // executable-specific overrides such as setuid/setgid.
    (void)new_task->exchange_creation_mask(parent_task->creation_mask());
    auto child_job = parent_task->job_identity_snapshot();
    child_job.internal.exec_completed = false;
    new_task->initialize_job_identity(child_job.internal, child_job.visible);
    new_task->signal_mask_store(parent_task->signal_mask_bits(), std::memory_order_relaxed);
    ker::mod::sys::signal::sync_task_signal_mask_cache(new_task);
    new_task->wki_prefer_inline = parent_task->wki_prefer_inline;
    new_task->wki_target_hostname = parent_task->wki_target_hostname;
    new_task->wki_target_flags = parent_task->wki_target_flags;
    new_task->wki_submitter_hostname = parent_task->wki_execution_identity().submitter_hostname;
    new_task->wki_remote_pid = (new_task->wki_submitter_hostname.front() != '\0' &&
                                std::strcmp(new_task->wki_submitter_hostname.data(), local_wki_hostname()) != 0)
                                   ? new_task->pid
                                   : 0;
    if (!new_task->wki_vfs_rules.clone_from(parent_task->wki_vfs_rules)) {
        cleanup_unpublished_task();
        return 0;
    }

    // Spawn file actions must be able to consume CLOEXEC source descriptors;
    // ordinary exec creation can omit them immediately.
    if (!clone_exec_fd_table_checked(parent_task, new_task, spawn_options != nullptr)) {
        cleanup_unpublished_task();
        return 0;
    }

    int options_error = 0;
    if (!apply_spawn_options(new_task, spawn_options, &options_error)) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(options_error);
    }
    if (spawn_options != nullptr) {
        close_spawn_cloexec_fds(new_task);
    }

    // Legacy WOS exec creation supplies console fallbacks. Option-aware
    // posix_spawn must preserve the exact post-action descriptor set, including
    // intentionally closed stdin/stdout/stderr descriptors.
    if (spawn_options == nullptr && !ensure_exec_stdio_fallbacks(new_task)) {
        cleanup_unpublished_task();
        return 0;
    }

    // Store executable path for /proc/self/exe
    {
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        std::memcpy(new_task->exe_path.data(), path, path_len);
        fixed_slot(new_task->exe_path, path_len) = '\0';
    }

    // Use the retained executable's metadata; pathname re-stat could select a
    // different inode after rename. Ordinary exec also refreshes saved IDs.
    security::Snapshot EXEC_CREDENTIALS;
    security::ExecIdentity const EXEC_IDENTITY{.uid = exec_stat.st_uid,
                                               .gid = exec_stat.st_gid,
                                               .set_uid = (exec_stat.st_mode & 04000) != 0,
                                               .set_gid = (exec_stat.st_mode & 02000) != 0,
                                               .nosuid = EXEC_NOSUID,
                                               .traced = parent_task->ptrace_traced,
                                               .mode = exec_stat.st_mode};
    if (PARENT_CREDENTIALS.prepare_exec(EXEC_IDENTITY, EXEC_CREDENTIALS) != 0 ||
        sched::resource::initialize_credentials(*new_task, EXEC_CREDENTIALS) != 0) {
        cleanup_unpublished_task();
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task created with PID: %x, parent: %x", new_task->pid, PARENT_PID);
#endif

    uint64_t user_stack_virt = new_task->thread->stack;

    uint64_t current_virt_offset = 0;

    auto copy_to_user_stack = [&](uint64_t vaddr, const void* data, size_t size) -> bool {
        if (size == 0) {
            return true;
        }
        uint64_t const END = vaddr + static_cast<uint64_t>(size);
        if (END < vaddr) {
            return false;
        }
        if (!mod::sched::threading::ensure_stack_backing(new_task->thread, new_task->pagemap, vaddr, END, new_task)) {
            exec_log::error("copyToStack: failed to back stack range [0x%llx,0x%llx)", static_cast<unsigned long long>(vaddr),
                            static_cast<unsigned long long>(END));
            return false;
        }

        auto const* src = static_cast<const uint8_t*>(data);
        size_t copied = 0;
        while (copied < size) {
            uint64_t const CUR = vaddr + copied;
            uint64_t const PAGE_VIRT = CUR & ~(mod::mm::paging::PAGE_SIZE - 1);
            uint64_t const PAGE_OFFSET = CUR & (mod::mm::paging::PAGE_SIZE - 1);
            size_t const CHUNK =
                (size - copied < mod::mm::paging::PAGE_SIZE - PAGE_OFFSET) ? size - copied : mod::mm::paging::PAGE_SIZE - PAGE_OFFSET;

            uint64_t const PAGE_PHYS = mod::mm::virt::translate(new_task->pagemap, PAGE_VIRT);
            if (PAGE_PHYS == mod::mm::virt::PADDR_INVALID) {
                exec_log::error("copyToStack: translate failed for stack vaddr 0x%llx", static_cast<unsigned long long>(PAGE_VIRT));
                return false;
            }

            auto* dest_ptr = reinterpret_cast<uint8_t*>(mod::mm::addr::get_virt_pointer(PAGE_PHYS)) + PAGE_OFFSET;
            std::memcpy(dest_ptr, src + copied, CHUNK);
            copied += CHUNK;
        }
        return true;
    };

    auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
        if (current_virt_offset + size > ker::mod::mm::USER_STACK_SIZE) {
            return 0;  // Stack overflow
        }
        current_virt_offset += size;
        uint64_t const VIRT_ADDR = user_stack_virt - current_virt_offset;

        if (!copy_to_user_stack(VIRT_ADDR, data, size)) {
            return 0;
        }

        return VIRT_ADDR;
    };

    auto push_string = [&](std::string_view str) -> uint64_t {
        size_t const LEN = str.size() + 1;  // Include null terminator
        if (current_virt_offset + LEN > ker::mod::mm::USER_STACK_SIZE) {
            return 0;
        }
        current_virt_offset += LEN;
        uint64_t const VIRT_ADDR = user_stack_virt - current_virt_offset;

        if (!copy_to_user_stack(VIRT_ADDR, str.data(), str.size())) {
            return 0;
        }
        char const ZERO = '\0';
        if (!copy_to_user_stack(VIRT_ADDR + str.size(), &ZERO, sizeof(ZERO))) {
            return 0;
        }

        return VIRT_ADDR;
    };

    // Push argv strings first (highest addresses on stack)
    auto* argv_addrs = new uint64_t[argv_count + 1];
    for (size_t i = 0; i < argv_count; i++) {
        argv_addrs[i] = push_string(argv[i]);
        if (argv_addrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push argv string");
            delete[] argv_addrs;
            cleanup_unpublished_task();
            return 0;
        }
    }
    argv_addrs[argv_count] = 0;

    // Push envp strings
    auto* envp_addrs = new uint64_t[envp_count + 1];
    for (size_t i = 0; i < envp_count; i++) {
        envp_addrs[i] = push_string(envp[i]);
        if (envp_addrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push envp string");
            delete[] envp_addrs;
            delete[] argv_addrs;
            cleanup_unpublished_task();
            return 0;
        }
    }
    envp_addrs[envp_count] = 0;

    // AT_EXECFN must refer to storage in the new image. In particular, ld.so
    // uses it to derive $ORIGIN before libc has initialized /proc helpers.
    uint64_t const EXECFN_ADDR = push_string(path);
    if (EXECFN_ADDR == 0) {
        dbg::log("wos_proc_exec: Failed to push AT_EXECFN string");
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_unpublished_task();
        return 0;
    }

    std::array<uint8_t, AT_RANDOM_BYTES> at_random{};
    if (!mod::random::entropy::get_bytes(at_random.data(), at_random.size())) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_unpublished_task();
        return 0;
    }
    uint64_t const AT_RANDOM_ADDR = push_to_stack(at_random.data(), at_random.size());
    std::memset(at_random.data(), 0, at_random.size());
    if (AT_RANDOM_ADDR == 0) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_unpublished_task();
        return 0;
    }
    new_task->at_random_addr = AT_RANDOM_ADDR;

    // Align to 16 bytes after string data, accounting for structured data parity.
    // Structured data: auxv (variable) + envp array + argv array + argc.
    // auxv: 7 core pairs, 5 identity pairs, optional AT_BASE, and AT_NULL.
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_QWORDS_BASE = 16 + security::exec_policy::IDENTITY_AUXV_WORDS;
        const size_t AUXV_QWORDS = AUXV_QWORDS_BASE + (new_task->interp_base != 0 ? 2 : 0);
        size_t const STRUCTURED_QWORDS = AUXV_QWORDS + (envp_count + 1) + (argv_count + 1) + 1;
        if (STRUCTURED_QWORDS % 2 != 0) {
            // Add 8 bytes padding so final rsp is 16-byte aligned
            uint64_t pad = 0;
            push_to_stack(&pad, sizeof(uint64_t));
        }
    }

    // Push auxv (System V ABI: auxv sits between envp NULL terminator and string data)
    {
        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_PHENT = 4;
        constexpr uint64_t AT_PHNUM = 5;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_BASE = 7;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_RANDOM = 25;
        constexpr uint64_t AT_EXECFN = 31;

        // Build auxv dynamically: always include core entries, conditionally add AT_BASE
        bool built_correct_auxv = true;
        ker::util::SmallVec<uint64_t, 30> auxv;
        built_correct_auxv &= auxv.push_back(AT_PAGESZ);
        built_correct_auxv &= auxv.push_back(mod::mm::paging::PAGE_SIZE);
        built_correct_auxv &= auxv.push_back(AT_ENTRY);
        built_correct_auxv &= auxv.push_back(new_task->entry);
        built_correct_auxv &= auxv.push_back(AT_PHDR);
        built_correct_auxv &= auxv.push_back(new_task->program_header_addr);
        built_correct_auxv &= auxv.push_back(AT_PHENT);
        built_correct_auxv &= auxv.push_back(new_task->program_header_ent_size);
        built_correct_auxv &= auxv.push_back(AT_PHNUM);
        built_correct_auxv &= auxv.push_back(new_task->program_header_count);
        if (new_task->interp_base != 0) {
            built_correct_auxv &= auxv.push_back(AT_BASE);
            built_correct_auxv &= auxv.push_back(new_task->interp_base);
        }
        built_correct_auxv &= auxv.push_back(AT_RANDOM);
        built_correct_auxv &= auxv.push_back(AT_RANDOM_ADDR);
        built_correct_auxv &= auxv.push_back(AT_EXECFN);
        built_correct_auxv &= auxv.push_back(EXECFN_ADDR);
        for (auto value : security::exec_policy::identity_auxv(EXEC_CREDENTIALS)) built_correct_auxv &= auxv.push_back(value);
        built_correct_auxv &= auxv.push_back(AT_NULL);
        built_correct_auxv &= auxv.push_back(0);

        if (!built_correct_auxv) {
            dbg::log("wos_proc_exec: Failed to build auxv");
            delete[] envp_addrs;
            delete[] argv_addrs;
            cleanup_unpublished_task();
            return 0;
        }

        for (int j = static_cast<int>(auxv.size()) - 1; j >= 0; j--) {
            uint64_t val = auxv.at(static_cast<size_t>(j));
            push_to_stack(&val, sizeof(uint64_t));
        }
    }

    // Push envp pointer array (with NULL terminator)
    uint64_t const ENVP_PTR = push_to_stack(envp_addrs, (envp_count + 1) * sizeof(uint64_t));
    delete[] envp_addrs;

    // Push argv pointer array (with NULL terminator)
    uint64_t const ARGV_PTR = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    delete[] argv_addrs;

    // Push argc last (rsp will point here)
    uint64_t argc = argv_count;
    push_to_stack(&argc, sizeof(uint64_t));

    new_task->context.frame.rsp = user_stack_virt - current_virt_offset;

    new_task->context.regs.rdi = argc;
    new_task->context.regs.rsi = ARGV_PTR;
    new_task->context.regs.rdx = ENVP_PTR;

    int remote_error = 0;
    ker::net::wki::TaskExecCredentialTransition const SPAWN_TRANSITION{
        .flags = static_cast<uint16_t>((EXEC_IDENTITY.nosuid ? ker::net::wki::WKI_EXEC_CREDENTIAL_NOSUID : 0) |
                                       (EXEC_IDENTITY.traced ? ker::net::wki::WKI_EXEC_CREDENTIAL_TRACED : 0)),
        .uid = EXEC_IDENTITY.uid,
        .gid = EXEC_IDENTITY.gid,
        .mode = EXEC_IDENTITY.mode & 07777U,
    };
    ker::net::wki::WkiRemoteSpawnSpec const REMOTE_SPAWN = {
        .argv = argv,
        .envp = envp,
        .cwd = new_task->cwd.data(),
        .error_out = &remote_error,
        .exec_credentials = &SPAWN_TRANSITION,
        .image_credentials = &PARENT_CREDENTIALS,
    };
    new_task->mark_exec_completed();
    if (!child_events::begin_publication(*parent_task, *new_task)) {
        cleanup_unpublished_task();
        return 0;
    }

    auto remote_result = ker::net::wki::wki_try_remote_spawn(new_task, REMOTE_SPAWN);
    consume_successful_one_shot_wki_target(new_task, remote_result);
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
        if (!sched::task::release_unpublished_process(parent_task, new_task)) {
            dbg::log("wos_proc_exec: remote publication lost child ownership for PID %x", CHILD_PID);
            // The child is already published remotely. Report its PID so a
            // posix_spawn caller cannot fall back and launch it a second time.
            return CHILD_VISIBLE_PID;
        }
        return CHILD_VISIBLE_PID;
    }
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
        cleanup_unpublished_task();
        return static_cast<uint64_t>(remote_error);
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Setup stack - argc=%d, argv=0x%x, envp=0x%x, rsp=0x%x", argc, ARGV_PTR, ENVP_PTR, new_task->context.frame.rsp);
    dbg::log("wos_proc_exec: Entry point (RIP) = 0x%x", new_task->context.frame.rip);
    dbg::log("wos_proc_exec: Task entry field = 0x%x", new_task->entry);
#endif

    // Use load-balanced task posting to distribute across CPUs
    if (!sched::post_task_balanced(new_task)) {
        dbg::log("wos_proc_exec: Failed to post task to scheduler");
        cleanup_unpublished_task();
        return 0;
    }

    // A legacy scheduler placement hook can turn this nominally local post
    // into a WKI proxy; that path commits inside wki_try_remote_spawn().
    if (!new_task->wki_proxy_task && !child_events::commit_publication(*new_task)) [[unlikely]] {
        exec_log::error("local publication lifecycle commit completed concurrently for PID %x", CHILD_PID);
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Successfully posted task '%s' to CPU %d", process_name, new_task->load_cpu());
#endif

    if (!sched::task::release_unpublished_process(parent_task, new_task)) {
        dbg::log("wos_proc_exec: local publication lost child ownership for PID %x", CHILD_PID);
        // post_task_balanced() already made the child runnable. A failure
        // result here would make mlibc retry via fork+exec and run it twice.
        return CHILD_VISIBLE_PID;
    }
    return CHILD_VISIBLE_PID;

    // Note: elfBuffer is now owned by the task and will be cleaned up when the task exits
}
}  // namespace

auto wos_proc_execve(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    ExecRequestArguments* request = nullptr;
    {
        std::unique_ptr<ExecRequestArguments> pending(new (std::nothrow) ExecRequestArguments);
        if (!pending) return static_cast<uint64_t>(-ENOMEM);
        pending->admitted_credentials = task->credentials();
        pending->memory_policy = task->memory_policy_snapshot();
        request = pending.get();
        if (!adopt_exec_arguments(*task, pending)) return static_cast<uint64_t>(-EAGAIN);
    }
    request->trace.begin(task, 0, WOS_PERF_CALLSITE());
    auto& snapshot = request->snapshot;
    auto& SNAPSHOT_STAGE = request->trace.stage(task, 0, ker::mod::perf::WkiPerfLocalProcOp::ARG_COPY, 0, WOS_PERF_CALLSITE());
    int const SNAPSHOT_RESULT = snapshot_exec_arguments(*task, path_addr, argv_addr, envp_addr, snapshot);
    end_local_proc_stage(task, ker::mod::perf::WkiPerfLocalProcOp::ARG_COPY, SNAPSHOT_STAGE, SNAPSHOT_RESULT, 0, WOS_PERF_CALLSITE());
    uint64_t const RESULT = SNAPSHOT_RESULT < 0 ? static_cast<uint64_t>(SNAPSHOT_RESULT)
                                                : wos_proc_execve_impl(snapshot.path.data(), snapshot.argv, snapshot.envp, gpr, 0);
    // Preparation has returned through every recursive loader frame. Only a
    // native success owns a completed image; remote success has already consumed
    // its submitted ELF and has no local image to activate.
    auto* prepared = task->owned_exec_preparation.load(std::memory_order_acquire);
    if (RESULT == 0 && prepared != nullptr) {
        // Recursive loader frames are gone. The tracked image now owns every
        // endpoint, including when its existing pointer transfers to the owner.
        prepared->trace = std::exchange(request->trace, {});
        if (!task->is_thread) return finish_native_exec_image(task, prepared, request->completion);
        uint64_t const HANDOFF_RESULT = handoff_worker_exec(*task, *prepared, *request);
        cleanup_exec_preparation_with_status(*task, static_cast<int32_t>(HANDOFF_RESULT));
        return HANDOFF_RESULT;
    }
    uint32_t const REMOTE_ID = request->remote_proxy_id;
    bool const REMOTE_WORKER = request->remote_worker_committed;
    if (task->owned_exec_arguments.load(std::memory_order_acquire) != nullptr)
        cleanup_exec_preparation_with_status(*task, static_cast<int32_t>(RESULT));
    if (RESULT == 0 && REMOTE_WORKER) ker::syscall::multiproc::wos_thread_exit_current();
    if (RESULT == 0 && REMOTE_ID != 0) publish_remote_exec_proxy(*task, REMOTE_ID);
    return RESULT;
}

namespace {
auto wos_proc_execve_impl(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr, int shebang_depth)
    -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    // POSIX execve: replace current process image with a new one.
    // On success, the sysret return path is patched to land at the new
    // binary's entry point (gpr is a local copy and NOT used).
    (void)gpr;

    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: No current task");
#endif
        return static_cast<uint64_t>(-ESRCH);
    }
    auto* const PROCESS_OWNER = task->credential_owner();
    if (PROCESS_OWNER == nullptr) return static_cast<uint64_t>(-ESRCH);
    auto* request = task->owned_exec_arguments.load(std::memory_order_acquire);
    auto* scratch = request != nullptr ? request->native_scratch(shebang_depth) : nullptr;
    if (scratch == nullptr) return static_cast<uint64_t>(-EAGAIN);
    size_t const TRACE_DEPTH = static_cast<size_t>(shebang_depth);
    request->trace.begin(task, TRACE_DEPTH, WOS_PERF_CALLSITE());

    // --- Inspect the immutable kernel argument snapshot before image preparation ---
    auto& ARG_COPY_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::ARG_COPY, 0, WOS_PERF_CALLSITE());
    std::array<char, EXEC_PATH_MAX> k_path{};
    int const PATH_COPY_RET = copy_exec_path(path, k_path);
    if (PATH_COPY_RET < 0) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::ARG_COPY, ARG_COPY_STAGE, PATH_COPY_RET, 0, WOS_PERF_CALLSITE());
        return static_cast<uint64_t>(PATH_COPY_RET);
    }
    const char* exec_path = k_path.data();

    size_t argv_count = 0;
    if (argv != nullptr) {
        while (argv[argv_count] != nullptr) {
            argv_count++;
        }
    }
    size_t envp_count = 0;
    if (envp != nullptr) {
        while (envp[envp_count] != nullptr) {
            envp_count++;
        }
    }

    // The syscall wrapper already owns every argument byte in a bounded
    // kernel snapshot. Shebang recursion borrows that storage and enclosing
    // kernel frames; all consumers below finish before those owners return.
    // Keep null vectors nonnull/empty for the existing remote direct-submit
    // classification, without a second set of fallible heap copies.
    std::array<const char*, 1> empty_vector{};
    const char* const* const K_ARGV = argv != nullptr ? argv : empty_vector.data();
    const char* const* const K_ENVP = envp != nullptr ? envp : empty_vector.data();
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::ARG_COPY, ARG_COPY_STAGE, 0, argv_count + envp_count, WOS_PERF_CALLSITE());

    // --- Read the ELF file ---
    auto& OPEN_ACCESS_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::OPEN_ACCESS, 0, WOS_PERF_CALLSITE());
    vfs::File* exec_file = nullptr;
    int const OPEN_RESULT = vfs::vfs_open_file_as(*task, request->admitted_credentials, exec_path, 0, &exec_file);
    if (OPEN_RESULT != 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: Failed to open '%s'", exec_path);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, OPEN_RESULT, 0, WOS_PERF_CALLSITE());
        return static_cast<uint64_t>(OPEN_RESULT);
    }

    bool const EXEC_NOSUID = vfs::file_mount_nosuid(*exec_file);
    vfs::Stat exec_stat{};
    int const PIN_RET = vfs::vfs_stabilize_exec_file(&exec_file);
    int const STAT_RET = PIN_RET == 0 ? vfs::vfs_fstat_file(exec_file, &exec_stat) : PIN_RET;
    if (STAT_RET < 0) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, STAT_RET, 0, WOS_PERF_CALLSITE());
        vfs::vfs_put_file(exec_file);
        return static_cast<uint64_t>(STAT_RET);
    }

    int const ACCESS_RET = check_exec_permission_from_stat(request->admitted_credentials, exec_stat);
    if (ACCESS_RET < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: vfs_access X_OK failed for '%s' (ret=%d)", exec_path, access_ret);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -EACCES, 0, WOS_PERF_CALLSITE());
        vfs::vfs_put_file(exec_file);
        return static_cast<uint64_t>(-EACCES);
    }

    ssize_t const FILE_SIZE = exec_stat.st_size;
    if (FILE_SIZE > 0) request->trace.attempts.at(TRACE_DEPTH).bytes = static_cast<uint64_t>(FILE_SIZE);
    if (FILE_SIZE <= 0) {
        dbg::log("wos_proc_execve: empty file '%s'", exec_path);
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
        vfs::vfs_put_file(exec_file);
        return static_cast<uint64_t>(-ENOEXEC);
    }

    auto release_exec_file_once = [&]() {
        if (exec_file != nullptr) {
            vfs::vfs_put_file(exec_file);
            exec_file = nullptr;
        }
    };

    bool const EXEC_LAZY_FILE_SEGMENTS = exec_file != nullptr && exec_lazy_file_segments_enabled();
    auto& prepared_elf = scratch->prepared_elf;
    PrepareFileBackedElfResult const PREPARED_RESULT =
        prepare_execve_file_backed_elf(*task, exec_file, static_cast<size_t>(FILE_SIZE), prepared_elf, EXEC_LAZY_FILE_SEGMENTS);
    bool const FILE_BACKED_ELF = PREPARED_RESULT == PrepareFileBackedElfResult::READY;
    if (PREPARED_RESULT == PrepareFileBackedElfResult::INVALID || PREPARED_RESULT == PrepareFileBackedElfResult::IO_ERROR ||
        PREPARED_RESULT == PrepareFileBackedElfResult::OUT_OF_MEMORY) {
        int error = -ENOEXEC;
        if (PREPARED_RESULT == PrepareFileBackedElfResult::OUT_OF_MEMORY) {
            error = -ENOMEM;
        } else if (PREPARED_RESULT == PrepareFileBackedElfResult::IO_ERROR) {
            error = -EIO;
        }
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, error, 0, WOS_PERF_CALLSITE());
        release_exec_file_once();
        return static_cast<uint64_t>(error);
    }

    auto* elf_buffer = FILE_BACKED_ELF ? prepared_elf.take_task_metadata() : new uint8_t[FILE_SIZE];
    size_t const ELF_BUFFER_SIZE = FILE_BACKED_ELF ? prepared_elf.task_metadata_size : static_cast<size_t>(FILE_SIZE);
    bool const ELF_BUFFER_COMPLETE = !FILE_BACKED_ELF;
    if (elf_buffer == nullptr) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: alloc failed for '%s' (%ld bytes)", exec_path, file_size);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        release_exec_file_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, 0, static_cast<uint64_t>(FILE_SIZE),
                         WOS_PERF_CALLSITE());

    auto& ELF_READ_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::ELF_READ,
                                                clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    ExecImageReadResult const READ_RESULT =
        FILE_BACKED_ELF
            ? ExecImageReadResult{.bytes_read = static_cast<ssize_t>(prepared_elf.bytes_read),
                                  .status = 0,
                                  .shebang_probe_size = sizeof(Elf64_Ehdr)}
            : read_exec_image_for_loader(exec_file, elf_buffer, static_cast<size_t>(FILE_SIZE), exec_path, true, EXEC_LAZY_FILE_SEGMENTS);
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::ELF_READ, ELF_READ_STAGE, READ_RESULT.status,
                         READ_RESULT.bytes_read > 0 ? static_cast<uint64_t>(READ_RESULT.bytes_read) : 0, WOS_PERF_CALLSITE());

    if (READ_RESULT.status < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: failed read for '%s' (status %d, got %ld, expect %ld)", exec_path, READ_RESULT.status,
                 READ_RESULT.bytes_read, file_size);
#endif
        release_exec_file_once();
        delete[] elf_buffer;
        return static_cast<uint64_t>(READ_RESULT.status);
    }

    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    size_t const SHEBANG_BYTES = READ_RESULT.shebang_probe_size != 0 ? READ_RESULT.shebang_probe_size : static_cast<size_t>(FILE_SIZE);
    if (parse_shebang_line(elf_buffer, SHEBANG_BYTES, &shebang)) {
        release_exec_file_once();
        delete[] elf_buffer;
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return static_cast<uint64_t>(-ELOOP);
        }
        return exec_shebang_script(
            exec_path, argv, envp, argv_count, shebang, shebang_depth,
            [&gpr](const char* interp, const char* const* argv2, const char* const* envp2, int depth) -> uint64_t {
                return wos_proc_execve_impl(interp, argv2, envp2, gpr, depth);
            },
            task->owned_exec_arguments.load(std::memory_order_acquire));
    }

    auto* elf_header = reinterpret_cast<Elf64_Ehdr*>(elf_buffer);
    if (elf_header->e_ident[EI_MAG0] != ELFMAG0 || elf_header->e_ident[EI_MAG1] != ELFMAG1 || elf_header->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_header->e_ident[EI_MAG3] != ELFMAG3 || elf_header->e_ident[EI_CLASS] != ELFCLASS64) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF magic check failed for '%s' (bytes: %02x %02x %02x %02x class=%02x)", exec_path,
                 elf_header->e_ident[0], elf_header->e_ident[1], elf_header->e_ident[2], elf_header->e_ident[3], elf_header->e_ident[4]);
#endif
        delete[] elf_buffer;
        release_exec_file_once();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    {
        auto& REMOTE_SPAWN_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::REMOTE_SPAWN, 0, WOS_PERF_CALLSITE());
        REMOTE_SPAWN_STAGE.bytes = static_cast<uint64_t>(FILE_SIZE);
        int remote_error = 0;
        auto const REMOTE = try_process_remote_exec(*task, elf_buffer, ELF_BUFFER_SIZE, ELF_BUFFER_COMPLETE, exec_path, K_ARGV, K_ENVP,
                                                    exec_stat, EXEC_NOSUID, remote_error);
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::REMOTE_SPAWN, REMOTE_SPAWN_STAGE,
                             REMOTE == ker::net::wki::WkiRemoteSpawnResult::FAILED ? remote_error : static_cast<int32_t>(REMOTE),
                             static_cast<uint64_t>(FILE_SIZE), WOS_PERF_CALLSITE());
        if (REMOTE != ker::net::wki::WkiRemoteSpawnResult::LOCAL) {
            consume_successful_one_shot_wki_target(task, REMOTE);
            release_exec_file_once();
            delete[] elf_buffer;
            return REMOTE == ker::net::wki::WkiRemoteSpawnResult::REMOTE ? 0 : static_cast<uint64_t>(remote_error);
        }
    }

    // --- Replace the pagemap with a fresh one ---
    // Note: We are executing in kernel context (syscall handler) so our
    // kernel mappings are active. We'll create a new user pagemap.
    auto& NEW_IMAGE_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::NEW_IMAGE, 0, WOS_PERF_CALLSITE());
    uint64_t const DEBUG_STAGING_PID = task->pid | EXEC_DEBUG_STAGING_BIT;
    if (DEBUG_STAGING_PID == task->pid) {
        release_exec_file_once();
        delete[] elf_buffer;
        return static_cast<uint64_t>(-EOVERFLOW);
    }
    loader::debug::unregister_process(DEBUG_STAGING_PID);
    auto* new_pagemap = mm::virt::create_pagemap();
    if (new_pagemap == nullptr) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        release_exec_file_once();
        delete[] elf_buffer;
        return static_cast<uint64_t>(-ENOMEM);
    }
    mm::virt::copy_kernel_mappings_to(new_pagemap);

    // --- Create new thread (user stack + TLS) ---
    ker::loader::elf::TlsModule tls_info{};
    bool const ELF_CONTRACT_VALID = FILE_BACKED_ELF ? loader::elf::inspect_tls(prepared_elf.view, tls_info)
                                                    : loader::elf::inspect_tls(elf_buffer, static_cast<size_t>(FILE_SIZE), tls_info);
    auto* new_thread = ELF_CONTRACT_VALID
                           ? mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, tls_info.tls_size, new_pagemap,
                                                                  PROCESS_OWNER->namespace_tid(), tls_info, task)
                           : nullptr;
    uint64_t new_mmap_cursor = 0;
    char* new_name = nullptr;
    LazyVmemRangeVec new_lazy_ranges;
    auto cleanup_new_image = [&]() {
        loader::debug::unregister_process(DEBUG_STAGING_PID);
        release_exec_file_once();
        release_lazy_file_refs(new_lazy_ranges);
        if (new_pagemap != nullptr) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(new_pagemap);
            mm::virt::destroy_user_space(new_pagemap, task->pid, new_name != nullptr ? new_name : task->name, "exec-new-image-cleanup");
            mm::virt::release_pagemap(new_pagemap, task->pid);
            new_pagemap = nullptr;
        }
        if (new_thread != nullptr) {
            new_thread->tls_phys_ptr = 0;
            new_thread->stack_phys_ptr = 0;
            mod::sched::threading::destroy_thread(new_thread);
            new_thread = nullptr;
        }
        if (new_name != nullptr) {
            delete[] new_name;
            new_name = nullptr;
        }
        if (elf_buffer != nullptr) {
            delete[] elf_buffer;
            elf_buffer = nullptr;
        }
    };
    if (!ELF_CONTRACT_VALID) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>(-ENOEXEC);
    }
    if (new_thread == nullptr) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (!mod::mm::user_layout::choose_mmap_cursor(new_mmap_cursor)) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -EIO, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>(-EIO);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, 0, 0, WOS_PERF_CALLSITE());

    // --- Load ELF into new pagemap ---
    auto& LOAD_ELF_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::LOAD_ELF,
                                                clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    auto& main_loader_lazy_ranges = scratch->main_lazy_ranges;
    loader::elf::ElfLoadOptions const MAIN_LOAD_OPTIONS{
        .register_special_symbols = true,
        .base_address = 0,
        .lazy_file_ranges = EXEC_LAZY_FILE_SEGMENTS ? &main_loader_lazy_ranges : nullptr,
        .debug_registry_pid = DEBUG_STAGING_PID,
        .image_role = mod::mm::user_layout::ImageRole::MAIN,
        .policy_owner = task,
    };
    loader::elf::ElfLoadResult elf_result =
        FILE_BACKED_ELF
            ? loader::elf::load_elf(prepared_elf.view, new_pagemap, task->pid, task->name, MAIN_LOAD_OPTIONS)
            : loader::elf::load_elf(elf_buffer, static_cast<size_t>(FILE_SIZE), new_pagemap, task->pid, task->name, MAIN_LOAD_OPTIONS);
    if (elf_result.entry_point == 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF load failed for '%s'", exec_path);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE,
                             (elf_result.error != 0 ? elf_result.error : -ENOEXEC), static_cast<uint64_t>(FILE_SIZE), WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>((elf_result.error != 0 ? elf_result.error : -ENOEXEC));
    }
    if (!append_exec_lazy_file_ranges(new_lazy_ranges, main_loader_lazy_ranges, exec_file, exec_stat)) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE, -ENOMEM, static_cast<uint64_t>(FILE_SIZE),
                             WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (!FILE_BACKED_ELF) {
        release_exec_file_once();
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE, 0, static_cast<uint64_t>(FILE_SIZE),
                         WOS_PERF_CALLSITE());

    uint64_t const NEW_EXEC_ENTRY = elf_result.entry_point;
    uint64_t new_initial_rip = elf_result.entry_point;
    uint64_t const NEW_PROGRAM_HEADER_ADDR = elf_result.program_header_addr;
    uint64_t const NEW_ELF_HEADER_ADDR = elf_result.elf_header_addr;
    uint64_t const NEW_IMAGE_VADDR_START = elf_result.image_start;
    uint64_t const NEW_IMAGE_VADDR_END = elf_result.image_end;
    uint16_t const NEW_PROGRAM_HEADER_COUNT = elf_result.program_header_count;
    uint16_t const NEW_PROGRAM_HEADER_ENT_SIZE = elf_result.program_header_ent_size;
    uint64_t new_interp_base = 0;
    uint64_t new_interp_vaddr_start = 0;
    uint64_t new_interp_vaddr_end = 0;

    // If the binary requests a dynamic linker (PT_INTERP), load it.
    if (elf_result.has_interp) {
        const char* const INTERP_PATH = std::begin(elf_result.interp_path);
        auto& LOAD_INTERP_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::LOAD_INTERP, 0, WOS_PERF_CALLSITE());

        int interp_open_flags = vfs::O_NOTIFY_CACHE_CHANGE;
        if (task->wki_submitter_hostname.front() == '\0') {
            interp_open_flags |= vfs::O_LOCAL;
        }
        vfs::File* interp_file = nullptr;
        int const INTERP_OPEN_RESULT =
            vfs::vfs_open_file_as(*task, request->admitted_credentials, INTERP_PATH, interp_open_flags, &interp_file);
        if (INTERP_OPEN_RESULT != 0) {
            dbg::log("wos_proc_execve: Failed to open interpreter '%s'", INTERP_PATH);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        vfs::Stat interp_stat{};
        int const INTERP_PIN_RET = vfs::vfs_stabilize_exec_file(&interp_file);
        int const INTERP_STAT_RET = INTERP_PIN_RET == 0 ? vfs::vfs_fstat_file(interp_file, &interp_stat) : INTERP_PIN_RET;
        int const INTERP_ACCESS = INTERP_STAT_RET == 0 ? check_exec_permission_from_stat(request->admitted_credentials, interp_stat) : 0;
        if (INTERP_ACCESS != 0) {
            vfs::vfs_put_file(interp_file);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, INTERP_ACCESS, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(INTERP_ACCESS);
        }
        ssize_t const INTERP_SIZE = INTERP_STAT_RET == 0 ? interp_stat.st_size : -1;
        if (INTERP_SIZE <= 0) {
            vfs::vfs_put_file(interp_file);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        auto& prepared_interp = scratch->prepared_interp;
        auto*& interp_buf = scratch->interp_buf;
        auto release_interp_file_once = [&]() {
            if (interp_file != nullptr) {
                prepared_interp.read_context.file = nullptr;
                vfs::vfs_put_file(interp_file);
                interp_file = nullptr;
            }
        };

        PrepareFileBackedElfResult const INTERP_PREPARED = prepare_file_backed_elf(
            interp_file, static_cast<size_t>(INTERP_SIZE), prepared_interp, mod::mm::user_layout::ImageRole::INTERPRETER);
        bool const FILE_BACKED_INTERP = INTERP_PREPARED == PrepareFileBackedElfResult::READY;
        if (!FILE_BACKED_INTERP && INTERP_PREPARED != PrepareFileBackedElfResult::FALLBACK) {
            int const ERROR = INTERP_PREPARED == PrepareFileBackedElfResult::OUT_OF_MEMORY ? -ENOMEM
                              : INTERP_PREPARED == PrepareFileBackedElfResult::IO_ERROR
                                  ? (prepared_interp.read_context.error != 0 ? prepared_interp.read_context.error : -EIO)
                                  : -ENOEXEC;
            release_interp_file_once();
            scratch->reset_interp_storage();
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, ERROR, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(ERROR);
        }
        bool const INTERP_LAZY_FILE_SEGMENTS = interp_file != nullptr && exec_lazy_file_segments_enabled();
        if (!FILE_BACKED_INTERP) {
            // Drop partial metadata before allocating the original sparse buffer.
            prepared_interp.reset();
            interp_buf = new uint8_t[INTERP_SIZE];
            if (interp_buf == nullptr) {
                release_interp_file_once();
                end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
                cleanup_new_image();
                return static_cast<uint64_t>(-ENOMEM);
            }
            ExecImageReadResult const INTERP_READ = read_exec_image_for_loader(interp_file, interp_buf, static_cast<size_t>(INTERP_SIZE),
                                                                               INTERP_PATH, false, INTERP_LAZY_FILE_SEGMENTS);
            if (INTERP_READ.status < 0) {
                release_interp_file_once();
                scratch->reset_interp_storage();
                end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, INTERP_READ.status, 0,
                                     WOS_PERF_CALLSITE());
                cleanup_new_image();
                return static_cast<uint64_t>(INTERP_READ.status);
            }
        }

        auto& interp_loader_lazy_ranges = scratch->interp_lazy_ranges;
        loader::elf::ElfLoadOptions const INTERP_LOAD_OPTIONS{
            .register_special_symbols = false,
            .base_address = 0,
            .lazy_file_ranges = INTERP_LAZY_FILE_SEGMENTS ? &interp_loader_lazy_ranges : nullptr,
            .debug_registry_pid = DEBUG_STAGING_PID,
            .image_role = mod::mm::user_layout::ImageRole::INTERPRETER,
            .policy_owner = task,
        };
        loader::elf::ElfLoadResult const INTERP_RESULT =
            FILE_BACKED_INTERP
                ? loader::elf::load_elf(prepared_interp.view, new_pagemap, task->pid, "ld.so", INTERP_LOAD_OPTIONS)
                : loader::elf::load_elf(interp_buf, static_cast<size_t>(INTERP_SIZE), new_pagemap, task->pid, "ld.so", INTERP_LOAD_OPTIONS);

        if (INTERP_RESULT.entry_point == 0) {
            int const ERROR = prepared_interp.read_context.error != 0 ? prepared_interp.read_context.error
                              : INTERP_RESULT.error != 0              ? INTERP_RESULT.error
                                                                      : -ENOEXEC;
            release_interp_file_once();
            scratch->reset_interp_storage();
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, ERROR, static_cast<uint64_t>(INTERP_SIZE),
                                 WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(ERROR);
        }
        if (!append_exec_lazy_file_ranges(new_lazy_ranges, interp_loader_lazy_ranges, interp_file, interp_stat)) {
            release_interp_file_once();
            scratch->reset_interp_storage();
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOMEM,
                                 static_cast<uint64_t>(INTERP_SIZE), WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOMEM);
        }
        release_interp_file_once();
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, 0, static_cast<uint64_t>(INTERP_SIZE),
                             WOS_PERF_CALLSITE());

        // Override entry point to the interpreter — ld.so reads AT_ENTRY from auxv
        new_initial_rip = INTERP_RESULT.entry_point;
        new_interp_base = INTERP_RESULT.load_base;
        new_interp_vaddr_start = INTERP_RESULT.image_start;
        new_interp_vaddr_end = INTERP_RESULT.image_end;

        scratch->reset_interp_storage();
    }

    auto& STACK_SETUP_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::STACK_SETUP, 0, WOS_PERF_CALLSITE());

    std::string_view const PATH_STR(exec_path, std::strlen(exec_path));
    const char* base_name = PATH_STR.data();
    if (size_t const SLASH = PATH_STR.rfind('/'); SLASH != std::string_view::npos) {
        base_name = PATH_STR.data() + SLASH + 1;
    }
    {
        size_t const BASE_LEN = std::strlen(base_name);
        new_name = new char[BASE_LEN + 1];
        if (new_name == nullptr) {
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOMEM);
        }
        std::memcpy(new_name, base_name, BASE_LEN + 1);
    }

    // Advertise only this captured native root attempt. Old kernels advertise
    // nothing; new libc then skips the optional operation entirely.
    LocalProcStage const STARTUP_ATTEMPT = request->trace.attempts.front();
    bool const STARTUP_TRACE =
        STARTUP_ATTEMPT.active && STARTUP_ATTEMPT.capture_session ==
                                      perf::current_wki_capture_session(perf::WkiPerfScope::LOCAL_PROC,
                                                                        static_cast<uint8_t>(perf::WkiPerfLocalProcOp::USERSPACE_STAGE));

    // --- Set up the user stack with argv/envp/auxv ---
    uint64_t user_stack_virt = new_thread->stack;
    uint64_t current_virt_offset = 0;

    auto copy_to_user_stack = [&](uint64_t vaddr, const void* data, size_t size) -> bool {
        if (size == 0) {
            return true;
        }
        uint64_t const END = vaddr + static_cast<uint64_t>(size);
        if (END < vaddr) {
            return false;
        }
        if (!mod::sched::threading::ensure_stack_backing(new_thread, new_pagemap, vaddr, END, task)) {
            exec_log::error("copyToStack: failed to back stack range [0x%llx,0x%llx)", static_cast<unsigned long long>(vaddr),
                            static_cast<unsigned long long>(END));
            return false;
        }

        auto const* src = static_cast<const uint8_t*>(data);
        size_t copied = 0;
        while (copied < size) {
            uint64_t const CUR = vaddr + copied;
            uint64_t const PAGE_VIRT = CUR & ~(mm::paging::PAGE_SIZE - 1);
            uint64_t const PAGE_OFFSET = CUR & (mm::paging::PAGE_SIZE - 1);
            size_t const CHUNK =
                (size - copied < mm::paging::PAGE_SIZE - PAGE_OFFSET) ? size - copied : mm::paging::PAGE_SIZE - PAGE_OFFSET;
            uint64_t const PAGE_PHYS = mm::virt::translate(new_pagemap, PAGE_VIRT);
            if (PAGE_PHYS == mm::virt::PADDR_INVALID) {
                exec_log::error("copyToStack: translate failed for stack vaddr 0x%llx", static_cast<unsigned long long>(PAGE_VIRT));
                return false;
            }
            auto* dest_ptr = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(PAGE_PHYS)) + PAGE_OFFSET;
            std::memcpy(dest_ptr, src + copied, CHUNK);
            copied += CHUNK;
        }
        return true;
    };

    auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
        if (current_virt_offset + size > ker::mod::mm::USER_STACK_SIZE) {
            return 0;
        }
        current_virt_offset += size;
        uint64_t const VIRT_ADDR = user_stack_virt - current_virt_offset;
        if (!copy_to_user_stack(VIRT_ADDR, data, size)) {
            return 0;
        }
        return VIRT_ADDR;
    };

    auto push_string = [&](const char* str) -> uint64_t {
        size_t const LEN = std::strlen(str) + 1;
        if (current_virt_offset + LEN > ker::mod::mm::USER_STACK_SIZE) {
            return 0;
        }
        current_virt_offset += LEN;
        uint64_t const VIRT_ADDR = user_stack_virt - current_virt_offset;
        if (!copy_to_user_stack(VIRT_ADDR, str, LEN)) {
            return 0;
        }
        return VIRT_ADDR;
    };

    auto*& argv_addrs = scratch->argv_addrs;
    argv_addrs = new uint64_t[argv_count + 1];
    if (argv_addrs == nullptr) {
        cleanup_new_image();
        return static_cast<uint64_t>(-ENOMEM);
    }
    for (size_t i = 0; i < argv_count; i++) {
        argv_addrs[i] = push_string(K_ARGV[i]);
        if (argv_addrs[i] == 0) {
            cleanup_new_image();
            return static_cast<uint64_t>(-E2BIG);
        }
    }
    argv_addrs[argv_count] = 0;

    auto*& envp_addrs = scratch->envp_addrs;
    envp_addrs = new uint64_t[envp_count + 1];
    if (envp_addrs == nullptr) {
        cleanup_new_image();
        return static_cast<uint64_t>(-ENOMEM);
    }
    for (size_t i = 0; i < envp_count; i++) {
        envp_addrs[i] = push_string(K_ENVP[i]);
        if (envp_addrs[i] == 0) {
            cleanup_new_image();
            return static_cast<uint64_t>(-E2BIG);
        }
    }
    envp_addrs[envp_count] = 0;

    // Keep the pathname in the replacement image so AT_EXECFN never points
    // back into the old address space destroyed by a successful execve().
    uint64_t const EXECFN_ADDR = push_string(exec_path);
    if (EXECFN_ADDR == 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }

    std::array<uint8_t, AT_RANDOM_BYTES> at_random{};
    if (!mod::random::entropy::get_bytes(at_random.data(), at_random.size())) {
        cleanup_new_image();
        return static_cast<uint64_t>(-EIO);
    }
    uint64_t const NEW_AT_RANDOM_ADDR = push_to_stack(at_random.data(), at_random.size());
    std::memset(at_random.data(), 0, at_random.size());
    if (NEW_AT_RANDOM_ADDR == 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }

    security::Snapshot stack_credentials;
    int stack_credential_error = 0;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        auto* owner = task->credential_owner();
        security::Snapshot before;
        stack_credential_error = sched::resource::prepare_exec_credentials(*task,
                                                                           {.uid = exec_stat.st_uid,
                                                                            .gid = exec_stat.st_gid,
                                                                            .set_uid = (exec_stat.st_mode & 04000) != 0,
                                                                            .set_gid = (exec_stat.st_mode & 02000) != 0,
                                                                            .nosuid = EXEC_NOSUID,
                                                                            .traced = owner != nullptr && owner->ptrace_traced,
                                                                            .mode = exec_stat.st_mode},
                                                                           before, stack_credentials, &request->admitted_credentials);
    }
    if (stack_credential_error != 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(stack_credential_error);
    }

    // Alignment
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_BASE_QWORDS = 16 + security::exec_policy::IDENTITY_AUXV_WORDS;
        size_t const AUXV_QWORDS = AUXV_BASE_QWORDS + (new_interp_base != 0 ? 2 : 0) + (STARTUP_TRACE ? 6 : 0);
        size_t const STRUCTURED_QWORDS = AUXV_QWORDS + (envp_count + 1) + (argv_count + 1) + 1;
        if (STRUCTURED_QWORDS % 2 != 0) {
            uint64_t pad = 0;
            push_to_stack(&pad, sizeof(uint64_t));
        }
    }

    // auxv
    {
        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_PHENT = 4;
        constexpr uint64_t AT_PHNUM = 5;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_BASE = 7;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_RANDOM = 25;
        constexpr uint64_t AT_EXECFN = 31;
        bool built_correct_auxv = true;
        ker::util::SmallVec<uint64_t, 36> auxv;
        built_correct_auxv &= auxv.push_back(AT_PAGESZ);
        built_correct_auxv &= auxv.push_back(mm::paging::PAGE_SIZE);
        built_correct_auxv &= auxv.push_back(AT_ENTRY);
        built_correct_auxv &= auxv.push_back(NEW_EXEC_ENTRY);
        built_correct_auxv &= auxv.push_back(AT_PHDR);
        built_correct_auxv &= auxv.push_back(NEW_PROGRAM_HEADER_ADDR);
        built_correct_auxv &= auxv.push_back(AT_PHENT);
        built_correct_auxv &= auxv.push_back(NEW_PROGRAM_HEADER_ENT_SIZE);
        built_correct_auxv &= auxv.push_back(AT_PHNUM);
        built_correct_auxv &= auxv.push_back(NEW_PROGRAM_HEADER_COUNT);
        if (new_interp_base != 0) {
            built_correct_auxv &= auxv.push_back(AT_BASE);
            built_correct_auxv &= auxv.push_back(new_interp_base);
        }
        built_correct_auxv &= auxv.push_back(AT_RANDOM);
        built_correct_auxv &= auxv.push_back(NEW_AT_RANDOM_ADDR);
        built_correct_auxv &= auxv.push_back(AT_EXECFN);
        built_correct_auxv &= auxv.push_back(EXECFN_ADDR);
        for (auto value : security::exec_policy::identity_auxv(stack_credentials)) built_correct_auxv &= auxv.push_back(value);
        if (STARTUP_TRACE) {
            built_correct_auxv &= auxv.push_back(WOS_AT_PERF_TRACE_VERSION);
            built_correct_auxv &= auxv.push_back(WOS_PERF_TRACE_VERSION);
            built_correct_auxv &= auxv.push_back(WOS_AT_PERF_TRACE_SESSION);
            built_correct_auxv &= auxv.push_back(STARTUP_ATTEMPT.capture_session);
            built_correct_auxv &= auxv.push_back(WOS_AT_PERF_TRACE_CORRELATION);
            built_correct_auxv &= auxv.push_back(STARTUP_ATTEMPT.correlation);
        }
        built_correct_auxv &= auxv.push_back(AT_NULL);
        built_correct_auxv &= auxv.push_back(0);

        if (!built_correct_auxv) {
            dbg::log("wos_proc_execve: Failed to build auxv");
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOMEM);
        }

        for (int j = static_cast<int>(auxv.size()) - 1; j >= 0; j--) {
            uint64_t val = auxv.at(static_cast<size_t>(j));
            push_to_stack(&val, sizeof(uint64_t));
        }
    }

    uint64_t const ENVP_PTR = push_to_stack(envp_addrs, (envp_count + 1) * sizeof(uint64_t));
    if (ENVP_PTR == 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }

    uint64_t const ARGV_PTR = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    if (ARGV_PTR == 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }

    uint64_t argc = argv_count;
    if (push_to_stack(&argc, sizeof(uint64_t)) == 0) {
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::STACK_SETUP, STACK_SETUP_STAGE, 0, current_virt_offset, WOS_PERF_CALLSITE());

    // Freshly spawned processes rewrite fs:[0] just before the first usermode
    // entry. execve() bypasses that path, so prepare the replacement TCB before
    // publishing any part of the image. Missing backing is a failed exec, not a
    // silent TLS/SafeStack fallback.
    uint64_t const TCB_PADDR = mm::virt::translate(new_pagemap, new_thread->fsbase);
    if (TCB_PADDR == mm::virt::PADDR_INVALID) {
        cleanup_new_image();
        return static_cast<uint64_t>(-EIO);
    }
    void* const TCB_SELF = mm::addr::get_virt_pointer(TCB_PADDR);
    std::memcpy(TCB_SELF, &new_thread->fsbase, sizeof(new_thread->fsbase));

    auto* const SSYM = loader::debug::get_process_symbol(DEBUG_STAGING_PID, "__safestack_unsafe_stack_ptr");
    if (SSYM != nullptr && SSYM->is_tls_offset) {
        if (SSYM->raw_value > UINT64_MAX - new_thread->tls_base_virt) {
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOEXEC);
        }
        uint64_t const DEST_VADDR = new_thread->tls_base_virt + SSYM->raw_value;
        uint64_t const DEST_PADDR = mm::virt::translate(new_pagemap, DEST_VADDR);
        if (DEST_PADDR == mm::virt::PADDR_INVALID) {
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOEXEC);
        }
        auto* dest_ptr = static_cast<uint64_t*>(mm::addr::get_virt_pointer(DEST_PADDR));
        *dest_ptr = new_thread->safestack_ptr_value;
    }

    auto& COMMIT_STAGE = request->trace.stage(task, TRACE_DEPTH, perf::WkiPerfLocalProcOp::COMMIT, 0, WOS_PERF_CALLSITE());
    uint64_t const NEW_RSP = user_stack_virt - current_virt_offset;
    auto const INITIAL_REGS = [&]() {
        cpu::GPRegs regs{};
        regs.rdi = new_initial_rip;
        regs.rsi = NEW_RSP;
        return regs;
    }();
    // Publish the pagemap, all mapping metadata, and the execution context as
    // one shared-VM transaction. Procfs, ptrace, mmap, fork, and thread
    // creation take the same guard and therefore cannot observe a mixed image.
    // No old-image storage is released until after the guard is dropped.
    {
        NativeExecImage const IMAGE{
            .entry = NEW_EXEC_ENTRY,
            .program_header_addr = NEW_PROGRAM_HEADER_ADDR,
            .elf_header_addr = NEW_ELF_HEADER_ADDR,
            .image_load_base = elf_result.load_base,
            .image_vaddr_start = NEW_IMAGE_VADDR_START,
            .image_vaddr_end = NEW_IMAGE_VADDR_END,
            .program_header_count = NEW_PROGRAM_HEADER_COUNT,
            .program_header_ent_size = NEW_PROGRAM_HEADER_ENT_SIZE,
            .elf_buffer = elf_buffer,
            .elf_buffer_size = ELF_BUFFER_SIZE,
            .elf_buffer_complete = ELF_BUFFER_COMPLETE,
            .exec_image_file = FILE_BACKED_ELF ? exec_file : nullptr,
            .exec_image_size = FILE_BACKED_ELF ? static_cast<uint64_t>(FILE_SIZE) : 0,
            .interp_base = new_interp_base,
            .interp_vaddr_start = new_interp_vaddr_start,
            .interp_vaddr_end = new_interp_vaddr_end,
            .at_random_addr = NEW_AT_RANDOM_ADDR,
            .mmap_next = new_mmap_cursor,
            .name = new_name,
            .path = exec_path,
            .debug_staging_pid = DEBUG_STAGING_PID,
            .initial_rip = new_initial_rip,
            .initial_rsp = NEW_RSP,
            .registers = INITIAL_REGS,
            .pagemap = new_pagemap,
            .thread = new_thread,
            .lazy_ranges = new_lazy_ranges,
            .stat = exec_stat,
            .nosuid = EXEC_NOSUID,
            .memory_policy = request->memory_policy,
            .one_shot_wki_target = (task->wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_ONESHOT) != 0,
            .admitted_credentials = request->admitted_credentials,
            .stack_credentials = stack_credentials,
        };
        std::unique_ptr<PreparedNativeExecImage> prepared(new (std::nothrow) PreparedNativeExecImage(IMAGE, task->pid));
        if (!prepared) {
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            return static_cast<uint64_t>(-ENOMEM);
        }
        // The owner has consumed name/lazy storage and owns these backing refs.
        // Completion will read pagemap/thread only from the adopted image.
        elf_buffer = nullptr;
        exec_file = nullptr;
        if (!adopt_exec_preparation(*task, prepared)) {
            // A rejected candidate must not replace the existing recovery slot.
            // Protect its synchronous destruction too, preserving an outer guard.
            bool const WAS_CLEANING = task->exec_preparation_teardown_in_progress.exchange(true, std::memory_order_acq_rel);
            prepared.reset();
            if (!WAS_CLEANING) {
                task->exec_preparation_teardown_in_progress.store(false, std::memory_order_release);
            }
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, -EAGAIN, 0, WOS_PERF_CALLSITE());
            return static_cast<uint64_t>(-EAGAIN);
        }
        request->completion = {.trace_depth = TRACE_DEPTH, .subject_pid = task->pid};
        return 0;
    }
}

template <typename Wait>
void transfer_native_worker_signals(ker::mod::sched::task::Task& owner, ker::mod::sched::task::Task& worker, uint64_t mask, Wait&& wait) {
    // Root publication has already closed new old-image worker admissions in
    // signal_local_as. Finish every earlier authorized effect before taking
    // the worker's pending bits and acknowledging its retirement.
    while (!worker.local_signal_admissions_drained()) wait();
    owner.signal_mask_store(mask);
    owner.signal_add_pending_mask(worker.signal_take_pending_mask(~uint64_t{0}));
}

// Called after all recursive preparation frames have returned. This executor
// still owns the adopted image; no loader-local pointer or destructor survives.
auto finish_native_exec_image(ker::mod::sched::task::Task* task, PreparedNativeExecImage* PREPARED, NativeExecCompletion completion,
                              WorkerExecHandoff* handoff) -> uint64_t {
    using namespace ker::mod;
    bool const OWNER_HANDOFF = handoff != nullptr;
    auto const INITIAL_REGS = PREPARED->view().registers;
    uint64_t const NEW_RSP = PREPARED->view().initial_rsp;
    uint64_t const new_initial_rip = PREPARED->view().initial_rip;
    auto* const new_thread = PREPARED->view().thread;
    auto const phys_pagemap = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(PREPARED->view().pagemap)));
    auto& COMMIT_STAGE = PREPARED->trace.pending.at(completion.trace_depth);
    uint64_t const TRACE_SUBJECT_PID = completion.subject_pid;
    auto const ENTRY_ATTEMPT = PREPARED->trace.attempts.front();
    {
        // Publication can block after some Task fields have changed. Defer
        // fatal recovery until failure or complete replacement-root activation.
        task->exec_preparation_teardown_in_progress.store(true, std::memory_order_release);
        int COMMIT_RESULT = prepare_native_exec_quiescence(*task, *PREPARED);
        if (COMMIT_RESULT == 0) COMMIT_RESULT = commit_native_exec_image(task, *PREPARED);
        if (COMMIT_RESULT < 0) {
            task->exec_preparation_teardown_in_progress.store(false, std::memory_order_release);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, COMMIT_RESULT, 0, WOS_PERF_CALLSITE());
            cleanup_exec_preparation_with_status(*task, COMMIT_RESULT);
            if (handoff != nullptr) acknowledge_worker_exec(*task, *handoff, COMMIT_RESULT);
            return static_cast<uint64_t>(COMMIT_RESULT);
        }

        asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");
        ker::mod::sys::context_switch::reset_fpu_state(task);

        // execve() returns directly via sysret instead of re-entering through the
        // scheduler, so refresh the live CPU's user TLS bases after the new CR3 is
        // active and before any new-image TLS access in userspace.
        cpu::wrfsbase(new_thread->fsbase);
        cpu_set_msr(IA32_KERNEL_GS_BASE, new_thread->gsbase);
        PREPARED->mark_activated();
        if (handoff != nullptr) {
            transfer_native_worker_signals(*task, *handoff->worker, handoff->signal_mask, [] { sched::kern_yield(); });
            task->main_thread_retired.store(false, std::memory_order_release);
            // Do not touch handoff after this store: the worker can retire and
            // destroy its argument request immediately. Sibling SIGKILL is later.
            acknowledge_worker_exec(*task, *handoff, 0);
            handoff = nullptr;
        }
        task->exec_preparation_teardown_in_progress.store(false, std::memory_order_release);
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, 0, 0, WOS_PERF_CALLSITE());

        // The replacement root/TLS are active before siblings can become the last
        // old-root GC owners. Wait without the publication/action mutexes so their
        // blocked syscalls and task-owned cleanup can complete cooperatively.
        retire_exec_siblings(*task);

        // Old-image siblings could publish sigaction or descriptor changes after
        // the image commit. Finalize shared state only after their cooperative
        // cleanup, while exec admission still excludes new workers.
        {
            mod::sys::MutexGuard action_guard(task->signal_action_mutex());
            task->reset_signal_state_for_exec();
        }

        // exec only closes FD_CLOEXEC descriptors after the replacement image is
        // committed; failed execve() therefore leaves the original descriptor
        // table intact. Snapshot first because vfs_close() mutates fd_table.
        for (;;) {
            FdSnapshot fds{};
            size_t const FD_COUNT = collect_cloexec_fds_locked(task, fds);
            if (FD_COUNT == 0) {
                break;
            }
            for (size_t i = 0; i < FD_COUNT; ++i) {
                vfs::vfs_close(static_cast<int>(fixed_slot(fds, i)));
            }
        }
        static_cast<void>(ensure_exec_stdio_fallbacks(task));

        // execve() replaces the current image in-place. Reclaim an exclusive old
        // address space now; if CLONE_VM/thread publishers still exist, their
        // last-publisher GC owns that shared root instead.
        auto& DESTROY_OLD_STAGE = PREPARED->trace.pending.at(completion.trace_depth);
        DESTROY_OLD_STAGE =
            begin_local_proc_stage_for_subject(TRACE_SUBJECT_PID, perf::WkiPerfLocalProcOp::DESTROY_OLD, 0, WOS_PERF_CALLSITE());
        ker::syscall::shm::shm_cleanup_for_task(task);
        cleanup_exec_preparation_with_status(*task, 0);

    }  // Exec admission and owned storage are gone before the on-stack return patch.

    // The request and prepared image are already destroyed; retain values only.
    // Failed exec never arms/replaces this successful image's entry marker.
    arm_native_exec_user_return(*task, ENTRY_ATTEMPT, new_initial_rip);

    // --- Update the sysret return path so it lands at the new binary ---
    //
    // The syscall return in syscall.asm uses `sysret`:
    //   - RCX (popped from the kernel stack) = return RIP
    //   - R11 (popped from the kernel stack) = RFLAGS
    //   - [gs:0x08] = user RSP
    //   - [gs:0x28] = saved RCX for diagnostic check
    //   - [gs:0x30] = saved R11 (RFLAGS)
    //   - CR3 = page table base
    //
    // We must update ALL of these to point at the new binary. The `gpr`
    // reference only modifies a local copy in syscall_handler (passed by
    // value), so it has no effect on the actual stack-saved registers.

    // 1. Compute the base of the pushq-saved register block on the kernel stack.
    //    gs:0x0 = kernel stack top (K).  After `sub rsp,8` (retval slot) +
    //    `pushq` (15 regs x 8 = 120 bytes), RSP = K-128.
    //    The GPRegs struct maps directly to K-128 (r15 at offset 0, rax at 0x70).
    //    The compiler accesses this as a stack-passed MEMORY-class parameter at
    //    the callee's rbp+0x10 = K-128.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t kern_stack_top = 0;
    asm volatile("movq %%gs:0x0, %0" : "=r"(kern_stack_top));
    auto* stack_base = reinterpret_cast<uint8_t*>(kern_stack_top - 128);

#ifdef EXEC_DEBUG
    // Log BEFORE patching the stack - dbg::log uses the stack and would
    // clobber the patched register slots if called after.
    dbg::log("wos_proc_execve: PID %x now running '%s' (entry 0x%lx, rsp 0x%lx)", task->pid, task->exe_path.data(), new_initial_rip,
             NEW_RSP);
#endif

    if (OWNER_HANDOFF) {
        asm volatile("cli" ::: "memory");
        task->deferred_task_switch = false;
        task->yield_switch = false;
        task->set_voluntary_blocked(false);
        task->context.regs = INITIAL_REGS;
        task->context.frame.rip = new_initial_rip;
        task->context.frame.rsp = NEW_RSP;
        task->context.frame.cs = 0x23;
        task->context.frame.ss = 0x1b;
        task->context.frame.flags = 0x202;
        task->context.frame.int_num = 0;
        task->context.frame.err_code = 0;
        sys::context_switch::record_saved_frame_class(task, task->context.frame, sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);
        wos_asm_enter_exec_owner(new_initial_rip, NEW_RSP, task->context.syscall_kernel_stack);
    }

    // === CRITICAL SECTION: No function calls below this point! ===
    // Any function call (including dbg::log) would use the kernel stack
    // and overwrite the patched register values.

    // 2. Patch the full on-stack GPRegs block so the new image gets the same
    // clean register state as a freshly spawned task instead of inheriting the
    // old syscall call-site's scratch values.
    // Retirement/cleanup can be preempted in kernel context, overwriting the
    // scheduler's saved registers. Only the private startup snapshot belongs
    // to this new user image.
    *reinterpret_cast<cpu::GPRegs*>(stack_base) = INITIAL_REGS;
    reinterpret_cast<cpu::GPRegs*>(stack_base)->rcx = new_initial_rip;
    reinterpret_cast<cpu::GPRegs*>(stack_base)->r11 = 0x202;  // IF set

    // 3. Update PerCpu scratch area so sysret diagnostic check passes and
    //    the correct user RSP is restored.
    asm volatile("movq %0, %%gs:0x28" : : "r"(new_initial_rip) : "memory");
    asm volatile("movq %0, %%gs:0x30" : : "r"(static_cast<uint64_t>(0x202)) : "memory");
    asm volatile("movq %0, %%gs:0x08" : : "r"(NEW_RSP) : "memory");

    // 4. Switch CR3 to the new pagemap so user-space sees the new mappings.
    asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");

    // Return 0.  The sysret path will pop the patched registers and jump to
    // the new entry point.
    return 0;
}
}  // namespace

void cancel_pending_exec_handoff(ker::mod::sched::task::Task& owner) {
    uint64_t const FLAGS = owner.resource_lock.lock_irqsave();
    auto* handoff = owner.pending_exec_handoff.exchange(nullptr, std::memory_order_acq_rel);
    if (handoff != nullptr) handoff->result.store(-ECANCELED, std::memory_order_release);
    auto* active = owner.active_exec_handoff.exchange(nullptr, std::memory_order_acq_rel);
    if (active != nullptr) active->result.store(-ECANCELED, std::memory_order_release);
    owner.resource_lock.unlock_irqrestore(FLAGS);
}

#ifdef WOS_SELFTEST
auto exec_selftest_native_signal_cutover_drains_worker() -> bool {
    using namespace ker::mod;
    using Task = sched::task::Task;
    constexpr uint64_t OWNER_PENDING = uint64_t{1} << (10 - 1);
    constexpr uint64_t WORKER_PENDING = uint64_t{1} << (12 - 1);
    constexpr uint64_t WORKER_LATE = uint64_t{1} << (14 - 1);
    constexpr uint64_t OWNER_LATE = uint64_t{1} << (15 - 1);
    auto const ROOT = security::Snapshot::bootstrap_root();
    security::Snapshot unrelated;
    if (ROOT.prepare({.change = security::Change::SET_UID, .real = 100}, unrelated) != 0) return false;
    bool ok = true;
    for (bool retired_main : {false, true}) {
        for (unsigned leases = 0; leases <= 2; ++leases) {
            auto owner = std::unique_ptr<Task>(new (std::nothrow) Task);
            auto worker = std::unique_ptr<Task>(new (std::nothrow) Task);
            if (!owner || !worker) return false;
            owner->type = worker->type = sched::task::TaskType::PROCESS;
            owner->pid = 0x7B7300;
            worker->pid = owner->pid + 1;
            worker->owner_pid = owner->pid;
            worker->is_thread = true;
            if (!sched::resource::bind_thread_owner(*worker, *owner)) return false;
            std::array<mm::paging::PageTable, 2> roots{};
            owner->pagemap = worker->pagemap = &roots.at(0);
            owner->main_thread_retired.store(retired_main);
            owner->signal_mask_store(OWNER_PENDING);
            worker->signal_mask_store(WORKER_PENDING);
            owner->signal_pending_store(OWNER_PENDING);
            worker->signal_pending_store(WORKER_PENDING);
            auto signal = [&](Task& target, const security::Snapshot& caller, int signum) -> int {
                if (!target.try_acquire()) return -ESRCH;
                return signal_as(target, caller, owner->job_identity(), signum);
            };
            // Uncommitted/rejected preparation leaves the old TID and state.
            ok &=
                signal(*worker, ROOT, 0) == 0 && signal(*worker, unrelated, 0) == -EPERM && worker->signal_pending_bits() == WORKER_PENDING;
            unsigned admitted = 0;
            for (unsigned i = 0; i < leases; ++i) {
                auto const ADMISSION = worker->begin_local_signal_admission();
                ok &= ADMISSION.local_lease;
                admitted += ADMISSION.local_lease ? 1U : 0U;
            }
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                owner->pagemap = &roots.at(1);
            }
            // Public signal routing must refuse the replaced TID even while
            // its Task and an earlier admission are still alive.
            ok &= signal(*worker, ROOT, 0) == -ESRCH && signal(*worker, ROOT, 12) == -ESRCH && signal(*worker, unrelated, 0) == -ESRCH &&
                  signal(*owner, ROOT, 0) == 0 && worker->signal_pending_bits() == WORKER_PENDING;
            unsigned waits = 0;
            transfer_native_worker_signals(*owner, *worker, WORKER_PENDING, [&] {
                ++waits;
                ok &= signal(*worker, ROOT, 0) == -ESRCH;
                worker->signal_generate_pending_mask(WORKER_LATE);
                // Owner-targeted occurrences need no transfer and must survive.
                owner->signal_generate_pending_mask(OWNER_LATE);
                while (admitted != 0) {
                    --admitted;
                    worker->end_local_signal_admission();
                }
            });
            uint64_t const EXPECTED = OWNER_PENDING | WORKER_PENDING | (leases != 0 ? WORKER_LATE | OWNER_LATE : 0);
            ok &= waits == (leases != 0 ? 1U : 0U) && admitted == 0 && owner->signal_pending_bits() == EXPECTED &&
                  worker->signal_pending_bits() == 0 && owner->signal_mask_bits() == WORKER_PENDING &&
                  worker->local_signal_admissions_drained() && owner->ref_count.load() == 2 && worker->ref_count.load() == 1 &&
                  owner->main_thread_retired.load() == retired_main;
            while (admitted != 0) {
                --admitted;
                worker->end_local_signal_admission();
            }
            owner->pagemap = worker->pagemap = nullptr;
        }
    }
    return ok;
}

auto exec_selftest_remote_signal_cutover_drains_actor() -> bool {
    using namespace ker::mod::sched;
    using Task = task::Task;
    constexpr uint64_t OWNER_PENDING = uint64_t{1} << (10 - 1);
    constexpr uint64_t WORKER_PENDING = uint64_t{1} << (12 - 1);
    constexpr uint64_t OWNER_LATE = uint64_t{1} << (15 - 1);
    constexpr uint64_t ACTOR_LATE = uint64_t{1} << (14 - 1);
    constexpr uint32_t ROUTE = 0x7B720001;
    bool ok = true;
    for (bool worker_exec : {false, true}) {
        for (unsigned leases = 0; leases < 4; ++leases) {
            auto owner = std::unique_ptr<Task>(new (std::nothrow) Task);
            auto worker = std::unique_ptr<Task>(new (std::nothrow) Task);
            auto sibling = std::unique_ptr<Task>(new (std::nothrow) Task);
            if (!owner || !worker || !sibling) return false;
            owner->type = worker->type = sibling->type = task::TaskType::PROCESS;
            owner->pid = 0x7B7200;
            worker->pid = owner->pid + 1;
            sibling->pid = owner->pid + 2;
            worker->owner_pid = sibling->owner_pid = owner->pid;
            worker->is_thread = sibling->is_thread = true;
            if (!resource::bind_thread_owner(*worker, *owner) || !resource::bind_thread_owner(*sibling, *owner)) return false;
            Task& actor = worker_exec ? *worker : *owner;
            Task::SignalHandlerTable handlers{};
            handlers.at(0).handler = 1;
            owner->initialize_signal_handlers(handlers);
            owner->signal_pending_store(OWNER_PENDING);
            worker->signal_pending_store(WORKER_PENDING);
            actor.signal_mask_store(WORKER_PENDING);
            owner->parent_death_signal.store(10);
            worker->parent_death_signal.store(12);
            bool owner_lease = (leases & 1) != 0;
            bool actor_lease = (leases & 2) != 0;
            if (owner_lease) ok &= owner->begin_local_signal_admission().local_lease;
            if (actor_lease) ok &= actor.begin_local_signal_admission().local_lease;
            unsigned waits = 0;
            auto finish_admitted_effects = [&] {
                ++waits;
                auto const OWNER = owner->begin_local_signal_admission();
                auto const ACTOR = actor.begin_local_signal_admission();
                ok &= OWNER.remote && !OWNER.local_lease && ACTOR.remote && !ACTOR.local_lease;
                if (OWNER.local_lease) owner->end_local_signal_admission();
                if (ACTOR.local_lease) actor.end_local_signal_admission();
                if (std::exchange(owner_lease, false)) {
                    owner->signal_generate_pending_mask(OWNER_LATE);
                    owner->end_local_signal_admission();
                }
                if (std::exchange(actor_lease, false)) {
                    actor.signal_generate_pending_mask(ACTOR_LATE);
                    actor.end_local_signal_admission();
                }
            };
            ker::net::wki::TaskExecSignals signals;
            capture_remote_exec_cutover_signals(*owner, actor, ROUTE, 77, signals, finish_admitted_effects);
            uint64_t const EXPECTED = OWNER_PENDING | (worker_exec ? WORKER_PENDING : 0) | ((leases & 1) != 0 ? OWNER_LATE : 0) |
                                      ((leases & 2) != 0 ? ACTOR_LATE : 0);
            ok &= waits == (leases != 0 ? 1U : 0U) && signals.pending == EXPECTED && signals.blocked == WORKER_PENDING &&
                  signals.ignored == 1 && signals.parent_death_signal == 10 && !owner_lease && !actor_lease;
            // A broken drain must fail without leaking the synthetic lease.
            if (std::exchange(owner_lease, false)) owner->end_local_signal_admission();
            if (std::exchange(actor_lease, false)) actor.end_local_signal_admission();
            auto const AFTER = actor.begin_local_signal_admission();
            ok &= AFTER.remote && !AFTER.local_lease && owner->local_signal_admissions_drained() && actor.local_signal_admissions_drained();
            if (AFTER.local_lease) actor.end_local_signal_admission();
            ok &= actor.wki_execution_identity().signal_task_id == ROUTE && actor.wki_execution_identity().proxy_task_id == 0 &&
                  !actor.wki_proxy_task && owner->ref_count.load() == 3;
            auto const OTHER = sibling->begin_local_signal_admission();
            ok &= !OTHER.remote && OTHER.local_lease;
            if (OTHER.local_lease) sibling->end_local_signal_admission();
        }
    }
    return ok;
}

auto exec_selftest_remote_cutover_refusal_is_atomic() -> bool {
    using namespace ker::mod;
    using Task = sched::task::Task;
    enum class Scenario { NO_ADMISSION, NOT_QUIESCED, IDS_CHANGED, TRACE_CHANGED, ADMISSION_RELEASED, NNP_CHANGED, COMMIT };
    constexpr uint64_t PID = 0x7B7400;
    if (auto* existing = sched::find_task_by_pid_safe(PID); existing != nullptr) {
        existing->release();
        return false;
    }
    bool ok = true;
    for (auto scenario : {Scenario::NO_ADMISSION, Scenario::NOT_QUIESCED, Scenario::IDS_CHANGED, Scenario::TRACE_CHANGED,
                          Scenario::ADMISSION_RELEASED, Scenario::NNP_CHANGED, Scenario::COMMIT}) {
        Task owner{};
        owner.type = sched::task::TaskType::PROCESS;
        owner.pid = PID;
        auto initial = security::Snapshot::bootstrap_root();
        if (initial.prepare({.change = security::Change::SET_RESGID, .real = 2002, .effective = 2002, .saved = 2000}, initial) != 0 ||
            initial.prepare({.change = security::Change::SET_RESUID, .real = 1002, .effective = 1002, .saved = 1000}, initial) != 0 ||
            sched::resource::initialize_credentials(owner, initial) != 0)
            return false;
        std::memcpy(owner.exe_path.data(), "/old", 5);
        std::memcpy(owner.wki_target_hostname.data(), "old-peer", 9);
        owner.wki_target_flags = Task::WKI_TARGET_FLAG_STRICT | Task::WKI_TARGET_FLAG_ONESHOT;
        auto const OLD_PATH = owner.exe_path;
        auto const OLD_TARGET = owner.wki_target_hostname;
        auto const OLD_FLAGS = owner.wki_target_flags;
        Task::FilesystemContext old_filesystem;
        if (!owner.capture_filesystem_context(old_filesystem)) return false;
        auto* const OLD_IMAGE = owner.elf_buffer;
        constexpr uint64_t PENDING = uint64_t{1} << (10 - 1);
        owner.signal_pending_store(PENDING);
        {
            RemoteExecPlan plan;
            if (!owner.try_acquire()) return false;
            plan.actor = &owner;
            plan.credentials = owner.credentials();
            plan.image_identity = {.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .mode = 06755};
            std::memcpy(plan.path.data(), "/candidate", 11);
            std::memcpy(plan.target.data(), "new-peer", 9);
            plan.target_flags = OLD_FLAGS;
            if (scenario == Scenario::NOT_QUIESCED) {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                if (!plan.admission.begin(owner)) return false;
            } else if (scenario != Scenario::NO_ADMISSION) {
                if (prepare_remote_exec_owner(owner, &plan) != 0) return false;
            }
            if (scenario == Scenario::ADMISSION_RELEASED) plan.admission.finish();
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                if (scenario == Scenario::TRACE_CHANGED) owner.ptrace_traced = true;
                if (scenario == Scenario::IDS_CHANGED &&
                    sched::resource::change_credentials(owner, {.change = security::Change::SET_EUID, .effective = 1000}) != 0)
                    return false;
                if (scenario == Scenario::NNP_CHANGED &&
                    sched::resource::change_credentials(owner, {.change = security::Change::NO_NEW_PRIVILEGES}) != 0)
                    return false;
            }
            auto const BEFORE = owner.credentials();
            ker::net::wki::TaskExecSignals signals{.ignored = 1, .blocked = 2, .pending = 4};
            constexpr uint32_t ROUTE = 0x7B740001;
            int const RESULT = cutover_remote_exec_owner(owner, ROUTE, 77, signals, &plan);
            bool const COMMITTED = scenario == Scenario::COMMIT;
            ok &= RESULT == (COMMITTED ? 0 : -EAGAIN) && plan.committed == COMMITTED;
            if (COMMITTED) {
                ok &= owner.credentials() == plan.replacement_credentials && owner.credentials().user() == security::Ids{1002, 0, 0} &&
                      std::strcmp(owner.exe_path.data(), "/candidate") == 0 && owner.wki_target_hostname.front() == '\0' &&
                      owner.wki_target_flags == Task::WKI_TARGET_FLAG_LOCAL && owner.wki_skip_legacy_placement &&
                      owner.wki_execution_identity().signal_task_id == ROUTE && signals.pending == PENDING;
            } else {
                ok &= owner.credentials() == BEFORE && owner.exe_path == OLD_PATH && owner.wki_target_hostname == OLD_TARGET &&
                      owner.wki_target_flags == OLD_FLAGS && !owner.wki_skip_legacy_placement &&
                      owner.wki_execution_identity().signal_task_id == 0 && owner.signal_pending_bits() == PENDING &&
                      signals.ignored == 1 && signals.blocked == 2 && signals.pending == 4;
            }
            Task::FilesystemContext filesystem;
            ok &= owner.capture_filesystem_context(filesystem) && filesystem.root == old_filesystem.root &&
                  filesystem.cwd == old_filesystem.cwd && owner.elf_buffer == OLD_IMAGE &&
                  owner.wki_execution_identity().proxy_task_id == 0 && !owner.wki_proxy_task && !owner.scheduler_published.load();
        }
        ok &= !owner.resource_exec_admission_closed && owner.resource_exec_quiescence_epoch.load() == 0 && owner.ref_count.load() == 1;
    }
    return ok;
}

auto exec_selftest_remote_plan_binding_and_rollback() -> bool {
    using Task = ker::mod::sched::task::Task;
    bool ok = true;
    for (bool accepted : {false, true}) {
        Task owner{};
        owner.type = ker::mod::sched::task::TaskType::PROCESS;
        std::array<uint8_t, 8> old{};
        owner.elf_buffer = old.data();
        owner.elf_buffer_size = old.size();
        owner.elf_buffer_complete = true;
        owner.is_elf_buffer_shared = true;
        std::memcpy(owner.exe_path.data(), "/old", 5);
        owner.set_wki_remote_pid(7);
        {
            RemoteExecPlan plan;
            char arg[] = "argument";
            char env[] = "KEY=value";
            const char* args[] = {arg, nullptr};
            const char* envs[] = {env, nullptr};
            if (!plan.copy_arguments(args, envs)) return false;
            arg[0] = 'X';
            env[0] = 'Y';
            ok &= std::strcmp(plan.argv[0], "argument") == 0 && std::strcmp(plan.envp[0], "KEY=value") == 0 && plan.argv[1] == nullptr &&
                  plan.envp[1] == nullptr;
            plan.bytes = new (std::nothrow) uint8_t[16];
            if (plan.bytes == nullptr) return false;
            plan.size = 16;
            std::memcpy(plan.path.data(), "/new", 5);
            std::memcpy(plan.target.data(), "peer", 5);
            plan.target_flags = Task::WKI_TARGET_FLAG_STRICT;
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                if (!plan.admission.begin(owner)) return false;
            }
            auto const candidate = plan.candidate();
            ok &= candidate.bytes.data() == plan.bytes && candidate.bytes.size() == 16 && !candidate.complete &&
                  std::strcmp(candidate.path, "/new") == 0 && std::strcmp(candidate.target, "peer") == 0 &&
                  candidate.target_flags == Task::WKI_TARGET_FLAG_STRICT;
            ok &= owner.elf_buffer == old.data() && owner.elf_buffer_size == old.size() && owner.is_elf_buffer_shared &&
                  owner.elf_buffer_complete && std::strcmp(owner.exe_path.data(), "/old") == 0 &&
                  owner.wki_execution_identity().remote_pid == 7 && owner.wki_target_hostname[0] == '\0' &&
                  !owner.wki_skip_legacy_placement && owner.resource_exec_admission_closed;
            if (accepted) {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                plan.publish_metadata(owner);
                plan.committed = true;
            }
            ok &= plan.bytes == candidate.bytes.data() && owner.elf_buffer == old.data() &&
                  owner.wki_execution_identity().remote_pid == 7 && std::strcmp(owner.exe_path.data(), accepted ? "/new" : "/old") == 0 &&
                  std::strcmp(owner.wki_target_hostname.data(), accepted ? "peer" : "") == 0 && owner.wki_skip_legacy_placement == accepted;
        }
        ok &= !owner.resource_exec_admission_closed && owner.ref_count.load() == 1;
        owner.elf_buffer = nullptr;
    }
    RemoteExecPlan empty;
    ok &= empty.copy_arguments(nullptr, nullptr) && empty.argv != nullptr && empty.envp != nullptr && empty.argv[0] == nullptr &&
          empty.envp[0] == nullptr;
    for (bool accepted : {false, true}) {
        Task owner{};
        owner.type = ker::mod::sched::task::TaskType::PROCESS;
        std::array<uint8_t, 16> image{};
        image.front() = 0x7f;
        const char* args[] = {"candidate", nullptr};
        const char* env[] = {"K=value", nullptr};
        std::memcpy(owner.exe_path.data(), "/old", 5);
        {
            RemoteExecPlan plan;
            if (!plan.borrow_current_input(image.data(), image.size(), args, env)) return false;
            plan.size = image.size();
            plan.complete = true;
            std::memcpy(plan.path.data(), "/new", 5);
            auto const CANDIDATE = plan.candidate();
            ok &= CANDIDATE.bytes.data() == image.data() && CANDIDATE.bytes.size() == image.size() && CANDIDATE.complete &&
                  plan.arguments() == args && plan.environment() == env && plan.bytes == nullptr && plan.argv == nullptr &&
                  plan.envp == nullptr && plan.strings == nullptr && std::strcmp(owner.exe_path.data(), "/old") == 0;
            if (accepted) {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                plan.publish_metadata(owner);
            }
            ok &= std::strcmp(owner.exe_path.data(), accepted ? "/new" : "/old") == 0;
        }
        // View teardown never owns these current loader/request bytes.
        ok &= image.front() == 0x7f && std::strcmp(args[0], "candidate") == 0 && std::strcmp(env[0], "K=value") == 0;
    }
    RemoteExecPlan borrowed_empty;
    ok &= borrowed_empty.borrow_current_input(nullptr, 0, nullptr, nullptr) && borrowed_empty.arguments() != nullptr &&
          borrowed_empty.environment() != nullptr && borrowed_empty.arguments()[0] == nullptr && borrowed_empty.environment()[0] == nullptr;
    return ok;
}

auto exec_selftest_remote_request_claim_and_cancel() -> bool {
    using Task = ker::mod::sched::task::Task;
    Task owner{};
    owner.type = ker::mod::sched::task::TaskType::PROCESS;
    bool ok = true;
    for (bool cancel : {false, true}) {
        WorkerExecHandoff handoff;
        handoff.remote = true;
        handoff.remote_request = new (std::nothrow) ExecRequestArguments;
        if (handoff.remote_request == nullptr) return false;
        auto* request = handoff.remote_request;
        request->remote.reset(new (std::nothrow) RemoteExecPlan);
        if (!request->remote) return false;
        owner.pending_exec_handoff.store(&handoff);
        if (cancel) {
            cancel_pending_exec_handoff(owner);
            ok &= owner.pending_exec_handoff.load() == nullptr && handoff.remote_request == request && handoff.result.load() == -ECANCELED;
        } else {
            ok &= claim_pending_exec_handoff(owner) == &handoff && handoff.remote_request == nullptr &&
                  owner.owned_exec_arguments.load() == request && owner.active_exec_handoff.load() == &handoff;
            cleanup_exec_preparation(owner);
            acknowledge_worker_exec(owner, handoff, -EAGAIN);
            ok &= owner.owned_exec_arguments.load() == nullptr && owner.active_exec_handoff.load() == nullptr &&
                  handoff.result.load() == -EAGAIN;
        }
    }
    return ok;
}
// BEGIN EXEC STDIO SCOPE SELFTESTS — append before the final WOS_SELFTEST endif.
namespace {
using StdioScopeTask = ker::mod::sched::task::Task;
namespace stdio_pid = ker::mod::sched::pid_ns;
namespace stdio_ipc = ker::mod::sched::ipc;
namespace stdio_mount = ker::vfs::mount_ns;
namespace stdio_resource = ker::mod::sched::resource;
namespace stdio_security = ker::mod::security;

struct StdioScopeFixture {
    std::unique_ptr<StdioScopeTask> task{new (std::nothrow) StdioScopeTask};
    uint64_t reserved_fd = UINT64_MAX;

    StdioScopeFixture() {
        if (task) {
            task->pid = ker::mod::sched::task::get_next_pid();
            task->type = ker::mod::sched::task::TaskType::PROCESS;
        }
    }
    ~StdioScopeFixture() {
        if (!task) return;
        // Exclusively owned, unpublished tables; close outside every IRQ lock.
        if (reserved_fd != UINT64_MAX) static_cast<void>(task->fd_table.cancel(reserved_fd, this));
        for (unsigned fd = 0; fd < StdioScopeTask::FD_TABLE_SIZE; ++fd) {
            auto* file = static_cast<vfs::File*>(task->fd_table.remove(fd));
            if (file != nullptr) vfs::vfs_put_file(file);
        }
    }
    auto add_control(unsigned fd) -> bool {
        if (!task) return false;
        auto* file = exec_selftest_make_file();
        if (file == nullptr) return false;
        file->fd = static_cast<int>(fd);
        file->open_flags = 1 | 0x800;
        file->fd_flags = 1;
        file->pos = 137;
        if (!task->fd_table.insert(fd, file)) {
            vfs::vfs_put_file(file);
            return false;
        }
        task->set_fd_cloexec(fd);
        return true;
    }
    auto reserve(unsigned fd) -> bool {
        if (!task || reserved_fd != UINT64_MAX) return false;
        reserved_fd = task->fd_table.reserve(fd, StdioScopeTask::FD_TABLE_SIZE, this);
        return reserved_fd == fd;
    }
};

struct StdioScopeFileObservation {
    vfs::File* file{};
    vfs::FileOperations* operations{};
    void* private_data{};
    off_t position{};
    int descriptor{}, flags{}, descriptor_flags{}, references{};
    vfs::FSType backend{};
    bool reserved{};
    auto operator==(const StdioScopeFileObservation&) const -> bool = default;
};

struct StdioScopeObservation {
    std::unique_ptr<std::array<StdioScopeFileObservation, StdioScopeTask::FD_TABLE_SIZE>> files{
        new (std::nothrow) std::array<StdioScopeFileObservation, StdioScopeTask::FD_TABLE_SIZE>{}};
    StdioScopeTask::FdCloexecBitmap cloexec{};
    stdio_resource::State resources{};
    stdio_security::Snapshot credentials{};
    size_t descriptors{}, reservations{};
    uint32_t task_references{};
    uint64_t thread_owners{}, cpu_threads{};
    int control_closes{};

    explicit StdioScopeObservation(StdioScopeTask& subject)
        : cloexec(subject.fd_cloexec),
          resources(subject.resources),
          credentials(subject.credentials()),
          descriptors(subject.fd_table.size()),
          reservations(subject.fd_table.reserved_count()),
          task_references(subject.ref_count.load()),
          thread_owners(subject.resource_thread_owners.load()),
          cpu_threads(subject.resource_cpu_threads.load()),
          control_closes(g_exec_selftest_close_count.load()) {
        if (!files) return;
        for (unsigned fd = 0; fd < StdioScopeTask::FD_TABLE_SIZE; ++fd) {
            auto& row = (*files)[fd];
            row.reserved = subject.fd_table.reserved(fd);
            row.file = static_cast<vfs::File*>(subject.fd_table.lookup(fd));
            if (row.file == nullptr) continue;
            row.operations = row.file->fops;
            row.private_data = row.file->private_data;
            row.position = row.file->pos;
            row.descriptor = row.file->fd;
            row.flags = row.file->open_flags;
            row.descriptor_flags = row.file->fd_flags;
            row.references = row.file->refcount.load();
            row.backend = row.file->fs_type;
        }
    }
    auto unchanged(StdioScopeTask& subject) const -> bool {
        if (!files || cloexec != subject.fd_cloexec || credentials != subject.credentials() || descriptors != subject.fd_table.size() ||
            reservations != subject.fd_table.reserved_count() || task_references != subject.ref_count.load() ||
            thread_owners != subject.resource_thread_owners.load() || cpu_threads != subject.resource_cpu_threads.load() ||
            control_closes != g_exec_selftest_close_count.load())
            return false;
        for (unsigned fd = 0; fd < StdioScopeTask::FD_TABLE_SIZE; ++fd) {
            StdioScopeFileObservation row{};
            row.reserved = subject.fd_table.reserved(fd);
            row.file = static_cast<vfs::File*>(subject.fd_table.lookup(fd));
            if (row.file != nullptr) {
                row.operations = row.file->fops;
                row.private_data = row.file->private_data;
                row.position = row.file->pos;
                row.descriptor = row.file->fd;
                row.flags = row.file->open_flags;
                row.descriptor_flags = row.file->fd_flags;
                row.references = row.file->refcount.load();
                row.backend = row.file->fs_type;
            }
            if ((*files)[fd] != row) return false;
        }
        for (unsigned id = 0; id < resources.entries.size(); ++id) {
            auto const& a = resources.entries[id];
            auto const& b = subject.resources.entries[id];
            if (a.limit.soft != b.limit.soft || a.limit.hard != b.limit.hard || a.current != b.current || a.peak != b.peak ||
                a.denied != b.denied || a.reason != b.reason || a.reserved != b.reserved)
                return false;
        }
        return resources.cpu_time_us == subject.resources.cpu_time_us && resources.cpu_user_time_us == subject.resources.cpu_user_time_us &&
               resources.cpu_system_time_us == subject.resources.cpu_system_time_us &&
               resources.cpu_last_soft_second == subject.resources.cpu_last_soft_second &&
               resources.cpu_hard_notified == subject.resources.cpu_hard_notified;
    }
};

struct StdioScopeNamespaceBaseline {
    size_t pids = stdio_pid::live_namespace_count();
    size_t ipc = stdio_ipc::live_namespace_count();
    size_t mounts = stdio_mount::live_namespace_count();
    auto restored() const -> bool {
        return pids == stdio_pid::live_namespace_count() && ipc == stdio_ipc::live_namespace_count() &&
               mounts == stdio_mount::live_namespace_count();
    }
};

auto stdio_scope_console_file(vfs::File* file, unsigned fd) -> bool {
    vfs::Stat stat{};
    return file != nullptr && file->fs_type == vfs::FSType::DEVFS && file->fops == vfs::devfs::get_devfs_fops() &&
           file->refcount.load() == 1 && file->fd == static_cast<int>(fd) && file->open_flags == (fd == 0 ? 0 : 1) && file->fd_flags == 0 &&
           vfs::vfs_fstat_file(file, &stat) == 0 && (stat.st_mode & vfs::S_IFMT) == vfs::S_IFCHR;
}

auto stdio_scope_initialize_views(StdioScopeTask& task, unsigned mask, const stdio_pid::NamespaceRef& pids,
                                  const stdio_ipc::NamespaceRef& names, const stdio_mount::NamespaceRef& mounts) -> bool {
    return ((mask & 1) == 0 || task.initialize_pid_namespace(pids)) && ((mask & 2) == 0 || task.initialize_ipc_namespace(names)) &&
           ((mask & 4) == 0 || task.initialize_mount_namespace(mounts));
}
}  // namespace

auto exec_selftest_stdio_host_and_private_views() -> bool {
    StdioScopeNamespaceBaseline const BASELINE;
    int const CONTROL_CLOSES = g_exec_selftest_close_count.load();
    // Console/devfs are genuinely initialized in phase3, before --selftest.
    // Require real opens: a missing backend must not make private tests pass.
    for (uint64_t limit : {uint64_t{256}, uint64_t{0}, uint64_t{1}, uint64_t{2}, uint64_t{3}}) {
        StdioScopeFixture host;
        if (!host.task || host.task->resources.set(static_cast<unsigned>(ker::abi::resource::Id::NOFILE), {limit, limit}, true,
                                                   StdioScopeTask::FD_TABLE_SIZE) != 0)
            return false;
        for (unsigned fd = 0; fd < 3; ++fd) host.task->set_fd_cloexec(fd);
        if (!ensure_exec_stdio_fallbacks(host.task.get()) || host.task->fd_table.size() != std::min(limit, uint64_t{3})) return false;
        for (unsigned fd = 0; fd < 3; ++fd) {
            auto* file = static_cast<vfs::File*>(host.task->fd_table.lookup(fd));
            if (fd < limit) {
                if (!stdio_scope_console_file(file, fd) || host.task->get_fd_cloexec(fd)) return false;
            } else if (file != nullptr || !host.task->get_fd_cloexec(fd)) {
                return false;
            }
        }
    }
    {
        StdioScopeFixture reserved;
        if (!reserved.reserve(0)) return false;
        StdioScopeObservation const BEFORE(*reserved.task);
        if (ensure_exec_stdio_fallbacks(reserved.task.get()) || !BEFORE.unchanged(*reserved.task)) return false;
    }
    using Network = stdio_security::compartment::Network;
    for (unsigned mask = 1; mask < 8; ++mask) {
        for (auto network : {Network::DENIED, Network::SHARED_HOST}) {
            for (bool nonroot : {false, true}) {
                stdio_pid::NamespaceRef pids, host;
                stdio_ipc::NamespaceRef names;
                stdio_mount::NamespaceRef mounts;
                if (stdio_pid::create_namespace(host, network, pids) != 0 || stdio_ipc::create_namespace(names) != 0 ||
                    stdio_mount::create_namespace(mounts) != 0)
                    return false;
                StdioScopeFixture parent, child;
                if (!parent.task || !child.task || !stdio_scope_initialize_views(*child.task, mask, pids, names, mounts)) return false;
                if (nonroot) {
                    stdio_security::Snapshot credentials;
                    if (stdio_security::Snapshot::restore_fields({.user = {100, 100, 100}, .group = {100, 100, 100}}, credentials) != 0 ||
                        stdio_resource::initialize_credentials(*child.task, credentials) != 0)
                        return false;
                }
                if ((mask & 1) != 0 && child.task->pid_namespace()->network() != network) return false;
                StdioScopeObservation const EMPTY(*child.task);
                if (!ensure_exec_stdio_fallbacks(child.task.get()) || !EMPTY.unchanged(*child.task)) return false;
                if (!parent.add_control(1) || !clone_exec_fd_table_checked(parent.task.get(), child.task.get(), true) ||
                    !child.reserve(7) ||
                    child.task->resources.set(static_cast<unsigned>(ker::abi::resource::Id::NOFILE), {0, 0}, true,
                                              StdioScopeTask::FD_TABLE_SIZE) != 0)
                    return false;
                auto* inherited = static_cast<vfs::File*>(child.task->fd_table.lookup(1));
                if (inherited == nullptr || inherited != parent.task->fd_table.lookup(1) || inherited->refcount.load() != 2 ||
                    !child.task->get_fd_cloexec(1))
                    return false;
                StdioScopeObservation const BEFORE(*child.task);
                StdioScopeObservation const PARENT(*parent.task);
                if (!ensure_exec_stdio_fallbacks(child.task.get()) || !BEFORE.unchanged(*child.task) || !PARENT.unchanged(*parent.task))
                    return false;
            }
        }
    }
    return BASELINE.restored() && g_exec_selftest_close_count.load() == CONTROL_CLOSES + 28;
}

auto exec_selftest_stdio_canonical_and_closed_views() -> bool {
    StdioScopeNamespaceBaseline const BASELINE;
    int const CONTROL_CLOSES = g_exec_selftest_close_count.load();
    if (ensure_exec_stdio_fallbacks(nullptr)) return false;
    {
        StdioScopeFixture unbound;
        if (!unbound.task || !unbound.add_control(7)) return false;
        unbound.task->is_thread = true;
        unbound.task->owner_pid = unbound.task->pid + 1;
        StdioScopeObservation const BEFORE(*unbound.task);
        if (ensure_exec_stdio_fallbacks(unbound.task.get()) || !BEFORE.unchanged(*unbound.task)) return false;
    }
    {
        stdio_pid::NamespaceRef pids;
        stdio_ipc::NamespaceRef names;
        stdio_mount::NamespaceRef mounts;
        if (stdio_pid::create_namespace(pids) != 0 || stdio_ipc::create_namespace(names) != 0 || stdio_mount::create_namespace(mounts) != 0)
            return false;
        StdioScopeFixture owner;
        if (!owner.task || !stdio_scope_initialize_views(*owner.task, 7, pids, names, mounts) || !owner.add_control(1)) return false;
        stdio_security::Snapshot credentials;
        if (stdio_security::Snapshot::restore_fields({.user = {100, 100, 100}, .group = {100, 100, 100}}, credentials) != 0 ||
            stdio_resource::initialize_credentials(*owner.task, credentials) != 0)
            return false;
        {
            StdioScopeFixture worker;
            if (!worker.task || !worker.add_control(7)) return false;
            worker.task->is_thread = true;
            worker.task->owner_pid = owner.task->pid;
            if (!stdio_resource::bind_thread_owner(*worker.task, *owner.task)) return false;
            owner.task->main_thread_retired.store(true);
            StdioScopeObservation const OWNER(*owner.task);
            StdioScopeObservation const WORKER(*worker.task);
            if (owner.task->ref_count.load() != 2 || worker.task->credential_owner() != owner.task.get() ||
                !ensure_exec_stdio_fallbacks(worker.task.get()) || !OWNER.unchanged(*owner.task) || !WORKER.unchanged(*worker.task))
                return false;
        }
        if (owner.task->ref_count.load() != 1 || owner.task->resource_cpu_threads.load() != 0 ||
            owner.task->resource_thread_owners.load() != 0)
            return false;
    }
    {
        stdio_pid::NamespaceRef pids;
        stdio_ipc::NamespaceRef names;
        stdio_mount::NamespaceRef mounts;
        if (stdio_pid::create_namespace(pids) != 0 || stdio_ipc::create_namespace(names) != 0 || stdio_mount::create_namespace(mounts) != 0)
            return false;
        StdioScopeFixture owner;
        if (!owner.task) return false;
        {
            StdioScopeFixture shadow;
            if (!shadow.task || !stdio_scope_initialize_views(*shadow.task, 7, pids, names, mounts) || !shadow.add_control(7)) return false;
            shadow.task->is_thread = true;
            shadow.task->owner_pid = owner.task->pid;
            if (!stdio_resource::bind_thread_owner(*shadow.task, *owner.task)) return false;
            pids.close();
            names.close();
            mounts.close();
            StdioScopeObservation const SHADOW(*shadow.task);
            if (!ensure_exec_stdio_fallbacks(shadow.task.get()) || owner.task->fd_table.size() != 3 || !SHADOW.unchanged(*shadow.task))
                return false;
            for (unsigned fd = 0; fd < 3; ++fd)
                if (!stdio_scope_console_file(static_cast<vfs::File*>(owner.task->fd_table.lookup(fd)), fd)) return false;
        }
        if (owner.task->ref_count.load() != 1 || owner.task->resource_cpu_threads.load() != 0 ||
            owner.task->resource_thread_owners.load() != 0)
            return false;
    }
    for (unsigned closed = 0; closed < 3; ++closed) {
        stdio_pid::NamespaceRef pids;
        stdio_ipc::NamespaceRef names;
        stdio_mount::NamespaceRef mounts;
        if (stdio_pid::create_namespace(pids) != 0 || stdio_ipc::create_namespace(names) != 0 || stdio_mount::create_namespace(mounts) != 0)
            return false;
        StdioScopeFixture subject;
        if (!subject.task || !stdio_scope_initialize_views(*subject.task, 7, pids, names, mounts) || !subject.add_control(1) ||
            !subject.reserve(7))
            return false;
        if (closed == 0) pids.close();
        if (closed == 1) names.close();
        if (closed == 2) mounts.close();
        StdioScopeObservation const BEFORE(*subject.task);
        if (ensure_exec_stdio_fallbacks(subject.task.get()) || !BEFORE.unchanged(*subject.task)) return false;
    }
    return BASELINE.restored() && g_exec_selftest_close_count.load() == CONTROL_CLOSES + 7;
}
// END EXEC STDIO SCOPE SELFTESTS

#endif

void service_pending_exec_handoff() {
    using namespace ker::mod;
    auto* owner = sched::get_current_task();
    if (owner == nullptr || owner->is_thread || owner->exit_in_progress.load(std::memory_order_acquire) ||
        owner->exec_preparation_teardown_in_progress.load(std::memory_order_acquire))
        return;
    auto* handoff = claim_pending_exec_handoff(*owner);
    if (handoff == nullptr) return;
    if (handoff->remote) {
        auto* request = owner->owned_exec_arguments.load(std::memory_order_acquire);
        auto const OUTCOME = run_remote_exec_owner(*owner, *request, handoff);
        uint32_t const ID = request->remote_proxy_id;
        if (OUTCOME.pending != nullptr) {
            auto* worker_request = OUTCOME.pending->worker->owned_exec_arguments.load(std::memory_order_acquire);
            if (worker_request != nullptr) worker_request->trace = std::exchange(request->trace, {});
        }
        cleanup_exec_preparation_with_status(*owner, OUTCOME.result == ker::net::wki::WkiRemoteSpawnResult::REMOTE ? 0 : OUTCOME.error);
        if (OUTCOME.pending != nullptr) {
            acknowledge_worker_exec(*owner, *OUTCOME.pending,
                                    OUTCOME.result == ker::net::wki::WkiRemoteSpawnResult::LOCAL ? 0 : OUTCOME.error);
        }
        if (OUTCOME.result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
            publish_remote_exec_proxy(*owner, ID);
            wos_asm_park_exec_owner(owner->context.syscall_kernel_stack);
        }
        return;
    }
    auto* prepared = owner->owned_exec_preparation.load(std::memory_order_acquire);
    if (prepared == nullptr) {
        acknowledge_worker_exec(*owner, *handoff, -EAGAIN);
        return;
    }
    // Success abandons this already-unwound owner return/retirement stack.
    // Failure preserves the old image and acknowledges the waiting worker.
    static_cast<void>(finish_native_exec_image(owner, prepared, handoff->completion, handoff));
}

}  // namespace ker::syscall::process
