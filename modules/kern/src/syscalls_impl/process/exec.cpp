#include "exec.hpp"

#include <abi/callnums/process.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>
#include <extern/elf.h>

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
#include <net/wki/remote_compute.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/user_layout.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/random/entropy.hpp>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/signal.hpp>
#include <platform/sys/usercopy.hpp>
#include <string_view>
#include <util/hcf.hpp>
#include <util/smallvec.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "net/wki/wki.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/threading.hpp"
#include "syscalls_impl/process/child_events.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "vfs/stat.hpp"
namespace ker::syscall::process {

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp,
                        const ker::abi::process::SpawnOptions* spawn_options, int shebang_depth) -> uint64_t;
auto wos_proc_execve_impl(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr, int shebang_depth)
    -> uint64_t;

constexpr int MAX_SHEBANG_DEPTH = 4;
constexpr size_t EXEC_PATH_MAX = 512;
constexpr size_t EXEC_ARG_BYTES_MAX = static_cast<size_t>(2) * 1024 * 1024;
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

void consume_successful_one_shot_wki_target(ker::mod::sched::task::Task* task, ker::net::wki::WkiRemoteSpawnResult result) {
    if (task == nullptr || result == ker::net::wki::WkiRemoteSpawnResult::FAILED ||
        (task->wki_target_flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_ONESHOT) == 0) {
        return;
    }

    task->wki_target_hostname.front() = '\0';
    // One-shot placement selects the system for the payload process tree, not
    // merely for the first executable in a wrapper chain.  Clearing to
    // automatic here lets a locally selected payload's fork/exec descendants
    // move independently, while remotely selected payloads stay receiver-local.
    // Keep both outcomes symmetric by pinning the successful payload locally.
    // A later explicit setwkitarget() call can still opt back into fan-out.
    task->wki_target_flags = ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL;
    // The rich exec path already made this task's placement decision. Prevent
    // scheduler publication from attempting the same placement again.
    task->wki_skip_legacy_placement = true;
}

using FdSnapshot = std::array<uint64_t, ker::mod::sched::task::Task::FD_TABLE_SIZE>;
using LazyVmemKind = ker::mod::sched::task::LazyVmemKind;
using LazyVmemRange = ker::mod::sched::task::LazyVmemRange;
using LazyVmemRangeVec = ker::mod::sched::task::LazyVmemRangeVec;

#ifdef WOS_SELFTEST
std::atomic<bool> g_exec_selftest_force_fd_clone_insert_failure{false};
std::atomic<bool> g_exec_selftest_force_stdio_insert_failure{false};
std::atomic<int> g_exec_selftest_close_count{0};

auto exec_selftest_close(vfs::File*) -> int {
    g_exec_selftest_close_count.fetch_add(1, std::memory_order_relaxed);
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

auto check_exec_permission_from_stat(const ker::mod::sched::task::Task* task, const vfs::Stat& statbuf) -> int {
    if (task == nullptr) {
        return -ESRCH;
    }

    auto const FILE_MODE = static_cast<uint32_t>(statbuf.st_mode & 07777);
    if (task->euid == 0) {
        return (FILE_MODE & 0111U) != 0 ? 0 : -EACCES;
    }

    uint32_t perm_bits = 0;
    if (task->euid == statbuf.st_uid) {
        perm_bits = (FILE_MODE >> 6U) & 7U;
    } else if (task->has_group(statbuf.st_gid)) {
        perm_bits = (FILE_MODE >> 3U) & 7U;
    } else {
        perm_bits = FILE_MODE & 7U;
    }

    return (perm_bits & 1U) != 0 ? 0 : -EACCES;
}

struct LocalProcStage {
    uint32_t correlation;
    uint64_t started_us;
};

auto begin_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
    -> LocalProcStage {
    LocalProcStage const STAGE = {
        .correlation = ker::mod::perf::next_wki_trace_correlation(),
        .started_us = ker::mod::time::get_us(),
    };
    record_local_proc_event(task, op, ker::mod::perf::WkiPerfPhase::BEGIN, STAGE.correlation, 0, aux, callsite);
    return STAGE;
}

auto end_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, const LocalProcStage& stage,
                          int32_t status, uint64_t bytes, uint64_t callsite) -> uint32_t {
    uint32_t const ELAPSED_US = clamp_perf_aux(ker::mod::time::get_us() - stage.started_us);
    record_local_proc_event(task, op, ker::mod::perf::WkiPerfPhase::END, stage.correlation, status, ELAPSED_US, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), 0, 0, status, ELAPSED_US, true,
                                       0, bytes);
    return ELAPSED_US;
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
    if (!FORCE_FAILURE && task->fd_table.lookup(fd) == nullptr) {
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

auto spawn_replace_fd_file(ker::mod::sched::task::Task* task, int32_t fd, vfs::File* file) -> bool {
    if (task == nullptr || file == nullptr || !spawn_fd_valid(fd)) {
        spawn_put_file(file);
        return false;
    }

    auto* existing = static_cast<vfs::File*>(nullptr);
    bool inserted = false;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    existing = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
    inserted = task->fd_table.insert(static_cast<uint64_t>(fd), file);
    if (inserted) {
        file->fd = fd;
        task->clear_fd_cloexec(static_cast<unsigned>(fd));
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    if (!inserted) {
        spawn_put_file(file);
        return false;
    }
    spawn_put_file(existing);
    return true;
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
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    spawn_put_file(existing);
    return true;
}

auto spawn_dup2_fd(ker::mod::sched::task::Task* task, int32_t srcfd, int32_t dstfd) -> bool {
    if (task == nullptr || !spawn_fd_valid(srcfd) || !spawn_fd_valid(dstfd)) {
        return false;
    }

    auto* displaced = static_cast<vfs::File*>(nullptr);
    bool ok = false;
    uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
    auto* src = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(srcfd)));
    if (src != nullptr) {
        if (srcfd == dstfd) {
            task->clear_fd_cloexec(static_cast<unsigned>(dstfd));
            ok = true;
        } else {
            src->refcount.fetch_add(1, std::memory_order_acq_rel);
            displaced = static_cast<vfs::File*>(task->fd_table.lookup(static_cast<uint64_t>(dstfd)));
            ok = task->fd_table.insert(static_cast<uint64_t>(dstfd), src);
            if (ok) {
                task->clear_fd_cloexec(static_cast<unsigned>(dstfd));
            } else {
                src->refcount.fetch_sub(1, std::memory_order_acq_rel);
                displaced = nullptr;
            }
        }
    }
    task->fd_table_lock.unlock_irqrestore(IRQF);

    spawn_put_file(displaced);
    return ok;
}

auto spawn_open_fd(ker::mod::sched::task::Task* task, const ker::abi::process::SpawnFdAction& action) -> bool {
    if (!spawn_fd_valid(action.fd) || action.path == nullptr) {
        return false;
    }

    int mode = static_cast<int>(action.mode);
    if ((action.oflag & vfs::O_CREAT) != 0 && task != nullptr) {
        mode &= ~static_cast<int>(task->umask);
    }

    auto* file = vfs::vfs_open_file(action.path, action.oflag, mode);
    if (file == nullptr) {
        return false;
    }
    return spawn_replace_fd_file(task, action.fd, file);
}

auto apply_spawn_options(ker::mod::sched::task::Task* task, const ker::abi::process::SpawnOptions* options) -> bool {
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

    for (uint64_t i = 0; i < options->action_count; ++i) {
        auto const& action = options->actions[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        bool ok = false;
        switch (static_cast<ker::abi::process::SpawnFdActionType>(action.type)) {
            case ker::abi::process::SpawnFdActionType::CLOSE:
                ok = spawn_close_fd(task, action.fd);
                break;
            case ker::abi::process::SpawnFdActionType::DUP2:
                ok = spawn_dup2_fd(task, action.srcfd, action.fd);
                break;
            case ker::abi::process::SpawnFdActionType::OPEN:
                ok = spawn_open_fd(task, action);
                break;
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

    if ((options->flags & ker::abi::process::SPAWN_FLAG_SETPGROUP) != 0) {
        if (options->pgroup < 0) {
            return false;
        }
        task->pgid = options->pgroup == 0 ? task->pid : static_cast<uint64_t>(options->pgroup);
    }
    return true;
}

auto ensure_exec_stdio_fallbacks(ker::mod::sched::task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    constexpr int STDIN_OPEN_FLAGS = 0;   // O_RDONLY
    constexpr int STDOUT_OPEN_FLAGS = 1;  // O_WRONLY

    for (unsigned fd = 0; fd < 3; ++fd) {
        {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            bool const PRESENT = task->fd_table.lookup(fd) != nullptr;
            task->fd_table_lock.unlock_irqrestore(IRQF);
            if (PRESENT) {
                continue;
            }
        }

        int const OPEN_FLAGS = fd == 0 ? STDIN_OPEN_FLAGS : STDOUT_OPEN_FLAGS;
        vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", OPEN_FLAGS, 0);
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
    const char** argv{};
    const char** envp{};
    size_t strings_used{};
    size_t budget_used{};

    ~ExecArgumentsSnapshot() {
        delete[] argv;
        delete[] envp;
        delete[] strings;
    }

    ExecArgumentsSnapshot() = default;
    ExecArgumentsSnapshot(const ExecArgumentsSnapshot&) = delete;
    auto operator=(const ExecArgumentsSnapshot&) -> ExecArgumentsSnapshot& = delete;
};

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

    ker::util::SmallVec<uint64_t, 16> string_addrs;
    for (size_t index = 0;; ++index) {
        if (snapshot.budget_used > EXEC_ARG_BYTES_MAX - sizeof(uint64_t)) {
            return -E2BIG;
        }

        uint64_t offset = 0;
        uint64_t entry_addr = 0;
        if (__builtin_mul_overflow(static_cast<uint64_t>(index), static_cast<uint64_t>(sizeof(uint64_t)), &offset) ||
            __builtin_add_overflow(vector_addr, offset, &entry_addr)) {
            return -EFAULT;
        }

        uint64_t string_addr = 0;
        if (!ker::mod::sys::usercopy::copy_value_from_task(task, entry_addr, string_addr)) {
            return -EFAULT;
        }
        snapshot.budget_used += sizeof(uint64_t);
        if (string_addr == 0) {
            break;
        }
        if (!string_addrs.push_back(string_addr)) {
            return -ENOMEM;
        }
    }

    size_t const COUNT = string_addrs.size();
    out = new (std::nothrow) const char*[COUNT + 1];
    if (out == nullptr) {
        return -ENOMEM;
    }

    if (COUNT != 0 && snapshot.strings == nullptr) {
        snapshot.strings = new (std::nothrow) char[EXEC_ARG_BYTES_MAX];
        if (snapshot.strings == nullptr) {
            return -ENOMEM;
        }
    }

    for (size_t index = 0; index < COUNT; ++index) {
        size_t const REMAINING = EXEC_ARG_BYTES_MAX - snapshot.budget_used;
        if (REMAINING == 0) {
            return -E2BIG;
        }

        char* const DEST = snapshot.strings + snapshot.strings_used;
        auto const STATUS = ker::mod::sys::usercopy::copy_cstring_from_task_status(task, string_addrs.at(index), DEST, REMAINING);
        if (STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT) {
            return -EFAULT;
        }
        if (STATUS == ker::mod::sys::usercopy::CStringCopyStatus::TOO_LONG) {
            return -E2BIG;
        }

        size_t const STRING_BYTES = std::strlen(DEST) + 1;
        out[index] = DEST;
        snapshot.strings_used += STRING_BYTES;
        snapshot.budget_used += STRING_BYTES;
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

auto read_file_fully(int fd, uint8_t* dst, size_t size, const char* path) -> ssize_t {
    if (dst == nullptr) {
        return -EINVAL;
    }
    size_t total = 0;
    int consecutive_errors = 0;
    constexpr int MAX_CONSECUTIVE_ERRORS = 3;

    while (total < size) {
        ssize_t const RC = vfs::vfs_pread(fd, dst + total, size - total, static_cast<off_t>(total));
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

    ~PreparedFileBackedElf() {
        delete[] program_headers;
        delete[] section_headers;
        delete[] section_names;
        delete[] task_metadata;
    }

    auto take_task_metadata() -> uint8_t* {
        uint8_t* const RESULT = task_metadata;
        task_metadata = nullptr;
        return RESULT;
    }
};

auto read_exec_file_exact(vfs::File* file, uint64_t logical_size, uint64_t offset, void* destination, size_t size) -> bool {
    if (file == nullptr || destination == nullptr || offset > logical_size || size > logical_size - offset ||
        offset > static_cast<uint64_t>(INT64_MAX)) {
        return false;
    }

    auto* out = static_cast<uint8_t*>(destination);
    size_t total = 0;
    int consecutive_errors = 0;
    constexpr int MAX_CONSECUTIVE_ERRORS = 3;
    while (total < size) {
        uint64_t const READ_OFFSET = offset + total;
        if (READ_OFFSET > static_cast<uint64_t>(INT64_MAX)) {
            return false;
        }
        ssize_t const RC = vfs::vfs_pread_file(file, out + total, size - total, static_cast<off_t>(READ_OFFSET));
        if (RC > 0) {
            total += static_cast<size_t>(RC);
            consecutive_errors = 0;
            continue;
        }
        if (RC == 0) {
            return false;
        }
        if (++consecutive_errors >= MAX_CONSECUTIVE_ERRORS) {
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
    return read_exec_file_exact(source->file, source->logical_size, offset, destination, size);
}

enum class PrepareFileBackedElfResult : uint8_t {
    READY,
    FALLBACK,
    INVALID,
    OUT_OF_MEMORY,
    IO_ERROR,
};

auto prepare_file_backed_elf(vfs::File* file, size_t file_size, PreparedFileBackedElf& out) -> PrepareFileBackedElfResult {
    if (file == nullptr || file_size < sizeof(Elf64_Ehdr) || file_size < EXEC_FILE_BACKED_ELF_MIN_SIZE) {
        return PrepareFileBackedElfResult::FALLBACK;
    }

    Elf64_Ehdr header{};
    if (!read_exec_file_exact(file, file_size, 0, &header, sizeof(header))) {
        return PrepareFileBackedElfResult::IO_ERROR;
    }
    out.bytes_read += sizeof(header);
    if (header.e_ident[EI_MAG0] != ELFMAG0 || header.e_ident[EI_MAG1] != ELFMAG1 || header.e_ident[EI_MAG2] != ELFMAG2 ||
        header.e_ident[EI_MAG3] != ELFMAG3 || header.e_ident[EI_CLASS] != ELFCLASS64 || header.e_phnum == 0 ||
        header.e_phentsize != sizeof(Elf64_Phdr)) {
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
    if (!read_exec_file_exact(file, file_size, header.e_phoff, out.program_headers, static_cast<size_t>(phdr_bytes))) {
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
        return PrepareFileBackedElfResult::FALLBACK;
    }

    if (header.e_shnum != 0) {
        uint64_t shdr_bytes = 0;
        if (header.e_shentsize != sizeof(Elf64_Shdr) || header.e_shstrndx >= header.e_shnum ||
            !exec_table_size(header.e_shnum, header.e_shentsize, shdr_bytes) || shdr_bytes > EXEC_FILE_BACKED_METADATA_MAX ||
            !exec_range_in_file(file_size, header.e_shoff, shdr_bytes)) {
            return PrepareFileBackedElfResult::INVALID;
        }
        out.section_headers = new Elf64_Shdr[header.e_shnum];
        if (out.section_headers == nullptr) {
            return PrepareFileBackedElfResult::OUT_OF_MEMORY;
        }
        if (!read_exec_file_exact(file, file_size, header.e_shoff, out.section_headers, static_cast<size_t>(shdr_bytes))) {
            return PrepareFileBackedElfResult::IO_ERROR;
        }
        out.bytes_read += static_cast<size_t>(shdr_bytes);

        const Elf64_Shdr& shstrtab = out.section_headers[header.e_shstrndx];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (shstrtab.sh_size > EXEC_FILE_BACKED_METADATA_MAX || !exec_range_in_file(file_size, shstrtab.sh_offset, shstrtab.sh_size)) {
            return PrepareFileBackedElfResult::INVALID;
        }
        if (shstrtab.sh_size != 0) {
            out.section_names = new char[static_cast<size_t>(shstrtab.sh_size)];
            if (out.section_names == nullptr) {
                return PrepareFileBackedElfResult::OUT_OF_MEMORY;
            }
            if (!read_exec_file_exact(file, file_size, shstrtab.sh_offset, out.section_names, static_cast<size_t>(shstrtab.sh_size))) {
                return PrepareFileBackedElfResult::IO_ERROR;
            }
            out.bytes_read += static_cast<size_t>(shstrtab.sh_size);
        }
        out.view.section_names_size = shstrtab.sh_size;
    }

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

    out.view.elf_header = header;
    out.view.program_headers = out.program_headers;
    out.view.section_headers = out.section_headers;
    out.view.section_names = out.section_names;
    out.read_context = {.file = file, .logical_size = file_size};
    out.view.read_at = exec_file_read_at;
    out.view.read_context = &out.read_context;
    out.view.logical_size = file_size;
    out.view.contiguous_base = nullptr;
    return PrepareFileBackedElfResult::READY;
}

auto read_file_range_fully(int fd, uint8_t* dst, size_t file_size, uint64_t offset, uint64_t size, const char* path, size_t& bytes_read)
    -> int;

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

auto read_exec_load_segment_for_loader(int fd, uint8_t* dst, size_t file_size, const Elf64_Phdr& ph, const Elf64_Phdr* program_headers,
                                       Elf64_Half ph_count, Elf64_Half ph_index, const char* path, bool allow_lazy_file_pages,
                                       size_t& bytes_read) -> int {
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

void release_lazy_file_refs(LazyVmemRangeVec& ranges) {
    for (const auto& range : ranges) {
        if (range.kind == LazyVmemKind::FILE_BACKED && range.file != nullptr) {
            vfs::vfs_put_file(range.file);
        }
    }
    ranges.clear();
}

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

auto read_file_range_fully(int fd, uint8_t* dst, size_t file_size, uint64_t offset, uint64_t size, const char* path, size_t& bytes_read)
    -> int {
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
        ssize_t const RC = vfs::vfs_pread(fd, out, REMAINING, static_cast<off_t>(READ_OFFSET));
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

auto read_full_exec_image(int fd, uint8_t* dst, size_t file_size, const char* path) -> ExecImageReadResult {
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

auto read_exec_section_if_needed(int fd, uint8_t* dst, size_t file_size, const Elf64_Shdr& section, const char* section_name,
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

auto read_sparse_elf_image(int fd, uint8_t* dst, size_t file_size, const char* path, size_t initial_bytes_read,
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

auto read_exec_image_for_loader(int fd, uint8_t* dst, size_t file_size, const char* path, bool read_static_relocation_metadata = true,
                                bool allow_lazy_file_segments = false) -> ExecImageReadResult {
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
                         const ShebangInfo& shebang, int shebang_depth, ExecFn exec_fn) -> uint64_t {
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
    ker::vfs::File fd0{};
    ker::vfs::File fd1{};
    ker::vfs::File fd255{};

    bool ok = task.fd_table.insert(0, &fd0) && task.fd_table.insert(1, &fd1) && task.fd_table.insert(255, &fd255);
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

    task.fd_table.remove(0);
    task.fd_table.remove(1);
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
    ok = ok && apply_spawn_options(&child, &options);
    close_spawn_cloexec_fds(&child);
    ok = ok && child.fd_table.lookup(SOURCE_FD) == nullptr && child.fd_table.lookup(DESTINATION_FD) == &source &&
         !child.get_fd_cloexec(DESTINATION_FD) && child.pgid == child.pid && source.refcount.load(std::memory_order_relaxed) == 2 &&
         displaced.refcount.load(std::memory_order_relaxed) == 1;

    release_task_fd_table_files(&child);
    ok = ok && source.refcount.load(std::memory_order_relaxed) == 1;
    parent.fd_table.remove(SOURCE_FD);
    parent.fd_table.remove(DESTINATION_FD);
    return ok;
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

auto create_file_backed_process_task(const char* name, vfs::File* owned_file, size_t file_size, const vfs::Stat& file_stat,
                                     uint64_t kernel_rsp) -> mod::sched::task::Task* {
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

    auto* const OWNER = mod::sched::get_current_task();
    if (OWNER == nullptr || !mod::sched::task::claim_unpublished_process(OWNER, new_task)) [[unlikely]] {
        mod::dbg::panic_handler("file-backed exec: cannot publish child recovery ownership");
        hcf();
    }

    loader::elf::ElfLazyLoadRangeVec loader_lazy_ranges;
    loader::elf::ElfLoadOptions const LOAD_OPTIONS{
        .register_special_symbols = true,
        .base_address = 0,
        .lazy_file_ranges = exec_lazy_file_segments_enabled() ? &loader_lazy_ranges : nullptr,
    };
    if (!new_task->initialize_process_image(prepared_elf.view, LOAD_OPTIONS) ||
        !append_exec_lazy_file_ranges(new_task->lazy_vmem_ranges, loader_lazy_ranges, new_task->exec_image_file, file_stat) ||
        !mod::sched::task::complete_unpublished_process_construction(new_task)) {
        if (!mod::sched::task::destroy_owned_unpublished_process(OWNER, new_task)) [[unlikely]] {
            mod::dbg::panic_handler("file-backed exec: lost child recovery ownership during teardown");
            hcf();
        }
        return nullptr;
    }
    return new_task;
}

auto wos_proc_exec(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    auto* parent = ker::mod::sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    ExecArgumentsSnapshot snapshot;
    int const SNAPSHOT_RESULT = snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, snapshot);
    if (SNAPSHOT_RESULT < 0) {
        return static_cast<uint64_t>(SNAPSHOT_RESULT);
    }
    return wos_proc_exec_impl(snapshot.path.data(), snapshot.argv, snapshot.envp, nullptr, 0);
}

auto wos_proc_spawn(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, uint64_t options_addr) -> uint64_t {
    if (ker::mod::power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    auto* parent = ker::mod::sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    ExecArgumentsSnapshot arguments;
    int const ARGUMENT_RESULT = snapshot_exec_arguments(*parent, path_addr, argv_addr, envp_addr, arguments);
    if (ARGUMENT_RESULT < 0) {
        return static_cast<uint64_t>(ARGUMENT_RESULT);
    }
    if (options_addr == 0) {
        return wos_proc_exec_impl(arguments.path.data(), arguments.argv, arguments.envp, nullptr, 0);
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
    uint64_t const RESULT = wos_proc_exec_impl(arguments.path.data(), arguments.argv, arguments.envp, &snapshot->options, 0);
    delete snapshot;
    return RESULT;
}

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp,
                        const ker::abi::process::SpawnOptions* spawn_options, int shebang_depth) -> uint64_t {
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
    uint64_t const PARENT_PID = sched::task::process_pid(*parent_task);

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Loading '%.*s'", static_cast<int>(STR.size()), STR.data());
#endif

    int const FD = vfs::vfs_open(STR, 0, 0);
    if (FD < 0) {
        dbg::log("wos_proc_exec: Failed to open file '%.*s'", static_cast<int>(STR.size()), STR.data());
        return 0;
    }

    vfs::Stat exec_stat{};
    int const STAT_RET = vfs::vfs_fstat(FD, &exec_stat);
    if (STAT_RET < 0) {
        dbg::log("wos_proc_exec: Failed to stat file '%.*s'", static_cast<int>(STR.size()), STR.data());
        vfs::vfs_close(FD);
        return 0;
    }

    int const ACCESS_RET = check_exec_permission_from_stat(parent_task, exec_stat);
    if (ACCESS_RET < 0) {
        dbg::log("wos_proc_exec: Execute permission denied for '%.*s'", static_cast<int>(STR.size()), STR.data());
        vfs::vfs_close(FD);
        return 0;
    }

    ssize_t const FILE_SIZE = exec_stat.st_size;
    if (FILE_SIZE <= 0) {
        dbg::log("wos_proc_exec: Invalid file size: %d", FILE_SIZE);
        vfs::vfs_close(FD);
        return 0;
    }

    vfs::File* exec_file = vfs::vfs_get_file_retain(parent_task, FD);
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
        vfs::vfs_close(FD);
        return 0;
    }

    auto* elf_buffer = FILE_BACKED_ELF ? prepared_elf.take_task_metadata() : new uint8_t[FILE_SIZE];
    size_t const ELF_BUFFER_SIZE = FILE_BACKED_ELF ? prepared_elf.task_metadata_size : static_cast<size_t>(FILE_SIZE);
    bool const ELF_BUFFER_COMPLETE = !FILE_BACKED_ELF;
    if (elf_buffer == nullptr) {
        dbg::log("wos_proc_exec: Failed to allocate buffer");
        release_exec_file_once();
        vfs::vfs_close(FD);
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
                                                : read_exec_image_for_loader(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), path);
    uint32_t const ELF_READ_US = clamp_perf_aux(time::get_us() - ELF_READ_STARTED_US);
    int32_t const ELF_READ_STATUS = READ_RESULT.status;
    record_local_proc_event(parent_task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::END, ELF_READ_CORR, ELF_READ_STATUS,
                            ELF_READ_US, WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::ELF_READ), 0, 0,
                             ELF_READ_STATUS, ELF_READ_US, true, 0,
                             READ_RESULT.bytes_read > 0 ? static_cast<uint64_t>(READ_RESULT.bytes_read) : 0);
    vfs::vfs_close(FD);

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
        return exec_shebang_script(
            path, argv, envp, argv_count, shebang, shebang_depth,
            [spawn_options](const char* interp, const char* const* argv2, const char* const* envp2, int depth) -> uint64_t {
                return wos_proc_exec_impl(interp, argv2, envp2, spawn_options, depth);
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

    auto* new_task =
        new sched::task::Task(process_name, FILE_BACKED_ELF ? 0 : reinterpret_cast<uint64_t>(elf_buffer),
                              FILE_BACKED_ELF ? 0 : static_cast<size_t>(FILE_SIZE), KERNEL_RSP, sched::task::TaskType::PROCESS);

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

    if (new_task->thread == nullptr || new_task->pagemap == nullptr || new_task->entry == 0) {
        dbg::log("wos_proc_exec: Failed to create task (OOM or ELF load failure)");
        cleanup_unpublished_task();
        return 0;
    }
    if (!sched::task::complete_unpublished_process_construction(new_task)) {
        cleanup_unpublished_task();
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task constructor completed successfully");
    dbg::log("wos_proc_exec: Entry point = 0x%x, RIP = 0x%x", new_task->entry, new_task->context.frame.rip);
#endif

    // Inherit process execution context from the parent before applying
    // executable-specific overrides such as setuid/setgid.
    new_task->cwd = parent_task->cwd;
    new_task->cwd_len = parent_task->cwd_len;
    new_task->root = parent_task->root;
    new_task->root_len = parent_task->root_len;
    new_task->uid = parent_task->uid;
    new_task->gid = parent_task->gid;
    new_task->euid = parent_task->euid;
    new_task->egid = parent_task->egid;
    new_task->suid = parent_task->suid;
    new_task->sgid = parent_task->sgid;
    new_task->umask = parent_task->umask;
    if (!new_task->supplementary_groups.clone_from(parent_task->supplementary_groups)) {
        cleanup_unpublished_task();
        return 0;
    }
    new_task->session_id = parent_task->session_id;
    new_task->pgid = (parent_task->pgid != 0) ? parent_task->pgid : PARENT_PID;
    new_task->controlling_tty = parent_task->controlling_tty;
    new_task->signal_mask_store(parent_task->signal_mask_bits(), std::memory_order_relaxed);
    ker::mod::sys::signal::sync_task_signal_mask_cache(new_task);
    new_task->wki_prefer_inline = parent_task->wki_prefer_inline;
    new_task->wki_target_hostname = parent_task->wki_target_hostname;
    new_task->wki_target_flags = parent_task->wki_target_flags;
    new_task->wki_submitter_hostname = parent_task->wki_submitter_hostname;
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

    if (!apply_spawn_options(new_task, spawn_options)) {
        cleanup_unpublished_task();
        return 0;
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

    // Handle setuid/setgid bits from the executable
    {
        vfs::Stat exec_st{};
        if (vfs::vfs_stat(path, &exec_st) == 0) {
            if ((exec_st.st_mode & 04000) != 0U) {  // S_ISUID
                new_task->euid = exec_st.st_uid;
                new_task->suid = exec_st.st_uid;
            }
            if ((exec_st.st_mode & 02000) != 0U) {  // S_ISGID
                new_task->egid = exec_st.st_gid;
                new_task->sgid = exec_st.st_gid;
            }
        }
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
        if (!mod::sched::threading::ensure_stack_backing(new_task->thread, new_task->pagemap, vaddr, END)) {
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
    // auxv: 7 core pairs (including AT_RANDOM and EXECFN), optional
    // AT_BASE pair, and the AT_NULL pair.
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_QWORDS_BASE = 16;
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
        ker::util::SmallVec<uint64_t, 20> auxv;
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

    ker::net::wki::WkiRemoteSpawnSpec const REMOTE_SPAWN = {
        .argv = argv,
        .envp = envp,
        .cwd = parent_task->cwd.data(),
    };
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
            return CHILD_PID;
        }
        return CHILD_PID;
    }
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
        cleanup_unpublished_task();
        return 0;
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
    dbg::log("wos_proc_exec: Successfully posted task '%s' to CPU %d", process_name, new_task->cpu);
#endif

    if (!sched::task::release_unpublished_process(parent_task, new_task)) {
        dbg::log("wos_proc_exec: local publication lost child ownership for PID %x", CHILD_PID);
        // post_task_balanced() already made the child runnable. A failure
        // result here would make mlibc retry via fork+exec and run it twice.
        return CHILD_PID;
    }
    return CHILD_PID;

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

    ExecArgumentsSnapshot snapshot;
    int const SNAPSHOT_RESULT = snapshot_exec_arguments(*task, path_addr, argv_addr, envp_addr, snapshot);
    if (SNAPSHOT_RESULT < 0) {
        return static_cast<uint64_t>(SNAPSHOT_RESULT);
    }
    return wos_proc_execve_impl(snapshot.path.data(), snapshot.argv, snapshot.envp, gpr, 0);
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
    uint32_t const EXEC_CORR = perf::next_wki_trace_correlation();
    uint64_t const EXEC_STARTED_US = time::get_us();
    record_local_proc_event(task, perf::WkiPerfLocalProcOp::EXECVE, perf::WkiPerfPhase::BEGIN, EXEC_CORR, 0,
                            static_cast<uint32_t>(shebang_depth), WOS_PERF_CALLSITE());

    // --- Copy argv/envp strings into kernel memory (before we destroy user mappings) ---
    LocalProcStage const ARG_COPY_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::ARG_COPY, 0, WOS_PERF_CALLSITE());
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

    // Deep-copy strings to kernel heap
    auto** k_argv = new char*[argv_count + 1];
    for (size_t i = 0; i < argv_count; i++) {
        size_t const LEN = std::strlen(argv[i]);
        k_argv[i] = new char[LEN + 1];
        std::memcpy(k_argv[i], argv[i], LEN + 1);
    }
    k_argv[argv_count] = nullptr;

    auto** k_envp = new char*[envp_count + 1];
    for (size_t i = 0; i < envp_count; i++) {
        size_t const LEN = std::strlen(envp[i]);
        k_envp[i] = new char[LEN + 1];
        std::memcpy(k_envp[i], envp[i], LEN + 1);
    }
    k_envp[envp_count] = nullptr;

    auto free_kernel_arg_env = [&]() {
        for (size_t i = 0; i < argv_count; i++) {
            delete[] k_argv[i];
        }
        delete[] k_argv;
        for (size_t i = 0; i < envp_count; i++) {
            delete[] k_envp[i];
        }
        delete[] k_envp;
    };
    bool kernel_arg_env_freed = false;
    auto free_kernel_arg_env_once = [&]() {
        if (!kernel_arg_env_freed) {
            free_kernel_arg_env();
            kernel_arg_env_freed = true;
        }
    };
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::ARG_COPY, ARG_COPY_STAGE, 0, argv_count + envp_count, WOS_PERF_CALLSITE());

    // --- Read the ELF file ---
    LocalProcStage const OPEN_ACCESS_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, 0, WOS_PERF_CALLSITE());
    int const FD = vfs::vfs_open(std::string_view(exec_path, std::strlen(exec_path)), 0, 0);
    if (FD < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: Failed to open '%s' (fd=%d)", exec_path, fd);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -ENOENT, 0, WOS_PERF_CALLSITE());
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOENT);
    }

    vfs::Stat exec_stat{};
    int const STAT_RET = vfs::vfs_fstat(FD, &exec_stat);
    if (STAT_RET < 0) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, STAT_RET, 0, WOS_PERF_CALLSITE());
        vfs::vfs_close(FD);
        free_kernel_arg_env();
        return static_cast<uint64_t>(STAT_RET);
    }

    int const ACCESS_RET = check_exec_permission_from_stat(task, exec_stat);
    if (ACCESS_RET < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: vfs_access X_OK failed for '%s' (ret=%d)", exec_path, access_ret);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -EACCES, 0, WOS_PERF_CALLSITE());
        vfs::vfs_close(FD);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EACCES);
    }

    ssize_t const FILE_SIZE = exec_stat.st_size;
    if (FILE_SIZE <= 0) {
        dbg::log("wos_proc_execve: empty file '%s'", exec_path);
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
        vfs::vfs_close(FD);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    vfs::File* exec_file = vfs::vfs_get_file_retain(task, FD);
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
        int error = -ENOEXEC;
        if (PREPARED_RESULT == PrepareFileBackedElfResult::OUT_OF_MEMORY) {
            error = -ENOMEM;
        } else if (PREPARED_RESULT == PrepareFileBackedElfResult::IO_ERROR) {
            error = -EIO;
        }
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, error, 0, WOS_PERF_CALLSITE());
        release_exec_file_once();
        vfs::vfs_close(FD);
        free_kernel_arg_env();
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
        vfs::vfs_close(FD);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOMEM);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, 0, static_cast<uint64_t>(FILE_SIZE),
                         WOS_PERF_CALLSITE());

    uint32_t const ELF_READ_CORR = perf::next_wki_trace_correlation();
    uint64_t const ELF_READ_STARTED_US = time::get_us();
    record_local_proc_event(task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::BEGIN, ELF_READ_CORR, 0,
                            clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    bool const EXEC_LAZY_FILE_SEGMENTS = exec_file != nullptr && exec_lazy_file_segments_enabled();
    ExecImageReadResult const READ_RESULT =
        FILE_BACKED_ELF
            ? ExecImageReadResult{.bytes_read = static_cast<ssize_t>(prepared_elf.bytes_read),
                                  .status = 0,
                                  .shebang_probe_size = sizeof(Elf64_Ehdr)}
            : read_exec_image_for_loader(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), exec_path, true, EXEC_LAZY_FILE_SEGMENTS);
    uint32_t const ELF_READ_US = clamp_perf_aux(time::get_us() - ELF_READ_STARTED_US);
    int32_t const ELF_READ_STATUS = READ_RESULT.status;
    record_local_proc_event(task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::END, ELF_READ_CORR, ELF_READ_STATUS, ELF_READ_US,
                            WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::ELF_READ), 0, 0,
                             ELF_READ_STATUS, ELF_READ_US, true, 0,
                             READ_RESULT.bytes_read > 0 ? static_cast<uint64_t>(READ_RESULT.bytes_read) : 0);
    vfs::vfs_close(FD);

    if (READ_RESULT.status < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: failed read for '%s' (status %d, got %ld, expect %ld)", exec_path, READ_RESULT.status,
                 READ_RESULT.bytes_read, file_size);
#endif
        release_exec_file_once();
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(READ_RESULT.status);
    }

    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    size_t const SHEBANG_BYTES = READ_RESULT.shebang_probe_size != 0 ? READ_RESULT.shebang_probe_size : static_cast<size_t>(FILE_SIZE);
    if (parse_shebang_line(elf_buffer, SHEBANG_BYTES, &shebang)) {
        release_exec_file_once();
        delete[] elf_buffer;
        free_kernel_arg_env_once();
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return static_cast<uint64_t>(-ELOOP);
        }
        return exec_shebang_script(exec_path, argv, envp, argv_count, shebang, shebang_depth,
                                   [&gpr](const char* interp, const char* const* argv2, const char* const* envp2, int depth) -> uint64_t {
                                       return wos_proc_execve_impl(interp, argv2, envp2, gpr, depth);
                                   });
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
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    {
        LocalProcStage const REMOTE_SPAWN_STAGE =
            begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::REMOTE_SPAWN, 0, WOS_PERF_CALLSITE());
        uint8_t* saved_elf_buffer = task->elf_buffer;
        size_t const SAVED_ELF_BUFFER_SIZE = task->elf_buffer_size;
        bool const SAVED_IS_ELF_BUFFER_SHARED = task->is_elf_buffer_shared;
        bool const SAVED_ELF_BUFFER_COMPLETE = task->elf_buffer_complete;
        bool const SAVED_WKI_SKIP_LEGACY_PLACEMENT = task->wki_skip_legacy_placement;
        uint64_t const SAVED_WKI_REMOTE_PID = task->wki_remote_pid;
        std::array<char, sched::task::Task::EXE_PATH_MAX> saved_exe_path = {};
        std::memcpy(saved_exe_path.data(), task->exe_path.data(), saved_exe_path.size());

        size_t path_len = std::strlen(exec_path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }

        task->elf_buffer = elf_buffer;
        task->elf_buffer_size = ELF_BUFFER_SIZE;
        task->is_elf_buffer_shared = false;
        task->elf_buffer_complete = ELF_BUFFER_COMPLETE;
        std::memcpy(task->exe_path.data(), exec_path, path_len);
        fixed_slot(task->exe_path, path_len) = '\0';

        ker::net::wki::WkiRemoteSpawnSpec const REMOTE_SPAWN = {
            .argv = k_argv,
            .envp = k_envp,
            .cwd = task->cwd.data(),
        };
        auto remote_result = ker::net::wki::wki_try_remote_spawn(task, REMOTE_SPAWN);
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::REMOTE_SPAWN, REMOTE_SPAWN_STAGE, static_cast<int32_t>(remote_result),
                             static_cast<uint64_t>(FILE_SIZE), WOS_PERF_CALLSITE());
        consume_successful_one_shot_wki_target(task, remote_result);

        task->elf_buffer = saved_elf_buffer;
        task->elf_buffer_size = SAVED_ELF_BUFFER_SIZE;
        task->is_elf_buffer_shared = SAVED_IS_ELF_BUFFER_SHARED;
        task->elf_buffer_complete = SAVED_ELF_BUFFER_COMPLETE;

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
            task->deferred_task_switch = true;
            task->yield_switch = false;
            task->set_wait_channel("wki_execve_proxy", ker::mod::sched::task::WaitChannelKind::WKI_EXECVE_PROXY);
            release_exec_file_once();
            free_kernel_arg_env_once();
            return 0;
        }

        std::memcpy(task->exe_path.data(), saved_exe_path.data(), saved_exe_path.size());
        task->wki_skip_legacy_placement = SAVED_WKI_SKIP_LEGACY_PLACEMENT;
        task->wki_remote_pid = SAVED_WKI_REMOTE_PID;

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
            release_exec_file_once();
            delete[] elf_buffer;
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-EHOSTUNREACH);
        }
    }

    // --- Replace the pagemap with a fresh one ---
    // Note: We are executing in kernel context (syscall handler) so our
    // kernel mappings are active. We'll create a new user pagemap.
    LocalProcStage const NEW_IMAGE_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, 0, WOS_PERF_CALLSITE());
    uint8_t* old_elf_buffer = task->elf_buffer;
    vfs::File* old_exec_image_file = task->exec_image_file;
    auto* old_thread = task->thread;
    uint64_t const DEBUG_STAGING_PID = task->pid | EXEC_DEBUG_STAGING_BIT;
    if (DEBUG_STAGING_PID == task->pid) {
        release_exec_file_once();
        delete[] elf_buffer;
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-EOVERFLOW);
    }
    loader::debug::unregister_process(DEBUG_STAGING_PID);
    auto* new_pagemap = mm::virt::create_pagemap();
    if (new_pagemap == nullptr) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        release_exec_file_once();
        delete[] elf_buffer;
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    {
        sched::task::Task pagemap_task{};
        pagemap_task.pagemap = new_pagemap;
        mm::virt::copy_kernel_mappings(&pagemap_task);
    }

    // --- Create new thread (user stack + TLS) ---
    ker::loader::elf::TlsModule tls_info{};
    bool const ELF_CONTRACT_VALID = FILE_BACKED_ELF ? loader::elf::inspect_tls(prepared_elf.view, tls_info)
                                                    : loader::elf::inspect_tls(elf_buffer, static_cast<size_t>(FILE_SIZE), tls_info);
    auto* new_thread = ELF_CONTRACT_VALID ? mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, tls_info.tls_size,
                                                                                 new_pagemap, task->pid, tls_info)
                                          : nullptr;
    uint64_t new_mmap_cursor = 0;
    char* new_name = nullptr;
    LazyVmemRangeVec new_lazy_ranges;
    bool new_lazy_ranges_published = false;
    auto cleanup_new_image = [&]() {
        loader::debug::unregister_process(DEBUG_STAGING_PID);
        release_exec_file_once();
        if (!new_lazy_ranges_published) {
            release_lazy_file_refs(new_lazy_ranges);
        }
        if (new_pagemap != nullptr) {
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(new_pagemap);
            mm::virt::destroy_user_space(new_pagemap, task->pid, new_name != nullptr ? new_name : task->name, "exec-new-image-cleanup");
            mm::virt::release_pagemap(new_pagemap);
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
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOEXEC);
    }
    if (new_thread == nullptr) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (!mod::mm::user_layout::choose_mmap_cursor(new_mmap_cursor)) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -EIO, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-EIO);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, 0, 0, WOS_PERF_CALLSITE());

    // --- Load ELF into new pagemap ---
    LocalProcStage const LOAD_ELF_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF,
                                                                 clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    loader::elf::ElfLazyLoadRangeVec main_loader_lazy_ranges;
    loader::elf::ElfLoadOptions const MAIN_LOAD_OPTIONS{
        .register_special_symbols = true,
        .base_address = 0,
        .lazy_file_ranges = EXEC_LAZY_FILE_SEGMENTS ? &main_loader_lazy_ranges : nullptr,
        .debug_registry_pid = DEBUG_STAGING_PID,
        .image_role = mod::mm::user_layout::ImageRole::MAIN,
    };
    loader::elf::ElfLoadResult elf_result =
        FILE_BACKED_ELF
            ? loader::elf::load_elf(prepared_elf.view, new_pagemap, task->pid, task->name, MAIN_LOAD_OPTIONS)
            : loader::elf::load_elf(elf_buffer, static_cast<size_t>(FILE_SIZE), new_pagemap, task->pid, task->name, MAIN_LOAD_OPTIONS);
    if (elf_result.entry_point == 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF load failed for '%s'", exec_path);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE, -ENOEXEC, static_cast<uint64_t>(FILE_SIZE),
                             WOS_PERF_CALLSITE());
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOEXEC);
    }
    if (!append_exec_lazy_file_ranges(new_lazy_ranges, main_loader_lazy_ranges, exec_file, exec_stat)) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE, -ENOMEM, static_cast<uint64_t>(FILE_SIZE),
                             WOS_PERF_CALLSITE());
        cleanup_new_image();
        free_kernel_arg_env_once();
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
        LocalProcStage const LOAD_INTERP_STAGE =
            begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, 0, WOS_PERF_CALLSITE());

        int interp_open_flags = vfs::O_NOTIFY_CACHE_CHANGE;
        if (task->wki_submitter_hostname.front() == '\0') {
            interp_open_flags |= vfs::O_LOCAL;
        }
        int const INTERP_FD = vfs::vfs_open(std::string_view(INTERP_PATH, std::strlen(INTERP_PATH)), interp_open_flags, 0);
        if (INTERP_FD < 0) {
            dbg::log("wos_proc_execve: Failed to open interpreter '%s'", INTERP_PATH);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        vfs::Stat interp_stat{};
        int const INTERP_STAT_RET = vfs::vfs_fstat(INTERP_FD, &interp_stat);
        ssize_t const INTERP_SIZE = INTERP_STAT_RET == 0 ? interp_stat.st_size : -1;
        if (INTERP_SIZE <= 0) {
            vfs::vfs_close(INTERP_FD);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        vfs::File* interp_file = vfs::vfs_get_file_retain(task, INTERP_FD);
        auto release_interp_file_once = [&]() {
            if (interp_file != nullptr) {
                vfs::vfs_put_file(interp_file);
                interp_file = nullptr;
            }
        };

        auto* interp_buf = new uint8_t[INTERP_SIZE];
        if (interp_buf == nullptr) {
            release_interp_file_once();
            vfs::vfs_close(INTERP_FD);
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOMEM);
        }
        bool const INTERP_LAZY_FILE_SEGMENTS = interp_file != nullptr && exec_lazy_file_segments_enabled();
        ExecImageReadResult const INTERP_READ = read_exec_image_for_loader(INTERP_FD, interp_buf, static_cast<size_t>(INTERP_SIZE),
                                                                           INTERP_PATH, false, INTERP_LAZY_FILE_SEGMENTS);
        vfs::vfs_close(INTERP_FD);

        if (INTERP_READ.status < 0) {
            release_interp_file_once();
            delete[] interp_buf;
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, INTERP_READ.status, 0,
                                 WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(INTERP_READ.status);
        }

        loader::elf::ElfLazyLoadRangeVec interp_loader_lazy_ranges;
        loader::elf::ElfLoadOptions const INTERP_LOAD_OPTIONS{
            .register_special_symbols = false,
            .base_address = 0,
            .lazy_file_ranges = INTERP_LAZY_FILE_SEGMENTS ? &interp_loader_lazy_ranges : nullptr,
            .debug_registry_pid = DEBUG_STAGING_PID,
            .image_role = mod::mm::user_layout::ImageRole::INTERPRETER,
        };
        loader::elf::ElfLoadResult const INTERP_RESULT =
            loader::elf::load_elf(interp_buf, static_cast<size_t>(INTERP_SIZE), new_pagemap, task->pid, "ld.so", INTERP_LOAD_OPTIONS);

        if (INTERP_RESULT.entry_point == 0) {
            release_interp_file_once();
            delete[] interp_buf;
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC,
                                 static_cast<uint64_t>(INTERP_SIZE), WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }
        if (!append_exec_lazy_file_ranges(new_lazy_ranges, interp_loader_lazy_ranges, interp_file, interp_stat)) {
            release_interp_file_once();
            delete[] interp_buf;
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOMEM,
                                 static_cast<uint64_t>(INTERP_SIZE), WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
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

        delete[] interp_buf;
    }

    LocalProcStage const STACK_SETUP_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::STACK_SETUP, 0, WOS_PERF_CALLSITE());

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
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOMEM);
        }
        std::memcpy(new_name, base_name, BASE_LEN + 1);
    }

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
        if (!mod::sched::threading::ensure_stack_backing(new_thread, new_pagemap, vaddr, END)) {
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

    auto* argv_addrs = new uint64_t[argv_count + 1];
    if (argv_addrs == nullptr) {
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    for (size_t i = 0; i < argv_count; i++) {
        argv_addrs[i] = push_string(k_argv[i]);
        if (argv_addrs[i] == 0) {
            delete[] argv_addrs;
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-E2BIG);
        }
    }
    argv_addrs[argv_count] = 0;

    auto* envp_addrs = new uint64_t[envp_count + 1];
    if (envp_addrs == nullptr) {
        delete[] argv_addrs;
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    for (size_t i = 0; i < envp_count; i++) {
        envp_addrs[i] = push_string(k_envp[i]);
        if (envp_addrs[i] == 0) {
            delete[] envp_addrs;
            delete[] argv_addrs;
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-E2BIG);
        }
    }
    envp_addrs[envp_count] = 0;

    // Keep the pathname in the replacement image so AT_EXECFN never points
    // back into the old address space destroyed by a successful execve().
    uint64_t const EXECFN_ADDR = push_string(exec_path);
    if (EXECFN_ADDR == 0) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-E2BIG);
    }

    std::array<uint8_t, AT_RANDOM_BYTES> at_random{};
    if (!mod::random::entropy::get_bytes(at_random.data(), at_random.size())) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-EIO);
    }
    uint64_t const NEW_AT_RANDOM_ADDR = push_to_stack(at_random.data(), at_random.size());
    std::memset(at_random.data(), 0, at_random.size());
    if (NEW_AT_RANDOM_ADDR == 0) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-E2BIG);
    }

    // Free kernel copies of argv/envp strings
    free_kernel_arg_env_once();

    // Alignment
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_BASE_QWORDS = 16;
        size_t const AUXV_QWORDS = AUXV_BASE_QWORDS + (new_interp_base != 0 ? 2 : 0);
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
        ker::util::SmallVec<uint64_t, 20> auxv;
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
        built_correct_auxv &= auxv.push_back(AT_NULL);
        built_correct_auxv &= auxv.push_back(0);

        if (!built_correct_auxv) {
            dbg::log("wos_proc_execve: Failed to build auxv");
            delete[] envp_addrs;
            delete[] argv_addrs;
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
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }
    delete[] envp_addrs;

    uint64_t const ARGV_PTR = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    if (ARGV_PTR == 0) {
        delete[] argv_addrs;
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }
    delete[] argv_addrs;

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

    LocalProcStage const COMMIT_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, 0, WOS_PERF_CALLSITE());
    uint64_t const NEW_RSP = user_stack_virt - current_virt_offset;
    auto* old_thread_to_destroy = old_thread;
    const char* old_name_to_destroy = nullptr;
    LazyVmemRangeVec old_lazy_ranges;

    // Publish the pagemap, all mapping metadata, and the execution context as
    // one shared-VM transaction. Procfs, ptrace, mmap, fork, and thread
    // creation take the same guard and therefore cannot observe a mixed image.
    // No old-image storage is released until after the guard is dropped.
    mm::paging::PageTable* old_pagemap_to_destroy = nullptr;
    bool old_pagemap_has_other_publishers = false;
    bool commit_published = false;
    {
        // Prevent a new CLONE_VM/thread publisher from appearing between the
        // sibling snapshot and the root swap. Existing siblings keep the old
        // address space alive; exec must never tear a shared root out from
        // under their execution or stable usercopy pins.
        ker::syscall::vmem::SharedVmemPublicationGuard publication_guard;
        old_pagemap_has_other_publishers = mod::sched::task_has_live_pagemap_sibling(task);
        commit_published = loader::debug::publish_staged_process(DEBUG_STAGING_PID, task->pid, new_name);
        if (commit_published) {
            task->entry = NEW_EXEC_ENTRY;
            task->program_header_addr = NEW_PROGRAM_HEADER_ADDR;
            task->elf_header_addr = NEW_ELF_HEADER_ADDR;
            task->image_load_base = elf_result.load_base;
            task->image_vaddr_start = NEW_IMAGE_VADDR_START;
            task->image_vaddr_end = NEW_IMAGE_VADDR_END;
            task->program_header_count = NEW_PROGRAM_HEADER_COUNT;
            task->program_header_ent_size = NEW_PROGRAM_HEADER_ENT_SIZE;
            task->elf_buffer = elf_buffer;
            task->elf_buffer_size = ELF_BUFFER_SIZE;
            task->is_elf_buffer_shared = false;
            task->elf_buffer_complete = ELF_BUFFER_COMPLETE;
            task->exec_image_file = FILE_BACKED_ELF ? exec_file : nullptr;
            task->exec_image_size = FILE_BACKED_ELF ? static_cast<uint64_t>(FILE_SIZE) : 0;
            task->interp_base = new_interp_base;
            task->interp_vaddr_start = new_interp_vaddr_start;
            task->interp_vaddr_end = new_interp_vaddr_end;
            task->at_random_addr = NEW_AT_RANDOM_ADDR;
            task->mmap_next.store(new_mmap_cursor, std::memory_order_relaxed);

            size_t path_len = std::strlen(exec_path);
            if (path_len >= sched::task::Task::EXE_PATH_MAX) {
                path_len = sched::task::Task::EXE_PATH_MAX - 1;
            }
            task->exe_path.fill('\0');
            std::memcpy(task->exe_path.data(), exec_path, path_len);

            old_name_to_destroy = task->name;
            task->name = new_name;
            new_name = nullptr;

            if ((exec_stat.st_mode & 04000) != 0U) {
                task->euid = exec_stat.st_uid;
                task->suid = exec_stat.st_uid;
            }
            if ((exec_stat.st_mode & 02000) != 0U) {
                task->egid = exec_stat.st_gid;
                task->sgid = exec_stat.st_gid;
            }

            task->signal_pending_store(0, std::memory_order_relaxed);
            task->in_signal_handler = false;
            task->do_sigreturn = false;
            for (auto& sh : task->sig_handlers) {
                sh = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};
            }

            task->context.frame.rip = new_initial_rip;
            task->context.frame.rsp = NEW_RSP;
            task->context.frame.ss = 0x1b;
            task->context.frame.cs = 0x23;
            task->context.frame.flags = 0x202;
            task->context.frame.int_num = 0;
            task->context.frame.err_code = 0;
            ker::mod::sys::context_switch::record_saved_frame_class(task, task->context.frame,
                                                                    ker::mod::sched::task::SavedFrameOrigin::SYNTHETIC_USER_RETURN);

            // Match the fresh-process entry contract used by wos_asm_enter_usermode:
            // startup code consumes argc/argv/envp from the initial stack, not GPRs.
            task->context.regs = cpu::GPRegs();
            task->context.regs.rdi = new_initial_rip;
            task->context.regs.rsi = NEW_RSP;

            old_pagemap_to_destroy = task->replace_pagemap_after_usercopy_quiescence(new_pagemap);
            task->thread = new_thread;
            swap_exec_lazy_ranges(task, new_lazy_ranges, old_lazy_ranges);
            new_lazy_ranges_published = true;
        }
    }
    if (!commit_published) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, -EIO, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        return static_cast<uint64_t>(-EIO);
    }

    elf_buffer = nullptr;
    if (FILE_BACKED_ELF) {
        exec_file = nullptr;
    }
    delete[] old_name_to_destroy;
    release_lazy_file_refs(old_lazy_ranges);

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

    if (old_elf_buffer != nullptr) {
        if (!ker::net::wki::wki_remote_compute_release_elf_buffer(old_elf_buffer)) {
            delete[] old_elf_buffer;
        }
    }
    if (old_exec_image_file != nullptr) {
        vfs::vfs_put_file(old_exec_image_file);
    }

    (void)argc;
    (void)ARGV_PTR;
    (void)ENVP_PTR;

    auto phys_pagemap = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(new_pagemap)));
    asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");
    ker::mod::sys::context_switch::reset_fpu_state(task);

    // execve() returns directly via sysret instead of re-entering through the
    // scheduler, so refresh the live CPU's user TLS bases after the new CR3 is
    // active and before any new-image TLS access in userspace.
    cpu::wrfsbase(new_thread->fsbase);
    cpu_set_msr(IA32_KERNEL_GS_BASE, new_thread->gsbase);
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, 0, 0, WOS_PERF_CALLSITE());

    // execve() replaces the current image in-place. Reclaim an exclusive old
    // address space now; if CLONE_VM/thread publishers still exist, their
    // last-publisher GC owns that shared root instead.
    LocalProcStage const DESTROY_OLD_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::DESTROY_OLD, 0, WOS_PERF_CALLSITE());
    ker::syscall::shm::shm_cleanup_for_task(task);
    if (old_pagemap_to_destroy != nullptr && old_pagemap_to_destroy != new_pagemap && !old_pagemap_has_other_publishers) {
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(old_pagemap_to_destroy);
        mm::virt::destroy_user_space(old_pagemap_to_destroy, task->pid, task->name, "exec-old-image");
        mm::virt::release_pagemap(old_pagemap_to_destroy);
    }
    if (old_thread_to_destroy != nullptr && old_thread_to_destroy != new_thread) {
        old_thread_to_destroy->tls_phys_ptr = 0;
        old_thread_to_destroy->stack_phys_ptr = 0;
        mod::sched::threading::destroy_thread(old_thread_to_destroy);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::DESTROY_OLD, DESTROY_OLD_STAGE, 0, 0, WOS_PERF_CALLSITE());

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
    dbg::log("wos_proc_execve: PID %x now running '%s' (entry 0x%lx, rsp 0x%lx)", task->pid, task->exe_path.data(), elf_result.entryPoint,
             new_rsp);
#endif
    uint32_t const EXEC_US = clamp_perf_aux(time::get_us() - EXEC_STARTED_US);
    record_local_proc_event(task, perf::WkiPerfLocalProcOp::EXECVE, perf::WkiPerfPhase::END, EXEC_CORR, 0, EXEC_US, WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::EXECVE), 0, 0, 0, EXEC_US, true,
                             0, static_cast<uint64_t>(FILE_SIZE));

    // === CRITICAL SECTION: No function calls below this point! ===
    // Any function call (including dbg::log) would use the kernel stack
    // and overwrite the patched register values.

    // 2. Patch the full on-stack GPRegs block so the new image gets the same
    // clean register state as a freshly spawned task instead of inheriting the
    // old syscall call-site's scratch values.
    *reinterpret_cast<cpu::GPRegs*>(stack_base) = task->context.regs;
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

}  // namespace ker::syscall::process
