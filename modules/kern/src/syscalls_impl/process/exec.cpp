#include "exec.hpp"

#include <abi/callnums/process.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>
#include <extern/elf.h>

// #define EXEC_DEBUG

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <net/wki/remote_compute.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/signal.hpp>
#include <string_view>
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
constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGSTOP = 19;
using exec_log = ker::mod::dbg::logger<"exec">;
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

auto clone_exec_fd_table_checked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return false;
    }

    bool ok = true;
    uint64_t const IRQF = parent->fd_table_lock.lock_irqsave();
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (!ok || val == nullptr) {
            return;
        }
        if (parent->get_fd_cloexec(static_cast<unsigned>(key))) {
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

    constexpr uint64_t MAX_SPAWN_ACTIONS = 32;
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
        task->sig_mask = options->sig_mask & ~UNBLOCKABLE;
        ker::mod::sys::signal::sync_task_signal_mask_cache(task);
    }

    // SPAWN_FLAG_SETPGROUP is accepted as a no-op to match the current mlibc
    // posix_spawn child path, which logs and ignores POSIX_SPAWN_SETPGROUP.
    return true;
}

auto ensure_exec_stdio_fallbacks(ker::mod::sched::task::Task* task) -> bool {
    if (task == nullptr) {
        return false;
    }

    for (unsigned fd = 0; fd < 3; ++fd) {
        {
            uint64_t const IRQF = task->fd_table_lock.lock_irqsave();
            bool const PRESENT = task->fd_table.lookup(fd) != nullptr;
            task->fd_table_lock.unlock_irqrestore(IRQF);
            if (PRESENT) {
                continue;
            }
        }

        vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
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
    auto stack_base = reinterpret_cast<uint64_t>(ker::mod::mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
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

void publish_exec_lazy_ranges(ker::mod::sched::task::Task* task, LazyVmemRangeVec& new_ranges) {
    if (task == nullptr) {
        release_lazy_file_refs(new_ranges);
        return;
    }

    LazyVmemRangeVec old_ranges;
    uint64_t const IRQF = task->lazy_vmem_lock.lock_irqsave();
    old_ranges = std::move(task->lazy_vmem_ranges);
    task->lazy_vmem_ranges = std::move(new_ranges);
    task->lazy_vmem_lock.unlock_irqrestore(IRQF);
    release_lazy_file_refs(old_ranges);
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

    ExecImageReadResult result =
        read_sparse_elf_image(fd, dst, file_size, path, bytes_read, read_static_relocation_metadata, allow_lazy_file_segments);
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
#endif

auto wos_proc_exec(const char* path, const char* const* argv, const char* const* envp) -> uint64_t {
    return wos_proc_exec_impl(path, argv, envp, nullptr, 0);
}

auto wos_proc_spawn(const char* path, const char* const* argv, const char* const* envp, const ker::abi::process::SpawnOptions* options)
    -> uint64_t {
    return wos_proc_exec_impl(path, argv, envp, options, 0);
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

    auto* elf_buffer = new uint8_t[FILE_SIZE];
    if (elf_buffer == nullptr) {
        dbg::log("wos_proc_exec: Failed to allocate buffer");
        vfs::vfs_close(FD);
        return 0;
    }

    uint32_t const ELF_READ_CORR = perf::next_wki_trace_correlation();
    uint64_t const ELF_READ_STARTED_US = time::get_us();
    record_local_proc_event(parent_task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::BEGIN, ELF_READ_CORR, 0,
                            clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    ExecImageReadResult const READ_RESULT = read_exec_image_for_loader(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), path);
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
        delete[] elf_buffer;
        return 0;
    }

    // Add memory barrier after reading to ensure visibility
    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    size_t const SHEBANG_BYTES = READ_RESULT.shebang_probe_size != 0 ? READ_RESULT.shebang_probe_size : static_cast<size_t>(FILE_SIZE);
    if (parse_shebang_line(elf_buffer, SHEBANG_BYTES, &shebang)) {
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
        delete[] elf_buffer;
        return 0;
    }

    if (elf_header->e_ident[EI_CLASS] != ELFCLASS64) {
        dbg::log("wos_proc_exec: Not a 64-bit ELF");
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

    uint64_t const KERNEL_RSP = allocate_kernel_stack();
    if (KERNEL_RSP == 0) {
        dbg::log("wos_proc_exec: Failed to allocate kernel stack");
        delete[] elf_buffer;
        return 0;
    }

    // DIAGNOSTIC: Detect stack corruption during Task constructor
    // Save known canary values on stack, check after constructor
    volatile uint64_t canary1 = 0xDEAD'BEEF'CAFE'BABEULL;  // NOLINT
    volatile uint64_t canary2 = 0x1234'5678'9ABC'DEF0ULL;  // NOLINT

    auto* new_task =
        new sched::task::Task(process_name, reinterpret_cast<uint64_t>(elf_buffer), KERNEL_RSP, sched::task::TaskType::PROCESS);

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
        delete[] elf_buffer;
        return 0;
    }

    if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
        dbg::log("wos_proc_exec: Failed to create task (OOM during thread/pagemap allocation)");

        ker::mod::mm::phys::page_free(reinterpret_cast<void*>(KERNEL_RSP - ker::mod::mm::KERNEL_STACK_SIZE));
        if (new_task != nullptr) {
            new_task->context.syscall_kernel_stack = 0;
        }
        delete new_task;

        delete[] elf_buffer;
        return 0;
    }
    auto cleanup_unpublished_task = [&]() {
        release_task_fd_table_files(new_task);
        delete new_task;
        delete[] elf_buffer;
    };

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task constructor completed successfully");
    dbg::log("wos_proc_exec: Entry point = 0x%x, RIP = 0x%x", new_task->entry, new_task->context.frame.rip);
#endif

    new_task->parent_pid = PARENT_PID;

    // Inherit process execution context from the parent before applying
    // executable-specific overrides such as setuid/setgid.
    new_task->cwd = parent_task->cwd;
    new_task->root = parent_task->root;
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
    new_task->sig_mask = parent_task->sig_mask;
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

    // Inherit file descriptors from parent, respecting FD_CLOEXEC (per-fd bitmap).
    if (!clone_exec_fd_table_checked(parent_task, new_task)) {
        cleanup_unpublished_task();
        return 0;
    }

    if (!apply_spawn_options(new_task, spawn_options)) {
        cleanup_unpublished_task();
        return 0;
    }

    // Ensure fds 0/1/2 are always set (open /dev/console if not inherited)
    if (!ensure_exec_stdio_fallbacks(new_task)) {
        cleanup_unpublished_task();
        return 0;
    }

    new_task->elf_buffer = elf_buffer;
    new_task->elf_buffer_size = FILE_SIZE;
    new_task->is_elf_buffer_shared = false;

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
    dbg::log("wos_proc_exec: Task created with PID: %x, parent: %x", new_task->pid, new_task->parent_pid);
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

    // Align to 16 bytes after string data, accounting for structured data parity.
    // Structured data: auxv (variable) + envp array + argv array + argc.
    // auxv: 4 core pairs (8 qwords) + optional AT_BASE pair (2 qwords) + AT_NULL pair (2 qwords)
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_QWORDS_BASE = 12;  // 5 core pairs (PAGESZ,ENTRY,PHDR,PHENT,PHNUM) + AT_NULL pair
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

        // Build auxv dynamically: always include core entries, conditionally add AT_BASE
        bool built_correct_auxv = true;
        ker::util::SmallVec<uint64_t, 16> auxv;
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
    auto remote_result = ker::net::wki::wki_try_remote_spawn(new_task, REMOTE_SPAWN);
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
        return new_task->pid;
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

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Successfully posted task '%s' to CPU %d", process_name, new_task->cpu);
#endif

    return new_task->pid;

    // Note: elfBuffer is now owned by the task and will be cleaned up when the task exits
}
}  // namespace

auto wos_proc_execve(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    return wos_proc_execve_impl(path, argv, envp, gpr, 0);
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

    auto* elf_buffer = new uint8_t[FILE_SIZE];
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
    ExecImageReadResult const READ_RESULT =
        read_exec_image_for_loader(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), exec_path, true, exec_file != nullptr);
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
        bool const SAVED_WKI_SKIP_LEGACY_PLACEMENT = task->wki_skip_legacy_placement;
        uint64_t const SAVED_WKI_REMOTE_PID = task->wki_remote_pid;
        std::array<char, sched::task::Task::EXE_PATH_MAX> saved_exe_path = {};
        std::memcpy(saved_exe_path.data(), task->exe_path.data(), saved_exe_path.size());

        size_t path_len = std::strlen(exec_path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }

        task->elf_buffer = elf_buffer;
        task->elf_buffer_size = static_cast<size_t>(FILE_SIZE);
        task->is_elf_buffer_shared = false;
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

        task->elf_buffer = saved_elf_buffer;
        task->elf_buffer_size = SAVED_ELF_BUFFER_SIZE;
        task->is_elf_buffer_shared = SAVED_IS_ELF_BUFFER_SHARED;

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
    auto* old_pagemap = task->pagemap;
    auto* old_thread = task->thread;
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
    ker::loader::elf::TlsModule const TLS_INFO = loader::elf::extract_tls_info(static_cast<void*>(elf_buffer));
    auto* new_thread =
        mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, TLS_INFO.tls_size, new_pagemap, task->pid, TLS_INFO);
    char* new_name = nullptr;
    LazyVmemRangeVec new_lazy_ranges;
    bool new_lazy_ranges_published = false;
    auto cleanup_new_image = [&]() {
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
    if (new_thread == nullptr) {
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::NEW_IMAGE, NEW_IMAGE_STAGE, 0, 0, WOS_PERF_CALLSITE());

    // execve() reuses the same PID. The loader debug registry is keyed by PID,
    // so we must discard the old image's symbol metadata before registering the
    // new ELF or lookups like __safestack_unsafe_stack_ptr can resolve against
    // stale offsets from the previous process image.
    loader::debug::unregister_process(task->pid);

    // --- Load ELF into new pagemap ---
    LocalProcStage const LOAD_ELF_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF,
                                                                 clamp_perf_aux(static_cast<uint64_t>(FILE_SIZE)), WOS_PERF_CALLSITE());
    loader::elf::ElfLazyLoadRangeVec main_loader_lazy_ranges;
    loader::elf::ElfLoadOptions const MAIN_LOAD_OPTIONS{
        .register_special_symbols = true,
        .base_address = 0,
        .lazy_file_ranges = exec_file != nullptr ? &main_loader_lazy_ranges : nullptr,
    };
    loader::elf::ElfLoadResult elf_result =
        loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(elf_buffer), new_pagemap, task->pid, task->name, MAIN_LOAD_OPTIONS);
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
    release_exec_file_once();
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_ELF, LOAD_ELF_STAGE, 0, static_cast<uint64_t>(FILE_SIZE),
                         WOS_PERF_CALLSITE());

    uint64_t const NEW_EXEC_ENTRY = elf_result.entry_point;
    uint64_t new_initial_rip = elf_result.entry_point;
    uint64_t const NEW_PROGRAM_HEADER_ADDR = elf_result.program_header_addr;
    uint64_t const NEW_ELF_HEADER_ADDR = elf_result.elf_header_addr;
    uint16_t const NEW_PROGRAM_HEADER_COUNT = elf_result.program_header_count;
    uint16_t const NEW_PROGRAM_HEADER_ENT_SIZE = elf_result.program_header_ent_size;
    uint64_t new_interp_base = 0;

    // If the binary requests a dynamic linker (PT_INTERP), load it.
    if (elf_result.has_interp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;
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
        ExecImageReadResult const INTERP_READ =
            read_exec_image_for_loader(INTERP_FD, interp_buf, static_cast<size_t>(INTERP_SIZE), INTERP_PATH, false, interp_file != nullptr);
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
            .base_address = INTERP_BASE,
            .lazy_file_ranges = interp_file != nullptr ? &interp_loader_lazy_ranges : nullptr,
        };
        loader::elf::ElfLoadResult const INTERP_RESULT = loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(interp_buf),
                                                                               new_pagemap, task->pid, "ld.so", INTERP_LOAD_OPTIONS);

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
        new_interp_base = INTERP_BASE;

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

    // Free kernel copies of argv/envp strings
    free_kernel_arg_env_once();

    // Alignment
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t const CURRENT_ADDR = user_stack_virt - current_virt_offset;
        uint64_t const ALIGNED = CURRENT_ADDR & ~(ALIGNMENT - 1);
        current_virt_offset += (CURRENT_ADDR - ALIGNED);

        constexpr size_t AUXV_BASE_QWORDS = 12;  // 5 core pairs (PAGESZ,ENTRY,PHDR,PHENT,PHNUM) + AT_NULL pair
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
        bool built_correct_auxv = true;
        ker::util::SmallVec<uint64_t, 16> auxv;
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

    LocalProcStage const COMMIT_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, 0, WOS_PERF_CALLSITE());

    // exec only closes FD_CLOEXEC descriptors after the new image is ready;
    // failed execve() must leave the original process image intact.
    // Snapshot descriptors first because vfs_close() mutates fd_table.
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

    if (old_elf_buffer != nullptr) {
        if (!ker::net::wki::wki_remote_compute_release_elf_buffer(old_elf_buffer)) {
            delete[] old_elf_buffer;
        }
    }

    task->entry = NEW_EXEC_ENTRY;
    task->program_header_addr = NEW_PROGRAM_HEADER_ADDR;
    task->elf_header_addr = NEW_ELF_HEADER_ADDR;
    task->program_header_count = NEW_PROGRAM_HEADER_COUNT;
    task->program_header_ent_size = NEW_PROGRAM_HEADER_ENT_SIZE;
    task->elf_buffer = elf_buffer;
    task->elf_buffer_size = FILE_SIZE;
    task->interp_base = new_interp_base;
    task->mmap_next.store(0, std::memory_order_relaxed);

    {
        size_t path_len = std::strlen(exec_path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        std::memcpy(task->exe_path.data(), exec_path, path_len);
        fixed_slot(task->exe_path, path_len) = '\0';
    }

    delete[] task->name;
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

    task->sig_pending = 0;
    task->in_signal_handler = false;
    task->do_sigreturn = false;
    for (auto& sh : task->sig_handlers) {
        sh = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};
    }

    static_cast<void>(ensure_exec_stdio_fallbacks(task));

    // --- Set up the task context to jump to the new binary ---
    uint64_t const NEW_RSP = user_stack_virt - current_virt_offset;
    task->context.frame.rip = new_initial_rip;
    task->context.frame.rsp = NEW_RSP;
    task->context.frame.ss = 0x1b;
    task->context.frame.cs = 0x23;
    task->context.frame.flags = 0x202;
    task->context.frame.int_num = 0;
    task->context.frame.err_code = 0;

    // Match the fresh-process entry contract used by wos_asm_enter_usermode:
    // startup code consumes argc/argv/envp from the initial stack, not GPRs.
    task->context.regs = cpu::GPRegs();
    task->context.regs.rdi = new_initial_rip;
    task->context.regs.rsi = NEW_RSP;
    (void)argc;
    (void)ARGV_PTR;
    (void)ENVP_PTR;

    // Freshly spawned processes rewrite fs:[0] just before the first usermode
    // entry. execve() bypasses that path, so repair the initial TCB self-pointer
    // here before any TLS access in ld.so / libc.
    if (new_thread != nullptr) {
        uint64_t const TCB_PADDR = mm::virt::translate(new_pagemap, new_thread->fsbase);
        if (TCB_PADDR != mm::virt::PADDR_INVALID) {
            auto* tcb_self = reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(TCB_PADDR));
            *tcb_self = new_thread->fsbase;
        }
    }

    // Initialize SafeStack TLS symbol if present
    auto* ssym = loader::debug::get_process_symbol(task->pid, "__safestack_unsafe_stack_ptr");
    if (new_thread != nullptr && (ssym != nullptr) && ssym->is_tls_offset) {
        uint64_t const DEST_VADDR = new_thread->tls_base_virt + ssym->raw_value;
        uint64_t const DEST_PADDR = mm::virt::translate(new_pagemap, DEST_VADDR);
        if (DEST_PADDR != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = static_cast<uint64_t*>(mm::addr::get_virt_pointer(DEST_PADDR));
            *dest_ptr = new_thread->safestack_ptr_value;
        }
    }

    // execve() returns directly via sysret instead of re-entering through the
    // scheduler, so we must refresh the live CPU's user TLS bases here.
    // Otherwise the CPU would keep the old image's FS_BASE / user GS_BASE and
    // immediately fault in the new process when libc touches TLS.
    if (new_thread != nullptr) {
        cpu::wrfsbase(new_thread->fsbase);
        cpu_set_msr(IA32_KERNEL_GS_BASE, new_thread->gsbase);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::COMMIT, COMMIT_STAGE, 0, 0, WOS_PERF_CALLSITE());

    auto* old_pagemap_to_destroy = old_pagemap;
    auto* old_thread_to_destroy = old_thread;
    old_pagemap = nullptr;
    old_thread = nullptr;

    // Publish the new execution context before old-image teardown. The release
    // paths below may block or yield, so scheduler/procfs observers must never
    // see task->pagemap/task->thread pointing at storage that is being freed.
    task->pagemap = new_pagemap;
    task->thread = new_thread;
    publish_exec_lazy_ranges(task, new_lazy_ranges);
    new_lazy_ranges_published = true;

    auto phys_pagemap = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(new_pagemap)));
    asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");

    // execve() replaces the current image in-place, so the old address space
    // and thread backing storage must be reclaimed now rather than deferred to
    // task GC. Otherwise each successful exec leaks another user stack/TLS set
    // plus the old pagemap's user pages.
    LocalProcStage const DESTROY_OLD_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::DESTROY_OLD, 0, WOS_PERF_CALLSITE());
    ker::syscall::shm::shm_cleanup_for_task(task);
    if (old_pagemap_to_destroy != nullptr && old_pagemap_to_destroy != new_pagemap) {
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
