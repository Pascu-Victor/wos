#include "exec.hpp"

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
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <util/smallvec.hpp>
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
#include "vfs/stat.hpp"
namespace ker::syscall::process {

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp, int shebang_depth) -> uint64_t;
auto wos_proc_execve_impl(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr, int shebang_depth)
    -> uint64_t;

constexpr int MAX_SHEBANG_DEPTH = 4;
constexpr size_t EXEC_PATH_MAX = 512;
using exec_log = ker::mod::dbg::logger<"exec">;

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

auto wos_proc_exec(const char* path, const char* const* argv, const char* const* envp) -> uint64_t {
    return wos_proc_exec_impl(path, argv, envp, 0);
}

namespace {
auto wos_proc_exec_impl(const char* path, const char* const* argv, const char* const* envp, int shebang_depth) -> uint64_t {
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
    uint64_t const PARENT_PID = parent_task->pid;

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
    ssize_t const BYTES_READ = read_file_fully(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), path);
    uint32_t const ELF_READ_US = clamp_perf_aux(time::get_us() - ELF_READ_STARTED_US);
    int32_t const ELF_READ_STATUS = BYTES_READ == FILE_SIZE ? 0 : -EIO;
    record_local_proc_event(parent_task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::END, ELF_READ_CORR, ELF_READ_STATUS,
                            ELF_READ_US, WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::ELF_READ), 0, 0,
                             ELF_READ_STATUS, ELF_READ_US, true, 0, BYTES_READ > 0 ? static_cast<uint64_t>(BYTES_READ) : 0);
    vfs::vfs_close(FD);

    if (BYTES_READ != FILE_SIZE) {
        dbg::log("wos_proc_exec: Failed to read file completely");
        delete[] elf_buffer;
        return 0;
    }

    // Add memory barrier after reading to ensure visibility
    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    if (parse_shebang_line(elf_buffer, static_cast<size_t>(FILE_SIZE), &shebang)) {
        delete[] elf_buffer;
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return 0;
        }
        return exec_shebang_script(path, argv, envp, argv_count, shebang, shebang_depth,
                                   [](const char* interp, const char* const* argv2, const char* const* envp2, int depth) -> uint64_t {
                                       return wos_proc_exec_impl(interp, argv2, envp2, depth);
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

        delete new_task;

        // TODO: Free kernel stack pages
        delete[] elf_buffer;
        return 0;
    }

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
    [[maybe_unused]] bool const CLONED_GROUPS = new_task->supplementary_groups.clone_from(parent_task->supplementary_groups);
    new_task->session_id = parent_task->session_id;
    new_task->pgid = (parent_task->pgid != 0) ? parent_task->pgid : parent_task->pid;
    new_task->controlling_tty = parent_task->controlling_tty;
    new_task->wki_prefer_inline = parent_task->wki_prefer_inline;
    new_task->wki_target_hostname = parent_task->wki_target_hostname;
    new_task->wki_target_flags = parent_task->wki_target_flags;
    new_task->wki_submitter_hostname = parent_task->wki_submitter_hostname;
    new_task->wki_remote_pid = (new_task->wki_submitter_hostname.front() != '\0' &&
                                std::strcmp(new_task->wki_submitter_hostname.data(), local_wki_hostname()) != 0)
                                   ? new_task->pid
                                   : 0;
    [[maybe_unused]] bool const CLONED_RULES = new_task->wki_vfs_rules.clone_from(parent_task->wki_vfs_rules);

    // Inherit file descriptors from parent, respecting FD_CLOEXEC (per-fd bitmap).
    // FDs with FD_CLOEXEC set are NOT inherited (closed on exec).
    // FDs without FD_CLOEXEC are inherited by incrementing refcount.
    // For fds 0/1/2 (stdin/stdout/stderr), if not inherited, re-open /dev/console.
    parent_task->fd_table.for_each([&](uint64_t key, void* val) {
        if (val == nullptr) {
            return;
        }
        auto* parent_file = static_cast<vfs::File*>(val);
        if (parent_task->get_fd_cloexec(static_cast<unsigned>(key))) {
            return;  // FD_CLOEXEC is set - do NOT inherit
        }
        parent_file->refcount.fetch_add(1, std::memory_order_acq_rel);
        [[maybe_unused]] bool const INSERTED = new_task->fd_table.insert(key, parent_file);
    });

    // Ensure fds 0/1/2 are always set (open /dev/console if not inherited)
    for (unsigned i = 0; i < 3; ++i) {
        if (new_task->fd_table.lookup(i) == nullptr) {
            vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (new_file != nullptr) {
                new_file->fops = vfs::devfs::get_devfs_fops();
                new_file->fd = static_cast<int>(i);
                new_file->refcount = 1;
                [[maybe_unused]] bool const INSERTED = new_task->fd_table.insert(i, new_file);
            }
        }
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
            delete new_task;
            delete[] elf_buffer;
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
            delete new_task;
            delete[] elf_buffer;
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
            delete new_task;
            delete[] elf_buffer;
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
        delete new_task;
        delete[] elf_buffer;
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
        delete new_task;
        delete[] elf_buffer;
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

    auto* elf_buffer = new uint8_t[FILE_SIZE];
    if (elf_buffer == nullptr) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: alloc failed for '%s' (%ld bytes)", exec_path, file_size);
#endif
        end_local_proc_stage(task, perf::WkiPerfLocalProcOp::OPEN_ACCESS, OPEN_ACCESS_STAGE, -ENOMEM, 0, WOS_PERF_CALLSITE());
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
    ssize_t const BYTES_READ = read_file_fully(FD, elf_buffer, static_cast<size_t>(FILE_SIZE), exec_path);
    uint32_t const ELF_READ_US = clamp_perf_aux(time::get_us() - ELF_READ_STARTED_US);
    int32_t const ELF_READ_STATUS = BYTES_READ == FILE_SIZE ? 0 : -EIO;
    record_local_proc_event(task, perf::WkiPerfLocalProcOp::ELF_READ, perf::WkiPerfPhase::END, ELF_READ_CORR, ELF_READ_STATUS, ELF_READ_US,
                            WOS_PERF_CALLSITE());
    perf::record_wki_summary(perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(perf::WkiPerfLocalProcOp::ELF_READ), 0, 0,
                             ELF_READ_STATUS, ELF_READ_US, true, 0, BYTES_READ > 0 ? static_cast<uint64_t>(BYTES_READ) : 0);
    vfs::vfs_close(FD);

    if (BYTES_READ != FILE_SIZE) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: short read for '%s' (got %ld, expect %ld)", exec_path, BYTES_READ, file_size);
#endif
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EIO);
    }

    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    if (parse_shebang_line(elf_buffer, static_cast<size_t>(FILE_SIZE), &shebang)) {
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
            task->wait_channel = "wki_execve_proxy";
            free_kernel_arg_env_once();
            return 0;
        }

        std::memcpy(task->exe_path.data(), saved_exe_path.data(), saved_exe_path.size());
        task->wki_skip_legacy_placement = SAVED_WKI_SKIP_LEGACY_PLACEMENT;
        task->wki_remote_pid = SAVED_WKI_REMOTE_PID;

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
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
    auto* new_thread = mod::sched::threading::create_thread(ker::mod::mm::USER_STACK_SIZE, TLS_INFO.tls_size, new_pagemap, TLS_INFO);
    char* new_name = nullptr;
    auto cleanup_new_image = [&]() {
        if (new_pagemap != nullptr) {
            mm::virt::destroy_user_space(new_pagemap, task->pid, new_name != nullptr ? new_name : task->name, "exec-new-image-cleanup");
            mm::phys::page_free(new_pagemap);
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
    loader::elf::ElfLoadResult elf_result =
        loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(elf_buffer), new_pagemap, task->pid, task->name);
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

        int const INTERP_FD = vfs::vfs_open(std::string_view(INTERP_PATH, std::strlen(INTERP_PATH)), 0, 0);
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

        auto* interp_buf = new uint8_t[INTERP_SIZE];
        ssize_t const INTERP_READ = read_file_fully(INTERP_FD, interp_buf, static_cast<size_t>(INTERP_SIZE), INTERP_PATH);
        vfs::vfs_close(INTERP_FD);

        if (INTERP_READ != INTERP_SIZE) {
            delete[] interp_buf;
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -EIO, 0, WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-EIO);
        }

        loader::elf::ElfLoadResult const INTERP_RESULT =
            loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(interp_buf), new_pagemap, task->pid, "ld.so", false, INTERP_BASE);

        if (INTERP_RESULT.entry_point == 0) {
            delete[] interp_buf;
            end_local_proc_stage(task, perf::WkiPerfLocalProcOp::LOAD_INTERP, LOAD_INTERP_STAGE, -ENOEXEC,
                                 static_cast<uint64_t>(INTERP_SIZE), WOS_PERF_CALLSITE());
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }
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
    while (!task->fd_table.empty()) {
        std::array<uint64_t, sched::task::Task::FD_TABLE_SIZE> fds{};
        size_t fd_count = 0;
        task->fd_table.for_each([&](uint64_t key, void* val) {
            if (val == nullptr) {
                return;
            }
            if (task->get_fd_cloexec(static_cast<unsigned>(key)) && fd_count < sched::task::Task::FD_TABLE_SIZE) {
                fixed_slot(fds, fd_count++) = key;
            }
        });

        if (fd_count == 0) {
            break;
        }

        for (size_t i = 0; i < fd_count; ++i) {
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

    for (unsigned i = 0; i < 3; ++i) {
        if (task->fd_table.lookup(i) == nullptr) {
            vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (new_file != nullptr) {
                new_file->fops = vfs::devfs::get_devfs_fops();
                new_file->fd = static_cast<int>(i);
                new_file->refcount = 1;
                [[maybe_unused]] bool const INSERTED = task->fd_table.insert(i, new_file);
            }
        }
    }

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

    // execve() replaces the current image in-place, so the old address space
    // and thread backing storage must be reclaimed now rather than deferred to
    // task GC. Otherwise each successful exec leaks another user stack/TLS set
    // plus the old pagemap's user pages.
    LocalProcStage const DESTROY_OLD_STAGE = begin_local_proc_stage(task, perf::WkiPerfLocalProcOp::DESTROY_OLD, 0, WOS_PERF_CALLSITE());
    ker::syscall::shm::shm_cleanup_for_task(task);
    if (old_pagemap != nullptr && old_pagemap != new_pagemap) {
        mm::virt::switch_to_kernel_pagemap();
        mm::virt::destroy_user_space(old_pagemap, task->pid, task->name, "exec-old-image");
        mm::phys::page_free(old_pagemap);
        old_pagemap = nullptr;
    }
    if (old_thread != nullptr && old_thread != new_thread) {
        old_thread->tls_phys_ptr = 0;
        old_thread->stack_phys_ptr = 0;
        mod::sched::threading::destroy_thread(old_thread);
    }
    end_local_proc_stage(task, perf::WkiPerfLocalProcOp::DESTROY_OLD, DESTROY_OLD_STAGE, 0, 0, WOS_PERF_CALLSITE());

    task->pagemap = new_pagemap;
    task->thread = new_thread;

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

    // Compute physical pagemap address before we enter the critical section
    auto phys_pagemap = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(new_pagemap)));

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
