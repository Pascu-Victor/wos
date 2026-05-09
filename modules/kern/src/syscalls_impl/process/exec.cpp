#include "exec.hpp"

#include <extern/elf.h>

// #define EXEC_DEBUG

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <net/wki/remote_compute.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <span>
#include <string_view>
#include <util/smallvec.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/threading.hpp"
#include "util/hcf.hpp"
#include "vfs/stat.hpp"
namespace ker::syscall::process {

auto wos_proc_exec_impl(const char* path, const char* const argv[], const char* const envp[], int shebang_depth) -> uint64_t;
auto wos_proc_execve_impl(const char* path, const char* const argv[], const char* const envp[], ker::mod::cpu::GPRegs& gpr,
                          int shebang_depth) -> uint64_t;

namespace {
constexpr int MAX_SHEBANG_DEPTH = 4;

struct ShebangInfo {
    std::array<char, ker::mod::sched::task::Task::EXE_PATH_MAX> interpreter = {};
    std::array<char, 256> argument = {};
    bool has_argument = false;
};

auto allocate_kernel_stack() -> uint64_t {
    auto stack_base = (uint64_t)ker::mod::mm::phys::pageAlloc(KERNEL_STACK_SIZE);
    if (stack_base == 0) {
        return 0;
    }

    return stack_base + KERNEL_STACK_SIZE;
}

auto parse_shebang_line(const uint8_t* file_data, size_t file_size, ShebangInfo* out) -> bool {
    if (file_data == nullptr || out == nullptr || file_size < 2 || file_data[0] != '#' || file_data[1] != '!') {
        return false;
    }

    size_t pos = 2;
    while (pos < file_size && (file_data[pos] == ' ' || file_data[pos] == '\t')) {
        pos++;
    }

    size_t interp_begin = pos;
    while (pos < file_size && file_data[pos] != '\n' && file_data[pos] != '\r' && file_data[pos] != ' ' && file_data[pos] != '\t') {
        pos++;
    }

    size_t interp_len = pos - interp_begin;
    if (interp_len == 0 || interp_len >= out->interpreter.size()) {
        return false;
    }
    std::memcpy(out->interpreter.data(), file_data + interp_begin, interp_len);
    out->interpreter[interp_len] = '\0';

    while (pos < file_size && (file_data[pos] == ' ' || file_data[pos] == '\t')) {
        pos++;
    }

    size_t arg_begin = pos;
    while (pos < file_size && file_data[pos] != '\n' && file_data[pos] != '\r') {
        pos++;
    }

    while (pos > arg_begin && (file_data[pos - 1] == ' ' || file_data[pos - 1] == '\t')) {
        pos--;
    }

    size_t arg_len = pos - arg_begin;
    if (arg_len == 0) {
        return true;
    }
    if (arg_len >= out->argument.size()) {
        return false;
    }

    std::memcpy(out->argument.data(), file_data + arg_begin, arg_len);
    out->argument[arg_len] = '\0';
    out->has_argument = true;
    return true;
}

template <typename ExecFn>
auto exec_shebang_script(const char* script_path, const char* const argv[], const char* const envp[], size_t argv_count,
                         const ShebangInfo& shebang, int shebang_depth, ExecFn&& exec_fn) -> uint64_t {
    size_t forwarded_args = argv_count > 0 ? (argv_count - 1) : 0;
    size_t new_argc = 2 + forwarded_args + (shebang.has_argument ? 1 : 0);
    auto** shebang_argv = new const char*[new_argc + 1];
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

    uint64_t rc = exec_fn(shebang.interpreter.data(), shebang_argv, envp, shebang_depth + 1);
    delete[] shebang_argv;
    return rc;
}

}  // namespace

auto wos_proc_exec(const char* path, const char* const argv[], const char* const envp[]) -> uint64_t {
    return wos_proc_exec_impl(path, argv, envp, 0);
}

auto wos_proc_exec_impl(const char* path, const char* const argv[], const char* const envp[], int shebang_depth) -> uint64_t {
    std::string_view str(path, std::strlen(path));
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
    uint64_t parent_pid = parent_task->pid;

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Loading '%.*s'", (int)str.size(), str.data());
#endif

    int fd = vfs::vfs_open(str, 0, 0);
    if (fd < 0) {
        dbg::log("wos_proc_exec: Failed to open file '%.*s'", (int)str.size(), str.data());
        return 0;
    }

    // Check execute permission
    int access_ret = vfs::vfs_access(path, 1 /* X_OK */);
    if (access_ret < 0) {
        dbg::log("wos_proc_exec: Execute permission denied for '%.*s'", (int)str.size(), str.data());
        vfs::vfs_close(fd);
        return 0;
    }

    ssize_t file_size = vfs::vfs_lseek(fd, 0, 2);
    if (file_size <= 0) {
        dbg::log("wos_proc_exec: Invalid file size: %d", file_size);
        vfs::vfs_close(fd);
        return 0;
    }
    vfs::vfs_lseek(fd, 0, 0);

    auto* elf_buffer = new uint8_t[file_size];
    if (elf_buffer == nullptr) {
        dbg::log("wos_proc_exec: Failed to allocate buffer");
        vfs::vfs_close(fd);
        return 0;
    }

    ssize_t bytes_read = 0;
    vfs::vfs_read(fd, elf_buffer, file_size, (size_t*)&bytes_read);
    vfs::vfs_close(fd);

    if (bytes_read != file_size) {
        dbg::log("wos_proc_exec: Failed to read file completely");
        delete[] elf_buffer;
        return 0;
    }

    // Add memory barrier after reading to ensure visibility
    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    if (parse_shebang_line(elf_buffer, static_cast<size_t>(file_size), &shebang)) {
        delete[] elf_buffer;
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return 0;
        }
        return exec_shebang_script(path, argv, envp, argv_count, shebang, shebang_depth,
                                   [](const char* interp, const char* const argv2[], const char* const envp2[], int depth) -> uint64_t {
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

    const char* process_name = str.data();
    for (size_t i = 0; i < str.size(); i++) {
        if (str[i] == '/') {
            process_name = str.data() + i + 1;
        }
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Creating task for '%s', parent PID: %x", process_name, parent_pid);
#endif

    uint64_t kernel_rsp = allocate_kernel_stack();
    if (kernel_rsp == 0) {
        dbg::log("wos_proc_exec: Failed to allocate kernel stack");
        delete[] elf_buffer;
        return 0;
    }

    // DIAGNOSTIC: Detect stack corruption during Task constructor
    // Save known canary values on stack, check after constructor
    volatile uint64_t canary1 = 0xDEAD'BEEF'CAFE'BABEULL;  // NOLINT
    volatile uint64_t canary2 = 0x1234'5678'9ABC'DEF0ULL;  // NOLINT

    auto* new_task = new sched::task::Task(process_name, (uint64_t)elf_buffer, kernel_rsp, sched::task::TaskType::PROCESS);

    // Check canaries for stack corruption
    if (canary1 != 0xDEAD'BEEF'CAFE'BABEULL || canary2 != 0x1234'5678'9ABC'DEF0ULL) {  // NOLINT
        dbg::log("STACK CORRUPTION DETECTED in exec!");
        dbg::log("  canary1=%lx (expect DEADBEEFCAFEBABE)", canary1);
        dbg::log("  canary2=%lx (expect 123456789ABCDEF0)", canary2);
        dbg::log("  newTask=%p, &canary1=%p, &canary2=%p", new_task, &canary1, &canary2);
        dbg::log("  stack RSP approx %p, kernelRsp=%lx", &new_task, kernel_rsp);
    }

    // Also check if newTask is suspiciously not in HHDM range
    auto task_addr = reinterpret_cast<uintptr_t>(new_task);
    if (task_addr != 0 && (task_addr < 0xffff800000000000ULL || task_addr >= 0xffff900000000000ULL)) {
        dbg::log("EXEC BUG: operator new returned non-HHDM ptr: %p", new_task);
        dbg::log("  expected range: 0xffff800000000000 - 0xffff900000000000");
        dbg::log("  &newTask on stack = %p, kernelRsp = %lx", &new_task, kernel_rsp);
        delete[] elf_buffer;
        return 0;
    }

    if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
        dbg::log("wos_proc_exec: Failed to create task (OOM during thread/pagemap allocation)");
        if (new_task != nullptr) {
            delete new_task;
        }
        // TODO: Free kernel stack pages
        delete[] elf_buffer;
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task constructor completed successfully");
    dbg::log("wos_proc_exec: Entry point = 0x%x, RIP = 0x%x", new_task->entry, new_task->context.frame.rip);
#endif

    new_task->parentPid = parent_pid;

    // Inherit process execution context from the parent before applying
    // executable-specific overrides such as setuid/setgid.
    memcpy(new_task->cwd, parent_task->cwd, sizeof(new_task->cwd));
    memcpy(new_task->root, parent_task->root, sizeof(new_task->root));
    new_task->uid = parent_task->uid;
    new_task->gid = parent_task->gid;
    new_task->euid = parent_task->euid;
    new_task->egid = parent_task->egid;
    new_task->suid = parent_task->suid;
    new_task->sgid = parent_task->sgid;
    new_task->umask = parent_task->umask;
    new_task->session_id = parent_task->session_id;
    new_task->pgid = (parent_task->pgid != 0) ? parent_task->pgid : parent_task->pid;
    new_task->controlling_tty = parent_task->controlling_tty;
    new_task->wki_prefer_inline = parent_task->wki_prefer_inline;
    memcpy(new_task->wki_target_hostname, parent_task->wki_target_hostname, sizeof(new_task->wki_target_hostname));
    new_task->wki_target_flags = parent_task->wki_target_flags;
    memcpy(new_task->wki_submitter_hostname, parent_task->wki_submitter_hostname, sizeof(new_task->wki_submitter_hostname));
    new_task->wki_remote_pid =
        (new_task->wki_submitter_hostname[0] != '\0' &&
         std::strcmp(new_task->wki_submitter_hostname, ker::net::wki::g_wki.local_hostname) != 0)
            ? new_task->pid
            : 0;
    [[maybe_unused]] bool cloned_rules = new_task->wki_vfs_rules.clone_from(parent_task->wki_vfs_rules);

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
        [[maybe_unused]] bool inserted = new_task->fd_table.insert(key, parent_file);
    });

    // Ensure fds 0/1/2 are always set (open /dev/console if not inherited)
    for (unsigned i = 0; i < 3; ++i) {
        if (new_task->fd_table.lookup(i) == nullptr) {
            vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (new_file != nullptr) {
                new_file->fops = vfs::devfs::get_devfs_fops();
                new_file->fd = static_cast<int>(i);
                new_file->refcount = 1;
                [[maybe_unused]] bool inserted = new_task->fd_table.insert(i, new_file);
            }
        }
    }

    new_task->elfBuffer = elf_buffer;
    new_task->elfBufferSize = file_size;
    new_task->isElfBufferShared = false;

    // Store executable path for /proc/self/exe
    {
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        memcpy(new_task->exe_path, path, path_len);
        new_task->exe_path[path_len] = '\0';
    }

    // Handle setuid/setgid bits from the executable
    {
        vfs::stat exec_st{};
        if (vfs::vfs_stat(path, &exec_st) == 0) {
            if (exec_st.st_mode & 04000) {  // S_ISUID
                new_task->euid = exec_st.st_uid;
                new_task->suid = exec_st.st_uid;
            }
            if (exec_st.st_mode & 02000) {  // S_ISGID
                new_task->egid = exec_st.st_gid;
                new_task->sgid = exec_st.st_gid;
            }
        }
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task created with PID: %x, parent: %x", new_task->pid, new_task->parentPid);
#endif

    uint64_t user_stack_virt = new_task->thread->stack;

    uint64_t current_virt_offset = 0;

    auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
        if (current_virt_offset + size > USER_STACK_SIZE) {
            return 0;  // Stack overflow
        }
        current_virt_offset += size;
        uint64_t virt_addr = user_stack_virt - current_virt_offset;

        uint64_t page_virt = virt_addr & ~(mod::mm::paging::PAGE_SIZE - 1);
        uint64_t page_offset = virt_addr & (mod::mm::paging::PAGE_SIZE - 1);

        uint64_t page_phys = mod::mm::virt::translate(new_task->pagemap, page_virt);
        if (page_phys == mod::mm::virt::PADDR_INVALID) {
            mod::dbg::log("exec pushData: translate failed for stack vaddr 0x%x - stack page not mapped", page_virt);
            hcf();
        }

        auto* dest_ptr = reinterpret_cast<uint8_t*>(mod::mm::addr::get_virt_pointer(page_phys)) + page_offset;
        std::memcpy(dest_ptr, data, size);

        return virt_addr;
    };

    auto push_string = [&](std::string_view str) -> uint64_t {
        size_t len = str.size() + 1;  // Include null terminator
        if (current_virt_offset + len > USER_STACK_SIZE) {
            return 0;
        }
        current_virt_offset += len;
        uint64_t virt_addr = user_stack_virt - current_virt_offset;

        uint64_t page_virt = virt_addr & ~(mod::mm::paging::PAGE_SIZE - 1);
        uint64_t page_offset = virt_addr & (mod::mm::paging::PAGE_SIZE - 1);

        uint64_t page_phys = mod::mm::virt::translate(new_task->pagemap, page_virt);
        if (page_phys == mod::mm::virt::PADDR_INVALID) {
            mod::dbg::log("exec pushString: translate failed for stack vaddr 0x%x - stack page not mapped", page_virt);
            hcf();
        }

        auto* dest_ptr = reinterpret_cast<uint8_t*>(mod::mm::addr::get_virt_pointer(page_phys)) + page_offset;
        std::memcpy(dest_ptr, str.data(), str.size());
        dest_ptr[str.size()] = '\0';

        return virt_addr;
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
        uint64_t current_addr = user_stack_virt - current_virt_offset;
        uint64_t aligned = current_addr & ~(ALIGNMENT - 1);
        current_virt_offset += (current_addr - aligned);

        constexpr size_t AUXV_QWORDS_BASE = 12;  // 5 core pairs (PAGESZ,ENTRY,PHDR,PHENT,PHNUM) + AT_NULL pair
        const size_t AUXV_QWORDS = AUXV_QWORDS_BASE + (new_task->interpBase != 0 ? 2 : 0);
        size_t structured_qwords = AUXV_QWORDS + (envp_count + 1) + (argv_count + 1) + 1;
        if (structured_qwords % 2 != 0) {
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
        built_correct_auxv &= auxv.push_back((uint64_t)mod::mm::paging::PAGE_SIZE);
        built_correct_auxv &= auxv.push_back(AT_ENTRY);
        built_correct_auxv &= auxv.push_back(new_task->entry);
        built_correct_auxv &= auxv.push_back(AT_PHDR);
        built_correct_auxv &= auxv.push_back(new_task->programHeaderAddr);
        built_correct_auxv &= auxv.push_back(AT_PHENT);
        built_correct_auxv &= auxv.push_back(new_task->programHeaderEntSize);
        built_correct_auxv &= auxv.push_back(AT_PHNUM);
        built_correct_auxv &= auxv.push_back(new_task->programHeaderCount);
        if (new_task->interpBase != 0) {
            built_correct_auxv &= auxv.push_back(AT_BASE);
            built_correct_auxv &= auxv.push_back(new_task->interpBase);
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
            uint64_t val = auxv[static_cast<size_t>(j)];
            push_to_stack(&val, sizeof(uint64_t));
        }
    }

    // Push envp pointer array (with NULL terminator)
    uint64_t envp_ptr = push_to_stack(envp_addrs, (envp_count + 1) * sizeof(uint64_t));
    delete[] envp_addrs;

    // Push argv pointer array (with NULL terminator)
    uint64_t argv_ptr = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    delete[] argv_addrs;

    // Push argc last (rsp will point here)
    uint64_t argc = argv_count;
    push_to_stack(&argc, sizeof(uint64_t));

    new_task->context.frame.rsp = user_stack_virt - current_virt_offset;

    new_task->context.regs.rdi = argc;
    new_task->context.regs.rsi = argv_ptr;
    new_task->context.regs.rdx = envp_ptr;

    ker::net::wki::WkiRemoteSpawnSpec remote_spawn = {
        .argv = argv,
        .envp = envp,
        .cwd = parent_task->cwd,
    };
    auto remote_result = ker::net::wki::wki_try_remote_spawn(new_task, remote_spawn);
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
        return new_task->pid;
    }
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
        delete new_task;
        delete[] elf_buffer;
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Setup stack - argc=%d, argv=0x%x, envp=0x%x, rsp=0x%x", argc, argv_ptr, envp_ptr, new_task->context.frame.rsp);
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

auto wos_proc_execve(const char* path, const char* const argv[], const char* const envp[], ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    return wos_proc_execve_impl(path, argv, envp, gpr, 0);
}

auto wos_proc_execve_impl(const char* path, const char* const argv[], const char* const envp[], ker::mod::cpu::GPRegs& gpr,
                          int shebang_depth) -> uint64_t {
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

    // --- Copy argv/envp strings into kernel memory (before we destroy user mappings) ---
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
        size_t len = std::strlen(argv[i]);
        k_argv[i] = new char[len + 1];
        std::memcpy(k_argv[i], argv[i], len + 1);
    }
    k_argv[argv_count] = nullptr;

    auto** k_envp = new char*[envp_count + 1];
    for (size_t i = 0; i < envp_count; i++) {
        size_t len = std::strlen(envp[i]);
        k_envp[i] = new char[len + 1];
        std::memcpy(k_envp[i], envp[i], len + 1);
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

    // --- Read the ELF file ---
    int fd = vfs::vfs_open(std::string_view(path, std::strlen(path)), 0, 0);
    if (fd < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: Failed to open '%s' (fd=%d)", path, fd);
#endif
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOENT);
    }

    int access_ret = vfs::vfs_access(path, 1 /* X_OK */);
    if (access_ret < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: vfs_access X_OK failed for '%s' (ret=%d)", path, access_ret);
#endif
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EACCES);
    }

    ssize_t file_size = vfs::vfs_lseek(fd, 0, 2);
    if (file_size < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: SEEK_END failed for '%s' (fileSize=%ld)", path, file_size);
#endif
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EIO);
    }
    if (file_size == 0) {
        dbg::log("wos_proc_execve: empty file '%s'", path);
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOEXEC);
    }
    vfs::vfs_lseek(fd, 0, 0);

    auto* elf_buffer = new uint8_t[file_size];
    if (elf_buffer == nullptr) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: alloc failed for '%s' (%ld bytes)", path, file_size);
#endif
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOMEM);
    }

    ssize_t bytes_read = 0;
    vfs::vfs_read(fd, elf_buffer, file_size, (size_t*)&bytes_read);
    vfs::vfs_close(fd);

    if (bytes_read != file_size) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: short read for '%s' (got %ld, expect %ld)", path, bytes_read, file_size);
#endif
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EIO);
    }

    __asm__ volatile("mfence" ::: "memory");

    ShebangInfo shebang = {};
    if (parse_shebang_line(elf_buffer, static_cast<size_t>(file_size), &shebang)) {
        delete[] elf_buffer;
        free_kernel_arg_env_once();
        if (shebang_depth >= MAX_SHEBANG_DEPTH) {
            return static_cast<uint64_t>(-ELOOP);
        }
        return exec_shebang_script(path, argv, envp, argv_count, shebang, shebang_depth,
                                   [&gpr](const char* interp, const char* const argv2[], const char* const envp2[], int depth) -> uint64_t {
                                       return wos_proc_execve_impl(interp, argv2, envp2, gpr, depth);
                                   });
    }

    auto* elf_header = reinterpret_cast<Elf64_Ehdr*>(elf_buffer);
    if (elf_header->e_ident[EI_MAG0] != ELFMAG0 || elf_header->e_ident[EI_MAG1] != ELFMAG1 || elf_header->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_header->e_ident[EI_MAG3] != ELFMAG3 || elf_header->e_ident[EI_CLASS] != ELFCLASS64) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF magic check failed for '%s' (bytes: %02x %02x %02x %02x class=%02x)", path, elf_header->e_ident[0],
                 elf_header->e_ident[1], elf_header->e_ident[2], elf_header->e_ident[3], elf_header->e_ident[4]);
#endif
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    {
        uint8_t* saved_elf_buffer = task->elfBuffer;
        size_t saved_elf_buffer_size = task->elfBufferSize;
        bool saved_is_elf_buffer_shared = task->isElfBufferShared;
        bool saved_wki_skip_legacy_placement = task->wki_skip_legacy_placement;
        uint64_t saved_wki_remote_pid = task->wki_remote_pid;
        std::array<char, sched::task::Task::EXE_PATH_MAX> saved_exe_path = {};
        std::memcpy(saved_exe_path.data(), task->exe_path, saved_exe_path.size());

        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }

        task->elfBuffer = elf_buffer;
        task->elfBufferSize = static_cast<size_t>(file_size);
        task->isElfBufferShared = false;
        std::memcpy(task->exe_path, path, path_len);
        task->exe_path[path_len] = '\0';

        ker::net::wki::WkiRemoteSpawnSpec remote_spawn = {
            .argv = k_argv,
            .envp = k_envp,
            .cwd = task->cwd,
        };
        auto remote_result = ker::net::wki::wki_try_remote_spawn(task, remote_spawn);

        task->elfBuffer = saved_elf_buffer;
        task->elfBufferSize = saved_elf_buffer_size;
        task->isElfBufferShared = saved_is_elf_buffer_shared;

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
            task->deferredTaskSwitch = true;
            task->yieldSwitch = false;
            task->wait_channel = "wki_execve_proxy";
            free_kernel_arg_env_once();
            return 0;
        }

        std::memcpy(task->exe_path, saved_exe_path.data(), saved_exe_path.size());
        task->wki_skip_legacy_placement = saved_wki_skip_legacy_placement;
        task->wki_remote_pid = saved_wki_remote_pid;

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
            delete[] elf_buffer;
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-EHOSTUNREACH);
        }
    }

    // --- Replace the pagemap with a fresh one ---
    // Note: We are executing in kernel context (syscall handler) so our
    // kernel mappings are active. We'll create a new user pagemap.
    uint8_t* old_elf_buffer = task->elfBuffer;
    auto* old_pagemap = task->pagemap;
    auto* old_thread = task->thread;
    auto* new_pagemap = mm::virt::createPagemap();
    if (new_pagemap == nullptr) {
        delete[] elf_buffer;
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }
    {
        sched::task::Task pagemap_task{};
        pagemap_task.pagemap = new_pagemap;
        mm::virt::copyKernelMappings(&pagemap_task);
    }

    // --- Create new thread (user stack + TLS) ---
    ker::loader::elf::TlsModule tls_info = loader::elf::extractTlsInfo((void*)(uint64_t)elf_buffer);
    auto* new_thread = mod::sched::threading::createThread(USER_STACK_SIZE, tls_info.tlsSize, new_pagemap, tls_info);
    char* new_name = nullptr;
    auto cleanup_new_image = [&]() {
        if (new_pagemap != nullptr) {
            mm::virt::destroyUserSpace(new_pagemap, task->pid, new_name != nullptr ? new_name : task->name, "exec-new-image-cleanup");
            mm::phys::pageFree(new_pagemap);
            new_pagemap = nullptr;
        }
        if (new_thread != nullptr) {
            new_thread->tlsPhysPtr = 0;
            new_thread->stackPhysPtr = 0;
            mod::sched::threading::destroyThread(new_thread);
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
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOMEM);
    }

    // execve() reuses the same PID. The loader debug registry is keyed by PID,
    // so we must discard the old image's symbol metadata before registering the
    // new ELF or lookups like __safestack_unsafe_stack_ptr can resolve against
    // stale offsets from the previous process image.
    loader::debug::unregisterProcess(task->pid);

    // --- Load ELF into new pagemap ---
    loader::elf::ElfLoadResult elf_result =
        loader::elf::loadElf((loader::elf::ElfFile*)(uint64_t)elf_buffer, new_pagemap, task->pid, task->name);
    if (elf_result.entryPoint == 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF load failed for '%s'", path);
#endif
        cleanup_new_image();
        free_kernel_arg_env_once();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    uint64_t new_exec_entry = elf_result.entryPoint;
    uint64_t new_initial_rip = elf_result.entryPoint;
    uint64_t new_program_header_addr = elf_result.programHeaderAddr;
    uint64_t new_elf_header_addr = elf_result.elfHeaderAddr;
    uint16_t new_program_header_count = elf_result.programHeaderCount;
    uint16_t new_program_header_ent_size = elf_result.programHeaderEntSize;
    uint64_t new_interp_base = 0;

    // If the binary requests a dynamic linker (PT_INTERP), load it.
    if (elf_result.hasInterp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;

        int interp_fd = vfs::vfs_open(std::string_view(elf_result.interpPath, std::strlen(elf_result.interpPath)), 0, 0);
        if (interp_fd < 0) {
            dbg::log("wos_proc_execve: Failed to open interpreter '%s'", elf_result.interpPath);
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        ssize_t interp_size = vfs::vfs_lseek(interp_fd, 0, 2);
        vfs::vfs_lseek(interp_fd, 0, 0);
        if (interp_size <= 0) {
            vfs::vfs_close(interp_fd);
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        auto* interp_buf = new uint8_t[interp_size];
        ssize_t interp_read = 0;
        vfs::vfs_read(interp_fd, interp_buf, interp_size, (size_t*)&interp_read);
        vfs::vfs_close(interp_fd);

        if (interp_read != interp_size) {
            delete[] interp_buf;
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-EIO);
        }

        loader::elf::ElfLoadResult interp_result =
            loader::elf::loadElf((loader::elf::ElfFile*)(uint64_t)interp_buf, new_pagemap, task->pid, "ld.so", false, INTERP_BASE);

        if (interp_result.entryPoint == 0) {
            delete[] interp_buf;
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        // Override entry point to the interpreter — ld.so reads AT_ENTRY from auxv
        new_initial_rip = interp_result.entryPoint;
        new_interp_base = INTERP_BASE;

        delete[] interp_buf;
    }

    std::string_view path_str(path, std::strlen(path));
    const char* base_name = path_str.data();
    for (size_t i = 0; i < path_str.size(); i++) {
        if (path_str[i] == '/') {
            base_name = path_str.data() + i + 1;
        }
    }
    {
        size_t base_len = std::strlen(base_name);
        new_name = new char[base_len + 1];
        if (new_name == nullptr) {
            cleanup_new_image();
            free_kernel_arg_env_once();
            return static_cast<uint64_t>(-ENOMEM);
        }
        std::memcpy(new_name, base_name, base_len + 1);
    }

    // --- Set up the user stack with argv/envp/auxv ---
    uint64_t user_stack_virt = new_thread->stack;
    uint64_t current_virt_offset = 0;

    auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
        if (current_virt_offset + size > USER_STACK_SIZE) return 0;
        current_virt_offset += size;
        uint64_t virt_addr = user_stack_virt - current_virt_offset;
        uint64_t page_virt = virt_addr & ~(mm::paging::PAGE_SIZE - 1);
        uint64_t page_offset = virt_addr & (mm::paging::PAGE_SIZE - 1);
        uint64_t page_phys = mm::virt::translate(new_pagemap, page_virt);
        if (page_phys == mm::virt::PADDR_INVALID) {
            dbg::log("exec pushData: translate failed for stack vaddr 0x%x", page_virt);
            hcf();
        }
        auto* dest_ptr = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(page_phys)) + page_offset;
        std::memcpy(dest_ptr, data, size);
        return virt_addr;
    };

    auto push_string = [&](const char* str) -> uint64_t {
        size_t len = std::strlen(str) + 1;
        if (current_virt_offset + len > USER_STACK_SIZE) {
            return 0;
        }
        current_virt_offset += len;
        uint64_t virt_addr = user_stack_virt - current_virt_offset;
        uint64_t page_virt = virt_addr & ~(mm::paging::PAGE_SIZE - 1);
        uint64_t page_offset = virt_addr & (mm::paging::PAGE_SIZE - 1);
        uint64_t page_phys = mm::virt::translate(new_pagemap, page_virt);
        if (page_phys == mm::virt::PADDR_INVALID) {
            dbg::log("exec pushString: translate failed for stack vaddr 0x%x", page_virt);
            hcf();
        }
        auto* dest_ptr = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(page_phys)) + page_offset;
        std::memcpy(dest_ptr, str, len);
        return virt_addr;
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
        uint64_t current_addr = user_stack_virt - current_virt_offset;
        uint64_t aligned = current_addr & ~(ALIGNMENT - 1);
        current_virt_offset += (current_addr - aligned);

        constexpr size_t AUXV_BASE_QWORDS = 12;  // 5 core pairs (PAGESZ,ENTRY,PHDR,PHENT,PHNUM) + AT_NULL pair
        size_t auxv_qwords = AUXV_BASE_QWORDS + (new_interp_base != 0 ? 2 : 0);
        size_t structured_qwords = auxv_qwords + (envp_count + 1) + (argv_count + 1) + 1;
        if (structured_qwords % 2 != 0) {
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
        built_correct_auxv &= auxv.push_back((uint64_t)mm::paging::PAGE_SIZE);
        built_correct_auxv &= auxv.push_back(AT_ENTRY);
        built_correct_auxv &= auxv.push_back(new_exec_entry);
        built_correct_auxv &= auxv.push_back(AT_PHDR);
        built_correct_auxv &= auxv.push_back(new_program_header_addr);
        built_correct_auxv &= auxv.push_back(AT_PHENT);
        built_correct_auxv &= auxv.push_back(new_program_header_ent_size);
        built_correct_auxv &= auxv.push_back(AT_PHNUM);
        built_correct_auxv &= auxv.push_back(new_program_header_count);
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
            uint64_t val = auxv[static_cast<size_t>(j)];
            push_to_stack(&val, sizeof(uint64_t));
        }
    }

    uint64_t envp_ptr = push_to_stack(envp_addrs, (envp_count + 1) * sizeof(uint64_t));
    if (envp_ptr == 0) {
        delete[] envp_addrs;
        delete[] argv_addrs;
        cleanup_new_image();
        return static_cast<uint64_t>(-E2BIG);
    }
    delete[] envp_addrs;

    uint64_t argv_ptr = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    if (argv_ptr == 0) {
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

    // exec only closes FD_CLOEXEC descriptors after the new image is ready;
    // failed execve() must leave the original process image intact.
    // Snapshot descriptors first because vfs_close() mutates fd_table.
    while (!task->fd_table.empty()) {
        uint64_t fds[sched::task::Task::FD_TABLE_SIZE]{};
        size_t fd_count = 0;
        task->fd_table.for_each([&](uint64_t key, void* val) {
            if (val == nullptr) {
                return;
            }
            if (task->get_fd_cloexec(static_cast<unsigned>(key)) && fd_count < sched::task::Task::FD_TABLE_SIZE) {
                fds[fd_count++] = key;
            }
        });

        if (fd_count == 0) {
            break;
        }

        for (size_t i = 0; i < fd_count; ++i) {
            vfs::vfs_close(static_cast<int>(fds[i]));
        }
    }

    if (old_elf_buffer != nullptr) {
        if (!ker::net::wki::wki_remote_compute_release_elf_buffer(old_elf_buffer)) {
            delete[] old_elf_buffer;
        }
    }

    task->entry = new_exec_entry;
    task->programHeaderAddr = new_program_header_addr;
    task->elfHeaderAddr = new_elf_header_addr;
    task->programHeaderCount = new_program_header_count;
    task->programHeaderEntSize = new_program_header_ent_size;
    task->elfBuffer = elf_buffer;
    task->elfBufferSize = file_size;
    task->interpBase = new_interp_base;

    {
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) path_len = sched::task::Task::EXE_PATH_MAX - 1;
        std::memcpy(task->exe_path, path, path_len);
        task->exe_path[path_len] = '\0';
    }

    if (task->name != nullptr) {
        delete[] const_cast<char*>(task->name);
    }
    task->name = new_name;
    new_name = nullptr;

    {
        vfs::stat exec_st{};
        if (vfs::vfs_stat(path, &exec_st) == 0) {
            if (exec_st.st_mode & 04000) {
                task->euid = exec_st.st_uid;
                task->suid = exec_st.st_uid;
            }
            if (exec_st.st_mode & 02000) {
                task->egid = exec_st.st_gid;
                task->sgid = exec_st.st_gid;
            }
        }
    }

    task->sigPending = 0;
    task->inSignalHandler = false;
    task->doSigreturn = false;
    for (auto& sh : task->sigHandlers) {
        sh = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};
    }

    for (unsigned i = 0; i < 3; ++i) {
        if (task->fd_table.lookup(i) == nullptr) {
            vfs::File* newFile = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (newFile != nullptr) {
                newFile->fops = vfs::devfs::get_devfs_fops();
                newFile->fd = static_cast<int>(i);
                newFile->refcount = 1;
                [[maybe_unused]] bool inserted = task->fd_table.insert(i, newFile);
            }
        }
    }

    // --- Set up the task context to jump to the new binary ---
    uint64_t new_rsp = user_stack_virt - current_virt_offset;
    task->context.frame.rip = new_initial_rip;
    task->context.frame.rsp = new_rsp;
    task->context.frame.ss = 0x1b;
    task->context.frame.cs = 0x23;
    task->context.frame.flags = 0x202;
    task->context.frame.intNum = 0;
    task->context.frame.errCode = 0;

    // Match the fresh-process entry contract used by _wOS_asm_enterUsermode:
    // startup code consumes argc/argv/envp from the initial stack, not GPRs.
    task->context.regs = cpu::GPRegs();
    task->context.regs.rdi = new_initial_rip;
    task->context.regs.rsi = new_rsp;
    (void)argc;
    (void)argv_ptr;
    (void)envp_ptr;

    // Freshly spawned processes rewrite fs:[0] just before the first usermode
    // entry. execve() bypasses that path, so repair the initial TCB self-pointer
    // here before any TLS access in ld.so / libc.
    if (new_thread != nullptr) {
        uint64_t tcb_paddr = mm::virt::translate(new_pagemap, new_thread->fsbase);
        if (tcb_paddr != mm::virt::PADDR_INVALID) {
            auto* tcb_self = reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(tcb_paddr));
            *tcb_self = new_thread->fsbase;
        }
    }

    // Initialize SafeStack TLS symbol if present
    auto* ssym = loader::debug::getProcessSymbol(task->pid, "__safestack_unsafe_stack_ptr");
    if (new_thread != nullptr && (ssym != nullptr) && ssym->isTlsOffset) {
        uint64_t dest_vaddr = new_thread->tlsBaseVirt + ssym->rawValue;
        uint64_t dest_paddr = mm::virt::translate(new_pagemap, dest_vaddr);
        if (dest_paddr != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = (uint64_t*)mm::addr::get_virt_pointer(dest_paddr);
            *dest_ptr = new_thread->safestackPtrValue;
        }
    }

    // execve() returns directly via sysret instead of re-entering through the
    // scheduler, so we must refresh the live CPU's user TLS bases here.
    // Otherwise the CPU would keep the old image's FS_BASE / user GS_BASE and
    // immediately fault in the new process when libc touches TLS.
    if (new_thread != nullptr) {
        cpu::wrfsbase(new_thread->fsbase);
        cpuSetMSR(IA32_KERNEL_GS_BASE, new_thread->gsbase);
    }

    // execve() replaces the current image in-place, so the old address space
    // and thread backing storage must be reclaimed now rather than deferred to
    // task GC. Otherwise each successful exec leaks another user stack/TLS set
    // plus the old pagemap's user pages.
    if (old_pagemap != nullptr && old_pagemap != new_pagemap) {
        mm::virt::switchToKernelPagemap();
        mm::virt::destroyUserSpace(old_pagemap, task->pid, task->name, "exec-old-image");
        mm::phys::pageFree(old_pagemap);
        old_pagemap = nullptr;
    }
    if (old_thread != nullptr && old_thread != new_thread) {
        old_thread->tlsPhysPtr = 0;
        old_thread->stackPhysPtr = 0;
        mod::sched::threading::destroyThread(old_thread);
    }

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
    // reference only modifies a local copy in syscallHandler (passed by
    // value), so it has no effect on the actual stack-saved registers.

    // 1. Compute the base of the pushq-saved register block on the kernel stack.
    //    gs:0x0 = kernel stack top (K).  After `sub rsp,8` (retval slot) +
    //    `pushq` (15 regs x 8 = 120 bytes), RSP = K-128.
    //    The GPRegs struct maps directly to K-128 (r15 at offset 0, rax at 0x70).
    //    The compiler accesses this as a stack-passed MEMORY-class parameter at
    //    the callee's rbp+0x10 = K-128.
    uint64_t kern_stack_top = 0;
    asm volatile("movq %%gs:0x0, %0" : "=r"(kern_stack_top));
    auto* stack_base = reinterpret_cast<uint8_t*>(kern_stack_top - 128);

#ifdef EXEC_DEBUG
    // Log BEFORE patching the stack - dbg::log uses the stack and would
    // clobber the patched register slots if called after.
    dbg::log("wos_proc_execve: PID %x now running '%s' (entry 0x%lx, rsp 0x%lx)", task->pid, task->exe_path, elf_result.entryPoint,
             new_rsp);
#endif
    // Compute physical pagemap address before we enter the critical section
    auto phys_pagemap = (uint64_t)mm::addr::get_phys_pointer((uint64_t)new_pagemap);

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
    asm volatile("movq %0, %%gs:0x30" : : "r"((uint64_t)0x202) : "memory");
    asm volatile("movq %0, %%gs:0x08" : : "r"(new_rsp) : "memory");

    // 4. Switch CR3 to the new pagemap so user-space sees the new mappings.
    asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");

    // Return 0.  The sysret path will pop the patched registers and jump to
    // the new entry point.
    return 0;
}

}  // namespace ker::syscall::process
