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

namespace {
auto allocate_kernel_stack() -> uint64_t {
    auto stack_base = (uint64_t)ker::mod::mm::phys::pageAlloc(KERNEL_STACK_SIZE);
    if (stack_base == 0) {
        return 0;
    }

    return stack_base + KERNEL_STACK_SIZE;
}

}  // namespace

auto wos_proc_exec(const char* path, const char* const argv[], const char* const envp[]) -> uint64_t {
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
    if ((parent_task->wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_NOINHERIT) == 0) {
        memcpy(new_task->wki_target_hostname, parent_task->wki_target_hostname, sizeof(new_task->wki_target_hostname));
        new_task->wki_target_flags = parent_task->wki_target_flags;
    }
    memcpy(new_task->wki_submitter_hostname, parent_task->wki_submitter_hostname, sizeof(new_task->wki_submitter_hostname));
    new_task->wki_vfs_rules.clone_from(parent_task->wki_vfs_rules);

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
        new_task->fd_table.insert(key, parent_file);
    });

    // Ensure fds 0/1/2 are always set (open /dev/console if not inherited)
    for (unsigned i = 0; i < 3; ++i) {
        if (new_task->fd_table.lookup(i) == nullptr) {
            vfs::File* new_file = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (new_file != nullptr) {
                new_file->fops = vfs::devfs::get_devfs_fops();
                new_file->fd = static_cast<int>(i);
                new_file->refcount = 1;
                new_task->fd_table.insert(i, new_file);
            }
        }
    }

    new_task->elfBuffer = elf_buffer;
    new_task->elfBufferSize = file_size;

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

        const size_t AUXV_QWORDS = 14 + (new_task->interpBase != 0 ? 2 : 0);
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
        constexpr uint64_t AT_EHDR = 33;

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
        built_correct_auxv &= auxv.push_back(AT_EHDR);
        built_correct_auxv &= auxv.push_back(new_task->elfHeaderAddr);
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

    uint8_t* old_elf_buffer = task->elfBuffer;
    size_t old_elf_buffer_size = task->elfBufferSize;
    bool old_skip_legacy_placement = task->wki_skip_legacy_placement;
    std::array<char, sched::task::Task::EXE_PATH_MAX> old_exe_path = {};
    std::memcpy(old_exe_path.data(), task->exe_path, old_exe_path.size());

    auto finish_remote_exec = [&]() -> uint64_t {
        for (unsigned i = 0; i < sched::task::Task::FD_TABLE_SIZE; ++i) {
            auto* fval = static_cast<vfs::File*>(task->fd_table.lookup(i));
            if (fval == nullptr) {
                continue;
            }
            if (task->get_fd_cloexec(i)) {
                vfs::vfs_close(static_cast<int>(i));
            }
        }

        if (old_elf_buffer != nullptr) {
            if (!ker::net::wki::wki_remote_compute_release_elf_buffer(old_elf_buffer)) {
                delete[] old_elf_buffer;
            }
        }

        {
            std::string_view path_str(path, std::strlen(path));
            const char* base_name = path_str.data();
            for (size_t i = 0; i < path_str.size(); i++) {
                if (path_str[i] == '/') {
                    base_name = path_str.data() + i + 1;
                }
            }
            size_t base_len = std::strlen(base_name);
            char* name_buf = new char[base_len + 1];
            std::memcpy(name_buf, base_name, base_len + 1);
            delete[] task->name;
            task->name = name_buf;
        }

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

        free_kernel_arg_env();
        task->wait_channel = "exec";
        task->deferredTaskSwitch = true;
        task->yieldSwitch = false;
        return 0;
    };

    // Try VFS_REF-based remote placement before loading the ELF locally. This
    // avoids submitter-side remote VFS reads for commands that will execute on
    // another node anyway.
    {
        task->elfBuffer = nullptr;
        task->elfBufferSize = 0;
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        std::memcpy(task->exe_path, path, path_len);
        task->exe_path[path_len] = '\0';

        ker::net::wki::WkiRemoteSpawnSpec remote_spawn = {
            .argv = k_argv,
            .envp = k_envp,
            .cwd = task->cwd,
        };
        auto remote_result = ker::net::wki::wki_try_remote_spawn(task, remote_spawn);
        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
            return finish_remote_exec();
        }

        task->elfBuffer = old_elf_buffer;
        task->elfBufferSize = old_elf_buffer_size;
        task->wki_skip_legacy_placement = old_skip_legacy_placement;
        std::memcpy(task->exe_path, old_exe_path.data(), old_exe_path.size());

        if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
            free_kernel_arg_env();
            return static_cast<uint64_t>(-EHOSTUNREACH);
        }
    }

    // --- Read the ELF file ---
    int fd = vfs::vfs_open(std::string_view(path, std::strlen(path)), 0, 0);
    if (fd < 0) {
        dbg::log("wos_proc_execve: Failed to open '%s' (fd=%d)", path, fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOENT);
    }

    int access_ret = vfs::vfs_access(path, 1 /* X_OK */);
    if (access_ret < 0) {
        dbg::log("wos_proc_execve: vfs_access X_OK failed for '%s' (ret=%d)", path, access_ret);
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EACCES);
    }

    ssize_t file_size = vfs::vfs_lseek(fd, 0, 2);
    if (file_size < 0) {
        dbg::log("wos_proc_execve: SEEK_END failed for '%s' (fileSize=%ld)", path, file_size);
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
        dbg::log("wos_proc_execve: alloc failed for '%s' (%ld bytes)", path, file_size);
        vfs::vfs_close(fd);
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOMEM);
    }

    ssize_t bytes_read = 0;
    vfs::vfs_read(fd, elf_buffer, file_size, (size_t*)&bytes_read);
    vfs::vfs_close(fd);

    if (bytes_read != file_size) {
        dbg::log("wos_proc_execve: short read for '%s' (got %ld, expect %ld)", path, bytes_read, file_size);
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EIO);
    }

    __asm__ volatile("mfence" ::: "memory");

    auto* elf_header = reinterpret_cast<Elf64_Ehdr*>(elf_buffer);
    if (elf_header->e_ident[EI_MAG0] != ELFMAG0 || elf_header->e_ident[EI_MAG1] != ELFMAG1 || elf_header->e_ident[EI_MAG2] != ELFMAG2 ||
        elf_header->e_ident[EI_MAG3] != ELFMAG3 || elf_header->e_ident[EI_CLASS] != ELFCLASS64) {
        dbg::log("wos_proc_execve: ELF magic check failed for '%s' (bytes: %02x %02x %02x %02x class=%02x)", path, elf_header->e_ident[0],
                 elf_header->e_ident[1], elf_header->e_ident[2], elf_header->e_ident[3], elf_header->e_ident[4]);
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-ENOEXEC);
    }

    task->elfBuffer = elf_buffer;
    task->elfBufferSize = static_cast<size_t>(file_size);
    {
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) {
            path_len = sched::task::Task::EXE_PATH_MAX - 1;
        }
        std::memcpy(task->exe_path, path, path_len);
        task->exe_path[path_len] = '\0';
    }

    ker::net::wki::WkiRemoteSpawnSpec remote_spawn = {
        .argv = k_argv,
        .envp = k_envp,
        .cwd = task->cwd,
    };
    auto remote_result = ker::net::wki::wki_try_remote_spawn(task, remote_spawn);
    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::REMOTE) {
        return finish_remote_exec();
    }

    task->elfBuffer = old_elf_buffer;
    task->elfBufferSize = old_elf_buffer_size;
    task->wki_skip_legacy_placement = old_skip_legacy_placement;
    std::memcpy(task->exe_path, old_exe_path.data(), old_exe_path.size());

    if (remote_result == ker::net::wki::WkiRemoteSpawnResult::FAILED) {
        delete[] elf_buffer;
        free_kernel_arg_env();
        return static_cast<uint64_t>(-EHOSTUNREACH);
    }

    // --- Close FD_CLOEXEC file descriptors (per-fd bitmap) ---
    task->fd_table.for_each([&](uint64_t key, void* val) {
        if (val == nullptr) {
            return;
        }
        if (task->get_fd_cloexec(static_cast<unsigned>(key))) {
            vfs::vfs_close(static_cast<int>(key));
        }
    });

    // --- Free old ELF buffer ---
    if (task->elfBuffer != nullptr) {
        if (!ker::net::wki::wki_remote_compute_release_elf_buffer(task->elfBuffer)) {
            delete[] task->elfBuffer;
        }
    }

    // --- Replace the pagemap with a fresh one ---
    // Note: We are executing in kernel context (syscall handler) so our
    // kernel mappings are active. We'll create a new user pagemap.
    auto* old_pagemap = task->pagemap;
    auto* new_pagemap = mm::virt::createPagemap();
    if (new_pagemap == nullptr) {
        delete[] elf_buffer;
        for (size_t i = 0; i < argv_count; i++) {
            delete[] k_argv[i];
        }
        delete[] k_argv;
        for (size_t i = 0; i < envp_count; i++) {
            delete[] k_envp[i];
        }
        delete[] k_envp;
        return static_cast<uint64_t>(-ENOMEM);
    }
    task->pagemap = new_pagemap;
    mm::virt::copyKernelMappings(task);

    // TODO: Properly tear down old pagemap user pages. For now we just leak
    // them - a full implementation would walk and free old user-space frames.
    (void)old_pagemap;

    // --- Create new thread (user stack + TLS) ---
    ker::loader::elf::TlsModule tls_info = loader::elf::extractTlsInfo((void*)(uint64_t)elf_buffer);
    auto* new_thread = mod::sched::threading::createThread(USER_STACK_SIZE, tls_info.tlsSize, new_pagemap, tls_info);
    if (new_thread == nullptr) {
        delete[] elf_buffer;
        for (size_t i = 0; i < argv_count; i++) {
            delete[] k_argv[i];
        }
        delete[] k_argv;
        for (size_t i = 0; i < envp_count; i++) {
            delete[] k_envp[i];
        }
        delete[] k_envp;
        return static_cast<uint64_t>(-ENOMEM);
    }

    // Free old thread if present
    // (note: the old thread's stack pages are in the old pagemap, already orphaned)
    task->thread = new_thread;

    // --- Load ELF into new pagemap ---
    loader::elf::ElfLoadResult elf_result =
        loader::elf::loadElf((loader::elf::ElfFile*)(uint64_t)elf_buffer, new_pagemap, task->pid, task->name);
    if (elf_result.entryPoint == 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF load failed for '%s'", path);
#endif
        delete[] elf_buffer;
        for (size_t i = 0; i < argv_count; i++) {
            delete[] k_argv[i];
        }
        delete[] k_argv;
        for (size_t i = 0; i < envp_count; i++) {
            delete[] k_envp[i];
        }
        delete[] k_envp;
        return static_cast<uint64_t>(-ENOEXEC);
    }

    task->entry = elf_result.entryPoint;
    task->programHeaderAddr = elf_result.programHeaderAddr;
    task->elfHeaderAddr = elf_result.elfHeaderAddr;
    task->programHeaderCount = elf_result.programHeaderCount;
    task->programHeaderEntSize = elf_result.programHeaderEntSize;
    task->elfBuffer = elf_buffer;
    task->elfBufferSize = file_size;
    task->interpBase = 0;

    // If the binary requests a dynamic linker (PT_INTERP), load it.
    if (elf_result.hasInterp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;

        int interp_fd = vfs::vfs_open(std::string_view(elf_result.interpPath, std::strlen(elf_result.interpPath)), 0, 0);
        if (interp_fd < 0) {
            dbg::log("wos_proc_execve: Failed to open interpreter '%s'", elf_result.interpPath);
            delete[] elf_buffer;
            free_kernel_arg_env();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        ssize_t interp_size = vfs::vfs_lseek(interp_fd, 0, 2);
        vfs::vfs_lseek(interp_fd, 0, 0);
        if (interp_size <= 0) {
            vfs::vfs_close(interp_fd);
            delete[] elf_buffer;
            free_kernel_arg_env();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        auto* interp_buf = new uint8_t[interp_size];
        ssize_t interp_read = 0;
        vfs::vfs_read(interp_fd, interp_buf, interp_size, (size_t*)&interp_read);
        vfs::vfs_close(interp_fd);

        if (interp_read != interp_size) {
            delete[] interp_buf;
            delete[] elf_buffer;
            free_kernel_arg_env();
            return static_cast<uint64_t>(-EIO);
        }

        loader::elf::ElfLoadResult interp_result =
            loader::elf::loadElf((loader::elf::ElfFile*)(uint64_t)interp_buf, new_pagemap, task->pid, "ld.so", false, INTERP_BASE);

        if (interp_result.entryPoint == 0) {
            delete[] interp_buf;
            delete[] elf_buffer;
            free_kernel_arg_env();
            return static_cast<uint64_t>(-ENOEXEC);
        }

        // Override entry point to the interpreter — ld.so reads AT_ENTRY from auxv
        elf_result.entryPoint = interp_result.entryPoint;
        task->interpBase = INTERP_BASE;

        delete[] interp_buf;
    }

    // Update executable path
    {
        size_t path_len = std::strlen(path);
        if (path_len >= sched::task::Task::EXE_PATH_MAX) path_len = sched::task::Task::EXE_PATH_MAX - 1;
        std::memcpy(task->exe_path, path, path_len);
        task->exe_path[path_len] = '\0';
    }

    // Update task name to the new executable's basename
    {
        std::string_view path_str(path, std::strlen(path));
        const char* base_name = path_str.data();
        for (size_t i = 0; i < path_str.size(); i++) {
            if (path_str[i] == '/') {
                base_name = path_str.data() + i + 1;
            }
        }
        size_t base_len = std::strlen(base_name);
        char* name_buf = new char[base_len + 1];
        std::memcpy(name_buf, base_name, base_len + 1);
        delete[] task->name;
        task->name = name_buf;
    }

    // Handle setuid/setgid from executable
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

    // Reset signals to default (POSIX: pending signals cleared, handlers reset)
    task->sigPending = 0;
    task->inSignalHandler = false;
    task->doSigreturn = false;
    for (auto& sh : task->sigHandlers) {
        sh = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};
    }

    // Ensure fds 0/1/2 exist
    for (unsigned i = 0; i < 3; ++i) {
        if (task->fd_table.lookup(i) == nullptr) {
            vfs::File* newFile = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (newFile != nullptr) {
                newFile->fops = vfs::devfs::get_devfs_fops();
                newFile->fd = static_cast<int>(i);
                newFile->refcount = 1;
                task->fd_table.insert(i, newFile);
            }
        }
    }

    // --- Set up the user stack with argv/envp/auxv ---
    uint64_t user_stack_virt = task->thread->stack;
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
    for (size_t i = 0; i < argv_count; i++) {
        argv_addrs[i] = push_string(k_argv[i]);
    }
    argv_addrs[argv_count] = 0;

    auto* envp_addrs = new uint64_t[envp_count + 1];
    for (size_t i = 0; i < envp_count; i++) {
        envp_addrs[i] = push_string(k_envp[i]);
    }
    envp_addrs[envp_count] = 0;

    // Free kernel copies of argv/envp strings
    free_kernel_arg_env();

    // Alignment
    {
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t current_addr = user_stack_virt - current_virt_offset;
        uint64_t aligned = current_addr & ~(ALIGNMENT - 1);
        current_virt_offset += (current_addr - aligned);

        size_t auxv_qwords = 14 + (task->interpBase != 0 ? 2 : 0);
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
        constexpr uint64_t AT_EHDR = 33;
        bool built_correct_auxv = true;
        ker::util::SmallVec<uint64_t, 16> auxv;
        built_correct_auxv &= auxv.push_back(AT_PAGESZ);
        built_correct_auxv &= auxv.push_back((uint64_t)mm::paging::PAGE_SIZE);
        built_correct_auxv &= auxv.push_back(AT_ENTRY);
        built_correct_auxv &= auxv.push_back(task->entry);
        built_correct_auxv &= auxv.push_back(AT_PHDR);
        built_correct_auxv &= auxv.push_back(task->programHeaderAddr);
        built_correct_auxv &= auxv.push_back(AT_PHENT);
        built_correct_auxv &= auxv.push_back(task->programHeaderEntSize);
        built_correct_auxv &= auxv.push_back(AT_PHNUM);
        built_correct_auxv &= auxv.push_back(task->programHeaderCount);
        built_correct_auxv &= auxv.push_back(AT_EHDR);
        built_correct_auxv &= auxv.push_back(task->elfHeaderAddr);
        if (task->interpBase != 0) {
            built_correct_auxv &= auxv.push_back(AT_BASE);
            built_correct_auxv &= auxv.push_back(task->interpBase);
        }
        built_correct_auxv &= auxv.push_back(AT_NULL);
        built_correct_auxv &= auxv.push_back(0);

        if (!built_correct_auxv) {
            dbg::log("wos_proc_execve: Failed to build auxv");
            delete[] envp_addrs;
            delete[] argv_addrs;
            delete new_thread;
            delete[] elf_buffer;
            return static_cast<uint64_t>(-ENOMEM);
        }

        for (int j = static_cast<int>(auxv.size()) - 1; j >= 0; j--) {
            uint64_t val = auxv[static_cast<size_t>(j)];
            push_to_stack(&val, sizeof(uint64_t));
        }
    }

    push_to_stack(envp_addrs, (envp_count + 1) * sizeof(uint64_t));
    delete[] envp_addrs;

    uint64_t argv_ptr = push_to_stack(argv_addrs, (argv_count + 1) * sizeof(uint64_t));
    delete[] argv_addrs;

    uint64_t argc = argv_count;
    push_to_stack(&argc, sizeof(uint64_t));

    // --- Set up the task context to jump to the new binary ---
    task->context.frame.rip = elf_result.entryPoint;
    task->context.frame.rsp = user_stack_virt - current_virt_offset;
    task->context.frame.ss = 0x1b;
    task->context.frame.cs = 0x23;
    task->context.frame.flags = 0x202;
    task->context.frame.intNum = 0;
    task->context.frame.errCode = 0;

    // Clear general purpose registers
    task->context.regs = cpu::GPRegs();
    task->context.regs.rdi = argc;
    task->context.regs.rsi = argv_ptr;

    // Initialize SafeStack TLS symbol if present
    auto* ssym = loader::debug::getProcessSymbol(task->pid, "__safestack_unsafe_stack_ptr");
    if ((ssym != nullptr) && ssym->isTlsOffset) {
        uint64_t dest_vaddr = task->thread->tlsBaseVirt + ssym->rawValue;
        uint64_t dest_paddr = mm::virt::translate(new_pagemap, dest_vaddr);
        if (dest_paddr != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = (uint64_t*)mm::addr::get_virt_pointer(dest_paddr);
            *dest_ptr = task->thread->safestackPtrValue;
        }
    }

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

    // Stack offsets (must match pushq order in syscall.asm / signal.cpp)
    constexpr int OFF_RCX = 0x60;
    constexpr int OFF_R11 = 0x20;
    constexpr int OFF_RDI = 0x48;
    constexpr int OFF_RSI = 0x50;

    uint64_t new_rsp = user_stack_virt - current_virt_offset;
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

    // 2. Patch the on-stack register slots that popq will restore.
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RCX) = elf_result.entryPoint;
    *reinterpret_cast<uint64_t*>(stack_base + OFF_R11) = 0x202;  // IF set
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RDI) = argc;
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RSI) = argv_ptr;

    // 3. Update PerCpu scratch area so sysret diagnostic check passes and
    //    the correct user RSP is restored.
    asm volatile("movq %0, %%gs:0x28" : : "r"(elf_result.entryPoint) : "memory");
    asm volatile("movq %0, %%gs:0x30" : : "r"((uint64_t)0x202) : "memory");
    asm volatile("movq %0, %%gs:0x08" : : "r"(new_rsp) : "memory");

    // 4. Switch CR3 to the new pagemap so user-space sees the new mappings.
    asm volatile("mov %0, %%cr3" : : "r"(phys_pagemap) : "memory");

    // Return 0.  The sysret path will pop the patched registers and jump to
    // the new entry point.
    return 0;
}

}  // namespace ker::syscall::process
