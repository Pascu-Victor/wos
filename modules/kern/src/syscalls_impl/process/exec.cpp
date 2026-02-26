#include "exec.hpp"

#include <extern/elf.h>

// #define EXEC_DEBUG

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <span>
#include <string_view>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::process {

namespace {
auto allocateKernelStack() -> uint64_t {
    auto stackBase = (uint64_t)ker::mod::mm::phys::pageAlloc(KERNEL_STACK_SIZE);
    if (stackBase == 0) {
        return 0;
    }

    return stackBase + KERNEL_STACK_SIZE;
}

}  // namespace

auto wos_proc_exec(const char* path, const char* const argv[], const char* const envp[]) -> uint64_t {
    std::string_view str(path, std::strlen(path));
    size_t argvCount = 0;
    if (argv != nullptr) {
        while (argv[argvCount] != nullptr) {
            argvCount++;
        }
    }

    size_t envpCount = 0;
    if (envp != nullptr) {
        while (envp[envpCount] != nullptr) {
            envpCount++;
        }
    }

    using namespace ker::mod;

    auto* parentTask = sched::get_current_task();
    if (parentTask == nullptr) {
        dbg::log("wos_proc_exec: No current task");
        return 0;
    }
    uint64_t parentPid = parentTask->pid;

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

    ssize_t fileSize = vfs::vfs_lseek(fd, 0, 2);
    if (fileSize <= 0) {
        dbg::log("wos_proc_exec: Invalid file size: %d", fileSize);
        vfs::vfs_close(fd);
        return 0;
    }
    vfs::vfs_lseek(fd, 0, 0);

    auto* elfBuffer = new uint8_t[fileSize];
    if (elfBuffer == nullptr) {
        dbg::log("wos_proc_exec: Failed to allocate buffer");
        vfs::vfs_close(fd);
        return 0;
    }

    ssize_t bytesRead = 0;
    vfs::vfs_read(fd, elfBuffer, fileSize, (size_t*)&bytesRead);
    vfs::vfs_close(fd);

    if (bytesRead != fileSize) {
        dbg::log("wos_proc_exec: Failed to read file completely");
        delete[] elfBuffer;
        return 0;
    }

    // Add memory barrier after reading to ensure visibility
    __asm__ volatile("mfence" ::: "memory");

    auto* elfHeader = reinterpret_cast<Elf64_Ehdr*>(elfBuffer);

    if (elfHeader->e_ident[EI_MAG0] != ELFMAG0 || elfHeader->e_ident[EI_MAG1] != ELFMAG1 || elfHeader->e_ident[EI_MAG2] != ELFMAG2 ||
        elfHeader->e_ident[EI_MAG3] != ELFMAG3) {
        dbg::log("wos_proc_exec: Not a valid ELF file");
        delete[] elfBuffer;
        return 0;
    }

    if (elfHeader->e_ident[EI_CLASS] != ELFCLASS64) {
        dbg::log("wos_proc_exec: Not a 64-bit ELF");
        delete[] elfBuffer;
        return 0;
    }

    const char* processName = str.data();
    for (size_t i = 0; i < str.size(); i++) {
        if (str[i] == '/') {
            processName = str.data() + i + 1;
        }
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Creating task for '%s', parent PID: %x", processName, parentPid);
#endif

    uint64_t kernelRsp = allocateKernelStack();
    if (kernelRsp == 0) {
        dbg::log("wos_proc_exec: Failed to allocate kernel stack");
        delete[] elfBuffer;
        return 0;
    }

    // DIAGNOSTIC: Detect stack corruption during Task constructor
    // Save known canary values on stack, check after constructor
    volatile uint64_t canary1 = 0xDEAD'BEEF'CAFE'BABEULL;
    volatile uint64_t canary2 = 0x1234'5678'9ABC'DEF0ULL;

    auto* newTask = new sched::task::Task(processName, (uint64_t)elfBuffer, kernelRsp, sched::task::TaskType::PROCESS);

    // Check canaries for stack corruption
    if (canary1 != 0xDEAD'BEEF'CAFE'BABEULL || canary2 != 0x1234'5678'9ABC'DEF0ULL) {
        dbg::log("STACK CORRUPTION DETECTED in exec!");
        dbg::log("  canary1=%lx (expect DEADBEEFCAFEBABE)", canary1);
        dbg::log("  canary2=%lx (expect 123456789ABCDEF0)", canary2);
        dbg::log("  newTask=%p, &canary1=%p, &canary2=%p", newTask, &canary1, &canary2);
        dbg::log("  stack RSP approx %p, kernelRsp=%lx", &newTask, kernelRsp);
    }

    // Also check if newTask is suspiciously not in HHDM range
    uintptr_t task_addr = reinterpret_cast<uintptr_t>(newTask);
    if (task_addr != 0 && (task_addr < 0xffff800000000000ULL || task_addr >= 0xffff900000000000ULL)) {
        dbg::log("EXEC BUG: operator new returned non-HHDM ptr: %p", newTask);
        dbg::log("  expected range: 0xffff800000000000 - 0xffff900000000000");
        dbg::log("  &newTask on stack = %p, kernelRsp = %lx", &newTask, kernelRsp);
        delete[] elfBuffer;
        return 0;
    }

    if (newTask == nullptr || newTask->thread == nullptr || newTask->pagemap == nullptr) {
        dbg::log("wos_proc_exec: Failed to create task (OOM during thread/pagemap allocation)");
        if (newTask != nullptr) {
            delete newTask;
        }
        // TODO: Free kernel stack pages
        delete[] elfBuffer;
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task constructor completed successfully");
    dbg::log("wos_proc_exec: Entry point = 0x%x, RIP = 0x%x", newTask->entry, newTask->context.frame.rip);
#endif

    newTask->parentPid = parentPid;

    // Inherit file descriptors from parent, respecting O_CLOEXEC / FD_CLOEXEC.
    // FDs with FD_CLOEXEC set are NOT inherited (closed on exec).
    // FDs without FD_CLOEXEC are inherited by incrementing refcount.
    // For fds 0/1/2 (stdin/stdout/stderr), if not inherited, re-open /dev/console.
    for (unsigned i = 0; i < mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (parentTask->fds[i] == nullptr) continue;
        auto* parentFile = static_cast<vfs::File*>(parentTask->fds[i]);

        if (parentFile->fd_flags & vfs::FD_CLOEXEC) {
            // FD_CLOEXEC is set — do NOT inherit
            continue;
        }

        // Inherit: increment refcount and share the File object
        parentFile->refcount++;
        newTask->fds[i] = parentFile;
    }

    // Ensure fds 0/1/2 are always set (open /dev/console if not inherited)
    for (unsigned i = 0; i < 3; ++i) {
        if (newTask->fds[i] == nullptr) {
            vfs::File* newFile = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (newFile != nullptr) {
                newFile->fops = vfs::devfs::get_devfs_fops();
                newFile->fd = static_cast<int>(i);
                newFile->refcount = 1;
                newTask->fds[i] = newFile;
            }
        }
    }

    newTask->elfBuffer = elfBuffer;
    newTask->elfBufferSize = fileSize;

    // Store executable path for /proc/self/exe
    {
        size_t pathLen = std::strlen(path);
        if (pathLen >= sched::task::Task::EXE_PATH_MAX) {
            pathLen = sched::task::Task::EXE_PATH_MAX - 1;
        }
        memcpy(newTask->exe_path, path, pathLen);
        newTask->exe_path[pathLen] = '\0';
    }

    // Handle setuid/setgid bits from the executable
    {
        vfs::stat exec_st{};
        if (vfs::vfs_stat(path, &exec_st) == 0) {
            if (exec_st.st_mode & 04000) {  // S_ISUID
                newTask->euid = exec_st.st_uid;
                newTask->suid = exec_st.st_uid;
            }
            if (exec_st.st_mode & 02000) {  // S_ISGID
                newTask->egid = exec_st.st_gid;
                newTask->sgid = exec_st.st_gid;
            }
        }
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Task created with PID: %x, parent: %x", newTask->pid, newTask->parentPid);
#endif

    uint64_t userStackVirt = newTask->thread->stack;

    uint64_t currentVirtOffset = 0;

    auto pushToStack = [&](const void* data, size_t size) -> uint64_t {
        if (currentVirtOffset + size > USER_STACK_SIZE) {
            return 0;  // Stack overflow
        }
        currentVirtOffset += size;
        uint64_t virtAddr = userStackVirt - currentVirtOffset;

        uint64_t pageVirt = virtAddr & ~(mod::mm::paging::PAGE_SIZE - 1);
        uint64_t pageOffset = virtAddr & (mod::mm::paging::PAGE_SIZE - 1);

        uint64_t pagePhys = mod::mm::virt::translate(newTask->pagemap, pageVirt);
        if (pagePhys == 0) {
            return 0;
        }

        auto* destPtr = reinterpret_cast<uint8_t*>(mod::mm::addr::getVirtPointer(pagePhys)) + pageOffset;
        std::memcpy(destPtr, data, size);

        return virtAddr;
    };

    auto pushString = [&](std::string_view str) -> uint64_t {
        size_t len = str.size() + 1;  // Include null terminator
        if (currentVirtOffset + len > USER_STACK_SIZE) {
            return 0;
        }
        currentVirtOffset += len;
        uint64_t virtAddr = userStackVirt - currentVirtOffset;

        uint64_t pageVirt = virtAddr & ~(mod::mm::paging::PAGE_SIZE - 1);
        uint64_t pageOffset = virtAddr & (mod::mm::paging::PAGE_SIZE - 1);

        uint64_t pagePhys = mod::mm::virt::translate(newTask->pagemap, pageVirt);
        if (pagePhys == 0) {
            return 0;
        }

        auto* destPtr = reinterpret_cast<uint8_t*>(mod::mm::addr::getVirtPointer(pagePhys)) + pageOffset;
        std::memcpy(destPtr, str.data(), str.size());
        destPtr[str.size()] = '\0';

        return virtAddr;
    };

    // Push argv strings first (highest addresses on stack)
    auto* argvAddrs = new uint64_t[argvCount + 1];
    for (size_t i = 0; i < argvCount; i++) {
        argvAddrs[i] = pushString(argv[i]);
        if (argvAddrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push argv string");
            delete[] argvAddrs;
            delete newTask;
            delete[] elfBuffer;
            return 0;
        }
    }
    argvAddrs[argvCount] = 0;

    // Push envp strings
    auto* envpAddrs = new uint64_t[envpCount + 1];
    for (size_t i = 0; i < envpCount; i++) {
        envpAddrs[i] = pushString(envp[i]);
        if (envpAddrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push envp string");
            delete[] envpAddrs;
            delete[] argvAddrs;
            delete newTask;
            delete[] elfBuffer;
            return 0;
        }
    }
    envpAddrs[envpCount] = 0;

    // Align to 16 bytes after string data, accounting for structured data parity.
    // Structured data: auxv (10 qwords) + envp array + argv array + argc.
    // Total structured qwords = 10 + (envpCount+1) + (argvCount+1) + 1 = envpCount + argvCount + 13.
    // If that count is odd, we need 8 extra bytes of padding so rsp ends 16-byte aligned.
    {
        constexpr uint64_t alignment = 16;
        uint64_t currentAddr = userStackVirt - currentVirtOffset;
        uint64_t aligned = currentAddr & ~(alignment - 1);
        currentVirtOffset += (currentAddr - aligned);

        size_t structuredQwords = 10 + (envpCount + 1) + (argvCount + 1) + 1;
        if (structuredQwords % 2 != 0) {
            // Add 8 bytes padding so final rsp is 16-byte aligned
            uint64_t pad = 0;
            pushToStack(&pad, sizeof(uint64_t));
        }
    }

    // Push auxv (System V ABI: auxv sits between envp NULL terminator and string data)
    {
        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_EHDR = 33;

        std::array<uint64_t, 10> auxvEntries = {AT_PAGESZ, (uint64_t)mod::mm::paging::PAGE_SIZE,
                                                AT_ENTRY,  newTask->entry,
                                                AT_PHDR,   newTask->programHeaderAddr,
                                                AT_EHDR,   newTask->elfHeaderAddr,
                                                AT_NULL,   0};

        for (int j = static_cast<int>(auxvEntries.size()) - 1; j >= 0; j--) {
            uint64_t val = auxvEntries[static_cast<size_t>(j)];
            pushToStack(&val, sizeof(uint64_t));
        }
    }

    // Push envp pointer array (with NULL terminator)
    uint64_t envpPtr = pushToStack(envpAddrs, (envpCount + 1) * sizeof(uint64_t));
    delete[] envpAddrs;

    // Push argv pointer array (with NULL terminator)
    uint64_t argvPtr = pushToStack(argvAddrs, (argvCount + 1) * sizeof(uint64_t));
    delete[] argvAddrs;

    // Push argc last (rsp will point here)
    uint64_t argc = argvCount;
    pushToStack(&argc, sizeof(uint64_t));

    newTask->context.frame.rsp = userStackVirt - currentVirtOffset;

    newTask->context.regs.rdi = argc;
    newTask->context.regs.rsi = argvPtr;
    newTask->context.regs.rdx = envpPtr;

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Setup stack - argc=%d, argv=0x%x, envp=0x%x, rsp=0x%x", argc, argvPtr, envpPtr, newTask->context.frame.rsp);
    dbg::log("wos_proc_exec: Entry point (RIP) = 0x%x", newTask->context.frame.rip);
    dbg::log("wos_proc_exec: Task entry field = 0x%x", newTask->entry);
#endif

    // Use load-balanced task posting to distribute across CPUs
    if (!sched::post_task_balanced(newTask)) {
        dbg::log("wos_proc_exec: Failed to post task to scheduler");
        delete newTask;
        delete[] elfBuffer;
        return 0;
    }

#ifdef EXEC_DEBUG
    dbg::log("wos_proc_exec: Successfully posted task '%s' to CPU %d", processName, newTask->cpu);
#endif

    return newTask->pid;

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

    // --- Read the ELF file ---
    int fd = vfs::vfs_open(std::string_view(path, std::strlen(path)), 0, 0);
    if (fd < 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: Failed to open '%s'", path);
#endif
        return static_cast<uint64_t>(-ENOENT);
    }

    int access_ret = vfs::vfs_access(path, 1 /* X_OK */);
    if (access_ret < 0) {
        vfs::vfs_close(fd);
        return static_cast<uint64_t>(-EACCES);
    }

    ssize_t fileSize = vfs::vfs_lseek(fd, 0, 2);
    if (fileSize <= 0) {
        vfs::vfs_close(fd);
        return static_cast<uint64_t>(-ENOEXEC);
    }
    vfs::vfs_lseek(fd, 0, 0);

    auto* elfBuffer = new uint8_t[fileSize];
    if (elfBuffer == nullptr) {
        vfs::vfs_close(fd);
        return static_cast<uint64_t>(-ENOMEM);
    }

    ssize_t bytesRead = 0;
    vfs::vfs_read(fd, elfBuffer, fileSize, (size_t*)&bytesRead);
    vfs::vfs_close(fd);

    if (bytesRead != fileSize) {
        delete[] elfBuffer;
        return static_cast<uint64_t>(-EIO);
    }

    __asm__ volatile("mfence" ::: "memory");

    auto* elfHeader = reinterpret_cast<Elf64_Ehdr*>(elfBuffer);
    if (elfHeader->e_ident[EI_MAG0] != ELFMAG0 || elfHeader->e_ident[EI_MAG1] != ELFMAG1 || elfHeader->e_ident[EI_MAG2] != ELFMAG2 ||
        elfHeader->e_ident[EI_MAG3] != ELFMAG3 || elfHeader->e_ident[EI_CLASS] != ELFCLASS64) {
        delete[] elfBuffer;
        return static_cast<uint64_t>(-ENOEXEC);
    }

    // --- Copy argv/envp strings into kernel memory (before we destroy user mappings) ---
    size_t argvCount = 0;
    if (argv != nullptr) {
        while (argv[argvCount] != nullptr) argvCount++;
    }
    size_t envpCount = 0;
    if (envp != nullptr) {
        while (envp[envpCount] != nullptr) envpCount++;
    }

    // Deep-copy strings to kernel heap
    auto** kArgv = new char*[argvCount + 1];
    for (size_t i = 0; i < argvCount; i++) {
        size_t len = std::strlen(argv[i]);
        kArgv[i] = new char[len + 1];
        std::memcpy(kArgv[i], argv[i], len + 1);
    }
    kArgv[argvCount] = nullptr;

    auto** kEnvp = new char*[envpCount + 1];
    for (size_t i = 0; i < envpCount; i++) {
        size_t len = std::strlen(envp[i]);
        kEnvp[i] = new char[len + 1];
        std::memcpy(kEnvp[i], envp[i], len + 1);
    }
    kEnvp[envpCount] = nullptr;

    // --- Close FD_CLOEXEC file descriptors ---
    for (unsigned i = 0; i < sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (task->fds[i] == nullptr) continue;
        auto* file = static_cast<vfs::File*>(task->fds[i]);
        if (file->fd_flags & vfs::FD_CLOEXEC) {
            vfs::vfs_close(static_cast<int>(i));
            task->fds[i] = nullptr;
        }
    }

    // --- Free old ELF buffer ---
    if (task->elfBuffer != nullptr) {
        delete[] task->elfBuffer;
    }

    // --- Replace the pagemap with a fresh one ---
    // Note: We are executing in kernel context (syscall handler) so our
    // kernel mappings are active. We'll create a new user pagemap.
    auto* oldPagemap = task->pagemap;
    auto* newPagemap = mm::virt::createPagemap();
    if (newPagemap == nullptr) {
        delete[] elfBuffer;
        for (size_t i = 0; i < argvCount; i++) delete[] kArgv[i];
        delete[] kArgv;
        for (size_t i = 0; i < envpCount; i++) delete[] kEnvp[i];
        delete[] kEnvp;
        return static_cast<uint64_t>(-ENOMEM);
    }
    task->pagemap = newPagemap;
    mm::virt::copyKernelMappings(task);

    // TODO: Properly tear down old pagemap user pages. For now we just leak
    // them — a full implementation would walk and free old user-space frames.
    (void)oldPagemap;

    // --- Create new thread (user stack + TLS) ---
    ker::loader::elf::TlsModule tlsInfo = loader::elf::extractTlsInfo((void*)(uint64_t)elfBuffer);
    auto* newThread = mod::sched::threading::createThread(USER_STACK_SIZE, tlsInfo.tlsSize, newPagemap, tlsInfo);
    if (newThread == nullptr) {
        delete[] elfBuffer;
        for (size_t i = 0; i < argvCount; i++) delete[] kArgv[i];
        delete[] kArgv;
        for (size_t i = 0; i < envpCount; i++) delete[] kEnvp[i];
        delete[] kEnvp;
        return static_cast<uint64_t>(-ENOMEM);
    }

    // Free old thread if present
    // (note: the old thread's stack pages are in the old pagemap, already orphaned)
    task->thread = newThread;

    // --- Load ELF into new pagemap ---
    loader::elf::ElfLoadResult elfResult =
        loader::elf::loadElf((loader::elf::ElfFile*)(uint64_t)elfBuffer, newPagemap, task->pid, task->name);
    if (elfResult.entryPoint == 0) {
#ifdef EXEC_DEBUG
        dbg::log("wos_proc_execve: ELF load failed for '%s'", path);
#endif
        delete[] elfBuffer;
        for (size_t i = 0; i < argvCount; i++) delete[] kArgv[i];
        delete[] kArgv;
        for (size_t i = 0; i < envpCount; i++) delete[] kEnvp[i];
        delete[] kEnvp;
        return static_cast<uint64_t>(-ENOEXEC);
    }

    task->entry = elfResult.entryPoint;
    task->programHeaderAddr = elfResult.programHeaderAddr;
    task->elfHeaderAddr = elfResult.elfHeaderAddr;
    task->elfBuffer = elfBuffer;
    task->elfBufferSize = fileSize;

    // Update executable path
    {
        size_t pathLen = std::strlen(path);
        if (pathLen >= sched::task::Task::EXE_PATH_MAX) pathLen = sched::task::Task::EXE_PATH_MAX - 1;
        std::memcpy(task->exe_path, path, pathLen);
        task->exe_path[pathLen] = '\0';
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
        if (task->fds[i] == nullptr) {
            vfs::File* newFile = vfs::devfs::devfs_open_path("/dev/console", 0, 0);
            if (newFile != nullptr) {
                newFile->fops = vfs::devfs::get_devfs_fops();
                newFile->fd = static_cast<int>(i);
                newFile->refcount = 1;
                task->fds[i] = newFile;
            }
        }
    }

    // --- Set up the user stack with argv/envp/auxv ---
    uint64_t userStackVirt = task->thread->stack;
    uint64_t currentVirtOffset = 0;

    auto pushToStack = [&](const void* data, size_t size) -> uint64_t {
        if (currentVirtOffset + size > USER_STACK_SIZE) return 0;
        currentVirtOffset += size;
        uint64_t virtAddr = userStackVirt - currentVirtOffset;
        uint64_t pageVirt = virtAddr & ~(mm::paging::PAGE_SIZE - 1);
        uint64_t pageOffset = virtAddr & (mm::paging::PAGE_SIZE - 1);
        uint64_t pagePhys = mm::virt::translate(newPagemap, pageVirt);
        if (pagePhys == 0) return 0;
        auto* destPtr = reinterpret_cast<uint8_t*>(mm::addr::getVirtPointer(pagePhys)) + pageOffset;
        std::memcpy(destPtr, data, size);
        return virtAddr;
    };

    auto pushString = [&](const char* str) -> uint64_t {
        size_t len = std::strlen(str) + 1;
        if (currentVirtOffset + len > USER_STACK_SIZE) return 0;
        currentVirtOffset += len;
        uint64_t virtAddr = userStackVirt - currentVirtOffset;
        uint64_t pageVirt = virtAddr & ~(mm::paging::PAGE_SIZE - 1);
        uint64_t pageOffset = virtAddr & (mm::paging::PAGE_SIZE - 1);
        uint64_t pagePhys = mm::virt::translate(newPagemap, pageVirt);
        if (pagePhys == 0) return 0;
        auto* destPtr = reinterpret_cast<uint8_t*>(mm::addr::getVirtPointer(pagePhys)) + pageOffset;
        std::memcpy(destPtr, str, len);
        return virtAddr;
    };

    auto* argvAddrs = new uint64_t[argvCount + 1];
    for (size_t i = 0; i < argvCount; i++) {
        argvAddrs[i] = pushString(kArgv[i]);
    }
    argvAddrs[argvCount] = 0;

    auto* envpAddrs = new uint64_t[envpCount + 1];
    for (size_t i = 0; i < envpCount; i++) {
        envpAddrs[i] = pushString(kEnvp[i]);
    }
    envpAddrs[envpCount] = 0;

    // Free kernel copies of argv/envp strings
    for (size_t i = 0; i < argvCount; i++) delete[] kArgv[i];
    delete[] kArgv;
    for (size_t i = 0; i < envpCount; i++) delete[] kEnvp[i];
    delete[] kEnvp;

    // Alignment
    {
        constexpr uint64_t alignment = 16;
        uint64_t currentAddr = userStackVirt - currentVirtOffset;
        uint64_t aligned = currentAddr & ~(alignment - 1);
        currentVirtOffset += (currentAddr - aligned);

        size_t structuredQwords = 10 + (envpCount + 1) + (argvCount + 1) + 1;
        if (structuredQwords % 2 != 0) {
            uint64_t pad = 0;
            pushToStack(&pad, sizeof(uint64_t));
        }
    }

    // auxv
    {
        constexpr uint64_t AT_NULL = 0, AT_PAGESZ = 6, AT_ENTRY = 9, AT_PHDR = 3, AT_EHDR = 33;
        std::array<uint64_t, 10> auxvEntries = {AT_PAGESZ, (uint64_t)mm::paging::PAGE_SIZE,
                                                AT_ENTRY,  task->entry,
                                                AT_PHDR,   task->programHeaderAddr,
                                                AT_EHDR,   task->elfHeaderAddr,
                                                AT_NULL,   0};
        for (int j = static_cast<int>(auxvEntries.size()) - 1; j >= 0; j--) {
            uint64_t val = auxvEntries[static_cast<size_t>(j)];
            pushToStack(&val, sizeof(uint64_t));
        }
    }

    pushToStack(envpAddrs, (envpCount + 1) * sizeof(uint64_t));
    delete[] envpAddrs;

    uint64_t argvPtr = pushToStack(argvAddrs, (argvCount + 1) * sizeof(uint64_t));
    delete[] argvAddrs;

    uint64_t argc = argvCount;
    pushToStack(&argc, sizeof(uint64_t));

    // --- Set up the task context to jump to the new binary ---
    task->context.frame.rip = elfResult.entryPoint;
    task->context.frame.rsp = userStackVirt - currentVirtOffset;
    task->context.frame.ss = 0x1b;
    task->context.frame.cs = 0x23;
    task->context.frame.flags = 0x202;
    task->context.frame.intNum = 0;
    task->context.frame.errCode = 0;

    // Clear general purpose registers
    task->context.regs = cpu::GPRegs();
    task->context.regs.rdi = argc;
    task->context.regs.rsi = argvPtr;

    // Initialize SafeStack TLS symbol if present
    auto* ssym = loader::debug::getProcessSymbol(task->pid, "__safestack_unsafe_stack_ptr");
    if (ssym && ssym->isTlsOffset) {
        uint64_t destVaddr = task->thread->tlsBaseVirt + ssym->rawValue;
        uint64_t destPaddr = mm::virt::translate(newPagemap, destVaddr);
        if (destPaddr != 0) {
            auto* destPtr = (uint64_t*)mm::addr::getVirtPointer(destPaddr);
            *destPtr = task->thread->safestackPtrValue;
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
    //    `pushq` (15 regs × 8 = 120 bytes), RSP = K-128.
    //    The GPRegs struct maps directly to K-128 (r15 at offset 0, rax at 0x70).
    //    The compiler accesses this as a stack-passed MEMORY-class parameter at
    //    the callee's rbp+0x10 = K-128.
    uint64_t kernStackTop;
    asm volatile("movq %%gs:0x0, %0" : "=r"(kernStackTop));
    uint8_t* stack_base = reinterpret_cast<uint8_t*>(kernStackTop - 128);

    // Stack offsets (must match pushq order in syscall.asm / signal.cpp)
    constexpr int OFF_RCX = 0x60;
    constexpr int OFF_R11 = 0x20;
    constexpr int OFF_RDI = 0x48;
    constexpr int OFF_RSI = 0x50;

    uint64_t newRsp = userStackVirt - currentVirtOffset;
#ifdef EXEC_DEBUG
    // Log BEFORE patching the stack — dbg::log uses the stack and would
    // clobber the patched register slots if called after.
    dbg::log("wos_proc_execve: PID %x now running '%s' (entry 0x%lx, rsp 0x%lx)", task->pid, task->exe_path, elfResult.entryPoint, newRsp);
#endif
    // Compute physical pagemap address before we enter the critical section
    auto physPagemap = (uint64_t)mm::addr::getPhysPointer((uint64_t)newPagemap);

    // === CRITICAL SECTION: No function calls below this point! ===
    // Any function call (including dbg::log) would use the kernel stack
    // and overwrite the patched register values.

    // 2. Patch the on-stack register slots that popq will restore.
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RCX) = elfResult.entryPoint;
    *reinterpret_cast<uint64_t*>(stack_base + OFF_R11) = 0x202;  // IF set
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RDI) = argc;
    *reinterpret_cast<uint64_t*>(stack_base + OFF_RSI) = argvPtr;

    // 3. Update PerCpu scratch area so sysret diagnostic check passes and
    //    the correct user RSP is restored.
    asm volatile("movq %0, %%gs:0x28" : : "r"((uint64_t)elfResult.entryPoint) : "memory");
    asm volatile("movq %0, %%gs:0x30" : : "r"((uint64_t)0x202) : "memory");
    asm volatile("movq %0, %%gs:0x08" : : "r"(newRsp) : "memory");

    // 4. Switch CR3 to the new pagemap so user-space sees the new mappings.
    asm volatile("mov %0, %%cr3" : : "r"(physPagemap) : "memory");

    // Return 0.  The sysret path will pop the patched registers and jump to
    // the new entry point.
    return 0;
}

}  // namespace ker::syscall::process
