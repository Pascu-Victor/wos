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

    auto* parentTask = sched::getCurrentTask();
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
        dbg::log("wos_proc_exec: Failed to open file");
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

    auto* newTask = new sched::task::Task(processName, (uint64_t)elfBuffer, kernelRsp, sched::task::TaskType::PROCESS);
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

    // Re-open file descriptors for the child process instead of sharing File objects.
    // This prevents race conditions when multiple processes access the same File
    // (e.g., concurrent readdir calls racing on f->pos).
    // For fd 0, 1, 2 (stdin/stdout/stderr), re-open /dev/console.
    // Other file descriptors are not inherited for now.
    for (unsigned i = 0; i < 3; ++i) {
        if (parentTask->fds[i] != nullptr) {
            // Re-open /dev/console for stdin/stdout/stderr
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

    auto alignStack = [&]() {
        constexpr uint64_t alignment = 8;
        uint64_t currentAddr = userStackVirt - currentVirtOffset;
        uint64_t aligned = currentAddr & ~(alignment - 1);
        currentVirtOffset += (currentAddr - aligned);
    };

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

    auto* envpAddrs = new uint64_t[envpCount + 1];
    for (size_t i = 0; i < envpCount; i++) {
        envpAddrs[i] = pushString(envp[i]);
        if (envpAddrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push envp string");
            delete[] envpAddrs;
            delete newTask;
            delete[] elfBuffer;
            return 0;
        }
    }
    envpAddrs[envpCount] = 0;

    auto* argvAddrs = new uint64_t[argvCount + 1];
    for (size_t i = 0; i < argvCount; i++) {
        argvAddrs[i] = pushString(argv[i]);
        if (argvAddrs[i] == 0) {
            dbg::log("wos_proc_exec: Failed to push argv string");
            delete[] argvAddrs;
            delete[] envpAddrs;
            delete newTask;
            delete[] elfBuffer;
            return 0;
        }
    }
    argvAddrs[argvCount] = 0;

    alignStack();

    uint64_t envpPtr = pushToStack(envpAddrs, (envpCount + 1) * sizeof(uint64_t));
    delete[] envpAddrs;

    uint64_t argvPtr = pushToStack(argvAddrs, (argvCount + 1) * sizeof(uint64_t));
    delete[] argvAddrs;

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
    if (!sched::postTaskBalanced(newTask)) {
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

}  // namespace ker::syscall::process
