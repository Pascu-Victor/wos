#include <abi/callnums/sys_log.h>
#include <dirent.h>
#include <sys/logging.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>

auto main(int argc, char** argv, char** envp) -> int {
    int a = *((int*)0x10 - 0x10);
    (void)a;
    int pid = ker::process::getpid();
    (void)envp;
    std::println("testprog: main() called");
    int tid = ker::multiproc::currentThreadId();
    // Log argc
    std::println("testprog[t:{},p:{}]: argc = {}", tid, pid, argc);
    // Log each argument
    for (int arg = 0; arg < argc; arg++) {
        std::println("testprog[t:{},p:{}]: argv[{}] = {}", tid, pid, arg, argv[arg]);
    }

    FILE* fileptr = fopen("/mnt/disk/hello.txt", "r");
    if (fileptr == nullptr) {
        std::println("testprog[t:{},p:{}]: Failed to open /mnt/disk/hello.txt", tid, pid);
    } else {
        std::println("testprog[t:{},p:{}]: Successfully opened /mnt/disk/hello.txt", tid, pid);
        constexpr size_t buffer_size = 128;
        std::array<char, buffer_size> buffer = {0};
        size_t bytes_read = fread(buffer.data(), 1, buffer_size - 1, fileptr);
        if (bytes_read > 0) {
            std::println("testprog[t:{},p:{}]: Read {} bytes from file:", tid, pid, bytes_read);
            std::println("testprog[t:{},p:{}]: {}", tid, pid, buffer.data());
        } else {
            std::println("testprog[t:{},p:{}]: Failed to read from file", tid, pid);
        }
        fclose(fileptr);
    }

    const auto* rootDir = "/mnt/disk";
    std::println("testprog[t:{},p:{}]: Attempting to open directory", tid, pid);
    DIR* dirp = opendir(rootDir);
    if (dirp == nullptr) {
        std::println("testprog[t:{},p:{}]: Failed to open directory", tid, pid);
    } else {
        std::println("testprog[t:{},p:{}]: Successfully opened directory", tid, pid);
        std::println("testprog[t:{},p:{}]: files in {}:", tid, pid, rootDir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(dirp)) != nullptr) {
            std::println("testprog[t:{},p:{}]: {}", tid, pid, static_cast<const char*>(entry->d_name));
        }
        closedir(dirp);
    }

    const auto* bootdir = "/boot";
    std::println("testprog[t:{},p:{}]: Attempting to open directory", tid, pid);
    DIR* bootdirp = opendir(bootdir);
    if (bootdirp == nullptr) {
        std::println("testprog[t:{},p:{}]: Failed to open directory", tid, pid);
    } else {
        std::println("testprog[t:{},p:{}]: Successfully opened directory", tid, pid);
        std::println("testprog[t:{},p:{}]: files in {}:", tid, pid, bootdir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(bootdirp)) != nullptr) {
            std::println("testprog[t:{},p:{}]: {}", tid, pid, static_cast<const char*>(entry->d_name));
        }
        closedir(bootdirp);  // Fixed: was closing wrong directory handle
    }

    // attempt mmap
    void* addr = (void*)0x123456780000;
    size_t size = 0x2000;  // 8KB
    int prot = PROT_READ | PROT_WRITE;
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    auto mmap_result = reinterpret_cast<int64_t>(mmap(addr, size, prot, flags, -1, 0));
    if (mmap_result < 0) {
        std::println("testprog[t:{},p:{}]: mmap failed with error code {}", tid, pid, mmap_result);
        return 1;
    }
    std::println("testprog[t:{},p:{}]: mmap succeeded at address {}", tid, pid, addr);

    // gigantic number of mallocs to test memory allocation
    constexpr int num_allocs = 1000;
    constexpr size_t alloc_size = 4096;  // 4KB
    std::array<void*, num_allocs> allocations = {nullptr};
    int randodata = 0;
    for (int i = 0; i < num_allocs; i++) {
        allocations[i] = new char[alloc_size];
        if (allocations[i] == nullptr) {
            std::println("testprog[t:{},p:{}]: malloc #{} failed", tid, pid, i);
            break;
        }
        // Write to the allocated memory to ensure it's usable
        memset(allocations[i], (randodata) ^ (uint64_t)(allocations[randodata % num_allocs]), alloc_size);
        randodata = (randodata + 37) % num_allocs;  // Fixed: use num_allocs not alloc_size
    }
    std::println("testprog[t:{},p:{}]: Completed {} mallocs of size {} bytes", tid, pid, num_allocs, alloc_size);

    return tid;
}
