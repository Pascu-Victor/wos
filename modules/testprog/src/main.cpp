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
    (void)envp;
    std::println("testprog: main() called");
    int pid = ker::abi::multiproc::getcpunum();
    // Log argc
    std::println("testprog[{}]: argc = {}", pid, argc);
    // Log each argument
    for (int arg = 0; arg < argc; arg++) {
        std::println("testprog[{}]: argv[{}] = {}", pid, arg, argv[arg]);
    }

    FILE* fileptr = fopen("/mnt/disk/hello.txt", "r");
    if (fileptr == nullptr) {
        std::println("testprog[{}]: Failed to open /mnt/disk/hello.txt", pid);
    } else {
        std::println("testprog[{}]: Successfully opened /mnt/disk/hello.txt", pid);
        constexpr size_t buffer_size = 128;
        std::array<char, buffer_size> buffer = {0};
        size_t bytes_read = fread(buffer.data(), 1, buffer_size - 1, fileptr);
        if (bytes_read > 0) {
            std::println("testprog[{}]: Read {} bytes from file:", pid, bytes_read);
            std::println("testprog[{}]: {}", pid, buffer.data());
        } else {
            std::println("testprog[{}]: Failed to read from file", pid);
        }
        fclose(fileptr);
    }

    const auto* rootDir = "/mnt/disk";
    std::println("testprog[{}]: Attempting to open directory", pid);
    DIR* dirp = opendir(rootDir);
    if (dirp == nullptr) {
        std::println("testprog[{}]: Failed to open directory", pid);
    } else {
        std::println("testprog[{}]: Successfully opened directory", pid);
        std::println("testprog[{}]: files in {}:", pid, rootDir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(dirp)) != nullptr) {
            std::println("testprog[{}]: {}", pid, static_cast<const char*>(entry->d_name));
        }
        closedir(dirp);
    }

    const auto* bootdir = "/boot";
    std::println("testprog[{}]: Attempting to open directory", pid);
    DIR* bootdirp = opendir(bootdir);
    if (bootdirp == nullptr) {
        std::println("testprog[{}]: Failed to open directory", pid);
    } else {
        std::println("testprog[{}]: Successfully opened directory", pid);
        std::println("testprog[{}]: files in {}:", pid, bootdir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(bootdirp)) != nullptr) {
            std::println("testprog[{}]: {}", pid, static_cast<const char*>(entry->d_name));
        }
        closedir(dirp);
    }

    // attempt mmap
    void* addr = (void*)0x123456780000;
    size_t size = 0x2000;  // 8KB
    int prot = PROT_READ | PROT_WRITE;
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    auto mmap_result = reinterpret_cast<int64_t>(mmap(addr, size, prot, flags, -1, 0));
    if (mmap_result < 0) {
        std::println("testprog[{}]: mmap failed with error code {}", pid, mmap_result);
        return 1;
    }
    std::println("testprog[{}]: mmap succeeded at address {}", pid, addr);

    // gigantic number of mallocs to test memory allocation
    constexpr int num_allocs = 1000;
    constexpr size_t alloc_size = 4096;  // 4KB
    std::array<void*, num_allocs> allocations = {nullptr};
    int randodata = 0;
    for (int i = 0; i < num_allocs; i++) {
        allocations[i] = new char[alloc_size];
        if (allocations[i] == nullptr) {
            std::println("testprog[{}]: malloc #{} failed", pid, i);
            break;
        }
        // Write to the allocated memory to ensure it's usable
        memset(allocations[i], (randodata) ^ (uint64_t)(allocations[randodata % alloc_size]), alloc_size);
        randodata = (randodata + 37) % alloc_size;
    }
    std::println("testprog[{}]: Completed {} mallocs of size {} bytes", pid, num_allocs, alloc_size);

    return pid;
}
