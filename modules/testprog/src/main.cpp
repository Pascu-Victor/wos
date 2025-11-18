#include <abi/callnums/sys_log.h>
#include <dirent.h>
#include <sys/logging.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>

// Alternative main function for testing with argc/argv
auto main(int argc, char** argv, char** envp) -> int {
    (void)envp;
    std::println("testprog: main() called");
    // Log argc
    std::println("testprog: argc = {}", argc);
    // Log each argument
    for (int arg = 0; arg < argc; arg++) {
        std::println("testprog: argv[{}] = {}", arg, argv[arg]);
    }

    const auto* rootDir = "/dev";  // assuming FAT32 is mounted here
    std::println("testprog: Attempting to open root directory");
    DIR* dirp = opendir(rootDir);
    if (dirp == nullptr) {
        std::println("testprog: Failed to open root directory");
    } else {
        std::println("testprog: Successfully opened root directory");
        std::println("testprog: files in root directory:");
        struct dirent* entry = nullptr;
        while ((entry = readdir(dirp)) != nullptr) {
            std::println("{}", static_cast<const char*>(entry->d_name));
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
        std::println("testprog: mmap failed with error code {}", mmap_result);
        return 1;
    }
    std::println("testprog: mmap succeeded at address {}", addr);

    return 0;
}
