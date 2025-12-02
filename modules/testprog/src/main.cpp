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

auto main(int argc, char** argv, char** envp) -> int {
    (void)envp;
    std::println("testprog: main() called");
    // Log argc
    std::println("testprog: argc = {}", argc);
    // Log each argument
    for (int arg = 0; arg < argc; arg++) {
        std::println("testprog: argv[{}] = {}", arg, argv[arg]);
    }

    FILE* fileptr = fopen("/mnt/disk/hello.txt", "r");
    if (fileptr == nullptr) {
        std::println("testprog: Failed to open /mnt/disk/hello.txt");
    } else {
        std::println("testprog: Successfully opened /mnt/disk/hello.txt");
        constexpr size_t buffer_size = 128;
        std::array<char, buffer_size> buffer = {0};
        size_t bytes_read = fread(buffer.data(), 1, buffer_size - 1, fileptr);
        if (bytes_read > 0) {
            std::println("testprog: Read {} bytes from file:", bytes_read);
            std::println("{}", buffer.data());
        } else {
            std::println("testprog: Failed to read from file");
        }
        fclose(fileptr);
    }

    const auto* rootDir = "/mnt/disk";
    std::println("testprog: Attempting to open directory");
    DIR* dirp = opendir(rootDir);
    if (dirp == nullptr) {
        std::println("testprog: Failed to open directory");
    } else {
        std::println("testprog: Successfully opened directory");
        std::println("testprog: files in {}:", rootDir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(dirp)) != nullptr) {
            std::println("{}", static_cast<const char*>(entry->d_name));
        }
        closedir(dirp);
    }

    const auto* bootdir = "/boot";
    std::println("testprog: Attempting to open directory");
    DIR* bootdirp = opendir(bootdir);
    if (bootdirp == nullptr) {
        std::println("testprog: Failed to open directory");
    } else {
        std::println("testprog: Successfully opened directory");
        std::println("testprog: files in {}:", bootdir);
        struct dirent* entry = nullptr;
        while ((entry = readdir(bootdirp)) != nullptr) {
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

    return 1234;
}
