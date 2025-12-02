#include <sys/logging.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <print>
#include <span>
#include <string_view>

#include "bits/ssize_t.h"
#include "callnums/sys_log.h"
#include "sys/callnums.h"

namespace {
constexpr size_t buffer_size = 64;
const char* const text = "hello-vfs";
}  // namespace

auto main() -> int {
    // Test: Basic tmpfs operations
    std::println("init: TEST: TMPFS Basic Operations");

    const char* const path = "/test";
    int fd = ker::abi::vfs::open(path, 0, 0);
    if (fd < 0) {
        std::println("init: open failed");
    } else {
        ker::abi::vfs::write(fd, text, strlen(text));
        std::println("init: wrote to /test");
        ker::abi::vfs::close(fd);
        std::println("init: closed fd for /test");
        fd = ker::abi::vfs::open(path, 0, 0);
        std::println("init: re-opened /test");
        std::array<char, buffer_size> buf = {0};
        ker::abi::vfs::read(fd, buf.data(), sizeof(buf) - 1);
        std::println("init: read buffer");
        std::println("init: buffer was: '{}'", buf.data());
        ker::abi::vfs::close(fd);
    }

    std::println("init: TMPFS test complete");

    // Test: FAT32 Mount
    std::println("init: TEST: FAT32 Filesystem Access");
    std::println("init: Attempting to open /mnt/disk/hello.txt");

    // kernel automatically mounts FAT32 at /mnt/disk if ATA device is found
    // Try to open and read from the mounted filesystem
    int fd_disk = ker::abi::vfs::open("/mnt/disk/hello.txt", 0, 0);

    if (fd_disk >= 0) {
        std::println("init: Successfully opened /mnt/disk/hello.txt!");

        constexpr size_t file_buf_size = 256;
        std::array<char, file_buf_size> disk_buf = {0};
        ssize_t bytes_read = ker::abi::vfs::read(fd_disk, disk_buf.data(), file_buf_size - 1);

        if (bytes_read > 0) {
            std::println("init: FAT32 File content: '{}'", disk_buf.data());
        } else {
            std::println("init: Failed to read from file");
        }

        // attempt to write to the file
        const char* write_text = "\nAppended by init process.";
        ssize_t bytes_written = ker::abi::vfs::write(fd_disk, write_text, strlen(write_text));
        if (bytes_written > 0) {
            std::println("init: Successfully wrote to /mnt/disk/hello.txt");
        } else {
            std::println("init: Failed to write to file (expected if filesystem is read-only)");
        }
        ker::abi::vfs::close(fd_disk);
    } else {
        std::println("init: Failed to open /mnt/disk/hello.txt (device not mounted or file missing)");
    }

    // Test: Process execution
    std::println("init: TEST: Process Execution");
    std::println("init: Testing process exec");
    int pids[30000] = {};
    for (int& pid : pids) {
        const char* progPath = "/mnt/disk/testprog";
        std::array<const char*, 4> argv = {"/mnt/disk/testprog", "arg1", "arg2", nullptr};
        std::array<const char*, 1> envp = {nullptr};

        std::println("init: Calling exec");

        uint64_t child_pid = ker::process::exec(progPath, argv.data(), envp.data());
        pid = static_cast<int>(child_pid);
        if (child_pid < 0) {
            std::println("init: exec failed (this is expected if testprog not in VFS)");
        } else {
            std::println("init: exec succeeded!");
        }
    }
    for (int& pid : pids) {
        int child_exit_code = 0;
        ker::process::waitpid(pid, &child_exit_code, 0);
        std::println("init: Child process {} exited with code {}", pid, child_exit_code);
    }
    std::println("init: All tests complete, looping...");

    // Loop forever
    // TODO: replace with idle process, init should not busy-wait but cannot exit
    while (true) {
        asm volatile("pause");
    }

    return 0;
}
