#include <sys/logging.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "bits/ssize_t.h"
#include "callnums/sys_log.h"
#include "sys/callnums.h"

namespace ker::logging {

auto log(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    if (str == nullptr) {
        return 1;
    }
    if (len == 0) {
        len = std::strlen(str);
    }
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::log),
                   static_cast<uint64_t>(reinterpret_cast<uintptr_t>(str)), len, static_cast<uint64_t>(device));
}

auto log(const char* str, abi::sys_log::sys_log_device device = abi::sys_log::sys_log_device::serial) -> uint64_t {
    if (str == nullptr) {
        return 1;
    }
    uint64_t len = std::strlen(str);
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::log),
                   static_cast<uint64_t>(reinterpret_cast<uintptr_t>(str)), len, static_cast<uint64_t>(device));
}

auto logLine(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    if (str == nullptr) {
        return 1;
    }
    if (len == 0) {
        len = std::strlen(str);
    }
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::logLine),
                   static_cast<uint64_t>(reinterpret_cast<uintptr_t>(str)), len, static_cast<uint64_t>(device));
}

auto logLine(const char* str, abi::sys_log::sys_log_device device = abi::sys_log::sys_log_device::serial) -> uint64_t {
    if (str == nullptr) {
        return 1;
    }
    uint64_t len = std::strlen(str);
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::logLine),
                   static_cast<uint64_t>(reinterpret_cast<uintptr_t>(str)), len, static_cast<uint64_t>(device));
}

}  // namespace ker::logging
namespace {
constexpr size_t buffer_size = 64;
const char* const text = "hello-vfs";
}  // namespace

auto main() -> int {
    // Test: Basic tmpfs operations
    ker::logging::logLine("init: TEST: TMPFS Basic Operations");

    const char* const path = "/test";
    int fd = ker::abi::vfs::open(path, 0, 0);
    if (fd < 0) {
        ker::logging::logLine("init: open failed");
    } else {
        ker::abi::vfs::write(fd, text, strlen(text));
        ker::logging::logLine("init: wrote to /test");
        ker::abi::vfs::close(fd);
        ker::logging::logLine("init: closed fd for /test");
        fd = ker::abi::vfs::open(path, 0, 0);
        ker::logging::logLine("init: re-opened /test");
        std::array<char, buffer_size> buf = {0};
        ker::abi::vfs::read(fd, buf.data(), sizeof(buf) - 1);
        ker::logging::logLine("init: read buffer");
        ker::logging::log("init: buffer was: '");
        ker::logging::log(buf.data(), strlen(buf.data()), ker::abi::sys_log::sys_log_device::serial);
        ker::logging::logLine("'");
        ker::abi::vfs::close(fd);
    }

    ker::logging::logLine("init: TMPFS test complete");

    // Test: FAT32 Mount
    ker::logging::logLine("init: TEST: FAT32 Filesystem Access");
    ker::logging::logLine("init: Attempting to open /mnt/disk/hello.txt");

    // kernel automatically mounts FAT32 at /mnt/disk if ATA device is found
    // Try to open and read from the mounted filesystem
    int fd_disk = ker::abi::vfs::open("/mnt/disk/hello.txt", 0, 0);

    if (fd_disk >= 0) {
        ker::logging::logLine("init: Successfully opened /mnt/disk/hello.txt!");

        constexpr size_t file_buf_size = 256;
        std::array<char, file_buf_size> disk_buf = {0};
        ssize_t bytes_read = ker::abi::vfs::read(fd_disk, disk_buf.data(), file_buf_size - 1);

        if (bytes_read > 0) {
            ker::logging::log("init: FAT32 File content: '");
            ker::logging::log(disk_buf.data(), bytes_read, ker::abi::sys_log::sys_log_device::serial);
            ker::logging::logLine("'");
        } else {
            ker::logging::logLine("init: Failed to read from file");
        }
        ker::abi::vfs::close(fd_disk);
    } else {
        ker::logging::logLine("init: Failed to open /mnt/disk/hello.txt (device not mounted or file missing)", 0,
                              ker::abi::sys_log::sys_log_device::serial);
    }

    // Test: Process execution
    ker::logging::logLine("init: TEST: Process Execution");
    ker::logging::logLine("init: Testing process exec");

    const char* progPath = "/mnt/disk/testprog";
    std::array<const char*, 4> argv = {"/mnt/disk/testprog", "arg1", "arg2", nullptr};
    std::array<const char*, 1> envp = {nullptr};

    ker::logging::logLine("init: Calling exec");

    uint64_t result = ker::process::exec(progPath, argv.data(), envp.data());

    if (result < 0) {
        ker::logging::logLine("init: exec failed (this is expected if testprog not in VFS)");
    } else {
        ker::logging::logLine("init: exec succeeded!");
    }

    ker::logging::logLine("init: All tests complete, looping...");

    // Loop forever
    // TODO: replace with idle process, init should not busy-wait but cannot exit
    while (true) {
        asm volatile("pause");
    }

    return 0;
}
