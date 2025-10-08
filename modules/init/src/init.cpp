#include <sys/logging.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <cstring>

#include "bits/ssize_t.h"
#include "callnums/sys_log.h"
#include "sys/callnums.h"

// Temporary veneers until a proper vDSO/ld.so exists:
// Implement ker::logging functions using the syscall ABI so symbols resolve locally.
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

}  // namespace ker::logging
namespace {
const char* const initmsg = "init: open failed";
constexpr size_t buffer_size = 64;
}  // namespace
auto main() -> int {
    // Create/open /test
    const char* path = "/test";
    int fd = ker::abi::vfs::open(path, 0, 0);
    if (fd < 0) {
        ker::logging::logLine(initmsg, 0, ker::abi::sys_log::sys_log_device::serial);
    } else {
        const char* text = "hello-vfs";
        ker::abi::vfs::write(fd, text, strlen(text));
        ker::logging::logLine("init: wrote to /test", 0, ker::abi::sys_log::sys_log_device::serial);
        std::array<char, buffer_size> buf = {0};
        ker::abi::vfs::read(fd, buf.data(), sizeof(buf) - 1);
        ker::logging::logLine("init: read buffer", 0, ker::abi::sys_log::sys_log_device::serial);
        ker::logging::log("init: buffer was: '", 0, ker::abi::sys_log::sys_log_device::serial);
        ker::logging::log(buf.data(), buf.size(), ker::abi::sys_log::sys_log_device::serial);
        ker::logging::logLine("'", 0, ker::abi::sys_log::sys_log_device::serial);
        ker::abi::vfs::close(fd);
    }
}
