#include <abi/callnums/sys_log.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/syscall.h>
#include <sys/vfs.h>

#include <cstdint>
#include <cstring>

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

// Alternative main function for testing with argc/argv
extern "C" auto main(int argc, char** argv, char** envp) -> int {
    (void)envp;

    ker::logging::logLine("testprog: main() called", 0, ker::abi::sys_log::sys_log_device::serial);

    // Log arguments
    ker::logging::log("testprog: argc = ", 0, ker::abi::sys_log::sys_log_device::serial);
    char buf[32];
    int n = argc;
    int i = 0;
    if (n == 0) {
        buf[i++] = '0';
    } else {
        char temp[32];
        int j = 0;
        while (n > 0) {
            temp[j++] = '0' + (n % 10);
            n /= 10;
        }
        while (j > 0) {
            buf[i++] = temp[--j];
        }
    }
    buf[i] = '\0';
    ker::logging::logLine(buf, 0, ker::abi::sys_log::sys_log_device::serial);

    // Log each argument
    for (int arg = 0; arg < argc; arg++) {
        ker::logging::log("testprog: argv[", 0, ker::abi::sys_log::sys_log_device::serial);
        // Convert arg to string
        n = arg;
        i = 0;
        if (n == 0) {
            buf[i++] = '0';
        } else {
            char temp[32];
            int j = 0;
            while (n > 0) {
                temp[j++] = '0' + (n % 10);
                n /= 10;
            }
            while (j > 0) {
                buf[i++] = temp[--j];
            }
        }
        buf[i] = '\0';
        ker::logging::log(buf, 0, ker::abi::sys_log::sys_log_device::serial);
        ker::logging::log("] = ", 0, ker::abi::sys_log::sys_log_device::serial);
        ker::logging::logLine(argv[arg], 0, ker::abi::sys_log::sys_log_device::serial);
    }

    return 0;
}
