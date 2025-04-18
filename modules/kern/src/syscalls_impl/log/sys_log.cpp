#include "sys_log.hpp"

namespace ker::syscall::log {
uint64_t sysLog(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, abi::sys_log::sys_log_device device) {
    switch (op) {
        case abi::sys_log::sys_log_ops::log:
            if (device == abi::sys_log::sys_log_device::serial) {
                mod::io::serial::write(str, len);
            } else if (device == abi::sys_log::sys_log_device::vga) {
                mod::dbg::logFbOnly(str);
            } else {
                mod::io::serial::write("Invalid sysLog device: ");
                mod::io::serial::write((uint64_t)device);
                mod::io::serial::write("\n");
                return 1;
            }

            break;
        case ker::abi::sys_log::sys_log_ops::logLine:
            if (device == abi::sys_log::sys_log_device::serial) {
                mod::io::serial::write(str, len);
                mod::io::serial::write("\n");
            } else if (device == abi::sys_log::sys_log_device::vga) {
                mod::dbg::logFbOnly(str);
                mod::dbg::logFbAdvance();
            } else {
                mod::io::serial::write("Invalid sysLog device: ");
                mod::io::serial::write((uint64_t)device);
                mod::io::serial::write("\n");
                return 1;
            }
            break;
        default:
            mod::io::serial::write("Invalid sysLog operation\n");
            return 1;
            break;
    }
    return 0;
}

}  // namespace ker::syscall::log
