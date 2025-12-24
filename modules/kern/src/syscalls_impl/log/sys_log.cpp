#include "sys_log.hpp"

#include <cstdint>
#include <cstring>

#include "abi/callnums/sys_log.h"
#include "mod/gfx/fb.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::syscall::log {
auto sysLog(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    switch (op) {
        case abi::sys_log::sys_log_ops::log:
            if (device == abi::sys_log::sys_log_device::serial) {
                if (str == nullptr) {
                    return 1;
                }
                if (len == 0) {
                    len = std::strlen(str);
                }
                mod::io::serial::write(str, len);
            } else if (device == abi::sys_log::sys_log_device::vga) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    mod::dbg::logFbOnly(str);
                } else {
                    mod::io::serial::write("framebuffer module is not compiled device is invalid: ");
                    mod::io::serial::write((uint64_t)device);
                    mod::io::serial::write("\n");
                    return 1;
                }
            } else {
                mod::io::serial::write("Invalid sysLog device: ");
                mod::io::serial::write((uint64_t)device);
                mod::io::serial::write("\n");
                return 1;
            }

            break;
        case ker::abi::sys_log::sys_log_ops::logLine:
            if (device == abi::sys_log::sys_log_device::serial) {
                if (str == nullptr) {
                    return 1;
                }
                if (len == 0) {
                    len = std::strlen(str);
                }
                mod::io::serial::write(str, len);
                mod::io::serial::write("\n");
            } else if (device == abi::sys_log::sys_log_device::vga) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    mod::dbg::logFbOnly(str);
                    mod::dbg::logFbAdvance();
                } else {
                    mod::io::serial::write("framebuffer module is not compiled device is invalid: ");
                    mod::io::serial::write((uint64_t)device);
                    mod::io::serial::write("\n");
                    return 1;
                }
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
