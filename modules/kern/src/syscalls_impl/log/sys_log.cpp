#include "sys_log.hpp"

#include <cstdint>
#include <cstring>

#include "abi/callnums/sys_log.h"
#include "mod/gfx/fb.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::syscall::log {
#include <platform/sched/get_current_pagemap.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

static constexpr size_t MAX_SYSLOG_COPY = 4096;

auto sysLog(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    auto* currentPagemap = _wOS_getCurrentPagemap();

    // Helper to safely copy from user/kernel pointer into a kernel buffer.
    auto safe_copy_to_kernel = [&](const char* src, uint64_t requested_len, char* dest, uint64_t& out_len) -> bool {
        if (!src) return false;
        uint64_t copied = 0;
        uint64_t max_to_copy = (requested_len == 0) ? MAX_SYSLOG_COPY : requested_len;
        // If src looks like a kernel pointer (high half), we can read directly
        uint64_t src_addr = (uint64_t)src;
        bool src_is_kernel = (src_addr & 0xffff800000000000ULL) != 0ULL;
        while (copied < max_to_copy) {
            uint64_t phys = 0;
            if (src_is_kernel) {
                // direct kernel virtual read
                phys = (uint64_t)ker::mod::mm::addr::getPhysPointer((ker::mod::mm::addr::vaddr_t)src_addr + copied);
                // If getPhysPointer fails, fall back to virtual direct read
                if (phys == 0) {
                    dest[copied] = *(const char*)(src_addr + copied);
                } else {
                    dest[copied] = *(const char*)ker::mod::mm::addr::getVirtPointer(phys + 0);
                }
            } else {
                if (!currentPagemap) return false;
                phys = ker::mod::mm::virt::translate(currentPagemap, (ker::mod::mm::addr::vaddr_t)(src_addr + copied));
                if (phys == 0) break;  // unmapped - stop copying
                dest[copied] = *(const char*)ker::mod::mm::addr::getVirtPointer(phys);
            }
            if (dest[copied] == '\0') {
                copied++;
                break;
            }
            copied++;
        }
        out_len = copied;
        return true;
    };

    switch (op) {
        case abi::sys_log::sys_log_ops::log: {
            if (device == abi::sys_log::sys_log_device::serial) {
                if (str == nullptr) {
                    return 1;
                }
                char buf[MAX_SYSLOG_COPY];
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str, 0, buf, copied)) return 1;
                    // if last byte is NUL, don't include it in write (strlen semantics)
                    if (copied && buf[copied - 1] == '\0') copied--;
                    if (copied == 0) return 0;
                    mod::io::serial::write(buf, copied);
                } else {
                    // Copy exactly 'len' bytes or until an unmapped page
                    if (!safe_copy_to_kernel(str, len, buf, copied)) return 1;
                    if (copied == 0) return 0;
                    mod::io::serial::write(buf, copied);
                }
            } else if (device == abi::sys_log::sys_log_device::vga) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    char buf[MAX_SYSLOG_COPY];
                    uint64_t copied = 0;
                    if (!safe_copy_to_kernel(str, 0, buf, copied)) return 1;
                    buf[(copied >= 1 && buf[copied - 1] == '\0') ? (copied - 1) : copied] = '\0';
                    mod::dbg::logFbOnly(buf);
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
        } break;
        case ker::abi::sys_log::sys_log_ops::logLine: {
            if (device == abi::sys_log::sys_log_device::serial) {
                if (str == nullptr) {
                    return 1;
                }
                char buf[MAX_SYSLOG_COPY];
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str, 0, buf, copied)) return 1;
                    if (copied && buf[copied - 1] == '\0') copied--;
                    if (copied == 0) {
                        mod::io::serial::write("\n");
                        return 0;
                    }
                    mod::io::serial::write(buf, copied);
                    mod::io::serial::write("\n");
                } else {
                    if (!safe_copy_to_kernel(str, len, buf, copied)) return 1;
                    if (copied == 0) {
                        mod::io::serial::write("\n");
                        return 0;
                    }
                    mod::io::serial::write(buf, copied);
                    mod::io::serial::write("\n");
                }
            } else if (device == abi::sys_log::sys_log_device::vga) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    char buf[MAX_SYSLOG_COPY];
                    uint64_t copied = 0;
                    if (!safe_copy_to_kernel(str, 0, buf, copied)) return 1;
                    buf[(copied >= 1 && buf[copied - 1] == '\0') ? (copied - 1) : copied] = '\0';
                    mod::dbg::logFbOnly(buf);
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
        } break;
        default:
            mod::io::serial::write("Invalid sysLog operation\n");
            return 1;
            break;
    }
    return 0;
}

}  // namespace ker::syscall::log
