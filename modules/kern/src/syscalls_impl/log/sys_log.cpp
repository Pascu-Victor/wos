#include "sys_log.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>

#include "abi/callnums/sys_log.h"
#include "mod/gfx/fb.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/dbg/journal.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/get_current_pagemap.hpp"

namespace ker::syscall::log {

namespace {

constexpr size_t MAX_SYSLOG_COPY = 4096;
using sys_log_logger = ker::mod::dbg::logger<"sys_log">;

template <size_t Size>
void nul_terminate_copy(std::array<char, Size>& buf, uint64_t copied, bool trim_trailing_nul) {
    size_t nul_index = copied < buf.size() ? static_cast<size_t>(copied) : buf.size() - 1;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    if (trim_trailing_nul && nul_index > 0 && buf[nul_index - 1] == '\0') {
        --nul_index;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    buf[nul_index] = '\0';
}

}  // namespace

auto sys_log(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, uint64_t device_or_level, const char* module) -> uint64_t {
    auto* current_pagemap = wos_get_current_pagemap();

    // Helper to safely copy from user/kernel pointer into a kernel buffer.
    auto safe_copy_to_kernel = [&](const char* src, char* dest, size_t dest_size, uint64_t copy_limit, uint64_t& copied_count) -> bool {
        if (src == nullptr || dest == nullptr || dest_size == 0) {
            return false;
        }
        uint64_t copied = 0;
        uint64_t const MAX_TO_COPY =
            std::min((copy_limit == 0) ? static_cast<uint64_t>(dest_size) : copy_limit, static_cast<uint64_t>(dest_size));
        // If src looks like a kernel pointer (high half), we can read directly
        auto const SRC_ADDR = reinterpret_cast<uint64_t>(src);
        bool const SRC_IS_KERNEL = (SRC_ADDR & 0xffff800000000000ULL) != 0ULL;
        while (copied < MAX_TO_COPY) {
            uint64_t phys = 0;
            if (SRC_IS_KERNEL) {
                // direct kernel virtual read
                phys = reinterpret_cast<uint64_t>(
                    ker::mod::mm::addr::get_phys_pointer(static_cast<ker::mod::mm::addr::vaddr_t>(SRC_ADDR) + copied));
                // If getPhysPointer fails, fall back to virtual direct read
                if (phys == 0) {
                    dest[copied] = *reinterpret_cast<const char*>(SRC_ADDR + copied);
                } else {
                    dest[copied] = *reinterpret_cast<const char*>(ker::mod::mm::addr::get_virt_pointer(phys + 0));
                }
            } else {
                if (!current_pagemap) {
                    return false;
                }
                phys = ker::mod::mm::virt::translate(current_pagemap, static_cast<ker::mod::mm::addr::vaddr_t>(SRC_ADDR + copied));
                if (phys == ker::mod::mm::virt::PADDR_INVALID) {
                    break;  // unmapped - stop copying
                }
                dest[copied] = *reinterpret_cast<const char*>(ker::mod::mm::addr::get_virt_pointer(phys));
            }
            if (dest[copied] == '\0') {
                copied++;
                break;
            }
            copied++;
        }
        copied_count = copied;
        return true;
    };

    switch (op) {
        case abi::sys_log::sys_log_ops::LOG: {
            auto device = static_cast<abi::sys_log::sys_log_device>(device_or_level);
            if (device == abi::sys_log::sys_log_device::SERIAL) {
                if (str == nullptr) {
                    return 1;
                }
                std::array<char, MAX_SYSLOG_COPY> buf{};
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), 0, copied)) {
                        return 1;
                    }
                    nul_terminate_copy(buf, copied, true);
                    if (buf.front() == '\0') {
                        return 0;
                    }
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(), 0);
                } else {
                    // Copy exactly 'len' bytes or until an unmapped page
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), len, copied)) {
                        return 1;
                    }
                    if (copied == 0) {
                        return 0;
                    }
                    nul_terminate_copy(buf, copied, false);
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(),
                                            copied < len ? mod::dbg::journal::JOURNAL_FLAG_TRUNCATED : 0);
                }
            } else if (device == abi::sys_log::sys_log_device::VGA) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    std::array<char, MAX_SYSLOG_COPY> buf{};
                    uint64_t copied = 0;
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), 0, copied)) {
                        return 1;
                    }
                    nul_terminate_copy(buf, copied, true);
                    mod::dbg::log_fb_only(buf.data());
                } else {
                    sys_log_logger::warn("framebuffer module is not compiled; device is invalid: %u", static_cast<unsigned>(device));
                    return 1;
                }
            } else {
                sys_log_logger::warn("invalid device: %u", static_cast<unsigned>(device));
                return 1;
            }
        } break;
        case ker::abi::sys_log::sys_log_ops::LOG_LINE: {
            auto device = static_cast<abi::sys_log::sys_log_device>(device_or_level);
            if (device == abi::sys_log::sys_log_device::SERIAL) {
                if (str == nullptr) {
                    return 1;
                }
                std::array<char, MAX_SYSLOG_COPY> buf{};
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), 0, copied)) {
                        return 1;
                    }
                    nul_terminate_copy(buf, copied, true);
                    if (buf.front() == '\0') {
                        mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", "", 0);
                        return 0;
                    }
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(), 0);
                } else {
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), len, copied)) {
                        return 1;
                    }
                    if (copied == 0) {
                        mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", "", 0);
                        return 0;
                    }
                    nul_terminate_copy(buf, copied, false);
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(),
                                            copied < len ? mod::dbg::journal::JOURNAL_FLAG_TRUNCATED : 0);
                }
            } else if (device == abi::sys_log::sys_log_device::VGA) {
                if constexpr (ker::mod::gfx::fb::WOS_HAS_GFX_FB) {
                    std::array<char, MAX_SYSLOG_COPY> buf{};
                    uint64_t copied = 0;
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), 0, copied)) {
                        return 1;
                    }
                    nul_terminate_copy(buf, copied, true);
                    mod::dbg::log_fb_only(buf.data());
                    mod::dbg::log_fb_advance();
                } else {
                    sys_log_logger::warn("framebuffer module is not compiled; device is invalid: %u", static_cast<unsigned>(device));
                    return 1;
                }
            } else {
                sys_log_logger::warn("invalid device: %u", static_cast<unsigned>(device));
                return 1;
            }
        } break;
        case ker::abi::sys_log::sys_log_ops::LOG_EX: {
            if (str == nullptr) {
                return 1;
            }
            std::array<char, MAX_SYSLOG_COPY> buf{};
            uint64_t copied = 0;
            if (!safe_copy_to_kernel(str, buf.data(), buf.size(), len, copied)) {
                return 1;
            }
            nul_terminate_copy(buf, copied, len == 0);

            std::array<char, mod::dbg::journal::JOURNAL_MODULE_MAX> module_buf{};
            uint64_t module_copied = 0;
            if (module != nullptr && safe_copy_to_kernel(module, module_buf.data(), module_buf.size(), 0, module_copied)) {
                nul_terminate_copy(module_buf, module_copied, true);
            } else {
                std::memcpy(module_buf.data(), "userspace", sizeof("userspace"));
            }

            uint64_t raw_level = device_or_level;
            if (raw_level > static_cast<uint64_t>(mod::dbg::LogLevel::PANIC)) {
                raw_level = static_cast<uint64_t>(mod::dbg::LogLevel::INFO);
            }
            mod::dbg::journal::emit(static_cast<mod::dbg::LogLevel>(raw_level), module_buf.data(), buf.data(),
                                    copied < len ? mod::dbg::journal::JOURNAL_FLAG_TRUNCATED : 0);
        } break;
        default:
            sys_log_logger::warn("invalid operation");
            return 1;
            break;
    }
    return 0;
}

}  // namespace ker::syscall::log
