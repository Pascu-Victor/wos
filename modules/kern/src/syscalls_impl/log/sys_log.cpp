#include "sys_log.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>

#include "abi/callnums/sys_log.h"
#include "mod/gfx/fb.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/dbg/journal.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/get_current_pagemap.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/mutex.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::syscall::log {

namespace {

constexpr size_t MAX_SYSLOG_COPY = 4096;
constexpr uint64_t NO_LOG_BLOCK_COOKIE = 0;
constexpr uint64_t NO_LOG_BLOCK_OWNER = 0;
constexpr uint64_t ANONYMOUS_LOG_BLOCK_OWNER = UINT64_MAX;
using sys_log_logger = ker::mod::dbg::logger<"sys_log">;

// Held across userspace log blocks so block-cookie records are emitted as one
// sys_log transaction. Exit cleanup releases the section if the owner dies
// before issuing LOG_BLOCK_END.
std::atomic<uint64_t> s_next_log_block_cookie{1};
ker::mod::sys::Mutex s_log_block_mutex{};
ker::mod::sys::Spinlock s_log_block_state_lock{};
uint64_t s_log_block_cookie = NO_LOG_BLOCK_COOKIE;
uint64_t s_log_block_owner_tid = NO_LOG_BLOCK_OWNER;
uint32_t s_log_block_depth = 0;

auto current_log_block_owner_token() -> uint64_t {
    if (ker::mod::sched::can_query_current_task()) {
        if (auto* task = ker::mod::sched::get_current_task(); task != nullptr && task->pid != 0) {
            return task->pid;
        }
    }
    return ANONYMOUS_LOG_BLOCK_OWNER;
}

auto current_task_owns_active_log_block() -> bool {
    uint64_t const OWNER = current_log_block_owner_token();
    uint64_t const FLAGS = s_log_block_state_lock.lock_irqsave();
    bool const OWNS = s_log_block_cookie != NO_LOG_BLOCK_COOKIE && s_log_block_owner_tid == OWNER && s_log_block_depth != 0;
    s_log_block_state_lock.unlock_irqrestore(FLAGS);
    return OWNS;
}

auto current_task_owns_log_block(uint64_t cookie) -> bool {
    if (cookie == NO_LOG_BLOCK_COOKIE) {
        return false;
    }
    uint64_t const OWNER = current_log_block_owner_token();
    uint64_t const FLAGS = s_log_block_state_lock.lock_irqsave();
    bool const OWNS = s_log_block_cookie == cookie && s_log_block_owner_tid == OWNER && s_log_block_depth != 0;
    s_log_block_state_lock.unlock_irqrestore(FLAGS);
    return OWNS;
}

auto begin_log_block() -> uint64_t {
    uint64_t const OWNER = current_log_block_owner_token();
    uint64_t flags = s_log_block_state_lock.lock_irqsave();
    if (s_log_block_owner_tid == OWNER && s_log_block_cookie != NO_LOG_BLOCK_COOKIE && s_log_block_depth != 0) {
        s_log_block_depth++;
        uint64_t const COOKIE = s_log_block_cookie;
        s_log_block_state_lock.unlock_irqrestore(flags);
        return COOKIE;
    }
    s_log_block_state_lock.unlock_irqrestore(flags);

    s_log_block_mutex.lock();

    uint64_t cookie = s_next_log_block_cookie.fetch_add(1, std::memory_order_relaxed);
    if (cookie == NO_LOG_BLOCK_COOKIE) {
        cookie = s_next_log_block_cookie.fetch_add(1, std::memory_order_relaxed);
    }

    flags = s_log_block_state_lock.lock_irqsave();
    s_log_block_cookie = cookie;
    s_log_block_owner_tid = OWNER;
    s_log_block_depth = 1;
    s_log_block_state_lock.unlock_irqrestore(flags);
    return cookie;
}

auto end_log_block(uint64_t cookie) -> bool {
    if (cookie == NO_LOG_BLOCK_COOKIE) {
        return false;
    }

    uint64_t const OWNER = current_log_block_owner_token();
    bool unlock_mutex = false;
    bool ended = false;

    uint64_t const FLAGS = s_log_block_state_lock.lock_irqsave();
    if (s_log_block_cookie == cookie && s_log_block_owner_tid == OWNER && s_log_block_depth != 0) {
        s_log_block_depth--;
        ended = true;
        if (s_log_block_depth == 0) {
            s_log_block_cookie = NO_LOG_BLOCK_COOKIE;
            s_log_block_owner_tid = NO_LOG_BLOCK_OWNER;
            unlock_mutex = true;
        }
    }
    s_log_block_state_lock.unlock_irqrestore(FLAGS);

    if (unlock_mutex) {
        s_log_block_mutex.unlock();
    }
    return ended;
}

void release_log_block_for_tid(uint64_t tid) {
    if (tid == NO_LOG_BLOCK_OWNER) {
        return;
    }

    bool unlock_mutex = false;
    uint64_t const FLAGS = s_log_block_state_lock.lock_irqsave();
    if (s_log_block_owner_tid == tid && s_log_block_cookie != NO_LOG_BLOCK_COOKIE && s_log_block_depth != 0) {
        s_log_block_cookie = NO_LOG_BLOCK_COOKIE;
        s_log_block_owner_tid = NO_LOG_BLOCK_OWNER;
        s_log_block_depth = 0;
        unlock_mutex = true;
    }
    s_log_block_state_lock.unlock_irqrestore(FLAGS);

    if (unlock_mutex) {
        s_log_block_mutex.unlock();
    }
}

class LogBlockGate {
   public:
    explicit LogBlockGate(bool enabled) : m_locked(enabled) {
        if (m_locked) {
            s_log_block_mutex.lock();
        }
    }
    ~LogBlockGate() {
        if (m_locked) {
            s_log_block_mutex.unlock();
        }
    }

    LogBlockGate(const LogBlockGate&) = delete;
    LogBlockGate(LogBlockGate&&) = delete;
    auto operator=(const LogBlockGate&) -> LogBlockGate& = delete;
    auto operator=(LogBlockGate&&) -> LogBlockGate& = delete;

   private:
    bool m_locked;
};

auto should_gate_unblocked_log() -> bool { return !current_task_owns_active_log_block(); }

auto should_gate_log_with_cookie(uint64_t cookie) -> bool { return cookie == NO_LOG_BLOCK_COOKIE && should_gate_unblocked_log(); }

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

auto sys_log(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, uint64_t device_or_level, const char* module,
             uint64_t cookie) -> uint64_t {
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
        case abi::sys_log::sys_log_ops::LOG_BLOCK_BEGIN:
            return begin_log_block();
        case abi::sys_log::sys_log_ops::LOG_BLOCK_END:
            return end_log_block(cookie) ? 0 : 1;
        case abi::sys_log::sys_log_ops::LOG: {
            if (cookie != NO_LOG_BLOCK_COOKIE && !current_task_owns_log_block(cookie)) {
                return 1;
            }
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
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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
            if (cookie != NO_LOG_BLOCK_COOKIE && !current_task_owns_log_block(cookie)) {
                return 1;
            }
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
                        LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
                        mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", "", 0);
                        return 0;
                    }
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(), 0);
                } else {
                    if (!safe_copy_to_kernel(str, buf.data(), buf.size(), len, copied)) {
                        return 1;
                    }
                    if (copied == 0) {
                        LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
                        mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", "", 0);
                        return 0;
                    }
                    nul_terminate_copy(buf, copied, false);
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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
            if (cookie != NO_LOG_BLOCK_COOKIE && !current_task_owns_log_block(cookie)) {
                return 1;
            }
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
            LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
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

void sys_log_cleanup_for_task(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    release_log_block_for_tid(task->pid);
}

}  // namespace ker::syscall::log
