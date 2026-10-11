#include "sys_log.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/security/compartment_host.hpp>

#include "abi/callnums/perf_trace.h"
#include "abi/callnums/sys_log.h"
#include "mod/gfx/fb.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/dbg/journal.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/mutex.hpp"
#include "platform/sys/spinlock.hpp"
#include "platform/sys/usercopy.hpp"

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

// The caller supplies a diagnostic milestone, never an identity or pointer.
// Image ownership and capture provenance are kernel values. No logging gate,
// allocation, usercopy or blocking is involved, including before userspace TLS.
auto record_userspace_startup_marker(ker::mod::sched::task::Task* task, uint64_t stage, uint64_t session, uint64_t correlation,
                                     uint64_t version, uint64_t reserved) -> uint64_t {
    using namespace ker::mod;
    if (version != WOS_PERF_TRACE_VERSION || reserved != 0 || stage < WOS_PERF_STAGE_IMAGE_ENTRY || stage > WOS_PERF_STAGE_RTLD_TCB_READY ||
        correlation > UINT32_MAX) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->is_thread) {
        return 0;
    }
    auto& trace = task->exec_image_trace;
    if (trace.pending || trace.capture_session == perf::PERF_ANY_SESSION || trace.capture_session != session ||
        trace.correlation != correlation ||
        session != perf::current_wki_capture_session(perf::WkiPerfScope::LOCAL_PROC,
                                                     static_cast<uint8_t>(perf::WkiPerfLocalProcOp::USERSPACE_STAGE))) {
        return 0;
    }
    static_assert(WOS_PERF_STAGE_RTLD_TCB_READY <= 8);  // emitted_user_stages is one byte.
    uint8_t const BIT = static_cast<uint8_t>(1U << (stage - 1));
    if ((trace.emitted_user_stages & BIT) != 0) {
        return 0;
    }
    if (stage != WOS_PERF_STAGE_IMAGE_ENTRY && (trace.emitted_user_stages & 1U) == 0) {
        return 0;
    }
    // Old libc can omit the intermediate stages. New ones must be ordered and
    // precede RTLD_READY; static CRT_ENTRY may legitimately come before them.
    if (stage >= WOS_PERF_STAGE_RTLD_DEPENDENCIES_READY) {
        constexpr uint8_t RTLD_READY_BIT = static_cast<uint8_t>(1U << (WOS_PERF_STAGE_RTLD_READY - 1U));
        if ((trace.emitted_user_stages & RTLD_READY_BIT) != 0) {
            return 0;
        }
        if (stage != WOS_PERF_STAGE_RTLD_DEPENDENCIES_READY && (trace.emitted_user_stages & (1U << (stage - 2))) == 0) {
            return 0;
        }
    }
    constexpr uint8_t MAIN_PREREQUISITES = (1U << (WOS_PERF_STAGE_RTLD_READY - 1)) | (1U << (WOS_PERF_STAGE_CRT_ENTRY - 1));
    if (stage == WOS_PERF_STAGE_MAIN_BEGIN && (trace.emitted_user_stages & MAIN_PREREQUISITES) != MAIN_PREREQUISITES) {
        return 0;
    }
    if (stage == WOS_PERF_STAGE_MAIN_END && (trace.emitted_user_stages & (1U << (WOS_PERF_STAGE_MAIN_BEGIN - 1))) == 0) {
        return 0;
    }
    trace.emitted_user_stages |= BIT;
    perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), task->pid, perf::WkiPerfScope::LOCAL_PROC,
                           static_cast<uint8_t>(perf::WkiPerfLocalProcOp::USERSPACE_STAGE), perf::WkiPerfPhase::POINT, 0, 0,
                           trace.correlation, 0, static_cast<uint32_t>(stage), WOS_PERF_CALLSITE(), session);
    return 0;
}

static_assert(static_cast<uint64_t>(ker::abi::callnums::SYS_LOG) == WOS_PERF_TRACE_SYS_LOG);
static_assert(static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::PERF_TRACE) == WOS_PERF_TRACE_LOG_OP);

auto sys_log_for_subject(ker::mod::sched::task::Task* current_task, ker::abi::sys_log::sys_log_ops op, uint64_t str_user_addr, uint64_t len,
                         uint64_t device_or_level, uint64_t module_user_addr, uint64_t cookie) -> uint64_t {
    bool const GLOBAL = op == abi::sys_log::sys_log_ops::LOG_BLOCK_BEGIN || op == abi::sys_log::sys_log_ops::LOG_BLOCK_END ||
                        ((op == abi::sys_log::sys_log_ops::LOG || op == abi::sys_log::sys_log_ops::LOG_LINE) &&
                         device_or_level == static_cast<uint64_t>(abi::sys_log::sys_log_device::VGA));
    if (GLOBAL) {
        int const SCOPE = mod::security::compartment::host_namespace_admission(current_task);
        if (SCOPE != 0) return static_cast<uint64_t>(SCOPE);
    }
    if (op == abi::sys_log::sys_log_ops::PERF_TRACE) {
        return record_userspace_startup_marker(current_task, str_user_addr, len, device_or_level, module_user_addr, cookie);
    }

    // A byte-counted log may emit a fully copied prefix when a later page
    // faults. NUL-terminated inputs are all-or-error because there is no ABI
    // field with which to report a partial string.
    auto safe_copy_to_kernel = [&](uint64_t src_user_addr, char* dest, size_t dest_size, uint64_t copy_limit,
                                   uint64_t& copied_count) -> bool {
        copied_count = 0;
        if (src_user_addr == 0 || dest == nullptr || dest_size == 0 || current_task == nullptr) {
            return false;
        }

        uint64_t const MAX_TO_COPY =
            std::min((copy_limit == 0) ? static_cast<uint64_t>(dest_size) : copy_limit, static_cast<uint64_t>(dest_size));

        if (copy_limit == 0) {
            if (!ker::mod::sys::usercopy::copy_cstring_from_task_strict(*current_task, src_user_addr, dest, dest_size)) {
                return false;
            }
            size_t text_len = 0;
            while (text_len + 1 < dest_size && dest[text_len] != '\0') {
                ++text_len;
            }
            copied_count = static_cast<uint64_t>(text_len + 1);
            return true;
        }

        auto const COPY =
            ker::mod::sys::usercopy::copy_from_task_partial(*current_task, src_user_addr, dest, static_cast<size_t>(MAX_TO_COPY));
        copied_count = COPY.bytes_copied;
        return !COPY.fault || COPY.bytes_copied != 0;
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
                if (str_user_addr == 0) {
                    return static_cast<uint64_t>(-EFAULT);
                }
                std::array<char, MAX_SYSLOG_COPY> buf{};
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), 0, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
                    }
                    nul_terminate_copy(buf, copied, true);
                    if (buf.front() == '\0') {
                        return 0;
                    }
                    LogBlockGate const GATE(should_gate_log_with_cookie(cookie));
                    mod::dbg::journal::emit(mod::dbg::LogLevel::INFO, "userspace", buf.data(), 0);
                } else {
                    // Copy exactly 'len' bytes or until an unmapped page
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), len, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
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
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), 0, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
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
                if (str_user_addr == 0) {
                    return static_cast<uint64_t>(-EFAULT);
                }
                std::array<char, MAX_SYSLOG_COPY> buf{};
                uint64_t copied = 0;
                if (len == 0) {
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), 0, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
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
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), len, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
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
                    if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), 0, copied)) {
                        return static_cast<uint64_t>(-EFAULT);
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
            if (str_user_addr == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            std::array<char, MAX_SYSLOG_COPY> buf{};
            uint64_t copied = 0;
            if (!safe_copy_to_kernel(str_user_addr, buf.data(), buf.size(), len, copied)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            nul_terminate_copy(buf, copied, len == 0);

            std::array<char, mod::dbg::journal::JOURNAL_MODULE_MAX> module_buf{};
            uint64_t module_copied = 0;
            if (module_user_addr != 0) {
                if (!safe_copy_to_kernel(module_user_addr, module_buf.data(), module_buf.size(), 0, module_copied)) {
                    return static_cast<uint64_t>(-EFAULT);
                }
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

auto sys_log(ker::abi::sys_log::sys_log_ops op, uint64_t str_user_addr, uint64_t len, uint64_t device_or_level, uint64_t module_user_addr,
             uint64_t cookie) -> uint64_t {
    return sys_log_for_subject(ker::mod::sched::get_current_task(), op, str_user_addr, len, device_or_level, module_user_addr, cookie);
}

void sys_log_cleanup_for_task(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    release_log_block_for_tid(task->pid);
}

}  // namespace ker::syscall::log
