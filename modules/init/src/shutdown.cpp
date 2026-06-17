#include "shutdown.h"

#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/callnums.h>
#include <sys/logging.h>
#include <sys/reboot.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "services.h"

namespace {

using init_log = wos::journal<"init">;

constexpr const char* REQUEST_PATH = "/run/wos-shutdown.request";
constexpr uint64_t FIVE_MINUTES_NS = 5ULL * 60ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr uint64_t ONE_MINUTE_NS = 60ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr long FINALIZER_FAILURE_SLEEP_MS = 1000;
constexpr uint64_t POWER_OP_PREPARE = 2;

volatile sig_atomic_t g_signal_bits = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr sig_atomic_t SIGNAL_WAKE_REQUEST = 1 << 0;
constexpr sig_atomic_t SIGNAL_REBOOT_NOW = 1 << 1;
constexpr sig_atomic_t SIGNAL_HALT_NOW = 1 << 2;
constexpr sig_atomic_t SIGNAL_POWEROFF_NOW = 1 << 3;

struct ScheduledShutdown {
    ShutdownAction action{ShutdownAction::NONE};
    uint64_t deadline_mono_ns{};
    bool active{};
    bool warned_five{};
    bool warned_one{};
    std::array<char, 32> original_when{};
};

ScheduledShutdown g_scheduled{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void signal_handler(int signo) {
    switch (signo) {
        case SIGHUP:
            g_signal_bits |= SIGNAL_WAKE_REQUEST;
            break;
        case SIGTERM:
            g_signal_bits |= SIGNAL_REBOOT_NOW;
            break;
        case SIGUSR1:
            g_signal_bits |= SIGNAL_HALT_NOW;
            break;
        case SIGUSR2:
            g_signal_bits |= SIGNAL_POWEROFF_NOW;
            break;
        default:
            break;
    }
}

auto monotonic_ns() -> uint64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

void sleep_ms(long milliseconds) {
    timespec const ts{
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000L * 1000L,
    };
    nanosleep(&ts, nullptr);
}

auto parse_u64(const char* s, uint64_t& out) -> bool {
    if (s == nullptr || *s == '\0') {
        return false;
    }
    uint64_t value = 0;
    while (*s >= '0' && *s <= '9') {
        uint64_t const DIGIT = static_cast<uint64_t>(*s - '0');
        if (value > (UINT64_MAX - DIGIT) / 10ULL) {
            return false;
        }
        value = (value * 10ULL) + DIGIT;
        ++s;
    }
    if (*s != '\0' && *s != '\n' && *s != '\r') {
        return false;
    }
    out = value;
    return true;
}

auto parse_action(const char* value) -> ShutdownAction {
    if (std::strcmp(value, "reboot") == 0) {
        return ShutdownAction::REBOOT;
    }
    if (std::strcmp(value, "poweroff") == 0) {
        return ShutdownAction::POWEROFF;
    }
    if (std::strcmp(value, "halt") == 0) {
        return ShutdownAction::HALT;
    }
    return ShutdownAction::NONE;
}

struct ParsedRequest {
    ShutdownAction action{ShutdownAction::NONE};
    uint64_t deadline_mono_ns{};
    bool cancel{};
    bool valid{};
    std::array<char, 32> original_when{};
};

void parse_line(char* line, ParsedRequest& request) {
    char* equals = std::strchr(line, '=');
    if (equals == nullptr) {
        return;
    }
    *equals = '\0';
    char* key = line;
    char* value = equals + 1;
    char* end = value + std::strlen(value);
    while (end > value && (end[-1] == '\n' || end[-1] == '\r')) {
        *--end = '\0';
    }

    if (std::strcmp(key, "action") == 0) {
        request.action = parse_action(value);
    } else if (std::strcmp(key, "deadline_mono_ns") == 0) {
        (void)parse_u64(value, request.deadline_mono_ns);
    } else if (std::strcmp(key, "cancel") == 0) {
        request.cancel = std::strcmp(value, "1") == 0;
    } else if (std::strcmp(key, "original_when") == 0) {
        std::snprintf(request.original_when.data(), request.original_when.size(), "%s", value);
    }
}

auto read_request_file(ParsedRequest& request) -> bool {
    int const FD = ker::abi::vfs::open(REQUEST_PATH, 0, 0);
    if (FD < 0) {
        return false;
    }
    std::array<char, 512> buffer{};
    ssize_t const READ = ker::abi::vfs::read(FD, buffer.data(), buffer.size() - 1);
    ker::abi::vfs::close(FD);
    ker::abi::vfs::unlink(REQUEST_PATH);
    if (READ <= 0) {
        return false;
    }
    buffer.at(static_cast<size_t>(READ)) = '\0';

    char* cursor = buffer.data();
    while (*cursor != '\0') {
        char* line = cursor;
        while (*cursor != '\0' && *cursor != '\n') {
            ++cursor;
        }
        if (*cursor == '\n') {
            *cursor++ = '\0';
        }
        parse_line(line, request);
    }

    request.valid = request.cancel || (request.action != ShutdownAction::NONE && request.deadline_mono_ns != 0);
    return request.valid;
}

void apply_request_file() {
    ParsedRequest request{};
    if (!read_request_file(request)) {
        return;
    }
    if (request.cancel) {
        if (g_scheduled.active) {
            init_log::info("shutdown: cancelled pending request");
        }
        g_scheduled = {};
        return;
    }

    g_scheduled.action = request.action;
    g_scheduled.deadline_mono_ns = request.deadline_mono_ns;
    g_scheduled.original_when = request.original_when;
    g_scheduled.warned_five = false;
    g_scheduled.warned_one = false;
    g_scheduled.active = true;
    init_log::info("shutdown: scheduled action=%d when=%s deadline=%llu", static_cast<int>(request.action), request.original_when.data(),
                   static_cast<unsigned long long>(request.deadline_mono_ns));
}

auto consume_signal_action(bool& applied_request) -> ShutdownAction {
    sig_atomic_t const BITS = g_signal_bits;
    g_signal_bits = 0;
    if ((BITS & SIGNAL_WAKE_REQUEST) != 0) {
        apply_request_file();
        applied_request = true;
    }
    if ((BITS & SIGNAL_REBOOT_NOW) != 0) {
        return ShutdownAction::REBOOT;
    }
    if ((BITS & SIGNAL_HALT_NOW) != 0) {
        return ShutdownAction::HALT;
    }
    if ((BITS & SIGNAL_POWEROFF_NOW) != 0) {
        return ShutdownAction::POWEROFF;
    }
    return ShutdownAction::NONE;
}

void maybe_log_deadline_warnings(uint64_t now_ns) {
    if (!g_scheduled.active || g_scheduled.deadline_mono_ns <= now_ns) {
        return;
    }
    uint64_t const REMAINING = g_scheduled.deadline_mono_ns - now_ns;
    if (!g_scheduled.warned_five && REMAINING <= FIVE_MINUTES_NS) {
        g_scheduled.warned_five = true;
        init_log::info("shutdown: scheduled action in <=5 minutes");
    }
    if (!g_scheduled.warned_one && REMAINING <= ONE_MINUTE_NS) {
        g_scheduled.warned_one = true;
        init_log::info("shutdown: scheduled action in <=1 minute");
    }
}

auto prepare_kernel_shutdown() -> int64_t { return static_cast<int64_t>(syscall(ker::abi::callnums::power, POWER_OP_PREPARE)); }

}  // namespace

void shutdown_init() {
    ker::abi::vfs::unlink(REQUEST_PATH);

    struct sigaction action{};
    action.sa_handler = signal_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    (void)sigaction(SIGHUP, &action, nullptr);
    (void)sigaction(SIGTERM, &action, nullptr);
    (void)sigaction(SIGUSR1, &action, nullptr);
    (void)sigaction(SIGUSR2, &action, nullptr);
}

auto shutdown_poll() -> ShutdownAction {
    bool applied_request = false;
    ShutdownAction const SIGNAL_ACTION = consume_signal_action(applied_request);
    if (SIGNAL_ACTION != ShutdownAction::NONE) {
        return SIGNAL_ACTION;
    }
    if (applied_request) {
        return ShutdownAction::NONE;
    }

    uint64_t const NOW = monotonic_ns();
    maybe_log_deadline_warnings(NOW);
    if (g_scheduled.active && g_scheduled.deadline_mono_ns <= NOW) {
        ShutdownAction const ACTION = g_scheduled.action;
        g_scheduled = {};
        init_log::info("shutdown: deadline reached; starting action=%d", static_cast<int>(ACTION));
        return ACTION;
    }
    return ShutdownAction::NONE;
}

auto shutdown_reboot_cmd(ShutdownAction action) -> int {
    switch (action) {
        case ShutdownAction::REBOOT:
            return RB_AUTOBOOT;
        case ShutdownAction::POWEROFF:
            return RB_POWER_OFF;
        case ShutdownAction::HALT:
            return RB_HALT_SYSTEM;
        case ShutdownAction::NONE:
        default:
            return RB_HALT_SYSTEM;
    }
}

void shutdown_perform(ShutdownAction action) {
    init_log::info("shutdown: stopping services");
    stop_services_for_shutdown();
    init_log::info("shutdown: preparing kernel threads");
    int64_t const PREPARE_RET = prepare_kernel_shutdown();
    if (PREPARE_RET != 0) {
        init_log::warn("shutdown: kernel prepare returned %lld", static_cast<long long>(PREPARE_RET));
    }
    init_log::info("shutdown: syncing filesystems before journald stop");
    (void)ker::abi::vfs::sync_vfs();
    stop_journald_for_shutdown();
    init_log::info("shutdown: final userspace sync");
    (void)ker::abi::vfs::sync_vfs();

    int const CMD = shutdown_reboot_cmd(action);
    int const RET = reboot(CMD);
    init_log::critical("shutdown: reboot syscall returned %d errno=%d", RET, errno);
    for (;;) {
        sleep_ms(FINALIZER_FAILURE_SLEEP_MS);
    }
}
