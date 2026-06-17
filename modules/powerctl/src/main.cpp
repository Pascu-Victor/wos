#include <fcntl.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/reboot.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {

constexpr const char* REQUEST_PATH = "/run/wos-shutdown.request";
constexpr const char* REQUEST_TMP_PATH = "/run/wos-shutdown.request.tmp";
constexpr uint64_t NSEC_PER_SEC = 1000000000ULL;
constexpr uint64_t SEC_PER_DAY = 24ULL * 60ULL * 60ULL;

enum class Action : uint8_t {
    REBOOT,
    POWEROFF,
    HALT,
};

auto program_basename(const char* path) -> const char* {
    if (path == nullptr) {
        return "";
    }
    const char* base = path;
    for (const char* p = path; *p != '\0'; ++p) {
        if (*p == '/') {
            base = p + 1;
        }
    }
    return base;
}

auto action_name(Action action) -> const char* {
    switch (action) {
        case Action::REBOOT:
            return "reboot";
        case Action::POWEROFF:
            return "poweroff";
        case Action::HALT:
            return "halt";
    }
    return "halt";
}

auto reboot_cmd(Action action) -> int {
    switch (action) {
        case Action::REBOOT:
            return RB_AUTOBOOT;
        case Action::POWEROFF:
            return RB_POWER_OFF;
        case Action::HALT:
            return RB_HALT_SYSTEM;
    }
    return RB_HALT_SYSTEM;
}

auto clock_ns(clockid_t clock_id, uint64_t& out) -> bool {
    timespec ts{};
    if (clock_gettime(clock_id, &ts) != 0) {
        return false;
    }
    out = (static_cast<uint64_t>(ts.tv_sec) * NSEC_PER_SEC) + static_cast<uint64_t>(ts.tv_nsec);
    return true;
}

auto parse_uint(const char* s, uint64_t& out) -> bool {
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
    if (*s != '\0') {
        return false;
    }
    out = value;
    return true;
}

auto parse_relative_minutes(const char* minutes_text, uint64_t mono_ns, uint64_t& deadline_mono_ns) -> bool {
    uint64_t minutes = 0;
    if (!parse_uint(minutes_text, minutes)) {
        return false;
    }
    deadline_mono_ns = mono_ns + (minutes * 60ULL * NSEC_PER_SEC);
    return true;
}

auto parse_when(const char* when, uint64_t& deadline_mono_ns) -> bool {
    uint64_t mono_ns = 0;
    if (!clock_ns(CLOCK_MONOTONIC, mono_ns)) {
        return false;
    }
    if (when == nullptr || std::strcmp(when, "now") == 0) {
        return parse_relative_minutes("0", mono_ns, deadline_mono_ns);
    }
    if (when[0] == '+') {
        return parse_relative_minutes(when + 1, mono_ns, deadline_mono_ns);
    }

    const char* colon = std::strchr(when, ':');
    if (colon == nullptr) {
        return false;
    }
    std::array<char, 8> hour_buf{};
    size_t const HOUR_LEN = static_cast<size_t>(colon - when);
    if (HOUR_LEN == 0 || HOUR_LEN >= hour_buf.size()) {
        return false;
    }
    std::memcpy(hour_buf.data(), when, HOUR_LEN);
    uint64_t hour = 0;
    uint64_t minute = 0;
    if (!parse_uint(hour_buf.data(), hour) || !parse_uint(colon + 1, minute) || hour > 23 || minute > 59) {
        return false;
    }

    uint64_t real_ns = 0;
    if (!clock_ns(CLOCK_REALTIME, real_ns)) {
        return false;
    }
    uint64_t const NOW_SEC_OF_DAY = (real_ns / NSEC_PER_SEC) % SEC_PER_DAY;
    uint64_t const TARGET_SEC_OF_DAY = (hour * 60ULL * 60ULL) + (minute * 60ULL);
    uint64_t delta_sec =
        TARGET_SEC_OF_DAY > NOW_SEC_OF_DAY ? TARGET_SEC_OF_DAY - NOW_SEC_OF_DAY : (SEC_PER_DAY - NOW_SEC_OF_DAY) + TARGET_SEC_OF_DAY;
    deadline_mono_ns = mono_ns + (delta_sec * NSEC_PER_SEC);
    return true;
}

auto write_all(int fd, const char* data, size_t len) -> bool {
    size_t written = 0;
    while (written < len) {
        ssize_t const RET = ker::abi::vfs::write(fd, data + written, len - written);
        if (RET <= 0) {
            return false;
        }
        written += static_cast<size_t>(RET);
    }
    return true;
}

auto send_request(Action action, const char* original_when, uint64_t deadline_mono_ns, bool cancel) -> int {
    int const FD = ker::abi::vfs::open(REQUEST_TMP_PATH, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (FD < 0) {
        return FD;
    }

    std::array<char, 512> request{};
    int const LEN = std::snprintf(request.data(), request.size(),
                                  "action=%s\n"
                                  "deadline_mono_ns=%llu\n"
                                  "original_when=%s\n"
                                  "requester_pid=%llu\n"
                                  "cancel=%d\n",
                                  action_name(action), static_cast<unsigned long long>(deadline_mono_ns),
                                  original_when != nullptr ? original_when : "now", static_cast<unsigned long long>(ker::process::getpid()),
                                  cancel ? 1 : 0);
    bool ok = LEN > 0 && static_cast<size_t>(LEN) < request.size() && write_all(FD, request.data(), static_cast<size_t>(LEN));
    ker::abi::vfs::close(FD);
    if (!ok) {
        ker::abi::vfs::unlink(REQUEST_TMP_PATH);
        return -EIO;
    }
    int const RENAME_RET = ker::abi::vfs::rename(REQUEST_TMP_PATH, REQUEST_PATH);
    if (RENAME_RET < 0) {
        ker::abi::vfs::unlink(REQUEST_TMP_PATH);
        return RENAME_RET;
    }
    int64_t const KILL_RET = ker::process::kill(1, SIGHUP);
    return KILL_RET < 0 ? static_cast<int>(KILL_RET) : 0;
}

void usage(const char* argv0) {
    std::fprintf(stderr, "usage: %s [-r|-p|-h] [-f] [now|+MINUTES|HH:MM]\n       %s -c\n", program_basename(argv0),
                 program_basename(argv0));
}

auto canonical_when(const char* when) -> const char* {
    if (when == nullptr || std::strcmp(when, "now") == 0) {
        return "+0";
    }
    return when;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    Action action = Action::POWEROFF;
    const char* const NAME = program_basename(argc > 0 ? argv[0] : "shutdown");
    if (std::strcmp(NAME, "reboot") == 0) {
        action = Action::REBOOT;
    } else if (std::strcmp(NAME, "halt") == 0) {
        action = Action::HALT;
    } else if (std::strcmp(NAME, "poweroff") == 0) {
        action = Action::POWEROFF;
    }

    bool force = false;
    bool cancel = false;
    const char* when = "now";
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "-r") == 0) {
            action = Action::REBOOT;
        } else if (std::strcmp(argv[i], "-p") == 0) {
            action = Action::POWEROFF;
        } else if (std::strcmp(argv[i], "-h") == 0) {
            action = Action::HALT;
        } else if (std::strcmp(argv[i], "-f") == 0) {
            force = true;
        } else if (std::strcmp(argv[i], "-c") == 0) {
            cancel = true;
        } else if (argv[i][0] == '-') {
            usage(argv[0]);
            return 2;
        } else {
            when = argv[i];
        }
    }

    if (force) {
        if (reboot(reboot_cmd(action)) != 0) {
            std::fprintf(stderr, "%s: reboot syscall failed: errno=%d\n", NAME, errno);
            return 1;
        }
        return 0;
    }

    uint64_t deadline = 0;
    if (!cancel && !parse_when(when, deadline)) {
        usage(argv[0]);
        return 2;
    }
    int const RET = send_request(action, canonical_when(when), deadline, cancel);
    if (RET < 0) {
        std::fprintf(stderr, "%s: failed to request shutdown: %d\n", NAME, RET);
        return 1;
    }
    return 0;
}
