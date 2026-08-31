#include "wki_chaos_workload.hpp"

#include <abi-bits/fcntl.h>
#include <abi-bits/in.h>
#include <abi-bits/mode_t.h>
#include <abi-bits/wait.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>  // NOLINT(modernize-deprecated-headers): WOS exposes PATH_MAX here.
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS declares kill()/signal() here.
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS declares nanosleep() here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>
#include <span>
#include <string_view>

namespace {

constexpr mode_t MODE_0600 = S_IRUSR | S_IWUSR;
constexpr mode_t MODE_0700 = S_IRUSR | S_IWUSR | S_IXUSR;
constexpr int MAX_TIMEOUT_MS = 300'000;
constexpr int POLL_SLICE_MS = 25;
constexpr int CHILD_KILL_GRACE_MS = 2'000;
constexpr std::size_t MAX_ID_LEN = 64;
constexpr std::size_t MAX_HOST_LEN = 63;
constexpr std::size_t I1_DATA_SIZE = std::size_t{16} * 1024;
constexpr std::size_t I1_ISSUE_CHUNK = std::size_t{2} * 1024;
constexpr std::size_t I1_DISCARD_PREFIX_SIZE = 64;
constexpr std::size_t VFS_PAYLOAD_SIZE = 4096;
constexpr std::size_t V2_LANE_COUNT = 8;
constexpr std::string_view DEFAULT_CHECKPOINT_DIR = "/tmp/wki-chaos-workload";
constexpr std::array<const char*, 7> I1_CHECKPOINTS{
    "pipe-open", "before-data", "data-issued", "before-close", "close-issued", "before-discard", "reader-discarded",
};
constexpr std::array<const char*, 3> C1_CHECKPOINTS{"submit-issued", "cancel-issued", "cancel-complete"};
constexpr std::array<const char*, 4> C2_CHECKPOINTS{"task-running", "exit-issued", "successor-running", "successor-complete"};
constexpr std::array<const char*, 9> V1_CHECKPOINTS{
    "file-open",     "before-write", "write-done",   "before-utimens", "utimens-done",
    "before-rename", "rename-done",  "before-close", "close-done",
};
constexpr std::array<const char*, 4> V2_CHECKPOINTS{"lanes-open", "operations-done", "before-close", "files-closed"};
constexpr std::array<const char*, 4> I2_CHECKPOINTS{"control-issued", "old-session-fenced", "successor-issued", "successor-complete"};

struct Options {
    const char* command = nullptr;
    const char* scenario = nullptr;
    const char* run_id = nullptr;
    const char* target = nullptr;
    const char* target2 = nullptr;
    const char* checkpoint = nullptr;
    const char* checkpoint_dir = DEFAULT_CHECKPOINT_DIR.data();
    int timeout_ms = 0;
    int data_fd = -1;
    int ready_fd = -1;
};

struct StateRecord {
    std::array<char, 16> scenario{};
    int index = -1;
    std::array<char, 65> checkpoint{};
    std::array<char, 16> phase{};
    int pid = -1;
};

struct RunContext {
    std::array<char, PATH_MAX> run_dir{};
    const char* scenario = nullptr;
    int timeout_ms = 0;
    int64_t deadline_ms = -1;
};

auto safe_id(const char* value, std::size_t max_len) -> bool {
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
    std::size_t const LENGTH = std::strlen(value);
    if (LENGTH > max_len || std::strcmp(value, ".") == 0 || std::strcmp(value, "..") == 0) {
        return false;
    }
    for (const unsigned char* cursor = reinterpret_cast<const unsigned char*>(value); *cursor != '\0'; ++cursor) {
        bool const VALID = (*cursor >= 'a' && *cursor <= 'z') || (*cursor >= 'A' && *cursor <= 'Z') || (*cursor >= '0' && *cursor <= '9') ||
                           *cursor == '.' || *cursor == '_' || *cursor == '-';
        if (!VALID) {
            return false;
        }
    }
    return true;
}

auto safe_absolute_dir(const char* value) -> bool {
    if (value == nullptr || value[0] != '/' || std::strlen(value) >= PATH_MAX - MAX_ID_LEN - 2) {
        return false;
    }
    const char* component = value + 1;
    while (true) {
        const char* slash = std::strchr(component, '/');
        std::size_t const LENGTH = slash == nullptr ? std::strlen(component) : static_cast<std::size_t>(slash - component);
        if (LENGTH == 0 || (LENGTH == 1 && component[0] == '.') || (LENGTH == 2 && component[0] == '.' && component[1] == '.')) {
            return false;
        }
        for (std::size_t i = 0; i < LENGTH; ++i) {
            unsigned char const C = static_cast<unsigned char>(component[i]);
            bool const VALID =
                (C >= 'a' && C <= 'z') || (C >= 'A' && C <= 'Z') || (C >= '0' && C <= '9') || C == '.' || C == '_' || C == '-';
            if (!VALID) {
                return false;
            }
        }
        if (slash == nullptr) {
            return true;
        }
        component = slash + 1;
    }
}

auto parse_int(const char* text, int minimum, int maximum, int* out) -> bool {
    if (text == nullptr || out == nullptr) {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    long const VALUE = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || errno != 0 || VALUE < minimum || VALUE > maximum) {
        return false;
    }
    *out = static_cast<int>(VALUE);
    return true;
}

auto monotonic_now_ms() -> int64_t {
    timespec now{};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1'000'000'000L) {
        return -1;
    }
    constexpr int64_t MS_PER_SECOND = 1'000;
    int64_t const NSEC_MS = static_cast<int64_t>(now.tv_nsec) / 1'000'000;
    int64_t const SECONDS = static_cast<int64_t>(now.tv_sec);
    if (SECONDS > (INT64_MAX - NSEC_MS) / MS_PER_SECOND) {
        return INT64_MAX;
    }
    return (SECONDS * MS_PER_SECOND) + NSEC_MS;
}

auto deadline_after_ms(int timeout_ms) -> int64_t {
    int64_t const NOW = monotonic_now_ms();
    if (NOW < 0 || NOW > INT64_MAX - timeout_ms) {
        return -1;
    }
    return NOW + timeout_ms;
}

auto remaining_ms(int64_t deadline_ms) -> int {
    int64_t const NOW = monotonic_now_ms();
    if (NOW < 0 || deadline_ms <= NOW) {
        return 0;
    }
    int64_t const REMAINING = deadline_ms - NOW;
    return static_cast<int>(REMAINING > INT_MAX ? INT_MAX : REMAINING);
}

void poll_pause() {
    timespec delay{.tv_sec = 0, .tv_nsec = 10'000'000L};
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

template <std::size_t N, typename... Args>
auto format_path(std::array<char, N>& out, const char* format, Args... args) -> bool {
    int const WRITTEN = std::snprintf(out.data(), out.size(), format, args...);
    return WRITTEN >= 0 && static_cast<std::size_t>(WRITTEN) < out.size();
}

auto write_all_fd(int fd, const void* data, std::size_t size) -> bool {
    auto* cursor = static_cast<const uint8_t*>(data);
    while (size > 0) {
        ssize_t const WRITTEN = write(fd, cursor, size);
        if (WRITTEN < 0 && errno == EINTR) {
            continue;
        }
        if (WRITTEN <= 0) {
            return false;
        }
        cursor += WRITTEN;
        size -= static_cast<std::size_t>(WRITTEN);
    }
    return true;
}

auto write_new_file(const char* path, const void* data, std::size_t size) -> bool {
    int const FD = open(path, O_WRONLY | O_CREAT | O_EXCL, MODE_0600);
    if (FD < 0) {
        return false;
    }
    bool ok = write_all_fd(FD, data, size);
    int saved_errno = ok ? 0 : errno;
    if (ok && fsync(FD) != 0) {
        ok = false;
        saved_errno = errno;
    }
    if (close(FD) != 0 && ok) {
        ok = false;
        saved_errno = errno;
    }
    if (!ok) {
        errno = saved_errno;
    }
    return ok;
}

auto read_bounded_file(const char* path, char* out, std::size_t capacity) -> ssize_t {
    if (capacity < 2) {
        errno = EINVAL;
        return -1;
    }
    int const FD = open(path, O_RDONLY);
    if (FD < 0) {
        return -1;
    }
    ssize_t const READ = read(FD, out, capacity - 1);
    int const SAVED_ERRNO = errno;
    close(FD);
    errno = SAVED_ERRNO;
    if (READ < 0) {
        return -1;
    }
    out[READ] = '\0';
    return READ;
}

auto file_exists(const char* path) -> bool {
    struct stat info{};
    return stat(path, &info) == 0;
}

auto unlink_if_present(const char* path) -> bool { return unlink(path) == 0 || errno == ENOENT; }

auto ensure_base_dir(const char* path) -> bool {
    if (mkdir(path, MODE_0700) == 0) {
        return true;
    }
    if (errno != EEXIST) {
        return false;
    }
    struct stat info{};
    return stat(path, &info) == 0 && S_ISDIR(info.st_mode) && (info.st_mode & 0777) == MODE_0700;
}

auto parse_options(int argc, char** argv, Options* out) -> bool {
    if (argc < 2 || out == nullptr) {
        return false;
    }
    out->command = argv[1];
    int index = 2;
    if (std::strcmp(out->command, "wait") != 0 && std::strcmp(out->command, "advance") != 0 && std::strcmp(out->command, "cleanup") != 0 &&
        std::strcmp(out->command, "helper") != 0) {
        out->scenario = out->command;
        out->command = "run";
    } else if (std::strcmp(out->command, "helper") == 0) {
        if (index >= argc) {
            return false;
        }
        out->scenario = argv[index++];
    }

    while (index < argc) {
        if (index + 1 >= argc) {
            return false;
        }
        const char* name = argv[index++];
        const char* value = argv[index++];
        if (std::strcmp(name, "--run-id") == 0) {
            out->run_id = value;
        } else if (std::strcmp(name, "--target") == 0) {
            out->target = value;
        } else if (std::strcmp(name, "--target2") == 0) {
            out->target2 = value;
        } else if (std::strcmp(name, "--checkpoint") == 0) {
            out->checkpoint = value;
        } else if (std::strcmp(name, "--checkpoint-dir") == 0) {
            out->checkpoint_dir = value;
        } else if (std::strcmp(name, "--timeout-ms") == 0) {
            if (!parse_int(value, 1, MAX_TIMEOUT_MS, &out->timeout_ms)) {
                return false;
            }
        } else if (std::strcmp(name, "--data-fd") == 0) {
            if (!parse_int(value, 0, INT_MAX, &out->data_fd)) {
                return false;
            }
        } else if (std::strcmp(name, "--ready-fd") == 0) {
            if (!parse_int(value, 0, INT_MAX, &out->ready_fd)) {
                return false;
            }
        } else {
            return false;
        }
    }
    return true;
}

auto valid_invocation(const Options& options) -> bool {
    bool const HAS_INTERNAL_FDS = options.data_fd >= 0 || options.ready_fd >= 0;
    if (std::strcmp(options.command, "helper") == 0) {
        return options.scenario != nullptr && options.run_id == nullptr && options.target == nullptr && options.target2 == nullptr &&
               options.checkpoint == nullptr && options.timeout_ms > 0 && options.data_fd >= 0 && options.ready_fd >= 0;
    }
    if (std::strcmp(options.command, "wait") == 0) {
        return options.scenario == nullptr && options.run_id != nullptr && options.target == nullptr && options.target2 == nullptr &&
               options.checkpoint != nullptr && options.timeout_ms > 0 && !HAS_INTERNAL_FDS;
    }
    if (std::strcmp(options.command, "advance") == 0) {
        return options.scenario == nullptr && options.run_id != nullptr && options.target == nullptr && options.target2 == nullptr &&
               options.checkpoint != nullptr && options.timeout_ms == 0 && !HAS_INTERNAL_FDS;
    }
    if (std::strcmp(options.command, "cleanup") == 0) {
        return options.scenario == nullptr && options.run_id != nullptr && options.target == nullptr && options.target2 == nullptr &&
               options.checkpoint == nullptr && options.timeout_ms > 0 && !HAS_INTERNAL_FDS;
    }
    return std::strcmp(options.command, "run") == 0 && options.scenario != nullptr && options.run_id != nullptr &&
           options.target != nullptr && options.target2 == nullptr && options.checkpoint == nullptr && options.timeout_ms > 0 &&
           !HAS_INTERNAL_FDS;
}

auto checkpoint_list(const char* scenario) -> std::span<const char* const> {
    if (scenario == nullptr) {
        return {};
    }
    if (std::strcmp(scenario, "C1") == 0) {
        return C1_CHECKPOINTS;
    }
    if (std::strcmp(scenario, "C2") == 0) {
        return C2_CHECKPOINTS;
    }
    if (std::strcmp(scenario, "V1") == 0) {
        return V1_CHECKPOINTS;
    }
    if (std::strcmp(scenario, "V2") == 0) {
        return V2_CHECKPOINTS;
    }
    if (std::strcmp(scenario, "I1") == 0) {
        return I1_CHECKPOINTS;
    }
    if (std::strcmp(scenario, "I2") == 0) {
        return I2_CHECKPOINTS;
    }
    return {};
}

auto checkpoint_index(const char* scenario, const char* checkpoint) -> int {
    auto const checkpoints = checkpoint_list(scenario);
    if (checkpoints.empty() || checkpoint == nullptr) {
        return -1;
    }
    for (std::size_t i = 0; i < checkpoints.size(); ++i) {
        if (std::strcmp(checkpoints[i], checkpoint) == 0) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

auto load_metadata(const char* run_dir, StateRecord* metadata, int* timeout_ms) -> bool {
    std::array<char, PATH_MAX> path{};
    std::array<char, 256> contents{};
    if (!format_path(path, "%s/metadata", run_dir) || read_bounded_file(path.data(), contents.data(), contents.size()) <= 0) {
        return false;
    }
    std::array<char, 8> version{};
    int parsed_timeout = 0;
    int const MATCHED =
        std::sscanf(contents.data(), "%7s %15s %d %d", version.data(), metadata->scenario.data(), &metadata->pid, &parsed_timeout);
    if (MATCHED != 4 || std::strcmp(version.data(), "v1") != 0 || metadata->pid <= 0 || parsed_timeout < 1 ||
        parsed_timeout > MAX_TIMEOUT_MS || checkpoint_list(metadata->scenario.data()).empty()) {
        return false;
    }
    if (timeout_ms != nullptr) {
        *timeout_ms = parsed_timeout;
    }
    return true;
}

auto load_state(const char* run_dir, StateRecord* state) -> bool {
    std::array<char, PATH_MAX> path{};
    std::array<char, 256> contents{};
    if (!format_path(path, "%s/state", run_dir) || read_bounded_file(path.data(), contents.data(), contents.size()) <= 0) {
        return false;
    }
    std::array<char, 8> version{};
    int const MATCHED = std::sscanf(contents.data(), "%7s %15s %d %64s %15s %d", version.data(), state->scenario.data(), &state->index,
                                    state->checkpoint.data(), state->phase.data(), &state->pid);
    return MATCHED == 6 && std::strcmp(version.data(), "v1") == 0 && state->pid > 0;
}

auto publish_state(const RunContext& context, int index, const char* checkpoint, const char* phase) -> bool {
    std::array<char, PATH_MAX> temporary{};
    std::array<char, PATH_MAX> destination{};
    std::array<char, 256> contents{};
    int const PID = static_cast<int>(getpid());
    if (!format_path(temporary, "%s/.state.%d.%d.%s", context.run_dir.data(), PID, index, phase) ||
        !format_path(destination, "%s/state", context.run_dir.data())) {
        errno = ENAMETOOLONG;
        return false;
    }
    int const LENGTH =
        std::snprintf(contents.data(), contents.size(), "v1 %s %d %s %s %d\n", context.scenario, index, checkpoint, phase, PID);
    if (LENGTH < 0 || static_cast<std::size_t>(LENGTH) >= contents.size() ||
        !write_new_file(temporary.data(), contents.data(), static_cast<std::size_t>(LENGTH))) {
        return false;
    }
    return rename(temporary.data(), destination.data()) == 0;
}

auto cancelled(const RunContext& context) -> bool {
    std::array<char, PATH_MAX> path{};
    return format_path(path, "%s/cancel", context.run_dir.data()) && file_exists(path.data());
}

auto checkpoint_wait(RunContext& context, int index, const char* name) -> bool {
    if (!publish_state(context, index, name, "ready")) {
        return false;
    }
    std::array<char, PATH_MAX> advance{};
    if (!format_path(advance, "%s/advance.%d.%s", context.run_dir.data(), index, name)) {
        errno = ENAMETOOLONG;
        return false;
    }
    while (remaining_ms(context.deadline_ms) > 0) {
        if (cancelled(context)) {
            errno = ECANCELED;
            return false;
        }
        if (file_exists(advance.data())) {
            return publish_state(context, index, name, "advanced");
        }
        poll_pause();
    }
    errno = ETIMEDOUT;
    return false;
}

auto initialize_run(const Options& options, RunContext* context) -> bool {
    if (!safe_id(options.run_id, MAX_ID_LEN) || !safe_id(options.target, MAX_HOST_LEN) || !safe_absolute_dir(options.checkpoint_dir) ||
        options.timeout_ms < 1 || checkpoint_list(options.scenario).empty() || !ensure_base_dir(options.checkpoint_dir) ||
        !format_path(context->run_dir, "%s/%s", options.checkpoint_dir, options.run_id)) {
        errno = EINVAL;
        return false;
    }
    if (mkdir(context->run_dir.data(), MODE_0700) != 0) {
        return false;
    }
    context->scenario = options.scenario;
    context->timeout_ms = options.timeout_ms;
    context->deadline_ms = deadline_after_ms(options.timeout_ms);
    if (context->deadline_ms < 0) {
        int const SAVED_ERRNO = errno;
        static_cast<void>(rmdir(context->run_dir.data()));
        errno = SAVED_ERRNO;
        return false;
    }
    std::array<char, PATH_MAX> metadata_path{};
    std::array<char, 256> metadata{};
    if (!format_path(metadata_path, "%s/metadata", context->run_dir.data())) {
        static_cast<void>(rmdir(context->run_dir.data()));
        errno = ENAMETOOLONG;
        return false;
    }
    int const LENGTH =
        std::snprintf(metadata.data(), metadata.size(), "v1 %s %d %d\n", options.scenario, static_cast<int>(getpid()), options.timeout_ms);
    bool const WRITTEN = LENGTH > 0 && static_cast<std::size_t>(LENGTH) < metadata.size() &&
                         write_new_file(metadata_path.data(), metadata.data(), static_cast<std::size_t>(LENGTH));
    if (!WRITTEN) {
        int const SAVED_ERRNO = errno;
        static_cast<void>(unlink(metadata_path.data()));
        static_cast<void>(rmdir(context->run_dir.data()));
        errno = SAVED_ERRNO;
    }
    return WRITTEN;
}

void finish_run(const RunContext& context, int result) {
    std::array<char, PATH_MAX> path{};
    std::array<char, 64> contents{};
    if (format_path(path, "%s/done", context.run_dir.data())) {
        int const LENGTH = std::snprintf(contents.data(), contents.size(), "v1 result=%d\n", result);
        if (LENGTH > 0 && static_cast<std::size_t>(LENGTH) < contents.size()) {
            static_cast<void>(write_new_file(path.data(), contents.data(), static_cast<std::size_t>(LENGTH)));
        }
    }
    static_cast<void>(
        publish_state(context, static_cast<int>(checkpoint_list(context.scenario).size()), "complete", result == 0 ? "passed" : "failed"));
}

auto wait_fd(int fd, short events, int64_t deadline_ms) -> bool {
    while (remaining_ms(deadline_ms) > 0) {
        pollfd descriptor{.fd = fd, .events = events, .revents = 0};
        int const TIMEOUT = std::min(POLL_SLICE_MS, remaining_ms(deadline_ms));
        int const READY = poll(&descriptor, 1, TIMEOUT);
        if (READY > 0) {
            if ((descriptor.revents & POLLNVAL) != 0) {
                errno = EBADF;
                return false;
            }
            // POLLERR and POLLHUP are readiness notifications. Retry the
            // requested I/O so it reports the authoritative error or EOF.
            if ((descriptor.revents & (events | POLLERR | POLLHUP)) != 0) {
                return true;
            }
        } else if (READY < 0 && errno != EINTR) {
            return false;
        }
    }
    errno = ETIMEDOUT;
    return false;
}

auto set_nonblocking(int fd, int* old_flags) -> bool {
    int const FLAGS = fcntl(fd, F_GETFL, 0);
    if (FLAGS < 0 || fcntl(fd, F_SETFL, FLAGS | O_NONBLOCK) != 0) {
        return false;
    }
    *old_flags = FLAGS;
    return true;
}

void restore_flags(int fd, int old_flags) {
    if (old_flags >= 0) {
        static_cast<void>(fcntl(fd, F_SETFL, old_flags));
    }
}

auto write_exact_timeout(int fd, const uint8_t* data, std::size_t size, int64_t deadline_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking(fd, &old_flags)) {
        return false;
    }
    std::size_t offset = 0;
    while (offset < size && remaining_ms(deadline_ms) > 0) {
        ssize_t const WRITTEN = write(fd, data + offset, size - offset);
        if (WRITTEN > 0) {
            offset += static_cast<std::size_t>(WRITTEN);
            continue;
        }
        if (WRITTEN < 0 && errno == EINTR) {
            continue;
        }
        if (WRITTEN < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_fd(fd, POLLOUT, deadline_ms)) {
                break;
            }
            continue;
        }
        restore_flags(fd, old_flags);
        return false;
    }
    restore_flags(fd, old_flags);
    if (offset != size) {
        errno = ETIMEDOUT;
        return false;
    }
    return true;
}

auto read_exact_timeout(int fd, uint8_t* data, std::size_t size, int64_t deadline_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking(fd, &old_flags)) {
        return false;
    }
    std::size_t offset = 0;
    while (offset < size && remaining_ms(deadline_ms) > 0) {
        ssize_t const READ = read(fd, data + offset, size - offset);
        if (READ > 0) {
            offset += static_cast<std::size_t>(READ);
            continue;
        }
        if (READ < 0 && errno == EINTR) {
            continue;
        }
        if (READ < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_fd(fd, POLLIN, deadline_ms)) {
                break;
            }
            continue;
        }
        restore_flags(fd, old_flags);
        errno = READ == 0 ? EPIPE : errno;
        return false;
    }
    restore_flags(fd, old_flags);
    if (offset != size) {
        errno = ETIMEDOUT;
        return false;
    }
    return true;
}

auto require_eof_timeout(int fd, int64_t deadline_ms) -> bool {
    uint8_t byte = 0;
    int old_flags = -1;
    if (!set_nonblocking(fd, &old_flags)) {
        return false;
    }
    while (remaining_ms(deadline_ms) > 0) {
        ssize_t const READ = read(fd, &byte, 1);
        if (READ == 0) {
            restore_flags(fd, old_flags);
            return true;
        }
        if (READ > 0) {
            restore_flags(fd, old_flags);
            errno = EOVERFLOW;
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (!wait_fd(fd, POLLIN, deadline_ms)) {
                break;
            }
            continue;
        }
        restore_flags(fd, old_flags);
        return false;
    }
    restore_flags(fd, old_flags);
    errno = ETIMEDOUT;
    return false;
}

auto wait_child(pid_t pid, int64_t deadline_ms, int* status) -> bool {
    while (remaining_ms(deadline_ms) > 0) {
        pid_t const WAITED = waitpid(pid, status, WNOHANG);
        if (WAITED == pid) {
            return true;
        }
        if (WAITED < 0 && errno != EINTR) {
            return false;
        }
        poll_pause();
    }
    static_cast<void>(kill(pid, SIGKILL));
    int64_t const KILL_DEADLINE = deadline_after_ms(CHILD_KILL_GRACE_MS);
    while (KILL_DEADLINE >= 0 && remaining_ms(KILL_DEADLINE) > 0) {
        pid_t const WAITED = waitpid(pid, status, WNOHANG);
        if (WAITED == pid) {
            errno = ETIMEDOUT;
            return false;
        }
        if (WAITED < 0 && errno != EINTR) {
            return false;
        }
        poll_pause();
    }
    errno = ETIMEDOUT;
    return false;
}

void terminate_child(pid_t pid) {
    if (pid <= 0) {
        return;
    }
    static_cast<void>(kill(pid, SIGKILL));
    int status = 0;
    int64_t const DEADLINE = deadline_after_ms(CHILD_KILL_GRACE_MS);
    while (DEADLINE >= 0 && remaining_ms(DEADLINE) > 0) {
        pid_t const WAITED = waitpid(pid, &status, WNOHANG);
        if (WAITED == pid || (WAITED < 0 && errno != EINTR)) {
            return;
        }
        poll_pause();
    }
}

auto fill_i1_data(std::array<uint8_t, I1_DATA_SIZE>* data) -> void {
    for (std::size_t i = 0; i < data->size(); ++i) {
        data->at(i) = static_cast<uint8_t>(((i * 131U) + 17U) & 0xFFU);
    }
}

auto helper_i1_read(const Options& options) -> int {
    if (options.data_fd < 0 || options.ready_fd < 0 || options.timeout_ms < 1) {
        return 64;
    }
    int64_t const DEADLINE = deadline_after_ms(options.timeout_ms);
    uint8_t const READY = 0xA1;
    if (DEADLINE < 0 || !write_exact_timeout(options.ready_fd, &READY, 1, DEADLINE)) {
        return 1;
    }
    std::array<uint8_t, I1_DATA_SIZE> expected{};
    std::array<uint8_t, I1_DATA_SIZE> received{};
    fill_i1_data(&expected);
    constexpr std::array<uint8_t, 3> ACKS{0xB1, 0xB2, 0xB3};
    std::size_t offset = 0;
    for (std::size_t stage = 0; stage < ACKS.size(); ++stage) {
        std::size_t const STAGE_SIZE = stage < 2 ? I1_ISSUE_CHUNK : received.size() - offset;
        if (!read_exact_timeout(options.data_fd, received.data() + offset, STAGE_SIZE, DEADLINE) ||
            !write_exact_timeout(options.ready_fd, &ACKS.at(stage), 1, DEADLINE)) {
            close(options.ready_fd);
            close(options.data_fd);
            return 1;
        }
        offset += STAGE_SIZE;
    }
    bool const EOF_OK = require_eof_timeout(options.data_fd, DEADLINE);
    uint8_t const EOF_ACK = 0xB4;
    bool const ACK_OK = EOF_OK && write_exact_timeout(options.ready_fd, &EOF_ACK, 1, DEADLINE);
    close(options.ready_fd);
    close(options.data_fd);
    return ACK_OK && received == expected ? 0 : 1;
}

auto helper_i1_discard_writer(const Options& options) -> int {
    if (options.data_fd < 0 || options.ready_fd < 0 || options.timeout_ms < 1) {
        return 64;
    }
    static_cast<void>(signal(SIGPIPE, SIG_IGN));
    int64_t const DEADLINE = deadline_after_ms(options.timeout_ms);
    std::array<uint8_t, I1_DISCARD_PREFIX_SIZE> prefix{};
    prefix.fill(0xD1);
    if (DEADLINE < 0 || !write_exact_timeout(options.data_fd, prefix.data(), prefix.size(), DEADLINE)) {
        return 1;
    }
    uint8_t const READY = 0xA2;
    if (!write_exact_timeout(options.ready_fd, &READY, 1, DEADLINE)) {
        return 1;
    }
    close(options.ready_fd);

    int old_flags = -1;
    if (!set_nonblocking(options.data_fd, &old_flags)) {
        return 1;
    }
    while (remaining_ms(DEADLINE) > 0) {
        ssize_t const WRITTEN = write(options.data_fd, prefix.data(), prefix.size());
        if (WRITTEN > 0 || (WRITTEN < 0 && errno == EINTR)) {
            continue;
        }
        if (WRITTEN < 0 && errno == EPIPE) {
            restore_flags(options.data_fd, old_flags);
            close(options.data_fd);
            return 0;
        }
        if (WRITTEN < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_fd(options.data_fd, POLLOUT, DEADLINE) && errno != ETIMEDOUT) {
                break;
            }
            continue;
        }
        break;
    }
    restore_flags(options.data_fd, old_flags);
    close(options.data_fd);
    return 1;
}

auto spawn_i1_helper(const char* target, const char* mode, int data_fd, int ready_fd, int close_data_fd, int close_ready_fd, int timeout_ms)
    -> pid_t {
    if (ker::process::setwkitarget(target, std::strlen(target), ker::process::WKI_TARGET_FLAG_STRICT) < 0) {
        return -1;
    }
    pid_t const PID = fork();
    if (PID == 0) {
        if (close_data_fd >= 0) {
            close(close_data_fd);
        }
        if (close_ready_fd >= 0) {
            close(close_ready_fd);
        }
        std::array<char, 16> data_fd_text{};
        std::array<char, 16> ready_fd_text{};
        std::array<char, 16> timeout_text{};
        std::array<char, 32> mode_text{};
        static_cast<void>(std::snprintf(data_fd_text.data(), data_fd_text.size(), "%d", data_fd));
        static_cast<void>(std::snprintf(ready_fd_text.data(), ready_fd_text.size(), "%d", ready_fd));
        static_cast<void>(std::snprintf(timeout_text.data(), timeout_text.size(), "%d", timeout_ms));
        static_cast<void>(std::snprintf(mode_text.data(), mode_text.size(), "%s", mode));
        auto executable = std::to_array("/usr/bin/testprog");
        auto workload = std::to_array("wki-chaos-workload");
        auto helper = std::to_array("helper");
        auto data_flag = std::to_array("--data-fd");
        auto ready_flag = std::to_array("--ready-fd");
        auto timeout_flag = std::to_array("--timeout-ms");
        std::array<char*, 12> arguments{
            executable.data(), workload.data(),      helper.data(),       mode_text.data(),    data_flag.data(), data_fd_text.data(),
            ready_flag.data(), ready_fd_text.data(), timeout_flag.data(), timeout_text.data(), nullptr,
        };
        execve(executable.data(), arguments.data(), nullptr);
        _exit(127);
    }
    int const SAVED_ERRNO = errno;
    static_cast<void>(ker::process::setwkitarget(nullptr, 0, 0));
    errno = SAVED_ERRNO;
    return PID;
}

auto await_helper_signal(int ready_fd, uint8_t expected, int64_t deadline_ms) -> bool {
    uint8_t ready = 0;
    return read_exact_timeout(ready_fd, &ready, 1, deadline_ms) && ready == expected;
}

auto spawn_remote_helper(const char* target, const char* mode, int data_fd, int ready_fd, int close_data_fd, int close_ready_fd,
                         int timeout_ms) -> pid_t {
    return spawn_i1_helper(target, mode, data_fd, ready_fd, close_data_fd, close_ready_fd, timeout_ms);
}

auto helper_compute_hold(const Options& options) -> int {
    if (options.data_fd < 0 || options.ready_fd < 0 || options.timeout_ms < 1) {
        return 64;
    }
    int64_t const DEADLINE = deadline_after_ms(options.timeout_ms);
    uint8_t const READY = 0xC1;
    uint8_t command = 0;
    if (DEADLINE < 0 || !write_exact_timeout(options.ready_fd, &READY, 1, DEADLINE) ||
        !read_exact_timeout(options.data_fd, &command, 1, DEADLINE)) {
        return 1;
    }
    close(options.ready_fd);
    close(options.data_fd);
    if (command == 0xC2) {
        return 0;
    }
    if (command == 0xC3) {
        static_cast<void>(signal(SIGTERM, SIG_DFL));
        while (true) {
            pause();
        }
    }
    return 1;
}

auto helper_i2_socket_getpeername(const Options& options) -> int {
    if (options.data_fd < 0 || options.ready_fd < 0) {
        return 64;
    }
    sockaddr_in peer{};
    socklen_t peer_length = sizeof(peer);
    int const RESULT = getpeername(options.data_fd, reinterpret_cast<sockaddr*>(&peer), &peer_length);
    close(options.ready_fd);
    close(options.data_fd);
    return RESULT == 0 && peer_length == sizeof(peer) && peer.sin_family == AF_INET && ntohl(peer.sin_addr.s_addr) == INADDR_LOOPBACK &&
                   peer.sin_port != 0
               ? 0
               : 1;
}

auto spawn_compute_child(const Options& options, const char* mode, std::array<int, 2>* command, std::array<int, 2>* ready) -> pid_t {
    if (pipe(command->data()) != 0) {
        return -1;
    }
    if (pipe(ready->data()) != 0) {
        close(command->at(0));
        close(command->at(1));
        command->fill(-1);
        return -1;
    }
    // The remote child must not retain the controller's ends. Exporting both
    // ends would start a WKI read pump on ready[0], competing with the
    // controller for the helper's READY byte.
    pid_t const PID =
        spawn_remote_helper(options.target, mode, command->at(0), ready->at(1), command->at(1), ready->at(0), options.timeout_ms);
    if (PID < 0) {
        close(command->at(0));
        close(command->at(1));
        close(ready->at(0));
        close(ready->at(1));
        command->fill(-1);
        ready->fill(-1);
        return -1;
    }
    close(command->at(0));
    command->at(0) = -1;
    close(ready->at(1));
    ready->at(1) = -1;
    return PID;
}

auto close_pair(std::array<int, 2>& fds) -> void {
    for (int& fd : fds) {
        if (fd >= 0) {
            close(fd);
            fd = -1;
        }
    }
}

auto run_c1(const Options& options, RunContext& context) -> int {
    std::array<int, 2> command{-1, -1};
    std::array<int, 2> ready{-1, -1};
    pid_t child = spawn_compute_child(options, "C1-hold", &command, &ready);
    int status = 0;
    // The receiver-side TASK_SUBMIT is held by the scenario before this
    // checkpoint. Killing the still-submitting proxy makes TASK_CANCEL race
    // ahead of receiver publication; the injector's exact rule counters are
    // the external barrier for that ordering.
    if (child < 0 || !checkpoint_wait(context, 0, C1_CHECKPOINTS.at(0))) {
        goto fail;
    }
    if (kill(child, SIGTERM) != 0 || !checkpoint_wait(context, 1, C1_CHECKPOINTS.at(1)) ||
        !wait_child(child, context.deadline_ms, &status)) {
        goto fail;
    }
    child = -1;
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGTERM || !checkpoint_wait(context, 2, C1_CHECKPOINTS.at(2))) {
        goto fail;
    }
    close_pair(command);
    close_pair(ready);
    return 0;
fail:
    close_pair(command);
    close_pair(ready);
    terminate_child(child);
    return 1;
}

auto run_c2(const Options& options, RunContext& context) -> int {
    std::array<int, 2> first_command{-1, -1};
    std::array<int, 2> first_ready{-1, -1};
    std::array<int, 2> next_command{-1, -1};
    std::array<int, 2> next_ready{-1, -1};
    pid_t first = spawn_compute_child(options, "C2-hold", &first_command, &first_ready);
    pid_t successor = -1;
    int status = 0;
    uint8_t exit_command = 0xC2;
    if (first < 0 || !await_helper_signal(first_ready[0], 0xC1, context.deadline_ms) ||
        !checkpoint_wait(context, 0, C2_CHECKPOINTS.at(0)) ||
        !write_exact_timeout(first_command[1], &exit_command, 1, context.deadline_ms) ||
        !checkpoint_wait(context, 1, C2_CHECKPOINTS.at(1)) || !wait_child(first, context.deadline_ms, &status)) {
        goto fail;
    }
    first = -1;
    if ((!WIFEXITED(status) && !WIFSIGNALED(status)) || (WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
        goto fail;
    }
    successor = spawn_compute_child(options, "C2-hold", &next_command, &next_ready);
    if (successor < 0 || !await_helper_signal(next_ready[0], 0xC1, context.deadline_ms) ||
        !checkpoint_wait(context, 2, C2_CHECKPOINTS.at(2)) ||
        !write_exact_timeout(next_command[1], &exit_command, 1, context.deadline_ms) ||
        !wait_child(successor, context.deadline_ms, &status)) {
        goto fail;
    }
    successor = -1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0 || !checkpoint_wait(context, 3, C2_CHECKPOINTS.at(3))) {
        goto fail;
    }
    close_pair(first_command);
    close_pair(first_ready);
    close_pair(next_command);
    close_pair(next_ready);
    return 0;
fail:
    close_pair(first_command);
    close_pair(first_ready);
    close_pair(next_command);
    close_pair(next_ready);
    terminate_child(first);
    terminate_child(successor);
    return 1;
}

auto remote_tmp_path(std::array<char, PATH_MAX>& out, const char* target, const char* scenario, const char* run_id, const char* suffix)
    -> bool {
    return safe_id(target, MAX_HOST_LEN) && safe_id(run_id, MAX_ID_LEN) &&
           format_path(out, "/wki/%s/tmp/wki-chaos-%s-%s-%s", target, scenario, run_id, suffix);
}

auto fill_vfs_data(std::array<uint8_t, VFS_PAYLOAD_SIZE>* data) -> void {
    for (std::size_t i = 0; i < data->size(); ++i) {
        data->at(i) = static_cast<uint8_t>(((i * 73U) + 0x5BU) & 0xFFU);
    }
}

auto run_v1(const Options& options, RunContext& context) -> int {
    std::array<char, PATH_MAX> original{};
    std::array<char, PATH_MAX> renamed{};
    std::array<uint8_t, VFS_PAYLOAD_SIZE> expected{};
    std::array<uint8_t, VFS_PAYLOAD_SIZE> observed{};
    int fd = -1;
    bool renamed_active = false;
    fill_vfs_data(&expected);
    if (!remote_tmp_path(original, options.target, "v1", options.run_id, "a") ||
        !remote_tmp_path(renamed, options.target, "v1", options.run_id, "b")) {
        return 1;
    }
    fd = open(original.data(), O_CREAT | O_EXCL | O_RDWR, MODE_0600);
    if (fd < 0 || !checkpoint_wait(context, 0, V1_CHECKPOINTS.at(0)) || !checkpoint_wait(context, 1, V1_CHECKPOINTS.at(1)) ||
        !write_exact_timeout(fd, expected.data(), expected.size(), context.deadline_ms) || fsync(fd) != 0 ||
        !checkpoint_wait(context, 2, V1_CHECKPOINTS.at(2)) || !checkpoint_wait(context, 3, V1_CHECKPOINTS.at(3))) {
        goto fail;
    }
    {
        constexpr std::array<timespec, 2> TIMES{{{.tv_sec = 1'234'567, .tv_nsec = 123}, {.tv_sec = 1'234'568, .tv_nsec = 456}}};
        if (utimensat(AT_FDCWD, original.data(), TIMES.data(), 0) != 0) {
            goto fail;
        }
    }
    if (!checkpoint_wait(context, 4, V1_CHECKPOINTS.at(4)) || !checkpoint_wait(context, 5, V1_CHECKPOINTS.at(5)) ||
        rename(original.data(), renamed.data()) != 0) {
        goto fail;
    }
    renamed_active = true;
    if (!checkpoint_wait(context, 6, V1_CHECKPOINTS.at(6)) || !checkpoint_wait(context, 7, V1_CHECKPOINTS.at(7)) || close(fd) != 0) {
        fd = -1;
        goto fail;
    }
    fd = -1;
    if (!checkpoint_wait(context, 8, V1_CHECKPOINTS.at(8))) {
        goto fail;
    }
    fd = open(renamed.data(), O_RDONLY);
    if (fd < 0 || !read_exact_timeout(fd, observed.data(), observed.size(), context.deadline_ms) ||
        !require_eof_timeout(fd, context.deadline_ms) || observed != expected || close(fd) != 0) {
        fd = -1;
        goto fail;
    }
    fd = -1;
    if (unlink(renamed.data()) != 0) {
        goto fail;
    }
    return 0;
fail:
    if (fd >= 0) {
        close(fd);
    }
    static_cast<void>(unlink(renamed_active ? renamed.data() : original.data()));
    return 1;
}

auto run_v2(const Options& options, RunContext& context) -> int {
    std::array<std::array<char, PATH_MAX>, V2_LANE_COUNT> paths{};
    std::array<int, V2_LANE_COUNT> fds{};
    fds.fill(-1);
    bool ok = true;
    for (std::size_t i = 0; i < paths.size(); ++i) {
        std::array<char, 16> suffix{};
        static_cast<void>(std::snprintf(suffix.data(), suffix.size(), "lane%zu", i));
        if (!remote_tmp_path(paths.at(i), options.target, "v2", options.run_id, suffix.data())) {
            ok = false;
            break;
        }
        fds.at(i) = open(paths.at(i).data(), O_CREAT | O_EXCL | O_RDWR, MODE_0600);
        if (fds.at(i) < 0) {
            ok = false;
            break;
        }
    }
    if (ok) {
        ok = checkpoint_wait(context, 0, V2_CHECKPOINTS.at(0));
    }
    for (std::size_t i = 0; ok && i < fds.size(); ++i) {
        uint8_t const VALUE = static_cast<uint8_t>(0x40U + i);
        ok = write_exact_timeout(fds.at(i), &VALUE, 1, context.deadline_ms) && fsync(fds.at(i)) == 0;
    }
    if (ok) {
        ok = checkpoint_wait(context, 1, V2_CHECKPOINTS.at(1)) && checkpoint_wait(context, 2, V2_CHECKPOINTS.at(2));
    }
    for (int& fd : fds) {
        if (fd >= 0) {
            ok = close(fd) == 0 && ok;
            fd = -1;
        }
    }
    if (ok) {
        ok = checkpoint_wait(context, 3, V2_CHECKPOINTS.at(3));
    }
    for (auto const& path : paths) {
        if (path.front() != '\0' && unlink(path.data()) != 0 && errno != ENOENT) {
            ok = false;
        }
    }
    return ok ? 0 : 1;
}

auto connect_with_deadline(int fd, const sockaddr_in& address, int64_t deadline_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking(fd, &old_flags)) {
        return false;
    }
    bool connected = false;
    int saved_errno = 0;
    while (remaining_ms(deadline_ms) > 0) {
        int const RESULT = connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        if (RESULT == 0 || errno == EISCONN) {
            connected = true;
            break;
        }
        if (errno != EINTR && errno != EINPROGRESS && errno != EALREADY && errno != EAGAIN && errno != EWOULDBLOCK) {
            saved_errno = errno;
            break;
        }
        if (!wait_fd(fd, POLLOUT, deadline_ms)) {
            saved_errno = errno;
            break;
        }
    }
    if (!connected && saved_errno == 0) {
        saved_errno = ETIMEDOUT;
    }
    restore_flags(fd, old_flags);
    if (!connected) {
        errno = saved_errno;
    }
    return connected;
}

auto accept_with_deadline(int listener, int64_t deadline_ms) -> int {
    int old_flags = -1;
    if (!set_nonblocking(listener, &old_flags)) {
        return -1;
    }
    int accepted = -1;
    int saved_errno = 0;
    while (remaining_ms(deadline_ms) > 0) {
        accepted = accept(listener, nullptr, nullptr);
        if (accepted >= 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            saved_errno = errno;
            break;
        }
        if (!wait_fd(listener, POLLIN, deadline_ms)) {
            saved_errno = errno;
            break;
        }
    }
    if (accepted < 0 && saved_errno == 0) {
        saved_errno = ETIMEDOUT;
    }
    restore_flags(listener, old_flags);
    if (accepted < 0) {
        errno = saved_errno;
    }
    return accepted;
}

auto create_i2_socket_pair(std::array<int, 2>& sockets, int64_t deadline_ms) -> bool {
    sockets.fill(-1);
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return false;
    }
    int const ONE = 1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    socklen_t address_length = sizeof(address);
    bool const LISTENING = setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &ONE, sizeof(ONE)) == 0 &&
                           bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0 && listen(listener, 1) == 0 &&
                           getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length) == 0 &&
                           address_length == sizeof(address) && address.sin_family == AF_INET && address.sin_port != 0;
    if (!LISTENING) {
        int const SAVED_ERRNO = errno;
        close(listener);
        errno = SAVED_ERRNO;
        return false;
    }
    sockets[0] = socket(AF_INET, SOCK_STREAM, 0);
    if (sockets[0] < 0 || !connect_with_deadline(sockets[0], address, deadline_ms)) {
        int const SAVED_ERRNO = errno;
        close(listener);
        close_pair(sockets);
        errno = SAVED_ERRNO;
        return false;
    }
    sockets[1] = accept_with_deadline(listener, deadline_ms);
    int const SAVED_ERRNO = errno;
    close(listener);
    if (sockets[1] < 0) {
        close_pair(sockets);
        errno = SAVED_ERRNO;
        return false;
    }
    return true;
}

auto spawn_i2_control(const Options& options, std::array<int, 2>& sockets, int64_t deadline_ms) -> pid_t {
    if (!create_i2_socket_pair(sockets, deadline_ms)) {
        return -1;
    }
    // The helper needs only the connected client. Closing the accepted peer in
    // the child before exec avoids exporting a second socket/pump remotely.
    pid_t const PID = spawn_i1_helper(options.target, "I2-socket-getpeername", sockets[0], sockets[1], sockets[1], -1, options.timeout_ms);
    if (PID < 0) {
        close_pair(sockets);
    }
    return PID;
}

auto run_i2(const Options& options, RunContext& context) -> int {
    std::array<int, 2> old_sockets{-1, -1};
    std::array<int, 2> successor_sockets{-1, -1};
    pid_t old_child = spawn_i2_control(options, old_sockets, context.deadline_ms);
    pid_t successor = -1;
    int status = 0;
    if (old_child < 0 || !checkpoint_wait(context, 0, I2_CHECKPOINTS.at(0))) {
        goto fail;
    }
    if (!wait_child(old_child, context.deadline_ms, &status) || (WIFEXITED(status) && WEXITSTATUS(status) == 0)) {
        old_child = -1;
        goto fail;
    }
    old_child = -1;
    if (!checkpoint_wait(context, 1, I2_CHECKPOINTS.at(1))) {
        goto fail;
    }
    close_pair(old_sockets);
    successor = spawn_i2_control(options, successor_sockets, context.deadline_ms);
    if (successor < 0 || !checkpoint_wait(context, 2, I2_CHECKPOINTS.at(2)) || !wait_child(successor, context.deadline_ms, &status)) {
        goto fail;
    }
    successor = -1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0 || !checkpoint_wait(context, 3, I2_CHECKPOINTS.at(3))) {
        goto fail;
    }
    close_pair(successor_sockets);
    return 0;
fail:
    close_pair(old_sockets);
    close_pair(successor_sockets);
    terminate_child(old_child);
    terminate_child(successor);
    return 1;
}

auto run_i1(const Options& options, RunContext& context) -> int {
    static_cast<void>(signal(SIGPIPE, SIG_IGN));
    std::array<int, 2> data{-1, -1};
    std::array<int, 2> ready{-1, -1};
    std::array<uint8_t, I1_DATA_SIZE> payload{};
    std::array<int, 2> discard_data{-1, -1};
    std::array<int, 2> discard_ready{-1, -1};
    std::array<uint8_t, I1_DISCARD_PREFIX_SIZE> prefix{};
    pid_t reader = -1;
    pid_t writer = -1;
    int reader_status = 0;
    int writer_status = 0;
    fill_i1_data(&payload);
    if (pipe(data.data()) != 0 || pipe(ready.data()) != 0) {
        goto fail;
    }
    reader = spawn_i1_helper(options.target, "I1-read", data[0], ready[1], data[1], ready[0], options.timeout_ms);
    if (reader < 0) {
        goto fail;
    }
    close(data[0]);
    data[0] = -1;
    close(ready[1]);
    ready[1] = -1;
    if (!await_helper_signal(ready[0], 0xA1, context.deadline_ms)) {
        goto fail;
    }
    if (!checkpoint_wait(context, 0, I1_CHECKPOINTS.at(0)) || !checkpoint_wait(context, 1, I1_CHECKPOINTS.at(1))) {
        goto fail;
    }

    if (!write_exact_timeout(data[1], payload.data(), I1_ISSUE_CHUNK, context.deadline_ms) ||
        !await_helper_signal(ready[0], 0xB1, context.deadline_ms) ||
        !write_exact_timeout(data[1], payload.data() + I1_ISSUE_CHUNK, I1_ISSUE_CHUNK, context.deadline_ms) ||
        !await_helper_signal(ready[0], 0xB2, context.deadline_ms) || !checkpoint_wait(context, 2, I1_CHECKPOINTS.at(2)) ||
        !write_exact_timeout(data[1], payload.data() + (2 * I1_ISSUE_CHUNK), payload.size() - (2 * I1_ISSUE_CHUNK), context.deadline_ms) ||
        !await_helper_signal(ready[0], 0xB3, context.deadline_ms) || !checkpoint_wait(context, 3, I1_CHECKPOINTS.at(3))) {
        goto fail;
    }
    if (close(data[1]) != 0) {
        data[1] = -1;
        goto fail;
    }
    data[1] = -1;
    if (!checkpoint_wait(context, 4, I1_CHECKPOINTS.at(4))) {
        goto fail;
    }
    if (!await_helper_signal(ready[0], 0xB4, context.deadline_ms) || !require_eof_timeout(ready[0], context.deadline_ms)) {
        goto fail;
    }
    close(ready[0]);
    ready[0] = -1;
    if (!wait_child(reader, context.deadline_ms, &reader_status) || !WIFEXITED(reader_status) || WEXITSTATUS(reader_status) != 0) {
        reader = -1;
        goto fail;
    }
    if (!checkpoint_wait(context, 5, I1_CHECKPOINTS.at(5))) {
        goto fail_discard;
    }
    if (pipe(discard_data.data()) != 0 || pipe(discard_ready.data()) != 0) {
        goto fail_discard;
    }
    writer = spawn_i1_helper(options.target, "I1-discard-writer", discard_data[1], discard_ready[1], discard_data[0], discard_ready[0],
                             options.timeout_ms);
    if (writer < 0) {
        goto fail_discard;
    }
    close(discard_data[1]);
    discard_data[1] = -1;
    close(discard_ready[1]);
    discard_ready[1] = -1;
    if (!await_helper_signal(discard_ready[0], 0xA2, context.deadline_ms) || !require_eof_timeout(discard_ready[0], context.deadline_ms)) {
        goto fail_discard;
    }
    close(discard_ready[0]);
    discard_ready[0] = -1;
    if (!read_exact_timeout(discard_data[0], prefix.data(), prefix.size(), context.deadline_ms)) {
        goto fail_discard;
    }
    for (uint8_t byte : prefix) {
        if (byte != 0xD1) {
            errno = EBADMSG;
            goto fail_discard;
        }
    }
    close(discard_data[0]);
    discard_data[0] = -1;
    if (!wait_child(writer, context.deadline_ms, &writer_status) || !WIFEXITED(writer_status) || WEXITSTATUS(writer_status) != 0) {
        writer = -1;
        goto fail_discard;
    }
    writer = -1;
    if (!checkpoint_wait(context, 6, I1_CHECKPOINTS.at(6))) {
        goto fail_discard;
    }
    return 0;

fail_discard:
    if (discard_data[0] >= 0) {
        close(discard_data[0]);
    }
    if (discard_data[1] >= 0) {
        close(discard_data[1]);
    }
    if (discard_ready[0] >= 0) {
        close(discard_ready[0]);
    }
    if (discard_ready[1] >= 0) {
        close(discard_ready[1]);
    }
    terminate_child(writer);
    return 1;

fail:
    if (data[0] >= 0) {
        close(data[0]);
    }
    if (data[1] >= 0) {
        close(data[1]);
    }
    if (ready[0] >= 0) {
        close(ready[0]);
    }
    if (ready[1] >= 0) {
        close(ready[1]);
    }
    terminate_child(reader);
    return 1;
}

auto resolve_run_dir(const Options& options, std::array<char, PATH_MAX>* out) -> bool {
    return safe_id(options.run_id, MAX_ID_LEN) && safe_absolute_dir(options.checkpoint_dir) &&
           format_path(*out, "%s/%s", options.checkpoint_dir, options.run_id);
}

auto control_wait(const Options& options) -> int {
    if (options.timeout_ms < 1 || !safe_id(options.checkpoint, MAX_ID_LEN)) {
        return 64;
    }
    std::array<char, PATH_MAX> run_dir{};
    if (!resolve_run_dir(options, &run_dir)) {
        return 64;
    }
    int64_t const DEADLINE = deadline_after_ms(options.timeout_ms);
    while (DEADLINE >= 0 && remaining_ms(DEADLINE) > 0) {
        StateRecord metadata{};
        StateRecord state{};
        if (load_metadata(run_dir.data(), &metadata, nullptr) && load_state(run_dir.data(), &state)) {
            int const REQUESTED = checkpoint_index(metadata.scenario.data(), options.checkpoint);
            if (REQUESTED < 0 || std::strcmp(metadata.scenario.data(), state.scenario.data()) != 0 || metadata.pid != state.pid) {
                return 1;
            }
            if (state.index == REQUESTED && std::strcmp(state.checkpoint.data(), options.checkpoint) == 0 &&
                std::strcmp(state.phase.data(), "ready") == 0) {
                return 0;
            }
            if (state.index > REQUESTED || (state.index == REQUESTED && (std::strcmp(state.checkpoint.data(), options.checkpoint) != 0 ||
                                                                         std::strcmp(state.phase.data(), "ready") != 0))) {
                return 1;
            }
        }
        poll_pause();
    }
    return 1;
}

auto control_advance(const Options& options) -> int {
    if (!safe_id(options.checkpoint, MAX_ID_LEN)) {
        return 64;
    }
    std::array<char, PATH_MAX> run_dir{};
    StateRecord metadata{};
    StateRecord state{};
    if (!resolve_run_dir(options, &run_dir) || !load_metadata(run_dir.data(), &metadata, nullptr) || !load_state(run_dir.data(), &state)) {
        return 1;
    }
    int const REQUESTED = checkpoint_index(metadata.scenario.data(), options.checkpoint);
    if (REQUESTED < 0 || state.index != REQUESTED || std::strcmp(state.checkpoint.data(), options.checkpoint) != 0 ||
        std::strcmp(state.phase.data(), "ready") != 0 || metadata.pid != state.pid) {
        return 1;
    }
    std::array<char, PATH_MAX> advance{};
    if (!format_path(advance, "%s/advance.%d.%s", run_dir.data(), REQUESTED, options.checkpoint)) {
        return 1;
    }
    constexpr std::string_view CONTENT = "advance\n";
    return write_new_file(advance.data(), CONTENT.data(), CONTENT.size()) ? 0 : 1;
}

auto control_cleanup(const Options& options) -> int {
    if (options.timeout_ms < 1) {
        return 64;
    }
    std::array<char, PATH_MAX> run_dir{};
    StateRecord metadata{};
    if (!resolve_run_dir(options, &run_dir)) {
        return 64;
    }
    struct stat run_info{};
    if (stat(run_dir.data(), &run_info) != 0) {
        return errno == ENOENT ? 0 : 1;
    }
    if (!S_ISDIR(run_info.st_mode) || (run_info.st_mode & 0777) != MODE_0700) {
        return 1;
    }
    if (!load_metadata(run_dir.data(), &metadata, nullptr)) {
        return rmdir(run_dir.data()) == 0 || errno == ENOENT ? 0 : 1;
    }
    if (metadata.pid <= 0) {
        return 1;
    }
    std::array<char, PATH_MAX> done{};
    std::array<char, PATH_MAX> cancel_path{};
    if (!format_path(done, "%s/done", run_dir.data()) || !format_path(cancel_path, "%s/cancel", run_dir.data())) {
        return 1;
    }
    if (!file_exists(done.data())) {
        constexpr std::string_view CANCEL = "cancel\n";
        if (!write_new_file(cancel_path.data(), CANCEL.data(), CANCEL.size()) && errno != EEXIST) {
            return 1;
        }
    }
    int64_t const DEADLINE = deadline_after_ms(options.timeout_ms);
    bool terminal = false;
    auto const checkpoints = checkpoint_list(metadata.scenario.data());
    while (DEADLINE >= 0 && remaining_ms(DEADLINE) > 0) {
        StateRecord state{};
        if (file_exists(done.data()) && load_state(run_dir.data(), &state) && state.pid == metadata.pid &&
            std::strcmp(state.scenario.data(), metadata.scenario.data()) == 0 && state.index == static_cast<int>(checkpoints.size()) &&
            std::strcmp(state.checkpoint.data(), "complete") == 0 &&
            (std::strcmp(state.phase.data(), "passed") == 0 || std::strcmp(state.phase.data(), "failed") == 0)) {
            terminal = true;
            break;
        }
        poll_pause();
    }
    if (!terminal) {
        return 1;
    }

    std::array<char, PATH_MAX> path{};
    for (std::size_t index = 0; index < checkpoints.size(); ++index) {
        if (!format_path(path, "%s/advance.%zu.%s", run_dir.data(), index, checkpoints[index]) || !unlink_if_present(path.data())) {
            return 1;
        }
        for (const char* phase : {"ready", "advanced"}) {
            if (!format_path(path, "%s/.state.%d.%zu.%s", run_dir.data(), metadata.pid, index, phase) || !unlink_if_present(path.data())) {
                return 1;
            }
        }
    }
    for (const char* phase : {"passed", "failed"}) {
        if (!format_path(path, "%s/.state.%d.%zu.%s", run_dir.data(), metadata.pid, checkpoints.size(), phase) ||
            !unlink_if_present(path.data())) {
            return 1;
        }
    }
    for (const char* name : {"state", "done", "cancel", "metadata"}) {
        if (!format_path(path, "%s/%s", run_dir.data(), name) || !unlink_if_present(path.data())) {
            return 1;
        }
    }
    return rmdir(run_dir.data()) == 0 || errno == ENOENT ? 0 : 1;
}

void usage() {
    std::println(stderr,
                 "usage: testprog wki-chaos-workload <C1|C2|V1|V2|I1|I2> --run-id ID --target HOST "
                 "[--checkpoint-dir DIR] --timeout-ms N\n"
                 "       testprog wki-chaos-workload wait --run-id ID --checkpoint NAME [--checkpoint-dir DIR] --timeout-ms N\n"
                 "       testprog wki-chaos-workload advance --run-id ID --checkpoint NAME [--checkpoint-dir DIR]\n"
                 "       testprog wki-chaos-workload cleanup --run-id ID [--checkpoint-dir DIR] --timeout-ms N");
}

}  // namespace

auto run_wki_chaos_workload(int argc, char** argv) -> int {
    Options options{};
    if (!parse_options(argc, argv, &options) || !valid_invocation(options)) {
        usage();
        return 64;
    }
    if (std::strcmp(options.command, "helper") == 0) {
        if (options.scenario != nullptr && std::strcmp(options.scenario, "I1-read") == 0) {
            return helper_i1_read(options);
        }
        if (options.scenario != nullptr && std::strcmp(options.scenario, "I1-discard-writer") == 0) {
            return helper_i1_discard_writer(options);
        }
        if (options.scenario != nullptr &&
            (std::strcmp(options.scenario, "C1-hold") == 0 || std::strcmp(options.scenario, "C2-hold") == 0)) {
            return helper_compute_hold(options);
        }
        if (options.scenario != nullptr && std::strcmp(options.scenario, "I2-socket-getpeername") == 0) {
            return helper_i2_socket_getpeername(options);
        }
        return 64;
    }
    if (std::strcmp(options.command, "wait") == 0) {
        return control_wait(options);
    }
    if (std::strcmp(options.command, "advance") == 0) {
        return control_advance(options);
    }
    if (std::strcmp(options.command, "cleanup") == 0) {
        return control_cleanup(options);
    }
    if (std::strcmp(options.command, "run") != 0 || options.scenario == nullptr || checkpoint_list(options.scenario).empty()) {
        usage();
        return 64;
    }

    RunContext context{};
    if (!initialize_run(options, &context)) {
        std::println(stderr, "wki-chaos-workload: failed to initialize run: {}", std::strerror(errno));
        return 1;
    }
    int result = 1;
    if (std::strcmp(options.scenario, "C1") == 0) {
        result = run_c1(options, context);
    } else if (std::strcmp(options.scenario, "C2") == 0) {
        result = run_c2(options, context);
    } else if (std::strcmp(options.scenario, "V1") == 0) {
        result = run_v1(options, context);
    } else if (std::strcmp(options.scenario, "V2") == 0) {
        result = run_v2(options, context);
    } else if (std::strcmp(options.scenario, "I1") == 0) {
        result = run_i1(options, context);
    } else if (std::strcmp(options.scenario, "I2") == 0) {
        result = run_i2(options, context);
    }
    int const RESULT = result;
    int const SAVED_ERRNO = errno;
    finish_run(context, RESULT);
    if (RESULT == 0) {
        std::println("wki-chaos-workload scenario={} run_id={} status=pass", options.scenario, options.run_id);
    } else {
        std::println(stderr, "wki-chaos-workload scenario={} run_id={} status=fail errno={} ({})", options.scenario, options.run_id,
                     SAVED_ERRNO, std::strerror(SAVED_ERRNO));
    }
    return RESULT;
}
