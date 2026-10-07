#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <utility>

#include "vfs_coherency.hpp"

namespace {
constexpr std::array<const char*, 3> FAMILIES{"posix", "ofd", "flock"};
class Fd {
   public:
    Fd() = default;
    explicit Fd(int fd) : fd_(fd) {}
    Fd(const Fd&) = delete;
    auto operator=(const Fd&) -> Fd& = delete;
    ~Fd() {
        if (fd_ >= 0) static_cast<void>(close(fd_));
    }
    auto get() const -> int { return fd_; }
    void adopt(int fd) { fd_ = fd; }
    auto finish() -> int { return fd_ < 0 ? 0 : close(std::exchange(fd_, -1)); }

   private:
    int fd_ = -1;
};
class Children {
   public:
    ~Children() {
        for (auto child : pids)
            if (child > 0) {
                static_cast<void>(kill(child, SIGKILL));
                while (waitpid(child, nullptr, 0) < 0 && errno == EINTR) {
                }
            }
    }
    std::array<pid_t, 3> pids{};
};
auto seconds() -> int64_t {
    timespec now{};
    return clock_gettime(CLOCK_MONOTONIC, &now) == 0 ? now.tv_sec : -1;
}
auto network_error(int value) -> bool {
    return value == EIO || value == EHOSTUNREACH || value == ENOTCONN || value == ECONNRESET || value == ESTALE || value == ETIMEDOUT;
}
auto lock(int fd, unsigned family, bool release, bool blocking = false) -> int {
    if (family == 2) return flock(fd, release ? LOCK_UN : LOCK_EX | (blocking ? 0 : LOCK_NB));
    struct flock request{};
    request.l_type = release ? F_UNLCK : F_WRLCK;
    request.l_whence = SEEK_SET;
    return fcntl(fd, family == 0 ? (blocking ? F_SETLKW : F_SETLK) : (blocking ? F_OFD_SETLKW : F_OFD_SETLK), &request);
}
class Recovery {
   public:
    Recovery(const char* root, std::string_view role, uint64_t run, std::string_view mode, unsigned timeout)
        : root_(root), role_(role), run_(run), mode_(mode), deadline_(seconds() + timeout) {}
    auto execute() -> bool {
        if (seconds() < 0) return fail("clock");
        bool const success = role_ == "owner" ? owner() : role_ == "client" ? client() : successor();
        if (success) {
            std::printf("VFS-RECOVERY-PASS role=%.*s run=%llu mode=%.*s\n", static_cast<int>(role_.size()), role_.data(),
                        static_cast<unsigned long long>(run_), static_cast<int>(mode_.size()), mode_.data());
            std::fflush(stdout);
        }
        return success;
    }

   private:
    auto fail(const char* stage) const -> bool {
        std::printf("VFS-RECOVERY-FAIL role=%.*s run=%llu stage=%s errno=%d\n", static_cast<int>(role_.size()), role_.data(),
                    static_cast<unsigned long long>(run_), stage, errno);
        std::fflush(stdout);
        return false;
    }
    auto pace() const -> bool {
        if (seconds() < 0 || seconds() >= deadline_) {
            errno = ETIMEDOUT;
            return false;
        }
        timespec const pause{0, 20'000'000};
        static_cast<void>(nanosleep(&pause, nullptr));
        return true;
    }
    auto open_leaf(const char* leaf, int flags) const -> int {
        std::array<char, 512> name{};
        int const length = std::snprintf(name.data(), name.size(), "%s/%s", root_, leaf);
        if (length < 0 || static_cast<size_t>(length) >= name.size()) {
            errno = ENAMETOOLONG;
            return -1;
        }
        return open(name.data(), flags, 0600);
    }
    auto mark(const char* stage) -> bool {
        Fd marker(open_leaf(stage, O_CREAT | O_EXCL | O_WRONLY));
        return marker.get() >= 0 && write(marker.get(), &run_, sizeof(run_)) == sizeof(run_) && marker.finish() == 0;
    }
    auto wait(const char* stage) -> bool {
        for (;;) {
            Fd marker(open_leaf(stage, O_RDONLY));
            uint64_t observed = 0;
            if (marker.get() >= 0 && pread(marker.get(), &observed, sizeof(observed), 0) == sizeof(observed) && observed == run_)
                return marker.finish() == 0;
            if (!pace()) return fail(stage);
        }
    }
    auto pending(uint64_t& result) -> bool {
#ifdef __WOS__
        FILE* stream = std::fopen("/proc/wki/netdiag", "r");
        if (stream == nullptr) return fail("diagnostic-open");
        std::array<char, 4096> line{};
        unsigned rows = 0, ends = 0;
        bool valid = true;
        unsigned lines = 0;
        while (lines++ < 1024 && std::fgets(line.data(), static_cast<int>(line.size()), stream) != nullptr) {
            valid &= std::strchr(line.data(), '\n') != nullptr;
            std::string_view const text(line.data());
            if (text.starts_with("wki_vfs_advisory ")) {
                ++rows;
                const char* token = std::strstr(line.data(), " pending=");
                char* end = nullptr;
                errno = 0;
                if (!text.starts_with("wki_vfs_advisory version=1 ") || token == nullptr)
                    valid = false;
                else {
                    result = std::strtoull(token + 9, &end, 10);
                    valid &= errno == 0 && end != token + 9 && (*end == ' ' || *end == '\n');
                }
            }
            ends += text == "wki_netdiag_end complete=1\n" ? 1U : 0U;
        }
        valid &= std::ferror(stream) == 0;
        valid &= std::fclose(stream) == 0;
        return valid && rows == 1 && ends == 1 ? true : fail("diagnostic-complete");
#else
        result = 0;
        return true;
#endif
    }
    auto snapshot(const char* stage) -> bool {
        std::printf("VFS-RECOVERY-SNAPSHOT-BEGIN stage=%s\n", stage);
#ifdef __WOS__
        FILE* stream = std::fopen("/proc/wki/netdiag", "r");
        if (stream == nullptr) return fail("snapshot-open");
        std::array<char, 4096> line{};
        unsigned rows = 0;
        bool complete = false;
        while (rows++ < 1024 && std::fgets(line.data(), static_cast<int>(line.size()), stream) != nullptr) {
            if (std::strchr(line.data(), '\n') == nullptr) {
                static_cast<void>(std::fclose(stream));
                return fail("snapshot-truncated-line");
            }
            std::fputs(line.data(), stdout);
            complete |= std::string_view(line.data()) == "wki_netdiag_end complete=1\n";
        }
        bool const valid = std::ferror(stream) == 0 && complete;
        int const closed = std::fclose(stream);
        if (!valid || closed != 0) return fail("snapshot-complete");
#endif
        std::printf("VFS-RECOVERY-SNAPSHOT-END stage=%s\n", stage);
        std::fflush(stdout);
        return true;
    }
    auto files(std::array<Fd, 3>& output, const char* prefix, int flags) -> bool {
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            std::array<char, 40> leaf{};
            std::snprintf(leaf.data(), leaf.size(), "%s-%s", prefix, FAMILIES.at(family));
            output.at(family).adopt(open_leaf(leaf.data(), flags));
            if (output.at(family).get() < 0) return fail(leaf.data());
        }
        return true;
    }
    auto conflicting(std::array<Fd, 3>& witnesses) -> bool {
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            errno = 0;
            if (lock(witnesses.at(family).get(), family, false) != -1 || errno != EAGAIN) return fail("witness-conflict");
        }
        return true;
    }
    auto released(std::array<Fd, 3>& witnesses, bool& result) -> bool {
        result = true;
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            errno = 0;
            int const acquired = lock(witnesses.at(family).get(), family, false);
            if (acquired == -1 && errno == EAGAIN) {
                result = false;
                continue;
            }
            if (acquired != 0 || lock(witnesses.at(family).get(), family, true) != 0) return fail("witness-release-probe");
        }
        return true;
    }
    auto owner() -> bool {
        if (mkdir(root_, 0700) != 0) return fail("fresh-root");
        std::array<Fd, 3> held{}, waiting{};
        if (!files(held, "held", O_CREAT | O_EXCL | O_RDWR) || !files(waiting, "wait", O_CREAT | O_EXCL | O_RDWR)) return false;
        uint64_t baseline = 0;
        if (!pending(baseline) || baseline != 0) return fail("pending-empty-baseline");
        for (unsigned family = 0; family < FAMILIES.size(); ++family)
            if (lock(waiting.at(family).get(), family, false) != 0) return fail("owner-blockers");
        if (!mark("initialized") || !wait("client-armed")) return fail("owner-setup");
#ifdef __WOS__
        for (;;) {
            uint64_t count = 0;
            if (!pending(count)) return false;
            if (count == 3) break;
            if (count > 3 || !pace()) return fail("pending-three");
        }
#endif
        if (!conflicting(held) || !snapshot("armed") || !mark("owner-armed")) return false;
#ifdef __WOS__
        std::printf("VFS-RECOVERY-READY role=owner pending=3 run=%llu\n", static_cast<unsigned long long>(run_));
#else
        std::printf("VFS-RECOVERY-READY role=owner pending=unobserved run=%llu\n", static_cast<unsigned long long>(run_));
#endif
        std::fflush(stdout);
        for (;;) {
            uint64_t count = 0;
            bool free = false;
            if (!pending(count) || !released(held, free)) return false;
            if (count == 0 && free) break;
            if (!pace()) return fail("old-owner-state-retired");
        }
        for (unsigned family = 0; family < FAMILIES.size(); ++family)
            if (lock(waiting.at(family).get(), family, true) != 0 || waiting.at(family).finish() != 0) return fail("owner-unlock");
        if (!snapshot("retired") || !mark("retired") || !wait("successor-held") || !conflicting(held) || !snapshot("successor-held") ||
            !mark("successor-verified") || !wait("successor-complete"))
            return fail("successor-witness");
        bool free = false;
        if (!released(held, free) || !free) return fail("successor-released");
        for (auto& descriptor : held)
            if (descriptor.finish() != 0) return fail("owner-close");
        return mark("owner-complete");
    }
    auto join(Children& children) -> bool {
        for (auto& child : children.pids) {
            for (;;) {
                int status = 0;
                pid_t const result = waitpid(child, &status, WNOHANG);
                if (result == child) {
                    child = 0;
                    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return fail("waiter-terminal");
                    break;
                }
                if ((result < 0 && errno != EINTR) || !pace()) return fail("waiter-join");
            }
        }
        return true;
    }
    auto client() -> bool {
        if (!wait("initialized")) return false;
        std::array<Fd, 3> held{};
        if (!files(held, "held", O_RDWR)) return false;
        for (unsigned family = 0; family < FAMILIES.size(); ++family)
            if (lock(held.at(family).get(), family, false) != 0) return fail("client-held");
        std::array<int, 2> ready{};
        if (pipe(ready.data()) != 0) return fail("ready-pipe");
        Fd read_ready(ready.at(0)), write_ready(ready.at(1));
        Children children;
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            pid_t const child = fork();
            if (child < 0) return fail("waiter-fork");
            if (child == 0) {
                for (auto& descriptor : held)
                    if (descriptor.finish() != 0) _exit(71);
                if (read_ready.finish() != 0) _exit(72);
                std::array<char, 40> leaf{};
                std::snprintf(leaf.data(), leaf.size(), "wait-%s", FAMILIES.at(family));
                Fd waiter(open_leaf(leaf.data(), O_RDWR));
                char const value = static_cast<char>(family);
                if (waiter.get() < 0 || write(write_ready.get(), &value, 1) != 1 || write_ready.finish() != 0) _exit(73);
                errno = 0;
                int const result = lock(waiter.get(), family, false, true);
                int const error = errno;
                std::printf("VFS-RECOVERY-WAITER family=%s result=%d errno=%d\n", FAMILIES.at(family), result, error);
                std::fflush(stdout);
                _exit(result == -1 && network_error(error) ? 0 : 74);
            }
            children.pids.at(family) = child;
        }
        if (write_ready.finish() != 0) return fail("ready-close");
        int const ready_flags = fcntl(read_ready.get(), F_GETFL);
        if (ready_flags < 0 || fcntl(read_ready.get(), F_SETFL, ready_flags | O_NONBLOCK) != 0) return fail("ready-nonblocking");
        std::array<bool, 3> seen{};
        for (unsigned index = 0; index < FAMILIES.size(); ++index) {
            char value = -1;
            for (;;) {
                ssize_t const result = read(read_ready.get(), &value, 1);
                if (result == 1) break;
                if (result != -1 || (errno != EAGAIN && errno != EINTR) || !pace()) return fail("ready-framing");
            }
            if (value < 0 || value >= 3 || seen.at(static_cast<unsigned>(value))) return fail("ready-framing");
            seen.at(static_cast<unsigned>(value)) = true;
        }
        if (read_ready.finish() != 0 || !mark("client-armed") || !wait("owner-armed") || !snapshot("armed")) return fail("client-ready");
        std::printf("VFS-RECOVERY-READY role=client waiters=3 run=%llu\n", static_cast<unsigned long long>(run_));
        std::fflush(stdout);
        if (mode_ == "session") {
            while (pace()) {
            }
            return fail("expected-host-reset");
        }
        if (!join(children)) return false;
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            auto& descriptor = held.at(family);
            struct stat metadata{};
            errno = 0;
            int const stat_result = fstat(descriptor.get(), &metadata);
            int const stat_error = errno;
            if (stat_result != -1 || !network_error(stat_error)) return fail("fenced-old-stat");
            errno = 0;
            int const result = descriptor.finish();
            int const error = errno;
            if (result != 0 && (result != -1 || !network_error(error))) return fail("fenced-old-close");
            std::printf("VFS-RECOVERY-OLD-FD family=%s stat_result=%d stat_errno=%d close_result=%d close_errno=%d\n", FAMILIES.at(family),
                        stat_result, stat_error, result, error);
        }
        if (!snapshot("fenced")) return false;
        std::printf("VFS-RECOVERY-FENCE-TERMINAL waiters=3 old_descriptors=0 run=%llu\n", static_cast<unsigned long long>(run_));
        std::fflush(stdout);
        return successor();
    }
    auto successor() -> bool {
        if (!wait("retired")) return false;
        std::array<Fd, 3> held{}, waiting{};
        if (!files(held, "held", O_RDWR) || !files(waiting, "wait", O_RDWR)) return false;
        for (unsigned family = 0; family < FAMILIES.size(); ++family) {
            if (lock(held.at(family).get(), family, false) != 0 || lock(waiting.at(family).get(), family, false) != 0 ||
                lock(waiting.at(family).get(), family, true) != 0 || waiting.at(family).finish() != 0)
                return fail("successor-acquire");
        }
        if (!snapshot("successor-held") || !mark("successor-held") || !wait("successor-verified")) return fail("successor-validation");
        for (unsigned family = 0; family < FAMILIES.size(); ++family)
            if (lock(held.at(family).get(), family, true) != 0 || held.at(family).finish() != 0) return fail("successor-close");
        return mark("successor-complete") && wait("owner-complete");
    }
    const char* root_;
    std::string_view role_;
    uint64_t run_;
    std::string_view mode_;
    int64_t deadline_;
};
auto number(const char* input, uint64_t& output) -> bool {
    if (input == nullptr || input[0] < '0' || input[0] > '9') return false;
    char* end = nullptr;
    errno = 0;
    output = std::strtoull(input, &end, 10);
    return errno == 0 && end != input && *end == '\0';
}
}  // namespace

auto run_vfs_coherency_recovery(int argc, char** argv) -> int {
    uint64_t run = 0, timeout = 300;
    if ((argc != 4 && argc != 5) || argv[0][0] != '/' || std::strcmp(argv[0], "/") == 0 ||
        (std::strcmp(argv[1], "owner") != 0 && std::strcmp(argv[1], "client") != 0 && std::strcmp(argv[1], "successor") != 0) ||
        !number(argv[2], run) || run == 0 || (std::strcmp(argv[3], "session") != 0 && std::strcmp(argv[3], "fence") != 0) ||
        (argc == 5 && (!number(argv[4], timeout) || timeout == 0 || timeout > 600))) {
        std::printf(
            "usage: testprog vfs-coherency-recovery ABSOLUTE_ROOT owner|client|successor RUN_ID session|fence [TIMEOUT_SECONDS(1..600)]\n");
        return 2;
    }
    Recovery test(argv[0], argv[1], run, argv[3], static_cast<unsigned>(timeout));
    return test.execute() ? 0 : 1;
}
