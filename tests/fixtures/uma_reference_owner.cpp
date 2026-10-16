#include "uma_reference_owner.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

#include "uma_reference_arm.hpp"
#include "uma_reference_catalog.hpp"

#ifdef WOS_UMA_OWNER_HOST
// Only the Host harness supplies these. Production uses actual WOS waitpid,
// including WNOWAIT, and ordinary kill. Linux waitpid does not offer WNOWAIT.
extern auto uma_owner_host_waitpid(pid_t, int*, int) -> pid_t;
extern auto uma_owner_host_kill(pid_t, int) -> int;
extern void uma_owner_host_after_consume();
extern void uma_owner_host_admitted();
#endif

namespace uma_reference {
namespace {
constexpr uint64_t SECOND = 1'000'000'000;
constexpr uint64_t GRACE = 5 * SECOND, MARGIN = 30 * SECOND;
constexpr uint64_t FILE_LIMIT = 16 * 1024 * 1024, OUTPUT_LIMIT = 64 * 1024 * 1024;
constexpr uint64_t TRANSPORT_LIMIT = 512 * 1024, RESERVE = 32768;
constexpr std::array<const char*, 4> KINDS{"begin", "end", "parent", "wait"};
constexpr std::array<uint64_t, 4> KIND_LIMITS{512, 768, 256, 256};
struct Fd {
    int value{-1};
    explicit Fd(int fd = -1) : value(fd) {}
    Fd(const Fd&) = delete;
    auto operator=(const Fd&) -> Fd& = delete;
    ~Fd() {
        if (value >= 0) (void)::close(value);
    }
    auto close() -> bool {
        int const old = value;
        value = -1;
        return old < 0 || ::close(old) == 0;
    }
};
auto now(uint64_t& out) -> bool {
    timespec t{};
    if (::clock_gettime(CLOCK_MONOTONIC, &t) != 0 || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= static_cast<long>(SECOND) ||
        static_cast<uint64_t>(t.tv_sec) > (UINT64_MAX - static_cast<uint64_t>(t.tv_nsec)) / SECOND)
        return false;
    out = static_cast<uint64_t>(t.tv_sec) * SECOND + static_cast<uint64_t>(t.tv_nsec);
    return true;
}
void pause_poll() {
    timespec t{0, 20'000'000};
    (void)::nanosleep(&t, nullptr);
}
auto child_wait(pid_t child, int& raw, int options) -> pid_t {
#ifdef WOS_UMA_OWNER_HOST
    return uma_owner_host_waitpid(child, &raw, options);
#else
    return ::waitpid(child, &raw, options);
#endif
}
auto signal_child(pid_t target, int sig) -> int {
#ifdef WOS_UMA_OWNER_HOST
    return uma_owner_host_kill(target, sig);
#else
    return ::kill(target, sig);
#endif
}
auto owner_uid() -> uid_t {
#ifdef WOS_UMA_OWNER_HOST
    return ::getuid();
#else
    return 0;
#endif
}
auto owner_gid() -> gid_t {
#ifdef WOS_UMA_OWNER_HOST
    return ::getgid();
#else
    return 0;
#endif
}
auto directory(int fd) -> bool {
    struct stat s{};
    return fd >= 0 && ::fstat(fd, &s) == 0 && S_ISDIR(s.st_mode) && (s.st_mode & 07777) == 0700 && s.st_uid == owner_uid() &&
           s.st_gid == owner_gid();
}
auto checked_root(const char* root) -> int {
    if (!root || root[0] != '/' || std::strlen(root) >= 512) return -1;
    Fd current(::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    size_t at = 1;
    while (root[at]) {
        size_t first = at;
        while (root[at] && root[at] != '/') ++at;
        if (first == at || at - first > 255) return -1;
        std::array<char, 256> part{};
        std::memcpy(part.data(), root + first, at - first);
        if (!std::strcmp(part.data(), ".") || !std::strcmp(part.data(), "..")) return -1;
        int const next = ::openat(current.value, part.data(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (next < 0) return -1;
        if (!current.close()) {
            (void)::close(next);
            return -1;
        }
        current.value = next;
        if (root[at]) ++at;
    }
    if (!directory(current.value)) return -1;
    int const fd = current.value;
    current.value = -1;
    return fd;
}
struct Admission {
    int error{}, pid{}, sid{}, pgid{};
};
static_assert(sizeof(Admission) == 16);
[[noreturn]] void child_exec(int ack, int log, const PhaseRequest& r, pid_t expected_parent, char* const* args, char* const* env) {
    auto finish = [&](int error) {
        Admission a{error, static_cast<int>(::getpid()), static_cast<int>(::getsid(0)), static_cast<int>(::getpgrp())};
        ssize_t n;
        do {
            n = ::write(ack, &a, sizeof(a));
        } while (n < 0 && errno == EINTR);
        return n == static_cast<ssize_t>(sizeof(a));
    };
    for (int sig : std::array<int, 3>{SIGTERM, SIGINT, SIGHUP}) {
        struct sigaction action{};
        action.sa_handler = SIG_DFL;
        if (::sigemptyset(&action.sa_mask) != 0 || ::sigaction(sig, &action, nullptr) != 0) {
            (void)finish(errno);
            ::_exit(126);
        }
    }
    pid_t const pid = ::getpid();
    if (!arm::process_admitted(expected_parent)) {
        (void)finish(EPERM);
        ::_exit(126);
    }
    if (pid <= 1 || ::setsid() != pid || ::getsid(0) != pid || ::getpgrp() != pid) {
        (void)finish(errno ? errno : EINVAL);
        ::_exit(126);
    }
    Fd null(::open("/dev/null", O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    struct stat s{}, log_stat{}, a{}, b{}, c{};
    if (ack < 3 || log < 3 || null.value < 3 || ::fstat(log, &log_stat) != 0 || !S_ISREG(log_stat.st_mode) || null.value < 0 ||
        ::fstat(null.value, &s) != 0 || !S_ISCHR(s.st_mode) || ::dup2(null.value, 0) != 0 || ::dup2(log, 1) != 1 || ::dup2(log, 2) != 2) {
        (void)finish(errno ? errno : EINVAL);
        ::_exit(126);
    }
    if (::fstat(0, &a) || ::fstat(1, &b) || ::fstat(2, &c) || a.st_dev != s.st_dev || a.st_ino != s.st_ino || b.st_dev != log_stat.st_dev ||
        b.st_ino != log_stat.st_ino || c.st_dev != b.st_dev || c.st_ino != b.st_ino || !null.close() || ::close(log) != 0) {
        (void)finish(errno);
        ::_exit(126);
    }
    for (int fd = 0; fd < 3; ++fd)
        if (::isatty(fd) != 0) {
            (void)finish(EINVAL);
            ::_exit(126);
        }
    Fd tty(::open("/dev/tty", O_RDONLY | O_NOCTTY | O_CLOEXEC));
    if (tty.value >= 0 || errno != ENXIO) {
        (void)finish(EINVAL);
        ::_exit(126);
    }
    (void)::umask(0022);
    if (!finish(0) || ::close(ack) != 0) ::_exit(126);
    (void)r;
    ::execve(args[0], args, env);
    ::_exit(126);
}
// Missing files are ordinary while a case is running; every observed object
// must already satisfy the same ownership/type/size envelope as saved decoding.
auto observed_file(int dir, const char* name, uint64_t limit, uint64_t& bytes, mode_t mode = 0) -> bool {
    struct stat s{};
    if (::fstatat(dir, name, &s, AT_SYMLINK_NOFOLLOW) != 0) return errno == ENOENT;
    if (!S_ISREG(s.st_mode) || s.st_nlink != 1 || s.st_uid != owner_uid() || s.st_gid != owner_gid() || s.st_size < 0 ||
        static_cast<uint64_t>(s.st_size) > limit || (mode && (s.st_mode & 07777) != mode))
        return false;
    bytes += static_cast<uint64_t>(s.st_size);
    return true;
}
// Mirrors the saved decoder's retained-output scope (all phase descendants
// except work/timing), including render/mandel/perf outputs and phase.log.
// These are finite observations, not a hard filesystem quota between polls.
auto retained_outputs(int dir, unsigned depth, unsigned& entries, uint64_t& bytes) -> bool {
    if (depth > 8) return false;
    int fd = ::openat(dir, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return false;
    DIR* stream = ::fdopendir(fd);
    if (!stream) {
        (void)::close(fd);
        return false;
    }
    bool valid = true;
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(stream);
        if (!entry) {
            if (errno) valid = false;
            break;
        }
        if (!std::strcmp(entry->d_name, ".") || !std::strcmp(entry->d_name, "..")) continue;
        if (++entries > 4096) {
            valid = false;
            break;
        }
        if (depth == 0 && (!std::strcmp(entry->d_name, "work") || !std::strcmp(entry->d_name, "timing"))) continue;
        struct stat s{};
        if (::fstatat(fd, entry->d_name, &s, AT_SYMLINK_NOFOLLOW) != 0 || s.st_uid != owner_uid() || s.st_gid != owner_gid()) {
            valid = false;
            break;
        }
        if (S_ISDIR(s.st_mode)) {
            Fd child(::openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
            if (child.value < 0 || !retained_outputs(child.value, depth + 1, entries, bytes)) {
                valid = false;
                break;
            }
        } else {
            if (!S_ISREG(s.st_mode) || s.st_nlink != 1 || s.st_size < 0 || static_cast<uint64_t>(s.st_size) > FILE_LIMIT) {
                valid = false;
                break;
            }
            bytes += static_cast<uint64_t>(s.st_size);
            if (bytes > OUTPUT_LIMIT) {
                valid = false;
                break;
            }
        }
    }
    return ::closedir(stream) == 0 && valid;
}
auto inspect_phase(int phase, int logs, int work, int timing, const PhaseRequest& r, PhaseOutcome& out) -> OwnerFailure {
    uint64_t output = r.prior_output_bytes, transport = r.prior_transport_bytes + RESERVE;
    unsigned entries{};
    uint64_t metadata_only{};
    if (!retained_outputs(phase, 0, entries, output) || !observed_file(phase, "summary.tsv", 65536, metadata_only, 0644))
        return OwnerFailure::OUTPUT;
    for (auto name : CASES) {
        std::array<char, 128> path{};
        auto filename = [&](const char* suffix) {
            int n = std::snprintf(path.data(), path.size(), "%.*s.%s", static_cast<int>(name.size()), name.data(), suffix);
            return n > 0 && static_cast<size_t>(n) < path.size();
        };
        struct stat marker{};
        if (!filename("timeout")) return OwnerFailure::OUTPUT;
        if (::fstatat(work, path.data(), &marker, AT_SYMLINK_NOFOLLOW) == 0) return OwnerFailure::TIMEOUT_MARKER;
        if (errno != ENOENT) return OwnerFailure::OUTPUT;
        if (!filename("log") || !observed_file(logs, path.data(), FILE_LIMIT, metadata_only, 0644)) return OwnerFailure::OUTPUT;
        for (size_t i = 0; i < KINDS.size(); ++i)
            if (!filename(KINDS[i]) || !observed_file(timing, path.data(), KIND_LIMITS[i], transport, 0600)) return OwnerFailure::OUTPUT;
    }
    if (output > out.output_highwater) out.output_highwater = output;
    if (transport > out.transport_highwater) out.transport_highwater = transport;
    return output > OUTPUT_LIMIT || transport > TRANSPORT_LIMIT ? OwnerFailure::OUTPUT : OwnerFailure::NONE;
}
}  // namespace

auto own_suite_phase(const PhaseRequest& r) -> PhaseOutcome {
    PhaseOutcome out{};
    auto fail = [&](OwnerFailure why) {
        if (out.failure == OwnerFailure::NONE) {
            out.failure = why;
            out.system_error = errno;
        }
    };
    uint64_t clock{};
    bool nonce = r.nonce && std::strlen(r.nonce) == 32;
    if (nonce)
        for (size_t i = 0; i < 32; ++i)
            nonce = nonce && ((r.nonce[i] >= '0' && r.nonce[i] <= '9') || (r.nonce[i] >= 'a' && r.nonce[i] <= 'f'));
    if (!r.phase || (std::strcmp(r.phase, "warmup") && std::strcmp(r.phase, "measure")) || !nonce || !now(clock) ||
        r.work_stop_ns <= clock || r.work_stop_ns > UINT64_MAX - MARGIN || r.prior_output_bytes > OUTPUT_LIMIT ||
        r.prior_transport_bytes > TRANSPORT_LIMIT - RESERVE) {
        fail(OwnerFailure::INVALID_REQUEST);
        return out;
    }
#ifdef WOS_UMA_OWNER_HOST
    const char* root_name = r.host_root;
    uint64_t const grace = r.host_grace_ns;
    if (!r.host_argv || !r.host_argv[0] || grace > GRACE) {
        fail(OwnerFailure::INVALID_REQUEST);
        return out;
    }
#else
    const char* root_name = "/var/lib/uma-reference";
    uint64_t const grace = GRACE;
    if (::getuid() != 0 || ::geteuid() != 0 || ::getgid() != 0 || ::getegid() != 0 || ::getgroups(0, nullptr) != 0) {
        fail(OwnerFailure::INVALID_REQUEST);
        return out;
    }
#endif
    if (!r.cancellation || *r.cancellation) {
        fail(OwnerFailure::CANCELLED);
        return out;
    }
    Fd root(checked_root(root_name));
    Fd phase(::openat(root.value, r.phase, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    Fd logs(::openat(phase.value, "logs", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    Fd work(::openat(phase.value, "work", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    Fd timing(::openat(phase.value, "timing", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!directory(root.value) || !directory(phase.value) || !directory(logs.value) || !directory(work.value) || !directory(timing.value)) {
        fail(OwnerFailure::SETUP);
        return out;
    }
    Fd log(::openat(phase.value, "phase.log", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    struct stat ls{};
    if (log.value < 3 || ::fstat(log.value, &ls) != 0 || !S_ISREG(ls.st_mode) || ls.st_nlink != 1 || ls.st_uid != owner_uid() ||
        ls.st_gid != owner_gid() || (ls.st_mode & 07777) != 0600) {
        fail(OwnerFailure::SETUP);
        return out;
    }
    int pipes[2]{-1, -1};
    if (::pipe(pipes) != 0) {
        fail(OwnerFailure::SETUP);
        return out;
    }
    Fd read_ack(pipes[0]), write_ack(pipes[1]);
    for (int fd : pipes)
        if (fd < 3 || ::fcntl(fd, F_SETFD, FD_CLOEXEC) != 0 || ::fcntl(fd, F_SETFL, O_NONBLOCK) != 0) {
            fail(OwnerFailure::SETUP);
            return out;
        }
    std::string const artifact = "WOS_SUITE_ARTIFACT_ROOT=" + std::string(root_name) + "/" + r.phase;
    std::string const matrix = "WOS_SUITE_COMMAND_MATRIX_ROOT=/root/wos-uma-reference/" + std::string(r.phase);
    std::string const run = "WOS_SUITE_RUN_ID=uma-reference-" + std::string(r.phase);
    std::string const phase_env = "WOS_SUITE_TIMING_PHASE=" + std::string(r.phase);
    std::string const nonce_env = "WOS_SUITE_TIMING_NONCE=" + std::string(r.nonce);
    std::string const deadline = "WOS_SUITE_TIMING_DEADLINE_NS=" + std::to_string(r.work_stop_ns);
    std::array<const char*, 17> env{{"PATH=/usr/bin:/usr/sbin:/bin:/sbin", "WOS_SUITE_TIMING_VERSION=1", "WOS_SUITE_SCALE=full",
                                     "WOS_SUITE_RUN_TESTD=1", "WOS_SUITE_RUN_TESTPROG_PERF=1", "WOS_SUITE_RUN_STRACE=1",
                                     "WOS_SUITE_RUN_PERF_TRACE=1", "WOS_SUITE_MANDEL_NODES=uma-reference-vm0", artifact.c_str(),
                                     matrix.c_str(), run.c_str(), phase_env.c_str(), nonce_env.c_str(), deadline.c_str(), "USER=root",
                                     "HOME=/root", "HOSTNAME=uma-reference-vm0"}};
    std::array<char*, 18> exported{};
    for (size_t i = 0; i < env.size(); ++i) exported[i] = const_cast<char*>(env[i]);
#ifdef WOS_UMA_OWNER_HOST
    char* const* args = r.host_argv;
#else
    std::array<char*, 2> fixed{const_cast<char*>("/usr/bin/wos-userland-suite"), nullptr};
    char* const* args = fixed.data();
#endif
    if (*r.cancellation) {
        fail(OwnerFailure::CANCELLED);
        return out;
    }
    pid_t const expected_parent = ::getpid();
    out.child = ::fork();
    if (out.child == 0) {
        (void)read_ack.close();
        child_exec(write_ack.value, log.value, r, expected_parent, args, exported.data());
    }
    if (out.child <= 1) {
        fail(OwnerFailure::SETUP);
        return out;
    }
    bool retained = true, terminal = false;
    if (!write_ack.close() || !log.close()) fail(OwnerFailure::SETUP);
    Admission admission{};
    size_t ack_bytes{};
    auto peek = [&]() -> bool {
        if (!retained || out.consumed) return false;
        int raw = -1;
        pid_t const got = child_wait(out.child, raw, WNOHANG | WNOWAIT);
        if (got < 0 && errno == EINTR) return true;
        if (got == 0) return true;
        if (got != out.child || (!WIFEXITED(raw) && !WIFSIGNALED(raw))) {
            retained = false;
            fail(OwnerFailure::WAIT_AUTHORITY);
            return false;
        }
        ++out.nonconsuming_completions;
        if (terminal && raw != out.observed_raw) {
            retained = false;
            fail(OwnerFailure::WAIT_AUTHORITY);
            return false;
        }
        terminal = true;
        out.observed_raw = raw;
        return true;
    };
    while (out.failure == OwnerFailure::NONE) {
        if (*r.cancellation) {
            fail(OwnerFailure::CANCELLED);
            break;
        }
        if (!now(clock)) {
            fail(OwnerFailure::CLOCK);
            break;
        }
        if (clock >= r.work_stop_ns) {
            fail(OwnerFailure::DEADLINE);
            break;
        }
        if (!out.admitted) {
            ssize_t const n = ::read(read_ack.value, reinterpret_cast<char*>(&admission) + ack_bytes, sizeof(admission) - ack_bytes);
            if (n > 0)
                ack_bytes += static_cast<size_t>(n);
            else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                fail(OwnerFailure::ACKNOWLEDGMENT);
                break;
            } else if (n == 0) {
                fail(OwnerFailure::ACKNOWLEDGMENT);
                break;
            }
            if (ack_bytes == sizeof(admission)) {
                if (admission.error || admission.pid != out.child || admission.sid != out.child || admission.pgid != out.child) {
                    fail(OwnerFailure::ACKNOWLEDGMENT);
                    break;
                }
                out.admitted = true;
#ifdef WOS_UMA_OWNER_HOST
                uma_owner_host_admitted();
#endif
                if (!read_ack.close()) {
                    fail(OwnerFailure::ACKNOWLEDGMENT);
                    break;
                }
            }
        }
        OwnerFailure const inspected = inspect_phase(phase.value, logs.value, work.value, timing.value, r, out);
        if (inspected != OwnerFailure::NONE) {
            fail(inspected);
            break;
        }
        if (!peek()) break;
        if (terminal && out.admitted) {
            if (out.observed_raw != 0) fail(OwnerFailure::RAW_STATUS);
            break;
        }
        // The first read may have observed EAGAIN immediately before the child
        // wrote its acknowledgment and exited. Retain terminal membership and
        // read the pipe again; only its complete validated record admits group
        // signals. EOF/partial/error still fails closed, never status alone.
        pause_poll();
    }
    if (out.failure != OwnerFailure::NONE && retained) {
        uint64_t start{};
        if (!now(start)) {
            fail(OwnerFailure::CLOCK);
            retained = false;
        }
        uint64_t const cleanup_end = start < r.work_stop_ns ? start + MARGIN : r.work_stop_ns + MARGIN;
        auto send = [&](int sig) {
            if (!peek()) return;
            // The exact child membership is retained even when its EXIT event
            // is visible. No numeric fallback if membership became uncertain.
            pid_t const target = out.admitted ? -out.child : out.child;
            if (sig == SIGTERM)
                ++out.term_calls;
            else
                ++out.kill_calls;
            if (signal_child(target, sig) != 0 && errno != ESRCH) fail(OwnerFailure::SIGNAL);
        };
        if (retained) send(SIGTERM);
        while (retained && now(clock) && clock - start < grace && clock < cleanup_end) {
            if (!peek()) break;
            pause_poll();
        }
        if (retained) send(SIGKILL);
        while (retained && !terminal) {
            if (!now(clock) || clock >= cleanup_end) {
                fail(OwnerFailure::REAP);
                break;
            }
            if (!peek()) break;
            if (!terminal) pause_poll();
        }
    }
    if (retained && terminal) {
        int raw = -1;
        pid_t got = -1;
        unsigned retries{};
        for (;;) {
            if (!now(clock) || clock >= r.work_stop_ns + MARGIN || ++retries > 4096) {
                fail(OwnerFailure::REAP);
                break;
            }
            got = child_wait(out.child, raw, WNOHANG);
            if (got >= 0 || errno != EINTR) break;
        }
        if (got == out.child) {
            out.consumed = true;
            ++out.consuming_completions;
            out.consumed_raw = raw;
            retained = false;
            if (raw != out.observed_raw) fail(OwnerFailure::WAIT_AUTHORITY);
#ifdef WOS_UMA_OWNER_HOST
            uma_owner_host_after_consume();
#endif
            // A late cancellation invalidates outcome, but cannot authorize a
            // signal once this successful consuming wait gave up membership.
            if (*r.cancellation) fail(OwnerFailure::CANCELLED);
        } else {
            retained = false;
            fail(OwnerFailure::WAIT_AUTHORITY);
        }
    } else if (!out.consumed)
        fail(OwnerFailure::REAP);
    out.raw_phase_zero =
        out.failure == OwnerFailure::NONE && out.admitted && out.consumed && out.observed_raw == 0 && out.consumed_raw == 0;
    return out;
}
}  // namespace uma_reference

namespace uma_reference::arm {
auto phase_output_bytes(const char* name, uint64_t& out) -> bool {
    if (!name || (std::strcmp(name, "warmup") && std::strcmp(name, "measure"))) return false;
    Fd root(checked_root(root_path()));
    Fd phase(::openat(root.value, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    unsigned entries{};
    uint64_t bytes{};
    if (!directory(phase.value) || !retained_outputs(phase.value, 0, entries, bytes)) return false;
    out = bytes;
    return true;
}
}  // namespace uma_reference::arm
