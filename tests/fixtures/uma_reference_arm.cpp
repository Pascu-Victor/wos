#include "uma_reference_arm.hpp"

#include <fcntl.h>
#include <grp.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#ifndef WOS_UMA_ARM_HOST
#include <sys/multiproc.h>
#include <sys/process.h>
#endif

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

extern char** environ;
#ifdef WOS_UMA_ARM_HOST
extern auto uma_arm_host_profile(uma_reference::arm::Profile&, pid_t, bool) -> bool;
extern auto uma_arm_host_poweroff() -> bool;
extern auto uma_arm_host_snapshot(ker::abi::init_control::StatusSnapshot&) -> int64_t;
#endif

namespace uma_reference::arm {
namespace {
volatile sig_atomic_t* active_cancel{};
constexpr std::array<int, 3> SIGNALS{SIGTERM, SIGINT, SIGHUP};
void cancelled(int) {
    if (active_cancel) *active_cancel = 1;
}
auto all_zero(const void* data, size_t bytes) -> bool {
    auto p = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < bytes; ++i)
        if (p[i]) return false;
    return true;
}
auto root_fd() -> int {
    // No-follow each component, including parents. The run directory is always
    // fresh0700; canonical /var and /var/lib need not have that private mode.
    const char* path = root_path();
    if (!path || path[0] != '/' || std::strlen(path) >= 512) return -1;
    int current = ::open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    size_t at = 1;
    while (current >= 0 && path[at]) {
        size_t begin = at;
        while (path[at] && path[at] != '/') ++at;
        std::array<char, 256> part{};
        if (begin == at || at - begin >= part.size()) {
            ::close(current);
            return -1;
        }
        std::memcpy(part.data(), path + begin, at - begin);
        if (!std::strcmp(part.data(), ".") || !std::strcmp(part.data(), "..")) {
            ::close(current);
            return -1;
        }
        int next = ::openat(current, part.data(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        bool closed = ::close(current) == 0;
        current = next;
        if (!closed) {
            if (next >= 0) ::close(next);
            return -1;
        }
        if (path[at]) ++at;
    }
    struct stat st{};
#ifdef WOS_UMA_ARM_HOST
    uid_t uid = ::getuid();
    gid_t gid = ::getgid();
#else
    uid_t uid = 0;
    gid_t gid = 0;
#endif
    if (current < 0 || ::fstat(current, &st) || !S_ISDIR(st.st_mode) || (st.st_mode & 07777) != 0700 || st.st_uid != uid ||
        st.st_gid != gid) {
        if (current >= 0) ::close(current);
        return -1;
    }
    return current;
}
auto write_all(int fd, const void* input, size_t size) -> bool {
    auto p = static_cast<const char*>(input);
    while (size) {
        auto n = ::write(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0 || static_cast<size_t>(n) > size) return false;
        p += n;
        size -= static_cast<size_t>(n);
    }
    return true;
}
auto clock_text(const char* data, size_t bytes, uint64_t resolution) -> bool {
    // Preserve all raw fields. Exact key order matches current procfs generator;
    // numeric telemetry can change, source/resolution/health cannot degrade.
    constexpr std::array<std::string_view, 24> keys{"version",
                                                    "source",
                                                    "invariant_tsc",
                                                    "frequency_hz",
                                                    "resolution_ns",
                                                    "conversion_multiplier",
                                                    "conversion_divisor",
                                                    "calibration_spread_ppm",
                                                    "watchdog_deviation_ns",
                                                    "kvm_registered_cpus",
                                                    "kvm_expected_cpus",
                                                    "kvm_watchdog_checks",
                                                    "rtc_valid",
                                                    "wall_steps",
                                                    "wall_slews",
                                                    "wall_rejected",
                                                    "wall_pending_ns",
                                                    "slew_limit_ppm",
                                                    "process_timer_real_depth",
                                                    "process_timer_wake_depth",
                                                    "process_timer_expirations",
                                                    "process_timer_late_us_max",
                                                    "process_timer_work_budget",
                                                    "clockevent"};
    std::string_view text(data, bytes), source;
    uint64_t frequency{}, registered{}, expected{}, res{}, invariant{};
    for (size_t k = 0; k < keys.size(); ++k) {
        auto end = text.find('\n');
        if (end == std::string_view::npos) return false;
        auto line = text.substr(0, end);
        text.remove_prefix(end + 1);
        auto eq = line.find('=');
        if (eq == std::string_view::npos || line.substr(0, eq) != keys[k]) return false;
        auto value = line.substr(eq + 1);
        if (k == 1) {
            source = value;
            continue;
        }
        if (k == 23) {
            if (value != "x2apic-oneshot") return false;
            continue;
        }
        if (k == 16 && value.starts_with('-')) value.remove_prefix(1);
        uint64_t v{};
        if (value.empty() || (value.size() > 1 && value.front() == '0')) return false;
        for (char c : value) {
            if (c < '0' || c > '9' || v > (UINT64_MAX - static_cast<unsigned>(c - '0')) / 10) return false;
            v = v * 10 + static_cast<unsigned>(c - '0');
        }
        if (k == 0 && v != 1) return false;
        if (k == 2) invariant = v;
        if (k == 3) frequency = v;
        if (k == 4) res = v;
        if ((k == 5 || k == 6) && v == 0) return false;
        if (k == 9) registered = v;
        if (k == 10) expected = v;
        if (k == 17 && v != 500) return false;
        if ((k == 2 || k == 12) && v > 1) return false;
    }
    if (!text.empty() || !frequency || res != resolution || !res) return false;
    if (source == "kvm-clock") return registered == 8 && expected == 8;
    // Actual state_name strings, including the initial safe SMP fallback.
    return source == "hpet:missing-tsc" || source == "hpet:noninvariant-tsc" || source == "hpet:calibration-failed" ||
           source == "hpet:smp-unqualified" || (source == "tsc" && invariant == 1);
}
}  // namespace

Cancellation::~Cancellation() {
    while (installed_) {
        --installed_;
        (void)::sigaction(SIGNALS[installed_], &old_[installed_], nullptr);
    }
    if (active_cancel == &value) active_cancel = nullptr;
}
auto Cancellation::install() -> bool {
    if (active_cancel || installed_ || value) return false;
    struct sigaction child{}, action{};
    if (::sigaction(SIGCHLD, nullptr, &child) || child.sa_handler != SIG_DFL || (child.sa_flags & SA_NOCLDWAIT)) return false;
    action.sa_handler = cancelled;
    if (::sigemptyset(&action.sa_mask)) return false;
    active_cancel = &value;
    for (int sig : SIGNALS) {
        if (::sigaction(sig, &action, &old_[installed_])) return false;
        ++installed_;
    }
    return true;
}
auto valid_nonce(const char* text) -> bool {
    if (!text || std::strlen(text) != 32) return false;
    for (size_t i = 0; i < 32; ++i)
        if (!((text[i] >= '0' && text[i] <= '9') || (text[i] >= 'a' && text[i] <= 'f'))) return false;
    return true;
}
auto profile_valid(const Profile& p, pid_t expected_parent, bool witness) -> bool {
    for (auto id : p.uid)
        if (id) return false;
    for (auto id : p.gid)
        if (id) return false;
    return p.groups == 0 && p.pid > 1 && p.pid <= INT64_MAX && p.parent == static_cast<uint64_t>(expected_parent) && p.tid == p.pid &&
           p.cpus == 8 && p.affinity == 0xff && p.authority == (witness ? 16 : 65535) && p.nnp == (witness ? 1 : 0) &&
           p.privileges == (witness ? 256 : 0) && p.priority == 0 && p.target_result == 0 && p.target_flags == 2 && p.empty_target;
}
auto process_admitted(pid_t expected_parent, bool witness) -> bool {
    Profile p{};
#ifdef WOS_UMA_ARM_HOST
    if (!uma_arm_host_profile(p, expected_parent, witness)) return false;
#else
    uid_t r{}, e{}, s{};
    gid_t gr{}, ge{}, gs{};
    if (::getresuid(&r, &e, &s) || ::getresgid(&gr, &ge, &gs)) return false;
    p.uid[0] = r;
    p.uid[1] = e;
    p.uid[2] = s;
    p.gid[0] = gr;
    p.gid[1] = ge;
    p.gid[2] = gs;
    p.groups = static_cast<uint64_t>(::getgroups(0, nullptr));
    p.pid = ker::process::getpid();
    p.parent = ker::process::getppid();
    p.tid = ker::multiproc::currentThreadId();
    p.cpus = ker::multiproc::nativeThreadCount();
    p.affinity = static_cast<uint64_t>(ker::multiproc::getThreadAffinityMask(p.tid));
    using C = ker::abi::process::CredentialControl;
    p.authority = ker::process::prctl(static_cast<int>(C::GET_AUTHORITY_CEILING), 0, 0, 0, 0);
    p.nnp = ker::process::prctl(static_cast<int>(C::GET_NO_NEW_PRIVILEGES), 0, 0, 0, 0);
    p.privileges = ker::process::prctl(static_cast<int>(C::GET_SERVICE_PRIVILEGES), 0, 0, 0, 0);
    errno = 0;
    p.priority = ::getpriority(PRIO_PROCESS, 0);
    if (errno) return false;
    std::array<char, 256> target{};
    p.target_result = ker::process::getwkitarget(target.data(), target.size(), &p.target_flags);
    p.empty_target = target[0] == 0;
#endif
    init::StatusSnapshot snapshot{};
    init::ServiceStatus selected{};
    return profile_valid(p, expected_parent, witness) && snapshot_read(snapshot) == 0 && snapshot_valid(snapshot, selected);
}
auto request_poweroff() -> bool {
#ifdef WOS_UMA_ARM_HOST
    return uma_arm_host_poweroff();
#else
    return ::kill(1, SIGUSR2) == 0;
#endif
}
auto environment_valid() -> bool {
    const char *user = std::getenv("USER"), *home = std::getenv("HOME"), *host = std::getenv("HOSTNAME");
    if (!user || std::strcmp(user, "root") || !home || std::strcmp(home, "/root") || !host || std::strcmp(host, "uma-reference-vm0"))
        return false;
    for (char** item = environ; item && *item; ++item)
        if (!std::strncmp(*item, "WOS_SUITE_", 10)) return false;
    return std::getenv("LLVM_PROFILE_FILE") == nullptr;
}
auto snapshot_read(init::StatusSnapshot& out) -> int64_t {
    struct Guarded {
        uint64_t before = 0x49a064;
        init::StatusSnapshot value{};
        uint64_t after = ~uint64_t{0x49a064};
    } g;
#ifdef WOS_UMA_ARM_HOST
    auto rc = uma_arm_host_snapshot(g.value);
#else
    auto rc = ker::process::init_status_read(&g.value);
#endif
    if (g.before != 0x49a064 || g.after != ~uint64_t{0x49a064}) return -EOVERFLOW;
    if (rc == 0) out = g.value;
    return rc;
}
auto snapshot_valid(const init::StatusSnapshot& s, init::ServiceStatus& selected) -> bool {
    if (s.size != 4160 || s.version != 1 || !s.sequence || !s.published_mono_ns || s.flags || s.service_count != SERVICES.size() ||
        s.reserved0 || !all_zero(s.reserved, sizeof(s.reserved)) ||
        (s.state != init::SupervisorState::STARTING && s.state != init::SupervisorState::RUNNING))
        return false;
    std::array<unsigned, SERVICES.size()> counts{};
    init::ServiceStatus found{};
    for (size_t i = 0; i < init::MAX_STATUS_SERVICES; ++i) {
        auto const& row = s.services[i];
        if (i >= s.service_count) {
            if (!all_zero(&row, sizeof(row))) return false;
            continue;
        }
        auto nul = static_cast<const char*>(std::memchr(row.service, 0, sizeof(row.service)));
        if (!nul || !all_zero(nul, sizeof(row.service) - static_cast<size_t>(nul - row.service)) || row.reserved0 || row.reserved1 ||
            !all_zero(row.reserved, sizeof(row.reserved)) || unsigned(row.state) < 1 || unsigned(row.state) > 8 ||
            unsigned(row.readiness) < 1 || unsigned(row.readiness) > 3 || (row.flags & ~7U) || row.history_count > 4 ||
            !row.state_since_mono_ns || row.state_since_mono_ns > s.published_mono_ns)
            return false;
        bool named = false;
        for (size_t n = 0; n < SERVICES.size(); ++n)
            if (!std::strcmp(row.service, SERVICES[n])) {
                named = true;
                ++counts[n];
                if (n == 6) found = row;
            }
        if (!named) return false;
        uint64_t previous{};
        for (size_t j = 0; j < 4; ++j) {
            auto const& t = row.history[j];
            if (j >= row.history_count) {
                if (!all_zero(&t, sizeof(t))) return false;
                continue;
            }
            if (!t.timestamp_mono_ns || t.timestamp_mono_ns < previous || t.timestamp_mono_ns > s.published_mono_ns || t.reserved0 ||
                unsigned(t.from_state) > 8 || unsigned(t.to_state) > 8 || unsigned(t.reason) < 1 || unsigned(t.reason) > 9)
                return false;
            previous = t.timestamp_mono_ns;
        }
    }
    for (auto count : counts)
        if (count != 1) return false;
    selected = found;
    return true;
}
auto running(const init::StatusSnapshot& s, pid_t controller, uint64_t& deadline) -> bool {
    init::ServiceStatus row{};
    if (!snapshot_valid(s, row) || row.state != init::ServiceState::RUNNING || row.readiness != init::ReadinessState::NOT_READY ||
        row.flags != 3 || row.generation != 1 || row.restart_count || row.pid != controller || row.pgid != controller || row.last_error ||
        row.last_wait_status || row.deadline_mono_ns <= row.state_since_mono_ns ||
        row.deadline_mono_ns - row.state_since_mono_ns != 600 * SECOND)
        return false;
    bool exec = false;
    for (size_t i = 0; i < row.history_count; ++i)
        if (row.history[i].reason == init::TransitionReason::SPAWN && row.history[i].to_state == init::ServiceState::RUNNING &&
            row.history[i].timestamp_mono_ns == row.state_since_mono_ns)
            exec = true;
    if (!exec) return false;
    deadline = row.deadline_mono_ns;
    return true;
}
auto consumed(const init::StatusSnapshot& s) -> bool {
    init::ServiceStatus row{};
    if (!snapshot_valid(s, row) || s.state != init::SupervisorState::RUNNING || row.state != init::ServiceState::EXITED ||
        row.readiness != init::ReadinessState::READY || row.flags != 3 || row.generation != 1 || row.restart_count || row.pid != -1 ||
        row.pgid != -1 || row.last_wait_status || row.last_error || row.deadline_mono_ns)
        return false;
    for (size_t i = 0; i < row.history_count; ++i)
        if (row.history[i].reason == init::TransitionReason::EXIT && row.history[i].to_state == init::ServiceState::EXITED &&
            row.history[i].timestamp_mono_ns >= row.state_since_mono_ns)
            return true;
    return false;
}
auto now(uint64_t& out) -> bool {
    timespec t{};
    if (::clock_gettime(CLOCK_MONOTONIC, &t) || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= long(SECOND) ||
        uint64_t(t.tv_sec) > (UINT64_MAX - uint64_t(t.tv_nsec)) / SECOND)
        return false;
    out = uint64_t(t.tv_sec) * SECOND + uint64_t(t.tv_nsec);
    return out != 0;
}
auto root_path() -> const char* {
#ifdef WOS_UMA_ARM_HOST
    return std::getenv("WOS_UMA_TEST_ROOT");
#else
    return ROOT;
#endif
}
auto save(const char* name, const void* data, size_t size) -> bool {
    if (!name || std::strchr(name, '/') || size == 0 || size > 8192) return false;
    int dir = root_fd();
    if (dir < 0) return false;
    int fd = ::openat(dir, name, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    struct stat st{};
    bool good = fd >= 0 && ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_nlink == 1 && (st.st_mode & 07777) == 0600 &&
                st.st_size == 0 && write_all(fd, data, size) && ::fsync(fd) == 0;
    if (fd >= 0 && ::close(fd)) good = false;
    if (::fsync(dir) || ::close(dir)) good = false;
    return good;
}
auto read(const char* name, void* data, size_t capacity, size_t& bytes) -> bool {
    if (!name || std::strchr(name, '/') || capacity > 8192) return false;
    int dir = root_fd();
    if (dir < 0) return false;
    int fd = ::openat(dir, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    bool good = ::close(dir) == 0;
    struct stat st{};
#ifdef WOS_UMA_ARM_HOST
    uid_t uid = ::getuid();
    gid_t gid = ::getgid();
#else
    uid_t uid = 0;
    gid_t gid = 0;
#endif
    good = fd >= 0 && good && ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_nlink == 1 && st.st_uid == uid && st.st_gid == gid &&
           (st.st_mode & 07777) == 0600 && st.st_size > 0 && uint64_t(st.st_size) <= capacity;
    size_t total{};
    while (good && total < static_cast<size_t>(st.st_size)) {
        auto n = ::read(fd, static_cast<char*>(data) + total, size_t(st.st_size) - total);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0 || size_t(n) > size_t(st.st_size) - total) {
            good = false;
            break;
        }
        total += size_t(n);
    }
    struct stat after{};
    if (good && (::fstat(fd, &after) || after.st_dev != st.st_dev || after.st_ino != st.st_ino || after.st_size != st.st_size ||
                 after.st_mtim.tv_sec != st.st_mtim.tv_sec || after.st_mtim.tv_nsec != st.st_mtim.tv_nsec))
        good = false;
    if (fd >= 0 && ::close(fd)) good = false;
    if (good) bytes = total;
    return good;
}
auto clock_capture(const char* name, uint64_t& resolution) -> bool {
    timespec t{};
    if (::clock_getres(CLOCK_MONOTONIC, &t) || t.tv_sec < 0 || t.tv_nsec < 0 || t.tv_nsec >= long(SECOND) ||
        uint64_t(t.tv_sec) > (UINT64_MAX - uint64_t(t.tv_nsec)) / SECOND)
        return false;
    uint64_t res = uint64_t(t.tv_sec) * SECOND + uint64_t(t.tv_nsec);
    std::array<char, 4096> text{};
#ifdef WOS_UMA_ARM_HOST
    const char* clock_path = std::getenv("WOS_UMA_CLOCK_SOURCE");
    if (!clock_path) return false;
#else
    const char* clock_path = "/proc/timekeeping";
#endif
    int fd = ::open(clock_path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return false;
    size_t n{};
    bool good = true;
    while (n < text.size()) {
        auto got = ::read(fd, text.data() + n, text.size() - n);
        if (got < 0 && errno == EINTR) continue;
        if (got < 0 || size_t(got) > text.size() - n) {
            good = false;
            break;
        }
        if (!got) break;
        n += size_t(got);
    }
    if (::close(fd)) good = false;
    if (!good || !n || n >= text.size() || !clock_text(text.data(), n, res) || !save(name, text.data(), n)) return false;
    resolution = res;
    return true;
}
auto clock_pair(uint64_t res) -> bool {
    std::array<char, 4096> a{}, b{};
    size_t an{}, bn{};
    if (!read("clock-before.txt", a.data(), a.size(), an) || !read("clock-after.txt", b.data(), b.size(), bn) ||
        !clock_text(a.data(), an, res) || !clock_text(b.data(), bn, res))
        return false;
    std::string_view av(a.data(), an), bv(b.data(), bn);
    auto field = [](std::string_view text, std::string_view key) {
        auto pos = text.find(key);
        if (pos == std::string_view::npos) return std::string_view{};
        text.remove_prefix(pos);
        return text.substr(0, text.find('\n'));
    };
    for (auto key :
         std::array<std::string_view, 9>{"source=", "invariant_tsc=", "frequency_hz=", "resolution_ns=", "conversion_multiplier=",
                                         "conversion_divisor=", "calibration_spread_ppm=", "kvm_registered_cpus=", "kvm_expected_cpus="})
        if (field(av, key) != field(bv, key)) return false;
    return true;
}
auto format_completion(char* out, size_t cap, const Completion& c) -> int {
    return std::snprintf(
        out, cap,
        "UMA_ARM v=1 nonce=%s controller=%llu warm=%llu measure=%llu init_deadline=%llu work_stop=%llu "
        "warm_first=%llu warm_last=%llu measure_first=%llu measure_last=%llu res=%llu warm_transport=%llu measure_transport=%llu "
        "warm_output=%llu measure_output=%llu cases=234 metrics=11 warm_peek=0 warm_wait=0 measure_peek=0 measure_wait=0\n",
        c.nonce.data(), (unsigned long long)c.controller, (unsigned long long)c.warm, (unsigned long long)c.measure,
        (unsigned long long)c.deadline, (unsigned long long)c.work_stop, (unsigned long long)c.phases[0].first,
        (unsigned long long)c.phases[0].last, (unsigned long long)c.phases[1].first, (unsigned long long)c.phases[1].last,
        (unsigned long long)c.phases[0].resolution, (unsigned long long)c.phases[0].transport, (unsigned long long)c.phases[1].transport,
        (unsigned long long)c.phases[0].output, (unsigned long long)c.phases[1].output);
}
auto parse_completion(const char* data, size_t bytes, const char* nonce, Completion& out) -> bool {
    if (!valid_nonce(nonce) || !bytes || bytes >= COMPLETION_LIMIT || data[bytes - 1] != '\n' || std::memchr(data, 0, bytes)) return false;
    std::array<char, COMPLETION_LIMIT> text{}, canonical{};
    std::memcpy(text.data(), data, bytes);
    Completion c{};
    unsigned long long v[14]{};
    int used{};
    int n = std::sscanf(
        text.data(),
        "UMA_ARM v=1 nonce=%32[0-9a-f] controller=%llu warm=%llu measure=%llu init_deadline=%llu work_stop=%llu "
        "warm_first=%llu warm_last=%llu measure_first=%llu measure_last=%llu res=%llu warm_transport=%llu measure_transport=%llu "
        "warm_output=%llu measure_output=%llu cases=234 metrics=11 warm_peek=0 warm_wait=0 measure_peek=0 measure_wait=0\n%n",
        c.nonce.data(), &v[0], &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9], &v[10], &v[11], &v[12], &v[13], &used);
    if (n != 15 || size_t(used) != bytes || std::strcmp(c.nonce.data(), nonce)) return false;
    c.controller = v[0];
    c.warm = v[1];
    c.measure = v[2];
    c.deadline = v[3];
    c.work_stop = v[4];
    c.phases[0] = {v[5], v[6], v[9], v[10], v[12], 117, 11};
    c.phases[1] = {v[7], v[8], v[9], v[11], v[13], 117, 11};
    if (format_completion(canonical.data(), canonical.size(), c) != int(bytes) || std::memcmp(text.data(), canonical.data(), bytes) ||
        c.controller <= 1 || c.controller > INT64_MAX || c.warm <= 1 || c.warm > INT64_MAX || c.measure <= 1 || c.measure > INT64_MAX ||
        c.controller == c.warm || c.controller == c.measure || c.warm == c.measure || c.deadline <= MARGIN ||
        c.work_stop != c.deadline - MARGIN || !c.phases[0].first || c.phases[0].first >= c.phases[0].last ||
        c.phases[0].last >= c.phases[1].first || c.phases[1].first >= c.phases[1].last || c.phases[1].last >= c.work_stop || !v[9] ||
        v[10] > 512 * 1024 || v[11] > 512 * 1024 - v[10] || v[10] + v[11] + 32768 > 512 * 1024 || v[12] > 64 * 1024 * 1024 ||
        v[13] > 64 * 1024 * 1024 - v[12])
        return false;
    out = c;
    return true;
}
auto audit_completion(const Completion& c) -> bool {
    PhaseAudit a{}, b{};
    if (!audit_phase("warmup", c.nonce.data(), c.work_stop, pid_t(c.warm), a) ||
        !audit_phase("measure", c.nonce.data(), c.work_stop, pid_t(c.measure), b) || !clock_pair(c.phases[0].resolution))
        return false;
    auto equal = [](const PhaseAudit& x, const PhaseAudit& y) {
        return x.first == y.first && x.last == y.last && x.resolution == y.resolution && x.transport == y.transport &&
               x.output == y.output && x.cases == y.cases && x.metrics == y.metrics;
    };
    return equal(a, c.phases[0]) && equal(b, c.phases[1]);
}
}  // namespace uma_reference::arm
