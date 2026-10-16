// Trusted private diagnostic witness; never a public NUMA policy endpoint.
#define WOS_SELFTEST 1
#include <fcntl.h>
#include <grp.h>
#include <signal.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <test/numa_lifecycle_census_wire.hpp>

namespace {
namespace nl = ker::test::numa_lifecycle;
using Control = ker::abi::process::CredentialControl;
constexpr uint64_t SIGNAL = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SIGNAL);
constexpr uint64_t DEBUG = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::KERNEL_DEBUG);
constexpr uint64_t IDS = (uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SET_UID)) |
                         (uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::SET_GID));
constexpr uint64_t INITIAL = SIGNAL | DEBUG | IDS;
constexpr uint64_t LOG_BYTES = 1024 * 1024;
constexpr uint64_t WHOLE_NS = 850'000'000'000;
constexpr char PROBE[] = "/usr/bin/testprog";
constexpr char DIRECTORY[] = "/var/lib/numa-lifecycle";
constexpr const char* PATHS[]{"/var/lib/numa-lifecycle/warm.log", "/var/lib/numa-lifecycle/measured.log",
                              "/var/lib/numa-lifecycle/begin.bin", "/var/lib/numa-lifecycle/end.bin",
                              "/var/lib/numa-lifecycle/witness.txt"};
struct alignas(4096) Transports {
    alignas(4096) nl::Request request{};
    alignas(4096) nl::Report output{};
    alignas(4096) nl::Report begin{};
    alignas(4096) nl::Report end{};
    alignas(4096) unsigned char zeros[4096]{};
};
Transports transport{};
int files[5]{-1, -1, -1, -1, -1};
uint64_t deadline_ns{}, nonce{}, cookie{};

auto control(Control operation, uint64_t value = 0) -> int64_t { return ker::process::prctl(static_cast<int>(operation), value, 0, 0, 0); }
auto identity(uint32_t uid, uint64_t authority) -> bool {
    uid_t r{}, e{}, s{};
    gid_t gr{}, ge{}, gs{};
    return ::getresuid(&r, &e, &s) == 0 && ::getresgid(&gr, &ge, &gs) == 0 && r == uid && e == uid && s == uid && gr == uid && ge == uid &&
           gs == uid && ::getgroups(0, nullptr) == 0 && control(Control::GET_AUTHORITY_CEILING) == static_cast<int64_t>(authority) &&
           control(Control::GET_NO_NEW_PRIVILEGES) == 1 &&
           control(Control::GET_SERVICE_PRIVILEGES) == static_cast<int64_t>(ker::abi::process::SERVICE_PRIVILEGES_LOCKED);
}
auto now_ns() -> uint64_t {
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 || value.tv_sec < 0 || value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000)
        return UINT64_MAX;
    auto const seconds = static_cast<uint64_t>(value.tv_sec);
    return seconds <= (UINT64_MAX - static_cast<uint64_t>(value.tv_nsec)) / 1'000'000'000
               ? seconds * 1'000'000'000 + static_cast<uint64_t>(value.tv_nsec)
               : UINT64_MAX;
}
auto pause_poll() -> bool {
    timespec const delay{.tv_sec = 0, .tv_nsec = 10'000'000};
    return ::nanosleep(&delay, nullptr) == 0 || errno == EINTR;
}
auto time_left() -> bool {
    auto const now = now_ns();
    return now != UINT64_MAX && now < deadline_ns;
}
auto write_all(int fd, const void* data, size_t bytes) -> bool {
    auto* current = static_cast<const unsigned char*>(data);
    while (bytes != 0) {
        auto const count = ::write(fd, current, bytes);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0 || static_cast<uint64_t>(count) > bytes) return false;
        current += count;
        bytes -= static_cast<size_t>(count);
    }
    return true;
}
void prefault_transport() {
    // Actual CPL3 writes AFTER consuming each fork child: pre-fork writable
    // transport can have become COW. Preserve every already-saved report byte.
    auto* bytes = reinterpret_cast<volatile unsigned char*>(&transport);
    for (size_t i = 0; i < sizeof(transport); ++i) {
        auto const value = bytes[i];
        bytes[i] = value;
    }
}
auto prepare_files() -> bool {
    if (::mkdir(DIRECTORY, 0700) != 0 && errno != EEXIST) return false;
    for (size_t i = 0; i < 5; ++i) {
        files[i] = ::open(PATHS[i], O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (files[i] < 0) return false;
        auto remaining = i < 2 ? LOG_BYTES : i < 4 ? sizeof(nl::Report) : uint64_t{4096};
        while (remaining != 0) {
            size_t const count = remaining < sizeof(transport.zeros) ? remaining : sizeof(transport.zeros);
            if (!write_all(files[i], transport.zeros, count)) return false;
            remaining -= count;
        }
        if (::fsync(files[i]) != 0 || ::lseek(files[i], 0, SEEK_SET) != 0) return false;
    }
    return true;
}
struct Child {
    int64_t pid{};
    bool owned{};
    auto poll(int32_t& status) -> int64_t {
        auto const result = ker::process::waitpid(pid, &status, WNOHANG, nullptr);
        if (result == pid || result == -ECHILD) owned = false;
        return result;
    }
    ~Child() {
        if (!owned) return;
        (void)ker::process::kill(pid, SIGKILL);
        auto const start = now_ns();
        if (start == UINT64_MAX || start > UINT64_MAX - 5'000'000'000) return;
        while (owned) {
            int32_t status{};
            auto const result = poll(status);
            if (!owned || (result != 0 && result != -EINTR)) return;
            auto const now = now_ns();
            if (now == UINT64_MAX || now >= start + 5'000'000'000 || !pause_poll()) return;
        }
    }
};
[[noreturn]] void run_child(int log, uint64_t parent, const char* seed) {
    if (ker::process::getppid() != parent || ::dup2(log, STDOUT_FILENO) != STDOUT_FILENO || ::dup2(log, STDERR_FILENO) != STDERR_FILENO ||
        ::setgroups(0, nullptr) != 0 || ::setgid(100) != 0 || ::setuid(100) != 0 || control(Control::DROP_AUTHORITY_CEILING, 0) != 0 ||
        !identity(100, 0))
        ::_exit(71);
    rlimit const limit{.rlim_cur = LOG_BYTES, .rlim_max = LOG_BYTES};
    if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) ::_exit(71);
    const char* const arguments[]{PROBE, "numa-lifecycle", "--cycles", "1000", "--seed", seed, "--timeout-ms", "350000", nullptr};
    const char* const environment[]{"LD_LIBRARY_PATH=/usr/lib", nullptr};
    (void)ker::process::execve(PROBE, arguments, environment);
    ::_exit(72);
}
auto child(unsigned index, uint64_t parent, const char* seed, int64_t& pid_out) -> bool {
    auto const pid = ker::process::fork();
    if (pid < 0) return false;
    if (pid == 0) run_child(files[index], parent, seed);
    if (pid <= 1 || static_cast<uint64_t>(pid) == parent) return false;
    Child owned{pid, true};
    pid_out = pid;
    for (;;) {
        int32_t status = -1;
        auto const result = owned.poll(status);
        if (result == pid) {
            if (status != 0) return false;
            prefault_transport();  // No census/usercopy pin yet.
            return true;
        }
        if ((result != 0 && result != -EINTR) || !time_left() || !pause_poll()) return false;
    }
}
auto poll_census(nl::Phase phase) -> bool {
    while (time_left()) {
        transport.request = {.version = nl::VERSION,
                             .bytes = sizeof(nl::Request),
                             .phase = static_cast<uint32_t>(phase),
                             .flags = 0,
                             .cookie = cookie,
                             .nonce = nonce,
                             .output_address = reinterpret_cast<uint64_t>(&transport.output),
                             .output_bytes = sizeof(nl::Report),
                             .reserved = {0, 0}};
        auto const result = static_cast<int64_t>(::syscall(static_cast<long>(ker::abi::callnums::vmem), nl::PRIVATE_VMEM_OP,
                                                           reinterpret_cast<uint64_t>(&transport.request), sizeof(nl::Request), 0, 0));
        auto const& h = transport.output.header;
        if (result != 0 || h.version != nl::VERSION || h.bytes != sizeof(nl::Report) || h.phase != static_cast<uint32_t>(phase) ||
            h.nonce != nonce || h.cookie == 0 || (cookie != 0 && h.cookie != cookie) ||
            h.required_coverage != nl::KERNEL_COVERAGE_REQUIRED || h.reserved != 0 || h.root_incarnation == 0 || h.calls == 0 ||
            h.capture_us >= h.absolute_deadline_us)
            return false;
        cookie = h.cookie;
        if (h.outcome == static_cast<uint64_t>(nl::Outcome::ACCEPT))
            return h.reason == 0 && h.coverage == nl::KERNEL_COVERAGE_REQUIRED && h.node_count >= 1 && h.node_count <= nl::MAX_NODES;
        if (h.outcome != static_cast<uint64_t>(nl::Outcome::WAIT) || h.coverage != 0 || !pause_poll()) return false;
    }
    return false;
}
auto save_record(int fd, const nl::Report& value) -> bool {
    return ::lseek(fd, 0, SEEK_SET) == 0 && write_all(fd, &value, sizeof(value)) && ::fsync(fd) == 0;
}
auto decimal(const char* text, uint64_t& value) -> bool {
    value = 0;
    if (text == nullptr || *text < '1' || *text > '9') return false;
    for (; *text != 0; ++text) {
        if (*text < '0' || *text > '9' || value > (UINT64_MAX - static_cast<unsigned>(*text - '0')) / 10) return false;
        value = value * 10 + static_cast<unsigned>(*text - '0');
    }
    return true;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc != 3 || std::strcmp(argv[1], "--nonce") != 0 || !decimal(argv[2], nonce) || ker::process::getppid() != 1 ||
        !identity(0, INITIAL))
        return 70;
    auto const parent = ker::process::getpid();
    auto const start = now_ns();
    if (parent <= 1 || parent > INT64_MAX || start == UINT64_MAX || start > UINT64_MAX - WHOLE_NS) return 70;
    deadline_ns = start + WHOLE_NS;
    (void)::umask(0077);
    prefault_transport();
    if (!prepare_files() || !poll_census(nl::Phase::PREPARE)) return 73;
    int64_t warm_pid{}, measured_pid{};
    if (!child(0, parent, argv[2], warm_pid) || !poll_census(nl::Phase::BEGIN)) return 74;
    transport.begin = transport.output;
    if (!child(1, parent, argv[2], measured_pid) || !poll_census(nl::Phase::END)) return 75;
    transport.end = transport.output;
    if (!identity(0, INITIAL) || ker::process::getppid() != 1 || !save_record(files[2], transport.begin) ||
        !save_record(files[3], transport.end))
        return 76;
    char record[512]{};
    auto const length = std::snprintf(
        record, sizeof(record),
        "NUMA_CENSUS_WITNESS version=1 nonce=%llu cookie=%llu pid=%llu warm_pid=%lld warm_cycles=1000 warm_wait0=1 "
        "measured_pid=%lld measured_cycles=1000 measured_wait0=1 kernel_coverage=255 external_work_wait=1 combined_coverage=511\n",
        static_cast<unsigned long long>(nonce), static_cast<unsigned long long>(cookie), static_cast<unsigned long long>(parent),
        static_cast<long long>(warm_pid), static_cast<long long>(measured_pid));
    if (length <= 0 || static_cast<size_t>(length) >= sizeof(record) || !write_all(files[4], record, static_cast<size_t>(length)) ||
        ::fsync(files[4]) != 0)
        return 76;
    for (auto& fd : files) {
        if (::close(fd) != 0) return 76;
        fd = -1;
    }
    if (control(Control::DROP_AUTHORITY_CEILING, SIGNAL) != 0 || !identity(0, SIGNAL) || ker::process::getppid() != 1 ||
        ker::process::kill(1, SIGUSR2) != 0)
        return 77;
    return 0;  // Original init must separately consume0; admission is not poweroff.
}
