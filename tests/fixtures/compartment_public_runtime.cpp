// Real WOS userspace qualification; Host and private service use the same ELF.
#include <fcntl.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/resource.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
namespace wire = ker::abi::compartment;
constexpr int RESULT_FD = 9;
constexpr uint64_t MAGIC = 0x39335055424c4943ULL;
constexpr const char* FIXTURE = "/usr/libexec/compartment-public-runtime";
constexpr const char* STATUS = "/srv/compartment-results/status";
// Keep a bounded guest receipt as well as init's journal-backed stdout. A
// missing journal message must not turn an ordinary refusal into a boot stall.
struct log {
    template <typename... Args>
    static void info(const char* format, Args... args) {
        char line[512]{};
        int const COUNT = std::snprintf(line, sizeof(line), format, args...);
        if (COUNT < 0 || static_cast<size_t>(COUNT) >= sizeof(line) - 1) return;
        line[COUNT] = '\n';
        size_t const BYTES = static_cast<size_t>(COUNT) + 1;
        (void)ker::logging::logEx("compartment-test", ker::abi::sys_log::sys_log_level::INFO, line, BYTES);
        (void)write(STDOUT_FILENO, line, BYTES);
        int const FD = open(STATUS, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
        if (FD >= 0) {
            (void)write(FD, line, BYTES);
            (void)fsync(FD);
            close(FD);
        }
    }
    template <typename... Args>
    static void error(const char* format, Args... args) {
        info(format, args...);
    }
};
struct Report {
    uint64_t magic{}, pid{}, parent{}, uid{}, checks{}, failures{}, failed_checks{};
};
auto text(const char* value) -> wire::String { return {reinterpret_cast<uint64_t>(value), std::strlen(value)}; }
auto parse_host_pid(const char* text, int64_t& result) -> bool {
    uint64_t value{};
    for (size_t i = 0; i < 20; ++i) {
        char const DIGIT = text[i];
        if (DIGIT == '\0') {
            if (i == 0 || value <= 2) return false;
            result = static_cast<int64_t>(value);
            return true;
        }
        if (DIGIT < '0' || DIGIT > '9') return false;
        auto const NUMBER = static_cast<uint64_t>(DIGIT - '0');
        if (value > (uint64_t{INT64_MAX} - NUMBER) / 10) return false;
        value = value * 10 + NUMBER;
    }
    return false;
}
auto wait(int64_t pid, int& status) -> int64_t {
    int64_t result;
    do {
        result = ker::process::waitpid(pid, &status, 0, nullptr);
    } while (result == -EINTR);
    return result;
}
auto service(int64_t host_pid) -> int {
    Report report{MAGIC, ker::process::getpid(), ker::process::getppid(), getuid()};
    auto expect = [&](bool condition) {
        ++report.checks;
        if (!condition) {
            ++report.failures;
            report.failed_checks |= uint64_t{1} << (report.checks - 1);
        }
    };
    expect(report.pid == 1);
    expect(report.parent == 0);
    expect(report.uid == 100 && geteuid() == 100 && getgid() == 100 && getegid() == 100);
    for (const char* path : {"/etc/passwd", "../../etc/passwd", "/app/../../../etc/passwd"}) {
        errno = 0;
        int const FD = open(path, O_RDONLY);
        expect(FD == -1 && errno == ENOENT);
        if (FD >= 0) close(FD);
    }
    int const MARKER = open("/same/name", O_RDONLY);
    expect(MARKER >= 0);
    if (MARKER >= 0) {
        char value[32]{};
        auto const COUNT = read(MARKER, value, sizeof(value));
        expect(COUNT == 13 && std::memcmp(value, "private-root\n", 13) == 0);
        close(MARKER);
    }
    errno = 0;
    int const SOCKET = socket(AF_INET, SOCK_STREAM, 0);
    expect(SOCKET == -1 && errno == EPERM);
    if (SOCKET >= 0) close(SOCKET);
    int const SHM = shmget(0x3933, 4096, IPC_CREAT | IPC_EXCL | 0600);
    expect(SHM >= 0);
    if (SHM >= 0) expect(shmctl(SHM, IPC_RMID, nullptr) == 0);
    int64_t const CHILD = ker::process::fork();
    if (CHILD == 0) {
        bool const IDENTITY = ker::process::getpid() == 2 && ker::process::getppid() == 1 && getuid() == 100;
        ker::process::exit(IDENTITY ? 0 : 31);
        __builtin_unreachable();
    }
    expect(CHILD == 2);
    if (CHILD > 0) {
        int status{};
        expect(wait(CHILD, status) == CHILD && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    errno = 0;
    expect(getpriority(PRIO_PROCESS, 0) == 0 && errno == 0);
    errno = 0;
    expect(getpriority(PRIO_PROCESS, 1) == 0 && errno == 0);
    errno = 0;
    expect(setpriority(PRIO_PROCESS, 0, 5) == 0 && errno == 0);
    errno = 0;
    expect(getpriority(PRIO_PROCESS, 1) == 5 && errno == 0);
    errno = 0;
    expect(setpriority(PRIO_PROCESS, 0, 4) == -1 && errno == EACCES);
    // The decimal Host PID is a guessed name, not an object grant. It is >2;
    // only local PID1 and the completed child PID2 exist in this namespace.
    // Raw wrappers preserve the full physical ID and return negative errno.
    expect(ker::process::getpriority(PRIO_PROCESS, host_pid) == -ESRCH);
    expect(ker::process::setpriority(PRIO_PROCESS, host_pid, 5) == -ESRCH);
    // This deliberately granted Host File lies outside the private path view.
    ++report.checks;
    if (write(RESULT_FD, &report, sizeof(report)) != static_cast<ssize_t>(sizeof(report))) return 33;
    return report.failures == 0 ? 0 : 32;
}
auto host() -> int {
    auto const HOST_PID = ker::process::getpid();
    log::info("BEGIN public-service uid=%u host-pid=%llu", getuid(), static_cast<unsigned long long>(HOST_PID));
    if (getuid() != 100 || HOST_PID <= 2 || HOST_PID > uint64_t{INT64_MAX} ||
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL) != 0)
        return 40;
    char host_pid[24];
    std::snprintf(host_pid, sizeof(host_pid), "%llu", static_cast<unsigned long long>(HOST_PID));
    constexpr std::array SOURCE{FIXTURE, "/lib/ld.so", "/usr/lib/libc.so", "/usr/lib/libjournal.so", "/srv/compartment-marker"};
    constexpr std::array DESTINATION{"/app/probe", "/lib/ld.so", "/usr/lib/libc.so", "/usr/lib/libjournal.so", "/same/name"};
    std::array<wire::Asset, SOURCE.size()> assets{};
    for (size_t i = 0; i < SOURCE.size(); ++i) {
        int const FD = open(SOURCE[i], O_RDONLY | O_CLOEXEC);
        if (FD < 0) {
            log::error("FAIL asset index=%zu errno=%d", i, errno);
            for (size_t j = 0; j < i; ++j) close(assets[j].source_fd);
            return 41;
        }
        assets[i] = {FD, i < 2 ? 0755U : 0644U, text(DESTINATION[i]), 0};
    }
    unsigned failures{};
    for (unsigned mode = 0; mode < 2; ++mode) {
        errno = 0;
        int const NICE_BEFORE = getpriority(PRIO_PROCESS, 0);
        bool const NICE_BEFORE_VALID = errno == 0;
        char path[80];
        std::snprintf(path, sizeof(path), "/srv/compartment-results/result-%u", mode);
        int const FD = open(path, O_CREAT | O_EXCL | O_RDWR, 0600);
        if (FD < 0) {
            log::error("FAIL result open mode=%u errno=%d", mode, errno);
            ++failures;
            continue;
        }
        int64_t pid = -1;
        if (mode == 0) {
            wire::Spec spec;
            for (size_t i = 0; i < spec.limits.size(); ++i) {
                auto const RESULT = static_cast<int64_t>(
                    syscall(ker::abi::callnums::process, uint64_t{48}, i, reinterpret_cast<uint64_t>(&spec.limits[i]), uint64_t{1}));
                if (RESULT < 0) {
                    log::error("FAIL limit id=%zu result=%lld", i, static_cast<long long>(RESULT));
                    ++failures;
                }
            }
            wire::Descriptor descriptor{FD, RESULT_FD, 0};
            std::array args{text("/app/probe"), text("--service"), text("--host-pid"), text(host_pid)};
            std::array env{text("LD_LIBRARY_PATH=/usr/lib")};
            spec.executable = args[0];
            spec.assets = reinterpret_cast<uint64_t>(assets.data());
            spec.asset_count = assets.size();
            spec.descriptors = reinterpret_cast<uint64_t>(&descriptor);
            spec.descriptor_count = 1;
            spec.argv = reinterpret_cast<uint64_t>(args.data());
            spec.argc = args.size();
            spec.envp = reinterpret_cast<uint64_t>(env.data());
            spec.envc = env.size();
            pid = static_cast<int64_t>(syscall(ker::abi::callnums::process, wire::CREATE_OPERATION, reinterpret_cast<uint64_t>(&spec)));
        } else {
            char descriptor[24];
            std::snprintf(descriptor, sizeof(descriptor), "%d", FD);
            std::array args{"/usr/bin/wos-compartment",
                            "--asset",
                            FIXTURE,
                            "/app/probe",
                            "0755",
                            "--asset",
                            "/lib/ld.so",
                            "/lib/ld.so",
                            "0755",
                            "--asset",
                            "/usr/lib/libc.so",
                            "/usr/lib/libc.so",
                            "0644",
                            "--asset",
                            "/usr/lib/libjournal.so",
                            "/usr/lib/libjournal.so",
                            "0644",
                            "--asset",
                            "/srv/compartment-marker",
                            "/same/name",
                            "0644",
                            "--fd",
                            static_cast<const char*>(descriptor),
                            "9",
                            "--env",
                            "LD_LIBRARY_PATH=/usr/lib",
                            "--",
                            "/app/probe",
                            "--service",
                            "--host-pid",
                            static_cast<const char*>(host_pid),
                            static_cast<const char*>(nullptr)};
            std::array<const char*, 2> env{"LD_LIBRARY_PATH=/usr/lib", nullptr};
            pid = static_cast<int64_t>(ker::process::exec(args[0], args.data(), env.data()));
        }
        int status{};
        bool const WAITED = pid > 0 && wait(pid, status) == pid;
        bool const EXITED = WAITED && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        Report report{};
        bool const READ = pread(FD, &report, sizeof(report), 0) == static_cast<ssize_t>(sizeof(report));
        errno = 0;
        int const NICE_AFTER = getpriority(PRIO_PROCESS, 0);
        bool const NICE_PRESERVED = NICE_BEFORE_VALID && errno == 0 && NICE_AFTER == NICE_BEFORE;
        bool const PASSED = EXITED && READ && report.magic == MAGIC && report.pid == 1 && report.parent == 0 && report.uid == 100 &&
                            report.checks == 21 && report.failures == 0 && NICE_PRESERVED;
        log::info(
            "%s mode=%u physical-pid=%lld waited=%u status=%d report=%u checks=%llu failures=%llu failed-mask=0x%llx "
            "host-nice-before=%d host-nice-after=%d host-nice-preserved=%u",
            PASSED ? "PASS" : "FAIL", mode, static_cast<long long>(pid), WAITED ? 1U : 0U, status, READ ? 1U : 0U,
            static_cast<unsigned long long>(report.checks), static_cast<unsigned long long>(report.failures),
            static_cast<unsigned long long>(report.failed_checks), NICE_BEFORE, NICE_AFTER, NICE_PRESERVED ? 1U : 0U);
        if (!PASSED) ++failures;
        (void)fsync(FD);
        close(FD);
    }
    for (auto const& asset : assets) close(asset.source_fd);
    log::info("%s public-service modes=2 failures=%u", failures == 0 ? "PASS" : "FAIL", failures);
    return failures == 0 ? 0 : 42;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    int64_t host_pid{};
    if (argc == 4 && std::strcmp(argv[1], "--service") == 0 && std::strcmp(argv[2], "--host-pid") == 0 && parse_host_pid(argv[3], host_pid))
        return service(host_pid);
    if (argc == 2 && std::strcmp(argv[1], "--host") == 0) return host();
    return 43;
}
