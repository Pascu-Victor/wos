#include "services.h"

#include <abi-bits/resource.h>
#include <bits/ssize_t.h>
#include <callnums/sys_log.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX sleep declarations live here.
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <span>

#include "env.h"
#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
constexpr int INTERACTIVE_SERVICE_NICE = -5;
constexpr size_t PIPE_READ = 0;
constexpr size_t PIPE_WRITE = 1;
constexpr uint32_t DROPBEAR_KEYGEN_TIMEOUT_MS = 30000;
constexpr uint32_t CHILD_WAIT_POLL_US = 1000;
constexpr uint64_t NSEC_PER_MSEC = 1000000ULL;
constexpr uint32_t SERVICE_TERM_TIMEOUT_MS = 2000;
constexpr uint32_t SERVICE_KILL_TIMEOUT_MS = 1000;
constexpr size_t MAX_TRACKED_SERVICES = 16;
constexpr uint32_t INIT_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT;
constexpr uint32_t SERVICE_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL;
using init_log = wos::journal<"init">;

struct ServiceEntry {
    const char* name{};
    uint64_t pid{};
    ServiceKind kind{};
    bool active{};
};

std::array<ServiceEntry, MAX_TRACKED_SERVICES> tracked_services{};

auto monotonic_ns() -> uint64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL) + static_cast<uint64_t>(ts.tv_nsec);
}

void reap_child_after_timeout(int64_t pid) {
    (void)ker::process::kill(pid, SIGKILL);
    for (uint32_t retry = 0; retry < 1000; ++retry) {
        int32_t reap_status = 0;
        int64_t const REAPED = ker::process::waitpid(pid, &reap_status, WNOHANG, nullptr);
        if (REAPED == pid || (REAPED < 0 && REAPED != -EINTR)) {
            return;
        }
        usleep(CHILD_WAIT_POLL_US);
    }
}

auto wait_for_child_timeout(int64_t pid, int32_t* status, uint32_t timeout_ms) -> bool {
    uint64_t const START_NS = monotonic_ns();
    uint64_t const TIMEOUT_NS = static_cast<uint64_t>(timeout_ms) * NSEC_PER_MSEC;
    uint64_t waited_us = 0;

    while ((START_NS != 0 && monotonic_ns() - START_NS <= TIMEOUT_NS) ||
           (START_NS == 0 && waited_us <= static_cast<uint64_t>(timeout_ms) * 1000ULL)) {
        int64_t const WAITED = ker::process::waitpid(pid, status, WNOHANG, nullptr);
        if (WAITED == pid) {
            return true;
        }
        if (WAITED < 0 && WAITED != -EINTR) {
            return false;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    reap_child_after_timeout(pid);
    return false;
}

auto wait_for_child_exit(int64_t pid, int32_t* status, uint32_t timeout_ms) -> bool {
    uint64_t const START_NS = monotonic_ns();
    uint64_t const TIMEOUT_NS = static_cast<uint64_t>(timeout_ms) * NSEC_PER_MSEC;
    uint64_t waited_us = 0;

    while ((START_NS != 0 && monotonic_ns() - START_NS <= TIMEOUT_NS) ||
           (START_NS == 0 && waited_us <= static_cast<uint64_t>(timeout_ms) * 1000ULL)) {
        int64_t const WAITED = ker::process::waitpid(pid, status, WNOHANG, nullptr);
        if (WAITED == pid) {
            return true;
        }
        if (WAITED < 0 && WAITED != -EINTR) {
            return true;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    return false;
}

auto fork_local_service_child() -> int64_t {
    (void)ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS);
    int64_t const PID = ker::process::fork();
    if (PID != 0) {
        (void)ker::process::setwkitarget(nullptr, 0, INIT_WKI_TARGET_FLAGS);
    }
    return PID;
}

}  // namespace

void register_service(const char* name, uint64_t pid, ServiceKind kind) {
    if (pid == 0) {
        return;
    }
    for (auto& service : tracked_services) {
        if (!service.active) {
            service.name = name;
            service.pid = pid;
            service.kind = kind;
            service.active = true;
            return;
        }
    }
    init_log::warn("init: service registry full; not tracking %s pid %llu", name != nullptr ? name : "?",
                   static_cast<unsigned long long>(pid));
}

void note_service_reaped(uint64_t pid) {
    if (pid == 0) {
        return;
    }
    for (auto& service : tracked_services) {
        if (service.active && service.pid == pid) {
            service.active = false;
            return;
        }
    }
}

namespace {

void stop_one_service(ServiceEntry& service) {
    if (!service.active || service.pid == 0) {
        return;
    }
    init_log::info("init: stopping %s pid %llu", service.name != nullptr ? service.name : "service",
                   static_cast<unsigned long long>(service.pid));
    (void)ker::process::kill(static_cast<int64_t>(service.pid), SIGTERM);
    int32_t status = 0;
    if (!wait_for_child_exit(static_cast<int64_t>(service.pid), &status, SERVICE_TERM_TIMEOUT_MS)) {
        init_log::warn("init: %s pid %llu did not stop after SIGTERM; sending SIGKILL", service.name != nullptr ? service.name : "service",
                       static_cast<unsigned long long>(service.pid));
        (void)ker::process::kill(static_cast<int64_t>(service.pid), SIGKILL);
        (void)wait_for_child_timeout(static_cast<int64_t>(service.pid), &status, SERVICE_KILL_TIMEOUT_MS);
    }
    service.active = false;
}

void stop_services_by_kind(ServiceKind kind) {
    for (auto& service : tracked_services) {
        if (service.active && service.kind == kind) {
            stop_one_service(service);
        }
    }
}

}  // namespace

void stop_services_for_shutdown() {
    stop_services_by_kind(ServiceKind::NETWORK);
    stop_services_by_kind(ServiceKind::NORMAL);
}

void stop_journald_for_shutdown() { stop_services_by_kind(ServiceKind::JOURNAL); }

auto spawn_local_service(const char* path, const char* const* argv, const char* const* envp) -> uint64_t {
    int64_t const PID = fork_local_service_child();
    if (PID < 0) {
        return 0;
    }
    if (PID == 0) {
        ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS);
        ker::process::execve(path, argv, envp);
        ker::process::exit(127);
        __builtin_unreachable();
    }
    return static_cast<uint64_t>(PID);
}

namespace {

// Spawn a process with stdout and stderr captured to the journal under `tag`.
// A drain child reads lines from the pipe and emits them as INFO journal entries.
// Returns the service PID, or 0 on failure.
template <size_t Argc, size_t Envc>
auto spawn_with_journal_stdio(const char* path, const std::array<const char*, Argc>& argv, const std::array<const char*, Envc>& envp,
                              const char* tag) -> uint64_t {
    std::array<int, 2> pipefd{};
    if (::pipe(pipefd.data()) != 0) {
        return 0;
    }

    int64_t const SERVICE_PID = fork_local_service_child();
    if (SERVICE_PID < 0) {
        ::close(pipefd.at(PIPE_READ));
        ::close(pipefd.at(PIPE_WRITE));
        return 0;
    }
    if (SERVICE_PID == 0) {
        // Service child: redirect stdout and stderr to the write end, then exec.
        ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS);
        ::dup2(pipefd.at(PIPE_WRITE), STDOUT_FILENO);
        ::dup2(pipefd.at(PIPE_WRITE), STDERR_FILENO);
        ::close(pipefd.at(PIPE_READ));
        ::close(pipefd.at(PIPE_WRITE));
        ker::process::execve(path, argv.data(), envp.data());
        ker::process::exit(127);
        __builtin_unreachable();
    }

    // Parent: close write end so the drain child sees EOF when the service exits.
    ::close(pipefd.at(PIPE_WRITE));

    int64_t const DRAIN_PID = fork_local_service_child();
    if (DRAIN_PID < 0) {
        ::close(pipefd.at(PIPE_READ));
        return static_cast<uint64_t>(SERVICE_PID);
    }
    if (DRAIN_PID == 0) {
        // Drain child: read lines from the pipe and emit each to the journal.
        std::array<char, 512> buf{};
        std::array<char, 512> line{};
        size_t line_len = 0;
        ssize_t n = 0;
        while ((n = ::read(pipefd.at(PIPE_READ), buf.data(), buf.size())) > 0) {
            for (char const C : std::span<const char>(buf.data(), static_cast<size_t>(n))) {
                if (C == '\n' || C == '\r') {
                    if (line_len > 0) {
                        line.at(line_len) = '\0';
                        ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::INFO, line.data(), static_cast<uint64_t>(line_len));
                        line_len = 0;
                    }
                } else if (line_len < line.size() - 1) {
                    line.at(line_len++) = C;
                }
            }
        }
        // Flush any partial line without trailing newline.
        if (line_len > 0) {
            line.at(line_len) = '\0';
            ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::INFO, line.data(), static_cast<uint64_t>(line_len));
        }
        ::close(pipefd.at(PIPE_READ));
        ker::process::exit(0);
        __builtin_unreachable();
    }

    // Parent: drain child owns the read end.
    ::close(pipefd.at(PIPE_READ));
    return static_cast<uint64_t>(SERVICE_PID);
}

}  // namespace

void start_journald() {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%llu]: spawning journald", static_cast<unsigned long long>(CPUNO));
    std::array<const char*, 2> argv = {"/sbin/journald", nullptr};
    InitEnv env = make_init_env();
    uint64_t const PID = spawn_local_service("/sbin/journald", argv.data(), env.envp.data());
    if (PID == 0) {
        init_log::warn("init[%llu]: failed to spawn journald", static_cast<unsigned long long>(CPUNO));
    } else {
        register_service("journald", PID, ServiceKind::JOURNAL);
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%llu]: journald spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%llu]: failed to lower journald priority (%lld)", static_cast<unsigned long long>(CPUNO),
                           static_cast<long long>(PRIO_RC));
        }
    }
}

void start_httpd() {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%llu]: spawning httpd (HTTP server on port 80)", static_cast<unsigned long long>(CPUNO));
    std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
    InitEnv httpd_env = make_init_env();
    uint64_t const HTTPD_PID = spawn_local_service("/sbin/httpd", httpd_argv.data(), httpd_env.envp.data());
    if (HTTPD_PID == 0) {
        init_log::error("init[%llu]: failed to spawn httpd", static_cast<unsigned long long>(CPUNO));
    } else {
        register_service("httpd", HTTPD_PID, ServiceKind::NETWORK);
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(HTTPD_PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%llu]: httpd spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(HTTPD_PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%llu]: failed to lower httpd priority (%lld)", static_cast<unsigned long long>(CPUNO),
                           static_cast<long long>(PRIO_RC));
        }
    }
}

void start_dropbear() {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();

    // Generate RSA host key if it doesn't exist
    int const KEY_FD = ker::abi::vfs::open("/etc/dropbear/dropbear_rsa_host_key", 0, 0);
    if (KEY_FD >= 0) {
        ker::abi::vfs::close(KEY_FD);
        init_log::info("init[%llu]: dropbear host key already exists", static_cast<unsigned long long>(CPUNO));
    } else {
        init_log::info("init[%llu]: generating dropbear RSA host key...", static_cast<unsigned long long>(CPUNO));
        const std::array<const char*, 6> KEYGEN_ARGV = {
            "/bin/dropbearkey", "-t", "rsa", "-f", "/etc/dropbear/dropbear_rsa_host_key", nullptr};
        InitEnv keygen_env = make_init_env();
        uint64_t const KEYGEN_PID = spawn_with_journal_stdio("/bin/dropbearkey", KEYGEN_ARGV, keygen_env.envp, "sshd");
        if (KEYGEN_PID == 0) {
            init_log::error("init[%llu]: failed to spawn dropbearkey", static_cast<unsigned long long>(CPUNO));
        } else {
            int32_t exit_code = 0;
            bool const KEYGEN_EXITED = wait_for_child_timeout(static_cast<int64_t>(KEYGEN_PID), &exit_code, DROPBEAR_KEYGEN_TIMEOUT_MS);
            if (KEYGEN_EXITED) {
                init_log::info("init[%llu]: dropbearkey exited with code %d", static_cast<unsigned long long>(CPUNO), exit_code);
            } else {
                init_log::warn("init[%llu]: dropbearkey did not exit within %ums; continuing boot", static_cast<unsigned long long>(CPUNO),
                               DROPBEAR_KEYGEN_TIMEOUT_MS);
            }
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    init_log::info("init[%llu]: spawning dropbear SSH server", static_cast<unsigned long long>(CPUNO));
    const std::array<const char*, 5> DROPBEAR_ARGV = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                      "-F",  // foreground, don't fork
                                                      nullptr};
    InitEnv dropbear_env = make_init_env();
    uint64_t const DROPBEAR_PID = spawn_with_journal_stdio("/bin/dropbear", DROPBEAR_ARGV, dropbear_env.envp, "sshd");
    if (DROPBEAR_PID == 0) {
        init_log::error("init[%llu]: failed to spawn dropbear", static_cast<unsigned long long>(CPUNO));
    } else {
        register_service("dropbear", DROPBEAR_PID, ServiceKind::NETWORK);
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(DROPBEAR_PID), INTERACTIVE_SERVICE_NICE);
        init_log::info("init[%llu]: dropbear spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(DROPBEAR_PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%llu]: failed to raise dropbear priority (%lld)", static_cast<unsigned long long>(CPUNO),
                           static_cast<long long>(PRIO_RC));
        }
    }
}

void start_testd() {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();

    // Only spawn testd if the binary is present (development/test builds only).
    int const PROBE_FD = ker::abi::vfs::open("/usr/bin/testd", 0, 0);
    if (PROBE_FD < 0) {
        return;
    }
    ker::abi::vfs::close(PROBE_FD);

    // Wait for all previously spawned services (netd, httpd, dropbear) to finish
    // initializing. There are no service-ready signals yet, so a fixed delay is used.
    init_log::info("init[%llu]: testd: waiting 10s for services to settle...", static_cast<unsigned long long>(CPUNO));
    struct timespec const SETTLE{.tv_sec = 10, .tv_nsec = 0};
    nanosleep(&SETTLE, nullptr);

    init_log::info("init[%llu]: spawning testd (kernel test daemon)", static_cast<unsigned long long>(CPUNO));
    std::array<const char*, 2> argv = {"/usr/bin/testd", nullptr};
    InitEnv env = make_init_env();
    uint64_t const PID = spawn_local_service("/usr/bin/testd", argv.data(), env.envp.data());
    if (PID == 0) {
        init_log::warn("init[%llu]: failed to spawn testd", static_cast<unsigned long long>(CPUNO));
    } else {
        register_service("testd", PID, ServiceKind::NORMAL);
        init_log::info("init[%llu]: testd spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(PID));
    }
}
