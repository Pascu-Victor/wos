#include "services.h"

#include <abi-bits/resource.h>
#include <bits/ssize_t.h>
#include <callnums/sys_log.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX sleep declarations live here.
#include <unistd.h>

#include <array>
#include <cstdint>
#include <ctime>
#include <span>

#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
constexpr size_t PIPE_READ = 0;
constexpr size_t PIPE_WRITE = 1;
using init_log = wos::journal<"init">;

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

    int64_t const SERVICE_PID = ker::process::fork();
    if (SERVICE_PID < 0) {
        ::close(pipefd.at(PIPE_READ));
        ::close(pipefd.at(PIPE_WRITE));
        return 0;
    }
    if (SERVICE_PID == 0) {
        // Service child: redirect stdout and stderr to the write end, then exec.
        // Root init uses NOINHERIT, so forked service children start with automatic
        // WKI placement. Re-pin daemons locally before exec; login/session
        // boundaries can opt their own descendants back into automatic placement.
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
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

    int64_t const DRAIN_PID = ker::process::fork();
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
    std::array<const char*, 1> envp = {nullptr};
    uint64_t const PID = ker::process::exec("/sbin/journald", argv.data(), envp.data());
    if (PID == 0) {
        init_log::warn("init[%llu]: failed to spawn journald", static_cast<unsigned long long>(CPUNO));
    } else {
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
    std::array<const char*, 1> httpd_envp = {nullptr};
    uint64_t const HTTPD_PID = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
    if (HTTPD_PID == 0) {
        init_log::error("init[%llu]: failed to spawn httpd", static_cast<unsigned long long>(CPUNO));
    } else {
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
        const std::array<const char*, 1> KEYGEN_ENVP = {nullptr};
        uint64_t const KEYGEN_PID = spawn_with_journal_stdio("/bin/dropbearkey", KEYGEN_ARGV, KEYGEN_ENVP, "sshd");
        if (KEYGEN_PID == 0) {
            init_log::error("init[%llu]: failed to spawn dropbearkey", static_cast<unsigned long long>(CPUNO));
        } else {
            int exit_code = 0;
            ker::process::waitpid(static_cast<int64_t>(KEYGEN_PID), &exit_code, 0, nullptr);
            init_log::info("init[%llu]: dropbearkey exited with code %d", static_cast<unsigned long long>(CPUNO), exit_code);
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    init_log::info("init[%llu]: spawning dropbear SSH server", static_cast<unsigned long long>(CPUNO));
    const std::array<const char*, 5> DROPBEAR_ARGV = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                      "-F",  // foreground, don't fork
                                                      nullptr};
    const std::array<const char*, 1> DROPBEAR_ENVP = {nullptr};
    uint64_t const DROPBEAR_PID = spawn_with_journal_stdio("/bin/dropbear", DROPBEAR_ARGV, DROPBEAR_ENVP, "sshd");
    if (DROPBEAR_PID == 0) {
        init_log::error("init[%llu]: failed to spawn dropbear", static_cast<unsigned long long>(CPUNO));
    } else {
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(DROPBEAR_PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%llu]: dropbear spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(DROPBEAR_PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%llu]: failed to lower dropbear priority (%lld)", static_cast<unsigned long long>(CPUNO),
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
    std::array<const char*, 1> envp = {nullptr};
    uint64_t const PID = ker::process::exec("/usr/bin/testd", argv.data(), envp.data());
    if (PID == 0) {
        init_log::warn("init[%llu]: failed to spawn testd", static_cast<unsigned long long>(CPUNO));
    } else {
        init_log::info("init[%llu]: testd spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                       static_cast<unsigned long long>(PID));
    }
}
