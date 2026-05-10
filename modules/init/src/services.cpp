#include "services.h"

#include <abi-bits/resource.h>
#include <bits/ssize_t.h>
#include <callnums/sys_log.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <ctime>

#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
using init_log = wos::journal<"init">;

// Spawn a process with stdout and stderr captured to the journal under `tag`.
// A drain child reads lines from the pipe and emits them as INFO journal entries.
// Returns the service PID, or 0 on failure.
uint64_t spawn_with_journal_stdio(const char* path, const char* const argv[], const char* const envp[], const char* tag) {
    int pipefd[2];
    if (::pipe(pipefd) != 0) {
        return 0;
    }

    int64_t const SERVICE_PID = ker::process::fork();
    if (SERVICE_PID < 0) {
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        return 0;
    }
    if (SERVICE_PID == 0) {
        // Service child: redirect stdout and stderr to the write end, then exec.
        // Root init uses NOINHERIT, so forked service children start with automatic
        // WKI placement. Re-pin daemons locally before exec; login/session
        // boundaries can opt their own descendants back into automatic placement.
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
        ::dup2(pipefd[1], STDOUT_FILENO);
        ::dup2(pipefd[1], STDERR_FILENO);
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        ker::process::execve(path, argv, envp);
        ker::process::exit(127);
        __builtin_unreachable();
    }

    // Parent: close write end so the drain child sees EOF when the service exits.
    ::close(pipefd[1]);

    int64_t const DRAIN_PID = ker::process::fork();
    if (DRAIN_PID < 0) {
        ::close(pipefd[0]);
        return static_cast<uint64_t>(SERVICE_PID);
    }
    if (DRAIN_PID == 0) {
        // Drain child: read lines from the pipe and emit each to the journal.
        char buf[512];
        char line[512];
        int line_len = 0;
        ssize_t n = 0;
        while ((n = ::read(pipefd[0], buf, sizeof(buf))) > 0) {
            for (ssize_t i = 0; i < n; i++) {
                char const C = buf[i];
                if (C == '\n' || C == '\r') {
                    if (line_len > 0) {
                        line[line_len] = '\0';
                        ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::INFO, line, static_cast<uint64_t>(line_len));
                        line_len = 0;
                    }
                } else if (line_len < static_cast<int>(sizeof(line)) - 1) {
                    line[line_len++] = C;
                }
            }
        }
        // Flush any partial line without trailing newline.
        if (line_len > 0) {
            line[line_len] = '\0';
            ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::INFO, line, static_cast<uint64_t>(line_len));
        }
        ::close(pipefd[0]);
        ker::process::exit(0);
        __builtin_unreachable();
    }

    // Parent: drain child owns the read end.
    ::close(pipefd[0]);
    return static_cast<uint64_t>(SERVICE_PID);
}

}  // namespace

void start_journald() {
    int const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%d]: spawning journald", CPUNO);
    std::array<const char*, 2> argv = {"/sbin/journald", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t const PID = ker::process::exec("/sbin/journald", argv.data(), envp.data());
    if (PID == 0) {
        init_log::warn("init[%d]: failed to spawn journald", CPUNO);
    } else {
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: journald spawned as PID %llu", CPUNO, static_cast<unsigned long long>(PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%d]: failed to lower journald priority (%lld)", CPUNO, static_cast<long long>(PRIO_RC));
        }
    }
}

void start_httpd() {
    int const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%d]: spawning httpd (HTTP server on port 80)", CPUNO);
    std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
    std::array<const char*, 1> httpd_envp = {nullptr};
    uint64_t const HTTPD_PID = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
    if (HTTPD_PID == 0) {
        init_log::error("init[%d]: failed to spawn httpd", CPUNO);
    } else {
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(HTTPD_PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: httpd spawned as PID %llu", CPUNO, static_cast<unsigned long long>(HTTPD_PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%d]: failed to lower httpd priority (%lld)", CPUNO, static_cast<long long>(PRIO_RC));
        }
    }
}

void start_dropbear() {
    int const CPUNO = ker::multiproc::currentThreadId();

    // Generate RSA host key if it doesn't exist
    int const KEY_FD = ker::abi::vfs::open("/etc/dropbear/dropbear_rsa_host_key", 0, 0);
    if (KEY_FD >= 0) {
        ker::abi::vfs::close(KEY_FD);
        init_log::info("init[%d]: dropbear host key already exists", CPUNO);
    } else {
        init_log::info("init[%d]: generating dropbear RSA host key...", CPUNO);
        std::array<const char*, 6> keygen_argv = {"/bin/dropbearkey", "-t", "rsa", "-f", "/etc/dropbear/dropbear_rsa_host_key", nullptr};
        std::array<const char*, 1> keygen_envp = {nullptr};
        uint64_t const KEYGEN_PID = spawn_with_journal_stdio("/bin/dropbearkey", keygen_argv.data(), keygen_envp.data(), "sshd");
        if (KEYGEN_PID == 0) {
            init_log::error("init[%d]: failed to spawn dropbearkey", CPUNO);
        } else {
            int exit_code = 0;
            ker::process::waitpid(static_cast<int64_t>(KEYGEN_PID), &exit_code, 0, nullptr);
            init_log::info("init[%d]: dropbearkey exited with code %d", CPUNO, exit_code);
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    init_log::info("init[%d]: spawning dropbear SSH server", CPUNO);
    std::array<const char*, 5> dropbear_argv = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                "-F",  // foreground, don't fork
                                                nullptr};
    std::array<const char*, 1> dropbear_envp = {nullptr};
    uint64_t const DROPBEAR_PID = spawn_with_journal_stdio("/bin/dropbear", dropbear_argv.data(), dropbear_envp.data(), "sshd");
    if (DROPBEAR_PID == 0) {
        init_log::error("init[%d]: failed to spawn dropbear", CPUNO);
    } else {
        int64_t const PRIO_RC = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(DROPBEAR_PID), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: dropbear spawned as PID %llu", CPUNO, static_cast<unsigned long long>(DROPBEAR_PID));
        if (PRIO_RC < 0) {
            init_log::warn("init[%d]: failed to lower dropbear priority (%lld)", CPUNO, static_cast<long long>(PRIO_RC));
        }
    }
}

void start_testd() {
    int const CPUNO = ker::multiproc::currentThreadId();

    // Only spawn testd if the binary is present (development/test builds only).
    int const PROBE_FD = ker::abi::vfs::open("/usr/bin/testd", 0, 0);
    if (PROBE_FD < 0) {
        return;
    }
    ker::abi::vfs::close(PROBE_FD);

    // Wait for all previously spawned services (netd, httpd, dropbear) to finish
    // initializing. There are no service-ready signals yet, so a fixed delay is used.
    init_log::info("init[%d]: testd: waiting 10s for services to settle...", CPUNO);
    struct timespec const SETTLE{.tv_sec = 10, .tv_nsec = 0};
    nanosleep(&SETTLE, nullptr);

    init_log::info("init[%d]: spawning testd (kernel test daemon)", CPUNO);
    std::array<const char*, 2> argv = {"/usr/bin/testd", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t const PID = ker::process::exec("/usr/bin/testd", argv.data(), envp.data());
    if (PID == 0) {
        init_log::warn("init[%d]: failed to spawn testd", CPUNO);
    } else {
        init_log::info("init[%d]: testd spawned as PID %llu", CPUNO, static_cast<unsigned long long>(PID));
    }
}
