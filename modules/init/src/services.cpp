#include "services.h"

#include <sys/logging.h>
#include <sys/process.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <print>

#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
using init_log = wos::journal<"init">;

// Spawn a process with stdout and stderr captured to the journal under `tag`.
// A drain child reads lines from the pipe and emits them as INFO journal entries.
// Returns the service PID, or 0 on failure.
static uint64_t spawn_with_journal_stdio(const char* path, const char* const argv[], const char* const envp[], const char* tag) {
    int pipefd[2];
    if (::pipe(pipefd) != 0) {
        return 0;
    }

    int64_t service_pid = ker::process::fork();
    if (service_pid < 0) {
        ::close(pipefd[0]);
        ::close(pipefd[1]);
        return 0;
    }
    if (service_pid == 0) {
        // Service child: redirect stdout and stderr to the write end, then exec.
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

    int64_t drain_pid = ker::process::fork();
    if (drain_pid < 0) {
        ::close(pipefd[0]);
        return static_cast<uint64_t>(service_pid);
    }
    if (drain_pid == 0) {
        // Drain child: read lines from the pipe and emit each to the journal.
        char buf[512];
        char line[512];
        int line_len = 0;
        ssize_t n;
        while ((n = ::read(pipefd[0], buf, sizeof(buf))) > 0) {
            for (ssize_t i = 0; i < n; i++) {
                char c = buf[i];
                if (c == '\n' || c == '\r') {
                    if (line_len > 0) {
                        line[line_len] = '\0';
                        ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::info, line, static_cast<uint64_t>(line_len));
                        line_len = 0;
                    }
                } else if (line_len < static_cast<int>(sizeof(line)) - 1) {
                    line[line_len++] = c;
                }
            }
        }
        // Flush any partial line without trailing newline.
        if (line_len > 0) {
            line[line_len] = '\0';
            ker::logging::logEx(tag, ker::abi::sys_log::sys_log_level::info, line, static_cast<uint64_t>(line_len));
        }
        ::close(pipefd[0]);
        ker::process::exit(0);
        __builtin_unreachable();
    }

    // Parent: drain child owns the read end.
    ::close(pipefd[0]);
    return static_cast<uint64_t>(service_pid);
}

}  // namespace

void start_journald() {
    int cpuno = ker::multiproc::currentThreadId();

    init_log::info("init[%d]: spawning journald", cpuno);
    std::array<const char*, 2> argv = {"/sbin/journald", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t pid = ker::process::exec("/sbin/journald", argv.data(), envp.data());
    if (pid == 0) {
        init_log::warn("init[%d]: failed to spawn journald", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(pid), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: journald spawned as PID %llu", cpuno, static_cast<unsigned long long>(pid));
        if (prio_rc < 0) {
            init_log::warn("init[%d]: failed to lower journald priority (%lld)", cpuno, static_cast<long long>(prio_rc));
        }
    }
}

void start_httpd() {
    int cpuno = ker::multiproc::currentThreadId();

    init_log::info("init[%d]: spawning httpd (HTTP server on port 80)", cpuno);
    std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
    std::array<const char*, 1> httpd_envp = {nullptr};
    uint64_t httpd_pid = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
    if (httpd_pid == 0) {
        init_log::error("init[%d]: failed to spawn httpd", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(httpd_pid), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: httpd spawned as PID %llu", cpuno, static_cast<unsigned long long>(httpd_pid));
        if (prio_rc < 0) {
            init_log::warn("init[%d]: failed to lower httpd priority (%lld)", cpuno, static_cast<long long>(prio_rc));
        }
    }
}

void start_dropbear() {
    int cpuno = ker::multiproc::currentThreadId();

    // Generate RSA host key if it doesn't exist
    int key_fd = ker::abi::vfs::open("/etc/dropbear/dropbear_rsa_host_key", 0, 0);
    if (key_fd >= 0) {
        ker::abi::vfs::close(key_fd);
        init_log::info("init[%d]: dropbear host key already exists", cpuno);
    } else {
        init_log::info("init[%d]: generating dropbear RSA host key...", cpuno);
        std::array<const char*, 6> keygen_argv = {"/bin/dropbearkey", "-t", "rsa", "-f", "/etc/dropbear/dropbear_rsa_host_key", nullptr};
        std::array<const char*, 1> keygen_envp = {nullptr};
        uint64_t keygen_pid = spawn_with_journal_stdio("/bin/dropbearkey", keygen_argv.data(), keygen_envp.data(), "sshd");
        if (keygen_pid == 0) {
            init_log::error("init[%d]: failed to spawn dropbearkey", cpuno);
        } else {
            int exit_code = 0;
            ker::process::waitpid((int64_t)keygen_pid, &exit_code, 0, nullptr);
            init_log::info("init[%d]: dropbearkey exited with code %d", cpuno, exit_code);
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    init_log::info("init[%d]: spawning dropbear SSH server", cpuno);
    std::array<const char*, 5> dropbear_argv = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                "-F",  // foreground, don't fork
                                                nullptr};
    std::array<const char*, 1> dropbear_envp = {nullptr};
    uint64_t dropbear_pid = spawn_with_journal_stdio("/bin/dropbear", dropbear_argv.data(), dropbear_envp.data(), "sshd");
    if (dropbear_pid == 0) {
        init_log::error("init[%d]: failed to spawn dropbear", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(dropbear_pid), BACKGROUND_SERVICE_NICE);
        init_log::info("init[%d]: dropbear spawned as PID %llu", cpuno, static_cast<unsigned long long>(dropbear_pid));
        if (prio_rc < 0) {
            init_log::warn("init[%d]: failed to lower dropbear priority (%lld)", cpuno, static_cast<long long>(prio_rc));
        }
    }
}

void start_testd() {
    int cpuno = ker::multiproc::currentThreadId();

    // Only spawn testd if the binary is present (development/test builds only).
    int probe_fd = ker::abi::vfs::open("/usr/bin/testd", 0, 0);
    if (probe_fd < 0) {
        return;
    }
    ker::abi::vfs::close(probe_fd);

    // Wait for all previously spawned services (netd, httpd, dropbear) to finish
    // initializing. There are no service-ready signals yet, so a fixed delay is used.
    init_log::info("init[%d]: testd: waiting 10s for services to settle...", cpuno);
    struct timespec settle{.tv_sec = 10, .tv_nsec = 0};
    nanosleep(&settle, nullptr);

    init_log::info("init[%d]: spawning testd (kernel test daemon)", cpuno);
    std::array<const char*, 2> argv = {"/usr/bin/testd", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t pid = ker::process::exec("/usr/bin/testd", argv.data(), envp.data());
    if (pid == 0) {
        init_log::warn("init[%d]: failed to spawn testd", cpuno);
    } else {
        init_log::info("init[%d]: testd spawned as PID %llu", cpuno, static_cast<unsigned long long>(pid));
    }
}
