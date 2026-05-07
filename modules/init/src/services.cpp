#include "services.h"

#include <sys/process.h>
#include <sys/vfs.h>
#include <time.h>

#include <array>
#include <cstdint>
#include <print>

#include "init_log.h"
#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
}

void start_journald() {
    int cpuno = ker::multiproc::currentThreadId();

    init_info("init[%d]: spawning journald", cpuno);
    std::array<const char*, 2> argv = {"/sbin/journald", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t pid = ker::process::exec("/sbin/journald", argv.data(), envp.data());
    if (pid == 0) {
        init_warn("init[%d]: failed to spawn journald", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(pid), BACKGROUND_SERVICE_NICE);
        init_info("init[%d]: journald spawned as PID %llu", cpuno, static_cast<unsigned long long>(pid));
        if (prio_rc < 0) {
            init_warn("init[%d]: failed to lower journald priority (%lld)", cpuno, static_cast<long long>(prio_rc));
        }
    }
}

void start_httpd() {
    int cpuno = ker::multiproc::currentThreadId();

    init_info("init[%d]: spawning httpd (HTTP server on port 80)", cpuno);
    std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
    std::array<const char*, 1> httpd_envp = {nullptr};
    uint64_t httpd_pid = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
    if (httpd_pid == 0) {
        init_error("init[%d]: failed to spawn httpd", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(httpd_pid), BACKGROUND_SERVICE_NICE);
        init_info("init[%d]: httpd spawned as PID %llu", cpuno, static_cast<unsigned long long>(httpd_pid));
        if (prio_rc < 0) {
            init_warn("init[%d]: failed to lower httpd priority (%lld)", cpuno, static_cast<long long>(prio_rc));
        }
    }
}

void start_dropbear() {
    int cpuno = ker::multiproc::currentThreadId();

    // Generate RSA host key if it doesn't exist
    int key_fd = ker::abi::vfs::open("/etc/dropbear/dropbear_rsa_host_key", 0, 0);
    if (key_fd >= 0) {
        ker::abi::vfs::close(key_fd);
        init_info("init[%d]: dropbear host key already exists", cpuno);
    } else {
        init_info("init[%d]: generating dropbear RSA host key...", cpuno);
        std::array<const char*, 6> keygen_argv = {"/bin/dropbearkey", "-t", "rsa", "-f", "/etc/dropbear/dropbear_rsa_host_key", nullptr};
        std::array<const char*, 1> keygen_envp = {nullptr};
        uint64_t keygen_pid = ker::process::exec("/bin/dropbearkey", keygen_argv.data(), keygen_envp.data());
        if (keygen_pid == 0) {
            init_error("init[%d]: failed to spawn dropbearkey", cpuno);
        } else {
            int exit_code = 0;
            ker::process::waitpid((int64_t)keygen_pid, &exit_code, 0, nullptr);
            init_info("init[%d]: dropbearkey exited with code %d", cpuno, exit_code);
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    init_info("init[%d]: spawning dropbear SSH server", cpuno);
    std::array<const char*, 5> dropbear_argv = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                "-F",  // foreground, don't fork
                                                nullptr};
    std::array<const char*, 1> dropbear_envp = {nullptr};
    uint64_t dropbear_pid = ker::process::exec("/bin/dropbear", dropbear_argv.data(), dropbear_envp.data());
    if (dropbear_pid == 0) {
        init_error("init[%d]: failed to spawn dropbear", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(dropbear_pid), BACKGROUND_SERVICE_NICE);
        init_info("init[%d]: dropbear spawned as PID %llu", cpuno, static_cast<unsigned long long>(dropbear_pid));
        if (prio_rc < 0) {
            init_warn("init[%d]: failed to lower dropbear priority (%lld)", cpuno, static_cast<long long>(prio_rc));
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
    init_info("init[%d]: testd: waiting 10s for services to settle...", cpuno);
    struct timespec settle{ .tv_sec = 10, .tv_nsec = 0 };
    nanosleep(&settle, nullptr);

    init_info("init[%d]: spawning testd (kernel test daemon)", cpuno);
    std::array<const char*, 2> argv = {"/usr/bin/testd", nullptr};
    std::array<const char*, 1> envp = {nullptr};
    uint64_t pid = ker::process::exec("/usr/bin/testd", argv.data(), envp.data());
    if (pid == 0) {
        init_warn("init[%d]: failed to spawn testd", cpuno);
    } else {
        init_info("init[%d]: testd spawned as PID %llu", cpuno, static_cast<unsigned long long>(pid));
    }
}
