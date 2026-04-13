#include "services.h"

#include <sys/process.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <print>

#include "sys/multiproc.h"

namespace {
constexpr int BACKGROUND_SERVICE_NICE = 10;
}

void start_httpd() {
    int cpuno = ker::multiproc::currentThreadId();

    std::println("init[{}]: spawning httpd (HTTP server on port 80)", cpuno);
    std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
    std::array<const char*, 1> httpd_envp = {nullptr};
    uint64_t httpd_pid = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
    if (httpd_pid == 0) {
        std::println("init[{}]: FAILED to spawn httpd", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(httpd_pid), BACKGROUND_SERVICE_NICE);
        std::println("init[{}]: httpd spawned as PID {}", cpuno, httpd_pid);
        if (prio_rc < 0) {
            std::println("init[{}]: WARNING: failed to lower httpd priority ({})", cpuno, prio_rc);
        }
    }
}

void start_dropbear() {
    int cpuno = ker::multiproc::currentThreadId();

    // Generate RSA host key if it doesn't exist
    int key_fd = ker::abi::vfs::open("/etc/dropbear/dropbear_rsa_host_key", 0, 0);
    if (key_fd >= 0) {
        ker::abi::vfs::close(key_fd);
        std::println("init[{}]: dropbear host key already exists", cpuno);
    } else {
        std::println("init[{}]: generating dropbear RSA host key...", cpuno);
        std::array<const char*, 6> keygen_argv = {"/bin/dropbearkey", "-t", "rsa", "-f", "/etc/dropbear/dropbear_rsa_host_key", nullptr};
        std::array<const char*, 1> keygen_envp = {nullptr};
        uint64_t keygen_pid = ker::process::exec("/bin/dropbearkey", keygen_argv.data(), keygen_envp.data());
        if (keygen_pid == 0) {
            std::println("init[{}]: FAILED to spawn dropbearkey", cpuno);
        } else {
            int exit_code = 0;
            ker::process::waitpid((int64_t)keygen_pid, &exit_code, 0, nullptr);
            std::println("init[{}]: dropbearkey exited with code {}", cpuno, exit_code);
        }
    }

    // Start dropbear in foreground mode (no fork/daemon)
    std::println("init[{}]: spawning dropbear SSH server", cpuno);
    std::array<const char*, 5> dropbear_argv = {"/bin/dropbear", "-r", "/etc/dropbear/dropbear_rsa_host_key",
                                                "-F",  // foreground, don't fork
                                                nullptr};
    std::array<const char*, 1> dropbear_envp = {nullptr};
    uint64_t dropbear_pid = ker::process::exec("/bin/dropbear", dropbear_argv.data(), dropbear_envp.data());
    if (dropbear_pid == 0) {
        std::println("init[{}]: FAILED to spawn dropbear", cpuno);
    } else {
        int64_t prio_rc = ker::process::setpriority(PRIO_PROCESS, static_cast<int64_t>(dropbear_pid), BACKGROUND_SERVICE_NICE);
        std::println("init[{}]: dropbear spawned as PID {}", cpuno, dropbear_pid);
        if (prio_rc < 0) {
            std::println("init[{}]: WARNING: failed to lower dropbear priority ({})", cpuno, prio_rc);
        }
    }
}
