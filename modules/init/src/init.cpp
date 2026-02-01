#include <cstdio>
#define _DEFAULT_SOURCE 1

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <print>
#include <span>
#include <string_view>

#include "bits/ssize_t.h"
#include "callnums/sys_log.h"
#include "sys/callnums.h"
#include "sys/multiproc.h"

namespace {
constexpr size_t buffer_size = 64;
constexpr int NUM_SUB_INITS = 20;
constexpr size_t FSTAB_BUF_SIZE = 4096;
constexpr size_t FIELD_MAX = 256;

// Simple atoi implementation
int simple_atoi(const char* str) {
    int result = 0;
    while (*str >= '0' && *str <= '9') {
        result = result * 10 + (*str - '0');
        str++;
    }
    return result;
}

// Simple itoa for small numbers (0-99)
void int_to_str(int val, char* buf) {
    if (val == 0) {
        buf[0] = '0';
        buf[1] = '\0';
        return;
    }
    int i = 0;
    char tmp[16];
    while (val > 0) {
        tmp[i++] = '0' + (val % 10);
        val /= 10;
    }
    int j = 0;
    while (i > 0) {
        buf[j++] = tmp[--i];
    }
    buf[j] = '\0';
}

}  // namespace

auto main(int argc, char** argv) -> int {
    int cpuno = ker::multiproc::currentThreadId();

    // Determine our role based on argc:
    // argc == 1: We are the root init - spawn sub-inits
    // argc >= 3: We are a sub-init - argv[1] = count, argv[2] = program to spawn

    if (argc >= 3) {
        // === SUB-INIT MODE ===
        // argv[0] = our path
        // argv[1] = number of programs to spawn
        // argv[2] = program path to spawn
        int spawn_count = simple_atoi(argv[1]);
        const char* prog_path = argv[2];

        std::println("sub-init[{}]: Starting - will spawn {} instances of '{}'", cpuno, spawn_count, prog_path);

        for (int i = 0; i < spawn_count; i++) {
            std::array<const char*, 4> child_argv = {prog_path, "child-arg1", "child-arg2", nullptr};
            std::array<const char*, 1> child_envp = {nullptr};

            uint64_t child_pid = ker::process::exec(prog_path, child_argv.data(), child_envp.data());
            if (child_pid == 0) {
                std::println("sub-init[{}]: Failed to exec '{}' (instance {})", cpuno, prog_path, i);
            } else {
                std::println("sub-init[{}]: Spawned '{}' as PID {} (instance {}/{})", cpuno, prog_path, child_pid, i + 1, spawn_count);
                int exit_code = 0;
                ker::process::waitpid((int64_t)child_pid, &exit_code, 0);
                std::println("sub-init[{}]: Child PID {} exited with code {}", cpuno, child_pid, exit_code);
            }
        }

        std::println("sub-init[{}]: All children completed, exiting", cpuno);
        return 0;
    }

    // === ROOT INIT MODE ===
    std::println("init[{}]: ROOT INIT starting", cpuno);

    // --- Mount filesystems from /etc/fstab ---
    {
        int fstab_fd = ker::abi::vfs::open("/etc/fstab", 0, 0);
        if (fstab_fd >= 0) {
            std::array<char, FSTAB_BUF_SIZE> fstab_buf{};
            ssize_t bytes_read = ker::abi::vfs::read(fstab_fd, fstab_buf.data(), FSTAB_BUF_SIZE - 1);
            ker::abi::vfs::close(fstab_fd);

            if (bytes_read > 0) {
                fstab_buf[static_cast<size_t>(bytes_read)] = '\0';
                std::println("init[{}]: parsing /etc/fstab ({} bytes)", cpuno, bytes_read);

                // Parse line by line
                char* line_start = fstab_buf.data();
                while (*line_start != '\0') {
                    // Find end of line
                    char* line_end = line_start;
                    while (*line_end != '\0' && *line_end != '\n') {
                        line_end++;
                    }
                    char saved = *line_end;
                    *line_end = '\0';

                    // Skip whitespace
                    char* p = line_start;
                    while (*p == ' ' || *p == '\t') {
                        p++;
                    }

                    // Skip comments and empty lines
                    if (*p != '#' && *p != '\0') {
                        // Extract fields: device mountpoint fstype options
                        std::array<char, FIELD_MAX> device{};
                        std::array<char, FIELD_MAX> mountpoint{};
                        std::array<char, FIELD_MAX> fstype{};

                        // Parse device field
                        size_t fi = 0;
                        while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                            device[fi++] = *p++;
                        }
                        device[fi] = '\0';

                        // Skip whitespace
                        while (*p == ' ' || *p == '\t') {
                            p++;
                        }

                        // Parse mountpoint field
                        fi = 0;
                        while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                            mountpoint[fi++] = *p++;
                        }
                        mountpoint[fi] = '\0';

                        // Skip whitespace
                        while (*p == ' ' || *p == '\t') {
                            p++;
                        }

                        // Parse fstype field
                        fi = 0;
                        while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                            fstype[fi++] = *p++;
                        }
                        fstype[fi] = '\0';

                        if (device[0] != '\0' && mountpoint[0] != '\0' && fstype[0] != '\0') {
                            // Create mount point directory
                            ker::abi::vfs::mkdir(mountpoint.data(), 0755);

                            // Mount filesystem
                            int ret = ker::abi::vfs::mount(device.data(), mountpoint.data(), fstype.data());
                            if (ret == 0) {
                                std::println("init[{}]: mounted {} at {} ({})", cpuno, device.data(), mountpoint.data(), fstype.data());
                            } else {
                                std::println(
                                    "init[{}]: FAILED to mount {} at {} "
                                    "({}): error {}",
                                    cpuno, device.data(), mountpoint.data(), fstype.data(), ret);
                            }
                        }
                    }

                    // Advance to next line
                    if (saved == '\0') {
                        break;
                    }
                    line_start = line_end + 1;
                }
            } else {
                std::println("init[{}]: /etc/fstab is empty", cpuno);
            }
        } else {
            std::println("init[{}]: no /etc/fstab found, skipping mounts", cpuno);
        }
    }

    // --- Spawn netd (DHCP network daemon) ---
    {
        std::println("init[{}]: spawning netd (DHCP daemon)", cpuno);
        std::array<const char*, 2> netd_argv = {"/sbin/netd", nullptr};
        std::array<const char*, 1> netd_envp = {nullptr};
        uint64_t netd_pid = ker::process::exec("/sbin/netd", netd_argv.data(), netd_envp.data());
        if (netd_pid == 0) {
            std::println("init[{}]: FAILED to spawn netd", cpuno);
        } else {
            std::println("init[{}]: netd spawned as PID {}", cpuno, netd_pid);
        }

        // Poll eth0 for IP address readiness (wait for DHCP to complete)
        int poll_sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (poll_sock >= 0) {
            struct ifreq ifr{};
            strncpy(ifr.ifr_name, "eth0", IFNAMSIZ - 1);

            constexpr long poll_timeout_secs = 10;
            struct timespec poll_start{};
            clock_gettime(CLOCK_MONOTONIC, &poll_start);
            bool net_ready = false;
            for (;;) {
                if (ioctl(poll_sock, SIOCGIFADDR, &ifr) == 0) {
                    auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
                    if (addr->sin_addr.s_addr != 0) {
                        char ip_str[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str));
                        std::println("init[{}]: eth0 configured with IP {}", cpuno, ip_str);
                        net_ready = true;
                        break;
                    }
                }
                struct timespec now{};
                clock_gettime(CLOCK_MONOTONIC, &now);
                if (now.tv_sec - poll_start.tv_sec >= poll_timeout_secs) {
                    break;
                }
                sched_yield();
            }
            close(poll_sock);
            if (!net_ready) {
                std::println("init[{}]: WARNING: eth0 not configured after polling, continuing anyway", cpuno);
            }
        }
    }

    // --- Spawn httpd (HTTP server on port 80) ---
    {
        std::println("init[{}]: spawning httpd (HTTP server on port 80)", cpuno);
        std::array<const char*, 2> httpd_argv = {"/sbin/httpd", nullptr};
        std::array<const char*, 1> httpd_envp = {nullptr};
        uint64_t httpd_pid = ker::process::exec("/sbin/httpd", httpd_argv.data(), httpd_envp.data());
        if (httpd_pid == 0) {
            std::println("init[{}]: FAILED to spawn httpd", cpuno);
        } else {
            std::println("init[{}]: httpd spawned as PID {}", cpuno, httpd_pid);
        }
    }

    // --- Spawn sub-init processes ---
    std::println("init[{}]: Will spawn {} sub-init processes", cpuno, NUM_SUB_INITS);

    // Configuration for each sub-init: (spawn_count, program_path)
    struct SubInitConfig {
        int spawn_count;
        const char* program;
    };

    std::array<SubInitConfig, NUM_SUB_INITS> configs = {{
        {.spawn_count = 2, .program = "/mnt/disk/testprog"}, {.spawn_count = 3, .program = "/mnt/disk/testprog"},
        {.spawn_count = 1, .program = "/mnt/disk/testprog"}, {.spawn_count = 2, .program = "/mnt/disk/testprog"},
        {.spawn_count = 1, .program = "/mnt/disk/testprog"}, {.spawn_count = 2, .program = "/mnt/disk/testprog"},
        {.spawn_count = 3, .program = "/mnt/disk/testprog"}, {.spawn_count = 1, .program = "/mnt/disk/testprog"},
        {.spawn_count = 2, .program = "/mnt/disk/testprog"}, {.spawn_count = 1, .program = "/mnt/disk/testprog"},
        {.spawn_count = 2, .program = "/mnt/disk/testprog"}, {.spawn_count = 3, .program = "/mnt/disk/testprog"},
        {.spawn_count = 1, .program = "/mnt/disk/testprog"}, {.spawn_count = 2, .program = "/mnt/disk/testprog"},
        {.spawn_count = 1, .program = "/mnt/disk/testprog"}, {.spawn_count = 2, .program = "/mnt/disk/testprog"},
        {.spawn_count = 3, .program = "/mnt/disk/testprog"}, {.spawn_count = 1, .program = "/mnt/disk/testprog"},
        {.spawn_count = 2, .program = "/mnt/disk/testprog"}, {.spawn_count = 1, .program = "/mnt/disk/testprog"},
    }};

    // Spawn sub-inits
    // std::array<uint64_t, NUM_SUB_INITS> sub_init_pids = {0};

    // for (int i = 0; i < NUM_SUB_INITS; i++) {
    //     std::array<char, 16> count_str = {};
    //     int_to_str(configs[i].spawn_count, count_str.data());

    //     // argv: [init_path, spawn_count, program_path, nullptr]
    //     std::array<const char*, 4> sub_argv = {"/sbin/init",        // argv[0]: our own path
    //                                            count_str.data(),    // argv[1]: number of programs to spawn
    //                                            configs[i].program,  // argv[2]: program to spawn
    //                                            nullptr};
    //     std::array<const char*, 1> sub_envp = {nullptr};

    //     uint64_t pid = ker::process::exec("/mnt/disk/init", sub_argv.data(), sub_envp.data());
    //     if (pid == 0) {
    //         std::println("init[{}]: Failed to spawn sub-init {}", cpuno, i);
    //     } else {
    //         sub_init_pids[i] = pid;
    //         std::println("init[{}]: Spawned sub-init {} as PID {} (will spawn {} x '{}')", cpuno, i, pid, configs[i].spawn_count,
    //                      configs[i].program);
    //     }
    // }

    // // Wait for all sub-inits to complete
    // std::println("init[{}]: Waiting for all sub-inits to complete...", cpuno);
    // for (int i = 0; i < NUM_SUB_INITS; i++) {
    //     if (sub_init_pids[i] != 0) {
    //         int exit_code = 0;
    //         ker::process::waitpid((int64_t)sub_init_pids[i], &exit_code, 0);
    //         std::println("init[{}]: Sub-init {} (PID {}) exited with code {}", cpuno, i, sub_init_pids[i], exit_code);
    //     }
    // }

    // std::println("init[{}]: All sub-inits completed! Process tree demo finished.", cpuno);
    // std::println("init[{}]: Total processes spawned: {} sub-inits + {} leaf programs", cpuno, NUM_SUB_INITS, 2 + 3 + 1 + 2 + 1);

    // --- Stress test: spawn 50,000 testprog instances in batches ---
    // {
    //     constexpr int STRESS_TOTAL = 30;
    //     constexpr int STRESS_BATCH = 10;

    //     std::println("init[{}]: === STRESS TEST: spawning {} processes in batches of {} ===", cpuno, STRESS_TOTAL, STRESS_BATCH);

    //     int total_spawned = 0;
    //     int total_completed = 0;
    //     int total_failed = 0;

    //     for (int batch = 0; total_spawned + total_failed < STRESS_TOTAL; batch++) {
    //         int this_batch = STRESS_BATCH;
    //         if (total_spawned + total_failed + this_batch > STRESS_TOTAL) {
    //             this_batch = STRESS_TOTAL - total_spawned - total_failed;
    //         }

    //         std::array<uint64_t, STRESS_BATCH> pids = {};
    //         int spawned = 0;

    //         // Spawn entire batch before waiting
    //         for (int i = 0; i < this_batch; i++) {
    //             std::array<const char*, 2> child_argv = {"/mnt/disk/testprog", nullptr};
    //             std::array<const char*, 1> child_envp = {nullptr};
    //             uint64_t pid = ker::process::exec("/mnt/disk/testprog", child_argv.data(), child_envp.data());
    //             if (pid != 0) {
    //                 pids[spawned++] = pid;
    //             } else {
    //                 total_failed++;
    //             }
    //         }
    //         total_spawned += spawned;

    //         // Wait for all in this batch
    //         for (int i = 0; i < spawned; i++) {
    //             int exit_code = 0;
    //             ker::process::waitpid(static_cast<int64_t>(pids[i]), &exit_code, 0);
    //             std::println("init[{}]: stress batch {}: child PID {} exited with code {}", cpuno, batch, pids[i], exit_code);
    //             total_completed++;
    //         }

    //         std::println("init[{}]: stress batch {}: spawned={}, done={}/{}, failed={}", cpuno, batch, spawned, total_completed,
    //                      STRESS_TOTAL, total_failed);
    //     }

    //     std::println("init[{}]: === STRESS TEST COMPLETE: spawned={}, completed={}, failed={} ===", cpuno, total_spawned,
    //     total_completed,
    //                  total_failed);
    // }

    // Keep init alive
    for (;;) {
        asm volatile("pause");
    }

    return 0;
}
