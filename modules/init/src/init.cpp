#include <sys/logging.h>
#include <sys/process.h>
#include <sys/vfs.h>

#include <array>
#include <cstdint>
#include <ctime>
#include <utility>

#include "fstab.h"
#include "network.h"
#include "services.h"
#include "sys/multiproc.h"

namespace {

using init_log = wos::journal<"init">;

// Simple atoi implementation
int simple_atoi(const char* str) {
    int result = 0;
    while (*str >= '0' && *str <= '9') {
        result = (result * 10) + (*str - '0');
        str++;
    }
    return result;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    int const CPUNO = ker::multiproc::currentThreadId();

    // Determine our role based on argc:
    // argc == 1: We are the root init - spawn sub-inits
    // argc >= 3: We are a sub-init - argv[1] = count, argv[2] = program to spawn

    if (argc >= 3) {
        // === SUB-INIT MODE ===
        // argv[0] = our path
        // argv[1] = number of programs to spawn
        // argv[2] = program path to spawn
        int const SPAWN_COUNT = simple_atoi(argv[1]);
        const char* prog_path = argv[2];

        init_log::info("sub-init[%d]: starting - will spawn %d instances of '%s'", CPUNO, SPAWN_COUNT, prog_path);

        for (int i = 0; i < SPAWN_COUNT; i++) {
            std::array<const char*, 4> child_argv = {prog_path, "child-arg1", "child-arg2", nullptr};
            std::array<const char*, 1> child_envp = {nullptr};

            uint64_t const CHILD_PID = ker::process::exec(prog_path, child_argv.data(), child_envp.data());
            if (CHILD_PID == 0) {
                init_log::error("sub-init[%d]: failed to exec '%s' (instance %d)", CPUNO, prog_path, i);
            } else {
                init_log::info("sub-init[%d]: spawned '%s' as PID %llu (instance %d/%d)", CPUNO, prog_path,
                               static_cast<unsigned long long>(CHILD_PID), i + 1, SPAWN_COUNT);
                int exit_code = 0;
                ker::process::waitpid(static_cast<int64_t>(CHILD_PID), &exit_code, 0, nullptr);
                init_log::info("sub-init[%d]: child PID %llu exited with code %d", CPUNO, static_cast<unsigned long long>(CHILD_PID),
                               exit_code);
            }
        }

        init_log::info("sub-init[%d]: all children completed, exiting", CPUNO);
        return 0;
    }

    // === ROOT INIT MODE ===
    init_log::info("init[%d]: root init starting", CPUNO);

    // Pin init itself to the local node. NOINHERIT makes forked children start
    // with automatic WKI placement; service launch code re-pins daemons locally
    // before exec, and login/session boundaries can opt descendants back into
    // automatic placement.
    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);

    mount_filesystems();

    // Pivot root from initramfs to the real rootfs.
    // After mount_filesystems(), the XFS rootfs is mounted at /rootfs.
    // pivot_root makes it appear as "/" for this task and all children.
    int const PIVOT_RET = ker::abi::vfs::pivot_root_vfs("/rootfs", "/rootfs/oldroot");
    if (PIVOT_RET < 0) {
        init_log::warn("init[%d]: pivot_root failed (ret=%d), continuing with initramfs root", CPUNO, PIVOT_RET);
    } else {
        init_log::info("init[%d]: pivot_root succeeded, root is now /rootfs", CPUNO);

        // Recreate /wki on the new root filesystem so WKI remote VFS mounts
        // survive the old initramfs root being unmounted.
        ker::abi::vfs::mkdir("/wki", 0755);

        // Unmount the old initramfs root to free its RAM.
        int const UMOUNT_RET = ker::abi::vfs::umount("/oldroot");
        if (UMOUNT_RET < 0) {
            init_log::warn("init[%d]: umount /oldroot failed (ret=%d)", CPUNO, UMOUNT_RET);
        } else {
            init_log::info("init[%d]: unmounted old initramfs root", CPUNO);
        }
    }

    ker::abi::vfs::mkdir("/var", 0755);
    ker::abi::vfs::mkdir("/var/log", 0755);
    ker::abi::vfs::mkdir("/var/log/journal", 0755);
    start_journald();
    start_network();
    start_httpd();
    start_dropbear();
    // start_testd();

    // Keep init alive and reap orphaned zombie children.
    // When children are still alive, block in waitpid() until one exits.
    // If init temporarily has no children at all, sleep instead of spinning.
    for (;;) {
        int32_t reap_status = 0;
        auto reap_pid = ker::process::waitpid(-1, &reap_status, 0, nullptr);
        if (std::cmp_equal(reap_pid, -1)) {
            struct timespec const IDLE_SLEEP{
                .tv_sec = 1,
                .tv_nsec = 0,
            };
            nanosleep(&IDLE_SLEEP, nullptr);
        }
    }

    return 0;
}
