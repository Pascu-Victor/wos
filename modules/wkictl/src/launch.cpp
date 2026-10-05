#include <sys/process.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include "wkictl/cli.hpp"
#include "wkictl/target.hpp"

namespace wkictl {

auto set_target_policy(const char* policy, uint32_t extra_flags) -> int64_t {
    if (policy == nullptr || policy[0] == '\0') {
        return -EINVAL;
    }

    uint32_t flags = extra_flags;
    const char* hostname = nullptr;
    if (std::strcmp(policy, "local") == 0) {
        flags |= ker::process::WKI_TARGET_FLAG_LOCAL;
    } else if (std::strcmp(policy, "remote") == 0 || std::strcmp(policy, "remotely") == 0) {
        flags |= ker::process::WKI_TARGET_FLAG_REMOTE;
    } else if (std::strcmp(policy, "balanced") == 0 || std::strcmp(policy, "anywhere") == 0) {
        flags |= ker::process::WKI_TARGET_FLAG_BALANCED;
    } else if (std::strcmp(policy, "auto") == 0) {
        if (extra_flags != 0) {
            return -EINVAL;
        }
    } else {
        hostname = policy;
        flags |= ker::process::WKI_TARGET_FLAG_STRICT;
    }

    return hostname == nullptr ? ker::process::setwkitarget(nullptr, 0, flags)
                               : ker::process::setwkitarget(hostname, std::strlen(hostname), flags);
}

auto run_locally(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }
    int64_t rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (rc < 0) {
        std::fprintf(stderr, "locally: failed to set local policy: %ld\n", static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

auto run_remotely(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }
    int64_t rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    if (rc < 0) {
        std::fprintf(stderr, "remotely: failed to set remote-preferred policy: %ld\n", static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

auto run_anywhere(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }
    int64_t rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_BALANCED);
    if (rc < 0) {
        std::fprintf(stderr, "anywhere: failed to set balanced policy: %ld\n", static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

auto run_on(int argc, char** argv) -> int {
    if (argc < 3) {
        return usage();
    }
    const char* hostname = argv[1];
    int64_t rc = ker::process::setwkitarget(hostname, std::strlen(hostname), ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc < 0) {
        std::fprintf(stderr, "on: failed to target '%s': %ld\n", hostname, static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 2);
}

auto run_homeward(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }

    std::array<char, 64> launcher = {};
    int64_t const LAUNCHER_LEN = ker::process::wki_launcher_node(launcher.data(), launcher.size());
    if (LAUNCHER_LEN <= 0 || launcher.front() == '\0') {
        std::fputs("homeward: failed to resolve launcher node\n", stderr);
        return 1;
    }

    int64_t rc = ker::process::setwkitarget(launcher.data(), static_cast<uint64_t>(LAUNCHER_LEN), ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc < 0) {
        std::fprintf(stderr, "homeward: failed to target launcher '%s': %ld\n", launcher.data(), static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

}  // namespace wkictl
