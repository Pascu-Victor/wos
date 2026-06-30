#include "wkictl/target.hpp"

#include <sys/process.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <print>
#include <string>

#include "wkictl/cli.hpp"

namespace {

auto flags_name(uint32_t flags) -> std::string {
    std::string out;
    auto append = [&out](const char* text) {
        if (!out.empty()) {
            out += '|';
        }
        out += text;
    };
    if ((flags & ker::process::WKI_TARGET_FLAG_STRICT) != 0) {
        append("strict");
    }
    if ((flags & ker::process::WKI_TARGET_FLAG_LOCAL) != 0) {
        append("local");
    }
    if ((flags & ker::process::WKI_TARGET_FLAG_REMOTE) != 0) {
        append("remote");
    }
    if ((flags & ker::process::WKI_TARGET_FLAG_NOINHERIT) != 0) {
        append("noinherit");
    }
    if (out.empty()) {
        out = "auto";
    }
    return out;
}

auto print_target() -> int {
    std::array<char, 64> hostname = {};
    uint32_t flags = 0;
    int64_t rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc < 0) {
        std::println(stderr, "wkictl: target get failed: {}", static_cast<long>(rc));
        return 1;
    }
    if (rc == 0) {
        std::println("target: host=<auto> flags={}", flags_name(flags));
    } else {
        std::println("target: host={} flags={}", hostname.data(), flags_name(flags));
    }
    return 0;
}

auto parse_policy_flags(int argc, char** argv, int start, uint32_t base, uint32_t* out) -> bool {
    uint32_t flags = base;
    for (int i = start; i < argc; ++i) {
        if (std::strcmp(argv[i], "strict") == 0) {
            flags |= ker::process::WKI_TARGET_FLAG_STRICT;
        } else if (std::strcmp(argv[i], "fallback") == 0 || std::strcmp(argv[i], "best-effort") == 0) {
            flags &= ~ker::process::WKI_TARGET_FLAG_STRICT;
        } else if (std::strcmp(argv[i], "noinherit") == 0) {
            flags |= ker::process::WKI_TARGET_FLAG_NOINHERIT;
        } else {
            return false;
        }
    }
    *out = flags;
    return true;
}

}  // namespace

namespace wkictl {

auto run_locally(int argc, char** argv) -> int {
    if (argc < 2) {
        return usage();
    }
    int64_t rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL);
    if (rc < 0) {
        std::println(stderr, "locally: failed to set local policy: {}", static_cast<long>(rc));
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
        std::println(stderr, "remotely: failed to set remote-preferred policy: {}", static_cast<long>(rc));
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
        std::println(stderr, "on: failed to target '{}': {}", hostname, static_cast<long>(rc));
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
        std::println(stderr, "homeward: failed to resolve launcher node");
        return 1;
    }

    int64_t rc = ker::process::setwkitarget(launcher.data(), static_cast<uint64_t>(LAUNCHER_LEN), ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc < 0) {
        std::println(stderr, "homeward: failed to target launcher '{}': {}", launcher.data(), static_cast<long>(rc));
        return 1;
    }
    return exec_command(argv + 1);
}

auto handle_target(int argc, char** argv) -> int {
    if (argc < 3 || std::strcmp(argv[2], "show") == 0) {
        return print_target();
    }
    if (std::strcmp(argv[2], "clear") == 0 || std::strcmp(argv[2], "auto") == 0) {
        int64_t rc = ker::process::setwkitarget(nullptr, 0, 0);
        if (rc < 0) {
            std::println(stderr, "wkictl: target clear failed: {}", static_cast<long>(rc));
            return 1;
        }
        return print_target();
    }
    if (std::strcmp(argv[2], "set") != 0 || argc < 4) {
        return usage();
    }

    const char* policy = argv[3];
    uint32_t flags = 0;
    const char* hostname = nullptr;
    if (std::strcmp(policy, "local") == 0) {
        flags = ker::process::WKI_TARGET_FLAG_LOCAL;
    } else if (std::strcmp(policy, "remote") == 0 || std::strcmp(policy, "remotely") == 0) {
        flags = ker::process::WKI_TARGET_FLAG_REMOTE;
    } else if (std::strcmp(policy, "auto") == 0) {
        flags = 0;
    } else {
        hostname = policy;
        flags = ker::process::WKI_TARGET_FLAG_STRICT;
    }
    if (!parse_policy_flags(argc, argv, 4, flags, &flags)) {
        return usage();
    }

    int64_t rc = hostname == nullptr ? ker::process::setwkitarget(nullptr, 0, flags)
                                     : ker::process::setwkitarget(hostname, std::strlen(hostname), flags);
    if (rc < 0) {
        std::println(stderr, "wkictl: target set failed: {}", static_cast<long>(rc));
        return 1;
    }
    return print_target();
}

}  // namespace wkictl
